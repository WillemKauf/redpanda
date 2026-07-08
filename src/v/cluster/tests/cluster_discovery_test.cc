// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "cluster/bootstrap_types.h"
#include "cluster/cluster_bootstrap_service.h"
#include "cluster/cluster_discovery.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "net/dns.h"
#include "net/server.h"
#include "rpc/rpc_server.h"
#include "test_utils/boost_fixture.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <optional>

using namespace std::chrono_literals;

namespace {

// Minimal bootstrap service that answers cluster_bootstrap_info with a canned
// reply, so we can drive cluster_discovery's outbound probe without standing up
// a full node. A cluster_uuid in the reply signals "a cluster already exists".
class fake_bootstrap_service : public cluster::cluster_bootstrap_service {
public:
    fake_bootstrap_service(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      std::optional<model::cluster_uuid> cluster_uuid)
      : cluster::cluster_bootstrap_service(sg, ssg)
      , _cluster_uuid(cluster_uuid) {}

    ss::future<cluster::cluster_bootstrap_info_reply> cluster_bootstrap_info(
      cluster::cluster_bootstrap_info_request,
      rpc::streaming_context&) override {
        cluster::cluster_bootstrap_info_reply r{};
        r.version = features::feature_table::get_latest_logical_version();
        r.empty_seed_starts_cluster = true;
        r.cluster_uuid = _cluster_uuid;
        r.node_uuid = model::node_uuid(uuid_t::create());
        co_return r;
    }

private:
    std::optional<model::cluster_uuid> _cluster_uuid;
};

// The probe queries peer seeds; keep the local ("self") address distinct so it
// is excluded and the fake server is the sole peer. `down_addr` is a seed with
// nothing listening, used to exercise probing past an unreachable peer.
const net::unresolved_address self_addr{"127.0.0.1", 34561};
const net::unresolved_address peer_addr{"127.0.0.1", 34562};
const net::unresolved_address down_addr{"127.0.0.1", 34563};

} // namespace

struct cluster_discovery_fixture {
    cluster_discovery_fixture()
      : _ssg(ss::create_smp_service_group({5000}).get())
      , _sg(ss::default_scheduling_group()) {
        // cluster_discovery reads the local node config to decide which peers
        // to probe. Point it at `peer_addr` and give it a distinct identity.
        config::node().rpc_server.set_value(self_addr);
        config::node().seed_servers.set_value(
          std::vector<config::seed_server>{config::seed_server{peer_addr}});
    }

    ~cluster_discovery_fixture() {
        stop_server();
        ss::destroy_smp_service_group(_ssg).get();
        config::node().rpc_server.reset();
        config::node().seed_servers.reset();
    }

    // Start a bootstrap service on `peer_addr` whose replies carry
    // `cluster_uuid` (or none, to model a peer that is not part of any
    // cluster).
    void
    start_peer(std::optional<model::cluster_uuid> cluster_uuid = std::nullopt) {
        net::server_configuration scfg("cluster_discovery_test_rpc");
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        scfg.addrs.emplace_back(net::resolve_dns(peer_addr).get(), nullptr);
        scfg.max_service_memory_per_core = static_cast<int64_t>(
          ss::memory::stats().total_memory() / 10);
        _server = std::make_unique<rpc::rpc_server>(std::move(scfg));
        _server->register_service<fake_bootstrap_service>(
          _sg, _ssg, cluster_uuid);
        _server->start();
    }

    void stop_server() {
        if (_server) {
            _server->stop().get();
            _server.reset();
        }
    }

    cluster::cluster_discovery make_discovery(
      std::optional<model::cluster_uuid> local_cluster_uuid = std::nullopt) {
        return cluster::cluster_discovery{
          model::node_uuid(uuid_t::create()), local_cluster_uuid, _as};
    }

    ss::abort_source _as;
    ss::smp_service_group _ssg;
    ss::scheduling_group _sg;
    std::unique_ptr<rpc::rpc_server> _server;
};

// A peer that reports a cluster_uuid means a cluster already exists: the probe
// must detect it.
FIXTURE_TEST(detects_existing_cluster, cluster_discovery_fixture) {
    start_peer(model::cluster_uuid(uuid_t::create()));

    auto discovery = make_discovery();
    BOOST_REQUIRE(discovery.try_detect_existing_cluster(2s).get());
}

// A single peer reporting a cluster_uuid is enough: the probe must still detect
// the cluster even when an earlier seed in the list is unreachable, rather than
// requiring every peer to answer.
FIXTURE_TEST(
  detects_cluster_via_one_reachable_peer, cluster_discovery_fixture) {
    // Probe an unreachable seed first, then the live one carrying the uuid.
    config::node().seed_servers.set_value(
      std::vector<config::seed_server>{
        config::seed_server{down_addr}, config::seed_server{peer_addr}});
    start_peer(model::cluster_uuid(uuid_t::create()));

    auto discovery = make_discovery();
    BOOST_REQUIRE(discovery.try_detect_existing_cluster(2s).get());
}

// A reachable peer that is not part of any cluster (no cluster_uuid) is
// definitive evidence that there is no cluster to join: the probe returns
// false without waiting out its budget.
FIXTURE_TEST(no_cluster_when_peer_has_no_uuid, cluster_discovery_fixture) {
    start_peer(/*cluster_uuid=*/std::nullopt);

    auto discovery = make_discovery();
    BOOST_REQUIRE(!discovery.try_detect_existing_cluster(2s).get());
}

// No peer is reachable (nothing is listening): the result is inconclusive, so
// the probe returns false and the caller falls back to the late founder path.
FIXTURE_TEST(no_cluster_when_peers_unreachable, cluster_discovery_fixture) {
    // Deliberately do not start_peer().
    auto discovery = make_discovery();
    BOOST_REQUIRE(!discovery.try_detect_existing_cluster(2s).get());
}

// A node that already holds a cluster_uuid locally knows a cluster exists
// without probing anyone.
FIXTURE_TEST(local_cluster_uuid_short_circuits, cluster_discovery_fixture) {
    // No peer running; detection must still succeed from local state alone.
    auto discovery = make_discovery(model::cluster_uuid(uuid_t::create()));
    BOOST_REQUIRE(discovery.try_detect_existing_cluster(2s).get());
}
