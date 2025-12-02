/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/committer.h"

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/compaction/committing_policy.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "ssx/when_all.h"

#include <exception>

namespace cloud_topics::l1 {

compaction_committer::compaction_committer(
  std::unique_ptr<committing_policy> policy, io* io, metastore* metastore)
  : _policy(std::move(policy))
  , _io(io)
  , _metastore(metastore) {}

ss::future<> compaction_committer::start() { co_return; }

ss::future<> compaction_committer::stop() {
    _as.request_abort();
    auto close_fut = _gate.close();
    co_await cancel_active_jobs();
    co_await std::move(close_fut);
}

void compaction_committer::start_upload_loop(compaction_job_id id) {
    ssx::spawn_with_gate(_gate, [this, id] {
        return upload_loop(id).handle_exception(
          [id](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
              vlogl(
                compaction_log,
                log_level,
                "Encountered exception in upload loop for job {}: {}",
                id,
                e);
          });
    });
}

bool compaction_committer::is_active() const {
    return !_gate.is_closed() && !_as.abort_requested();
}

ss::future<> compaction_committer::upload_loop(compaction_job_id id) {
    job_state* job = get_job_by_id(id);
    std::exception_ptr e;
    try {
        while (is_active()) {
            constexpr std::chrono::seconds poll_frequency(10);
            try {
                co_await job->upload_sem.wait(
                  poll_frequency,
                  std::max(job->upload_sem.current(), size_t(1)));
            } catch (const ss::semaphore_timed_out&) {
                // Fall through
            }

            if (!job->staging_file_and_md_infos.empty()) {
                if (_policy->should_commit()) {
                    auto updates = std::exchange(
                      job->staging_file_and_md_infos, {});
                    start_uploads(job, std::move(updates));
                }
            }

            if (job->all_uploads_inflight()) {
                // Await inflight uploads and break out of upload_loop.
                co_return co_await await_inflight_uploads(job);
            }
        }
    } catch (...) {
        e = std::current_exception();
    }

    if (!e) {
        // If `!is_active()`, we fall through to here.
        e = std::make_exception_ptr(ss::abort_requested_exception());
    }

    job->all_uploads_complete.set_exception(e);
    std::rethrow_exception(e);
}

ss::future<compaction_job_id>
compaction_committer::begin_compaction_job(model::topic_id_partition tidp) {
    auto id = create_compaction_job_id();

    auto metadata_builder_res
      = co_await l1::retry_metastore_op_with_default_rtc(
        [this]() { return _metastore->object_builder(); }, _as);
    if (!metadata_builder_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Could not create object metadata builder for compaction of tidp {}: "
          "{}. Aborting.",
          tidp,
          metadata_builder_res.error());
        throw std::runtime_error("Couldn't begin compaction job");
    }
    auto metadata_builder = std::move(metadata_builder_res).value();

    _compaction_jobs.emplace(
      id, std::make_unique<job_state>(id, tidp, std::move(metadata_builder)));
    start_upload_loop(id);

    vlog(
      compaction_log.debug,
      "Started compaction job for tidp {} under id {}",
      tidp,
      id);

    co_return id;
}

void compaction_committer::add_l1_object(
  compaction_job_id id, file_and_md_info file_and_info) {
    job_state* job = get_job_by_id(id);

    vassert(
      job->s == job_state::state::in_progress,
      "Cannot add l1 object to finalized job.");

    vlog(
      compaction_log.trace,
      "Pushing file of size {}, offsets ({}-{}) for compaction of tidp {}, job "
      "{}",
      file_and_info.info.size_bytes,
      file_and_info.ntp_md.base_offset,
      file_and_info.ntp_md.last_offset,
      file_and_info.ntp_md.tidp,
      id);

    auto update_response = _policy->on_update(file_and_info);

    job->staging_file_and_md_infos.push_back(std::move(file_and_info));

    if (update_response == committing_policy::update_response::preempt) {
        job->upload_sem.signal();
    }
}

ss::future<> compaction_committer::finalize_job(
  compaction_job_id id,
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges) {
    job_state* job = get_job_by_id(id);

    vlog(compaction_log.debug, "Marking job {} as finalized", id);
    job->s = job_state::state::finalized;
    job->upload_sem.signal();

    vlog(compaction_log.debug, "Awaiting completion of uploads for job {}", id);

    auto fut = co_await ss::coroutine::as_future(
      ssx::with_timeout_abortable(
        job->all_uploads_complete.get_future(), model::no_timeout, _as));

    // Now safe to extract/remove the underlying job state from
    // `_compaction_jobs`.
    auto job_ptr_opt = _compaction_jobs.extract(id);
    vassert(job_ptr_opt.has_value(), "concurrency issue?");
    auto job_ptr = std::move(job_ptr_opt.value().second);

    finalize_op op = finalize_op::compact_objects;
    if (fut.failed()) {
        auto e = fut.get_exception();
        auto lvl = ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                                 : ss::log_level::warn;
        vlogl(
          compaction_log,
          lvl,
          "Failed to put all compaction objects for job {} to object storage: "
          "{}",
          id,
          e);
        // If an error was encountered during committing, don't attempt to
        // `compact_objects` with the `metastore`.
        op = finalize_op::replace_objects;
    }

    co_return co_await do_finalize_job(
      std::move(job_ptr),
      std::move(new_cleaned_ranges),
      std::move(removed_tombstone_ranges),
      op);
}

ss::future<> compaction_committer::do_finalize_job(
  job_ptr_t job,
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges,
  finalize_op op) {
    switch (op) {
    case finalize_op::compact_objects:
        return do_compact_objects(
          std::move(job),
          std::move(new_cleaned_ranges),
          std::move(removed_tombstone_ranges));
    case finalize_op::replace_objects:
        return do_replace_objects(std::move(job));
    }
}

metastore::compaction_update compaction_committer::make_compaction_update(
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges) {
    return metastore::compaction_update{
      .new_cleaned_ranges = std::move(new_cleaned_ranges),
      .removed_tombstones_ranges = std::move(removed_tombstone_ranges),
      .cleaned_at = model::timestamp::now()};
}

ss::future<> compaction_committer::do_compact_objects(
  job_ptr_t job,
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges) {
    auto compaction_update = make_compaction_update(
      std::move(new_cleaned_ranges), std::move(removed_tombstone_ranges));

    auto compaction_update_str = fmt::format("{}", compaction_update);
    metastore::compaction_map_t compact_map;
    compact_map.emplace(job->tidp, std::move(compaction_update));
    auto commit_res = co_await l1::retry_metastore_op_with_default_rtc(
      [this, &job, compact_map = std::move(compact_map)]() {
          return _metastore->compact_objects(
            *job->metadata_builder, std::move(compact_map));
      },
      _as);

    if (!commit_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Could not commit metastore compact_objects update {} for compaction "
          "of tidp {}, job id {}: {}.",
          compaction_update_str,
          job->tidp,
          job->id,
          commit_res.error());
        // We couldn't commit the metastore update, but we should at least try
        // to replace the objects so as not to discard our hard IO work.
        co_return co_await do_replace_objects(std::move(job));
    }

    vlog(
      compaction_log.info,
      "Finalized compaction of tidp {}, job id {} with metastore "
      "compact_objects update {}",
      job->tidp,
      job->id,
      compaction_update_str);
}

ss::future<> compaction_committer::do_replace_objects(job_ptr_t job) {
    auto replace_res = co_await l1::retry_metastore_op_with_default_rtc(
      [this, &job]() {
          return _metastore->replace_objects(*job->metadata_builder);
      },
      _as);

    if (!replace_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Could not commit metastore replace_objects update during compaction "
          "of tidp {}, job id {}: {}.",
          job->tidp,
          job->id,
          replace_res.error());
        co_return;
    }

    vlog(
      compaction_log.info,
      "Finalized compaction of tidp {}, job id {} with metastore "
      "replace_objects update",
      job->tidp,
      job->id);
}

void compaction_committer::start_uploads(
  job_state* job, chunked_circular_buffer<file_and_md_info> updates) {
    while (!updates.empty()) {
        auto update = std::move(updates.front());
        updates.pop_front();
        auto inflight_upload = do_upload(job, std::move(update));
        job->inflight_uploads.push_back(std::move(inflight_upload));
    }
}

ss::future<compaction_committer::expected_t> compaction_committer::do_upload(
  job_state* job, file_and_md_info file_and_info) {
    auto holder = co_await job->metadata_builder_mutex.get_units();
    auto& metadata_builder = job->metadata_builder;
    auto oid_res = metadata_builder->get_or_create_object_for(job->tidp);
    if (!oid_res.has_value()) {
        co_return std::unexpected(
          error{
            .t = error::type::builder_failure,
            .msg = fmt::format("{}", oid_res.error())});
    }

    auto oid = std::move(oid_res).value();

    auto put_res = co_await put_object_with_retries(
      oid, file_and_info.staging_file.get());

    co_await file_and_info.staging_file->remove();

    if (!put_res.has_value()) {
        std::ignore = metadata_builder->remove_pending_object(oid);
        co_return std::unexpected(put_res.error());
    }

    vlog(
      compaction_log.trace,
      "Uploaded file {} ({}-{}) as part of compaction of tidp {}, job {}",
      oid,
      file_and_info.ntp_md.base_offset,
      file_and_info.ntp_md.last_offset,
      job->tidp,
      job->id);

    auto add_res = metadata_builder->add(oid, file_and_info.ntp_md);
    if (!add_res.has_value()) {
        std::ignore = metadata_builder->remove_pending_object(oid);
        co_return std::unexpected(
          error{
            .t = error::type::builder_failure,
            .msg = fmt::format("{}", add_res.error())});
    }

    auto finish_res = metadata_builder->finish(
      oid, file_and_info.info.footer_offset, file_and_info.info.size_bytes);
    if (!finish_res.has_value()) {
        std::ignore = metadata_builder->remove_pending_object(oid);
        co_return std::unexpected(
          error{
            .t = error::type::builder_failure,
            .msg = fmt::format("{}", finish_res.error())});
    }

    co_return expected_t{};
}

ss::future<compaction_committer::expected_t>
compaction_committer::put_object_with_retries(
  object_id oid, staging_file* file) {
    ss::sstring err;
    static constexpr int num_upload_retries = 5;
    for (int tries = num_upload_retries; tries > 0; --tries) {
        auto put_res = co_await _io->put_object(oid, file, &_as);
        if (put_res.has_value()) {
            co_return expected_t{};
        } else {
            err = fmt::format(
              "Failed to put object {}: {}",
              oid,
              static_cast<int>(put_res.error()));
            vlog(compaction_log.debug, "{}", err);
        }
    }

    co_return std::unexpected(
      error{.t = error::type::io_failure, .msg = std::move(err)});
}

ss::future<> compaction_committer::await_inflight_uploads(job_state* job) {
    auto inflight_uploads = std::exchange(job->inflight_uploads, {});
    using res_t = chunked_vector<expected_t>;
    auto res = co_await ss::coroutine::as_future(
      ssx::when_all_succeed<res_t>(std::move(inflight_uploads)));
    vassert(
      !res.failed(),
      "expected that these futures wouldn't throw, just propagate an error.");

    auto res_vec = std::move(res).get();

    auto failed = res_vec | std::views::filter([](const expected_t& res) {
                      return !res.has_value();
                  });

    bool success = failed.empty();
    if (!success) {
        auto err = fmt::format(
          "{}",
          fmt::join(
            failed | std::views::transform([](const expected_t& res) {
                return res.error();
            }),
            ", "));
        job->all_uploads_complete.set_exception(std::runtime_error(err));
    } else {
        job->all_uploads_complete.set_value();
    }
}

ss::future<> compaction_committer::cancel_active_jobs() {
    static constexpr size_t max_concurrent_removal = 1024;
    for (auto& [job, state] : _compaction_jobs) {
        state->upload_sem.broken();
        state->metadata_builder_mutex.broken();
        state->all_uploads_complete.set_exception(
          ss::abort_requested_exception());
        co_await ss::max_concurrent_for_each(
          state->staging_file_and_md_infos,
          max_concurrent_removal,
          [](auto& file_and_md_info) {
              return file_and_md_info.staging_file->remove();
          });
    }
}

} // namespace cloud_topics::l1
