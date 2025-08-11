// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction_reducer.h"

#include "compaction/filter.h"
#include "config/configuration.h"
#include "model/timeout_clock.h"
#include "storage/chunk_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/exceptions.h"
#include "storage/logger.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_utils.h"
#include "utils/file_io.h"

#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <optional>

namespace storage {

template<typename Sink_T>
storage_compaction_source<Sink_T>::storage_compaction_source(
  storage::disk_log_impl* log,
  std::optional<model::offset> new_start_offset,
  const compaction::compaction_config& cfg)
  : _log(log)
  , _new_start_offset(new_start_offset)
  , _cfg(cfg)
  , _probe(log->get_probe()) {}

template<typename Sink_T>
ss::future<> storage_compaction_source<Sink_T>::initialize_source() {
    _segs = _log->find_sliding_range(_cfg, _new_start_offset);
    auto& segs = *_segs;
    for (auto& s : segs) {
        if (_cfg.asrc) {
            _cfg.asrc->check();
        }

        co_await _log->segment_self_compact(_cfg, s);
    }

    // Remove any of the segments from the back of the set that have already
    // been cleanly compacted. They would be no-ops to compact.
    while (!segs.empty()) {
        if (segs.back()->has_clean_compact_timestamp()) {
            segs.pop_back();
        } else {
            break;
        }
    }

    // Remove any segments from the front of the set that don't have any
    // compactible records. They would be no-ops to compact.
    while (!segs.empty()) {
        auto s = segs.front();
        if (s->may_have_compactible_records()) {
            break;
        }
        // For all intents and purposes, these segments are already cleanly
        // compacted.
        bool marked_as_cleanly_compacted
          = co_await internal::mark_segment_as_finished_window_compaction(
            s, true, _probe);
        if (marked_as_cleanly_compacted) {
            _log->subtract_dirty_segment_bytes(s->size_bytes());
        }
        segs.pop_front();
    }

    _b_it = segs.rbegin();
    _f_it = segs.begin();
}

template<typename Sink_T>
ss::future<>
storage_compaction_source<Sink_T>::initialize_sink(compaction::reducer::sink&) {
    if constexpr (std::is_same_v<Sink_T, storage_compaction_sink>) {
        // auto& casted_sink = static_cast<storage_compaction_sink&>(sink);
        // co_await casted_sink.filter_segments(_segs.value());
    } else {
        static_assert(false);
    }
    co_return;
}

template<typename Sink_T>
bool storage_compaction_source<Sink_T>::is_end_of_stream() const {
    auto& segs = *_segs;
    if (segs.empty()) {
        vlog(
          gclog.debug,
          "[{}] no segments to compact (all segments were already cleanly "
          "compacted, or did not have any compactible records)",
          _log->config().ntp());
    }

    return segs.empty();
}

template<typename Sink_T>
ss::future<bool> storage_compaction_source<Sink_T>::end_of_stream() const {
    auto& segs = *_segs;
    if (segs.empty()) {
        // Nothing to compact.
        co_return false;
    }
    if (!_cfg.min_offset_fully_indexed.has_value()) {
        // We're going to perform all of the chunked window compaction routine
        // now.
        // TODO: Unfortunately this totally goes against the spirit of the local
        // storage compaction re-write. Chunked compaction doesn't fit into the
        // generic "backwards/forwards" pass implementation designed here, but
        // we need to do it to make progress with compaction. We can get rid of
        // this ugliness when we decouple compaction state from `segment` state
        // completely.
        auto fut = co_await ss::coroutine::as_future(
          _log->chunked_sliding_window_compact(_cfg, segs));
        if (fut.failed()) {
            std::rethrow_exception(fut.get_exception());
        }
        // We don't need to perform the forward pass now.
        co_return false;
    }
    std::optional<model::offset> new_start_offset
      = _cfg.min_offset_fully_indexed;
    if (new_start_offset == segs.front()->offsets().get_base_offset()) {
        // We have indexed up to the first segment in the sliding
        // range (not necessarily equivalent to the first segment in the log-
        // segments may have been removed from the sliding range if they were
        // already cleanly compacted or had no compactible offsets). Reset the
        // start offset to allow new segments into the sliding window range.
        new_start_offset.reset();
    }

    _log->set_last_compaction_window_start_offset(new_start_offset);

    co_return true;
}

template<typename Sink_T>
ss::future<ss::stop_iteration>
storage_compaction_source<Sink_T>::backward_pass_iteration() {
    if (_b_it == _segs->rend()) {
        co_return ss::stop_iteration::yes;
    }
    if (_cfg.asrc) {
        _cfg.asrc->check();
    }
    auto seg = *_b_it;
    auto& map = *_cfg.hash_key_map;
    auto indexed = co_await _log->index_segment_in_offset_map(_cfg, seg, map);

    if (indexed) {
        _cfg.min_offset_fully_indexed = seg->offsets().get_base_offset();
    } else {
        // The offset map is full. Note that we may have only partially
        // indexed a segment, but it's safe to use this index. If no new
        // segments come in, the next time we compact, we need to start
        // from this segment for completeness.
        vlog(gclog.debug, "Segment not fully indexed: {}", seg->filename());
        co_return ss::stop_iteration::yes;
    }

    ++_b_it;
    co_return ss::stop_iteration::no;
}

template<typename Sink_T>
ss::future<ss::stop_iteration>
storage_compaction_source<Sink_T>::forward_pass_iteration(
  compaction::reducer::sink& sink) {
    if (_f_it == _segs->end()) {
        co_return ss::stop_iteration::yes;
    }

    if (_cfg.asrc) {
        _cfg.asrc->check();
    }

    auto s = *_f_it;
    auto& map = *_cfg.hash_key_map;

    if constexpr (std::is_same_v<Sink_T, storage_compaction_sink>) {
        // Unfortunately, local storage still needs to roll based on `segment`s,
        // not just batches.
        auto seg_opt
          = co_await static_cast<storage_compaction_sink&>(_sink).maybe_roll(s);
        if (seg_opt.has_value()) {
            co_await _log->replace_with_segment(seg_opt.value(), _ids);
            _ids.clear();
        }
    }

    auto read_holder = co_await s->read_lock();
    if (s->is_closed()) {
        throw segment_closed_exception(fmt::format(
          "Aborting compaction, segment {} was closed while waiting for read "
          "lock.",
          s->filename()));
    }

    auto rdr = internal::create_segment_full_reader(
      s, _cfg, _probe, std::move(read_holder));

    auto segment_last_offset = s->offsets().get_committed_offset();
    auto compaction_placeholder_enabled
      = _log->feature_table().local().is_active(
        features::feature::compaction_placeholder_batch);
    const bool past_tombstone_delete_horizon
      = internal::is_past_tombstone_delete_horizon(s, _cfg);
    bool may_have_tombstone_records = false;
    bool has_transaction_batches = false;
    model::offset max_removed_offset = model::offset::min();

    auto is_latest_record = [&map](
                              const model::record_batch& b,
                              const model::record& r) -> ss::future<bool> {
        return compaction::is_latest_record_for_key(map, b, r);
    };

    const auto& ntp = _log->config().ntp();
    auto& probe = _probe;
    auto record_filter = [f = std::move(is_latest_record),
                          ntp,
                          segment_last_offset,
                          compaction_placeholder_enabled,
                          past_tombstone_delete_horizon,
                          &may_have_tombstone_records,
                          &probe,
                          &has_transaction_batches,
                          &max_removed_offset](
                           const model::record_batch& b,
                           const model::record& r,
                           bool is_last_record_in_batch) {
        return internal::should_keep(
          b,
          r,
          ntp,
          is_last_record_in_batch,
          compaction_placeholder_enabled,
          f,
          probe,
          segment_last_offset,
          past_tombstone_delete_horizon,
          may_have_tombstone_records,
          has_transaction_batches,
          max_removed_offset);
    };

    auto filter = compaction::filter(
      std::move(record_filter),
      sink,
      compaction_placeholder_enabled,
      segment_last_offset,
      ntp);
    auto stats = co_await std::move(rdr).consume(
      std::move(filter), model::no_timeout);
    if (stats.has_removed_data()) {
        vlog(
          gclog.info,
          "Compaction filtering removing data from {}: {}",
          s->filename(),
          stats);
    } else {
        vlog(
          gclog.debug,
          "Compaction filtering not removing any records from {}: {}",
          s->filename(),
          stats);
    }

    // TODO: max removed offset

    ++_f_it;
    co_return ss::stop_iteration::no;
}

storage_compaction_sink::storage_compaction_sink(
  storage::disk_log_impl* log, const compaction::compaction_config& cfg)
  : _log(log)
  , _cfg(cfg)
  , _probe(log->get_probe()) {}

// ss::future<> storage_compaction_sink::filter_segments(segment_set& src_segs)
// {
//     // Filter src_segs by performing a pass over it.
//     segment_set::underlying_t filtered_src_segs;
//
//     auto it = src_segs.begin();
//     size_t current_size{0};
//     model::term_id current_term{(*it)->offsets().get_term()};
//
//     // Ensure that the adjacent segments do not span an offset space greater
//     // than the maximum value that can be represented by a uint32_t. This
//     must
//     // be enforced due to use of roaring::bitmap in the
//     compacted_offset_list,
//     // which is used to deduplicate records during self compaction and
//     sliding
//     // window compaction. Overflows in this area could lead to incorrect
//     // compaction.
//     auto valid_offset_range = [](
//                                 ss::lw_shared_ptr<segment>& first,
//                                 ss::lw_shared_ptr<segment>& last) -> bool {
//         auto base_offset_first_seg = first->offsets().get_base_offset();
//         auto dirty_offset_last_seg = last->offsets().get_dirty_offset();
//         int64_t offset_delta = dirty_offset_last_seg()
//                                - base_offset_first_seg();
//         static constexpr int64_t u32_max = static_cast<int64_t>(
//           std::numeric_limits<uint32_t>::max());
//         return offset_delta <= u32_max;
//     };
//
//     std::pair<segment_set::iterator, segment_set::iterator> current_range = {
//       it, it};
//
//     while (it != src_segs.end()) {
//         auto& seg = *it;
//         auto seg_size = seg->size_bytes();
//         auto seg_term = seg->offsets().get_term();
//
//         current_size += seg_size;
//
//         bool term_boundary = seg_term != current_term;
//         bool size_boundary
//           = current_size
//             > config::shard_local_cfg().max_compacted_log_segment_size();
//         bool is_valid_offset_range = valid_offset_range(
//           *current_range.first, seg);
//
//         if (term_boundary || size_boundary || !is_valid_offset_range) {
//             auto num_segments_in_range = std::distance(
//               current_range.first, current_range.second);
//             auto needs_rewrite = num_segments_in_range > 1
//                                  || co_await segment_needs_rewrite(
//                                    *current_range.first,
//                                    _cfg,
//                                    *_cfg.hash_key_map);
//             if (needs_rewrite) {
//                 filtered_src_segs.insert(
//                   filtered_src_segs.end(),
//                   current_range.first,
//                   current_range.second);
//             }
//
//             current_range = {it, std::next(it)};
//             current_size = seg_size;
//             current_term = seg_term;
//
//         } else {
//             ++current_range.second;
//         }
//         ++it;
//     }
//
//     auto num_segments_in_range = std::distance(
//       current_range.first, current_range.second);
//     auto needs_rewrite = num_segments_in_range > 1
//                          || co_await segment_needs_rewrite(
//                            *current_range.first, _cfg, *_cfg.hash_key_map);
//     if (needs_rewrite) {
//         filtered_src_segs.insert(
//           filtered_src_segs.end(), current_range.first,
//           current_range.second);
//     }
//
//     vlog(
//       gclog.info,
//       "Filtered segment set of size {} to {}",
//       src_segs.size(),
//       filtered_src_segs.size());
//     // src_segs = segment_set(std::move(filtered_src_segs));
// }

ss::future<> storage_compaction_sink::finalize() {
    if (!_appender) {
        co_return;
    }
    co_await roll();
}

ss::future<>
storage_compaction_sink::maybe_roll(ss::lw_shared_ptr<segment> seg) {
    // Sink needs (re)-initialization if:
    // 1. `!_appender` (i.e initializing for the first time)
    // 2. current `_appender->file_byte_offset() + seg->size_bytes() >=
    // max_compacted_log_segment_size`
    // 3. `seg->term()` differs from `_raft_term`.
    // 4. `seg->dirty_offset() - _base_offset` exceeds the maximum value
    // that can be represented by a `uint32_t`. For cases (2-4), writers
    // must be flushed before re-initialization.

    if (!_segment) {
        // Initializing for the first time. Checking just one of the
        // contained member variables for existence is valid for checking
        // all of them.
        co_await initialize_segment(seg);
    } else {
        // Book-keep removed dirty bytes. We need to know both the number of
        // bytes removed from dirty segments as well as the total size of
        // `segment`s which _may_ be part of a totally clean segment operation.
        auto prev_seg = _accumulated_segments.back();
        if (!prev_seg->has_clean_compact_timestamp()) {
            auto size_before = prev_seg->size_bytes();
            auto size_after = _appender->file_byte_offset()
                              - _acc.prev_appender_size;
            _acc.removed_dirty_bytes += size_before - size_after;
            if (
              prev_seg->offsets().get_base_offset()
              >= _cfg.min_offset_fully_indexed) {
                _acc.dirty_turning_clean_bytes += prev_seg->size_bytes();
            }
        }

        bool size_boundary
          = _appender->file_byte_offset() + seg->size_bytes()
            >= config::shard_local_cfg().max_compacted_log_segment_size;
        bool term_boundary = seg->offsets().get_term()
                             != prev_seg->offsets().get_term();
        static constexpr int64_t u32_max = static_cast<int64_t>(
          std::numeric_limits<uint32_t>::max());
        bool offset_boundary = seg->offsets().get_dirty_offset()
                                 - _replace_segment->offsets().get_base_offset()
                               >= u32_max;
        bool needs_roll = size_boundary || term_boundary || offset_boundary;
        if (needs_roll) {
            co_await roll();
            co_await initialize_segment(seg);
        }
    }

    _acc.total_bytes += seg->size_bytes();
    _acc.prev_appender_size = _appender->file_byte_offset();
}

ss::future<>
storage_compaction_sink::initialize_segment(ss::lw_shared_ptr<segment> seg) {
    auto tmpname = seg->path().to_compaction_staging();
    auto idx_tmpname = tmpname.to_index();
    auto cidx_tmpname = tmpname.to_compacted_index();
    auto idx_base_offset = seg->offsets().get_base_offset();
    auto apply_offset = internal::should_apply_delta_time_offset(
      _log->feature_table());

    // Open the `segment_appender` and `compacted_index` writers with truncation
    // (as a safety precaution)
    static constexpr bool truncate = true;
    _tmpname = tmpname;
    _appender = co_await internal::make_segment_appender(
      tmpname,
      segment_appender::write_behind_memory / internal::chunks().chunk_size(),
      std::nullopt,
      _log->resources(),
      _cfg.sanitizer_config,
      truncate);
    _idx = std::make_unique<index_state>(
      index_state::make_empty_index(idx_base_offset, apply_offset));
    _cidx = make_file_backed_compacted_index(
      cidx_tmpname, truncate, _log->resources(), _cfg.sanitizer_config);
    _replace_segment = seg;

    _tmp_file_tracker.emplace(
      _cfg.files_to_cleanup,
      std::vector<std::filesystem::path>{tmpname, cidx_tmpname});
}

ss::future<> storage_compaction_sink::roll() {
    auto segment_modify_lock
      = co_await _log->segment_rewrite_lock().get_units();

    // Evict segment readers and prevent new ones from being added to the cache
    chunked_vector<ss::future<readers_cache::range_lock_holder>> holder_futs;
    holder_futs.reserve(_accumulated_segments.size());
    for (auto& s : _accumulated_segments) {
        holder_futs.push_back(_log->readers().evict_segment_readers(s));
    }

    auto holders = co_await ss::when_all_succeed(
      holder_futs.begin(), holder_futs.end());

    // lock the range. only metadata (e.g. open/rename/delete) i/o occurs with
    // these locks held so it is a relatively short duration. all of the data
    // copying and compaction i/o occurred above with no locks held. 5 retries
    // with a max lock timeout of 1 second. if we don't get the locks there is
    // probably a reader. compaction will revisit.
    static constexpr auto write_lock_timeout = std::chrono::seconds(1);
    static constexpr auto write_lock_retries = 5;
    try {
        auto locks = co_await internal::write_lock_segments(
          _accumulated_segments, write_lock_timeout, write_lock_retries);
    } catch (const ss::semaphore_timed_out&) {
        throw std::runtime_error(fmt::format(
          "Aborting compaction of {} segments ([{}-{}]), timed out waiting for "
          "write locks",
          _accumulated_segments.size(),
          _accumulated_segments.front()->filename(),
          _accumulated_segments.back()->filename()));
    }

    // Check if any `segment`s were closed while waiting for locks
    auto any_segments_closed = std::ranges::any_of(
      _accumulated_segments, &segment::is_closed);
    if (any_segments_closed) {
        throw segment_closed_exception(fmt::format(
          "Aborting compaction of {} segments ([{}-{}]), segments were closed "
          "while waiting for locks",
          _accumulated_segments.size(),
          _accumulated_segments.front()->filename(),
          _accumulated_segments.back()->filename()));
    }

    // Check if any `segment`s were mutated (e.g. truncation) while waiting for
    // locks
    vassert(
      _generations.size() == _accumulated_segments.size(),
      "Each segment must have corresponding generation");
    for (const auto& [s, gen_id] :
         std::views::zip(_accumulated_segments, _generations)) {
        if (s->get_generation_id() != gen_id) {
            throw generation_id_mismatch_exception(fmt::format(
              "Aborting compaction of {} segments ([{}-{}]), segment {} was "
              "mutated while compacting",
              _accumulated_segments.size(),
              _accumulated_segments.front()->filename(),
              _accumulated_segments.back()->filename(),
              s->filename()));
        }
    }

    // Check if abort source was triggered
    if (_cfg.asrc) {
        _cfg.asrc->check();
    }

    vlog(
      gclog.info,
      "[{}] Compacting {} segments in interval [{}, {}]",
      _log->config().ntp(),
      _accumulated_segments.size(),
      _accumulated_segments.front()->filename(),
      _accumulated_segments.back()->filename());

    // Perform IO _after_ all the appropriate early return checks.
    auto appender = std::exchange(_appender, nullptr);
    auto idx = std::exchange(_idx, nullptr);
    auto cidx = std::exchange(_cidx, nullptr);

    co_await cidx->close();
    co_await appender->close();

    // Truncate the existing `segment_index` on disk.
    co_await _replace_segment->index().drop_all_data();

    // Rename the underlying `segment` data file.
    co_await internal::do_swap_data_file_handles(
      _tmpname.value(), _replace_segment, _cfg, _probe, cidx->size_bytes());

    // Flush new `segment_index` to disk.
    _replace_segment->index().swap_index_state(std::move(*idx));
    _replace_segment->force_set_commit_offset_from_index();
    co_await _replace_segment->index().flush();

    // Replace the `.compaction_index` on disk.
    auto cidx_tmpname = _tmpname->to_compacted_index();
    auto cidx_name = _replace_segment->path().to_compacted_index();
    co_await ss::rename_file(cidx_tmpname.string(), cidx_name.string());

    // Reset the batch cache index
    co_await _replace_segment->reset_batch_cache_index();

    // We can only mark the replacement segment as cleanly compacted if every
    // accumulated segment was already cleanly compacted OR if the accumulated
    // segment was fully indexed in this round of compaction.
    auto is_clean_compacted = std::ranges::all_of(
      _accumulated_segments, [this](const auto& s) {
          return s->has_clean_compact_timestamp()
                 || s->offsets().get_base_offset()
                      >= _cfg.min_offset_fully_indexed;
      });

    // Remove old segments
    for (auto seg_it = std::next(_accumulated_segments.begin());
         seg_it != _accumulated_segments.end();
         ++seg_it) {
        co_await _log->erase_segment(*seg_it);
    }

    _replace_segment->advance_generation();

    vlog(
      gclog.info,
      "[{}] Compaction produced segment {} from {} segments ([{}-{}], {} "
      "bytes)",
      _log->config().ntp(),
      _replace_segment,
      _accumulated_segments.size(),
      _accumulated_segments.front()->filename(),
      _accumulated_segments.back()->filename(),
      _acc.total_bytes);

    // `_log` book-keeping
    const auto size_after = appender->size_bytes();
    const ssize_t removed_bytes = ssize_t(_acc.total_bytes)
                                  - ssize_t(size_after);

    // We must deduct the entirety of accumulated dirty bytes (i.e all of
    // the bytes in previously dirty `segment`s that are now considered clean)
    // if we are marking the `segment` clean. Otherwise, we only deduct the
    // number of bytes removed from dirty `segment`s in the log.
    ssize_t dirty_removed_bytes = is_clean_compacted
                                    ? _acc.dirty_turning_clean_bytes
                                    : _acc.removed_dirty_bytes;

    // Mark the segment as completed window compaction, and possibly set the
    // clean_compact_timestamp in it's index.
    co_await internal::mark_segment_as_finished_window_compaction(
      _replace_segment, is_clean_compacted, _probe);

    _log->subtract_dirty_segment_bytes(dirty_removed_bytes);
    _log->subtract_closed_segment_bytes(removed_bytes);

    _probe.segment_compacted();
    _probe.add_compaction_removed_bytes(removed_bytes);

    compaction_result res(_acc.total_bytes, size_after);
    _log->compaction_ratio().update(res.compaction_ratio());

    // Reset `sink` local state.
    _acc.reset();
    _accumulated_segments.clear();
    _generations.clear();
    _tmp_file_tracker->clear();
}

ss::future<ss::stop_iteration> storage_compaction_sink::operator()(
  model::record_batch b, model::compression c) {
    co_await write_batch(std::move(b), c);
    co_return ss::stop_iteration::no;
}

ss::future<> storage_compaction_sink::write_batch(
  model::record_batch b, model::compression c) {
    bool compactible_batch = compaction::is_compactible(
      _log->config().ntp(), b.header());
    if (compactible_batch) {
        co_await model::for_each_record(
          b, [&batch = b, this](const model::record& r) {
              auto& hdr = batch.header();
              return _cidx->index(
                hdr.type,
                hdr.attrs.is_control(),
                r.key(),
                batch.base_offset(),
                r.offset_delta());
          });
    }

    auto batch = co_await internal::compress_batch(c, std::move(b));
    const auto start_pos = _appender->file_byte_offset();
    const auto header_size = batch.header().size_bytes;
    _acc.index_acc += header_size;
    // do not set broker_timestamp in this index, leave the operation to the
    // caller who has more context
    if (_idx->maybe_index(
          _acc.index_acc,
          segment_index::default_data_buffer_step,
          start_pos,
          batch.base_offset(),
          batch.last_offset(),
          batch.header().first_timestamp,
          batch.header().max_timestamp,
          std::nullopt,
          _replace_segment->path().is_internal_topic()
            || batch.header().type == model::record_batch_type::raft_data,
          compactible_batch ? batch.header().record_count : 0)) {
        _acc.index_acc = 0;
    }
    co_await _appender->append(batch);
    vassert(
      _appender->file_byte_offset() == start_pos + header_size,
      "Size must be deterministic. Expected:{} == {}",
      _appender->file_byte_offset(),
      start_pos + header_size);
}

template class storage_compaction_source<storage_compaction_sink>;

} // namespace storage
