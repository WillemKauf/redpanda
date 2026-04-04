/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "compaction/state.h"
#include "model/fundamental.h"

namespace storage::local_compaction {

using compaction_state = compaction::compaction_state<model::offset>;
using compaction_offsets = compaction::compaction_offsets<model::offset>;
using model_offset_interval_set
  = compaction::compaction_offset_interval_set<model::offset>;

} // namespace storage::local_compaction
