/*
 * partition_balance.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"

#include "node.h"

#include "fabric/partition.h"


//==========================================================
// Forward declarations.
//

struct as_namespace_s;


//==========================================================
// Typedefs & constants.
//

typedef enum {
	PB_TASK_EMIG_TRANSFER,
	PB_TASK_EMIG_SIGNAL_ALL_DONE
} pb_task_type;

typedef struct pb_task_s {
	cf_node dest;
	struct as_namespace_s* ns;
	uint32_t pid;
	uint64_t cluster_key;
	pb_task_type type;
	uint32_t tx_flags;
} pb_task;

#define MAX_RACK_ID 1000000


//==========================================================
// Public API - regulate migrations.
//

void as_partition_balance_disallow_migrations();
bool as_partition_balance_are_migrations_allowed();
void as_partition_balance_synchronize_migrations();


//==========================================================
// Public API - balance partitions.
//

void as_partition_balance_init();
bool as_partition_balance_is_init_resolved();
void as_partition_balance_revert_to_orphan();
void as_partition_balance();

uint64_t as_partition_balance_remaining_migrations();


//==========================================================
// Public API - migration-related as_partition methods.
//

bool as_partition_pending_migrations(as_partition* p);

void as_partition_emigrate_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, uint32_t tx_flags);
as_migrate_result as_partition_immigrate_start(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, cf_node source_node);
as_migrate_result as_partition_immigrate_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, cf_node source_node);
as_migrate_result as_partition_migrations_all_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key);

// Counter that tells clients partition ownership has changed.
extern cf_atomic32 g_partition_generation;
