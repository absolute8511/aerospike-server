/*
 * migrate.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

/*
 * The migration module moves partition data from node to node
*/

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "util.h"

#include "base/index.h"
#include "base/datamodel.h"


// For receiver-side migration flow-control.
// By default, allow up to 2 concurrent migrates from each member of the cluster.
#define AS_MIGRATE_DEFAULT_MAX_NUM_INCOMING (2 * AS_CLUSTER_SZ)

/*
 *  Default lifetime (in ms) for a migrate recv control object to stay in the recv control
 *  hash table after receiving the first START event.  This provides a time window to de-bounce
 *  re-transmitted migrate START message from crossing paths with the DONE ACK message.  After
 *  that interval, the RX control object will be reaped by the reaper thread.
 *
 *  (A value of 0 disables this feature and reaps objects immediately upon receipt of the DONE event.)
 */
#define AS_MIGRATE_DEFAULT_RX_LIFETIME_MS (60 * 1000) // 1 minute

/*
 *  Maximum permissible number of migrate xmit threads.
 */
#define MAX_NUM_MIGRATE_XMIT_THREADS  (100)

typedef enum as_migrate_state_e {
	AS_MIGRATE_STATE_DONE,
	AS_MIGRATE_STATE_START,
	AS_MIGRATE_STATE_ERROR,
	AS_MIGRATE_STATE_EAGAIN
} as_migrate_state;

#define AS_MIGRATE_RX_STATE_SUBRECORD 1
#define AS_MIGRATE_RX_STATE_RECORD 2
typedef uint8_t as_partition_mig_rx_state;

// an a 'START' notification, the callback may return a value.
// If that value is -1, the migration will be abandoned (with 'ERROR' notification)
// If the value is -2, the migration will be tried again later (a subsequent START notification
// will be tried later)
//

typedef enum as_migrate_result_e {
	AS_MIGRATE_OK,
	AS_MIGRATE_FAIL,
	AS_MIGRATE_AGAIN,
	AS_MIGRATE_ALREADY_DONE
} as_migrate_result;

// A data structure for temporarily en-queuing partition migrations.
typedef struct partition_migrate_record_s {
	cf_node *dest;
	uint dest_sz;
	as_namespace *ns;
	as_partition_id pid;
	uint64_t cluster_key;
	uint32_t tx_flags;
} partition_migrate_record;

// Listen for migration messages
void as_migrate_init();

// Set the number of migrate xmit threads.
int as_migrate_set_num_xmit_threads(int n_threads);

bool
as_migrate_is_incoming(cf_digest *subrec_digest, uint64_t version, as_partition_id partition_id, int state);

// migrate a tree to a node
// and find out when it's done
int as_migrate(const partition_migrate_record *pmr, bool is_migrate_state_done);

// 0 if successfully found a migrate to cancel
// -1 if failed for unknown reasons
// -2 if failed because the migrate was not found
int as_migrate_cancel(cf_node dst, as_namespace *ns, as_partition_id partition);

/*
 *  Print information about migration to the log.
 */
void as_migrate_dump(bool verbose);

as_migrate_result as_partition_migrate_rx(as_migrate_state s,
		as_namespace *ns, as_partition_id pid, uint64_t orig_cluster_key,
		cf_node source_node);
as_migrate_result as_partition_migrate_tx(as_migrate_state s,
		as_namespace *ns, as_partition_id pid, uint64_t orig_cluster_key,
		uint32_t tx_flags);

/*
 * Check and return if passed in version is found in migration incoming ldt version hash
 */
int
as_migrate_is_incoming_version(uint64_t version, uint64_t *found_version);
