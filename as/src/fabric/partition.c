/*
 * partition.c
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
 *  Overview
 *  ========
 *
 *  Whenever cluster state change, node assignments of the partition changes.
 *  This leads to movement of the partition from one node to another, this is
 *  called partition migration.  For example
 *
 *  Cluster: [N1, N2]
 *  P1       : Master N1 and Replica N2
 *  Cluster: [N1, N2, N3]
 *  P1       : Master N3 and replica N1
 *  Partition P1 has to be moved to N3 which has the master copy of partition in
 *  the new cluster view.
 *
 *  Following keywords are used while describing the whole partition migration
 *  logic
 *
 *  - Partition Node hash list:	[p->replica]
 *    The hash value list with the nodes ordered. Master comes first followed
 *    by replica in current cluster view and then all other nodes.
 *
 *  - Primary Version: [p->primary_version_info]
 *    In partition node hash list, version of partition on the, first node with
 *    some valid version of data. (We need to maintain only first node
 *    information)
 *
 *  - First Non Primary Versions: [p->dupl, p->dupl_vinfo]
 *    In partition node hash list all the first nodes with the version not
 *    matching primary version (This maintains array of nodes along with the
 *    version as there could be multiple versions). Data is maintained only in
 *    first node all subsequent node with copy of duplicate version is dropped
 *
 *  - Write Journal :
 *    -- When normal writes come in, journal is written when the writes are not
 *       applied. Any DESYNC node (desync is always in replica	list) receiving
 *       incoming migration does not apply write to the record but log the
 *       operation in write_journal.
 *
 *    -- All the nodes in the replica list which has primary sync copy take the
 *       writes and apply it.
 *
 *    -- All the nodes in the replica list which has non primary sync copy reject
 *       writes.
 *
 *    -- All the nodes with DESYNC partition take writes as long as nothing is
 *       migrated into it.  Once master has received data from all the duplicates
 *       it transfers data to all nodes in replica list at that time all DESYNC
 *       nodes will write journal.
 *
 *    Golden rule is at any point of time DESYNC partition receives data from
 *    only one source. Once it has become SYNC it can get from multiple sources
 *    and merge ? Why is it needed  ?????
 *
 *  - Replication Factor: [p->p_repl_factor]
 *    The number of replica system maintain. All the nodes in the replica list
 *    after replication factor does not have partition in stable cluster view.
 *
 *  - Replica List : [p->replica up to N where N < p->p_repl_factor]
 *    List of master and replica nodes in the new cluster view. All the nodes
 *    within replication factor in the nodes hash list is replica list
 *
 *  - DESYNC Partition:
 *    Partition on a node in the replica list is DESYNC state if it has no data.
 *    replica[0] is master
 *
 *  - SYNC PARTITION:
 *    Partition on a node in the replica list is put in SYNC state if it has
 *    some version of data for that partition with it.
 *
 *  - ZOMBIE:
 *    Partition on nodes outside the replica list is put in ZOMBIE state if it
 *    has some version of data for that partition with it.	 \
 *
 *  - WAIT:
 *    Partition is put into wait state while moving from SYNC or ZOMBIE to
 *    ABSENT. This state is stage is reached when there are pending writes
 *    are there. And is needed to make sure any new writes, while last few
 *    writes are getting flushed is not allowed.
 *
 *    NB: this today is done after indicating to master that migration is done
 *        but ideally should be done after that (see order of DoneMigrate:
 *        and CompletedMigrate: in migrate_xmit_fn in migrate.c)
 *
 *  - ABSENT:
 *    Partition on the nodes outside the replica list put in the ABSENT state
 *    if it has no data.
 *
 *  ALGORITHM
 *  =========
 *
 *  - Master or acting master are the only nodes where all the data is merged
 *    and duplicates are resolved
 *
 *  - Master if DESYNC gets data from the First Primary Version node AKA origin
 *    (This is acting master while master id desync and does the merge).
 *
 *  - Writes which come in while migration was going on and master is DESYNC
 *    is proxied to the origin which does merge/apply write and replicate to
 *    the replica list.
 *
 *  - Merge is duplicate resolution. Bring in all the duplicates to the master
 *    /acting master to apply writes. And replicate it to all the nodes in the
 *    replica set.
 *
 *  - On receiving replicate request, DESYNC nodes in replica list write
 *    journals (including master). SYNC node in replica list reject write while
 *    merge is going on.
 *
 *  - Master becomes SYNC once it has received data from acting master. Before
 *    turning into SYNC after migration is finished master applies the write
 *    journal.
 *
 *  - SYNC master requests data from all the duplicates. Once it has got data
 *    from all the nodes. It ships back the final value to all the nodes in
 *    the replica list.
 *
 * NB: Please note that write journalling and write rejection is
 *     primarily relevant only in the world where replication was delta
 *     replication. But current (5/13) we do not do delta replication but we
 *     ship the entire record. So write_journal and write rejection in current
 *     world is not relevant. Revisit and fix comment
 */

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_queue.h"

#include "fault.h"
#include "util.h"

#include "base/cfg.h"
#include "base/cluster_config.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/thr_write.h"
#include "fabric/fabric.h"
#include "fabric/migrate.h"
#include "fabric/paxos.h"
#include "storage/storage.h"


// #define PARTITION_INFO_CHECK 1


// Using int for 4-byte size, but maintaining bool semantics.
static volatile int g_allow_migrations = true;
static volatile int g_multi_node = false;

#define BALANCE_INIT_UNRESOLVED 0
#define BALANCE_INIT_RESOLVED   1

typedef enum {
	MASTER_NOT_WAITING,
	MASTER_WAITING,
	MASTER_DONE_WAITING
} master_wait_state;

static volatile int g_balance_init = BALANCE_INIT_UNRESOLVED;


// Return number of partitions found in storage.
int
as_partition_get_state_from_storage(as_namespace *ns, bool *partition_states)
{
	memset(partition_states, 0, sizeof(bool) * AS_PARTITIONS);

	int n_found = 0;

	for (int j = 0; j < AS_PARTITIONS; j++) {
		as_partition_vinfo vinfo;
		size_t vinfo_len = sizeof(vinfo);

		// Find if the value has been set in storage.
		if (as_storage_info_get(ns, j, (uint8_t *)&vinfo, &vinfo_len) == 0) {
			if (vinfo_len == sizeof(as_partition_vinfo)) {
				if (! is_partition_null(&vinfo)) {
					partition_states[j] = true;
					n_found++;
				}
			}
			// else - treat partition as lost - common on startup
		}
		// else - TODO - is this serious?
	}

	return n_found;
}


void
set_partition_version_in_storage(as_namespace *ns, size_t pid, as_partition_vinfo *vinfo, bool flush)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL, "invalid partition id");
	cf_assert(vinfo, AS_PARTITION, CF_CRITICAL, "invalid version info");

	if (as_storage_info_set(ns, pid, (uint8_t *)vinfo, sizeof(as_partition_vinfo)) == 0) {
		// TODO flushing the data on every version change could be expensive
		if (flush) { // flush if specified
			as_storage_info_flush(ns);
		}
	}
	else {
		cf_warning(AS_PARTITION, "{%s:%d} setting version in storage failed, version %"PRIx64" will not be set in storage!",
				ns->name, pid, vinfo->iid);
	}
}


void
clear_partition_version_in_storage(as_namespace *ns, size_t pid, bool flush)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL, "invalid partition id");

	as_partition_vinfo null_vinfo;

	memset(&null_vinfo, 0, sizeof(null_vinfo));
	set_partition_version_in_storage(ns, pid, &null_vinfo, flush);
}


// Partition version accessor functions.

void
generate_new_partition_version(as_partition_vinfo *new_vinfo)
{
	memset(new_vinfo, 0, sizeof(as_partition_vinfo));
	new_vinfo->iid = as_paxos_get_cluster_key();
	new_vinfo->vtp[0] = (uint16_t)1;
}


bool
is_partition_null(as_partition_vinfo *new_vinfo)
{
	return 0 == new_vinfo->iid;
}


bool
increase_partition_version_tree_path(as_partition_vinfo *vinfo, cf_node fsn, cf_node *old_sl, const char* n, size_t pid)
{
	// Find the first sync node's index in the old_succession list.
	size_t old_fsn_index;
	bool found = false;

	for (int k = 0; k < g_config.paxos_max_cluster_size; k++) {
		if (old_sl[k] == fsn) {
			old_fsn_index = k;
			found = true;
			break;
		}
	}

	if (! found) {
		return false;
	}

	int i;

	for (i = 0; i < AS_PARTITION_MAX_VERSION; i++) {
		if (vinfo->vtp[i] == 0) {
			vinfo->vtp[i] = old_fsn_index + 1;
			break;
		}
	}

	// If we run out of space generate a new number -
	// all optimizations with merging existing versions will be lost.
	if (i == AS_PARTITION_MAX_VERSION) {
		generate_new_partition_version(vinfo);
	}

	return true;
}


// Set the version of the partition to the new value but only if the old one
// matches. This function is only called from as_partition_balance_new, so do
// not flush to storage.
void
set_new_partition_version(as_partition_vinfo *dest,
		as_partition_vinfo *old, as_partition_vinfo *new,
		as_namespace *ns, size_t pid)
{
	if (! dest || ! new || ! old || ! ns) {
		return;
	}

	if (memcmp(dest, old, sizeof(as_partition_vinfo)) == 0) {
		set_partition_version_in_storage(ns, pid, new, false);
		memcpy(dest, new, sizeof(as_partition_vinfo));
	}
}


void
print_partition_versions(const char* n, size_t pid, as_partition_vinfo *part1, const char *mess1, as_partition_vinfo *part2, const char *mess2)
{
	cf_warning(AS_PARTITION, "{%s:%d} %s %"PRIx64":%"PRIx64"-%"PRIx64" %s %"PRIx64":%"PRIx64"-%"PRIx64,
			n, pid,
			mess1,
			part1->iid, *(uint64_t*)&part1->vtp[0], *(uint64_t*)&part1->vtp[8],
			mess2,
			part2->iid, *(uint64_t*)&part2->vtp[0], *(uint64_t*)&part2->vtp[8]);
}


void
as_partition_allow_migrations()
{
	cf_info(AS_PARTITION, "ALLOW MIGRATIONS");
	g_allow_migrations = true;
}


void
as_partition_disallow_migrations()
{
	cf_info(AS_PARTITION, "DISALLOW MIGRATIONS");
	g_allow_migrations = false;
}


bool
as_partition_get_migration_flag()
{
	return g_allow_migrations;
}


// Reinitialize an as_partition.
// Should always be called while holding the partition lock.
void
as_partition_reinit(as_partition *p, as_namespace *ns, int pid)
{
	cf_assert(p, AS_PARTITION, CF_CRITICAL, "invalid partition");

	memset(p->replica, 0, sizeof(cf_node) * g_config.paxos_max_cluster_size);
	p->origin = 0;
	p->target = 0;
	p->state = AS_PARTITION_STATE_ABSENT;
	p->pending_migrate_tx = 0;
	p->pending_migrate_rx = 0;

	memset(p->replica_tx_onsync, 0, sizeof(p->replica_tx_onsync));

	p->n_dupl = 0;
	memset(p->dupl_nodes, 0, sizeof(p->dupl_nodes));
	p->master_wait_state = MASTER_NOT_WAITING;
	memset(&p->primary_version_info, 0, sizeof(p->primary_version_info));
	memset(&p->version_info, 0, sizeof(p->version_info));
	memset(p->old_sl, 0, sizeof(p->old_sl));
	p->p_repl_factor = ns->replication_factor;
	p->current_outgoing_ldt_version = 0;

	p->cluster_key = 0;

	as_index_tree *t = p->vp;

	// First initialization is the only time there's a null tree pointer.
	if (! p->vp && ! ns->cold_start) {
		if (! ns->tree_roots) {
			cf_crash(AS_PARTITION, "ns %s pid %d has null tree roots", ns->name,
					pid);
		}

		p->vp = as_index_tree_resume(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				&ns->tree_roots[pid]);

		// There's no going back to cold start now - do so the harsh way.
		if (! p->vp) {
			cf_crash(AS_PARTITION, "ns %s pid %d fail tree resume", ns->name,
					pid);
		}
	}
	else {
		p->vp = as_index_tree_create(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				ns->tree_roots ? &ns->tree_roots[pid] : NULL);
	}

	if (t) {
		as_index_tree_release(t, ns);
	}

	as_index_tree *sub_t = p->sub_vp;

	// First initialization is the only time there's a null tree pointer.
	if (! p->sub_vp && ! ns->cold_start) {
		if (! ns->sub_tree_roots) {
			cf_crash(AS_PARTITION, "ns %s pid %d has null sub-tree roots",
					ns->name, pid);
		}

		p->sub_vp = as_index_tree_resume(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				&ns->sub_tree_roots[pid]);

		// There's no going back to cold start now - do so the harsh way.
		if (! p->sub_vp) {
			cf_crash(AS_PARTITION, "ns %s pid %d fail tree resume", ns->name,
					pid);
		}
	}
	else {
		p->sub_vp = as_index_tree_create(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				ns->sub_tree_roots ? &ns->sub_tree_roots[pid] : NULL);
	}

	if (sub_t) {
		as_index_tree_release(sub_t, ns);
	}
}


// Set a partition to be in the desync state.
// Should always be called within partition lock.
// Set the state variable and clean out the version info.
void
set_partition_desync_lockfree(as_partition *p, as_partition_vinfo *vinfo,
		as_namespace *ns, size_t pid, bool flush)
{
	p->state = AS_PARTITION_STATE_DESYNC;

	as_index_tree *t = p->vp;

	p->vp = as_index_tree_create(ns->arena,
			(as_index_value_destructor)&as_record_destroy, ns,
			ns->tree_roots ? &ns->tree_roots[pid] : NULL);
	as_index_tree_release(t, ns);

	as_index_tree *sub_t = p->sub_vp;

	p->sub_vp = as_index_tree_create(ns->arena,
			(as_index_value_destructor)&as_record_destroy, ns,
			ns->sub_tree_roots ? &ns->sub_tree_roots[pid] : NULL);
	as_index_tree_release(sub_t, ns);

	clear_partition_version_in_storage(ns, pid, flush);
	memset(vinfo, 0, sizeof(as_partition_vinfo));
}


// Set a partition to be in the absent state.
// Should always be called within partition lock.
// Set the state variable and clean out the version info.
void
set_partition_absent_lockfree(as_partition *p, as_partition_vinfo *vinfo, as_namespace *ns, size_t pid, bool flush)
{
	as_index_tree *t = p->vp;

	p->vp = as_index_tree_create(ns->arena, (as_index_value_destructor)&as_record_destroy, ns, ns->tree_roots ? &ns->tree_roots[pid] : NULL);
	// A Change:  Set the State BEFORE the tree release, just in case that
	// is opening too large of a time window.
	p->state = AS_PARTITION_STATE_ABSENT; // Move the state setting ABOVE the tree release.

	if (t) {
		as_index_tree_release(t, ns);
	}

	as_index_tree *sub_t = p->sub_vp;

	p->sub_vp = as_index_tree_create(ns->arena, (as_index_value_destructor)&as_record_destroy, ns, ns->sub_tree_roots ? &ns->sub_tree_roots[pid] : NULL);

	if (sub_t) {
		as_index_tree_release(sub_t, ns);
	}

	p->current_outgoing_ldt_version = 0;
	clear_partition_version_in_storage(ns, pid, flush);
	memset(vinfo, 0, sizeof(as_partition_vinfo));
}


// Set a partition to be in the sync state.
// Should always be called within partition lock.
// Set the state variables and initialize new version info.
void
set_partition_sync_lockfree(as_partition *p, size_t pid, as_namespace *ns, bool flush)
{
	p->state = AS_PARTITION_STATE_SYNC;

	// If the node is master and it partition_vinfo is already set, do nothing.
	if  (g_config.self_node == p->replica[0] && ! is_partition_null(&p->version_info)) {
		if (memcmp(&p->version_info, &p->primary_version_info, sizeof(as_partition_vinfo)) != 0) {
			cf_warning(AS_PARTITION, "{%s:%d} Attempt to set a master sync partition to a non-primary version value", ns->name, pid);
		}

		return;
	}

	if  (is_partition_null(&p->primary_version_info)) {
		cf_warning(AS_PARTITION, "{%s:%d} Failed: Attempt to set partition sync with primary version NULL", ns->name, pid);
		return;
	}

	// Set the version in storage if needed
	set_partition_version_in_storage(ns, pid, &p->primary_version_info, flush);
	// Copy new partition version. it is always set to the the primary version.
	memcpy(&p->version_info, &p->primary_version_info, sizeof(as_partition_vinfo));
}


void
as_partition_init(as_partition *p, as_namespace *ns, int pid)
{
	pthread_mutex_init(&p->lock, 0);

	p->vp = NULL;
	p->sub_vp = NULL;
	as_partition_reinit(p, ns, pid);
}


// Summarize the partition states, populating the supplied structure.
void
as_partition_getstates(as_partition_states *ps)
{
	size_t active_partition_count = 0;

	memset(ps, 0, sizeof(as_partition_states));

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		size_t ns_absent_partitions = 0;

		for (int j = 0; j < AS_PARTITIONS; j++) {
			as_partition *p = &ns->partitions[j];

			pthread_mutex_lock(&p->lock);

			switch (p->state) {
			case AS_PARTITION_STATE_UNDEF:
				ps->undef++;
				break;
			case AS_PARTITION_STATE_SYNC:
			{
				cf_node n;

				if (0 == p->target) {
					n = p->origin != (cf_node)0 ? p->origin : p->replica[0];
				}
				else {
					n = p->origin != (cf_node)0 ? p->origin : g_config.self_node;
				}

				if (g_config.self_node == n) {
					ps->sync_actual++;
				}
				else {
					ps->sync_replica++;
				}
			}
			break;

			case AS_PARTITION_STATE_DESYNC:
				ps->desync++;
				break;
			case AS_PARTITION_STATE_ZOMBIE:
				ps->zombie++;
				break;
			case AS_PARTITION_STATE_ABSENT:
				ps->absent++;
				ns_absent_partitions++;
				break;
			default:
				cf_crash(AS_PARTITION, "{%s:%d} in illegal state %d",
						ns->name, j, (int)p->state);
			}


			if (p->pending_migrate_tx != 0 || p->pending_migrate_rx != 0 ||
					p->origin != 0 || p->n_dupl != 0) {
				active_partition_count++;
			}

			ps->n_objects += p->vp->elements;
			ps->n_ref_count += cf_rc_count(p->vp);
			ps->n_sub_objects += p->sub_vp->elements;
			ps->n_sub_ref_count += cf_rc_count(p->sub_vp);

			pthread_mutex_unlock(&p->lock);
		}

		cf_atomic_int_set(&ns->n_absent_partitions, ns_absent_partitions);
		cf_atomic_int_set(&ns->n_actual_partitions, ps->sync_actual);
	}
}


static int
find_in_replica_list(as_partition *p, cf_node self)
{
	int my_index = -1;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (p->replica[i] == (cf_node)0) {
			break;
		}

		if (p->replica[i] == self) {
			my_index = i;
			break;
		}
	}

	return my_index;
}


static void
as_partition_health_check(as_namespace *ns, size_t pid, as_partition *p,
		int my_index)
{
	as_partition_vinfo *pvinfo = &ns->partitions[pid].version_info;
	bool is_sync    = (p->state == AS_PARTITION_STATE_SYNC);
	bool is_desync  = (p->state == AS_PARTITION_STATE_DESYNC);
	bool is_zombie  = (p->state == AS_PARTITION_STATE_ZOMBIE);
	bool is_master  = (0 == my_index);
	bool is_replica = (0 < my_index) && (my_index < p->p_repl_factor);
	bool is_primary = memcmp(pvinfo, &p->primary_version_info,
			sizeof(as_partition_vinfo)) == 0;
	bool migrating_to_master = (p->target != 0);

	// State consistency checks.
	if (migrating_to_master) {
		if (p->target != p->replica[0]) {
			cf_warning(AS_PARTITION, "{%s:%d} Partition state error on write reservation. Target of migration not master node",
					ns->name, pid);
		}

		if (! ((is_zombie && is_primary) || (is_replica && is_sync && is_primary))) {
			cf_warning(AS_PARTITION, "{%s:%d} Partition state error on write reservation. Illegal state in node migrating to master",
					ns->name, pid);
		}
	}

	if (((is_replica && is_desync) || (is_replica && is_sync && ! is_primary))
			&& p->origin != p->replica[0]) {
		cf_warning(AS_PARTITION, "{%s:%d} Partition state error on write reservation. origin does not match master",
				ns->name, pid);
	}
	else if (is_replica && is_sync && is_primary && ! migrating_to_master
			&& p->origin && p->origin != p->replica[0]) {
		cf_warning(AS_PARTITION, "{%s:%d} Partition state error on write reservation. replica sync node's origin does not match master",
				ns->name, pid);
	}
	else if (is_master && is_desync && p->origin == (cf_node)0) {
		cf_warning(AS_PARTITION, "{%s:%d} Partition state error on write reservation. Origin node is NULL for non-sync master",
				ns->name, pid);
	}

	for (int i = 0; i < p->p_repl_factor; i++) {
		if (p->replica[i] == (cf_node)0
				&& as_partition_balance_is_init_resolved()) {
			cf_warning(AS_PARTITION, "{%s:%d} Detected state error. Replica list contains null node at position %d",
					ns->name, pid, i);
			cf_atomic_int_incr(&g_config.err_replica_null_node);
		}
	}

	for (int i = p->p_repl_factor; i < g_config.paxos_max_cluster_size; i++) {
		if (p->replica[i] != (cf_node)0) {
			cf_warning(AS_PARTITION, "{%s:%d} Detected state error. Replica list contains non null node %"PRIx64" at position %d",
					ns->name, pid, p->replica[i], i);
			cf_atomic_int_incr(&g_config.err_replica_non_null_node);
		}
	}
}


static cf_atomic32 g_partition_check_counter = 0;


// Find best node to handle read/write. Called within partition lock.
static cf_node
find_sync_copy(as_namespace *ns, size_t pid, as_partition *p, bool is_read)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL,
			"invalid partition id");
	cf_assert(p, AS_PARTITION, CF_CRITICAL, "invalid partition");

	cf_node n = (cf_node)0;
	cf_node self = g_config.self_node;
	// Find location of self in replica list, returns -1 if not found.
	int my_index = find_in_replica_list(p, self);

	// Do health check occasionally (expensive to do for every read/write).
	if ((cf_atomic32_incr(&g_partition_check_counter) & 0x0FFF) == 0) {
		as_partition_health_check(ns, pid, p, my_index);
	}

	// Find an appropriate copy of this partition.
	//
	// Return this node if:
	//		- node is (eventual) master and sync
	//		- node is migrating to master (i.e. is acting master)
	// Return origin node if:
	//		- node is (eventual) master and desync
	// Return this node if:
	//		- it's a read, node is replica, and has no origin
	// Otherwise, return (eventual) master.

	bool is_sync    = (p->state == AS_PARTITION_STATE_SYNC);
	bool is_desync  = (p->state == AS_PARTITION_STATE_DESYNC);
	bool is_master  = (0 == my_index);
	bool is_replica = (0 < my_index) && (my_index < p->p_repl_factor);
	bool migrating_to_master = (p->target != 0);

	if ((is_master && is_sync) || migrating_to_master) {
		n = self;
	}
	else if (is_master && is_desync) {
		n = p->origin;
	}
	else if (is_read && is_replica && p->origin == (cf_node)0) {
		n = self;
	}
	else {
		n = p->replica[0];
	}

	if (n == 0 && as_partition_balance_is_init_resolved()) {
		cf_warning(AS_PARTITION, "{%s:%d} Returning null node, could not find sync copy of this partition my_index %d, master %"PRIx64" replica %"PRIx64" origin %"PRIx64,
				ns->name, pid, my_index, p->replica[0], p->replica[1], p->origin);
		cf_atomic_int_incr(&g_config.err_sync_copy_null_master);
	}

	return n;
}


// If this node is an eventual master, return the acting master, else return 0.
cf_node
as_partition_proxyee_redirect(as_namespace *ns, as_partition_id pid)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL, "invalid partition id");

	as_partition* p = &ns->partitions[pid];
	cf_node self = g_config.self_node;

	pthread_mutex_lock(&p->lock);
	bool is_master = (0 == find_in_replica_list(p, self));
	bool is_desync = (p->state == AS_PARTITION_STATE_DESYNC);
	cf_node eventual = p->origin;
	pthread_mutex_unlock(&p->lock);

	return is_master && is_desync ? eventual : (cf_node)0;
}


// A rare case where the source and dest both have a copy, NOT THE ACTUAL
// RESERVATION.
void
as_partition_reservation_copy(as_partition_reservation *dst, as_partition_reservation *src)
{
	dst->ns = src->ns;
	dst->is_write = src->is_write;
	dst->pid = src->pid;
	dst->p = src->p;
	dst->state = src->state;
	dst->tree = src->tree;
	dst->sub_tree = src->sub_tree;
	dst->n_dupl = src->n_dupl;
	memcpy(dst->dupl_nodes, src->dupl_nodes, sizeof(cf_node) * dst->n_dupl);
	dst->cluster_key = src->cluster_key;
	memcpy(&dst->vinfo, &src->vinfo, sizeof(as_partition_vinfo));
}


// Simply moves the reservation from one structure to the other.
void
as_partition_reservation_move(as_partition_reservation *dst, as_partition_reservation *src)
{
	as_partition_reservation_copy(dst, src);
	memset(src, 0, sizeof(as_partition_reservation));
}


// Update a reservation on a partition.
void
as_partition_reserve_update_state(as_partition_reservation *rsv)
{
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");

	pthread_mutex_lock(&rsv->p->lock);

	rsv->state = rsv->p->state;
	rsv->n_dupl = rsv->p->n_dupl;
	memcpy(rsv->dupl_nodes, rsv->p->dupl_nodes, sizeof(cf_node) * rsv->n_dupl);
	rsv->cluster_key = rsv->p->cluster_key;

	if (! is_partition_null(&rsv->p->version_info)) {
		memcpy(&rsv->vinfo, &rsv->p->version_info, sizeof(as_partition_vinfo));
	}
	else {
		memcpy(&rsv->vinfo, &rsv->p->primary_version_info, sizeof(as_partition_vinfo));
	}

	pthread_mutex_unlock(&rsv->p->lock);
}


// Obtain a write reservation on a partition, or get the address of a
// node who can.
// On success, the provided as_partition_reservation * is filled in with the appropriate
// reserved tree, namespace, etc and the pending write count is incremented;
// On failure, the provided reservation is not touched or initialized
//
// In either case, the node is returned.
int
as_partition_reserve_read_write(as_namespace *ns, as_partition_id pid,
		as_partition_reservation *rsv, cf_node *node,
		bool is_read, uint64_t *cluster_key)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL,
			"invalid partition");

	int rv = -1;
	as_partition *p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	uint64_t ck = p->cluster_key;
	cf_node n = find_sync_copy(ns, pid, p, is_read);

	// If we're aren't writeable, return.
	if (n != g_config.self_node) {
		goto finish;
	}

	// This should always be true (desyncs will be caught above in the
	// migration path checking).
	if (AS_PARTITION_STATE_SYNC == p->state
			|| AS_PARTITION_STATE_ZOMBIE == p->state) {
		rsv->ns = ns;
		rsv->is_write = is_read ? false : true;
		rsv->pid = pid;
		rsv->p = p;

		cf_rc_reserve(p->vp);
		rsv->tree = p->vp;
		cf_rc_reserve(p->sub_vp);
		rsv->sub_tree = p->sub_vp;

		rsv->state = p->state;

		rsv->n_dupl = p->n_dupl;
		memcpy(rsv->dupl_nodes, p->dupl_nodes, sizeof(cf_node) * rsv->n_dupl);

		rsv->cluster_key = p->cluster_key;

		// Copy version info. this is guaranteed to not be null as the state is
		// SYNC or ZOMBIE.
		memcpy(&rsv->vinfo, &p->version_info, sizeof(as_partition_vinfo));
		rv = 0;
	}
	else { // safety!
		memset(rsv, 0, sizeof(*rsv));
	}

finish:
	pthread_mutex_unlock(&p->lock);

	if (node) {
		*node = n;
	}

	if (cluster_key) {
		*cluster_key = ck;
	}

	return rv;
}


// Reserve a partition without doing any safety checking and bypassing the
// partition state lock.
void
as_partition_reserve_lockfree(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL,
			"invalid partition");

	as_partition *p = &ns->partitions[pid];

	rsv->ns = ns;
	rsv->is_write = false;
	rsv->pid = pid;
	rsv->p = p;

	cf_rc_reserve(p->vp);
	rsv->tree = p->vp;
	cf_rc_reserve(p->sub_vp);
	rsv->sub_tree = p->sub_vp;

	rsv->state = p->state;

	rsv->n_dupl = p->n_dupl;
	memcpy(rsv->dupl_nodes, p->dupl_nodes, sizeof(cf_node) * rsv->n_dupl);

	rsv->cluster_key = p->cluster_key;

	if (! is_partition_null(&p->version_info)) {
		memcpy(&rsv->vinfo, &p->version_info, sizeof(as_partition_vinfo));
	}
	else {
		memcpy(&rsv->vinfo, &p->primary_version_info,
				sizeof(as_partition_vinfo));
	}
}


int
as_partition_reserve_migrate_timeout(as_namespace *ns, as_partition_id pid,
		as_partition_reservation *rsv, cf_node *node, int timeout_ms)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL,
			"invalid partition");

	as_partition *p = &ns->partitions[pid];

	struct timespec tp;
	cf_set_wait_timespec(timeout_ms, &tp);

	if (0 != pthread_mutex_timedlock(&p->lock, &tp)) {
		return -1;
	}

	as_partition_reserve_lockfree(ns, pid, rsv);

	pthread_mutex_unlock(&p->lock);

	if (node) {
		*node = g_config.self_node;
	}

	return 0;
}


// Reserve a partition for migration; this bypasses most all safety
// checking, so never returns failure even though it has a return code.
void
as_partition_reserve_migrate(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv, cf_node *node)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");
	cf_assert((pid < AS_PARTITIONS), AS_PARTITION, CF_CRITICAL,
			"invalid partition");

	as_partition *p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	as_partition_reserve_lockfree(ns, pid, rsv);

	pthread_mutex_unlock(&p->lock);

	if (node) {
		*node = g_config.self_node;
	}
}


// Obtain a write reservation on a partition, or get the address of a
// node who can.
// On success, the provided as_partition_reservation * is filled in with the appropriate
// reserved tree, namespace, etc and the pending write count is incremented;
// On failure, the provided reservation is not touched or initialized
//
// In either case, the node is returned. */
int
as_partition_reserve_write(as_namespace *ns, as_partition_id pid,
		as_partition_reservation *rsv, cf_node *node, uint64_t *cluster_key)
{
	return as_partition_reserve_read_write(ns, pid, rsv, node, false,
			cluster_key);
}


// Reserve a partition for reads.
// return value 0 means the reservation was taken, -1 means not
// in either case, the node is always filled out with who should be contacted
int
as_partition_reserve_read(as_namespace *ns, as_partition_id pid,
		as_partition_reservation *rsv, cf_node *node, uint64_t *cluster_key)
{
	return as_partition_reserve_read_write(ns, pid, rsv, node, true,
			cluster_key);
}


// Reserve a partition for query.
// return value 0 means the reservation was taken, -1 means not.
int
as_partition_reserve_query(as_namespace *ns, as_partition_id pid,
		as_partition_reservation *rsv)
{
	return as_partition_reserve_write(ns, pid, rsv, NULL, NULL);
}


// Reserves all query-able partitions.
// Returns the number of partitions reserved.
int
as_partition_prereserve_query(as_namespace * ns, bool can_partition_query[],
		as_partition_reservation rsv[])
{
	int reserved = 0;

	for (int i = 0; i < AS_PARTITIONS; i++) {
		if (as_partition_reserve_query(ns, i, &rsv[i])) {
			can_partition_query[i] = false;
		}
		else {
			can_partition_query[i] = true;
			reserved++;
		}
	}

	return reserved;
}


// Release a reservation on a partition without holding the lock.
void
as_partition_release_lockfree(as_partition_reservation *rsv)
{
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");
	cf_assert(rsv->p, AS_PARTITION, CF_CRITICAL, "invalid reservation partition");
	cf_assert(rsv->tree, AS_PARTITION, CF_CRITICAL, "invalid reservation tree");

	as_index_tree_release(rsv->tree, rsv->ns);
	as_index_tree_release(rsv->sub_tree, rsv->ns);

	// safety
	rsv->tree = 0;
	rsv->sub_tree = 0;
	rsv->p = 0;
	rsv->ns = 0;
	rsv->n_dupl = 0;
	memset(rsv->dupl_nodes, 0, sizeof(rsv->dupl_nodes));
	rsv->cluster_key = 0;
	memset(&rsv->vinfo, 0, sizeof(as_partition_vinfo));
}


// Release a reservation on a partition's tree, and decrement the pending
// write count if appropriate.
void
as_partition_release(as_partition_reservation *rsv)
{
	cf_assert(rsv, AS_PARTITION, CF_CRITICAL, "invalid reservation");
	cf_assert(rsv->p, AS_PARTITION, CF_CRITICAL, "invalid reservation partition");
	cf_assert(rsv->tree, AS_PARTITION, CF_CRITICAL, "invalid reservation tree");

	pthread_mutex_lock(&rsv->p->lock);

	as_index_tree_release(rsv->tree, rsv->ns);
	as_index_tree_release(rsv->sub_tree, rsv->ns);

	pthread_mutex_unlock(&rsv->p->lock);

	// safety
	rsv->tree = 0;
	rsv->sub_tree = 0;
	rsv->p = 0;
	rsv->ns = 0;
	memset(rsv->dupl_nodes, 0, sizeof(cf_node) * rsv->n_dupl);
	rsv->n_dupl = 0;
	rsv->cluster_key = 0;
	memset(&rsv->vinfo, 0, sizeof(as_partition_vinfo));
}


// Get the node ID of a read replica for a given partition in a namespace;
// preferentially return the local node if possible.
cf_node
as_partition_getreplica_read(as_namespace *ns, as_partition_id pid)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");

	as_partition *p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	cf_node n = find_sync_copy(ns, pid, p, true);

	pthread_mutex_unlock(&p->lock);

	return n;
}


// Get the node ID of a read replica for a given partition in a namespace;
// preferentially return the local node if possible. This function is meant to
// return exclusively the read-only replicas or the proles. This function won't
// return master nodes which are read and write.
cf_node
as_partition_getreplica_prole(as_namespace *ns, as_partition_id pid)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");

	as_partition *p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	// Check is this is a master node.
	cf_node n = find_sync_copy(ns, pid, p, false);

	if (n == g_config.self_node) {
		// It's a master, return 0.
		n = (cf_node)0;
	}
	else {
		// Not a master, see if it's a prole.
		n = find_sync_copy(ns, pid, p, true);
	}

	pthread_mutex_unlock(&p->lock);

	return n;
}


// Get a list of all the node IDs that are replicas for a specified
// partition: place the list in *nv and return the number of nodes found.
int
as_partition_getreplica_readall(as_namespace *ns, as_partition_id pid, cf_node *nv)
{
	int c = 0;
	as_partition *p = NULL;
	cf_node self = g_config.self_node;

	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");
	p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// Break at the end of the list.
		if ((cf_node)0 == p->replica[i]) {
			break;
		}

		// Don't ever include yourself.
		if (self == p->replica[i]) {
			continue;
		}

		// Copy the node ID into the user-supplied vector.
		nv[c++] = p->replica[i];
	}

	pthread_mutex_unlock(&p->lock);

	return c;
}


// Get the node ID of the node that is the actual for the specified
// partition.
cf_node
as_partition_getreplica_write(as_namespace *ns, as_partition_id pid)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");

	as_partition *p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	cf_node n = find_sync_copy(ns, pid, p, false);

	pthread_mutex_unlock(&p->lock);

	return n;
}


// currently an exact copy of getreplica_write, but we'll see if this can be
// optimized later-on, for example returning status instead of cf_node.
// Get the node ID of the node that is the actual for the specified
// partition.
cf_node
as_partition_getreplica_master(as_namespace *ns, as_partition_id pid)
{
	cf_assert(ns, AS_PARTITION, CF_CRITICAL, "invalid namespace");

	as_partition *p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	cf_node n = find_sync_copy(ns, pid, p, false);

	pthread_mutex_unlock(&p->lock);

	return n;
}


void
as_partition_get_replicas_all(as_namespace *ns, as_partition_id pid, bool *owned, int n_repl)
{
	for (int j = 0; j < n_repl; j++) {
		owned[j] = false;
	}

	as_partition *p = &ns->partitions[pid];
	cf_node self = g_config.self_node;

	pthread_mutex_lock(&p->lock);

	int my_index = find_in_replica_list(p, self); // -1 if node is not found
	bool am_master = (my_index == 0 && p->state == AS_PARTITION_STATE_SYNC) || p->target != 0;

	if (am_master) {
		owned[0] = true;
	}
	else if (my_index > 0 && p->origin == 0 && my_index < n_repl) {
		// Check my_index < n_repl only because n_repl could be out-of-sync with
		// (less than) partition's replica list count.
		owned[my_index] = true;
	}

	pthread_mutex_unlock(&p->lock);
}


// Reduce the entire set of write replicas I have into a particular dyn_buf
// suitable for handing to an inquisitive client.
void
as_partition_getreplica_write_str(cf_dyn_buf *db)
{
	size_t db_sz = db->used_sz;

	for (uint i = 0 ; i < g_config.n_namespaces ; i++ ) {
		as_namespace *ns = g_config.namespaces[i];

		for (uint j = 0 ; j < AS_PARTITIONS ; j++) {
			if (g_config.self_node == as_partition_getreplica_write(ns, j) ) {
				cf_dyn_buf_append_string(db, ns->name);
				cf_dyn_buf_append_char(db, ':');
				cf_dyn_buf_append_int(db, j);
				cf_dyn_buf_append_char(db, ';');
			}
		}
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


#define BITMAP_SIZE ((AS_PARTITIONS + 7) / 8)
#define B64_BITMAP_SIZE (((BITMAP_SIZE + 2) / 3) * 4)

void
as_partition_getreplica_master_str(cf_dyn_buf *db)
{
	uint8_t master_bitmap[BITMAP_SIZE];
	char b64_bitmap[B64_BITMAP_SIZE];

	size_t db_sz = db->used_sz;

	for (uint i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		memset(master_bitmap, 0, BITMAP_SIZE);
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		for (uint j = 0; j < AS_PARTITIONS; j++) {
			if (g_config.self_node == as_partition_getreplica_master(ns, j) ) {
				master_bitmap[j >> 3] |= (1 << (7 - (j & 7)));
			}
		}

		cf_b64_encode(master_bitmap, BITMAP_SIZE, b64_bitmap);
		cf_dyn_buf_append_buf(db, (uint8_t*)b64_bitmap, B64_BITMAP_SIZE);
		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


void
as_partition_getreplica_read_str(cf_dyn_buf *db)
{
	size_t db_sz = db->used_sz;

	for (uint i = 0 ; i < g_config.n_namespaces ; i++ ) {
		as_namespace *ns = g_config.namespaces[i];

		for (uint j = 0 ; j < AS_PARTITIONS ; j++) {
			if (g_config.self_node == as_partition_getreplica_read(ns, j) ) {
				cf_dyn_buf_append_string(db, ns->name);
				cf_dyn_buf_append_char(db, ':');
				cf_dyn_buf_append_int(db, j);
				cf_dyn_buf_append_char(db, ';');
			}
		}
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


void
as_partition_getreplica_prole_str(cf_dyn_buf *db)
{
	uint8_t prole_bitmap[BITMAP_SIZE];
	char b64_bitmap[B64_BITMAP_SIZE];

	size_t db_sz = db->used_sz;

	for (uint i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		memset(prole_bitmap, 0, sizeof(uint8_t) * BITMAP_SIZE);
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		for (uint j = 0; j < AS_PARTITIONS; j++) {
			if (g_config.self_node == as_partition_getreplica_prole(ns, j) ) {
				prole_bitmap[j >> 3] |= (1 << (7 - (j & 7)));
			}
		}

		cf_b64_encode(prole_bitmap, BITMAP_SIZE, b64_bitmap);
		cf_dyn_buf_append_buf(db, (uint8_t*)b64_bitmap, B64_BITMAP_SIZE);
		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


void
as_partition_get_replicas_all_str(cf_dyn_buf *db)
{
	size_t db_sz = db->used_sz;

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		int n_repl = (int)ns->replication_factor;

		cf_dyn_buf_append_int(db, n_repl);

		uint8_t repl_bitmaps[n_repl][BITMAP_SIZE];

		memset(repl_bitmaps, 0, sizeof(repl_bitmaps));

		bool owned[n_repl];

		for (uint32_t j = 0; j < AS_PARTITIONS; j++) {
			as_partition_get_replicas_all(ns, (as_partition_id)j, owned,
					n_repl);

			for (int n = 0; n < n_repl; n++) {
				if (owned[n]) {
					repl_bitmaps[n][j >> 3] |= (1 << (7 - (j & 7)));
				}
			}
		}

		char b64_bitmaps[n_repl][B64_BITMAP_SIZE];

		for (int n = 0; n < n_repl; n++) {
			cf_dyn_buf_append_char(db, ',');
			cf_b64_encode(repl_bitmaps[n], BITMAP_SIZE, b64_bitmaps[n]);
			cf_dyn_buf_append_buf(db, (uint8_t*)b64_bitmaps[n],
					B64_BITMAP_SIZE);
		}

		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


// Definition for the partition-info data:
// name:part_id:STATE:replica_count(int):origin:target:migrate_tx:migrate_rx:sz
char
as_partition_getstate_str(int state)
{
	switch (state) {
	case AS_PARTITION_STATE_UNDEF:
		return 'U';
	case AS_PARTITION_STATE_SYNC:
		return 'S';
	case AS_PARTITION_STATE_DESYNC:
		return 'D';
	case AS_PARTITION_STATE_ZOMBIE:
		return 'Z';
	case AS_PARTITION_STATE_ABSENT:
		return 'A';
	default:
		return '?';
	}
}


char
as_partition_gettxmigstate_str(int state)
{
	switch (state) {
	case AS_PARTITION_MIG_TX_STATE_NONE:
		return 'N';
	case AS_PARTITION_MIG_TX_STATE_SUBRECORD:
		return 'C';
	case AS_PARTITION_MIG_TX_STATE_RECORD:
		return 'P';
	default:
		return '?';
	}
}


void
as_partition_getinfo_str(cf_dyn_buf *db)
{
	size_t db_sz = db->used_sz;

	for (uint i = 0 ; i < g_config.n_namespaces ; i++ ) {
		as_namespace *ns = g_config.namespaces[i];

		for (uint j = 0 ; j < AS_PARTITIONS ; j++) {
			as_partition *p = &ns->partitions[j];
			char state_c = as_partition_getstate_str(p->state);

			// Find myself in the replica list.
			int replica_idx;

			for (replica_idx = 0; replica_idx < g_config.paxos_max_cluster_size; replica_idx++) {
				if (p->replica[replica_idx] == 0) {
					break;
				}

				if (p->replica[replica_idx] == g_config.self_node) {
					break;
				}
			}

			// This is debugging info.
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_int(db, j); // part_id
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_char(db, state_c);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_int(db, replica_idx); // partition index
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->origin);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->target);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->pending_migrate_tx);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->pending_migrate_rx);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, (uint64_t) p->vp->elements);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, (uint64_t) p->sub_vp->elements);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, p->current_outgoing_ldt_version);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_int(db, j);
			cf_dyn_buf_append_char(db, '-');
			cf_dyn_buf_append_uint64(db, p->version_info.iid);
			cf_dyn_buf_append_char(db, '-');
			cf_dyn_buf_append_uint64(db, p->version_info.vtp[0]);
			cf_dyn_buf_append_char(db, '-');
			cf_dyn_buf_append_uint64(db, p->version_info.vtp[8]);
			cf_dyn_buf_append_char(db, ';');
		}
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db); // take back the final ';'
	}
}

#ifdef PARTITION_INFO_CHECK
// Use this to dump out records from a partition -- especially records that
// should not be there (like, in an ABSENT partition).
static void
test_reduce_cb(as_index* r, void* udata)
{
	if (r && r->generation > 0) {
		// This function is called once for each record (as_index) object.
		cf_info_digest(AS_PARTITION, &r->key, "[REDUCE] RefCount(%u) Gen(%u) VoidTime(%u): ",
				r->rc, r->generation, r->void_time);
	}
	else {
		cf_info(AS_PARTITION, "[REDUCE_E] EMPTY RECORD: Rec Ptr(%p)", r);
	}
}
#endif


void
as_partition_get_master_prole_stats(as_namespace* ns,
		as_master_prole_stats* p_stats)
{
	p_stats->n_master_records = 0;
	p_stats->n_prole_records = 0;
	p_stats->n_master_sub_records = 0;
	p_stats->n_prole_sub_records = 0;

	cf_node self = g_config.self_node;

	for (int pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition *p = &ns->partitions[pid];

		pthread_mutex_lock(&p->lock);

		int my_index = find_in_replica_list(p, self); // -1 if node is not found
		bool am_master = (my_index == 0 && p->state == AS_PARTITION_STATE_SYNC) || p->target != 0;

		if (am_master) {
			p_stats->n_master_records += p->vp->elements;
			p_stats->n_master_sub_records += p->sub_vp->elements;
		}
		else if (my_index > 0 && p->origin == 0) {
			p_stats->n_prole_records += p->vp->elements;
			p_stats->n_prole_sub_records += p->sub_vp->elements;
		}
#ifdef PARTITION_INFO_CHECK
		// else we don't own a copy of this partition...  but maybe we need
		// to check and see if there's some residual data.
		else {
			int pcnt = 0;
			int tree_rc = 0;

			if (p->vp) {
				pcnt = p->vp->elements;
				tree_rc = cf_rc_count(p->vp);
			}

			// If this partition has values, print the stats and then also use
			// the iterator callback routine (test_reduce_cb) to print out the
			// record's digest, VoidTime and Generation.
			if (pcnt) {
				cf_info(AS_PARTITION, "[ATTENTION]<get_master_prole_stats()> NS(%s) Pid(%u) P State(%u) TPtr(%p) TRef(%d) P Cnt(%u) PendRx(%d) PenTx(%d) TreeCount(%u)",
						ns->name, pid, p->state, p->vp, tree_rc, pcnt,
						p->pending_migrate_rx, p->pending_migrate_tx,
						g_config.nsup_tree_count);

				if (p->vp && p->state == AS_PARTITION_STATE_ABSENT) {
					cf_info(AS_PARTITION, "[ATTENTION]<get_master_prole_stats()> Showing Contents of Absent Partition(%d)",
							pid);
					as_index_reduce_sync(p->vp, test_reduce_cb, NULL);
				}
			}
		}
#endif

		pthread_mutex_unlock(&p->lock);
	}
}


void
partition_migrate_record_fill(partition_migrate_record *pmr, cf_node dest,
		as_namespace *ns, as_partition_id pid, uint64_t cluster_key,
		uint32_t tx_flags)
{
	pmr->dest = dest;
	pmr->ns = ns;
	pmr->pid = pid;
	pmr->tx_flags = tx_flags;
	pmr->cluster_key = cluster_key;
}


void
apply_write_journal(as_namespace *ns, size_t pid)
{
	as_partition_reservation prsv;

	as_partition_reserve_lockfree(ns, pid, &prsv);

	if (0 != as_write_journal_apply(&prsv)) {
		cf_warning(AS_PARTITION, "{%s:%d} couldn't apply write journal",
				ns->name, pid);
	}

	as_partition_release_lockfree(&prsv);
}


// A callback from the migration unit to register state transitions
// This one called for migrates initiated from here, for transmit
// the only state we're really interested in is "DONE".
as_migrate_result
as_partition_migrate_tx(as_migrate_state s, as_namespace *ns,
		as_partition_id pid, uint64_t orig_cluster_key, uint32_t tx_flags)
{
	bool acting_master = (tx_flags & TX_FLAGS_ACTING_MASTER) != 0;
	bool migration_request = (tx_flags & TX_FLAGS_REQUEST) != 0;
	as_partition *p = &ns->partitions[pid];

	if (AS_MIGRATE_STATE_DONE != s) {
		if (s == AS_MIGRATE_STATE_ERROR) {
			;
		}
		else if (s == AS_MIGRATE_STATE_EAGAIN) {
			cf_warning(AS_PARTITION, "migration tx callback: migrate failed {%s:%d}", ns->name, pid);
		}
		else {
			cf_warning(AS_PARTITION, "migration tx callback: unknown notification %d {%s:%d}", (int) s, ns->name, pid);
		}

		return AS_MIGRATE_FAIL;
	}

	pthread_mutex_lock(&p->lock);

	if (orig_cluster_key != as_paxos_get_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	// Flush writes on the acting master now that it has completed filling the
	// real master with data.
	if (acting_master) {
		p->target = 0;
		p->n_dupl = 0;
		memset(p->dupl_nodes, 0, sizeof(p->dupl_nodes));
	}

	// Check if the migrate has been canceled by a partition rebalancing due to
	// a paxos vote. If this is the case, release the lock and return failure.
	// Otherwise, continue.
	if (p->pending_migrate_tx == 0) {
		cf_warning(AS_PARTITION, "{%s:%d} Concurrency event. Paxos reconfiguration occurred during migrate_tx?",
				ns->name, pid);
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	p->pending_migrate_tx--;

	if (! migration_request) {
		int64_t migrates_tx_remaining = cf_atomic_int_decr(
				&ns->migrate_tx_partitions_remaining);

		if (migrates_tx_remaining < 0){
			cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld) tx partitions schedule exceeded, possibly a race with prior migration",
					ns->name, pid, p->pending_migrate_tx, migrates_tx_remaining);
		}
	}

	p->current_outgoing_ldt_version = 0;

	if (AS_PARTITION_STATE_ZOMBIE == p->state && 0 == p->pending_migrate_tx) {
		set_partition_absent_lockfree(p, &ns->partitions[pid].version_info, ns, pid, true);
		cf_atomic_int_incr(&g_config.partition_generation);
	}

	pthread_mutex_unlock(&p->lock);

	return AS_MIGRATE_OK;
}


// When on the receiving end of a migrate, be notified that
// a migrate is starting (and give the chance to say "not now"),
// or be notified that the reception of the migrate data has concluded
//
// Return values meaning: (should change to enum)
//
// There are the following cases:
// 1. This node is DESYNC master and is receiving primary version
//    Apply journal and set state to sync (writes are allowed)
//    Request any duplicate copies. If there are none, migrate
//    data to replicas as indicated by migrate_tx_onsync[r]
// 2. This node is SYNC master and is receiving duplicates
//    If all duplicate merges are completed, then migrate merged
//    data to replicas as indicated by migrate_tx_onsync[r]
// 3. This node is SYNC replica with a duplicate partition copy
//    and is receiving a migrate request from master
//    Check that writes are disabled on this node
//    Schedule migrate to master
// 4. This node is SYNC replica and is receiving data from master
//    after a duplicate partition merge
//    Apply the merges and at the end, enable writes, if needed.
// 5. This node is DESYNC replica and is receiving data from
//    master.
//    Apply journal and set state to sync (writes are allowed).
// 6. This node is SYNC non-replica with a duplicate partition
//    copy (zombie) and receives a migrate request from master
//    Check that writes are disabled on this node.
//    Schedule migrate to master, at completion switch to absent.
as_migrate_result
as_partition_migrate_rx(as_migrate_state s, as_namespace *ns,
		as_partition_id pid, uint64_t orig_cluster_key, cf_node source_node)
{
	if (! g_allow_migrations) {
		return AS_MIGRATE_AGAIN;
	}

	as_migrate_result rv = AS_MIGRATE_OK;
	as_partition *p = &ns->partitions[pid];

	switch (s) {
	case AS_MIGRATE_STATE_START:
	{
		pthread_mutex_lock(&p->lock);

		if (orig_cluster_key != as_paxos_get_cluster_key()) {
			pthread_mutex_unlock(&p->lock);
			return AS_MIGRATE_AGAIN;
		}

		// Receiver-side migration flow control check.
		int64_t num_incoming = cf_atomic_int_incr(&g_config.migrate_num_incoming);

		if (num_incoming > g_config.migrate_max_num_incoming) {
			cf_atomic_int_decr(&g_config.migrate_num_incoming);
			cf_atomic_int_incr(&g_config.migrate_num_incoming_refused);
			pthread_mutex_unlock(&p->lock);
			return AS_MIGRATE_AGAIN;
		}

		cf_queue *mq = cf_queue_create(sizeof(partition_migrate_record), false);

		switch (p->state) {
		case AS_PARTITION_STATE_UNDEF:
			cf_warning(AS_PARTITION, "{%s:%d} migrate rx start while in state %d, fail",
					ns->name, pid, p->state);
			rv = AS_MIGRATE_FAIL;
			break;
		case AS_PARTITION_STATE_ABSENT:
			cf_warning(AS_PARTITION, "{%s:%d} migrate rx start while in state %d, already done (pending %d origin %"PRIx64")",
					ns->name, pid, p->state, p->pending_migrate_rx, p->origin);
			rv = AS_MIGRATE_ALREADY_DONE;
			break;
		case AS_PARTITION_STATE_DESYNC:
			if (0 != as_write_journal_start(ns, pid)) {
				// theoretically, a journal start fails only when there's already another journal in progress
				cf_warning(AS_PARTITION, "{%s:%d} could not start journal, continuing",
						ns->name, pid);
			}
			rv = AS_MIGRATE_OK;
			break;
		case AS_PARTITION_STATE_SYNC: // Allow migrations into sync
		case AS_PARTITION_STATE_ZOMBIE: // This is a migration request
			// A non-master node with flag waiting_for_master == true
			// Master just became sync and is requesting the duplicate
			// schedule a migrate now
			// do not decrement the migrate count since we expect
			// another migrate from master after the merge completes
			if (g_config.self_node != p->replica[0]) {
				if (p->master_wait_state == MASTER_WAITING) {
					if (source_node != p->replica[0]) {
						// this is a state corruption error
						cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. Waiting node received migrate request from non-master",
								ns->name, pid);
						rv = AS_MIGRATE_FAIL;
						break; // out of switch
					}

					p->master_wait_state = MASTER_DONE_WAITING;
					p->pending_migrate_tx++; // Send request to dupl node

					partition_migrate_record r;
					partition_migrate_record_fill(&r, p->replica[0], ns,
							pid, orig_cluster_key, TX_FLAGS_NONE);
					cf_queue_push(mq, &r);

					rv = AS_MIGRATE_ALREADY_DONE;
					break;
				}

				if (p->master_wait_state == MASTER_DONE_WAITING) {
					// Handles duplicate start request.
					rv = AS_MIGRATE_ALREADY_DONE;
					break;
				}
			}

			if (p->state == AS_PARTITION_STATE_ZOMBIE) {
				cf_warning(AS_PARTITION, "{%s:%d} migrate rx start while in zombie state %d, fail",
						ns->name, pid, p->state);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			// The node receiving a migrate can either be a master node
			//   or a replica node.
			// If it is the master node, then the node's origin must be
			//   null (only duplicate migrations allowed into master)
			// If it is a replica node, then it can be one of two cases:
			//  Case 1: A duplicate node
			//    In this case, add a journal for storing writes.
			//  Case 2: A sync replica of the primary partition version
			//    In this case, add a journal for storing writes.
			if (g_config.self_node != p->replica[0]) {
				bool is_replica = false;

				for (int i = 1; i < p->p_repl_factor; i++) {
					if (g_config.self_node == p->replica[i]) {
						is_replica = true;
						break;
					}
				}

				if (! is_replica) {
					// this is a state corruption error
					cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. NON replica node received migrate request",
							ns->name, pid);
					rv = AS_MIGRATE_FAIL;
					break; // out of switch
				}

				if (source_node != p->replica[0]) {
					// this is a state corruption error
					cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. SYNC replica node received migrate request from non-master",
							ns->name, pid);
					rv = AS_MIGRATE_FAIL;
					break; // out of switch
				}

				if (p->origin != p->replica[0]) {
					cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. SYNC replica node receiving migrate request has origin set to non-master",
							ns->name, pid);
					rv = AS_MIGRATE_FAIL;
					break; // out of switch
				}

				// Set the state to be DESYNC
				// and leave the tree as is. The new writes will be written
				// to the journal and re-applied after the merged records
				// are migrated from the master.
				p->state = AS_PARTITION_STATE_DESYNC;

				// Open the write journal and set state to DESYNC
				if (0 != as_write_journal_start(ns, pid)) {
					// Theoretically, a journal start fails only when
					// there's already another journal in progress.
					cf_warning(AS_PARTITION, "{%s:%d} could not start journal, continuing",
							ns->name, pid);
				}
				// TODO: Should we always apply journals during a partition
				//       re-balance for desync nodes that have their
				//       versions set?
			}
			else {
				// Migrations into SYNC Master must ONLY be from nodes
				// containing duplicate partitions.
				if (p->origin != (cf_node)0) {
					// this is a state corruption error.
					cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. SYNC Master has origin set to non-NULL",
							ns->name, pid);
					rv = AS_MIGRATE_FAIL;
					break; // out of switch
				}

				bool dupl_node_found = false;

				for (int i = 0; i < p->n_dupl; i++) {
					if (source_node == p->dupl_nodes[i]) {
						dupl_node_found = true;
						break;
					}
				}

				if (! dupl_node_found) {
					// This has been determined NOT to be a state corruption
					// error - Retransmitted START after a done completes on a
					// DSYNC master. This shouldn't affect emigrating node.
					cf_detail(AS_PARTITION, "{%s:%d} migrate rx aborted. SYNC Master receiving migrate from node not in duplicate list",
							ns->name, pid);
					rv = AS_MIGRATE_FAIL;
					break; // out of switch
				}
			}

			rv = AS_MIGRATE_OK;
			break;
		}

		if (rv == AS_MIGRATE_OK) {
			// Total number of incoming migrations accepted.
			cf_atomic_int_incr(&g_config.migrate_num_incoming_accepted);
		}
		else {
			// Migration has been rejected, incoming migration not expected.
			cf_atomic_int_decr(&g_config.migrate_num_incoming);
		}

		pthread_mutex_unlock(&p->lock);

		// Run all the queued migrations: this happens after the release of
		// the state lock to ensure that writes have begun to flow to their
		// new homes.
		partition_migrate_record pmr;

		while (0 == cf_queue_pop(mq, &pmr, 0)) {
			as_migrate_emigrate(&pmr, false);
		}

		cf_queue_destroy(mq);

		break;
	}
	case AS_MIGRATE_STATE_DONE:
	{
		pthread_mutex_lock(&p->lock);

		if (orig_cluster_key != as_paxos_get_cluster_key()) {
			rv = AS_MIGRATE_FAIL;
			pthread_mutex_unlock(&p->lock);
			break; // out of switch
		}

		if (p->pending_migrate_rx == 0) {
			cf_warning(AS_PARTITION, "{%s:%d} Concurrency event. Paxos reconfiguration occurred during migrate_rx?",
					ns->name, pid);
			rv = AS_MIGRATE_FAIL;
			pthread_mutex_unlock(&p->lock);
			break; // out of switch
		}

		// Check if there are any final migrates to be done after merges and
		// schedule these now.
		cf_queue *mq = cf_queue_create(sizeof(partition_migrate_record), false);
		as_partition_state orig_p_state = p->state;

		switch (orig_p_state) {
		case AS_PARTITION_STATE_UNDEF:
		case AS_PARTITION_STATE_ABSENT:
		case AS_PARTITION_STATE_ZOMBIE:
			// Check for illegal state.
			cf_warning(AS_PARTITION, "{%s:%p} received migrate done into bad state partition: %p ",
					ns->name, pid, p->state);
			rv = AS_MIGRATE_FAIL;
			break; // out of switch
		case AS_PARTITION_STATE_DESYNC:
			if (p->origin != source_node || p->pending_migrate_rx == 0) {
				// This is a state corruption error.
				cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. State error for desync partition",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break; // out of switch
			}

			p->pending_migrate_rx--;

			int64_t migrates_rx_remaining = cf_atomic_int_decr(
					&ns->migrate_rx_partitions_remaining);

			if (migrates_rx_remaining < 0) {
				cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld) rx partitions schedule exceeded, possibly a race with prior migration",
						ns->name, pid, p->pending_migrate_rx, migrates_rx_remaining);
			}

			p->origin = 0;

			apply_write_journal(ns, pid);

			set_partition_sync_lockfree(p, pid, ns, true);
			cf_atomic_int_incr(&g_config.partition_generation);

			// If this is not a master, we are done.
			if (g_config.self_node != p->replica[0]) {
				if (p->pending_migrate_rx != 0) {
					// This is a state corruption error.
					cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. rx %d is non zero",
							ns->name, pid, p->pending_migrate_rx);
					rv = AS_MIGRATE_FAIL;
				}

				break; // out of switch
			}

			// A desync master has just become sync.
			if (p->pending_migrate_tx != 0) {
				// This is a state corruption error.
				cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. tx %d value is non-zero for master that just turned sync after migrate",
						ns->name, pid, p->pending_migrate_tx);
				rv = AS_MIGRATE_FAIL;
				break; // out of switch
			}

			// Send migrate message to node containing every duplicate
			// version. This is really a request for migrate, so only
			// the start message will be sent and the node having the
			// duplicate will respond with migrate already done.
			// (the type doesn't matter here since it's a "false migrate")
			for (int i = 0; i < p->n_dupl; i++) {
				p->pending_migrate_tx++; // Send request to dupl node

				partition_migrate_record r;
				partition_migrate_record_fill(&r, p->dupl_nodes[i],
						ns, pid, orig_cluster_key, TX_FLAGS_REQUEST);

				cf_queue_push(mq, &r);
			}

			// Tally the migrate_rx and migrate_tx - they need to be
			// identical
			if (p->pending_migrate_tx != p->pending_migrate_rx) {
				// this is a state corruption error
				cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. rx %d and tx %d values mismatch",
						ns->name, pid, p->pending_migrate_rx, p->pending_migrate_tx);
				rv = AS_MIGRATE_FAIL;
				break; // out of switch
			}

			// Continue to code block below where the tx_onsync processing
			// occurs, fall into next block. The state is sync now.
		case AS_PARTITION_STATE_SYNC:
			// A sync node has received a migrate_rx DONE message.
			// This node has to be the master node receiving duplicate messages.
			if (g_config.self_node != p->replica[0]) {
				// this is a state corruption error
				cf_warning(AS_PARTITION, "{%s:%d} migrate rx aborted. State error for sync partition",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break; // out of switch
			}

			// Master node that is sync getting a migrate end. should be
			// duplicate data check p->origin and p->dupl_nodes for matching
			// the sources.
			if (p->n_dupl > 0) {
				bool found = false;
				int i = 0;

				for (i = 0; i < p->n_dupl; i++) {
					if (p->dupl_nodes[i] == source_node) {
						found = true;
						break;
					}
				}

				if (found) {
					if (i == (p->n_dupl - 1)) { // delete last entry
						p->dupl_nodes[i] = (cf_node)0;
					}
					else { // copy last entry into deleted entry
						p->dupl_nodes[i] = p->dupl_nodes[p->n_dupl - 1];
						p->dupl_nodes[p->n_dupl - 1] = (cf_node)0;
					}

					p->n_dupl--;
					p->pending_migrate_rx--;

					int64_t migrates_rx_remaining = cf_atomic_int_decr(
							&ns->migrate_rx_partitions_remaining);

					if (migrates_rx_remaining < 0) {
						cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld)  rx partitions schedule exceeded, possibly a race with prior migration",
								ns->name, pid, p->pending_migrate_rx, migrates_rx_remaining);
					}
				}
				else {
					// We get here when DESYNC (empty) master becomes
					// SYNC and there were duplicates. The first sync
					// node is not a member of the dupl_nodes array.

					if (orig_p_state != AS_PARTITION_STATE_DESYNC) {
						cf_warning(AS_PARTITION, "{%s:%d} source node %"PRIx64" not found in migration rx state",
								ns->name, pid, source_node);
						rv = AS_MIGRATE_FAIL;
						break; // out of switch
					}
				}
			}
			// else -
				// No duplicates - we get here e.g. if master & prole(s)
				// are desync and there is a single partition version
				// coming from a zombie to here - the desync master.
				// Might now do migration(s) to prole(s) - don't break!

			if (p->pending_migrate_rx > 0) {
				break; // out of switch
			}

			// Check if there are any replicas that are not sync and need
			// migration.
			for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
				if (p->replica_tx_onsync[i] == true) {
					p->replica_tx_onsync[i] = false;
					p->pending_migrate_tx++;

					partition_migrate_record r;
					partition_migrate_record_fill(&r, p->replica[i],
							ns, pid, orig_cluster_key, TX_FLAGS_NONE);

					cf_queue_push(mq, &r);
				}
			}
		}

		cf_atomic_int_incr(&g_config.partition_generation);

		pthread_mutex_unlock(&p->lock);

		// Run all the queued migrations: this happens after the release of
		// the state lock to ensure that writes have begun to flow to their
		// new homes.
		partition_migrate_record pmr;

		while (0 == cf_queue_pop(mq, &pmr, 0)) {
			as_migrate_emigrate(&pmr, true);
		}

		cf_queue_destroy(mq);

		// For receiver-side migration flow control.
		cf_atomic_int_decr(&g_config.migrate_num_incoming);

		break;
	}
	case AS_MIGRATE_STATE_ERROR:
	case AS_MIGRATE_STATE_EAGAIN:
		cf_warning(AS_PARTITION, "partition migrate failure! {%s:%d}", ns->name, pid);
		break;
	default:
		break;
	}

	return rv;
}


// Reduce the replication factor to 1 if the cluster size is less than or equal
// to the specified limit.
void
as_partition_set_ns_replication_factor(int new_cluster_size)
{
	bool reduce_repl = false;

	cf_info(AS_PARTITION, "setting replication factors: cluster size %d, paxos single replica limit %d",
			new_cluster_size, g_config.paxos_single_replica_limit);

	if (new_cluster_size <= g_config.paxos_single_replica_limit) {
		reduce_repl = true;
	}

	// Normal case - set replication factor.
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		uint16_t max_repl = ns->cfg_replication_factor > new_cluster_size ?
				new_cluster_size : ns->cfg_replication_factor;

		ns->replication_factor = reduce_repl ? 1 : max_repl;

		cf_info(AS_PARTITION, "{%s} replication factor is %d", ns->name,
				ns->replication_factor);
	}
}


// Define the macros for accessing the HV and hv_slindex arrays.
#define HV(x, y) hv_ptr[(x * g_config.paxos_max_cluster_size) + y]
#define HV_SLINDEX(x, y) hv_slindex_ptr[(x * g_config.paxos_max_cluster_size) + y]


// Returns true if group_id is unique within nodes list indices less than n.
bool
is_group_distinct_before_n(const as_partition *ptn, const cf_node hv_ptr[],
		const int hv_slindex_ptr[], const cc_node_t group_id, const int index) {
	const uint16_t pid = ptn->partition_id;

	for (int cur_i = 0; cur_i < index; cur_i++) {
		const cf_node cur_node = HV(pid, cur_i);
		const cc_node_t cur_group_id = cc_compute_group_id(cur_node);

		if (cur_group_id == group_id) {
			return false;
		}
	}

	return true;
}


// Adjust the Partition Map Array (HV) and SuccessionList Index Array to
// Accommodate the GROUP (rack) rules for replicas (proles).  The first
// "Replication Factor" number of nodes after the zero entry (the master) MUST
// have different group ids.  We'll do a pair-wise swap of entries to make
// sure that the first N node values have different group ids than the nodes
// that preceded it.
// Assumes the topology has already been verified to support the Rack-Aware
// rules.
void
as_partition_adjust_hv_and_slindex(const as_partition *ptn, cf_node hv_ptr[],
		int hv_slindex_ptr[])
{
	const uint rf = ptn->p_repl_factor;
	const uint16_t pid = ptn->partition_id;
	const uint64_t cluster_size = g_config.paxos->cluster_size;
	const uint16_t n_groups = g_config.cluster.group_count;
	const uint16_t n_needed = n_groups < rf ? n_groups : rf;

	int next_i = n_needed; // next candidate index to swap with

	for (int cur_i = 1; cur_i < n_needed; cur_i++) {
		const cf_node cur_node = HV(pid, cur_i);
		const cc_group_t cur_group_id = cc_compute_group_id(cur_node);

		if (cur_node == (cf_node)0) {
			cf_crash(AS_PARTITION, "null node found within cluster_size");
		}

		// If cur_group is unique for nodes < cur_i then continue to next node.
		if (! is_group_distinct_before_n(ptn, hv_ptr, hv_slindex_ptr,
				cur_group_id, cur_i)) {
			// Find a group after cur_i that is unique for groups before cur_i.
			int swap_i = cur_i; // if swap cannot be found then no change
			for (; next_i < cluster_size; next_i++) {
				const cf_node next_node = HV(pid, next_i);
				const cc_group_t next_group_id = cc_compute_group_id(next_node);

				if (next_node == (cf_node)0) {
					cf_crash(AS_PARTITION, "null node found within cluster_size");
				}

				if (is_group_distinct_before_n(ptn, hv_ptr, hv_slindex_ptr,
						next_group_id, cur_i)) {
					swap_i = next_i;
					next_i++;
					break;
				}
			}

			if (swap_i == cur_i) {
				// No other distinct groups found. This shouldn't be possible.
				// We should reach n_needed first.
				cf_crash(AS_PARTITION, "can't find a diff cur:%d swap:%d repl:%d clsz:%d ptn:%d",
						cur_i, swap_i, rf, cluster_size, pid);
			}

			// Now swap cur_i with swap_i.
			// Swap node.
			HV(pid, cur_i) ^= HV(pid, swap_i);
			HV(pid, swap_i) = HV(pid, cur_i) ^ HV(pid, swap_i);
			HV(pid, cur_i) ^= HV(pid, swap_i);

			// Swap slindex.
			HV_SLINDEX(pid, cur_i) ^= HV_SLINDEX(pid, swap_i);
			HV_SLINDEX(pid, swap_i) = HV_SLINDEX(pid, cur_i) ^ HV_SLINDEX(pid, swap_i);
			HV_SLINDEX(pid, cur_i) ^= HV_SLINDEX(pid, swap_i);
		}
	}
}


// Check that we have more than one group in our paxos succession list,
// otherwise we don't have a valid cluster topology.
// Assumes namespace replication factors have been updated for this round.
void
as_partition_cluster_topology_info(const as_paxos *paxos_p) {
	const cf_node * succession = paxos_p->succession;
	const uint64_t cluster_size = g_config.paxos->cluster_size;

	uint32_t distinct_groups = 0;
	cluster_config_t cc; // structure to hold state of the group

	cc_cluster_config_defaults(&cc);

	// Verify that there are at least *replication-factor* groups present.
	for (int cur_i = 0;
			succession[cur_i] != (cf_node)0 && cur_i < cluster_size;
			cur_i++) {
		const cc_group_t cur_group = cc_compute_group_id(succession[cur_i]);
		cc_add_fullnode_group_entry(&cc, succession[cur_i]);

		int prev_i;

		for (prev_i = 0; prev_i < cur_i; prev_i++) {
			const cc_group_t prev_group = cc_compute_group_id(
					succession[prev_i]);

			if (prev_group == cur_group) {
				break;
			}
		}

		if (prev_i == cur_i) { // group is unique
			distinct_groups++;
		}
	}

	cc.cluster_state = cc_get_cluster_state(&cc);
	g_config.cluster.cluster_state = cc.cluster_state;
	g_config.cluster.group_count = cc.group_count;

	// Show the state of the cluster -- list the contents of each group. Dump
	// this all to the log.
	cc_show_cluster_state(&cc);
}


void
as_migrate_increment_all_tx_fail() {
	for (int i = 0; i < g_config.n_namespaces; i++) {
		// All namespaces will fail to migrate.
		as_namespace *ns = g_config.namespaces[i];
		cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
	}
}


void
as_partition_balance()
{
	// Shortcut pointers.
	as_paxos *paxos = g_config.paxos;
	cf_node *succession = paxos->succession;
	bool *alive = paxos->alive;
	cf_node self = g_config.self_node;

	if (! succession || ! alive) {
		cf_warning(AS_PARTITION, "imbalance: succession list is uninitialized: couldn't start migrate");
		as_migrate_increment_all_tx_fail();
		return;
	}

	if ((cf_node)0 == self) {
		cf_warning(AS_PARTITION, "imbalance: node value is uninitialized: couldn't start migrate");
		as_migrate_increment_all_tx_fail();
		return;
	}

	bool found_error = false;
	size_t cluster_size = 0;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (succession[i] == (cf_node)0) {
			cluster_size = i;

			// Make sure that rest of succession list is empty.
			for (int j = i; j < g_config.paxos_max_cluster_size; j++) {
				if (succession[j] != (cf_node)0) {
					found_error = true;
				}
			}

			break;
		}
	}

	if (found_error || cluster_size == 0) {
		cf_warning(AS_PARTITION, "imbalance: succession list is corrupted: couldn't start migrate");
		as_migrate_increment_all_tx_fail();
		return;
	}

	paxos->cluster_size = cluster_size;
	cf_info(AS_PARTITION, "CLUSTER SIZE = %d", paxos->cluster_size);

	// Find this node's index in the succession list.
	size_t self_index;
	found_error = true;

	for (int i = 0; i < cluster_size; i++) {
		if (succession[i] == self) {
			self_index = i;
			found_error = false;
			break;
		}
	}

	if (found_error) {
		cf_warning(AS_PARTITION, "imbalance: can't find self in succession list: couldn't start migrate");
		as_migrate_increment_all_tx_fail();
		return;
	}

	// Check that the global state table is well formed.
	found_error = false;

	for (int i = 0; i < g_config.n_namespaces; i++) {
		for (int j = 0; j < cluster_size; j++) {
			if (NULL == paxos->c_partition_vinfo[i][j]) {
				found_error = true;
			}
		}
	}

	if (found_error) {
		cf_warning(AS_PARTITION, "imbalance: global state is corrupted: couldn't start migrate");
		as_migrate_increment_all_tx_fail();
		as_paxos_set_cluster_integrity(paxos, false);
		return;
	}

	cf_info(AS_PARTITION, "Global state is well formed");

	as_paxos_set_cluster_integrity(paxos, true);

	// Check that this partition's global state is the same as its local state.
	found_error = false;

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		for (int j = 0; j < AS_PARTITIONS; j++) {
			if (memcmp(&ns->partitions[j].version_info,
					&paxos->c_partition_vinfo[i][self_index][j],
					sizeof(as_partition_vinfo)) != 0) {
				found_error = true;
				print_partition_versions(ns->name, j,
						&ns->partitions[j].version_info, "Global",
						&paxos->c_partition_vinfo[i][self_index][j], "Local");
				break;
			}
		}
	}

	if (found_error) {
		cf_warning(AS_PARTITION, "imbalance: global state is not identical to local state: couldn't start migrate");
		as_migrate_increment_all_tx_fail();
		return;
	}

	// Figure out effective replication factor in the face of node failures.
	as_partition_set_ns_replication_factor(cluster_size);

	// Print rack aware info.
	as_partition_cluster_topology_info(paxos);

	// Populate an array that, for each partition, holds all of the potential
	// successor nodes (a list of cluster-size node ids).  This is a "packed
	// array" of bytes that holds a fixed size two dimensional array:
	// Outer dimension (columns) are the partitions (4096 of them) and
	// Inner dimension (rows) are the individual cluster nodes (set by paxos).
	// We do our own indexing into it: Column (partition id) * "size of" the
	// row, plus the row offset (in bytes).
	// NOTE: it would have been nice for this to be a regular two-dimensional
	// array, but given that it's really a run-time allocated structure, it
	// doesn't fit well with a compiled language.  It was necessary to create
	// a SINGLE dimensional array and compute the inner (dynamic) offsets
	// on the fly.  Hence, the use of the HV() macro.
	// Also -- it would be a potential HUGE waste of memory, to statically
	// allocate two arrays -- one with 64 bit objects,  and one with
	// 32 bit objects of size 4096 * 127 (also, times # of name-spaces).
	// ==> cf_node hv_ptr[4096][127];
	// ==> int hv_slindex[4096][127];
	//
	// HV Array: HV = "Hash Value"  (values are 64 bit "cf_node" values).
	// Here is a conceptual view:
	//  Partition Map Diagram
	//  Example System:  4 nodes, 8 partitions
	//
	//      Nodes in the Cluster
	//      +-----------------------+
	//      | 101 | 102 | 203 | 204 |
	//      +-----------------------+
	//
	//  Paxos Global Succession List: (Sorted in Descending order)
	//      +-----------------------+
	//  SL  | 204 | 203 | 102 | 101 | (Holds Node Names in succession order)
	//      +-----------------------+
	//  SLI |   0 |   1 |   2 |   3 | (Holds the INDEX of the names in the SL)
	//      +-----------------------+
	//
	//       Initial Randomization of the Partition Table.
	//       Start with Succession List, Randomize.  Remember where the
	//       node names were in SL by using the SL Index (SLI)
	//       This is known in the code as the Hash Value (HV) Array and
	//       the HV Succession List Index (SLINDEX) Array.
	//   P#
	//  +===+-----------------------+
	//  | 0 | 102 | 203 | 204 | 101 | SLI(2, 1, 0, 3)
	//  +===+-----------------------+
	//  | 1 | 204 | 101 | 102 | 203 | SLI(0, 3, 2, 1)
	//  +===+-----------------------+
	//  | 2 | 101 | 102 | 203 | 204 | SLI(3, 2, 1, 0)
	//  +===+-----------------------+
	//  | 3 | 203 | 204 | 101 | 102 | SLI(1, 0, 3, 2)
	//  +===+-----------------------+
	//  | 4 | 101 | 203 | 102 | 204 | SLI(3, 1, 2, 0)
	//  +===+-----------------------+
	//  | 5 | 203 | 101 | 204 | 102 | SLI(1, 3, 0, 2)
	//  +===+-----------------------+
	//  | 6 | 102 | 101 | 203 | 204 | SLI(2, 3, 1, 0)
	//  +===+-----------------------+
	//  | 7 | 204 | 203 | 102 | 101 | SLI(0, 1, 2, 3)
	//  +===+-----------------------+
	//
	// There is a companion array: The Succession List Index array (slindex),
	// that shows the position of a node in the global paxos succession list.
	// SLINDEX Array: SLINDEX = "Succession List Index", meaning, the index of
	// the "Hash Value" in the succession list.
	// (values are integers, since they are just the array index values of
	// the node names in the succession list).
	//
	// So -- to recap:
	//  * Global Paxos Succession list (one per cluster):
	//    It lists ALL nodes in the cluster.
	//  * Partition Succession list (HV): Shows master and replica(s) node
	//    values per partition (the 64 bit cf_node value).
	//  * Partition Succession list index (slindex): Basically shows the same
	//    information as the HV array (above), but instead of listing the
	//    the cf_node value itself, it lists the INDEX (i.e. array offset) of
	//    the node value in the global paxos succession list.
	//
	// Now, with Rack Awareness, we're adding another wrinkle in the mix. For
	// each partition, we're changing how the replica list is calculated.
	// It used to work like this:
	// * For each partition:
	//   * Randomize a copy of the succession list (using pseudo random
	//     function)
	//   * The node in position 0 is the master, and the remaining nodes in the
	//     list are the replicas (proles).
	//   * Truncate the list to the replication factor:
	//     In most cases, it's replication factor 2:  One master, one Prole.
	//
	// That was the old way. That no longer works for rack awareness.
	// Successors for a partition can NOT be in the same group as the master.
	// So, implicitly, we start with the HV array, but when we use information
	// from it -- it is important to know if we want "generic" node information,
	// (for which HV() can be used), or true partition succession list
	// information (for which the partition->replica[] must be used).
	//
	//  CHANGES FOR "RACK AWARE" Groups
	//      Nodes and Groups in the cluster
	//      +-----------------------+
	//      |  Group 1  |  Group 2  |
	//      +-----------------------+
	//      | 101 | 102 | 203 | 204 |
	//      +-----------------------+
	// Paxos Succession List stays the same (204, 203, 102, 101)
	//
	//      P#
	//     +===+-----------------------+
	//     | 0 | 102 | 203 | 204 | 101 | SLI(2, 1, 0, 3)
	//     +===+-----------------------+
	//     | 1 | 204 | 101 | 102 | 203 | SLI(0, 3, 2, 1)
	//     +===+-----------------------+
	//     | 2 | 101 |<102>| 203 | 204 | SLI(3, 2, 1, 0)  <<== Adjustment needed
	//     +===+-----------------------+
	//     | 3 | 203 |<204>| 101 | 102 | SLI(1, 0, 3, 2)  <<== Adjustment needed
	//     +===+-----------------------+
	//     | 4 | 101 | 203 | 102 | 204 | SLI(3, 1, 2, 0)
	//     +===+-----------------------+
	//     | 5 | 203 | 101 | 204 | 102 | SLI(1, 3, 0, 2)
	//     +===+-----------------------+
	//     | 6 | 102 |<101>| 203 | 204 | SLI(2, 3, 1, 0)  <<== Adjustment needed
	//     +===+-----------------------+
	//     | 7 | 204 |<203>| 102 | 101 | SLI(0, 1, 2, 3)  <<== Adjustment needed
	//     +===+-----------------------+
	//
	//  The table rows are truncated to "replication factor", which is usually
	//  two.
	//   P#
	//  +===+-----------+
	//  | 0 | 102 | 203 | SLI(2, 1)
	//  +===+-----------+
	//  | 1 | 204 | 101 | SLI(0, 3)
	//  +===+-----------+
	//  | 2 | 101 | 203 | SLI(3, 1) (Adjusted)
	//  +===+-----------+
	//  | 3 | 203 | 101 | SLI(1, 3) (Adjusted)
	//  +===+-----------+
	//  | 4 | 101 | 203 | SLI(3, 1)
	//  +===+-----------+
	//  | 5 | 203 | 101 | SLI(1, 3)
	//  +===+-----------+
	//  | 6 | 102 | 203 | SLI(2, 1) (Adjusted)
	//  +===+-----------+
	//  | 7 | 204 | 102 | SLI(0, 2) (Adjusted)
	//  +===+-----------+

	// NOTE:
	// The partition->replica[] arrays are generated directly from the HV array,
	// as are other balance_new() sections.  Since we've simply done a slight
	// reorder of the HV rows (which were pseudo-random in the first place),
	// nothing else in this file needed to be changed. (6/2013 tjl)
	//
	// This section builds the 2 dim packed byte array;
	// For each partition, it creates a row of cluster nodes, randomized.
	int hv_ptr_sz = AS_PARTITIONS * g_config.paxos_max_cluster_size * sizeof(cf_node);
	cf_node *hv_ptr = cf_malloc(hv_ptr_sz);

	int hv_slindex_ptr_sz = AS_PARTITIONS * g_config.paxos_max_cluster_size * sizeof(int);
	int *hv_slindex_ptr = cf_malloc(hv_slindex_ptr_sz);

	if (! hv_slindex_ptr || ! hv_ptr) {
		cf_crash(AS_PARTITION, "as_partition_balance: couldn't allocate partition state tables: %s",
				cf_strerror(errno));
	}

	memset(hv_ptr, 0, hv_ptr_sz);
	memset(hv_slindex_ptr, 0, hv_slindex_ptr_sz);

	// <HV SECTION> <HV_SECTION> <HV SECTION> <HV_SECTION> <HV SECTION>
	// <HV SECTION> <HV_SECTION> <HV SECTION> <HV_SECTION> <HV SECTION>
	// Build the array of successor nodes for each partition.
	for (int i = 0; i < AS_PARTITIONS; i++) {
		for (int j = 0; j < cluster_size; j++) {
			struct hashbuf {
				uint64_t n, p;
			} h;

			if (0 == succession[j]) {
				continue;
			}

			// Compute the hash value for this (node, partition) tuple.
			// We separately compute the FNV-1a hash of each fragment of
			// the tuple, then hash them together with a One-at-a-time hash;
			// this method seems to give fairly good distribution.  We then
			// stash the node's numerical ID in last few bits.
			h.p = cf_hash_fnv(&i, sizeof(int));
			h.n = cf_hash_fnv(&succession[j], sizeof(cf_node));
			HV(i, j) = cf_hash_oneatatime(&h, sizeof(struct hashbuf));
			HV(i, j) &= AS_CLUSTER_SZ_MASKP;
			HV(i, j) += j;
		} // end for each node in cluster

		// Sort the hashed node values and then convert the hash values BACK
		// into node IDs (mask everything out except our node index id bits).
		// Then, Use the ID to get the original node values out of the
		// succession list, but save the index bits for the SL Index array.
		qsort(&hv_ptr[i * g_config.paxos_max_cluster_size], cluster_size,
				sizeof(cf_node), cf_compare_uint64ptr);
		for (int j = 0; j < cluster_size; j++) {
			if (0 == HV(i, j)) {
				break;
			}

			HV_SLINDEX(i, j) = (int)(HV(i, j) & AS_CLUSTER_SZ_MASKN);

			// Overwrite the above-written hashed value with the correct
			// succession list value based on the bits of the node entry that
			// were stashed in the lower byte (and isolated by the mask).
			HV(i, j) = succession[(int)(HV(i, j) & AS_CLUSTER_SZ_MASKN)];
		} // end for each node in cluster
	} // end for each partition

	int n_new_versions = 0;

	// Generate the new partition version based on the cluster key and use this
	// as the instance id for all the newly initialized partitions. The version
	// tree path is set to the value "[1]".
	as_partition_vinfo new_version_for_lost_partitions;
	memset(&new_version_for_lost_partitions, 0, sizeof(new_version_for_lost_partitions));
	generate_new_partition_version(&new_version_for_lost_partitions);

	if (is_partition_null(&new_version_for_lost_partitions)) {
		// What do we do here?
		cf_warning(AS_PAXOS, "null partition ID generated");
	}

	size_t n_lost = 0;
	size_t n_unique = 0;
	size_t n_recreate = 0;
	size_t n_duplicate = 0;

	size_t n_total = g_config.n_namespaces * AS_PARTITIONS;
	uint64_t orig_cluster_key = as_paxos_get_cluster_key();

	cf_queue mig_q;
	cf_queue* mq = &mig_q;
	cf_queue_init(mq, sizeof(partition_migrate_record),
			AS_PARTITIONS * g_config.n_namespaces, false);

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		cf_atomic_int_set(&ns->migrate_tx_partitions_initial, 0);
		cf_atomic_int_set(&ns->migrate_tx_partitions_remaining, 0);
		cf_atomic_int_set(&ns->migrate_rx_partitions_initial, 0);
		cf_atomic_int_set(&ns->migrate_rx_partitions_remaining, 0);

		int ns_pending_migrate_rx = 0;
		int ns_pending_migrate_tx = 0;
		int ns_pending_migrate_tx_later = 0;

		for (int j = 0; j < AS_PARTITIONS; j++) {
			as_partition *p = &ns->partitions[j];
			partition_migrate_record pmr;

			pthread_mutex_lock(&p->lock);

			uint old_repl_factor = p->p_repl_factor;
			p->p_repl_factor = ns->replication_factor;

			if (g_config.cluster_mode != CL_MODE_NO_TOPOLOGY &&
					paxos->cluster_size > 1) {
				as_partition_adjust_hv_and_slindex(p, hv_ptr, hv_slindex_ptr);
			}

			memset(p->replica, 0, g_config.paxos_max_cluster_size * sizeof(cf_node));
			memcpy(p->replica, &hv_ptr[j * g_config.paxos_max_cluster_size], p->p_repl_factor * sizeof(cf_node));

			p->origin = 0;
			p->target = 0;
			p->current_outgoing_ldt_version = 0;

			p->pending_migrate_tx = 0;
			p->pending_migrate_rx = 0;
			memset(p->replica_tx_onsync, 0, sizeof(p->replica_tx_onsync));

			p->n_dupl = 0;
			memset(p->dupl_nodes, 0, sizeof(p->dupl_nodes));
			p->master_wait_state = MASTER_NOT_WAITING;
			memset(&p->primary_version_info, 0, sizeof(p->primary_version_info));

			// Check if any of the replicas for this partition in the old
			// succession list are missing from the new succession list.
			bool create_new_partition_version = false;

			for (int k = 0;  k < old_repl_factor; k++) {
				bool found = false;

				if (p->old_sl[k] == 0) {
					continue;
				}

				for (int l = 0; l < cluster_size; l++) {
					if (p->old_sl[k] == succession[l]) {
						found = true;
						break;
					}
				}

				if (! found) {
					create_new_partition_version = true;
					break;
				}
			}

			// Detect if there is a write journal and apply it here.
			// TODO - yes, this happens... try to understand it better.
			if (p->state == AS_PARTITION_STATE_DESYNC &&
					! is_partition_null(&p->version_info)) {
				cf_info(AS_PARTITION, "{%s:%d} applying write journal from previous rebalance",
						ns->name, j);

				apply_write_journal(ns, j);
				p->state = AS_PARTITION_STATE_SYNC;
			}

			// Do some integrity checks on partition state.
			if (is_partition_null(&ns->partitions[j].version_info)) {
				bool ok = (p->state != AS_PARTITION_STATE_SYNC &&
						p->state != AS_PARTITION_STATE_ZOMBIE);

				if (! ok) {
					cf_warning(AS_PARTITION,
							"{%s:%d} partition version is null but state is SYNC or ZOMBIE or WAIT %d %"PRIx64,
							ns->name, j, p->state, self);
				}
			}
			else {
				bool ok = (p->state == AS_PARTITION_STATE_SYNC ||
						p->state == AS_PARTITION_STATE_ZOMBIE);

				if (! ok) {
					cf_warning(AS_PARTITION,
							"{%s:%d} partition version is not null but state is not SYNC/ZOMBIE/WAIT  %d %"PRIx64,
							ns->name, j, p->state, self);
				}
			}

			// Number of unique versions of this partition.
			size_t n_found = 0;

			// First version in this partition's succession list.
			as_partition_vinfo f_vinfo;
			memset(&f_vinfo, 0, sizeof(f_vinfo));

			// Info for duplicate versions.
			size_t n_dupl = 0;
			cf_node dupl_nodes[AS_CLUSTER_SZ];
			as_partition_vinfo dupl_pvinfo[AS_CLUSTER_SZ];
			memset(dupl_nodes, 0, sizeof(dupl_nodes));
			memset(dupl_pvinfo, 0, sizeof(dupl_pvinfo));

			for (int k = 0; k < cluster_size; k++) {
				size_t n_index = HV_SLINDEX(j, k);
				as_partition_vinfo *vinfo = &paxos->c_partition_vinfo[i][n_index][j];

				if (is_partition_null(vinfo)) {
					continue;
				}

				if (n_found == 0) {
					// First encounter of this partition.
					n_found++;
					memcpy(&f_vinfo, vinfo, sizeof(*vinfo));
					memcpy(&p->primary_version_info, vinfo, sizeof(*vinfo));
					continue;
				}

				// Check if this partition version is different than the
				// ones already encountered.
				bool found = as_partition_vinfo_same(&f_vinfo, vinfo);

				if (! found) {
					for (int l = 0; l < n_dupl; l++) {
						found = as_partition_vinfo_same(&dupl_pvinfo[l], vinfo);

						if (found) {
							break;
						}
					}
				}

				if (! found) {
					dupl_nodes[n_dupl] = HV(j, k);
					memcpy(&dupl_pvinfo[n_dupl], vinfo, sizeof(*vinfo));
					n_dupl++;
					n_found++;
				}
			}

			bool partition_is_lost = false;

			if (n_found == 0) {
				partition_is_lost = true;
				n_lost++;
			}
			else if (n_found == 1) {
				n_unique++;
			}
			else {
				n_duplicate++;
			}

			// First create new empty partitions for missing partitions if this
			// node is a replica. Essentially, all replicas will simultaneously
			// create new versions of this partition using the version number
			// derived from the cluster key.
			if (partition_is_lost) {
				partition_is_lost = false;
				n_recreate++;

				int loop_end = cluster_size < p->p_repl_factor ?
						cluster_size : p->p_repl_factor;

				for (int k = 0; k < loop_end; k++) {
					int n_index = HV_SLINDEX(j, k);
					cf_node n_node = HV(j, k);

					if (n_node == self) {
						// There are no sync copies of this partition available
						// within the cluster and this node is a replica, so
						// reinitialize a valid empty partition.
						as_partition_reinit(p, ns, j);
						memset(p->replica, 0, g_config.paxos_max_cluster_size * sizeof(cf_node));
						memcpy(p->replica, &hv_ptr[j * g_config.paxos_max_cluster_size],
								p->p_repl_factor * sizeof(cf_node));
						memcpy(&p->primary_version_info,
								&new_version_for_lost_partitions,
								sizeof(p->primary_version_info));
						set_partition_sync_lockfree(p, j, ns, false);
					}

					memcpy(&paxos->c_partition_vinfo[i][n_index][j],
							&new_version_for_lost_partitions,
							sizeof(new_version_for_lost_partitions));
					paxos->c_partition_size[i][n_index][j] = p->vp->elements;
					paxos->c_partition_size[i][n_index][j] += p->sub_vp->elements;
				}
			}

			// Compute which of the replicas is not sync.
			bool is_sync[AS_CLUSTER_SZ];
			int first_sync_node = -1;
			int my_index_in_hvlist = -1;
			memset(is_sync, 0, sizeof(is_sync));

			// Note - might need to look beyond replicas to find a sync node.
			for (int k = 0; k < cluster_size; k++) {
				int n_index = HV_SLINDEX(j, k);

				is_sync[k] = ! is_partition_null(&paxos->c_partition_vinfo[i][n_index][j]);

				if (is_sync[k] && first_sync_node < 0) {
					first_sync_node = k;

					as_partition_vinfo old_vinfo, new_vinfo;
					memcpy(&old_vinfo, &paxos->c_partition_vinfo[i][n_index][j], sizeof(as_partition_vinfo));
					memcpy(&new_vinfo, &old_vinfo, sizeof(as_partition_vinfo));

					// Increment the version information of the partition
					// if we have the primary version of this partition.
					if (create_new_partition_version) {
						bool version_changed = increase_partition_version_tree_path(
								&new_vinfo, HV(j, k), p->old_sl, ns->name, j);

						if (version_changed) {
							n_new_versions++;
							set_new_partition_version(&p->version_info, &old_vinfo, &new_vinfo, ns, j);
							memcpy(&p->primary_version_info, &new_vinfo, sizeof(p->primary_version_info));
						}
					}
				}

				if (HV(j, k) == self) {
					my_index_in_hvlist = k;
				}

				if (succession[n_index] != HV(j, k)) {
					cf_warning(AS_PARTITION, "{%s:%d} State Error. Node id mismatch hash %"PRIx64" slist %"PRIx64,
							ns->name, j, HV(j, k), succession[n_index]);
				}
			} // end for each node in cluster

			if (my_index_in_hvlist < 0) {
				cf_warning(AS_PARTITION, "{%s:%d} State Error. Cannot find self in hash value list %"PRIx64,
						ns->name, j, self);
			}

			if (first_sync_node < 0 && ! partition_is_lost) {
				cf_warning(AS_PARTITION, "{%s:%d} State Error. Cannot find first sync node for resident partition %"PRIx64,
						ns->name, j, self);
			}

			// Create migration requests as needed.
			switch (partition_is_lost) {
			case false:

				// Master
				//  If not sync switch to desync and wait for migration for
				//  primary wait for migration from each duplicate partition
				//  send merged data to all replicas.
				//  allow reads (read-all)
				//  allow writes
				if (my_index_in_hvlist == 0) { // I Master!
					// Do the following only if the master is not sync.
					if (! is_sync[0]) {
						// Master is not sync. Wait for the partition to be
						// sent from another node this node may or may not be a
						// replica.
						p->pending_migrate_rx++;
						p->origin = HV(j, first_sync_node);
						set_partition_desync_lockfree(p,
								&ns->partitions[j].version_info, ns, j,
								false);
					}
					else {
						p->state = AS_PARTITION_STATE_SYNC;
					}

					// If there are duplicates, the master will expect
					// migrations from the first sync node of each
					// duplicate partition version. This information is
					// stored in the duplicate data structures of the partition
					if (n_dupl > 0) {
						p->n_dupl = n_dupl;
						memcpy(p->dupl_nodes, dupl_nodes, sizeof(cf_node) * p->n_dupl);

						for (int k = 0; k < p->n_dupl; k++) {
							p->pending_migrate_rx++;
						}
					}

					// If master is sync and there are no duplicate partitions
					// schedule all the migrates to non-sync replicas right away.
					if (p->pending_migrate_rx == 0) {
						int loop_end = cluster_size < p->p_repl_factor ?
								cluster_size : p->p_repl_factor;

						for (int k = 1; k < loop_end; k++) {
							if (! is_sync[k]) {
								// Schedule a migrate of this partition.
								p->pending_migrate_tx++;
								partition_migrate_record_fill(&pmr,
										HV(j, k), ns, j,
										orig_cluster_key, TX_FLAGS_NONE);
								cf_queue_push(mq, &pmr);
							}
						}

						break; // out of switch
					}

					// Either master is not sync or it is waiting for
					// duplicate partition versions or both. Schedule
					// delayed migrates of merged data to replicas. All
					// replicas will be migrated to in case duplicate
					// partitions exist. Only non-sync partitions will be
					// migrated to in case there are no duplicate partitions.
					int loop_end = cluster_size < p->p_repl_factor ?
							cluster_size : p->p_repl_factor;

					for (int k = 1; k < loop_end; k++) {
						// Schedule a delayed migrate of this partition.
						if (p->n_dupl > 0 || ! is_sync[k]) {
							p->replica_tx_onsync[k] = true;
							ns_pending_migrate_tx_later++;
						}
					}

					break; // out of switch
				}

				// Non Sync.
				//     if replica, switch to desync wait for migration from
				//     master.
				//     if not replica, move to absent.
				if (! is_sync[my_index_in_hvlist]) { // Not sync
					if (my_index_in_hvlist < p->p_repl_factor) {
						// Wait for the master to send data.
						p->pending_migrate_rx++;
						p->origin = HV(j, 0);
						set_partition_desync_lockfree(p,
								&ns->partitions[j].version_info, ns, j,
								false);
					}
					else {
						set_partition_absent_lockfree(p,
								&ns->partitions[j].version_info, ns, j,
								false);
					}

					break; // out of switch
				}

				// Sync Node - Non-Master:
				//    if this is the first sync node then send partition
				//     over to master
				//        if not a replica, switch to zombie mode an//
				//         transition to absent later
				//        if a replica, then wait for migration from master
				//    if this is not the first sync node
				//        if a replica, then wait for migration from master
				//        if not a replica, set to absent
				//
				//    If sync or zombie node has primary version,
				//        allow reads (read-all)
				//        allow writes
				//    If sync or zombie node has duplicate version
				//        allow reads (read-all)
				//        reject writes

				// If this is the first sync node of the primary
				// version of this partition, schedule an
				// immediate migrate to the master node of this partition
				// If this is the first sync node of a duplicate
				// version of this partition, schedule an
				// immediate migrate to the master node of this partition only if the master is sync
				if (my_index_in_hvlist == first_sync_node ||
						(cf_contains64(dupl_nodes, n_dupl, self) && is_sync[0])) {
					// Schedule a migrate of this partition to the master
					// node. The p->target needs to be set to indicate this
					// node is migrating data to the master. The last
					// parameter to the partition_migrate_record_fill()
					// contains the flush flag that is used to determine
					// what to do about pending writes once the migration
					// is over.
					p->pending_migrate_tx++;

					// The first sync node in the list is going to be the
					// acting master node. Set the p->target variable and
					// also initialize the duplicate array. This data will
					// be used during the time this node performs the
					// acting role as master.
					if (my_index_in_hvlist == first_sync_node) {
						p->target = HV(j, 0);

						if (n_dupl > 0) {
							p->n_dupl = n_dupl;
							memcpy(p->dupl_nodes, dupl_nodes, sizeof(cf_node) * p->n_dupl);
						}

						partition_migrate_record_fill(&pmr, HV(j, 0), ns, j,
								orig_cluster_key, TX_FLAGS_ACTING_MASTER);
					}
					else {
						partition_migrate_record_fill(&pmr, HV(j, 0), ns, j,
								orig_cluster_key, TX_FLAGS_NONE);
					}

					cf_queue_push(mq, &pmr);
				}
				// Wait for master to flag that it is sync before
				// transmitting the partition
				else if (cf_contains64(dupl_nodes, n_dupl, self) && ! is_sync[0]) {
					p->master_wait_state = MASTER_WAITING;
					ns_pending_migrate_tx_later++;
				}

				// If this is a replica and there are duplicate partitions
				// then wait for migration from master.
				if (my_index_in_hvlist < p->p_repl_factor) {
					if (n_dupl > 0) {
						p->pending_migrate_rx++;
						p->origin = HV(j, 0);
					}

					p->state = AS_PARTITION_STATE_SYNC;

					break; // out of switch
				}

				// Not a replica. Partition will enter zombie state if it
				// has pending work. Otherwise, we discard the partition.
				if (p->pending_migrate_tx || p->master_wait_state == MASTER_WAITING) {
					p->state = AS_PARTITION_STATE_ZOMBIE;
				}
				else  { // throwing away duplicate partition
					set_partition_absent_lockfree(p,
							&ns->partitions[j].version_info, ns, j, false);
				}

				break;

			case true:
			default:
				cf_crash(AS_PARTITION, "{%s:%d} State Error.");
				break;
			}

			// Copy the new succession list over the old succession list.
			memcpy(p->old_sl, &hv_ptr[j * g_config.paxos_max_cluster_size],
					sizeof(cf_node) * g_config.paxos_max_cluster_size);

			p->cluster_key = orig_cluster_key;

			ns_pending_migrate_rx += p->pending_migrate_rx;
			ns_pending_migrate_tx += p->pending_migrate_tx;

			pthread_mutex_unlock(&p->lock);
		} // end for each partition

		int ns_pending_migrate_tx_total = ns_pending_migrate_tx + ns_pending_migrate_tx_later;

		cf_info(AS_PARTITION, "{%s} re-balanced, expected migrations - (%d tx, %d rx)",
				ns->name, ns_pending_migrate_tx_total, ns_pending_migrate_rx);

		cf_atomic_int_set(&ns->migrate_tx_partitions_initial, ns_pending_migrate_tx_total);
		cf_atomic_int_set(&ns->migrate_tx_partitions_remaining, ns_pending_migrate_tx_total);

		cf_atomic_int_set(&ns->migrate_rx_partitions_initial, ns_pending_migrate_rx);
		cf_atomic_int_set(&ns->migrate_rx_partitions_remaining, ns_pending_migrate_rx);
	} // end for each namespace.

	// All partitions now have replicas assigned, ok to allow transactions.
	g_balance_init = BALANCE_INIT_RESOLVED;

	// Note - if we decide this is the best place to first increment this
	// counter, we could get rid of g_balance_init and just use this instead.
	cf_atomic_int_incr(&g_config.partition_generation);

	cf_info(AS_PAXOS, "global partition state: total %d lost %d unique %d duplicate %d",
			n_total, n_lost, n_unique, n_duplicate);
	cf_info(AS_PAXOS, "partition state after fixing lost partitions (master): total %d lost %d unique %d duplicate %d",
			n_total, n_lost - n_recreate, n_unique + n_recreate, n_duplicate);
	cf_info(AS_PAXOS, "%d new partition version tree paths generated",
			n_new_versions);

	if (n_total != (n_lost + n_unique + n_duplicate)) {
		cf_warning(AS_PAXOS, "global partition state error: total %d lost %d unique %d duplicate %d",
				n_total, n_lost, n_unique, n_duplicate);
	}

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_storage_info_flush(g_config.namespaces[i]);
	}

	as_partition_allow_migrations();

	partition_migrate_record pmr;

	while (cf_queue_pop(mq, &pmr, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		as_migrate_emigrate(&pmr, false);
	}

	cf_queue_destroy(mq);

	cf_free(hv_ptr);
	cf_free(hv_slindex_ptr);
} // end as_partition_balance()


// Initially, every partition is either ABSENT, or a version was read from
// storage and it is SYNC.
void
as_partition_balance_init()
{
	g_config.paxos->cluster_size = 1;
	as_paxos_set_cluster_integrity(g_config.paxos, true);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		ns->replication_factor = 1;

		uint32_t n_stored = 0;

		for (uint32_t j = 0; j < AS_PARTITIONS; j++) {
			as_partition *p = &ns->partitions[j];

			p->p_repl_factor = 1;

			if (! is_partition_null(&p->version_info)) {
				memcpy(p->replica, &g_config.self_node, sizeof(cf_node));
				p->primary_version_info = p->version_info;
				n_stored++;
			}
			else {
				// Stores the vinfo length, even when the vinfo is zeroed.
				clear_partition_version_in_storage(ns, j, false);
			}

			p->old_sl[0] = g_config.self_node;
			p->cluster_key = as_paxos_get_cluster_key();
		}

		if (n_stored < AS_PARTITIONS) {
			as_storage_info_flush(ns);
		}

		cf_info(AS_PARTITION, "{%s} %u partitions: found %u absent, %u stored",
				ns->name, AS_PARTITIONS, AS_PARTITIONS - n_stored, n_stored);
	}
}


// If this node encounters other nodes at startup, prevent it from switching to
// a single-node cluster - any initially ABSENT partitions participate in paxos
// and balancing as ABSENT.
void
as_partition_balance_init_multi_node_cluster()
{
	g_multi_node = true;
}


// If we do not encounter other nodes at startup, all initially ABSENT
// partitions are assigned a new version and converted to SYNC.
void
as_partition_balance_init_single_node_cluster()
{
	as_partition_vinfo new_vinfo;

	memset(&new_vinfo, 0, sizeof(new_vinfo));
	generate_new_partition_version(&new_vinfo);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		uint32_t n_promoted = 0;

		for (uint32_t j = 0; j < AS_PARTITIONS; j++) {
			as_partition *p = &ns->partitions[j];

			if (is_partition_null(&p->version_info)) {
				// For nsup, which is allowed to operate while we're doing this.
				pthread_mutex_lock(&p->lock);

				p->state = AS_PARTITION_STATE_SYNC;
				memcpy(p->replica, &g_config.self_node, sizeof(cf_node));

				p->version_info = new_vinfo;
				p->primary_version_info = new_vinfo;
				g_config.paxos->c_partition_vinfo[i][0][j] = new_vinfo;
				set_partition_version_in_storage(ns, j, &new_vinfo, false);

				n_promoted++;

				pthread_mutex_unlock(&p->lock);
			}
		}

		if (n_promoted != 0) {
			as_storage_info_flush(ns);
		}

		cf_info(AS_PARTITION, "{%s} %u absent partitions promoted to master",
				ns->name, n_promoted);
	}

	// Ok to allow transactions.
	g_balance_init = BALANCE_INIT_RESOLVED;

	cf_atomic_int_incr(&g_config.partition_generation);
}


// Has the node resolved as operating either in a multi-node cluster or as a
// single-node cluster?
bool
as_partition_balance_is_init_resolved()
{
	return g_balance_init == BALANCE_INIT_RESOLVED;
}


// Has this node encountered other nodes?
bool
as_partition_balance_is_multi_node_cluster()
{
	return g_multi_node;
}


// A partition is queryable only when the node is master or origin
// BEWARE. No partition lock is being taken here.
// This is done to avoid a deadlock between sindex and apply journal
bool
as_partition_is_queryable_lockfree(as_namespace * ns, as_partition * p)
{
	cf_node self             = g_config.self_node;
	bool is_sync             = (p->state == AS_PARTITION_STATE_SYNC);
	bool migrating_to_master = (p->target != 0);
	bool is_master           = (p->replica[0] == self);

	return (is_master && is_sync) || migrating_to_master;
}
