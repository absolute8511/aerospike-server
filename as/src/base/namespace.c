/*
 * namespace.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 * Operations on namespaces
 *
 */

//==========================================================
// Includes.
//

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_hash_math.h"

#include "dynbuf.h"
#include "fault.h"
#include "hist.h"
#include "linear_hist.h"
#include "meminfo.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/system_metadata.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//


//==========================================================
// Globals.
//

static as_namespace_id g_namespace_id_counter = 0;


//==========================================================
// Forward declarations.
//


//==========================================================
// Inlines & macros.
//

// Generate a hash value which does not collide with nsid (32 to UINT32_MAX)
// Note: For namespaces whose fnv hash value falls between 0-32 may collide with
// namespaces whose value falls between 32-64 as we are adding 32. Hoping that
// it is low probability. We will know if it happens as server crashes up front.
static inline uint32_t
ns_name_hash(char *name)
{
	uint32_t hv = cf_hash_fnv32((const uint8_t *)name, strlen(name));

	if (hv <= NAMESPACE_MAX_NUM) {
		hv += NAMESPACE_MAX_NUM;
	}

	return hv;
}


//==========================================================
// Public API.
//

// Create a new namespace and hook it up in the data structure
as_namespace *
as_namespace_create(char *name)
{
	if (strlen(name) >= AS_ID_NAMESPACE_SZ) {
		cf_warning(AS_NAMESPACE, "can't create namespace: name length too long");
		return NULL;
	}

	if (g_config.n_namespaces >= AS_NAMESPACE_SZ) {
		cf_warning(AS_NAMESPACE, "can't create namespace: already have %d", AS_NAMESPACE_SZ);
		return NULL;
	}

	for (int i = 0; i < g_config.n_namespaces; i++) {
		if (0 == strcmp(g_config.namespaces[i]->name, name)) {
			cf_warning(AS_NAMESPACE, "can't create namespace: namespace %s mentioned again in the configuration", name);
			return NULL;
		}
	}

	uint32_t cur_namehash = ns_name_hash(name);

	for (int i = 0; i < g_config.n_namespaces; i++) {
		if (g_config.namespaces[i]->namehash == cur_namehash) {
			cf_crash_nostack(AS_XDR, "namespace %s's hash value collides with namespace %s",
					g_config.namespaces[i]->name, name);
		}
	}

	as_namespace *ns = cf_malloc(sizeof(as_namespace));
	cf_assert(ns, AS_NAMESPACE, "%s as_namespace allocation failed", name);

	// Set all members 0/NULL/false to start with.
	memset(ns, 0, sizeof(as_namespace));

	strncpy(ns->name, name, AS_ID_NAMESPACE_SZ - 1);
	ns->name[AS_ID_NAMESPACE_SZ - 1] = '\0';
	ns->id = ++g_namespace_id_counter; // note that id is 1-based
	ns->namehash = cur_namehash;

	if (-1 == (ns->jem_arena = cf_alloc_create_arena())) {
		cf_crash(AS_NAMESPACE, "can't create JEMalloc arena for namespace %s", name);
	} else {
		cf_info(AS_NAMESPACE, "Created JEMalloc arena #%d for namespace \"%s\"", ns->jem_arena, name);
	}

	ns->cold_start = false; // try warm restart unless told not to
	ns->arena = NULL; // can't create the arena until the configuration has been done

	//--------------------------------------------
	// Configuration defaults.
	//

	ns->replication_factor = 1; // not 0 in case clients get map before initial rebalance
	ns->cfg_replication_factor = 2;
	ns->memory_size = 1024LL * 1024LL * 1024LL * 4LL; // default memory limit is 4G per namespace
	ns->default_ttl = 0; // default time-to-live is unlimited
	ns->cold_start_evict_ttl = 0xFFFFffff; // unless this is specified via config file, use evict void-time saved in device header
	ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
	ns->data_in_index = false;
	ns->evict_hist_buckets = 10000; // for 30 day TTL, bucket width is 4 minutes 20 seconds
	ns->evict_tenths_pct = 5; // default eviction amount is 0.5%
	ns->hwm_disk_pct = 50; // evict when device usage exceeds 50%
	ns->hwm_memory_pct = 60; // evict when memory usage exceeds 50% of namespace memory-size
	ns->ldt_enabled = false; // By default ldt is not enabled
	ns->ldt_gc_sleep_us = 500; // Default is sleep for .5Ms. This translates to constant 2k Subrecord
							   // GC per second.
	ns->ldt_page_size = 8192; // default ldt page size is 8192
	ns->max_ttl = MAX_ALLOWED_TTL; // 10 years
	ns->migrate_order = 5;
	ns->migrate_retransmit_ms = 1000 * 5; // 5 seconds
	ns->migrate_sleep = 1;
	ns->obj_size_hist_max = OBJ_SIZE_HIST_NUM_BUCKETS;
	ns->single_bin = false;
	ns->tomb_raider_eligible_age = 60 * 60 * 24; // 1 day
	ns->tomb_raider_period = 60 * 60 * 24; // 1 day
	ns->tree_shared.n_lock_pairs = 8;
	ns->tree_shared.n_sprigs = 64;
	ns->stop_writes_pct = 90; // stop writes when 90% of either memory or disk is used

	// Set default server policies which are used only when the corresponding override is true:
	ns->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
	ns->read_consistency_level_override = false;
	ns->write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
	ns->write_commit_level_override = false;

	ns->storage_type = AS_STORAGE_ENGINE_MEMORY;
	ns->storage_data_in_memory = true;
	// Note - default true is consistent with AS_STORAGE_ENGINE_MEMORY, but
	// cfg.c will set default false for AS_STORAGE_ENGINE_SSD.

	ns->storage_filesize = 1024LL * 1024LL * 1024LL * 16LL; // default file size is 16G per file
	ns->storage_scheduler_mode = NULL; // null indicates default is to not change scheduler mode
	ns->storage_write_block_size = 1024 * 1024;
	ns->storage_defrag_lwm_pct = 50; // defrag if occupancy of block is < 50%
	ns->storage_defrag_queue_min = 0; // don't defrag unless the queue has this many eligible wblocks (0: defrag anything queued)
	ns->storage_defrag_sleep = 1000; // sleep this many microseconds between each wblock
	ns->storage_defrag_startup_minimum = 10; // defrag until >= 10% disk is writable before joining cluster
	ns->storage_flush_max_us = 1000 * 1000; // wait this many microseconds before flushing inactive current write buffer (0 = never)
	ns->storage_fsync_max_us = 0; // fsync interval in microseconds (0 = never)
	ns->storage_max_write_cache = 1024 * 1024 * 64;
	ns->storage_min_avail_pct = 5; // stop writes when < 5% disk is writable
	ns->storage_post_write_queue = 256; // number of wblocks per device used as post-write cache
	ns->storage_tomb_raider_sleep = 1000; // sleep this many microseconds between each device read
	ns->storage_write_threads = 1;

	// XDR
	ns->enable_xdr = false;
	ns->sets_enable_xdr = true; // ship all the sets by default
	ns->ns_forward_xdr_writes = false; // forwarding of xdr writes is disabled by default
	ns->ns_allow_nonxdr_writes = true; // allow nonxdr writes by default
	ns->ns_allow_xdr_writes = true; // allow xdr writes by default
	cf_vector_pointer_init(&ns->xdr_dclist_v, 3, 0);

	// SINDEX
	ns->n_bytes_sindex_memory = 0;
	ns->sindex_num_partitions = DEFAULT_PARTITIONS_PER_INDEX;

	// Geospatial query within defaults
	ns->geo2dsphere_within_strict = true;
	ns->geo2dsphere_within_min_level = 1;
	ns->geo2dsphere_within_max_level = 30;
	ns->geo2dsphere_within_max_cells = 12;
	ns->geo2dsphere_within_level_mod = 1;
	ns->geo2dsphere_within_earth_radius_meters = 6371000;  // Wikipedia, mean

	//
	// END - Configuration defaults.
	//--------------------------------------------

	g_config.namespaces[g_config.n_namespaces] = ns;
	g_config.n_namespaces++;

	return ns;
}


void
as_namespaces_init(bool cold_start_cmd, uint32_t instance)
{
	uint32_t stage_capacity = as_mem_check();

	as_namespaces_setup(cold_start_cmd, instance, stage_capacity);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		// Done with temporary sets configuration array.
		if (ns->sets_cfg_array) {
			cf_free(ns->sets_cfg_array);
		}

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_init(ns, pid);
		}

		as_truncate_init(ns);
		as_sindex_init(ns);
	}

	as_truncate_init_smd();
	// TODO - move sindex SMD initialization into sindex, as with truncate.

	// Must be done before as_storage_init() populates the indexes.
	int retval = as_smd_create_module(SINDEX_MODULE,
				as_smd_majority_consensus_merge, NULL,
				NULL, NULL,
				as_sindex_smd_accept_cb, NULL,
				NULL, NULL);

	cf_assert(retval == 0, AS_NAMESPACE, "failed to create sindex SMD module (rv %d)", retval);

	// Wait for Secondary Index SMD to be completely restored.
	while (! g_sindex_smd_restored) {
		usleep(1000);
	}
}


bool
as_namespace_configure_sets(as_namespace *ns)
{
	for (uint32_t i = 0; i < ns->sets_cfg_count; i++) {
		uint32_t idx;
		cf_vmapx_err result = cf_vmapx_put_unique(ns->p_sets_vmap, ns->sets_cfg_array[i].name, &idx);

		if (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS) {
			as_set* p_set = NULL;

			if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set)) != CF_VMAPX_OK) {
				// Should be impossible - just verified idx.
				cf_warning(AS_NAMESPACE, "unexpected error %d", result);
				return false;
			}

			// Transfer configurable metadata.
			p_set->stop_writes_count = ns->sets_cfg_array[i].stop_writes_count;
			p_set->disable_eviction = ns->sets_cfg_array[i].disable_eviction;
			p_set->enable_xdr = ns->sets_cfg_array[i].enable_xdr;
		}
		else {
			// Maybe exceeded max sets allowed, but try failing gracefully.
			cf_warning(AS_NAMESPACE, "vmap error %d", result);
			return false;
		}
	}

	return true;
}


as_namespace *
as_namespace_get_byname(char *name)
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (0 == strcmp(ns->name, name)) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_byid(uint32_t id)
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (id == (uint32_t)ns->id) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_bybuf(uint8_t *buf, size_t len)
{
	if (len >= AS_ID_NAMESPACE_SZ) {
		return NULL;
	}

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (memcmp(buf, ns->name, len) == 0 && ns->name[len] == 0) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_bymsgfield(as_msg_field *fp)
{
	return as_namespace_get_bybuf(fp->data, as_msg_field_get_value_sz(fp));
}


#define CL_TERA_BYTES	1099511627776L
#define CL_PETA_BYTES	1125899906842624L

void
as_namespace_eval_write_state(as_namespace *ns, bool *hwm_breached, bool *stop_writes)
{
	*hwm_breached = false;
	*stop_writes = false;

	// Compute the space limits on this namespace
	uint64_t mem_lim = ns->memory_size;
	uint64_t ssd_lim = ns->ssd_size;

	// Compute the high-watermarks - memory.
	uint64_t mem_hwm = (mem_lim * ns->hwm_memory_pct) / 100;
	uint64_t mem_stop_writes = (mem_lim * ns->stop_writes_pct) / 100;

	// Compute the high-watermark - disk.
	uint64_t ssd_hwm = (ssd_lim * ns->hwm_disk_pct) / 100;

	// compute disk size of namespace
	uint64_t disk_sz = 0;
	int disk_avail_pct = 0;

	as_storage_stats(ns, &disk_avail_pct, &disk_sz);

	// Protection check! Make sure we are not wrapped around for the disk_sz and erroneously evict!
	if (disk_sz > CL_PETA_BYTES) {
		cf_warning(AS_NAMESPACE, "namespace disk bytes big %"PRIu64" please bring node down to reset counter", disk_sz);
		disk_sz = 0;
	}
	// Protection check! Make sure we are not wrapped around for the memory counter and erroneously evict!
	if (cf_atomic_int_get(ns->n_bytes_memory) > CL_TERA_BYTES) {
		cf_warning(AS_NAMESPACE, "namespace memory bytes big %"PRIu64" please bring node down to reset counter", cf_atomic_int_get(ns->n_bytes_memory));
		cf_atomic_int_set(&ns->n_bytes_memory, 0);
	}

	// compute memory size of namespace
	// compute index size - index is always stored in memory
	uint64_t index_sz = cf_atomic64_get(ns->n_objects) * as_index_size_get(ns);
	uint64_t sub_index_sz = cf_atomic64_get(ns->n_sub_objects) * as_index_size_get(ns);
	uint64_t tombstone_index_sz = cf_atomic64_get(ns->n_tombstones) * as_index_size_get(ns);
	uint64_t sindex_sz = cf_atomic64_get(ns->n_bytes_sindex_memory);
	uint64_t data_in_memory_sz = cf_atomic_int_get(ns->n_bytes_memory);
	uint64_t memory_sz = index_sz + sub_index_sz + tombstone_index_sz + data_in_memory_sz + sindex_sz;

	// Possible reasons for eviction or stopping writes.
	// (We don't use all combinations, but in case we change our minds...)
	static const char* reasons[] = {
		"", " (memory)", " (disk)", " (memory & disk)", " (disk avail pct)", " (memory & disk avail pct)", " (disk & disk avail pct)", " (all)"
	};

	// check if the high water mark is breached
	uint32_t how_breached = 0x0;

	if (memory_sz > mem_hwm) {
		*hwm_breached = true;
		how_breached = 0x1;
	}

	if (disk_sz > ssd_hwm) {
		*hwm_breached = true;
		how_breached |= 0x2;
	}

	// check if the writes should be stopped
	uint32_t why_stopped = 0x0;

	if (memory_sz > mem_stop_writes) {
		*stop_writes = true;
		why_stopped = 0x1;
	}

	if (disk_avail_pct < (int)ns->storage_min_avail_pct) {
		*stop_writes = true;
		why_stopped |= 0x4;
	}

	if (*hwm_breached || *stop_writes) {
		cf_warning(AS_NAMESPACE, "{%s} hwm_breached %s%s, stop_writes %s%s, memory sz:%"PRIu64" (%"PRIu64" + %"PRIu64") hwm:%"PRIu64" sw:%"PRIu64", disk sz:%"PRIu64" hwm:%"PRIu64,
				ns->name, *hwm_breached ? "true" : "false", reasons[how_breached], *stop_writes ? "true" : "false", reasons[why_stopped],
				memory_sz, index_sz, data_in_memory_sz, mem_hwm, mem_stop_writes,
				disk_sz, ssd_hwm);
	}
	else {
		cf_debug(AS_NAMESPACE, "{%s} hwm_breached %s%s, stop_writes %s%s, memory sz:%"PRIu64" (%"PRIu64" + %"PRIu64") hwm:%"PRIu64" sw:%"PRIu64", disk sz:%"PRIu64" hwm:%"PRIu64,
				ns->name, *hwm_breached ? "true" : "false", reasons[how_breached], *stop_writes ? "true" : "false", reasons[why_stopped],
				memory_sz, index_sz, data_in_memory_sz, mem_hwm, mem_stop_writes,
				disk_sz, ssd_hwm);
	}
}

const char *
as_namespace_get_set_name(as_namespace *ns, uint16_t set_id)
{
	// Note that set_id is 1-based, but cf_vmap index is 0-based.
	// (This is because 0 in the index structure means 'no set'.)

	if (set_id == INVALID_SET_ID) {
		return NULL;
	}

	as_set *p_set;

	return cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) == CF_VMAPX_OK ?
			p_set->name : NULL;
}

uint16_t
as_namespace_get_set_id(as_namespace *ns, const char *set_name)
{
	uint32_t idx;

	return cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx) == CF_VMAPX_OK ?
			(uint16_t)(idx + 1) : INVALID_SET_ID;
}

// At the moment this is only used by the enterprise build security feature.
uint16_t
as_namespace_get_create_set_id(as_namespace *ns, const char *set_name)
{
	if (! set_name) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "null set name");
		return INVALID_SET_ID;
	}

	uint32_t idx;
	cf_vmapx_err result = cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx);

	if (result == CF_VMAPX_OK) {
		return (uint16_t)(idx + 1);
	}

	if (result == CF_VMAPX_ERR_NAME_NOT_FOUND) {
		result = cf_vmapx_put_unique(ns->p_sets_vmap, set_name, &idx);

		if (result == CF_VMAPX_ERR_NAME_EXISTS) {
			return (uint16_t)(idx + 1);
		}

		if (result == CF_VMAPX_ERR_BAD_PARAM) {
			cf_warning(AS_NAMESPACE, "set name %s too long", set_name);
			return INVALID_SET_ID;
		}

		if (result == CF_VMAPX_ERR_FULL) {
			cf_warning(AS_NAMESPACE, "at set names limit, can't add %s", set_name);
			return INVALID_SET_ID;
		}

		if (result != CF_VMAPX_OK) {
			// Currently, remaining errors are all some form of out-of-memory.
			cf_warning(AS_NAMESPACE, "error %d, can't add %s", result, set_name);
			return INVALID_SET_ID;
		}

		return (uint16_t)(idx + 1);
	}

	// Should be impossible.
	cf_warning(AS_NAMESPACE, "unexpected error %d", result);
	return INVALID_SET_ID;
}

int
as_namespace_set_set_w_len(as_namespace *ns, const char *set_name, size_t len,
		uint16_t *p_set_id, bool apply_restrictions)
{
	as_set *p_set;

	if (as_namespace_get_create_set_w_len(ns, set_name, len, &p_set,
			p_set_id) != 0) {
		return -1;
	}

	if (apply_restrictions && as_set_stop_writes(p_set)) {
		return -2;
	}

	cf_atomic64_incr(&p_set->n_objects);

	return 0;
}

int
as_namespace_get_create_set_w_len(as_namespace *ns, const char *set_name,
		size_t len, as_set **pp_set, uint16_t *p_set_id)
{
	cf_assert(set_name, AS_NAMESPACE, "null set name");
	cf_assert(len != 0, AS_NAMESPACE, "empty set name");

	uint32_t idx;
	cf_vmapx_err result = cf_vmapx_get_index_w_len(ns->p_sets_vmap, set_name,
			len, &idx);

	if (result == CF_VMAPX_ERR_NAME_NOT_FOUND) {
		// Special case handling for name too long.
		if (len >= AS_SET_NAME_MAX_SIZE) {
			char bad_name[AS_SET_NAME_MAX_SIZE];

			memcpy(bad_name, set_name, AS_SET_NAME_MAX_SIZE - 1);
			bad_name[AS_SET_NAME_MAX_SIZE - 1] = 0;

			cf_warning(AS_NAMESPACE, "set name %s... too long", bad_name);
			return -1;
		}

		result = cf_vmapx_put_unique_w_len(ns->p_sets_vmap, set_name, len,
				&idx);

		// Since this function can be called via many functions simultaneously.
		// Need to handle race, So handle CF_VMAPX_ERR_NAME_EXISTS.
		if (result == CF_VMAPX_ERR_FULL) {
			cf_warning(AS_NAMESPACE, "at set names limit, can't add set");
			return -1;
		}

		if (result != CF_VMAPX_OK && result != CF_VMAPX_ERR_NAME_EXISTS) {
			cf_warning(AS_NAMESPACE, "error %d, can't add set", result);
			return -1;
		}
	}
	else if (result != CF_VMAPX_OK) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "unexpected error %d", result);
		return -1;
	}

	if (pp_set) {
		if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx,
				(void**)pp_set)) != CF_VMAPX_OK) {
			// Should be impossible - just verified idx.
			cf_warning(AS_NAMESPACE, "unexpected error %d", result);
			return -1;
		}
	}

	if (p_set_id) {
		*p_set_id = (uint16_t)(idx + 1);
	}

	return 0;
}

as_set*
as_namespace_get_set_by_name(as_namespace *ns, const char *set_name)
{
	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx) != CF_VMAPX_OK) {
		return NULL;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set) !=
			CF_VMAPX_OK) {
		// Should be impossible - just verified idx.
		cf_crash(AS_NAMESPACE, "unexpected vmap error");
	}

	return p_set;
}

as_set*
as_namespace_get_set_by_id(as_namespace *ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return NULL;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) !=
			CF_VMAPX_OK) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "unexpected - record with set-id not in vmap");
		return NULL;
	}

	return p_set;
}

as_set*
as_namespace_get_record_set(as_namespace *ns, const as_record *r)
{
	return as_namespace_get_set_by_id(ns, as_index_get_set_id(r));
}

static void
append_set_props(as_set *p_set, cf_dyn_buf *db)
{
	// Statistics:

	cf_dyn_buf_append_string(db, "objects=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->n_objects));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "tombstones=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->n_tombstones));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "memory_data_bytes=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->n_bytes_memory));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "truncate_lut=");
	cf_dyn_buf_append_uint64(db, p_set->truncate_lut);
	cf_dyn_buf_append_char(db, ':');

	// Configuration:

	cf_dyn_buf_append_string(db, "stop-writes-count=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->stop_writes_count));
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "set-enable-xdr=");
	if (cf_atomic32_get(p_set->enable_xdr) == AS_SET_ENABLE_XDR_TRUE) {
		cf_dyn_buf_append_string(db, "true");
	}
	else if (cf_atomic32_get(p_set->enable_xdr) == AS_SET_ENABLE_XDR_FALSE) {
		cf_dyn_buf_append_string(db, "false");
	}
	else if (cf_atomic32_get(p_set->enable_xdr) == AS_SET_ENABLE_XDR_DEFAULT) {
		cf_dyn_buf_append_string(db, "use-default");
	}
	else {
		cf_dyn_buf_append_uint32(db, cf_atomic32_get(p_set->enable_xdr));
	}
	cf_dyn_buf_append_char(db, ':');

	cf_dyn_buf_append_string(db, "disable-eviction=");
	cf_dyn_buf_append_string(db, IS_SET_EVICTION_DISABLED(p_set) ? "true" : "false");
	cf_dyn_buf_append_char(db, ';');
}

void
as_namespace_get_set_info(as_namespace *ns, const char *set_name, cf_dyn_buf *db)
{
	as_set *p_set;

	if (set_name) {
		if (cf_vmapx_get_by_name(ns->p_sets_vmap, set_name, (void**)&p_set) == CF_VMAPX_OK) {
			append_set_props(p_set, db);
		}

		return;
	}

	for (uint32_t idx = 0; idx < cf_vmapx_count(ns->p_sets_vmap); idx++) {
		if (cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set) == CF_VMAPX_OK) {
			cf_dyn_buf_append_string(db, "ns=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_string(db, "set=");
			cf_dyn_buf_append_string(db, p_set->name);
			cf_dyn_buf_append_char(db, ':');
			append_set_props(p_set, db);
		}
	}
}

void
as_namespace_adjust_set_memory(as_namespace *ns, uint16_t set_id,
		int64_t delta_bytes)
{
	if (set_id == INVALID_SET_ID) {
		return;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) != CF_VMAPX_OK) {
		cf_warning(AS_NAMESPACE, "set_id %u - failed to get as_set from vmap", set_id);
		return;
	}

	if (cf_atomic64_add(&p_set->n_bytes_memory, delta_bytes) < 0) {
		cf_warning(AS_NAMESPACE, "set_id %u - n_bytes_memory went negative!", set_id);
	}
}

void
as_namespace_release_set_id(as_namespace *ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) != CF_VMAPX_OK) {
		return;
	}

	if (cf_atomic64_decr(&p_set->n_objects) < 0) {
		cf_warning(AS_NAMESPACE, "set_id %u - n_objects went negative!", set_id);
	}
}

void
as_namespace_get_bins_info(as_namespace *ns, cf_dyn_buf *db, bool show_ns)
{
	if (show_ns) {
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');
	}

	if (ns->single_bin) {
		cf_dyn_buf_append_string(db, "[single-bin]");
	}
	else {
		uint32_t bin_count = cf_vmapx_count(ns->p_bin_name_vmap);

		cf_dyn_buf_append_string(db, "bin_names=");
		cf_dyn_buf_append_uint32(db, bin_count);
		cf_dyn_buf_append_string(db, ",bin_names_quota=");
		cf_dyn_buf_append_uint32(db, BIN_NAMES_QUOTA);

		for (uint32_t i = 0; i < bin_count; i++) {
			cf_dyn_buf_append_char(db, ',');
			cf_dyn_buf_append_string(db, as_bin_get_name_from_id(ns, (uint16_t)i));
		}
	}

	if (show_ns) {
		cf_dyn_buf_append_char(db, ';');
	}
}

void
as_namespace_get_hist_info(as_namespace *ns, char *set_name, char *hist_name,
		cf_dyn_buf *db, bool show_ns)
{
	if (show_ns) {
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');
	}

	if (set_name == NULL || set_name[0] == 0) {
		if (strcmp(hist_name, "ttl") == 0) {
			cf_dyn_buf_append_string(db, "ttl=");
			linear_hist_get_info(ns->ttl_hist, db);
			cf_dyn_buf_append_char(db, ';');
		} else if (strcmp(hist_name, "objsz") == 0) {
			if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
				cf_dyn_buf_append_string(db, "objsz=");
				linear_hist_get_info(ns->obj_size_hist, db);
				cf_dyn_buf_append_char(db, ';');
			} else {
				cf_dyn_buf_append_string(db, "hist-not-applicable");
			}
		} else {
			cf_dyn_buf_append_string(db, "error-unknown-hist-name");
		}
	} else {
		uint16_t set_id = as_namespace_get_set_id(ns, set_name);
		if (set_id != INVALID_SET_ID) {
			if (strcmp(hist_name, "ttl") == 0) {
				if (ns->set_ttl_hists[set_id]) {
					cf_dyn_buf_append_string(db, "ttl=");
					linear_hist_get_info(ns->set_ttl_hists[set_id], db);
					cf_dyn_buf_append_char(db, ';');
				} else {
					cf_dyn_buf_append_string(db, "hist-unavailable");
				}
			} else if (strcmp(hist_name, "objsz") == 0) {
				if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
					if (ns->set_obj_size_hists[set_id]) {
						cf_dyn_buf_append_string(db, "objsz=");
						linear_hist_get_info(ns->set_obj_size_hists[set_id], db);
						cf_dyn_buf_append_char(db, ';');
					} else {
						cf_dyn_buf_append_string(db, "hist-unavailable");
					}
				} else {
					cf_dyn_buf_append_string(db, "hist-not-applicable");
				}
			} else {
				cf_dyn_buf_append_string(db, "error-unknown-hist-name");
			}
		} else {
			cf_dyn_buf_append_string(db, "error-unknown-set-name");
		}
	}
}
