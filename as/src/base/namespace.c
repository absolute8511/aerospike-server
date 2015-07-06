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

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"

#include "dynbuf.h"
#include "fault.h"
#include "hist.h"
#include "jem.h"
#include "meminfo.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/system_metadata.h"
#include "storage/storage.h"


static cf_atomic32 namespace_id_counter = 0;

// Create a new namespace and hook it up in the data structure

as_namespace *
as_namespace_create(char *name, uint16_t replication_factor)
{
	if (strlen(name) >= AS_ID_NAMESPACE_SZ) {
		cf_warning(AS_NAMESPACE, "can't create namespace: name length too long");
		return NULL;
	}

	if (replication_factor < 2) {
		cf_warning(AS_NAMESPACE, "can't create namespace: need replication factor >= 2");
		return NULL;
	}

	if (g_config.namespaces >= AS_NAMESPACE_SZ) {
		cf_warning(AS_NAMESPACE, "can't create namespace: already have %d", AS_NAMESPACE_SZ);
		return NULL;
	}

	for (int i = 0; i < g_config.namespaces; i++) {
		if (0 == strcmp(g_config.namespace[i]->name, name)) {
			cf_warning(AS_NAMESPACE, "can't create namespace: namespace %s mentioned again in the configuration", name);
			return NULL;
		}
	}

	as_namespace *ns = cf_malloc(sizeof(as_namespace));
	cf_assert(ns, AS_NAMESPACE, CF_CRITICAL, "%s as_namespace allocation failed", name);

	// Set all members 0/NULL/false to start with.
	memset(ns, 0, sizeof(as_namespace));

	strncpy(ns->name, name, AS_ID_NAMESPACE_SZ - 1);
	ns->name[AS_ID_NAMESPACE_SZ - 1] = '\0';
	ns->id = cf_atomic32_incr(&namespace_id_counter);

#ifdef USE_JEM
	if (-1 == (ns->jem_arena = jem_create_arena())) {
		cf_crash(AS_NAMESPACE, "can't create JEMalloc arena for namespace %s", name);
	} else {
		cf_info(AS_NAMESPACE, "Created JEMalloc arena #%d for namespace \"%s\"", ns->jem_arena, name);
	}
	as_namespace_set_jem_arena(ns->name, ns->jem_arena);
#endif

	ns->cold_start = false; // try warm restart unless told not to
	ns->arena = NULL; // can't create the arena until the configuration has been done

	//--------------------------------------------
	// Configuration defaults.
	//

	ns->replication_factor = replication_factor;
	ns->cfg_replication_factor = replication_factor;
	ns->memory_size = 1024LL * 1024LL * 1024LL * 4LL; // default memory limit is 4G per namespace
	ns->default_ttl = 0; // default time-to-live is unlimited
	ns->enable_xdr = false;
	ns->sets_enable_xdr = true; // ship all the sets by default
	ns->ns_forward_xdr_writes = false; // forwarding of xdr writes is disabled by default
	ns->ns_allow_nonxdr_writes = true; // allow nonxdr writes by default
	ns->ns_allow_xdr_writes = true; // allow xdr writes by default
	ns->allow_versions = false;
	ns->cold_start_evict_ttl = 0xFFFFffff; // unless this is specified via config file, use evict void-time saved in device header
	ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
	ns->data_in_index = false;
	ns->evict_tenths_pct = 5; // default eviction amount is 0.5%
	ns->hwm_disk = 0.5; // default high water mark for eviction is 50%
	ns->hwm_memory = 0.6; // default high water mark for eviction is 60%
	ns->ldt_enabled = false; // By default ldt is not enabled
	ns->ldt_page_size = 8192; // default ldt page size is 8192
	ns->ldt_gc_sleep_us = 500; // Default is sleep for .5Ms. This translates to constant 2k Subrecord
							   // GC per second.
	ns->obj_size_hist_max = OBJ_SIZE_HIST_NUM_BUCKETS;
	ns->single_bin = false;
	ns->stop_writes_pct = 0.9; // stop writes when 90% of either memory or disk is used

	// Set default server policies which are used only when the corresponding override is true:
	ns->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
	ns->read_consistency_level_override = false;
	ns->write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
	ns->write_commit_level_override = false;

	ns->storage_type = AS_STORAGE_ENGINE_MEMORY;
	ns->storage_data_in_memory = true;
	// Note - default true is consistent with AS_STORAGE_ENGINE_MEMORY, but
	// cfg.c will set default false for AS_STORAGE_ENGINE_SSD and KV.

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
	ns->storage_num_write_blocks = 64; // number of write blocks to use with KV store devices
	ns->storage_post_write_queue = 256; // number of wblocks per device used as post-write cache
	ns->storage_read_block_size = 64 * 1024; // size in bytes of read buffers to use with KV store devices
	// [Note - current FusionIO maximum read buffer size is 1MB - 512B.]
	ns->storage_write_smoothing_period = 0; // seconds of write data to use for smoothing (default 0 = off)
	ns->storage_write_threads = 1;

	// SINDEX
	ns->sindex_data_max_memory = ULONG_MAX;
	ns->sindex_data_memory_used = 0;
	ns->sindex_cfg_var_hash = NULL;
	ns->sindex_num_partitions = 32;

	//
	// END - Configuration defaults.
	//--------------------------------------------

	g_config.namespace[g_config.namespaces] = ns;
	g_config.namespaces++;

	char hist_name[HISTOGRAM_NAME_SIZE];
	// Note - histograms' ranges MUST be set before use.

	sprintf(hist_name, "%s object size histogram", name);
	ns->obj_size_hist = linear_histogram_create(hist_name, 0, 0, OBJ_SIZE_HIST_NUM_BUCKETS);

	sprintf(hist_name, "%s evict histogram", name);
	ns->evict_hist = linear_histogram_create(hist_name, 0, 0, EVICTION_HIST_NUM_BUCKETS);

	sprintf(hist_name, "%s ttl histogram", name);
	ns->ttl_hist = linear_histogram_create(hist_name, 0, 0, EVICTION_HIST_NUM_BUCKETS);

	return ns;
}


void
as_namespaces_init(bool cold_start_cmd, uint32_t instance)
{
	// Sanity-check the persistent memory scheme. TODO - compile-time assert.
	as_xmem_scheme_check();

	uint32_t stage_capacity = as_mem_check();

	if (cold_start_cmd) {
		cf_info(AS_NAMESPACE, "got cold-start command");
	}

	for (int i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		// Cold start if manually forced.
		ns->cold_start = cold_start_cmd;

		as_namespace_setup(ns, instance, stage_capacity);

		// Done with temporary sets configuration array.
		if (ns->sets_cfg_array) {
			cf_free(ns->sets_cfg_array);
		}

		// set partition id inside partition object.
		for (uint i = 0; i < AS_PARTITIONS; i++) {
			ns->partitions[i].partition_id = i;
		}
		for (uint i = 0; i < AS_PARTITIONS; i++) {
			as_partition_init(&ns->partitions[i], ns, i);
		}

		as_sindex_init(ns);
	}

	// Register Secondary Index module with the majority merge policy callback.
	// Secondary index metadata is restored for all namespaces. Must be done
	// before as_storage_init() populates the indexes.
	int retval = as_smd_create_module(SINDEX_MODULE,
			as_smd_majority_consensus_merge, NULL, as_sindex_smd_accept_cb,
			NULL, as_sindex_smd_can_accept_cb, NULL);

	if (0 > retval) {
		cf_crash(AS_NAMESPACE, "failed to create SMD module \"%s\" (rv %d)",
				SINDEX_MODULE, retval);
	}

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
		cf_vmapx_err result = cf_vmapx_put_unique(ns->p_sets_vmap, &ns->sets_cfg_array[i], &idx);

		if (result == CF_VMAPX_ERR_NAME_EXISTS) {
			as_set* p_set = NULL;

			if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set)) != CF_VMAPX_OK) {
				// Should be impossible - just verified idx.
				cf_warning(AS_NAMESPACE, "unexpected error %d", result);
				return false;
			}

			// Rewrite configurable metadata - config values may have changed.
			p_set->stop_write_count = ns->sets_cfg_array[i].stop_write_count;
			p_set->evict_hwm_count = ns->sets_cfg_array[i].evict_hwm_count;
			p_set->enable_xdr = ns->sets_cfg_array[i].enable_xdr;
		}
		else if (result != CF_VMAPX_OK) {
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
	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (0 == strcmp(ns->name, name)) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_byid(uint id)
{
	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (id == ns->id) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_bybuf(byte *buf, size_t len)
{
	if (len >= AS_ID_NAMESPACE_SZ) {
		return NULL;
	}

	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (memcmp(buf, ns->name, len) == 0 && ns->name[len] == 0) {
			return ns;
		}
	}

	return NULL;
}


as_namespace *
as_namespace_get_bymsgfield(as_msg_field *fp)
{
	return as_namespace_get_bybuf((byte *)fp->data, as_msg_field_get_value_sz(fp));
}


as_namespace *
as_namespace_get_bymsgfield_unswap(as_msg_field *fp)
{
	return as_namespace_get_bybuf((byte *)fp->data, as_msg_field_get_value_sz_unswap(fp));
}


as_namespace_id
as_namespace_getid_bymsgfield(as_msg_field *fp)
{
	if (as_msg_field_get_value_sz(fp) >= AS_ID_NAMESPACE_SZ) {
		return -1;
	}

	as_namespace_id		ns_id = -1;
	uint lim = g_config.namespaces;
	uint i;
	for (i = 0; i < lim; i++) {
		as_namespace *ns = g_config.namespace[i];
		if (strncmp((char *)fp->data, ns->name, as_msg_field_get_value_sz(fp)) == 0) {
			ns_id = ns->id;
			break;
		}
	}

	return ns_id;
}


#define CL_TERA_BYTES	1099511627776L
#define CL_PETA_BYTES	1125899906842624L

void
as_namespace_eval_write_state(as_namespace *ns, bool *hwm_breached, bool *stop_writes)
{
	cf_assert(ns, AS_NAMESPACE, CF_WARNING, "NULL namespace");
	cf_assert(hwm_breached, AS_NAMESPACE, CF_WARNING, "NULL parameter, hwm_breached");
	cf_assert(stop_writes, AS_NAMESPACE, CF_WARNING, "NULL parameter, stop_writes");

	*hwm_breached = false;
	*stop_writes = false;

	// Compute the space limits on this namespace
	uint64_t mem_lim = ns->memory_size;
	uint64_t ssd_lim = ns->ssd_size;

	// Compute the high-watermarks - memory.
	uint64_t mem_hwm = mem_lim * ns->hwm_memory;
	uint64_t mem_stop_writes = mem_lim * ns->stop_writes_pct;

	// Compute the high-watermark - disk.
	uint64_t ssd_hwm = ssd_lim * ns->hwm_disk;

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
	uint64_t index_sz = cf_atomic_int_get(ns->n_objects) * as_index_size_get(ns);
	uint64_t sub_index_sz = cf_atomic_int_get(ns->n_sub_objects) * as_index_size_get(ns);
	uint64_t sindex_sz = as_sindex_get_ns_memory_used(ns);
	uint64_t data_in_memory_sz = cf_atomic_int_get(ns->n_bytes_memory);
	uint64_t memory_sz = index_sz + sub_index_sz + data_in_memory_sz + sindex_sz;

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
		cf_info(AS_NAMESPACE, "{%s} hwm_breached %s%s, stop_writes %s%s, memory sz:%"PRIu64" (%"PRIu64" + %"PRIu64") hwm:%"PRIu64" sw:%"PRIu64", disk sz:%"PRIu64" hwm:%"PRIu64,
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

/* as_namespace_bless
 * Bless a namespace: set all its partitions to be consistent */
void
as_namespace_bless(as_namespace *ns)
{
	if (NULL == ns) {
		return;
	}

	for (int i = 0; i < AS_PARTITIONS; i++) {
		as_partition_bless(&ns->partitions[i]);
	}
}

int as_namespace_check_set_limits(as_set *p_set, as_namespace *ns) {
	// Check limits on the num_elements in a set.
	uint64_t stop_write_count = cf_atomic64_get(p_set->stop_write_count);
	uint64_t num_elements = cf_atomic64_get(p_set->num_elements);

	// If the number of objects crossed the stop_write_count or the set
	// is to deleted, return error.
	if ((stop_write_count && (num_elements >= stop_write_count)) || IS_SET_DELETED(p_set)) {
		return AS_NAMESPACE_SET_THRESHOLD_EXCEEDED;
	}

	return 0;
}

const char *as_namespace_get_set_name(as_namespace *ns, uint16_t set_id)
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

uint16_t as_namespace_get_set_id(as_namespace *ns, const char *set_name)
{
	uint32_t idx;

	return cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx) == CF_VMAPX_OK ?
			(uint16_t)(idx + 1) : INVALID_SET_ID;
}

// At the moment this is only used by the enterprise build security feature.
uint16_t as_namespace_get_create_set_id(as_namespace *ns, const char *set_name)
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
		as_set set;

		memset(&set, 0, sizeof(set));

		// Check name length just once, here at insertion. (Other vmap calls are
		// safe if name is too long - they return CF_VMAPX_ERR_NAME_NOT_FOUND.)
		strncpy(set.name, set_name, AS_SET_NAME_MAX_SIZE);

		if (set.name[AS_SET_NAME_MAX_SIZE - 1]) {
			set.name[AS_SET_NAME_MAX_SIZE - 1] = 0;

			cf_warning(AS_NAMESPACE, "set name %s... too long", set.name);
			return INVALID_SET_ID;
		}

		set.num_elements = 0; // *not* adding an element
		result = cf_vmapx_put_unique(ns->p_sets_vmap, &set, &idx);

		if (result == CF_VMAPX_ERR_NAME_EXISTS) {
			return (uint16_t)(idx + 1);
		}

		if (result == CF_VMAPX_ERR_FULL) {
			cf_warning(AS_NAMESPACE, "at set names limit, can't add %s", set.name);
			return INVALID_SET_ID;
		}

		if (result != CF_VMAPX_OK) {
			// Currently, remaining errors are all some form of out-of-memory.
			cf_warning(AS_NAMESPACE, "error %d, can't add %s", result, set.name);
			return INVALID_SET_ID;
		}

		return (uint16_t)(idx + 1);
	}

	// Should be impossible.
	cf_warning(AS_NAMESPACE, "unexpected error %d", result);
	return INVALID_SET_ID;
}

int as_namespace_get_create_set(as_namespace *ns, const char *set_name, uint16_t *p_set_id, bool check_threshold)
{
	if (! set_name) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "null set name");
		return -1;
	}

	uint32_t idx;
	cf_vmapx_err result = cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx);
	bool already_in_vmap = false;

	if (result == CF_VMAPX_OK) {
		already_in_vmap = true;
	}
	else if (result == CF_VMAPX_ERR_NAME_NOT_FOUND) {
		as_set set;

		memset(&set, 0, sizeof(set));

		// Check name length just once, here at insertion. (Other vmap calls are
		// safe if name is too long - they return CF_VMAPX_ERR_NAME_NOT_FOUND.)
		strncpy(set.name, set_name, AS_SET_NAME_MAX_SIZE);

		if (set.name[AS_SET_NAME_MAX_SIZE - 1]) {
			set.name[AS_SET_NAME_MAX_SIZE - 1] = 0;

			cf_info(AS_NAMESPACE, "set name %s... too long", set.name);
			return -1;
		}

		set.num_elements = 1;
		result = cf_vmapx_put_unique(ns->p_sets_vmap, &set, &idx);

		if (result == CF_VMAPX_ERR_NAME_EXISTS) {
			already_in_vmap = true;
		}
		else if (result == CF_VMAPX_ERR_FULL) {
			cf_info(AS_NAMESPACE, "at set names limit, can't add %s", set.name);
			return -1;
		}
		else if (result != CF_VMAPX_OK) {
			// Currently, remaining errors are all some form of out-of-memory.
			cf_info(AS_NAMESPACE, "error %d, can't add %s", result, set.name);
			return -1;
		}
	}
	else {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "unexpected error %d", result);
		return -1;
	}

	if (already_in_vmap) {
		as_set *p_set;

		if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set)) != CF_VMAPX_OK) {
			// Should be impossible - just verified idx.
			cf_warning(AS_NAMESPACE, "unexpected error %d", result);
			return -1;
		}

		// If check is requested, don't exceed set's limits.
		if (check_threshold &&
				as_namespace_check_set_limits(p_set, ns) == AS_NAMESPACE_SET_THRESHOLD_EXCEEDED) {
			return AS_NAMESPACE_SET_THRESHOLD_EXCEEDED;
		}

		// The set passed all tests - need to increment its num_elements.
		cf_atomic64_incr(&p_set->num_elements);
	}

	*p_set_id = (uint16_t)(idx + 1);

	return 0;
}

as_set *as_namespace_init_set(as_namespace *ns, const char *set_name)
{
	if (! set_name) {
		return NULL;
	}

	uint32_t idx;
	cf_vmapx_err result = cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx);

	if (result == CF_VMAPX_ERR_NAME_NOT_FOUND) {
		as_set set;

		memset(&set, 0, sizeof(set));

		// Check name length just once, here at insertion. (Other vmap calls are
		// safe if name is too long - they return CF_VMAPX_ERR_NAME_NOT_FOUND.)
		strncpy(set.name, set_name, AS_SET_NAME_MAX_SIZE);

		if (set.name[AS_SET_NAME_MAX_SIZE - 1]) {
			set.name[AS_SET_NAME_MAX_SIZE - 1] = 0;

			cf_info(AS_NAMESPACE, "set name %s... too long", set.name);
			return NULL;
		}

		result = cf_vmapx_put_unique(ns->p_sets_vmap, &set, &idx);

		// Since this function can be called via info, need to handle race with
		// as_namespace_get_create_set() that returns CF_VMAPX_ERR_NAME_EXISTS.
		if (result != CF_VMAPX_OK && result != CF_VMAPX_ERR_NAME_EXISTS) {
			cf_warning(AS_NAMESPACE, "unexpected error %d", result);
			return NULL;
		}
	}
	else if (result != CF_VMAPX_OK) {
		// Should be impossible.
		cf_warning(AS_NAMESPACE, "unexpected error %d", result);
		return NULL;
	}

	as_set *p_set = NULL;

	if ((result = cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set)) != CF_VMAPX_OK) {
		// Should be impossible - just verified idx.
		cf_warning(AS_NAMESPACE, "unexpected error %d", result);
		return NULL;
	}

	return p_set;
}

static void
append_set_props(as_set *p_set, cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "n_objects=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->num_elements));
	cf_dyn_buf_append_char(db, ':');
	cf_dyn_buf_append_string(db, "set-stop-write-count=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->stop_write_count));
	cf_dyn_buf_append_char(db, ':');
	cf_dyn_buf_append_string(db, "set-evict-hwm-count=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(p_set->evict_hwm_count));
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
	cf_dyn_buf_append_string(db, "set-delete=");
	if (IS_SET_DELETED(p_set)) {
		cf_dyn_buf_append_string(db, "true");
	}
	else {
		cf_dyn_buf_append_string(db, "false");
	}
	cf_dyn_buf_append_char(db, ';');
}

void as_namespace_get_set_info(as_namespace *ns, const char *set_name, cf_dyn_buf *db)
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
			cf_dyn_buf_append_string(db, "ns_name=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_string(db, "set_name=");
			cf_dyn_buf_append_string(db, p_set->name);
			cf_dyn_buf_append_char(db, ':');
			append_set_props(p_set, db);
		}
	}
}

void as_namespace_release_set_id(as_namespace *ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) != CF_VMAPX_OK) {
		return;
	}

	if (cf_atomic64_decr(&p_set->num_elements) < 0) {
		cf_warning(AS_NAMESPACE, "set_id %u - num_elements went negative!", set_id);
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

		cf_dyn_buf_append_string(db, "num-bin-names=");
		cf_dyn_buf_append_uint32(db, bin_count);
		cf_dyn_buf_append_string(db, ",bin-names-quota=");
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
			linear_histogram_get_info(ns->ttl_hist, db);
			cf_dyn_buf_append_char(db, ';');
		} else if (strcmp(hist_name, "evict") == 0) {
			cf_dyn_buf_append_string(db, "evict=");
			linear_histogram_get_info(ns->evict_hist, db);
			cf_dyn_buf_append_char(db, ';');
		} else if (strcmp(hist_name, "objsz") == 0) {
			if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
				cf_dyn_buf_append_string(db, "objsz=");
				linear_histogram_get_info(ns->obj_size_hist, db);
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
					linear_histogram_get_info(ns->set_ttl_hists[set_id], db);
					cf_dyn_buf_append_char(db, ';');
				} else {
					cf_dyn_buf_append_string(db, "hist-unavailable");
				}
			} else if (strcmp(hist_name, "evict") == 0) {
				if (ns->set_evict_hists[set_id]) {
					cf_dyn_buf_append_string(db, "evict=");
					linear_histogram_get_info(ns->set_evict_hists[set_id], db);
					cf_dyn_buf_append_char(db, ';');
				} else {
					cf_dyn_buf_append_string(db, "hist-unavailable");
				}
			} else {
				cf_dyn_buf_append_string(db, "error-unknown-hist-name");
			}
		} else {
			cf_dyn_buf_append_string(db, "error-unknown-set-name");
		}
	}
}

#ifdef USE_JEM
/*
 *  Mapping between a namespace name and a JEMalloc arena number.
 */
typedef struct ns2arena_s {
	char *ns;                  // Namespace name.
	int arena;                 // JEMalloc arena (-1 means none.)
} ns2arena_t;

/*
 *  Global array mapping namespace names to the corresponding JEMalloc arenas.
 */
static ns2arena_t g_ns2arena[AS_NAMESPACE_SZ] =
{
	{ NULL, -1 }
};

/*
 *  Set the JEMalloc arena for the given namespace.
 *  The namespace must already be set.
 *  Return 0 if successful, else -1.
 */
int
as_namespace_set_jem_arena(char *ns, int arena)
{
	int retval = -1;

	ns2arena_t *p = g_ns2arena;

	while (p->ns) {
		if (!strcmp(p->ns, ns)) {
			p->arena = arena;
			retval = 0;
			break;
		} else {
			p++;
		}
	}

	if (retval) {
		p->ns = ns;
		p->arena = arena;
		retval = 0;
	}

	return retval;
}

/*
 *  Return the JEMalloc arena for the given namepace name.
 *  (-1 means none set.)
 */
int
as_namespace_get_jem_arena(char *ns)
{
	int arena = -1;
	ns2arena_t *p = g_ns2arena;

	while (p->ns) {
		if (!strcmp(p->ns, ns)) {
			arena = p->arena;
			break;
		} else {
			p++;
		}
	}

	return arena;
}
#endif
