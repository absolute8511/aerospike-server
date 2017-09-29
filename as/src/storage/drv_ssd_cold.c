/*
 * drv_ssd_cold.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include "storage/drv_ssd.h"
#include "storage/storage.h"
#include "base/index.h"
#include "fault.h"

struct udata_used_here_only {
	drv_ssds *ssds;
	as_index_tree *tree;
	int ith;
	bool is_ldt_sub;
};

static void
ssd_resume_index_reduce_callback (as_index *value, void *data)
{
	struct udata_used_here_only *udata;
	as_namespace *ns;
	as_partition *partition;
	as_index_tree *tree;

	udata = (struct udata_used_here_only*)data;
	ns = udata->ssds->ns;
	tree = udata->tree;
	partition = &ns->partitions[udata->ith];

	// check
	as_partition_id pid = as_partition_getid(value->key);
	if (pid != udata->ith) {
		cf_warning(AS_DRV_SSD, "index don't match, it said %d, but we're traving %d", pid, udata->ith);
		return;
	}

	drv_ssds *ssds = udata->ssds;
	drv_ssd *ssd = NULL;
	for (int i=0; i<ssds->n_ssds; i++) {
		if (ssds->ssds[i].file_id == value->storage_key.ssd.file_id) {
			ssd = &ssds->ssds[i];
			break;
		}
	}
	if (ssd == NULL) {
		cf_warning(AS_DRV_SSD, "don't know which ssd this record belong to: %d", value->key);
		return;
	}

	// I'm not sure is this ok? delete while reducing
	if (value->void_time != 0) {
		// The threshold may be ~ now, or it may be in the future if eviction
		// has been happening.
		uint32_t threshold_void_time =
				cf_atomic32_get(ns->cold_start_threshold_void_time);

		// If the record is set to expire before the threshold, delete it.
		// (Note that if a record is skipped here, then later we encounter a
		// version with older generation but bigger (not expired) void-time,
		// that older version gets resurrected.)
		if (value->void_time < threshold_void_time) {
			cf_detail(AS_DRV_SSD, "record-add deleting void-time %u < threshold %u",
					value->void_time, threshold_void_time);

			as_index_delete(udata->is_ldt_sub? partition->sub_vp : partition->vp, &value->key);
			// as_record_done(&r_ref, ns);
			ssd->record_add_expired_counter++;
			return;
		}

		// If the record is beyond max-ttl, either it's rogue data (from
		// improperly coded clients) or it's data the users don't want anymore
		// (user decreased the max-ttl setting). No such check is needed for
		// the subrecords ...
		if (ns->max_ttl != 0 && ! udata->is_ldt_sub) {
			if (value->void_time > ns->cold_start_max_void_time) {
				cf_debug(AS_DRV_SSD, "record-add deleting void-time %u > max %u",
						value->void_time, ns->cold_start_max_void_time);

				as_index_delete(partition->vp, &value->key);
				// as_record_done(&r_ref, ns);
				ssd->record_add_max_ttl_counter++;
				return;
			}
		}
	}

	// Update maximum void-times.
	cf_atomic_int_setmax(&partition->max_void_time, value->void_time);
	cf_atomic_int_setmax(&ns->max_void_time, value->void_time);

	ssd->record_add_unique_counter++;

	uint32_t size = (uint32_t)RBLOCKS_TO_BYTES(value->storage_key.ssd.n_rblocks);
	uint32_t wblock_id = RBLOCK_ID_TO_WBLOCK_ID(ssd, value->storage_key.ssd.rblock_id);

	ssd->inuse_size += size;
	ssd->alloc_table->wblock_state[wblock_id].inuse_sz += size;

	tree->elements++;
	cf_atomic_int_add(&ns->n_objects, 1);
}

void
ssd_resume_devices(drv_ssds *ssds)
{
	int i;
	as_partition *partition;
	struct udata_used_here_only udata;

	for (i=0; i<AS_PARTITIONS; i++) {
		udata.ssds = ssds;
		udata.ith = i;

		// skip the partition not belong to this ssd
		if (! ssds->get_state_from_storage[i]) {
			continue;
		}

		partition = &ssds->ns->partitions[i];

		// ldt decide the index tree in vp or sub_vp
		udata.tree = partition->vp;
		udata.is_ldt_sub = false;
		as_index_reduce_sync(partition->vp, ssd_resume_index_reduce_callback, (void*)&udata);

		udata.tree = partition->sub_vp;
		udata.is_ldt_sub = true;
		as_index_reduce_sync(partition->sub_vp, ssd_resume_index_reduce_callback, (void*)&udata);
	}
}
