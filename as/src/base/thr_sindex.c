/*
 * thr_sindex.c
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
 * SYNOPSIS
 * This file implements supporting threads for the secondary index implementation.
 * Currently following two main threads are implemented here
 *
 * -  Secondary index defrag thread which walks sweeps through secondary indexes
 *   and cleanup the stale entries by looking up digest in the primary index.
 *
 * -  Secondary index thread which cleans up secondary index entry for a particular
 *    partitions
 *
 */

#include "base/thr_sindex.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "ai_obj.h"
#include "ai_btree.h"
#include "fault.h"
#include "hist.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/job_manager.h"
#include "base/monitor.h"
#include "base/secondary_index.h"

int as_spop_populate(as_sindex* si);

#define RELEASE_ITERATORS(icol) \
do {                                \
	init_ai_obj(&i_col);             \
	n_offset = 0;                   \
} while(0);


// All this is global because Aerospike Index is single threaded
pthread_rwlock_t g_sindex_rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t g_ai_rwlock     = PTHREAD_RWLOCK_INITIALIZER;
pthread_t g_sindex_populate_th;
pthread_t g_sindex_destroy_th;
pthread_t g_sindex_defrag_th;
cf_queue *g_sindex_populate_q;
cf_queue *g_sindex_destroy_q;
cf_queue *g_sindex_populateall_done_q;
cf_queue *g_q_objs_to_defrag;
bool      g_sindex_boot_done;

int
ll_sindex_gc_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
as_sindex_gc_release_defrag_arr_to_queue(void *v)
{
	objs_to_defrag_arr *dt = (objs_to_defrag_arr *)v;
	if (cf_queue_sz(g_q_objs_to_defrag) < SINDEX_GC_QUEUE_HIGHWATER) {
		cf_queue_push(g_q_objs_to_defrag, &dt);
	} 
	else {
		cf_free(dt);
	}
}

void
ll_sindex_gc_destroy_fn(cf_ll_element *ele)
{
	ll_sindex_gc_element * node = (ll_sindex_gc_element *) ele;
	if (node) {
		as_sindex_gc_release_defrag_arr_to_queue((void *)(node->objs_to_defrag));	
		cf_free(node);
	}
}

objs_to_defrag_arr *
as_sindex_gc_get_defrag_arr(void)
{
	objs_to_defrag_arr *dt;
	if (cf_queue_pop(g_q_objs_to_defrag, &dt, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		dt = cf_malloc(sizeof(objs_to_defrag_arr));
	}
	dt->num = 0;
	return dt;
}

void
as_sindex_gc_histogram_dumpall()
{
	if (g_config.sindex_gc_enable_histogram == false) {
		return;
	}
	
	if (g_config._sindex_gc_validate_obj_hist) {
		histogram_dump(g_config._sindex_gc_validate_obj_hist);
	}
	if (g_config._sindex_gc_delete_obj_hist) {
		histogram_dump(g_config._sindex_gc_delete_obj_hist);
	}
	if (g_config._sindex_gc_pimd_rlock_hist) {
		histogram_dump(g_config._sindex_gc_pimd_rlock_hist);
	}
	if (g_config._sindex_gc_pimd_wlock_hist) {
		histogram_dump(g_config._sindex_gc_pimd_wlock_hist);
	}
	
	return;
}

// Main thread which looks at the request of the populating index
void *
as_sindex__populate_fn(void *param)
{
	while(1) {
		as_sindex *si;
		cf_queue_pop(g_sindex_populate_q, &si, CF_QUEUE_FOREVER);
		if (si->flag & AS_SINDEX_FLAG_POPULATING) {
			// Earlier scan to populate index is still going on, push it back
			// into the queue to look at it later. this is problem only when
			// there are multiple populating threads currently there is only 1.
			cf_queue_push(g_sindex_populate_q, &si);
		} else {
			cf_debug(AS_SINDEX, "Populating index %s", si->imd->iname);
			si->flag |= AS_SINDEX_FLAG_POPULATING;
			as_spop_populate(si);
		}
	}
	return NULL;
}


// Main thread which looks at the request of the destroy of index
void *
as_sindex__destroy_fn(void *param)
{
	while(1) {
		as_sindex *si;
		cf_queue_pop(g_sindex_destroy_q, &si, CF_QUEUE_FOREVER);

		SINDEX_GWLOCK();
		cf_assert((si->state == AS_SINDEX_DESTROY),
				AS_SINDEX, CF_CRITICAL, " Invalid state %d at cleanup expected %d for %p and %s", si->state, AS_SINDEX_DESTROY, si, (si) ? ((si->imd) ? si->imd->iname : NULL) : NULL);
		int rv = as_sindex__delete_from_set_binid_hash(si->ns, si->imd);
		if (rv) {
			cf_warning(AS_SINDEX, "Delete from set_binid hash fails with error %d", rv);
		}
		SINDEX_WLOCK(&si->imd->slock);
		ai_btree_destroy(si->imd);
		// Free entire usage counter for this index after the destroy
		// code... alc code does not do it.
		as_sindex_release_data_memory(si->imd, si->data_memory_used);
		as_sindex_destroy_pmetadata(si);
		si->state = AS_SINDEX_INACTIVE;
		si->flag  = 0;
		si->ns->sindex_cnt--;
		as_sindex_metadata *imd = si->imd;
		si->imd = NULL;

		char iname[AS_ID_INAME_SZ];
		memset(iname, 0, AS_ID_INAME_SZ);
		snprintf(iname, strlen(imd->iname) + 1, "%s", imd->iname);
		shash_delete(si->ns->sindex_iname_hash, (void *)iname);

		
		si->ns      = NULL;
		si->simatch = -1;

		// remember this is going to release the write lock
		// of meta-data first. This is the only special case
		// where both GLOCK and LOCK is called together
		SINDEX_UNLOCK(&imd->slock);
		SINDEX_GUNLOCK();

		if (si->new_imd) {
			as_sindex_metadata *recreate_imd = si->new_imd;
			as_sindex_update(recreate_imd);
			si->new_imd = NULL;
		}

		as_sindex_imd_free(imd);
		cf_rc_free(imd);
	}
	return NULL;
}

void
as_sindex_update_defrag_stat(as_sindex *si, uint32_t r, uint64_t start_time_ms)
{
	cf_atomic64_add(&si->stats.n_deletes,        r);
	cf_atomic64_add(&si->stats.n_objects,        -r);
	cf_atomic64_add(&si->stats.n_defrag_records, r);
	cf_atomic64_add(&si->stats.defrag_time, cf_getms() - start_time_ms);
}

/*
 * Core of sindex defragic logic.
 * Determines which pimd needs to be defragged
 * Reserves, build si and build pimd
 * Returns :
 * 		 0  - Success
 * 		-1  - go to next si
 * 		-2  - go to next ns
 * Notes-
 * 		Caller needs to release the ref count of sindex(si)
*/
int
as_sindex_get_pimd_to_defrag(as_namespace *ns, int *si_index, int *p_index, as_sindex_pmetadata** pimd,
		as_sindex ** sindex, int *si_defraged)
{
	if (*p_index >= NUM_SINDEX_PARTITIONS) {
		// pimd reaches max limit. Switch to next si.
		*p_index = 0;
		(*si_index)++;
		(*si_defraged)++;
	}

	// This check may result in skipping of some indexes.
	// i.e When a sindex is created/dropped while the defrag is running on other sindexes of same namespace.
	// They will be covered in next iteration. Overall its a performance gain.
	if (*si_index >= AS_SINDEX_MAX || *si_defraged >= ns->sindex_cnt) {
		return -2;
	}

	SINDEX_GRLOCK();
	as_sindex * si = &ns->sindex[*si_index];

	if (!si) {
		SINDEX_GUNLOCK();
		cf_warning(AS_SINDEX, "Allocated sindex was found as null.");
		return -2;
	}
	if (si->state != AS_SINDEX_ACTIVE) {
		// Skip to next sindex in the same namespace
		SINDEX_GUNLOCK();
		*si_index = *si_index + 1;
		*p_index  = 0;
		return -1;
	}

	AS_SINDEX_RESERVE(si);
	SINDEX_GUNLOCK();
	as_sindex_metadata *  imd = si->imd;
	*pimd                     = &imd->pimd[*p_index];
	*sindex                   = si;
	cf_detail(AS_SINDEX, "Defragging pimd %d of sindex %s on namespace %s and set %s",
			*p_index, si->imd->iname, si->imd->ns_name, si->imd->set);
	return 0;
}

/*
 * This thread/function continually runs over an secondary index to clean
 * up the unnecessary/expired digests.
 * If the data is on disk when record is deleted to avoid reading from the disk,
 * the delete from the secondary index is not done inline.
 *
 * Note: If the record comes back after being deleted on an on-disk namespace, there is a probability
 * that there was not enough time to defrag that. TODO -- FIX IT
 *
 * Flow :
 * 				GET PIMD ----> BUILD DEFRAG_LIST
 *				   /|\              |
 * 					|				|
 *					|			   \|/
 *				  DEFRAG THE DEFRAG_LIST
 *
 * Controlling parameters   : Modifiable through clinfo
 * 1. defrag_max_units      - Max units it will defrag in one iteration(i.e before sleeping) default -- 1000 units
 * 2. defrag_period(ms)     - Minimum time delay between two iteration of sindex defrag.     default -- 1    msec
 *
 * Takes a lock on pimd while defragging
 * TODO : Aerospike Index layer is probably doing a lot of mallocs and copy.
 *          It can be avoided.
 */
void *
as_sindex__defrag_fn(void *udata)
{
	cf_debug(AS_SINDEX, "Secondary index defrag thread started !!");
	g_config.sindex_gc_enable_histogram = false;

	char hist_name[64];
	sprintf(hist_name, "sindex_gc_validate_obj");
	if (NULL == ( g_config._sindex_gc_validate_obj_hist = histogram_create(hist_name, HIST_MILLISECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex gc histogram (validate_obj_hist)");

	sprintf(hist_name, "sindex_gc_delete_obj");
	if (NULL == ( g_config._sindex_gc_delete_obj_hist = histogram_create(hist_name, HIST_MILLISECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex gc histogram (validate_obj_hist)");

	sprintf(hist_name, "sindex_gc_pimd_rlock_hist");
	if (NULL == ( g_config._sindex_gc_pimd_rlock_hist = histogram_create(hist_name, HIST_MILLISECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex gc histogram (pimd_rlock)");

	sprintf(hist_name, "sindex_gc_pimd_wlock_hist");
	if (NULL == ( g_config._sindex_gc_pimd_wlock_hist = histogram_create(hist_name, HIST_MILLISECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex gc histogram (pimd_wlock)");

	while (!g_sindex_boot_done) {
		sleep(10);
		continue;
	}

	uint16_t ns_id = 0;
	while (true) {
		as_namespace *ns = g_config.namespace[ns_id];
		if (!ns || (ns->sindex_cnt == 0)) {
			goto next_ns;
		}

		uint64_t      last_time        = cf_getms();
		uint64_t      curr_time        = 0;
		int           si_index         = 0;
		int           p_index          = 0;
		ai_obj        i_col;                                      // Numeric type sindexes iterator
		init_ai_obj(&i_col);
		long          n_offset         = 0;
		int           sindex_defraged  = 0;
		long          defrag_period    = 0;
		long          limit            = 0;
		// loop through all sindexes and pimds to defrag
		while (1) {
			// Sleep for remainder of defrag period
			curr_time                        = cf_getms();
			uint64_t diff                    = curr_time - last_time;
			g_config.sindex_gc_activity_dur += diff;
			if (diff < defrag_period) {
				g_config.sindex_gc_inactivity_dur += (defrag_period - diff);
				usleep(1000 * (defrag_period - diff));
			}
			last_time = cf_getms();

			// Get pimd to defrag..
			as_sindex           * si;
			as_sindex_pmetadata * pimd;
			int retval     = as_sindex_get_pimd_to_defrag(ns, &si_index, &p_index, &pimd, &si, &sindex_defraged);
			if (retval != 0) {
				if (retval == -1) {
					// To avoid cases in which a sindex is dropped in middle of defragging
					RELEASE_ITERATORS(icol)
					continue;
				}
				if (retval == -2) {
					break;
				}
			}

			if (!pimd || !si) {
				break;
			}
			limit          = (long)si->config.defrag_max_units;
			defrag_period  = (long)si->config.defrag_period;

			// This can be use to control the defrag thread.
			// Setting defrag_max_units as 0 can allow a user
			// to stop defragging of a sindex
			if( limit <= 0 ) {
				si_index++;
				sindex_defraged++;
				p_index = 0;
				RELEASE_ITERATORS(icol)
				AS_SINDEX_RELEASE(si);
				continue;
			}

			uint64_t start_time         = 0;
			uint64_t pimd_rlock_time_ns = 0;
			uint64_t processed          = 0;
			uint64_t found              = 0;
			// Create Defrag List
			start_time                  = cf_getms();
			cf_ll defrag_list;
			cf_ll_init(&defrag_list, &ll_sindex_gc_destroy_fn, false);

			int ret = 0;
			int limit_per_iteration = limit > 100 ? 100 : limit;
			for (int i = 0; i < limit; i += limit_per_iteration) {
				SINDEX_RLOCK(&pimd->slock);
				SET_TIME_FOR_SINDEX_GC_HIST(pimd_rlock_time_ns);
				ret  = ai_btree_build_defrag_list(si->imd, pimd, &i_col, &n_offset, limit_per_iteration, &processed, &found, &defrag_list);	
				SINDEX_GC_HIST_INSERT_DATA_POINT(sindex_gc_pimd_rlock_hist, pimd_rlock_time_ns);
				SINDEX_UNLOCK(&pimd->slock);
				pimd_rlock_time_ns = 0;
				if (ret != AS_SINDEX_CONTINUE) {
					break;
				}
			}

			g_config.sindex_gc_list_creation_time    += (cf_getms() - start_time);
			g_config.sindex_gc_objects_validated     += processed;
			g_config.sindex_gc_garbage_found         += found;
			int listsize                              = cf_ll_size(&defrag_list);

			// Run Defrag..
			uint64_t deleted = 0;
			start_time = cf_getms();
			if ( (ret != AS_SINDEX_ERR ) && (listsize > 0) ) {
				ulong    wl_lim             = 10;
				uint64_t start_time         = cf_getms();
				uint64_t pimd_wlock_time_ns = 0; 
				bool     more               = true;
				while (more) {
					SINDEX_WLOCK(&pimd->slock);
					SET_TIME_FOR_SINDEX_GC_HIST(pimd_wlock_time_ns);
					more = ai_btree_defrag_list(si->imd, pimd, &defrag_list, wl_lim, &deleted);
					SINDEX_GC_HIST_INSERT_DATA_POINT(sindex_gc_pimd_wlock_hist, pimd_wlock_time_ns);
					SINDEX_UNLOCK(&pimd->slock);
					pimd_wlock_time_ns = 0;
				}
				cf_detail(AS_SINDEX, "Deleted %d units of attempted %d units from index %s", listsize, limit, si->imd->iname);
				as_sindex_update_defrag_stat(si, deleted, start_time);	
			}

			// Release list
			cf_ll_reduce(&defrag_list, true /*forward*/, ll_sindex_gc_reduce_fn, NULL);
			if ((ret == AS_SINDEX_DONE) || (ret == AS_SINDEX_ERR)) {
				RELEASE_ITERATORS(icol)
				p_index++;
			}
			g_config.sindex_gc_list_deletion_time += (cf_getms() - start_time);
			g_config.sindex_gc_garbage_cleaned    += deleted;

			AS_SINDEX_RELEASE(si);
		}
next_ns:
		sleep(1);
		ns_id = (ns_id + 1) % g_config.namespaces;
	}
	return(0);
}


/*
 * Secondary index main defrag thread, it keeps watching out for request to
 * the defrag, Client API to set up aerospike facing meta data for the secondary index
 * and setting all the initial things
 *
 * Parameter:
 *		 sindex_metadata:  (in/out) Index meta-data structure
 *
 * Caller:
 *		aerospike
 * Return:
 *		0: On success
 *		-1: On failure
 * Synchronization:
 * 		Acquires the meta lock.
 */
void
as_sindex_thr_init()
{
	// Thread request read lock on this recursively could possibly cause deadlock. Caller
	// should be careful with that
	pthread_rwlockattr_t rwattr;
	if (!g_q_objs_to_defrag) {
		g_q_objs_to_defrag = cf_queue_create(sizeof(void *), true);
	}
	if (0 != pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_SINDEX, "pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (0 != pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash( AS_SINDEX, "pthread_rwlockattr_setkind_np: %s", cf_strerror(errno));

	// Aerospike Index Metadata lock
	if (0 != pthread_rwlock_init(&g_ai_rwlock, &rwattr)) {
		cf_crash(AS_SINDEX, " Could not create secondary index ddl mutex ");
	}

	// Sindex Metadata lock
	if (0 != pthread_rwlock_init(&g_sindex_rwlock, &rwattr)) {
		cf_crash(AS_SINDEX, " Could not create secondary index ddl mutex ");
	}

	g_sindex_populate_q = cf_queue_create(sizeof(as_sindex *), true);
	if (0 != pthread_create(&g_sindex_populate_th, 0, as_sindex__populate_fn, 0)) {
		cf_crash(AS_INDEX, " Could not create sindex populate thread ");
	}

	g_sindex_destroy_q = cf_queue_create(sizeof(as_sindex *), true);
	if (0 != pthread_create(&g_sindex_destroy_th, 0, as_sindex__destroy_fn, 0)) {
		cf_crash(AS_INDEX, " Could not create sindex destroy thread ");
	}

	if (0 != pthread_create(&g_sindex_defrag_th, 0, as_sindex__defrag_fn, 0)) {
		cf_crash(AS_INDEX, " Could not create sindex defrag thread ");
	}

	g_sindex_populateall_done_q = cf_queue_create(sizeof(int), true);
	// At the beginning it is false. It is set to true when all the sindex
	// are populated.
	g_sindex_boot_done = false;

	ai_btree_init();
}



//==============================================================================
// Secondary index populator.
//

// spop_job - derived class header:
typedef struct spop_job_s {
	// Base object must be first:
	as_job			_base;

	// Derived class data:
	as_sindex*		si;
	uint64_t		si_desync_cnt;

	cf_atomic64		n_reduced;
} spop_job;

spop_job* spop_job_create(as_namespace* ns, uint16_t set_id, as_sindex* si);

// as_job_manager instance for secondary index populator:
static as_job_manager g_spop_manager;


//------------------------------------------------
// Sindex populator public API.
//

void
as_spop_init()
{
	// TODO - check for failure and cf_crash?
	// TODO - config for max counts?
	// Initialize with maximum threads since first use is always populate-all at
	// startup. The thread pool will be down-sized right after that.
	as_job_manager_init(&g_spop_manager, MAX_POPULATOR_THREADS, 100, 100);
}

int
as_spop_populate(as_sindex* si)
{
	as_sindex_metadata *imd = si->imd;
	as_namespace *ns = as_namespace_get_byname(imd->ns_name);

	if (! ns) {
		cf_warning(AS_SINDEX, "sindex populate %s ns %s - unrecognized namespace", imd->iname, imd->ns_name);
		as_sindex_populate_done(si);
		AS_SINDEX_RELEASE(si);
		return -1;
	}

	uint16_t set_id = INVALID_SET_ID;

	if (imd->set && (set_id = as_namespace_get_set_id(ns, imd->set)) == INVALID_SET_ID) {
		cf_warning(AS_SCAN, "sindex populate %s ns %s - set %s not found", imd->iname, imd->ns_name, imd->set);
		as_sindex_populate_done(si);
		AS_SINDEX_RELEASE(si);
		return -3;
	}

	spop_job* job = spop_job_create(ns, set_id, si);

	if (! job) {
		cf_warning(AS_SCAN, "sindex populate %s ns %s set %s - job alloc failed", imd->iname, imd->ns_name, imd->set);
		as_sindex_populate_done(si);
		AS_SINDEX_RELEASE(si);
		return -2;
	}

	// Can't fail for this kind of job.
	as_job_manager_start_job(&g_spop_manager, (as_job*)job);

	return 0;
}

int
as_spop_populate_all(as_namespace* ns)
{
	spop_job* job = spop_job_create(ns, INVALID_SET_ID, NULL);

	if (! job) {
		cf_warning(AS_SCAN, "sindex populate-all ns %s - job alloc failed", ns->name);
		return -2;
	}

	// Can't fail for this kind of job.
	as_job_manager_start_job(&g_spop_manager, (as_job*)job);

	return 0;
}

void
as_spop_resize_thread_pool(uint32_t n_threads)
{
	as_job_manager_resize_thread_pool(&g_spop_manager, n_threads);
}


//------------------------------------------------
// spop_job derived class implementation.
//

void spop_job_slice(as_job* _job, as_partition_reservation* rsv);
void spop_job_finish(as_job* _job);
void spop_job_destroy(as_job* _job);
void spop_job_info(as_job* _job, as_mon_jobstat* stat);

const as_job_vtable spop_job_vtable = {
		spop_job_slice,
		spop_job_finish,
		spop_job_destroy,
		spop_job_info
};

void spop_job_reduce_cb(as_index_ref* r_ref, void* udata);

//
// spop_job creation.
//

spop_job*
spop_job_create(as_namespace* ns, uint16_t set_id, as_sindex* si)
{
	spop_job* job = cf_malloc(sizeof(spop_job));

	if (! job) {
		return NULL;
	}

	as_job_init((as_job*)job, &spop_job_vtable, &g_spop_manager,
			RSV_MIGRATE, 0, ns, set_id, AS_JOB_PRIORITY_MEDIUM);

	job->si = si;
	job->si_desync_cnt = si->desync_cnt;
	job->n_reduced = 0;

	return job;
}

//
// spop_job mandatory scan_job interface.
//

void
spop_job_slice(as_job* _job, as_partition_reservation* rsv)
{
	as_index_reduce(rsv->p->vp, spop_job_reduce_cb, (void*)_job);
}

void
spop_job_finish(as_job* _job)
{
	spop_job* job = (spop_job*)_job;

	as_sindex_ticker_done(_job->ns, job->si, _job->start_ms);

	if (job->si) {
		as_sindex_populate_done(job->si);
		job->si->stats.loadtime = cf_getms() - _job->start_ms;
		AS_SINDEX_RELEASE(job->si);
	}
	else {
		as_sindex_boot_populateall_done(_job->ns);
	}
}

void
spop_job_destroy(as_job* _job)
{
}

void
spop_job_info(as_job* _job, as_mon_jobstat* stat)
{
	strcat(stat->jdata, ":job-type=sindex-populator");
}

//
// spop_job utilities.
//

void
spop_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_job* _job = (as_job*)udata;
	spop_job* job = (spop_job*)_job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	if (job->si) {
		if (! as_sindex_isactive(job->si) || job->si->desync_cnt > job->si_desync_cnt) {
			as_record_done(r_ref, ns);
			as_job_manager_abandon_job(_job->mgr, _job, AS_PROTO_RESULT_FAIL_UNKNOWN);
			return;
		}

		cf_atomic64_decr(&job->si->stats.recs_pending);
	}

	as_sindex_ticker(ns, job->si, cf_atomic64_incr(&job->n_reduced), _job->start_ms);

	as_index *r = r_ref->r;

	if (_job->set_id != INVALID_SET_ID && as_index_get_set_id(r) != _job->set_id) {
		as_record_done(r_ref, ns);
		return;
	}

	if (r->void_time != 0 && r->void_time < as_record_void_time_get()) {
		as_record_done(r_ref, ns);
		return;
	}

	as_storage_rd rd;
	as_storage_record_open(ns, r, &rd, &r->key);
	rd.n_bins = as_bin_get_n_bins(r, &rd);
	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];
	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	if (job->si) {
		SINDEX_GRLOCK();
		AS_SINDEX_RESERVE(job->si);
		SINDEX_GUNLOCK();
		as_sindex_put_rd(job->si, &rd);
	}
	else {
		as_sindex_putall_rd(ns, &rd);
	}

	as_storage_record_close(r, &rd);
	as_record_done(r_ref, ns);

	cf_atomic64_incr(&_job->n_records_read);
}
