/*
 * thr_nsup.c
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
 * namespace supervisor
 */

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/param.h> // for MIN and MAX

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"
#include "hist.h"
#include "queue.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "storage/storage.h"


//==========================================================
// Globals.
//

pthread_t g_nsup_thread;


//==========================================================
// Helpers used by both cold-start and runtime eviction.
//

//------------------------------------------------
// Return true a specified fraction of the time.
//
static inline bool
random_delete(uint32_t tenths_pct)
{
	return (rand() % 1000) < tenths_pct;
}

//------------------------------------------------
// Convert void-time to TTL, handling edge cases.
//
static inline uint32_t
void_time_to_ttl(uint32_t void_time, uint32_t now)
{
	return void_time == 0xFFFFffff ? 0xFFFFffff : (void_time > now ? void_time - now : 0);
}


//==========================================================
// Eviction during cold-start.
//
// No real need for this to be in thr_nsup.c, except maybe
// for convenient comparison to run-time eviction.
//

#define NUM_EVICT_THREADS 24
#define EVAL_WRITE_STATE_FREQUENCY 1024


//------------------------------------------------
// Reduce callback prepares for cold-start eviction.
// - builds cold-start eviction histogram
//
typedef struct cold_start_evict_prep_info_s {
	as_namespace*		ns;
	linear_histogram*	hist;
} cold_start_evict_prep_info;

static void
cold_start_evict_prep_reduce_cb(as_index_ref* r_ref, void* udata)
{
	cold_start_evict_prep_info* p_info = (cold_start_evict_prep_info*)udata;
	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		linear_histogram_insert_data_point(p_info->hist, void_time);
	}

	as_record_done(r_ref, p_info->ns);
}

//------------------------------------------------
// Threads prepare for cold-start eviction.
//
typedef struct evict_prep_thread_info_s {
	as_namespace*		ns;
	cf_atomic32			pid;
	linear_histogram*	hist;
} evict_prep_thread_info;

void*
run_cold_start_evict_prep(void* udata)
{
	evict_prep_thread_info* p_info = (evict_prep_thread_info*)udata;
	as_namespace *ns = p_info->ns;

	cold_start_evict_prep_info cb_info;

	cb_info.ns = ns;
	cb_info.hist = p_info->hist;

	int pid;

	while ((pid = (int)cf_atomic32_incr(&p_info->pid)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup.
		as_index_reduce(ns->partitions[pid].vp, cold_start_evict_prep_reduce_cb, &cb_info);
	}

	return NULL;
}

//------------------------------------------------
// Reduce callback evicts records on cold-start.
// - evicts based on calculated threshold
//
typedef struct cold_start_evict_info_s {
	as_namespace*	ns;
	as_partition*	p_partition;
	uint32_t		high_void_time;
	uint32_t		mid_tenths_pct;
	uint32_t		num_evicted;
	uint32_t		num_0_void_time;
} cold_start_evict_info;

static void
cold_start_evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	cold_start_evict_info* p_info = (cold_start_evict_info*)udata;
	as_namespace* ns = p_info->ns;
	as_partition* p_partition = p_info->p_partition;
	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		if (void_time < ns->cold_start_threshold_void_time ||
				(void_time < p_info->high_void_time && random_delete(p_info->mid_tenths_pct))) {
			// TODO - should be done as part of as_record_destroy().
			if (ns->storage_data_in_memory) {
				as_storage_rd rd;

				rd.r = r_ref->r;
				rd.ns = ns;
				rd.n_bins = as_bin_get_n_bins(r_ref->r, &rd);
				rd.bins = as_bin_get_all(r_ref->r, &rd, NULL);

				cf_atomic_int_sub(&p_partition->n_bytes_memory, as_storage_record_get_n_bytes_memory(&rd));
			}

			as_index_delete(p_partition->vp, &r_ref->r->key);
			p_info->num_evicted++;
		}
	}
	else {
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Threads do cold-start eviction.
//
typedef struct evict_thread_info_s {
	as_namespace*	ns;
	cf_atomic32		pid;
	uint32_t		high_void_time;
	uint32_t		mid_tenths_pct;
	cf_atomic32		total_evicted;
	cf_atomic32		total_0_void_time;
} evict_thread_info;

void*
run_cold_start_evict(void* udata)
{
	evict_thread_info* p_info = (evict_thread_info*)udata;
	as_namespace* ns = p_info->ns;

	cold_start_evict_info cb_info;

	memset(&cb_info, 0, sizeof(cb_info));
	cb_info.ns = ns;
	cb_info.high_void_time = p_info->high_void_time;
	cb_info.mid_tenths_pct = p_info->mid_tenths_pct;

	int pid;

	while ((pid = (int)cf_atomic32_incr(&p_info->pid)) < AS_PARTITIONS) {
		// Don't bother with partition reservations - it's startup.
		as_partition* p_partition = &ns->partitions[pid];

		cb_info.p_partition = p_partition;
		as_index_reduce(p_partition->vp, cold_start_evict_reduce_cb, &cb_info);
	}

	cf_atomic32_add(&p_info->total_evicted, cb_info.num_evicted);
	cf_atomic32_add(&p_info->total_0_void_time, cb_info.num_0_void_time);

	return NULL;
}

//------------------------------------------------
// Get the cold-start histogram's TTL range.
//
static uint64_t
get_cold_start_ttl_range(as_namespace* ns, uint32_t now)
{
	uint64_t max_void_time = 0;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		uint64_t partition_max_void_time = cf_atomic_int_get(ns->partitions[n].max_void_time);

		if (partition_max_void_time > max_void_time) {
			max_void_time = partition_max_void_time;
		}
	}

	// If configuration specifies max-ttl, use that to cap the namespace
	// maximum void-time.

	if (ns->max_ttl > 0) {
		uint64_t cap = ns->max_ttl + now;

		if (max_void_time > cap) {
			max_void_time = cap;
		}
	}

	// Convert to TTL - used for cold-start histogram range.
	return max_void_time > now ? max_void_time - now : 0;
}

//------------------------------------------------
// Set cold-start eviction thresholds.
//
static void
set_cold_start_thresholds(as_namespace* ns, linear_histogram* hist, uint32_t* p_high_void_time, uint32_t* p_mid_tenths_pct)
{
	uint64_t low_void_time;
	uint64_t high_void_time;
	bool is_last = linear_histogram_get_thresholds_for_fraction(hist, ns->evict_tenths_pct, &low_void_time, &high_void_time, p_mid_tenths_pct);

	if (high_void_time == 0) {
		cf_warning(AS_NSUP, "{%s} cold-start can't evict %s", ns->name, is_last ? "everything" : "- no records eligible");
	}
	else if (is_last) {
		cf_info(AS_NSUP, "{%s} cold-start evicting into last bucket: using infinity for high void-time %u", ns->name, (uint32_t)high_void_time);
	}

	// See comment in get_thresholds() about evicting from last bucket.

	// TODO - consider only using random deletion if it's the last bucket, i.e.
	// set low_void_time to high_void_time unless it's the last bucket.

	cf_atomic32_set(&ns->cold_start_threshold_void_time, (uint32_t)low_void_time);
	*p_high_void_time = is_last ? 0xFFFFffff : (uint32_t)high_void_time;
}

//------------------------------------------------
// Cold-start eviction, called by drv_ssd.c.
// Returns false if a serious problem occurred and
// we can't proceed.
//
bool
as_cold_start_evict_if_needed(as_namespace* ns)
{
	pthread_mutex_lock(&ns->cold_start_evict_lock);

	// Only go further than here every thousand record add attempts.
	if (ns->cold_start_record_add_count++ % EVAL_WRITE_STATE_FREQUENCY != 0) {
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return true;
	}

	uint32_t now = as_record_void_time_get();

	// Update threshold void-time if we're past it.
	if (now > cf_atomic32_get(ns->cold_start_threshold_void_time)) {
		cf_atomic32_set(&ns->cold_start_threshold_void_time, now);
	}

	// Evaluate whether or not we need to evict.
	bool hwm_breached = false, stop_writes = false;

	as_namespace_eval_write_state(ns, &hwm_breached, &stop_writes);

	// Are we out of control?
	if (stop_writes) {
		cf_warning(AS_NSUP, "{%s} hit stop-writes limit", ns->name);
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return false;
	}

	// If we don't need to evict, we're done.
	if (! hwm_breached) {
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return true;
	}

	// We want to evict, but are we allowed to do so?
	if (! g_config.nsup_startup_evict) {
		cf_warning(AS_NSUP, "{%s} hwm breached but not allowed to evict", ns->name);
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return false;
	}

	// We may evict - set up the cold-start eviction histogram.
	cf_info(AS_NSUP, "{%s} cold-start building eviction histogram ...", ns->name);

	uint64_t ttl_range = get_cold_start_ttl_range(ns, now);

	char hist_name[HISTOGRAM_NAME_SIZE];

	sprintf(hist_name, "%s cold-start evict histogram", ns->name);
	linear_histogram* cold_start_evict_hist = linear_histogram_create(hist_name, now, ttl_range, EVICTION_HIST_NUM_BUCKETS);

	// Split these tasks across multiple threads.
	pthread_t evict_threads[NUM_EVICT_THREADS];

	// Reduce all partitions to build the eviction histogram.
	evict_prep_thread_info prep_thread_info;

	prep_thread_info.ns = ns;
	prep_thread_info.pid = -1;
	prep_thread_info.hist = cold_start_evict_hist;

	for (int n = 0; n < NUM_EVICT_THREADS; n++) {
		if (pthread_create(&evict_threads[n], NULL, run_cold_start_evict_prep, (void*)&prep_thread_info) != 0) {
			cf_warning(AS_NSUP, "{%s} failed to create evict-prep thread %d", ns->name, n);
		}
	}

	for (int n = 0; n < NUM_EVICT_THREADS; n++) {
		pthread_join(evict_threads[n], NULL);
	}
	// Now we're single-threaded again.

	evict_thread_info thread_info;

	memset(&thread_info, 0, sizeof(thread_info));
	thread_info.ns = ns;
	thread_info.pid = -1;

	// Calculate the eviction thresholds.
	set_cold_start_thresholds(ns, cold_start_evict_hist, &thread_info.high_void_time, &thread_info.mid_tenths_pct);

	cf_info(AS_NSUP, "{%s} cold-start evict ttls %u,%d,0.%03u", ns->name,
			void_time_to_ttl(cf_atomic32_get(ns->cold_start_threshold_void_time), now), void_time_to_ttl(thread_info.high_void_time, now), thread_info.mid_tenths_pct);

	linear_histogram_dump(cold_start_evict_hist);
	linear_histogram_destroy(cold_start_evict_hist);

	srand(time(NULL));

	// Reduce all partitions to evict based on the thresholds.
	for (int n = 0; n < NUM_EVICT_THREADS; n++) {
		if (pthread_create(&evict_threads[n], NULL, run_cold_start_evict, (void*)&thread_info) != 0) {
			cf_warning(AS_NSUP, "{%s} failed to create evict thread %d", ns->name, n);
		}
	}

	for (int n = 0; n < NUM_EVICT_THREADS; n++) {
		pthread_join(evict_threads[n], NULL);
	}
	// Now we're single-threaded again.

	cf_info(AS_NSUP, "{%s} cold-start evicted %u records, found %u 0-void-time records", ns->name, thread_info.total_evicted, thread_info.total_0_void_time);

	// TODO - do we really need this?
	if (thread_info.total_evicted == 0) {
		cf_warning(AS_NSUP, "{%s} could not evict any records", ns->name);
		pthread_mutex_unlock(&ns->cold_start_evict_lock);
		return false;
	}

	pthread_mutex_unlock(&ns->cold_start_evict_lock);
	return true;
}

//
// END - Eviction during cold-start.
//==========================================================

//==========================================================
// Temporary dangling prole garbage collection.
//

typedef struct garbage_collect_info_s {
	as_namespace*	ns;
	as_index_tree*	p_tree;
	uint32_t		now;
	uint32_t		num_deleted;
} garbage_collect_info;

static void
garbage_collect_reduce_cb(as_index_ref* r_ref, void* udata)
{
	garbage_collect_info* p_info = (garbage_collect_info*)udata;
	uint32_t void_time = r_ref->r->void_time;

	// If we're past void-time plus safety margin, delete the record.
	if (void_time != 0 && p_info->now > void_time + g_config.prole_extra_ttl) {
		as_index_delete(p_info->p_tree, &r_ref->r->key);
		p_info->num_deleted++;
	}

	as_record_done(r_ref, p_info->ns);
}

static int
garbage_collect_next_prole_partition(as_namespace* ns, int pid)
{
	as_partition_reservation rsv;
	AS_PARTITION_RESERVATION_INIT(rsv);

	// Look for the next prole partition past pid, but loop only once over all
	// partitions.
	for (int n = 0; n < AS_PARTITIONS; n++) {
		// Increment pid and wrap if necessary.
		if (++pid == AS_PARTITIONS) {
			pid = 0;
		}

		// Note - may want a new method to get these under a single partition
		// lock, but for now just do the two separate reserve calls.
		if (0 == as_partition_reserve_write(ns, pid, &rsv, 0, 0)) {
			// This is a master partition - continue.
			as_partition_release(&rsv);
			AS_PARTITION_RESERVATION_INIT(rsv);
		}
		else if (0 == as_partition_reserve_read(ns, pid, &rsv, 0, 0)) {
			// This is a prole partition - garbage collect and break.
			cf_atomic_int_incr(&g_config.nsup_tree_count);

			garbage_collect_info cb_info;

			cb_info.ns = ns;
			cb_info.p_tree = rsv.p->vp;
			cb_info.now = as_record_void_time_get();
			cb_info.num_deleted = 0;

			// Reduce the partition, deleting long-expired records.
			as_index_reduce(rsv.p->vp, garbage_collect_reduce_cb, &cb_info);

			if (cb_info.num_deleted != 0) {
				cf_info(AS_NSUP, "namespace %s pid %d: %u expired proles",
						ns->name, pid, cb_info.num_deleted);
			}

			as_partition_release(&rsv);
			cf_atomic_int_decr(&g_config.nsup_tree_count);

			// Do only one partition per nsup loop.
			break;
		}
	}

	return pid;
}

//
// END - Temporary dangling prole garbage collection.
//==========================================================


#define EVICT_HIST_TTL_RANGE (3600 * 24 * 5) // 5 days

static cf_queue* g_p_nsup_delete_q = NULL;
static pthread_t g_nsup_delete_thread;
static pthread_t g_ldt_sub_gc_thread;

int
as_nsup_queue_get_size()
{
	return g_p_nsup_delete_q ? cf_queue_sz(g_p_nsup_delete_q) : 0;
}

// Make sure a huge nsup deletion wave won't blow delete queue up.
#define DELETE_Q_SAFETY_THRESHOLD	10000
#define DELETE_Q_SAFETY_SLEEP_us	1000 // 1 millisecond
#define LDT_SUB_GC_SAFETY_SLEEP_us  1000

// Wait for delete queue to clear.
#define DELETE_Q_CLEAR_SLEEP_us		1000 // 1 millisecond

typedef struct record_delete_info_s {
	as_namespace*	ns;
	cf_digest		digest;
} record_delete_info;


//------------------------------------------------
// Run thread to handle delete queue.
//
void*
run_nsup_delete(void* pv_data)
{
	while (true) {
		record_delete_info q_item;

		if (CF_QUEUE_OK != cf_queue_pop(g_p_nsup_delete_q, (void*)&q_item, CF_QUEUE_FOREVER)) {
			cf_crash(AS_NSUP, "nsup delete queue pop failed");
		}

		// Generate a delete transaction for this digest, and hand it to tsvc.

		size_t sz = sizeof(cl_msg);
		size_t ns_name_len = strlen(q_item.ns->name);

		sz += sizeof(as_msg_field) + ns_name_len;
		sz += sizeof(as_msg_field) + sizeof(cf_digest);

		cl_msg* msgp = cf_malloc(sz + 32); // TODO - remove the extra 32

		cf_assert(msgp, AS_NSUP, CF_CRITICAL, "malloc failed: %s", cf_strerror(errno));

		msgp->proto.version = PROTO_VERSION;
		msgp->proto.type = PROTO_TYPE_AS_MSG;
		msgp->proto.sz = sz - sizeof(as_proto);
		msgp->msg.header_sz = sizeof(as_msg);
		msgp->msg.info1 = 0;
		msgp->msg.info2 = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE;
		msgp->msg.info3 = 0;
		msgp->msg.unused = 0;
		msgp->msg.generation = 0;
		msgp->msg.record_ttl = 0;
		msgp->msg.transaction_ttl = 0;
		msgp->msg.n_fields = 2;
		msgp->msg.n_ops = 0;

		uint8_t* buf = msgp->msg.data;
		as_msg_field* fp;

		fp = (as_msg_field*)buf;
		fp->type = AS_MSG_FIELD_TYPE_NAMESPACE;
		fp->field_sz = 1 + ns_name_len; // 1 for the type field
		memcpy(fp->data, q_item.ns->name, ns_name_len);
		buf += sizeof(as_msg_field) + ns_name_len;

		fp = (as_msg_field*)buf;
		fp->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
		fp->field_sz = 1 + sizeof(cf_digest); // 1 for the type field
		*(cf_digest*)fp->data = q_item.digest;

		// Leave in network order. The fact that the digest is filled out means
		// it won't get swapped back.

		// INIT_TR
		as_transaction tr;
		as_transaction_init(&tr, &q_item.digest, msgp);
		tr.flag |= AS_TRANSACTION_FLAG_NSUP_DELETE;

		MICROBENCHMARK_RESET();

		if (0 != thr_tsvc_enqueue(&tr)) {
			cf_warning(AS_DEMARSHAL, "nsup failed tsvc enqueue");
			cf_free(msgp);
		}

		// Throttle - don't overwhelm tsvc queue.
		if (g_config.nsup_delete_sleep != 0) {
			usleep(g_config.nsup_delete_sleep);
		}
	}

	return NULL;
}

//------------------------------------------------
// Queue a record for deletion.
//
static void
queue_for_delete(as_namespace* ns, cf_digest* p_digest)
{
	record_delete_info q_item;

	q_item.ns = ns; // not bothering with namespace reservation
	q_item.digest = *p_digest;

	if (CF_QUEUE_OK != cf_queue_push(g_p_nsup_delete_q, (void*)&q_item)) {
		cf_crash(AS_NSUP, "nsup delete queue push failed");
	}
}

//------------------------------------------------
// Reduce callback prepares to evict sets.
// - does set deletion
// - does general expiration
// - builds set eviction & set TTL histograms
//
typedef struct sets_evict_prep_info_s {
	as_namespace*	ns;
	uint32_t		now;
	bool*			sets_deleting;
	bool*			sets_evicting;
	uint32_t		num_deleted;
	uint32_t		num_expired;
} sets_evict_prep_info;

static void
sets_evict_prep_reduce_cb(as_index_ref* r_ref, void* udata)
{
	sets_evict_prep_info* p_info = (sets_evict_prep_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t set_id = as_index_get_set_id(r_ref->r);

	if (p_info->sets_deleting[set_id]) {
		queue_for_delete(ns, &r_ref->r->key);
		p_info->num_deleted++;

		as_record_done(r_ref, ns);
		return;
	}

	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		if (p_info->now > void_time) {
			queue_for_delete(ns, &r_ref->r->key);
			p_info->num_expired++;
		}
		else if (set_id != 0) { // TODO - build no-set histograms?
			if (p_info->sets_evicting[set_id]) {
				linear_histogram_insert_data_point(ns->set_evict_hists[set_id], void_time);
			}

			if (ns->set_ttl_hists[set_id]) {
				linear_histogram_insert_data_point(ns->set_ttl_hists[set_id], void_time);
			}
		}
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback evicts sets.
// - evicts based on sets' thresholds
// - builds object size & general TTL histograms
// - counts 0-void-time records
//
typedef struct sets_evict_info_s {
	as_namespace*	ns;
	bool*			sets_evicting;
	uint32_t*		low_void_times;
	uint32_t*		high_void_times;
	uint32_t*		mid_tenths_pcts;
	uint32_t		num_evicted;
	uint32_t		num_0_void_time;
} sets_evict_info;

static void
sets_evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	sets_evict_info* p_info = (sets_evict_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t set_id = as_index_get_set_id(r_ref->r);
	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		if (p_info->sets_evicting[set_id] &&
				(void_time < p_info->low_void_times[set_id] ||
						(void_time < p_info->high_void_times[set_id] &&
								random_delete(p_info->mid_tenths_pcts[set_id])))) {
			queue_for_delete(ns, &r_ref->r->key);
			p_info->num_evicted++;
		}
		else {
			linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);
			linear_histogram_insert_data_point(ns->ttl_hist, void_time);
		}
	}
	else {
		linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback deletes sets.
// - does set deletion
// - does general expiration
// - builds object size & general TTL histograms
// - counts 0-void-time records
//
typedef struct sets_delete_info_s {
	as_namespace*	ns;
	uint32_t		now;
	bool*			sets_deleting;
	uint32_t		num_deleted;
	uint32_t		num_expired;
	uint32_t		num_0_void_time;
} sets_delete_info;

static void
sets_delete_reduce_cb(as_index_ref* r_ref, void* udata)
{
	sets_delete_info* p_info = (sets_delete_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t set_id = as_index_get_set_id(r_ref->r);

	if (p_info->sets_deleting[set_id]) {
		queue_for_delete(ns, &r_ref->r->key);
		p_info->num_deleted++;

		as_record_done(r_ref, ns);
		return;
	}

	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		if (p_info->now > void_time) {
			queue_for_delete(ns, &r_ref->r->key);
			p_info->num_expired++;
		}
		else {
			linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);
			linear_histogram_insert_data_point(ns->ttl_hist, void_time);
		}
	}
	else {
		linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback prepares for general eviction.
// - builds object size, general eviction & TTL histograms
// - counts 0-void-time records
//
typedef struct evict_prep_info_s {
	as_namespace*	ns;
	uint32_t		num_0_void_time;
} evict_prep_info;

static void
evict_prep_reduce_cb(as_index_ref* r_ref, void* udata)
{
	evict_prep_info* p_info = (evict_prep_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t void_time = r_ref->r->void_time;

	linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);

	if (void_time != 0) {
		linear_histogram_insert_data_point(ns->evict_hist, void_time);
		linear_histogram_insert_data_point(ns->ttl_hist, void_time);
	}
	else {
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback evicts records.
// - evicts based on general threshold
//
typedef struct evict_info_s {
	as_namespace*	ns;
	uint32_t		low_void_time;
	uint32_t		high_void_time;
	uint32_t		mid_tenths_pct;
	uint32_t		num_evicted;
} evict_info;

static void
evict_reduce_cb(as_index_ref* r_ref, void* udata)
{
	evict_info* p_info = (evict_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		if (void_time < p_info->low_void_time ||
				(void_time < p_info->high_void_time && random_delete(p_info->mid_tenths_pct))) {
			queue_for_delete(ns, &r_ref->r->key);
			p_info->num_evicted++;
		}
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce callback expires records.
// - does general expiration
// - builds object size & general TTL histograms
// - counts 0-void-time records
//
typedef struct expire_info_s {
	as_namespace*	ns;
	uint32_t		now;
	uint32_t		num_expired;
	uint32_t		num_0_void_time;
} expire_info;

static void
expire_reduce_cb(as_index_ref* r_ref, void* udata)
{
	expire_info* p_info = (expire_info*)udata;
	as_namespace* ns = p_info->ns;
	uint32_t void_time = r_ref->r->void_time;

	if (void_time != 0) {
		if (p_info->now > void_time) {
			queue_for_delete(ns, &r_ref->r->key);
			p_info->num_expired++;
		}
		else {
			linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);
			linear_histogram_insert_data_point(ns->ttl_hist, void_time);
		}
	}
	else {
		linear_histogram_insert_data_point(ns->obj_size_hist, r_ref->r->storage_key.ssd.n_rblocks);
		p_info->num_0_void_time++;
	}

	as_record_done(r_ref, ns);
}

//------------------------------------------------
// Reduce all master partitions, using specified
// functionality. Throttle to make sure deletions
// generated by reducing each partition don't blow
// up the delete queue.
//
static void
reduce_master_partitions(as_namespace* ns, as_index_reduce_fn cb, void* udata, uint32_t* p_n_waits, const char* tag)
{
	as_partition_reservation rsv;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		if (0 != as_partition_reserve_write(ns, n, &rsv, 0, 0)) {
			continue;
		}

		cf_atomic_int_incr(&g_config.nsup_tree_count);

		as_index_reduce(rsv.p->vp, cb, udata);

		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.nsup_tree_count);

		while (cf_queue_sz(g_p_nsup_delete_q) > DELETE_Q_SAFETY_THRESHOLD) {
			usleep(DELETE_Q_SAFETY_SLEEP_us);
			(*p_n_waits)++;
		}

		cf_debug(AS_NSUP, "{%s} %s done partition index %d, waits %u", ns->name, tag, n, *p_n_waits);
	}
}

//------------------------------------------------
// Reduce all subtrees, using specified
// functionality.
//
static void
sub_reduce_partitions(as_namespace* ns, as_index_reduce_fn cb, void* udata, uint32_t* p_n_waits, const char* tag)
{
	as_partition_reservation rsv;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		as_partition_reserve_migrate(ns, n, &rsv, 0);
		cf_atomic_int_incr(&g_config.nsup_subtree_count);

		as_index_reduce(rsv.p->sub_vp, cb, udata);

		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.nsup_subtree_count);

		usleep(LDT_SUB_GC_SAFETY_SLEEP_us);

		// TODO - add more throttling logic? Also need to become smart about not
		// reading the record. Can't rely on disk defrag for this because it may
		// choose not to defrag a block at all.

		cf_debug(AS_NSUP, "{%s} %s done partition index %d, waits %u", ns->name, tag, n, *p_n_waits);
	}
}

//------------------------------------------------
// Get the TTL range for histograms.
//
static uint64_t
get_ttl_range(as_namespace* ns, uint32_t now)
{
	uint64_t max_master_void_time = 0;
	as_partition_reservation rsv;

	for (int n = 0; n < AS_PARTITIONS; n++) {
		if (0 != as_partition_reserve_write(ns, n, &rsv, 0, 0)) {
			continue;
		}

		as_partition_release(&rsv);

		uint64_t partition_max_void_time = cf_atomic_int_get(ns->partitions[n].max_void_time);

		if (partition_max_void_time > max_master_void_time) {
			max_master_void_time = partition_max_void_time;
		}
	}

	// If configuration specifies max-ttl, use that to cap the namespace
	// maximum void-time.

	if (ns->max_ttl > 0) {
		uint64_t cap = ns->max_ttl + now;

		if (max_master_void_time > cap) {
			max_master_void_time = cap;
		}
	}

	// Convert to TTL - used for histogram ranges.
	return max_master_void_time > now ? max_master_void_time - now : 0;
}

//------------------------------------------------
// Lazily create and clear a set's eviction
// histogram.
//
static void
clear_set_evict_hist(as_namespace* ns, uint32_t set_id, uint32_t now, uint64_t ttl_range)
{
	if (! ns->set_evict_hists[set_id]) {
		char hist_name[HISTOGRAM_NAME_SIZE];

		sprintf(hist_name, "%s set %u evict histogram", ns->name, set_id);
		ns->set_evict_hists[set_id] = linear_histogram_create(hist_name, 0, 0, EVICTION_HIST_NUM_BUCKETS);
	}

	linear_histogram_clear(ns->set_evict_hists[set_id], now, MIN(ttl_range, EVICT_HIST_TTL_RANGE));
}

//------------------------------------------------
// Lazily create and clear a set's TTL histogram.
//
static void
clear_set_ttl_hist(as_namespace* ns, uint32_t set_id, uint32_t now, uint64_t ttl_range)
{
	if (! ns->set_ttl_hists[set_id]) {
		char hist_name[HISTOGRAM_NAME_SIZE];

		sprintf(hist_name, "%s set %u ttl histogram", ns->name, set_id);
		ns->set_ttl_hists[set_id] = linear_histogram_create(hist_name, 0, 0, EVICTION_HIST_NUM_BUCKETS);
	}

	linear_histogram_clear(ns->set_ttl_hists[set_id], now, ttl_range);
}

//------------------------------------------------
// Get sets' eviction thresholds.
//
static void
get_set_thresholds(as_namespace* ns, bool* sets_evicting, uint32_t num_sets, uint32_t now, uint32_t* low_void_times, uint32_t* high_void_times, uint32_t* mid_tenths_pcts)
{
	for (uint32_t j = 0; j < num_sets; j++) {
		uint32_t set_id = j + 1;

		if (! sets_evicting[set_id]) {
			continue;
		}

		as_set* p_set;

		if (cf_vmapx_get_by_index(ns->p_sets_vmap, j, (void**)&p_set) != CF_VMAPX_OK) {
			continue;
		}

		// A set's num_elements and evict_hwm_count include both master and
		// prole records, but we evict only masters.
		uint64_t master_limit = cf_atomic64_get(p_set->evict_hwm_count) / ns->replication_factor;
		uint64_t n_masters = linear_histogram_get_total(ns->set_evict_hists[set_id]);

		// If we're under the limit, turn eviction round into more expiration.
		// (Either the prep round deleted or expired records, or num_elements
		// was bigger than evict_hwm_count due only to excess proles.)
		if (master_limit >= n_masters) {
			low_void_times[set_id] = high_void_times[set_id] = as_record_void_time_get();
			continue;
		}

		// We're over the limit - evict excess master records.
		uint64_t master_excess = n_masters - master_limit;

		uint64_t low_void_time;
		uint64_t high_void_time;
		uint32_t mid_tenths_pct;
		bool is_last = linear_histogram_get_thresholds_for_subtotal(ns->set_evict_hists[set_id], master_excess, &low_void_time, &high_void_time, &mid_tenths_pct);

		if (is_last || high_void_time == 0) {
			cf_info(AS_NSUP, "{%s} set %s evicting using ttl histogram", ns->name, p_set->name);

			is_last = linear_histogram_get_thresholds_for_subtotal(ns->set_ttl_hists[set_id], master_excess, &low_void_time, &high_void_time, &mid_tenths_pct);
		}

		if (high_void_time == 0) {
			cf_warning(AS_NSUP, "{%s} set %s can't evict %s", ns->name, p_set->name, is_last ? "everything" : "- no records eligible");
		}
		else if (is_last) {
			cf_info(AS_NSUP, "{%s} set %s evicting into last bucket: using infinity for high void-time %u", ns->name, p_set->name, (uint32_t)high_void_time);
		}

		// See comment in get_thresholds() about evicting from last bucket.

		low_void_times[set_id] = (uint32_t)low_void_time;
		high_void_times[set_id] = is_last ? 0xFFFFffff : (uint32_t)high_void_time;
		mid_tenths_pcts[set_id] = mid_tenths_pct;

		cf_info(AS_NSUP, "{%s} set %s evict ttls %u,%d,0.%03u", ns->name, p_set->name,
				void_time_to_ttl(low_void_times[set_id], now), void_time_to_ttl(high_void_times[set_id], now), mid_tenths_pct);

		linear_histogram_save_info(ns->set_evict_hists[set_id]);
		linear_histogram_save_info(ns->set_ttl_hists[set_id]);
	}
}

//------------------------------------------------
// Get general eviction thresholds.
//
static void
get_thresholds(as_namespace* ns, uint32_t* p_low_void_time, uint32_t* p_high_void_time, uint32_t* p_mid_tenths_pct)
{
	uint64_t low_void_time;
	uint64_t high_void_time;
	bool is_last = linear_histogram_get_thresholds_for_fraction(ns->evict_hist, ns->evict_tenths_pct, &low_void_time, &high_void_time, p_mid_tenths_pct);

	if (is_last || high_void_time == 0) {
		cf_info(AS_NSUP, "{%s} evicting using ttl histogram", ns->name);

		is_last = linear_histogram_get_thresholds_for_fraction(ns->ttl_hist, ns->evict_tenths_pct, &low_void_time, &high_void_time, p_mid_tenths_pct);
	}

	if (high_void_time == 0) {
		cf_warning(AS_NSUP, "{%s} can't evict %s", ns->name, is_last ? "everything" : "- no records eligible");
	}
	else if (is_last) {
		cf_info(AS_NSUP, "{%s} evicting into last bucket: using infinity for high void-time %u", ns->name, (uint32_t)high_void_time);
	}

	// When evicting from the last bucket, set the high threshold void-time (and
	// TTL) to infinity to make sure everything in the bucket is eligible for
	// eviction, including records with void-times bigger than a cap, or bigger
	// than a threshold that's inaccurate due to rounding errors.

	*p_low_void_time = (uint32_t)low_void_time;
	*p_high_void_time = is_last ? 0xFFFFffff : (uint32_t)high_void_time;
}

//------------------------------------------------
// Stats per namespace at the end of an nsup lap.
//
static void
update_stats(as_namespace* ns, uint64_t n_master, uint64_t n_0_void_time,
		uint64_t n_expired_records, uint64_t n_evicted_records,
		uint64_t n_deleted_set_records, uint64_t n_evicted_set_records,
		uint32_t evict_ttl_low, uint32_t evict_ttl_high, uint32_t evict_mid_tenths_pct,
		uint32_t n_set_waits, uint32_t n_clear_waits, uint32_t n_general_waits, uint64_t start_ms)
{
	if (n_expired_records != 0) {
		cf_atomic_int_add(&g_config.stat_expired_objects, n_expired_records);
		cf_atomic_int_add(&ns->n_expired_objects, n_expired_records);
	}

	if (n_evicted_records != 0) {
		cf_atomic_int_set(&g_config.stat_evicted_objects_time, evict_ttl_high);
		cf_atomic_int_add(&g_config.stat_evicted_objects, n_evicted_records);
		cf_atomic_int_add(&ns->n_evicted_objects, n_evicted_records);
	}

	if (n_deleted_set_records != 0) {
		cf_atomic_int_add(&g_config.stat_deleted_set_objects, n_deleted_set_records);
		cf_atomic_int_add(&ns->n_deleted_set_objects, n_deleted_set_records);
	}

	if (n_evicted_set_records != 0) {
		cf_atomic_int_add(&g_config.stat_evicted_set_objects, n_evicted_set_records);
		cf_atomic_int_add(&ns->n_evicted_set_objects, n_evicted_set_records);
	}

	ns->non_expirable_objects = n_0_void_time;

	uint64_t total_duration_ms = cf_getms() - start_ms;

	ns->nsup_cycle_duration = (uint32_t)(total_duration_ms / 1000);
	ns->nsup_cycle_sleep_pct = total_duration_ms == 0 ? 0 : (uint32_t)((n_general_waits * 100) / total_duration_ms);

	cf_info(AS_NSUP, "{%s} Records: %"PRIu64", %"PRIu64" 0-vt, %"
			PRIu64"(%"PRIu64") expired, %"PRIu64"(%"PRIu64") evicted, %"
			PRIu64"(%"PRIu64") set deletes, %"PRIu64"(%"PRIu64") set evicted. "
			"Evict ttls: %u,%d,0.%03u. Waits: %u,%u,%u. Total time: %"PRIu64" ms",
			ns->name, n_master, n_0_void_time,
			n_expired_records, ns->n_expired_objects, n_evicted_records, ns->n_evicted_objects,
			n_deleted_set_records, ns->n_deleted_set_objects, n_evicted_set_records, ns->n_evicted_set_objects,
			evict_ttl_low, evict_ttl_high, evict_mid_tenths_pct, n_set_waits, n_clear_waits, n_general_waits, total_duration_ms);
}

//------------------------------------------------
// LDT supervisor thread "run" function.
//
void *
thr_ldt_sup(void *arg)
{
	cf_info(AS_LDT, "LDT supervisor started");

	uint64_t last_time = cf_get_seconds();

	for ( ; ; ) {
		// Wake up every 1 second to check the nsup timeout.
		struct timespec delay = { 1, 0 };
		nanosleep(&delay, NULL);

		uint64_t curr_time = cf_get_seconds();

		last_time = curr_time;

		// Iterate over every namespace.
		for (int i = 0; i < g_config.namespaces; i++) {
			as_namespace *ns = g_config.namespace[i];

			if (! ns->ldt_enabled) {
				cf_detail(AS_LDT, "{%s} ldt sub skip", ns->name);
				continue;
			}

			cf_detail(AS_LDT, "{%s} ldt sup start", ns->name);

			ldt_sub_gc_info linfo;
			linfo.ns = ns;

			uint32_t n_general_waits;

			// Reduce all partitions, cleaning up stale sub-records.
			sub_reduce_partitions(ns, as_ldt_sub_gc_fn, &linfo, &n_general_waits, "ldt subtree gc");
		}
	}

	return NULL;
}

//------------------------------------------------
// Namespace supervisor thread "run" function.
//
void *
thr_nsup(void *arg)
{
	cf_info(AS_NSUP, "namespace supervisor started");

	// Garbage-collect long-expired proles, one partition per loop.
	int prole_pids[g_config.namespaces];

	for (int n = 0; n < g_config.namespaces; n++) {
		prole_pids[n] = -1;
	}

	uint64_t last_time = cf_get_seconds();

	for ( ; ; ) {
		// Wake up every 1 second to check the nsup timeout.
		struct timespec delay = { 1, 0 };
		nanosleep(&delay, NULL);

		uint64_t curr_time = cf_get_seconds();

		if ((curr_time - last_time) < g_config.nsup_period) {
			continue; // period has not been reached for running eviction check
		}

		last_time = curr_time;

		// Iterate over every namespace.
		for (int i = 0; i < g_config.namespaces; i++) {
			uint64_t start_ms = cf_getms();

			as_namespace *ns = g_config.namespace[i];

			cf_info(AS_NSUP, "{%s} nsup start", ns->name);

			linear_histogram_clear(ns->obj_size_hist, 0, cf_atomic32_get(ns->obj_size_hist_max));

			// The "now" used for all expiration and eviction.
			uint32_t now = as_record_void_time_get();

			// Get the histogram range - used by all histograms.
			uint64_t ttl_range = get_ttl_range(ns, now);

			linear_histogram_clear(ns->ttl_hist, now, ttl_range);

			uint64_t n_expired_records = 0;
			uint64_t n_0_void_time_records = 0;
			uint64_t n_deleted_set_records = 0;
			uint64_t n_evicted_set_records = 0;
			uint32_t n_set_waits = 0;

			uint32_t num_sets = cf_vmapx_count(ns->p_sets_vmap);

			bool do_set_deletion = false;
			bool do_set_eviction = false;

			// Giving these max possible size to spare us checking each record's
			// set-id during index reduce.
			bool sets_deleting[AS_SET_MAX_COUNT + 1];
			bool sets_evicting[AS_SET_MAX_COUNT + 1];

			memset(sets_deleting, 0, sizeof(sets_deleting));
			memset(sets_evicting, 0, sizeof(sets_evicting));

			for (uint32_t j = 0; j < num_sets; j++) {
				uint32_t set_id = j + 1;

				clear_set_evict_hist(ns, set_id, now, ttl_range);
				clear_set_ttl_hist(ns, set_id, now, ttl_range);

				as_set* p_set;

				if (cf_vmapx_get_by_index(ns->p_sets_vmap, j, (void**)&p_set) == CF_VMAPX_OK) {
					uint64_t num_elements = cf_atomic64_get(p_set->num_elements);

					if (IS_SET_DELETED(p_set)) {
						if (num_elements != 0) {
							sets_deleting[set_id] = true;
							do_set_deletion = true;

							cf_info(AS_NSUP, "{%s} deleting set %s", ns->name, p_set->name);
							// If we're deleting, that'll take care of eviction.
							continue;
						}

						SET_DELETED_OFF(p_set);
					}

					uint64_t evict_hwm_count = cf_atomic64_get(p_set->evict_hwm_count);

					if (evict_hwm_count != 0 && num_elements > evict_hwm_count) {
						sets_evicting[set_id] = true;
						do_set_eviction = true;

						cf_info(AS_NSUP, "{%s} evicting set %s [%d %d]", ns->name, p_set->name, num_elements, evict_hwm_count);
					}
				}
			}

			if (do_set_eviction) {
				// Set eviction is necessary.

				sets_evict_prep_info cb_info1;

				memset(&cb_info1, 0, sizeof(cb_info1));
				cb_info1.ns = ns;
				cb_info1.now = now;
				cb_info1.sets_deleting = sets_deleting;
				cb_info1.sets_evicting = sets_evicting;

				// Reduce master partitions, building histograms to calculate
				// set thresholds. Also do set deletion and general expiration.
				reduce_master_partitions(ns, sets_evict_prep_reduce_cb, &cb_info1, &n_set_waits, "sets-evict-prep");

				n_deleted_set_records = cb_info1.num_deleted;
				n_expired_records = cb_info1.num_expired;

				// Determine sets' eviction thresholds.
				uint32_t low_void_times[num_sets];
				uint32_t high_void_times[num_sets];
				uint32_t mid_tenths_pcts[num_sets];

				memset(low_void_times, 0, sizeof(low_void_times));
				memset(high_void_times, 0, sizeof(high_void_times));
				memset(mid_tenths_pcts, 0, sizeof(mid_tenths_pcts));

				get_set_thresholds(ns, sets_evicting, num_sets, now, low_void_times, high_void_times, mid_tenths_pcts);

				sets_evict_info cb_info2;

				memset(&cb_info2, 0, sizeof(cb_info2));
				cb_info2.ns = ns;
				cb_info2.sets_evicting = sets_evicting;
				cb_info2.low_void_times = low_void_times;
				cb_info2.high_void_times = high_void_times;
				cb_info2.mid_tenths_pcts = mid_tenths_pcts;

				// Reduce master partitions, deleting records up to thresholds.
				reduce_master_partitions(ns, sets_evict_reduce_cb, &cb_info2, &n_set_waits, "sets-evict");

				n_evicted_set_records = cb_info2.num_evicted;
				n_0_void_time_records = cb_info2.num_0_void_time;
			}
			else if (do_set_deletion) {
				// Set eviction is not necessary, only set deletion.

				sets_delete_info cb_info;

				memset(&cb_info, 0, sizeof(cb_info));
				cb_info.ns = ns;
				cb_info.now = now;
				cb_info.sets_deleting = sets_deleting;

				// Reduce master partitions, doing set deletion and general
				// expiration.
				reduce_master_partitions(ns, sets_delete_reduce_cb, &cb_info, &n_set_waits, "sets-delete");

				n_deleted_set_records = cb_info.num_deleted;
				n_expired_records = cb_info.num_expired;
				n_0_void_time_records = cb_info.num_0_void_time;
			}

			// Wait for delete queue to clear, to reduce the chance we'll need
			// to do general eviction.

			uint32_t n_clear_waits = 0;

			while (cf_queue_sz(g_p_nsup_delete_q) > 0) {
				usleep(DELETE_Q_CLEAR_SLEEP_us);
				n_clear_waits++;
			}

			uint64_t n_evicted_records = 0;
			uint32_t evict_ttl_low = 0;
			uint32_t evict_ttl_high = 0;
			uint32_t evict_mid_tenths_pct = 0;
			uint32_t n_general_waits = 0;

			// Check whether or not we need to do general eviction.

			bool hwm_breached = false, stop_writes = false;

			as_namespace_eval_write_state(ns, &hwm_breached, &stop_writes);

			// Store the state of the threshold breaches.
			cf_atomic32_set(&ns->stop_writes, stop_writes ? 1 : 0);
			cf_atomic32_set(&ns->hwm_breached, hwm_breached ? 1 : 0);

			if (hwm_breached) {
				// Eviction is necessary.

				linear_histogram_clear(ns->obj_size_hist, 0, cf_atomic32_get(ns->obj_size_hist_max));
				linear_histogram_clear(ns->evict_hist, now, MIN(ttl_range, EVICT_HIST_TTL_RANGE));
				linear_histogram_clear(ns->ttl_hist, now, ttl_range);

				evict_prep_info cb_info1;

				memset(&cb_info1, 0, sizeof(cb_info1));
				cb_info1.ns = ns;

				// Reduce master partitions, building histograms to calculate
				// general eviction threshold.
				reduce_master_partitions(ns, evict_prep_reduce_cb, &cb_info1, &n_general_waits, "evict-prep");

				n_0_void_time_records = cb_info1.num_0_void_time;

				evict_info cb_info2;

				memset(&cb_info2, 0, sizeof(cb_info2));
				cb_info2.ns = ns;

				// Determine general eviction thresholds.
				get_thresholds(ns, &cb_info2.low_void_time, &cb_info2.high_void_time, &cb_info2.mid_tenths_pct);

				evict_ttl_low = void_time_to_ttl(cb_info2.low_void_time, now);
				evict_ttl_high = void_time_to_ttl(cb_info2.high_void_time, now);
				evict_mid_tenths_pct = cb_info2.mid_tenths_pct;

				cf_info(AS_NSUP, "{%s} evict ttls %u,%d,0.%03u", ns->name, evict_ttl_low, evict_ttl_high, evict_mid_tenths_pct);

				// Reduce master partitions, deleting records up to threshold.
				// (This automatically deletes expired records.)
				reduce_master_partitions(ns, evict_reduce_cb, &cb_info2, &n_general_waits, "evict");

				n_evicted_records = cb_info2.num_evicted;

				linear_histogram_dump(ns->evict_hist);
				linear_histogram_save_info(ns->evict_hist);

				// Save the eviction depth in the device header(s) so it can be
				// used to speed up cold start.
				as_storage_save_evict_void_time(ns, cb_info2.low_void_time);
			}
			else if (! (do_set_deletion || do_set_eviction)) {
				// Eviction is not necessary, only expiration. (But if set
				// deletion and/or eviction was done, expiration has already
				// been done.)

				expire_info cb_info;

				memset(&cb_info, 0, sizeof(cb_info));
				cb_info.ns = ns;
				cb_info.now = now;

				// Reduce master partitions, deleting expired records.
				reduce_master_partitions(ns, expire_reduce_cb, &cb_info, &n_general_waits, "expire");

				n_expired_records = cb_info.num_expired;
				n_0_void_time_records = cb_info.num_0_void_time;
			}

			linear_histogram_dump(ns->obj_size_hist);
			linear_histogram_save_info(ns->obj_size_hist);
			linear_histogram_dump(ns->ttl_hist);
			linear_histogram_save_info(ns->ttl_hist);

			update_stats(ns, linear_histogram_get_total(ns->ttl_hist) + n_0_void_time_records, n_0_void_time_records,
					n_expired_records, n_evicted_records,
					n_deleted_set_records, n_evicted_set_records,
					evict_ttl_low, evict_ttl_high, evict_mid_tenths_pct,
					n_set_waits, n_clear_waits, n_general_waits, start_ms);

			// Garbage-collect long-expired proles, one partition per loop.
			if (g_config.prole_extra_ttl != 0) {
				prole_pids[i] = garbage_collect_next_prole_partition(ns, prole_pids[i]);
			}
		}

		uint32_t n_clear_waits = 0;

		while (cf_queue_sz(g_p_nsup_delete_q) > 0) {
			usleep(DELETE_Q_CLEAR_SLEEP_us);
			n_clear_waits++;
		}

		if (n_clear_waits) {
			cf_info(AS_NSUP, "nsup clear waits: %u", n_clear_waits);
		}
	}

	return NULL;
}

//------------------------------------------------
// Start supervisor threads.
//
void
as_nsup_start()
{
	// Seed the random number generator.
	srand(time(NULL));

	// Create queue for nsup-generated deletions.
	if (NULL == (g_p_nsup_delete_q = cf_queue_create(sizeof(record_delete_info), true))) {
		cf_crash(AS_NSUP, "nsup delete queue create failed");
	}

	// Start thread to handle all nsup-generated deletions.
	if (0 != pthread_create(&g_nsup_delete_thread, 0, run_nsup_delete, NULL)) {
		cf_crash(AS_NSUP, "nsup delete thread create failed");
	}

	// Start namespace supervisor thread to do expiration & eviction.
	if (0 != pthread_create(&g_nsup_thread, NULL, thr_nsup, NULL)) {
		cf_crash(AS_NSUP, "nsup thread create failed");
	}

	// Start LDT supervisor thread to do all sub-record deletions.
	if (0 != pthread_create(&g_ldt_sub_gc_thread, 0, thr_ldt_sup, NULL)) {
		cf_crash(AS_NSUP, "ldt nsup thread create failed");
	}
}
