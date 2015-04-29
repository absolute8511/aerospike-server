/*
 * thr_tscan.c
 *
 * Copyright (C) 2011-2014 Aerospike, Inc.
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

#include "base/thr_scan.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "aerospike/as_module.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "fault.h"
#include "msg.h"
#include "queue.h"
#include "rchash.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/monitor.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/thr_rw_internal.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
#include "fabric/fabric.h"
#include "storage/storage.h"


#define SCAN_JOB_TYPE_PARTITION 1   // scans by iterating through partitions
#define SCAN_JOB_TYPE_STORAGE   2   // scans by stream reading through storage
#define SCAN_JOB_TYPE_SINDEX_POPULATE 3 // scans by iterating through partition and calls
										// secondary index to insert into it
#define SCAN_JOB_TYPE_SINDEX_POPULATEALL 4 // Secondary index scan of all entries in a name-space
										   // Applicable only for warm-restart (or) Data-in-memory
#define SCAN_JOB_IS_POPULATOR(job) \
	((job->job_type == SCAN_JOB_TYPE_SINDEX_POPULATE) \
		|| (job->job_type == SCAN_JOB_TYPE_SINDEX_POPULATEALL))

#define SCAN_PARTITION_STATE_NOT_DONE   0
#define SCAN_PARTITION_STATE_FINISHED   1
#define SCAN_PARTITION_STATE_NOT_AVAIL  2
#define SCAN_PARTITION_STATE_FAILED     3
#define SCAN_PARTITION_STATE_ABORTED	4

#define MAX_SCAN_UDF_TRANSACTIONS 20 // The higher the value the more aggressive it will be
#define MAX_SCAN_UDF_WORKITEM_PER_ITERATION 10
#define MAX_SCAN_UDF_WORKITEM 100
#define SCAN_PRIORITY_AUTO  0
#define SCAN_PRIORITY_LOW   1
#define SCAN_PRIORITY_MEDIUM 2
#define SCAN_PRIORITY_HIGH  3
#define SCAN_JOB_HASH_PERSIST_TIME 5*60*1000 // For how long do we want to persist the scan job hash - 5 mins

#define TSCAN_FIELD_OP          0
#define TSCAN_FIELD_NAMESPACE   1
#define TSCAN_FIELD_DIGEST      2
#define TSCAN_FIELD_GENERATION  3
#define TSCAN_FIELD_RECORD      4
#define TSCAN_FIELD_CLUSTER_KEY 5
#define TSCAN_FIELD_RECORD_TTL  6
#define TSCAN_FIELD_REC_PROPS   7
#define TSCAN_FIELD_JOB_ID      8

// States a scan job can be in
#define AS_SCAN_JOB_ABORTED 	0x00000001
#define AS_SCAN_JOB_DONE		0x00000002
#define AS_SCAN_JOB_FINISHED	0x00000004
#define AS_SCAN_JOB_PENDING		0x00000008

// Job aborted flag is set when the client sends the abort signal for that particular job
#define SCAN_JOB_ABORTED_OFF(job)  	(cf_atomic32_set(&job->flag, cf_atomic32_get(job->flag) &  ~AS_SCAN_JOB_ABORTED))
#define SCAN_JOB_ABORTED_ON(job)   	(cf_atomic32_set(&job->flag, cf_atomic32_get(job->flag) |  AS_SCAN_JOB_ABORTED))
#define IS_SCAN_JOB_ABORTED(job) 	(cf_atomic32_get(job->flag) & AS_SCAN_JOB_ABORTED)

// Job done flag is set when the job has completed its work, it might still be in pending state (that is, not yet
// deleted from the scan job hash
#define SCAN_JOB_DONE_OFF(job) 		(cf_atomic32_set(&job->flag, cf_atomic32_get(job->flag) &  ~AS_SCAN_JOB_DONE))
#define SCAN_JOB_DONE_ON(job)   	(cf_atomic32_set(&job->flag, cf_atomic32_get(job->flag) |  AS_SCAN_JOB_DONE))
#define IS_SCAN_JOB_DONE(job) 		(cf_atomic32_get(job->flag) & AS_SCAN_JOB_DONE)

// Job pending flag is set when the job is waiting to be deleted from the job scan hash, it might already be done with
// the complete processing
#define SCAN_JOB_PENDING_OFF(job)  	(cf_atomic32_set(&job->flag, cf_atomic32_get(job->flag) &  ~AS_SCAN_JOB_PENDING))
#define SCAN_JOB_PENDING_ON(job)   	(cf_atomic32_set(&job->flag, cf_atomic32_get(job->flag) |  AS_SCAN_JOB_PENDING))
#define IS_SCAN_JOB_PENDING(job) 	(cf_atomic32_get(job->flag) & AS_SCAN_JOB_PENDING)

// Scan type(normal,background,foreground,sindex)
#define SCAN_NORMAL     1
#define SCAN_UDF_BG     2
#define SCAN_UDF_FG     3
#define SCAN_SINDEX     4
#define SCAN_AGGR       5

//#define SEQ_SCAN 0
//#define PRL_SCAN 0

// TODO - eventually, we'll do better than this hard-wired value.
const size_t INITIAL_BUFBUILDER_SIZE = 200 * 1024 * 1024;
const size_t SCAN_PROTO_LIMIT = 100 * 1024 * 1024;

msg_template tscan_mt[] = {
	{ TSCAN_FIELD_OP,       M_FT_UINT32 }, // operation
	{ TSCAN_FIELD_NAMESPACE, M_FT_BUF },
	{ TSCAN_FIELD_DIGEST, M_FT_BUF },
	{ TSCAN_FIELD_GENERATION, M_FT_UINT32 },
	{ TSCAN_FIELD_RECORD,   M_FT_BUF } ,   // pickled data
	{ TSCAN_FIELD_CLUSTER_KEY, M_FT_UINT64 },
	{ TSCAN_FIELD_RECORD_TTL, M_FT_UINT32 },
	{ TSCAN_FIELD_REC_PROPS, M_FT_BUF },
	{ TSCAN_FIELD_JOB_ID, M_FT_UINT64 }
};


// Hash containing current scan jobs: key = tid, value = scan_job
static rchash *g_scan_job_hash = NULL;
// Hash containing the prole action of a particular job. Note all proles (ie replica 2, 3 etc) all share the same hash
// static rchash *g_scan_job_prole_hash = NULL;
static cf_atomic32 g_scan_job_tid = 0;

// default # of threads for the partition work
static pthread_t g_scan_worker_th_array[MAX_SCAN_THREADS];
static cf_queue *g_scan_partition_work_q_array[MAX_SCAN_THREADS];

static pthread_t g_scan_udf_job_th;
static cf_queue *g_scan_udf_job_q;
static cf_queue *g_scan_job_slotq;

typedef struct {
	uint64_t tid;    // transaction_id
	uint32_t pid;    // partition_id
} scan_job_workitem;

typedef struct {
	uint8_t partition_done[AS_PARTITIONS];
} scan_job_partitions_tracker;

void  scan_job_release_and_destroy(tscan_job *job);
int   tscan_send_fin_to_client(tscan_job *job, uint32_t result_code);
int   tscan_send_response_to_client( tscan_job *job, uint8_t *buf, size_t len);
int   tscan_enqueue_udfjob(tscan_job *job);
int   tscan_start_job(tscan_job *job, as_transaction *tr, bool scan_disconnected_job);
void  dump_digest(cf_digest *dd) {
	uint8_t *d = (uint8_t *) dd;
	cf_warning(AS_SCAN, "0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x", d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9]);
	cf_warning(AS_SCAN, "0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x", d[10], d[11], d[12], d[13], d[14], d[15], d[16], d[17], d[18], d[19]);
}

// Note : This function returns scan-type which is different from job-type
// Caller-beware : length of scan_type to be checked.

const char *scan_type_array[] = {"NORMAL", "BACKGROUND_UDF", "FOREGROUND_UDF", "SINDEX_POPULATOR", "UNKNOWN"};
const size_t scan_type_array_max = (sizeof(scan_type_array) / sizeof(const char *)) - 1;


int
tscan_add_val_response(tscan_task_data * task, const as_val * val, bool success)
{
	uint32_t msg_sz = 0;
	as_val_tobuf(val, NULL, &msg_sz);
	if (0 == msg_sz) {
		cf_warning(AS_PROTO, "particle to buf: could not copy data!");
	}
	int ret = as_msg_make_val_response_bufbuilder(val, &task->bb, msg_sz, success); // error handling
	if (ret != 0) {
		cf_warning(AS_SCAN, "Weird there is space but still the packing failed "
				"available = %d msg size = %d",
				task->bb->alloc_sz - task->bb->used_sz, msg_sz);
	}
	return 0;
}

as_stream_status
tscan_agg_ostream_write(const as_stream *s, as_val *v)
{
	tscan_task_data * task = as_stream_source(s);
	if (!v) {
		return AS_STREAM_OK;
	}
	if (tscan_add_val_response(task, v, true)) {
		as_val_destroy(v);
		return AS_STREAM_ERR;
	}
	as_val_destroy(v);
	return AS_STREAM_OK;
}

const as_stream_hooks tscan_agg_istream_hooks = {
		.destroy  = NULL,
		.read     = as_aggr_istream_read,
		.write    = NULL
};

const as_stream_hooks tscan_agg_ostream_hooks = {
		.destroy  = NULL,
		.read     = NULL,
		.write    = tscan_agg_ostream_write
};

bool
tscan_mem_op(mem_tracker *mt, uint32_t num_bytes, memtracker_op op)
{
	bool ret = true;
	if (!mt || !mt->udata) {
		return false;
	}
#if 0
	uint64_t val = 0;
	as_query_transaction *qtr = (as_query_transaction *)mt->udata;
	if (qtr) return false;

	if (op == MEM_RESERVE) {
		val = cf_atomic_int_add(&g_config.udf_runtime_gmemory_used, num_bytes);
		if (val > g_config.udf_runtime_max_gmemory) {
			cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used, num_bytes);
			ret = false;
			goto END;
		}

		val = cf_atomic_int_add(&qtr->udf_runtime_memory_used, num_bytes);
		if (val > g_config.udf_runtime_max_memory) {
			cf_atomic_int_sub(&qtr->udf_runtime_memory_used, num_bytes);
			cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used, num_bytes);
			ret = false;
			goto END;
		}
	} else if (op == MEM_RELEASE) {
		cf_atomic_int_sub(&qtr->udf_runtime_memory_used, num_bytes);
	} else if (op == MEM_RESET) {
		cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used,
				qtr->udf_runtime_memory_used);
		qtr->udf_runtime_memory_used = 0;
	} else {
		ret = false;
	}
END:
#endif 
	return ret;
}

void
tscan_set_error(void * caller)
{
}

as_aggr_caller_type
tscan_get_type()
{
	return AS_AGGR_SCAN;
}

const as_aggr_caller_intf scan_aggr_caller_qintf = {
	.set_error = tscan_set_error,
	.mem_op = tscan_mem_op,
	.get_type = tscan_get_type
};


const char *
tscan_get_type_str(uint8_t scan_type)
{
	return scan_type_array[scan_type <= (uint8_t)scan_type_array_max ? (scan_type - 1) : scan_type_array_max];
}

// NB: Caller holds a write hash lock _BE_CAREFUL_ if you intend to take
// lock inside this function
int
as_tscan_udf_tr_complete( as_transaction *tr, int retcode )
{
	tscan_job *job      = (tscan_job *)tr->udata.req_udata;
	if (!job) {
		cf_warning(AS_SCAN, "Complete called with invalid job id");
		return -1;
	}
	uint64_t start_time = tr->start_time;
	uint64_t processing_time = (cf_getns() - start_time) / 1000000;
	uint64_t completed  = cf_atomic64_incr(&job->uit_completed);
	uint64_t queued     = cf_atomic_int_decr(&job->uit_queued);

	// Calculate total processing time of udf transactions completed so far
	cf_atomic64_set(&job->uit_total_run_time, cf_atomic64_add(&job->uit_total_run_time, processing_time));
	cf_detail(AS_SCAN, "UDF: Internal transaction completed %d, remaining %d, processing_time for this transaction %d, total txn processing time %"PRIu64"",
			completed, queued, processing_time, job->uit_total_run_time);
	scan_job_release_and_destroy(job);
	return 0;
}

static inline void
tscan_job_update_pstatus(tscan_job *job, uint32_t pid, int status)
{
	((scan_job_partitions_tracker *) job->udata)->partition_done[pid] = status;

	//scan progress update after each partition scanned
	cf_atomic_int_incr(&job->n_partitions_scanned);
}

bool
tscan_job_is_finished(tscan_job *job)
{
	for (int i = 0; i < AS_PARTITIONS; i++) {
		if (((scan_job_partitions_tracker *) job->udata)->partition_done[i] == SCAN_PARTITION_STATE_NOT_DONE) {
			return false;
		}
	}
	return true;
}

tscan_job *
tscan_job_create(uint64_t trid)
{
	tscan_job *job = cf_rc_alloc( sizeof(tscan_job) );
	//cf_info(AS_SCAN, "UDF: Reserved %d", cf_rc_count(job));
	if (!job)
		return(0);

	memset (job, 0, sizeof(tscan_job));

	job->tid = trid != 0 ? trid : (uint64_t)cf_atomic32_add(&g_scan_job_tid, 1);
	job->cluster_key = as_paxos_get_cluster_key();
	job->n_threads        = 1;
	job->scan_pct         = 100;
	job->si               = NULL;
	job->binlist          = NULL;
	job->start_time       = cf_getms();
	job->end_time         = 0;
	job->net_io_bytes     = 0;
	job->mem_buf          = 0;
	job->result           = AS_PROTO_RESULT_OK;

	SCAN_JOB_ABORTED_OFF(job);
	SCAN_JOB_DONE_OFF(job);
	SCAN_JOB_PENDING_OFF(job);
	cf_atomic_int_set(&job->n_partitions_scanned, 0);
	cf_atomic_int_set(&job->uit_queued, 0);
	cf_atomic_int_set(&job->uit_completed, 0);
	cf_atomic64_set(&job->uit_total_run_time, 0);
	pthread_mutex_init( &job->LOCK, 0 );
	return(job);
}

void
scan_job_done(tscan_job *job)
{
	if (job->result == AS_PROTO_RESULT_OK) {
		cf_atomic_int_incr(&g_config.tscan_succeeded);
	}

	job->end_time = cf_getms();

	cf_info(AS_SCAN, "SCAN JOB DONE  [id =%"PRIu64": ns= %s set=%s scanned=%ld expired=%ld set_diff=%ld elapsed=%ld (ms)]",
			job->tid, job->ns->name, as_namespace_get_set_name(job->ns, job->set_id),
			job->n_obj_scanned, job->n_obj_expired, job->n_obj_set_diff, job->end_time - job->start_time);

	// Print end of sindex ticker here:
	if (SCAN_JOB_IS_POPULATOR(job)) {
		as_sindex_ticker_done(job->ns, job->si, job->start_time);
		
		if (job->job_type == SCAN_JOB_TYPE_SINDEX_POPULATEALL) {
			as_sindex_boot_populateall_done(job->ns);
		}
	}
	else {
		tscan_send_fin_to_client(job, job->result);
	}
}

void
scan_udf_job_done(tscan_job *job, int rsp)
{
	if (rsp == AS_PROTO_RESULT_OK) {
		cf_atomic_int_incr(&g_config.tscan_succeeded);
	}

	job->end_time = cf_getms();

	cf_info(AS_SCAN, "SCAN JOB DONE  [id =%"PRIu64": ns= %s set=%s scanned=%ld expired=%ld set_diff=%ld elapsed=%ld (ms)]",
			job->tid, job->ns->name, as_namespace_get_set_name(job->ns, job->set_id),
			job->n_obj_scanned, job->n_obj_expired, job->n_obj_set_diff, job->end_time - job->start_time);

	cf_info(AS_SCAN, "SCAN UDF       [%s:%s]" , job->call.filename, job->call.function);
	cf_info(AS_SCAN, "SCAN UDF STATS [scanned %"PRIu64 " expired %"PRIu64 " set_diff %"PRIu64 " succeeded %"PRIu64 " failed %"PRIu64 " updated %"PRIu64" ]",
			cf_atomic_int_get(job->n_obj_scanned), cf_atomic_int_get(job->n_obj_expired), cf_atomic_int_get(job->n_obj_set_diff),
			cf_atomic_int_get(job->n_obj_udf_success), cf_atomic_int_get(job->n_obj_udf_failed), cf_atomic_int_get(job->n_obj_udf_updated));
}

void
tscan_job_destructor( void *udata )
{
	tscan_job *job = (tscan_job *)udata;

	if (job->call.udf_type != AS_SCAN_UDF_OP_BACKGROUND) {
		scan_job_done(job);
	}
	if (job->hasudf) {
		udf_call_destroy(&job->call);
		job->hasudf = false;
	}
	if (job->fd_h) {
		cf_detail(AS_SCAN, "UDF: refcount of fd at destruction count is %d", cf_rc_count(job->fd_h));
		AS_RELEASE_FILE_HANDLE(job->fd_h);
		job->fd_h = NULL;
	}
	if (job->ns) {
		job->ns = NULL;
	}
	if (job->binlist) {
		cf_vector_destroy(job->binlist);
		job->binlist = NULL;
	}
	if (job->msgp) {
		cf_free(job->msgp);
		job->msgp = NULL;
	}
	if (job->udata) {
		if ((job->job_type == SCAN_JOB_TYPE_PARTITION)
				|| (job->job_type == SCAN_JOB_TYPE_SINDEX_POPULATE))
		{
			cf_free(job->udata);
			job->udata = NULL;
			if (job->job_type == SCAN_JOB_TYPE_SINDEX_POPULATE) {
				job->si->stats.loadtime = cf_getms() - job->start_time;
				AS_SINDEX_RELEASE(job->si);
			}
		}
		// @TODO clean job type specific allocation for storage-based scanning
	}
	pthread_mutex_destroy(&job->LOCK);

	if (cf_rc_count(job) != 0) {
		cf_warning(AS_SCAN, "likely ref-count problem with scan_job %p", job);
	}
}

// Note that an rchash_delete() also does exactly this.
void
scan_job_release_and_destroy(tscan_job *job)
{
	if (0 == cf_rc_release(job)) {
		tscan_job_destructor(job);
		cf_rc_free(job);
	}
}

uint32_t
tscan_job_tid_hash(void *value, uint32_t keylen)
{
	return( *(uint32_t *)value);
}

int
as_tscan_sindex_populate(void *param)
{
	as_sindex *si = (as_sindex *)param;
	as_sindex_metadata *imd = si->imd;
	as_namespace *ns = as_namespace_get_byname(imd->ns_name);
	if (!ns) {
		cf_warning(AS_SCAN, "Scan Request On Unavailable Namespace");
		AS_SINDEX_RELEASE(si);
		return(-1);
	}

	uint16_t set_id = INVALID_SET_ID;
	if (imd->set) {
		char set_name[strlen(imd->set) + 1];
		strcpy(set_name, imd->set);
		set_id = as_namespace_get_set_id(ns, set_name);
		// There is a valid set-name in the sindex-creation command
		// but that is a non-existent set : do not start background scan
		if (set_id == INVALID_SET_ID) {
			cf_info(AS_SCAN, "Not starting background scan for ns:%s si:%s because set:%s does not exist. ",
					imd->ns_name, imd->iname, imd->set);
			as_sindex_populate_done(si);
			// Reservation taken during sindex_create gets released only when
			// scan-job is destroyed. So make sure that every return from this
			// function does a rc-release.
			AS_SINDEX_RELEASE(si);
			return(-3);
		}
	}

	tscan_job *job = tscan_job_create(0);
	if (!job) {
		AS_SINDEX_RELEASE(si);
		return -2;
	}
	job->fd_h        = NULL;
	job->ns          = ns;
	job->set_id      = set_id;
	job->nobindata   = false; // look at the data
	// Normal priority
	job->n_threads   = g_config.sindex_populator_scan_priority;
	job->job_type    = SCAN_JOB_TYPE_SINDEX_POPULATE;
	job->scan_type   = SCAN_SINDEX;
	job->cluster_key = as_paxos_get_cluster_key();
	job->si          = si;
	job->fail_on_cluster_change = false; // continue if cluster view changes
	job->si_start_desync_cnt    = si->desync_cnt;
	// Assigning int to uint64_t, int < uint64_t, so this copy is ok, but reverse = truncation
	si->stats.recs_pending      = ns->n_objects;
	job->scan_state_logged      = AS_SCAN_STARTED;

	// Add job to global hash for tracking purposes.
	int hp_rc;
	if (RCHASH_OK != (hp_rc = rchash_put_unique(g_scan_job_hash, &job->tid, sizeof(job->tid), job))) {
		cf_warning(AS_SCAN, "Failed to create Background index creation for index %s:%s:%s. Error = %d",
				imd->ns_name, imd->set, imd->iname, hp_rc);
		cf_warning(AS_SCAN, "not starting scan %d because rchash_put() failed with error %d", job->tid, hp_rc);
		scan_job_release_and_destroy(job);
		return -2;
	}

	// It is internal work no tr or no reply to be sent to the client.
	int rsp = tscan_start_job(job, NULL, false);
	if (rsp < 0) {
		cf_warning(AS_SCAN, "Failed to start Background index creation for index %s:%s:%s",
				imd->ns_name, imd->set, imd->iname);
		rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid));
		return -2;
	}
	cf_detail(AS_SCAN, "Started Background index creation for index %s:%s:%s",
			imd->ns_name, imd->set, imd->iname);
	cf_atomic_int_incr(&g_config.tscan_initiate);
	return 0;
}

/* Function to parse all the primary-indexes at boot-time and generate
 * all the secondary indices in the system.
 * It gets called for warm-restart ( both SSD & in-memory) and
 * cold-restart (for SSD alone).
 * */
int
as_tscan_sindex_populateall(void *param)
{
	as_namespace *ns = (as_namespace *)param;
	tscan_job *job = tscan_job_create(0);
	if (!job) {
		return -2;
	}
	job->fd_h        = NULL;
	job->ns          = ns;
	job->set_id      = INVALID_SET_ID;
	job->nobindata   = false; // look at the data
	// Normal priority.
	job->n_threads   = g_config.sindex_populator_scan_priority;
	job->job_type    = SCAN_JOB_TYPE_SINDEX_POPULATEALL;
	job->scan_type   = SCAN_SINDEX;
	job->cluster_key = as_paxos_get_cluster_key();
	job->si          = NULL;
	job->fail_on_cluster_change = false; // continue if cluster view changes
	job->si_start_desync_cnt    = 0;
	job->scan_state_logged      = AS_SCAN_STARTED;

	// Add job to global hash for tracking purposes.
	int hp_rc;
	if (RCHASH_OK != (hp_rc = rchash_put_unique(g_scan_job_hash, &job->tid, sizeof(job->tid), job))) {
		cf_warning(AS_SCAN, "Failed to create background scan job for namespace %s index population", ns->name);
		cf_warning(AS_SCAN, "not starting scan %d because rchash_put() failed with error %d", job->tid, hp_rc);
		scan_job_release_and_destroy(job);
		return -2;
	}

	// It is internal work no tr or no reply to be sent to the client.
	int rsp = tscan_start_job(job, NULL, false);
	if (rsp < 0) {
		cf_warning(AS_SCAN, "Failed to create background scan job for namespace %s index population", ns->name);
		rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid));
		return -2;
	}
	cf_detail(AS_SCAN, "Started Background namespace %s index creation job", ns->name);
	cf_atomic_int_incr(&g_config.tscan_initiate);
	return 0;
}

// Send ack for successful start of disconnected job to client.
int
tscan_send_disconnected_job_ack_to_client(int fd)
{
	cl_msg m;

	m.proto.version = PROTO_VERSION;
	m.proto.type = PROTO_TYPE_AS_MSG;
	m.proto.sz = sizeof(as_msg);
	as_proto_swap(&m.proto);
	m.msg.header_sz = sizeof(as_msg);
	m.msg.info1 = 0;
	m.msg.info2 = 0;
	m.msg.info3 = 0;
	m.msg.unused = 0;
	m.msg.result_code = AS_PROTO_RESULT_OK;
	m.msg.generation = 0;
	m.msg.record_ttl = 0;
	m.msg.transaction_ttl = 0;
	m.msg.n_fields = 0;
	m.msg.n_ops = 0;
	as_msg_swap_header(&m.msg);

	uint8_t *buf = (uint8_t *)&m;
	int pos = 0;

	while (pos < sizeof(m)) {
		int rv = send(fd, buf + pos, sizeof(m) - pos, MSG_NOSIGNAL);

		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_info(AS_SCAN, "disconnected-job ack %d errno %d fd %d", rv, errno, fd);
				return -1;
			}
		}
		else {
			pos += rv;
		}
	}

	return 0;
}

// This routine always unlocks the job lock.
int
tscan_enqueue_udfjob(tscan_job *job)
{
	int rsp = 0;
	if (job->job_type == SCAN_JOB_TYPE_PARTITION) {

		if (job->cur_partition_id >= AS_PARTITIONS) {
			// This is a code path that should never happen may happen immediately
			// after the last job is queued
			if (job->cur_partition_id > (AS_PARTITIONS + 1)) {
				cf_warning(AS_SCAN, "UDF: partition id %d unexpected for job %d", job->cur_partition_id, job->tid);
			}
			pthread_mutex_unlock(&job->LOCK);
			// This is set to 0 so that the correct higher level cleanup can happen.
			// However, this code path should never be reached!
			return 0;
		}
		// Cache priority information for this iteration and
		// release the lock.
		int n_threads = job->n_threads;
		pthread_mutex_unlock(&job->LOCK);

		int cycler = rand() % n_threads;
		uint32_t limit = job->cur_partition_id + (MAX_SCAN_UDF_WORKITEM_PER_ITERATION * n_threads);

		if (limit > AS_PARTITIONS) {
			limit = AS_PARTITIONS;
		}

		while (job->cur_partition_id < limit) {
			scan_job_workitem workitem;
			workitem.tid = job->tid;
			workitem.pid = job->cur_partition_id;
			byte slot;

			if (CF_QUEUE_OK != cf_queue_pop(g_scan_job_slotq, &slot, CF_QUEUE_FOREVER)) {
				// This is a code path that should never happen.
				cf_warning(AS_SCAN, "UDF scan job slot queue pop failed");
				break;
			}

			cf_detail(AS_SCAN, "UDF: Adding new work item for job [%"PRIu64" %u %d] slot %d", job->tid, workitem.pid, cycler, slot);
			cf_queue_push(g_scan_partition_work_q_array[cycler++ % n_threads], &workitem);

			job->cur_partition_id++;
		}

		cf_detail(AS_SCAN, "UDF: Iteration over for job %"PRIu64" next pid %u ", job->tid, job->cur_partition_id);

		// Debug only, check how many of the queues have workitems.
		for (int i = 0; i < MAX_SCAN_THREADS; i++) {
			cf_detail(AS_SCAN, "%d has %d workitems", i, cf_queue_sz(g_scan_partition_work_q_array[i]));
		}
	} else {
		cf_warning(AS_SCAN, "UDF: Unknown scan udf job type .. internal error .. aborting !!");
		SCAN_JOB_ABORTED_ON(job);
		// rsp = -2; // TODO: how to set the result code for all kinds of aborts
		pthread_mutex_unlock(&job->LOCK);
	}
	return rsp;
}

int tscan_start_job(tscan_job *job, as_transaction *tr, bool scan_disconnected_job)
{
	int rsp = 0;
	if ((job->job_type == SCAN_JOB_TYPE_PARTITION)
			|| (SCAN_JOB_IS_POPULATOR(job))) {
		scan_job_partitions_tracker * ptracker = cf_malloc(sizeof(scan_job_partitions_tracker));
		if (ptracker == NULL) {
			return -2;
		}
		memset(ptracker, 0, sizeof(scan_job_partitions_tracker));
		int cycler = 0;
		job->udata = ptracker;
		// Disconnected job that started successfully - tell the client.
		if (scan_disconnected_job) {
			tscan_send_disconnected_job_ack_to_client(tr->proto_fd_h->fd);
			AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
			tr->proto_fd_h = 0;
		}
		// Connected job that started successfully - take over open file handle.
		else if (tr) {
			job->fd_h = tr->proto_fd_h;
			job->fd_h->fh_info |= FH_INFO_DONOT_REAP;
			tr->proto_fd_h = 0;
			cf_detail(AS_SCAN, "UDF: refcount of fd at the start of the time %d", cf_rc_count(job->fd_h));
		}

		// Print start of sindex ticker here:
		if (SCAN_JOB_IS_POPULATOR(job)) {
			as_sindex_ticker_start(job->ns, job->si);
		}

		for (int i = 0; i < AS_PARTITIONS; i++) {
			scan_job_workitem workitem;
			workitem.tid = job->tid;
			workitem.pid = i;
			// Note : tscan_partition_thr() is the function that gets called by this queue-push.
			// This gets called during tscan_init and a separate thread gets created for each index
			// of the work-q. This thread i then infinitely waits on a queue-push for index i and
			// processes the work item.
			cf_queue_push( g_scan_partition_work_q_array[cycler++ % job->n_threads], &workitem);
		}

		// Debug only, check how many of the queues have workitems.
		for (int i = 0; i < MAX_SCAN_THREADS; i++) {
			cf_detail(AS_SCAN, "%d has %d workitems", i, cf_queue_sz(g_scan_partition_work_q_array[i]));
		}
	} else if (job->job_type == SCAN_JOB_TYPE_STORAGE) {
		// @TODO streaming disk
		//rsp = as_storage_init_scan_job(job);
	} else {
		rsp = -2;
	}
	return rsp;
}

int
tscan_start_udfjob(tscan_job *job, as_transaction *tr, bool disconnected)
{
	// Route the udf job to parallel UDF job manager. We only support
	// per record UDF at this point for the scan.
	job->fd_h               = tr->proto_fd_h;
	job->fd_h->fh_info     |= FH_INFO_DONOT_REAP;
	tr->proto_fd_h          = 0;
	job->cur_partition_id   = 0; // start from partition_id
	cf_detail(AS_SCAN, "UDF: Pushed scan UDF job into the queue [%"PRIu64" %p %p]", job->tid, job, job->fd_h);
	cf_detail(AS_SCAN, "UDF: refcount of fd at the start of the time %d", cf_rc_count(job->fd_h));

	// If we have a background scan udf, send a response back to the client now.
	if ((job->call.udf_type == AS_SCAN_UDF_OP_BACKGROUND)
			|| (disconnected)) {
		tscan_send_fin_to_client(job, AS_PROTO_RESULT_OK);
		job->fd_h->fh_info &= ~FH_INFO_DONOT_REAP;
		cf_rc_release(job->fd_h);
		job->fd_h           = 0;
	}

	return cf_queue_push(g_scan_udf_job_q, &job);
}

int
as_tscan_get_pending_job_count()
{
	return rchash_get_size(g_scan_job_hash);
}


cf_vector *
tscan_binlist_from_op(as_namespace *ns, as_msg *msgp)
{
	cf_vector *binlist  = cf_vector_create(AS_ID_BIN_SZ, 5, 0);
	as_msg_op *op = 0;
	int n = 0;
	int n_bins = 0;

	while ((op = as_msg_op_iterate(msgp, op, &n))) {
		int binnamesz = op->name_sz;
		char binname[AS_ID_BIN_SZ];
		memset(binname, 0, AS_ID_BIN_SZ);
		memcpy(&binname, op->name, op->name_sz);
		binname[binnamesz] = 0;
		cf_vector_append(binlist, (void *)binname);
		n_bins++;
	}
	if (n_bins == 0) {
		cf_vector_destroy(binlist);
		binlist = NULL;
	}
	return binlist;
}


extern int
as_aggr_call_init(as_aggr_call *call, as_transaction *txn, void *caller,
		const as_aggr_caller_intf *caller_intf, const as_stream_hooks *istream_hooks,
		const as_stream_hooks *ostream_hooks, as_namespace *ns, bool is_scan);

// Start the scan of the system.
//
//	Has the responsibility to free tr->msgp
//
// return codes
//  0 - ok
// -1 - bad parameters
// -2 - internal errors
// -3 - proper response. cluster in migration
// -4 - set name is valid but set doesn't exist
// -5 - unsupported feature (e.g. Scans with UDF)
int
as_tscan(as_transaction *tr)
{
	cf_info(AS_SCAN, "scan job received");
	// Required param checking NS and set. Scan option is optional.
	as_msg_field *nsfp = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_NAMESPACE);
	if (!nsfp) {
		cf_warning(AS_SCAN, "scan request no namespace");
		if (tr->msgp) cf_free(tr->msgp);
		return -1;
	}
	as_namespace *ns = as_namespace_get_bymsgfield(nsfp);
	if (!ns) {
		cf_warning(AS_SCAN, "scan with unavailable namespace");
		if (tr->msgp) cf_free(tr->msgp);
		return(-1);
	}

	// Get set, if it's passed to us.
	uint16_t set_id = INVALID_SET_ID;
	as_msg_field *p_set_field = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET);
	if (p_set_field != NULL && as_msg_field_get_value_sz(p_set_field) != 0) {
		uint32_t set_name_len = as_msg_field_get_value_sz(p_set_field);
		char     set_name[set_name_len + 1];

		memcpy(set_name, p_set_field->data, set_name_len);
		set_name[set_name_len] = '\0';

		set_id = as_namespace_get_set_id(ns, set_name);

		// A non-empty set name that isn't found should not trigger a scan
		// of the whole namespace.
		if (set_id == INVALID_SET_ID) {
			cf_info(AS_SCAN, "scanning non-existent set %s", set_name);
			if (tr->msgp) cf_free(tr->msgp);
			return -4;
		}
	}

	// SCAN OPTIONS
	bool scan_fail_on_cluster_change = false;
	bool scan_disconnected_job       = false;
	int  scan_priority               = SCAN_PRIORITY_AUTO;
	int  scan_pct                    = 100;
	as_msg_field *scan_options       = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SCAN_OPTIONS);

	if (scan_options != NULL) {
		// First, just make sure the 2 bytes are there, and log them.
		if (as_msg_field_get_value_sz(scan_options) != 2) {
			cf_info(AS_SCAN, "scan_option is not 2 bytes, ignoring request");
			if (tr->msgp) cf_free(tr->msgp);
			return -2;
		}

		cf_info(AS_SCAN, "scan_option 0x%x 0x%x", scan_options->data[0], scan_options->data[1]);

		scan_disconnected_job =
			AS_MSG_FIELD_SCAN_DISCONNECTED_JOB & scan_options->data[0] ? true : false;
		if (scan_disconnected_job && (tr->trid == 0)) {
			cf_info(AS_SCAN, "disconnected job needs trid, ignoring request");
			if (tr->msgp) cf_free(tr->msgp);
			return -2;
		}

		scan_fail_on_cluster_change =
			AS_MSG_FIELD_SCAN_FAIL_ON_CLUSTER_CHANGE & scan_options->data[0] ? true : false;
		if (scan_fail_on_cluster_change &&
				cf_atomic32_get(g_config.migrate_progress_send) > 0) {
			cf_info(AS_SCAN, "not starting scan because migration pending ");
			if (tr->msgp) cf_free(tr->msgp);
			return -3;
		}

		scan_priority = AS_MSG_FIELD_SCAN_PRIORITY(scan_options->data[0]);
		scan_pct      = scan_options->data[1];
	}


	// JOB SETUP
	tscan_job *job = tscan_job_create(tr->trid);
	if (!job) {
		if (tr->msgp) cf_free(tr->msgp);
		return -2;
	}

	// Have a pointer to msgp to free it later.
	job->msgp = tr->msgp;
	if (as_aggr_call_init(&job->agg_call, tr, job, &scan_aggr_caller_qintf,
			&tscan_agg_istream_hooks, &tscan_agg_ostream_hooks, ns, true) == 0) {
		job->hasudf = false;
		job->scan_type = SCAN_AGGR;
	}
	else if (udf_call_init(&job->call, tr)) {
		job->hasudf = false;
		job->scan_type = SCAN_NORMAL;
	}
	else {
		job->hasudf = true;
		if (job->call.udf_type != AS_SCAN_UDF_OP_BACKGROUND) {
			// Eventually we will support this -- but for now, it is a request of an
			// unsupported feature (-5).
			cf_info(AS_SCAN, "Only Background Scan UDF supported !!");
			scan_job_release_and_destroy(job);
			return -5;
		} else {
			job->scan_type = SCAN_UDF_BG;
		}
	}

	job->binlist = tscan_binlist_from_op(ns, &tr->msgp->msg);
	if (!job->binlist) {
		cf_info(AS_SCAN, "NO bins specified select all");
	}

	job->ns                     = ns;
	job->set_id                 = set_id;
	job->fail_on_cluster_change = scan_fail_on_cluster_change;
	job->scan_pct               = scan_pct;

	if (tr->msgp->msg.info1 & AS_MSG_INFO1_GET_NOBINDATA) {
		job->nobindata          = true;
		cf_detail(AS_SCAN, "received scan request to scan only digests");
	}

	job->job_type               = SCAN_JOB_TYPE_PARTITION;
	if (scan_priority == SCAN_PRIORITY_AUTO) {
		job->n_threads = 3;
		// For data-on-disk, will look at how many disks we have.
		if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
			as_storage_attributes s_attr;
			as_storage_namespace_attributes_get(ns, &s_attr);
			job->n_threads      = s_attr.n_devices > MAX_SCAN_THREADS
									? MAX_SCAN_THREADS : s_attr.n_devices;
		}
	}
	if (scan_priority == SCAN_PRIORITY_LOW) {
		job->n_threads          = 1;
	}
	if (scan_priority == SCAN_PRIORITY_MEDIUM) {
		job->n_threads          = 3;
	}
	if (scan_priority == SCAN_PRIORITY_HIGH) {
		job->n_threads          = 5;
	}

	cf_info(AS_SCAN, "scan option: Fail if cluster change %s", scan_fail_on_cluster_change ? "True" : "False");
	cf_info(AS_SCAN, "scan option: Background Job %s", scan_disconnected_job ? "True" : "False");
	cf_info(AS_SCAN, "scan option: priority is %d n_threads %d job_type %d", scan_priority, job->n_threads, job->job_type);
	cf_info(AS_SCAN, "scan option: scan_pct is %d ", scan_pct);
	int hp_rc;
	// Add job to global hash for tracking purposes.
	if (RCHASH_OK != (hp_rc = rchash_put_unique(g_scan_job_hash, &job->tid, sizeof(job->tid), job))) {
		cf_warning(AS_SCAN, "not starting scan %d because rchash_put() failed with error %d", job->tid, hp_rc);
		scan_job_release_and_destroy(job);
		return -2;
	}

	// If successful - for a connected job tr->fd_h is transferred to job, and
	// for a disconnected job, ack is sent to client and file handle released.
	// If not successful - rchash_delete() calls tscan_job_destructor() (and
	// tr->proto_fd_h remains valid so caller can send error msg to client).
	int rsp = 0;
	if (job->hasudf) {
		rsp = tscan_start_udfjob(job, tr, scan_disconnected_job);
	} else {
		rsp = tscan_start_job(job, tr, scan_disconnected_job);
	}

	if (rsp < 0) {
		cf_warning(AS_SCAN, "Failed to start %s job rv = %d, Aborting Scan ...",
				job->hasudf ? "UDF" : "", rsp);
		rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid));
		rsp = -2;
	} else {
		cf_atomic_int_incr(&g_config.tscan_initiate);
	}
	return(rsp);
}

// Call back function from UDF to add result to reply buffer.
void
tscan_add_result(as_result *res, udf_call *call, void *udata)
{
	//tscan_task_data *u = (tscan_task_data *)udata;
}

// Creates an internal transaction for per record UDF execution triggered
// from inside generator. The generator could be scan job generating digest
// or query generating digest.
int
as_internal_scan_udf_txn_setup(tr_create_data * d)
{
	tscan_job *job = (tscan_job *)d->udata;
	// Throttle the number of transactions that we queue in the transaction
	// queue to a fixed number.
	// TODO: Stop polling here.
	while(job->uit_queued >= (MAX_SCAN_UDF_TRANSACTIONS * job->n_threads)) {
		cf_detail(AS_SCAN, "UDF: scan transactions [%d] exceeded the maximum "
				"configured limit", job->uit_queued);

		// Another way of tuning the scan job, sleep depending on the priority of the job.
		// Lower the priority, longer the sleep. We have seen the CPU usage go down
		// from 350% in a 4 node cluster with high priority to 30% with low priority.
		switch (job->n_threads) {
			case 1:
				usleep(10000);
				break;
			case 3:
				usleep(100);
				break;
			case 5:
			default:
				usleep(3);
				break;
		}
	}

	as_transaction tr;
	memset(&tr, 0, sizeof(as_transaction));
	// Pass on the create metadata structure to create an internal transaction.
	if (as_transaction_create(&tr, d)) {
		return -1;
	}

	tr.udata.req_cb    = as_tscan_udf_tr_complete;
	tr.udata.req_udata = d->udata;
	tr.udata.req_type  = UDF_SCAN_REQUEST;
	tr.flag            |= AS_TRANSACTION_FLAG_INTERNAL;

	cf_atomic_int_incr(&job->uit_queued);
	cf_detail(AS_SCAN, "UDF: [%d] internal transactions enqueued", job->uit_queued);

	cf_rc_reserve(job);

	// Reset start time.
	tr.start_time = cf_getns();
	if (0 != thr_tsvc_enqueue(&tr)) {
		// TODO: should you drop or requeue?. What is the implication of a drop?
		cf_warning(AS_SCAN, "UDF: Failed to queue transaction for digest %"PRIx64", "
				"number of transactions enqueued [%d] .. dropping current "
				"transaction.. ", tr.keyd, job->uit_queued);
		cf_free(tr.msgp);
		tr.msgp = 0;
		cf_rc_release(job);
		cf_atomic_int_decr(&job->uit_queued);
		return -1;
	}
	return 0;
}

void
tscan_ll_recl_destroy_fn(cf_ll_element *ele)
{
	ll_recl_element * node = (ll_recl_element *) ele;
	if (node) {
		if (node->dig_arr) {
			cf_free(node->dig_arr);
		}
		cf_free(node);
	}
}

extern dig_arr_t * getDigestArray(void);

int
tscan_add_digest_list(cf_ll * recl, cf_digest * digest, int * dnum)
{
	cf_ll_element *ele = recl->tail;
	bool create = ele == NULL;
	dig_arr_t *dt;
	if (!create) {
		dt = ((ll_recl_element*)ele)->dig_arr;
		if (dt->num == 51) { // NUM_DIGS_PER_ARR
			create = true;
		}
	}
	if (create) {
		dt = getDigestArray();
		if (!dt) {
			return -1;
		}
		ll_recl_element * node;
		node = cf_malloc(sizeof(ll_recl_element));
		node->dig_arr = dt;
		cf_ll_append(recl, (cf_ll_element *)node);
	}
	memcpy(&dt->digs[dt->num], digest, CF_DIGEST_KEY_SZ);
	dt->num++;
	if (dnum) {
		*dnum = *dnum + 1;
	}
	return 0;
}

void
tscan_recl_cleanup(cf_ll *recl)
{
	if (recl) {
		cf_ll_iterator * iter = NULL;
		iter                  = cf_ll_getIterator(recl, true /*forward*/);
		if (iter) {
			cf_ll_element *ele;
			while ((ele = cf_ll_getNext(iter))) {
				ll_recl_element * node;
				node = (ll_recl_element *) ele;
				dig_arr_t * dt = node->dig_arr;
				node->dig_arr =  NULL;
				if (dt) {
					releaseDigArrToQueue((void *)dt);
				}
			}
		}
		cf_ll_releaseIterator(iter);
	}
}

int
tscan_ll_recl_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
tscan_add_aggr_result(char *res, tscan_task_data * task, bool success)
{
	const as_val * v = (as_val *) as_string_new (res, false);
	tscan_add_val_response(task, v, success);
	as_val_destroy(v);
}

extern int as_aggr__process(as_aggr_call * ap_call, cf_ll * ap_recl, void * udata, as_result * ap_res);

typedef struct tscan_aggr_tr_udata {
	cf_ll *recl;
	tscan_task_data *task;
} tscan_aggr_tr_udata_t;

void
tscan_aggr_tree_reduce_fn(as_index_ref *r_ref, void *udata)
{
	as_index *r = r_ref->r;
	tscan_aggr_tr_udata_t * d_ptr = (tscan_aggr_tr_udata_t *)(udata);
	// If this is a valid set, check against the set of the record.
	if (d_ptr->task->set_id != INVALID_SET_ID) {
		if (as_index_get_set_id(r) != d_ptr->task->set_id) {
			cf_detail(AS_SCAN, "Set mismatch %s %s",
					as_namespace_get_set_name(d_ptr->task->ns, d_ptr->task->set_id),
					as_namespace_get_set_name(d_ptr->task->ns, as_index_get_set_id(r)));
			if (d_ptr->task->si) {
				cf_atomic64_decr(&d_ptr->task->si->stats.recs_pending);
			}
			cf_atomic_int_incr(&(d_ptr->task->pjob->n_obj_set_diff));
			as_record_done(r_ref, d_ptr->task->ns);
			return;
		}
	}
	tscan_add_digest_list (d_ptr->recl, &(r->key), NULL);
	as_record_done(r_ref, d_ptr->task->ns);
}


//
// Reduce a tree, build a response.
//
// Callback function that gets called for every primary-index generated from
// tree-reduce.
// This is called for all scans including sindex populate/populate-all,
// UDF scans etc.
//
void
tscan_tree_reduce(as_index_ref *r_ref, void *udata)
{
	tscan_task_data *u = (tscan_task_data *) udata;
	uint64_t buf_builder_size;
	uint64_t resp_buf_limit;
	if (u->ns->ldt_enabled) {
		buf_builder_size = u->ns->scan_ldt_buff_max;
		resp_buf_limit = buf_builder_size / 2;
	} else {
		buf_builder_size = INITIAL_BUFBUILDER_SIZE;
		resp_buf_limit = SCAN_PROTO_LIMIT;
	}

	if (*u->aborted) {
		as_record_done(r_ref, u->ns);
		return;
	}

	cf_buf_builder **bb_r = &(u->bb);

	as_index *r = r_ref->r;
	// Check to see that this isn't an expired record waiting to die.
	if (r->void_time && r->void_time < as_record_void_time_get()) {
		if (u->si) cf_atomic64_decr(&u->si->stats.recs_pending);
		cf_atomic_int_incr(&(u->pjob->n_obj_expired));
		as_record_done(r_ref, u->ns);
		if (SCAN_JOB_IS_POPULATOR(u->pjob)) {
			uint64_t scanned = (uint64_t)cf_atomic_int_get(u->pjob->n_obj_scanned) + 
							(uint64_t)cf_atomic_int_get(u->pjob->n_obj_set_diff) + 
							(uint64_t)cf_atomic_int_get(u->pjob->n_obj_expired);
			as_sindex_ticker(u->pjob->ns, u->si, scanned, u->pjob->start_time);
		}
		return;
	}

	// If this is a valid set, check against the set of the record.
	if (u->set_id != INVALID_SET_ID) {
		if (as_index_get_set_id(r) != u->set_id) {
			cf_detail(AS_SCAN, "Set mismatch %s %s",
					as_namespace_get_set_name(u->ns, u->set_id),
					as_namespace_get_set_name(u->ns, as_index_get_set_id(r)));
			if (u->si) cf_atomic64_decr(&u->si->stats.recs_pending);
			cf_atomic_int_incr(&(u->pjob->n_obj_set_diff));
			as_record_done(r_ref, u->ns);
			if (SCAN_JOB_IS_POPULATOR(u->pjob)) {
				uint64_t scanned = (uint64_t)cf_atomic_int_get(u->pjob->n_obj_scanned) + 
							(uint64_t)cf_atomic_int_get(u->pjob->n_obj_set_diff) + 
							(uint64_t)cf_atomic_int_get(u->pjob->n_obj_expired);
				as_sindex_ticker(u->pjob->ns, u->si, scanned, u->pjob->start_time);
			}
			return;
		}
	}

	// Increment the number of objects scanned outside of all the checks for
	// udf/si, etc.
	// It is important to take in account the objects from different set and expired objects
	uint64_t n_obj_scanned = (uint64_t)cf_atomic_int_incr(&(u->pjob->n_obj_scanned)) + 
							(uint64_t)cf_atomic_int_get(u->pjob->n_obj_set_diff) + 
							(uint64_t)cf_atomic_int_get(u->pjob->n_obj_expired);

	// Execute if UDF Call is defined. UDF cannot be executed inline. The logic
	// is to create a internal transaction.
	if (u->call) {
		// Fill up the create transaction structure with the digest and udf call.
		tr_create_data d;
		memset(&d, 0, sizeof(tr_create_data));
		d.digest   = r->key;
		d.call     = u->call;
		d.ns       = u->ns;
		d.msg_type = AS_MSG_INFO2_WRITE; // UDF execution is
		// Write transaction type.
		d.fd_h     = u->fd_h;
		d.udata    = u->pjob;

		// Release the record reference lock before enqueuing the transaction,
		// it would be taken care of while the transaction is processed.
		as_record_done(r_ref, u->ns);

		// Setup the internal udf transaction / throttle / enqueue.
		// TODO: what to do with ret.
		as_internal_scan_udf_txn_setup(&d);
		goto END;
	}

	// If there is no udf call associated, go through the normal process.
	if (u->nobindata) {
		if (! *bb_r) {
			*bb_r = cf_buf_builder_create_size(buf_builder_size);
			cf_atomic_int_add(&u->pjob->mem_buf, (*bb_r)->alloc_sz);
		}

		size_t old_allocsz = (*bb_r)->alloc_sz;

		if (as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED)) {
			as_storage_rd rd;
			as_storage_record_open(u->ns, r, &rd, &r->key);
			as_msg_make_response_bufbuilder(r, &rd, bb_r, true, NULL, true, true, true, u->binlist);
			as_storage_record_close(r, &rd);
		}
		else {
			as_msg_make_response_bufbuilder(r, NULL, bb_r, true, u->ns->name, true, false, true, u->binlist);
		}

		if ((*bb_r)->alloc_sz > old_allocsz) {
			cf_atomic_int_add(&u->pjob->mem_buf, (*bb_r)->alloc_sz - old_allocsz);
		}
	}
	else {
		// Make sure it's brought in from storage.
		as_storage_rd rd;
		as_storage_record_open(u->ns, r, &rd, &r->key);
		rd.n_bins = as_bin_get_n_bins(r, &rd);

		// Figure out which bins you want - for now, all.
		as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

		rd.bins = as_bin_get_all(r, &rd, stack_bins);
		rd.n_bins = as_bin_inuse_count(&rd);
		uint64_t memory_bytes = 0;
		if (rd.ns->storage_data_in_memory) {
			memory_bytes = as_storage_record_get_n_bytes_memory(&rd);
		}

		// This reduce function is called for loading data into secondary index.
		if (SCAN_JOB_IS_POPULATOR(u->pjob)) {
			
			if (u->si) {
				cf_atomic64_decr(&u->si->stats.recs_pending);
				SINDEX_GRLOCK();
				AS_SINDEX_RESERVE(u->si);
				SINDEX_GUNLOCK();
				as_sindex_put_rd(u->si, &rd);
				as_sindex_ticker(u->pjob->ns, u->si, n_obj_scanned, u->pjob->start_time);
			} else {
				// No input si mentioned, so go ahead and populate all of the entries.
				// SCAN_JOB_TYPE_SINDEX_POPULATEALL case.
				// This function will parse all the ns si's, reserve it and read the data.
				as_sindex_putall_rd(u->ns, &rd);
				as_sindex_ticker(u->pjob->ns, NULL, n_obj_scanned, u->pjob->start_time);
			}

		} else {
			if (! *bb_r) {
				*bb_r = cf_buf_builder_create_size(buf_builder_size);
				cf_atomic_int_add(&u->pjob->mem_buf, (*bb_r)->alloc_sz);
			}
			size_t old_allocsz = (*bb_r)->alloc_sz;
			as_msg_make_response_bufbuilder(r, &rd, bb_r, false, NULL, true, true, true, u->binlist);
			if ((*bb_r)->alloc_sz > old_allocsz) {
				cf_atomic_int_add(&u->pjob->mem_buf, (*bb_r)->alloc_sz - old_allocsz);
			}
		}
		as_storage_record_close(r, &rd);
	}

	as_record_done(r_ref, u->ns);

	if (! SCAN_JOB_IS_POPULATOR(u->pjob)) {
		// If we exceed the proto size limit, send accumulated data back to client
		// and reset the buf-builder to start a new proto.
		if ((*bb_r)->used_sz > resp_buf_limit) {
			pthread_mutex_lock(&u->pjob->LOCK);

			if (0 != tscan_send_response_to_client(u->pjob, (*bb_r)->buf, (*bb_r)->used_sz)) {
				*u->aborted = true;
			}

			pthread_mutex_unlock(&u->pjob->LOCK);

			// Hack - didn't want to add a reset method to cf_buf_builder.
			(*bb_r)->used_sz = 0;
		}

		if (IS_SCAN_JOB_ABORTED(u->pjob)) {
			*u->aborted = true;
			u->pjob->result = AS_PROTO_RESULT_FAIL_SCAN_ABORT;
		}
	}

END:
	u->yield_count++;
	if (u->yield_count % g_config.scan_priority == 0) {
		// UDF should sleep an order of magnitude more.
		if (u->call) {
			usleep(10 * g_config.scan_sleep);
		} else {
			usleep(g_config.scan_sleep);
		}
	}
}

//
// Sends the actual proto response to the requesting client.
//
// -1 fd no longer there
//
int
tscan_send_response_to_client( tscan_job *job, uint8_t *buf, size_t len)
{
	// Make a proto header.
	as_proto proto;
	proto.version = PROTO_VERSION;
	proto.type = PROTO_TYPE_AS_MSG;
	proto.sz = len;
	as_proto_swap(&proto);

	if (job->fd_h == 0) return(-1);

	// Keep the reaper at bay.
	job->fd_h->last_used = cf_getms();

	int pos = 0, rv;
	while (pos < 8) {
		rv = send(job->fd_h->fd, ((uint8_t *) &proto) + pos, 8 - pos , MSG_NOSIGNAL | MSG_MORE);
		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_info(AS_SCAN, "tid %"PRIu64": scan send response error returned %d errno %d fd %d ", job->tid, rv, errno, job->fd_h->fd);
				AS_RELEASE_FILE_HANDLE(job->fd_h);
				job->fd_h = 0;
				return(-1);
			}
		}
		else {
			pos += rv;
		}
	};
	pos = 0;
	while (pos < len) {
		rv = send(job->fd_h->fd, buf + pos, len - pos, MSG_NOSIGNAL );
		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_info(AS_SCAN, "tid %"PRIu64": scan send response error returned %d errno %d fd %d ", job->tid, rv, errno, job->fd_h->fd);
				AS_RELEASE_FILE_HANDLE(job->fd_h);
				job->fd_h = 0;
				return(-1);
			}
		}
		else {
			pos += rv;
		}
	}
	cf_atomic_int_add(&job->net_io_bytes, (len + 8));   // 8 bytes for proto header
	cf_debug(AS_SCAN, "tid %"PRIu64": response to client fd %d bytes %u", job->tid, job->fd_h->fd, len);
	return(0);
}

//
// Send "finished" to the client.
//
// -1 fd no longer there
//
int
tscan_send_fin_to_client(tscan_job *job, uint32_t result_code)
{
	cl_msg      m;

	if (job == NULL) {
		cf_info(AS_SCAN, "no job");
		return 0;
	}
	if (job->fd_h == NULL) {
		cf_info(AS_SCAN, "tid %"PRIu64": no more fh. Probably client closed up connection", job->tid);
		return 0;
	}
	int fd = job->fd_h->fd;

	cf_info(AS_SCAN, "Scan Job %"PRIu64": send final message: fd %d result %d", job->tid, fd, result_code);

	m.proto.version = PROTO_VERSION;
	m.proto.type = PROTO_TYPE_AS_MSG;
	m.proto.sz = sizeof(as_msg);
	as_proto_swap(&m.proto);
	m.msg.header_sz = sizeof(as_msg);
	m.msg.info1 = 0;
	m.msg.info2 = 0;
	m.msg.info3 = AS_MSG_INFO3_LAST;
	m.msg.unused = 0;
	m.msg.result_code = result_code;
	m.msg.generation = 0;
	m.msg.record_ttl = 0;
	m.msg.transaction_ttl = 0;
	m.msg.n_fields = 0;
	m.msg.n_ops = 0;
	as_msg_swap_header(&m.msg);

	uint8_t *buf = (uint8_t *) &m;

	int pos = 0;
	while (pos < sizeof(m)) {
		int rv = send(fd, buf + pos, sizeof(m) - pos, MSG_NOSIGNAL);
		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_info(AS_SCAN, "write fin %d errno %d fd %d", rv, errno, fd);
				return(-1);
			}
		}
		else {
			pos += rv;
		}
	}
	cf_atomic_int_add(&job->net_io_bytes, (sizeof(m)));
	return(0);
}

void *
scan_udf_job_manager(void *q_to_wait_on)
{
	cf_queue *job_queue = (cf_queue *)q_to_wait_on;
	do {
		tscan_job *job = NULL;
		cf_detail(AS_SCAN, "Waiting for job queue");
		if (0 != cf_queue_pop(job_queue, &job, CF_QUEUE_FOREVER)) {
			cf_crash(AS_SCAN, "scan_partition: unable to pop from scan worker queue");
		}

		pthread_mutex_lock(&job->LOCK);

		// If the job is done, figure out who to notify, and get off this queue.
		if (IS_SCAN_JOB_DONE(job)) {
			cf_detail(AS_SCAN, "UDF: Popped existing scan UDF job from the queue [%"PRIu64" %p %p] @ partition %d",
					job->tid, job, job->fd_h, job->cur_partition_id);

			// Special case: might be marked done but still has a transaction dangling, wait.
			if (cf_atomic_int_get(job->uit_queued) > 0) {
				cf_detail(AS_SCAN, "UDF: Waiting for all transactions to "
						"complete, [%d] remaining", job->uit_queued);
				pthread_mutex_unlock(&job->LOCK);
				// TODO: what if this fails?
				cf_queue_push(job_queue, &job);
				goto NextElement;
			}
			// Release reference for the queue element.
			if ( IS_SCAN_JOB_ABORTED(job) && !IS_SCAN_JOB_PENDING(job)) {
				cf_atomic_int_incr(&g_config.tscan_aborted);
				cf_info(AS_SCAN, "UDF: Transactions completed, User Aborting scan job %"PRIu64"", job->tid);
				scan_udf_job_done(job, AS_PROTO_RESULT_FAIL_SCAN_ABORT);  // TODO: what is response code
			}
			// If the scan job is pending, the job scan hash cannot be deleted as it is being persisted for
			// a fixed amount of time to retrieve information on the job, so do not call scan_udf_job_done.
			// That will be called only once after the job is done.
			else if(!IS_SCAN_JOB_PENDING(job)) {
				cf_info(AS_SCAN, "UDF: Transactions completed, finishing scan job %"PRIu64"", job->tid);
				scan_udf_job_done(job, 0);
			}

			// If the job is around for less than 5 min,  set the pending job flag and queue again.
			// This is to get the stats of the job even after its completion.
			if (job->end_time && (cf_getms() - job->end_time < SCAN_JOB_HASH_PERSIST_TIME)) {
				SCAN_JOB_PENDING_ON(job);
				pthread_mutex_unlock(&job->LOCK);
				// We have to keep the job around for stats, but free fd now.
				if (job->fd_h) {
					AS_RELEASE_FILE_HANDLE(job->fd_h);
					job->fd_h = NULL;
				}
				cf_queue_push(job_queue, &job);
				goto NextElement;
			}

			cf_debug(AS_SCAN, "UDF: Done Job %lu have finished waiting and will now be removed ",
					job->tid);

			// Job can now be happily deleted from the hash.
			SCAN_JOB_PENDING_OFF(job);
			pthread_mutex_unlock(&job->LOCK);

			// Destroy now and don't requeue.
			// Using similar thinking as with default scan.
			rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid));
			goto NextElement;
		}

		// If the job is not fully initialized, initialize.
		if (!job->udata) {
			cf_detail(AS_SCAN, "UDF: Popped new scan UDF job from the queue [%"PRIu64" %p %p]", job->tid, job, job->fd_h);
			scan_job_partitions_tracker * ptracker = cf_malloc(sizeof(scan_job_partitions_tracker));
			if (ptracker == NULL) {
				// Release reference for the queue element.
				cf_warning(AS_SCAN, "UDF: Could not allocate memory for tracker .. Aborting Scan UDF !!");
				scan_udf_job_done(job, -2);  // TODO: What is the response code?
				pthread_mutex_unlock(&job->LOCK);
				rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid));
				goto NextElement;
			}
			memset(ptracker, 0, sizeof(scan_job_partitions_tracker));
			job->udata = ptracker;
			cf_atomic_int_incr(&g_config.tscan_initiate);
			cf_detail(AS_SCAN, "UDF: Successfully initiated scan UDF job [%"PRIu64" %p %p]", job->tid, job, job->fd_h);
		}

		int rsp = 0;
		if ( !IS_SCAN_JOB_ABORTED(job) ) {
			// This unlocks the job.
			rsp = tscan_enqueue_udfjob(job);
		} else {
			pthread_mutex_unlock(&job->LOCK);
		}

		// Push the job back into the queue for the next iteration if enqueing of udf job
		// is successful in this iteration.
		if (0 == rsp) {
			if (0 != cf_queue_push(job_queue, &job)) {
				cf_warning(AS_SCAN, "UDF: Transactions Aborted, Internal error .. failed queue push!!! %"PRIu64"", job->tid);
				scan_udf_job_done(job, -2);  // TODO: what is the response code?
				pthread_mutex_unlock(&job->LOCK);
				rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid));
				goto NextElement;
			}
			cf_detail(AS_SCAN, "UDF: Pushed existing scan UDF job into the queue [%"PRIu64" %p %p] @ partition %d",
					job->tid, job, job->fd_h, job->cur_partition_id);
		}
NextElement:
		usleep(1000);
	} while(1);
	return (0);
}

int
as_tscan_abort(uint64_t trid)
{
	tscan_job *job = NULL;
	// Get the job from the transaction id.
	if (RCHASH_OK != rchash_get(g_scan_job_hash, &trid, sizeof(trid), (void **) &job)) {
		cf_info(AS_SCAN, "Scan job with transaction id [%"PRIu64"] does not exist anymore", trid);
		return -1;
	}
	pthread_mutex_lock(&job->LOCK);
	SCAN_JOB_ABORTED_ON(job);
	pthread_mutex_unlock(&job->LOCK);
	scan_job_release_and_destroy(job);
	return 0;
}

// For every job in the g_scan_job_hash, print the statistics.
int
as_tscan_list_job_reduce_fn (void *key, uint32_t keylen, void *object, void *udata)
{
	tscan_job * job = (tscan_job*)object;
	cf_dyn_buf * db = (cf_dyn_buf*) udata;
	cf_dyn_buf_append_string(db, "job_id=");
	cf_dyn_buf_append_uint64(db, job->tid);

	switch (job->scan_type) {
		case SCAN_NORMAL:
			cf_dyn_buf_append_string(db, ":job_type=SCAN_NORMAL");
			break;
		case SCAN_UDF_BG:
			cf_dyn_buf_append_string(db, ":job_type=SCAN_UDF_BG");
			break;
		case SCAN_UDF_FG:
			cf_dyn_buf_append_string(db, ":job_type=SCAN_UDF_FG");
			break;
		case SCAN_SINDEX:
			cf_dyn_buf_append_string(db, ":job_type=SCAN_SINDEX");
			break;
		default:
			cf_dyn_buf_append_string(db, ":job_type=undefined");
	}
	// Update namespace and set information.
	cf_dyn_buf_append_string(db, ":namespace=");
	if(job->ns) {
		cf_dyn_buf_append_string(db, cf_atomic_int_get(job->ns->name));
	}

	const char * set_name = as_namespace_get_set_name(job->ns, job->set_id);
	cf_dyn_buf_append_string(db, ":set=");
	if (set_name) {
		cf_dyn_buf_append_string(db, set_name);
	}
	else {
		cf_dyn_buf_append_string(db, "null");
	}

	// Status of the job.
	if (IS_SCAN_JOB_ABORTED(job)) {
		cf_dyn_buf_append_string(db, ":job_status=ABORTED");
	}
	else if (IS_SCAN_JOB_DONE(job)) {
		cf_dyn_buf_append_string(db, ":job_status=DONE");
	}
	else {
		cf_dyn_buf_append_string(db, ":job_status=IN PROGRESS");
	}

	cf_dyn_buf_append_string(db, ":job_progress(%)=");
	cf_dyn_buf_append_uint64(db, cf_atomic_int_get(((job->n_partitions_scanned) * 100) / AS_PARTITIONS));

	switch (job->n_threads) {
		case 1:
			cf_dyn_buf_append_string(db, ":job_priority=low");
			break;
		case 3:
			cf_dyn_buf_append_string(db, ":job_priority=medium");
			break;
		case 5:
			cf_dyn_buf_append_string(db, ":job_priority=high");
			break;
		default:
			cf_dyn_buf_append_string(db, ":job_priority=undefined");
	}

	if ((job->scan_type == SCAN_UDF_BG) ||
			(job->scan_type == SCAN_UDF_FG)) {
		// Update udf information.
		if(job->call.filename) {
			cf_dyn_buf_append_string(db, ":udf_filename=");
			cf_dyn_buf_append_string(db, job->call.filename);
		}
		if(job->call.function) {
			cf_dyn_buf_append_string(db, ":udf_function=");
			cf_dyn_buf_append_string(db, job->call.function);
		}
		cf_dyn_buf_append_string(db, ":udf_success=");
		cf_dyn_buf_append_uint64(db, cf_atomic_int_get(job->n_obj_udf_success));

		cf_dyn_buf_append_string(db, ":udf_failed=");
		cf_dyn_buf_append_uint64(db, cf_atomic_int_get(job->n_obj_udf_failed));

		cf_dyn_buf_append_string(db, ":udf_updated=");
		cf_dyn_buf_append_uint64(db, cf_atomic_int_get(job->n_obj_udf_updated));

		cf_dyn_buf_append_string(db, ":udf_avg_run_time(ms)=");
		cf_dyn_buf_append_int(db, job->uit_completed ? (job->uit_total_run_time / job->uit_completed) : 0);
	}

	// Statistics
	cf_dyn_buf_append_string(db, ":scanned_records=");
	cf_dyn_buf_append_uint64(db, cf_atomic_int_get(job->n_obj_scanned));

	cf_dyn_buf_append_string(db, ":expired_records=");
	cf_dyn_buf_append_uint64(db, cf_atomic_int_get(job->n_obj_expired));

	cf_dyn_buf_append_string(db, ":set_diff_records=");
	cf_dyn_buf_append_uint64(db, cf_atomic_int_get(job->n_obj_set_diff));

	// The time taken for actual processing of the job.
	cf_dyn_buf_append_string(db, ":processing_time(ms)=");
	cf_dyn_buf_append_uint64(db, job->end_time > job->start_time ? job->end_time - job->start_time : 0);

	uint64_t curr_time = cf_getms();
	cf_dyn_buf_append_string(db, ":time_elapsed(ms)=");
	cf_dyn_buf_append_uint64(db, (curr_time - job->start_time));
	cf_dyn_buf_append_string(db, ";");
	return 0;
}

int
as_tscan_list(char *name, cf_dyn_buf *db)
{
	uint32_t size = rchash_get_size(g_scan_job_hash);
	// No elements in the scan job hash, return failure.
	if(!size) {
		cf_dyn_buf_append_string(db, "No running scans");
	}
	// Else go through all the jobs in the hash and list their statistics.
	else {
		rchash_reduce(g_scan_job_hash, as_tscan_list_job_reduce_fn, db);
	}
	return 0;
}

// This function gets called for both scan and udf on a per-partition basis.
void *
tscan_partition_thr(void *q_to_wait_on)
{
	cf_queue *worker_queue = (cf_queue *)q_to_wait_on;

	do {
		scan_job_workitem workitem;
		tscan_task_data u;
		memset(&u, 0, sizeof(tscan_task_data));
		tscan_job *job = NULL;

		bool job_early_terminate = false;
		uint32_t partition_state =  SCAN_PARTITION_STATE_FINISHED;

		cf_debug(AS_SCAN, "waiting for workitem");
		if (0 != cf_queue_pop(worker_queue, &workitem, CF_QUEUE_FOREVER)) {
			cf_crash(AS_SCAN, "scan_partition: unable to pop from scan worker queue");
		}
		// Getting a work item.
		if (RCHASH_OK != rchash_get(g_scan_job_hash, &workitem.tid, sizeof(workitem.tid), (void **) &job)) {
			cf_debug(AS_SCAN, "scan_partition: job %d no longer exists %d. Do nothing", workitem.tid, workitem.pid);
			continue;
		}

		// We've found the corresponding job.
		// Check the cluster_key and see if we need to cancel out.
		if (job->fail_on_cluster_change && job->cluster_key != as_paxos_get_cluster_key()) {
			cf_info(AS_SCAN, "scan_partition: job early terminate due to cluster change %d {%s:%d}.", workitem.tid, job->ns->name, workitem.pid);
			job->result = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
			job_early_terminate = true;
			goto WorkItemDone;
		}

		// Received abort signal from the client.
		if( IS_SCAN_JOB_ABORTED(job) ) {
			cf_atomic_int_incr(&g_config.tscan_aborted);
			job_early_terminate = true;
			cf_detail(AS_SCAN, "UDF: Client initiated abort on job %"PRIu64"", job->tid);
			job->result = AS_PROTO_RESULT_FAIL_SCAN_ABORT;
			goto WorkItemDone;
		}

		// Iterating through the partition data.
		as_partition_reservation rsv;
		AS_PARTITION_RESERVATION_INIT(rsv);
		// Skip for all the nodes except qnode. Secondary index query is
		// serviced on qnode so it is a good idea to build it quickly without
		// waiting for migration to finish.

		// Following condition is true for sindex-populate and sindex-populate-all.
		// Former : one valid sindex entry is getting inserted preceded by partition reservation.
		// Latter : All sindex entries are getting populated during warm-restart and cold-restart with SSD.
		if (SCAN_JOB_IS_POPULATOR(job)) {
			// For index population case ... we have two situations
			// 1. In normal operating environment we do read reservation so
			//    it gets created on both master and replica.
			// 2. For the case where sindex is loading lazily and migrations are
			//    on sindex should be populated at the qnode as well. Because
			//    we cannot wait for entire migration to finish to load sindex
			//    up.
			//
			// Other options would be to do migrate reserve but you do not want
			// to create indexes on all the nodes ... because query is not going
			// to look at it anyway.
			//
			// Side effect: It is going to put read load on ssd.
			// To keep the sindex updated after paxos state change, we are taking a migrate
			// reserve on the partition.
			// Drwaback is that it will consume a lot of memory.
			// TODO: Find a smart way to solve this problem.
			as_partition_reserve_migrate(job->ns, workitem.pid, &rsv, 0);
		} else {
			if (0 != as_partition_reserve_write(job->ns, workitem.pid, &rsv, NULL, NULL)) {
				cf_debug(AS_SCAN, "scan_partition: could not reserve write: %d {%s:%d}", workitem.tid, job->ns->name, workitem.pid);
				partition_state = SCAN_PARTITION_STATE_NOT_AVAIL;
				goto WorkItemDone;
			}
		}
		// Calculate required sample objects from each partition.
		uint32_t sample_obj_cnt = (uint32_t)(((uint64_t)rsv.p->vp->elements * (uint64_t)job->scan_pct) / 100);
		cf_debug(AS_SCAN, "scan_partition: need to scan %d objects from partition %d", sample_obj_cnt, workitem.pid);

		uint64_t buf_builder_size;
		if (job->ns->ldt_enabled) {
			buf_builder_size = job->ns->scan_ldt_buff_max;
		} else {
			buf_builder_size = INITIAL_BUFBUILDER_SIZE;
		}	

		if (job->fd_h) {
			u.bb = cf_buf_builder_create_size(buf_builder_size);
			if (u.bb == NULL) {
				cf_info(AS_SCAN, "scan_partition: could not create buf builder: %d {%s:%d}", workitem.tid, job->ns->name, workitem.pid);
				as_partition_release(&rsv);
				partition_state = SCAN_PARTITION_STATE_FAILED;
				goto WorkItemDone;
			}
			cf_atomic_int_add(&job->mem_buf, u.bb->alloc_sz);
			//cf_rc_reserve(job->fd_h);
			u.fd_h = job->fd_h;
		}

		u.ns          = job->ns;
		u.yield_count = 0;
		u.nobindata   = job->nobindata; // set to false for sindex populate & populate-all
		u.nodata      = job->fd_h ? false : true;
		u.set_id      = job->set_id;
		u.si          = job->si;  // in the case of SCAN_JOB_TYPE_SINDEX_POPULATEALL, si = null
		u.binlist     = job->binlist;
		u.rsv         = &rsv;
		u.job_id      = job->tid;
		u.pjob        = job;
		u.aborted     = &job_early_terminate;

		if (job->hasudf) {
			cf_detail(AS_SCAN, "UDF: Scan job %"PRIu64" has udf", job->tid);
			u.call = &job->call;
			u.aggr_call = NULL;
		} else if (job->scan_type == SCAN_AGGR) {
			u.call = NULL;
			u.aggr_call = &job->agg_call;
		} else {
			u.call = NULL;
			u.aggr_call = NULL;
		}

		// Skip if index is gone.
		if (SCAN_JOB_IS_POPULATOR(u.pjob)) {
			if (u.si) {
				if (!as_sindex_isactive(u.si)) {
					if (job->scan_state_logged != AS_SCAN_ABORTED) {
						cf_info(AS_SCAN, "Index is no more there bailing out of scan");
						job->scan_state_logged = AS_SCAN_ABORTED;
					}
					as_partition_release(&rsv);
					partition_state = SCAN_PARTITION_STATE_ABORTED;
					goto WorkItemDone;
				}
				if (u.si->desync_cnt > job->si_start_desync_cnt) {
					cf_detail(AS_SCAN, "The secondary index has gone out of sync from primary "
							"aborting scan ... index %d needs to be repaired ", u.si->imd->iname);
					as_partition_release(&rsv);
					partition_state = SCAN_PARTITION_STATE_ABORTED;
					goto WorkItemDone;
				}
			}
		}

		cf_atomic_int_incr(&g_config.scan_tree_count);
		// @TODO put in histogram.
		// Reduce into a bufbuilder.

		// This function calls an index-reduce on all the nodes of the tree, its left and right
		// and for each one of those index entries, call the cb-function : tscan_tree_reduce.
		if ( u.aggr_call ) {
			cf_ll * recl = cf_malloc(sizeof(cf_ll));

			if (!recl) {
				cf_warning(AS_SCAN, "unable to create digest list: out of memory");
				job_early_terminate = true;
			}
			else {
				cf_ll_init(recl, tscan_ll_recl_destroy_fn, false /*no lock*/);

				tscan_aggr_tr_udata_t tree_reduce_udata = {
					.recl = recl,
					.task = &u 
				};

				as_index_reduce(rsv.tree, tscan_aggr_tree_reduce_fn, (void *)&tree_reduce_udata);

				as_result   *res    = as_result_new();
				int ret             = as_aggr__process(u.aggr_call, recl, &u, res);
				if (ret != 0) {
					char *rs = as_module_err_string(ret);
					if (res->value != NULL) {
						as_string * lua_s   = as_string_fromval(res->value);
						char *      lua_err = (char *) as_string_tostring(lua_s);
						if (lua_err != NULL) {
							int l_rs_len = strlen(rs);
							rs = cf_realloc(rs, l_rs_len + strlen(lua_err) + 4);
							sprintf(&rs[l_rs_len], " : %s", lua_err);
						}
					}
					tscan_add_aggr_result(rs, &u, false);
					cf_free(rs);
					job_early_terminate = true;
				}

				as_result_destroy(res);
				tscan_recl_cleanup(recl);
				cf_ll_reduce(recl, true /*forward*/, tscan_ll_recl_reduce_fn, NULL);
				cf_free(recl);
			}
		}
		else {
			as_index_reduce_partial(rsv.tree, sample_obj_cnt, tscan_tree_reduce, (void *)&u);
		}

		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.scan_tree_count);

		if (!SCAN_JOB_IS_POPULATOR(job)) {
			// Send data back to client.
			if (u.bb && u.bb->used_sz) {
				pthread_mutex_lock(&job->LOCK);
				int sresp = tscan_send_response_to_client( job, u.bb->buf, u.bb->used_sz );
				if (sresp != 0) {
					job_early_terminate = true;
				}
				pthread_mutex_unlock(&job->LOCK);
			}
			if (u.bb) {
				cf_atomic_int_sub(&job->mem_buf, u.bb->alloc_sz);
				cf_buf_builder_free(u.bb);
			}
		}

WorkItemDone:
		pthread_mutex_lock(&job->LOCK);

		// Increment partitions-scanned counter here.
		tscan_job_update_pstatus(job, workitem.pid, partition_state);

		cf_debug(AS_SCAN, "Workitem id= %d, scanned partitions= %d, job id=%lu", workitem.pid, job->n_partitions_scanned, job->tid);

		bool clean_job = false;
		// Send fin to client if the whole job is done
		if (tscan_job_is_finished(job) || job_early_terminate) {
			if (job->job_type == SCAN_JOB_TYPE_SINDEX_POPULATE) {
				as_sindex_metadata *imd = (as_sindex_metadata *)job->si->imd;
				cf_detail(AS_SCAN, "job %"PRIu64" done for index %s.%s %d", job->tid, imd->ns_name, imd->iname, job_early_terminate);
				// NB it cleans up imd do not use it after this.
				if (job->si_start_desync_cnt == job->si->desync_cnt) {
					job->si->desync_cnt = 0;
					// A single scan successfully completed, zero out desync_cnt.
				}
				as_sindex_populate_done(job->si);
				clean_job = true;
			}
			else {
				// Do not exit from the scan job unless all the internal UDF transactions
				// queued by this guy are finished.
				if (job->hasudf) {
					cf_detail(AS_SCAN, "UDF: Last workitem finished [%"PRIu64" %p %p] @ %d outstanding %d ",
							job->tid, job, job->fd_h, job->uit_queued);
					SCAN_JOB_DONE_ON(job);
				} else {
					clean_job = true;
				}
			}
		}
		pthread_mutex_unlock(&job->LOCK);

		if (job->hasudf) {
			int i = 0;
			cf_queue_push(g_scan_job_slotq, &i);
		}

		if (clean_job) {
			// Note that we never get here for a UDF job - UDF jobs are removed
			// from g_scan_job_hash only in scan_udf_job_manager().

			// Note also that this call won't destroy and free the job, since
			// we're still within the hash_get's reference.
			if (RCHASH_OK == rchash_delete(g_scan_job_hash, &job->tid, sizeof(job->tid))) {
				cf_debug(AS_SCAN, "scan job %d for '%s' - %s", job->tid, job->ns->name,
						job_early_terminate ? "terminated early" : "completed");
			}
		}

		// Release the reference from the hash_get.
		scan_job_release_and_destroy(job);

		cf_debug(AS_SCAN, "finished workitem");
	} while(1);

	return(0);
}

static cf_atomic32   init_counter = 0;

void
as_tscan_init()
{
	// Make sure initialization is done only once.
	if (1 != cf_atomic32_incr(&init_counter)) {
		return;
	}

	// Global job hash to keep track of the job.
	rchash_create(&g_scan_job_hash, tscan_job_tid_hash, tscan_job_destructor, sizeof(uint64_t), 64, RCHASH_CR_MT_MANYLOCK);

	// Startup # of threads for partition based scanning.
	// Each thread has one queue. This allows the application to dynamically
	// pick how many threads a job will use.
	for (uint i = 0; i < MAX_SCAN_THREADS; i++) {
		g_scan_partition_work_q_array[i] = cf_queue_create(sizeof(scan_job_workitem), true);
		if (g_scan_partition_work_q_array[i] == NULL) {
			cf_crash(AS_SCAN, "can't create queue %d", i);
		}
		if (0 != pthread_create(&g_scan_worker_th_array[i], 0, tscan_partition_thr, (void *)g_scan_partition_work_q_array[i])) {
			cf_crash(AS_SCAN, "can't create scan thread %d", i);
		}
	}

	g_scan_job_slotq = cf_queue_create(sizeof(char), true);
	for (int i = 0; i < MAX_SCAN_UDF_WORKITEM; i++) {
		int val = 0;
		cf_queue_push(g_scan_job_slotq, &val);
	}
	g_scan_udf_job_q = cf_queue_create(sizeof(tscan_job *), true);
	if (0 != pthread_create(&g_scan_udf_job_th, 0, scan_udf_job_manager, (void *)g_scan_udf_job_q)) {
		cf_crash(AS_SCAN, "can't create scan job thread ");
	}

	cf_info(AS_SCAN, "started %d threads", MAX_SCAN_THREADS);
}

typedef struct scan_jobstat_s {
	int               index;
	as_mon_jobstat ** jobstat;
	int               max_size;
} scan_jobstat;

void
tscan_fill_jobstat(tscan_job *job, as_mon_jobstat *stat)
{
	stat->trid          = job->tid;
	stat->cpu           = 0;
	stat->mem           = cf_atomic_int_get(job->mem_buf);
	stat->run_time      = job->end_time ? (job->end_time - job->start_time) : (cf_getms() - job->start_time);
	stat->recs_read     = job->n_obj_scanned;
	stat->net_io_bytes  = cf_atomic_int_get(job->net_io_bytes);
	stat->priority      = job->n_threads;
	strcpy(stat->ns, job->ns->name);

	const char * set_name = as_namespace_get_set_name(job->ns, job->set_id);
	if (set_name) {
		strcpy(stat->set, set_name);
	} else {
		strcpy(stat->set, "NULL");
	}

	if (IS_SCAN_JOB_ABORTED(job)) {
		strcpy(stat->status, "ABORTED");
	} else if (IS_SCAN_JOB_DONE(job)) {
		strcpy(stat->status, "DONE");
	} else strcpy(stat->status, "IN_PROGRESS");

	stat->jdata[0]        = '\0';

	sprintf(stat->jdata, "job-type=%s:job-progress=%ld", tscan_get_type_str(job->scan_type), ((job->n_partitions_scanned) * 100) / AS_PARTITIONS);

	char *specific_data = stat->jdata + strlen(stat->jdata);
	if ((job->scan_type == SCAN_UDF_BG)
			|| (job->scan_type == SCAN_UDF_FG))
	{
		sprintf(specific_data, ":udf-filename=%s:udf-function=%s:udf-success=%ld:"
				"udf-failed=%ld:udf-updated=%ld:udf-avg-runtime(ms)=%ld",
				job->call.filename,
				job->call.function,
				cf_atomic_int_get(job->n_obj_udf_success),
				cf_atomic_int_get(job->n_obj_udf_failed),
				cf_atomic_int_get(job->n_obj_udf_updated),
				job->uit_completed ? (job->uit_total_run_time / job->uit_completed) : 0
			   );
	} else if (job->scan_type == SCAN_SINDEX) {
		sprintf(specific_data, ":indexname=%s", job->si->imd->iname);
	}
}

/*
 * Populates the as_mon_jobstat and return to mult-key lookup monitoring infrastructure.
 * Serves as a callback function.
 *
 * Returns -
 *      NULL - In case of failure.
 *      as_mon_jobstat - On success.
 */
as_mon_jobstat *
as_tscan_get_jobstat(uint64_t trid)
{
	as_mon_jobstat *stat;
	tscan_job *job = NULL;
	// Get the job from the transaction id.
	if (RCHASH_OK != rchash_get(g_scan_job_hash, &trid, sizeof(trid), (void **) &job)) {
		cf_info(AS_SCAN, "Scan job with transaction id [%"PRIu64"] does not exist anymore", trid );
		return NULL;
	}
	stat            = cf_malloc(sizeof(as_mon_jobstat));
	if (!stat) return NULL;
	tscan_fill_jobstat(job, stat);
	scan_job_release_and_destroy(job);
	return stat;
}

int
as_mon_scan_jobstat_reduce_fn (void *key, uint32_t keylen, void *object, void *udata)
{
	tscan_job * job = (tscan_job*)object;
	scan_jobstat *job_pool = (scan_jobstat*) udata;

	if ( job_pool->index >= job_pool->max_size) return 0;
	as_mon_jobstat * stat = *(job_pool->jobstat);
	stat                  = stat + job_pool->index;
	tscan_fill_jobstat(job, stat);
	(job_pool->index)++;
	return 0;
}

as_mon_jobstat *
as_tscan_get_jobstat_all(int * size)
{
	*size = rchash_get_size(g_scan_job_hash);
	if(*size == 0) return 0;

	as_mon_jobstat * job_stats;
	scan_jobstat   * job_pool;

	job_stats          = (as_mon_jobstat *) cf_malloc(sizeof(as_mon_jobstat) * (*size));
	job_pool           = (scan_jobstat *) cf_malloc(sizeof(scan_jobstat));
	job_pool->jobstat  = &job_stats;
	job_pool->index    = 0;
	job_pool->max_size = *size;
	rchash_reduce(g_scan_job_hash, as_mon_scan_jobstat_reduce_fn, job_pool);
	return job_stats;
}
