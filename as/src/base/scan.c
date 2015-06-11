/*
 * scan.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
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
 * along with this program. If not, see http://www.gnu.org/licenses/
 */

//==============================================================================
// Includes.
//

#include "base/scan.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "aerospike/as_module.h"
#include "aerospike/as_stream.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_queue_priority.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "fault.h"

#include "ai_btree.h"
extern dig_arr_t* getDigestArray(void);

#include "base/aggr.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/monitor.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_memtracker.h"



//==============================================================================
// Typedefs and forward declarations.
//

//----------------------------------------------------------
// Scan types.
//

typedef enum {
	SCAN_TYPE_BASIC		= 0,
	SCAN_TYPE_AGGR		= 1,
	SCAN_TYPE_UDF_BG	= 2,

	SCAN_TYPE_UNKNOWN	= -1
} scan_type;

static inline const char*
scan_type_str(scan_type type)
{
	switch (type) {
	case SCAN_TYPE_BASIC:
		return "basic";
	case SCAN_TYPE_AGGR:
		return "aggregation";
	case SCAN_TYPE_UDF_BG:
		return "background-udf";
	default:
		return "?";
	}
}

//----------------------------------------------------------
// scan_job - base class header.
//

struct scan_job_s;
struct scan_job_info_s;
typedef void (*scan_job_slice_fn)(struct scan_job_s* _job, as_partition_reservation* rsv);
typedef void (*scan_job_finish_fn)(struct scan_job_s* _job);
typedef void (*scan_job_destroy_fn)(struct scan_job_s* _job);
typedef void (*scan_job_info_fn)(struct scan_job_s* _job, as_mon_jobstat* stat);

typedef struct scan_job_vtable_s {
	scan_job_slice_fn	slice_fn;
	scan_job_finish_fn	finish_fn;
	scan_job_destroy_fn	destroy_fn;
	scan_job_info_fn	info_mon_fn;
} scan_job_vtable;

typedef struct scan_job_s {
	// Mandatory interface for derived classes:
	scan_job_vtable		vtable;

	// Unique identifier:
	uint64_t			trid;

	// Scan scope:
	as_namespace*		ns;
	uint16_t			set_id;

	// Handle active phase:
	pthread_mutex_t		requeue_lock;
	int					priority; // TODO - as_thread_pool_priority
	cf_atomic32			active_rc;
	volatile int		next_pid;
	volatile int		abandoned;

	// For tracking:
	uint64_t			start_ms;
	uint64_t			finish_ms;
	cf_atomic64			n_records_read;
} scan_job;

void scan_job_init(scan_job* job, const scan_job_vtable* vtable, uint64_t trid,
		as_namespace* ns, uint16_t set_id, int priority);
void scan_job_destroy(scan_job* job);
void scan_job_finish(scan_job* job);
static inline void scan_job_active_reserve(scan_job* job);
static inline void scan_job_active_release(scan_job* job);
void scan_job_slice(void* task);
void scan_job_info(scan_job* job, as_mon_jobstat* stat);

//----------------------------------------------------------
// scan_job - derived classes' public methods.
//

struct basic_scan_job_s;
struct basic_scan_job_s* basic_scan_job_create(as_transaction* tr, as_namespace* ns, uint16_t set_id);

struct aggr_scan_job_s;
struct aggr_scan_job_s* aggr_scan_job_create(as_transaction* tr, as_namespace* ns, uint16_t set_id);

struct udf_bg_scan_job_s;
struct udf_bg_scan_job_s* udf_bg_scan_job_create(as_transaction* tr, as_namespace* ns, uint16_t set_id);

//----------------------------------------------------------
// scan_thread_pool - class header.
//

typedef struct scan_thread_pool_s {
	pthread_mutex_t		lock;
	cf_queue_priority*	dispatch_queue;
	cf_queue*			complete_queue;
	uint32_t			n_threads;
} scan_thread_pool;

typedef void (*scan_thread_pool_task_fn)(void* task);

// Same as cf_queue_priority scheme, so no internal conversion needed:
typedef enum {
	THREAD_POOL_PRIORITY_HIGH	= 1,
	THREAD_POOL_PRIORITY_MEDIUM	= 2,
	THREAD_POOL_PRIORITY_LOW	= 3
} scan_thread_pool_priority;

static inline const char*
thread_pool_priority_str(scan_thread_pool_priority priority)
{
	switch (priority) {
	case THREAD_POOL_PRIORITY_LOW:
		return "low";
	case THREAD_POOL_PRIORITY_MEDIUM:
		return "medium";
	case THREAD_POOL_PRIORITY_HIGH:
		return "high";
	default:
		return "?";
	}
}

static inline scan_thread_pool_priority
thread_pool_priority(int proto_priority)
{
	switch (proto_priority) {
	case 1:
		return THREAD_POOL_PRIORITY_LOW;
	default:
	case 2:
		return THREAD_POOL_PRIORITY_MEDIUM;
	case 3:
		return THREAD_POOL_PRIORITY_HIGH;
	}
}

bool scan_thread_pool_init(scan_thread_pool* pool, uint32_t n_threads);
void scan_thread_pool_shutdown(scan_thread_pool* pool);
bool scan_thread_pool_resize(scan_thread_pool* pool, uint32_t n_threads);
bool scan_thread_pool_queue_task(scan_thread_pool* pool, scan_thread_pool_task_fn task_fn, void* task, scan_thread_pool_priority priority);
bool scan_thread_pool_remove_task(scan_thread_pool* pool, void* task);

//----------------------------------------------------------
// scan_manager - class header.
//

typedef struct scan_manager_s {
	pthread_mutex_t		lock;
	scan_thread_pool	thread_pool;
	cf_queue*			active_scans;
	cf_queue*			finished_scans;
} scan_manager;

void scan_manager_init(scan_manager* manager);
int scan_manager_start_job(scan_manager* manager, scan_job* job);
static inline void scan_manager_requeue_job(scan_manager* manager, scan_job* job);
void scan_manager_finish_job(scan_manager* manager, scan_job* job);
void scan_manager_abandon_job(scan_manager* manager, scan_job* job, int reason);
bool scan_manager_abort_job(scan_manager* manager, uint64_t trid);
int scan_manager_abort_all_jobs(scan_manager* manager);
bool scan_manager_change_job_priority(scan_manager* manager, uint64_t trid, int priority);
void scan_manager_limit_finished_jobs(scan_manager* manager);
void scan_manager_resize_thread_pool(scan_manager* manager, uint32_t n_threads);
as_mon_jobstat* scan_manager_get_job_info(scan_manager* manager, uint64_t trid);
as_mon_jobstat* scan_manager_get_info(scan_manager* manager, int* size);
int scan_manager_get_active_job_count(scan_manager* manager);

//----------------------------------------------------------
// Non-class-specific utilities.
//

typedef struct scan_options_s {
	int			priority;
	bool		fail_on_cluster_change;
	uint32_t	sample_pct;
} scan_options;

static inline const char*
scan_result_str(int result_code)
{
	switch (result_code) {
	case 0:
		return "ok";
	case AS_PROTO_RESULT_FAIL_UNKNOWN:
		return "abandoned-unknown";
	case AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH:
		return "abandoned-cluster-key";
	case AS_PROTO_RESULT_FAIL_SCAN_ABORT:
		return "user-aborted";
	default:
		return "abandoned-?";
	}
}

int get_scan_ns(as_msg* m, as_namespace** p_ns);
int get_scan_set_id(as_msg* m, as_namespace* ns, uint16_t* p_set_id);
scan_type get_scan_type(as_transaction* tr);
void get_scan_options(as_msg* m, scan_options* options);
void set_blocking(int fd, bool block);
size_t send_blocking_response_chunk(int fd, uint8_t* buf, size_t size);
size_t send_blocking_response_fin(int fd, int result_code);



//==============================================================================
// Constants.
//

const size_t INIT_BUF_BUILDER_SIZE = 1024 * 1024 * 2;
const size_t SCAN_CHUNK_LIMIT = 1024 * 1024;
const uint32_t MAX_UDF_ACTIVE_TRANSACTIONS = 256; // TODO - what ???



//==============================================================================
// Globals.
//

static scan_manager g_scan_manager;
static cf_atomic32 g_scan_trid = 0;



//==============================================================================
// Public API.
//

void
as_scan_init()
{
	// TODO - check for failure and cf_crash?
	scan_manager_init(&g_scan_manager);
}

int
as_scan(as_transaction *tr)
{
	cf_info(AS_SCAN, "scan job received");

	int result;
	as_namespace* ns = NULL;
	uint16_t set_id = INVALID_SET_ID;

	if ((result = get_scan_ns(&tr->msgp->msg, &ns)) != 0 ||
		(result = get_scan_set_id(&tr->msgp->msg, ns, &set_id)) != 0) {
		cf_free(tr->msgp);
		return result;
	}

	scan_job* job = NULL;

	switch (get_scan_type(tr)) {
	case SCAN_TYPE_BASIC:
		job = (scan_job*)basic_scan_job_create(tr, ns, set_id);
		break;
	case SCAN_TYPE_AGGR:
		job = (scan_job*)aggr_scan_job_create(tr, ns, set_id);
		break;
	case SCAN_TYPE_UDF_BG:
		job = (scan_job*)udf_bg_scan_job_create(tr, ns, set_id);
		break;
	default:
		break;
	}

	if (! job) {
		cf_free(tr->msgp);
		return tr->result_code;
	}

	if ((result = scan_manager_start_job(&g_scan_manager, job)) != 0) {
		scan_job_destroy(job);
	}

	return result;
}

void
as_scan_resize_thread_pool(uint32_t n_threads)
{
	scan_manager_resize_thread_pool(&g_scan_manager, n_threads);
}

int
as_scan_get_active_job_count()
{
	return scan_manager_get_active_job_count(&g_scan_manager);
}

int
as_scan_list(char* name, cf_dyn_buf* db)
{
	as_mon_info_cmd("scan", NULL, 0, 0, db);
	return 0;
}

as_mon_jobstat*
as_scan_get_jobstat(uint64_t trid)
{
	return scan_manager_get_job_info(&g_scan_manager, trid);
}

as_mon_jobstat*
as_scan_get_jobstat_all(int* size)
{
	return scan_manager_get_info(&g_scan_manager, size);
}

int
as_scan_abort(uint64_t trid)
{
	return scan_manager_abort_job(&g_scan_manager, trid) ? 0 : -1;
}

int
as_scan_abort_all()
{
	return scan_manager_abort_all_jobs(&g_scan_manager);
}


//==============================================================================
// Non-class-specific utilities.
//

int
get_scan_ns(as_msg* m, as_namespace** p_ns)
{
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! f) {
		return AS_PROTO_RESULT_FAIL_NAMESPACE;
	}

	as_namespace* ns = as_namespace_get_bymsgfield(f);

	if (! ns) {
		return AS_PROTO_RESULT_FAIL_NAMESPACE;
	}

	*p_ns = ns;

	return 0;
}

int
get_scan_set_id(as_msg* m, as_namespace* ns, uint16_t* p_set_id)
{
	uint16_t set_id = INVALID_SET_ID;
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);

	if (f && as_msg_field_get_value_sz(f) != 0) {
		uint32_t set_name_len = as_msg_field_get_value_sz(f);
		char set_name[set_name_len + 1];

		memcpy(set_name, f->data, set_name_len);
		set_name[set_name_len] = '\0';
		set_id = as_namespace_get_set_id(ns, set_name);

		if (set_id == INVALID_SET_ID) {
			return AS_PROTO_RESULT_FAIL_NOTFOUND;
		}
	}

	*p_set_id = set_id;

	return 0;
}

scan_type
get_scan_type(as_transaction* tr)
{
	as_msg_field *filename_f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_FILENAME);

	if (! filename_f && ! tr->udata.req_udata) {
		return SCAN_TYPE_BASIC;
	}

	as_msg_field *udf_op_f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_OP);

	if (udf_op_f && *udf_op_f->data == (uint8_t)AS_SCAN_UDF_OP_AGGREGATE) {
		return SCAN_TYPE_AGGR;
	}

	if (tr->udata.req_udata || (udf_op_f && *udf_op_f->data == (uint8_t)AS_SCAN_UDF_OP_BACKGROUND)) {
		return SCAN_TYPE_UDF_BG;
	}

	return SCAN_TYPE_UNKNOWN;
}

void
get_scan_options(as_msg* m, scan_options* options)
{
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SCAN_OPTIONS);

	if (f) {
		options->priority = AS_MSG_FIELD_SCAN_PRIORITY(f->data[0]);
		options->fail_on_cluster_change = (AS_MSG_FIELD_SCAN_FAIL_ON_CLUSTER_CHANGE & f->data[0]) != 0;
		options->sample_pct = f->data[1];
	}
}

void
set_blocking(int fd, bool block)
{
	int flags = fcntl(fd, F_GETFL, 0);

	if (flags < 0) {
		cf_crash(AS_SCAN, "error getting flags on fd %d (%d: %s)", fd, errno,
				cf_strerror(errno));
	}

	flags = block ? flags & ~O_NONBLOCK : flags | O_NONBLOCK;

	if (fcntl(fd, F_SETFL, flags) < 0) {
		cf_crash(AS_SCAN, "error setting flags on fd %d (%d: %s)", fd, errno,
				cf_strerror(errno));
	}
}

size_t
send_blocking_response_chunk(int fd, uint8_t* buf, size_t size)
{
	as_proto proto;

	proto.version = PROTO_VERSION;
	proto.type = PROTO_TYPE_AS_MSG;
	proto.sz = size;
	as_proto_swap(&proto);

	int rv = send(fd, (uint8_t*)&proto, sizeof(as_proto), MSG_NOSIGNAL | MSG_MORE);

	if (rv != sizeof(as_proto)) {
		// TODO - warning
		return 0;
	}

	if ((rv = send(fd, buf, size, MSG_NOSIGNAL)) != size) {
		// TODO - warning
		return 0;
	}

	return sizeof(as_proto) + size;
}

size_t
send_blocking_response_fin(int fd, int result_code)
{
	cl_msg m;

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

	int rv = send(fd, (uint8_t*)&m, sizeof(cl_msg), MSG_NOSIGNAL);

	if (rv != sizeof(cl_msg)) {
		// TODO - warning
		return 0;
	}

	return sizeof(cl_msg);
}



//==============================================================================
// scan_job base class implementation.
//

//----------------------------------------------------------
// scan_job typedefs and forward declarations.
//

static inline const char* scan_job_safe_set_name(scan_job* job);
static inline uint32_t scan_job_progress(scan_job* job);

//----------------------------------------------------------
// scan_job public API.
//

void
scan_job_init(scan_job* job, const scan_job_vtable* vtable, uint64_t trid,
		as_namespace* ns, uint16_t set_id, int priority)
{
	memset(job, 0, sizeof(job));

	job->vtable		= *vtable;

	job->trid		= trid != 0 ? trid : (uint64_t)cf_atomic32_incr(&g_scan_trid);
	job->ns			= ns;
	job->set_id		= set_id;
	job->priority	= thread_pool_priority(priority);

	pthread_mutex_init(&job->requeue_lock, NULL);
}

void
scan_job_destroy(scan_job* job)
{
	job->vtable.destroy_fn(job);

	pthread_mutex_destroy(&job->requeue_lock);
	cf_free(job);
}

void
scan_job_finish(scan_job* job)
{
	job->vtable.finish_fn(job);
	scan_manager_finish_job(&g_scan_manager, job);
}

static inline void
scan_job_active_reserve(scan_job* job)
{
	cf_atomic32_incr(&job->active_rc);
}

static inline void
scan_job_active_release(scan_job* job)
{
	if (cf_atomic32_decr(&job->active_rc) == 0) {
		scan_job_finish(job);
	}
}

void
scan_job_slice(void* task)
{
	scan_job* job = (scan_job*)task;

	int pid = job->next_pid;
	as_partition_reservation rsv;

	while (pid < AS_PARTITIONS &&
			as_partition_reserve_write(job->ns, pid, &rsv, NULL, NULL) != 0) {
		pid++;
	}

	if (pid == AS_PARTITIONS) {
		scan_job_active_release(job);
		return;
	}

	pthread_mutex_lock(&job->requeue_lock);

	if (job->abandoned != 0) {
		pthread_mutex_unlock(&job->requeue_lock);
		as_partition_release(&rsv);
		scan_job_active_release(job);
		return;
	}

	if ((job->next_pid = pid + 1) < AS_PARTITIONS) {
		scan_job_active_reserve(job);
		scan_manager_requeue_job(&g_scan_manager, job);
	}

	pthread_mutex_unlock(&job->requeue_lock);

	job->vtable.slice_fn(job, &rsv);

	as_partition_release(&rsv);
	scan_job_active_release(job);
}

void
scan_job_info(scan_job* job, as_mon_jobstat* stat)
{
	uint64_t now = cf_getms();
	bool done = job->finish_ms != 0;
	uint64_t since_start_ms = now - job->start_ms;
	uint64_t since_finish_ms = done ? now - job->finish_ms : 0; // TODO - incorporate this ???
	uint64_t active_ms = done ? job->finish_ms - job->start_ms : since_start_ms;

	stat->trid		= job->trid;
	stat->run_time	= active_ms;
	stat->recs_read	= cf_atomic64_get(job->n_records_read);
	stat->priority	= job->priority; // TODO - flip to proto priority or make them the same!

	strcpy(stat->ns, job->ns->name);
	strcpy(stat->set, scan_job_safe_set_name(job));

	char status[64];
	sprintf(status, "%s(%s)", done ? "done" : "active", scan_result_str(job->abandoned));
	as_strncpy(stat->status, status, sizeof(stat->status));

	sprintf(stat->jdata, "job-progress=%u", scan_job_progress(job));

	job->vtable.info_mon_fn(job, stat);
}

//----------------------------------------------------------
// scan_job utilities.
//

static inline const char*
scan_job_safe_set_name(scan_job* job)
{
	const char* set_name = as_namespace_get_set_name(job->ns, job->set_id);

	return set_name ? set_name : "null";
}

static inline uint32_t
scan_job_progress(scan_job* job)
{
	return ((uint32_t)job->next_pid * 100) / AS_PARTITIONS;
}



//==============================================================================
// scan_thread_pool class implementation.
//

//----------------------------------------------------------
// scan_thread_pool typedefs and forward declarations.
//

typedef struct scan_thread_pool_qtask_s {
	scan_thread_pool_task_fn	task_fn;
	void*						task;
} scan_thread_pool_qtask;

uint32_t scan_thread_pool_create_threads(scan_thread_pool* pool, uint32_t count);
void scan_thread_pool_shutdown_threads(scan_thread_pool* pool, uint32_t count);
void* scan_thread_pool_run(void* udata);
int scan_thread_pool_delete_cb(void* buf, void* task);

//----------------------------------------------------------
// scan_thread_pool public API.
//

bool
scan_thread_pool_init(scan_thread_pool* pool, uint32_t n_threads)
{
	pthread_mutex_init(&pool->lock, NULL);

	// Initialize queues.
	pool->dispatch_queue = cf_queue_priority_create(sizeof(scan_thread_pool_qtask), true);
	pool->complete_queue = cf_queue_create(sizeof(uint32_t), true);

	// Start detached threads.
	pool->n_threads = scan_thread_pool_create_threads(pool, n_threads);

	return pool->n_threads == n_threads;
}

void
scan_thread_pool_shutdown(scan_thread_pool* pool)
{
	scan_thread_pool_shutdown_threads(pool, pool->n_threads);
	cf_queue_priority_destroy(pool->dispatch_queue);
	cf_queue_destroy(pool->complete_queue);
	pthread_mutex_destroy(&pool->lock);
}

bool
scan_thread_pool_resize(scan_thread_pool* pool, uint32_t n_threads)
{
	pthread_mutex_lock(&pool->lock);

	bool result = true;

	if (n_threads != pool->n_threads) {
		if (n_threads < pool->n_threads) {
			// Shutdown excess threads.
			scan_thread_pool_shutdown_threads(pool, pool->n_threads - n_threads);
			pool->n_threads = n_threads;
		}
		else {
			// Start new detached threads.
			pool->n_threads += scan_thread_pool_create_threads(pool, n_threads - pool->n_threads);
			result = pool->n_threads == n_threads;
		}
	}

	pthread_mutex_unlock(&pool->lock);

	return result;
}

bool
scan_thread_pool_queue_task(scan_thread_pool* pool, scan_thread_pool_task_fn task_fn, void* task, scan_thread_pool_priority priority)
{
	if (pool->n_threads == 0) {
		// No threads are running to process task.
		// TODO - warning
	}

	scan_thread_pool_qtask qtask = { task_fn, task };

	return cf_queue_priority_push(pool->dispatch_queue, &qtask, priority) == CF_QUEUE_OK;
}

bool
scan_thread_pool_remove_task(scan_thread_pool* pool, void* task)
{
	scan_thread_pool_qtask qtask = { NULL, NULL };

	cf_queue_priority_reduce_pop(pool->dispatch_queue, &qtask, scan_thread_pool_delete_cb, task);

	return qtask.task != NULL;
}

//----------------------------------------------------------
// scan_thread_pool utilities.
//

uint32_t
scan_thread_pool_create_threads(scan_thread_pool* pool, uint32_t count)
{
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	uint32_t n_threads_created = 0;
	pthread_t thread;

	for (uint32_t i = 0; i < count; i++) {
		if (pthread_create(&thread, &attrs, scan_thread_pool_run, pool) == 0) {
			n_threads_created++;
		}
	}

	return n_threads_created;
}

void
scan_thread_pool_shutdown_threads(scan_thread_pool* pool, uint32_t count)
{
	// Send terminator tasks to kill 'count' threads.
	scan_thread_pool_qtask task = { NULL, NULL };

	for (uint32_t i = 0; i < count; i++) {
		cf_queue_priority_push(pool->dispatch_queue, &task, CF_QUEUE_PRIORITY_HIGH);
	}

	// Wait till threads finish.
	uint32_t complete;

	for (uint32_t i = 0; i < count; i++) {
		cf_queue_pop(pool->complete_queue, &complete, CF_QUEUE_FOREVER);
	}
}

void*
scan_thread_pool_run(void* udata)
{
	scan_thread_pool* pool = (scan_thread_pool*)udata;
	scan_thread_pool_qtask qtask;

	// Retrieve tasks from queue and execute.
	while (cf_queue_priority_pop(pool->dispatch_queue, &qtask, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		// A null task indicates thread should be shut down.
		if (! qtask.task_fn) {
			break;
		}

		// Run task.
		qtask.task_fn(qtask.task);
	}

	// Send thread completion event back to caller.
	uint32_t complete = 1;

	cf_queue_push(pool->complete_queue, &complete);

	return NULL;
}

int
scan_thread_pool_delete_cb(void* buf, void* task)
{
	return ((scan_thread_pool_qtask*)buf)->task == task ? -1 : 0;
}



//==============================================================================
// scan_manager class implementation.
//

//----------------------------------------------------------
// scan_manager typedefs and forward declarations.
//

typedef struct find_item_s {
	uint64_t	trid;
	scan_job*	job;
	bool		remove;
} find_item;

typedef struct info_item_s {
	scan_job**	p_job;
} info_item;

void scan_manager_evict_finished_jobs(scan_manager* manager);
int scan_manager_find_cb(void* buf, void* udata);
scan_job* scan_manager_find_job(cf_queue* scans, uint64_t trid, bool remove);
static inline scan_job* scan_manager_find_any(scan_manager* manager, uint64_t trid);
static inline scan_job* scan_manager_find_active(scan_manager* manager, uint64_t trid);
static inline scan_job* scan_manager_remove_active(scan_manager* manager, uint64_t trid);
int scan_manager_info_cb(void* buf, void* udata);

//----------------------------------------------------------
// scan_manager public API.
//

void
scan_manager_init(scan_manager* manager)
{
	pthread_mutex_init(&manager->lock, NULL);

	scan_thread_pool_init(&manager->thread_pool, g_config.scan_threads);

	// Initialize queues.
	manager->active_scans = cf_queue_create(sizeof(scan_job*), false);
	manager->finished_scans = cf_queue_create(sizeof(scan_job*), false);
}

int
scan_manager_start_job(scan_manager* manager, scan_job* job)
{
	pthread_mutex_lock(&manager->lock);

	if (cf_queue_sz(manager->active_scans) >= g_config.scan_max_active) {
		pthread_mutex_unlock(&manager->lock);
		return AS_PROTO_RESULT_FAIL_FORBIDDEN;
	}

	// Make sure trid is unique.
	if (scan_manager_find_any(manager, job->trid)) {
		pthread_mutex_unlock(&manager->lock);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	job->start_ms = cf_getms();
	scan_job_active_reserve(job);
	cf_queue_push(manager->active_scans, &job);
	scan_thread_pool_queue_task(&manager->thread_pool, scan_job_slice, job, job->priority);

	cf_atomic_int_incr(&g_config.scans_initiated);

	pthread_mutex_unlock(&manager->lock);
	return 0;
}

static inline void
scan_manager_requeue_job(scan_manager* manager, scan_job* job)
{
	scan_thread_pool_queue_task(&manager->thread_pool, scan_job_slice, job, job->priority);
}

void
scan_manager_finish_job(scan_manager* manager, scan_job* job)
{
	pthread_mutex_lock(&manager->lock);

	scan_manager_remove_active(manager, job->trid);
	job->finish_ms = cf_getms();
	cf_queue_push(manager->finished_scans, &job);
	scan_manager_evict_finished_jobs(manager);

	cf_atomic_int_incr(job->abandoned == 0 ? &g_config.scans_succeeded : &g_config.scans_abandoned);

	pthread_mutex_unlock(&manager->lock);
}

void
scan_manager_abandon_job(scan_manager* manager, scan_job* job, int reason)
{
	pthread_mutex_lock(&job->requeue_lock);
	job->abandoned = reason;
	bool found = scan_thread_pool_remove_task(&manager->thread_pool, job);
	pthread_mutex_unlock(&job->requeue_lock);

	if (found) {
		scan_job_active_release(job);
	}
}

bool
scan_manager_abort_job(scan_manager* manager, uint64_t trid)
{
	pthread_mutex_lock(&manager->lock);

	scan_job* job = scan_manager_find_active(manager, trid);

	if (! job) {
		pthread_mutex_unlock(&manager->lock);
		return false;
	}

	pthread_mutex_lock(&job->requeue_lock);
	job->abandoned = AS_PROTO_RESULT_FAIL_SCAN_ABORT;
	bool found = scan_thread_pool_remove_task(&manager->thread_pool, job);
	pthread_mutex_unlock(&job->requeue_lock);

	pthread_mutex_unlock(&manager->lock);

	if (found) {
		scan_job_active_release(job);
	}

	return true;
}

int
scan_manager_abort_all_jobs(scan_manager* manager)
{
	pthread_mutex_lock(&manager->lock);

	int n_jobs = cf_queue_sz(manager->active_scans);

	if (n_jobs == 0) {
		pthread_mutex_unlock(&manager->lock);
		return 0;
	}

	scan_job* jobs[n_jobs];
	info_item item = { jobs };

	cf_queue_reduce(manager->active_scans, scan_manager_info_cb, &item);

	bool found[n_jobs];

	for (int i = 0; i < n_jobs; i++) {
		scan_job* job = jobs[i];

		pthread_mutex_lock(&job->requeue_lock);
		job->abandoned = AS_PROTO_RESULT_FAIL_SCAN_ABORT;
		found[i] = scan_thread_pool_remove_task(&manager->thread_pool, job);
		pthread_mutex_unlock(&job->requeue_lock);
	}

	pthread_mutex_unlock(&manager->lock);

	for (int i = 0; i < n_jobs; i++) {
		if (found[i]) {
			scan_job_active_release(jobs[i]);
		}
	}

	return n_jobs;
}

bool
scan_manager_change_job_priority(scan_manager* manager, uint64_t trid, int priority)
{
	pthread_mutex_lock(&manager->lock);

	scan_job* job = scan_manager_find_active(manager, trid);

	if (! job) {
		pthread_mutex_unlock(&manager->lock);
		return false;
	}

	pthread_mutex_lock(&job->requeue_lock);
	job->priority = thread_pool_priority(priority);
	// TODO - implement priority change for thread pool.
	pthread_mutex_unlock(&job->requeue_lock);

	pthread_mutex_unlock(&manager->lock);
	return true;
}

void
scan_manager_limit_finished_jobs(scan_manager* manager)
{
	pthread_mutex_lock(&manager->lock);
	scan_manager_evict_finished_jobs(manager);
	pthread_mutex_unlock(&manager->lock);
}

void
scan_manager_resize_thread_pool(scan_manager* manager, uint32_t n_threads)
{
	scan_thread_pool_resize(&manager->thread_pool, n_threads);
}

as_mon_jobstat*
scan_manager_get_job_info(scan_manager* manager, uint64_t trid)
{
	pthread_mutex_lock(&manager->lock);

	scan_job* job = scan_manager_find_any(manager, trid);

	if (! job) {
		pthread_mutex_unlock(&manager->lock);
		return NULL;
	}

	as_mon_jobstat* stat = cf_malloc(sizeof(as_mon_jobstat));

	if (stat) {
		scan_job_info(job, stat);
	}

	pthread_mutex_unlock(&manager->lock);
	return stat; // caller must free this
}

as_mon_jobstat*
scan_manager_get_info(scan_manager* manager, int* size)
{
	*size = 0;

	pthread_mutex_lock(&manager->lock);

	int n_jobs = cf_queue_sz(manager->active_scans) + cf_queue_sz(manager->finished_scans);

	if (n_jobs == 0) {
		pthread_mutex_unlock(&manager->lock);
		return NULL;
	}

	size_t stats_size = sizeof(as_mon_jobstat) * n_jobs;
	as_mon_jobstat* stats = cf_malloc(stats_size);

	if (! stats) {
		pthread_mutex_unlock(&manager->lock);
		return NULL;
	}

	scan_job* jobs[n_jobs];
	info_item item = { jobs };

	cf_queue_reduce(manager->active_scans, scan_manager_info_cb, &item);
	cf_queue_reduce(manager->finished_scans, scan_manager_info_cb, &item);

	memset(stats, 0, stats_size);

	for (int i = 0; i < n_jobs; i++) {
		scan_job_info(jobs[i], &stats[i]);
	}

	pthread_mutex_unlock(&manager->lock);

	*size = n_jobs;
	return stats; // caller must free this
}

int
scan_manager_get_active_job_count(scan_manager* manager)
{
	pthread_mutex_lock(&manager->lock);
	int n_jobs = cf_queue_sz(manager->active_scans);
	pthread_mutex_unlock(&manager->lock);

	return n_jobs;
}

//----------------------------------------------------------
// scan_manager utilities.
//

void
scan_manager_evict_finished_jobs(scan_manager* manager)
{
	int max_allowed = (int)g_config.scan_max_done;

	while (cf_queue_sz(manager->finished_scans) > max_allowed) {
		scan_job* evict_job;

		cf_queue_pop(manager->finished_scans, &evict_job, 0);
		scan_job_destroy(evict_job);
	}
}

int
scan_manager_find_cb(void* buf, void* udata)
{
	scan_job* job = *(scan_job**)buf;
	find_item* match = (find_item*)udata;

	if (match->trid == job->trid) {
		match->job = job;
		return match->remove ? -2 : -1;
	}

	return 0;
}

scan_job*
scan_manager_find_job(cf_queue* scans, uint64_t trid, bool remove)
{
	find_item item = { trid, NULL, remove };

	cf_queue_reduce(scans, scan_manager_find_cb, &item);

	return item.job;
}

static inline scan_job*
scan_manager_find_any(scan_manager* manager, uint64_t trid)
{
	scan_job* job = scan_manager_find_job(manager->active_scans, trid, false);

	if (! job) {
		job = scan_manager_find_job(manager->finished_scans, trid, false);
	}

	return job;
}

static inline scan_job*
scan_manager_find_active(scan_manager* manager, uint64_t trid)
{
	return scan_manager_find_job(manager->active_scans, trid, false);
}

static inline scan_job*
scan_manager_remove_active(scan_manager* manager, uint64_t trid)
{
	return scan_manager_find_job(manager->active_scans, trid, true);
}

int
scan_manager_info_cb(void* buf, void* udata)
{
	scan_job* job = *(scan_job**)buf;
	info_item* item = (info_item*)udata;

	*item->p_job++ = job;

	return 0;
}



//==============================================================================
// conn_scan_job derived class implementation - not final class.
//

//----------------------------------------------------------
// conn_scan_job typedefs and forward declarations.
//

typedef struct conn_scan_job_s {
	// Base object must be first:
	scan_job		_base;

	// Derived class data:
	pthread_mutex_t	fd_lock;
	as_file_handle*	fd_h;

	uint64_t		net_io_bytes;
} conn_scan_job;

void conn_scan_job_init(conn_scan_job* job, as_transaction* tr);
void conn_scan_job_finish(conn_scan_job* job);
bool conn_scan_job_send_response(conn_scan_job* job, uint8_t* buf, size_t size);
void conn_scan_job_release_fd(conn_scan_job* job);
void conn_scan_job_info(conn_scan_job* job, as_mon_jobstat* stat);

//----------------------------------------------------------
// conn_scan_job API.
//

void
conn_scan_job_init(conn_scan_job* job, as_transaction* tr)
{
	pthread_mutex_init(&job->fd_lock, NULL);

	// Take ownership of socket from transaction.
	job->fd_h = tr->proto_fd_h;
	job->fd_h->fh_info |= FH_INFO_DONOT_REAP;
	set_blocking(job->fd_h->fd, true);
	tr->proto_fd_h = NULL;
}

void
conn_scan_job_finish(conn_scan_job* job)
{
	scan_job* _job = (scan_job*)job;

	if (job->fd_h) {
		job->net_io_bytes += send_blocking_response_fin(job->fd_h->fd, _job->abandoned);
		conn_scan_job_release_fd(job);
	}

	pthread_mutex_destroy(&job->fd_lock);
}

bool
conn_scan_job_send_response(conn_scan_job* job, uint8_t* buf, size_t size)
{
	pthread_mutex_lock(&job->fd_lock);

	if (! job->fd_h) {
		pthread_mutex_unlock(&job->fd_lock);
		// Job already abandoned.
		return false;
	}

	size_t size_sent = send_blocking_response_chunk(job->fd_h->fd, buf, size);

	if (size_sent == 0) {
		conn_scan_job_release_fd(job);
		pthread_mutex_unlock(&job->fd_lock);
		scan_manager_abandon_job(&g_scan_manager, (scan_job*)job, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return false;
	}

	job->net_io_bytes += size_sent;

	pthread_mutex_unlock(&job->fd_lock);
	return true;
}

void
conn_scan_job_release_fd(conn_scan_job* job)
{
	set_blocking(job->fd_h->fd, false);
	job->fd_h->fh_info &= ~FH_INFO_DONOT_REAP;
	job->fd_h->last_used = cf_getms();
	AS_RELEASE_FILE_HANDLE(job->fd_h);
	job->fd_h = NULL;
}

void
conn_scan_job_info(conn_scan_job* job, as_mon_jobstat* stat)
{
	stat->net_io_bytes = job->net_io_bytes;
}



//==============================================================================
// basic_scan_job derived class implementation.
//

//----------------------------------------------------------
// basic_scan_job typedefs and forward declarations.
//

typedef struct basic_scan_job_s {
	// Base object must be first:
	conn_scan_job	_base;

	// Derived class data:
	uint64_t		cluster_key;
	bool			fail_on_cluster_change;
	bool			metadata_only;
	uint32_t		sample_pct;
	cf_vector*		bin_names;
} basic_scan_job;

void basic_scan_job_slice(scan_job* _job, as_partition_reservation* rsv);
void basic_scan_job_finish(scan_job* _job);
void basic_scan_job_destroy(scan_job* _job);
void basic_scan_job_info(scan_job* _job, as_mon_jobstat* stat);

static const scan_job_vtable basic_scan_job_vtable = {
		basic_scan_job_slice,
		basic_scan_job_finish,
		basic_scan_job_destroy,
		basic_scan_job_info
};

typedef struct basic_scan_slice_s {
	basic_scan_job*		job;
	cf_buf_builder**	bb_r;
} basic_scan_slice;

void basic_scan_job_reduce_cb(as_index_ref* r_ref, void* udata);
cf_vector* bin_names_from_op(as_msg* m, int* result);

//----------------------------------------------------------
// basic_scan_job public API.
//

basic_scan_job*
basic_scan_job_create(as_transaction* tr, as_namespace* ns, uint16_t set_id)
{
	basic_scan_job* job = cf_malloc(sizeof(basic_scan_job));

	if (! job) {
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		return NULL;
	}

	scan_options options = { 0, false, 100 };
	get_scan_options(&tr->msgp->msg, &options);

	scan_job_init((scan_job*)job, &basic_scan_job_vtable, tr->trid, ns, set_id,
			options.priority);

	job->cluster_key = as_paxos_get_cluster_key();
	job->fail_on_cluster_change = options.fail_on_cluster_change;
	job->metadata_only = (tr->msgp->msg.info1 & AS_MSG_INFO1_GET_NOBINDATA) != 0;
	job->sample_pct = options.sample_pct;
	job->bin_names = bin_names_from_op(&tr->msgp->msg, &tr->result_code);

	if (! job->bin_names && tr->result_code != AS_PROTO_RESULT_OK) {
		scan_job_destroy((scan_job*)job);
		return NULL;
	}

	// TODO - detect migrations more rigorously!
	if (options.fail_on_cluster_change && cf_atomic_int_get(g_config.migrate_progress_send) != 0) {
		// TODO - was AS_PROTO_RESULT_FAIL_UNAVAILABLE - ok?
		tr->result_code = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
		scan_job_destroy((scan_job*)job);
		return NULL;
	}

	conn_scan_job_init((conn_scan_job*)job, tr);

	// Normal scans don't need anything in the message beyond here.
	cf_free(tr->msgp);

	return job;
}

//----------------------------------------------------------
// basic_scan_job mandatory scan_job interface.
//

void
basic_scan_job_slice(scan_job* _job, as_partition_reservation* rsv)
{
	basic_scan_job* job = (basic_scan_job*)_job;
	as_index_tree* tree = rsv->p->vp;
	cf_buf_builder* bb = cf_buf_builder_create_size(INIT_BUF_BUILDER_SIZE);

	if (! bb) {
		scan_manager_abandon_job(&g_scan_manager, _job, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	basic_scan_slice slice = { job, &bb };

	if (job->sample_pct == 100) {
		as_index_reduce(tree, basic_scan_job_reduce_cb, (void*)&slice);
	}
	else {
		uint32_t sample_count = (uint32_t)(((uint64_t)tree->elements * (uint64_t)job->sample_pct) / 100);
		as_index_reduce_partial(tree, sample_count, basic_scan_job_reduce_cb, (void*)&slice);
	}

	if (bb->used_sz != 0) {
		conn_scan_job_send_response((conn_scan_job*)job, bb->buf, bb->used_sz);
	}

	// TODO - guts don't check buf_builder realloc failures rigorously.
	cf_buf_builder_free(bb);
}

void
basic_scan_job_finish(scan_job* _job)
{
	conn_scan_job_finish((conn_scan_job*)_job);
}

void
basic_scan_job_destroy(scan_job* _job)
{
	basic_scan_job* job = (basic_scan_job*)_job;

	if (job->bin_names) {
		cf_vector_destroy(job->bin_names);
	}
}

void
basic_scan_job_info(scan_job* _job, as_mon_jobstat* stat)
{
	conn_scan_job_info((conn_scan_job*)_job, stat);

	char *extra = stat->jdata + strlen(stat->jdata);

	sprintf(extra, ":job-type=%s", scan_type_str(SCAN_TYPE_BASIC));
}

//----------------------------------------------------------
// basic_scan_job utilities.
//

void
basic_scan_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	basic_scan_slice* slice = (basic_scan_slice*)udata;
	basic_scan_job* job = slice->job;
	scan_job* _job = (scan_job*)job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	if (job->fail_on_cluster_change && job->cluster_key != as_paxos_get_cluster_key()) {
		as_record_done(r_ref, ns);
		scan_manager_abandon_job(&g_scan_manager, _job, AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH);
		return;
	}

	as_index *r = r_ref->r;

	if (_job->set_id != INVALID_SET_ID && as_index_get_set_id(r) != _job->set_id) {
		as_record_done(r_ref, ns);
		return;
	}

	if (r->void_time != 0 && r->void_time < as_record_void_time_get()) {
		as_record_done(r_ref, ns);
		return;
	}

	if (job->metadata_only) {
		if (as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED)) {
			as_storage_rd rd;
			as_storage_record_open(ns, r, &rd, &r->key);
			as_msg_make_response_bufbuilder(r, &rd, slice->bb_r, true, NULL, true, true, true, NULL);
			as_storage_record_close(r, &rd);
		}
		else {
			as_msg_make_response_bufbuilder(r, NULL, slice->bb_r, true, ns->name, true, false, true, NULL);
		}
	}
	else {
		as_storage_rd rd;
		as_storage_record_open(ns, r, &rd, &r->key);
		rd.n_bins = as_bin_get_n_bins(r, &rd);
		as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];
		rd.bins = as_bin_get_all(r, &rd, stack_bins);
		as_msg_make_response_bufbuilder(r, &rd, slice->bb_r, false, NULL, true, true, true, job->bin_names);
		as_storage_record_close(r, &rd);
	}

	as_record_done(r_ref, ns);

	cf_atomic64_incr(&_job->n_records_read);

	cf_buf_builder* bb = *slice->bb_r;

	// If we exceed the proto size limit, send accumulated data back to client
	// and reset the buf-builder to start a new proto.
	if (bb->used_sz > SCAN_CHUNK_LIMIT) {
		if (! conn_scan_job_send_response((conn_scan_job*)job, bb->buf, bb->used_sz)) {
			return;
		}

		cf_buf_builder_reset(bb);
	}

	// TODO - throttling config ???
}

cf_vector*
bin_names_from_op(as_msg* m, int* result)
{
	if (m->n_ops == 0) {
		*result = AS_PROTO_RESULT_OK;
		return NULL;
	}

	cf_vector *v  = cf_vector_create(AS_ID_BIN_SZ, m->n_ops, 0);

	as_msg_op *op = NULL;
	int n = 0;

	while ((op = as_msg_op_iterate(m, op, &n)) != NULL) {
		if (op->name_sz >= AS_ID_BIN_SZ) {
			*result = AS_PROTO_RESULT_FAIL_BIN_NAME;
			cf_vector_destroy(v);
			return NULL;
		}

		char bin_name[AS_ID_BIN_SZ];

		memcpy(bin_name, op->name, op->name_sz);
		bin_name[op->name_sz] = 0;
		cf_vector_append_unique(v, (void *)bin_name);
	}

	return v;
}



//==============================================================================
// aggr_scan_job derived class implementation.
//

//----------------------------------------------------------
// aggr_scan_job typedefs and forward declarations.
//

typedef struct aggr_scan_job_s {
	// Base object must be first:
	conn_scan_job	_base;

	// Derived class data:
	cl_msg*			msgp;
	as_aggr_call	aggr_call;
} aggr_scan_job;

void aggr_scan_job_slice(scan_job* _job, as_partition_reservation* rsv);
void aggr_scan_job_finish(scan_job* _job);
void aggr_scan_job_destroy(scan_job* _job);
void aggr_scan_job_info(scan_job* _job, as_mon_jobstat* stat);

static const scan_job_vtable aggr_scan_job_vtable = {
		aggr_scan_job_slice,
		aggr_scan_job_finish,
		aggr_scan_job_destroy,
		aggr_scan_job_info
};

typedef struct aggr_scan_slice_s {
	aggr_scan_job*		job;
	cf_ll*				ll;
	cf_buf_builder**	bb_r;
} aggr_scan_slice;

void aggr_scan_job_reduce_cb(as_index_ref* r_ref, void* udata);
bool aggr_scan_add_digest(cf_ll* ll, cf_digest* keyd);
void aggr_scan_ll_destroy_fn(cf_ll_element* e);
int aggr_scan_ll_reduce_fn(cf_ll_element* e, void* udata);
void aggr_scan_set_error(void* caller);
bool aggr_scan_mem_op(mem_tracker* mt, uint32_t num_bytes, memtracker_op op);
as_aggr_caller_type aggr_scan_get_type();
as_stream_status aggr_scan_ostream_write(const as_stream* s, as_val* v);

const as_aggr_caller_intf aggr_scan_caller_qintf = {
		.set_error	= aggr_scan_set_error,
		.mem_op		= aggr_scan_mem_op,
		.get_type	= aggr_scan_get_type
};

const as_stream_hooks aggr_scan_istream_hooks = {
		.destroy	= NULL,
		.read		= as_aggr_istream_read,
		.write		= NULL
};

const as_stream_hooks aggr_scan_ostream_hooks = {
		.destroy	= NULL,
		.read		= NULL,
		.write		= aggr_scan_ostream_write
};

void aggr_scan_add_val_response(aggr_scan_slice* slice, const as_val* val, bool success);

//----------------------------------------------------------
// aggr_scan_job public API.
//

aggr_scan_job*
aggr_scan_job_create(as_transaction* tr, as_namespace* ns, uint16_t set_id)
{
	aggr_scan_job* job = cf_malloc(sizeof(aggr_scan_job));

	if (! job) {
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		return NULL;
	}

	scan_options options = { 0, false, 100 };
	get_scan_options(&tr->msgp->msg, &options);

	scan_job_init((scan_job*)job, &aggr_scan_job_vtable, tr->trid, ns, set_id,
			options.priority);

	if (as_aggr_call_init(&job->aggr_call, tr, job, &aggr_scan_caller_qintf,
			&aggr_scan_istream_hooks, &aggr_scan_ostream_hooks, ns, true) != 0) {
		tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
		return NULL;
	}

	conn_scan_job_init((conn_scan_job*)job, tr);

	job->msgp = tr->msgp;

	return job;
}

//----------------------------------------------------------
// aggr_scan_job mandatory scan_job interface.
//

void
aggr_scan_job_slice(scan_job* _job, as_partition_reservation* rsv)
{
	aggr_scan_job* job = (aggr_scan_job*)_job;
	as_index_tree* tree = rsv->p->vp;
	cf_ll ll;
	cf_buf_builder* bb = cf_buf_builder_create_size(INIT_BUF_BUILDER_SIZE);

	if (! bb) {
		scan_manager_abandon_job(&g_scan_manager, _job, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	cf_ll_init(&ll, aggr_scan_ll_destroy_fn, false);

	aggr_scan_slice slice = { job, &ll, &bb };

	as_index_reduce(tree, aggr_scan_job_reduce_cb, (void*)&slice);

	as_result* res = as_result_new();
	int ret = as_aggr__process(&job->aggr_call, &ll, (void*)&slice, res);

	if (ret != 0) {
		char* rs = as_module_err_string(ret);

		if (res->value) {
			as_string* lua_s = as_string_fromval(res->value);
			char* lua_err = (char*)as_string_tostring(lua_s);

			if (lua_err) {
				int l_rs_len = strlen(rs);

				rs = cf_realloc(rs, l_rs_len + strlen(lua_err) + 4);
				sprintf(&rs[l_rs_len], " : %s", lua_err);
			}
		}

		const as_val* v = (as_val*)as_string_new(rs, false);

		aggr_scan_add_val_response(&slice, v, true);
		as_val_destroy(v);
		cf_free(rs);
		scan_manager_abandon_job(&g_scan_manager, _job, AS_PROTO_RESULT_FAIL_UNKNOWN);
	}

	cf_ll_reduce(&ll, true, aggr_scan_ll_reduce_fn, NULL);

	if (bb->used_sz != 0) {
		conn_scan_job_send_response((conn_scan_job*)job, bb->buf, bb->used_sz);
	}

	// TODO - guts don't check buf_builder realloc failures rigorously.
	cf_buf_builder_free(bb);
}

void
aggr_scan_job_finish(scan_job* _job)
{
	aggr_scan_job* job = (aggr_scan_job*)_job;

	conn_scan_job_finish((conn_scan_job*)job);

	cf_free(job->msgp);
	job->msgp = NULL;
}

void
aggr_scan_job_destroy(scan_job* _job)
{
	aggr_scan_job* job = (aggr_scan_job*)_job;

	if (job->msgp) {
		cf_free(job->msgp);
	}
}

void
aggr_scan_job_info(scan_job* _job, as_mon_jobstat* stat)
{
	conn_scan_job_info((conn_scan_job*)_job, stat);

	char *extra = stat->jdata + strlen(stat->jdata);

	sprintf(extra, ":job-type=%s", scan_type_str(SCAN_TYPE_AGGR));
}

//----------------------------------------------------------
// aggr_scan_job utilities.
//

void
aggr_scan_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	aggr_scan_slice* slice = (aggr_scan_slice*)udata;
	aggr_scan_job* job = slice->job;
	scan_job* _job = (scan_job*)job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	as_index* r = r_ref->r;

	if (_job->set_id != INVALID_SET_ID && as_index_get_set_id(r) != _job->set_id) {
		as_record_done(r_ref, ns);
		return;
	}

	if (r->void_time != 0 && r->void_time < as_record_void_time_get()) {
		as_record_done(r_ref, ns);
		return;
	}

	if (! aggr_scan_add_digest(slice->ll, &r->key)) {
		as_record_done(r_ref, ns);
		scan_manager_abandon_job(&g_scan_manager, _job, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	cf_atomic64_incr(&_job->n_records_read);
	as_record_done(r_ref, ns);
}

bool
aggr_scan_add_digest(cf_ll* ll, cf_digest* keyd)
{
	ll_recl_element* tail_recl_e = (ll_recl_element*)ll->tail;
	dig_arr_t* dig_arr;

	if (tail_recl_e) {
		dig_arr = tail_recl_e->dig_arr;

		if (dig_arr->num == NUM_DIGS_PER_ARR) {
			tail_recl_e = NULL;
		}
	}

	if (! tail_recl_e) {
		if ((dig_arr = getDigestArray()) == NULL) {
			return false;
		}

		if ((tail_recl_e = cf_malloc(sizeof(ll_recl_element))) == NULL) {
			return false;
		}

		tail_recl_e->dig_arr = dig_arr;
		cf_ll_append(ll, (cf_ll_element *)tail_recl_e);
	}

	dig_arr->digs[dig_arr->num] = *keyd;
	dig_arr->num++;

	return true;
}

void
aggr_scan_ll_destroy_fn(cf_ll_element* e)
{
	ll_recl_element* recl_e = (ll_recl_element*)e;

	releaseDigArrToQueue((void *)recl_e->dig_arr);
	cf_free(recl_e);
}

int
aggr_scan_ll_reduce_fn(cf_ll_element* e, void* udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
aggr_scan_set_error(void* caller)
{
}

bool
aggr_scan_mem_op(mem_tracker* mt, uint32_t num_bytes, memtracker_op op)
{
	return mt && mt->udata;
}

as_aggr_caller_type
aggr_scan_get_type()
{
	return AS_AGGR_SCAN;
}

as_stream_status
aggr_scan_ostream_write(const as_stream* s, as_val* v)
{
	aggr_scan_slice* slice = (aggr_scan_slice*)as_stream_source(s);

	if (v) {
		aggr_scan_add_val_response(slice, v, true);
		as_val_destroy(v);
	}

	return AS_STREAM_OK;
}

void
aggr_scan_add_val_response(aggr_scan_slice* slice, const as_val* val, bool success)
{
	uint32_t size = 0;

	as_val_tobuf(val, NULL, &size); // sizing only
	as_msg_make_val_response_bufbuilder(val, slice->bb_r, size, success);

	cf_buf_builder* bb = *slice->bb_r;
	conn_scan_job* conn_job = (conn_scan_job*)slice->job;

	// If we exceed the proto size limit, send accumulated data back to client
	// and reset the buf-builder to start a new proto.
	if (bb->used_sz > SCAN_CHUNK_LIMIT) {
		if (! conn_scan_job_send_response(conn_job, bb->buf, bb->used_sz)) {
			return;
		}

		cf_buf_builder_reset(bb);
	}
}



//==============================================================================
// udf_bg_scan_job derived class implementation.
//

//----------------------------------------------------------
// udf_bg_scan_job typedefs and forward declarations.
//

typedef struct udf_bg_scan_job_s {
	// Base object must be first:
	scan_job		_base;

	// Derived class data:
	cl_msg*			msgp;
	udf_call		call;
	cf_atomic32		n_active_tr;

	cf_atomic64		n_successful_tr;
	cf_atomic64		n_failed_tr;
} udf_bg_scan_job;

void udf_bg_scan_job_slice(scan_job* _job, as_partition_reservation* rsv);
void udf_bg_scan_job_finish(scan_job* _job);
void udf_bg_scan_job_destroy(scan_job* _job);
void udf_bg_scan_job_info(scan_job* _job, as_mon_jobstat* stat);

static const scan_job_vtable udf_bg_scan_job_vtable = {
		udf_bg_scan_job_slice,
		udf_bg_scan_job_finish,
		udf_bg_scan_job_destroy,
		udf_bg_scan_job_info
};

void udf_bg_scan_job_reduce_cb(as_index_ref* r_ref, void* udata);
int udf_bg_scan_tr_complete(as_transaction *tr, int retcode);

//----------------------------------------------------------
// as_scan public API for udf_bg_scan_job.
//

udf_call*
as_scan_get_udf_call(void* req_udata)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)req_udata;

	return &job->call;
}

//----------------------------------------------------------
// udf_bg_scan_job public API.
//

udf_bg_scan_job*
udf_bg_scan_job_create(as_transaction* tr, as_namespace* ns, uint16_t set_id)
{
	udf_bg_scan_job* job = cf_malloc(sizeof(udf_bg_scan_job));

	if (! job) {
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		return NULL;
	}

	scan_options options = { 0, false, 100 };
	get_scan_options(&tr->msgp->msg, &options);

	scan_job_init((scan_job*)job, &udf_bg_scan_job_vtable, tr->trid, ns, set_id,
			options.priority);

	if (udf_call_init(&job->call, tr) != 0) {
		tr->result_code = AS_PROTO_RESULT_FAIL_PARAMETER;
		return NULL;
	}

	if (as_msg_send_fin(tr->proto_fd_h->fd, AS_PROTO_RESULT_OK) != 0) {
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		return NULL;
	}

	tr->proto_fd_h->last_used = cf_getms();
	AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
	tr->proto_fd_h = NULL;

	job->msgp = tr->msgp;

	return job;
}

//----------------------------------------------------------
// udf_bg_scan_job mandatory scan_job interface.
//

void
udf_bg_scan_job_slice(scan_job* _job, as_partition_reservation* rsv)
{
	as_index_reduce(rsv->p->vp, udf_bg_scan_job_reduce_cb, (void*)_job);
}

void
udf_bg_scan_job_finish(scan_job* _job)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;

	while (cf_atomic32_get(job->n_active_tr) != 0) {
		usleep(100);
	}

	cf_free(job->msgp);
	job->msgp = NULL;
}

void
udf_bg_scan_job_destroy(scan_job* _job)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;

	if (job->msgp) {
		cf_free(job->msgp);
	}
}

void
udf_bg_scan_job_info(scan_job* _job, as_mon_jobstat* stat)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;
	char *extra = stat->jdata + strlen(stat->jdata);

	// TODO - dangerous! Let's reform the monitor...
	sprintf(extra, ":job-type=%s:udf-filename=%s:udf-function=%s:udf-success=%ld:udf-failed=%ld",
			scan_type_str(SCAN_TYPE_UDF_BG),
			job->call.filename, job->call.function,
			cf_atomic64_get(job->n_successful_tr),
			cf_atomic64_get(job->n_failed_tr));

	stat->net_io_bytes = sizeof(cl_msg); // size of original synchronous fin
}

//----------------------------------------------------------
// udf_bg_scan_job utilities.
//

void
udf_bg_scan_job_reduce_cb(as_index_ref* r_ref, void* udata)
{
	scan_job* _job = (scan_job*)udata;
	udf_bg_scan_job* job = (udf_bg_scan_job*)_job;
	as_namespace* ns = _job->ns;

	if (_job->abandoned != 0) {
		as_record_done(r_ref, ns);
		return;
	}

	as_index* r = r_ref->r;

	if (_job->set_id != INVALID_SET_ID && as_index_get_set_id(r) != _job->set_id) {
		as_record_done(r_ref, ns);
		return;
	}

	if (r->void_time != 0 && r->void_time < as_record_void_time_get()) {
		as_record_done(r_ref, ns);
		return;
	}

	while (cf_atomic32_get(job->n_active_tr) > MAX_UDF_ACTIVE_TRANSACTIONS) {
		usleep(50); // TODO - what ???
	}

	tr_create_data d;

	d.digest	= r->key;
	d.ns		= ns;
	d.set[0]	= 0;				// TODO - transfer set name ???
	d.call		= &job->call;
	d.msg_type	= AS_MSG_INFO2_WRITE;
	d.fd_h		= NULL;
	d.trid		= 0;				// TODO - transfer trid ???
	d.udata		= NULL;				// TODO - unused - why is it there ???

	// Release record lock before enqueuing transaction.
	as_record_done(r_ref, ns);

	as_transaction tr;

	if (as_transaction_create(&tr, &d) != 0) {
		scan_manager_abandon_job(&g_scan_manager, _job, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	tr.udata.req_cb		= udf_bg_scan_tr_complete;
	tr.udata.req_udata	= (void*)job;
	tr.udata.req_type	= UDF_SCAN_REQUEST;

	// TODO - should these be done in as_transaction_create() ???
	tr.flag |= AS_TRANSACTION_FLAG_INTERNAL;
	tr.microbenchmark_is_resolve = false;

	cf_atomic64_incr(&_job->n_records_read);
	cf_atomic32_incr(&job->n_active_tr);

	thr_tsvc_enqueue(&tr);
}

int
udf_bg_scan_tr_complete(as_transaction *tr, int retcode)
{
	udf_bg_scan_job* job = (udf_bg_scan_job*)tr->udata.req_udata;

	cf_atomic32_decr(&job->n_active_tr);
	cf_atomic64_incr(retcode == 0 ? &job->n_successful_tr : &job->n_failed_tr);

	return 0;
}
