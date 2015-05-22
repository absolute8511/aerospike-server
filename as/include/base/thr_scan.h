/*
 * thr_scan.h
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
 * scan function declarations
 */

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"

#include "base/aggr.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/udf_rw.h"


// Scan udf types.
// Client can send either background or client udf (response for every udf).
typedef enum as_scan_udf_op {
	AS_SCAN_UDF_NONE,
	AS_SCAN_UDF_OP_AGGREGATE,
	AS_SCAN_UDF_OP_BACKGROUND,
	AS_SCAN_UDF_OP_UDF
} as_scan_udf_op;

typedef enum as_scan_state_logged {
	AS_SCAN_STARTED,
	AS_SCAN_IN_PROGRESS,
	AS_SCAN_ABORTED,
	AS_SCAN_FINISHED
} as_scan_state_logged;

typedef struct {
	pthread_mutex_t     LOCK;                       // lock to protect against multiple threads working on the same job
	as_file_handle *    fd_h;                       // holds the response fd we're going to have to send back to
	uint64_t            tid;
	as_namespace *      ns;                         // namespace for which the job should be applied to
	uint16_t            set_id;                     // id of the set which the job should be applied to
	bool                nobindata;                  // From incoming message: if the scan does not want bin data
	bool                fail_on_cluster_change;     // require a stable (non-moving) cluster
	int                 job_type;                   // JOB_TYPE_PARTITION or JOB_TYPE_STORAGE
	int                 n_threads;                  // how many threads to dispatch work to in case of JOB_TYPE_PARTITION
	int                 scan_pct;
	uint64_t            window;                     // max bytes to get/send_back to client each time
	uint64_t            cluster_key;                // the cluster key under which the job was started under
	void *              udata;                      // opaque? structure used by subsystem to "get data"
	cf_atomic32			flag;						// The current state(s) the job can be in (aborted, done, finished, pending etc)
	as_sindex *         si;
	uint64_t			si_start_desync_cnt;
	cf_vector *         binlist;
	as_scan_state_logged scan_state_logged;			// To log the aborted scan only once per namespace not for each partition.
	cl_msg *            msgp;
	volatile int        result;                     // not used by UDF scan jobs

	// Scan UDF specific fields
	bool                hasudf;              		// Has record UDF
	udf_call            call;                		// udf_call if there is UDF
	as_aggr_call        agg_call;                   // Stream UDF Details
	cf_atomic_int       uit_queued;    				// Throttling: max in flight scan
	uint8_t             scan_type;                  //scan type (normal,background,foreground,sindex)
	// UDF transaction per job
	cf_atomic_int       uit_completed; 				// Number of udf transactions successfully completed
	cf_atomic64			uit_total_run_time;			// Average transaction processing time for all udf internal transactions
	uint16_t			cur_partition_id;
	cf_atomic_int		n_partitions_scanned;
	uint64_t            start_time;               	// time when job is created
	uint64_t            end_time;                 	// time when job processing ends
	cf_atomic_int       net_io_bytes;
	cf_atomic_int       mem_buf;
	cf_atomic_int       n_obj_scanned;            	// how many objects has been scanned
	cf_atomic_int       n_obj_udf_failed;         	// if a udf, any failure in execution
	cf_atomic_int       n_obj_udf_success;         	// if a udf, success in execution
	cf_atomic_int       n_obj_udf_updated;        	// if a udf, if there was a successful local write
	cf_atomic_int       n_obj_expired;              // how many expired objects have been encountered
	cf_atomic_int       n_obj_set_diff;             // how many mismatched set-id objects have been encountered
} tscan_job;

typedef struct {
	byte                type;
	as_namespace *      ns;
	as_file_handle *    fd_h;
	cf_buf_builder *    bb;
	uint                yield_count;
	bool                nobindata;
	bool                nodata;
	as_partition_reservation *rsv;
	tscan_job           *pjob;
	uint64_t            job_id;
	uint16_t            set_id;                     // set id, if given to us
	as_sindex *         si;
	cf_vector *         binlist;
	udf_call *          call;                       // read copy @TODO should be ref counted
	as_aggr_call *      aggr_call;                  // read copy @TODO should be ref counted
	bool *				aborted;
} tscan_task_data;

/* Function declarations */
extern void as_scan_init();
extern int as_scan(as_transaction *tr);
extern int as_scan_kill(int tid);
extern int as_tscan_list(char *name, cf_dyn_buf *db);
extern int as_tscan_abort(uint64_t trid);
extern int as_tscan_abort_all();

// Call when the incoming fd blows up
extern void as_scan_cleanup_fd(int fd);

// Threaded scan functionality.
extern void as_tscan_init();
extern int  as_tscan(as_transaction *tr);
extern int  as_tscan_udf_tr_complete(as_transaction *tr, int retcode);


// Query functionality.
extern int as_query();

// SINDEX populate.
extern int as_tscan_sindex_populate(void *param);
extern int as_tscan_sindex_populateall(void *param);

// Scan module for monitoring.
extern as_mon_jobstat * as_tscan_get_jobstat(uint64_t trid);
extern as_mon_jobstat * as_tscan_get_jobstat_all(int * size);
#define MAX_SCAN_THREADS 32
