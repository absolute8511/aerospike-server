/*
 * scan.h
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
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

#pragma once

#include <stdint.h>
#include "dynbuf.h"
#include "base/monitor.h"
#include "base/transaction.h"
#include "base/udf_rw.h"

// TODO - move to proto.h?
// These values correspond to client protocol values - do not change them!
typedef enum as_scan_udf_op {
	AS_SCAN_UDF_NONE			= 0,
	AS_SCAN_UDF_OP_AGGREGATE	= 1,
	AS_SCAN_UDF_OP_BACKGROUND	= 2,
	AS_SCAN_UDF_OP_UDF			= 3		// not supported yet
} as_scan_udf_op;

void as_scan_init();
int as_scan(as_transaction *tr);
void as_scan_limit_active_jobs(uint32_t max_active);
void as_scan_limit_finished_jobs(uint32_t max_done);
void as_scan_resize_thread_pool(uint32_t n_threads);
int as_scan_get_active_job_count();
int as_scan_list(char* name, cf_dyn_buf* db);
as_mon_jobstat* as_scan_get_jobstat(uint64_t trid);
as_mon_jobstat* as_scan_get_jobstat_all(int* size);
int as_scan_abort(uint64_t trid);
int as_scan_abort_all();

udf_call* as_scan_get_udf_call(void* req_udata);
