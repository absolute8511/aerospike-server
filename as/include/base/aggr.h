/*
 * aggr.h
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

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "aerospike/as_rec.h"
#include "aerospike/as_stream.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_ll.h"

#include "ai_btree.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/udf_memtracker.h"
#include "base/udf_record.h"
#include "base/udf_rw.h"


typedef enum as_aggr_caller_type {
	AS_AGGR_SCAN,
	AS_AGGR_QUERY,
} as_aggr_caller_type;

typedef struct as_aggr_caller_intf_s {
	void (*set_error)(void * caller);
	bool (*mem_op)(mem_tracker *mt, uint32_t num_bytes, memtracker_op op);
	as_aggr_caller_type (*get_type)();
} as_aggr_caller_intf;

typedef struct query_record_s {
	as_rec                   * urec;
	void                     * caller;
	udf_record               * urecord;
	bool                       read;
	as_partition_reservation * rsv;
} query_record;

typedef struct as_aggr_istream_s {
	cf_ll_iterator     * iter;
	as_rec             * rec;
	as_index_keys_arr  * keys_arr;
	int                  skv_offset;
	as_namespace       * ns;
	as_aggr_caller_type (*get_type)();
} as_aggr_istream;

typedef enum as_query_udf_op {
	AS_QUERY_UDF_OP_UDF,
	AS_QUERY_UDF_OP_AGGREGATE,
	AS_QUERY_UDF_OP_MR
} as_query_udf_op;

typedef struct as_aggr_call_s {
	bool                   active;
	as_namespace           * ns;
	char                   filename[UDF_MAX_STRING_SZ];
	char                   function[UDF_MAX_STRING_SZ];
	as_msg_field           * arglist;
	void                   * caller;
	const as_aggr_caller_intf    * caller_intf;
	const as_stream_hooks        * istream_hooks;
	const as_stream_hooks        * ostream_hooks;
} as_aggr_call;

as_val * as_aggr_istream_read(const as_stream *s); 
int as_aggr_call_init(as_aggr_call *call, as_transaction *txn, void *caller,
		const as_aggr_caller_intf *caller_intf, const as_stream_hooks *istream_hooks,
		const as_stream_hooks *ostream_hooks, as_namespace *ns, bool is_scan);
