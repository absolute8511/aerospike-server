/*
 * aggr.h
 *
 * Copyright (C) 2014-2015 Aerospike, Inc.
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
#include "aerospike/as_result.h"
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


typedef struct {
	as_stream_status           (* ostream_write) (void *, as_val *);
	void                       (* set_error)     (void *, int);
	as_partition_reservation * (* ptn_reserve)   (void *, as_namespace *, as_partition_id, as_partition_reservation *);
	void                       (* ptn_release)   (void *, as_partition_reservation *);
	bool                       (* pre_check)     (void *, udf_record *, void *);
} as_aggr_hooks;

typedef struct {
	udf_def                   def;
	const as_aggr_hooks     * aggr_hooks;
} as_aggr_call;

int as_aggr_process(as_namespace *ns, as_aggr_call * ag_call, cf_ll * ap_recl, void * udata, as_result * ap_res);
