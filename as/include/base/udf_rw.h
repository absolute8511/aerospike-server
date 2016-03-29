/*
 * udf_rw.h
 *
 * Copyright (C) 2013-2015 Aerospike, Inc.
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

#include "aerospike/as_list.h"
#include "aerospike/as_result.h"
#include "citrusleaf/cf_digest.h"

#include "util.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/write_request.h"


// parameter read off from a transaction
#define UDF_MAX_STRING_SZ 128

typedef struct udf_def_s {
	char			filename[UDF_MAX_STRING_SZ];
	char			function[UDF_MAX_STRING_SZ];
	as_msg_field  * arglist;
	uint8_t         type;
} udf_def;

typedef struct udf_call_s {
	udf_def         *def;
	as_transaction	*tr;
} udf_call;

typedef int (*iudf_cb)(as_transaction *tr, int retcode);

typedef enum {
	UDF_UNDEF_REQUEST = -1,
	UDF_SCAN_REQUEST  = 0,
	UDF_QUERY_REQUEST = 1
} iudf_type;

typedef struct iudf_origin_s {
	udf_def		def;
	iudf_type	type;
	iudf_cb		cb;
	void *		udata;
} iudf_origin;

typedef enum {
	UDF_OPTYPE_NONE,
	UDF_OPTYPE_READ,
	UDF_OPTYPE_WRITE,
	UDF_OPTYPE_DELETE,
	UDF_OPTYPE_LDT_READ,
	UDF_OPTYPE_LDT_WRITE,
	UDF_OPTYPE_LDT_DELETE
} udf_optype;

#define UDF_OP_IS_DELETE(op) \
	(((op) == UDF_OPTYPE_DELETE) || ((op) == UDF_OPTYPE_LDT_DELETE))
#define UDF_OP_IS_READ(op) \
	(((op) == UDF_OPTYPE_READ) || ((op) == UDF_OPTYPE_LDT_READ))
#define UDF_OP_IS_WRITE(op) \
	(((op) == UDF_OPTYPE_WRITE) || ((op) == UDF_OPTYPE_LDT_WRITE))
#define UDF_OP_IS_LDT(op) \
	(((op) == UDF_OPTYPE_LDT_READ)        \
		|| ((op) == UDF_OPTYPE_LDT_WRITE) \
		|| ((op) == UDF_OPTYPE_LDT_DELETE))



// Executes the script on a local record
// **************************************************************************************************
void	 as_udf_rw_init(void);
int      udf_rw_local(udf_call *call, write_request *wr, udf_optype *optype);
// **************************************************************************************************

// UDF_CALL related functions
// **************************************************************************************************
udf_call *udf_rw_call_def_init_internal(udf_call * call, as_transaction *tr);
udf_call *udf_rw_call_def_init_from_msg(udf_call *call, as_transaction *tr);
udf_def *udf_def_init_from_msg(udf_def *def, const as_transaction *tr);
void udf_rw_call_destroy(udf_call *call);
// **************************************************************************************************
