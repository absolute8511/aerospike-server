/*
 * rw_utils_ce.c
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stdint.h>

#include "fault.h"
#include "msg.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"
#include "transaction/udf.h"


//==========================================================
// Public API.
//

bool
generation_check(const as_record* r, const as_msg* m)
{
	if ((m->info2 & AS_MSG_INFO2_GENERATION) != 0) {
		return m->generation == r->generation;
	}

	if ((m->info2 & AS_MSG_INFO2_GENERATION_GT) != 0) {
		return m->generation > r->generation;
	}

	return true; // no generation requirement
}


int
set_delete_durablility(const as_transaction* tr, as_storage_rd* rd)
{
	if (as_transaction_is_durable_delete(tr)) {
		cf_warning(AS_RW, "durable delete is an enterprise feature");
		return AS_PROTO_RESULT_FAIL_ENTERPRISE_ONLY;
	}

	return 0;
}


//==========================================================
// Private API - for enterprise separation only.
//

bool
create_only_check(const as_record* r, const as_msg* m)
{
	// Ok (return true) if no requirement.
	return (m->info2 & AS_MSG_INFO2_CREATE_ONLY) == 0;
}


void
write_delete_record(as_record* r, as_index_tree* tree)
{
	as_index_delete(tree, &r->keyd);
}


udf_optype
udf_finish_delete(udf_record* urecord)
{
	return (urecord->flag & UDF_RECORD_FLAG_PREEXISTS) != 0 ?
			UDF_OPTYPE_DELETE : UDF_OPTYPE_NONE;
}


uint32_t
dup_res_pack_info(const as_record* r, as_namespace* ns)
{
	return 0;
}


bool
dup_res_ignore_pickle(const uint8_t* buf, uint32_t info)
{
	return as_record_pickle_is_binless(buf);
}


bool
dup_res_should_retry_transaction(rw_request* rw, uint32_t result_code)
{
	// TODO - JUMP - can get this from 3.14.x nodes or older - retry if so.
	return result_code == AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
}


void
dup_res_translate_result_code(rw_request* rw)
{
	rw->result_code = AS_PROTO_RESULT_OK;
}


void
repl_write_flag_pickle(const as_transaction* tr, const uint8_t* buf,
		uint32_t* info)
{
	// Do nothing.
}


bool
repl_write_pickle_is_drop(const uint8_t* buf, uint32_t info)
{
	return as_record_pickle_is_binless(buf);
}


bool
repl_write_should_retransmit_replicas(rw_request* rw, uint32_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH:
		rw->xmit_ms = 0; // force retransmit on next cycle
		return true;
	default:
		return false;
	}
}
