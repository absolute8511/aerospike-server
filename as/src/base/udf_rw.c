/*
 * udf_rw.c
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
 * User Defined Function execution engine
 *
 */

#include "base/udf_rw.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_log.h"
#include "aerospike/as_module.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_types.h"
#include "aerospike/mod_lua.h"

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"
#include "hist_track.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/ldt_aerospike.h"
#include "base/ldt_record.h"
#include "base/proto.h"
#include "base/rec_props.h"
#include "base/thr_rw_internal.h"
#include "base/thr_scan.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "base/udf_aerospike.h"
#include "base/udf_arglist.h"
#include "base/udf_cask.h"
#include "base/udf_memtracker.h"
#include "base/udf_timer.h"
#include "base/write_request.h"

as_aerospike g_as_aerospike;

extern udf_call *as_query_get_udf_call(void *ptr);

/* Internal Function: Packs up passed in data into as_bin which is
 *                    used to send result after the UDF execution.
 */
static bool
make_send_bin(as_namespace *ns, as_bin *bin, uint8_t **sp_pp, uint32_t sp_sz,
			  const char *key, int  vtype,  void *val, size_t vlen)
{
	uint8_t *   v           = NULL;
	int64_t     unswapped_int = 0;
	uint8_t     *sp_p = *sp_pp;

	uint32_t tsz = as_particle_size_from_mem((as_particle_type)vtype, (uint8_t *)val, (uint32_t)vlen);

	if (tsz > sp_sz) {
		sp_p = cf_malloc(tsz);
		if (!sp_p) {
			cf_warning(AS_UDF, "data too much. malloc failed. going down. bin %s not sent back", key);
			return(-1);
		}
	}

	as_bin_init(ns, bin, key/*name*/);

	switch (vtype) {
		case AS_PARTICLE_TYPE_NULL:
		{
			v = NULL;
			break;
		}
		case AS_PARTICLE_TYPE_INTEGER:
		{
			if (vlen != 8) {
				cf_crash(AS_UDF, "unexpected int %d", vlen);
			}
            unswapped_int = * (int64_t *) val;
			v = (uint8_t *) &unswapped_int;
			break;
		}
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_STRING:
		case AS_PARTICLE_TYPE_LIST:
		case AS_PARTICLE_TYPE_MAP:
			v = val;
			break;
		default:
		{
			cf_warning(AS_UDF, "unrecognized object type %d ignored", vtype);
			return -1;
		}
	}

	as_bin_particle_stack_from_mem(bin, sp_p, vtype, v, vlen);

	*sp_pp = sp_p;
	return 0;
}

/* Internal Function: Workhorse function to send response back to the client
 * 					  after UDF execution.
 *
 * caller:
 * 		send_success
 * 		send_failure
 *
 * Assumption: The call should be setup properly pointing to the tr.
 *
 * Special Handling: If it is background scan udf job do not sent any
 * 					 response to client
 * 					 If it is scan job ...do not cleanup the fd it will
 * 					 be done by the scan thread after scan is finished
 */
static int
send_response(udf_call *call, const char *key, int vtype, void *val,
			  size_t vlen)
{
	as_transaction *    tr          = call->transaction;
	as_namespace *      ns          = tr->rsv.ns;
	uint32_t            generation  = tr->generation;
	uint32_t            sp_sz       = 1024 * 16;
	uint32_t            void_time   = tr->void_time;
	uint32_t            written_sz  = 0;
	bool                keep_fd     = false;
	as_bin              stack_bin;
	as_bin            * bin         = &stack_bin;

	// space for the stack particles
	uint8_t             stack_particle_buf[sp_sz];
	uint8_t *           sp_p        = stack_particle_buf;

	if (call->udf_type == AS_SCAN_UDF_OP_BACKGROUND) {
		// If we are doing a background UDF scan, do not send any result back
		if (tr->udata.req_type == UDF_SCAN_REQUEST) {
			cf_detail(AS_UDF, "UDF: Background transaction, send no result back. "
					"Parent job id [%"PRIu64"]", ((tscan_job*)(tr->udata.req_udata))->tid);
			if (strncmp(key, "FAILURE", 8) == 0) {
				cf_atomic_int_incr(&((tscan_job*)(tr->udata.req_udata))->n_obj_udf_failed);
			} else if (strncmp(key, "SUCCESS", 8) == 0) {
				cf_atomic_int_incr(&((tscan_job*)(tr->udata.req_udata))->n_obj_udf_success);
			}
		}
		return 0;
	} else if (call->udf_type == AS_SCAN_UDF_OP_UDF) {
		// Do not release fd now, scan will do it at the end of all internal
		// 	udf transactions
		cf_detail(AS_UDF, "UDF: Internal udf transaction, do not release fd");
		keep_fd = true;
	}

	if (0 != make_send_bin(ns, bin, &sp_p, sp_sz, key, vtype, val, vlen)) {
		return(-1);
	}

	// this is going to release the file descriptor
	if (keep_fd && tr->proto_fd_h) cf_rc_reserve(tr->proto_fd_h);

	single_transaction_response(
		tr, ns, NULL/*ops*/, &bin, 1,
		generation, void_time, &written_sz, NULL);

	if (sp_p != stack_particle_buf) {
		cf_free(sp_p);
	}
	return 0;
} // end send_response()

/**
 * Send failure notification for CDT (list, map) serialization error.
 */
static inline int
send_cdt_failure(udf_call *call, int vtype, void *val, size_t vlen)
{
	call->transaction->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
	return send_response(call, "FAILURE", vtype, val, vlen);
}

int
udf_rw_get_ldt_error(void *val, size_t vlen) 
{
	// We need to do a quick look to see if this is an LDT error string.  If it
	// is, then we'll look deeper.  We start looking after the first space.
	char * charptr;
	char * valptr = (char *) val;
	long   error_code;

	// Start with the space, if it exists, as the marker for where we start
	// looking for the LDT error contents.
	if ((charptr = strchr((const char *) val, ' ')) != NULL) {
		// We must be at least 10 chars from the end, so if we're less than that
		// we are obviously not looking at an LDT error.
		if (&charptr[9] < &valptr[vlen]) {
			if (memcmp(&charptr[5], ":LDT-", 5) == 0) {
				error_code = strtol(&charptr[1], NULL, 10);
				cf_debug(AS_UDF, "LDT Error: Code(%ld) String(%s)",
						error_code, (char *) val);
				return error_code;
			}
		}
	}
	return 0;
}

// Parses the error message coming from lua world. This parsing matches the error
// codes in ldt_error.lua.
//
// Currently this parsing happens at
// 1. Lua world
// 2. Server world (This funtion and associated #define)
// 3. All the clients.
//
// TODO: Should probably be part of common. Or some sort of error database so
// this can scale.
void 
udf_rw_update_ldt_err_stats(as_namespace *ns, as_result *res)
{
	if (!ns->ldt_enabled || !res) {
		return;
	}

	if (res->is_success) {
		return;
	}

	as_string * s   = as_string_fromval(res->value);
	char *      rs  = (char *) as_string_tostring(s);

    if ( s ) {
		long code = udf_rw_get_ldt_error(rs, as_string_len(s));
		switch (code) {
			case ERR_TOP_REC_NOT_FOUND: 
				cf_atomic_int_incr(&ns->lstats.ldt_err_toprec_not_found);
				break;
			case ERR_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_item_not_found);
				break;
			case ERR_INTERNAL:
				cf_atomic_int_incr(&ns->lstats.ldt_err_internal);
				break;
			case ERR_UNIQUE_KEY:
				cf_atomic_int_incr(&ns->lstats.ldt_err_unique_key_violation);
				break;
			case ERR_INSERT:
				cf_atomic_int_incr(&ns->lstats.ldt_err_insert_fail);
				break;
			case ERR_SEARCH:
				cf_atomic_int_incr(&ns->lstats.ldt_err_search_fail);
				break;
			case ERR_DELETE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_delete_fail);
				break;
			case ERR_VERSION:
				cf_atomic_int_incr(&ns->lstats.ldt_err_version_mismatch);
				break;

			case ERR_CAPACITY_EXCEEDED:
				cf_atomic_int_incr(&ns->lstats.ldt_err_capacity_exceeded);
				break;
			case ERR_INPUT_PARM:
				cf_atomic_int_incr(&ns->lstats.ldt_err_param);
				break;

			case ERR_TYPE_MISMATCH:
				cf_atomic_int_incr(&ns->lstats.ldt_err_op_bintype_mismatch);
				break;

			case ERR_NULL_BIN_NAME:
			case ERR_BIN_NAME_NOT_STRING:
			case ERR_BIN_NAME_TOO_LONG:
				cf_atomic_int_incr(&ns->lstats.ldt_err_param);
				break;

			case ERR_TOO_MANY_OPEN_SUBRECS:
				cf_atomic_int_incr(&ns->lstats.ldt_err_too_many_open_subrec);
				break;
			case ERR_SUB_REC_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_subrec_not_found);
				break;
			case ERR_BIN_DOES_NOT_EXIST:
				cf_atomic_int_incr(&ns->lstats.ldt_err_bin_does_not_exist);
				break;
			case ERR_BIN_ALREADY_EXISTS:
				cf_atomic_int_incr(&ns->lstats.ldt_err_bin_exits);
				break;
			case ERR_BIN_DAMAGED:
				cf_atomic_int_incr(&ns->lstats.ldt_err_bin_damaged);
				break;

			case ERR_SUBREC_POOL_DAMAGED:
			case ERR_SUBREC_DAMAGED:
			case ERR_SUBREC_OPEN:
			case ERR_SUBREC_UPDATE:
			case ERR_SUBREC_CREATE:
			case ERR_SUBREC_DELETE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_subrec_internal);
				break;

			case ERR_SUBREC_CLOSE:
			case ERR_TOPREC_UPDATE:
			case ERR_TOPREC_CREATE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_toprec_internal);
				break;


			case ERR_FILTER_BAD:
			case ERR_FILTER_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_filter);
				break;
			case ERR_KEY_BAD:
			case ERR_KEY_FIELD_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_key);
				break;
			case ERR_INPUT_CREATESPEC:
				cf_atomic_int_incr(&ns->lstats.ldt_err_createspec);
				break;
			case ERR_INPUT_USER_MODULE_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_usermodule);
				break;

			case ERR_INPUT_TOO_LARGE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_input_too_large);
				break;
			case ERR_NS_LDT_NOT_ENABLED:
				cf_atomic_int_incr(&ns->lstats.ldt_err_ldt_not_enabled);
				break;
			
			default:
				cf_atomic_int_incr(&ns->lstats.ldt_err_unknown);
		}
    } else {
		cf_atomic_int_incr(&ns->lstats.ldt_err_unknown);
	}
	return;
}

/**
 * Send failure notification of general UDF execution, but check for special
 * LDT errors and return specific Wire Protocol error codes for these cases:
 * (1) Record not found (2)
 * (2) LDR Collection item not found (125)
 *
 * All other errors get the generic 100 (UDF FAIL) code.
 *
 * Parse (Actually, probe) the error string, and if we see this pattern:
 * FileName:Line# 4digits:LDT-<Error String>
 * For example:
 * .../aerospike-lua-core/src/ldt/lib_llist.lua:982: 0002:LDT-Top Record Not Found
 * All UDF errors (LDT included), have the "filename:line# " prefix, and then
 * LDT errors follow that with a known pattern:
 * (4 digits, colon, LDT-<Error String>).
 * We will check the error string by looking for specific markers after the
 * the space that follows the filename:line#.  If we see the markers, we will
 * parse the LDT error code and use that as the wire protocol error if it is
 * one of the special ones:
 * (1)  "0002:LDT-Top Record Not Found"
 * (2)  "0125:LDT-Item Not Found"
 */
static inline int
send_udf_failure(udf_call *call, int vtype, void *val, size_t vlen)
{
	long error_code = udf_rw_get_ldt_error(val, vlen);

	if (error_code) {

		if (error_code == AS_PROTO_RESULT_FAIL_NOTFOUND ||
			error_code == AS_PROTO_RESULT_FAIL_COLLECTION_ITEM_NOT_FOUND) {

			call->transaction->result_code = error_code;
			// Send an "empty" response, with no failure bin.
			as_transaction *    tr          = call->transaction;
			single_transaction_response(tr, tr->rsv.ns, NULL/*ops*/,
					NULL /*bin*/, 0 /*nbins*/, 0, 0, NULL, NULL);
			return 0;
		}
	}

	cf_debug(AS_UDF, "Non-special LDT or General UDF Error(%s)", (char *) val);

	call->transaction->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
	return send_response(call, "FAILURE", vtype, val, vlen);
}

static inline int
send_success(udf_call *call, int vtype, void *val, size_t vlen)
{
	return send_response(call, "SUCCESS", vtype, val, vlen);
}

/*
 * Internal Function: Entry function from UDF code path to send
 * 					  success result to the caller. Performs
 * 					  value translation.
 */
void
send_result(as_result * res, udf_call * call, void *udata)
{
	// The following "no-op" line serves to quiet the compiler warning of an
	// otherwise unused variable.
	(void)udata;
	as_val * v = res->value;
	if ( res->is_success ) {

		if ( cf_context_at_severity(AS_UDF, CF_DETAIL) ) {
			char * str = as_val_tostring(v);
			cf_detail(AS_UDF, "SUCCESS: %s", str);
			cf_free(str);
		}

		if ( v != NULL ) {
			switch( as_val_type(v) ) {
				case AS_NIL:
				{
					send_success(call, AS_PARTICLE_TYPE_NULL, NULL, 0);
					break;
				}
				case AS_BOOLEAN:
				{
					as_boolean * b = as_boolean_fromval(v);
					int64_t bi = as_boolean_tobool(b) == true ? 1 : 0;
					send_success(call, AS_PARTICLE_TYPE_INTEGER, &bi, 8);
					break;
				}
				case AS_INTEGER:
				{
					as_integer * i = as_integer_fromval(v);
					int64_t ri = as_integer_toint(i);
					send_success(call, AS_PARTICLE_TYPE_INTEGER, &ri, 8);
					break;
				}
				case AS_STRING:
				{
					// this looks bad but it just pulls the pointer
					// out of the object
					as_string * s = as_string_fromval(v);
					char * rs = (char *) as_string_tostring(s);
					send_success(call, AS_PARTICLE_TYPE_STRING, rs, as_string_len(s));
					break;
				}
				case AS_BYTES:
				{
					as_bytes * b = as_bytes_fromval(v);
					uint8_t * rs = as_bytes_get(b);
					send_success(call, AS_PARTICLE_TYPE_BLOB, rs, as_bytes_size(b));
					break;
				}
				case AS_MAP:
				case AS_LIST:
				{
					as_buffer buf;
					as_buffer_init(&buf);

					as_serializer s;
					as_msgpack_init(&s);

					int res = as_serializer_serialize(&s, v, &buf);

					if (res != 0) {
						const char * error = "Complex Data Type Serialization failure";
						cf_warning(AS_UDF, "%s (%d)", (char *)error, res);
						as_buffer_destroy(&buf);
						send_cdt_failure(call, AS_PARTICLE_TYPE_STRING, (char *)error, strlen(error));
					}
					else {
						// Do not use this until after cf_detail_binary() can accept larger buffers.
						// cf_detail_binary(AS_UDF, buf.data, buf.size, CF_DISPLAY_HEX_COLUMNS, 
						// "serialized %d bytes: ", buf.size);
						send_success(call, to_particle_type(as_val_type(v)), buf.data, buf.size);
						// Not needed stack allocated - unless serialize has internal state
						// as_serializer_destroy(&s);
						as_buffer_destroy(&buf);
					}

					break;
				}
				default:
				{
					cf_debug(AS_UDF, "SUCCESS: VAL TYPE UNDEFINED %d\n", as_val_type(v));
					send_success(call, AS_PARTICLE_TYPE_STRING, NULL, 0);
					break;
				}
			}
		} else {
			send_success(call, AS_PARTICLE_TYPE_NULL, NULL, 0);
		}
	} else { // Else -- NOT success
		as_string * s   = as_string_fromval(v);
		char *      rs  = (char *) as_string_tostring(s);

		cf_debug(AS_UDF, "FAILURE when calling %s %s %s", call->filename, call->function, rs);
		send_udf_failure(call, AS_PARTICLE_TYPE_STRING, rs, as_string_len(s));
	}
}

/**
 * Initialize a new UDF. This populates the udf_call from information
 * in the current transaction. If passed in transaction has req_data it is
 * assumed to be internal and the UDF information is picked from the udata
 * associated with it. TODO: Do not overload please define flag for this.
 *
 *
 * Parameter:
 * 		tr the transaction to build a udf_call from
 *
 * Returns"
 * 		return a new udf_call (Caller needs to free it up)
 * 		NULL in case of failure
 */
int
udf_call_init(udf_call * call, as_transaction * tr)
{

	call->active   = false;
	call->udf_type = AS_SCAN_UDF_NONE;
	as_msg_field *  filename = NULL;
	as_msg_field *  function = NULL;
	as_msg_field *  arglist =  NULL;

	if (tr->udata.req_udata) {
		udf_call *ucall = NULL;
		if (tr->udata.req_type == UDF_SCAN_REQUEST) {
			ucall = &((tscan_job *)(tr->udata.req_udata))->call;
		} else if (tr->udata.req_type == UDF_QUERY_REQUEST) {
			ucall = as_query_get_udf_call(tr->udata.req_udata);
		}

		if (ucall) {
			strncpy(call->filename, ucall->filename, sizeof(ucall->filename));
			strncpy(call->function, ucall->function, sizeof(ucall->function));
			call->transaction = tr;
			call->active      = true;
			call->arglist     = ucall->arglist;
			call->udf_type    = ucall->udf_type;
			if (tr->udata.req_type == UDF_SCAN_REQUEST) {
				cf_atomic_int_incr(&g_config.udf_scan_rec_reqs);
			} else if (tr->udata.req_type == UDF_QUERY_REQUEST) {
				cf_atomic_int_incr(&g_config.udf_query_rec_reqs);
			}
		}
		// TODO: return proper macros
		return 0;
	}

	// Check the type of udf
	as_msg_field *  op = NULL;
	op = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_OP);
	if (!op) {
		// Normal udf operation, no special type
		call->udf_type = 0;
	} else {
		// We got a udf type from the server
		byte optype;
		memcpy(&optype, (byte *)op->data, sizeof(optype));
		if(optype == AS_SCAN_UDF_OP_UDF) {
			cf_debug(AS_UDF, "UDF scan op received");
			call->udf_type = AS_SCAN_UDF_OP_UDF;
		} else if(optype == AS_SCAN_UDF_OP_BACKGROUND) {
			cf_debug(AS_UDF, "UDF scan background op received");
			call->udf_type = AS_SCAN_UDF_OP_BACKGROUND;
		} else {
			cf_warning(AS_UDF, "Undefined udf type received over protocol");
			goto Cleanup;
		}
	}
	filename = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_FILENAME);
	if ( filename ) {
		function = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_FUNCTION);
		if ( function ) {
			arglist = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_ARGLIST);
			if ( arglist ) {
				call->transaction = tr;
				as_msg_field_get_strncpy(filename, &call->filename[0], sizeof(call->filename));
				as_msg_field_get_strncpy(function, &call->function[0], sizeof(call->function));
				call->arglist = arglist;
				call->active = true;
				cf_detail(AS_UDF, "UDF Request Unpacked %s %s", call->filename, call->function);
				return 0;
			}
		}
	}
Cleanup:
	call->transaction = NULL;
	call->filename[0] = 0;
	call->function[0] = 0;
	call->arglist = NULL;

	return 1;
}

/*
 * Cleans up udf call
 *
 * Returns: 0 on success
 */
void
udf_call_destroy(udf_call * call)
{
	call->transaction = NULL;
	call->arglist = NULL;
}

/*
 * Looks at the flags set in udf_record and determines if it is
 * read / write or delete operation
 */
void 
udf_rw_getop(udf_record *urecord, udf_optype *urecord_op)
{ 
	if (urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) {
		// Check if the record is not deleted after an update
		if ( urecord->flag & UDF_RECORD_FLAG_OPEN) {
			*urecord_op = UDF_OPTYPE_WRITE;
		} 
		else {
			// If the record has updates and it is not open, 
			// and if it pre-existed it's an update followed by a delete.
			if ( urecord->flag & UDF_RECORD_FLAG_PREEXISTS) {
				*urecord_op = UDF_OPTYPE_DELETE;
			} 
			// If the record did not pre-exist and is updated
			// and it is not open, then it is create followed by
			// delete essentially no_op.
			else {
				*urecord_op = UDF_OPTYPE_NONE;
			}
		}
	} else if ((urecord->flag & UDF_RECORD_FLAG_PREEXISTS)
			   && !(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		*urecord_op  = UDF_OPTYPE_DELETE;
	} else {
		*urecord_op  = UDF_OPTYPE_READ;
	}

	// If there exists a record reference but no bin of the record is in use,
	// delete the record. remove from the tree. Only LDT_RECORD here not needed
	// for LDT_SUBRECORD (only do it if requested by UDF). All the SUBRECORD of
	// removed LDT_RECORD will be lazily cleaned up by defrag.
	if (!(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)
			&& (urecord->flag & UDF_RECORD_FLAG_OPEN)
			&& !as_bin_inuse_has(urecord->rd)) {
		as_transaction *tr = urecord->tr;
		as_index_delete(tr->rsv.tree, &tr->keyd);
		urecord->starting_memory_bytes = 0;
		*urecord_op                    = UDF_OPTYPE_DELETE;
	}
}

/*
 * Helper for udf_rw_post_processing().
 */
void
udf_rw_write_post_processing(as_transaction *tr, as_storage_rd *rd,
		uint8_t **pickled_buf, size_t *pickled_sz, uint32_t *pickled_void_time,
		as_rec_props *p_pickled_rec_props, int64_t memory_bytes)
{
	update_metadata_in_index(tr, true, rd->r);

	pickle_info pickle;

	pickle_all(rd, &pickle);

	*pickled_buf = pickle.buf;
	*pickled_sz = pickle.buf_size;
	*pickled_void_time = pickle.void_time;
	p_pickled_rec_props->p_data = pickle.rec_props_data;
	p_pickled_rec_props->size = pickle.rec_props_size;

	tr->generation = rd->r->generation;
	tr->void_time = rd->r->void_time;

	if (tr->rsv.ns->storage_data_in_memory) {
		account_memory(tr, rd, memory_bytes);
	}
}

/* Internal Function: Does the post processing for the UDF record after the
 *					  UDF execution. Does the following:
 *		1. Record is closed
 *		2. urecord_op is updated to delete in case there is no bin left in it.
 *		3. record->pickled_buf is populated before the record is close in case
 *		   it was write operation
 *		4. UDF updates cache is cleared
 *
 *	Returns: Nothing
 *
 *	Parameters: urecord          - UDF record to operate on
 *				urecord_op (out) - Populated with the optype
 */
void
udf_rw_post_processing(udf_record *urecord, udf_optype *urecord_op, uint16_t set_id)
{
	as_storage_rd      *rd   = urecord->rd;
	as_transaction     *tr   = urecord->tr;
	as_index_ref    *r_ref   = urecord->r_ref;

	// INIT
	urecord->pickled_buf     = NULL;
	urecord->pickled_sz      = 0;
	urecord->pickled_void_time     = 0;
	as_rec_props_clear(&urecord->pickled_rec_props);
	bool udf_xdr_ship_op = false;

	udf_rw_getop(urecord, urecord_op);

	if (UDF_OP_IS_DELETE(*urecord_op)
			|| UDF_OP_IS_WRITE(*urecord_op)) {
		udf_xdr_ship_op = true;
	}

	cf_detail(AS_UDF, "FINISH working with LDT Record %p %p %p %p %d", &urecord,
			urecord->tr, urecord->r_ref, urecord->rd,
			(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN));

	if (*urecord_op == UDF_OPTYPE_WRITE)	{
		cf_detail_digest(AS_UDF, &rd->keyd, "Committing Changes n_bins %d", as_bin_get_n_bins(r_ref->r, rd));

		size_t  rec_props_data_size = as_storage_record_rec_props_size(rd);
		uint8_t rec_props_data[rec_props_data_size];
		if (rec_props_data_size > 0) {
			as_storage_record_set_rec_props(rd, rec_props_data);
		}

		udf_rw_write_post_processing(tr, rd, &urecord->pickled_buf,
			&urecord->pickled_sz, &urecord->pickled_void_time,
			&urecord->pickled_rec_props, urecord->starting_memory_bytes);

		// Now ok to accommodate a new stored key...
		if (! as_index_is_flag_set(r_ref->r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
			if (rd->ns->storage_data_in_memory) {
				as_record_allocate_key(r_ref->r, rd->key, rd->key_size);
			}

			as_index_set_flags(r_ref->r, AS_INDEX_FLAG_KEY_STORED);
		}
		// ... or drop a stored key.
		else if (as_index_is_flag_set(r_ref->r, AS_INDEX_FLAG_KEY_STORED) && ! rd->key) {
			if (rd->ns->storage_data_in_memory) {
				as_record_remove_key(r_ref->r);
			}

			as_index_clear_flags(r_ref->r, AS_INDEX_FLAG_KEY_STORED);
		}
	}

	// Collect the record information (for XDR) before closing the record
	as_generation generation = 0;
	if (urecord->flag & UDF_RECORD_FLAG_OPEN) {
		generation = r_ref->r->generation;
		set_id = as_index_get_set_id(r_ref->r);
	}
	urecord->op = *urecord_op;
	// Close the record for all the cases
	udf_record_close(urecord);

	// Write to XDR pipe after closing the record, in order to release the record lock as
	// early as possible.
	if (udf_xdr_ship_op == true) {
		if (UDF_OP_IS_WRITE(*urecord_op)) {
			cf_detail(AS_UDF, "UDF write shipping for key %" PRIx64, tr->keyd);
			xdr_write(tr->rsv.ns, tr->keyd, generation, 0, false, set_id);
		} else if (UDF_OP_IS_DELETE(*urecord_op)) {
			cf_detail(AS_UDF, "UDF delete shipping for key %" PRIx64, tr->keyd);
			xdr_write(tr->rsv.ns, tr->keyd, generation, 0, true, set_id);
		}
	}
}

/*
 * Function based on the UDF result and the result of UDF call along
 * with the optype information update the UDF stats and LDT stats.
 *
 * Parameter:
 *  	op:           execute optype
 *  	is_success :  In case the UDF operation was successful
 *  	ret        :  return value of UDF execution
 *
 *  Returns: nothing
*/
void
udf_rw_update_stats(as_namespace *ns, udf_optype op, int ret, bool is_success, bool is_ldt)
{
	if (is_ldt) {
		if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&ns->lstats.ldt_read_reqs);
		else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&ns->lstats.ldt_delete_reqs);
		else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&ns->lstats.ldt_write_reqs);

		if (ret == 0) {
			if (is_success) {
				if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&ns->lstats.ldt_read_success);
				else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&ns->lstats.ldt_delete_success);
				else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&ns->lstats.ldt_write_success);
			} else {
				cf_atomic_int_incr(&ns->lstats.ldt_errs);
			}
		} else {
			cf_atomic_int_incr(&g_config.udf_lua_errs);
		}
	} 

	if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&g_config.udf_read_reqs);
	else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&g_config.udf_delete_reqs);
	else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&g_config.udf_write_reqs);

	if (ret == 0) {
		if (is_success) {
			if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&g_config.udf_read_success);
			else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&g_config.udf_delete_success);
			else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&g_config.udf_write_success);
		} else {
			if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&g_config.udf_read_errs_other);
			else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&g_config.udf_delete_errs_other);
			else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&g_config.udf_write_errs_other);
		}
	} else {
		cf_info(AS_UDF,"lua error, ret:%d",ret);
		cf_atomic_int_incr(&g_config.udf_lua_errs);
	}
}

/*
 *  Write the record to the storage in case there are write and closes the
 *  record and frees up the stuff. With the pickled buf for each udf_record
 *  it create single pickled buf for the entire LDT to be sent to the remote
 *  for replica.
 *
 *  Parameter:
 *  	lrecord : LDT record to operate on
 *  	pickled_* (out) to be populated is null if there was delete
 *		lrecord_op (out) is set properly for the entire ldt
 *	set_id : Set id for record. Passed for delete operation.
 *
 *  Returns: true always
 */
extern uint32_t as_storage_record_size(as_storage_rd *rd);
bool
udf_rw_finish(ldt_record *lrecord, write_request *wr, udf_optype * lrecord_op, uint16_t set_id)
{
	int subrec_count = 0;
	// LDT: Commit all the changes being done to the all records.
	// TODO: remove limit of 6 (note -- it's temporarily up to 20)
	udf_optype urecord_op = UDF_OPTYPE_READ;
	*lrecord_op           = UDF_OPTYPE_READ;
	udf_record *h_urecord = as_rec_source(lrecord->h_urec);
	bool is_ldt           = false;
	int  ret              = 0;
	uint32_t total_flat_size = 0; 

	udf_rw_getop(h_urecord, &urecord_op);
	// In case required 
	// wr->pickled_ldt_version = lrecord->version;

	if (urecord_op == UDF_OPTYPE_DELETE) {
		wr->pickled_buf      = NULL;
		wr->pickled_sz       = 0;
		wr->pickled_void_time   = 0;
		as_rec_props_clear(&wr->pickled_rec_props);
		wr->ldt_rectype_bits = h_urecord->ldt_rectype_bits;
		*lrecord_op  = UDF_OPTYPE_DELETE;
	} else {

		if (urecord_op == UDF_OPTYPE_WRITE) {
			*lrecord_op = UDF_OPTYPE_WRITE;
		}

		FOR_EACH_SUBRECORD(i, j, lrecord) {
			urecord_op = UDF_OPTYPE_READ;
			is_ldt = true;
			subrec_count++;
			udf_record *c_urecord = &lrecord->chunk[i].slots[j].c_urecord;
			if (g_config.ldt_benchmarks) {
				udf_rw_getop(c_urecord, &urecord_op);
				if (UDF_OP_IS_WRITE(urecord_op)) {
					if (c_urecord->tr->rsv.ns
						&& NAMESPACE_HAS_PERSISTENCE(c_urecord->tr->rsv.ns)
						&& c_urecord->rd) {
						total_flat_size += as_storage_record_size(c_urecord->rd);
					}
				}
			}
			udf_rw_post_processing(c_urecord, &urecord_op, set_id);
		}

		// Process the parent record in the end .. this is to make sure
		// the lock is held till the end. 
		if (g_config.ldt_benchmarks) {
			udf_rw_getop(h_urecord, &urecord_op);
			if (UDF_OP_IS_WRITE(urecord_op)) { 
				if (h_urecord->tr->rsv.ns
					&& NAMESPACE_HAS_PERSISTENCE(h_urecord->tr->rsv.ns)
					&& h_urecord->rd) {
					total_flat_size += as_storage_record_size(h_urecord->rd);
				}
			}
		}
		udf_rw_post_processing(h_urecord, &urecord_op, set_id);

		if (is_ldt) {
			// Create the multiop pickled buf for thr_rw.c
			ret = as_ldt_record_pickle(lrecord, &wr->pickled_buf, &wr->pickled_sz, &wr->pickled_void_time);
			FOR_EACH_SUBRECORD(i, j, lrecord) {
				udf_record *c_urecord = &lrecord->chunk[i].slots[j].c_urecord;
				// Cleanup in case pickle code bailed out
				// 1. either because this single node run no replica
				// 2. failed to pack stuff up.
				udf_record_cleanup(c_urecord, true);
			}
		} else {
			// Normal UDF case simply pass on pickled buf created for the record
			wr->pickled_buf       = h_urecord->pickled_buf;
			wr->pickled_sz        = h_urecord->pickled_sz;
			wr->pickled_void_time = h_urecord->pickled_void_time;
			wr->pickled_rec_props = h_urecord->pickled_rec_props;
			wr->ldt_rectype_bits = h_urecord->ldt_rectype_bits;
			udf_record_cleanup(h_urecord, false);
		}
	}
	udf_record_cleanup(h_urecord, true);
	if (UDF_OP_IS_WRITE(*lrecord_op) && (lrecord->udf_context & UDF_CONTEXT_LDT)) {
		// When showing in histogram the record which touch 0 subrecord and 1 subrecord 
		// will show up in same bucket. +1 for record as well. So all the request which 
		// touch subrecord as well show up in 2nd bucket
		histogram_insert_raw(g_config.ldt_update_io_bytes_hist, total_flat_size);
		histogram_insert_raw(g_config.ldt_update_record_cnt_hist, subrec_count + 1);
	}

	if (is_ldt) {
		if (UDF_OP_IS_WRITE(*lrecord_op)) {
			*lrecord_op = UDF_OPTYPE_LDT_WRITE;
		} else if (UDF_OP_IS_DELETE(*lrecord_op)) {
			*lrecord_op = UDF_OPTYPE_LDT_DELETE;
		} else if (UDF_OP_IS_READ(*lrecord_op)) {
			*lrecord_op = UDF_OPTYPE_LDT_READ;
		}
	}

	if (ret) {
		cf_warning(AS_LDT, "Pickeling failed with %d", ret);
		return false;
	} else {
		return true;
	}
}

/*
 * UDF time tracker hook
 */
uint64_t
as_udf__end_time(time_tracker *tt)
{
	ldt_record *lr  = (ldt_record *) tt->udata;
	if (!lr) return -1;
	udf_record *r   = (udf_record *) as_rec_source(lr->h_urec);
	if (!r)  return -1;
	// If user has not specified timeout pick the max on server
	// side
	return (r->tr->end_time)
		   ? r->tr->end_time
		   : r->tr->start_time + g_config.transaction_max_ns;
}

/*
 * UDF memory tracker hook
 * Todo comments
 */
bool
as_udf__mem_op(mem_tracker *mt, uint32_t num_bytes, memtracker_op op)
{
	bool ret = true;
	if (!mt || !mt->udata) {
		return false;
	}
	uint64_t val = 0;

	udf_record *r = (udf_record *) mt->udata;
	if (r) return false;

	if (op == MEM_RESERVE) {
		val = cf_atomic_int_add(&g_config.udf_runtime_gmemory_used, num_bytes);
		if (val > g_config.udf_runtime_max_gmemory) {
			cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used, num_bytes);
			ret = false;
			goto END;
		}

		val = cf_atomic_int_add(&r->udf_runtime_memory_used, num_bytes);
		if (val > g_config.udf_runtime_max_memory) {
			cf_atomic_int_sub(&r->udf_runtime_memory_used, num_bytes);
			cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used, num_bytes);
			ret = false;
			goto END;
		}
	} else if (op == MEM_RELEASE) {
		cf_atomic_int_sub(&r->udf_runtime_memory_used, num_bytes);
	} else if (op == MEM_RESET) {
		cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used,
			r->udf_runtime_memory_used);
		r->udf_runtime_memory_used = 0;
	} else {
		ret = false;
	}
END:
	return ret;
}

/*
 * Wrapper function over call to lua.  Setup arglist and memory tracker before
 * making the call.
 *
 * Returns: return from the UDF execution
 *
 * Parameter: call - udf call being executed
 * 			  rec  - as_rec on which UDF needs to be operated. The caller
 * 			         sets it up
 * 			  res  - Result to be populated by execution
 *
 */
int
udf_apply_record(udf_call * call, as_rec *rec, as_result *res)
{
	as_list         arglist;
	as_list_init(&arglist, call->arglist, &udf_arglist_hooks);

	cf_detail(AS_UDF, "Calling %s.%s()", call->filename, call->function);
	mem_tracker udf_mem_tracker = {
		.udata  = as_rec_source(rec),
		.cb     = as_udf__mem_op,
	};
	udf_memtracker_setup(&udf_mem_tracker);

	// Setup time tracker
	time_tracker udf_timer_tracker = {
		.udata     = as_rec_source(rec),
		.end_time  = as_udf__end_time
	};
	udf_timer_setup(&udf_timer_tracker);
	as_timer timer;
	as_timer_init(&timer, &udf_timer_tracker, &udf_timer_hooks);

	as_udf_context ctx = {
		.as         = &g_ldt_aerospike,
		.timer      = &timer,
		.memtracker = NULL
	};

	uint64_t now = cf_getns();
	int ret_value = as_module_apply_record(&mod_lua, &ctx,
			call->filename, call->function, rec, &arglist, res);
	cf_hist_track_insert_data_point(g_config.ut_hist, now);
	if (g_config.ldt_benchmarks) {
		ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
		if (lrecord->udf_context & UDF_CONTEXT_LDT) {
			histogram_insert_data_point(g_config.ldt_hist, now);
		}
	}
	udf_memtracker_cleanup();
	udf_timer_cleanup();
	as_list_destroy(&arglist);

	return ret_value;
}

/*
 * Current send response call back for the UDF execution
 *
 * Side effect : Will clean up response udata and data in it.
 *               caller should not refer to it after this
 */
int udf_rw_sendresponse(as_transaction *tr, int retcode)
{
	cf_detail(AS_UDF, "Sending UDF Request response=%p retcode=%d", tr->udata.res_udata, retcode);
	udf_call      * call = ((udf_response_udata *)tr->udata.res_udata)->call;
	as_result     * res  = ((udf_response_udata *)tr->udata.res_udata)->res;
	tr->result_code      = retcode;
	call->transaction    = tr;
	send_result(res, call, NULL);
	as_result_destroy(res);
	udf_call_destroy(call);
	cf_free(call);
	cf_free(tr->udata.res_udata);
	return 0;
}

/*
 * Function to set up the response callback functions
 * See udf_rw_complete for the details of logic
 */
int udf_rw_addresponse(as_transaction *tr, void *udata)
{
	if (!tr) {
		cf_warning(AS_UDF, "Invalid Transaction");
		return -1;
	}
	tr->udata.res_cb    = udf_rw_sendresponse;
	tr->udata.res_udata = udata;
	return 0;
}

/*
 * Main workhorse function which is parallel to write_local called from
 * internal_rw_start. Does the following
 *
 * 1. Opens up the record if it exists
 * 2. Sets up UDF record
 * 3. Sets up encapsulating LDT record (Before execution we do not know if
 * 	  UDF is for record or LDT)
 * 4. Calls function to run UDF
 * 5. Call udf_rw_finish to wrap up execution
 * 6. Either sends response back to client or based on response
 *    setup response callback in transaction.
 *
 * Parameter:
 * 		call - UDF call to be executed
 * 		pickled_buf
 * 		pickled_sz
 * 		pickled_void_time (OUT) Filled up when request returns in case
 * 		                  there was a write. This is used for
 * 		                  replication
 * 		op   - (OUT) Returns op type of operation performed by UDF
 *
 * Returns: Always 0
 *
 * Side effect
 * 	pickled buf is populated user should free it up.
 *  Setups response callback should be called at the end of transaction.
 */
int
udf_rw_local(udf_call * call, write_request *wr, udf_optype *op)
{
	*op = UDF_OPTYPE_NONE;

	// Step 1: Setup UDF Record and LDT record
	as_transaction *tr = call->transaction;
	as_index_ref    r_ref;
	r_ref.skip_lock = false;

	as_storage_rd  rd;

	udf_record urecord;
	udf_record_init(&urecord);

	ldt_record lrecord;
	ldt_record_init(&lrecord);

	urecord.tr                 = tr;
	urecord.r_ref              = &r_ref;
	urecord.rd                 = &rd;
	as_rec          urec;
	as_rec_init(&urec, &urecord, &udf_record_hooks);

	// NB: rec needs to be in the heap. Once passed in to the lua scope if
	// this val get assigned it may get garbage collected post stack context
	// is lost. In conjunction the destroy hook for this rec is set to NULL
	// to avoid attempting any garbage collection. For ldt_record clean up
	// and post processing has to be in process context under transactional
	// protection.
	as_rec  *lrec = as_rec_new(&lrecord, &ldt_record_hooks);

	// Link lrecord and urecord
	lrecord.h_urec             = &urec;
	urecord.lrecord            = &lrecord;
	urecord.keyd               = tr->keyd;

	// Set id for XDR shipping.
	uint32_t set_id = INVALID_SET_ID;

	// Step 2: Setup Storage Record
	int rec_rv = as_record_get(tr->rsv.tree, &tr->keyd, &r_ref, tr->rsv.ns);

	if (rec_rv == 0 && as_record_is_expired(r_ref.r)) {
		// If record is expired, pretend it was not found.
		as_record_done(&r_ref, tr->rsv.ns);
		rec_rv = -1;
	}

	if (rec_rv == 0) {
		urecord.flag   |= UDF_RECORD_FLAG_OPEN;
		urecord.flag   |= UDF_RECORD_FLAG_PREEXISTS;
		cf_detail(AS_UDF, "Open %p %x %"PRIx64"", &urecord, urecord.flag, *(uint64_t *)&tr->keyd);
		udf_storage_record_open(&urecord);

		as_msg *m = &tr->msgp->msg;

		// If both the record and the message have keys, check them.
		if (rd.key) {
			if (msg_has_key(m) && ! check_msg_key(m, &rd)) {
				udf_record_close(&urecord);
				call->transaction->result_code = AS_PROTO_RESULT_FAIL_KEY_MISMATCH;
				// Necessary to complete transaction, but error string would be
				// ignored by client, so don't bother sending one.
				send_response(call, "FAILURE", AS_PARTICLE_TYPE_NULL, NULL, 0);
				// free everything we created - the rec destroy with ldt_record hooks
				// destroys the ldt components and the attached "base_rec"
				ldt_record_destroy(lrec);
				as_rec_destroy(lrec);
				return 0;
			}
		}
		else {
			// If the message has a key, apply it to the record.
			if (! get_msg_key(m, &rd)) {
				udf_record_close(&urecord);
				call->transaction->result_code = AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE;
				send_response(call, "FAILURE", AS_PARTICLE_TYPE_NULL, NULL, 0);
				ldt_record_destroy(lrec);
				as_rec_destroy(lrec);
				return 0;
			}

			urecord.flag |= UDF_RECORD_FLAG_METADATA_UPDATED;
		}

		// While opening parent record read the record from the disk. Property
		// map is created from LUA world. The version can only be get in case
		// the property map bin is there. If not there the record is normal
		// record
		uint64_t lversion;
		int rv = as_ldt_parent_storage_get_version(&rd, &lversion, false,__FILE__, __LINE__);
		if (rv == 0) {
			lrecord.version = lversion;
		} else {
			lrecord.version = as_ldt_generate_version();
		}
		cf_detail_digest(AS_LDT, &urecord.keyd, "LDT_VERSION Read Version From Storage %ld rv=%d",
				  lrecord.version, rv);

		// Save the set-ID for XDR.
		// In case of deletion, this information will not be available later.
		// This information will be used only in case of xdr deletion ship.
		set_id = as_index_get_set_id(urecord.r_ref->r);
	} else {
		urecord.flag   &= ~(UDF_RECORD_FLAG_OPEN
							| UDF_RECORD_FLAG_STORAGE_OPEN
							| UDF_RECORD_FLAG_PREEXISTS);
	}


	// entry point for all SMD-UDF's(LDT) calls, not called for other UDF's.
	// At this point, we wont know if its a regular record or a LDT UDF.
	cf_detail(AS_UDF, "START working with LDT/Regular Record UDF operation %p %p %p %p %d", &urecord,
			urecord.tr, urecord.r_ref, urecord.rd,
			(urecord.flag & UDF_RECORD_FLAG_STORAGE_OPEN));

	// Step 3: Run UDF
	as_result       *res = as_result_new();
	as_val_reserve(lrec);
	int ret_value        = udf_apply_record(call, lrec, res);
	as_namespace *  ns   = tr->rsv.ns;
	// Capture the success of the Lua call to use below
	bool success = res->is_success;

	if (ret_value == 0) {

		if (lrecord.udf_context & UDF_CONTEXT_LDT) {
			histogram_insert_raw(g_config.ldt_io_record_cnt_hist, lrecord.subrec_io + 1);
		}

		if (udf_rw_finish(&lrecord, wr, op, set_id) == false) {
			// replication did not happen what to do now ??
			cf_warning(AS_UDF, "Investigate udf_rw_finish() result");
		}

		udf_rw_update_ldt_err_stats(ns, res);

		if (UDF_OP_IS_READ(*op) || *op == UDF_OPTYPE_NONE) {
			send_result(res, call, NULL);
			as_result_destroy(res);
		} else {
			udf_response_udata *udata = cf_malloc(sizeof(udf_response_udata));
			udata->call               =  call;
			udata->res                =  res;
			cf_detail(AS_UDF, "Setting UDF Request Response data=%p with udf op %d", udata, *op);
			udf_rw_addresponse(tr, udata);
		}

		// TODO this is not the right place for counter, put it at proper place
		if(tr->udata.req_udata && tr->udata.req_type == UDF_SCAN_REQUEST) {
			cf_atomic_int_add(&((tscan_job*)(tr->udata.req_udata))->n_obj_udf_updated, (*op == UDF_OPTYPE_WRITE));
		}

	} else {
		udf_record_close(&urecord);
		char *rs = as_module_err_string(ret_value);
		call->transaction->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
		send_response(call, "FAILURE", AS_PARTICLE_TYPE_STRING, rs, strlen(rs));
		cf_free(rs);
		as_result_destroy(res);
	}

	udf_rw_update_stats(ns, *op, ret_value, success, (lrecord.udf_context & UDF_CONTEXT_LDT));

	// free everything we created - the rec destroy with ldt_record hooks
	// destroys the ldt components and the attached "base_rec"
	ldt_record_destroy(lrec);
	as_rec_destroy(lrec);

	return 0;
}

/*
 * Function called to determine if needs to call udf_rw_complete.
 * See the definition below for detail
 *
 * NB: Callers needs to hold necessary protection
 */
bool
udf_rw_needcomplete_wr( write_request *wr )
{
	if (!wr) {
		cf_warning(AS_UDF, "Invalid write request");
		return false;
	}
	ureq_data *ureq = (ureq_data *)&wr->udata;
	if (ureq->req_cb && ureq->req_udata) {
		return true;
	}
	if (ureq->res_cb && ureq->res_udata) {
		return true;
	}
	return false;
}

/*
 * Function called to determine if needs to call udf_rw_complete.
 * See the definition below for detail
 *
 * NB: Callers needs to hold necessary protection
 */
bool
udf_rw_needcomplete( as_transaction *tr )
{
	if (!tr) {
		cf_warning(AS_UDF, "Invalid write request");
		return false;
	}
	ureq_data *ureq = (ureq_data *)&tr->udata;
	if (ureq->req_cb && ureq->req_udata) {
		return true;
	}
	if (ureq->res_cb && ureq->res_udata) {
		return true;
	}
	return false;
}

/*
 * Function called when the transaction performing UDF finishes.
 * It looks at the request and response callback set with the request
 * and response udata.
 *
 * Request Callback:  It is set by special request to be run at the end
 * 					  of a request. Current user is UDF scan which sets
 * 					  it up in the internal transaction to perform run
 * 					  UDF of scanned data.
 * Response Callback: It is set for sending response to client at the
 * 					  end of the transaction. Currently used by UDF
 * 					  to send back special results after the replication
 * 					  has finished.
 *
 * NB: Callback function is response to make sure the data is intact
 *     and properly handled. And its synchronization if required.
 *
 * Returns : Nothing
 *
 * Caller:
 * 		proxy_msg_fn
 * 		proxy_retransmit_reduce_fn
 * 		write_request_destructor
 * 		internal_rw_start
 * 		as_rw_start
 * 		rw_retransmit_reduce_fn
 * 		thr_tsvc_read
 * 		udf_rw_complete
 */
void
udf_rw_complete(as_transaction *tr, int retcode, char *filename, int lineno )
{
	cf_debug(AS_UDF, "[ENTER] file(%s) line(%d)", filename, lineno );

	if ( !tr ) {
		cf_warning(AS_UDF, "Invalid internal request");
	}
	ureq_data *ureq = &tr->udata;
	if (ureq->req_cb && ureq->req_udata) {
		ureq->req_cb( tr, retcode );
	}

	if (ureq->res_cb && ureq->res_udata) {
		ureq->res_cb( tr, retcode);
	}
	cf_detail(AS_UDF, "UDF_COMPLETED:[%s:%d] %p %p %p %p %p",
			filename, lineno, tr , ureq->req_cb, ureq->req_udata, ureq->res_cb, ureq->res_udata);
	UREQ_DATA_RESET(&tr->udata);
}

/*
 * Internal Function: Serialize passed in as_val into passed in buffer.
 * 					  msgpack is used for packing
 *
 * 					  Note that passed in buffer should have enough space
 * 					  .. caller is responsible for figuring out space required
 * 					  and allocate it.
 *
 * 					  If passed in buffer is NULL only the required size is
 * 					  calculated.
 *
 * Parameters:
 * 		val     : as_val which needs to be laid out on buffer
 * 		buf     : buffer to lay serialize value on.
 * 		size    : size of the laid out data
 *
 * Return value : nothing. If all goes good the buffer is properly
 * 				  filled up and size value is set.
 *
 * Callers:
 * 		as_msg_make_val_response_bufbuilder
 * 		as_query__add_val_response
 */
void
as_val_tobuf(const as_val *v, uint8_t *buf, uint32_t *size)
{
	if ( v != NULL ) {
		switch( as_val_type(v) ) {
			case AS_NIL:
			{
				*size = 0;
				break;
			}
			case AS_INTEGER:
			{
				*size = 8;
				if (buf) {
					as_integer * i = as_integer_fromval(v);
					int64_t ri = __cpu_to_be64(as_integer_toint(i));
					memcpy(buf, &ri, *size);
				}
				break;
			}
			case AS_STRING:
			{
				as_string * s = as_string_fromval(v);
				*size = as_string_len(s);
				if (buf) {
					char * rs = (char *) as_string_tostring(s);
					memcpy(buf, rs, *size);
				}
				break;
			}
			case AS_MAP:
			case AS_LIST:
			{
				as_buffer asbuf;
				as_buffer_init(&asbuf);

				as_serializer s;
				as_msgpack_init(&s);

				int res = as_serializer_serialize(&s, (as_val*)v, &asbuf);

				if (res != 0) {
					cf_warning(AS_UDF, "List serialization failure (%d)", res);
					as_buffer_destroy(&asbuf);
					break;
				}
				*size = asbuf.size;
				if (buf) {
					memcpy(buf, asbuf.data, asbuf.size);
				}
				// not needed as it is stack allocated
				// as_serializer_destroy(&s);
				as_buffer_destroy(&asbuf);
				break;
			}
			// TODO: Resolve. Can we actually MAKE a value (the bin name) for
			// an LDT value?  Users should never see a real LDT value.
			case AS_LDT:
			{
				as_buffer asbuf;
				as_buffer_init(&asbuf);

				as_serializer s;
				as_msgpack_init(&s);

				as_string as_str;
				as_string_init( &as_str, "INT LDT BIN NAME", false );

				int res = as_serializer_serialize(&s, (as_val*) &as_str, &asbuf);

				if (res != 0) {
					cf_warning(AS_UDF, "LDT serialization failure (%d)", res);
					as_buffer_destroy(&asbuf);
					break;
				}
				*size = asbuf.size;
				if (buf) {
					memcpy(buf, asbuf.data, asbuf.size);
				}
				// not needed as it is stack allocated
				// as_serializer_destroy(&s);
				as_buffer_destroy(&asbuf);
				break;

			}
			default:
			{
				cf_debug(AS_UDF, "SUCCESS: VAL TYPE UNDEFINED %d\n",
						 as_val_type(v));
				*size = 0;
			}
		}
	}
	else {
		*size = 0;
	}
}

/*
 * Internal Function: Convert value in passed in as_bin into as_val.
 *                    This function allocates memory. Caller needs
 *                    to free it.
 *
 * Parameters:
 * 		bin    : bin for which as_val needs to be created
 *
 * Return value :
 * 		value  (as_val*) in case of success
 * 		NULL  in case of failure
 *
 * Description:
 * 		Based on the type of data as_val is allocated and data
 * 		and filled into it before returning. For the collections
 * 		map/list data is de-serialized before putting it into as_val.
 *
 * Callers:
 * 		udf_record_storage_get
 */
as_val *
as_val_frombin(as_bin *bb)
{
	as_val *value = NULL;
	uint8_t type = as_particle_type_convert(as_bin_get_particle_type(bb));

	switch ( type ) {
		case AS_PARTICLE_TYPE_INTEGER:
		{
			int64_t     i = 0;
			as_bin_particle_to_mem(bb, (uint8_t *) &i);
			value = (as_val *) as_integer_new(i);
			break;
		}
		case AS_PARTICLE_TYPE_STRING:
		{
			uint32_t psz = as_bin_particle_mem_size(bb);

			char * buf = cf_malloc(psz + 1);
			if (!buf) {
				return value;
			}

			as_bin_particle_to_mem(bb, (uint8_t *) buf);

			buf[psz] = '\0';

			value = (as_val *) as_string_new(buf, true /*ismalloc*/);
			break;
		}
		case AS_PARTICLE_TYPE_BLOB:
		{

			uint8_t *pbuf;
			uint32_t psz = as_bin_particle_ptr(bb, &pbuf);

			uint8_t *buf = cf_malloc(psz);
			if (!buf) {
				return value;
			}
			memcpy(buf, pbuf, psz);

			value = (as_val *) as_bytes_new_wrap(buf, psz, true);
			break;
		}
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST:
		{

			as_buffer     buf;
			as_buffer_init(&buf);

			as_serializer s;
			as_msgpack_init(&s);

			uint32_t sz = as_bin_particle_ptr(bb, (uint8_t **) &buf.data);
			buf.capacity = sz;
			buf.size = sz;

			as_serializer_deserialize(&s, &buf, &value);
			as_serializer_destroy(&s);
			break;
		}

		default:
		{
			value = NULL;
			break;
		}
	}
	return value;
}

int
to_particle_type(int from_as_type)
{
	switch (from_as_type) {
		case AS_NIL:
			return AS_PARTICLE_TYPE_NULL;
			break;
		case AS_BOOLEAN:
		case AS_INTEGER:
			return AS_PARTICLE_TYPE_INTEGER;
			break;
		case AS_STRING:
			return AS_PARTICLE_TYPE_STRING;
		case AS_BYTES:
			return AS_PARTICLE_TYPE_BLOB;
		case AS_LIST:
			return AS_PARTICLE_TYPE_LIST;
		case AS_MAP:
			return AS_PARTICLE_TYPE_MAP;
		case AS_UNKNOWN:
		case AS_REC:
		case AS_PAIR:
		default:
			cf_warning(AS_UDF, "unmappable type %d", from_as_type);
			break;
	}
	return AS_PARTICLE_TYPE_NULL;
}

static const cf_fault_severity as_level_map[5] = {
	[AS_LOG_LEVEL_ERROR] = CF_WARNING,
	[AS_LOG_LEVEL_WARN]	= CF_WARNING,
	[AS_LOG_LEVEL_INFO]	= CF_INFO,
	[AS_LOG_LEVEL_DEBUG] = CF_DEBUG,
	[AS_LOG_LEVEL_TRACE] = CF_DETAIL
};

static bool
as_udf_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	extern cf_fault_severity cf_fault_filter[CF_FAULT_CONTEXT_UNDEF];
	cf_fault_severity severity = as_level_map[level];

	if (severity > cf_fault_filter[AS_UDF]) {
		return true;
	}

	va_list ap;
	va_start(ap, fmt);
	char message[1024] = { '\0' };
	vsnprintf(message, 1024, fmt, ap);
	va_end(ap);

	cf_fault_event(AS_UDF, severity, file, NULL, line, message);
	return true;
}

void
as_udf_init(void)
{
	// Configure mod_lua.
	as_module_configure(&mod_lua, &g_config.mod_lua);

	// Setup logger for mod_lua.
	as_log_set_callback(as_udf_log_callback);

	if (0 > udf_cask_init()) {
		cf_crash(AS_UDF, "failed to initialize UDF cask");
	}

	as_aerospike_init(&g_as_aerospike, NULL, &udf_aerospike_hooks);
	// Assuming LDT UDF also comes in from udf_rw.
	ldt_init();
}
