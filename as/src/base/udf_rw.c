/*
 * udf_rw.c
 *
 * Copyright (C) 2012-2015 Aerospike, Inc.
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
#include "base/scan.h"
#include "base/thr_rw_internal.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "base/udf_aerospike.h"
#include "base/udf_arglist.h"
#include "base/udf_cask.h"
#include "base/udf_timer.h"
#include "base/write_request.h"
#include "base/xdr_serverside.h"

/*
 * Extern
 */
as_aerospike g_as_aerospike;

extern udf_call *as_query_get_udf_call(void *ptr);


// UDF Network Send Interface
// **************************************************************************************************
/* Internal Function: Packs up passed in data into as_bin which is
 *                    used to send result after the UDF execution.
 */

/* Workhorse function to send response back to the client after UDF execution.
 *
 * Assumption: The call should be setup properly pointing to the tr.
 *
 * Special Handling: If it is background udf job do not send any
 * 					 response to client
 */
int
send_response(udf_call *call, const char *bin_name, const as_val *val)
{
	// NO response if background UDF
	if (call->def.type == AS_UDF_OP_BACKGROUND) {
		return 0;
	}
	// Note - this function quietly handles a null val. The response call will
	// be given a bin with a name but not 'in use', and it does the right thing.

	as_bin stack_bin;
	as_bin *bin = &stack_bin;

	uint32_t particle_size = as_particle_size_from_asval(val);

	static const size_t MAX_STACK_SIZE = 32 * 1024;
	uint8_t stack_particle[particle_size > MAX_STACK_SIZE ? 0 : particle_size];
	uint8_t *particle_buf = stack_particle;

	if (particle_size > MAX_STACK_SIZE) {
		particle_buf = (uint8_t *)cf_malloc(particle_size);

		if (! particle_buf) {
			cf_warning(AS_UDF, "failed alloc for particle size %u", particle_size);
			return -1;
		}
	}

	as_transaction *tr = call->tr;
	as_namespace *ns = tr->rsv.ns;

	as_bin_init(ns, bin, bin_name);
	as_bin_particle_stack_from_asval(bin, particle_buf, val);

	single_transaction_response(tr, ns, NULL, &bin, 1, tr->generation, tr->void_time, NULL, NULL);

	if (particle_buf != stack_particle) {
		cf_free(particle_buf);
	}

	return 0;
} // end send_response()

static inline int
send_failure(udf_call *call, const as_val *val)
{
	return send_response(call, "FAILURE", val);
}

static inline int
send_failure_str(udf_call *call, const char *err_str, size_t len)
{
	if (! err_str) {
		// Better than sending an as_string with null value.
		return send_failure(call, NULL);
	}

	as_string stack_s;
	as_string_init_wlen(&stack_s, (char *)err_str, len, false);

	return send_failure(call, as_string_toval(&stack_s));
}

/**
 * Send failure notification of general UDF execution, but check for special
 * LDT errors and return specific Wire Protocol error codes for these cases:
 * (1) Record not found (2)
 * (2) LDT Collection item not found (125)
 *
 * All other errors get the generic 100 (UDF FAIL) code.
 */
static inline int
send_udf_failure(udf_call *call, const as_string *s)
{
	char *val = as_string_tostring(s);
	size_t vlen = as_string_len((as_string *)s); // TODO - make as_string_len() take const
	long error_code = ldt_get_error_code(val, vlen);

	if (error_code) {

		if (error_code == AS_PROTO_RESULT_FAIL_NOTFOUND ||
			error_code == AS_PROTO_RESULT_FAIL_COLLECTION_ITEM_NOT_FOUND) {

			call->tr->result_code = error_code;
			// Send an "empty" response, with no failure bin.
			as_transaction *    tr          = call->tr;
			single_transaction_response(tr, tr->rsv.ns, NULL/*ops*/,
					NULL /*bin*/, 0 /*nbins*/, 0, 0, NULL, NULL);
			return 0;
		}
	}

	cf_debug(AS_UDF, "Non-special LDT or General UDF Error(%s)", (char *) val);

	call->tr->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
	return send_failure(call, as_string_toval(s));
}

static inline int
send_success(udf_call *call, const as_val *val)
{
	// TODO - could check result and switch to send_failure()?
	return send_response(call, "SUCCESS", val);
}

/*
 * Internal Function: Entry function from UDF code path to send
 * 					  success result to the caller. Performs
 * 					  value translation.
 */
void
send_result(as_result * res, udf_call * call)
{
	as_val * v = res->value;
	if ( res->is_success ) {

		if ( cf_context_at_severity(AS_UDF, CF_DETAIL) ) {
			char * str = as_val_tostring(v);
			cf_detail(AS_UDF, "SUCCESS: %s", str);
			cf_free(str);
		}

		send_success(call, v);

	} else { // Else -- NOT success
		if (as_val_type(v) == AS_STRING) {
			send_udf_failure(call, as_string_fromval(v));
		} else {
			char lua_err_str[1024];
			size_t len = (size_t)sprintf(lua_err_str, "%s:0: in function %s() - error() argument type not handled", call->def.filename, call->def.function);

			call->tr->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
			send_failure_str(call, lua_err_str, len);
		}
	}
}
// **************************************************************************************************


/*
 * UDF Call Utility functions
 */
// **************************************************************************************************

/**
 * Initialize UDF from tr->udata. It is for internal UDF transactions
 * 
 * Returns:
 * 		0 on if found 
 * 		-1 if not found
 */
int
udf_rw_call_init_internal(udf_call * call, as_transaction * tr)
{
	udf_call *ucall = NULL;
	if (tr->udata.req_type == UDF_SCAN_REQUEST) {
		ucall = as_scan_get_udf_call(tr->udata.req_udata);
	} else if (tr->udata.req_type == UDF_QUERY_REQUEST) {
		ucall = as_query_get_udf_call(tr->udata.req_udata);
	}

	if (ucall) {
		strncpy (call->def.filename, ucall->def.filename, sizeof(ucall->def.filename));
		strncpy (call->def.function, ucall->def.function, sizeof(ucall->def.function));
		call->tr          = tr;
		call->def.arglist = ucall->def.arglist;
		call->def.type    = ucall->def.type;
		if (tr->udata.req_type == UDF_SCAN_REQUEST) {
			cf_atomic_int_incr(&g_config.udf_scan_rec_reqs);
		} else if (tr->udata.req_type == UDF_QUERY_REQUEST) {
			cf_atomic_int_incr(&g_config.udf_query_rec_reqs);
		}
		return 0;
	} 
	return -1;
}

/**
 * Initialize udf_call data structure from the msg over the wire
 *
 * Returns:
 * 		0 on success
 * 		-1 on failure
 */
int
udf_rw_call_init_from_msg(udf_call * call, as_msg *msg)
{
	call->def.type = AS_UDF_OP_KVS;
	as_msg_field *  filename = NULL;
	as_msg_field *  function = NULL;
	as_msg_field *  arglist =  NULL;

	// Check the type of udf
	as_msg_field *  op = NULL;
	op = as_msg_field_get(msg, AS_MSG_FIELD_TYPE_UDF_OP);
	if ( op ) {
		memcpy(&call->def.type, (byte *)op->data, sizeof(as_udf_op));
	}

	filename = as_msg_field_get(msg, AS_MSG_FIELD_TYPE_UDF_FILENAME);
	if ( filename ) {
		function = as_msg_field_get(msg, AS_MSG_FIELD_TYPE_UDF_FUNCTION);
		if ( function ) {
			arglist = as_msg_field_get(msg, AS_MSG_FIELD_TYPE_UDF_ARGLIST);
			if ( arglist ) {
				as_msg_field_get_strncpy(filename, &call->def.filename[0], sizeof(call->def.filename));
				as_msg_field_get_strncpy(function, &call->def.function[0], sizeof(call->def.function));
				call->def.arglist = arglist;
				return 0;
			}
		}
	}

	call->tr = NULL;
	call->def.filename[0] = 0;
	call->def.function[0] = 0;
	call->def.arglist = NULL;

	return -1;
}

/*
 * Cleans up udf call
 *
 * Returns: 0 on success
 */
void
udf_rw_call_destroy(udf_call * call)
{
	call->tr = NULL;
	call->def.arglist = NULL;
}
// **************************************************************************************************


static inline bool
udf_zero_bins_left(udf_record *urecord)
{
	if (!(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)
			&& (urecord->flag & UDF_RECORD_FLAG_OPEN)
			&& !as_bin_inuse_has(urecord->rd)) {
		return true;
	} else {
		return false;
	}
}

/*
 * Looks at the flags set in udf_record and determines if it is
 * read / write or delete operation
 */
static void 
getop(udf_record *urecord, udf_optype *urecord_op)
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

	if (udf_zero_bins_left(urecord)) {
		*urecord_op  = UDF_OPTYPE_DELETE;
	}
}

/*
 * Helper for post_processing().
 */
static void
write_udf_post_processing(as_transaction *tr, as_storage_rd *rd,
		uint8_t **pickled_buf, size_t *pickled_sz,
		as_rec_props *p_pickled_rec_props, int64_t memory_bytes)
{
	update_metadata_in_index(tr, true, rd->r);

	pickle_info pickle;

	pickle_all(rd, &pickle);

	*pickled_buf = pickle.buf;
	*pickled_sz = pickle.buf_size;
	p_pickled_rec_props->p_data = pickle.rec_props_data;
	p_pickled_rec_props->size = pickle.rec_props_size;

	tr->generation = rd->r->generation;
	tr->void_time = rd->r->void_time;

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
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
static void
post_processing(udf_record *urecord, udf_optype *urecord_op, uint16_t set_id)
{
	as_storage_rd      *rd   = urecord->rd;
	as_transaction     *tr   = urecord->tr;
	as_index_ref    *r_ref   = urecord->r_ref;

	// INIT
	urecord->pickled_buf     = NULL;
	urecord->pickled_sz      = 0;
	as_rec_props_clear(&urecord->pickled_rec_props);
	bool udf_xdr_ship_op = false;

	getop(urecord, urecord_op);

	if (UDF_OP_IS_DELETE(*urecord_op)
			|| UDF_OP_IS_WRITE(*urecord_op)) {
		udf_xdr_ship_op = true;
	}

	cf_detail(AS_UDF, "FINISH working with LDT Record %p %p %p %p %d", &urecord,
			urecord->tr, urecord->r_ref, urecord->rd,
			(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN));

	// If there exists a record reference but no bin of the record is in use,
	// delete the record. remove from the tree. Only LDT_RECORD here not needed
	// for LDT_SUBRECORD (only do it if requested by UDF). All the SUBRECORD of
	// removed LDT_RECORD will be lazily cleaned up by defrag.
	if (udf_zero_bins_left(urecord)) {
		as_transaction *tr = urecord->tr;
		as_index_delete(tr->rsv.tree, &tr->keyd);
		urecord->starting_memory_bytes = 0;
		*urecord_op                    = UDF_OPTYPE_DELETE;
	}
	else if (*urecord_op == UDF_OPTYPE_WRITE)	{
		cf_detail_digest(AS_UDF, &rd->keyd, "Committing Changes n_bins %d", as_bin_get_n_bins(r_ref->r, rd));

		size_t  rec_props_data_size = as_storage_record_rec_props_size(rd);
		uint8_t rec_props_data[rec_props_data_size];
		if (rec_props_data_size > 0) {
			as_storage_record_set_rec_props(rd, rec_props_data);
		}

		write_udf_post_processing(tr, rd, &urecord->pickled_buf,
			&urecord->pickled_sz, &urecord->pickled_rec_props,
			urecord->starting_memory_bytes);

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
static void
update_stats(as_namespace *ns, udf_optype op, int ret, bool is_success, bool is_ldt)
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
 *  Returns: 0 on success 
 *           otherwise on failure
 */
static int
rw_finish(ldt_record *lrecord, write_request *wr, udf_optype * lrecord_op, uint16_t set_id)
{
	int subrec_count = 0;
	udf_optype h_urecord_op = UDF_OPTYPE_READ;
	*lrecord_op           = UDF_OPTYPE_READ;
	udf_record *h_urecord = as_rec_source(lrecord->h_urec);
	bool is_ldt           = false;
	int  ret              = 0;

	getop(h_urecord, &h_urecord_op);

	if (h_urecord_op == UDF_OPTYPE_DELETE) {
		post_processing(h_urecord, &h_urecord_op, set_id);
		wr->pickled_buf      = NULL;
		wr->pickled_sz       = 0;
		as_rec_props_clear(&wr->pickled_rec_props);
		*lrecord_op  = UDF_OPTYPE_DELETE;
	} else {

		if (h_urecord_op == UDF_OPTYPE_WRITE) {
			*lrecord_op = UDF_OPTYPE_WRITE;
		}

		FOR_EACH_SUBRECORD(i, j, lrecord) {
			udf_optype c_urecord_op = UDF_OPTYPE_READ;
			udf_record *c_urecord = &lrecord->chunk[i].slots[j].c_urecord;
			getop(c_urecord, &c_urecord_op);

			if (UDF_OP_IS_WRITE(c_urecord_op)) {
				is_ldt = true;
				subrec_count++;
			}
			post_processing(c_urecord, &c_urecord_op, set_id);
		}

		// Process the parent record in the end .. this is to make sure
		// the lock is held till the end. 
		post_processing(h_urecord, &h_urecord_op, set_id);

		if (is_ldt) {
			// Create the multiop pickled buf for thr_rw.c
			ret = as_ldt_record_pickle(lrecord, &wr->pickled_buf, &wr->pickled_sz);
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
			wr->pickled_rec_props = h_urecord->pickled_rec_props;
			udf_record_cleanup(h_urecord, false);
		}
	}
	udf_record_cleanup(h_urecord, true);
	if (UDF_OP_IS_WRITE(*lrecord_op) && (lrecord->udf_context & UDF_CONTEXT_LDT)) {
		// When showing in histogram the record which touch 0 subrecord and 1 subrecord 
		// will show up in same bucket. +1 for record as well. So all the request which 
		// touch subrecord as well show up in 2nd bucket
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

	return ret;
}

/*
 * UDF time tracker hook
 */
static uint64_t
end_time(time_tracker *tt)
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
	as_list_init(&arglist, call->def.arglist, &udf_arglist_hooks);

	// Setup time tracker
	time_tracker udf_timer_tracker = {
		.udata     = as_rec_source(rec),
		.end_time  = end_time
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
			call->def.filename, call->def.function, rec, &arglist, res);
	cf_hist_track_insert_data_point(g_config.ut_hist, now);
	if (g_config.ldt_benchmarks) {
		ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
		if (lrecord->udf_context & UDF_CONTEXT_LDT) {
			histogram_insert_data_point(g_config.ldt_hist, now);
		}
	}
	udf_timer_cleanup();
	as_list_destroy(&arglist);

	return ret_value;
}

/*
 *
 * UDF Callback response interface 
 */
// **************************************************************************************************

/*
 * Current send response call back for the UDF execution
 *
 * Side effect : Will clean up response udata and data in it.
 *               caller should not refer to it after this
 */
int response_cb(as_transaction *tr, int retcode)
{
	udf_call      * call = ((udf_response_udata *)tr->udata.res_udata)->call;
	as_result     * res  = ((udf_response_udata *)tr->udata.res_udata)->res;
	tr->result_code      = retcode;
	call->tr             = tr;
	send_result(res, call);
	as_result_destroy(res);
	udf_rw_call_destroy(call);
	cf_free(call);
	cf_free(tr->udata.res_udata);
	return 0;
}

/*
 * Function to set up the response callback functions
 * See udf_rw_complete for the details of logic
 */
static inline int 
set_response_cb(as_transaction *tr, void *udata)
{
	if (!tr) {
		cf_warning(AS_UDF, "Invalid Transaction");
		return -1;
	}
	tr->udata.res_cb    = response_cb;
	tr->udata.res_udata = udata;
	return 0;
}
// **************************************************************************************************


/*
 * Main workhorse function which is parallel to write_local called from
 * internal_rw_start. Does the following
 *
 * 1. Opens up the record if it exists
 * 2. Sets up UDF record
 * 3. Sets up encapsulating LDT record (Before execution we do not know if
 * 	  UDF is for record or LDT)
 * 4. Calls function to run UDF
 * 5. Call rw_finish to wrap up execution
 * 6. Either sends response back to client or based on response
 *    setup response callback in transaction.
 *
 * Parameter:
 * 		call - UDF call to be executed
 * 		wr   - write_request 
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
	as_transaction *tr = call->tr;
	as_index_ref    r_ref;
	r_ref.skip_lock = false;

	as_storage_rd  rd;

	udf_record urecord;
	udf_record_init(&urecord, true);

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

		rec_rv = udf_storage_record_open(&urecord);

		if (rec_rv == -1) {
			udf_record_close(&urecord);
			call->tr->result_code = AS_PROTO_RESULT_FAIL_BIN_NAME; // overloaded... add bin_count error?
			send_failure(call, NULL);
			goto Cleanup;
		}

		as_msg *m = &tr->msgp->msg;

		// If both the record and the message have keys, check them.
		if (rd.key) {
			if (msg_has_key(m) && ! check_msg_key(m, &rd)) {
				udf_record_close(&urecord);
				call->tr->result_code = AS_PROTO_RESULT_FAIL_KEY_MISMATCH;
				send_failure(call, NULL);
				goto Cleanup;
			}
		}
		else {
			// If the message has a key, apply it to the record.
			if (! get_msg_key(m, &rd)) {
				udf_record_close(&urecord);
				call->tr->result_code = AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE;
				send_failure(call, NULL);
				goto Cleanup;
			}
			urecord.flag |= UDF_RECORD_FLAG_METADATA_UPDATED;
		}

		// If LDT parent record read version
		if (as_ldt_parent_storage_get_version(&rd, &lrecord.version, false,__FILE__, __LINE__)) {
			lrecord.version = as_ldt_generate_version();
		}

		// Save the set-ID for XDR in case record is deleted.
		set_id = as_index_get_set_id(urecord.r_ref->r);
	} else {
		urecord.flag   &= ~(UDF_RECORD_FLAG_OPEN
							| UDF_RECORD_FLAG_STORAGE_OPEN
							| UDF_RECORD_FLAG_PREEXISTS);
	}


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

		if (rw_finish(&lrecord, wr, op, set_id)) {
			// replication did not happen what to do now ??
			cf_warning(AS_UDF, "Investigate rw_finish() result");
		}

		if (!success) {
			ldt_update_err_stats(ns, res->value);
		}

		if (UDF_OP_IS_READ(*op) || *op == UDF_OPTYPE_NONE) {
			send_result(res, call);
			as_result_destroy(res);
		} else {
			udf_response_udata *udata = cf_malloc(sizeof(udf_response_udata));
			udata->call               =  call;
			udata->res                =  res;
			cf_detail(AS_UDF, "Setting UDF Request Response data=%p with udf op %d", udata, op);
			set_response_cb(tr, udata);
		}

	} else {
		udf_record_close(&urecord);
		char *rs = as_module_err_string(ret_value);
		call->tr->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
		send_failure_str(call, rs, strlen(rs));
		cf_free(rs);
		as_result_destroy(res);
	}

	update_stats(ns, *op, ret_value, success, (lrecord.udf_context & UDF_CONTEXT_LDT));

Cleanup:
	// free everything we created - the rec destroy with ldt_record hooks
	// destroys the ldt components and the attached "base_rec"
	ldt_record_destroy(&lrecord);
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
 * 		process_request
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

static const cf_fault_severity as_level_map[5] = {
	[AS_LOG_LEVEL_ERROR] = CF_WARNING,
	[AS_LOG_LEVEL_WARN]	= CF_WARNING,
	[AS_LOG_LEVEL_INFO]	= CF_INFO,
	[AS_LOG_LEVEL_DEBUG] = CF_DEBUG,
	[AS_LOG_LEVEL_TRACE] = CF_DETAIL
};

static bool
log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
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
as_udf_rw_init(void)
{
	// Configure mod_lua.
	as_module_configure(&mod_lua, &g_config.mod_lua);

	// Setup logger for mod_lua.
	as_log_set_callback(log_callback);

	if (0 > udf_cask_init()) {
		cf_crash(AS_UDF, "failed to initialize UDF cask");
	}

	as_aerospike_init(&g_as_aerospike, NULL, &udf_aerospike_hooks);
	// Assuming LDT UDF also comes in from udf_rw.
	ldt_init();
}
