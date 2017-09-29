/*
 * delete.c
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

#include "transaction/delete.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/xdr_serverside.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward Declarations.
//

bool start_delete_dup_res(rw_request* rw, as_transaction* tr);
bool start_delete_repl_write(rw_request* rw, as_transaction* tr);
bool delete_dup_res_cb(rw_request* rw);
bool delete_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
void delete_repl_write_cb(rw_request* rw);

void send_delete_response(as_transaction* tr);
void delete_timeout_cb(rw_request* rw);

static inline void
client_delete_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_client_delete_success);
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_delete_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_delete_error);
		break;
	case AS_PROTO_RESULT_FAIL_NOTFOUND:
		cf_atomic64_incr(&ns->n_client_delete_not_found);
		break;
	}
}


//==========================================================
// Public API.
//

transaction_status
as_delete_start(as_transaction* tr)
{
	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_FORBIDDEN;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	if (delete_storage_overloaded(tr)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_DEVICE_OVERLOAD;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->id, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);
	transaction_status status = rw_request_hash_insert(&hkey, rw, tr);

	// If rw_request wasn't inserted in hash, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_delete_response(tr);
		}

		return status;
	}
	// else - rw_request is now in hash, continue...

	if (g_config.write_duplicate_resolution_disable ||
			as_transaction_is_nsup_delete(tr)) {
		// Note - preventing duplicate resolution this way allows
		// rw_request_destroy() to handle dup_msg[] cleanup correctly.
		tr->rsv.n_dupl = 0;
	}

	// If there are duplicates to resolve, start doing so.
	// TODO - should we bother if there's no generation check?
	if (tr->rsv.n_dupl != 0) {
		if (! start_delete_dup_res(rw, tr)) {
			rw_request_hash_delete(&hkey, rw);
			tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
			send_delete_response(tr);
			return TRANS_DONE_ERROR;
		}

		// Started duplicate resolution.
		return TRANS_IN_PROGRESS;
	}
	// else - no duplicate resolution phase, apply operation to master.

	// If error, transaction is finished.
	if ((status = delete_master(tr, rw)) != TRANS_IN_PROGRESS) {
		rw_request_hash_delete(&hkey, rw);
		send_delete_response(tr);
		return status;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr->rsv.p,
			rw->dest_nodes);

	// If we don't need replica writes, transaction is finished.
	// TODO - consider a single-node fast path bypassing hash?
	if (rw->n_dest_nodes == 0) {
		rw_request_hash_delete(&hkey, rw);
		send_delete_response(tr);
		return TRANS_DONE_SUCCESS;
	}

	if (! start_delete_repl_write(rw, tr)) {
		rw_request_hash_delete(&hkey, rw);
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		send_delete_response(tr);
		return TRANS_DONE_ERROR;
	}

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction flow.
//

bool
start_delete_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send dup-res message.

	if (! dup_res_make_message(rw, tr)) {
		return false;
	}

	rw->respond_client_on_master_completion = respond_on_master_complete(tr);

	pthread_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, delete_dup_res_cb, delete_timeout_cb);
	send_rw_messages(rw);

	pthread_mutex_unlock(&rw->lock);

	return true;
}


bool
start_delete_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-delete message.

	if (! repl_write_make_message(rw, tr)) {
		return false;
	}

	rw->respond_client_on_master_completion = respond_on_master_complete(tr);

	if (rw->respond_client_on_master_completion) {
		// Don't wait for replication. When replication is complete, we won't
		// call send_delete_response() again.
		send_delete_response(tr);
	}

	pthread_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, delete_repl_write_cb, delete_timeout_cb);
	send_rw_messages(rw);

	pthread_mutex_unlock(&rw->lock);

	return true;
}


bool
delete_dup_res_cb(rw_request* rw)
{
	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	transaction_status status = delete_master(&tr, rw);

	if (status == TRANS_DONE_ERROR) {
		send_delete_response(&tr);
		return true;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr.rsv.p,
			rw->dest_nodes);

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		send_delete_response(&tr);
		return true;
	}

	if (! delete_repl_write_after_dup_res(rw, &tr)) {
		tr.result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		send_delete_response(&tr);
		return true;
	}

	// Started replica write - don't delete rw_request from hash.
	return false;
}


bool
delete_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	if (! repl_write_make_message(rw, tr)) {
		return false;
	}

	if (rw->respond_client_on_master_completion) {
		// Don't wait for replication. When replication is complete, we won't
		// call send_delete_response() again.
		send_delete_response(tr);
	}

	repl_write_reset_rw(rw, tr, delete_repl_write_cb);
	send_rw_messages(rw);

	return true;
}


void
delete_repl_write_cb(rw_request* rw)
{
	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	send_delete_response(&tr);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

void
send_delete_response(as_transaction* tr)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any && tr->origin != FROM_NSUP) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

	switch (tr->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(tr->from.proto_fd_h, tr->result_code, 0, 0, NULL,
				NULL, 0, NULL, as_transaction_trid(tr), NULL);
		client_delete_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_PROXY:
		as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
				tr->result_code, 0, 0, NULL, NULL, 0, NULL,
				as_transaction_trid(tr), NULL);
		break;
	case FROM_NSUP:
		break;
	case FROM_BATCH:
	case FROM_IUDF:
		// Should be impossible for batch reads and internal UDFs to get here.
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // needed only for respond-on-master-complete
}


void
delete_timeout_cb(rw_request* rw)
{
	if (! rw->from.any && rw->origin != FROM_NSUP) {
		return; // lost race against dup-res or repl-write callback
	}

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_PROTO_RESULT_FAIL_TIMEOUT, 0,
				0, NULL, NULL, 0, NULL, rw_request_trid(rw), NULL);
		client_delete_update_stats(rw->rsv.ns, AS_PROTO_RESULT_FAIL_TIMEOUT);
		break;
	case FROM_PROXY:
		break;
	case FROM_NSUP:
		break;
	case FROM_BATCH:
	case FROM_IUDF:
		// Should be impossible for batch reads and internal UDFs to get here.
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - delete master.
//

transaction_status
drop_master(as_transaction* tr, as_index_ref* r_ref, rw_request* rw)
{
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_index_tree* tree = tr->rsv.tree;
	as_record* r = r_ref->r;

	// Check generation requirement, if any.
	if (! generation_check(r, m)) {
		as_record_done(r_ref, ns);
		cf_atomic64_incr(&ns->n_fail_generation);
		tr->result_code = AS_PROTO_RESULT_FAIL_GENERATION;
		return TRANS_DONE_ERROR;
	}

	bool check_key = as_transaction_has_key(tr);

	if (ns->storage_data_in_memory || check_key) {
		as_storage_rd rd;
		as_storage_record_open(ns, r, &rd);

		// Check the key if required.
		// Note - for data-not-in-memory a key check is expensive!
		if (check_key && as_storage_record_get_key(&rd) &&
				! check_msg_key(m, &rd)) {
			as_storage_record_close(&rd);
			as_record_done(r_ref, ns);
			tr->result_code = AS_PROTO_RESULT_FAIL_KEY_MISMATCH;
			return TRANS_DONE_ERROR;
		}

		if (ns->storage_data_in_memory) {
			delete_adjust_sindex(&rd);
		}

		as_storage_record_close(&rd);
	}

	// Generate a binless pickle. but don't generate pickled rec-props - these
	// are useless for a drop.
	rw->pickled_sz = sizeof(uint16_t);
	rw->pickled_buf = cf_malloc(rw->pickled_sz);

	cf_assert(rw->pickled_buf, AS_RW, "failed pickle allocation");

	*(uint16_t*)rw->pickled_buf = 0;

	// Save the set-ID for XDR.
	uint16_t set_id = as_index_get_set_id(r);

	as_index_delete(tree, &tr->keyd);
	as_record_done(r_ref, ns);

	if (xdr_must_ship_delete(ns, as_transaction_is_nsup_delete(tr),
			as_msg_is_xdr(m))) {
		xdr_write(ns, tr->keyd, 0, 0, XDR_OP_TYPE_DROP, set_id, NULL);
	}

	return TRANS_IN_PROGRESS;
}
