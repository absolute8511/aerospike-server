/*
 * proxy.c
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

#include "transaction/proxy.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_shash.h"

#include "dynbuf.h"
#include "fault.h"
#include "msg.h"
#include "node.h"
#include "socket.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/stats.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"


//==========================================================
// Typedefs & constants.
//

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	PROXY_FIELD_OP,
	PROXY_FIELD_TID,
	PROXY_FIELD_DIGEST,
	PROXY_FIELD_REDIRECT,
	PROXY_FIELD_AS_PROTO, // request as_proto - currently contains only as_msg's
	PROXY_FIELD_CLUSTER_KEY,
	PROXY_FIELD_TIMEOUT_MS, // deprecated
	PROXY_FIELD_INFO,

	NUM_PROXY_FIELDS
} proxy_msg_field;

#define PROXY_OP_REQUEST 1
#define PROXY_OP_RESPONSE 2
#define PROXY_OP_RETURN_TO_SENDER 3

// LDT-related.
#define PROXY_INFO_SHIPPED_OP 0x0001

const msg_template proxy_mt[] = {
	{ PROXY_FIELD_OP, M_FT_UINT32 },
	{ PROXY_FIELD_TID, M_FT_UINT32 },
	{ PROXY_FIELD_DIGEST, M_FT_BUF },
	{ PROXY_FIELD_REDIRECT, M_FT_UINT64 },
	{ PROXY_FIELD_AS_PROTO, M_FT_BUF },
	{ PROXY_FIELD_CLUSTER_KEY, M_FT_UINT64 },
	{ PROXY_FIELD_TIMEOUT_MS, M_FT_UINT32 },
	{ PROXY_FIELD_INFO, M_FT_UINT32 },
};

COMPILER_ASSERT(sizeof(proxy_mt) / sizeof(msg_template) == NUM_PROXY_FIELDS);

#define PROXY_MSG_SCRATCH_SIZE 128

typedef struct proxy_request_s {
	uint32_t		msg_fields;

	uint8_t			origin;
	uint8_t			from_flags;

	union {
		void*				any;
		as_file_handle*		proto_fd_h;
		as_batch_shared*	batch_shared;
		// No need yet for other members of this union.
	} from;

	// No need yet for a 'from_data" union.
	uint32_t		batch_index;

	uint64_t		start_time;
	uint64_t		end_time;

	// Handle retransmits.
	msg*			fab_msg;
	cf_clock		xmit_ms;
	uint32_t		retry_interval_ms;

	// The node we're diverting to.
	cf_node			dest;

	uint32_t		pid;

	as_namespace*	ns;

	// LDT-related.
	rw_request*		rw; // origin of 'ship-op' proxies
} proxy_request;


//==========================================================
// Forward Declarations.
//

void* run_proxy_retransmit(void* arg);
int proxy_retransmit_reduce_fn(const void* key, void* data, void* udata);
int proxy_retransmit_send(proxy_request* pr);

int proxy_msg_cb(cf_node src, msg* m, void* udata);

void proxyer_handle_response(msg* m, uint32_t tid);
int proxyer_handle_client_response(msg* m, proxy_request* pr);
int proxyer_handle_batch_response(msg* m, proxy_request* pr);
void proxyer_handle_return_to_sender(msg* m, uint32_t tid);

void proxyee_handle_request(cf_node src, msg* m, uint32_t tid);

void shipop_response_handler(msg* m, proxy_request* pr);
void shipop_handle_client_response(msg* m, rw_request* rw);
void shipop_timeout_handler(proxy_request* pr);

static inline uint32_t
proxy_hash_fn(const void* value)
{
	return *(const uint32_t*)value;
}

static inline void
error_response(cf_node src, uint32_t tid, uint32_t error)
{
	as_proxy_send_response(src, tid, error, 0, 0, NULL, NULL, 0, NULL, 0, NULL);
}

static inline void
client_proxy_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_client_proxy_complete);
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_proxy_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_proxy_error);
		break;
	}
}

static inline void
batch_sub_proxy_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_batch_sub_proxy_complete);
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_batch_sub_proxy_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_batch_sub_proxy_error);
		break;
	}
}


//==========================================================
// Globals.
//

static shash* g_proxy_hash = NULL;
static cf_atomic32 g_proxy_tid = 0;


//==========================================================
// Public API.
//

void
as_proxy_init()
{
	shash_create(&g_proxy_hash, proxy_hash_fn, sizeof(uint32_t),
			sizeof(proxy_request), 4 * 1024, SHASH_CR_MT_MANYLOCK);

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	if (pthread_create(&thread, &attrs, run_proxy_retransmit, NULL) != 0) {
		cf_crash(AS_RW, "failed to create proxy retransmit thread");
	}

	as_fabric_register_msg_fn(M_TYPE_PROXY, proxy_mt, sizeof(proxy_mt),
			PROXY_MSG_SCRATCH_SIZE, proxy_msg_cb, NULL);
}


uint32_t
as_proxy_hash_count()
{
	return shash_get_size(g_proxy_hash);
}


// Proxyer - divert a transaction request to another node.
bool
as_proxy_divert(cf_node dst, as_transaction* tr, as_namespace* ns,
		uint64_t cluster_key)
{
	uint32_t pid = as_partition_getid(&tr->keyd);

	// Get a fabric message and fill it out.

	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		cf_warning(AS_PROXY, "failed to get fabric msg");
		return false;
	}

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_type set_type = tr->origin == FROM_BATCH ?
			MSG_SET_COPY : MSG_SET_HANDOFF_MALLOC;

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void*)tr->msgp,
			as_proto_size_get(&tr->msgp->proto), set_type);
	msg_set_uint64(m, PROXY_FIELD_CLUSTER_KEY, cluster_key);

	// Set up a proxy_request and insert it in the hash.

	proxy_request pr;

	pr.msg_fields = tr->msg_fields;

	pr.origin = tr->origin;
	pr.from_flags = tr->from_flags;
	pr.from.any = tr->from.any;
	pr.batch_index = tr->from_data.batch_index;

	pr.start_time = tr->start_time;
	pr.end_time = tr->end_time;

	pr.fab_msg = m;
	pr.xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	pr.retry_interval_ms = g_config.transaction_retry_ms;

	pr.dest = dst;
	pr.pid = pid;
	pr.ns = ns;

	pr.rw = NULL;

	if (shash_put(g_proxy_hash, &tid, &pr) != SHASH_OK) {
		cf_warning(AS_PROXY, "failed shash put");
		as_fabric_msg_put(m);
		return false;
	}

	tr->msgp = NULL; // pattern, not needed
	tr->from.any = NULL; // pattern, not needed

	// Send fabric message to remote node.

	msg_incr_ref(m);

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}

	return true;
}


// Proxyee - transaction reservation failed here, tell proxyer to try again.
void
as_proxy_return_to_sender(const as_transaction* tr, as_namespace* ns)
{
	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	uint32_t pid = as_partition_getid(&tr->keyd);
	cf_node redirect_node = as_partition_proxyee_redirect(ns, pid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RETURN_TO_SENDER);
	msg_set_uint32(m, PROXY_FIELD_TID, tr->from_data.proxy_tid);
	msg_set_uint64(m, PROXY_FIELD_REDIRECT,
			redirect_node == (cf_node)0 ? tr->from.proxy_node : redirect_node);

	if (as_fabric_send(tr->from.proxy_node, m, AS_FABRIC_CHANNEL_RW) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


// Proxyee - transaction completed here, send response to proxyer.
void
as_proxy_send_response(cf_node dst, uint32_t proxy_tid, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op** ops, as_bin** bins,
		uint16_t bin_count, as_namespace* ns, uint64_t trid,
		const char* set_name)
{
	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, proxy_tid);

	size_t msg_sz = 0;
	cl_msg* msgp = as_msg_make_response_msg(result_code, generation, void_time,
			ops, bins, bin_count, ns, 0, &msg_sz, trid, set_name);

	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (uint8_t*)msgp, msg_sz,
			MSG_SET_HANDOFF_MALLOC);

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


// Proxyee - transaction completed here, send response to proxyer.
void
as_proxy_send_ops_response(cf_node dst, uint32_t proxy_tid, cf_dyn_buf* db)
{
	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, proxy_tid);

	uint8_t* msgp = db->buf;
	size_t msg_sz = db->used_sz;

	if (db->is_stack) {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_COPY);
	}
	else {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz,
				MSG_SET_HANDOFF_MALLOC);
		db->buf = NULL; // the fabric owns the buffer now
	}

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


// LDT-related.
void
as_proxy_shipop(cf_node dst, rw_request* rw)
{
	uint32_t pid = as_partition_getid(&rw->keyd);

	// Get a fabric message and fill it out.

	msg* m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void*)&rw->keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void*)rw->msgp,
			as_proto_size_get(&rw->msgp->proto), MSG_SET_HANDOFF_MALLOC);
	msg_set_uint64(m, PROXY_FIELD_CLUSTER_KEY, as_exchange_cluster_key());
	msg_set_uint32(m, PROXY_FIELD_INFO, PROXY_INFO_SHIPPED_OP);

	rw->msgp = NULL;

	// Set up a proxy_request and insert it in the hash.

	proxy_request pr;

	pr.msg_fields = rw->msg_fields;

	pr.origin = 0;
	pr.from_flags = 0;
	pr.from.any = NULL;
	pr.batch_index = 0;

	pr.start_time = rw->start_time;
	pr.end_time = rw->end_time;

	pr.fab_msg = m;
	pr.xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	pr.retry_interval_ms = g_config.transaction_retry_ms;

	pr.dest = dst;
	pr.pid = pid;
	pr.ns = NULL; // needed only for retry, which ship-op doesn't do

	cf_rc_reserve(rw);
	pr.rw = rw;

	if (shash_put(g_proxy_hash, &tid, &pr) != SHASH_OK) {
		as_fabric_msg_put(m);
		return;
	}

	// Send fabric message to remote node.

	msg_incr_ref(m);

	if (as_fabric_send(dst, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


//==========================================================
// Local helpers - proxyer.
//

void
proxyer_handle_response(msg* m, uint32_t tid)
{
	proxy_request pr;

	if (shash_get_and_delete(g_proxy_hash, &tid, &pr) != SHASH_OK) {
		// Some other response (or timeout) has already finished this pr.
		return;
	}

	if (pr.rw) {
		shipop_response_handler(m, &pr);
		return;
	}

	cf_assert(pr.from.any, AS_PROXY, "origin %u has null 'from'", pr.origin);

	int result;

	switch (pr.origin) {
	case FROM_CLIENT:
		result = proxyer_handle_client_response(m, &pr);
		client_proxy_update_stats(pr.ns, result);
		break;
	case FROM_BATCH:
		result = proxyer_handle_batch_response(m, &pr);
		batch_sub_proxy_update_stats(pr.ns, result);
		// Note - no worries about msgp, proxy divert copied it.
		break;
	case FROM_PROXY:
	case FROM_IUDF:
	case FROM_NSUP:
		// Should be impossible for proxyee, internal UDFs, and nsup deletes to
		// get here.
	default:
		cf_crash(AS_PROXY, "unexpected transaction origin %u", pr.origin);
		break;
	}

	pr.from.any = NULL; // pattern, not needed

	as_fabric_msg_put(pr.fab_msg);

	// Note that this includes both origins.
	if (pr.ns->proxy_hist_enabled) {
		histogram_insert_data_point(pr.ns->proxy_hist, pr.start_time);
	}
}


int
proxyer_handle_client_response(msg* m, proxy_request* pr)
{
	uint8_t* proto;
	size_t proto_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, &proto, &proto_sz,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	as_file_handle* fd_h = pr->from.proto_fd_h;

	if (cf_socket_send_all(&fd_h->sock, proto, proto_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		as_end_of_transaction_force_close(fd_h);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	as_end_of_transaction_ok(fd_h);
	return AS_PROTO_RESULT_OK;
}


int
proxyer_handle_batch_response(msg* m, proxy_request* pr)
{
	cl_msg* msgp;
	size_t msgp_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp, &msgp_sz,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	cf_digest* keyd;

	if (msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_crash(AS_PROXY, "original msg get for digest failed");
	}

	as_batch_add_proxy_result(pr->from.batch_shared, pr->batch_index, keyd,
			msgp, msgp_sz);

	return AS_PROTO_RESULT_OK;
}


void
proxyer_handle_return_to_sender(msg* m, uint32_t tid)
{
	proxy_request* pr;
	pthread_mutex_t* lock;

	if (shash_get_vlock(g_proxy_hash, &tid, (void**)&pr, &lock) != SHASH_OK) {
		// Some other response (or timeout) has already finished this pr.
		return;
	}

	// 0 origin has been observed here during upgrade from 3.7.4 to 3.13.0.
	// Adding defensive code until we understand how this could happen.
	if (pr->rw || pr->origin == 0) {
		cf_warning(AS_PROXY, "unexpected return to sender for ship-op tid %u rw %p origin %d from-flags %x",
				tid, pr->rw, pr->origin, pr->from_flags);
		pthread_mutex_unlock(lock);
		return;
	}

	cf_node redirect_node;

	if (msg_get_uint64(m, PROXY_FIELD_REDIRECT, &redirect_node) == 0
			&& redirect_node != g_config.self_node
			&& redirect_node != (cf_node)0) {
		// If this node was a "random" node, i.e. neither acting nor eventual
		// master, it diverts to the eventual master (the best it can do.) The
		// eventual master must inform this node about the acting master.
		pr->dest = redirect_node;
		pr->xmit_ms = cf_getms() + g_config.transaction_retry_ms;

		msg_incr_ref(pr->fab_msg);

		if (as_fabric_send(pr->dest, pr->fab_msg, AS_FABRIC_CHANNEL_RW) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(pr->fab_msg);
		}

		pthread_mutex_unlock(lock);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_crash(AS_PROXY, "original msg get for digest failed");
	}

	cl_msg* msgp;

	// TODO - inefficient! Should be a way to 'take' a buffer from msg.
	if (msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp, NULL,
			MSG_GET_COPY_MALLOC) != 0) {
		cf_crash(AS_PROXY, "original msg get for proto failed");
	}

	// Put the as_msg on the normal queue for processing.
	as_transaction tr;
	as_transaction_init_head(&tr, keyd, msgp);
	// msgp might not have digest - batch sub-transactions, old clients.
	// For old clients, will compute it again from msgp key and set.

	tr.msg_fields = pr->msg_fields;
	tr.origin = pr->origin;
	tr.from_flags = pr->from_flags;
	tr.from.any = pr->from.any;
	tr.from_data.batch_index = pr->batch_index;
	tr.start_time = pr->start_time;

	as_tsvc_enqueue(&tr);

	as_fabric_msg_put(pr->fab_msg);

	shash_delete_lockfree(g_proxy_hash, &tid);
	pthread_mutex_unlock(lock);
}


//==========================================================
// Local helpers - proxyee.
//

void
proxyee_handle_request(cf_node src, msg* m, uint32_t tid)
{
	cf_digest* keyd;

	if (msg_get_buf(m, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for digest failed");
		error_response(src, tid, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	cl_msg* msgp;
	size_t msgp_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp, &msgp_sz,
			MSG_GET_COPY_MALLOC) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		error_response(src, tid, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	// Sanity check as_proto fields.
	as_proto* proto = &msgp->proto;

	if (! as_proto_wrapped_is_valid(proto, msgp_sz)) {
		cf_warning(AS_PROXY, "bad proto: version %u, type %u, sz %lu [%lu]",
				proto->version, proto->type, (uint64_t)proto->sz, msgp_sz);
		error_response(src, tid, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	uint32_t info = 0;

	msg_get_uint32(m, PROXY_FIELD_INFO, &info);

	if ((info & PROXY_INFO_SHIPPED_OP) != 0) {
		uint64_t cluster_key;

		if (msg_get_uint64(m, PROXY_FIELD_CLUSTER_KEY, &cluster_key) == 0 &&
				cluster_key != as_exchange_cluster_key()) {
			error_response(src, tid, AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH);
			return;
		}
	}

	// Put the as_msg on the normal queue for processing.
	as_transaction tr;
	as_transaction_init_head(&tr, keyd, msgp);
	// msgp might not have digest - batch sub-transactions, old clients.
	// For old clients, will compute it again from msgp key and set.

	tr.start_time = cf_getns();

	tr.origin = FROM_PROXY;
	tr.from.proxy_node = src;
	tr.from_data.proxy_tid = tid;
	as_transaction_proxyee_prepare(&tr);

	// For batch sub-transactions, make sure we flag them so they're not
	// mistaken for multi-record transactions (which never proxy).
	if (as_transaction_has_no_key_or_digest(&tr)) {
		tr.from_flags |= FROM_FLAG_BATCH_SUB;
	}

	// Flag shipped ops.
	if ((info & PROXY_INFO_SHIPPED_OP) != 0) {
		tr.from_flags |= FROM_FLAG_SHIPPED_OP;
	}

	as_tsvc_enqueue(&tr);
}


//==========================================================
// Local helpers - retransmit.
//

void*
run_proxy_retransmit(void* arg)
{
	while (true) {
		usleep(75 * 1000);

		now_times now;

		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		shash_reduce_delete(g_proxy_hash, proxy_retransmit_reduce_fn, &now);
	}

	return NULL;
}


int
proxy_retransmit_reduce_fn(const void* key, void* data, void* udata)
{
	proxy_request* pr = data;
	now_times* now = (now_times*)udata;

	// Handle timeouts.

	if (now->now_ns > pr->end_time) {
		if (pr->rw) {
			shipop_timeout_handler(pr);
			as_fabric_msg_put(pr->fab_msg);
			return SHASH_REDUCE_DELETE;
		}

		cf_assert(pr->from.any, AS_PROXY,
				"origin %u has null 'from'", pr->origin);

		switch (pr->origin) {
		case FROM_CLIENT:
			// TODO - when it becomes important enough, find a way to echo trid.
			as_msg_send_reply(pr->from.proto_fd_h, AS_PROTO_RESULT_FAIL_TIMEOUT,
					0, 0, NULL, NULL, 0, NULL, 0, NULL);
			client_proxy_update_stats(pr->ns, AS_PROTO_RESULT_FAIL_TIMEOUT);
			break;
		case FROM_BATCH:
			as_batch_add_error(pr->from.batch_shared, pr->batch_index,
					AS_PROTO_RESULT_FAIL_TIMEOUT);
			// Note - no worries about msgp, proxy divert copied it.
			batch_sub_proxy_update_stats(pr->ns, AS_PROTO_RESULT_FAIL_TIMEOUT);
			break;
		case FROM_PROXY:
		case FROM_IUDF:
		case FROM_NSUP:
			// Proxyees, internal UDFs, and nsup deletes don't proxy.
		default:
			cf_crash(AS_PROXY, "unexpected transaction origin %u", pr->origin);
			break;
		}

		pr->from.any = NULL; // pattern, not needed
		as_fabric_msg_put(pr->fab_msg);

		return SHASH_REDUCE_DELETE;
	}

	// Handle retransmits. (Ship-ops are exempt.)

	if (pr->xmit_ms < now->now_ms && ! pr->rw) {
		// Update the retry interval, exponentially.
		pr->xmit_ms = now->now_ms + pr->retry_interval_ms;
		pr->retry_interval_ms *= 2;

		return proxy_retransmit_send(pr);
	}

	return SHASH_OK;
}


int
proxy_retransmit_send(proxy_request* pr)
{
	while (true) {
		g_stats.proxy_retry++;

		msg_incr_ref(pr->fab_msg);

		int rv = as_fabric_send(pr->dest, pr->fab_msg, AS_FABRIC_CHANNEL_RW);

		if (rv == AS_FABRIC_SUCCESS) {
			return SHASH_OK;
		}

		as_fabric_msg_put(pr->fab_msg);

		if (rv != AS_FABRIC_ERR_NO_NODE) {
			// Should never get here - 'queue full' error is impossible for
			// medium priority...
			return SHASH_ERR;
		}

		// The node I'm proxying to is no longer up. Find another node. (Easier
		// to just send to the master and not pay attention to whether it's read
		// or write.)
		cf_node new_dst = as_partition_writable_node(pr->ns, pr->pid);

		// Partition is frozen - try more retransmits, but will likely time out.
		if (new_dst == (cf_node)0) {
			return SHASH_OK;
		}

		// Destination is self - abandon proxy and try local transaction.
		if (new_dst == g_config.self_node) {
			cf_digest* keyd;

			msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
					MSG_GET_DIRECT);

			cl_msg* msgp;

			msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (uint8_t**)&msgp,
					NULL, MSG_GET_COPY_MALLOC);

			as_transaction tr;
			as_transaction_init_head(&tr, keyd, msgp);
			// msgp might not have digest - batch sub-transactions, old clients.
			// For old clients, will compute it again from msgp key and set.

			tr.msg_fields = pr->msg_fields;
			tr.origin = pr->origin;
			tr.from_flags = pr->from_flags;
			tr.from.any = pr->from.any;
			tr.from_data.batch_index = pr->batch_index;
			tr.start_time = pr->start_time;

			as_tsvc_enqueue(&tr);

			as_fabric_msg_put(pr->fab_msg);

			return SHASH_REDUCE_DELETE;
		}

		// Original destination - just wait for the next retransmit.
		if (new_dst == pr->dest) {
			return SHASH_OK;
		}

		// Different destination - retry immediately. This is the reason for the
		// while loop. Is this too complicated to bother with?
		pr->dest = new_dst;
	}

	// For now, it's impossible to get here.
	return SHASH_ERR;
}


//==========================================================
// Local helpers - handle PROXY fabric messages.
//

int
proxy_msg_cb(cf_node src, msg* m, void* udata)
{
	uint32_t op;

	if (msg_get_uint32(m, PROXY_FIELD_OP, &op) != 0) {
		cf_warning(AS_PROXY, "msg get for op failed");
		as_fabric_msg_put(m);
		return 0;
	}

	uint32_t tid;

	if (msg_get_uint32(m, PROXY_FIELD_TID, &tid) != 0) {
		cf_warning(AS_PROXY, "msg get for tid failed");
		as_fabric_msg_put(m);
		return 0;
	}

	switch (op) {
	case PROXY_OP_REQUEST:
		proxyee_handle_request(src, m, tid);
		break;
	case PROXY_OP_RESPONSE:
		proxyer_handle_response(m, tid);
		break;
	case PROXY_OP_RETURN_TO_SENDER:
		proxyer_handle_return_to_sender(m, tid);
		break;
	default:
		cf_warning(AS_PROXY, "received unexpected message op %u", op);
		break;
	}

	as_fabric_msg_put(m);
	return 0;
}


//==========================================================
// Local helpers - LDT-related.
//

void
shipop_response_handler(msg* m, proxy_request* pr)
{
	cf_assert(pr->origin == 0, AS_PROXY, "ship-op pr origin not 0");

	rw_request* rw = pr->rw;

	pthread_mutex_lock(&rw->lock);

	if (! rw->from.any) {
		// Lost race against timeout.
		pthread_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		return;
	}

	// This node is the resolving node (node @ which duplicate resolution was
	// triggered) - respond to various possible origins.

	switch (rw->origin) {
	case FROM_CLIENT:
		shipop_handle_client_response(m, rw);
		break;
	case FROM_PROXY:
		msg_preserve_fields(m, 2, PROXY_FIELD_OP, PROXY_FIELD_AS_PROTO);
		// Fake the ORIGINATING proxy tid.
		msg_set_uint32(m, PROXY_FIELD_TID, rw->from_data.proxy_tid);
		msg_incr_ref(m);
		if (as_fabric_send(rw->from.proxy_node, m, AS_FABRIC_CHANNEL_RW) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(pr->fab_msg);
		}
		break;
	case FROM_IUDF:
		rw->from.iudf_orig->cb(rw->from.iudf_orig->udata, 0);
		break;
	case FROM_BATCH:
	case FROM_NSUP:
		// Should be impossible for batch reads and nsup deletes to get here.
	default:
		cf_crash(AS_PROXY, "unexpected transaction origin %u", rw->origin);
		break;
	}

	// Signal to destructor and timeout that origin response is handled.
	rw->from.any = NULL;

	pthread_mutex_unlock(&rw->lock);

	// This node is the ship-op initiator - remove rw_request from hash.

	rw_request_hkey hkey = { rw->rsv.ns->id, rw->keyd };

	rw_request_hash_delete(&hkey, rw);
	rw_request_release(rw);
	pr->rw = NULL;
}


void
shipop_handle_client_response(msg* m, rw_request* rw)
{
	uint8_t* proto;
	size_t proto_sz;

	if (msg_get_buf(m, PROXY_FIELD_AS_PROTO, &proto, &proto_sz,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_PROXY, "msg get for proto failed");
		return;
	}

	as_file_handle* fd_h = rw->from.proto_fd_h;

	if (cf_socket_send_all(&fd_h->sock, proto, proto_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		as_end_of_transaction_force_close(fd_h);
		return;
	}

	as_end_of_transaction_ok(fd_h);
}


void
shipop_timeout_handler(proxy_request* pr)
{
	rw_request* rw = pr->rw;

	pthread_mutex_lock(&rw->lock);

	// Invoke the "original" timeout handler - it knows which stats to affect.
	rw->timeout_cb(rw);

	pthread_mutex_unlock(&rw->lock);

	rw_request_hkey hkey = { rw->rsv.ns->id, rw->keyd };

	rw_request_hash_delete(&hkey, rw);
	rw_request_release(rw);
	pr->rw = NULL;
}
