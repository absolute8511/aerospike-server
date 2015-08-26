/*
 * thr_proxy.c
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
 * Basic proxy system, which is called when there's an operation that can't be
 * done on this node, and is then forwarded to another node for processing.
 */


#include "base/thr_proxy.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "msg.h"
#include "util.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
#include "base/write_request.h"
#include "fabric/fabric.h"
#include "fabric/paxos.h"


// Turn these OFF for production.
//#define DEBUG 1
//#define DEBUG_VERBOSE 1
//#define EXTRA_CHECKS 1

// Template for migrate messages.
#define PROXY_FIELD_OP 0
#define PROXY_FIELD_TID 1
#define PROXY_FIELD_DIGEST 2
#define PROXY_FIELD_REDIRECT 3
#define PROXY_FIELD_AS_PROTO 4 // request as_proto - currently contains only as_msg's
#define PROXY_FIELD_CLUSTER_KEY 5
#define PROXY_FIELD_TIMEOUT_MS 6
#define PROXY_FIELD_INFO 7

#define PROXY_INFO_SHIPPED_OP 0x0001

#define PROXY_OP_REQUEST 1
#define PROXY_OP_RESPONSE 2
#define PROXY_OP_REDIRECT 3

msg_template proxy_mt[] = {
	{ PROXY_FIELD_OP, M_FT_UINT32 },
	{ PROXY_FIELD_TID, M_FT_UINT32 },
	{ PROXY_FIELD_DIGEST, M_FT_BUF },
	{ PROXY_FIELD_REDIRECT, M_FT_UINT64 },
	{ PROXY_FIELD_AS_PROTO, M_FT_BUF },
	{ PROXY_FIELD_CLUSTER_KEY, M_FT_UINT64 },
	{ PROXY_FIELD_TIMEOUT_MS, M_FT_UINT32 },
	{ PROXY_FIELD_INFO, M_FT_UINT32 },
};

typedef struct {

	as_file_handle	*fd_h;       // holds the response fd we're going to have to send back to
	msg		        *fab_msg;    // this is the fabric message in case we have to retransmit
	cf_clock         xmit_ms;    // the ms time of the NEXT retransmit
	uint32_t         retry_interval_ms; // number of ms to wait next time
	uint64_t         start_time;
	uint64_t         end_time;

	cf_node          dest; // the node we're sending to
	// These are very helpful in the error case of needing to re-redirect.
	// Arguably, it's so rare that it's better to recalculate these.
	as_partition_id  pid;
	as_namespace    *ns;
	// Write request on resolving node.
	write_request   *wr;
	// Common batch data.
	struct as_batch_shared* batch_shared;
	uint32_t batch_index;
} proxy_request;


static cf_atomic32   g_proxy_tid = 0;

static shash *g_proxy_hash = 0;

static pthread_t g_proxy_retransmit_th;


uint32_t
proxy_id_hash(void *value)
{
	return *(uint32_t *)value;
}


// Sometimes it's good for other units (currently, thr_write) to be able to tell
// whether two proxy messages are really the "same". This function assumes
// you've already compared the nodes they came from.
int
as_proxy_msg_compare(msg *m1, msg *m2)
{
	uint32_t tid1;
	uint32_t tid2;
	msg_get_uint32(m1, PROXY_FIELD_TID, &tid1);
	msg_get_uint32(m2, PROXY_FIELD_TID, &tid2);
	return tid1 == tid2 ? 0 : -1;
}


void as_proxy_set_stat_counters(int rv) {
	if (rv == 0) {
		cf_atomic_int_incr(&g_config.stat_proxy_success);
	}
	else {
		cf_atomic_int_incr(&g_config.stat_proxy_errs);
	}
}


// Make a request to another node.
//
// Note: there's a cheat here. 'as_msg' is used in a raw form, and includes
// structured data (version - type - nfields - sz ...) which should be made more
// wire-protocol-friendly.
int
as_proxy_divert(cf_node dst, as_transaction *tr, as_namespace *ns, uint64_t cluster_key)
{
	cf_detail(AS_PROXY, "proxy divert");

	cf_atomic_int_incr(&g_config.stat_proxy_reqs);
	if (tr->msgp && (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR)) {
		cf_atomic_int_incr(&g_config.stat_proxy_reqs_xdr);
	}
	as_partition_id pid = as_partition_getid(tr->keyd);

	if (dst == 0) {
		// Get the list of replicas.
		dst = as_partition_getreplica_read(ns, pid);
	}

	// Create a fabric message, fill it out.
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);
	if (!m)	{
		return -1;
	}

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void *) &tr->keyd, sizeof(cf_digest), MSG_SET_COPY);
	msg_set_type msettype = tr->batch_shared ? MSG_SET_COPY : MSG_SET_HANDOFF_MALLOC;
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void *) tr->msgp, as_proto_size_get(&tr->msgp->proto), msettype);
	msg_set_uint64(m, PROXY_FIELD_CLUSTER_KEY, cluster_key);
	msg_set_uint32(m, PROXY_FIELD_TIMEOUT_MS, tr->msgp->msg.transaction_ttl);

	tr->msgp = 0;

	cf_debug_digest(AS_PROXY, &tr->keyd, "proxy_divert: fab_msg %p dst %"PRIx64, m, dst);

	// Fill out a retransmit structure, insert into the retransmit hash.
	msg_incr_ref(m);
	proxy_request pr;
	pr.start_time = tr->start_time;
	pr.end_time = (tr->end_time != 0) ? tr->end_time : pr.start_time + g_config.transaction_max_ns;
	pr.fd_h = tr->proto_fd_h;
	tr->proto_fd_h = 0;
	pr.fab_msg = m;
	pr.xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	pr.retry_interval_ms = g_config.transaction_retry_ms;
	pr.dest = dst;
	pr.pid = pid;
	pr.ns = ns;
	pr.wr = NULL;
	pr.batch_shared = tr->batch_shared;
	pr.batch_index = tr->batch_index;

	if (0 != shash_put(g_proxy_hash, &tid, &pr)) {
		cf_debug(AS_PROXY, " shash_put failed, need cleanup code");
		return -1;
	}

	// Send to the remote node.
	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_debug(AS_PROXY, "as_proxy_divert: returned error %d", rv);
		as_fabric_msg_put(m);
	}

	cf_atomic_int_incr(&g_config.proxy_initiate);

	return 0;
}


int
as_proxy_shipop(cf_node dst, write_request *wr)
{
	as_partition_id pid = as_partition_getid(wr->keyd);

	if (dst == 0) {
		cf_crash(AS_PROXY, "the destination should never be zero");
	}

	// Create a fabric message, fill it out.
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);
	if (!m)	{
		return -1;
	}

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void *) &wr->keyd, sizeof(cf_digest), MSG_SET_COPY);
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void *) wr->msgp, as_proto_size_get(&wr->msgp->proto), MSG_SET_HANDOFF_MALLOC);
	msg_set_uint64(m, PROXY_FIELD_CLUSTER_KEY, as_paxos_get_cluster_key());
	msg_set_uint32(m, PROXY_FIELD_TIMEOUT_MS, wr->msgp->msg.transaction_ttl);
	wr->msgp = 0;

	// If it is shipped op.
	uint32_t info = 0;
	info |= PROXY_INFO_SHIPPED_OP;
	msg_set_uint32(m, PROXY_FIELD_INFO, info);

	cf_detail_digest(AS_PROXY, &wr->keyd, "SHIPPED_OP %s->WINNER msg %p Proxy Sent to %"PRIx64" %p tid(%d)",
			wr->proxy_msg ? "NONORIG" : "ORIG", m, dst, wr, tid);

	// Fill out a retransmit structure, insert into the retransmit hash.
	msg_incr_ref(m);
	proxy_request pr;
	pr.start_time  = wr->start_time;
	pr.end_time    = (wr->end_time != 0) ? wr->end_time : pr.start_time + g_config.transaction_max_ns;
	cf_rc_reserve(wr);
	pr.wr          = wr;
	pr.fab_msg     = m;
	pr.xmit_ms     = cf_getms() + g_config.transaction_retry_ms;
	pr.retry_interval_ms = g_config.transaction_retry_ms;
	pr.dest        = dst;
	pr.pid         = pid;
	pr.fd_h        = NULL;
	pr.batch_shared = NULL;
	pr.batch_index = 0;

	if (0 != shash_put(g_proxy_hash, &tid, &pr)) {
		cf_info(AS_PROXY, " shash_put failed, need cleanup code");
		return -1;
	}

	// Send to the remote node.
	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_detail(AS_PROXY, "SHIPPED_OP ORIG [Digest %"PRIx64"] Failed with %d", *(uint64_t *)&wr->keyd, rv);
		as_fabric_msg_put(m);
	}

	wr->shipped_op_initiator = true;
	cf_atomic_int_incr(&g_config.ldt_proxy_initiate);

	return 0;
}


/*
 * The work horse function to process the acknowledgment for the duplicate op.
 * It is received after the intended node has finished performing the op. In
 * case of success the op would have been successfully performed and replicated.
 * In case of failure the op would not have been performed anywhere.
 *
 * The retransmit is handled by making sure op hangs from the write hash as long
 * as it is not applied or failed. Any attempt to perform next operation has to
 * hang behind it unless it is finished. Also operation is assigned a timestamp
 * so that there is some protection in case the op arrives out of order, or the
 * same op comes back again. That would be a duplicate op ...
 *
 * Received a op message - I'm a winner duplicate on this partition. Perform the
 * UDF op and replicate to all the nodes in the replica list. We only replicate
 * the subrecord if the partition is in subrecord migration phase. If not, ship
 * both subrecord and record. In case partition is read replica on this node, do
 * the write and signal back that I'm done.
 *
 * THUS - PROLE SIDE
 *
 * is_write is misnamed. Differentiates between the 'duplicate' phase and the
 *    'operation' phase. If is_write == false, we're in the 'duplicate' phase.
 *
 * Algorithm
 *
 * This code is called when op is shipped to the winner node.
 *
 * 1. Assert that current node is indeed the winner node.
 * 2. Assert the cluster key matches.
 * 3. Create a transaction and apply the UDF. Create an internal transaction and
 *    make sure it does some sort of reservation and applies the write and
 *    replicates to replica set. Once the write is done it sends the op ack.
 *
 *    TODO: How do you handle retransmits?
 *    TODO: How do you handle partition reservation? Is it something special.
 *    TODO: How to send along with replication request? Same infra should be
 *          used by normal replication as well.
 *
 *    There won't be any deadlock because the requests are triggered from the
 *    write. Get down to the udf apply code. Replicate to replica set and then
 *    make sure the response is sent back to the originating node. This node has
 *    to make sure the replication actually did succeed.
 *
 * In the response code you need to add the callback function.
 */
int
as_proxy_shipop_response_hdlr(msg *m, proxy_request *pr, bool *free_msg)
{
	int rv            = -1;
	write_request *wr = pr->wr;
	if (!wr) {
		return -1;
	}
	cf_assert((pr->fd_h == NULL), AS_PROXY, CF_WARNING, "fd_h set for shipop proxy response");

	// If there is a write request hanging from pr then this is a response to
	// the proxy ship op request. This node is the resolving node (node @ which
	// duplicate resolution was triggered). It could be:
	// 1. Originating node [where the request was sent from the client] - in
	//    that case send response back to the client directly.
	// 2. Non-originating node [where the request arrived as a regular proxy] -
	//    in that case send response back to the proxy originating node.

	// Case 1: Non-originating node.
	if (wr->proxy_msg) {
		// Remember that "digest" gets printed at the end of cf_detail_digest().
		// Fake the ORIGINATING Proxy tid
		uint32_t transaction_id = 0;
		msg_get_uint32(wr->proxy_msg, PROXY_FIELD_TID, &transaction_id);
		msg_set_uint32(m, PROXY_FIELD_TID, transaction_id);
		cf_detail_digest(AS_PROXY, &(wr->keyd), "SHIPPED_OP NON-ORIG :: Got Op Response(%p) :", wr);
		cf_detail_digest(AS_PROXY, &(wr->keyd), "SHIPPED_OP NON-ORIG :: Back Forwarding Response for tid (%d). : ", transaction_id);
		if (0 != (rv = as_fabric_send(wr->proxy_node, m, AS_FABRIC_PRIORITY_MEDIUM))) {
			cf_detail_digest(AS_PROXY, &wr->keyd, "SHIPPED_OP NONORIG Failed Forwarding Response");
			as_fabric_msg_put(m);
		}
		*free_msg = false;
	}
	// Case 2: Originating node.
	else {
		cf_detail_digest(AS_PROXY, &wr->keyd, "SHIPPED_OP ORIG Got Op Response");
		pthread_mutex_lock(&wr->lock);
		if (wr->proto_fd_h) {
			if (!wr->proto_fd_h->fd) {
				cf_warning_digest(AS_PROXY, &wr->keyd, "SHIPPED_OP ORIG Missing fd in proto_fd ");
			}
			else {
				as_proto *proto;
				size_t proto_sz;
				if (0 != msg_get_buf(m, PROXY_FIELD_AS_PROTO, (byte **) &proto, &proto_sz, MSG_GET_DIRECT)) {
					cf_info(AS_PROXY, "msg get buf failed!");
				}
				size_t pos = 0;
				while (pos < proto_sz) {
					rv = send(wr->proto_fd_h->fd, (((uint8_t *)proto) + pos), proto_sz - pos, MSG_NOSIGNAL);
					if (rv > 0) {
						pos += rv;
					}
					else if (rv < 0) {
						if (errno != EWOULDBLOCK) {
							// Common message when a client aborts.
							cf_debug(AS_PROTO, "protocol proxy write fail: fd %d "
									"sz %d pos %d rv %d errno %d",
									wr->proto_fd_h->fd, proto_sz, pos, rv, errno);
							shutdown(wr->proto_fd_h->fd, SHUT_RDWR);
							break;
						}
						usleep(1); // yield
					}
					else {
						cf_info(AS_PROTO, "protocol write fail zero return: fd %d sz %d pos %d ",
								wr->proto_fd_h->fd, proto_sz, pos);
						shutdown(wr->proto_fd_h->fd, SHUT_RDWR);
						break;
					}
				}
				cf_detail_digest(AS_PROXY, &wr->keyd, "SHIPPED_OP ORIG Response Sent to Client");
			}
			wr->proto_fd_h->t_inprogress = false;
			AS_RELEASE_FILE_HANDLE(wr->proto_fd_h);
			wr->proto_fd_h = 0;
		} else {
			// this may be NULL if the request has already timedout and the wr proto_fd_h
			// will be cleaned up by then
			cf_detail_digest(AS_PROXY, &wr->keyd, "SHIPPED_OP ORIG Missing proto_fd ");

			// Note: This may be needed if this is node where internal scan or query
			// UDF is initiated where it happens so that there is migration is going
			// on and the request get routed to the remote node which is winning node
			// This request may need the req_cb to be called.
			if (udf_rw_needcomplete_wr(wr)) {
				as_transaction tr;
				write_request_init_tr(&tr, wr);
				udf_rw_complete(&tr, 0, __FILE__, __LINE__);
				if (tr.proto_fd_h) {
					tr.proto_fd_h->t_inprogress = false;
					AS_RELEASE_FILE_HANDLE(tr.proto_fd_h);
					tr.proto_fd_h = 0;
				}
			}
		}
		pthread_mutex_unlock(&wr->lock);
	}

	// This node is shipOp initiator. Remove it from the Global
	// hash
	global_keyd gk;
	gk.ns_id = wr->rsv.ns->id;
	gk.keyd = wr->keyd;
	g_write_hash_delete(&gk);

	WR_RELEASE(pr->wr);
	pr->wr = NULL;
	return 0;
}


// Incoming messages start here.
// - Could get a request that we need to service.
// - Could get a response to one of our requests - need to find the request and
//   send the real response to the remote end.
int
proxy_msg_fn(cf_node id, msg *m, void *udata)
{
	int rv;

	if (cf_rc_count((void*)m) == 0) {
		cf_debug(AS_PROXY, " proxy_msg_fn was given a refcount 0 message! Someone has been naugty %p", m);
		return -1;
	}

	uint32_t op = 99999;
	msg_get_uint32(m, PROXY_FIELD_OP, &op);
	uint32_t transaction_id = 0;
	msg_get_uint32(m, PROXY_FIELD_TID, &transaction_id);

	cf_detail(AS_PROXY, "received proxy message: tid %d type %d from %"PRIx64, transaction_id, op, id);

	switch (op) {
		case PROXY_OP_REQUEST:
		{
			cf_atomic_int_incr(&g_config.proxy_action);

#ifdef DEBUG
			cf_debug(AS_PROXY, "Proxy_msg: received request");
#ifdef DEBUG_VERBOSE
			msg_dump(m, "incoming proxy msg");
#endif
#endif
			cf_digest *key;
			size_t sz = 0;
			if (0 != msg_get_buf(m, PROXY_FIELD_DIGEST, (byte **) &key, &sz, MSG_GET_DIRECT)) {
				cf_info(AS_PROXY, "proxy msg function: no digest, problem");
				as_fabric_msg_put(m);
				return 0;
			}
			cl_msg *msgp;
			size_t as_msg_sz = 0;
			if (0 != msg_get_buf(m, PROXY_FIELD_AS_PROTO, (byte **) &msgp, &as_msg_sz, MSG_GET_COPY_MALLOC)) {
				cf_info(AS_PROXY, "proxy msg function: no as msg, problem");
				as_fabric_msg_put(m);
				return 0;
			}

			uint64_t cluster_key = 0;
			if (0 != msg_get_uint64(m, PROXY_FIELD_CLUSTER_KEY, &cluster_key)) {
				cf_info(AS_PROXY, "proxy msg function: no cluster key, problem");
				as_fabric_msg_put(m);
				return 0;
			}

			// This is allowed to fail - this is a new field, and gets defaulted
			// to 0 if it doesn't exist.
			uint32_t timeout_ms = 0;
			msg_get_uint32(m, PROXY_FIELD_TIMEOUT_MS, &timeout_ms);
//			cf_info(AS_PROXY, "proxy msg: received timeout_ms of %d",timeout_ms);

			// Put the as_msg on the normal queue for processing.
			// INIT_TR
			as_transaction tr;
			as_transaction_init(&tr, key, msgp);
			tr.incoming_cluster_key = cluster_key;
			tr.end_time             = (timeout_ms != 0) ? ((uint64_t)timeout_ms * 1000000) + tr.start_time : 0;
			tr.proxy_node           = id;
			tr.proxy_msg            = m;

			// Check here if this is shipped op.
			uint32_t info = 0;
			msg_get_uint32(m, PROXY_FIELD_INFO, &info);
			if (info & PROXY_INFO_SHIPPED_OP) {
				tr.flag |= AS_TRANSACTION_FLAG_SHIPPED_OP;
				cf_detail_digest(AS_PROXY, &tr.keyd, "SHIPPED_OP WINNER Operation Received");
			} else {
				cf_detail_digest(AS_PROXY, &tr.keyd, "Received Proxy Request digest tid(%d)", tr.trid);
			}

			MICROBENCHMARK_RESET();

			if (0 != thr_tsvc_enqueue(&tr)) {
				cf_warning(AS_PROXY, "tsvc enqueue failed ~~ dropping incoming proxy request message!");
				as_fabric_msg_put(m);
			}
		}
		break;

		case PROXY_OP_RESPONSE:
		{
#ifdef DEBUG
			// Got the response from the actual endpoint.
			cf_debug(AS_PROXY, " proxy: received response! tid %d node %"PRIx64, transaction_id, id);
#ifdef DEBUG_VERBOSE
			msg_dump(m, "incoming proxy response");
#endif
#endif

			// Look up the element.
			proxy_request pr;
			bool free_msg = true;
			if (SHASH_OK == shash_get_and_delete(g_proxy_hash, &transaction_id, &pr)) {
				// Found the element (sometimes we get two acks so it's OK for
				// an ack to not find the transaction).

				if (pr.wr) {
					as_proxy_shipop_response_hdlr(m, &pr, &free_msg);
				} else {
					as_proto *proto;
					size_t proto_sz;
					if (0 != msg_get_buf(m, PROXY_FIELD_AS_PROTO, (byte **) &proto, &proto_sz, MSG_GET_DIRECT)) {
						cf_info(AS_PROXY, "msg get buf failed!");
					}

#ifdef DEBUG_VERBOSE
					cf_debug(AS_PROXY, "proxy: sending proto response: ptr %p sz %"PRIu64" %d", proto, proto_sz, pr.fd);
					for (size_t _i = 0; _i < proto_sz; _i++) {
						fprintf(stderr, " %x", ((byte *)proto)[_i]);
						if (_i % 16 == 15) {
							fprintf(stderr, "\n");
						}
					}
#endif

#ifdef EXTRA_CHECKS
					as_proto proto_copy = *proto;
					as_proto_swap(&proto_copy);
					if (proto_copy.sz + 8 != proto_sz) {
						cf_info(AS_PROXY, "BONE BONE BONE!!!");
						cf_info(AS_PROXY, "proto sz: %"PRIu64" sz %u", (uint64_t) proto_copy.sz, proto_sz);
					}
#endif

					// Write to the file descriptor.
					cf_detail(AS_PROXY, "direct write fd %d", pr.fd_h->fd);
					cf_assert(pr.fd_h->fd, AS_PROXY, CF_WARNING, "attempted write to fd 0");

					if (pr.batch_shared) {
						cf_digest* digest;
						size_t digest_sz = 0;

						if (msg_get_buf(pr.fab_msg, PROXY_FIELD_DIGEST, (byte **)&digest, &digest_sz, MSG_GET_DIRECT) == 0) {
							as_batch_add_proxy_result(pr.batch_shared, pr.batch_index, digest, (cl_msg*)proto, proto_sz);
							as_proxy_set_stat_counters(0);
						}
						else {
							cf_warning(AS_PROXY, "Failed to find batch proxy digest %u", transaction_id);
							as_batch_add_error(pr.batch_shared, pr.batch_index, AS_PROTO_RESULT_FAIL_UNKNOWN);
							as_proxy_set_stat_counters(-1);
						}
						cf_hist_track_insert_data_point(g_config.px_hist, pr.start_time);
					}
					else {
						size_t pos = 0;
						while (pos < proto_sz) {
							rv = send(pr.fd_h->fd, (((uint8_t *)proto) + pos), proto_sz - pos, MSG_NOSIGNAL);
							if (rv > 0) {
								pos += rv;
							}
							else if (rv < 0) {
								if (errno != EWOULDBLOCK) {
									// Common message when a client aborts.
									cf_debug(AS_PROTO, "protocol proxy write fail: fd %d sz %d pos %d rv %d errno %d", pr.fd_h->fd, proto_sz, pos, rv, errno);
									shutdown(pr.fd_h->fd, SHUT_RDWR);
									as_proxy_set_stat_counters(-1);
									goto SendFin;
								}
								usleep(1); // yield
							}
							else {
								cf_info(AS_PROTO, "protocol write fail zero return: fd %d sz %d pos %d ", pr.fd_h->fd, proto_sz, pos);
								shutdown(pr.fd_h->fd, SHUT_RDWR);
								as_proxy_set_stat_counters(-1);
								goto SendFin;
							}
						}
						as_proxy_set_stat_counters(0);
SendFin:
						cf_hist_track_insert_data_point(g_config.px_hist, pr.start_time);

						// Return the fabric message or the direct file descriptor -
						// after write and complete.
						pr.fd_h->t_inprogress = false;
						AS_RELEASE_FILE_HANDLE(pr.fd_h);
						pr.fd_h = 0;
					}
					as_fabric_msg_put(pr.fab_msg);
					pr.fab_msg = 0;
				}
			}
			else {
				cf_debug(AS_PROXY, "proxy: received result but no transaction, tid %d", transaction_id);
				as_proxy_set_stat_counters(-1);
			}

			if (free_msg) {
				as_fabric_msg_put(m);
			}
		}
		break;

		case PROXY_OP_REDIRECT:
		{
			// Sometimes the destination we proxied a request to isn't able to
			// satisfy it (for example, their copy of the partition in question
			// might be desync).
			cf_node new_dst = 0;
			msg_get_uint64(m, PROXY_FIELD_REDIRECT, &new_dst);
			cf_detail(AS_PROXY, "proxy redirect message: transaction %d to node %"PRIx64, transaction_id, new_dst);

			// Look in the proxy retransmit hash for the tid.
			proxy_request *pr;
			pthread_mutex_t *pr_lock;
			int r = 0;
			if (0 != (r = shash_get_vlock(g_proxy_hash, &transaction_id, (void **)&pr, &pr_lock))) {
				cf_debug(AS_PROXY, "redirect: could not find transaction %d", transaction_id);
				as_fabric_msg_put(m);
				return -1;
			}

			if (g_config.self_node == new_dst) {

				// Although we don't know we're the final destination, undo the
				// proxy-nature and put back on the main queue. Dangerous, as it
				// leaves open the possibility of a looping message.

				cf_digest *key;
				size_t sz = 0;
				if (0 != msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (byte **) &key, &sz, MSG_GET_DIRECT)) {
					cf_warning(AS_PROXY, "op_redirect: proxy msg function: no digest, problem");
					pthread_mutex_unlock(pr_lock);
					as_fabric_msg_put(m);
					return -1;
				}

				cl_msg *msgp;
				sz = 0;
				if (0 != msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (byte **) &msgp, &sz, MSG_GET_COPY_MALLOC)) {
					cf_warning(AS_PROXY, "op_redirect: proxy msg function: no as proto, problem");
					pthread_mutex_unlock(pr_lock);
					as_fabric_msg_put(m);
					return -1;
				}

				// Put the as_msg on the normal queue for processing.
				// INIT_TR
				as_transaction tr;
				as_transaction_init(&tr, key, msgp);
				tr.start_time = pr->start_time; // start time
				tr.end_time   = pr->end_time;
				tr.proto_fd_h = pr->fd_h;

				MICROBENCHMARK_RESET();

				if (0 != thr_tsvc_enqueue(&tr)) {
					cf_warning(AS_PROXY, "queue");
					cf_crash(AS_PROXY, "could not enqueue proxy redirect");
				}

				as_fabric_msg_put(pr->fab_msg);
				shash_delete_lockfree(g_proxy_hash, &transaction_id);
			}
			else {
				// Change the destination, update the retransmit time.
				pr->dest = new_dst;
				pr->xmit_ms = cf_getms() + 1;

				// Send it.
				msg_incr_ref(pr->fab_msg);
				if (0 != (rv = as_fabric_send(pr->dest, pr->fab_msg, AS_FABRIC_PRIORITY_MEDIUM))) {
					cf_debug(AS_PROXY, "redirect: change destination: %"PRIx64" send error %d", pr->dest, rv);
					as_fabric_msg_put(pr->fab_msg);
				}
			}

			pthread_mutex_unlock(pr_lock);
		}
		as_fabric_msg_put(m);
		break;
		default:
			cf_debug(AS_PROXY, "proxy_msg_fn: received unknown, unsupported message %d from remote endpoint", op);
			msg_dump(m, "proxy received unknown msg");
			as_fabric_msg_put(m);
			break;
	} // end switch

	return 0;
} // end proxy_msg_fn()


// Send a redirection message - consumes the message.
int
as_proxy_send_redirect(cf_node dst, msg *m, cf_node rdst)
{
	int rv;
	uint32_t tid;
	msg_get_uint32(m, PROXY_FIELD_TID, &tid);

	msg_reset(m);
	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REDIRECT);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_uint64(m, PROXY_FIELD_REDIRECT, rdst);

	if (0 != (rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM))) {
		cf_debug(AS_PROXY, "sending redirection failed: fabric send error %d", rv);
		as_fabric_msg_put(m);
	}

	return 0;
} // end as_proxy_send_redirect()


// Looked up the message in the store. Time to send the response value back to
// the requester. The CF_BYTEARRAY is handed off in this case. If you want to
// keep a reference, then keep the reference yourself.
int
as_proxy_send_response(cf_node dst, msg *m, uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op **ops, as_bin **bins, uint16_t bin_count,
		as_namespace *ns, uint64_t trid, const char *setname)
{
	uint32_t tid;
	msg_get_uint32(m, PROXY_FIELD_TID, &tid);

#ifdef DEBUG
	cf_debug(AS_PROXY, "proxy send response: message %p bytearray %p tid %d", m, result_code, tid);
#endif

	msg_reset(m);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);

	size_t msg_sz = 0;
	cl_msg * msgp = as_msg_make_response_msg(result_code, generation, void_time, ops,
			bins, bin_count, ns, 0, &msg_sz, trid, setname);

	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (byte *) msgp, msg_sz, MSG_SET_HANDOFF_MALLOC);

	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_debug(AS_PROXY, "sending proxy response: fabric send err %d, catch you on the retry", rv);
		as_fabric_msg_put(m);
	}

	return 0;
} // end as_proxy_send_response()

int
as_proxy_send_ops_response(cf_node dst, msg *m, cf_dyn_buf *db)
{
	uint32_t tid;
	msg_get_uint32(m, PROXY_FIELD_TID, &tid);

#ifdef DEBUG
	cf_debug(AS_PROXY, "proxy send response: message %p bytearray %p tid %d", m, result_code, tid);
#endif

	msg_reset(m);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);

	uint8_t *msgp = db->buf;
	size_t msg_sz = db->used_sz;

	if (db->is_stack) {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_COPY);
	}
	else {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_HANDOFF_MALLOC);
		db->buf = NULL; // the fabric owns the buffer now
	}

	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_debug(AS_PROXY, "sending proxy response: fabric send err %d, catch you on the retry", rv);
		as_fabric_msg_put(m);
	}

	return 0;
} // end as_proxy_send_ops_response()


//
// RETRANSMIT FUNCTIONS
//

typedef struct now_times_s {
	uint64_t now_ns;
	uint64_t now_ms;
} now_times;

// Reduce through the outstanding requests and retransmit sometime.
int
proxy_retransmit_reduce_fn(void *key, void *data, void *udata)
{
	proxy_request *pr = data;
	now_times *p_now = (now_times*)udata;

	if (pr->xmit_ms < p_now->now_ms) {

		cf_debug(AS_PROXY, "proxy_retransmit: now %"PRIu64" xmit_ms %"PRIu64" m %p", p_now->now_ms, pr->xmit_ms, pr->fab_msg);

		// Determine if the time is too much, and terminate if so.
		if (p_now->now_ns > pr->end_time) {

			// Can get very verbose, when another server is slow.
			cf_debug(AS_PROXY, "proxy_retransmit: too old request %d ms: terminating (dest %"PRIx64" {%s:%d}",
					(p_now->now_ns - pr->start_time) / 1000000, pr->dest, pr->ns->name, pr->pid);

			if (pr->wr) {
				cf_detail_digest(AS_PROXY, &pr->wr->keyd, "SHIPPED_OP Proxy Retransmit Timeout ...");
				cf_atomic_int_incr(&g_config.ldt_proxy_timeout);
				pthread_mutex_lock(&pr->wr->lock);
				// Note: This may be needed if this is node where internal scan or query
				// UDF is initiated where it happens so that there is migration is going
				// on and the request get routed to the remote node which is winning node
				// This request may need the req_cb to be called.
				if (udf_rw_needcomplete_wr(pr->wr)) {
					as_transaction tr;
					write_request_init_tr(&tr, pr->wr);
					udf_rw_complete(&tr, 0, __FILE__, __LINE__);
					if (tr.proto_fd_h) {
						AS_RELEASE_FILE_HANDLE(tr.proto_fd_h);
					}
				}
				pthread_mutex_unlock(&pr->wr->lock);
				WR_RELEASE(pr->wr);
				pr->wr            = NULL;
			}

			if (pr->fab_msg) {
				as_fabric_msg_put(pr->fab_msg);
				pr->fab_msg = 0;
			}
			if (pr->ns) {
				pr->ns = 0;
			}
			if (pr->fd_h) {
				if (pr->batch_shared) {
					as_batch_add_error(pr->batch_shared, pr->batch_index, AS_PROTO_RESULT_FAIL_TIMEOUT);
				}
				else {
					pr->fd_h->t_inprogress = false;
					shutdown(pr->fd_h->fd, SHUT_RDWR);
					AS_RELEASE_FILE_HANDLE(pr->fd_h);
					pr->fd_h = 0;
				}
			}

			return SHASH_REDUCE_DELETE;
		}

		// Update the retry interval, exponentially.
		pr->xmit_ms = p_now->now_ms + pr->retry_interval_ms;
		pr->retry_interval_ms *= 2;

		// msg_dump(pr->fab_msg, "proxy retransmit");
		msg_incr_ref(pr->fab_msg);
		int try = 0;

Retry:
		;
		if (try++ > 5) {
			cf_info(AS_PROXY, "retransmit loop detected: bailing");
			as_fabric_msg_put(pr->fab_msg);
			return 0;
		}

		if (pr->wr) {
			cf_detail_digest(AS_PROXY, &pr->wr->keyd, "SHIPPED_OP Proxy Retransmit... NOOP");
			as_fabric_msg_put(pr->fab_msg);
			return 0;
		}

		cf_atomic_int_incr(&g_config.proxy_retry);

		int rv = as_fabric_send(pr->dest, pr->fab_msg, AS_FABRIC_PRIORITY_MEDIUM);

		if (rv == 0) {
			return 0;
		}

		if (rv == AS_FABRIC_ERR_QUEUE_FULL) {
			cf_debug(AS_PROXY, "retransmit queue full");
			as_fabric_msg_put(pr->fab_msg);
			cf_atomic_int_incr(&g_config.proxy_retry_q_full);
			return -1;
		}
		else if (rv == -3) {

			// The node I'm proxying to is no longer up. Find another node.
			// (Easier to just send to the master and not pay attention to
			// whether it's read or write.)
			cf_node new_dst = as_partition_getreplica_write(pr->ns, pr->pid);
			cf_debug(AS_PROXY, "node failed with proxies in flight: trying alternative node %"PRIx64, new_dst);

			// Need to "unproxy" and divert back to main queue because
			// destination is self.
			if (new_dst == g_config.self_node) {

				cf_digest *keyp;
				size_t sz = 0;
				msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (byte **) &keyp, &sz, MSG_GET_DIRECT);

				// INIT_TR
				as_transaction tr;
				as_transaction_init(&tr, keyp, NULL);
				// TODO - why not pr.start_time?
				tr.end_time   = pr->end_time;
				tr.proto_fd_h = pr->fd_h;

				sz = 0;
				msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (byte **) & (tr.msgp), &sz, MSG_GET_COPY_MALLOC);

				cf_atomic_int_incr(&g_config.proxy_unproxy);

				MICROBENCHMARK_RESET();

				if (0 != thr_tsvc_enqueue(&tr)) {
					cf_warning(AS_PROXY, "queue");
					// Hope the next time around has more luck.
					return 0;
				}

				// Getting deleted - cleanup the proxy request.
				as_fabric_msg_put(pr->fab_msg);
				return SHASH_REDUCE_DELETE;

			}
			else if (new_dst == pr->dest) {
				// Just wait for the next retransmit.
				cf_atomic_int_incr(&g_config.proxy_retry_same_dest);
				as_fabric_msg_put(pr->fab_msg);
			}
			else {
				// Not self, not same, redo. Don't need to return fab-msg-ref
				// because I'm just going to send again.
				cf_atomic_int_incr(&g_config.proxy_retry_new_dest);
				pr->dest = new_dst;
				goto Retry;
			}


		}
		else {
			as_fabric_msg_put(pr->fab_msg);
			cf_info(AS_PROXY, "retransmit: send failed, unknown error: dst %"PRIx64" rv %d", pr->dest, rv);
			return -2;
		}
	}

	return 0;
} // end proxy_retransmit_reduce_fn()


void *
proxy_retransmit_fn(void *unused)
{
	while (1) {
		usleep(75 * 1000);

		cf_detail(AS_PROXY, "proxy retransmit: size %d", shash_get_size(g_proxy_hash));

		now_times now;
		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		shash_reduce_delete(g_proxy_hash, proxy_retransmit_reduce_fn, (void *) &now);
	}

	return NULL;
} // end proxy_retransmit_fn()


// This function is called when there's a paxos change and the partitions have
// updated. If the change is a delete, then go through the retransmit structure
// and move forward any transactions that are destined to the newly departed
// node.
int
proxy_node_delete_reduce_fn(void *key, void *data, void *udata)
{
	proxy_request *pr = data;
	cf_node *node = (cf_node *)udata;

	if (pr->dest == *node) {
		pr->xmit_ms = 0;

		uint32_t	*tid = (uint32_t *) data;
		cf_debug(AS_PROXY, "node fail: speed proxy transaction tid %d", *tid);
	}

	return 0;
}


typedef struct as_proxy_paxos_change_struct_t {
	cf_node succession[AS_CLUSTER_SZ];
	cf_node deletions[AS_CLUSTER_SZ];
} as_proxy_paxos_change_struct;

// Discover nodes in the hash table that are no longer in the succession list.
int
proxy_node_succession_reduce_fn(void *key, void *data, void *udata)
{
	as_proxy_paxos_change_struct *del = (as_proxy_paxos_change_struct *)udata;
	proxy_request *pr = data;

	// Check if this dest is in the succession list.
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (pr->dest == del->succession[i]) {
			return 0;
		}
	}

	// dest is not in succession list - mark it to be deleted.
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// If an empty slot exists, then it means key is not there yet.
		if (del->deletions[i] == (cf_node)0)
		{
			del->deletions[i] = pr->dest;
			return 0;
		}
		// If dest already exists, return.
		if (pr->dest == del->deletions[i]) {
			return 0;
		}
	}

	// Should not get here.
	return 0;
}


void
as_proxy_paxos_change(as_paxos_generation gen, as_paxos_change *change, cf_node succession[], void *udata)
{
	if ((NULL == change) || (1 > change->n_change)) {
		return;
	}

	as_proxy_paxos_change_struct del;
	memset(&del, 0, sizeof(as_proxy_paxos_change_struct));
	memcpy(del.succession, succession, sizeof(cf_node)*g_config.paxos_max_cluster_size);

	// Find out if the request is sync.
	if (change->type[0] == AS_PAXOS_CHANGE_SYNC) {
		// Iterate through the proxy hash table and find nodes that are not in
		// the succession list.
		shash_reduce(g_proxy_hash, proxy_node_succession_reduce_fn, (void *) &del);

		// If there are any nodes to be deleted, execute the deletion algorithm.
		for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
			if ((cf_node)0 != del.deletions[i]) {
				cf_info(AS_PROXY, "notified: REMOVE node %"PRIx64"", del.deletions[i]);
				shash_reduce(g_proxy_hash, proxy_node_delete_reduce_fn, (void *) &del.deletions[i]);
			}
		}

		return;
	}

	// This is the deprecated case where this code is called at the end of a
	// paxos transaction commit.

	for (int i = 0; i < change->n_change; i++) {
		if (change->type[i] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) {
			cf_info(AS_PROXY, "notified: REMOVE node %"PRIx64, change->id[i]);
			shash_reduce(g_proxy_hash, proxy_node_delete_reduce_fn, (void *) & (change->id[i]));
		}
	}
}


uint32_t
as_proxy_inprogress()
{
	return shash_get_size(g_proxy_hash);
}


void
as_proxy_init()
{
	shash_create(&g_proxy_hash, proxy_id_hash, sizeof(uint32_t), sizeof(proxy_request), 4 * 1024, SHASH_CR_MT_MANYLOCK);

	pthread_create(&g_proxy_retransmit_th, 0, proxy_retransmit_fn, 0);

	as_fabric_register_msg_fn(M_TYPE_PROXY, proxy_mt, sizeof(proxy_mt), proxy_msg_fn, NULL);

	as_paxos_register_change_callback(as_proxy_paxos_change, 0);
}
