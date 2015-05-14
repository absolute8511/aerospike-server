/*
 * thr_tsvc.c
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

#include "base/thr_tsvc.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"

#include "fault.h"
#include "queue.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/thr_batch.h"
#include "base/thr_info.h"
#include "base/thr_proxy.h"
#include "base/thr_scan.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "storage/storage.h"


// These must all be OFF in production.
// #define DEBUG 1
// #define DEBUG_VERBOSE 1
// #define DIGEST_VALIDATE 1

// Would you like to dump packets to the log?
// #define DUMP_SYNC_ERROR 1

// Would you like to break the server into the debugger so we can examine a
// failure?
// #define VERIFY_BREAK 1

// Forward declaration.
int thr_tsvc_enqueue_slow(as_transaction *tr);


static void
dump_msg(cl_msg *msgp)
{
	cf_info(AS_TSVC, "message dump: proto: version %d type %d size %"PRIu64,
			msgp->proto.version, msgp->proto.type, (uint64_t) msgp->proto.sz);
	uint64_t sz = msgp->proto.sz;
	uint8_t *d = msgp->proto.data;
	for (uint64_t i = 0; sz > 0; ) {
		if (sz >= 16) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]);
			i += 16;
			sz -= 16;
		}
		else if (sz == 15) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14]);
			i += 15;
			sz -= 15;
		}
		else if (sz == 14) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13]);
			i += 14;
			sz -= 14;
		}
		else if (sz == 13) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x : %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12]);
			i += 13;
			sz -= 13;
		}
		else if (sz == 12) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11]);
			i += 12;
			sz -= 12;
		}
		else if (sz == 11) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10]);
			i += 11;
			sz -= 11;
		}
		else if (sz == 10) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9]);
			i += 10;
			sz -= 10;
		}
		else if (sz == 9) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x : %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8]);
			i += 9;
			sz -= 9;
		}
		else if (sz == 8) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7]);
			i += 8;
			sz -= 8;
		}
		else if (sz == 7) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5], d[6]);
			i += 7;
			sz -= 7;
		}
		else if (sz == 6) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4], d[5]);
			i += 6;
			sz -= 6;
		}
		else if (sz == 5) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x : %02x",
					(int)i, d[0], d[1], d[2], d[3], d[4]);
			i += 5;
			sz -= 5;
		}
		else if (sz == 4) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x %02x",
					(int)i, d[0], d[1], d[2], d[3]);
			i += 4;
			sz -= 4;
		}
		else if (sz == 3) {
			cf_info(AS_TSVC, " %06u - %02x %02x %02x",
					(int)i, d[0], d[1], d[2]);
			i += 3;
			sz -= 3;
		}
		else if (sz == 2) {
			cf_info(AS_TSVC, " %06u - %02x %02x",
					(int)i, d[0], d[1]);
			i += 2;
			sz -= 2;
		}
		else if (sz == 1) {
			cf_info(AS_TSVC, " %06u - %02x",
					(int)i, d[0]);
			i += 1;
			sz -= 1;
		}

		if (sz >= 16) {
			d += 16;
		}
		else {
			d += sz;
		}
	}
}


// Sanity-check the message.
// Returns:
//   0: On success.
// 	-1: If caller should return.
// 	 1: If caller should jump to cleanup and not free msg.
// 	 2: If caller should jump to cleanup and free msg.
int
transaction_check_msg(as_transaction *tr)
{
	cl_msg *msgp = tr->msgp;

	if (msgp == 0) {
		cf_warning(AS_TSVC, " incoming transaction has no message, illegal protofd %p proxymsg %p", tr->proto_fd_h, tr->proxy_msg);
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
		return -1;
	}

#if 0
	// WARNING! This happens legally in one place, where thr_nsup is deleting
	// elements. If expiration/eviction is off, you should never see this!
	if ((tr->proto_fd == 0) && (tr->proxy_msg == 0)) {
		cf_warning(AS_TSVC , "bad message");
		return 2;
	}
#endif

#ifdef DEBUG_VERBOSE
	fprintf(stderr, "msgp DEBUG: thr_tsvc: version %d type %d nfields %d sz %"PRIu64"\n",
			msgp->proto.version, msgp->proto.type, msgp->msg.n_fields,  (uint64_t) msgp->proto.sz);
	for (uint64_t j = 0; j < msgp->proto.sz; j++) {
		fprintf(stderr, "%02x ", msgp->proto.data[j]);
		if (j % 16 == 15) fprintf(stderr, "\n");
	}
	fprintf(stderr, "\n");
#endif

	if (msgp->proto.version != PROTO_VERSION) {
		cf_info(AS_TSVC, "can't process message: wrong version %d expecting %d",
				msgp->proto.version, PROTO_VERSION);
#ifdef DUMP_SYNC_ERROR
		dump_msg(msgp);
#endif
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
		return 1;
	}

	if (msgp->proto.type >= PROTO_TYPE_MAX) {
		cf_info(AS_TSVC, "can't process message: invalid type %d should be %d or less",
				msgp->proto.type, PROTO_TYPE_MAX);
#ifdef DUMP_SYNC_ERROR
		dump_msg(msgp);
#endif
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
		return 1;
	}

	if (msgp->proto.sz > PROTO_SIZE_MAX) {
		cf_info(AS_TSVC, "can't process message: invalid size %"PRIu64" should be %d or less",
				msgp->proto.sz, PROTO_SIZE_MAX);
#ifdef DUMP_SYNC_ERROR
		dump_msg(msgp);
#endif
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
		return 1;
	}

	if ((msgp->proto.type != PROTO_TYPE_INFO) && (msgp->proto.type != PROTO_TYPE_AS_MSG)) {
		cf_info(AS_TSVC, "received unknown message type %d, ignoring", msgp->proto.type);
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
		return 2;
	}

	if (msgp->proto.sz > g_config.dump_message_above_size) {
		dump_msg(msgp);
	}
	return 0;
}


static inline bool
is_udf(cl_msg *msgp)
{
	return as_msg_field_get(&msgp->msg, AS_MSG_FIELD_TYPE_UDF_FILENAME) != NULL;
}


int
as_rw_process_result(int rv, as_transaction *tr, bool *free_msgp)
{
	if (-2 == rv) {
		cf_debug_digest(AS_TSVC, &(tr->keyd), "write re-attempt: ");
		as_partition_release(&tr->rsv);
		cf_atomic_int_decr(&g_config.rw_tree_count);
		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(error_hist);
		thr_tsvc_enqueue(tr);
		*free_msgp = false;
	} else if (-3 == rv) {
		cf_debug(AS_TSVC,
				"write in progress on key - delay and reattempt");
		as_partition_release(&tr->rsv);
		cf_atomic_int_decr(&g_config.rw_tree_count);
		MICROBENCHMARK_HIST_INSERT_P(error_hist);
		as_fabric_msg_put(tr->proxy_msg);
	} else {
		cf_debug(AS_TSVC,
				"write start failed: rv %d proto result %d", rv,
				tr->result_code);
		as_partition_release(&tr->rsv);
		cf_atomic_int_decr(&g_config.rw_tree_count);
		if (tr->result_code == 0) {
			cf_warning(AS_TSVC,
					"   warning: failure should have set protocol result code");
			tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		if (tr->proto_fd_h) {
			as_transaction_error(tr, tr->result_code);
		}
		else if (tr->proxy_msg) {
			if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
				cf_detail_digest(AS_RW, &(tr->keyd),
						"SHIPPED_OP :: Sending ship op reply, rc %d to (%"PRIx64") ::",
						tr->result_code, tr->proxy_node);
			}
			else {
				cf_detail(AS_RW,
						"sending proxy reply, rc %d to %"PRIx64"",
						tr->result_code, tr->proxy_node);
			}
			as_proxy_send_response(tr->proxy_node, tr->proxy_msg,
					tr->result_code, 0, 0, 0, 0, 0, tr->trid, NULL);
		}
		return -1;
	}
	return 0;
}


// Handle the transaction, including proxy to another node if necessary.
void
process_transaction(as_transaction *tr)
{
	cf_node dest;

	// There are two different types of cluster keys.  There is a "global"
	// cluster key (CK) for a node -- which shows the state of THIS node
	// compared to the paxos principle.  Then there is the partition CK that
	// shows the state of a partition compared to this node or incoming
	// messages. Note that in steady state, everything will match:
	// Paxos CK == Message CK == node CK == partition CK.  However, during
	// cluster transition periods, the different CKs show which states the
	// incoming messages, the nodes and the partitions are in. When they don't
	// match, that means some things are out of sync and special attention must
	// be given to what we're doing. In some cases we will quietly retry
	// internally, and in other cases we may return to the client with a
	// CLUSTER_KEY_MISMATCH error, which is a sign for them to retry.
	uint64_t partition_cluster_key = 0;
	uint64_t node_cluster_key = 0;

	int rv;
	bool free_msgp = true;
	as_namespace *ns = 0;

	MICROBENCHMARK_HIST_INSERT_AND_RESET_P(q_wait_hist);
	if (!tr || !tr->msgp) {
		return;
	}
	cl_msg *msgp = tr->msgp;

	int retval = transaction_check_msg(tr);
	if (retval == -1) {
		return;
	} else if (retval == 1) {
		free_msgp = false;
		goto Cleanup;
	} else if (retval == 2) {
		free_msgp = true;
		goto Cleanup;
	}

	if (msgp->proto.type == PROTO_TYPE_INFO) {

		// Info request - process it.
		if (0 == as_info(tr)) {
			free_msgp = false;
		}

		goto Cleanup;
	}

	if (! as_partition_balance_is_init_resolved() &&
			(tr->flag & AS_TRANSACTION_FLAG_NSUP_DELETE) == 0) {
		if (tr->preprocessed) {
			// It's very possible proxy transactions get here.
	        cf_debug(AS_TSVC, "rejecting fabric transaction - initial partition balance unresolved");
		}
		else {
	        cf_warning(AS_TSVC, "rejecting client transaction - initial partition balance unresolved");
		}
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_UNAVAILABLE);
		goto Cleanup;
	}

	// First, check that the socket is authenticated.
	if (tr->proto_fd_h) {
		uint8_t result = as_security_check(tr->proto_fd_h, PERM_NONE);

		if (result != AS_PROTO_RESULT_OK) {
			as_security_log(tr->proto_fd_h, result, PERM_NONE, NULL, NULL);
			as_transaction_error(tr, (uint32_t)result);
			goto Cleanup;
		}
	}

	// If this key digest hasn't been computed yet, prepare the transaction -
	// side effect, swaps the bytes (cases where transaction is requeued, the
	// transation may have already been preprocessed...) If the message hasn't
	// been swizzled yet, swizzle it,
	if (!tr->preprocessed) {
		if (0 != (rv = as_transaction_prepare(tr))) {
			// transaction_prepare() return values:
			// 0:  OK
			// -1: General error.
			// -2: Request received with no key (scan or query).
			// -3: Request received with digest array (batch).
			if (rv == -2) {
				// Has no key or digest, which means it's a either table scan or
				// a secondary index query. If query options (i.e where clause)
				// defined then go through as_query path otherwise default to
				// as_tscan.
				int rr = 0;
				if (as_msg_field_get(&msgp->msg,
						AS_MSG_FIELD_TYPE_INDEX_RANGE) != NULL) {
					cf_detail(AS_TSVC, "Received Query Request(%"PRIx64")", tr->trid);
					cf_atomic64_incr(&g_config.query_reqs);
					if (! as_security_check_data_op(tr, &msgp->msg, ns,
							is_udf(msgp) ? PERM_UDF_QUERY : PERM_QUERY)) {
						goto Cleanup;
					}
					// Responsibility of query layer to free the msgp.
					free_msgp = false;
					rr = as_query(tr);   // <><><> Q U E R Y <><><>
					if (rr != 0) {
						cf_atomic64_incr(&g_config.query_fail);
						cf_debug(AS_TSVC, "Query failed with error %d",
								tr->result_code);
						as_transaction_error(tr, tr->result_code);
					}
				} else {
					cf_debug(AS_TSVC, "Received Scan Request: TrID(%"PRIx64")", tr->trid);
					// We got a scan, it might be for udfs, no need to know now,
					// for now, do not free msgp for all the cases. Should take
					// care of it inside as_tscan.
					if (! as_security_check_data_op(tr, &msgp->msg, ns,
							is_udf(msgp) ? PERM_UDF_SCAN : PERM_SCAN)) {
						goto Cleanup;
					}
					free_msgp = false;
					rr = as_tscan(tr);   // <><><> S C A N <><><>
					// Process the scan return codes:
					// -1 :: bad parameters
					// -2 :: internal errors
					// -3 :: proper response, cluster in migration
					// -4 :: set name is valid but set doesn't exist
					// -5 :: unsupported feature (e.g. scans with UDF)
					if (rr != 0) {
						cf_info(AS_TSVC, "Scan failed with error %d", rr);
						as_transaction_error(tr,
								 rr == -1 ? AS_PROTO_RESULT_FAIL_NAMESPACE :
								(rr == -3 ? AS_PROTO_RESULT_FAIL_UNAVAILABLE :
								(rr == -4 ? AS_PROTO_RESULT_FAIL_NOTFOUND :
								(rr == -5 ? AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE :
											AS_PROTO_RESULT_FAIL_UNKNOWN))));
					}
				}
			} else if (rv == -3) {
				// Has digest array, is batch - msgp gets freed through cleanup.
				if (! as_security_check_data_op(tr, &msgp->msg, ns, PERM_READ)) {
					goto Cleanup;
				}
				if (0 != as_batch(tr)) {
					cf_info(AS_TSVC, "error from batch function");
					as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
					cf_atomic_int_incr(&g_config.batch_errors);
				}
			} else if (rv == -4) {
				cf_info(AS_TSVC, "bailed due to bad protocol. Returning failure to client");
				dump_msg(msgp);
				as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
			}
			else {
				// All other transaction_prepare() errors.
				cf_info(AS_TSVC, "failed in prepare %d", rv);
				as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
			}
			goto Cleanup;
		} // end transaction pre-processing
	} // end if not preprocessed

#ifdef DIGEST_VALIDATE
	as_transaction_digest_validate(tr);
#endif

	// We'd like to check the timeout of the transaction further up, so it
	// applies to scan, batch, etc. However, the field hasn't been swapped in
	// until now (during the transaction prepare) so here is ok for now.
	if (tr->end_time != 0 && cf_getns() > tr->end_time) {
		cf_debug(AS_TSVC, "thr_tsvc: found expired transaction in queue, aborting");
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_TIMEOUT);
		goto Cleanup;
	}

	// Find the namespace.
	as_msg_field *nsfp = as_msg_field_get(&msgp->msg, AS_MSG_FIELD_TYPE_NAMESPACE);
	if (!nsfp) {
		cf_warning(AS_TSVC, "no namespace in protocol request");
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_NAMESPACE);
		goto Cleanup;
	}

	ns = as_namespace_get_bymsgfield(nsfp);
	if (!ns) {
		char nsprint[AS_ID_NAMESPACE_SZ];
		uint32_t ns_sz = as_msg_field_get_value_sz(nsfp);
		uint32_t len = ns_sz;
		if (ns_sz >= sizeof(nsprint)) {
			ns_sz = sizeof(nsprint) - 1;
		}
		memcpy(nsprint, nsfp->data, ns_sz);
		nsprint[ns_sz] = 0;
		cf_warning(AS_TSVC, "unknown namespace %s %d %p %u in protocol request - check configuration file",
				nsprint, len, nsfp, nsfp->field_sz);

		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_NAMESPACE);
		goto Cleanup;
	}

	// Process the transaction.
	if ((msgp->msg.info2 & AS_MSG_INFO2_WRITE)
			|| (msgp->msg.info1 & AS_MSG_INFO1_READ)) {
		cf_detail_digest(AS_TSVC, &(tr->keyd), "  wr  tr %p fd %d proxy(%"PRIx64") ::",
				tr, tr->proto_fd_h ? tr->proto_fd_h->fd : 0, tr->proxy_node);

		// Obtain a write reservation - or get the node that would satisfy.
		if (msgp->msg.info2 & AS_MSG_INFO2_WRITE) {
			// If there is udata in the transaction, it's a udf internal
			// transaction, so don't free msgp here as other internal udf
			// transactions might end up using it later. Free when the scan job
			// is complete.
			if (tr->udata.req_udata) {
				free_msgp = false;
			}
			else if (tr->proto_fd_h && ! as_security_check_data_op(tr, &msgp->msg, ns, PERM_WRITE)) {
				goto Cleanup;
			}

			// If the transaction is "shipped proxy op" to the winner node then
			// just do the migrate reservation.
			if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
				as_partition_reserve_migrate(ns, as_partition_getid(tr->keyd),
						&tr->rsv, &dest);
				partition_cluster_key = tr->rsv.cluster_key;
				cf_debug(AS_TSVC,
						"[Write MIGRATE CASE]: Partition CK(%"PRIx64")", partition_cluster_key);
				rv = 0;
			} else {
				rv = as_partition_reserve_write(ns,
						as_partition_getid(tr->keyd), &tr->rsv, &dest, &partition_cluster_key);
				cf_debug(AS_TSVC, "[WRITE CASE]: Partition CK(%"PRIx64")", partition_cluster_key);
			}

			if (g_config.write_duplicate_resolution_disable == true) {
				// Zombie writes can be a real drain on performance, so turn
				// them off in an emergency.
				tr->rsv.n_dupl = 0;
				// memset(tr->rsv.dupl_nodes, 0, sizeof(tr->rsv.dupl_nodes));
			}
			if (rv == 0) {
				cf_atomic_int_incr(&g_config.rw_tree_count);
			}
		}
		else {  // <><><> READ Transaction <><><>

			if (tr->proto_fd_h && ! as_security_check_data_op(tr, &msgp->msg, ns, PERM_READ)) {
				goto Cleanup;
			}

			// If the transaction is "shipped proxy op" to the winner node then
			// just do the migrate reservation.
			if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
				as_partition_reserve_migrate(ns, as_partition_getid(tr->keyd),
						&tr->rsv, &dest);
				partition_cluster_key = tr->rsv.cluster_key;
				cf_debug(AS_TSVC, "[Read MIGRATE CASE]: Partition CK(%"PRIx64")", partition_cluster_key);
				rv = 0;
			} else {
				rv = as_partition_reserve_read(ns, as_partition_getid(tr->keyd),
						&tr->rsv, &dest, &partition_cluster_key);
				cf_debug(AS_TSVC, "[READ CASE]: Partition CK(%"PRIx64")", partition_cluster_key);
			}

			if (rv == 0) {
				cf_atomic_int_incr(&g_config.rw_tree_count);
			}
			if ((0 == rv) & (tr->rsv.n_dupl > 0)) {
				// A duplicate merge is in progress, upgrade to a write
				// reservation.
				as_partition_release(&tr->rsv);
				cf_atomic_int_decr(&g_config.rw_tree_count);
				rv = as_partition_reserve_write(ns,
						as_partition_getid(tr->keyd), &tr->rsv, &dest, &partition_cluster_key);
				if (rv == 0) {
					cf_atomic_int_incr(&g_config.rw_tree_count);
				}
			}
		}
		if (dest == 0) {
			cf_crash(AS_TSVC, "invalid destination while reserving partition");
		}

		// If a partition reservation was obtained and the cluster keys (CK)
		// DO NOT match, then release this reservation.
		//
		// Note that this execution path drops into the PROXY DIVERT case if the
		// transaction CK does not match the partition CK. This is a reasonable
		// action, since if the cluster got reorganized, it is likely that THIS
		// NODE will no longer be the master for THIS partition, and thus we
		// will need proxy_divert() to send it to the right node.
		//
		// Also, note that incoming_CK is ZERO is when it comes from the CLIENT,
		// so any message from the client ALWAYS MATCHES. The OTHER case
		// (non-zero) is when it is sent by proxy - and THEN it is important
		// that the C Keys match.
		bool cluster_keys_match = (tr->incoming_cluster_key == 0)
				|| (tr->incoming_cluster_key == partition_cluster_key);
		if ((0 == rv) && !cluster_keys_match) {
			// Transaction and Partition cluster keys DO NOT MATCH, AND this msg
			// was sent by another node (not the client).
			ns = 0;
			as_partition_release(&tr->rsv);
			cf_atomic_int_decr(&g_config.rw_tree_count);
			memset(&tr->rsv, -1, sizeof(tr->rsv)); // probably not needed
			cf_debug_digest(AS_TSVC, &(tr->keyd),
					"Trans/Part Cluster Key Mismatch: ReQ! P(%u) XCK(%"PRIx64") PtCK(%"PRIx64"): ",
					tr->rsv.pid, tr->incoming_cluster_key, partition_cluster_key);
			cf_atomic_int_incr(&g_config.stat_cluster_key_trans_to_proxy_retry);
			// Execution path naturally drops into the proxy handling section
			// below, which is the "else" clause of this next "if".
		}

		if ((0 == rv) && cluster_keys_match) {
			// Even though the Transaction cluster key matches the current state
			// of the partition (checked above), there's more to test. If the
			// partition cluster key does not match the node cluster key, that
			// would mean that the new partition map and the state of all of the
			// partitions are not yet consistent with this node.
			//
			// If the node and partition are not in sync, then we need to set
			// aside this transaction until the node and partition are in sync
			// so that we can process the transaction.
			node_cluster_key = as_paxos_get_cluster_key();
			if (node_cluster_key != partition_cluster_key) {
				// This transaction is NOT READY to be processed. We must set it
				// aside until the proper cluster state is restored. We queue
				// this transaction on the "slow queue" to give it time to rest
				// while the cluster state becomes consistent again.
				as_partition_release(&tr->rsv);
				cf_atomic_int_decr(&g_config.rw_tree_count);
				thr_tsvc_enqueue_slow(tr);
				ns = NULL;
				free_msgp = false;
				goto Cleanup;
			}

			ns = 0; // got a reservation
			tr->microbenchmark_is_resolve = false;
			if (msgp->msg.info2 & AS_MSG_INFO2_WRITE) {
				// Do the WRITE.
				cf_detail_digest(AS_TSVC, &(tr->keyd),
						"AS_WRITE_START  dupl(%d) : ", tr->rsv.n_dupl);

				MICROBENCHMARK_HIST_INSERT_AND_RESET_P(wt_q_process_hist);

				rv = as_write_start(tr); // <><> WRITE <><>
				if (rv == 0) {
					free_msgp = false;
				}
			}
			else {
				// Do the READ.
				cf_detail_digest(AS_TSVC, &(tr->keyd),
						"AS_READ_START  dupl(%d) :", tr->rsv.n_dupl);

				MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_q_process_hist);

				rv = as_read_start(tr); // <><> READ <><>
				if (rv == 0) {
					free_msgp = false;
				}
			}

			// Process the return value from as_rw_start():
			// -1 :: "report error to requester"
			// -2 :: "try again"
			// -3 :: "duplicate proxy request, drop"
			if (0 != rv) {
				rv = as_rw_process_result(rv, tr, &free_msgp); 
			}
			if (free_msgp == true) {
				cf_free(msgp);
				free_msgp = false;
			}
			if (rv) {
				goto Cleanup;
			}
		} else {
			// rv != 0 or cluster keys DO NOT MATCH.
			//
			// Make sure that if it is shipped op it is not further redirected.
			if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
		
				int ret_code = 0;
				if (!cluster_keys_match) {
					ret_code = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
				} else {
					cf_warning(AS_RW,
							"Failing the shipped op due to reservation error %d",
							rv);
					ret_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
				}

				as_proxy_send_response(tr->proxy_node, tr->proxy_msg,
						ret_code, 0, 0, 0, 0, 0, tr->trid, NULL);

			} else if (tr->proto_fd_h) {
				// Divert the transaction into the proxy system; in this case, no
				// reservation was obtained. Pass the cluster key along. Note that
				// if we landed here because of a CLUSTER KEY MISMATCH,
				// (transaction CK != Partition CK), then it is probably the case
				// that we have to forward this request by proxy, since the
				// partition for this transaction has probably moved and is no
				// longer appropriate for this node.

				// Proxy divert - reroute client message. Note that
				// as_proxy_divert() consumes the msgp.
				cf_detail(AS_PROXY, "proxy divert (wr) to %("PRIx64")", dest);
				// Originating node, no write request associated.
				as_proxy_divert(dest, tr, ns, partition_cluster_key);
				ns = 0;
				free_msgp = false;
			} else if (tr->proxy_msg) {
				// Reroute proxy msg: in this case, send the request back to the
				// original node for a retry. dest returned by partition
				// reservation is meaningless here.
				if (!cluster_keys_match) {
					dest = tr->proxy_node;
					cf_debug_digest(AS_PROXY, &(tr->keyd),
							"[CLUSTER KEY MISMATCH] detected sending proxy redirect back to first node(%"PRIx64"): ", dest);
				}
				// Proxy redirect.
				cf_debug_digest(AS_PROXY, &(tr->keyd),
						"proxy REDIRECT (wr) to(%"PRIx64") :", dest);
				as_proxy_send_redirect(tr->proxy_node, tr->proxy_msg, dest);
			}
			if (free_msgp == true) {
				cf_free(msgp);
				free_msgp = false;
			}

			goto Cleanup;
		} // end else "other" transaction
	} // end if read or write
	else {
		cf_info(AS_TSVC, " acting on transaction neither read nor write, error");
		if (ns) {
			ns = 0;
		}
		if (tr->proto_fd_h) {
			as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);

			if (free_msgp == true) {
				cf_free(msgp);
				free_msgp = false;
			}
		} else {
			if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
				cf_info(AS_RW,
						"sending ship op reply, rc AS_PROTO_RESULT_FAIL_PARAMETER to %"PRIx64"",
						tr->proxy_node);
			}
			else {
				cf_detail(AS_RW,
						"sending proxy reply, rc AS_PROTO_RESULT_FAIL_PARAMETER to %"PRIx64"",
						tr->proxy_node);
			}
			as_proxy_send_response(tr->proxy_node, tr->proxy_msg,
					AS_PROTO_RESULT_FAIL_PARAMETER, 0, 0, 0, 0, 0, tr->trid, NULL);
			if (free_msgp == true) {
				cf_free(msgp);
				free_msgp = false;
			}
		}
		goto Cleanup;
	}

	cf_detail(AS_TSVC, "message service complete tr %p", tr);

Cleanup:

	if (free_msgp) {
		cf_free(msgp);
	}
} // end process_transaction()


// Service transactions - arg is the queue we're to service.
void *
thr_tsvc(void *arg)
{
	cf_queue *q = (cf_queue *) arg;

	cf_assert(arg, AS_TSVC, CF_CRITICAL, "invalid argument");

	// Wait for a transaction to arrive.
	for ( ; ; ) {
		as_transaction tr;
		if (0 != cf_queue_pop(q, &tr, CF_QUEUE_FOREVER)) {
			cf_crash(AS_TSVC, "unable to pop from transaction queue");
		}
		process_transaction(&tr);
	}

	return NULL;
} // end thr_tsvc()


// The weak cousin of thr_tsvc(), this thread waits for one or more transactions
// to appear on the slow queue - waits for a brief moment, then pushes all of
// the "slow" queue transactions back on to "fast" queue. The purpose of this
// queue is to delay processing for those transactions that are not currently
// ready to be processed - usually because they are seeing a "cluster key
// mismatch" state. Note that we process ALL transactions on the slow queue
// after a brief wait, so that none of the transactions on the queue are
// unequally penalized.
void *
thr_tsvc_slow(void *null_arg)
{
	cf_queue *q = g_config.transaction_slow_q;

	cf_assert(q, AS_TSVC, CF_CRITICAL, "invalid Q argument");

	// Wait for a transaction to arrive.
	for ( ; ; ) {
		as_transaction tr;
		if (0 != cf_queue_pop(q, &tr, CF_QUEUE_FOREVER)) {
			cf_crash(AS_TSVC, "unable to pop from SLOW transaction queue");
		}
		cf_atomic_int_incr(&g_config.stat_slow_trans_queue_pop);
		cf_atomic_int_incr(&g_config.stat_slow_trans_queue_batch_pop);

		usleep(500); // sleep 500 micro-seconds

		// Get the current size of the queue to transfer all remaining ...
		int counter = cf_queue_sz(q);
		// ... then transfer the one just popped, so it's not included in the
		// count even if it gets returned to the slow queue immediately.
		thr_tsvc_enqueue(&tr);

		// Transfer all.
		while (counter-- > 0) {
			if (0 != cf_queue_pop(q, &tr, CF_QUEUE_NOWAIT)) {
				cf_warning(AS_TSVC, "Empty Queue during Slow Trans Queue Clear");
				break;
			}
			cf_atomic_int_incr(&g_config.stat_slow_trans_queue_pop);
			thr_tsvc_enqueue(&tr);
		}
	}

	return NULL;
} // end thr_tsvc_slow()


// Called at init time by as.c; must match the init sequence chosen.
tsvc_namespace_devices *g_tsvc_devices_a = 0;

pthread_t* g_transaction_threads;
pthread_t g_slow_queue_thread;

static inline pthread_t*
transaction_thread(int i, int j)
{
	return g_transaction_threads + (g_config.n_transaction_threads_per_queue * i) + j;
}

void
as_tsvc_init()
{
	int n_queues = 0;
	g_tsvc_devices_a = (tsvc_namespace_devices *) cf_malloc(sizeof(tsvc_namespace_devices) * g_config.namespaces);

	for (int i = 0 ; i < g_config.namespaces ; i++) {
		as_namespace *ns = g_config.namespace[i];
		tsvc_namespace_devices *dev = &g_tsvc_devices_a[i];
		dev->n_sz = strlen(ns->name);
		strcpy(dev->n_name, ns->name);
		as_storage_attributes s_attr;
		as_storage_namespace_attributes_get(ns, &s_attr);

		dev->n_devices = s_attr.n_devices;
		if (dev->n_devices) {
			// Use 1 queue per read, 1 queue per write, for each device.
			dev->queue_offset = n_queues;
			n_queues += dev->n_devices * 2; // one queue per device per read/write
		} else {
			// No devices - it's an in-memory only namespace.
			dev->queue_offset = n_queues;
			n_queues += 2; // one read queue one write queue
		}
	}
	if (n_queues > MAX_TRANSACTION_QUEUES) {
		cf_crash(AS_TSVC, "# of queues required for use-queue-per-device is too much %d, must be < %d. Please reconfigure w/o use-queue-per-device",
				n_queues, MAX_TRANSACTION_QUEUES);
	}

	if (g_config.use_queue_per_device) {
		g_config.n_transaction_queues = n_queues;
		cf_info(AS_TSVC, "device queues: %d queues with %d threads each",
				g_config.n_transaction_queues, g_config.n_transaction_threads_per_queue);
	} else {
		cf_info(AS_TSVC, "shared queues: %d queues with %d threads each",
				g_config.n_transaction_queues, g_config.n_transaction_threads_per_queue);
	}

	// Create the transaction queues.
	for (int i = 0; i < g_config.n_transaction_queues ; i++) {
		g_config.transactionq_a[i] = cf_queue_create(sizeof(as_transaction), true);
	}

	// Allocate the transaction threads that service all the queues.
	g_transaction_threads = cf_malloc(sizeof(pthread_t) * g_config.n_transaction_queues * g_config.n_transaction_threads_per_queue);

	if (! g_transaction_threads) {
		cf_crash(AS_TSVC, "tsvc pthread_t array allocation failed");
	}

	// Start all the transaction threads.
	for (int i = 0; i < g_config.n_transaction_queues; i++) {
		for (int j = 0; j < g_config.n_transaction_threads_per_queue; j++) {
			if (0 != pthread_create(transaction_thread(i, j), NULL, thr_tsvc, (void*)g_config.transactionq_a[i])) {
				cf_crash(AS_TSVC, "tsvc thread %d:%d create failed", i, j);
			}
		}
	}

	// Now that we have the regular transaction queues set up, we set up the
	// special SLOW queue - where we place transactions that need to pause a bit
	// before running again.
	g_config.transaction_slow_q = cf_queue_create(sizeof(as_transaction), true);

	// Start the thread that manages the slow queue.
	if (0 != pthread_create(&g_slow_queue_thread, NULL, thr_tsvc_slow, NULL)) {
		cf_crash(AS_TSVC, "tsvc slow queue thread create failed");
	}
} // end thr_tsvc_init()


// Peek into packet and decide if transaction can be executed inline in
// demarshal thread or if it must be enqueued, and handle appropriately.
int
thr_tsvc_process_or_enqueue(as_transaction *tr)
{
	// If transaction is for data-in-memory namespace, process in this thread.
	if (g_config.allow_inline_transactions &&
			g_config.n_namespaces_in_memory != 0 &&
					(g_config.n_namespaces_not_in_memory == 0 ||
							as_msg_peek_data_in_memory(tr->msgp))) {
		process_transaction(tr);
		return 0;
	}

	// Transaction is for data-not-in-memory namespace - process via queues.
	return thr_tsvc_enqueue(tr);
}


// Decide which queue to use, and enqueue transaction.
int
thr_tsvc_enqueue(as_transaction *tr)
{
#if 0
	// WARNING! This happens legally in one place, where thr_nsup is deleting
	// elements. If expiration/eviction is off, you should never see this!
	if ((tr->proto_fd == 0) && (tr->proxy_msg == 0)) raise(SIGINT);
#endif

	uint32_t n_q = 0;

	if (g_config.use_queue_per_device) {
		// In queue-per-device mode, we must peek to find out which device (and
		// so which queue) this transaction is destined for.
		proto_peek ppeek;
		as_msg_peek(tr->msgp, &ppeek);

		if (ppeek.ns_n_devices) {
			// Namespace with storage backing.
			// q order: read_dev1, read_dev2, read_dev3, write_dev1, write_dev2, write_dev3
			// See ssd_get_file_id() in drv_ssd.c for device assignment.
			if (ppeek.info1 & AS_MSG_INFO1_READ) {
				n_q = (ppeek.keyd.digest[8] % ppeek.ns_n_devices) + ppeek.ns_queue_offset;
			}
			else {
				n_q = (ppeek.keyd.digest[8] % ppeek.ns_n_devices) + ppeek.ns_queue_offset + ppeek.ns_n_devices;
			}
		}
		else {
			// Namespace is memory only.
			// q order: read, write
			if (ppeek.info1 & AS_MSG_INFO1_READ) {
				n_q = ppeek.ns_queue_offset;
			}
			else {
				n_q = ppeek.ns_queue_offset + 1;
			}
		}
	}
	else {
		// In default mode, transaction can go on any queue - distribute evenly.
		n_q = (g_config.transactionq_current++) % g_config.n_transaction_queues;
	}

	cf_queue *q;

	if ((q = g_config.transactionq_a[n_q]) == NULL) {
		cf_warning(AS_TSVC, "transaction queue #%d not initialized!", n_q);
		return -1;
	}

	return cf_queue_push(q, tr);
} // end thr_tsvc_enqueue()


// Similar to the regular thr_tsvc_enqueue(), this function enqueues a
// transaction on the "slow" queue. The purpose of this queue is simply to slow
// down the processing of any transaction placed here, as the transaction is
// likely not ready for processing for the next milli-second or two. Rather than
// re-queue it on the regular (fast) thr_tsvc queue (which results in "spin"),
// we enqueue transactions here that need to take a small breather before being
// thrust back into action.
int
thr_tsvc_enqueue_slow(as_transaction *tr)
{
	cf_queue *q = g_config.transaction_slow_q;

	cf_assert(q, AS_TSVC, CF_CRITICAL, "invalid Q argument");

	cf_atomic_int_incr(&g_config.stat_slow_trans_queue_push);

	return cf_queue_push(q, tr);
} // end thr_tsvc_enqueue_slow()


// Get one of the most interesting load statistics: the transaction queue depth.
int
thr_tsvc_queue_get_size()
{
	int qs = 0;

	for (int i = 0; i < g_config.n_transaction_queues; i++) {
		if (g_config.transactionq_a[i]) {
			qs += cf_queue_sz(g_config.transactionq_a[i]);
		}
		else {
			cf_detail(AS_TSVC, "no queue when getting size");
		}
	}

	return qs;
} // end thr_tsvc_queue_get_size()
