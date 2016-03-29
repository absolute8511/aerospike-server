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
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "fault.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/scan.h"
#include "base/secondary_index.h"
#include "base/security.h"
#include "base/thr_batch.h"
#include "base/thr_proxy.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
#include "fabric/fabric.h"
#include "storage/storage.h"


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

		if (tr->proxy_msg) {
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
					tr->result_code, 0, 0, 0, 0, 0, 0, as_transaction_trid(tr), NULL);
		}
		else {
			as_transaction_error(tr, tr->result_code);
		}
		return -1;
	}
	return 0;
}


// Handle the transaction, including proxy to another node if necessary.
void
process_transaction(as_transaction *tr)
{
	int rv;
	bool free_msgp = true;
	cl_msg *msgp = tr->msgp;
	as_msg *m = &msgp->msg;

	// Calculate end_time based on message transaction TTL. May be recalculating
	// for re-queued transactions, but nice if end_time not copied on/off queue.
	if (m->transaction_ttl != 0) {
		tr->end_time = tr->start_time +
				((uint64_t)m->transaction_ttl * 1000000);

		// Did the transaction time out while on the queue?
		if (cf_getns() > tr->end_time) {
			cf_debug(AS_TSVC, "transaction timed out in queue");
			as_transaction_error(tr, AS_PROTO_RESULT_FAIL_TIMEOUT);
			goto Cleanup;
		}
	}
	// else - can't incorporate g_config.transaction_max_ns before as_query()

	// Have we finished the very first partition balance?
	if (! as_partition_balance_is_init_resolved() &&
			(tr->flag & AS_TRANSACTION_FLAG_NSUP_DELETE) == 0) {
		cf_debug(AS_TSVC, "rejecting transaction - initial partition balance unresolved");
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_UNAVAILABLE);
		goto Cleanup;
	}

	// Check that the socket is authenticated.
	if (tr->proto_fd_h) {
		uint8_t result = as_security_check(tr->proto_fd_h, PERM_NONE);

		if (result != AS_PROTO_RESULT_OK) {
			as_security_log(tr->proto_fd_h, result, PERM_NONE, NULL, NULL);
			as_transaction_error(tr, (uint32_t)result);
			goto Cleanup;
		}
	}

	// All transactions must have a namespace.
	as_msg_field *nf = as_msg_field_get(m, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! nf) {
		cf_warning(AS_TSVC, "no namespace in protocol request");
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_NAMESPACE);
		goto Cleanup;
	}

	as_namespace *ns = as_namespace_get_bymsgfield(nf);

	if (! ns) {
		char ns_name[AS_ID_NAMESPACE_SZ];
		uint32_t ns_sz = as_msg_field_get_value_sz(nf);
		uint32_t len = ns_sz < sizeof(ns_name) ? ns_sz : sizeof(ns_name) - 1;

		memcpy(ns_name, nf->data, len);
		ns_name[len] = 0;

		cf_warning(AS_TSVC, "unknown namespace %s (%u) in protocol request - check configuration file",
				ns_name, ns_sz);

		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_NAMESPACE);
		goto Cleanup;
	}

	//------------------------------------------------------
	// Multi-record transaction.
	//

	if (as_transaction_is_multi_record(tr)) {
		if (as_transaction_is_batch_direct(tr)) {
			// Old batch.
			if (! as_security_check_data_op(tr, ns, PERM_READ)) {
				as_transaction_error(tr, tr->result_code);
				goto Cleanup;
			}

			if ((rv = as_batch_direct_queue_task(tr, ns)) != 0) {
				as_transaction_error(tr, rv);
				cf_atomic_int_incr(&g_config.batch_errors);
			}
		}
		else if (as_transaction_is_query(tr)) {
			// Query.
			cf_atomic64_incr(&g_config.query_reqs);

			if (! as_security_check_data_op(tr, ns,
					as_transaction_is_udf(tr) ? PERM_UDF_QUERY : PERM_QUERY)) {
				as_transaction_error(tr, tr->result_code);
				goto Cleanup;
			}

			if (as_query(tr, ns) == 0) {
				free_msgp = false;
			}
			else {
				cf_atomic64_incr(&g_config.query_fail);
				as_transaction_error(tr, tr->result_code);
			}
		}
		else {
			// Scan.
			if (! as_security_check_data_op(tr, ns,
					as_transaction_is_udf(tr) ? PERM_UDF_SCAN : PERM_SCAN)) {
				as_transaction_error(tr, tr->result_code);
				goto Cleanup;
			}

			if ((rv = as_scan(tr, ns)) == 0) {
				free_msgp = false;
			}
			else {
				as_transaction_error(tr, rv);
			}
		}

		goto Cleanup;
	}

	//------------------------------------------------------
	// Single-record transaction.
	//

	// May now incorporate g_config.transaction_max_ns if appropriate.
	// TODO - should g_config.transaction_max_ns = 0 be special?
	if (tr->end_time == 0) {
		tr->end_time = tr->start_time + g_config.transaction_max_ns;

		// Again - did the transaction time out while on the queue?
		if (cf_getns() > tr->end_time) {
			cf_debug(AS_TSVC, "transaction timed out in queue");
			as_transaction_error(tr, AS_PROTO_RESULT_FAIL_TIMEOUT);
			goto Cleanup;
		}
	}

	// All single-record transactions must have a digest, or a key from which
	// to calculate it.
	if (as_transaction_has_digest(tr)) {
		// Modern client - just copy digest into tr.

		as_msg_field *df = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
		uint32_t digest_sz = as_msg_field_get_value_sz(df);

		if (digest_sz != sizeof(cf_digest)) {
			cf_warning(AS_TSVC, "digest msg field size %u", digest_sz);
			as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
			goto Cleanup;
		}

		tr->keyd = *(cf_digest *)df->data;
	}
	else if (! as_transaction_is_batch_sub(tr)) {
		// Old client - calculate digest from key & set, directly into tr.

		as_msg_field *kf = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
		uint32_t key_sz = as_msg_field_get_value_sz(kf);

		as_msg_field *sf = as_transaction_has_set(tr) ?
				as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;
		uint32_t set_sz = sf ? as_msg_field_get_value_sz(sf) : 0;

		cf_digest_compute2(sf->data, set_sz, kf->data, key_sz, &tr->keyd);
	}
	// else - batch sub-transactions already (and only) have digest in tr.

	// Sanity check - must know which code path to follow.
	if ((msgp->msg.info2 & AS_MSG_INFO2_WRITE) == 0 &&
			(msgp->msg.info1 & AS_MSG_INFO1_READ) == 0) {
		cf_warning(AS_TSVC, "transaction is neither read nor write - unexpected");
		as_transaction_error(tr, AS_PROTO_RESULT_FAIL_PARAMETER);
		goto Cleanup;
	}

	// Process the transaction.
	cf_detail_digest(AS_TSVC, &(tr->keyd), "  wr  tr %p fd %d proxy(%"PRIx64") ::",
			tr, tr->proto_fd_h ? tr->proto_fd_h->fd : 0, tr->proxy_node);

	cf_node dest;
	uint64_t partition_cluster_key = 0;

	// Obtain a write reservation - or get the node that would satisfy.
	if (msgp->msg.info2 & AS_MSG_INFO2_WRITE) {
		if (tr->proto_fd_h && ! as_security_check_data_op(tr, ns, PERM_WRITE)) {
			as_transaction_error(tr, tr->result_code);
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

		if (tr->proto_fd_h && ! as_security_check_data_op(tr, ns, PERM_READ)) {
			as_transaction_error(tr, tr->result_code);
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

	if (0 == rv) {
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
			as_rw_process_result(rv, tr, &free_msgp);
		}
	} else {
		// rv != 0 (reservation failed)
		//
		// Make sure that if it is shipped op it is not further redirected.
		if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
			cf_warning(AS_RW,
					"Failing the shipped op due to reservation error %d",
					rv);

			as_proxy_send_response(tr->proxy_node, tr->proxy_msg,
					AS_PROTO_RESULT_FAIL_UNKNOWN, 0, 0, 0, 0, 0, 0, as_transaction_trid(tr), NULL);
		}
		else if (tr->proto_fd_h) {
			// Divert the transaction into the proxy system; in this case, no
			// reservation was obtained. Pass the cluster key along.

			// Proxy divert - reroute client message. Note that
			// as_proxy_divert() consumes the msgp.
			cf_detail(AS_PROXY, "proxy divert (wr) to %("PRIx64")", tr->proxy_node);
			// Originating node, no write request associated.
			as_proxy_divert(dest, tr, ns, partition_cluster_key);
			free_msgp = false;
		}
		else if (tr->proxy_msg) {
			as_partition_id pid = as_partition_getid(tr->keyd);
			cf_node redirect_node = as_partition_proxyee_redirect(ns, pid);

			as_proxy_return_to_sender(tr, redirect_node);
		}
		else if (tr->iudf_orig) {
			cf_debug(AS_TSVC,"Internal transaction. Partition reservation failed or cluster key mismatch:%d", rv);
			tr->iudf_orig->cb(tr, AS_PROTO_RESULT_FAIL_UNKNOWN);
			tr->iudf_orig = NULL;
		}
	} // end else "other" transaction

	cf_detail(AS_TSVC, "message service complete tr %p", tr);

Cleanup:
	// Batch transactions should never free msgp.
	if (free_msgp && !tr->batch_shared) {
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

		MICROBENCHMARK_HIST_INSERT_AND_RESET(q_wait_hist);

		process_transaction(&tr);
	}

	return NULL;
} // end thr_tsvc()


pthread_t* g_transaction_threads;

static inline pthread_t*
transaction_thread(int i, int j)
{
	return g_transaction_threads + (g_config.n_transaction_threads_per_queue * i) + j;
}

void
as_tsvc_init()
{
	int n_queues = 0;

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		// TODO - get rid of this when we make general use of ns->n_devices, and
		// set it in config parse:
		as_storage_attributes s_attr;
		as_storage_namespace_attributes_get(ns, &s_attr);

		ns->n_devices = s_attr.n_devices;

		if (ns->n_devices > 0) {
			// Use 1 queue per read, 1 queue per write, for each device.
			ns->dev_q_offset = n_queues;
			n_queues += ns->n_devices * 2; // one queue per device per read/write
		} else {
			// No devices - it's an in-memory only namespace.
			ns->dev_q_offset = n_queues;
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
							as_msg_peek_data_in_memory(&tr->msgp->msg))) {
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
		as_msg_peek(tr, &ppeek);

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
		cf_crash(AS_TSVC, "transaction queue #%d not initialized!", n_q);
	}

	if (cf_queue_push(q, tr) != 0) {
		cf_crash(AS_TSVC, "transaction queue push failed - out of memory?");
	}

	return 0;
} // end thr_tsvc_enqueue()


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
