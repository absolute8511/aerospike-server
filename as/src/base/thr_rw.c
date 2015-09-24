/*
 * thr_rw.c
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
 * This file now contains both read and write logic.
 *
 * On each server are some number of transaction queues, and an equal number
 * of transaction threads that service the queues. Transaction messages may
 * come in from external application or they may be generated
 * internally (couldn't process the request initially, re generate the request for
 * processing at a later time).
 *
 */

#include "base/feature.h"

#include "base/thr_rw_internal.h"

#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <strings.h>

#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aerospike/as_list.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_shash.h"

#include "jem.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/secondary_index.h"
#include "base/thr_proxy.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
#include "base/write_request.h"
#include "base/xdr_serverside.h"
#include "fabric/fabric.h"
#include "fabric/paxos.h"
#include "storage/storage.h"
#include "fabric/migrate.h"

msg_template rw_mt[] =
{
	{ RW_FIELD_OP, M_FT_UINT32 },
	{ RW_FIELD_RESULT, M_FT_UINT32 },
	{ RW_FIELD_NAMESPACE, M_FT_BUF },
	{ RW_FIELD_NS_ID, M_FT_UINT32 },
	{ RW_FIELD_GENERATION, M_FT_UINT32 },
	{ RW_FIELD_DIGEST, M_FT_BUF },
	{ RW_FIELD_VINFOSET, M_FT_BUF },
	{ RW_FIELD_AS_MSG, M_FT_BUF },
	{ RW_FIELD_CLUSTER_KEY, M_FT_UINT64 },
	{ RW_FIELD_RECORD, M_FT_BUF },
	{ RW_FIELD_TID, M_FT_UINT32 },
	{ RW_FIELD_VOID_TIME, M_FT_UINT32 },
	{ RW_FIELD_INFO, M_FT_UINT32 },
	{ RW_FIELD_REC_PROPS, M_FT_BUF },
	{ RW_FIELD_MULTIOP, M_FT_BUF },
	{ RW_FIELD_LDT_VERSION, M_FT_UINT64 },
};
// General Debug Stmts
// #define DEBUG 1
// Specific Flag to dump the MSG
// #define DEBUG_MSG 1
// #define TRACK_WR 1
// #define EXTRA_CHECKS 1
static cf_atomic32 g_rw_tid = 0;
static rchash *g_write_hash = 0;
static pthread_t g_rw_retransmit_th;

// HELPER
void print_digest(u_char *d) {
	printf("0x");
	for (int i = 0; i < CF_DIGEST_KEY_SZ; i++)
		printf("%02x", d[i]);
}

void g_write_hash_delete(global_keyd *gk) {
	rchash_delete(g_write_hash, gk, sizeof(global_keyd));
}

// forward references internal to the file
void rw_complete(write_request *wr, as_transaction *tr, as_index_ref *r_ref);
void read_local(as_transaction *tr, as_index_ref *r_ref);
int write_local(as_transaction *tr, write_local_generation *wlg,
				uint8_t **pickled_buf, size_t *pickled_sz,
				as_rec_props *p_pickled_rec_props, cf_dyn_buf *db);
int write_journal(as_transaction *tr, write_local_generation *wlg); // only do write
int write_delete_journal(as_transaction *tr, bool is_subrec);
static void release_proto_fd_h(as_file_handle *proto_fd_h);
void xdr_write(as_namespace *ns, cf_digest keyd, as_generation generation,
			   cf_node masternode, bool is_delete, uint16_t set_id);

/*
 ** queue for async replication
 **
 ** this is an unsafe way to replicate, because the replication queue does not
 ** persist in any way
 */

void rw_replicate_process_ack(cf_node node, msg * m, bool is_write);
void rw_replicate_async(cf_node node, msg *m);
int rw_replicate_init(void);
int rw_multi_process(cf_node node, msg *m);

uint32_t
write_digest_hash(void *value, uint32_t value_len)
{
	global_keyd *gkd = value;

	return ((gkd->keyd.digest[DIGEST_SCRAMBLE_BYTE1] << 16)
			| (gkd->keyd.digest[DIGEST_SCRAMBLE_BYTE2] << 8)
			| (gkd->keyd.digest[DIGEST_SCRAMBLE_BYTE3]));
}

/*
 Put pending transaction request back on the main transaction queue
 */
void
write_request_restart(wreq_tr_element *w)
{
	as_transaction *tr = &w->tr;
	cf_debug(AS_RW, "as rw start:  QUEUEING BACK (%d:%p) %"PRIx64"",
			 tr->proto_fd_h ? tr->proto_fd_h->fd : 0, tr->proxy_msg, *(uint64_t*)&tr->keyd);

	MICROBENCHMARK_RESET_P();

	if (0 != thr_tsvc_enqueue(tr)) {
		cf_warning(AS_RW,
				"WRITE REQUEST: FAILED queueing back request %"PRIx64"",
				*(uint64_t*)&tr->keyd);
		cf_free(tr->msgp);
		tr->msgp = 0;
	}
	cf_atomic_int_decr(&g_config.n_waiting_transactions);
	cf_free(w);
}

write_request *
write_request_create(void) {
	write_request *wr = cf_rc_alloc( sizeof(write_request) );

	// NB: The things are zeroed out only upto rsv.
	//     Any element added post this won't be zero'ed out.
	memset(wr, 0, offsetof(write_request, rsv));
#ifdef TRACK_WR
	wr_track_create(wr);
#endif
	cf_assert(wr, AS_RW, CF_WARNING, "cf_rc_alloc");
	cf_atomic_int_incr(&g_config.write_req_object_count);
	wr->ready = false;
	wr->tid = cf_atomic32_incr(&g_rw_tid);
	wr->rsv_valid = false;
	wr->proto_fd_h = 0;
	wr->dest_msg = 0;
	wr->proxy_msg = 0;
	wr->msgp = 0;
	wr->pickled_buf = 0;
	as_rec_props_clear(&wr->pickled_rec_props);
	wr->respond_client_on_master_completion = false;
	wr->replication_fire_and_forget = false;
	// Set the initial limit on wr lifetime to guarantee finite life-span.
	// (Will be reset relative to the transaction end time if/when the wr goes ready.)
	cf_atomic64_set(&(wr->end_time), cf_getns() + g_config.transaction_max_ns);

	// Initialize with the default consistency guarantee levels.
	// (These will be re-set later according to combining the server's namespace
	//   and the client transaction policy settings.)
	wr->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
	wr->write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;

	// initialize waiting transaction queue
	wr->wait_queue_head = NULL;
	if (0 != pthread_mutex_init(&wr->lock, 0))
		cf_crash(AS_RW, "couldn't initialize partition vinfo set lock: %s",
				cf_strerror(errno));

	// initialize atomic integers
	wr->trans_complete = 0;
	wr->shipped_op     = false;
	wr->shipped_op_initiator = false;

	wr->dest_sz = 0;
	UREQ_DATA_INIT(&wr->udata);
	memset((void *) & (wr->dup_msg[0]), 0, sizeof(wr->dup_msg));

	wr->batch_shared = 0;
	wr->batch_index = 0;
	return (wr);
}

int write_request_init_tr(as_transaction *tr, void *wreq) {
	// INIT_TR
	write_request *wr = (write_request *) wreq;
	tr->incoming_cluster_key = 0;
	tr->start_time = wr->start_time;
	tr->end_time = cf_atomic64_get(wr->end_time);

	// In case wr->proto_fd_h is set it is considered to be case system is
	// bailing out to set it to NULL once it has been handed over to transaction
	tr->proto_fd_h = wr->proto_fd_h;
	wr->proto_fd_h = 0;
	tr->proxy_node = wr->proxy_node;
	tr->proxy_msg = wr->proxy_msg;
	wr->proxy_msg = 0;
	tr->keyd = wr->keyd;

	// Partition reservation and msg are freed when the write request is destroyed
	as_partition_reservation_copy(&tr->rsv, &wr->rsv);
	tr->msgp = wr->msgp;
	tr->result_code = AS_PROTO_RESULT_OK;
	tr->trid = 0;
	tr->preprocessed = true;
	tr->flag = 0;

	tr->generation = 0;
	tr->void_time = 0;
	tr->microbenchmark_is_resolve = false;

	if (wr->shipped_op)
		tr->flag |= AS_TRANSACTION_FLAG_SHIPPED_OP;
	UREQ_DATA_COPY(&tr->udata, &wr->udata);
	UREQ_DATA_RESET(&wr->udata);
	tr->microbenchmark_time = wr->microbenchmark_time;
	tr->batch_shared = wr->batch_shared;
	tr->batch_index = wr->batch_index;

#if 0
	if (wr->is_read) {
		MICROBENCHMARK_HIST_INSERT_AND_RESET(rt_resolve_wait_hist);
	} else {
		MICROBENCHMARK_HIST_INSERT_AND_RESET(wt_resolve_wait_hist);
	}

	MICROBENCHMARK_RESET();
	tr->microbenchmark_is_resolve = true;
#endif
	return 0;
}

void write_request_destructor(void *object) {
	write_request *wr = object;

#ifdef TRACK_WR
	wr_track_destroy(wr);
#endif

	if (udf_rw_needcomplete_wr(wr)) {
		as_transaction tr;
		write_request_init_tr(&tr, wr);
		cf_warning(AS_RW,
				"UDF request not complete ... Completing it nonetheless !!!");
		udf_rw_complete(&tr, 0, __FILE__, __LINE__);
	}

	if (wr->dest_msg)
		as_fabric_msg_put(wr->dest_msg);
	if (wr->proxy_msg)
		as_fabric_msg_put(wr->proxy_msg);
	if (wr->msgp) {
		cf_free(wr->msgp);
		wr->msgp = 0;
	}
	if (wr->pickled_buf)
		cf_free(wr->pickled_buf);
	if (wr->pickled_rec_props.p_data)
		cf_free(wr->pickled_rec_props.p_data);
	cf_dyn_buf_free(&wr->response_db);
	if (wr->rsv_valid) {
		as_partition_release(&wr->rsv);
		cf_atomic_int_decr(&g_config.rw_tree_count);
	}
	if (wr->proto_fd_h)
		AS_RELEASE_FILE_HANDLE(wr->proto_fd_h);
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (wr->dup_msg[i])
			as_fabric_msg_put(wr->dup_msg[i]);
	}
	wreq_tr_element *e = wr->wait_queue_head;
	while (e) {
		wreq_tr_element *next = e->next;
		write_request_restart(e);
		e = next;
	}
	pthread_mutex_destroy(&wr->lock);

	cf_atomic_int_decr(&g_config.write_req_object_count);
	memset(wr, 0xff, sizeof(write_request));
	return;
}

void rw_request_dump(write_request *wr, char *msg) {
	cf_info(AS_RW, "Dump write request: tid %d : %p %s", wr->tid, wr, msg);
	pthread_mutex_lock(&wr->lock);
	cf_info(AS_RW,
			" %p: ready %d isread %d trans_c %d dupl_trans_c %d rsv_valid %d : %s",
			wr, wr->ready, wr->is_read, wr->trans_complete, wr->dupl_trans_complete, wr->rsv_valid, ((wr->wait_queue_head == 0) ? "" : "QUEUE"));
	cf_info(AS_RW, " %p: protofd %p proxynode %"PRIx64" proxymsg %p",
			wr, wr->proto_fd_h, wr->proxy_node, wr->proxy_msg);

	cf_info(AS_RW, " %p: dest sz %d", wr->dest_sz);
	for (int i = 0; i < wr->dest_sz; i++)
		cf_info(AS_RW,
				" %p: dest: %d node %"PRIx64" complete %d dupmsg %p dup_rc %d",
				wr, i, wr->dest_nodes[i], wr->dest_complete[i], wr->dup_msg[i], wr->dup_result_code[i]);

	pthread_mutex_unlock(&wr->lock);
}

int rw_dump_reduce(void *key, uint32_t keylen, void *data, void *udata) {
	write_request *wr = data;
	rw_request_dump(wr, "");
	return (0);
}

/*
 * An in-flight transaction has dependencies on proles - send
 * the transaction message to any prole which has not yet responded.
 */
void send_messages(write_request *wr) {
	/* Iterate over every destination node -- send what we have to */
	for (int i = 0; i < wr->dest_sz; i++) {

		if (wr->dest_complete[i] == true)
			continue;

		WR_TRACK_INFO(wr, "send_messages: sending message");
		// Retransmit the message
		msg_incr_ref(wr->dest_msg);
		if (wr->rsv_valid)
			cf_debug(AS_RW,
					"resending rw request: {%s:%d} node %"PRIx64" digest %"PRIx64"",
					wr->rsv.ns->name, wr->rsv.pid, wr->dest_nodes[i], wr->keyd);
		else
			cf_debug(AS_RW,
					"resending rw request: no reservation node %"PRIx64" digest %"PRIx64"",
					wr->dest_nodes[i], wr->keyd);
		int rv = as_fabric_send(wr->dest_nodes[i], wr->dest_msg,
				AS_FABRIC_PRIORITY_MEDIUM);
		if (rv != 0) {
			as_fabric_msg_put(wr->dest_msg);
			if ( rv == AS_FABRIC_ERR_NO_NODE ) {
				// One of the nodes has disappeared; mark it as completed
				// Policy issue: this code says when a node vanishes while writing, then
				// simply complete the write back to the sender, because at least its written to the
				// master (here). Another policy would be to pick up the new list, and write to the new
				// replica.
				cf_debug(AS_RW,
						"can't send write retranmit: no node {%s:%d} digest %"PRIx64"",
						wr->rsv.ns->name, wr->rsv.pid, wr->keyd);
				wr->dest_complete[i] = true;
				// Handle the case for the duplicate merge
				wr->dup_result_code[i] = AS_PROTO_RESULT_FAIL_UNKNOWN;
			} else {
				// Unhandled failure cases -1 general explosion, -2 queue fail
				cf_crash(AS_RW,
						"unhandled return from as_fabric_send: %d  {%s:%d}",
						rv, wr->rsv.ns->name, wr->rsv.pid);
			}
		}
	}
}

void as_rw_set_stat_counters(bool is_read, int rv, as_transaction *tr) {
	int result_code = tr ? tr->result_code : 0;
	if (is_read) {
		if (rv == 0) {
			if (result_code == AS_PROTO_RESULT_FAIL_NOTFOUND)
				cf_atomic_int_incr(&g_config.stat_read_errs_notfound);
			else
				cf_atomic_int_incr(&g_config.stat_read_success);
		} else
			cf_atomic_int_incr(&g_config.stat_read_errs_other);
	} else {
		if (rv == 0)
			cf_atomic_int_incr(&g_config.stat_write_success);
		else {
			cf_atomic_int_incr(&g_config.stat_write_errs);
			if (result_code == AS_PROTO_RESULT_FAIL_NOTFOUND)
				cf_atomic_int_incr(&g_config.stat_write_errs_notfound);
			else
				cf_atomic_int_incr(&g_config.stat_write_errs_other);
		}
	}
}

int
rw_msg_setup_infobits(msg *m, as_transaction *tr, bool has_udf, bool is_ldt_sub, bool is_ldt_parent)
{
	uint32_t info = 0;
	if (tr->msgp && (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR))
		info |= RW_INFO_XDR;
	if (tr->flag & AS_TRANSACTION_FLAG_SINDEX_TOUCHED) {
		info |= RW_INFO_SINDEX_TOUCHED;
	}
	if (tr->flag & AS_TRANSACTION_FLAG_NSUP_DELETE)
		info |= RW_INFO_NSUP_DELETE;

	if (tr->rsv.ns->ldt_enabled) {
		// Nothing is set means it is normal record
		if (is_ldt_sub) {
			cf_detail(AS_RW,
					"MULTI_OP : Set Up Replication Message for the LDT Subrecord %"PRIx64"",
					*(uint64_t*)&tr->keyd);
			info |= RW_INFO_LDT_SUBREC;
		}
		else {
			if (is_ldt_parent) {
				cf_detail(AS_RW,
						"MULTI_OP : Set Up Replication Message for the LDT Record %"PRIx64"",
						*(uint64_t*)&tr->keyd);
				info |= RW_INFO_LDT_PARENTREC;
			}
			info |= RW_INFO_LDT;
		}
	}

	/* UDF write being replicated */
	if (has_udf) {
		info |= RW_INFO_UDF_WRITE;
	}

	msg_set_uint32(m, RW_FIELD_INFO, info);

	return 0;
}

void
rw_msg_setup_ldt_fields(msg *m, as_transaction *tr, cf_digest *keyd)
{
	// Send the Partition version info and current ldt outgoing migration version
	// at the master ..
	// only specific to LDT used to decide whether to write source version or
	// prole version. When doing replication ..
	// -- In case the prole node has partition version same
	//    as master the ldt version in the destination version stamped in LDT rec
	//    and subrec.
	// -- in case the prole node has partition version different from master this
	//    is replication while migration is still going on and versions have not
	//    consolidated. In those cases version at the source node is replicated
	msg_set_buf(m, RW_FIELD_VINFOSET, (uint8_t *)&tr->rsv.p->version_info, sizeof(as_partition_vinfo), MSG_SET_COPY);
	cf_detail_digest(AS_RW, keyd,
			"MULTI_OP : Set Up Replication Message for the LDT current outgoing "
			" migrate version %ld for partition %d", tr->rsv.p->current_outgoing_ldt_version, tr->rsv.p->partition_id);
	if (tr->rsv.p->current_outgoing_ldt_version != 0) {
		msg_set_uint64(m, RW_FIELD_LDT_VERSION, tr->rsv.p->current_outgoing_ldt_version);
	}
}

int
rw_msg_setup(msg *m, as_transaction *tr, cf_digest *keyd,
		uint8_t ** p_pickled_buf, size_t pickled_sz,
		as_rec_props * p_pickled_rec_props, int op, bool has_udf, bool is_subrec)
{
	// setup the write message
	msg_set_buf(m, RW_FIELD_DIGEST, (void *) keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_uint64(m, RW_FIELD_CLUSTER_KEY, tr->rsv.cluster_key);
	msg_set_buf(m, RW_FIELD_NAMESPACE, (byte *) tr->rsv.ns->name,
			strlen(tr->rsv.ns->name), MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_ID, tr->rsv.ns->id);
	msg_set_uint32(m, RW_FIELD_OP, op);

	if (op == RW_OP_WRITE) {

		msg_set_uint32(m, RW_FIELD_GENERATION, tr->generation);
		msg_set_uint32(m, RW_FIELD_VOID_TIME, tr->void_time);

		// Send this along with parent packet as well. This is required
		// in case the LDT parent replication is done without MULTI_OP.
		// Example touch of the some other bin in the LDT parent
		if (!is_subrec) {
			rw_msg_setup_ldt_fields(m, tr, keyd);
		}

		if (*p_pickled_buf) {

			// Use info in property map for writes
			bool is_ldt_sub;
			bool is_ldt_parent;
			as_ldt_get_property(p_pickled_rec_props, &is_ldt_parent, &is_ldt_sub);
			rw_msg_setup_infobits(m, tr, has_udf, is_ldt_sub, is_ldt_parent);

			msg_set_unset(m, RW_FIELD_AS_MSG);
			msg_set_buf(m, RW_FIELD_RECORD, (void *) *p_pickled_buf, pickled_sz,
					MSG_SET_HANDOFF_MALLOC);
			*p_pickled_buf = NULL;

			if (p_pickled_rec_props && p_pickled_rec_props->p_data) {
				msg_set_buf(m, RW_FIELD_REC_PROPS, p_pickled_rec_props->p_data,
						p_pickled_rec_props->size, MSG_SET_HANDOFF_MALLOC);
				as_rec_props_clear(p_pickled_rec_props);
			}
		} else { // deletes come here
			if ((tr->msgp->msg.info2 & AS_MSG_INFO2_DELETE) == 0) {
				uint32_t info = 0;
				msg_get_uint32(m, RW_FIELD_INFO, &info);
				cf_crash(AS_RW, "sending delete but DELETE msg flag not set - info 0x%x", info);
			}
			cf_detail_digest(AS_RW, keyd, "Send delete to replica ");
			msg_set_buf(m, RW_FIELD_AS_MSG, (void *) tr->msgp,
					as_proto_size_get(&tr->msgp->proto), MSG_SET_COPY);
			msg_set_unset(m, RW_FIELD_RECORD);

			rw_msg_setup_infobits(m, tr, has_udf, is_subrec, false);
		}
	} else if (op == RW_OP_DUP) {
		msg_set_uint32(m, RW_FIELD_OP, RW_OP_DUP);
		if (tr->rsv.n_dupl > 1) {
			cf_debug(AS_RW, "{%s:%d} requesting duplicates from %d nodes, very "
					 "unlikely, digest %"PRIx64"",
					 tr->rsv.ns->name, tr->rsv.pid, tr->rsv.n_dupl, *(uint64_t*)&tr->keyd);
		}
	} else if (op == RW_OP_MULTI) {
		// TODO: What is meaning of generation and TTL here ???
		msg_set_uint32(m, RW_FIELD_GENERATION, tr->generation);
		msg_set_uint32(m, RW_FIELD_VOID_TIME, tr->void_time);

		rw_msg_setup_ldt_fields(m, tr, keyd);

		msg_set_uint32(m, RW_FIELD_INFO, RW_INFO_LDT);
		msg_set_unset(m, RW_FIELD_AS_MSG);
		msg_set_unset(m, RW_FIELD_RECORD);
		msg_set_unset(m, RW_FIELD_REC_PROPS);
		msg_set_buf(m, RW_FIELD_MULTIOP, (void *) *p_pickled_buf, pickled_sz,
				MSG_SET_HANDOFF_MALLOC);
		*p_pickled_buf = NULL;
	}

	return 0;
}
// Setups up basic write request. Fills up
// - Namespace / Namespace ID
// - Digest
// - Transaction ID
// - destination nodes data
// - Cluster key
//
// Caller needs to fill up
//
// - op (read/write/duplicate/op)
// - Any other relevant data e.g generation / TTL for the write request.
//
// Expectation: wr should have pickled_buf, pickled_sz, pickled_void_time, dest_nodes
//              and number of dest nodes set properly
//              and passed in tr should have been pre-processed.
//
// Side effect: wr->dest_msg is allocated and populated
//
int
write_request_setup(write_request *wr, as_transaction *tr, int optype)
{
	// create the write message
	if (!wr->dest_msg) {
		if (!(wr->dest_msg = as_fabric_msg_get(M_TYPE_RW))) {
			// [Note:  This can happen when the limit on number of RW "msg" objects is reached.]
			cf_detail(AS_RW,
					"failed to allocate a msg of type %d ~~ bailing out of transaction",
					M_TYPE_RW);
			return -1;
		}
	}

	rw_msg_setup(wr->dest_msg, tr, &wr->keyd, &wr->pickled_buf, wr->pickled_sz,
			&wr->pickled_rec_props, optype, wr->has_udf, false);

	if (wr->shipped_op) {
		cf_detail(AS_RW,
				"SHIPPED_OP WINNER [Digest %"PRIx64"] Initiating Replication for %s op ",
				*(uint64_t *)&wr->keyd, wr->is_read ? "Read" : "Write");
	}

	if (wr->dest_msg) {
		msg_set_uint32(wr->dest_msg, RW_FIELD_TID, wr->tid);
	}

	for (uint i = 0; i < wr->dest_sz; i++) {
		wr->dest_complete[i] = false;
		wr->dup_msg[i] = 0;
		wr->dup_result_code[i] = 0;
	}
	return 0;
}

// Write request standard cleanup function. This is used to cleanup requests when they
// are finished. The associated cleanup done is freeing up as_msg releasing partition
// reservation etc, release proto_fd if it is not released.
//
// Parameter:
//    wr or tr: Never called with both tr and wr set to NULL.
//
// Note:
//
// 1. If called with first_time = true set that would mean wr is not setup at all.
//
// 2. If called with first_time = false then wr and tr both should be valid and
//    and certain field like msgp should be same.
//
// Caller:
//
// Called after the rw_complete is called if the request was successful. In those
// cases rw_complete would replied to the client and cleaned up proto_fd.
//
// Called in case some error has happened. In those cases the proto_fd is released
// and the transaction msgp is freed. Nothing will be sent back to the client and
// let the client timeout the request.
int
rw_cleanup(write_request *wr, as_transaction *tr, bool first_time,
		bool release, int line)
{
	if (!wr || !tr) {
		cf_crash(AS_RW, "Invalid cleanup call [wr=%p tr=%p] .."
				 " not cleanup resource", wr, tr);
		return -1;
	}

	WR_TRACK_INFO(wr, "internal_rw_start - Cleanup Request");
	cf_detail(AS_RW, "{%s:%d} internal_rw_start: COMPLETE %"PRIx64" "
			"%s result code %d",
			tr->rsv.ns->name, tr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE", tr->result_code);

	if (wr->is_read) {
		cf_hist_track_insert_data_point(g_config.rt_hist, tr->start_time);
	} else {
		// Update Write Stats. Don't count Deletes or UDF calls.
		if (! tr->proxy_msg && (tr->msgp->msg.info2 & AS_MSG_INFO2_DELETE) == 0 && ! wr->has_udf) {
			cf_hist_track_insert_data_point(g_config.wt_hist, tr->start_time);
		}
	}
	if (first_time) {
		if ((wr->msgp != NULL) || wr->rsv_valid) {
			cf_warning_digest(AS_RW, &wr->keyd,
					"{%s:%d} rw_cleanup @ %d: illegal state write-request set in first call for %s-request n-dupl %u dupl-nodes[0] %lx [%p %p %d] ",
					tr->rsv.ns->name, tr->rsv.pid, line, wr->is_read ? "read" : "write",
					(uint32_t)tr->rsv.n_dupl, tr->rsv.dupl_nodes[0],
					wr->msgp, tr->msgp, wr->rsv_valid);
		}
		// ATTENTION PLEASE... The msgp and partition reservation
		// is not moved but copied to the running transaction from wr. So
		// clean them up only when it is first time. If it is second time
		// that is called after the duplicate resolution or response from
		// replica the cleanup will happen when the write request is destroyed
		as_partition_release(&tr->rsv);
		cf_atomic_int_decr(&g_config.rw_tree_count);
		if (tr->msgp) {
			cf_free(tr->msgp);
			tr->msgp = 0;
		}
	} else {
		if ((wr->msgp != tr->msgp) || !wr->rsv_valid) {
			cf_warning_digest(AS_RW, &wr->keyd,
					"{%s:%d} rw_cleanup @ %d: illegal state write-request set in second call for %s-request n-dupl %u dupl-nodes[0] %lx [%p %p %d] ",
					tr->rsv.ns->name, tr->rsv.pid, line, wr->is_read ? "read" : "write",
					(uint32_t)tr->rsv.n_dupl, tr->rsv.dupl_nodes[0],
					wr->msgp, tr->msgp, wr->rsv_valid);
		}
	}

	if (tr->proto_fd_h) {
		if (release) {
			cf_detail(AS_RW, "releasing proto_fd_h %d:%p",
					tr->proto_fd_h, line);
			release_proto_fd_h(tr->proto_fd_h);
		} else {
			AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
		}
		tr->proto_fd_h = 0;
	}

	return 0;
}

// When starting execution all UDFs are assumed to be writes. If it turns out a
// UDF was a read, flip the stats - decrement write counters and increment read
// counters.
void
as_rw_update_stat(write_request *wr)
{
	cf_atomic_int_decr(&g_config.write_master);
	cf_atomic_int_decr(&g_config.stat_write_reqs);
	cf_atomic_int_incr(&g_config.stat_read_reqs);
}

// Main entry point of triggering the local operations. Both read/write/UDF.
//
// Caller:
// 1. Main transaction thread context when the user request from the user
//    comes. as_rw_start
//
// 2. Fabric thread when *ALL* the reply for the duplicate resolution request
//    comes back and it is time to apply the op. In case the partition has
//    duplicates wr->dupl_tran_complete is 1 when all the value are resolved
//
// Task:
// 1.  Performs read/write/apply udf on the local record and trigger replication
//     to the replica set.
// 2.  In addition to this triggers duplicate resolution in case there are
//     duplicates of partition. This function gets called after the resolution
//     has finished. Based on state information wr this function performs 1
//     above after the duplicate resolution has finished.
//
// NB: This is only run on the *master* or *acting master* (when master is
//     desync) for the transaction. The transaction protection with the
//     write request hash is maintained on this node.
//
// Sync:
//    Caller makes entry into the write hash. So this function is called under
//    protection of write hash.
//
static int
internal_rw_start(as_transaction *tr, write_request *wr, bool *delete)
{
	/* INIT */
	*delete            = false;
	bool first_time    = false;
	bool dupl_resolved = true;
	int rv             = 0;
	bool is_delete     = (tr->msgp->msg.info2 & AS_MSG_INFO2_DELETE);

	if ((wr->dupl_trans_complete == 0) && (tr->rsv.n_dupl > 0))
		dupl_resolved = false;
	if ((tr->rsv.n_dupl == 0) || !dupl_resolved) {
		first_time = true;
	} else {
		// change transaction id for the second time
		wr->tid = cf_atomic32_incr(&g_rw_tid);
	}

	if (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB) {
		cf_crash(AS_RW, "Invalid transaction state");
	}


	// 1. Short Circuit Read if a record is found locally, and we don't need
	//    strong read consistency, then make sure that we do not go through
	//    duplicate processing
	if ((wr->is_read == true)
		&& (g_config.transaction_repeatable_read == false)
		&& (wr->read_consistency_level == AS_POLICY_CONSISTENCY_LEVEL_ONE)) {

		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_internal_hist);
		as_index_ref r_ref;
		r_ref.skip_lock = false;
		int rec_rv = as_record_get(tr->rsv.tree, &tr->keyd, &r_ref, tr->rsv.ns);
		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_tree_hist);

		if (rec_rv == 0) {
			// if the record is found (no matter whether there are duplicates
			// or not) construct the record lock structure so that thr_tsvc_read
			// does not reopen the record
			// send the response to requester (consumes the tr, basically)
			WR_TRACK_INFO(wr, "internal_rw_start: Short Circuit For Reads");
			rw_complete(wr, tr, &r_ref);
			rw_cleanup(wr, tr, first_time, false, __LINE__);
			as_rw_set_stat_counters(true, 0, tr);
			*delete = true;
			return (0);
		} else if ((tr->rsv.n_dupl == 0) || dupl_resolved) {
			//  if record not found
			//     - If there are no duplicates
			//     - If duplicate are resolved
			// send the response to requester (consumes the tr, basically)
			WR_TRACK_INFO(wr, "internal_rw_start: Short Circuit For Reads");
			tr->result_code = AS_PROTO_RESULT_FAIL_NOTFOUND;
			rw_complete(wr, tr, NULL);
			rw_cleanup(wr, tr, first_time, false, __LINE__);
			as_rw_set_stat_counters(true, 0, tr);
			*delete = true;
			return (0);
		} else {
			// if record not found
			//     - if there are duplicates then go do duplicate
			//       resolution. To get hold of some value if there
			//       is any in cluster
		}
	}

	// 2. Detect if there are duplicates create a duplicate-request and start
	//    the duplicate-fetch phase
	if (!dupl_resolved) {
		cf_atomic_int_incr(&g_config.stat_duplicate_operation);

		WR_TRACK_INFO(wr, "internal_rw_start: starting duplicate phase");
		cf_debug(AS_RW, "{%s:%d} as_write_start: duplicate partitions "
				"encountered %d %"PRIx64"",
				tr->rsv.ns->name, tr->rsv.pid, tr->rsv.n_dupl, tr->rsv.dupl_nodes[0]);

		wr->dest_sz = tr->rsv.n_dupl;
		memcpy(wr->dest_nodes, tr->rsv.dupl_nodes,
				wr->dest_sz * sizeof(cf_node));
		if (write_request_setup(wr, tr, RW_OP_DUP)) {
			rw_cleanup(wr, tr, first_time, true, __LINE__);
			*delete = true;
			return (0);
		}
	}
	// 3. Duplicates are either unnecessary or resolved.
	// Start the actual operation
	else {
		// Short circuit for reads, after duplicate resolution record will
		// already be open.
		if ((wr->is_read == true)
			&& ((g_config.transaction_repeatable_read == true)
				|| (wr->read_consistency_level == AS_POLICY_CONSISTENCY_LEVEL_ALL))) {
			MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_internal_hist);
			WR_TRACK_INFO(wr, "internal_rw_start: Short Circuit for Read After Duplicate Resolution");
			rw_complete(wr, tr, NULL);
			rw_cleanup(wr, tr, first_time, false, __LINE__);
			as_rw_set_stat_counters(true, 0, tr);
			*delete = true;
			return (0);
		} else {
			cf_atomic_int_incr(&g_config.write_master);
		}

		udf_optype op = UDF_OPTYPE_NONE;
		/* Commit the write locally */
		if (is_delete) {
			rv = write_delete_local(tr, false, 0, ! g_config.generation_disable);
			WR_TRACK_INFO(wr, "internal_rw_start: delete local done ");
			cf_detail(AS_RW,
					"write_delete_local for digest returns %d, %d digest %"PRIx64"",
					rv, tr->result_code, *(uint64_t*)&tr->keyd);
		} else {
			// If the XDR is enabled and if the user configured to stop the writes if there is no XDR
			// and if the xdr digestpipe is not opened fail the writes with appropriate return value
			// We cannot do this check inside write_local() because that function is used for replica
			// writes as well. We do not want to stop the writes on replica if the master write succeeded.
			if ((g_config.xdr_cfg.xdr_global_enabled == true)
					&& (g_config.xdr_cfg.xdr_digestpipe_fd == -1)
					&& (tr->rsv.ns && tr->rsv.ns->enable_xdr == true)
					&& (g_config.xdr_cfg.xdr_stop_writes_noxdr == true)) {
				tr->result_code = AS_PROTO_RESULT_FAIL_NOXDR;
				cf_atomic_int_incr(&g_config.err_write_fail_noxdr);
				cf_debug(AS_RW,
						"internal_rw_start: XDR is enabled but XDR digest pipe is not open.");
				rv = -1;
			} else {
				// see if we have scripts to execute
				udf_call *call = cf_malloc(sizeof(udf_call));
				udf_call_init(call, tr);

				if (call->active) {
					wr->has_udf = true;
					if (tr->rsv.p->qnode != g_config.self_node) {
						cf_detail(AS_RW, "Applying UDF at the non qnode");
					}

					rv = udf_rw_local(call, wr, &op);

					if (UDF_OP_IS_DELETE(op)) {
						tr->msgp->msg.info2 |= AS_MSG_INFO2_DELETE;
						is_delete = true;
					} else if (UDF_OP_IS_READ(op) || op == UDF_OPTYPE_NONE) {
						// update stats to move from normal to uDF requests
						as_rw_update_stat(wr);
						// return early if the record was not updated
						udf_call_destroy(call);
						cf_free(call);
						call = NULL;
						if (udf_rw_needcomplete(tr)) {
							udf_rw_complete(tr, tr->result_code, __FILE__,
									__LINE__);
						}
						rw_cleanup(wr, tr, first_time, false, __LINE__);
						as_rw_set_stat_counters(true, rv, tr);
						*delete = true;
						return 0;
					}
				} else {
					wr->has_udf = false;
					cf_free(call);

					write_local_generation wlg;
					wlg.use_gen_check = false;
					wlg.use_gen_set = false;
					wlg.use_msg_gen = true;

					rv = write_local(tr, &wlg, &wr->pickled_buf,
							&wr->pickled_sz, &wr->pickled_rec_props,
							&wr->response_db);
					WR_TRACK_INFO(wr, "internal_rw_start: write local done ");
				}
				if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) {
					cf_detail(AS_RW,
							"[Digest %"PRIx64" Shipped OP] Finished Transaction ret = %d",
							*(uint64_t *)&tr->keyd, rv);
				}
			} // end, no XDR problems
		} // end, else this is a write.

		// from this function: -2 means retry, -1 means forever - like, gen mismatch or similar
		if (rv != 0) {
			if ((rv == -2) && (tr->proto_fd_h == 0) && (tr->proxy_msg == 0)) {
				cf_crash(AS_RW,
						"Can't retry a write if all data has been stripped out");
			}

			// If first time, the caller will free msgp and the partition or retry request
			if (!first_time) {
				if (RW_TR_WR_MISMATCH(tr, wr)) {
					cf_warning(AS_RW,
							"{%s:%d} as_write_start internal: Illegal state "
							"in write request after writing data %d %"PRIx64"",
							tr->rsv.ns->name, tr->rsv.pid, tr->rsv.n_dupl, tr->rsv.dupl_nodes[0]);
				}
				// If there is a permanent error and it is not the first time,
				// send data back to the client and don't leak a connection
				if (rv == -1) {
					MICROBENCHMARK_RESET_P();
					rw_complete(wr, tr, NULL);
					rw_cleanup(wr, tr, false, false, __LINE__);
					rv = 0;
				}
			}
			WR_TRACK_INFO(wr, "internal_rw_start: returning non-zero error ");
			as_rw_set_stat_counters(false, rv, tr);
			*delete = true;
			cf_detail(AS_RW, "UDF_%s %s:%d",
					UDF_OP_IS_LDT(op) ? "LDT" : "RECORD", __FILE__, __LINE__);
			return (rv);
		} else {
			cf_detail(AS_RW, "write succeeded");
		}

		/* Get the target replica set, which should exclude ourselves (but
		 * do a sanity check just to be sure) */
		// Also pick the qnode to ship data to, unless it is master
		bool qnode_found = true;
		cf_node nodes[AS_CLUSTER_SZ];
		memset(nodes, 0, sizeof(nodes));
		int node_sz = as_partition_getreplica_readall(tr->rsv.ns, tr->rsv.pid,
				nodes);
		for (uint i = 0; i < node_sz; i++) {
			if (nodes[i] == g_config.self_node)
				cf_crash(AS_RW,
						"target replica set contains ourselves");
			if (nodes[i] == tr->rsv.p->qnode)
				qnode_found = true;
		}
		// TODO: We could optimize by not sending writes to replicas that reject_writes
		// if qnode not in replica list && not master. Add it to
		// the list of node to ship writes to. Assert that current
		// node is master node. Writes should never happen from non
		// master node unless it is shipped op.

		// TODO: We are allowing write to go to non-master node prole here.
		// At non-master it will fail but it has already been written at master.
		// Ideally we should roll back.
		if ((g_config.self_node != tr->rsv.p->replica[0])
				&& (as_paxos_get_cluster_key() == tr->rsv.cluster_key)
				&& !(tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP)
				&& (tr->rsv.p->target == 0)) {
			cf_warning(AS_RW, "internal_rw_start called from non-master "
				"node %"PRIx64", with TRANSACTION_FLAG_SHIPPED_OP not set and without any cluster key "
				"mismatch too. Cluster key is %"PRIx64", cluster size = %d my id %"PRIx64"",
				g_config.self_node, tr->rsv.cluster_key, g_config.paxos->cluster_size , g_config.self_node);
			//PRINT_STACK();
		}

		if ((!qnode_found) && (tr->rsv.p->qnode != tr->rsv.p->replica[0])) {
			nodes[node_sz] = tr->rsv.p->qnode;
			node_sz++;
		}
		/* Short circuit for one-replica writes */
		if (0 == node_sz) {

			if (is_delete == false) { // only record true writes
				if (tr->microbenchmark_is_resolve) {
					MICROBENCHMARK_HIST_INSERT_P(wt_resolve_hist);
				} else {
					MICROBENCHMARK_HIST_INSERT_P(wt_internal_hist);
				}
			}
			if (tr->rsv.n_dupl > 0)
				cf_detail(AS_RW,
						"{%s:%d} internal_rw_start: COMPLETE %"PRIx64" %s result code %d",
						tr->rsv.ns->name, tr->rsv.pid, *(uint64_t *) & (wr->keyd), is_delete ? "DELETE" : "UPDATE", tr->result_code);
			else
				cf_detail(AS_RW,
						"{%s:%d} internal_rw_start: COMPLETE %"PRIx64" %s result code %d",
						tr->rsv.ns->name, tr->rsv.pid, *(uint64_t *) & (wr->keyd), is_delete ? "DELETE" : "UPDATE", tr->result_code);

			WR_TRACK_INFO(wr, "internal_rw_start: Short Circuit for Single Replica Writes");
			rw_complete(wr, tr, NULL);
			rw_cleanup(wr, tr, first_time, false, __LINE__);
			as_rw_set_stat_counters(false, 0, tr);
			*delete = true;
			cf_detail(AS_RW, "FINISH UDF_%s %s:%d",
					UDF_OP_IS_LDT(op) ? "LDT" : "RECORD", __FILE__, __LINE__);
			return (0);
		}

		wr->dest_sz = node_sz;
		memcpy(wr->dest_nodes, nodes, node_sz * sizeof(cf_node));
		int fabric_op = RW_OP_WRITE;
		if (UDF_OP_IS_LDT(op)) {
			fabric_op = RW_OP_MULTI;
		}

		if (write_request_setup(wr, tr, fabric_op)) {
			rw_cleanup(wr, tr, first_time, true, __LINE__);
			*delete = true;
			return (0);
		}
		if (UDF_OP_IS_LDT(op)) {
			cf_detail(AS_RW,
					"MULTI_OP: Sent Replication Request for LDT %"PRIx64"",
					*(uint64_t*)&wr->keyd);
		}
	} // if - we're in operation phase

	// if we wanted to fast-path response to client (must check we are not in duplicate resolution case)
	bool client_requested_fast_path = (TRANSACTION_COMMIT_LEVEL(tr) == AS_POLICY_COMMIT_LEVEL_MASTER);

	if ((g_config.respond_client_on_master_completion || g_config.replication_fire_and_forget || client_requested_fast_path)
		&& (tr->rsv.n_dupl == 0)) {

		wr->respond_client_on_master_completion =
			g_config.respond_client_on_master_completion || client_requested_fast_path;

		// start the replication, make sure real replication doesn't happen
		if (g_config.replication_fire_and_forget) {
			for (uint i = 0; i < wr->dest_sz; i++) {
				rw_replicate_async(wr->dest_nodes[i], wr->dest_msg);
				wr->dest_nodes[i] = 0;
				wr->dest_complete[i] = true;
				wr->dup_result_code[i] = 0;
			}
			wr->dest_sz = 0;
			wr->replication_fire_and_forget = true;
			wr->respond_client_on_master_completion = true;
		}

		// signal back to client
		cf_debug(AS_RW, "respond_client_on_master_completion");
		wr->respond_client_on_master_completion = true;
		rw_complete(wr, tr, NULL);
		tr->proto_fd_h = NULL;
		tr->proxy_msg = NULL;
		UREQ_DATA_RESET(&wr->udata);

		// if fire and forget, we're done, get out
		if (wr->replication_fire_and_forget) {
			rw_cleanup(wr, tr, first_time, false, __LINE__);
			as_rw_set_stat_counters(wr->is_read, 0, tr);
			*delete = true;
			return (0);
		} else {
			// Do not call rw_cleanup. Only response has been sent
			// in rw_complete. Replication, et al., still needs to
			// happen.
		}
	}

	// Set up the write request structure and enable it
	// steals everything from the transaction
	if (tr->proto_fd_h) {
		wr->proto_fd_h = tr->proto_fd_h;
		tr->proto_fd_h = 0;
	}
	wr->proxy_node = tr->proxy_node;
	wr->proxy_msg  = tr->proxy_msg;
	wr->shipped_op = (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP) ? true : false;

	UREQ_DATA_COPY(&wr->udata, &tr->udata);

	if (first_time == true) {
		as_partition_reservation_move(&wr->rsv, &tr->rsv);
		wr->rsv_valid = true;
		wr->msgp = tr->msgp;
		tr->msgp = 0;
		wr->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
		wr->retry_interval_ms = g_config.transaction_retry_ms;
		wr->start_time = tr->start_time;
		cf_atomic64_set(&(wr->end_time),
				(tr->end_time != 0) ? tr->end_time : wr->start_time + g_config.transaction_max_ns);
		wr->ready = true;
		WR_TRACK_INFO(wr, "internal_rw_start: first time - tr->wr ");
	}

	cf_debug(AS_RW, "write: sending request to %d nodes", wr->dest_sz);
#ifdef DEBUG_MSG
	msg_dump(wr->dest_msg, "rw start outoing msg");
#endif

	wr->microbenchmark_time = cf_getns();

	if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP)
		cf_detail(AS_RW, "[Digest %"PRIx64" Shipped OP] Replication Initiated",
				*(uint64_t *)&tr->keyd);

	send_messages(wr);

	if (wr->is_read == false) {
		if (!is_delete) {
			if (tr->microbenchmark_is_resolve) {
				MICROBENCHMARK_HIST_INSERT_P(wt_resolve_hist);
			} else {
				MICROBENCHMARK_HIST_INSERT_P(wt_internal_hist);
			}
		}
	} else {
		if (tr->microbenchmark_is_resolve) {
			MICROBENCHMARK_HIST_INSERT_P(rt_resolve_hist);
		} else {
			MICROBENCHMARK_HIST_INSERT_P(rt_internal_hist);
		}
	}

	return (0);
}

//  This is called from thr_tsvc when we start a transaction.
//  (through a helper function that determines whether we're doing reads or writes)
//
// Allocate a new transaction structure.
// Find all the nodes you need to send to. If you're the only replica, signal
// an immediate write and forget all the foolishness.
// Create the message you need to send, which is pretty much just the as_msg
// Create the retransmit structure and file it away
// send the message to all the read replicas
//
// -1 means "report error to requester"
// -2 means "try again"
// -3 means "duplicate proxy request, drop"
//
// tr->rsv - the reservations necessary to write. If 0 is returned, that
// reservation will be used/consumed here on out. In an error condition, the
// reservation will not be touched

int as_rw_start(as_transaction *tr, bool is_read) {
	int rv;
	as_namespace *ns = tr->rsv.ns;

	cf_assert(tr, AS_RW, CF_CRITICAL, "invalid transaction");
	cf_assert(tr->rsv.p, AS_RW, CF_CRITICAL, "invalid reservation");
	cf_assert(ns, AS_RW, CF_CRITICAL,
			"invalid reservation");
	cf_assert(ns->name, AS_RW, CF_CRITICAL,
			"invalid reservation");

	if (! is_read) {
		// If we're doing a "real" write, check that we aren't backed up.
		if ((tr->msgp->msg.info2 & AS_MSG_INFO2_DELETE) == 0 &&
				as_storage_overloaded(ns)) {
			tr->result_code = AS_PROTO_RESULT_FAIL_DEVICE_OVERLOAD;
			return -1;
		}

		if ((tr->msgp->msg.info1 & AS_MSG_INFO1_XDR)) {
			if (ns->ns_allow_xdr_writes == false) {
				tr->result_code = AS_PROTO_RESULT_FAIL_FORBIDDEN;
				cf_atomic_int_incr(&g_config.err_write_fail_forbidden);
				cf_debug(AS_RW, "Disallowing xdr write in namespace %s", ns->name);
				return -1;
			}
		} else {
			if (ns->ns_allow_nonxdr_writes == false) {
				tr->result_code = AS_PROTO_RESULT_FAIL_FORBIDDEN;
				cf_atomic_int_incr(&g_config.err_write_fail_forbidden);
				cf_debug(AS_RW, "Disallowing non-xdr write in namespace %s", ns->name);
				return -1;
			}
		}
	}

	write_request *wr = write_request_create();

	wr->is_read = is_read;

	wr->trans_complete = 0;
	wr->dupl_trans_complete = (tr->rsv.n_dupl > 0) ? 0 : 1;

	cf_debug_digest(AS_RW, &(tr->keyd), "[PROCESS KEY] {%s:%u} Self(%"PRIx64") Read(%d):",
			ns->name, tr->rsv.p->partition_id, g_config.self_node, is_read );

	wr->keyd = tr->keyd;

	// Transaction Consistency Guarantees:
	//   Use the client's requested guarantee level for this transaction
	//   unless the corresponding server's namespace override is enabled.
	wr->read_consistency_level = TRANSACTION_CONSISTENCY_LEVEL(tr);
	wr->write_commit_level = TRANSACTION_COMMIT_LEVEL(tr);

	wr->batch_shared = tr->batch_shared;
	wr->batch_index = tr->batch_index;

	// Fetching the write_request out of the hash table
	global_keyd gk;
	gk.ns_id = ns->id;
	gk.keyd = tr->keyd;

	cf_rc_reserve(wr); // need to keep an extra reference count in case it inserts
	rv = rchash_put_unique(g_write_hash, &gk, sizeof(gk), wr);
	if (rv == RCHASH_ERR_FOUND) {
		// could be a retransmit. Get the transaction that's there and compare
		// of course it might not be there anymore, but that's OK
		write_request *wr2;
		if (0 == rchash_get(g_write_hash, &gk, sizeof(gk), (void **) &wr2)) {
			pthread_mutex_lock(&wr2->lock);
			if ((wr2->ready == true) && (wr2->proxy_msg != 0)
					&& (wr2->proxy_node == tr->proxy_node)) {

				if (as_proxy_msg_compare(tr->proxy_msg, wr2->proxy_msg) == 0) {

					cf_debug(AS_RW,
							"proxy_write_start: duplicate, ignoring {%s:%d} %"PRIx64"",
							ns->name, tr->rsv.pid, *(uint64_t*)&tr->keyd);
					cf_rc_release(wr);
					WR_TRACK_INFO(wr, "as_rw_start: proxy - ignored");
					WR_RELEASE(wr);
					pthread_mutex_unlock(&wr2->lock);
					WR_RELEASE(wr2);
					cf_atomic_int_incr(&g_config.err_duplicate_proxy_request);
					return (-3);
				}
			}
			// don't allow this queue to back up. The algorithm is (currently) n2
			// because *all* wait queue elements are re-inserted into the queue, which
			// then get re-queued. HACK for now - making the algorithm non-n2, or
			// punting out expired transactions from this list, would be a very good thing
			// too
			if (wr2->wait_queue_head && g_config.transaction_pending_limit) {
				int wq_depth = 0;
				wreq_tr_element *e = wr2->wait_queue_head;
				while (e) {
					wq_depth++;
					e = e->next;
				}
				// allow a depth of 2 - only
				if (wq_depth > g_config.transaction_pending_limit) {
					cf_debug(AS_RW,
							"as_rw_start: pending limit, ignoring {%s:%d} %"PRIx64"",
							ns->name, tr->rsv.pid, *(uint64_t*)&tr->keyd);
					cf_rc_release(wr);
					WR_TRACK_INFO(wr, "as_rw_start: pending - limit");
					WR_RELEASE(wr);
					pthread_mutex_unlock(&wr2->lock);
					WR_RELEASE(wr2);
					cf_atomic_int_incr(&g_config.err_rw_pending_limit);
					tr->result_code = AS_PROTO_RESULT_FAIL_KEY_BUSY;
					return (-1);
				}
			}
			/*
			 * Stash this request away in a transaction structure so that we may later add this to the transaction queue
			 */
			// INIT_TR
			wreq_tr_element *e = cf_malloc( sizeof(wreq_tr_element) );
			if (!e)
				cf_crash(AS_RW, "cf_malloc");
			e->tr.incoming_cluster_key = tr->incoming_cluster_key;
			e->tr.start_time = tr->start_time;
			e->tr.end_time = tr->end_time;
			e->tr.proto_fd_h = tr->proto_fd_h;
			tr->proto_fd_h = 0;
			e->tr.proxy_node = tr->proxy_node;
			e->tr.proxy_msg = tr->proxy_msg;
			tr->proxy_msg = 0;
			e->tr.keyd = tr->keyd;
			AS_PARTITION_RESERVATION_INIT(e->tr.rsv);
			e->tr.result_code = AS_PROTO_RESULT_OK;
			e->tr.msgp = tr->msgp;
			e->tr.trid = tr->trid;
			tr->msgp = 0;
			e->tr.preprocessed = true;
			e->tr.flag = 0;
			UREQ_DATA_COPY(&e->tr.udata, &tr->udata);
			e->tr.batch_shared = tr->batch_shared;
			e->tr.batch_index = tr->batch_index;

			// add this transactions to the queue
			e->next = wr2->wait_queue_head;
			wr2->wait_queue_head = e;

			pthread_mutex_unlock(&wr2->lock);

			WR_TRACK_INFO(wr, "as_rw_start: add: wait queue");

			cf_atomic_int_incr(&g_config.n_waiting_transactions);

			cf_detail_digest(AS_RW, &tr->keyd,
					"as rw start:  write in progress QUEUEING returning 0 (%d:%p:%"PRIx64") wr(%p) %ld",
					tr->proto_fd_h ? tr->proto_fd_h->fd : 0, tr->proxy_msg, tr->proxy_node, wr2, tr->trid);

			as_partition_release(&tr->rsv);
			cf_atomic_int_decr(&g_config.rw_tree_count);

			WR_RELEASE(wr2);
			rv = 0;
		} else {
			rv = -2;
			cf_atomic_int_incr(&g_config.err_rw_request_not_found);
			WR_TRACK_INFO(wr, "as_rw_start: not found: return -2");
			cf_detail(AS_RW,
					"as rw start:  could not find request in hash table! returning -2 {%s.%d} (%d:%p) %"PRIx64"",
					ns->name, tr->rsv.pid, tr->proto_fd_h ? tr->proto_fd_h->fd : 0, tr->proxy_msg, *(uint64_t*)&tr->keyd);
		}
		cf_rc_release(wr);
		WR_TRACK_INFO(wr, "as_rw_start: 694");
		WR_RELEASE(wr);
		return (rv);
	} // end if wr found in write hash
	else if (rv != 0) {
		cf_info(AS_RW,
				"as_write_start:  unknown reason %d can't put unique? {%s.%d} (%d:%p) %"PRIx64"",
				rv, ns->name, tr->rsv.pid, tr->proto_fd_h ? tr->proto_fd_h->fd : 0, tr->proxy_msg, *(uint64_t*)&tr->keyd);
		WR_TRACK_INFO(wr, "as_rw_start: 701");
		udf_rw_complete(tr, -2, __FILE__, __LINE__);
		UREQ_DATA_RESET(&tr->udata);
		WR_RELEASE(wr);
		cf_atomic_int_incr(&g_config.err_rw_cant_put_unique);
		return (-2);
	}

	if (is_read) {
		cf_atomic_int_incr(&g_config.stat_read_reqs);
		if (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR) {
			cf_atomic_int_incr(&g_config.stat_read_reqs_xdr);
		}
	} else {
		cf_atomic_int_incr(&g_config.stat_write_reqs);
		if (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR) {
			cf_atomic_int_incr(&g_config.stat_write_reqs_xdr);
		}
	}

	cf_detail(AS_RW, "{%s:%d} as_rw_start: CREATING request %"PRIx64" %s",
			ns->name, tr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");

	// this is debug print code --- stash a copy of the ns name + pid allowing
	// printing after we've sent the wr on its way
	char str[6];
	memset(str, 0, 6);
	strncpy(str, ns->name, 5);
	int pid = tr->rsv.pid;

	pthread_mutex_lock(&wr->lock);

	bool must_delete = false;

	if (is_read) {
		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_start_hist);
	} else {
		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(wt_start_hist);
	}

	rv = internal_rw_start(tr, wr, &must_delete);
	pthread_mutex_unlock(&wr->lock);

	if (must_delete == true) {
		WR_TRACK_INFO(wr, "as_rw_start: deleting rchash");
		cf_detail(AS_RW, "{%s:%d} as_rw_start: DELETING request %"PRIx64" %s",
				str, pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");
		rchash_delete(g_write_hash, &gk, sizeof(gk));
	}

	WR_TRACK_INFO(wr, "as_rw_start: returning");
	WR_RELEASE(wr);

	return rv;
} // end as_rw_start()

int as_write_start(as_transaction *tr) {
	cf_assert(tr, AS_RW, CF_CRITICAL, "invalid transaction");
	cf_assert(tr->rsv.p, AS_RW, CF_CRITICAL, "invalid reservation");

	return as_rw_start(tr, false);
}

int as_read_start(as_transaction *tr) {
	cf_assert(tr, AS_RW, CF_CRITICAL, "invalid transaction");
	cf_assert(tr->rsv.p, AS_RW, CF_CRITICAL, "invalid reservation");

	// On read request if transaction repeatable read is set to false and there
	// are no duplicates ... do not do hard work of setting up write
	// request. The semantics is then of L0 read .. the data being
	// read is physically consistent copy.
	//
	// Just read the data and send it back ...
	//
	if ((g_config.transaction_repeatable_read == false)
		&& (tr->rsv.n_dupl == 0)
		&& (tr->msgp->msg.info1 & AS_MSG_INFO1_READ)
		&& (TRANSACTION_CONSISTENCY_LEVEL(tr) == AS_POLICY_CONSISTENCY_LEVEL_ONE)) {
		cf_atomic_int_incr(&g_config.stat_read_reqs);
		if (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR) {
			cf_atomic_int_incr(&g_config.stat_read_reqs_xdr);
		}

		rw_complete(NULL, tr, NULL);

		// This code does task of rw_cleanup ... like releasing
		// reservation cleaning up msgp etc ...
		// (Todo) consolidate duplicate code
		if (! tr->proxy_node) {
			cf_hist_track_insert_data_point(g_config.rt_hist, tr->start_time);
		}

		as_partition_release(&tr->rsv);
		cf_atomic_int_decr(&g_config.rw_tree_count);
		if (tr->msgp) {
			cf_free(tr->msgp);
			tr->msgp = 0;
		}
		as_rw_set_stat_counters(true, 0, tr);
		return 0;
	} else {
		return as_rw_start(tr, true);
	}
}

void rw_msg_get_ldt_dupinfo(as_record_merge_component *c, msg *m) {
	uint32_t info = 0;
	c->flag = AS_COMPONENT_FLAG_DUP;
	if (0 == msg_get_uint32(m, RW_FIELD_INFO, &info)) {
		if (info & RW_INFO_LDT_SUBREC) {
			c->flag |= AS_COMPONENT_FLAG_LDT_SUBREC;
			cf_warning(AS_RW, "Subrec Component Not Expected for migrate");
		}

		if (info & RW_INFO_LDT_PARENTREC) {
			c->flag |= AS_COMPONENT_FLAG_LDT_REC;
		}

		if (info & RW_INFO_LDT_DUMMY) {
			c->flag |= AS_COMPONENT_FLAG_LDT_DUMMY;
		}

		if (info & RW_INFO_LDT_ESR) {
			c->flag |= AS_COMPONENT_FLAG_LDT_ESR;
			cf_warning(AS_RW, "ESR Component Not Expected for migrate");
		}
		cf_detail(AS_LDT, "LDT info %d", c->flag);

	} else {
		cf_warning(AS_LDT,
				"Incomplete Migration information resorting to defaults !!!");
	}
	return;
}

int
finish_rw_process_prole_ack(write_request *wr, uint32_t result_code)
{
    cf_debug(AS_RW, "Prole Ack");
	if (wr->shipped_op) {
		cf_detail_digest(AS_RW, &wr->keyd, "SHIPPED_OP WINNER [Digest %"PRIx64"] Replication Done",
				*(uint64_t *)&wr->keyd);
	}

	// TODO:: We bail out if wr->msgp is not set. Use case
	// LSO_CHUNK write request does not set msgp. The above check of
	// is_subrecord should always catch it but being paranoid
	if (!wr->msgp) {
		return true;
	}

	cf_detail(AS_RW,
			"finish_rw_process_prole_ack: write operation phase complete, %"PRIx64,
			wr->keyd);

	if (wr->proxy_msg && cf_rc_count(wr->proxy_msg) == 0) {
		cf_warning(AS_RW,
				"rw transaction complete: proxy message but no reference count");
	}

	// INIT_TR
	as_transaction tr;
	write_request_init_tr(&tr, wr);

	if (wr->shipped_op) {
		tr.flag |= AS_TRANSACTION_FLAG_SHIPPED_OP;
	}

	cf_detail(AS_RW,
			"write process ack complete: fd %d result code %d, %"PRIx64"",
			wr->proto_fd_h ? wr->proto_fd_h->fd : 0, result_code, wr->keyd);

	// It is critical that the write complete must be done before
	// the wr is removed from the table. The table protects against
	// other write transactions on the same key, and we need to make sure
	// the response is built before another writer changes the value, in cases
	// where the transaction included read requests.

	tr.microbenchmark_time = wr->microbenchmark_time;
	MICROBENCHMARK_HIST_INSERT_AND_RESET(wt_master_wait_prole_hist);

	tr.microbenchmark_is_resolve = false;

	if (!wr->respond_client_on_master_completion) {
		rw_complete(wr, &tr, NULL);
		rw_cleanup(wr, &tr, false, false, __LINE__);
	}

	as_rw_set_stat_counters(false, 0, &tr);
	if (wr->rsv.n_dupl > 0)
		cf_detail(AS_RW,
				"{%s:%d} finish_rw_process_ack: COMPLETE %"PRIx64" %s result code %d",
				tr.rsv.ns->name, tr.rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE", tr.result_code);
	else
		cf_detail(AS_RW,
				"{%s:%d} finish_rw_process_ack: COMPLETE %"PRIx64" %s result code %d",
				tr.rsv.ns->name, tr.rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE", tr.result_code);

	/* If the read following the write was proxied, tr.proto
	 * will have been cleared for us; reflect that change
	 * into the write request structure so that the msgp isn't
	 * freed twice */
	if (0L == tr.msgp)
		wr->msgp = 0;

	WR_TRACK_INFO(wr, "finish_rw_process_ack: compeleted write_prole phase");
	return (true);
}

bool
finish_rw_process_dup_ack(write_request *wr)
{
	cf_detail(AS_RW, "finish rw process dup ack: duplicate phase %"PRIx64"",
			wr->keyd);

	int comp_sz = 0;
	as_record_merge_component components[wr->dest_sz];
	memset(&components, 0, sizeof(components));
	for (int i = 0; i < wr->dest_sz; i++) {

		// benign - we don't send to nodes that have vanished
		if (wr->dup_msg[i] == 0) {
			cf_debug(AS_RW,
					"finish_rw_process_dup_ack: no dup msg in slot %d, early completion due to cluster change",
					i);
			continue;
		}

		msg *m = wr->dup_msg[i];

		uint32_t result_code = -1;
		if (0 != msg_get_uint32(m, RW_FIELD_RESULT, &result_code)) {
			cf_warning(AS_RW,
					"finish_rw_process_dup_ack: received message without result field");
			continue;
		}
		if (result_code != 0) {
			cf_debug(AS_RW,
					"finish_rw_process_dup_ack: result code %d digest %"PRIx64"",
					result_code, wr->keyd);
			continue;
		}
		if (wr->rsv.ns->ldt_enabled) {
			rw_msg_get_ldt_dupinfo(&components[comp_sz], m);
		}
		// take the different components for passing to merge
		uint8_t *buf = 0;
		size_t buf_sz = 0;

		// Note - older versions expect RW_FIELD_VINFOSET field here.

		uint32_t generation = 0;
		if (0 != msg_get_uint32(m, RW_FIELD_GENERATION, &generation)) {
			cf_info(AS_RW,
					"finish_rw_process_dup_ack: received dup-response with no generation, %"PRIx64"",
					wr->keyd);
			continue;
		}
		components[comp_sz].generation = generation;

		uint32_t void_time = 0;
		if (0 != msg_get_uint32(m, RW_FIELD_VOID_TIME, &void_time)) {
			cf_info(AS_RW,
					"finish_rw_process_dup_ack: received dup-response with no void_time, %"PRIx64"",
					wr->keyd);
		}
		components[comp_sz].void_time = void_time;

		if (!COMPONENT_IS_LDT(&components[comp_sz])) {
			if (0 != msg_get_buf(m, RW_FIELD_RECORD, &buf, &buf_sz,
						MSG_GET_DIRECT)) {
				cf_info(AS_RW,
						"finish_rw_process_dup_ack: received dup-response with no data (ok for deleted?), %"PRIx64"",
						wr->keyd);
				continue;
			}
			components[comp_sz].record_buf = buf;
			components[comp_sz].record_buf_sz = buf_sz;

			// and the metadata, if it's there and we're allowed to use it
			buf = NULL;
			buf_sz = 0;
			if (0 == msg_get_buf(m, RW_FIELD_REC_PROPS, &buf, &buf_sz,
						MSG_GET_DIRECT) && buf && buf_sz) {
				cf_debug(AS_RW,
						"finish_rw_process_dup_ack: received message with record properties");
				components[comp_sz].rec_props.p_data = buf;
				components[comp_sz].rec_props.size = buf_sz;
			} else {
				cf_debug(AS_RW, "finish_rw_process_dup_ack: received message without record properties");
			}
			cf_detail(AS_RW, "NON LDT COMPONENT");
		} else {
			cf_detail(AS_RW, "LDT COMPONENT");
		}
		comp_sz++;
	}

	cf_detail(AS_RW, "finish_rw_process_dup_ack: comp_sz %d, %"PRIx64"",
			comp_sz, wr->keyd);
	// updates the local in-memory representation
	int rv         = 0;
	wr->shipped_op = false;
	int winner_idx = -1;
	if (comp_sz > 0) {
		rv = as_record_flatten(&wr->rsv, &wr->keyd, comp_sz, components,
				&winner_idx);
	}

	// Free up the dup messages
	for (uint i = 0; i < wr->dest_sz; i++) {
		if (wr->dup_msg[i]) {
			as_fabric_msg_put(wr->dup_msg[i]);
			wr->dup_msg[i] = 0;
		}
	}

	// In case remote node wins after resolution and has bin call returns
	if (rv == -2) {
		if (winner_idx < 0) {
			cf_warning(AS_LDT, "Unexpected winner @ index %d.. resorting to 0", winner_idx);
			winner_idx = 0;
		}
		cf_detail_digest(AS_RW, &wr->keyd,
				"SHIPPED_OP %s Shipping %s op to %"PRIx64"",
				wr->proxy_msg ? "NONORIG" : "ORIG",
				wr->is_read ? "Read" : "Write",
				wr->dest_nodes[winner_idx]);
		as_ldt_shipop(wr, wr->dest_nodes[winner_idx]);
		// Assume OK for now with respect to stats
		as_rw_set_stat_counters(true, 0, 0);
		return false;
	}

	cf_detail_digest(AS_RW, &wr->keyd,
			"SHIPPED_OP %s=WINNER locally apply %s op after "
			"flatten @ %"PRIx64"",
			wr->proxy_msg ? "NONORIG" : "ORIG",
			wr->is_read ? "Read" : "Write",
			g_config.self_node);

	// move to next phase after duplicate merge
	wr->dest_sz = 0;

	// INIT_TR
	as_transaction tr;
	write_request_init_tr(&tr, wr);
	WR_TRACK_INFO(wr, "finish_rw_process_dup_ack: compeleted duplicate phase");
	if (wr->is_read) {
		MICROBENCHMARK_HIST_INSERT_AND_RESET(rt_resolve_wait_hist);
	} else {
		MICROBENCHMARK_HIST_INSERT_AND_RESET(wt_resolve_wait_hist);
	}

	MICROBENCHMARK_RESET();
	tr.microbenchmark_is_resolve = true;

	bool must_delete = true;
	rv = internal_rw_start(&tr, wr, &must_delete);
	if (rv != 0) {
		// This is second time call... internal_rw_start does need full in those cases.
		// Simply return
		cf_info(AS_RW,
				"internal rw start returns error %d. No data will be sent to client. Possible resource leak.",
				rv);
	}
	return must_delete;
}

//
// return code: false if transaction not complete yet
//   true if it is
//
bool
finish_rw_process_ack(write_request *wr, uint32_t result_code)
{
	for (uint32_t node_id = 0; node_id < wr->dest_sz; node_id++) {
		if (wr->dest_complete[node_id] == false) {
			return false;
		}
	}
	// Figure out the ack is coming for which request type.
	//   - If dupl_trans_complete is 0 then it is duplicate resolution ack
	//   - Else is it prole ack
	//
	// If so, use the atomic to make sure only one response does finish
	// processing for duplicate resolution.
	if (wr->dupl_trans_complete == 0) { // in duplicate phase
		if (1 == cf_atomic32_incr(&wr->dupl_trans_complete)) {
			return finish_rw_process_dup_ack(wr);
		}
	} else if (1 == cf_atomic32_incr(&wr->trans_complete)) {
		return finish_rw_process_prole_ack(wr, result_code);
	}
	return (false);
}

void
rw_process_cluster_key_mismatch(write_request *wr, global_keyd *gk)
{
	cf_debug(AS_RW,
			"{%s:%d} rw_process_cluster_key_mismatch: CLUSTER KEY MISMATCH rsp %"PRIx64" %s",
			wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");
	bool must_delete = false;
	pthread_mutex_lock(&wr->lock);
	if (wr->dupl_trans_complete == 0) {
		if (1 == cf_atomic32_incr(&wr->dupl_trans_complete)) {
			cf_atomic32_incr(&wr->trans_complete);
			// also complete the next transaction. we are bailing out
			// INIT_TR
			as_transaction tr;
			write_request_init_tr(&tr, wr);
			MICROBENCHMARK_RESET();

			// In order to re-write the prole, we actually REDO the transaction
			// by re-queuing it and doing it ALL over again.
			cf_atomic_int_incr(&g_config.stat_cluster_key_err_ack_dup_trans_reenqueue);

			cf_debug_digest(AS_RW, &(wr->keyd), "[RE-ENQUEUE JOB from CK ERR ACK:1] TrID(0) SelfNode(%"PRIx64")",
					g_config.self_node );
			if (0 != thr_tsvc_enqueue(&tr)) {
				cf_warning(AS_RW, "queue rw_process_cluster_key_mismatch failure");
				cf_free(wr->msgp);
			}
			wr->msgp = 0;
			WR_TRACK_INFO(wr, "rw_process_cluster_key_mismatch: cluster key mismatch deleting - duplicate ");
			must_delete = true;
		}
	} else if (1 == cf_atomic32_incr(&wr->trans_complete)) {
		// INIT_TR
		as_transaction tr;
		write_request_init_tr(&tr, wr);
		MICROBENCHMARK_RESET();

		cf_atomic_int_incr(&g_config.stat_cluster_key_err_ack_rw_trans_reenqueue);

		cf_debug_digest(AS_RW, &(wr->keyd), "[RE-ENQUEUE JOB from CK ERR ACK:2] TrID(0) SelfNode(%"PRIx64")",
				g_config.self_node );

		if (0 != thr_tsvc_enqueue(&tr)) {
			cf_warning(AS_RW, "queue rw_process_cluster_key_mismatch failure");
			cf_free(wr->msgp);
		}
		wr->msgp = 0; // NULL this out so that the write_destructor does not free this pointer.
		WR_TRACK_INFO(wr, "rw_process_cluster_key_mismatch: cluster key mismatch deleting - final ");
		must_delete = true;
	}
	pthread_mutex_unlock(&wr->lock);
	if (must_delete) {
		rchash_delete(g_write_hash, gk, sizeof(global_keyd));
	}
}
//
// Read-Write Prole message Acknowledge code path. Either is response to prole write
// request, or is dup (if !is_write)
//
void
rw_process_ack(cf_node node, msg *m, bool is_write)
{
	cf_detail(AS_RW, "rw process ack: from %"PRIx64, node);

	uint32_t ns_id;
	if (0 != msg_get_uint32(m, RW_FIELD_NS_ID, &ns_id)) {
		cf_info(AS_RW, "rw process ack: no namespace");
#ifdef DEBUG_MSG
		msg_dump(m, "rw incoming no nsid");
#endif
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_ack_internal);
		return;
	}

	uint32_t tid;
	if (0 != msg_get_uint32(m, RW_FIELD_TID, &tid)) {
		cf_info(AS_RW, "rw process ack: no tid");
#ifdef DEBUG_MSG
		msg_dump(m, "rw incoming no tid");
#endif
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_ack_internal);
		return;
	}

	uint32_t result_code;
	if (0 != msg_get_uint32(m, RW_FIELD_RESULT, &result_code)) {
		cf_info(AS_RW, "rw process ack: no result_code");
#ifdef DEBUG_MSG
		msg_dump(m, "rw incoming noresult");
#endif
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_ack_internal);
		return;
	}

	cf_digest * keyd = NULL;
	size_t sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_DIGEST, (byte **) &keyd, &sz,
			MSG_GET_DIRECT)) {
		cf_info(AS_RW, "rw process ack: no digest");
#ifdef DEBUG_MSG
		msg_dump(m, "rw incoming nodigest");
#endif
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_ack_internal);
		return;
	}

	// look up the digest & namespace in the write hash
	global_keyd gk;
	gk.ns_id = ns_id;
	gk.keyd = *keyd;
	write_request *wr;
	if (RCHASH_OK != rchash_get(g_write_hash, &gk, sizeof(gk), (void **) &wr)) {
		cf_debug(AS_RW, "rw_process_ack: pending transaction, drop");
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_ack_nomatch);
		return;
	}

	if (wr->tid != tid) {
		cf_debug(AS_RW, "rw process ack: retransmit ack after we moved on");
#ifdef DEBUG_MSG
		msg_dump(m, "rw tid mismatch");
#endif
		as_fabric_msg_put(m);

		WR_TRACK_INFO(wr, "w_process_ack: tid mismatch");
		WR_RELEASE(wr);
		cf_atomic_int_incr(&g_config.rw_err_ack_nomatch);
		return;
	}

	if (wr->ready == false) {
		cf_warning(AS_RW,
				"write process ack: write request not 'ready': investigate! fd %d",
				wr->proto_fd_h ? wr->proto_fd_h->fd : 0);
		cf_atomic_int_incr(&g_config.rw_err_ack_internal);
		goto Out;
	}

	if ((wr->dupl_trans_complete == 0) && is_write) {
		cf_warning(AS_RW,
				"rw process ack: dupl not complete, but received write ack: not legal (retransmit?) investigate! fd %d",
				wr->proto_fd_h ? wr->proto_fd_h->fd : 0);
		cf_atomic_int_incr(&g_config.rw_err_ack_internal);
		goto Out;
	}

	WR_TRACK_INFO(wr, "rw_process_ack: entering");

	if (result_code == AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH) {
		rw_process_cluster_key_mismatch(wr, &gk);
	}
	else if (result_code != AS_PROTO_RESULT_OK) {
		cf_debug_digest(AS_RW, "{%s:%d} rw_process_ack: Processing unexpected response(%d):",
				wr->rsv.ns->name, wr->rsv.pid, result_code );
	}

	cf_debug(AS_RW, "{%s:%d} rw_process_ack: Processing response %"PRIx64" %s",
			wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");

	pthread_mutex_lock(&wr->lock);

	// 2. Now check to see if all are complete. If not wait for all messages
	// to arrive
	uint32_t node_id;
	for (node_id = 0; node_id < wr->dest_sz; node_id++) {
		if (wr->tid != tid) {
			cf_debug(AS_RW, "rw process ack: retransmit after we moved on");
			cf_atomic_int_incr(&g_config.rw_err_ack_nomatch);
			break;
		}
		if (node == wr->dest_nodes[node_id]) {
			if (wr->dest_complete[node_id] == false) {
				wr->dest_complete[node_id] = true;
				// Handle the case for the duplicate merge
				if (is_write == false) { // duplicate-phase messages are arriving
					wr->dup_result_code[node_id] = result_code;
					if (wr->dup_msg[node_id] != 0) {
						cf_debug(AS_RW,
								"{%s:%d} dup process ack: received duplicate response from node %"PRIx64"",
								wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *)(&wr->keyd));
					} else {
						wr->dup_msg[node_id] = m;
						m = 0;
						cf_detail(AS_RW,
								"{%s:%d} write process ack: received response from node %"PRIx64"",
								wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *)(&wr->keyd));
					}
				}
			} else
				cf_debug(AS_RW,
						"{%s:%d} write process ack: Ignoring duplicate response for read result code %d",
						wr->rsv.ns->name, wr->rsv.pid, result_code);
			break;
		}
	}
	// received a message from a node that was unexpected -
	if (node_id == wr->dest_sz) {
		cf_debug(AS_RW,
				"rw process ack: received ack from node %"PRIx64" not in transmit list, ignoring",
				node);
		pthread_mutex_unlock(&wr->lock);
		cf_atomic_int_incr(&g_config.rw_err_ack_badnode);
		goto Out;
	}

	// 3. We now know this node's write/read is complete. Finish processing
	WR_TRACK_INFO(wr, "finish_rw_process_ack: entering");
	bool finished = finish_rw_process_ack(wr, AS_PROTO_RESULT_OK);
	pthread_mutex_unlock(&wr->lock);

	if (finished) {
		cf_detail(AS_RW,
				"{%s:%d} write process ack: DELETING request %"PRIx64" %s",
				wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");

		WR_TRACK_INFO(wr, "rw_process_ack: deleting rchash");
		rchash_delete(g_write_hash, &gk, sizeof(gk));
	}

Out:
	if (m)
		as_fabric_msg_put(m);

	WR_TRACK_INFO(wr, "rw_process_ack: returning");
	WR_RELEASE(wr);

} // end rw_process_ack()

void
ops_complete(as_transaction *tr, cf_dyn_buf *db)
{
	if (tr->proto_fd_h) {
		if (0 != as_msg_send_ops_reply(tr->proto_fd_h, db)) {
			cf_warning(AS_RW, "can't send ops reply, fd %d",
					tr->proto_fd_h->fd);
		}

		tr->proto_fd_h = 0;
	}
	else if (tr->proxy_msg) {
		as_proxy_send_ops_response(tr->proxy_node, tr->proxy_msg, db);
		tr->proxy_msg = 0;
	}
	else {
		cf_warning(AS_RW, "ops_complete with no proto_fd_h or proxy_msg");
	}
}

void
write_complete(write_request *wr, as_transaction *tr)
{
	if (wr->response_db.buf) {
		ops_complete(tr, &wr->response_db);

		MICROBENCHMARK_HIST_INSERT_P(wt_net_hist);

		return;
	}

	if (udf_rw_needcomplete(tr)) {
		udf_rw_complete(tr, tr->result_code, __FILE__, __LINE__);
	}
	else if (tr->proto_fd_h) {
		if (0 != as_msg_send_reply(tr->proto_fd_h, tr->result_code,
				tr->generation, tr->void_time, NULL, NULL, 0, NULL, NULL,
				tr->trid, NULL)) {
			cf_warning(AS_RW, "can't send reply to client, fd %d",
					tr->proto_fd_h->fd);
		}

		tr->proto_fd_h = 0;
	}
	else if (tr->proxy_msg) {
		as_proxy_send_response(tr->proxy_node, tr->proxy_msg, tr->result_code,
				0, 0, NULL, NULL, 0, NULL, tr->trid, NULL);
	}
	// else something is really wrong ...

	MICROBENCHMARK_HIST_INSERT_P(wt_net_hist);
}

/*
 * Complete the request. Respond back to the requester, which is either
 * client or proxy source node. Also free the proto. This is done to avoid
 * any futher communication.
 */
void
rw_complete(write_request *wr, as_transaction *tr, as_index_ref *r_ref)
{
	as_msg *m = &tr->msgp->msg;

	if (m->info2 & AS_MSG_INFO2_WRITE) {
		write_complete(wr, tr);
	}
	else {
		read_local(tr, r_ref);

		if (wr && (m->info1 & AS_MSG_INFO1_BATCH)) {
			wr->msgp = NULL;
		}
	}

	if (tr->proto_fd_h != 0) {
		AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
		tr->proto_fd_h = 0;
	}
}

cf_queue *g_rw_dup_q;
#define DUP_THREAD_MAX 16
pthread_t g_rw_dup_th[DUP_THREAD_MAX];

typedef struct {
	cf_node node;
	msg *m;
} dup_element;

void
rw_dup_prole(cf_node node, msg *m)
{
	uint32_t rv = 1;
	int result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;

	cf_digest *keyd;
	size_t sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_DIGEST, (byte **) &keyd, &sz,
			MSG_GET_DIRECT)) {
		cf_info(AS_RW, "dup process received message without digest");
		cf_atomic_int_incr(&g_config.rw_err_dup_internal);
		goto Out1;
	}

	uint64_t cluster_key;
	if (0 != msg_get_uint64(m, RW_FIELD_CLUSTER_KEY, &cluster_key)) {
		cf_warning(AS_RW, "dup process received message without cluster key");
		cf_atomic_int_incr(&g_config.rw_err_dup_internal);
		goto Out1;
	}

	uint8_t *ns_name = 0;
	size_t ns_name_len;
	if (0 != msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT)) {
		cf_warning(AS_RW, "dup process received message without namespace");
		cf_atomic_int_incr(&g_config.rw_err_dup_internal);
		goto Out1;
	}
	as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);
	if (!ns) {
		cf_info(AS_RW, "get abort invalid namespace received");
		cf_atomic_int_incr(&g_config.rw_err_dup_internal);
		goto Out1;
	}

	// NB need to use the _migrate variant here so we can write into desync
	as_partition_reservation rsv;
	AS_PARTITION_RESERVATION_INIT(rsv);
	as_partition_reserve_migrate(ns, as_partition_getid(*keyd), &rsv, 0);
	cf_atomic_int_incr(&g_config.dup_tree_count);
	ns = 0;

	if (rsv.cluster_key != cluster_key) {
		cf_debug(AS_RW, "{%s:%d} write process: CLUSTER KEY MISMATCH %"PRIx64,
				rsv.ns->name, rsv.pid, *(uint64_t *)keyd);
		result_code = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
		cf_atomic_int_incr(&g_config.rw_err_dup_cluster_key);
		goto Out2;
	}

	// get record
	as_index_ref r_ref;
	r_ref.skip_lock = false;
	rv = as_record_get(rsv.tree, keyd, &r_ref, rsv.ns);
	if (rv != 0) {
		result_code = AS_PROTO_RESULT_FAIL_NOTFOUND;
		cf_debug_digest(AS_RW, keyd, "[REC NOT FOUND:1]<rw_dup_prole()>PID(%u) Pstate(%d):",
				rsv.pid, rsv.p->state );
		goto Out2;
	}
	as_index *r = r_ref.r;
	uint32_t info = 0;

	if (rsv.ns->ldt_enabled && as_ldt_record_is_parent(r)) {
		// NB: We search only on main tree in the code here because
		// duplicate resolution request is always for the LDT_REC.
		info |= RW_INFO_LDT_PARENTREC;
		info |= RW_INFO_LDT_DUMMY;
		// If LDT record make it run on the winner node
		cf_detail(AS_RW, "LDT_DUP: Duplicate Record IS LDT return LDT_DUMMY");
	} else if (rsv.ns->ldt_enabled && as_ldt_record_is_sub(r)) {
		cf_warning(AS_RW, "Invalid duplicate request ... for ldt sub received");
		result_code = AS_PROTO_RESULT_FAIL_NOTFOUND;
		goto Out3;
	} else {
		uint8_t *buf;
		size_t buf_len;
		as_storage_rd rd;

		if (0 != as_storage_record_open(rsv.ns, r, &rd, keyd)) {
			cf_debug(AS_RW, "pickle: couldn't open record");
			msg_set_unset(m, RW_FIELD_VINFOSET);
			cf_atomic_int_incr(&g_config.rw_err_dup_internal);
			goto Out3;
		}

		rd.n_bins = as_bin_get_n_bins(r, &rd);
		as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];
		rd.bins = as_bin_get_all(r, &rd, stack_bins);

		if (0 != as_record_pickle(r, &rd, &buf, &buf_len)) {
			cf_info(AS_RW, "pickle: could not allocate memory");
			msg_set_unset(m, RW_FIELD_VINFOSET);
			cf_atomic_int_incr(&g_config.rw_err_dup_internal);
			as_storage_record_close(r, &rd);
			goto Out3;
		}

		as_storage_record_get_key(&rd);

		size_t  rec_props_data_size = as_storage_record_rec_props_size(&rd);
		uint8_t rec_props_data[rec_props_data_size];

		if (rec_props_data_size > 0) {
			as_storage_record_set_rec_props(&rd, rec_props_data);
			msg_set_buf(m, RW_FIELD_REC_PROPS, rd.rec_props.p_data,
					rd.rec_props.size, MSG_SET_COPY);
			// TODO - better to use as_storage_record_copy_rec_props() and
			// MSG_SET_HANDOFF_MALLOC?
		}

		as_storage_record_close(r, &rd);

		info |= RW_INFO_MIGRATION;

		msg_set_buf(m, RW_FIELD_RECORD, (void *) buf, buf_len,
				MSG_SET_HANDOFF_MALLOC);
	}

	/* Indicate it is a duplicate resolution / migration  write */
	msg_set_uint32(m, RW_FIELD_INFO, info);

	// Note - older versions expect RW_FIELD_VINFOSET, so must send it for now.
	static const uint8_t VINFO_BUF_0[] = { 0, 0, 0, 0 };
	msg_set_buf(m, RW_FIELD_VINFOSET, VINFO_BUF_0, 4, MSG_SET_COPY);

	msg_set_uint32(m, RW_FIELD_GENERATION, r->generation);
	msg_set_uint32(m, RW_FIELD_VOID_TIME, r->void_time);

	result_code = AS_PROTO_RESULT_OK;

Out3:
	as_record_done(&r_ref, rsv.ns);
	r = 0;

Out2:
	as_partition_release(&rsv);
	cf_atomic_int_decr(&g_config.dup_tree_count);

Out1:
	msg_set_uint32(m, RW_FIELD_OP, RW_OP_DUP_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result_code);
	msg_set_unset(m, RW_FIELD_NAMESPACE);

	int rv2 = as_fabric_send(node, m, AS_FABRIC_PRIORITY_HIGH);
	if (rv2 != 0) {
		cf_debug(AS_RW, "write process: send fabric message bad return %d",
				rv2);
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_dup_send);
	}
}

int
rw_dup_process(cf_node node, msg *m)
{
	cf_atomic_int_incr(&g_config.read_dup_prole);
	if (g_config.n_transaction_duplicate_threads == 0) {
		rw_dup_prole(node, m);
		return (0);
	}

	// if could result in an IO, don't want to run too long on the fabric
	// thread - queue it
	dup_element e;
	e.node = node;
	e.m = m;
	if (0 != cf_queue_push(g_rw_dup_q, &e)) {
		cf_warning(AS_RW, "dup process: could not queue");
		as_fabric_msg_put(m);
	}
	return (0);
}

void *
rw_dup_worker_fn(void *yeah_yeah_yeah) {

	for (;;) {

		dup_element e;

		if (0 != cf_queue_pop(g_rw_dup_q, &e, CF_QUEUE_FOREVER)) {
			cf_crash(AS_RW, "unable to pop from dup work queue");
		}

		cf_detail(AS_RW,
				"dup_process: prole received request message from %"PRIx64,
				e.node);
#ifdef DEBUG_MSG
		msg_dump(m, "rw incoming dup");
#endif

		rw_dup_prole(e.node, e.m);

	}
	return (0);
}

int rw_dup_init() {
	if (g_config.n_transaction_duplicate_threads > 0) {

		g_rw_dup_q = cf_queue_create(sizeof(dup_element), true);
		if (!g_rw_dup_q)
			return (-1);

		if (g_config.n_transaction_duplicate_threads > DUP_THREAD_MAX) {
			cf_warning(AS_RW,
					"configured duplicate threads %d: reducing to maximum of %d",
					g_config.n_transaction_duplicate_threads, DUP_THREAD_MAX);
			g_config.n_transaction_duplicate_threads = DUP_THREAD_MAX;
		}

		for (int i = 0; i < g_config.n_transaction_duplicate_threads; i++) {
			if (0 != pthread_create(&g_rw_dup_th[i], 0, rw_dup_worker_fn, 0)) {
				cf_crash(AS_RW,
						"can't create worker threads for duplicate resolution");
			}
		}
	}
	return (0);
}



// If the replication request is coming from partition version which
// is different then
// - For LDT parent record
//
//    - Skip replication unless there is incoming migration from replication source
//      node and replication is in the RECORD mode.
//
//    - If not migrating it will either be happening in future in that case replicating
//      parent before subrec is order violation or would have been already migrated
//      in this case replication partition would match.
//
// - For LDT Subrec
//    - If Vs == Vd then write subrecord with the ldt version on the destination.
//    - if Vs != Vd then write the subrecord with the version at the source.
//
//  Look in function as_ldt_set_prole_subrec_version
//
// Note that the replication can only come from the winning node (it could either be
// master/designate master or non-master node). So we need not track all the incoming
// migration states only current would do. If the data is coming from the node which
// is _NOT_ migrating it is ok to overwrite the parent record without generation check.
int
as_ldt_check_and_get_prole_version(cf_digest *keyd, as_partition_reservation *rsv,
			ldt_prole_info *linfo, uint32_t info, as_storage_rd *rd, bool is_create, char *fname, int lineno)
{
	if (rsv->ns->ldt_enabled) {
		bool is_ldt_parent = (info & RW_INFO_LDT_PARENTREC);

		if (is_ldt_parent) {
			if (linfo->replication_partition_version_match) {
				linfo->ldt_prole_version = 0;
				// If parent record does not exist. In that case for source version itself
				// is used @ prole
				int rv = -1;
				if (rd && !is_create) {
					rv = as_ldt_parent_storage_get_version(rd, &linfo->ldt_prole_version, true ,__FILE__, __LINE__);
					if (0 == rv) {
						linfo->ldt_prole_version_set = true;
					}
				}
				if (rv) {
					linfo->ldt_prole_version_set = false;
					cf_detail(AS_RW, "prole version not set because %p==NULL or %d != 0", rd, rv);
				}
			} else {
				if (as_migrate_is_incoming(keyd, linfo->ldt_source_version, rsv->p->partition_id, AS_MIGRATE_RX_STATE_RECORD)) {
					cf_detail_digest(AS_RW, keyd, "MULTI_OP(%s:%d): Write Parent Record in Partition %d with version with %ld version [source:%ld prole:%ld]",
							fname, lineno, rsv->p->partition_id,
							linfo->replication_partition_version_match ? linfo->ldt_prole_version:linfo->ldt_source_version,
							linfo->ldt_source_version, linfo->ldt_prole_version);
				} else {
					cf_detail_digest(AS_RW, keyd, "MULTI_OP(%s:%d): Skip Write of Parent Record in Partition %d with version with %ld version [source:%ld prole:%ld]",
						fname, lineno, rsv->p->partition_id,
						linfo->replication_partition_version_match ? linfo->ldt_prole_version:linfo->ldt_source_version,
						linfo->ldt_source_version, linfo->ldt_prole_version);

					// Should bail out way earlier than this
					goto Out;
				}
			}
		}
		cf_detail_digest(AS_RW, keyd, "MULTI_OP(%s:%d): Write %s %sRecord in Partition %d with %s Partition Version [create:%d source mig:%ld prole:%ld]",
				fname, lineno, (info & RW_INFO_LDT_ESR) ? "ESR" : "",  (is_ldt_parent) ? "" : "Sub", rsv->p->partition_id,
				linfo->replication_partition_version_match ? "Matching" : "Non Matching",
				is_create, linfo->ldt_source_version, linfo->ldt_prole_version);
	}
	return 0;
Out:
	return -1;
}

int
as_ldt_set_prole_subrec_version(cf_digest *keyd, as_partition_reservation *rsv,
			const ldt_prole_info *linfo, uint32_t info)
{
	if (rsv->ns->ldt_enabled) {
		bool is_subrec = ((info & RW_INFO_LDT_SUBREC) || (info & RW_INFO_LDT_ESR));
		int type = 0;
		if (is_subrec) {
			if (linfo->replication_partition_version_match) {
				if (linfo->ldt_prole_version_set) {
					// ldt_version should be set
					cf_debug_digest(AS_RW, keyd, "Has Set %d type Version %ld from %ld for %s", type, linfo->ldt_prole_version, as_ldt_subdigest_getversion(keyd), (info & RW_INFO_LDT_ESR) ? "esr" : "subrec");
					as_ldt_subdigest_setversion(keyd, linfo->ldt_prole_version);
					cf_detail_digest(AS_RW, keyd, "Set Prole Version %ld", linfo->ldt_prole_version);
					type = 1;
				} else {
					cf_debug_digest(AS_RW, keyd, "No Parent record setting source version for subrecord");
					type = 2;
				}
			} else if (linfo->ldt_source_version_set) {
				// ldt_version should be set as source
				cf_debug_digest(AS_RW, keyd, "Has Set %d type Version %ld from %ld for %s", type, linfo->ldt_source_version, as_ldt_subdigest_getversion(keyd), (info & RW_INFO_LDT_ESR) ? "esr" : "subrec");
				as_ldt_subdigest_setversion(keyd, linfo->ldt_source_version);
			} else {
				cf_debug_digest(AS_RW, keyd, "Has skipped %d type Version %ld from %ld for %s", type, linfo->ldt_source_version, as_ldt_subdigest_getversion(keyd), (info & RW_INFO_LDT_ESR) ? "esr" : "subrec");
			}
		}
	}
	return 0;
}
//
// Case where you get a pickled value that must overwrite
// whatever was there, instead of a write local
//
int
write_local_pickled(cf_digest *keyd, as_partition_reservation *rsv,
        uint8_t *pickled_buf, size_t pickled_sz,
        const as_rec_props *p_rec_props, as_generation generation,
        uint32_t void_time, cf_node masternode, uint32_t info, ldt_prole_info *linfo)
{
	if (! as_storage_has_space(rsv->ns)) {
		cf_warning(AS_RW, "{%s}: write_local_pickled: drives full", rsv->ns->name);
		return -1;
	}

	as_storage_rd rd;
	as_index_ref r_ref;
	uint64_t memory_bytes = 0;
	r_ref.skip_lock       = false;
	as_index_tree *tree   = rsv->tree;
	bool is_subrec        = false;
	bool is_ldt_parent    = false;
	bool is_create        = false;
	bool do_destroy       = false;

	if (rsv->ns->ldt_enabled) {
		if ((info & RW_INFO_LDT_SUBREC)
				|| (info & RW_INFO_LDT_ESR)) {
			cf_detail(AS_RW,
					"LDT Subrecord Replication Request Received %"PRIx64"\n",
					*(uint64_t *)keyd);
			tree = rsv->sub_tree;
			is_subrec = true;
		} else if (info & RW_INFO_LDT_PARENTREC) {
			is_ldt_parent = true;
		}
	}

	int rv      = as_record_get_create(tree, keyd, &r_ref, rsv->ns, is_subrec);
	as_index *r = r_ref.r;

	if (rv < 0) {
		cf_warning_digest(AS_RW, keyd, "{%s} write_local_pickled: fail as_record_get_create() rv = %d", rsv->ns->name, rv);
		return -1;
	}

	if (rv == 1) {
		as_storage_record_create(rsv->ns, r, &rd, keyd);
		is_create = true;
	}
	else {
		as_storage_record_open(rsv->ns, r, &rd, keyd);
	}

	bool has_sindex = (info & RW_INFO_SINDEX_TOUCHED) != 0;

	rd.ignore_record_on_device = ! has_sindex && ! is_ldt_parent;

	rd.n_bins = as_bin_get_n_bins(r, &rd);
	uint16_t newbins = ntohs(*(uint16_t *) pickled_buf);

	if (! rd.ns->storage_data_in_memory && ! rd.ns->single_bin && newbins > rd.n_bins) {
		rd.n_bins = newbins;
	}

	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	uint32_t stack_particles_sz = rd.ns->storage_data_in_memory ? 0 : as_record_buf_get_stack_particles_sz(pickled_buf);
	// 256 as upper bound on the LDT control bin, we may write version below
	uint8_t stack_particles[stack_particles_sz + 256];
	uint8_t *p_stack_particles = stack_particles;

	// Check is duplication in case code is coming from multi op
	if (as_ldt_check_and_get_prole_version(keyd, rsv, linfo, info, &rd, is_create, __FILE__, __LINE__)) {
		do_destroy = true;
		goto Out;
	}

	if (rv != 1) {
		memory_bytes = as_storage_record_get_n_bytes_memory(&rd);
	}

	// Blindly overwrite property as in incoming record
	as_record_set_properties(&rd, p_rec_props);
	cf_detail(AS_RW, "TO PINDEX FROM MASTER Digest=%"PRIx64" bits %d \n",
			*(uint64_t *)&rd.keyd, as_ldt_record_get_rectype_bits(r));

	if (0 != (rv = as_record_unpickle_replace(r, &rd, pickled_buf, pickled_sz, &p_stack_particles, has_sindex))) {
		do_destroy = true;
		goto Out;
		// Is there any clean up that must be done here???
	}

	r->generation = generation;
	r->void_time = void_time;

	as_storage_record_adjust_mem_stats(&rd, memory_bytes);

	uint64_t version_to_set = 0;
    bool     set_version    = false;
	// Set the ldt prole version if there be need
	if (is_ldt_parent) {
		if (linfo->replication_partition_version_match && linfo->ldt_prole_version_set) {
			version_to_set = linfo->ldt_prole_version;
			set_version    = true;
		} else if (!linfo->replication_partition_version_match) {
			// TODO: Cannot come here without source version set if the 
			// partition versin does not match. Check upfront should not
			// allow parent record to be written. Fix the code organization
			version_to_set = linfo->ldt_source_version;
			set_version    = true;
		}
	}

	if (set_version) {
		int pbytes = as_ldt_parent_storage_set_version(&rd, version_to_set, p_stack_particles, __FILE__, __LINE__);
		if (pbytes < 0) {
			cf_warning(AS_LDT, "write_local_pickled: LDT Parent storage version set failed %d", pbytes);
			// Todo Rollback
		} else {
			p_stack_particles += pbytes;
		}
		cf_detail_digest(AS_LDT, keyd, "Wrote the destination version match %s set version (%ld) out of prole (%d:%ld) source(%ld)",
						linfo->replication_partition_version_match ? "true" : "false", version_to_set, linfo->ldt_prole_version_set, linfo->ldt_prole_version, linfo->ldt_source_version);
	} else {
		cf_detail_digest(AS_LDT, keyd, "Wrote the destination version match %s set version (%ld) out of prole (%d:%ld) source(%ld)",
						linfo->replication_partition_version_match ? "true" : "false", version_to_set, linfo->ldt_prole_version_set, linfo->ldt_prole_version, linfo->ldt_source_version);
	}

Out:
	if (is_create && do_destroy) {
		as_index_delete(tree, keyd);
	}

	as_storage_record_close(r, &rd);

	if ((tree == 0) || (rsv->ns == 0) || (rsv->p == 0)) {
		cf_crash(AS_RW,
				"record merge: bad reservation. tree %p ns %p part %p",
				tree, rsv->ns, rsv->p);
		return (-1);
	}

	uint16_t set_id = as_index_get_set_id(r);
	as_record_done(&r_ref, rsv->ns);

	// Do XDR write if
	// 1. If the write is not a migration write and
	// 2. If the write is a non-XDR write or
	// 3. If the write is a XDR write and forwarding is enabled (either globally or for namespace).
	if ((info & RW_INFO_MIGRATION) != RW_INFO_MIGRATION) {
		if (   ((info & RW_INFO_XDR) != RW_INFO_XDR)
			|| (   (g_config.xdr_cfg.xdr_forward_xdrwrites == true)
				|| (rsv->ns->ns_forward_xdr_writes == true))) {
			xdr_write(rsv->ns, *keyd, r->generation, masternode, false, set_id);
		}
	}

	if (!as_bin_inuse_has(&rd)) {
		// INIT_TR
		as_transaction tr;
		as_transaction_init(&tr, keyd, NULL);
		tr.rsv          = *rsv;
		write_delete_local(&tr, false, masternode, false);
	}

	return (0);
}

int
as_rw_get_ldt_info(ldt_prole_info *linfo, msg *m, as_partition_reservation *rsv)
{
	linfo->ldt_source_version = 0;
	linfo->ldt_source_version_set = false;
	linfo->ldt_prole_version_set = false;
	linfo->replication_partition_version_match = false;

	int not_found = 0;
	as_partition_vinfo *source_partition_version = NULL;
	size_t vinfo_sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_VINFOSET, (byte **) &source_partition_version,
			&vinfo_sz, MSG_GET_DIRECT)) {
		not_found++;
		cf_debug(AS_LDT, "MULTI_OP: Message Without Partition Version");
	} else {
		linfo->replication_partition_version_match = as_partition_vinfo_same(source_partition_version, &rsv->p->version_info);
/*
		cf_info(AS_RW, "MULTI_OP: Version matches");
		cf_info(AS_RW, "%ld %d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d", source_partition_version->iid,
				source_partition_version->vtp[0], source_partition_version->vtp[1],	source_partition_version->vtp[2],
				source_partition_version->vtp[3], source_partition_version->vtp[4],	source_partition_version->vtp[5],
				source_partition_version->vtp[6], source_partition_version->vtp[7],	source_partition_version->vtp[8],
				source_partition_version->vtp[9], source_partition_version->vtp[10],	source_partition_version->vtp[11],
				source_partition_version->vtp[12], source_partition_version->vtp[13],	source_partition_version->vtp[14],
				source_partition_version->vtp[15]);

		cf_info(AS_RW, "%ld %d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d", rsv->p->version_info.iid,
				rsv->p->version_info.vtp[0], rsv->p->version_info.vtp[1],	rsv->p->version_info.vtp[2],
				rsv->p->version_info.vtp[3], rsv->p->version_info.vtp[4],	rsv->p->version_info.vtp[5],
				rsv->p->version_info.vtp[6], rsv->p->version_info.vtp[7],	rsv->p->version_info.vtp[8],
				rsv->p->version_info.vtp[9], rsv->p->version_info.vtp[10],	rsv->p->version_info.vtp[11],
				rsv->p->version_info.vtp[12], rsv->p->version_info.vtp[13],	rsv->p->version_info.vtp[14],
				rsv->p->version_info.vtp[15]);
*/
	}

	if (0 != msg_get_uint64(m, RW_FIELD_LDT_VERSION, &linfo->ldt_source_version)) {
		cf_debug(AS_RW, "MULTI_OP: Message Without LDT source version Field");
	} else {
		linfo->ldt_source_version_set = true;
	}
	cf_detail(AS_RW, "MULTI_OP: LDT Info [%ld %d %d]", linfo->ldt_source_version, linfo->ldt_prole_version_set, linfo->replication_partition_version_match);
	return not_found;
}


//
// received a write message
//
// If is_write == false, we're in the 'duplicate' phase
//

int
write_process(cf_node node, msg *m, bool respond)
{
#ifdef DEBUG_MSG
	msg_dump(m, "rw incoming");
#endif

	uint32_t rv = 1;
	uint32_t result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;

	cf_digest	*keyd;
	size_t sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_DIGEST, (byte **) &keyd, &sz,
			MSG_GET_DIRECT)) {
		cf_debug(AS_RW, "write process received message with out digest");
		cf_atomic_int_incr(&g_config.rw_err_write_internal);
		goto Out;
	}

	uint64_t cluster_key;
	if (0 != msg_get_uint64(m, RW_FIELD_CLUSTER_KEY, &cluster_key)) {
		cf_warning(AS_RW, "write process received message without cluster key");
		cf_atomic_int_incr(&g_config.rw_err_write_internal);
		goto Out;
	}

	as_generation generation = 0;
	if (0 != msg_get_uint32(m, RW_FIELD_GENERATION, &generation)) {
		cf_detail(AS_RW, "write process recevied message without generation");
	}

	cl_msg *msgp = 0;
	size_t msgp_sz = 0;
	uint8_t *pickled_buf;
	size_t pickled_sz;
	if (0 != msg_get_buf(m, RW_FIELD_AS_MSG, (byte **) &msgp, &msgp_sz,
			MSG_GET_DIRECT)) {

		pickled_sz = 0;
		if (0 != msg_get_buf(m, RW_FIELD_RECORD, (byte **) &pickled_buf,
				&pickled_sz, MSG_GET_DIRECT)) {

			cf_debug(AS_RW,
					"write process received message without AS MSG or RECORD");
			cf_atomic_int_incr(&g_config.rw_err_write_internal);
			goto Out;
		}
	}

	uint8_t *ns_name = 0;
	size_t ns_name_len;
	if (0 != msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT)) {
		cf_info(AS_RW, "write process: no namespace");
		cf_atomic_int_incr(&g_config.rw_err_dup_internal);
		goto Out;
	}

	as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);
	if (!ns) {
		cf_info(AS_RW, "get abort invalid namespace received");
		cf_atomic_int_incr(&g_config.rw_err_dup_internal);
		goto Out;
	}

	as_rec_props rec_props;
	if (0 != msg_get_buf(m, RW_FIELD_REC_PROPS, (byte **) &rec_props.p_data,
			(size_t*) &rec_props.size, MSG_GET_DIRECT)) {
		cf_debug(AS_RW,
				"write process received message without record properties");
	}

	if (msgp) {
		msgp->msg.info2 &= (AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE);
		// INIT_TR
		as_transaction tr;
		as_transaction_init(&tr, keyd, msgp);

		/* NB need to use the _migrate variant here so we can write into desync
		 * partitions - and that never fails */
		as_partition_reserve_migrate(ns, as_partition_getid(tr.keyd), &tr.rsv, 0);
		cf_atomic_int_incr(&g_config.wprocess_tree_count);

		// check here if this is prole delete caused by nsup
		// If yes we need to tell XDR NOT to ship the delete
		uint32_t info = 0;
		msg_get_uint32(m, RW_FIELD_INFO, &info);

		ldt_prole_info linfo;
		if ((info & RW_INFO_LDT) && as_rw_get_ldt_info(&linfo, m, &tr.rsv)) {
			goto Out;
		}

		if( tr.rsv.state == AS_PARTITION_STATE_ABSENT ||
			tr.rsv.state == AS_PARTITION_STATE_WAIT)
		{
			cf_debug_digest(AS_RW, keyd, "[PROLE STATE MISMATCH:1] TID(0) Partition PID(%u) State is Absent or other(%u). Return to Sender.",
					tr.rsv.pid, tr.rsv.state );
			result_code = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
			// We're going to have to retry this Prole Write operation.  We'll
			// do this by telling the master to retry (which will cause the
			// master to re-enqueue the transaction).
			cf_atomic_int_incr(&g_config.stat_cluster_key_prole_retry);
			cf_debug_digest(AS_RW, keyd, "[CK MISMATCH] P PID(%u) State ABSENT or other(%u):",
					tr.rsv.pid, tr.rsv.state );
		} else
		{
			cf_debug_digest(AS_RW, keyd, "[PROLE write]: SingleBin(%d) generation(%d):",
					ns->single_bin, generation );


			if (as_ldt_check_and_get_prole_version(keyd, &tr.rsv, &linfo, info, NULL, false, __FILE__, __LINE__)) {
				result_code = AS_PROTO_RESULT_OK;
				as_partition_release(&tr.rsv); // returns reservation a few lines up
				cf_atomic_int_decr(&g_config.wprocess_tree_count);
				goto Out;
			}

			if (info & RW_INFO_NSUP_DELETE) {
				tr.flag |= AS_TRANSACTION_FLAG_NSUP_DELETE;
			}

			if ((info & RW_INFO_LDT_SUBREC)
					|| (info & RW_INFO_LDT_ESR)) {
				tr.flag |= AS_TRANSACTION_FLAG_LDT_SUB;
				cf_detail(AS_RW,
						"LDT Subrecord Replication Request Received %"PRIx64"\n",
						*(uint64_t*)keyd);
			}
			if (info & RW_INFO_UDF_WRITE) {
				cf_atomic_int_incr(&g_config.udf_replica_writes);
			}
			if (msgp->msg.info2 & AS_MSG_INFO2_DELETE) {
				rv = write_delete_local(&tr, true, node, false);
			} else {
				cf_crash_digest(AS_RW, keyd, "replica write trying to use write_local()");
			}

			cf_debug_digest(AS_RW, keyd, "Local RW: rv %d result code(%d)",
					rv, tr.result_code);

			if (rv == 0) {
				tr.result_code = 0;
			} else {
				// TODO: Handle case when its a Replace operation in write_local,
				if ((tr.result_code == AS_PROTO_RESULT_FAIL_NOTFOUND)
						&& (msgp->msg.info2 & AS_MSG_INFO2_DELETE)) {
					cf_atomic_int_incr(&g_config.err_write_fail_prole_delete);
				} else {
					cf_info_digest(AS_RW, &(tr.keyd),
							"rw prole operation: failed, ns(%s) rv(%d) result code(%d) : ",
							ns->name, rv, tr.result_code);
				}
			}
			result_code = tr.result_code;
		}

		as_partition_release(&tr.rsv);
		cf_atomic_int_decr(&g_config.wprocess_tree_count);
	} // end input msg case

	else {
		// This is the write-pickled case, where currently all prole writes go.
		int missing_fields = 0;

		uint32_t void_time = 0;
		if (0 != msg_get_uint32(m, RW_FIELD_VOID_TIME, &void_time)) {
			cf_warning(AS_RW,
					"write process received message without void_time");
			missing_fields++;
		}

		uint32_t info = 0;
		if (0 != msg_get_uint32(m, RW_FIELD_INFO, &info)) {
			cf_warning(AS_RW,
					"write process received message without info field");
			missing_fields++;
		}

		if (missing_fields) {
			cf_warning(AS_RW,
					"write process received message with %d missing fields ~~ returning result fail unknown",
					missing_fields);
			result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
			goto Out;
		}

		as_partition_reservation rsv;
		as_partition_reserve_migrate(ns, as_partition_getid(*keyd), &rsv, 0);
		cf_atomic_int_incr(&g_config.wprocess_tree_count);

		ldt_prole_info linfo;
		if ((info & RW_INFO_LDT) && as_rw_get_ldt_info(&linfo, m, &rsv)) {
			goto Out;
		}

		// See if we're being asked to write into an ABSENT PROLE PARTITION.
		// If so, then DO NOT WRITE.  Instead, return an error so that the
		// Master will retry with the correct node.
		if (rsv.state == AS_PARTITION_STATE_ABSENT ||
			rsv.state == AS_PARTITION_STATE_WAIT)
		{
			result_code = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
			cf_atomic_int_incr(&g_config.stat_cluster_key_prole_retry);
			cf_debug_digest(AS_RW, keyd,
					"[PROLE STATE MISMATCH:2] TID(0) P PID(%u) State:ABSENT or other(%u). Return to Sender. :",
					rsv.pid, rsv.state  );

		} else {
			cf_debug_digest(AS_RW, keyd, "Write Pickled: PID(%u) PState(%d) Gen(%d):",
					rsv.pid, rsv.p->state, generation);

			int rsp = write_local_pickled(keyd, &rsv, pickled_buf, pickled_sz,
					&rec_props, generation, void_time, node, info, &linfo);
			if (rsp != 0) {
				cf_info_digest(AS_RW, keyd, "[NOTICE] writing pickled failed(%d):", rsp );
				result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
			} else {
				result_code = AS_PROTO_RESULT_OK;
			}

		} // end else valid Partition state

		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.wprocess_tree_count);
	} // end else write record

Out:

	if (result_code != AS_PROTO_RESULT_OK) {
		if (result_code == AS_PROTO_RESULT_FAIL_GENERATION) {
			cf_atomic_int_incr(&g_config.err_write_fail_prole_generation);
		} else if (result_code == AS_PROTO_RESULT_FAIL_UNKNOWN) {
			cf_atomic_int_incr(&g_config.err_write_fail_prole_unknown);
		} else if (result_code == AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH ) {
			cf_atomic_int_incr(&g_config.rw_err_write_cluster_key);
		}
	}

	uint32_t info = 0;
	msg_get_uint32(m, RW_FIELD_INFO, &info);
	if ((info & RW_INFO_LDT_SUBREC) || (info & RW_INFO_LDT_ESR)) {
		cf_detail_digest(AS_RW, keyd,
				"LDT Subrecord Replication Request Response Sent: rc(%d) :",
				result_code);
	}

	// clear out the old message, change op to ack, set result code and add new response if any
	msg_set_unset(m, RW_FIELD_AS_MSG);
	msg_set_unset(m, RW_FIELD_RECORD);
	msg_set_unset(m, RW_FIELD_REC_PROPS);
	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result_code);

	if (respond) {
		uint64_t start_ns = 0;
		if (g_config.microbenchmarks) {
			start_ns = cf_getns();
		}
		int rv2 = as_fabric_send(node, m, AS_FABRIC_PRIORITY_MEDIUM);
		if (g_config.microbenchmarks && start_ns) {
			histogram_insert_data_point(g_config.prole_fabric_send_hist,
					start_ns);
		}

		if (rv2 != 0) {
			cf_debug(AS_RW, "write process: send fabric message bad return %d",
					rv2);
			as_fabric_msg_put(m);
			cf_atomic_int_incr(&g_config.rw_err_write_send);
		}
	}

	return (0);
}

bool
msg_has_key(as_msg* m)
{
	return as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY) != NULL;
}

bool
check_msg_key(as_msg* m, as_storage_rd* rd)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);

	if (! f) {
		cf_warning(AS_RW, "no key sent for key check");
		return false;
	}

	uint32_t key_size = as_msg_field_get_value_sz(f);
	uint8_t* key = f->data;

	if (key_size != rd->key_size || memcmp(key, rd->key, key_size) != 0) {
		cf_warning(AS_RW, "key mismatch - end of universe?");
		return false;
	}

	return true;
}

//
// Returns a AS_PROTO_RESULT
// masternode gets passed to XDR if we are shipping this write.
// masternode is 0 if this node is master, otherwise its nodeid
int
write_delete_local(as_transaction *tr, bool journal, cf_node masternode,
		bool check_gen)
{
	// Shortcut pointers & flags.
	as_msg *m = tr->msgp ? &tr->msgp->msg : NULL;
	as_namespace *ns = tr->rsv.ns;

	if ((AS_PARTITION_STATE_SYNC != tr->rsv.state) && journal) {
		if (AS_PARTITION_STATE_DESYNC != tr->rsv.state)
			cf_debug(AS_RW, "journal delete: unusual state %d",
					(int)tr->rsv.state);
		if (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB) {
			cf_detail_digest(AS_LDT, &tr->keyd, "LDT Subrecord journalling and skipping %"PRIx64"\n");
		}

		write_delete_journal(tr, (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB));
		return (0);
	} else if (AS_PARTITION_STATE_DESYNC == tr->rsv.state) {
		cf_detail(AS_RW,
				"{%s:%d} write_delete_local: partition is desync - writes will flow from master",
				ns->name, tr->rsv.pid);
		return (0);
	}
	as_index_tree *tree = tr->rsv.tree;

	if (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB) {
		cf_detail(AS_RW, "LDT Subrecord Delete Request Received %"PRIx64"\n",
				*(uint64_t *)&tr->keyd);
		tree = tr->rsv.sub_tree;
	}

	if (!tree) {
		cf_crash(AS_RW, "Tree is NULL bad bad bad bad !!!");
	}

	as_index_ref r_ref;
	r_ref.skip_lock = false;

	if (0 != as_record_get(tree, &tr->keyd, &r_ref, ns)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_NOTFOUND;

		if (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB) {
			cf_detail_digest(AS_LDT, &tr->keyd,  "LDT Subrecord Delete Request did not find record %"PRIx64"\n");
		}
		return -1;
	}

	as_index *r = r_ref.r;

	// Check generation requirement, if any.
	if (check_gen && m &&
			(((m->info2 & AS_MSG_INFO2_GENERATION) && m->generation != r->generation) ||
			 ((m->info2 & AS_MSG_INFO2_GENERATION_GT) && m->generation <= r->generation))) {
		as_record_done(&r_ref, ns);
		tr->result_code = AS_PROTO_RESULT_FAIL_GENERATION;
		if (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB) {
			cf_detail_digest(AS_LDT, &tr->keyd, "LDT Subrecord Delete Request gen check failed %"PRIx64"\n");
		}

		return -1;
	}

	bool check_key = m && msg_has_key(m);

	if (ns->storage_data_in_memory || check_key) {
		as_storage_rd rd;
		as_storage_record_open(ns, r, &rd, &tr->keyd);

		// Check the key if required.
		// Note - for data-not-in-memory a key check is expensive!
		if (check_key && as_storage_record_get_key(&rd) &&
				! check_msg_key(m, &rd)) {
			as_storage_record_close(r, &rd);
			as_record_done(&r_ref, ns);
			tr->result_code = AS_PROTO_RESULT_FAIL_KEY_MISMATCH;
			return -1;
		}

		if (ns->storage_data_in_memory) {
			// Remove record from secondary index. In case data is not in memory
			// then we won't have record in that case secondary index entry is
			// cleaned up by background sindex defrag thread.
			if (as_sindex_ns_has_sindex(ns)) {
				rd.n_bins = as_bin_get_n_bins(r, &rd);
				rd.bins = as_bin_get_all(r, &rd, NULL);

				int sindex_ret = AS_SINDEX_OK;
				const char* set_name = as_index_get_set_name(r, ns);

				SINDEX_GRLOCK();
				SINDEX_BINS_SETUP(sbins, ns->sindex_cnt);
				int sbins_populated = 0;
				for (int i = 0; i < rd.n_bins; i++) {
					sbins_populated += as_sindex_sbins_from_bin(ns, set_name, &rd.bins[i], &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
				}
				SINDEX_GUNLOCK();

				cf_debug(AS_SINDEX,
						"Delete @ %s %d digest %ld", __FILE__, __LINE__, *(uint64_t *)&rd.keyd);
				if (sbins_populated) {
					sindex_ret = as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, &rd.keyd);
					as_sindex_sbin_freeall(sbins, sbins_populated);
				}
				if (sindex_ret != AS_SINDEX_OK)
					cf_debug(AS_SINDEX,
							"Failed: %d", as_sindex_err_str(sindex_ret));
			}
		}

		as_storage_record_close(r, &rd);
	}

	// Save the set-ID for XDR.
	uint16_t set_id = as_index_get_set_id(r);

	as_index_delete(tree, &tr->keyd);
	cf_atomic_int_incr(&g_config.stat_delete_success);
	as_record_done(&r_ref, ns);

	// Check if XDR needs to ship this delete

	if (g_config.xdr_cfg.xdr_delete_shipping_enabled == true) {
		// Do not ship delete if it is result of eviction/migrations etc.
		// unless we have config setting of shipping these type of deletes.
		// Ship the deletes coming from application directly.
		if ((tr->flag & AS_TRANSACTION_FLAG_NSUP_DELETE)
				&& (g_config.xdr_cfg.xdr_nsup_deletes_enabled == false)) {
			cf_atomic_int_incr(&g_config.stat_nsup_deletes_not_shipped);
			cf_detail(AS_RW, "write delete: Got delete from nsup.");
		} else {
			cf_detail(AS_RW, "write delete: Got delete from user.");
			// If this delete is a result of XDR shipping, dont write it to the digest pipe
			// unless the user configured the server to forward the XDR writes. If this is
			// a normal delete issued by application, write it to the digest pipe.
			if (   (m && !(m->info1 & AS_MSG_INFO1_XDR))
				|| (   (g_config.xdr_cfg.xdr_forward_xdrwrites == true)
					|| (ns->ns_forward_xdr_writes == true))) {
				cf_debug(AS_RW, "write delete: Got delete from user.");
				xdr_write(ns, tr->keyd, tr->generation, masternode, true, set_id);
			}
		}
	}

	return 0;
}



//==============================================================================
// Utilities used by write_local() and UDF code.
//

int
as_record_set_set_from_msg(as_record *r, as_namespace *ns, as_msg *m)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);

	if (! f || as_msg_field_get_value_sz(f) == 0) {
		return 0;
	}

	size_t msg_set_name_len = as_msg_field_get_value_sz(f);
	char msg_set_name[msg_set_name_len + 1];

	memcpy((void*)msg_set_name, (const void*)f->data, msg_set_name_len);
	msg_set_name[msg_set_name_len] = 0;

	// Given the name, find/assign the set-ID and write it in the as_index.
	return as_index_set_set(r, ns, msg_set_name, true);
}

bool
get_msg_key(as_msg* m, as_storage_rd* rd)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);

	if (! f) {
		return true;
	}

	if (rd->ns->single_bin && rd->ns->storage_data_in_memory) {
		// For now we just ignore the key - should we fail out of write_local()?
		cf_warning(AS_RW, "can't store key if data-in-memory & single-bin");
		return false;
	}

	rd->key_size = as_msg_field_get_value_sz(f);
	rd->key = f->data;

	return true;
}

void
update_metadata_in_index(as_transaction *tr, bool increment_generation,
		as_index *r)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;

	if (m->record_ttl == 0xFFFFffff) {
		// TTL = -1 sets record_void time to "never expires".
		r->void_time = 0;
	}
	else if (m->record_ttl != 0) {
		// Check for sizes that might be too large - limit it to 0xFFFFffff.
		uint64_t temp_big = (uint64_t)as_record_void_time_get() + (uint64_t)m->record_ttl;

		if (temp_big > 0xFFFFffff) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} ttl %u causes void-time overflow, clamping void-time to max ",
					ns->name, m->record_ttl);

			r->void_time = 0xFFFFffff;
		}
		else {
			if (m->record_ttl > MAX_TTL_WARNING && ns->max_ttl == 0) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} ttl %u exceeds %u - set config value max-ttl to suppress this warning ",
						ns->name, m->record_ttl, MAX_TTL_WARNING);
			}

			r->void_time = (uint32_t)temp_big;
		}
	}
	else if (ns->default_ttl != 0) {
		// TTL = 0 set record_void time to default ttl value.
		r->void_time = as_record_void_time_get() + ns->default_ttl;
	}
	else {
		r->void_time = 0;
	}

	if (as_ldt_record_is_sub(r)) {
		// Sub-records never expire by themselves.
		r->void_time = 0;
	}

	if (increment_generation) {
		r->generation++;

		// The generation might wrap - 0 is reserved as "uninitialized".
		if (r->generation == 0) {
			r->generation = 1;
		}
	}
}

bool
pickle_all(as_storage_rd *rd, pickle_info *pickle)
{
	if (0 != as_record_pickle(rd->r, rd, &pickle->buf, &pickle->buf_size)) {
		return false;
	}

	pickle->rec_props_data = NULL;
	pickle->rec_props_size = 0;

	// TODO - we could avoid this copy (and maybe even not do this here at all)
	// if all callers malloced rdp->rec_props.p_data upstream for hand-off...
	if (rd->rec_props.p_data) {
		pickle->rec_props_size = rd->rec_props.size;
		pickle->rec_props_data = cf_malloc(pickle->rec_props_size);

		if (! pickle->rec_props_data) {
			cf_free(pickle->buf);
			return false;
		}

		memcpy(pickle->rec_props_data, rd->rec_props.p_data,
				pickle->rec_props_size);
	}

	return true;
}



//==============================================================================
// write_local() utilities.
//

static void
write_local_failed(as_transaction* tr, as_index_ref* r_ref,
		bool record_created, as_index_tree* tree, as_storage_rd* rd,
		int result_code)
{
	if (r_ref) {
		if (record_created) {
			as_index_delete(tree, &tr->keyd);
		}

		if (rd) {
			as_storage_record_close(r_ref->r, rd);
		}

		as_record_done(r_ref, tr->rsv.ns);
	}

	switch (result_code) {
	case AS_PROTO_RESULT_FAIL_NOTFOUND:
		cf_atomic_int_incr(&g_config.err_write_fail_not_found);
		break;
	case AS_PROTO_RESULT_FAIL_GENERATION:
		cf_atomic_int_incr(&g_config.err_write_fail_generation);
		if (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR) {
			cf_atomic_int_incr(&g_config.err_write_fail_generation_xdr);
		}
		break;
	case AS_PROTO_RESULT_FAIL_PARAMETER:
		cf_atomic_int_incr(&g_config.err_write_fail_parameter);
		break;
	case AS_PROTO_RESULT_FAIL_RECORD_EXISTS:
		cf_atomic_int_incr(&g_config.err_write_fail_key_exists);
		break;
	case AS_PROTO_RESULT_FAIL_BIN_EXISTS:
		cf_atomic_int_incr(&g_config.err_write_fail_bin_exists);
		break;
	case AS_PROTO_RESULT_FAIL_PARTITION_OUT_OF_SPACE:
		cf_atomic_int_incr(&g_config.err_out_of_space);
		break;
	case AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE:
		cf_atomic_int_incr(&g_config.err_write_fail_incompatible_type);
		break;
	case AS_PROTO_RESULT_FAIL_RECORD_TOO_BIG:
		cf_atomic_int_incr(&g_config.err_write_fail_record_too_big);
		break;
	case AS_PROTO_RESULT_FAIL_BIN_NOT_FOUND:
		cf_atomic_int_incr(&g_config.err_write_fail_bin_not_found);
		break;
	case AS_PROTO_RESULT_FAIL_KEY_MISMATCH:
		cf_atomic_int_incr(&g_config.err_write_fail_key_mismatch);
		break;
	case AS_PROTO_RESULT_FAIL_BIN_NAME:
		cf_atomic_int_incr(&g_config.err_write_fail_bin_name);
		break;
	case AS_PROTO_RESULT_FAIL_FORBIDDEN:
		cf_atomic_int_incr(&g_config.err_write_fail_forbidden);
		break;
	case AS_PROTO_RESULT_FAIL_UNKNOWN:
	default:
		cf_atomic_int_incr(&g_config.err_write_fail_unknown);
		break;
	}

	tr->result_code = result_code;
}

int write_local_preprocessing(as_transaction *tr, write_local_generation *wlg,
		bool *is_done)
{
	*is_done = true;

	as_msg *m = &tr->msgp->msg;
	cf_detail(AS_RW, "WRITE LOCAL: info %02x %02x nops %d",
			m->info1, m->info2, m->n_ops);

	as_namespace *ns = tr->rsv.ns;

	// ns->stop_writes is set by thr_nsup if configured threshold is breached.
	if (cf_atomic32_get(ns->stop_writes) == 1) {
		cf_debug(AS_RW, "{%s}: write_local: failed by stop-writes", ns->name);
		write_local_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_PARTITION_OUT_OF_SPACE);
		return -1;
	}

	if (! as_storage_has_space(ns)) {
		cf_warning(AS_RW, "{%s}: write_local: drives full", ns->name);
		write_local_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_PARTITION_OUT_OF_SPACE);
		return -1;
	}

	// Fail if record_ttl is neither "use namespace default" flag (0) nor
	// "never expire" flag (0xFFFFffff), and it exceeds configured max_ttl.
	if (m->record_ttl != 0 && m->record_ttl != 0xFFFFffff &&
			ns->max_ttl != 0 && m->record_ttl > ns->max_ttl) {
		cf_info(AS_RW, "write_local: incoming ttl %u too big compared to %u", m->record_ttl, ns->max_ttl);
		write_local_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
		return -1;
	}

	// Fail if disallow_null_setname is true and set name is absent or empty.
	if (ns->disallow_null_setname) {
		as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);

		if (! f || as_msg_field_get_value_sz(f) == 0) {
			cf_info(AS_RW, "write_local: null/empty set name not allowed for namespace %s", ns->name);
			write_local_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
			return -1;
		}
	}

	if (tr->rsv.reject_writes) {
		cf_debug(AS_RW, "{%s:%d} write_local: partition rejects writes - writes will flow from master. digest %"PRIx64"",
				ns->name, tr->rsv.pid, *(uint64_t*)&tr->keyd);
		return 0;
	}
	else if (AS_PARTITION_STATE_DESYNC == tr->rsv.state) {
		cf_debug(AS_RW, "{%s:%d} write_local: partition is desync - writes will flow from master. digest %"PRIx64"",
				ns->name, tr->rsv.pid, *(uint64_t*)&tr->keyd);
		return 0;
	}

	*is_done = false;
	return 0;
}

static bool
check_msg_set_name(as_msg* m, const char* set_name)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);

	if (! f || as_msg_field_get_value_sz(f) == 0) {
		if (set_name) {
			cf_warning(AS_RW, "op overwriting record in set '%s' has no set name",
					set_name);
		}

		return true;
	}

	size_t msg_set_name_len = as_msg_field_get_value_sz(f);

	if (! set_name ||
			strncmp(set_name, (const char*)f->data, msg_set_name_len) != 0 ||
			set_name[msg_set_name_len] != 0) {
		char msg_set_name[msg_set_name_len + 1];

		memcpy((void*)msg_set_name, (const void*)f->data, msg_set_name_len);
		msg_set_name[msg_set_name_len] = 0;

		cf_warning(AS_RW, "op overwriting record in set '%s' has different set name '%s'",
				set_name ? set_name : "(null)", msg_set_name);
		return false;
	}

	return true;
}

// Not a nice way to specify a read-all op - dictated by backward compatibility.
// Note - must check this before checking for normal read op!
static inline bool
op_is_read_all(as_msg_op *op, as_msg *m)
{
	return op->name_sz == 0 && op->op == AS_MSG_OP_READ && (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
}

int
write_local_policies(as_transaction *tr, bool *p_must_not_create,
		bool *p_record_level_replace, bool *p_must_fetch_data,
		bool *p_increment_generation)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;
	bool info1_get_all = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	bool must_not_create =
			(m->info3 & AS_MSG_INFO3_UPDATE_ONLY) ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) ||
			(m->info3 & AS_MSG_INFO3_BIN_REPLACE_ONLY);

	bool record_level_replace =
			(m->info3 & AS_MSG_INFO3_CREATE_OR_REPLACE) ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY);

	bool must_fetch_data = false;

	bool increment_generation = false;

	bool has_read_all_op = false;
	bool generates_response_bin = false;

	// Loop over ops to check and modify flags.
	as_msg_op *op = NULL;
	int i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (op->op != AS_MSG_OP_MC_TOUCH) {
			increment_generation = true;
		}

		if (OP_IS_TOUCH(op->op)) {
			if (record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: touch op can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			must_not_create = true;
			must_fetch_data = true;
			continue;
		}

		if (ns->data_in_index && ! is_embedded_particle_type(op->particle_type) &&
				// Allow AS_PARTICLE_TYPE_NULL, although bin-delete operations
				// are not likely in single-bin configuration.
				op->particle_type != AS_PARTICLE_TYPE_NULL) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: can't write non-integer in data-in-index configuration ", ns->name);
			return AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}

		if (op->name_sz >= AS_ID_BIN_SZ) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: bin name too long (%d) ", ns->name, op->name_sz);
			return AS_PROTO_RESULT_FAIL_BIN_NAME;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			if (op->particle_type == AS_PARTICLE_TYPE_NULL && record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: bin delete can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}
		else if (OP_IS_MODIFY(op->op)) {
			if (record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: modify op can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			must_fetch_data = true;
		}
		else if (op_is_read_all(op, m)) {
			if (respond_all_ops) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: read-all op can't have respond-all-ops flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (has_read_all_op) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: can't have more than one read-all op ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			has_read_all_op = true;
			must_fetch_data = true;
		}
		else if (op->op == AS_MSG_OP_READ) {
			generates_response_bin = true;
			must_fetch_data = true;
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			if (record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: cdt modify op can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			generates_response_bin = true; // CDT modify may generate a response bin
			must_fetch_data = true;
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			generates_response_bin = true;
			must_fetch_data = true;
		}
	}

	if (has_read_all_op && generates_response_bin) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: read-all op can't mix with ops that generate response bins ", ns->name);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (info1_get_all && ! has_read_all_op) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: get-all flag set with no read-all op ", ns->name);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	*p_must_not_create = must_not_create;
	*p_record_level_replace = record_level_replace;
	*p_must_fetch_data = must_fetch_data;
	*p_increment_generation = increment_generation;

	return 0;
}

int
write_local_handle_msg_key(as_transaction *tr, as_storage_rd *rd)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;

	if (as_index_is_flag_set(rd->r, AS_INDEX_FLAG_KEY_STORED)) {
		// Key stored for this record - be sure it gets rewritten.

		// This will force a device read for non-data-in-memory, even if
		// must_fetch_data is false! Since there's no advantage to using the
		// loaded block after this if must_fetch_data is false, leave the
		// subsequent code as-is.
		// TODO - use client-sent key instead of device-stored key if available?
		if (! as_storage_record_get_key(rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: can't get stored key ", ns->name);
			return AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		// Check the client-sent key, if any, against the stored key.
		if (msg_has_key(m) && ! check_msg_key(m, rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: key mismatch ", ns->name);
			return AS_PROTO_RESULT_FAIL_KEY_MISMATCH;
		}
	}
	// If we got a key without a digest, it's an old client, not a cue to store
	// the key. (Remove this check when we're sure all old C clients are gone.)
	else if (as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE)) {
		// Key not stored for this record - store one if sent from client. For
		// data-in-memory, don't allocate the key until we reach the point of no
		// return. Also don't set AS_INDEX_FLAG_KEY_STORED flag until then.
		if (! get_msg_key(m, rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: can't store key ", ns->name);
			return AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE;
		}
	}

	return 0;
}

// For now, used only for read ops.
int
write_local_bin_check(as_transaction *tr, as_bin *bin)
{
	// Shortcut pointers.
	as_namespace *ns = tr->rsv.ns;

	if (bin && as_bin_is_hidden(bin)) {
		// Note - if single-bin, this likely means the bin state is corrupt.
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: cannot manipulate hidden bin directly ", ns->name);
		return AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	return 0;
}

bool
write_local_sindex_update(as_namespace *ns, const char *set_name,
		cf_digest *keyd, as_bin* old_bins, uint32_t n_old_bins,
		as_bin* new_bins, uint32_t n_new_bins)
{
	int sbins_populated = 0;
	bool not_just_created[n_new_bins];

	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		not_just_created[i_new] = false;
	}

	SINDEX_GRLOCK();

	// Maximum number of sindexes which can be changed in one transaction is
	// 2 * ns->sindex_cnt.
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);

	// For every old bin, find the corresponding new bin (if any) and adjust the
	// secondary index if the bin was modified. If no corresponding new bin is
	// found, it means the old bin was deleted - also adjust the secondary index
	// accordingly.

	for (int32_t i_old = 0; i_old < (int32_t)n_old_bins; i_old++) {
		as_bin *b_old = &old_bins[i_old];
		bool found = false;

		// Loop over new bins. Start at old bin index (if possible) and go down,
		// wrapping around to do the higher indexes last. This will find a match
		// (if any) very quickly - instantly, unless there were bins deleted.

		bool any_new = n_new_bins != 0;
		int32_t n_new_minus_1 = (int32_t)n_new_bins - 1;
		int32_t i_new = n_new_minus_1 < i_old ? n_new_minus_1 : i_old;

		while (any_new) {
			as_bin *b_new = &new_bins[i_new];

			if (b_old->id == b_new->id) {
				if (as_bin_get_particle_type(b_old) != as_bin_get_particle_type(b_new) ||
						b_old->particle != b_new->particle) {
					// TODO - might want a "diff" method that takes two bins and
					// detects the (rare) case when a particle was rewritten
					// with the exact old value.
					sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b_old, &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
					sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b_new, &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
				}

				found = true;
				not_just_created[i_new] = true;
				break;
			}

			if (--i_new < 0 && (i_new = n_new_minus_1) <= i_old) {
				break;
			}

			if (i_new == i_old) {
				break;
			}
		}

		if (! found) {
			sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b_old, &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
		}
	}

	// Now find the new bins that are just-created bins. We've marked the others
	// in the loop above, so any left are just-created.

	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		if (not_just_created[i_new]) {
			continue;
		}

		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, &new_bins[i_new], &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
	}

	SINDEX_GUNLOCK();

	if (sbins_populated) {
		uint64_t start_ns = g_config.microbenchmarks ? cf_getns() : 0;

		as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, keyd);
		as_sindex_sbin_freeall(sbins, sbins_populated);

		if (start_ns != 0) {
			histogram_insert_data_point(g_config.write_sindex_hist, start_ns);
		}

		return true;
	}

	return false;
}

void
write_local_dim_unwind(as_bin* old_bins, uint32_t n_old_bins, as_bin* new_bins,
		uint32_t n_new_bins, as_bin* cleanup_bins, uint32_t n_cleanup_bins)
{
	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		as_bin *b_new = &new_bins[i_new];

		if (! as_bin_inuse(b_new)) {
			break;
		}

		// Embedded particles have no-op destructors - skip loop over old bins.
		if (as_bin_is_embedded_particle(b_new)) {
			continue;
		}

		as_particle *p_new = b_new->particle;

		for (uint32_t i_old = 0; i_old < n_old_bins; i_old++) {
			as_bin *b_old = &old_bins[i_old];

			if (b_new->id == b_old->id) {
				if (p_new != as_bin_get_particle(b_old)) {
					as_bin_particle_destroy(b_new, true);
				}

				break;
			}
		}
	}

	for (uint32_t i_cleanup = 0; i_cleanup < n_cleanup_bins; i_cleanup++) {
		as_bin *b_cleanup = &cleanup_bins[i_cleanup];
		as_particle *p_cleanup = b_cleanup->particle;

		for (uint32_t i_old = 0; i_old < n_old_bins; i_old++) {
			as_bin *b_old = &old_bins[i_old];

			if (b_cleanup->id == b_old->id) {
				if (p_cleanup != as_bin_get_particle(b_old)) {
					as_bin_particle_destroy(b_cleanup, true);
				}

				break;
			}
		}
	}

	// The index element's as_bin_space pointer still points at old bins.
}

void
write_local_dim_single_bin_unwind(as_bin* old_bin, as_bin* new_bin,
		as_bin* cleanup_bins, uint32_t n_cleanup_bins)
{
	as_particle *p_old = as_bin_get_particle(old_bin);

	if (! as_bin_is_embedded_particle(new_bin) &&
			new_bin->particle != p_old) {
		as_bin_particle_destroy(new_bin, true);
	}

	for (uint32_t i_cleanup = 0; i_cleanup < n_cleanup_bins; i_cleanup++) {
		as_bin *b_cleanup = &cleanup_bins[i_cleanup];

		if (b_cleanup->particle != p_old) {
			as_bin_particle_destroy(b_cleanup, true);
		}
	}

	*new_bin = *old_bin;
}

void
write_local_pickle_unwind(pickle_info *pickle)
{
	cf_free(pickle->buf);

	if (pickle->rec_props_data) {
		cf_free(pickle->rec_props_data);
	}
}

typedef struct index_metadata_s {
	uint32_t	void_time;
	uint16_t	generation;
} index_metadata;

void
write_local_update_index_metadata(as_transaction *tr, bool increment_generation,
		index_metadata *old, as_index *r)
{
	old->void_time = r->void_time;
	old->generation = r->generation;

	update_metadata_in_index(tr, increment_generation, r);
}

void
write_local_index_metadata_unwind(index_metadata *old, as_index *r)
{
	r->void_time = old->void_time;
	r->generation = old->generation;
}

static inline void
append_bin_to_destroy(as_bin *b, as_bin *bins, uint32_t *p_n_bins)
{
	if (as_bin_inuse(b) && ! as_bin_is_embedded_particle(b)) {
		bins[(*p_n_bins)++] = *b;
	}
}

// Also used by read path.
static inline void
destroy_stack_bins(as_bin *stack_bins, uint32_t n_bins)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin_particle_destroy(&stack_bins[i], true);
	}
}



//==============================================================================
// write_local() bin operations, for all configurations.
//

int
write_local_bin_ops_loop(as_transaction *tr, as_storage_rd *rd,
		as_msg_op **ops, as_bin *response_bins, uint32_t *p_n_response_bins,
		as_bin *result_bins, uint32_t *p_n_result_bins,
		cf_dyn_buf *particles_db,
		as_bin *cleanup_bins, uint32_t *p_n_cleanup_bins)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;
	bool create_only = (m->info2 & AS_MSG_INFO2_BIN_CREATE_ONLY) != 0;
	bool replace_only = (m->info3 & AS_MSG_INFO3_BIN_REPLACE_ONLY) != 0;

	int result;

	as_msg_op *op = NULL;
	int i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (OP_IS_TOUCH(op->op)) {
			continue;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			// AS_PARTICLE_TYPE_NULL means delete the bin.
			// TODO - should this even be allowed for single-bin?
			if (op->particle_type == AS_PARTICLE_TYPE_NULL) {
				int32_t j = as_bin_get_index_from_buf(rd, op->name, op->name_sz);

				if (j != -1) {
					if (ns->storage_data_in_memory) {
						append_bin_to_destroy(&rd->bins[j], cleanup_bins, p_n_cleanup_bins);
					}

					as_bin_set_empty_shift(rd, j);
				}
			}
			// It's a regular bin write.
			else {
				as_bin *b = as_bin_get_or_create_from_buf(rd, op->name, op->name_sz, create_only, replace_only, &result);

				if (! b) {
					return result;
				}

				if (ns->storage_data_in_memory) {
					as_bin cleanup_bin = *b;

					if ((result = as_bin_particle_alloc_from_client(b, op)) < 0) {
						cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_particle_alloc_from_client() ", ns->name);
						return -result;
					}

					append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
				}
				else {
					if ((result = as_bin_particle_stack_from_client(b, particles_db, op)) < 0) {
						cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_particle_stack_from_client() ", ns->name);
						return -result;
					}
				}
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		// Modify an existing bin value.
		else if (OP_IS_MODIFY(op->op)) {
			as_bin *b = as_bin_get_or_create_from_buf(rd, op->name, op->name_sz, create_only, replace_only, &result);

			if (! b) {
				return result;
			}

			if (ns->storage_data_in_memory) {
				as_bin cleanup_bin = *b;

				if ((result = as_bin_particle_alloc_modify_from_client(b, op)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_particle_alloc_modify_from_client() ", ns->name);
					return -result;
				}

				append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
			}
			else {
				if ((result = as_bin_particle_stack_modify_from_client(b, particles_db, op)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_particle_stack_modify_from_client() ", ns->name);
					return -result;
				}
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op_is_read_all(op, m)) {
			for (uint16_t i = 0; i < rd->n_bins; i++) {
				as_bin *b = &rd->bins[i];

				if (! as_bin_inuse(b)) {
					break;
				}

				// ops array will not be not used in this case.
				response_bins[(*p_n_response_bins)++] = *b;
			}
		}
		else if (op->op == AS_MSG_OP_READ) {
			as_bin *b = as_bin_get_from_buf(rd, op->name, op->name_sz);

			if ((result = write_local_bin_check(tr, b)) != 0) {
				return result;
			}

			if (b) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = *b;
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			as_bin *b = as_bin_get_or_create_from_buf(rd, op->name, op->name_sz, create_only, replace_only, &result);

			if (! b) {
				return result;
			}

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if (ns->storage_data_in_memory) {
				as_bin cleanup_bin = *b;

				if ((result = as_bin_cdt_alloc_modify_from_client(b, op, &result_bin)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_cdt_alloc_modify_from_client() ", ns->name);
					return -result;
				}

				append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
			}
			else {
				if ((result = as_bin_cdt_stack_modify_from_client(b, particles_db, op, &result_bin)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_cdt_alloc_modify_from_client() ", ns->name);
					return -result;
				}
			}

			if (respond_all_ops || as_bin_inuse(&result_bin)) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}

			if (! as_bin_inuse(b)) {
				// TODO - could do better than finding index from name.
				as_bin_set_empty_shift(rd, as_bin_get_index_from_buf(rd, op->name, op->name_sz));
			}
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			as_bin *b = as_bin_get_from_buf(rd, op->name, op->name_sz);

			if ((result = write_local_bin_check(tr, b)) != 0) {
				return result;
			}

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_cdt_read_from_client(b, op, &result_bin)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_bin_cdt_read_from_client() ", ns->name);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: unknown bin op %u ", ns->name, op->op);
			return AS_PROTO_RESULT_FAIL_PARAMETER;
		}
	}

	return 0;
}

int
write_local_bin_ops(as_transaction *tr, as_storage_rd *rd,
		cf_dyn_buf *particles_db,
		as_bin *cleanup_bins, uint32_t *p_n_cleanup_bins, cf_dyn_buf *db)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;
	as_index *r = rd->r;
	bool has_read_all_op = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;

	as_msg_op *ops[m->n_ops];
	as_bin response_bins[has_read_all_op ? rd->n_bins : m->n_ops];
	as_bin result_bins[m->n_ops];

	uint32_t n_response_bins = 0;
	uint32_t n_result_bins = 0;

	int result = write_local_bin_ops_loop(tr, rd,
			ops, response_bins, &n_response_bins, result_bins, &n_result_bins,
			particles_db, cleanup_bins, p_n_cleanup_bins);

	if (result != 0) {
		destroy_stack_bins(result_bins, n_result_bins);
		return result;
	}

	if (n_response_bins == 0) {
		// If 'ordered-ops' flag was not set, and there were no read ops or CDT
		// ops with results, there's no response to build and send later.
		return 0;
	}

	as_bin *bins[n_response_bins];

	for (uint32_t i = 0; i < n_response_bins; i++) {
		as_bin *b = &response_bins[i];

		bins[i] = as_bin_inuse(b) ? b : NULL;
	}

	size_t msg_sz = 0;
	uint8_t *msgp = (uint8_t *)as_msg_make_response_msg(AS_PROTO_RESULT_OK,
			r->generation, r->void_time, has_read_all_op ? NULL : ops, bins,
			(uint16_t)n_response_bins, ns, NULL, &msg_sz, tr->trid, NULL);

	destroy_stack_bins(result_bins, n_result_bins);

	if (! msgp)	{
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed make response msg ", ns->name);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Stash the message, to be sent later.
	db->buf = msgp;
	db->is_stack = false;
	db->alloc_sz = msg_sz;
	db->used_sz = msg_sz;

	return 0;
}



//==============================================================================
// write_local() splits based on configuration - data-in-memory & single-bin.
//
// These splits handle the bin operations part of write_local() which are very
// different depending on configuration.
//

#define STACK_PARTICLES_SIZE (1024 * 1024)

//================================================
// Data-in-memory, single-bin.
//
int
write_local_dim_single_bin(as_transaction *tr, as_storage_rd *rd,
		bool record_created, bool increment_generation,
		pickle_info *pickle, cf_dyn_buf *db, bool *is_delete)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;
	as_index *r = rd->r;

	rd->n_bins = 1;

	// Set rd->bins!
	// For data-in-memory:
	// - if just created record - sets rd->bins to empty bin embedded in index
	// - otherwise - sets rd->bins to existing embedded bin
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = 0;

	if (! record_created) {
		memory_bytes = as_storage_record_get_n_bytes_memory(rd);
	}

#ifdef USE_JEM
	// Set this thread's JEMalloc arena to one used by the current namespace for long-term storage.
	int arena = as_namespace_get_jem_arena(ns->name);
	cf_debug(AS_RW, "Setting JEMalloc arena #%d for long-term storage in namespace \"%s\"", arena, ns->name);
	jem_set_arena(arena);
#endif

	//------------------------------------------------------
	// Copy existing bin into old_bin to enable unwinding.
	//

	as_bin old_bin = *rd->bins;

	// Collect bins (old or intermediate versions) to destroy on cleanup.
	as_bin cleanup_bins[m->n_ops];
	uint32_t n_cleanup_bins = 0;

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_local_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bin to write.
	//

	int result = write_local_bin_ops(tr, rd, NULL, cleanup_bins, &n_cleanup_bins, db);

	if (result != 0) {
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
		return result;
	}

	//------------------------------------------------------
	// Created the new bin to write.
	//

	// Pickle before writing - can't fail after.
	if (! pickle_all(rd, pickle)) {
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	uint64_t start_ns = g_config.microbenchmarks ? cf_getns() : 0;

	rd->write_to_device = true;

	if ((result = as_storage_record_close(r, rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_storage_record_close() ", ns->name);
		write_local_pickle_unwind(pickle);
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
		return -result;
	}

	if (g_config.microbenchmarks && start_ns) {
		histogram_insert_data_point(g_config.write_storage_close_hist, start_ns);
	}

	//------------------------------------------------------
	// Cleanup - destroy relevant bins, can't unwind after.
	//

	destroy_stack_bins(cleanup_bins, n_cleanup_bins);

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	*is_delete = ! as_bin_inuse_has(rd);

	return 0;
}


//================================================
// Data-in-memory, multi-bin.
//
int
write_local_dim(as_transaction *tr, const char *set_name, as_storage_rd *rd,
		bool record_level_replace, bool increment_generation,
		pickle_info *pickle, cf_dyn_buf *db, bool *is_delete)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;
	as_index *r = rd->r;

	// For data-in-memory - number of bins in existing record.
	rd->n_bins = as_bin_get_n_bins(r, rd);

	// Set rd->bins!
	// For data-in-memory:
	// - if just created record - sets rd->bins to NULL
	// - otherwise - sets rd->bins to existing (already populated) bins array
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(rd);

#ifdef USE_JEM
	// Set this thread's JEMalloc arena to one used by the current namespace for long-term storage.
	int arena = as_namespace_get_jem_arena(ns->name);
	cf_debug(AS_RW, "Setting JEMalloc arena #%d for long-term storage in namespace \"%s\"", arena, ns->name);
	jem_set_arena(arena);
#endif

	//------------------------------------------------------
	// Copy existing bins to new space, and keep old bins
	// intact for sindex adjustment and so it's possible to
	// unwind on failure.
	//

	uint32_t n_old_bins = (uint32_t)rd->n_bins;
	uint32_t n_new_bins = n_old_bins + m->n_ops; // can't be more than this

	size_t old_bins_size = n_old_bins * sizeof(as_bin);
	size_t new_bins_size = n_new_bins * sizeof(as_bin);

	as_bin* old_bins = rd->bins;
	as_bin new_bins[n_new_bins];

	if (old_bins_size == 0 || record_level_replace) {
		memset(new_bins, 0, new_bins_size);
	}
	else {
		memcpy(new_bins, old_bins, old_bins_size);
		memset(new_bins + n_old_bins, 0, new_bins_size - old_bins_size);
	}

	rd->n_bins = (uint16_t)n_new_bins;
	rd->bins = new_bins;

	// Collect bins (old or intermediate versions) to destroy on cleanup.
	as_bin cleanup_bins[m->n_ops];
	uint32_t n_cleanup_bins = 0;

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_local_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bins to write.
	//

	int result = write_local_bin_ops(tr, rd, NULL, cleanup_bins, &n_cleanup_bins, db);

	if (result != 0) {
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return result;
	}

	//------------------------------------------------------
	// Created the new bins to write.
	//

	// Adjust - find the actual number of new bins.
	rd->n_bins = as_bin_inuse_count(rd);
	n_new_bins = (uint32_t)rd->n_bins;
	new_bins_size = n_new_bins * sizeof(as_bin);

	as_bin_space* new_bin_space = (as_bin_space*)
			cf_malloc(sizeof(as_bin_space) + new_bins_size);

	if (! new_bin_space) {
		cf_warning(AS_RW, "write_local: failed alloc new as_bin_space");
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Pickle before writing - can't fail after.
	if (! pickle_all(rd, pickle)) {
		cf_free(new_bin_space);
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	uint64_t start_ns = g_config.microbenchmarks ? cf_getns() : 0;

	rd->write_to_device = true;

	if ((result = as_storage_record_close(r, rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_storage_record_close() ", ns->name);
		write_local_pickle_unwind(pickle);
		cf_free(new_bin_space);
		write_local_index_metadata_unwind(&old_metadata, r);
		write_local_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return -result;
	}

	if (g_config.microbenchmarks && start_ns) {
		histogram_insert_data_point(g_config.write_storage_close_hist, start_ns);
	}

	//------------------------------------------------------
	// Success - adjust sindex, looking at old and new bins.
	//

	if (as_sindex_ns_has_sindex(ns) &&
			write_local_sindex_update(ns, set_name, &tr->keyd,
					old_bins, n_old_bins, new_bins, n_new_bins)) {
		tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
	}

	//------------------------------------------------------
	// Cleanup - destroy relevant bins, can't unwind after.
	//

	if (record_level_replace) {
		destroy_stack_bins(old_bins, n_old_bins);
	}

	destroy_stack_bins(cleanup_bins, n_cleanup_bins);


	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Fill out new_bin_space.
	new_bin_space->n_bins = rd->n_bins;
	memcpy((void*)new_bin_space->bins, new_bins, new_bins_size);

	// Swizzle the index element's as_bin_space pointer.
	cf_free(as_index_get_bin_space(r));
	as_index_set_bin_space(r, new_bin_space);

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
		// TODO - should we check allocation failure?
		as_record_allocate_key(r, rd->key, rd->key_size);
		as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	*is_delete = ! as_bin_inuse_has(rd);

	return 0;
}


//================================================
// Data-not-in-memory, single-bin.
//
int
write_local_ssd_single_bin(as_transaction *tr, as_storage_rd *rd,
		bool must_fetch_data, bool increment_generation,
		pickle_info *pickle, cf_dyn_buf *db, bool *is_delete)
{
	// Shortcut pointers.
	as_namespace *ns = tr->rsv.ns;
	as_index *r = rd->r;

	rd->ignore_record_on_device = ! must_fetch_data;
	rd->n_bins = 1;

	as_bin stack_bin;

	// Set rd->bins!
	// For non-data-in-memory:
	// - if just created record, or must_fetch_data is false - sets rd->bins to
	//		empty stack_bin
	// - otherwise - sets rd->bins to stack_bin, reads existing record off
	//		device and populates bin (including particle pointer into block
	//		buffer)
	int result = as_storage_rd_load_bins(rd, &stack_bin);

	if (result < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_storage_rd_load_bins()", ns->name);
		return -result;
	}

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_local_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bin to write.
	//

	cf_dyn_buf_define_size(particles_db, STACK_PARTICLES_SIZE);

	if ((result = write_local_bin_ops(tr, rd, &particles_db, NULL, NULL, db)) != 0) {
		cf_dyn_buf_free(&particles_db);
		write_local_index_metadata_unwind(&old_metadata, r);
		return result;
	}

	//------------------------------------------------------
	// Created the new bin to write.

	// Pickle before writing - bins may disappear on as_storage_record_close().
	if (! pickle_all(rd, pickle)) {
		cf_dyn_buf_free(&particles_db);
		write_local_index_metadata_unwind(&old_metadata, r);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	uint64_t start_ns = g_config.microbenchmarks ? cf_getns() : 0;

	rd->write_to_device = true;

	int write_result = as_storage_record_close(r, rd);

	if (write_result < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_storage_record_close() ", ns->name);
		write_local_pickle_unwind(pickle);
		cf_dyn_buf_free(&particles_db);
		write_local_index_metadata_unwind(&old_metadata, r);
		return -write_result;
	}

	if (g_config.microbenchmarks && start_ns) {
		histogram_insert_data_point(g_config.write_storage_close_hist, start_ns);
	}

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
		as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	*is_delete = ! as_bin_inuse_has(rd);
	cf_dyn_buf_free(&particles_db);

	return 0;
}


//================================================
// Data-not-in-memory, multi-bin.
//
int
write_local_ssd(as_transaction *tr, const char *set_name, as_storage_rd *rd,
		bool must_fetch_data, bool record_level_replace, bool increment_generation,
		pickle_info *pickle, cf_dyn_buf *db, bool *is_delete)
{
	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;
	as_index *r = rd->r;
	bool has_sindex = as_sindex_ns_has_sindex(ns);

	// If it's not touch or modify, determine if we must read existing record.
	if (! must_fetch_data) {
		must_fetch_data = has_sindex || ! record_level_replace;
	}

	rd->ignore_record_on_device = ! must_fetch_data;

	// For non-data-in-memory:
	// - if just created record, or must_fetch_data is false - 0
	// - otherwise - number of bins in existing record
	rd->n_bins = as_bin_get_n_bins(r, rd);

	uint32_t n_old_bins = (uint32_t)rd->n_bins;
	uint32_t n_new_bins = n_old_bins + m->n_ops; // can't be more than this

	// Needed for as_storage_rd_load_bins() to clear all unused bins.
	rd->n_bins = (uint16_t)n_new_bins;

	// Stack space for resulting record's bins.
	as_bin old_bins[n_old_bins];
	as_bin new_bins[n_new_bins];

	// Set rd->bins!
	// For non-data-in-memory:
	// - if just created record, or must_fetch_data is false - sets rd->bins to
	//		empty new_bins
	// - otherwise - sets rd->bins to new_bins, reads existing record off device
	//		and populates bins (including particle pointers into block buffer)
	int result = as_storage_rd_load_bins(rd, new_bins);

	if (result < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_storage_rd_load_bins()", ns->name);
		return -result;
	}

	//------------------------------------------------------
	// Copy old bins (if any) - which are currently in new
	// bins array - to old bins array, for sindex purposes.
	//

	if (has_sindex && n_old_bins != 0) {
		memcpy(old_bins, new_bins, n_old_bins * sizeof(as_bin));
	}

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_local_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bins to write.
	//

	cf_dyn_buf_define_size(particles_db, STACK_PARTICLES_SIZE);

	if ((result = write_local_bin_ops(tr, rd, &particles_db, NULL, NULL, db)) != 0) {
		cf_dyn_buf_free(&particles_db);
		write_local_index_metadata_unwind(&old_metadata, r);
		return result;
	}

	//------------------------------------------------------
	// Created the new bins to write.
	//

	// Adjust - find the actual number of new bins.
	rd->n_bins = as_bin_inuse_count(rd);
	n_new_bins = (uint32_t)rd->n_bins;

	// Pickle before writing - bins may disappear on as_storage_record_close().
	if (! pickle_all(rd, pickle)) {
		cf_dyn_buf_free(&particles_db);
		write_local_index_metadata_unwind(&old_metadata, r);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	uint64_t start_ns = g_config.microbenchmarks ? cf_getns() : 0;

	rd->write_to_device = true;

	if ((result = as_storage_record_close(r, rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: failed as_storage_record_close() ", ns->name);
		write_local_pickle_unwind(pickle);
		cf_dyn_buf_free(&particles_db);
		write_local_index_metadata_unwind(&old_metadata, r);
		return -result;
	}

	if (g_config.microbenchmarks && start_ns) {
		histogram_insert_data_point(g_config.write_storage_close_hist, start_ns);
	}

	//------------------------------------------------------
	// Success - adjust sindex, looking at old and new bins.
	//

	if (has_sindex &&
			write_local_sindex_update(ns, set_name, &tr->keyd,
					old_bins, n_old_bins, new_bins, n_new_bins)) {
		tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
	}

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
		as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	*is_delete = ! as_bin_inuse_has(rd);
	cf_dyn_buf_free(&particles_db);

	return 0;
}



//==============================================================================
// write_local()
//
// Currently only writes from the client, or proxies, come through here. Replica
// writes and migration writes come through write_local_pickled().
//

int
write_local(as_transaction *tr, write_local_generation *wlg,
		uint8_t **pickled_buf, size_t *pickled_sz,
		as_rec_props *p_pickled_rec_props, cf_dyn_buf *db)
{
	//------------------------------------------------------
	// Perform checks that don't need to loop over ops, or
	// create or find (and lock) the as_index.
	//

	bool is_done = false;
	int result = write_local_preprocessing(tr, wlg, &is_done);

	if (is_done) {
		return result;
	}

	//------------------------------------------------------
	// Loop over ops to set some essential policy flags.
	//

	bool must_not_create;
	bool record_level_replace;
	bool must_fetch_data;
	bool increment_generation;

	if (0 != (result = write_local_policies(tr, &must_not_create,
			&record_level_replace, &must_fetch_data, &increment_generation))) {
		write_local_failed(tr, 0, false, 0, 0, result);
		return -1;
	}

	//------------------------------------------------------
	// Find or create the as_index and get a reference -
	// this locks the record. Perform all checks that don't
	// need the as_storage_rd.
	//

	// Shortcut pointers.
	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;

	// Use the appropriate partition tree.
	as_index_tree *tree = tr->rsv.tree;

	if (ns->ldt_enabled && (tr->flag & AS_TRANSACTION_FLAG_LDT_SUB)) {
		tree = tr->rsv.sub_tree;
	}

	// Find or create as_index, populate as_index_ref, lock record.
	as_index_ref r_ref;
	r_ref.skip_lock = false;
	as_index *r = NULL;
	bool record_created = false;

	if (must_not_create) {
		if (0 != as_record_get(tree, &tr->keyd, &r_ref, ns)) {
			write_local_failed(tr, 0, record_created, tree, 0, AS_PROTO_RESULT_FAIL_NOTFOUND);
			return -1;
		}

		r = r_ref.r;

		if (as_record_is_expired(r)) {
			write_local_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_NOTFOUND);
			return -1;
		}
	}
	else {
		int rv = as_record_get_create(tree, &tr->keyd, &r_ref, ns, false);

		if (rv < 0) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: fail as_record_get_create() ", ns->name);
			write_local_failed(tr, 0, record_created, tree, 0, AS_PROTO_RESULT_FAIL_UNKNOWN);
			return -1;
		}

		r = r_ref.r;
		record_created = rv == 1;

		// If it's an expired record, pretend it's a fresh create.
		if (! record_created && as_record_is_expired(r)) {
			as_record_destroy(r, ns);
			as_record_initialize(&r_ref, ns);
			cf_atomic_int_incr(&ns->n_objects);
			record_created = true;
		}
	}

	// Enforce record-level create-only existence policy.
	if ((m->info2 & AS_MSG_INFO2_CREATE_ONLY) && ! record_created) {
		write_local_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_RECORD_EXISTS);
		return -1;
	}

	// Check generation requirement, if any.
	if (! g_config.generation_disable &&
			(((m->info2 & AS_MSG_INFO2_GENERATION) && m->generation != r->generation) ||
			 ((m->info2 & AS_MSG_INFO2_GENERATION_GT) && m->generation <= r->generation))) {
		write_local_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_GENERATION);
		return -1;
	}

	// If creating record, write set-ID into index.
	if (record_created) {
		int rv_set = as_record_set_set_from_msg(r, ns, m);

		if (rv_set == -1) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_local: set can't be added ", ns->name);
			write_local_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
			return -1;
		}
		else if (rv_set == -2) {
			write_local_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_FORBIDDEN);
			return -1;
		}
	}

	// Shortcut set name.
	const char* set_name = as_index_get_set_name(r, ns);

	// If record existed, check that as_msg set name matches.
	if (! record_created && ! check_msg_set_name(m, set_name)) {
		write_local_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
		return -1;
	}

	//------------------------------------------------------
	// Open or create the as_storage_rd, and handle record
	// metadata.
	//

	as_storage_rd rd;

	if (record_created) {
		as_storage_record_create(ns, r, &rd, &tr->keyd);
	}
	else {
		as_storage_record_open(ns, r, &rd, &tr->keyd);
	}

	// Deal with key storage as needed.
	if (0 != (result = write_local_handle_msg_key(tr, &rd))) {
		write_local_failed(tr, &r_ref, record_created, tree, &rd, result);
		return -1;
	}

	bool was_ldt_parent = false;

	// Record-level replace can't maintain an LDT.
	if (ns->ldt_enabled && as_ldt_record_is_parent(r) && record_level_replace) {
		as_index_clear_flags(r, AS_INDEX_FLAG_SPECIAL_BINS);
		was_ldt_parent = true; // so we can unwind
	}

	// Assemble record properties from index information.
	size_t rec_props_data_size = as_storage_record_rec_props_size(&rd);
	uint8_t rec_props_data[rec_props_data_size];

	if (rec_props_data_size > 0) {
		as_storage_record_set_rec_props(&rd, rec_props_data);
	}

	//------------------------------------------------------
	// Split write_local() according to configuration to
	// handle record bins.
	//

	pickle_info pickle;
	bool is_delete;

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			result = write_local_dim_single_bin(tr, &rd,
					record_created, increment_generation,
					&pickle, db, &is_delete);
		}
		else {
			result = write_local_dim(tr, set_name, &rd,
					record_level_replace, increment_generation,
					&pickle, db, &is_delete);
		}
	}
	else {
		if (ns->single_bin) {
			result = write_local_ssd_single_bin(tr, &rd,
					must_fetch_data, increment_generation,
					&pickle, db, &is_delete);
		}
		else {
			result = write_local_ssd(tr, set_name, &rd,
					must_fetch_data, record_level_replace, increment_generation,
					&pickle, db, &is_delete);
		}
	}

	if (result != 0) {
		if (was_ldt_parent) {
			as_index_set_flags(r, AS_INDEX_FLAG_SPECIAL_BINS);
		}

		write_local_failed(tr, &r_ref, record_created, tree, &rd, result);
		return -1;
	}

	//------------------------------------------------------
	// Done - gather function's output, release the record
	// lock, and do XDR write if appropriate.
	//

	*pickled_buf = pickle.buf;
	*pickled_sz = pickle.buf_size;
	p_pickled_rec_props->p_data = pickle.rec_props_data;
	p_pickled_rec_props->size = pickle.rec_props_size;

	tr->generation = r->generation;
	tr->void_time = r->void_time;

	// Get set-id before releasing.
	uint16_t set_id = as_index_get_set_id(r_ref.r);

	// If we ended up with no bins, delete the record.
	if (is_delete) {
		as_index_delete(tree, &tr->keyd);
		cf_atomic_int_incr(&g_config.stat_delete_success);
	}
	// Or (normally) adjust max void-times.
	else if (r->void_time != 0) {
		cf_atomic_int_setmax( &ns->max_void_time, r->void_time);
		cf_atomic_int_setmax( &tr->rsv.p->max_void_time, r->void_time);
	}

	as_record_done(&r_ref, ns);

	// Don't send an XDR delete if it's disallowed.
	if (is_delete && ! g_config.xdr_cfg.xdr_delete_shipping_enabled) {
		return 0;
	}

	// Do an XDR write if the write is a non-XDR write or is an XDR write with
	// forwarding enabled.
	if ((m->info1 & AS_MSG_INFO1_XDR) == 0 ||
			g_config.xdr_cfg.xdr_forward_xdrwrites ||
			ns->ns_forward_xdr_writes) {
		xdr_write(ns, tr->keyd, tr->generation, 0, is_delete, set_id);
	}

	return 0;
}

//
// End write_local()
//==============================================================================



typedef struct {
	as_namespace_id ns_id;
	as_partition_id part_id;
} __attribute__ ((__packed__)) journal_hash_key;

uint32_t journal_hash_fn(void *value) {
	journal_hash_key *jhk = (journal_hash_key *) value;
	return ((uint32_t) jhk->part_id);
}

typedef struct {
	as_namespace *ns; // reference count held
	as_partition_id part_id; // won't vanish as long as ns refcount ok
	cf_digest digest; // where to find the data
	cl_msg *msgp; // data to write
	bool delete; // or a delete flag if it's a delete
	write_local_generation wlg;
	bool is_subrec;
} journal_queue_element;

//
// The journal hash has a journal_hash_key as its key - a namespace_id and partition_id
// and a queue as a value. That queue holds journal queue elements.
// Use of the hash must be covered by the journal_lock
//

shash *journal_hash = 0;
pthread_mutex_t journal_lock = PTHREAD_MUTEX_INITIALIZER;

//
// Write a record to the journal for later application
//
// This is called in the same code path as write_local, and write_local
// consumes nothing. You can't even be sure that some of these pointers
// (like the proto) is really a malloc/free pointer.
// So although it hurts, take a copy

int write_journal(as_transaction *tr, write_local_generation *wlg) {
	cf_detail(AS_RW, "write to journal: %"PRIx64, *(uint64_t*)&tr->keyd);

	if (!journal_hash)
		return (0);

	journal_queue_element jqe;
	jqe.ns = tr->rsv.ns;
	jqe.part_id = tr->rsv.pid;
	jqe.digest = tr->keyd;
	jqe.msgp = cf_malloc(sizeof(as_proto) + tr->msgp->proto.sz);
	memcpy(jqe.msgp, tr->msgp, sizeof(as_proto) + tr->msgp->proto.sz);
	jqe.delete = false;
	jqe.is_subrec = false;
	jqe.wlg = *wlg;

	journal_hash_key jhk;
	jhk.ns_id = jqe.ns->id;
	jhk.part_id = tr->rsv.pid;

	cf_queue *j_q;

#ifdef JOURNAL_HASH_CHECKING
	{
		as_msg *msgp = (as_msg *)jqe.msgp;
		as_msg_op *op = 0;
		char stupidbufk[128], stupidbufv[128];
		int i = 0;

		as_msg_key *kfp;
		kfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_KEY);
		memset(stupidbufk, 0, 128);
		memcpy(stupidbufk, kfp->key, 11);

		fprintf(stderr, "J key: %s\n", stupidbufk);

		while ((op = as_msg_op_iterate(msgp, op, &i))) {
			if (AS_MSG_OP_WRITE != op->op)
				continue;

			cf_digest d;
			memset(stupidbufv, 0, 128);
			stupidbufv[0] = 3;
			memcpy(stupidbufv + sizeof(uint8_t), as_msg_op_get_value_p(op), as_msg_op_get_value_sz(op));
			cf_digest_compute((void *)stupidbufv, 11, &d);
			fprintf(stderr, "J write: %"PRIx64" %"PRIx64" %d %s\n", *(uint64_t *)&jqe.digest, *(uint64_t *)&d, as_msg_op_get_value_sz(op), stupidbufv);
		}
	}
#endif

	pthread_mutex_lock(&journal_lock);
	if (SHASH_OK != shash_get(journal_hash, &jhk, &j_q)) {
		if (jqe.msgp) {
			cf_free(jqe.msgp);
		}
		pthread_mutex_unlock(&journal_lock);
		return (0);
	}

	cf_queue_push(j_q, &jqe);

	pthread_mutex_unlock(&journal_lock);

	return (0);
}

//
// Write into the journal a "delete" element
//

int write_delete_journal(as_transaction *tr, bool is_subrec) {
	cf_detail(AS_RW, "write to delete journal: %"PRIx64"", *(uint64_t*)&tr->keyd);

	if (journal_hash == 0)
		return (0);

	journal_queue_element jqe;
	jqe.ns = tr->rsv.ns;
	jqe.part_id = tr->rsv.pid;
	jqe.digest = tr->keyd;
	jqe.msgp = 0;
	jqe.delete = true;
	jqe.is_subrec = is_subrec;

	journal_hash_key jhk;
	jhk.ns_id = jqe.ns->id;
	jhk.part_id = jqe.part_id;

	cf_queue *j_q;

	pthread_mutex_lock(&journal_lock);

	if (SHASH_OK != shash_get(journal_hash, &jhk, &j_q)) {
		pthread_mutex_unlock(&journal_lock);
		return (0);
	}

	cf_queue_push(j_q, &jqe);

	pthread_mutex_unlock(&journal_lock);

	return (0);
}

//
// Opens a given journal for business
//

int as_write_journal_start(as_namespace *ns, as_partition_id pid) {
	cf_detail(AS_RW, "write journal start! {%s:%d}", ns->name, (int)pid);

	pthread_mutex_lock(&journal_lock);
	if (journal_hash == 0) {
		// Note - A non-lock hash table because we always use it under the journal lock
		shash_create(&journal_hash, journal_hash_fn, sizeof(journal_hash_key),
				sizeof(cf_queue *), 1024, 0);
	}
	journal_hash_key jhk;
	jhk.ns_id = ns->id;
	jhk.part_id = pid;

	cf_queue *journal_q;

	// if there's another journal stored, clean it
	if (SHASH_OK == shash_get_and_delete(journal_hash, &jhk, &journal_q)) {
		cf_debug(AS_RW,
				" warning: journal_start with journal already existing {%s:%d}",
				ns, (int)pid);
		journal_queue_element jqe;
		while (0 == cf_queue_pop(journal_q, &jqe, CF_QUEUE_FOREVER)) {
			if (jqe.msgp) {
				cf_free(jqe.msgp);
			}
		}
		cf_queue_destroy(journal_q);
	}

	journal_q = cf_queue_create(sizeof(journal_queue_element), false);
	if (SHASH_OK != shash_put_unique(journal_hash, &jhk, (void *) &journal_q)) {
		cf_queue_destroy(journal_q);
		pthread_mutex_unlock(&journal_lock);
		cf_debug(AS_RW,
				" warning: write_start_journal on already started journal, {%s:%d}, error",
				ns->name, (int)pid);
		return (-1);
	}
	pthread_mutex_unlock(&journal_lock);

	return (0);
}

//
// Applies the journal on a particular namespace - and removes the journal
//
// NB you must hold the partition state lock when you apply the journal
int as_write_journal_apply(as_partition_reservation *prsv) {
	cf_debug(AS_RW, "write journal apply! {%s:%d}",
			prsv->ns->name, (int)prsv->pid);

	pthread_mutex_lock(&journal_lock);
	if (!journal_hash) {
		cf_debug(AS_RW, "[NOTE] calling journal apply with no starts! {%s:%d}", prsv->ns->name, (int)prsv->pid);
		pthread_mutex_unlock(&journal_lock);
		return (-1);
	}
	journal_hash_key jhk;
	jhk.ns_id = prsv->ns->id;
	jhk.part_id = prsv->pid;
	cf_queue *journal_q;
	if (SHASH_OK != shash_get_and_delete(journal_hash, &jhk, &journal_q)) {
		cf_warning(AS_RW,
				" warning: journal_apply on non-existant journal {%s:%d}",
				prsv->ns->name, (int)prsv->pid);
		pthread_mutex_unlock(&journal_lock);
		return (-1);
	}
	pthread_mutex_unlock(&journal_lock);

	// got the queue, got the journal, use it
	journal_queue_element jqe;
	while (0 == cf_queue_pop(journal_q, &jqe, CF_QUEUE_NOWAIT)) {
		// INIT_TR
		as_transaction tr;
		as_transaction_init(&tr, &jqe.digest, jqe.msgp);
		as_partition_reservation_copy(&tr.rsv, prsv);
		tr.rsv.is_write = true;
		tr.rsv.state = AS_PARTITION_STATE_JOURNAL_APPLY; // doesn't matter
#ifdef JOURNAL_HASH_CHECKING
		{
			as_msg *msgp = (as_msg *)jqe.proto;
			as_msg_op *op = 0;
			char stupidbufk[128], stupidbufv[128];
			int i = 0;

			as_msg_key *kfp;
			kfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_KEY);
			memset(stupidbufk, 0, 128);
			memcpy(stupidbufk, kfp->key, 11);

			fprintf(stderr, "Ja key: %s\n", stupidbufk);

			while ((op = as_msg_op_iterate(msgp, op, &i))) {
				if (AS_MSG_OP_WRITE != op->op)
					continue;

				cf_digest d;
				memset(stupidbufv, 0, 128);
				stupidbufv[0] = 3;
				memcpy(stupidbufv + sizeof(uint8_t), as_msg_op_get_value_p(op), as_msg_op_get_value_sz(op));
				cf_digest_compute((void *)stupidbufv, 11, &d);
				fprintf(stderr, "Ja write: %"PRIx64" %"PRIx64" %d %s\n", *(uint64_t *)&jqe.digest, *(uint64_t *)&d, as_msg_op_get_value_sz(op), stupidbufv);
			}
		}
#endif

		int rv;
		if (jqe.is_subrec) {
			tr.flag |= AS_TRANSACTION_FLAG_LDT_SUB;
		}
		if (jqe.delete == true)
			rv = write_delete_local(&tr, false, 0, false);
		else
			rv = write_local(&tr, &jqe.wlg, 0, 0, 0, 0);

		cf_detail(AS_RW, "write journal: wrote: rv %d key %"PRIx64,
				rv, *(uint64_t *)&tr.keyd);

		if (jqe.msgp) {
			cf_free(jqe.msgp);
		}
	}

	cf_queue_destroy(journal_q);

	return (0);
}

int
write_process_op(as_transaction *tr, cl_msg *msgp, cf_node node, as_generation generation)
{
	as_namespace *ns = tr->rsv.ns;
	int rv = 0;
	if (msgp->msg.info2 & AS_MSG_INFO2_DELETE) {
		rv = write_delete_local(tr, true, node, false);
	} else {
		cf_crash_digest(AS_RW, &tr->keyd, "replica write trying to use write_local()");
	}

	if (rv == 0) {
		tr->result_code = 0;
	} else {
		// TODO: Handle case when its a Replace operation in write_local,
		// it also returns AS_PROTO_RESULT_FAIL_NOTFOUND
		if ((tr->result_code == AS_PROTO_RESULT_FAIL_NOTFOUND)
				&& (msgp->msg.info2 & AS_MSG_INFO2_DELETE)) {
			cf_atomic_int_incr(&g_config.err_write_fail_prole_delete);
		} else {
			cf_info(AS_RW,
					"rw prole operation: failed, ns %s rv %d result code %d, "
					"digest %"PRIx64"",
					ns->name, rv, tr->result_code, *(uint64_t*)&tr->keyd);
		}
	}

	return tr->result_code;
}

int
write_process_new(cf_node node, msg *m, as_partition_reservation *rsvp, bool f_respond, ldt_prole_info *linfo)
{
	uint32_t result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
	bool local_reserve = false;

	cf_digest *keyd;
	size_t sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_DIGEST, (byte **) &keyd, &sz,
			MSG_GET_DIRECT)) {
		cf_debug(AS_RW, "write process received message with out digest");
		cf_atomic_int_incr(&g_config.rw_err_write_internal);
		goto Out;
	}

	uint64_t cluster_key;
	if (0 != msg_get_uint64(m, RW_FIELD_CLUSTER_KEY, &cluster_key)) {
		cf_warning(AS_RW, "write process received message without cluster key");
		cf_atomic_int_incr(&g_config.rw_err_write_internal);
		goto Out;
	}

	as_generation generation = 0;
	if (0 != msg_get_uint32(m, RW_FIELD_GENERATION, &generation)) {
		goto Out;
	}

	cl_msg *msgp = 0;
	size_t msgp_sz = 0;
	uint8_t *pickled_buf = NULL;
	size_t pickled_sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_AS_MSG, (byte **) &msgp, &msgp_sz,
			MSG_GET_DIRECT)) {

		pickled_sz = 0;
		if (0 != msg_get_buf(m, RW_FIELD_RECORD, (byte **) &pickled_buf,
				&pickled_sz, MSG_GET_DIRECT)) {

			cf_debug(AS_RW,
					"write process received message without AS MSG or RECORD");
			cf_atomic_int_incr(&g_config.rw_err_write_internal);
			goto Out;
		}
	}


	as_rec_props rec_props;
	if (0 != msg_get_buf(m, RW_FIELD_REC_PROPS, (byte **) &rec_props.p_data,
			(size_t*) &rec_props.size, MSG_GET_DIRECT)) {
		cf_debug(AS_RW,
				"write process received message without record properties");
	}

	int missing_fields = 0;

	uint32_t void_time = 0;
	if (0 != msg_get_uint32(m, RW_FIELD_VOID_TIME, &void_time)) {
		cf_warning(AS_RW, "write process received message without void_time");
		missing_fields++;
	}

	uint32_t info = 0;
	if (0 != msg_get_uint32(m, RW_FIELD_INFO, &info)) {
		cf_warning(AS_RW, "write process received message without info field");
		missing_fields++;
	}

	if (missing_fields) {
		cf_warning(AS_RW,
				"write process received message with %d missing fields ~~ returing result fail unknown",
				missing_fields);
		result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		goto Out;
	}

	as_partition_reservation rsv;
	if (!rsvp) {
		uint8_t *ns_name = 0;
		size_t ns_name_len;
		if (0 != msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
				MSG_GET_DIRECT)) {
			cf_info(AS_RW, "write process: no namespace");
			cf_atomic_int_incr(&g_config.rw_err_dup_internal);
			goto Out;
		}

		as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);
		if (!ns) {
			cf_info(AS_RW, "get abort invalid namespace received");
			cf_atomic_int_incr(&g_config.rw_err_dup_internal);
			goto Out;
		}

		as_partition_reserve_migrate(ns, as_partition_getid(*keyd), &rsv, 0);
		cf_atomic_int_incr(&g_config.wprocess_tree_count);
		local_reserve = true;
		rsvp = &rsv;
	}

	as_namespace *ns = rsvp->ns;

	if (info & RW_INFO_LDT) {
		memset(linfo, 1, sizeof(ldt_prole_info));
		if  (as_rw_get_ldt_info(linfo, m, rsvp)) {
			cf_warning(AS_LDT, "Could not find ldt info at prole");
			return AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	if (as_ldt_check_and_get_prole_version(keyd, rsvp, linfo, info, NULL, false, __FILE__, __LINE__)) {
		// If parent cannot be due to incoming migration it is ok
		// continue and allow subrecords to be replicated
		result_code = AS_PROTO_RESULT_OK;
		goto Out;
	}

	// Set the version in subrec digest if need be.
	as_ldt_set_prole_subrec_version(keyd, rsvp, linfo, info);

	if (info & RW_INFO_UDF_WRITE) {
		cf_atomic_int_incr(&g_config.udf_replica_writes);
	}

	// Two ways to tell a prole to write something -
	// a) By operation shipping means that the prole has been passed the input
	// message, and needs to process it. Following cases use it right now
	// - Delete
	// - Single Bin Operation
	// - Write (does not use it because it is not idempotent)
	//
	// b) By data shipping means that the new record (in which case it writes
	// the entire record) is sent and the node simply overwrites it.
	int rv = 0;
	if (msgp) {
		// INIT_TR
		as_transaction tr;
		as_transaction_init(&tr, keyd, msgp);

		tr.rsv = *rsvp;

		// Check here if this is prole delete caused by nsup
		// If yes we need to tell XDR not to ship the delete
		if (info & RW_INFO_NSUP_DELETE) {
			tr.flag |= AS_TRANSACTION_FLAG_NSUP_DELETE;
		}

		if (ns->ldt_enabled) {
			if ((info & RW_INFO_LDT_SUBREC)
					|| (info & RW_INFO_LDT_ESR)) {
				tr.flag |= AS_TRANSACTION_FLAG_LDT_SUB;
				cf_detail_digest(AS_RW, keyd,
						"LDT Subrecord Replication Request Received ");
			}
		}

		result_code = write_process_op(&tr, msgp, node, generation);
	} else {
		rv = write_local_pickled(keyd, rsvp, pickled_buf, pickled_sz,
				&rec_props, generation, void_time, node, info, linfo);
		if (rv == 0) {
			result_code = AS_PROTO_RESULT_OK;
		} else {
			result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
			cf_info(AS_RW, "Writing pickled failed %d for digest %"PRIx64,
					rv, *(uint64_t *) keyd);
		}
	}

	if (rsvp->ns->ldt_enabled) {
		if ((info & RW_INFO_LDT_SUBREC)
				|| (info & RW_INFO_LDT_ESR)) {
			cf_detail(AS_RW,
					"MULTI_OP: Subrecord Replication Request Processed %"PRIx64" rv=%d",
					*(uint64_t * )keyd, result_code);
		}
	}

Out:
	if (local_reserve) {
		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.wprocess_tree_count);
	}

	if (result_code != AS_PROTO_RESULT_OK) {
		if (result_code == AS_PROTO_RESULT_FAIL_GENERATION) {
			cf_atomic_int_incr(&g_config.err_write_fail_prole_generation);
		} else if (result_code == AS_PROTO_RESULT_FAIL_UNKNOWN) {
			cf_atomic_int_incr(&g_config.err_write_fail_prole_unknown);
		}
	}

	msg_set_unset(m, RW_FIELD_AS_MSG);
	msg_set_unset(m, RW_FIELD_RECORD);
	msg_set_unset(m, RW_FIELD_REC_PROPS);
	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result_code);

	if (f_respond) {
		uint64_t start_ns = 0;
		if (g_config.microbenchmarks) {
			start_ns = cf_getns();
		}
		int rv2 = as_fabric_send(node, m, AS_FABRIC_PRIORITY_MEDIUM);
		if (g_config.microbenchmarks && start_ns) {
			histogram_insert_data_point(g_config.prole_fabric_send_hist,
					start_ns);
		}

		if (rv2 != 0) {
			cf_debug(AS_RW, "write process: send fabric message bad return %d",
					rv2);
			as_fabric_msg_put(m);
			cf_atomic_int_incr(&g_config.rw_err_write_send);
		}
	}

	return (0);
}

//
// Incoming messages start here
// * could get a request that we need to service
// * could get a response to one of our requests - need to find the request
//   and send the real response to the remote end
//
int
write_msg_fn(cf_node id, msg *m, void *udata)
{
	uint32_t op = 99999;
	if (0 != msg_get_uint32(m, RW_FIELD_OP, &op)) {
		cf_warning(AS_RW,
				"write_msg_fn received message without operation field");
	}

	switch (op) {

	case RW_OP_WRITE: {
		uint64_t start_ns = 0;

		if (g_config.microbenchmarks) {
			start_ns = cf_getns();
		}

		cf_atomic_int_incr(&g_config.write_prole);

		write_process(id, m, true);

		if (g_config.microbenchmarks && start_ns) {
			histogram_insert_data_point(g_config.wt_prole_hist, start_ns);
		}

		break;
	}

	case RW_OP_WRITE_ACK:

		// different path if we're using async replication
		if (g_config.replication_fire_and_forget) {
			rw_replicate_process_ack(id, m, true);
		} else {
			rw_process_ack(id, m, true);
		}

		break;

	case RW_OP_DUP:

		cf_atomic_int_incr(&g_config.read_dup_prole);

		rw_dup_process(id, m);

		break;

	case RW_OP_DUP_ACK:

		rw_process_ack(id, m, false);

		break;

	case RW_OP_MULTI:
	{
		uint64_t start_ns = 0;
		if (g_config.ldt_benchmarks) {
			start_ns = cf_getns();
		}
		cf_detail(AS_RW, "MULTI_OP: Received Multi Op Request");
		rw_multi_process(id, m);
		cf_detail(AS_RW, "MULTI_OP: Processed Multi Op Request");

		if (g_config.ldt_benchmarks && start_ns) {
			histogram_insert_data_point(g_config.ldt_multiop_prole_hist, start_ns);
		}

		break;
	}

	case RW_OP_MULTI_ACK:

		cf_detail(AS_RW, "MULTI_OP: Received Multi Op Ack");
		rw_process_ack(id, m, true);
		cf_detail(AS_RW, "MULTI_OP: Processed Multi Op Ack");

		break;

	default:
		cf_debug(AS_RW,
				"write_msg_fn: received unknown, unsupported message %d from remote endpoint",
				op);
		as_fabric_msg_put(m);
		break;
	}

	return (0);
} // end write_msg_fn()

// Helper function used to clean up a tr or wr proto_fd_h in a number of places.
static void release_proto_fd_h(as_file_handle *proto_fd_h) {
	shutdown(proto_fd_h->fd, SHUT_RDWR);
	proto_fd_h->inuse = false;
	AS_RELEASE_FILE_HANDLE(proto_fd_h);
}

typedef struct now_times_s {
	uint64_t now_ns;
	uint64_t now_ms;
} now_times;

int
rw_retransmit_reduce_fn(void *key, uint32_t keylen, void *data, void *udata)
{
	write_request *wr = data;
	now_times *p_now = (now_times*)udata;

	if (p_now->now_ns > cf_atomic64_get(wr->end_time)) {

		if (!wr->ready) {
			cf_detail(AS_RW, "Timing out never-ready wr %p", wr);

			if (!(wr->proto_fd_h)) {
				cf_detail(AS_RW, "No proto_fd_h on wr %p", wr);
			}
		}

		cf_atomic_int_incr(&g_config.stat_rw_timeout);
		if (wr->ready) {
			if ( ! wr->has_udf ) {
				cf_hist_track_insert_data_point(g_config.wt_hist, wr->start_time);
			}
		}

		pthread_mutex_lock(&wr->lock);
		if (udf_rw_needcomplete_wr(wr)) {
			as_transaction tr;
			write_request_init_tr(&tr, wr);
			udf_rw_complete(&tr, AS_PROTO_RESULT_FAIL_TIMEOUT, __FILE__, __LINE__);
			if (tr.proto_fd_h) {
				AS_RELEASE_FILE_HANDLE(tr.proto_fd_h);
			}
		} else {
			if (wr->batch_shared) {
				as_batch_add_error(wr->batch_shared, wr->batch_index, AS_PROTO_RESULT_FAIL_TIMEOUT);
			}
			else {
				if (wr->proto_fd_h) {
					release_proto_fd_h(wr->proto_fd_h);
					wr->proto_fd_h = 0;
				}
			}
		}
		pthread_mutex_unlock(&wr->lock);

		return (RCHASH_REDUCE_DELETE);
	}

	// cases where the wr is inserted in the table but all structures are not ready yet
	if (wr->ready == false) {
		return (0);
	}

	if (wr->xmit_ms < p_now->now_ms) {

		bool finished = false;

		pthread_mutex_lock(&wr->lock);
		if (wr->rsv.n_dupl > 0)
			cf_debug(AS_RW,
					"{%s:%d} rw retransmit reduce fn: RETRANSMITTING %"PRIx64" %s",
					wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");
		else
			cf_debug(AS_RW,
					"{%s:%d} rw retransmit reduce fn: RETRANSMITTING %"PRIx64" %s",
					wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");

		wr->xmit_ms = p_now->now_ms + wr->retry_interval_ms;
		wr->retry_interval_ms *= 2;

		WR_TRACK_INFO(wr, "rw_retransmit_reduce_fn: retransmitting ");
		send_messages(wr);
		// No such ack processing for the shipped_op initiator. The request
		// will get processed when the response for the shipped operation is
		// finished
		if (!wr->shipped_op_initiator) {
			finished = finish_rw_process_ack(wr, AS_PROTO_RESULT_OK);
		} else {
			cf_debug(AS_LDT, "Skipping process ack for LDT ship op initiator");
		}
		pthread_mutex_unlock(&wr->lock);

		if (finished == true) {
			if (wr->rsv.n_dupl > 0)
				cf_debug(AS_RW,
						"{%s:%d} rw retransmit reduce fn: DELETING request %"PRIx64" %s",
						wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");
			else
				cf_debug(AS_RW,
						"{%s:%d} rw retransmit reduce fn: DELETING request %"PRIx64" %s",
						wr->rsv.ns->name, wr->rsv.pid, *(uint64_t *) & (wr->keyd), wr->is_read ? "READ" : "WRITE");
			WR_TRACK_INFO(wr, "rw_retransmit_reduce_fn: deleting ");
			return (RCHASH_REDUCE_DELETE);
		}
	}

	return (0);
} // end rw_retransmit_reduce_fn()

void *
rw_retransmit_fn(void *unused)
{
	while (1) {

		usleep(130 * 1000);

		now_times now;
		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		rchash_reduce(g_write_hash, rw_retransmit_reduce_fn, &now);

#ifdef DEBUG
		// SUPER DEBUG --- catching some kind of leak of rchash
		if ( rchash_get_size(g_write_hash) > 1000) {

			rchash_reduce( g_write_hash, rw_dump_reduce, 0);
		}
#endif

	};
	return (0);
}

//
// Eventually, we'd like to have a hash table and a queue
// but realistically, all you need to do is send, because the
// fabric is pretty reliable. Expand this section when you want
// to make the system more reliable
//
cf_queue *g_rw_replication_q = 0;
#define RW_REPLICATION_THREAD_MAX 16
pthread_t g_rw_replication_th[RW_REPLICATION_THREAD_MAX];

typedef struct {
	cf_node node;
	msg *m;
} rw_replication_element;

void
rw_replicate_async(cf_node node, msg *m)
{
	// send this message to the remote node
	msg_incr_ref(m);
	int rv = as_fabric_send(node, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		// couldn't send, don't try again, for now
		as_fabric_msg_put(m);
	}

	return;
}

//
// Either is write, or is dup (if !is_write)
//
void
rw_replicate_process_ack(cf_node node, msg *m, bool is_write)
{
	if (m)
		as_fabric_msg_put(m);

	return;
}

void *
rw_replication_worker_fn(void *yeah_yeah_yeah)
{
	for (;;) {

		rw_replication_element e;

		if (0 != cf_queue_pop(g_rw_replication_q, &e, CF_QUEUE_FOREVER)) {
			cf_crash(AS_RW, "unable to pop from dup work queue");
		}

		cf_detail(AS_RW,
				"replication_process: sending message to destination to %"PRIx64,
				e.node);
#ifdef DEBUG_MSG
		msg_dump(m, "rw incoming dup");
#endif

	}
	return (0);
}

int rw_replicate_init(void) {
#if 0
	g_rw_replication_q = cf_queue_create( sizeof(rw_replication_element) , true /*multithreaded*/);
	if (!g_rw_replication_q) return(-1);

	for (int i = 0; i < 1; i++) {
		if (0 != pthread_create(&g_rw_replication_th[i], 0, rw_replication_worker_fn, 0)) {
			cf_crash(AS_RW, "can't create worker threads for duplicate resolution");
		}
	}
#endif
	return (0);
}

//
// This function is called right after a partition re-balance
// Any write in progress to the now-gone node can be quickly retransmitted to the proper node
//

int
write_node_delete_reduce_fn(void *key, uint32_t keylen, void *data,
		void *udata)
{
	write_request *wr = data;
	cf_node *node = (cf_node *) udata;

	for (int i = 0; i < wr->dest_sz; i++) {
		if ((wr->dest_complete[i] == 0) && (wr->dest_nodes[i] == *node)) {
			cf_debug(AS_RW, "removed: speed up retransmit");
			wr->xmit_ms = 0;
		}
	}

	return (0);
}

typedef struct as_rw_paxos_change_struct_t {
	cf_node succession[AS_CLUSTER_SZ];
	cf_node deletions[AS_CLUSTER_SZ];
} as_rw_paxos_change_struct;

/*
 * write_node_succession_reduce_fn
 * discover nodes in the hash table that are no longer in the succession list
 */
int
write_node_succession_reduce_fn(void *key, uint32_t keylen, void *data,
		void *udata)
{
	as_rw_paxos_change_struct *del = (as_rw_paxos_change_struct *) udata;
	write_request *wr = data;
	bool node_in_slist = false;

	for (int i = 0; i < wr->dest_sz; i++) {
		/* check if this key is in the succession list */
		node_in_slist = false;
		for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
			if (wr->dest_nodes[i] == del->succession[j]) {
				node_in_slist = true;
				break;
			}
		}
		if (false == node_in_slist) {
			for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
				/* if an empty slot exists, then it means key is not there yet */
				if (del->deletions[j] == (cf_node) 0) {
					del->deletions[j] = wr->dest_nodes[i];
					break;
				}
				/* if key already exists, return */
				if (wr->dest_nodes[i] == del->deletions[i])
					break;
			}
		}
	}

	return (0);
}

void
rw_paxos_change(as_paxos_generation gen, as_paxos_change *change,
		cf_node succession[], void *udata)
{
	if ((NULL == change) || (1 > change->n_change))
		return;
	as_rw_paxos_change_struct del;
	memset(&del, 0, sizeof(as_rw_paxos_change_struct));
	memcpy(del.succession, succession,
			sizeof(cf_node) * g_config.paxos_max_cluster_size);
	/*
	 * Find out if the request is sync.
	 */
	if (change->type[0] == AS_PAXOS_CHANGE_SYNC) {
		//Update the XDR cluster map. Piggybacking on the rw callback instead of adding a new one.
		xdr_clmap_update(AS_PAXOS_CHANGE_SYNC, succession,
				g_config.paxos_max_cluster_size);

		/*
		 * Iterate through the write hash table and find nodes that are not in the succession list
		 * Remove these entries from the hash table
		 */
		rchash_reduce(g_write_hash, write_node_succession_reduce_fn,
				(void *) &del);

		/*
		 * if there are any nodes to be deleted, then execute the deletion algorithm
		 */
		for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
			if ((cf_node) 0 != del.deletions[i]) {
				cf_debug(AS_RW, "notified: REMOVE node %"PRIx64"",
						del.deletions[i]);
				rchash_reduce(g_write_hash, write_node_delete_reduce_fn,
						(void *) & (del.deletions[i]));
			}
		}
		return;
	}

	/* This the deprecated case where this code is called at the end of a paxos transaction commit */
	cf_node changed_nodes[0];
	for (int i = 0; i < change->n_change; i++)
		if (change->type[i] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) {

			//Remove from the XDR cluster map
			changed_nodes[0] = change->id[i];
			xdr_clmap_update(change->type[i], changed_nodes, 1);

			cf_debug(AS_RW, "notified: REMOVE node %"PRIx64, change->id[i]);
			rchash_reduce(g_write_hash, write_node_delete_reduce_fn,
					(void *)&(change->id[i]));
		} else if (change->type[i] == AS_PAXOS_CHANGE_SUCCESSION_ADD) {

			//Add to the XDR cluster map
			changed_nodes[0] = change->id[i];
			xdr_clmap_update(change->type[i], changed_nodes, 1);
		}

	return;
}

uint32_t
as_write_inprogress()
{
	if (g_write_hash)
		return (rchash_get_size(g_write_hash));
	else
		return (0);

}

void
as_write_init()
{
	rchash_create(&g_write_hash, write_digest_hash, write_request_destructor,
			sizeof(global_keyd), 32 * 1024, RCHASH_CR_MT_MANYLOCK);

	pthread_create(&g_rw_retransmit_th, 0, rw_retransmit_fn, 0);

	if (0 != rw_dup_init()) {
		cf_crash(AS_RW, "couldn't initialize duplicate write unit threads");
		return;
	}

	if (0 != rw_replicate_init()) {
		cf_crash(AS_RW, "couldn't initialize write unit threads");
		return;
	}

	as_fabric_register_msg_fn(M_TYPE_RW, rw_mt, sizeof(rw_mt), write_msg_fn,
			0 /* udata */);

	as_paxos_register_change_callback(rw_paxos_change, 0);

#ifdef TRACK_WR
	wr_track_init();
#endif

}

void
single_transaction_response(as_transaction *tr, as_namespace *ns,
		as_msg_op **ops, as_bin **response_bins, uint16_t n_bins,
		uint32_t generation, uint32_t void_time, uint *written_sz,
		char *setname)
{

	cf_detail_digest(AS_RW, NULL, "[ENTER] NS(%s)", ns->name );

	if (tr->proto_fd_h) {
		if (tr->batch_shared) {
			as_batch_add_result(tr, ns, setname, generation, void_time,
				n_bins, response_bins, ops);
		}
		else {
			if (0 != as_msg_send_reply(tr->proto_fd_h, tr->result_code,
					generation, void_time, ops, response_bins, n_bins, ns,
					written_sz, tr->trid, setname)) {
				cf_info(AS_RW, "rw: can't send reply, fd %d rc %d",
						tr->proto_fd_h->fd, tr->result_code);
			}
		}
		tr->proto_fd_h = 0;
	} else if (tr->proxy_msg) {
		// it was a proxy request - hand it back to the proxy for responding
		if (tr->flag & AS_TRANSACTION_FLAG_SHIPPED_OP)
			cf_detail(AS_RW,
					"[Digest %"PRIx64" Shipped OP] sending reply, rc %d to %"PRIx64"",
					*(uint64_t *)&tr->keyd, tr->result_code, tr->proxy_node);
		else
			cf_detail(AS_RW, "sending proxy reply, rc %d to %"PRIx64"",
					tr->result_code, tr->proxy_node);

		as_proxy_send_response(tr->proxy_node, tr->proxy_msg, tr->result_code,
				generation, void_time, ops, response_bins, n_bins, ns, tr->trid,
				setname);
		tr->proxy_msg = 0;
	} else {
		// In this case, this is a call from write_process() above.
		// create the response message (this is a new malloc that will be handed off to fabric (see end of write_process())
		// Can we even get here from write_process()? Or is this dead code?
		if (! tr->msgp) {
			size_t msg_sz = 0;
			tr->msgp = as_msg_make_response_msg(tr->result_code, generation,
					void_time, ops, response_bins, n_bins, ns, (cl_msg *) NULL,
					&msg_sz, tr->trid, setname);
			// TODO - if it turns out this is normal, demote to debug:
			cf_warning_digest(AS_RW, &tr->keyd,
					"{%s} thr_tsvc_read returns response message for duplicate read %p ",
					ns->name, tr->msgp);
		}
		else {
			// We can get here if a timeout in rw_retransmit_reduce_fn() zeros
			// the proto_fd_h out from under a live finish_rw_process_ack()...
			cf_warning_digest(AS_RW, &tr->keyd,
					"{%s} prevented overwrite and leak of tr->msgp %p ",
					ns->name, tr->msgp);
		}
	}
}

// Compute length of the wait queue linked list on a wr.
static int
wq_len(wreq_tr_element *wq)
{
	int len = 0;

	while (wq) {
		len++;
		wq = wq->next;
	}

	return len;
}

static uint64_t g_now;

// Print interesting info. about a single wr.
static int
dump_rw_reduce_fn(void *key, uint32_t keylen, void *data,
		void *udata)
{
	write_request *wr = data;
	int *counter = (int *) udata;

	pthread_mutex_lock(&wr->lock);
	cf_info(AS_RW,
			"gwh[%d]: wr %p rc %d ready %d et %ld xm %ld (delta %ld) ri %d pb %p |wq| %d",
			*counter, wr, cf_rc_count(wr), wr->ready, cf_atomic64_get(wr->end_time) / 1000000,
			wr->xmit_ms, wr->xmit_ms - g_now, wr->retry_interval_ms, wr->pickled_buf, wq_len(wr->wait_queue_head));
	pthread_mutex_unlock(&wr->lock);

	*counter += 1;

	return (0);
}

// Dump info. about all wr objects in the "g_write_hash".
void
as_dump_wr()
{
	if (g_write_hash) {
		int counter = 0;
		g_now = cf_getms();
		cf_info(AS_RW, "There are %d entries in g_write_hash @ time = %ld:",
				rchash_get_size(g_write_hash), g_now);
		rchash_reduce(g_write_hash, dump_rw_reduce_fn, &counter);
	} else {
		cf_warning(AS_RW, "No g_write_hash!");
	}
}


//==============================================================================
// read_local() and utilities.
//

void
read_local_done(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd,
		int result_code)
{
	uint32_t generation = 0;
	uint32_t void_time = 0;

	if (r_ref) {
		if (rd) {
			as_storage_record_close(r_ref->r, rd);
		}

		generation = r_ref->r->generation;
		void_time = r_ref->r->void_time;

		as_record_done(r_ref, tr->rsv.ns);
	}

	tr->result_code = result_code;

	MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_cleanup_hist);

	single_transaction_response(tr, tr->rsv.ns, NULL, NULL, 0, generation,
			void_time, NULL, NULL);

	MICROBENCHMARK_HIST_INSERT_P(rt_net_hist);
}

void
read_local(as_transaction *tr, as_index_ref *r_ref)
{
	if (tr->result_code == AS_PROTO_RESULT_FAIL_NOTFOUND) {
		read_local_done(tr, NULL, NULL, AS_PROTO_RESULT_FAIL_NOTFOUND);
		return;
	}

	as_msg *m = &tr->msgp->msg;
	as_namespace *ns = tr->rsv.ns;

	as_index_ref r_ref_stack;
	r_ref_stack.skip_lock = false;

	if (! r_ref) {
		r_ref = &r_ref_stack;

		int rv = as_record_get(tr->rsv.tree, &tr->keyd, r_ref, ns);

		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_tree_hist);

		if (rv != 0) {
			read_local_done(tr, NULL, NULL, AS_PROTO_RESULT_FAIL_NOTFOUND);
			return;
		}
	}

	as_record *r = r_ref->r;
	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd, &tr->keyd);

	MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_storage_open_hist);

	// Check if it's an expired record.
	if (as_record_is_expired(r)) {
		read_local_done(tr, r_ref, &rd, AS_PROTO_RESULT_FAIL_NOTFOUND);
		return;
	}

	// Check the key if required.
	// Note - for data-not-in-memory "exists" ops, key check is expensive!
	if (msg_has_key(m) &&
			as_storage_record_get_key(&rd) && ! check_msg_key(m, &rd)) {
		read_local_done(tr, r_ref, &rd, AS_PROTO_RESULT_FAIL_KEY_MISMATCH);
		return;
	}

	if ((m->info1 & AS_MSG_INFO1_GET_NOBINDATA) != 0) {
		read_local_done(tr, r_ref, &rd, AS_PROTO_RESULT_OK);
		return;
	}

	rd.n_bins = as_bin_get_n_bins(r, &rd);

	as_bin stack_bins[ns->storage_data_in_memory ? 0 : rd.n_bins];

	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	if (! as_bin_inuse_has(&rd)) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: found record with no bins ", ns->name);
		read_local_done(tr, r_ref, &rd, AS_PROTO_RESULT_FAIL_NOTFOUND);
		return;
	}

	uint32_t bin_count = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0 ?
			rd.n_bins : m->n_ops;

	as_msg_op *ops[bin_count];
	as_msg_op **p_ops = ops;
	as_bin *response_bins[bin_count];
	uint16_t n_bins = 0;

	as_bin result_bins[bin_count];
	uint32_t n_result_bins = 0;

	if ((m->info1 & AS_MSG_INFO1_GET_ALL) != 0) {
		p_ops = NULL;
		n_bins = as_bin_inuse_count(&rd);
		as_bin_get_all_p(&rd, response_bins);
	}
	else {
		bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;
		int result;

		as_msg_op *op = 0;
		int n = 0;

		while ((op = as_msg_op_iterate(m, op, &n)) != NULL) {
			if (op->op == AS_MSG_OP_READ) {
				as_bin *b = as_bin_get_from_buf(&rd, op->name, op->name_sz);

				if (b || respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = b;
				}
			}
			else if (op->op == AS_MSG_OP_CDT_READ) {
				as_bin *b = as_bin_get_from_buf(&rd, op->name, op->name_sz);

				if (b) {
					as_bin *rb = &result_bins[n_result_bins];
					as_bin_set_empty(rb);

					if ((result = as_bin_cdt_read_from_client(b, op, rb)) < 0) {
						cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: failed as_bin_cdt_read_from_client() ", ns->name);
						destroy_stack_bins(result_bins, n_result_bins);
						read_local_done(tr, r_ref, &rd, -result);
						return;
					}

					if (as_bin_inuse(rb)) {
						b = rb;
						n_result_bins++;
					}
				}

				if (b || respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = b;
				}
			}
			else {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} read_local: unexpected bin op %u ", ns->name, op->op);
				destroy_stack_bins(result_bins, n_result_bins);
				read_local_done(tr, r_ref, &rd, AS_PROTO_RESULT_FAIL_PARAMETER);
				return;
			}
		}
	}

	uint32_t written_sz = 0;
	const char *set_name = (m->info1 & AS_MSG_INFO1_XDR) != 0 ?
			as_index_get_set_name(r, ns) : NULL;

	MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_storage_read_hist);

	single_transaction_response(tr, ns, p_ops, response_bins, n_bins,
			r->generation, r->void_time, &written_sz, (char *)set_name);

	MICROBENCHMARK_HIST_INSERT_AND_RESET_P(rt_net_hist);

	destroy_stack_bins(result_bins, n_result_bins);
	as_storage_record_close(r, &rd);
	as_record_done(r_ref, ns);

	MICROBENCHMARK_HIST_INSERT_P(rt_cleanup_hist);

#ifdef HISTOGRAM_OBJECT_LATENCY
	if (written_sz && (m->info1 & AS_MSG_INFO1_READ) ) {
		if (written_sz < 200) {
			histogram_insert_data_point(g_config.read0_hist, tr->start_time);
		} else if (written_sz < 500 ) {
			histogram_insert_data_point(g_config.read1_hist, tr->start_time);
		} else if (written_sz < (1 * 1024) ) {
			histogram_insert_data_point(g_config.read2_hist, tr->start_time);
		} else if (written_sz < (2 * 1024) ) {
			histogram_insert_data_point(g_config.read3_hist, tr->start_time);
		} else if (written_sz < (5 * 1024) ) {
			histogram_insert_data_point(g_config.read4_hist, tr->start_time);
		} else if (written_sz < (10 * 1024) ) {
			histogram_insert_data_point(g_config.read5_hist, tr->start_time);
		} else if (written_sz < (20 * 1024) ) {
			histogram_insert_data_point(g_config.read6_hist, tr->start_time);
		} else if (written_sz < (50 * 1024) ) {
			histogram_insert_data_point(g_config.read7_hist, tr->start_time);
		} else if (written_sz < (100 * 1024) ) {
			histogram_insert_data_point(g_config.read8_hist, tr->start_time);
		} else {
			histogram_insert_data_point(g_config.read9_hist, tr->start_time);
		}
	}
#endif
}

//
// End read_local()
//==============================================================================


/* Recieved multi op request.
 *
 * 1. Uncompress if required. Pick up each message and call write_msg_fn on
 * 	  each of it.
 *
 * 2. Pack the response into a single multi op ack back to the requesting node.
 *
 * First users
 *
 * 1. Prole write for LDT where record and subrecord are all packed in single
 *    packet and sent. This is to achieve semantics closer to atomic movement
 *    of changes [reduces effect of network misbehaves]
 *
 * 2. Prole write for operation on secondary index. Because we ship the entire
 *    record .. any write operation on the prole is blind write, it does not
 *    read the on disk state. Hence the secondary index updates are also sent
 *    along with the pickled buf of write.
 *
 *    TODO: This multiple message apply is not atomic. Also though they
 *    			 are under single partition and namespace reservation they
 *    			 are not under same lock. Which would need more changes. Any
 *    			 ways expectation is that the master holds synchronization.
 */
int
rw_multi_process(cf_node node, msg *m)
{
	cf_detail(AS_RW, "MULTI_OP: Received multi op");
	uint32_t result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
	int ret = 0;
	bool reserved = false;

	cf_digest *keyd;
	size_t sz = 0;

	if (0 != msg_get_buf(m, RW_FIELD_DIGEST, (byte **) &keyd, &sz,
			MSG_GET_DIRECT)) {
		cf_warning(AS_RW, "MULTI_OP: Message without Digest");
		goto Out;
	}

	uint64_t cluster_key;
	if (0 != msg_get_uint64(m, RW_FIELD_CLUSTER_KEY, &cluster_key)) {
		cf_warning(AS_RW, "MULTI_OP: Message without Cluster Key");
		goto Out;
	}

	uint8_t *ns_name = 0;
	size_t ns_name_len;
	if (0 != msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT)) {
		cf_warning(AS_RW, "MULTI_OP: Message without Namespace");
		goto Out;
	}
	as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);
	if (!ns) {
		cf_warning(AS_RW, "MULTI_OP: Message Namespace Not found");
		goto Out;
	}

	uint32_t info = 0;
	if (0 != msg_get_uint32(m, RW_FIELD_INFO, &info)) {
		cf_warning(AS_RW, "MULTI_OP: Message Without Info Field");
		goto Out;
	}

	uint8_t *pickled_buf = NULL;
	size_t pickled_sz = 0;
	if (0 != msg_get_buf(m, RW_FIELD_MULTIOP, (byte **) &pickled_buf,
			&pickled_sz, MSG_GET_DIRECT)) {
		cf_warning(AS_RW, "MULTI_OP: Message Without Buffer");
		goto Out;
	}

	as_partition_reservation rsv;
	as_partition_reserve_migrate(ns, as_partition_getid(*keyd), &rsv, 0);
	cf_atomic_int_incr(&g_config.wprocess_tree_count);
	reserved = true;
	if (rsv.state == AS_PARTITION_STATE_ABSENT ||
		rsv.state == AS_PARTITION_STATE_WAIT)
	{
		result_code = AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH;
		cf_atomic_int_incr(&g_config.stat_cluster_key_prole_retry);
		cf_debug_digest(AS_RW, keyd,
				"[PROLE STATE MISMATCH:2] TID(0) P PID(%u) State:ABSENT or other(%u). Return to Sender. :",
				rsv.pid, rsv.state  );
		goto Out;
	}
	ldt_prole_info linfo;
	memset(&linfo, 1, sizeof(ldt_prole_info));

	int offset = 0;
	int count = 0;
	while (1) {
		uint8_t *buf = (uint8_t *) (pickled_buf + offset);
		size_t sz = pickled_sz - offset;
		if (!sz)
			break;

		uint32_t op_msg_len = 0;
		msg_type op_msg_type = 0;
		msg *op_msg = NULL;

		cf_detail(AS_RW, "MULTI_OP: Stage 1[%d] [%p,%d,%d,%d,%d,%d]",
				count, buf, pickled_sz, sz, offset, op_msg_type, op_msg_len);
		if (0 != msg_get_initial(&op_msg_len, &op_msg_type,
				(const uint8_t *) buf, sz)) {
			ret = -1;
			goto Out;
		}

		cf_detail(AS_RW, "MULTI_OP: Stage 2[%d] [%p,%d,%d,%d,%d,%d]",
				count, buf, pickled_sz, sz, offset, op_msg_type, op_msg_len);
		op_msg = as_fabric_msg_get(op_msg_type);
		if (!op_msg) {
			cf_warning(AS_RW, "MULTI_OP: Running out of fabric message");
			ret = -2;
			goto Out;
		}

		if (msg_parse(op_msg, (const uint8_t *) buf, sz, false)) {
			ret = -3;
			as_fabric_msg_put(op_msg);
			goto Out;
		}

		offset += op_msg_len;
		ret = 0;
		if (op_msg_type == M_TYPE_SINDEX) {
			cf_detail(AS_RW, "MULTI_OP: Received Sindex multi op");
		} else {
			cf_detail(AS_RW, "MULTI_OP: Received LDT multi op");
			ret = write_process_new(node, op_msg, &rsv, false, &linfo);
		}
		if (ret) {
			ret = -3;
			as_fabric_msg_put(op_msg);
			goto Out;
		}
		as_fabric_msg_put(op_msg);
		count++;
	}
	result_code = AS_PROTO_RESULT_OK;
Out:

	if (ret) {
		cf_warning(AS_RW,
				"MULTI_OP: Internal Error ... Multi Op Message Corrupted ");
		as_fabric_msg_put(m);
	}

	if (reserved) {
		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.wprocess_tree_count);
		reserved = false;
	}

	msg_set_unset(m, RW_FIELD_AS_MSG);
	msg_set_unset(m, RW_FIELD_RECORD);
	msg_set_unset(m, RW_FIELD_REC_PROPS);
	msg_set_uint32(m, RW_FIELD_OP, RW_OP_MULTI_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result_code);

	uint64_t start_ns = 0;
	if (g_config.microbenchmarks) {
		start_ns = cf_getns();
	}
	int rv2 = as_fabric_send(node, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (g_config.microbenchmarks && start_ns) {
		histogram_insert_data_point(g_config.prole_fabric_send_hist, start_ns);
	}

	if (rv2 != 0) {
		cf_debug(AS_RW, "write process: send fabric message bad return %d",
				rv2);
		as_fabric_msg_put(m);
		cf_atomic_int_incr(&g_config.rw_err_write_send);
	}
	return 0;
}
