/*
 * replica_write.c
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

#include "transaction/replica_write.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/rec_props.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/truncate.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "transaction/delete.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

uint32_t pack_info_bits(as_transaction* tr, bool has_udf);
void send_repl_write_ack(cf_node node, msg* m, uint32_t result);

int drop_replica(as_partition_reservation* rsv, cf_digest* keyd,
		bool is_nsup_delete, bool is_xdr_op, cf_node master);
int write_replica(as_partition_reservation* rsv, cf_digest* keyd,
		uint8_t* pickled_buf, size_t pickled_sz,
		const as_rec_props* p_rec_props, as_generation generation,
		uint32_t void_time, uint64_t last_update_time, cf_node master,
		uint32_t info);


//==========================================================
// Public API.
//

bool
repl_write_make_message(rw_request* rw, as_transaction* tr)
{
	if (rw->dest_msg) {
		msg_reset(rw->dest_msg);
	}
	else if (! (rw->dest_msg = as_fabric_msg_get(M_TYPE_RW))) {
		return false;
	}

	// TODO - remove this when we're comfortable:
	cf_assert(rw->pickled_buf, AS_RW, "making repl-write msg with null pickle");

	as_namespace* ns = tr->rsv.ns;
	msg* m = rw->dest_msg;

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE);
	msg_set_buf(m, RW_FIELD_NAMESPACE, (uint8_t*)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_NS_ID, ns->id);
	msg_set_buf(m, RW_FIELD_DIGEST, (void*)&tr->keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_uint32(m, RW_FIELD_TID, rw->tid);

	if (tr->generation != 0) {
		msg_set_uint32(m, RW_FIELD_GENERATION, tr->generation);
	}

	if (tr->void_time != 0) {
		msg_set_uint32(m, RW_FIELD_VOID_TIME, tr->void_time);
	}

	if (tr->last_update_time != 0) {
		msg_set_uint64(m, RW_FIELD_LAST_UPDATE_TIME, tr->last_update_time);
	}

	// TODO - deal with this on the write-becomes-drop & udf-drop paths?
	clear_delete_response_metadata(rw, tr);

	uint32_t info = pack_info_bits(tr, rw->has_udf);

	repl_write_flag_pickle(tr, rw->pickled_buf, &info);

	msg_set_buf(m, RW_FIELD_RECORD, (void*)rw->pickled_buf, rw->pickled_sz,
			MSG_SET_HANDOFF_MALLOC);

	// Make sure destructor doesn't free this.
	rw->pickled_buf = NULL;

	// TODO - replace rw->pickled_rec_props with individual fields.
	if (rw->pickled_rec_props.p_data) {
		const char* set_name;
		uint32_t set_name_size;

		if (as_rec_props_get_value(&rw->pickled_rec_props,
				CL_REC_PROPS_FIELD_SET_NAME, &set_name_size,
				(uint8_t**)&set_name) == 0) {
			msg_set_buf(m, RW_FIELD_SET_NAME, (const uint8_t *)set_name,
					set_name_size - 1, MSG_SET_COPY);
		}

		uint32_t key_size;
		uint8_t* key;

		if (as_rec_props_get_value(&rw->pickled_rec_props,
				CL_REC_PROPS_FIELD_KEY, &key_size, &key) == 0) {
			msg_set_buf(m, RW_FIELD_KEY, key, key_size, MSG_SET_COPY);
		}
	}

	if (info != 0) {
		msg_set_uint32(m, RW_FIELD_INFO, info);
	}

	return true;
}


void
repl_write_setup_rw(rw_request* rw, as_transaction* tr,
		repl_write_done_cb repl_write_cb, timeout_done_cb timeout_cb)
{
	rw->msgp = tr->msgp;
	tr->msgp = NULL;

	rw->msg_fields = tr->msg_fields;
	rw->origin = tr->origin;
	rw->from_flags = tr->from_flags;

	rw->from.any = tr->from.any;
	rw->from_data.any = tr->from_data.any;
	tr->from.any = NULL;

	rw->start_time = tr->start_time;
	rw->benchmark_time = tr->benchmark_time;

	as_partition_reservation_copy(&rw->rsv, &tr->rsv);
	// Hereafter, rw_request must release reservation - happens in destructor.

	rw->end_time = tr->end_time;
	rw->generation = tr->generation;
	rw->void_time = tr->void_time;

	rw->repl_write_cb = repl_write_cb;
	rw->timeout_cb = timeout_cb;

	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	for (int i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
	}

	// Allow retransmit thread to destroy rw_request as soon as we unlock.
	rw->is_set_up = true;
}


void
repl_write_reset_rw(rw_request* rw, as_transaction* tr, repl_write_done_cb cb)
{
	// Reset rw->from.any which was set null in tr setup. (And note that
	// tr->from.any will be null here in respond-on-master-complete mode.)
	rw->from.any = tr->from.any;

	// Needed for response to origin.
	rw->generation = tr->generation;
	rw->void_time = tr->void_time;

	rw->repl_write_cb = cb;

	// TODO - is this better than not resetting? Note - xmit_ms not volatile.
	rw->xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	rw->retry_interval_ms = g_config.transaction_retry_ms;

	for (int i = 0; i < rw->n_dest_nodes; i++) {
		rw->dest_complete[i] = false;
	}
}


void
repl_write_handle_op(cf_node node, msg* m)
{
	uint8_t* ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, RW_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no namespace");
		send_repl_write_ack(node, m, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	as_namespace* ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_RW, "repl_write_handle_op: invalid namespace");
		send_repl_write_ack(node, m, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl_write_handle_op: no digest");
		send_repl_write_ack(node, m, AS_PROTO_RESULT_FAIL_UNKNOWN);
		return;
	}

	as_partition_reservation rsv;

	as_partition_reserve_migrate(ns, as_partition_getid(keyd), &rsv, NULL);

	if (rsv.reject_repl_write) {
		as_partition_release(&rsv);
		send_repl_write_ack(node, m, AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH);
		return;
	}

	uint32_t info = 0;

	msg_get_uint32(m, RW_FIELD_INFO, &info);

	uint8_t* pickled_buf;
	size_t pickled_sz;

	uint32_t result;

	// TODO POST-JUMP - reverse if & else, un-indent...
	if (msg_get_buf(m, RW_FIELD_RECORD, (uint8_t**)&pickled_buf, &pickled_sz,
			MSG_GET_DIRECT) == 0) {
		if (repl_write_pickle_is_drop(pickled_buf, info)) {
			result = drop_replica(&rsv, keyd,
					(info & RW_INFO_NSUP_DELETE) != 0,
					(info & RW_INFO_XDR) != 0,
					node);

			as_partition_release(&rsv);
			send_repl_write_ack(node, m, result);

			return;
		}

		as_generation generation;

		if (msg_get_uint32(m, RW_FIELD_GENERATION, &generation) != 0) {
			cf_warning(AS_RW, "repl_write_handle_op: no generation");
			as_partition_release(&rsv);
			send_repl_write_ack(node, m, AS_PROTO_RESULT_FAIL_UNKNOWN);
			return;
		}

		uint64_t last_update_time;

		if (msg_get_uint64(m, RW_FIELD_LAST_UPDATE_TIME,
				&last_update_time) != 0) {
			cf_warning(AS_RW, "repl_write_handle_op: no last-update-time");
			as_partition_release(&rsv);
			send_repl_write_ack(node, m, AS_PROTO_RESULT_FAIL_UNKNOWN);
			return;
		}

		uint32_t void_time = 0;

		msg_get_uint32(m, RW_FIELD_VOID_TIME, &void_time);

		uint8_t *set_name = NULL;
		size_t set_name_len = 0;

		msg_get_buf(m, RW_FIELD_SET_NAME, &set_name, &set_name_len,
				MSG_GET_DIRECT);

		uint8_t *key = NULL;
		size_t key_size = 0;

		msg_get_buf(m, RW_FIELD_KEY, &key, &key_size, MSG_GET_DIRECT);

		size_t rec_props_data_size = as_rec_props_size_all(set_name,
				set_name_len, key, key_size);
		uint8_t rec_props_data[rec_props_data_size];
		as_rec_props rec_props = { 0 };

		if (rec_props_data_size != 0) {
			rec_props.p_data = rec_props_data;

			as_rec_props_fill_all(&rec_props, rec_props.p_data, set_name,
					set_name_len, key, key_size);
		}

		result = write_replica(&rsv, keyd, pickled_buf, pickled_sz, &rec_props,
				generation, void_time, last_update_time, node, info);
	}
	else {
		cf_warning(AS_RW, "repl_write_handle_op: no msg or pickle");
		result = AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	as_partition_release(&rsv);
	send_repl_write_ack(node, m, result);
}


void
repl_write_handle_ack(cf_node node, msg* m)
{
	uint32_t ns_id;

	if (msg_get_uint32(m, RW_FIELD_NS_ID, &ns_id) != 0) {
		cf_warning(AS_RW, "repl-write ack: no ns-id");
		as_fabric_msg_put(m);
		return;
	}

	cf_digest* keyd;

	if (msg_get_buf(m, RW_FIELD_DIGEST, (uint8_t**)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_RW, "repl-write ack: no digest");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t tid;

	if (msg_get_uint32(m, RW_FIELD_TID, &tid) != 0) {
		cf_warning(AS_RW, "repl-write ack: no tid");
		as_fabric_msg_put(m);
		return;
	}

	// TODO - handle failure results other than CLUSTER_KEY_MISMATCH.
	uint32_t result_code;

	if (msg_get_uint32(m, RW_FIELD_RESULT, &result_code) != 0) {
		cf_warning(AS_RW, "repl-write ack: no result_code");
		as_fabric_msg_put(m);
		return;
	}

	// TODO - force retransmit to happen faster than default.
	if (result_code == AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH) {
		as_fabric_msg_put(m);
		return;
	}

	rw_request_hkey hkey = { ns_id, *keyd };
	rw_request* rw = rw_request_hash_get(&hkey);

	if (! rw) {
		// Extra ack, after rw_request is already gone.
		as_fabric_msg_put(m);
		return;
	}

	pthread_mutex_lock(&rw->lock);

	if (rw->tid != tid) {
		// Extra ack, rw_request is that of newer transaction for same digest.
		pthread_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	int i;

	for (i = 0; i < rw->n_dest_nodes; i++) {
		if (rw->dest_nodes[i] != node) {
			continue;
		}

		if (rw->dest_complete[i]) {
			// Extra ack for this replica write.
			pthread_mutex_unlock(&rw->lock);
			rw_request_release(rw);
			as_fabric_msg_put(m);
			return;
		}

		rw->dest_complete[i] = true;

		break;
	}

	if (i == rw->n_dest_nodes) {
		cf_warning(AS_RW, "repl-write ack: from non-dest node %lx", node);
		pthread_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	for (int j = 0; j < rw->n_dest_nodes; j++) {
		if (! rw->dest_complete[j]) {
			// Still haven't heard from all duplicates.
			pthread_mutex_unlock(&rw->lock);
			rw_request_release(rw);
			as_fabric_msg_put(m);
			return;
		}
	}

	if (! rw->from.any && rw->origin != FROM_NSUP &&
			! rw->respond_client_on_master_completion) {
		// Lost race against timeout in retransmit thread.
		pthread_mutex_unlock(&rw->lock);
		rw_request_release(rw);
		as_fabric_msg_put(m);
		return;
	}

	if (! rw->respond_client_on_master_completion) {
		rw->repl_write_cb(rw);
	}

	pthread_mutex_unlock(&rw->lock);

	rw_request_hash_delete(&hkey, rw);
	rw_request_release(rw);
	as_fabric_msg_put(m);
}


//==========================================================
// Local helpers - messages.
//

uint32_t
pack_info_bits(as_transaction* tr, bool has_udf)
{
	uint32_t info = 0;

	if (as_transaction_is_xdr(tr)) {
		info |= RW_INFO_XDR;
	}

	if ((tr->flags & AS_TRANSACTION_FLAG_SINDEX_TOUCHED) != 0) {
		info |= RW_INFO_SINDEX_TOUCHED;
	}

	if ((tr->from_flags & FROM_FLAG_NSUP_DELETE) != 0) {
		info |= RW_INFO_NSUP_DELETE;
	}

	if (has_udf) {
		info |= RW_INFO_UDF_WRITE;
	}

	return info;
}


void
send_repl_write_ack(cf_node node, msg* m, uint32_t result)
{
	msg_preserve_fields(m, 3, RW_FIELD_NS_ID, RW_FIELD_DIGEST, RW_FIELD_TID);

	msg_set_uint32(m, RW_FIELD_OP, RW_OP_WRITE_ACK);
	msg_set_uint32(m, RW_FIELD_RESULT, result);

	if (as_fabric_send(node, m, AS_FABRIC_CHANNEL_RW) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


//==========================================================
// Local helpers - drop  or write replicas.
//

int
drop_replica(as_partition_reservation* rsv, cf_digest* keyd,
		bool is_nsup_delete, bool is_xdr_op, cf_node master)
{
	// Shortcut pointers & flags.
	as_namespace* ns = rsv->ns;
	as_index_tree* tree = rsv->tree;

	as_index_ref r_ref;
	r_ref.skip_lock = false;

	if (as_record_get(tree, keyd, &r_ref) != 0) {
		return AS_PROTO_RESULT_FAIL_NOTFOUND;
	}

	as_record* r = r_ref.r;

	if (ns->storage_data_in_memory) {
		record_delete_adjust_sindex(r, ns);
	}

	// Save the set-ID for XDR.
	uint16_t set_id = as_index_get_set_id(r);

	as_index_delete(tree, keyd);
	as_record_done(&r_ref, ns);

	if (xdr_must_ship_delete(ns, is_nsup_delete, is_xdr_op)) {
		xdr_write(ns, *keyd, 0, master, XDR_OP_TYPE_DROP, set_id, NULL);
	}

	return AS_PROTO_RESULT_OK;
}


int
write_replica(as_partition_reservation* rsv, cf_digest* keyd,
		uint8_t* pickled_buf, size_t pickled_sz,
		const as_rec_props* p_rec_props, as_generation generation,
		uint32_t void_time, uint64_t last_update_time, cf_node master,
		uint32_t info)
{
	as_namespace* ns = rsv->ns;

	if (! as_storage_has_space(ns)) {
		cf_warning(AS_RW, "{%s} write_replica: drives full", ns->name);
		return AS_PROTO_RESULT_FAIL_OUT_OF_SPACE;
	}

	CF_ALLOC_SET_NS_ARENA(ns);

	as_index_tree* tree = rsv->tree;

	as_index_ref r_ref;
	r_ref.skip_lock = false;

	int rv = as_record_get_create(tree, keyd, &r_ref, ns);

	if (rv < 0) {
		cf_warning_digest(AS_RW, keyd, "{%s} write_replica: fail as_record_get_create() ", ns->name);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	as_record* r = r_ref.r;
	as_storage_rd rd;
	bool is_create = false;

	if (rv == 1) {
		as_storage_record_create(ns, r, &rd);
		is_create = true;
	}
	else {
		as_storage_record_open(ns, r, &rd);
	}

	bool has_sindex = (info & RW_INFO_SINDEX_TOUCHED) != 0 ||
			// Because replica write arriving on empty node (before migration)
			// may not have above flag set if master write didn't touch sindex.
			// Note - can't use record_has_sindex() here since r won't yet have
			// set-id.
			// TODO - refactor to be able to use set-id.
			// TODO - missing replica writes that would have touched sindex must
			// be accounted for when we implement re-replication.
			(is_create && as_sindex_ns_has_sindex(ns));

	rd.ignore_record_on_device = ! has_sindex;
	as_storage_rd_load_n_bins(&rd); // TODO - handle error returned

	// TODO - we really need an inline utility for this!
	uint16_t newbins = ntohs(*(uint16_t*)pickled_buf);

	if (! rd.ns->storage_data_in_memory && ! rd.ns->single_bin &&
			newbins > rd.n_bins) {
		rd.n_bins = newbins;
	}

	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

	as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned

	int32_t stack_particles_sz = 0;

	if (! rd.ns->storage_data_in_memory) {
		stack_particles_sz = as_record_buf_get_stack_particles_sz(pickled_buf);

		if (stack_particles_sz < 0) {
			if (is_create) {
				as_index_delete(tree, keyd);
			}

			as_storage_record_close(&rd);
			as_record_done(&r_ref, ns);

			return -stack_particles_sz;
		}
	}

	uint8_t stack_particles[stack_particles_sz];
	uint8_t* p_stack_particles = stack_particles;

	uint64_t memory_bytes = is_create ?
			0 : as_storage_record_get_n_bytes_memory(&rd);

	as_record_set_properties(&rd, p_rec_props);

	if (is_create) {
		r->last_update_time = last_update_time;

		if (as_truncate_record_is_truncated(r, ns)) {
			as_index_delete(tree, keyd);
			as_storage_record_close(&rd);
			as_record_done(&r_ref, ns);

			return AS_PROTO_RESULT_FAIL_FORBIDDEN;
		}
	}

	if (as_record_unpickle_replace(r, &rd, pickled_buf, pickled_sz,
			&p_stack_particles, has_sindex) != 0) {
		if (is_create) {
			as_index_delete(tree, keyd);
		}

		as_storage_record_close(&rd);
		as_record_done(&r_ref, ns);

		return AS_PROTO_RESULT_FAIL_UNKNOWN; // TODO - better granularity?
	}

	r->generation = generation;
	r->void_time = truncate_void_time(ns, void_time);
	r->last_update_time = last_update_time;

	bool is_durable_delete = as_record_apply_replica(&rd, info, tree);

	uint16_t set_id = as_index_get_set_id(r);
	xdr_op_type op_type = XDR_OP_TYPE_WRITE;

	if (is_durable_delete) {
		generation = 0;
		op_type = XDR_OP_TYPE_DURABLE_DELETE;
	}

	as_storage_record_adjust_mem_stats(&rd, memory_bytes);
	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	// Don't send an XDR delete if it's disallowed.
	if (is_durable_delete && ! is_xdr_delete_shipping_enabled()) {
		// TODO - should we also not ship if there was no record here before?
		return AS_PROTO_RESULT_OK;
	}

	// Do XDR write if the write is a non-XDR write or forwarding is enabled.
	if ((info & RW_INFO_XDR) == 0 ||
			is_xdr_forwarding_enabled() || ns->ns_forward_xdr_writes) {
		xdr_write(ns, *keyd, generation, master, op_type, set_id, NULL);
	}

	return AS_PROTO_RESULT_OK;
}
