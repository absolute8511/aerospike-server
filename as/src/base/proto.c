/*
 * proto.c
 *
 * Copyright (C) 2008-2015 Aerospike, Inc.
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
 * Handles protocol duties, gets data onto the wire & off again
 */

#include "base/proto.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <asm/byteorder.h>

#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "fault.h"
#include "socket.h"

#include "base/as_stap.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "storage/storage.h"


void
as_proto_swap(as_proto *p)
{
	uint8_t	 version = p->version;
	uint8_t  type = p->type;
	p->version = p->type = 0;
	p->sz = __be64_to_cpup((__u64 *)p);
	p->version = version;
	p->type = type;
}

void
as_msg_swap_header(as_msg *m)
{
	m->generation = ntohl(m->generation);
	m->record_ttl = ntohl(m->record_ttl);
	m->transaction_ttl = ntohl(m->transaction_ttl);
	m->n_fields = ntohs(m->n_fields);
	m->n_ops = ntohs(m->n_ops);
}

void
as_msg_swap_op(as_msg_op *op)
{
	op->op_sz = ntohl(op->op_sz);
}

void
as_msg_swap_field(as_msg_field *mf)
{
	mf->field_sz = ntohl(mf->field_sz);
}

//
// This function will attempt to fill the passed in buffer,
// but if too small, will malloc and return that.
// Either way it returns what it filled in.
//

cl_msg *
as_msg_make_response_msg(uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op **ops, as_bin **bins, uint16_t bin_count,
		as_namespace *ns, cl_msg *msgp_in, size_t *msg_sz_in, uint64_t trid,
		const char *setname)
{
	size_t msg_sz = sizeof(cl_msg);

	msg_sz += sizeof(as_msg_op) * bin_count;

	for (uint16_t i = 0; i < bin_count; i++) {
		if (ops) {
			msg_sz += ops[i]->name_sz;
		}
		else if (bins[i]) {
			msg_sz += ns->single_bin ?
					0 : strlen(as_bin_get_name_from_id(ns, bins[i]->id));
		}
		else {
			cf_crash(AS_PROTO, "making response message with null bin and op");
		}

		if (bins[i]) {
			msg_sz += as_bin_particle_client_value_size(bins[i]);
		}
	}

	if (trid != 0) {
		msg_sz += sizeof(as_msg_field) + sizeof(trid);
	}

	uint32_t setname_len = 0;

	if (setname) {
		setname_len = strlen(setname);
		msg_sz += sizeof(as_msg_field) + setname_len;
	}

	uint8_t *b;

	if (! msgp_in || *msg_sz_in < msg_sz) {
		b = cf_malloc(msg_sz);

		if (! b) {
			return NULL;
		}
	}
	else {
		b = (uint8_t *)msgp_in;
	}

	*msg_sz_in = msg_sz;

	uint8_t *buf = b;
	cl_msg *msgp = (cl_msg *)buf;

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type = PROTO_TYPE_AS_MSG;
	msgp->proto.sz = msg_sz - sizeof(as_proto);
	as_proto_swap(&msgp->proto);

	as_msg *m = &msgp->msg;

	m->header_sz = sizeof(as_msg);
	m->info1 = 0;
	m->info2 = 0;
	m->info3 = 0;
	m->unused = 0;
	m->result_code = result_code;
	m->generation = generation;
	m->record_ttl = void_time;
	m->transaction_ttl = 0;
	m->n_ops = bin_count;
	m->n_fields = 0;

	buf += sizeof(cl_msg);

	if (trid != 0) {
		m->n_fields++;

		as_msg_field *trfield = (as_msg_field *)buf;

		trfield->field_sz = 1 + sizeof(uint64_t);
		trfield->type = AS_MSG_FIELD_TYPE_TRID;
		*(uint64_t *)trfield->data = cf_swap_to_be64(trid);

		buf += sizeof(as_msg_field) + sizeof(uint64_t);
		as_msg_swap_field(trfield);
	}

	if (setname) {
		m->n_fields++;

		as_msg_field *trfield = (as_msg_field *)buf;

		trfield->field_sz = 1 + setname_len;
		trfield->type = AS_MSG_FIELD_TYPE_SET;
		memcpy(trfield->data, setname, setname_len);

		buf += sizeof(as_msg_field) + setname_len;
		as_msg_swap_field(trfield);
	}

	as_msg_swap_header(m);

	for (uint16_t i = 0; i < bin_count; i++) {
		as_msg_op *op = (as_msg_op *)buf;

		op->version = 0;

		if (ops) {
			op->op = ops[i]->op;
			memcpy(op->name, ops[i]->name, ops[i]->name_sz);
			op->name_sz = ops[i]->name_sz;
		}
		else {
			op->op = AS_MSG_OP_READ;
			op->name_sz = as_bin_memcpy_name(ns, op->name, bins[i]);
		}

		op->op_sz = 4 + op->name_sz;

		buf += sizeof(as_msg_op) + op->name_sz;
		buf += as_bin_particle_to_client(bins[i], op);

		as_msg_swap_op(op);
	}

	return (cl_msg *)b;
}


// Send a response made by write_local().
// TODO - refactor and share with as_msg_send_reply().
int
as_msg_send_ops_reply(as_file_handle *fd_h, cf_dyn_buf *db)
{
	if (! cf_socket_exists(&fd_h->sock)) {
		cf_crash(AS_PROTO, "fd is NULL");
	}

	if (cf_socket_send_all(&fd_h->sock, db->buf, db->used_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		cf_debug(AS_PROTO, "protocol write fail: fd %d sz %zu errno %d",
				CSFD(&fd_h->sock), db->used_sz, errno);
		as_end_of_transaction_force_close(fd_h);
		return -1;
	}

	as_end_of_transaction_ok(fd_h);
	return 0;
}


// NB: this uses the same logic as the bufbuild function
// as_msg_make_response_bufbuilder() but does not build a buffer and simply
// returns sizing information.  This is required for query runtime memory
// accounting.
// returns -1 in case of error
// otherwise returns resize value
size_t as_msg_response_msgsize(as_record *r, as_storage_rd *rd, bool nobindata,
		char *nsname, bool use_sets, cf_vector *binlist)
{

	// Sanity checks. Either rd should be there or nobindata and nsname should be present.
	if (!(rd || (nobindata && nsname))) {
		cf_detail(AS_PROTO, "Neither storage record nor nobindata is set. Skipping the record.");
		return -1;
	}

	// figure out the size of the entire buffer
	int         set_name_len = 0;
	const char *set_name     = NULL;
	int         ns_len       = rd ? strlen(rd->ns->name) : strlen(nsname);

	if (use_sets && as_index_get_set_id(r) != INVALID_SET_ID) {
		as_namespace *ns = NULL;

		if (rd) {
			ns = rd->ns;
		} else if (nsname) {
			ns = as_namespace_get_byname(nsname);
		}
		if (!ns) {
			cf_info(AS_PROTO, "Cannot get namespace, needed to get set information. Skipping record.");
			return -1;
		}
		set_name = as_index_get_set_name(r, ns);
		if (set_name) {
			set_name_len = strlen(set_name);
		}
	}

	int msg_sz = sizeof(as_msg);
	msg_sz += sizeof(as_msg_field) + sizeof(cf_digest);
	msg_sz += sizeof(as_msg_field) + ns_len;
	if (set_name) {
		msg_sz += sizeof(as_msg_field) + set_name_len;
	}

	int in_use_bins = as_bin_inuse_count(rd);
	int list_bins   = 0;

	if (nobindata == false) {
		if(binlist) {
			int binlist_sz = cf_vector_size(binlist);
			for(uint16_t i = 0; i < binlist_sz; i++) {
				char binname[AS_ID_BIN_SZ];
				cf_vector_get(binlist, i, (void*)&binname);
				cf_debug(AS_PROTO, " Binname projected inside is |%s|", binname);
				as_bin *p_bin = as_bin_get(rd, binname);
				if (!p_bin)
				{
					cf_debug(AS_PROTO, "To be projected bin |%s| not found", binname);
					continue;
				}
				cf_debug(AS_PROTO, "Adding bin |%s| to projected bins", binname);
				list_bins++;
				msg_sz += sizeof(as_msg_op);
				msg_sz += rd->ns->single_bin ? 0 : strlen(binname);
				msg_sz += (int)as_bin_particle_client_value_size(p_bin);
			}
		}
		else {
			msg_sz += sizeof(as_msg_op) * in_use_bins; // the bin headers
			for (uint16_t i = 0; i < in_use_bins; i++) {
				as_bin *p_bin = &rd->bins[i];
				msg_sz += rd->ns->single_bin ? 0 : strlen(as_bin_get_name_from_id(rd->ns, p_bin->id));
				msg_sz += (int)as_bin_particle_client_value_size(p_bin);
			}
		}
	}
	return msg_sz;
}



int as_msg_make_response_bufbuilder(as_record *r, as_storage_rd *rd,
		cf_buf_builder **bb_r, bool nobindata, char *nsname, bool include_ldt_data,
		bool include_key, bool skip_empty_records, cf_vector *binlist)
{
	// Sanity checks. Either rd should be there or nobindata and nsname should be present.
	if (!(rd || (nobindata && nsname))) {
		cf_detail(AS_PROTO, "Neither storage record nor nobindata is set. Skipping the record.");
		return 0;
	}

	// figure out the size of the entire buffer
	int         set_name_len = 0;
	const char *set_name     = NULL;
	int         ns_len       = rd ? strlen(rd->ns->name) : strlen(nsname);

	if (as_index_get_set_id(r) != INVALID_SET_ID) {
		as_namespace *ns = NULL;

		if (rd) {
			ns = rd->ns;
		} else if (nsname) {
			ns = as_namespace_get_byname(nsname);
		}
		if (!ns) {
			cf_info(AS_PROTO, "Cannot get namespace, needed to get set information. Skipping record.");
			return -1;
		}
		set_name = as_index_get_set_name(r, ns);
		if (set_name) {
			set_name_len = strlen(set_name);
		}
	}

	uint8_t* key = NULL;
	uint32_t key_size = 0;

	if (include_key && as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED)) {
		if (! as_storage_record_get_key(rd)) {
			cf_info(AS_PROTO, "can't get key - skipping record");
			return -1;
		}

		key = rd->key;
		key_size = rd->key_size;
	}

	uint16_t n_fields = 2;
	int msg_sz = sizeof(as_msg);
	msg_sz += sizeof(as_msg_field) + sizeof(cf_digest);
	msg_sz += sizeof(as_msg_field) + ns_len;
	if (set_name) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + set_name_len;
	}
	if (key) {
		n_fields++;
		msg_sz += sizeof(as_msg_field) + key_size;
	}

	int list_bins   = 0;
	int in_use_bins = rd ? (int)as_bin_inuse_count(rd) : 0;
	as_val *ldt_bin_vals[in_use_bins];

	if (! nobindata) {
		if (binlist) {
			int binlist_sz = cf_vector_size(binlist);

			for (uint16_t i = 0; i < binlist_sz; i++) {
				char binname[AS_ID_BIN_SZ];

				cf_vector_get(binlist, i, (void*)&binname);

				as_bin *p_bin = as_bin_get(rd, binname);

				if (! p_bin) {
					continue;
				}

				msg_sz += sizeof(as_msg_op);
				msg_sz += rd->ns->single_bin ? 0 : strlen(binname);

				if (as_bin_is_hidden(p_bin)) {
					if (include_ldt_data) {
						msg_sz += (int)as_ldt_particle_client_value_size(rd, p_bin, &ldt_bin_vals[list_bins]);
					}
					else {
						ldt_bin_vals[list_bins] = NULL;
					}
				}
				else {
					msg_sz += (int)as_bin_particle_client_value_size(p_bin);
				}

				list_bins++;
			}

			// Don't return an empty record.
			if (skip_empty_records && list_bins == 0) {
				return 0;
			}
		}
		else {
			msg_sz += sizeof(as_msg_op) * in_use_bins;

			for (uint16_t i = 0; i < in_use_bins; i++) {
				as_bin *p_bin = &rd->bins[i];

				msg_sz += rd->ns->single_bin ? 0 : strlen(as_bin_get_name_from_id(rd->ns, p_bin->id));

				if (as_bin_is_hidden(p_bin)) {
					if (include_ldt_data) {
						msg_sz += (int)as_ldt_particle_client_value_size(rd, p_bin, &ldt_bin_vals[i]);
					}
					else {
						ldt_bin_vals[i] = NULL;
					}
				}
				else {
					msg_sz += (int)as_bin_particle_client_value_size(p_bin);
				}
			}
		}
	}

	uint8_t *b;
	cf_buf_builder_reserve(bb_r, msg_sz, &b);

	// set up the header
	uint8_t *buf = b;
	as_msg *msgp = (as_msg *) buf;

	msgp->header_sz = sizeof(as_msg);
	msgp->info1 = (nobindata ? AS_MSG_INFO1_GET_NOBINDATA : 0);
	msgp->info2 = 0;
	msgp->info3 = 0;
	msgp->unused = 0;
	msgp->result_code = 0;
	msgp->generation = r->generation;
	msgp->record_ttl = r->void_time;
	msgp->transaction_ttl = 0;
	msgp->n_fields = n_fields;
	if (rd) {
		if (binlist)
			msgp->n_ops = list_bins;
		else
			msgp->n_ops = in_use_bins;
	} else {
		msgp->n_ops = 0;
	}
	as_msg_swap_header(msgp);

	buf += sizeof(as_msg);

	as_msg_field *mf = (as_msg_field *) buf;
	mf->field_sz = sizeof(cf_digest) + 1;
	mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
	memcpy(mf->data, &r->keyd, sizeof(cf_digest));
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + sizeof(cf_digest);

	mf = (as_msg_field *) buf;
	mf->field_sz = ns_len + 1;
	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	if (rd) {
		memcpy(mf->data, rd->ns->name, ns_len);
	} else {
		memcpy(mf->data, nsname, ns_len);
	}
	as_msg_swap_field(mf);
	buf += sizeof(as_msg_field) + ns_len;

	if (set_name) {
		mf = (as_msg_field *) buf;
		mf->field_sz = set_name_len + 1;
		mf->type = AS_MSG_FIELD_TYPE_SET;
		memcpy(mf->data, set_name, set_name_len);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + set_name_len;
	}

	if (key) {
		mf = (as_msg_field *) buf;
		mf->field_sz = key_size + 1;
		mf->type = AS_MSG_FIELD_TYPE_KEY;
		memcpy(mf->data, key, key_size);
		as_msg_swap_field(mf);
		buf += sizeof(as_msg_field) + key_size;
	}

	if (nobindata) {
		return 0;
	}

	if (binlist) {
		list_bins = 0;

		int binlist_sz = cf_vector_size(binlist);

		for (uint16_t i = 0; i < binlist_sz; i++) {
			char binname[AS_ID_BIN_SZ];
			cf_vector_get(binlist, i, (void*)&binname);

			as_bin *p_bin = as_bin_get(rd, binname);

			if (! p_bin) {
				continue;
			}

			as_msg_op *op = (as_msg_op *)buf;

			op->op = AS_MSG_OP_READ;
			op->version = 0;
			op->name_sz = as_bin_memcpy_name(rd->ns, op->name, p_bin);
			op->op_sz = 4 + op->name_sz;

			buf += sizeof(as_msg_op) + op->name_sz;

			if (as_bin_is_hidden(p_bin)) {
				buf += as_ldt_particle_to_client(ldt_bin_vals[list_bins], op);
			}
			else {
				buf += as_bin_particle_to_client(p_bin, op);
			}

			list_bins++;

			as_msg_swap_op(op);
		}
	}
	else {
		for (uint16_t i = 0; i < in_use_bins; i++) {
			as_msg_op *op = (as_msg_op *)buf;

			op->op = AS_MSG_OP_READ;
			op->version = 0;
			op->name_sz = as_bin_memcpy_name(rd->ns, op->name, &rd->bins[i]);
			op->op_sz = 4 + op->name_sz;

			buf += sizeof(as_msg_op) + op->name_sz;

			if (as_bin_is_hidden(&rd->bins[i])) {
				buf += as_ldt_particle_to_client(ldt_bin_vals[i], op);
			}
			else {
				buf += as_bin_particle_to_client(&rd->bins[i], op);
			}

			as_msg_swap_op(op);
		}
	}

	return 0;
}

int
as_msg_make_error_response_bufbuilder(cf_digest *keyd, int result_code, cf_buf_builder **bb_r, char *nsname)
{
	// figure out the size of the entire buffer
	int ns_len = strlen(nsname);
	int msg_sz = sizeof(as_msg);
	msg_sz += sizeof(as_msg_field) + sizeof(cf_digest);
	msg_sz += sizeof(as_msg_field) + ns_len;

	uint8_t *b;
	cf_buf_builder_reserve(bb_r, msg_sz, &b);

	// set up the header
	uint8_t *buf = b;
	as_msg *msgp = (as_msg *) buf;

	msgp->header_sz = sizeof(as_msg);
	msgp->info1 = 0;
	msgp->info2 = 0;
	msgp->info3 = 0;
	msgp->unused = 0;
	msgp->result_code = result_code;
	msgp->generation = 0;
	msgp->record_ttl = 0;
	msgp->transaction_ttl = 0;
	msgp->n_fields = 2;
	msgp->n_ops = 0;
	as_msg_swap_header(msgp);

	buf += sizeof(as_msg);

	as_msg_field *mf = (as_msg_field *) buf;
	mf->field_sz = sizeof(cf_digest) + 1;
	mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
	memcpy(mf->data, keyd, sizeof(cf_digest));
	as_msg_swap_field(mf);

	buf += sizeof(as_msg_field) + sizeof(cf_digest);

	mf = (as_msg_field *) buf;
	mf->field_sz = ns_len + 1;
	mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, nsname, ns_len);
	as_msg_swap_field(mf);

	return(0);
}

//
// Transmit the response.
// Run over the as_msg_bin array. If there's a corresponding entry in the data bin,
// send that data.
//
// Note that if it's an "all bins" there might be no op.
// If it's a read with missing data, there might be no bin
// but you're guaranteed one or the other.

#define MSG_STACK_BUFFER_SZ (1024 * 16)

int
as_msg_send_reply(as_file_handle *fd_h, uint32_t result_code, uint32_t generation,
		uint32_t void_time, as_msg_op **ops, as_bin **bins, uint16_t bin_count,
		as_namespace *ns, uint64_t trid, const char *setname)
{
	// most cases are small messages - try to stack alloc if we can
	uint8_t fb[MSG_STACK_BUFFER_SZ];
	size_t msg_sz = sizeof(fb);
//	memset(fb,0xff,msg_sz);  // helpful to see what you might not be setting

	uint8_t *msgp = (uint8_t *) as_msg_make_response_msg( result_code, generation,
					void_time, ops, bins, bin_count, ns,
					(cl_msg *)fb, &msg_sz, trid, setname);

	if (!msgp)	return(-1);

	if (! cf_socket_exists(&fd_h->sock)) {
		cf_warning(AS_PROTO, "write to NULL fd internal error");
		cf_crash(AS_PROTO, "send reply: can't write to NULL fd");
	}

//	cf_detail(AS_PROTO, "write fd %d",fd);
	int rv;

	if (cf_socket_send_all(&fd_h->sock, msgp, msg_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		// Common when a client aborts.
		cf_debug(AS_PROTO, "protocol write fail: fd %d sz %zu",
				CSFD(&fd_h->sock), msg_sz);
		as_end_of_transaction_force_close(fd_h);
		rv = -1;
	}
	else {
		as_end_of_transaction_ok(fd_h);
		rv = 0;
	}

	if (msgp != fb)
		cf_free(msgp);

	return(rv);
}

bool
as_msg_peek_data_in_memory(const as_msg *m)
{
	as_msg_field *f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! f) {
		// Should never happen, but don't bark here.
		return false;
	}

	as_namespace *ns = as_namespace_get_bymsgfield(f);

	// If ns is null, don't be the first to bark.
	return ns && ns->storage_data_in_memory;
}

uint8_t *
as_msg_write_header(uint8_t *buf, size_t msg_sz, uint8_t info1, uint8_t info2,
		uint8_t info3, uint32_t generation, uint32_t record_ttl,
		uint32_t transaction_ttl, uint32_t n_fields, uint32_t n_ops)
{
	cl_msg *msg = (cl_msg *) buf;
	msg->proto.version = PROTO_VERSION;
	msg->proto.type = PROTO_TYPE_AS_MSG;
	msg->proto.sz = msg_sz - sizeof(as_proto);
	msg->msg.header_sz = sizeof(as_msg);
	msg->msg.info1 = info1;
	msg->msg.info2 = info2;
	msg->msg.info3 = info3;
	msg->msg.unused = 0;
	msg->msg.result_code = 0;
	msg->msg.generation = generation;
	msg->msg.record_ttl = record_ttl;
	msg->msg.transaction_ttl = transaction_ttl;
	msg->msg.n_fields = n_fields;
	msg->msg.n_ops = n_ops;
	return (buf + sizeof(cl_msg));
}

uint8_t * as_msg_write_fields(uint8_t *buf, const char *ns, int ns_len,
		const char *set, int set_len, const cf_digest *d, uint64_t trid)
{
	// printf("write_fields\n");
	// lay out the fields
	as_msg_field *mf = (as_msg_field *) buf;
	as_msg_field *mf_tmp = mf;

	if (ns) {
		mf->type = AS_MSG_FIELD_TYPE_NAMESPACE;
		mf->field_sz = ns_len + 1;
		// printf("write_fields: ns: write_fields: %d\n", mf->field_sz);
		memcpy(mf->data, ns, ns_len);
		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;
	}

	// Not currently used, but it's at least plausible we'll do so in future.
	if (set && set_len != 0) {
		mf->type = AS_MSG_FIELD_TYPE_SET;
		mf->field_sz = set_len + 1;
		//printf("write_fields: set: write_fields: %d\n", mf->field_sz);
		memcpy(mf->data, set, set_len);
		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;
	}

	// Not currently used, but it's at least plausible we'll do so in future.
	if (trid) {
		mf->type = AS_MSG_FIELD_TYPE_TRID;
		//Convert the transaction-id to network byte order (big-endian)
		uint64_t trid_nbo = __cpu_to_be64(trid); //swaps in place
		mf->field_sz = sizeof(trid_nbo) + 1;
		//printf("write_fields: trid: write_fields: %d\n", mf->field_sz);
		memcpy(mf->data, &trid_nbo, sizeof(trid_nbo));
		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;
	}

	if (d) {
		mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
		mf->field_sz = sizeof(cf_digest) + 1;
		memcpy(mf->data, d, sizeof(cf_digest));
		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;
	}

	return (uint8_t *) mf_tmp;
}

const char SUCCESS_BIN_NAME[] = "SUCCESS";
const char FAILURE_BIN_NAME[] = "FAILURE";
const int SUCCESS_BIN_NAME_LEN = (const int)sizeof(SUCCESS_BIN_NAME) - 1;
const int FAILURE_BIN_NAME_LEN = (const int)sizeof(FAILURE_BIN_NAME) - 1;

int
as_msg_make_val_response_bufbuilder(const as_val *val, cf_buf_builder **bb_r, int val_sz, bool success)
{
	int msg_sz        = sizeof(as_msg);
	msg_sz           += sizeof(as_msg_op) + val_sz;
	if (success) {
		msg_sz       += SUCCESS_BIN_NAME_LEN;  // fake bin name
	} else {
		msg_sz       += FAILURE_BIN_NAME_LEN;  // fake bin name
	}
	uint8_t *b;
	cf_buf_builder_reserve(bb_r, msg_sz, &b);

	// set up the header
	as_msg *msgp      = (as_msg *)b;

	msgp->header_sz   = sizeof(as_msg);
	msgp->info1       = 0;
	msgp->info2       = 0;
	msgp->info3       = 0;
	msgp->unused      = 0;
	msgp->result_code = 0; // Default is OK
	msgp->generation  = 0;
	msgp->record_ttl  = 0;
	msgp->transaction_ttl = 0;
	msgp->n_fields    = 0; // No Fields corresponding to aggregation response
	msgp->n_ops       = 1; // only 1 bin
	as_msg_swap_header(msgp);

	as_msg_op *op     = (as_msg_op *)(b + sizeof(as_msg));

	op->op            = AS_MSG_OP_READ;
	if (success) {
		op->name_sz = SUCCESS_BIN_NAME_LEN;
		memcpy(op->name, SUCCESS_BIN_NAME, op->name_sz);
	} else {
		op->name_sz = FAILURE_BIN_NAME_LEN;
		memcpy(op->name, FAILURE_BIN_NAME, op->name_sz);
	}
	op->op_sz         = 4 + op->name_sz;
	op->version       = 0;

	as_particle_asval_to_client(val, op);

	as_msg_swap_op(op);
	return(0);
}

int
as_msg_send_response(cf_socket *sock, uint8_t* buf, size_t len, int flags)
{
	if (cf_socket_send_all(sock, buf, len, flags, CF_SOCKET_TIMEOUT) < 0) {
		return -1;
	}

	return 0;
}

int
as_msg_send_fin(cf_socket *sock, uint32_t result_code)
{
	cl_msg m;
	m.proto.version = PROTO_VERSION;
	m.proto.type = PROTO_TYPE_AS_MSG;
	m.proto.sz = sizeof(as_msg);
	as_proto_swap(&m.proto);
	m.msg.header_sz = sizeof(as_msg);
	m.msg.info1 = 0;
	m.msg.info2 = 0;
	m.msg.info3 = AS_MSG_INFO3_LAST;
	m.msg.unused = 0;
	m.msg.result_code = result_code;
	m.msg.generation = 0;
	m.msg.record_ttl = 0;
	m.msg.transaction_ttl = 0;
	m.msg.n_fields = 0;
	m.msg.n_ops = 0;
	as_msg_swap_header(&m.msg);

	return as_msg_send_response(sock, (uint8_t*) &m, sizeof(m), MSG_NOSIGNAL);
}

#define AS_NETIO_MAX_IO_RETRY         5
static pthread_t      g_netio_th;
static pthread_t      g_netio_slow_th;
static cf_queue     * g_netio_queue      = 0;
static cf_queue     * g_netio_slow_queue = 0;

int
as_netio_send_packet(as_file_handle *fd_h, cf_buf_builder *bb_r, uint32_t *offset, bool blocking)
{
#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	uint32_t len  = bb_r->used_sz;
	uint8_t *buf  = bb_r->buf;

	as_proto proto;
	proto.version = PROTO_VERSION;
	proto.type    = PROTO_TYPE_AS_MSG;
	proto.sz      = len - 8;
	as_proto_swap(&proto);

    memcpy(bb_r->buf, &proto, 8); 

	uint32_t pos = *offset;

	ASD_QUERY_SENDPACKET_STARTING(nodeid, pos, len);

	int rv;
	int retry = 0;
	cf_detail(AS_PROTO," Start At %p %d %d", buf, pos, len);
	while (pos < len) {
		rv = cf_socket_send(&fd_h->sock, buf + pos, len - pos, MSG_NOSIGNAL);
		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_debug(AS_PROTO, "Packet send response error returned %d errno %d fd %d", rv, errno, CSFD(&fd_h->sock));
				return AS_NETIO_IO_ERR;
			}
			if (!blocking && (retry > AS_NETIO_MAX_IO_RETRY)) {
				*offset = pos;
				cf_detail(AS_PROTO," End At %p %d %d", buf, pos, len);
				ASD_QUERY_SENDPACKET_CONTINUE(nodeid, pos);
				return AS_NETIO_CONTINUE;
			}
			retry++;
			// bigger packets so try few extra times 
			usleep(100);
		}
		else {
			pos += rv;
		}
	}
	ASD_QUERY_SENDPACKET_FINISHED(nodeid);
	return AS_NETIO_OK;
}

void *
as_netio_th(void *q_to_wait_on) {
	cf_queue *           q = (cf_queue*)q_to_wait_on;
	while (true) {
		as_netio io;
		if (cf_queue_pop(q, &io, CF_QUEUE_FOREVER) != 0) {
			cf_crash(AS_PROTO, "Failed to pop from IO worker queue.");
		}
		if (io.slow) {
			usleep(g_config.proto_slow_netio_sleep_ms * 1000);
		}
		as_netio_send(&io, g_netio_slow_queue, false);
	}
}

void 
as_netio_init()
{
	g_netio_queue = cf_queue_create(sizeof(as_netio), true);
	if (!g_netio_queue)
		cf_crash(AS_PROTO, "Failed to create netio queue");
	if (pthread_create(&g_netio_th, NULL, as_netio_th, (void *)g_netio_queue))
		cf_crash(AS_PROTO, "Failed to create netio thread");

	g_netio_slow_queue = cf_queue_create(sizeof(as_netio), true);
	if (!g_netio_slow_queue)
		cf_crash(AS_PROTO, "Failed to create netio slow queue");
	if (pthread_create(&g_netio_slow_th, NULL, as_netio_th, (void *)g_netio_slow_queue))
		cf_crash(AS_PROTO, "Failed to create netio slow thread");
}

/*
 * Based on io object send buffer to the network, if fails
 * the queues it up to be picked by the asynchronous queueing
 * thread
 *
 * vtable:
 *
 * start_cb: Callback to the module before the real IO is started.
 *           it returns the status 
 *           AS_NETIO_OK: Everythin ok go ahead with IO
 *           AS_NETIO_ERR: If there was issue like abort/err/timeout etc.
 *
 * finish_cb: Callback to the module with the status code of the IO call
 *            AS_NETIO_OK: Everything went fine
 *            AS_NETIO_CONTINUE: The IO was requeued. Generally is noop in finish_cb
 *            AS_NETIO_ERR: IO erred out due to some issue.
 *
 *            The function should do the needful like release ref to user
 *            data etc.
 *
 * Return Code:
 * AS_NETIO_OK: Everything is fine normal code flow. Both the start_cb
 *              finish were called
 *
 * AS_NETIO_ERR: Something failed either in calling module start_cb or 
 *               while doing network IO. finish_cb is called.
 *              
 * Consumption:
 *     this function consumes qtr reference. It calls finish_cb which releases
 *     ref to qtr
 *     In case of AS_NETIO_CONTINUE: This function also consumes bb_r and ref for 
 *     fd_h. The background thread is responsible for freeing up bb_r and release
 *     ref to fd_h.
 */
int
as_netio_send(as_netio *io, void *q_to_use, bool blocking)
{
	cf_queue *q = (cf_queue *)q_to_use;

	int ret = io->start_cb(io, io->seq);

	if (ret == AS_NETIO_OK) {
		ret     = io->finish_cb(io, as_netio_send_packet(io->fd_h, io->bb_r, &io->offset, blocking));
	} 
	else {
		ret     = io->finish_cb(io, ret);
	}
    // If needs requeue then requeue it
	switch (ret) {
		case AS_NETIO_CONTINUE:
			if (!q) {
				cf_queue_push(g_netio_queue, io);
	 		}
			else {
				io->slow = true;
				cf_queue_push(q, io);
			}
			break;
		default:
            ret = AS_NETIO_OK;
			break;
	}
    return ret;
}
