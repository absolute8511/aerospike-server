/*
 * proto.c
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
#include <netinet/in.h>
#include <sys/socket.h>

#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_vector.h"

#include "dynbuf.h"
#include "fault.h"
#include "jem.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
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

#if 0 // if you don't have that nice linux swap
void
as_proto_swap_header(as_proto *p)
{
	uint8_t *buf = (uint8_t *)p;
	uint8_t _t;
	_t = buf[2];
	buf[2] = buf[7];
	buf[7] = _t;
	_t = buf[3];
	buf[3] = buf[6];
	buf[6] = _t;
	_t = buf[4];
	buf[4] = buf[5];
	buf[5] = _t;
}
#endif


void
as_msg_swap_header(as_msg *m)
{
	m->generation = ntohl(m->generation);
	m->record_ttl =  ntohl(m->record_ttl);
	m->transaction_ttl = ntohl(m->transaction_ttl);
	m->n_fields = ntohs(m->n_fields);
	m->n_ops = ntohs(m->n_ops);
}

void
as_msg_swap_op(as_msg_op *op)
{
	op->op_sz = ntohl(op->op_sz);
}

// fields better be swapped before you call this

int
as_msg_swap_ops(as_msg *m, void *limit)
{
	as_msg_op *op = 0;
	int *n = 0; // 0ing actually not necessary

	while ((op = as_msg_op_iterate(m, op, n))) {
		if ((void*)op >= limit) return(-1);
		as_msg_swap_op(op);
	}
	return(0);
}

void
as_msg_swap_field(as_msg_field *mf)
{
	mf->field_sz = ntohl(mf->field_sz);
}

// swaps all the fields but nothing else

int
as_msg_swap_fields(as_msg *m, void *limit)
{
	as_msg_field *mf = (as_msg_field *) m->data;

	for (int i = 0; i < m->n_fields; i++) {
		if ((void *)mf >= limit)	return(-1);
		as_msg_swap_field(mf);
		mf = as_msg_field_get_next(mf);
	}
	return(0);
}

int
as_msg_swap_fields_and_ops(as_msg *m, void *limit)
{
	as_msg_field *mf = (as_msg_field *) m->data;

	for (int i = 0; i < m->n_fields; i++) {
		if ((void *)mf >= limit) {
			return(-1);
		}
		as_msg_swap_field(mf);
		mf = as_msg_field_get_next(mf);
	}

	as_msg_op *op = (as_msg_op *)mf;
	for (int i = 0; i < m->n_ops; i++) {
		if ((void *)op >= limit) {
			return -1;
		}
		as_msg_swap_op(op);
		op = as_msg_op_get_next(op);
	}
	return(0);
}

//
// This function will attempt to fill the passed in buffer,
// but if too small, will malloc and return that.
// Either way it returns what it filled in.
//

//PROTOTYPE
int _as_particle_tobuf(as_bin *b, byte *buf, uint32_t *sz, bool tojson);

cl_msg *
as_msg_make_response_msg( uint32_t result_code, uint32_t generation, uint32_t void_time,
		as_msg_op **ops, as_bin **bins, uint16_t bin_count, as_namespace *ns,
		cl_msg *msgp_in, size_t *msg_sz_in, uint64_t trid, const char *setname)
{
	int setname_len = 0;
	// figure out the size of the entire buffer
	int msg_sz = sizeof(cl_msg);
	msg_sz += sizeof(as_msg_op) * bin_count; // the bin headers
	for (uint16_t i = 0; i < bin_count; i++) {
		if (bins[i]) {
			msg_sz += ns->single_bin ? 0 :
					  strlen(as_bin_get_name_from_id(ns, bins[i]->id));
			uint32_t psz;
			if (as_bin_is_hidden(bins[i])) {
				psz = 0;
			} else {
				_as_particle_tobuf(bins[i], 0, &psz, false); // get size
			}
			msg_sz += psz;
		}
		else if (ops[i])  // no bin, only op, no particle size
			msg_sz += ops[i]->name_sz;
		else
			cf_warning(AS_PROTO, "internal error!");
	}

	//If a transaction-id is sent by the client, we should send it back in a field
	if (trid != 0) {
		msg_sz += (sizeof(as_msg_field) + sizeof(trid));
	}

	// If setname is present, we will send it as a field. Account for its space overhead.
	if (setname != 0) {
		setname_len = strlen(setname);
		msg_sz += (sizeof(as_msg_field) + setname_len);
	}

	// most cases are small messages - try to stack alloc if we can
	byte *b;
	if ((0 == msgp_in) || (*msg_sz_in < msg_sz)) {
		b = cf_malloc(msg_sz);
		if (!b)	return(0);
	}
	else {
		b = (byte *) msgp_in;
	}
	*msg_sz_in = msg_sz;

	// set up the header
	byte *buf = b; // current buffer pointer
	cl_msg *msgp = (cl_msg *) buf;
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

	// Count the number of fields that we are going to send back
	if (trid != 0) {
		m->n_fields++;
	}
	if (setname != NULL) {
		m->n_fields++;
	}
	as_msg_swap_header(m);

	buf += sizeof(cl_msg);

	//If we have to send back the transaction-id, we have fields to send back
	if (trid != 0) {
		as_msg_field *trfield = (as_msg_field *) buf;
		//Allow space for the message field header
		buf += sizeof(as_msg_field);

		//Fill the field header
		trfield->type = AS_MSG_FIELD_TYPE_TRID;
		//Copy the transaction-id as field data in network byte order (big-endian)
		uint64_t trid_nbo = __cpu_to_be64(trid);
		trfield->field_sz = sizeof(trid_nbo);
		memcpy(trfield->data, &trid_nbo, sizeof(trid_nbo));
		as_msg_swap_field(trfield);

		//Allow space for the message field data
		buf += sizeof(trid_nbo);
	}

	// If we have to send back the setname, we have fields to send back
	if (setname != NULL) {
		as_msg_field *trfield = (as_msg_field *) buf;
		// Allow space for the message field header
		buf += sizeof(as_msg_field);

		// Fill the field header
		trfield->type = AS_MSG_FIELD_TYPE_SET;
		trfield->field_sz = setname_len + 1;
		memcpy(trfield->data, setname, setname_len);
		as_msg_swap_field(trfield);

		// Allow space for the message field data
		buf += setname_len;
	}

	// over all bins, copy into the buffer
	for (uint16_t i = 0; i < bin_count; i++) {

		as_msg_op *op = (as_msg_op *)buf;
		buf += sizeof(as_msg_op);

		op->op = AS_MSG_OP_READ;

		if (bins[i]) {
			op->version = as_bin_get_version(bins[i], ns->single_bin);
			op->name_sz = as_bin_memcpy_name(ns, op->name, bins[i]);
		}
		else {
			op->version = 0;
			memcpy(op->name, ops[i]->name, ops[i]->name_sz);
			op->name_sz = ops[i]->name_sz;
		}

		buf += op->name_sz;

		// cf_detail(AS_PROTO, "make response: bin %d %s : version %d",i,bins[i]->name,op->version);

		// Since there are two variable bits, the size is everything after the
		// data bytes - and this is only the head, we're patching up the rest
		// in a minute.
		op->op_sz = 4 + op->name_sz;

		if (bins[i] && as_bin_inuse(bins[i])) {
			op->particle_type = as_particle_type_convert(as_bin_get_particle_type(bins[i]));

			uint32_t psz = msg_sz - (buf - b); // size remaining in buffer, for safety
			if (as_bin_is_hidden(bins[i])) {
				op->particle_type = AS_PARTICLE_TYPE_NULL;
				psz = 0; // packet of size NULL
			} else {
				if (0 != _as_particle_tobuf(bins[i], buf, &psz, false)) {
					cf_warning(AS_PROTO, "particle to buf: could not copy data!");
				}
			}
			buf += psz;
			op->op_sz += psz;
		}
		else {
			op->particle_type = AS_PARTICLE_TYPE_NULL;
		}

		as_msg_swap_op(op);

	}

	return((cl_msg *) b);
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
				cf_debug(AS_PROTO, " Binname projected inside is |%s| \n", binname);
				as_bin *p_bin = as_bin_get (rd, (uint8_t*)binname, strlen(binname));
				if (!p_bin)
				{
					cf_debug(AS_PROTO, "To be projected bin |%s| not found \n", binname);
					continue;
				}
				cf_debug(AS_PROTO, "Adding bin |%s| to projected bins |%s| \n", binname);
				list_bins++;
				msg_sz += sizeof(as_msg_op);
				msg_sz += rd->ns->single_bin ? 0 : strlen(binname);
				uint32_t psz;
				as_particle_tobuf(p_bin, 0, &psz); // get size
				msg_sz += psz;
			}
		}
		else {
			msg_sz += sizeof(as_msg_op) * in_use_bins; // the bin headers
			for (uint16_t i = 0; i < in_use_bins; i++) {
				as_bin *p_bin = &rd->bins[i];
				msg_sz += rd->ns->single_bin ? 0 : strlen(as_bin_get_name_from_id(rd->ns, p_bin->id));
				uint32_t psz;
				as_particle_tobuf(p_bin, 0, &psz); // get size
				msg_sz += psz;
			}
		}
	}
	return msg_sz;
}



int as_msg_make_response_bufbuilder(as_record *r, as_storage_rd *rd,
		cf_buf_builder **bb_r, bool nobindata, char *nsname, bool use_sets,
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
	int in_use_bins = 0;
	if (rd) {
		in_use_bins = as_bin_inuse_count(rd);
	}

	if (nobindata == false) {
		if(binlist) {
			int binlist_sz = cf_vector_size(binlist);
			for(uint16_t i = 0; i < binlist_sz; i++) {
				char binname[AS_ID_BIN_SZ];
				cf_vector_get(binlist, i, (void*)&binname);
				cf_debug(AS_PROTO, " Binname projected inside is |%s| \n", binname);
				as_bin *p_bin = as_bin_get (rd, (uint8_t*)binname, strlen(binname));
				if (!p_bin)
				{
					cf_debug(AS_PROTO, "To be projected bin |%s| not found \n", binname);
					continue;
				}
				cf_debug(AS_PROTO, "Adding bin |%s| to projected bins |%s| \n", binname);
				list_bins++;
				msg_sz += sizeof(as_msg_op);
				msg_sz += rd->ns->single_bin ? 0 : strlen(binname);
				uint32_t psz;
				if (as_bin_is_hidden(p_bin)) {
					psz = 0;
				} else {
					as_particle_tobuf(p_bin, 0, &psz); // get size
				}
				msg_sz += psz;
			}

			// Don't return an empty record.
			if (skip_empty_records && list_bins == 0) {
				return 0;
			}
		}
		else {
			msg_sz += sizeof(as_msg_op) * in_use_bins; // the bin headers
			for (uint16_t i = 0; i < in_use_bins; i++) {
				as_bin *p_bin = &rd->bins[i];
				msg_sz += rd->ns->single_bin ? 0 : strlen(as_bin_get_name_from_id(rd->ns, p_bin->id));
				uint32_t psz;
				if (as_bin_is_hidden(p_bin)) {
					psz = 0;
				} else {
					as_particle_tobuf(p_bin, 0, &psz); // get size
				}
				msg_sz += psz;
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
	if (rd) {
		memcpy(mf->data, &rd->keyd, sizeof(cf_digest));
	} else {
		memcpy(mf->data, &r->key, sizeof(cf_digest));
	}
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
		goto Out;
	}

	if(binlist) {
		int binlist_sz = cf_vector_size(binlist);
		for(uint16_t i = 0; i < binlist_sz; i++) {

			char binname[AS_ID_BIN_SZ];
			cf_vector_get(binlist, i, (void*)&binname);
			cf_debug(AS_PROTO, " Binname projected inside is |%s| \n", binname);
			as_bin *p_bin = as_bin_get (rd, (uint8_t*)binname, strlen(binname));
			if (!p_bin) // should it be checked before ???
				continue;

			as_msg_op *op = (as_msg_op *)buf;
			buf += sizeof(as_msg_op);

			op->op = AS_MSG_OP_READ;

			op->name_sz = as_bin_memcpy_name(rd->ns, op->name, p_bin);
			buf += op->name_sz;

			// Since there are two variable bits, the size is everything after
			// the data bytes - and this is only the head, we're patching up
			// the rest in a minute.
			op->op_sz = 4 + op->name_sz;

			if (as_bin_inuse(p_bin)) {
				op->particle_type = as_particle_type_convert(as_bin_get_particle_type(p_bin));
				op->version = as_bin_get_version(p_bin, rd->ns->single_bin);

				uint32_t psz = msg_sz - (buf - b); // size remaining in buffer, for safety
				if (as_bin_is_hidden(p_bin)) {
                	op->particle_type = AS_PARTICLE_TYPE_NULL;
					psz = 0;
				} else {
					if (0 != as_particle_tobuf(p_bin, buf, &psz)) {
						cf_warning(AS_PROTO, "particle to buf: could not copy data!");
					}
				}
				buf += psz;
				op->op_sz += psz;
			}
			else {
				cf_debug(AS_PROTO, "Whoops !! bin not in use");
				op->particle_type = AS_PARTICLE_TYPE_NULL;
			}
			as_msg_swap_op(op);
		}
	}
	else {
		// over all bins, copy into the buffer
		for (uint16_t i = 0; i < in_use_bins; i++) {

			as_msg_op *op = (as_msg_op *)buf;
			buf += sizeof(as_msg_op);

			op->op = AS_MSG_OP_READ;

			op->name_sz = as_bin_memcpy_name(rd->ns, op->name, &rd->bins[i]);
			buf += op->name_sz;

			// Since there are two variable bits, the size is everything after
			// the data bytes - and this is only the head, we're patching up
			// the rest in a minute.
			op->op_sz = 4 + op->name_sz;

			if (as_bin_inuse(&rd->bins[i])) {
				op->particle_type = as_particle_type_convert(as_bin_get_particle_type(&rd->bins[i]));
				op->version = as_bin_get_version(&rd->bins[i], rd->ns->single_bin);

				uint32_t psz = msg_sz - (buf - b); // size remaining in buffer, for safety
				if (as_bin_is_hidden(&rd->bins[i])) {
                	op->particle_type = AS_PARTICLE_TYPE_NULL;
					psz = 0;
				} else {
					if (0 != as_particle_tobuf(&rd->bins[i], buf, &psz)) {
						cf_warning(AS_PROTO, "particle to buf: could not copy data!");
					}
				}
				buf += psz;
				op->op_sz += psz;
			}
			else {
				op->particle_type = AS_PARTICLE_TYPE_NULL;
			}
			as_msg_swap_op(op);
		}
	}
Out:
	return(0);
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
	buf += sizeof(as_msg_field) + ns_len;

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
		as_namespace *ns, uint *written_sz, uint64_t trid, const char *setname)
{
	int rv = 0;

	// most cases are small messages - try to stack alloc if we can
	byte fb[MSG_STACK_BUFFER_SZ];
	size_t msg_sz = sizeof(fb);
//	memset(fb,0xff,msg_sz);  // helpful to see what you might not be setting

	uint8_t *msgp = (uint8_t *) as_msg_make_response_msg( result_code, generation,
					void_time, ops, bins, bin_count, ns,
					(cl_msg *)fb, &msg_sz, trid, setname);

	if (!msgp)	return(-1);

	if (fd_h->fd == 0) {
		cf_warning(AS_PROTO, "write to fd 0 internal error");
		cf_crash(AS_PROTO, "send reply: can't write to fd 0");
	}

//	cf_detail(AS_PROTO, "write fd %d",fd);

	size_t pos = 0;
	while (pos < msg_sz) {
		int rv = send(fd_h->fd, msgp + pos, msg_sz - pos, MSG_NOSIGNAL);
		if (rv > 0) {
			pos += rv;
		}
		else if (rv < 0) {
			if (errno != EWOULDBLOCK) {
				// common message when a client aborts
				cf_debug(AS_PROTO, "protocol write fail: fd %d sz %zd pos %zd rv %d errno %d", fd_h->fd, msg_sz, pos, rv, errno);
				shutdown(fd_h->fd, SHUT_RDWR);
				rv = -1;
				goto Exit;
			}
			usleep(1); // Yield
		} else {
			cf_info(AS_PROTO, "protocol write fail zero return: fd %d sz %d pos %d ", fd_h->fd, msg_sz, pos);
			shutdown(fd_h->fd, SHUT_RDWR);
			rv = -1;
			goto Exit;
		}
	}

	// good for stats as a higher layer
	if (written_sz) *written_sz = msg_sz;

Exit:
	if ((uint8_t *)msgp != fb)
		cf_free(msgp);

	fd_h->t_inprogress = false;
	AS_RELEASE_FILE_HANDLE(fd_h);

	return(rv);

}

int
as_msg_send_error(as_file_handle *fd_h, uint32_t result_code)
{
	return as_msg_send_reply(fd_h, result_code, 0, 0, NULL, NULL, 0, NULL, NULL, 0, NULL);
}

bool
as_msg_peek_data_in_memory(cl_msg *msgp)
{
	if (! msgp ||
			msgp->proto.version != PROTO_VERSION ||
			msgp->proto.type != PROTO_TYPE_AS_MSG) {
		return false;
	}

	as_msg_field *f = as_msg_field_get(&msgp->msg, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! f) {
		return false;
	}

	as_namespace *ns = as_namespace_get_bymsgfield_unswap(f);

	return ns && ns->storage_data_in_memory;
}

/*
** this function works on unswapped data
*/

void
as_msg_peek( cl_msg *msgp, proto_peek *peek )
{
	memset(peek, 0, sizeof(proto_peek));

	if (msgp == 0) return;

	if (msgp->proto.version != PROTO_VERSION ||
			msgp->proto.type != PROTO_TYPE_AS_MSG) {
		return;
	}

	as_msg *m = &msgp->msg;
	peek->info1 = m->info1;
	peek->info2 = m->info2;

	if (m->n_fields == 0) {
		return;
	}

	int n_fields = m->n_fields;
	bool swap = n_fields < 10 ? false : true;
	if (swap) n_fields = ntohs(n_fields);

	as_msg_field *kdfp = 0;
	as_msg_field *sfp = 0;
	as_msg_field *kfp = 0;
	as_msg_field *nfp = 0;

	// over all the fields
	as_msg_field *mf = (as_msg_field *) m->data;
	uint i = 0;
	for (; i < n_fields ; i++) {
		switch (mf->type) {
			case AS_MSG_FIELD_TYPE_DIGEST_RIPE:
				kdfp = mf;
				break;
			case AS_MSG_FIELD_TYPE_SET:
				sfp = mf;
				break;
			case AS_MSG_FIELD_TYPE_KEY:
				kfp = mf;
				break;
			case AS_MSG_FIELD_TYPE_NAMESPACE:
				nfp = mf;
				break;
		}
		if (swap)
			mf = as_msg_field_get_next_unswap(mf);
		else
			mf = as_msg_field_get_next(mf);
	}

	// find the key
	if (kdfp) {
		peek->keyd = *(cf_digest *)kdfp->data;
	}
	else {
		if (kfp) {
			if (sfp == 0 || as_msg_field_get_value_sz(sfp) == 0) {
				int ksz = swap ? as_msg_field_get_value_sz_unswap(kfp) : as_msg_field_get_value_sz(kfp);
				cf_digest_compute(kfp->data, ksz, &peek->keyd);
			}
			else {
				int ksz = swap ? as_msg_field_get_value_sz_unswap(kfp) : as_msg_field_get_value_sz(kfp);
				int ssz = swap ? as_msg_field_get_value_sz_unswap(sfp) : as_msg_field_get_value_sz(sfp);
				cf_digest_compute2(sfp->data, ssz, kfp->data, ksz, &peek->keyd);
			}
		}
	}

	// find the namespace
	if (nfp) {
		uint32_t nsz = swap ? as_msg_field_get_value_sz_unswap(nfp) : as_msg_field_get_value_sz(nfp);
		if (nsz >= AS_ID_NAMESPACE_SZ) goto no_ns; // this should be illegal
		for (int i = 0; i < g_config.namespaces; i++) {
			tsvc_namespace_devices *ndev = &g_tsvc_devices_a[i];
			if (ndev->n_sz != nsz) continue;
			if (0 == memcmp(ndev->n_name, nfp->data, nsz)) {
				peek->ns_queue_offset = ndev->queue_offset;
				peek->ns_n_devices = ndev->n_devices;
				/*
				if (peek->info1 & AS_MSG_INFO1_READ) {
					cf_info(AS_PROTO, "read peek %s gives ofst %d n_dev %d",ndev->n_name,peek->ns_queue_offset,peek->ns_n_devices);
				} else {
					cf_info(AS_PROTO, "write peek %s gives ofst %d n_dev %d",ndev->n_name,peek->ns_queue_offset,peek->ns_n_devices);
				}
				*/
				break;
			}
		}
	}
no_ns:

	return;
}

uint8_t *
as_msg_write_header(uint8_t *buf, size_t msg_sz, uint info1, uint info2,
		uint info3, uint32_t generation, uint32_t record_ttl,
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
		const char *set, int set_len, const cf_digest *d, cf_digest *d_ret,
		uint64_t trid, as_msg_field *scan_param_field, void * c)
{
	udf_call * call = (udf_call*) c;
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

	if (set && set_len != 0) {
		mf->type = AS_MSG_FIELD_TYPE_SET;
		mf->field_sz = set_len + 1;
		//printf("write_fields: set: write_fields: %d\n", mf->field_sz);
		memcpy(mf->data, set, set_len);
		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;
	}

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

	if (scan_param_field) {
		mf->type = AS_MSG_FIELD_TYPE_SCAN_OPTIONS;
		mf->field_sz = sizeof(as_msg_field) + 1;
		//printf("write_fields: scan: write_fields: %d\n", mf->field_sz);
		memcpy(mf->data, scan_param_field, sizeof(as_msg_field));
		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;
	}

	/**
	 * UDF
	 */
	if ( call ) {

		int len = 0;

		// Append filename to message fields
		len = strlen(call->filename) * sizeof(char);
		mf->type = AS_MSG_FIELD_TYPE_UDF_FILENAME;
		mf->field_sz =  len + 1;
		memcpy(mf->data, call->filename, len);

		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;

		// Append function name to message fields
		len = strlen(call->function) * sizeof(char);
		mf->type = AS_MSG_FIELD_TYPE_UDF_FUNCTION;
		mf->field_sz =  len + 1;
		memcpy(mf->data, call->function, len);

		mf_tmp = as_msg_field_get_next(mf);
		mf = mf_tmp;

		// Append arglist to message fields
		if (call->arglist) {
			len = call->arglist->field_sz * sizeof(char);
			mf->type = AS_MSG_FIELD_TYPE_UDF_ARGLIST;
			mf->field_sz = len + 1;
			memcpy(mf->data, call->arglist->data, len);

			mf_tmp = as_msg_field_get_next(mf);
			mf = mf_tmp;
		}

	}
	if (d) {
		mf->type = AS_MSG_FIELD_TYPE_DIGEST_RIPE;
		mf->field_sz = sizeof(cf_digest) + 1;
		memcpy(mf->data, d, sizeof(cf_digest));
		mf_tmp = as_msg_field_get_next(mf);
		if (d_ret)
			memcpy(d_ret, d, sizeof(cf_digest));

		mf = mf_tmp;

	}
	return ( (uint8_t *) mf_tmp );
}

int
as_msg_make_val_response_bufbuilder(const as_val *val, cf_buf_builder **bb_r, int val_sz, bool success)
{
	int msg_sz        = sizeof(as_msg);
	msg_sz           += sizeof(as_msg_op) + val_sz;
	if (success) {
		msg_sz       += strlen("SUCCESS");  // fake bin name
	} else {
		msg_sz       += strlen("FAILURE");  // fake bin name
	}
	uint8_t *b;
	cf_buf_builder_reserve(bb_r, msg_sz, &b);

	// set up the header
	uint8_t *buf      = b;
	as_msg *msgp      = (as_msg *) buf;

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

	buf              += sizeof(as_msg);
	as_msg_op *op     = (as_msg_op *)buf;
	buf              += sizeof(as_msg_op);

	op->op            = AS_MSG_OP_READ;
	if (success) {
		op->name_sz       = strlen("SUCCESS");
		memcpy(op->name, "SUCCESS", op->name_sz);
	} else {
		op->name_sz       = strlen("FAILURE");
		memcpy(op->name, "FAILURE", op->name_sz);
	}
	op->op_sz         = 4 + op->name_sz;
	op->particle_type = to_particle_type(as_val_type(val));
	op->version       = 0;
	buf              += op->name_sz;

	uint32_t psz      = msg_sz - (buf - b); // size remaining in buffer, for safety
	as_val_tobuf(val, buf, &psz);
	if (psz == 0) {
		cf_warning(AS_PROTO, "particle to buf: could not copy data!");
	}
	buf              += psz;
	op->op_sz        += psz;
	as_msg_swap_op(op);
	return(0);
}

int
as_msg_send_response(int fd, uint8_t* buf, size_t len, int flags)
{
	int rv;
	int pos = 0;

	while (pos < len) {
		rv = send(fd, buf + pos, len - pos, flags);

		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_info(AS_PROTO, "send response error returned %d errno %d fd %d", rv, errno, fd);
				return -1;
			}
		}
		else {
			pos += rv;
		}
	}
	return 0;
}

int
as_msg_send_fin(int fd, uint32_t result_code)
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

	return as_msg_send_response(fd, (uint8_t*) &m, sizeof(m), MSG_NOSIGNAL);
}

#define AS_NETIO_MAX_IO_RETRY         5
static pthread_t      g_netio_th;
static pthread_t      g_netio_slow_th;
static cf_queue     * g_netio_queue      = 0;
static cf_queue     * g_netio_slow_queue = 0;
void                * as_query__netio_th(void *q_to_wait_on);

int
as_query__send_packet(as_file_handle *fd_h, cf_buf_builder *bb_r, uint32_t *offset, bool blocking)
{
	uint32_t len  = bb_r->used_sz;
	uint8_t *buf  = bb_r->buf;

	as_proto proto;
	proto.version = PROTO_VERSION;
	proto.type    = PROTO_TYPE_AS_MSG;
	proto.sz      = len - 8;
	as_proto_swap(&proto);

    memcpy(bb_r->buf, &proto, 8); 

	uint32_t pos = *offset;
	int rv;
	int retry = 0;
	cf_detail(AS_PROTO," Start At %p %d %d", buf, pos, len);
	while (pos < len) {
		rv = send(fd_h->fd, buf + pos, len - pos, MSG_NOSIGNAL );
		if (rv <= 0) {
			if (errno != EAGAIN) {
				cf_warning(AS_PROTO, "Packet send response error returned %d errno %d fd %d", rv, errno, fd_h->fd);
				shutdown(fd_h->fd, SHUT_RDWR);
				return AS_NETIO_ERR;
			}
			if (!blocking && (retry > AS_NETIO_MAX_IO_RETRY)) {
				*offset = pos;
				cf_detail(AS_PROTO," End At %p %d %d", buf, pos, len);
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
		if (as_netio_send(&io, g_netio_slow_queue, false) != AS_NETIO_CONTINUE) {
			AS_RELEASE_FILE_HANDLE(io.fd_h);
			cf_buf_builder_free(io.bb_r);
		};
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
 * Caller is responsible for freeing up stuff in in the io structure
 *
 * qtr related stuff will be taken care of when callback is called.
 * Callback is called upfront to be able check for timeout
 *
 * In case of success or failure module specific callback is called
 * returns nothing
 */
int
as_netio_send(as_netio *io, void *q_to_use, bool blocking)
{
	cf_queue *q = (cf_queue *)q_to_use;
	
   int ret = io->start_cb(io, io->seq);

	if (ret == AS_NETIO_OK) {
		ret     = io->finish_cb(io, as_query__send_packet(io->fd_h, io->bb_r, &io->offset, blocking));
	} 
	else {
		ret     = io->finish_cb(io, ret);
	}
    // If needs requeue then requeue it
	switch(ret) {
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
