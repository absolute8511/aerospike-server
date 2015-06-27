/*
 * transaction.c
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
 * Operations on transactions
 */

#include "base/transaction.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/thr_scan.h"
#include "base/udf_rw.h"


/* as_transaction_prepare
 * Prepare a transaction that has just been received from the wire.
 * NB: This should only be called once on any given transaction, because it swaps bytes! */

/*
** the layout for the digest is:
** type byte - set
** set's bytes
** type byte - key
** key's bytes
**
** Notice that in the case of the 'key', the payload includes a particle-type byte.
**
** return -2 means no digest no key
** return -3 means batch digest request
** return -4 means bad protocol data  (but no longer used??)
*/

/*
 * Code to init the fields in a transaction.  Use this instead of memset.
 *
 * NB: DO NOT SHUFFLE INIT ORDER .. it is based on elements found in the
 *     structure.
 */
void
as_transaction_init(as_transaction *tr, cf_digest *keyd, cl_msg *msgp)
{
	tr->msgp                      = msgp;
	if (keyd) {
		tr->keyd                  = *keyd;
		tr->preprocessed          = true;
	} else {
		tr->keyd                  = cf_digest_zero;
		tr->preprocessed          = false;
	}
	tr->incoming_cluster_key      = 0;
	tr->trid                      = 0;
	tr->flag                      = 0;
	tr->generation                = 0;
	tr->result_code               = AS_PROTO_RESULT_OK;
	tr->proto_fd_h                = 0;
	tr->start_time                = cf_getns();
	tr->end_time                  = 0;

	AS_PARTITION_RESERVATION_INIT(tr->rsv);

	tr->microbenchmark_time       = 0;
	tr->microbenchmark_is_resolve = false;

	tr->proxy_node                = 0;
	tr->proxy_msg                 = 0;

	tr->incoming_cluster_key      = 0;

	UREQ_DATA_INIT(&tr->udata);

	tr->batch_shared              = 0;
	tr->batch_index               = 0;

	tr->void_time                 = 0;
}

/*
  The transaction prepare function fills out the fields of the tr structure,
  using the information in the msg structure. It also swaps the fields in the
  message header (which are originally in network byte order).

  Once the prepare function has been called on the transaction, the
  'preprocessed' flag is set and the transaction cannot be prepared again.
  Returns:
  0:  OK
  -1: General Error
  -2: Request received with no key
  -3: Request received with digest array
*/
int as_transaction_prepare(as_transaction *tr) {
	cl_msg *msgp = tr->msgp;
	as_msg *m = &msgp->msg;

//	cf_assert(tr, AS_PROTO, CF_CRITICAL, "invalid transaction");

	void *limit = ((void *)m) + msgp->proto.sz;

	// Check processed flag.  It's a non-fatal error to call this function again.
	if( tr->preprocessed )
		return(0);

	if (0 != cf_digest_compare(&tr->keyd, &cf_digest_zero)) {
		cf_warning(AS_RW, "Internal inconsistency: transaction has keyd, but marked not swizzled");
	}

	as_msg_swap_header(m);
	if (0 != as_msg_swap_fields_and_ops(m, limit)) {
		cf_info(AS_PROTO, "msg swap field and ops returned error");
		return(-1);
	}

	if (m->n_fields>PROTO_NFIELDS_MAX_WARNING) {
		cf_info(AS_PROTO, "received too many n_fields! %d",m->n_fields);
    }

	// Set the transaction end time if available.
	if (m->transaction_ttl) {
//		cf_debug(AS_PROTO, "received non-zero transaction ttl: %d",m->transaction_ttl);
		tr->end_time = tr->start_time + ((uint64_t)m->transaction_ttl * 1000000);
	}

	// Set the preprocessed flag. All exits from here on out are considered
	// processed since the msg header has been swapped and the transaction
	// end time has been set.
	tr->preprocessed = true;
	tr->flag         = 0;
	UREQ_DATA_INIT(&tr->udata);
	as_msg_field *sfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY);
	if (sfp) {
		cf_debug(AS_PROTO, "received request with digest array, batch");
		return(-3);
	}

	// If the client had sent a transaction-id, set it as the attribute for the transaction.
	as_msg_field *tridfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_TRID);
	if (tridfp) {
		//We know that the network byte order is big-endian.
		//So, convert the 64bit integer from big-endian to host byte order.
		uint64_t trid_nbo;
		memcpy(&trid_nbo, tridfp->data, sizeof(trid_nbo));
		tr->trid = __be64_to_cpu(trid_nbo);
	}

	// Sent digest? Use!
	as_msg_field *dfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
	if (dfp) {
//		cf_detail(AS_PROTO, "transaction prepare: received sent digest\n");
		if (as_msg_field_get_value_sz(dfp) != sizeof(cf_digest)) {
			cf_info(AS_PROTO, "sent bad digest size %d, recomputing digest",as_msg_field_get_value_sz(dfp));
			goto Compute;
		}
		memcpy(&tr->keyd, dfp->data, sizeof(cf_digest));
	}
	// Not sent, so compute.
	else {
Compute:		;
		as_msg_field *kfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
		if (!kfp) {
			cf_detail(AS_PROTO, "received request with no key, scan");
			return(-2);
		}
        if (as_msg_field_get_value_sz(kfp)> PROTO_FIELD_LENGTH_MAX) {
	        cf_info(AS_PROTO, "key field too big %d. Is it for real?",as_msg_field_get_value_sz(kfp));
        }

		as_msg_field *sfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
		if (sfp == 0 || as_msg_field_get_value_sz(sfp) == 0) {
			cf_digest_compute(kfp->data, as_msg_field_get_value_sz(kfp), &tr->keyd);
			// cf_info(AS_PROTO, "computing key for sz %d %"PRIx64" bytes %d %d %d %d",as_msg_field_get_value_sz(kfp),*(uint64_t *)&tr->keyd,kfp->data[0],kfp->data[1],kfp->data[2],kfp->data[3]);
		}
		else {
            if (as_msg_field_get_value_sz(sfp)> PROTO_FIELD_LENGTH_MAX) {
		        cf_info(AS_PROTO, "set field too big %d. Is this for real?",as_msg_field_get_value_sz(sfp));
            }
			cf_digest_compute2(sfp->data, as_msg_field_get_value_sz(sfp),
						kfp->data, as_msg_field_get_value_sz(kfp),
						&tr->keyd);
			// cf_info(AS_PROTO, "computing set with key for sz %d %"PRIx64" bytes %d %d",as_msg_field_get_value_sz(kfp),*(uint64_t *)&tr->keyd,kfp->data[0],kfp->data[1],kfp->data[2],kfp->data[3]);
		}
	}

	return(0);
}

int
as_transaction_digest_validate(as_transaction *tr)
{
	cl_msg *msgp = tr->msgp;
	as_msg *m = &msgp->msg;

	cf_info(AS_PROTO, "digest compare succeeded");

	// Can only validate if we have two things to compare.
	as_msg_field *dfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
	if (dfp == 0) {
		cf_info(AS_PROTO, "no incoming protocol digest to validate");
		return(0);
	}
	if (as_msg_field_get_value_sz(dfp) != sizeof(cf_digest)) {
		cf_info(AS_PROTO, "sent bad digest size %d, can't validate",as_msg_field_get_value_sz(dfp));
		return(-1);
	}

	// Pull out the key and do the computation the same way as above.
	cf_digest computed;
	memset(&computed, 0, sizeof(cf_digest) );

	as_msg_field *kfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
	if (!kfp) {
		cf_info(AS_PROTO, "received request with no key and no digest, validation failed");
		return(-1);
	}

	as_msg_field *sfp = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
	if (sfp == 0 || as_msg_field_get_value_sz(sfp) == 0) {
		cf_digest_compute(kfp->data, as_msg_field_get_value_sz(kfp), &tr->keyd);
	}
	else {
		cf_digest_compute2(sfp->data, as_msg_field_get_value_sz(sfp),
					kfp->data, as_msg_field_get_value_sz(kfp),
					&computed);
	}

	if (0 == memcmp(&computed, &tr->keyd, sizeof(computed))) {
		cf_info(AS_PROTO, "digest compare failed. wire: %"PRIx64" computed: %"PRIx64,
			*(uint64_t *)&tr->keyd, *(uint64_t *) &computed );
		return(-1);
	}

	cf_info(AS_PROTO, "digest compare succeeded");

	return(0);
}

/* Create an internal transaction.
 * parameters:
 *     tr_create_data : Details for creating transaction
 *     tr             : to be filled
 *
 * return :  0  on success
 *           -1 on failure
 */
int
as_transaction_create( as_transaction *tr, tr_create_data *  trc_data)
{
	tr_create_data * d = (tr_create_data*) trc_data;
	udf_call * call    = d->call;
	uint64_t now       = cf_getns();

	// Get namespace and set lengths.
	int ns_len        = strlen(d->ns->name);
	int set_len       = strlen(d->set);

	// Figure out the size of the message.
	size_t  msg_sz = sizeof(cl_msg);

	msg_sz += sizeof(as_msg_field) + ns_len;

	if (set_len != 0) {
		msg_sz += sizeof(as_msg_field) + set_len;
	}

	msg_sz += sizeof(as_msg_field) + sizeof(cf_digest);
	msg_sz += sizeof(as_msg_field) + sizeof(d->trid);

	// Udf call structure will go as a part of the transaction udata.
	// Do not pack it in the message.
	cf_debug(AS_PROTO, "UDF : Msg size for internal transaction is %d", msg_sz);

	// Allocate space in the buffer.
	uint8_t * buf   = cf_malloc(msg_sz); memset(buf, 0, msg_sz);
	if (!buf) {
		return -1;
	}
	uint8_t * buf_r = buf;

	// Calculation of number of fields:
	// n_fields = ( ns ? 1 : 0 ) + (set ? 1 : 0) + (digest ? 1 : 0) + (trid ? 1 : 0) + (call ? 3 : 0);

	// Write the header.
	buf = as_msg_write_header(buf, msg_sz, 0, d->msg_type, 0, 0, 0, 0, 2 /*n_fields*/, 0);

	// Now write the fields.
	buf = as_msg_write_fields(buf, d->ns->name, ns_len, d->set, set_len, &(d->digest), 0, 0 , 0, 0);

	tr->incoming_cluster_key = 0;
	// Using the scan job fd. Reservation of this fd takes place at the
	// transaction service time, when the write_request is reserved.
	if (call->udf_type == AS_SCAN_UDF_OP_BACKGROUND) {
		tr->proto_fd_h = NULL;
	} else {
		cf_rc_reserve(d->fd_h);
		tr->proto_fd_h = d->fd_h;
	}
	tr->start_time   = now; // set transaction start time
	tr->end_time     = 0;   // TODO: should it timeout as scan parent job
	tr->proxy_node   = 0;   // will change if scan job can be proxied
	tr->proxy_msg    = 0;
	tr->trid         = 0;	// at this point we don't know the transaction-id
	tr->generation   = 0;
	tr->microbenchmark_time = 0;
	tr->flag         = 0;
	tr->msgp         = (cl_msg *) buf_r;
	tr->keyd         = d->digest;
	tr->preprocessed = true;
	AS_PARTITION_RESERVATION_INIT(tr->rsv);
	tr->result_code  = AS_PROTO_RESULT_OK;
	UREQ_DATA_INIT(&tr->udata);
	tr->batch_shared = 0;
	tr->batch_index = 0;
	tr->void_time = 0;
	return 0;
}

void
as_transaction_error(as_transaction* tr, uint32_t error_code)
{
	if (tr->proto_fd_h) {
		if (tr->batch_shared) {
			as_batch_add_error(tr->batch_shared, tr->batch_index, error_code);
			// Clear this transaction's msgp so calling code does not free it.
			tr->msgp = 0;
		}
		else {
			as_msg_send_reply(tr->proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, NULL, tr->trid, NULL);
			tr->proto_fd_h = 0;
			MICROBENCHMARK_HIST_INSERT_P(error_hist);
			cf_atomic_int_incr(&g_config.err_tsvc_requests);
		}
	}
}

// Helper to release transaction file handles.
void
as_release_file_handle(as_file_handle *proto_fd_h)
{
	close(proto_fd_h->fd);
	proto_fd_h->fh_info &= ~FH_INFO_DONOT_REAP;
	proto_fd_h->fd = -1;

	if (proto_fd_h->proto)	{
		as_proto *p = proto_fd_h->proto;

		if ((p->version != PROTO_VERSION) || (p->type >= PROTO_TYPE_MAX)) {
			cf_info(AS_AS, "release file handle: bad proto buf, corruption");
		}
		else {
			cf_free(proto_fd_h->proto);
			proto_fd_h->proto = NULL;
		}
	}

	if (proto_fd_h->security_filter) {
		as_security_filter_destroy(proto_fd_h->security_filter);
		proto_fd_h->security_filter = NULL;
	}

	cf_atomic_int_incr(&g_config.proto_connections_closed);
}
