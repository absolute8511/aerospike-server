/*
 * rw_utils.c
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

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h" // xdr_allows_write
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"
#include "msg.h"

#include "base/cfg.h" // xdr_allows_write
#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/proto.h" // xdr_allows_write
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

// TODO - really? we can't hide this behind an XDR stub?
bool
xdr_allows_write(as_transaction* tr)
{
	if (as_transaction_is_xdr(tr)) {
		if (tr->rsv.ns->ns_allow_xdr_writes) {
			return true;
		}
	}
	else {
		if (tr->rsv.ns->ns_allow_nonxdr_writes || tr->origin == FROM_NSUP) {
			return true;
		}
	}

	cf_atomic_int_incr(&tr->rsv.ns->n_fail_xdr_forbidden);

	return false;
}


void
send_rw_messages(rw_request* rw)
{
	for (int i = 0; i < rw->n_dest_nodes; i++) {
		if (rw->dest_complete[i]) {
			continue;
		}

		msg_incr_ref(rw->dest_msg);

		int rv = as_fabric_send(rw->dest_nodes[i], rw->dest_msg,
				AS_FABRIC_CHANNEL_RW);

		if (rv != AS_FABRIC_SUCCESS) {
			if (rv != AS_FABRIC_ERR_NO_NODE) {
				// Can't get AS_FABRIC_ERR_QUEUE_FULL for MEDIUM priority.
				cf_crash(AS_RW, "unexpected fabric send result %d", rv);
			}

			as_fabric_msg_put(rw->dest_msg);

			// Mark as complete although we won't have a response msg.
			rw->dest_complete[i] = true;
		}
	}
}


int
set_set_from_msg(as_record* r, as_namespace* ns, as_msg* m)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
	size_t name_len = (size_t)as_msg_field_get_value_sz(f);

	if (name_len == 0) {
		return 0;
	}

	// Given the name, find/assign the set-ID and write it in the as_index.
	return as_index_set_set_w_len(r, ns, (const char*)f->data, name_len, true);
}


// Caller must have checked that key is present in message.
bool
check_msg_key(as_msg* m, as_storage_rd* rd)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
	uint32_t key_size = as_msg_field_get_value_sz(f);
	uint8_t* key = f->data;

	if (key_size != rd->key_size || memcmp(key, rd->key, key_size) != 0) {
		cf_warning(AS_RW, "key mismatch - end of universe?");
		return false;
	}

	return true;
}


bool
get_msg_key(as_transaction* tr, as_storage_rd* rd)
{
	if (! as_transaction_has_key(tr)) {
		return true;
	}

	as_msg_field* f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_KEY);

	if (rd->ns->single_bin && rd->ns->storage_data_in_memory) {
		cf_warning(AS_RW, "{%s} can't store key if data-in-memory & single-bin",
				tr->rsv.ns->name);
		return false;
	}

	rd->key_size = as_msg_field_get_value_sz(f);
	rd->key = f->data;

	return true;
}


int
handle_msg_key(as_transaction* tr, as_storage_rd* rd)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	if (as_index_is_flag_set(rd->r, AS_INDEX_FLAG_KEY_STORED)) {
		// Key stored for this record - be sure it gets rewritten.

		// This will force a device read for non-data-in-memory, even if
		// must_fetch_data is false! Since there's no advantage to using the
		// loaded block after this if must_fetch_data is false, leave the
		// subsequent code as-is.
		if (! as_storage_record_get_key(rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} can't get stored key ",
					ns->name);
			return AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		// Check the client-sent key, if any, against the stored key.
		if (as_transaction_has_key(tr) && ! check_msg_key(m, rd)) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} key mismatch ", ns->name);
			return AS_PROTO_RESULT_FAIL_KEY_MISMATCH;
		}
	}
	// If we got a key without a digest, it's an old client, not a cue to store
	// the key. (Remove this check when we're sure all old C clients are gone.)
	else if (as_transaction_has_digest(tr)) {
		// Key not stored for this record - store one if sent from client. For
		// data-in-memory, don't allocate the key until we reach the point of no
		// return. Also don't set AS_INDEX_FLAG_KEY_STORED flag until then.
		if (! get_msg_key(tr, rd)) {
			return AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE;
		}
	}

	return 0;
}


void
update_metadata_in_index(as_transaction* tr, bool increment_generation,
		as_record* r)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	uint64_t now = cf_clepoch_milliseconds();

	switch (m->record_ttl) {
	case TTL_NAMESPACE_DEFAULT:
		if (ns->default_ttl != 0) {
			// Set record void-time using default TTL value.
			r->void_time = (now / 1000) + ns->default_ttl;
		}
		else {
			// Default TTL is "never expire".
			r->void_time = 0;
		}
		break;
	case TTL_NEVER_EXPIRE:
		// Set record to "never expire".
		r->void_time = 0;
		break;
	case TTL_DONT_UPDATE:
		// Do not change record's void time.
		break;
	default:
		// Apply non-special m->record_ttl directly. Have already checked
		// m->record_ttl <= 10 years, so no overflow etc.
		r->void_time = (now / 1000) + m->record_ttl;
		break;
	}

	if (as_ldt_record_is_sub(r)) {
		// Sub-records never expire by themselves.
		r->void_time = 0;
	}

	// Note - last-update-time is not allowed to go backwards!
	if (r->last_update_time < now) {
		r->last_update_time = now;
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
pickle_all(as_storage_rd* rd, rw_request* rw)
{
	if (as_record_pickle(rd->r, rd, &rw->pickled_buf, &rw->pickled_sz) != 0) {
		return false;
	}

	// TODO - we could avoid this copy (and maybe even not do this here at all)
	// if all callers malloc'd rd->rec_props.p_data upstream for hand-off...
	if (rd->rec_props.p_data) {
		rw->pickled_rec_props.size = rd->rec_props.size;
		rw->pickled_rec_props.p_data = cf_malloc(rd->rec_props.size);

		if (! rw->pickled_rec_props.p_data) {
			return false;
		}

		memcpy(rw->pickled_rec_props.p_data, rd->rec_props.p_data,
				rd->rec_props.size);
	}

	return true;
}


// If called for data-not-in-memory, this may read record from drive!
// TODO - rename as as_record_... and move to record.c?
void
record_delete_adjust_sindex(as_record* r, as_namespace* ns)
{
	if (! record_has_sindex(r, ns)) {
		return;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);
	as_storage_rd_load_n_bins(&rd);

	as_bin stack_bins[ns->storage_data_in_memory ? 0 : rd.n_bins];

	as_storage_rd_load_bins(&rd, stack_bins);

	remove_from_sindex(ns, as_index_get_set_name(r, ns), &r->keyd, rd.bins,
			rd.n_bins);

	as_storage_record_close(&rd);
}


// Remove record from secondary index. Called only for data-in-memory. If
// data-not-in-memory, existing record is not read, and secondary index entry is
// cleaned up by background sindex defrag thread.
// TODO - rename as as_record_... and move to record.c?
void
delete_adjust_sindex(as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;

	if (! record_has_sindex(rd->r, ns)) {
		return;
	}

	as_storage_rd_load_n_bins(rd);
	as_storage_rd_load_bins(rd, NULL);

	remove_from_sindex(ns, as_index_get_set_name(rd->r, ns), &rd->r->keyd,
			rd->bins, rd->n_bins);
}


// TODO - rename as as_record_..., move to record.c, take r instead of set_name,
// and lose keyd parameter?
void
remove_from_sindex(as_namespace* ns, const char* set_name, cf_digest* keyd,
		as_bin* bins, uint32_t n_bins)
{
	SINDEX_GRLOCK();

	SINDEX_BINS_SETUP(sbins, ns->sindex_cnt);

	as_sindex* si_arr[ns->sindex_cnt];
	int si_arr_index = 0;
	int sbins_populated = 0;

	// Reserve matching sindexes.
	for (int i = 0; i < (int)n_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				bins[i].id, &si_arr[si_arr_index]);
	}

	for (int i = 0; i < (int)n_bins; i++) {
		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, &bins[i],
				&sbins[sbins_populated], AS_SINDEX_OP_DELETE);
	}

	SINDEX_GRUNLOCK();

	if (sbins_populated) {
		as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, keyd);
		as_sindex_sbin_freeall(sbins, sbins_populated);
	}

	as_sindex_release_arr(si_arr, si_arr_index);
}


bool
xdr_must_ship_delete(as_namespace* ns, bool is_nsup_delete, bool is_xdr_op)
{
	if (! is_xdr_delete_shipping_enabled()) {
		return false;
	}

	// If this delete is a result of expiration/eviction, don't ship it unless
	// configured to do so.
	if (is_nsup_delete && ! is_xdr_nsup_deletes_enabled()) {
		return false;
	}

	return ! is_xdr_op ||
			// If this delete is a result of XDR shipping, don't ship it unless
			// configured to do so.
			is_xdr_forwarding_enabled() || ns->ns_forward_xdr_writes;
}
