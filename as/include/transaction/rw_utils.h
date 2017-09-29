/*
 * rw_utils.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/udf_record.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"
#include "transaction/udf.h"


//==========================================================
// Typedefs and constants.
//

typedef struct now_times_s {
	uint64_t now_ns;
	uint64_t now_ms;
} now_times;

typedef struct rw_paxos_change_struct_t {
	cf_node succession[AS_CLUSTER_SZ];
	cf_node deletions[AS_CLUSTER_SZ];
} rw_paxos_change_struct;

// For now, use only for as_msg record_ttl special values.
#define TTL_NAMESPACE_DEFAULT	0
#define TTL_NEVER_EXPIRE		((uint32_t)-1)
#define TTL_DONT_UPDATE			((uint32_t)-2)


//==========================================================
// Public API.
//

bool xdr_allows_write(as_transaction* tr);
void send_rw_messages(rw_request* rw);
bool generation_check(const as_record* r, const as_msg* m);
int set_set_from_msg(as_record* r, as_namespace* ns, as_msg* m);
int set_delete_durablility(const as_transaction* tr, as_storage_rd* rd);
bool check_msg_key(as_msg* m, as_storage_rd* rd);
bool get_msg_key(as_transaction* tr, as_storage_rd* rd);
int handle_msg_key(as_transaction* tr, as_storage_rd* rd);
void update_metadata_in_index(as_transaction* tr, bool increment_generation, as_record* r);
bool pickle_all(as_storage_rd* rd, rw_request* rw);
void record_delete_adjust_sindex(as_record* r, as_namespace* ns);
void delete_adjust_sindex(as_storage_rd* rd);
void remove_from_sindex(as_namespace* ns, const char* set_name, cf_digest* keyd, as_bin* bins, uint32_t n_bins);
bool xdr_must_ship_delete(as_namespace* ns, bool is_nsup_delete, bool is_xdr_op);


// TODO - rename as as_record_... and move to record.c?
static inline bool
record_has_sindex(const as_record* r, as_namespace* ns)
{
	if (! as_sindex_ns_has_sindex(ns)) {
		return false;
	}

	as_set* set = as_namespace_get_record_set(ns, r);

	return set ? set->n_sindexes != 0 : ns->n_setless_sindexes != 0;
}


static inline bool
respond_on_master_complete(as_transaction* tr)
{
	return tr->origin == FROM_CLIENT &&
			(g_config.respond_client_on_master_completion ||
			TRANSACTION_COMMIT_LEVEL(tr) == AS_POLICY_COMMIT_LEVEL_MASTER);
}


static inline void
destroy_stack_bins(as_bin* stack_bins, uint32_t n_bins)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin_particle_destroy(&stack_bins[i], true);
	}
}


// Not a nice way to specify a read-all op - dictated by backward compatibility.
// Note - must check this before checking for normal read op!
static inline bool
op_is_read_all(as_msg_op* op, as_msg* m)
{
	return op->name_sz == 0 && op->op == AS_MSG_OP_READ &&
			(m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
}


static inline bool
is_valid_ttl(as_namespace* ns, uint32_t ttl)
{
	// Note - for now, ttl must be as_msg record_ttl.
	// Note - ttl <= ns->max_ttl includes ttl == TTL_NAMESPACE_DEFAULT.
	return ttl <= ns->max_ttl ||
			ttl == TTL_NEVER_EXPIRE || ttl == TTL_DONT_UPDATE;
}


static inline void
clear_delete_response_metadata(rw_request* rw, as_transaction* tr)
{
	// If write became delete, respond to origin with no metadata.
	if (as_record_pickle_is_binless(rw->pickled_buf)) {
		tr->generation = 0;
		tr->void_time = 0;
		tr->last_update_time = 0;
	}
}


//==========================================================
// Private API - for enterprise separation only.
//

bool create_only_check(const as_record* r, const as_msg* m);
void write_delete_record(as_record* r, as_index_tree* tree);

udf_optype udf_finish_delete(udf_record* urecord);

void dup_res_flag_pickle(const uint8_t* buf, uint32_t* info);
bool dup_res_ignore_pickle(const uint8_t* buf, const msg* m);

void repl_write_flag_pickle(const as_transaction* tr, const uint8_t* buf, uint32_t* info);
bool repl_write_pickle_is_drop(const uint8_t* buf, uint32_t info);
