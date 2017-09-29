/*
 * migrate.c
 *
 * Copyright (C) 2008-2017 Aerospike, Inc.
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

// migrate.c
// Moves a partition from one machine to another using the fabric messaging
// system.


//==========================================================
// Includes.
//

#include "fabric/migrate.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h> // for alloca() only
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_rchash.h"
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "storage/storage.h"


//==========================================================
// Constants and typedefs.
//

const msg_template migrate_mt[] = {
		{ MIG_FIELD_OP, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_1, M_FT_UINT32 },
		{ MIG_FIELD_EMIG_ID, M_FT_UINT32 },
		{ MIG_FIELD_NAMESPACE, M_FT_BUF },
		{ MIG_FIELD_PARTITION, M_FT_UINT32 },
		{ MIG_FIELD_DIGEST, M_FT_BUF },
		{ MIG_FIELD_GENERATION, M_FT_UINT32 },
		{ MIG_FIELD_RECORD, M_FT_BUF },
		{ MIG_FIELD_CLUSTER_KEY, M_FT_UINT64 },
		{ MIG_FIELD_UNUSED_9, M_FT_BUF },
		{ MIG_FIELD_VOID_TIME, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_11, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_12, M_FT_BUF },
		{ MIG_FIELD_INFO, M_FT_UINT32 },
		{ MIG_FIELD_LDT_VERSION, M_FT_UINT64 },
		{ MIG_FIELD_LDT_PDIGEST, M_FT_BUF },
		{ MIG_FIELD_LDT_EDIGEST, M_FT_BUF },
		{ MIG_FIELD_LDT_PGENERATION, M_FT_UINT32 },
		{ MIG_FIELD_LDT_PVOID_TIME, M_FT_UINT32 },
		{ MIG_FIELD_LAST_UPDATE_TIME, M_FT_UINT64 },
		{ MIG_FIELD_FEATURES, M_FT_UINT32 },
		{ MIG_FIELD_UNUSED_21, M_FT_UINT32 },
		{ MIG_FIELD_META_RECORDS, M_FT_BUF },
		{ MIG_FIELD_META_SEQUENCE, M_FT_UINT32 },
		{ MIG_FIELD_META_SEQUENCE_FINAL, M_FT_UINT32 },
		{ MIG_FIELD_PARTITION_SIZE, M_FT_UINT64 },
		{ MIG_FIELD_SET_NAME, M_FT_BUF },
		{ MIG_FIELD_KEY, M_FT_BUF },
		{ MIG_FIELD_LDT_BITS, M_FT_UINT32 },
		{ MIG_FIELD_EMIG_INSERT_ID, M_FT_UINT64 }
};

COMPILER_ASSERT(sizeof(migrate_mt) / sizeof(msg_template) == NUM_MIG_FIELDS);

#define MIG_MSG_SCRATCH_SIZE 192

#define MIGRATE_RETRANSMIT_STARTDONE_MS 1000 // for now, not configurable
#define MIGRATE_RETRANSMIT_SIGNAL_MS 1000 // for now, not configurable
#define MAX_BYTES_EMIGRATING (16 * 1024 * 1024)

#define IMMIGRATION_DEBOUNCE_MS (60 * 1000) // 1 minute

typedef struct pickled_record_s {
	cf_digest     keyd;
	uint32_t      generation;
	uint32_t      void_time;
	uint64_t      last_update_time;
	uint8_t       *record_buf; // pickled!
	size_t        record_len;

	// For LDT only:
	cf_digest     pkeyd;
	cf_digest     ekeyd;
	uint64_t      ldt_version;
} pickled_record;

typedef enum {
	EMIG_RESULT_DONE,
	EMIG_RESULT_START,
	EMIG_RESULT_ERROR,
	EMIG_RESULT_EAGAIN
} emigration_result;

typedef enum {
	// Order matters - we use an atomic set-max that relies on it.
	EMIG_STATE_ACTIVE,
	EMIG_STATE_FINISHED,
	EMIG_STATE_ABORTED
} emigration_state;

typedef struct emigration_pop_info_s {
	uint32_t order;
	uint64_t dest_score;
	uint64_t n_elements;

	uint64_t avoid_dest;
} emigration_pop_info;

typedef struct emigration_reinsert_ctrl_s {
	uint64_t xmit_ms; // time of last xmit - 0 when done
	emigration *emig;
	msg *m;
} emigration_reinsert_ctrl;

typedef struct immigration_ldt_version_s {
	uint64_t incoming_ldt_version;
	uint16_t pid;
} __attribute__((__packed__)) immigration_ldt_version;


//==========================================================
// Globals.
//

cf_rchash *g_emigration_hash = NULL;
cf_rchash *g_immigration_hash = NULL;

static uint64_t g_avoid_dest = 0;
static cf_atomic32 g_emigration_id = 0;
static cf_queue g_emigration_q;
static shash *g_immigration_ldt_version_hash;


//==========================================================
// Forward declarations and inlines.
//

// Various initializers and destructors.
void emigration_init(emigration *emig);
void emigration_destroy(void *parm);
int emigration_reinsert_destroy_reduce_fn(const void *key, void *data, void *udata);
void immigration_destroy(void *parm);
void pickled_record_destroy(pickled_record *pr);

// Emigration.
void *run_emigration(void *arg);
void emigration_pop(emigration **emigp);
int emigration_pop_reduce_fn(void *buf, void *udata);
void emigration_hash_insert(emigration *emig);
void emigration_hash_delete(emigration *emig);
bool emigrate_transfer(emigration *emig);
emigration_result emigrate(emigration *emig);
emigration_result emigrate_tree(emigration *emig);
void *run_emigration_reinserter(void *arg);
void emigrate_tree_reduce_fn(as_index_ref *r_ref, void *udata);
bool emigrate_record(emigration *emig, msg *m);
int emigration_reinsert_reduce_fn(const void *key, void *data, void *udata);
emigration_result emigration_send_start(emigration *emig);
emigration_result emigration_send_done(emigration *emig);
void emigrate_signal(emigration *emig);

// Immigration.
void *run_immigration_reaper(void *arg);
int immigration_reaper_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata);

// Migrate fabric message handling.
int migrate_receive_msg_cb(cf_node src, msg *m, void *udata);
void immigration_handle_start_request(cf_node src, msg *m);
void immigration_ack_start_request(cf_node src, msg *m, uint32_t op);
void immigration_handle_insert_request(cf_node src, msg *m);
void immigration_handle_done_request(cf_node src, msg *m);
void immigration_handle_all_done_request(cf_node src, msg *m);
void emigration_handle_insert_ack(cf_node src, msg *m);
void emigration_handle_ctrl_ack(cf_node src, msg *m, uint32_t op);

// Info API helpers.
int emigration_dump_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata);
int immigration_dump_reduce_fn(const void *key, uint32_t keylen, void *object, void *udata);

// LDT-related.
int as_ldt_fill_mig_msg(const emigration *emig, msg *m, const pickled_record *pr, uint16_t ldt_bits, uint32_t *info);
void as_ldt_fill_precord(pickled_record *pr, uint16_t ldt_bits, as_storage_rd *rd, const emigration *emig);
int as_ldt_get_migrate_info(immigration *immig, as_record_merge_component *c, msg *m);


static inline uint32_t
immigration_hashfn(const void *value, uint32_t value_len)
{
	return ((const immigration_hkey *)value)->emig_id;
}


//==========================================================
// Public API.
//

void
as_migrate_init()
{
	g_avoid_dest = (uint64_t)g_config.self_node;

	cf_queue_init(&g_emigration_q, sizeof(emigration*), 4096, true);

	if (cf_rchash_create(&g_emigration_hash, cf_rchash_fn_u32,
			emigration_destroy, sizeof(uint32_t), 64,
			CF_RCHASH_CR_MT_MANYLOCK) != CF_RCHASH_OK) {
		cf_crash(AS_MIGRATE, "couldn't create emigration hash");
	}

	if (cf_rchash_create(&g_immigration_hash, immigration_hashfn,
			immigration_destroy, sizeof(immigration_hkey), 64,
			CF_RCHASH_CR_MT_BIGLOCK) != CF_RCHASH_OK) {
		cf_crash(AS_MIGRATE, "couldn't create immigration hash");
	}

	// Looks like an as_priority_thread_pool, but the reduce-pop is different.

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	for (uint32_t i = 0; i < g_config.n_migrate_threads; i++) {
		if (pthread_create(&thread, &attrs, run_emigration, NULL) != 0) {
			cf_crash(AS_MIGRATE, "failed to create emigration thread");
		}
	}

	if (pthread_create(&thread, &attrs, run_immigration_reaper, NULL) != 0) {
		cf_crash(AS_MIGRATE, "failed to create immigration reaper thread");
	}

	if (shash_create(&g_immigration_ldt_version_hash, cf_shash_fn_u32,
			sizeof(immigration_ldt_version), sizeof(void *), 64,
			SHASH_CR_MT_MANYLOCK) != SHASH_OK) {
		cf_crash(AS_MIGRATE, "couldn't create immigration ldt version hash");
	}

	as_fabric_register_msg_fn(M_TYPE_MIGRATE, migrate_mt, sizeof(migrate_mt),
			MIG_MSG_SCRATCH_SIZE, migrate_receive_msg_cb, NULL);
}


// Kicks off an emigration.
void
as_migrate_emigrate(const partition_migrate_record *pmr)
{
	emigration *emig = cf_rc_alloc(sizeof(emigration));

	cf_assert(emig, AS_MIGRATE, "failed emigration malloc");

	emig->dest = pmr->dest;
	emig->cluster_key = pmr->cluster_key;
	emig->id = cf_atomic32_incr(&g_emigration_id);
	emig->type = pmr->type;
	emig->tx_flags = pmr->tx_flags;
	emig->state = EMIG_STATE_ACTIVE;
	emig->aborted = false;

	// Create these later only when we need them - we'll get lots at once.
	emig->bytes_emigrating = 0;
	emig->reinsert_hash = NULL;
	emig->insert_id = 0;
	emig->ctrl_q = NULL;
	emig->meta_q = NULL;

	AS_PARTITION_RESERVATION_INIT(emig->rsv);
	as_partition_reserve_migrate(pmr->ns, pmr->pid, &emig->rsv, NULL);

	cf_atomic_int_incr(&emig->rsv.ns->migrate_tx_instance_count);

	// Generate new LDT version before starting the migration for a record.
	// This would mean that every time an outgoing migration is triggered it
	// will actually cause the system to create new version of the data.
	// It could possibly blow up the versions of subrec... Look at the
	// enhancement in migration algorithm which makes sure the migration
	// only happens in case data is different based on the comparison of
	// record rather than subrecord and cleans up old versions aggressively.
	//
	// No new version if data is migrating out of master.
	if (emig->rsv.ns->ldt_enabled) {
		emig->rsv.p->current_outgoing_ldt_version = as_ldt_generate_version();
		emig->tx_state = AS_PARTITION_MIG_TX_STATE_SUBRECORD;
	}
	else {
		emig->tx_state = AS_PARTITION_MIG_TX_STATE_RECORD;
		emig->rsv.p->current_outgoing_ldt_version = 0;
	}

	if (cf_queue_push(&g_emigration_q, &emig) != CF_QUEUE_OK) {
		cf_crash(AS_MIGRATE, "failed emigration queue push");
	}
}


// LDT-specific.
//
// Searches for incoming version based on passed in incoming migrate_ldt_vesion
// and partition_id. migrate rxstate match is also performed if it is passed.
// Check is skipped if zero.
// Return:
//     True:  If there is incoming migration
//     False: if no matching incoming migration found
bool
as_migrate_is_incoming(cf_digest *subrec_digest, uint64_t version,
		uint32_t partition_id, int rx_state)
{
	immigration *immig;
	immigration_ldt_version ldtv;

	ldtv.incoming_ldt_version = version;
	ldtv.pid = partition_id;

	if (shash_get(g_immigration_ldt_version_hash, &ldtv, &immig) == SHASH_OK) {
		return rx_state != 0 ? immig->rx_state == rx_state : true;
	}

	return false;
}


// Called via info command. Caller has sanity-checked n_threads.
void
as_migrate_set_num_xmit_threads(uint32_t n_threads)
{
	if (g_config.n_migrate_threads > n_threads) {
		// Decrease the number of migrate transmit threads to n_threads.
		while (g_config.n_migrate_threads > n_threads) {
			void *death_msg = NULL;

			// Send terminator (NULL message).
			if (cf_queue_push(&g_emigration_q, &death_msg) != CF_QUEUE_OK) {
				cf_warning(AS_MIGRATE, "failed to queue thread terminator");
				return;
			}

			g_config.n_migrate_threads--;
		}
	}
	else {
		// Increase the number of migrate transmit threads to n_threads.
		pthread_t thread;
		pthread_attr_t attrs;

		pthread_attr_init(&attrs);
		pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

		while (g_config.n_migrate_threads < n_threads) {
			if (pthread_create(&thread, &attrs, run_emigration, NULL) != 0) {
				cf_warning(AS_MIGRATE, "failed to create emigration thread");
				return;
			}

			g_config.n_migrate_threads++;
		}
	}
}


// Called via info command - print information about migration to the log.
void
as_migrate_dump(bool verbose)
{
	cf_info(AS_MIGRATE, "migration info:");
	cf_info(AS_MIGRATE, "---------------");
	cf_info(AS_MIGRATE, "number of emigrations in g_emigration_hash: %d",
			cf_rchash_get_size(g_emigration_hash));
	cf_info(AS_MIGRATE, "number of requested emigrations waiting in g_emigration_q : %d",
			cf_queue_sz(&g_emigration_q));
	cf_info(AS_MIGRATE, "number of immigrations in g_immigration_hash: %d",
			cf_rchash_get_size(g_immigration_hash));
	cf_info(AS_MIGRATE, "current emigration id: %d", g_emigration_id);

	if (verbose) {
		int item_num = 0;

		if (cf_rchash_get_size(g_emigration_hash) > 0) {
			cf_info(AS_MIGRATE, "contents of g_emigration_hash:");
			cf_info(AS_MIGRATE, "------------------------------");

			cf_rchash_reduce(g_emigration_hash, emigration_dump_reduce_fn,
					&item_num);
		}

		if (cf_rchash_get_size(g_immigration_hash) > 0) {
			item_num = 0;

			cf_info(AS_MIGRATE, "contents of g_immigration_hash:");
			cf_info(AS_MIGRATE, "-------------------------------");

			cf_rchash_reduce(g_immigration_hash, immigration_dump_reduce_fn,
					&item_num);
		}
	}
}


//==========================================================
// Local helpers - various initializers and destructors.
//

void
emigration_init(emigration *emig)
{
	shash_create(&emig->reinsert_hash, cf_shash_fn_u32, sizeof(uint64_t),
			sizeof(emigration_reinsert_ctrl), 16 * 1024, SHASH_CR_MT_MANYLOCK);

	cf_assert(emig->reinsert_hash, AS_MIGRATE, "failed to create hash");

	emig->ctrl_q = cf_queue_create(sizeof(int), true);

	cf_assert(emig->ctrl_q, AS_MIGRATE, "failed to create queue");

	emig->meta_q = emig_meta_q_create();
}


// Destructor handed to rchash.
void
emigration_destroy(void *parm)
{
	emigration *emig = (emigration *)parm;

	if (emig->reinsert_hash) {
		shash_reduce_delete(emig->reinsert_hash,
				emigration_reinsert_destroy_reduce_fn, NULL);
		shash_destroy(emig->reinsert_hash);
	}

	if (emig->ctrl_q) {
		cf_queue_destroy(emig->ctrl_q);
	}

	if (emig->meta_q) {
		emig_meta_q_destroy(emig->meta_q);
	}

	if (emig->rsv.p) {
		cf_atomic_int_decr(&emig->rsv.ns->migrate_tx_instance_count);

		as_partition_release(&emig->rsv);
	}
}


int
emigration_reinsert_destroy_reduce_fn(const void *key, void *data, void *udata)
{
	emigration_reinsert_ctrl *ri_ctrl = (emigration_reinsert_ctrl *)data;

	as_fabric_msg_put(ri_ctrl->m);

	return SHASH_REDUCE_DELETE;
}


void
emigration_release(emigration *emig)
{
	if (cf_rc_release(emig) == 0) {
		emigration_destroy((void *)emig);
		cf_rc_free(emig);
	}
}


// Destructor handed to rchash.
void
immigration_destroy(void *parm)
{
	immigration *immig = (immigration *)parm;
	immigration_ldt_version ldtv;

	ldtv.incoming_ldt_version = immig->incoming_ldt_version;
	ldtv.pid = immig->pid;

	if (immig->rsv.p) {
		as_partition_release(&immig->rsv);
	}

	shash_delete(g_immigration_ldt_version_hash, &ldtv);

	immig_meta_q_destroy(&immig->meta_q);

	cf_atomic_int_decr(&immig->ns->migrate_rx_instance_count);
}


void
immigration_release(immigration *immig)
{
	if (cf_rc_release(immig) == 0) {
		immigration_destroy((void *)immig);
		cf_rc_free(immig);
	}
}


void
pickled_record_destroy(pickled_record *pr)
{
	cf_free(pr->record_buf);
}


//==========================================================
// Local helpers - emigration.
//

void *
run_emigration(void *arg)
{
	while (true) {
		emigration *emig;

		emigration_pop(&emig);

		// This is the case for intentionally stopping the migrate thread.
		if (! emig) {
			break; // signal of death
		}

		if (emig->cluster_key != as_exchange_cluster_key()) {
			emigration_hash_delete(emig);
			continue;
		}

		as_namespace *ns = emig->rsv.ns;
		bool requeued = false;

		// Add the emigration to the global hash so acks can find it.
		emigration_hash_insert(emig);

		switch (emig->type) {
		case EMIG_TYPE_TRANSFER:
			cf_atomic_int_incr(&ns->migrate_tx_partitions_active);
			requeued = emigrate_transfer(emig);
			cf_atomic_int_decr(&ns->migrate_tx_partitions_active);
			break;
		case EMIG_TYPE_SIGNAL_ALL_DONE:
			cf_atomic_int_incr(&ns->migrate_signals_active);
			emigrate_signal(emig);
			cf_atomic_int_decr(&ns->migrate_signals_active);
			break;
		default:
			cf_crash(AS_MIGRATE, "bad emig type %u", emig->type);
			break;
		}

		if (! requeued) {
			emigration_hash_delete(emig);
		}
	}

	return NULL;
}


void
emigration_pop(emigration **emigp)
{
	emigration_pop_info best;

	best.order = 0xFFFFffff;
	best.dest_score = 0;
	best.n_elements = 0xFFFFffffFFFFffff;

	best.avoid_dest = 0;

	if (cf_queue_reduce_pop(&g_emigration_q, (void *)emigp, CF_QUEUE_FOREVER,
			emigration_pop_reduce_fn, &best) != CF_QUEUE_OK) {
		cf_crash(AS_MIGRATE, "emigration queue reduce pop failed");
	}
}


int
emigration_pop_reduce_fn(void *buf, void *udata)
{
	emigration_pop_info *best = (emigration_pop_info *)udata;
	emigration *emig = *(emigration **)buf;

	if (! emig || // null emig terminates thread
			emig->cluster_key != as_exchange_cluster_key()) {
		return -1; // process immediately
	}

	if (emig->ctrl_q && cf_queue_sz(emig->ctrl_q) > 0) {
		// This emig was requeued after its start command got an ACK_EAGAIN,
		// likely because dest hit 'migrate-max-num-incoming'. A new ack has
		// arrived - if it's ACK_OK, don't leave remote node hanging.

		return -1; // process immediately
	}

	if (emig->type == EMIG_TYPE_SIGNAL_ALL_DONE) {
		return -1; // process immediately
	}

	if (best->avoid_dest == 0) {
		best->avoid_dest = g_avoid_dest;
	}

	uint32_t order = emig->rsv.ns->migrate_order;
	uint64_t dest_score = (uint64_t)emig->dest - best->avoid_dest;
	uint64_t n_elements = as_index_tree_size(emig->rsv.tree);

	if (order < best->order ||
			(order == best->order &&
					(dest_score > best->dest_score ||
							(dest_score == best->dest_score &&
									n_elements < best->n_elements)))) {
		best->order = order;
		best->dest_score = dest_score;
		best->n_elements = n_elements;

		g_avoid_dest = (uint64_t)emig->dest;

		return -2; // candidate
	}

	return 0; // not interested
}


void
emigration_hash_insert(emigration *emig)
{
	if (! emig->ctrl_q) {
		emigration_init(emig); // creates emig->ctrl_q etc.

		cf_rchash_put(g_emigration_hash, (void *)&emig->id, sizeof(emig->id),
				(void *)emig);
	}
}


void
emigration_hash_delete(emigration *emig)
{
	if (emig->ctrl_q) {
		cf_rchash_delete(g_emigration_hash, (void *)&emig->id,
				sizeof(emig->id));
	}
	else {
		emigration_release(emig);
	}
}


bool
emigrate_transfer(emigration *emig)
{
	emigration_result result = emigrate(emig);
	as_namespace *ns = emig->rsv.ns;

	if (result == EMIG_RESULT_EAGAIN) {
		// Remote node refused migration, requeue and fetch another.
		if (cf_queue_push(&g_emigration_q, &emig) != CF_QUEUE_OK) {
			cf_crash(AS_MIGRATE, "failed emigration queue push");
		}

		return true; // requeued
	}

	if (result == EMIG_RESULT_DONE) {
		as_partition_emigrate_done(ns, emig->rsv.p->id, emig->cluster_key,
				emig->tx_flags);
	}
	else {
		cf_assert(result == EMIG_RESULT_ERROR, AS_MIGRATE, "unexpected emigrate result %d",
				result);
	}

	emig->tx_state = AS_PARTITION_MIG_TX_STATE_NONE;

	return false; // did not requeue
}


emigration_result
emigrate(emigration *emig)
{
	emigration_result result;

	//--------------------------------------------
	// Send START request.
	//
	if ((result = emigration_send_start(emig)) != EMIG_RESULT_START) {
		return result;
	}

	//--------------------------------------------
	// Send whole sub-tree - may block a while.
	//
	if (emig->rsv.ns->ldt_enabled) {
		if ((result = emigrate_tree(emig)) != EMIG_RESULT_DONE) {
			return result;
		}
	}

	emig->tx_state = AS_PARTITION_MIG_TX_STATE_RECORD;

	//--------------------------------------------
	// Send whole tree - may block a while.
	//
	if ((result = emigrate_tree(emig)) != EMIG_RESULT_DONE) {
		return result;
	}

	//--------------------------------------------
	// Send DONE request.
	//
	return emigration_send_done(emig);
}


emigration_result
emigrate_tree(emigration *emig)
{
	bool is_subrecord = emig->tx_state == AS_PARTITION_MIG_TX_STATE_SUBRECORD;
	as_index_tree *tree = is_subrecord ? emig->rsv.sub_tree : emig->rsv.tree;

	if (as_index_tree_size(tree) == 0) {
		return EMIG_RESULT_DONE;
	}

	cf_atomic32_set(&emig->state, EMIG_STATE_ACTIVE);

	pthread_t thread;

	if (pthread_create(&thread, NULL, run_emigration_reinserter, emig) != 0) {
		cf_crash(AS_MIGRATE, "could not start reinserter thread");
	}

	as_index_reduce(tree, emigrate_tree_reduce_fn, emig);

	// Sets EMIG_STATE_FINISHED only if not already EMIG_STATE_ABORTED.
	cf_atomic32_setmax(&emig->state, EMIG_STATE_FINISHED);

	pthread_join(thread, NULL);

	return emig->state == EMIG_STATE_ABORTED ?
			EMIG_RESULT_ERROR : EMIG_RESULT_DONE;
}


void *
run_emigration_reinserter(void *arg)
{
	emigration *emig = (emigration *)arg;
	emigration_state emig_state;

	// Reduce over the reinsert hash until finished.
	while ((emig_state = cf_atomic32_get(emig->state)) != EMIG_STATE_ABORTED) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			cf_atomic32_set(&emig->state, EMIG_STATE_ABORTED);
			return NULL;
		}

		usleep(1000);

		if (shash_get_size(emig->reinsert_hash) == 0) {
			if (emig_state == EMIG_STATE_FINISHED) {
				return NULL;
			}

			continue;
		}

		shash_reduce(emig->reinsert_hash, emigration_reinsert_reduce_fn,
				(void *)cf_getms());
	}

	return NULL;
}


void
emigrate_tree_reduce_fn(as_index_ref *r_ref, void *udata)
{
	emigration *emig = (emigration *)udata;
	as_namespace *ns = emig->rsv.ns;

	if (emig->aborted) {
		as_record_done(r_ref, ns);
		return; // no point continuing to reduce this tree
	}

	if (emig->cluster_key != as_exchange_cluster_key()) {
		as_record_done(r_ref, ns);
		emig->aborted = true;
		cf_atomic32_set(&emig->state, EMIG_STATE_ABORTED);
		return; // no point continuing to reduce this tree
	}

	if (! should_emigrate_record(emig, r_ref)) {
		as_record_done(r_ref, ns);
		return;
	}

	//--------------------------------------------
	// Read the record and pickle it.
	//

	as_index *r = r_ref->r;
	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	as_storage_rd_load_n_bins(&rd); // TODO - handle error returned

	as_bin stack_bins[ns->storage_data_in_memory ? 0 : rd.n_bins];

	as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned

	pickled_record pr;

	if (as_record_pickle(r, &rd, &pr.record_buf, &pr.record_len) != 0) {
		cf_warning(AS_MIGRATE, "failed migrate record pickle");
		as_storage_record_close(&rd);
		as_record_done(r_ref, ns);
		return;
	}

	pr.keyd = r->keyd;
	pr.generation = r->generation;
	pr.void_time = r->void_time;
	pr.last_update_time = r->last_update_time;

	as_storage_record_get_key(&rd);

	const char *set_name = as_index_get_set_name(r, ns);
	uint32_t ldt_bits = (uint32_t)as_ldt_record_get_rectype_bits(r);
	uint32_t key_size = rd.key_size;
	uint8_t key[key_size];

	if (key_size != 0) {
		memcpy(key, rd.key, key_size);
	}

	as_ldt_fill_precord(&pr, (uint16_t)ldt_bits, &rd, emig);

	as_storage_record_close(&rd);
	as_record_done(r_ref, ns);

	//--------------------------------------------
	// Fill and send the fabric message.
	//

	msg *m = as_fabric_msg_get(M_TYPE_MIGRATE);

	if (! m) {
		cf_warning(AS_MIGRATE, "imbalance: failed to get fabric msg");
		cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
		pickled_record_destroy(&pr);
		emig->aborted = true;
		cf_atomic32_set(&emig->state, EMIG_STATE_ABORTED);
		return;
	}

	uint32_t info = 0;

	if (as_ldt_fill_mig_msg(emig, m, &pr, (uint16_t)ldt_bits, &info) != 0) {
		// Skipping stale version subrecord shipping.
		as_fabric_msg_put(m);
		pickled_record_destroy(&pr);
		return;
	}

	emigration_flag_pickle(pr.record_buf, &info);

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_INSERT);
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_buf(m, MIG_FIELD_DIGEST, (const uint8_t *)&pr.keyd,
			sizeof(cf_digest), MSG_SET_COPY);
	msg_set_uint32(m, MIG_FIELD_GENERATION, pr.generation);
	msg_set_uint64(m, MIG_FIELD_LAST_UPDATE_TIME, pr.last_update_time);

	if (pr.void_time != 0) {
		msg_set_uint32(m, MIG_FIELD_VOID_TIME, pr.void_time);
	}

	if (info != 0) {
		msg_set_uint32(m, MIG_FIELD_INFO, info);
	}

	// Note - after MSG_SET_HANDOFF_MALLOCs, no need to destroy pickled_record.

	if (set_name) {
		msg_set_buf(m, MIG_FIELD_SET_NAME, (const uint8_t *)set_name,
				strlen(set_name), MSG_SET_COPY);
	}

	if (key_size != 0) {
		msg_set_buf(m, MIG_FIELD_KEY, key, key_size, MSG_SET_COPY);
	}

	if (ldt_bits != 0) {
		msg_set_uint32(m, MIG_FIELD_LDT_BITS, ldt_bits);
	}

	msg_set_buf(m, MIG_FIELD_RECORD, pr.record_buf, pr.record_len,
			MSG_SET_HANDOFF_MALLOC);

	// This might block if the queues are backed up but a failure is a
	// hard-fail - can't notify other side.
	if (! emigrate_record(emig, m)) {
		cf_warning(AS_MIGRATE, "imbalance: failed to emigrate record");
		cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
		emig->aborted = true;
		cf_atomic32_set(&emig->state, EMIG_STATE_ABORTED);
		return;
	}

	cf_atomic_int_incr(&ns->migrate_records_transmitted);

	if (ns->migrate_sleep != 0) {
		usleep(ns->migrate_sleep);
	}

	uint32_t waits = 0;

	while (cf_atomic32_get(emig->bytes_emigrating) > MAX_BYTES_EMIGRATING &&
			emig->cluster_key == as_exchange_cluster_key()) {
		usleep(1000);

		// Temporary paranoia to inform us old nodes aren't acking properly.
		if (++waits % (ns->migrate_retransmit_ms * 4) == 0) {
			cf_warning(AS_MIGRATE, "missing acks from node %lx", emig->dest);
		}
	}
}


bool
emigrate_record(emigration *emig, msg *m)
{
	uint64_t insert_id = emig->insert_id++;

	msg_set_uint64(m, MIG_FIELD_EMIG_INSERT_ID, insert_id);

	emigration_reinsert_ctrl ri_ctrl;

	msg_incr_ref(m); // the reference in the hash
	ri_ctrl.m = m;
	ri_ctrl.emig = emig;
	ri_ctrl.xmit_ms = cf_getms();

	if (shash_put(emig->reinsert_hash, &insert_id, &ri_ctrl) != SHASH_OK) {
		cf_warning(AS_MIGRATE, "emigrate record failed shash put");
		as_fabric_msg_put(m);
		return false;
	}

	cf_atomic32_add(&emig->bytes_emigrating, (int32_t)msg_get_wire_size(m));

	if (as_fabric_send(emig->dest, m, AS_FABRIC_CHANNEL_BULK) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}

	return true;
}


int
emigration_reinsert_reduce_fn(const void *key, void *data, void *udata)
{
	emigration_reinsert_ctrl *ri_ctrl = (emigration_reinsert_ctrl *)data;
	as_namespace *ns = ri_ctrl->emig->rsv.ns;
	uint64_t now = (uint64_t)udata;

	if (ri_ctrl->xmit_ms + ns->migrate_retransmit_ms < now) {
		msg_incr_ref(ri_ctrl->m);

		if (as_fabric_send(ri_ctrl->emig->dest, ri_ctrl->m,
				AS_FABRIC_CHANNEL_BULK) != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(ri_ctrl->m);
			return -1; // this will stop the reduce
		}

		ri_ctrl->xmit_ms = now;
		cf_atomic_int_incr(&ns->migrate_record_retransmits);
	}

	return 0;
}


emigration_result
emigration_send_start(emigration *emig)
{
	as_namespace *ns = emig->rsv.ns;
	msg *m = as_fabric_msg_get(M_TYPE_MIGRATE);

	if (! m) {
		cf_warning(AS_MIGRATE, "failed to get fabric msg");
		return EMIG_RESULT_ERROR;
	}

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START);
	msg_set_uint32(m, MIG_FIELD_FEATURES, MY_MIG_FEATURES);
	msg_set_uint64(m, MIG_FIELD_PARTITION_SIZE,
			as_index_tree_size(emig->rsv.tree));
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_uint64(m, MIG_FIELD_CLUSTER_KEY, emig->cluster_key);
	msg_set_buf(m, MIG_FIELD_NAMESPACE, (const uint8_t *)ns->name,
			strlen(ns->name), MSG_SET_COPY);
	msg_set_uint32(m, MIG_FIELD_PARTITION, emig->rsv.p->id);

	msg_set_uint64(m, MIG_FIELD_LDT_VERSION,
			emig->rsv.p->current_outgoing_ldt_version);

	uint64_t start_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			as_fabric_msg_put(m);
			return EMIG_RESULT_ERROR;
		}

		uint64_t now = cf_getms();

		if (cf_queue_sz(emig->ctrl_q) == 0 &&
				start_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now) {
			msg_incr_ref(m);

			if (as_fabric_send(emig->dest, m, AS_FABRIC_CHANNEL_CTRL) !=
					AS_FABRIC_SUCCESS) {
				as_fabric_msg_put(m);
			}

			start_xmit_ms = now;
		}

		int op;

		if (cf_queue_pop(emig->ctrl_q, &op, MIGRATE_RETRANSMIT_STARTDONE_MS) ==
				CF_QUEUE_OK) {
			switch (op) {
			case OPERATION_START_ACK_OK:
				as_fabric_msg_put(m);
				return EMIG_RESULT_START;
			case OPERATION_START_ACK_EAGAIN:
				as_fabric_msg_put(m);
				return EMIG_RESULT_EAGAIN;
			case OPERATION_START_ACK_FAIL:
				cf_warning(AS_MIGRATE, "imbalance: dest refused migrate with ACK_FAIL");
				cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
				as_fabric_msg_put(m);
				return EMIG_RESULT_ERROR;
			default:
				cf_warning(AS_MIGRATE, "unexpected ctrl op %d", op);
				break;
			}
		}
	}

	// Should never get here.
	cf_crash(AS_MIGRATE, "unexpected - exited infinite while loop");

	return EMIG_RESULT_ERROR;
}


emigration_result
emigration_send_done(emigration *emig)
{
	msg *m = as_fabric_msg_get(M_TYPE_MIGRATE);

	if (! m) {
		cf_warning(AS_MIGRATE, "imbalance: failed to get fabric msg");
		cf_atomic_int_incr(&emig->rsv.ns->migrate_tx_partitions_imbalance);
		return EMIG_RESULT_ERROR;
	}

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE);
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);

	uint64_t done_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			as_fabric_msg_put(m);
			return EMIG_RESULT_ERROR;
		}

		uint64_t now = cf_getms();

		if (done_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now) {
			msg_incr_ref(m);

			if (as_fabric_send(emig->dest, m, AS_FABRIC_CHANNEL_CTRL) !=
					AS_FABRIC_SUCCESS) {
				as_fabric_msg_put(m);
			}

			done_xmit_ms = now;
		}

		int op;

		if (cf_queue_pop(emig->ctrl_q, &op, MIGRATE_RETRANSMIT_STARTDONE_MS) ==
				CF_QUEUE_OK) {
			if (op == OPERATION_DONE_ACK) {
				as_fabric_msg_put(m);
				return EMIG_RESULT_DONE;
			}
		}
	}

	// Should never get here.
	cf_crash(AS_MIGRATE, "unexpected - exited infinite while loop");

	return EMIG_RESULT_ERROR;
}


void
emigrate_signal(emigration *emig)
{
	as_namespace *ns = emig->rsv.ns;
	msg *m = as_fabric_msg_get(M_TYPE_MIGRATE);

	if (! m) {
		cf_warning(AS_MIGRATE, "signal: failed to get fabric msg");
		return;
	}

	switch (emig->type) {
	case EMIG_TYPE_SIGNAL_ALL_DONE:
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_ALL_DONE);
		break;
	default:
		cf_crash(AS_MIGRATE, "signal: bad emig type %u", emig->type);
		break;
	}

	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_uint64(m, MIG_FIELD_CLUSTER_KEY, emig->cluster_key);
	msg_set_buf(m, MIG_FIELD_NAMESPACE, (const uint8_t *)ns->name,
			strlen(ns->name), MSG_SET_COPY);
	msg_set_uint32(m, MIG_FIELD_PARTITION, emig->rsv.p->id);

	uint64_t signal_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_exchange_cluster_key()) {
			as_fabric_msg_put(m);
			return;
		}

		uint64_t now = cf_getms();

		if (signal_xmit_ms + MIGRATE_RETRANSMIT_SIGNAL_MS < now) {
			msg_incr_ref(m);

			if (as_fabric_send(emig->dest, m, AS_FABRIC_CHANNEL_CTRL) !=
					AS_FABRIC_SUCCESS) {
				as_fabric_msg_put(m);
			}

			signal_xmit_ms = now;
		}

		int op;

		if (cf_queue_pop(emig->ctrl_q, &op, MIGRATE_RETRANSMIT_SIGNAL_MS) ==
				CF_QUEUE_OK) {
			switch (op) {
			case OPERATION_ALL_DONE_ACK:
				cf_atomic_int_decr(&ns->migrate_signals_remaining);
				as_fabric_msg_put(m);
				return;
			default:
				cf_warning(AS_MIGRATE, "signal: unexpected ctrl op %d", op);
				break;
			}
		}
	}
}


//==========================================================
// Local helpers - immigration.
//

void *
run_immigration_reaper(void *arg)
{
	while (true) {
		cf_rchash_reduce(g_immigration_hash, immigration_reaper_reduce_fn,
				NULL);
		sleep(1);
	}

	return NULL;
}


int
immigration_reaper_reduce_fn(const void *key, uint32_t keylen, void *object,
		void *udata)
{
	immigration *immig = (immigration *)object;

	if (immig->start_recv_ms == 0) {
		// If the start time isn't set, immigration is still being processed.
		return CF_RCHASH_OK;
	}

	if (immig->cluster_key != as_exchange_cluster_key() ||
			(immig->done_recv_ms != 0 && cf_getms() > immig->done_recv_ms +
					IMMIGRATION_DEBOUNCE_MS)) {
		if (immig->start_result == AS_MIGRATE_OK &&
				// If we started ok, must be a cluster key change - make sure
				// DONE handler doesn't also decrement active counter.
				cf_atomic32_incr(&immig->done_recv) == 1) {
			as_namespace *ns = immig->rsv.ns;

			if (cf_atomic_int_decr(&ns->migrate_rx_partitions_active) < 0) {
				cf_warning(AS_MIGRATE, "migrate_rx_partitions_active < 0");
				cf_atomic_int_incr(&ns->migrate_rx_partitions_active);
			}
		}

		return CF_RCHASH_REDUCE_DELETE;
	}

	return CF_RCHASH_OK;
}


//==========================================================
// Local helpers - migrate fabric message handling.
//

int
migrate_receive_msg_cb(cf_node src, msg *m, void *udata)
{
	uint32_t op;

	if (msg_get_uint32(m, MIG_FIELD_OP, &op) != 0) {
		cf_warning(AS_MIGRATE, "received message with no op");
		as_fabric_msg_put(m);
		return 0;
	}

	switch (op) {
	//--------------------------------------------
	// Emigration - handle requests:
	//
	case OPERATION_MERGE_META:
		emigration_handle_meta_batch_request(src, m);
		break;

	//--------------------------------------------
	// Immigration - handle requests:
	//
	case OPERATION_START:
		immigration_handle_start_request(src, m);
		break;
	case OPERATION_INSERT:
		immigration_handle_insert_request(src, m);
		break;
	case OPERATION_DONE:
		immigration_handle_done_request(src, m);
		break;
	case OPERATION_ALL_DONE:
		immigration_handle_all_done_request(src, m);
		break;

	//--------------------------------------------
	// Emigration - handle acknowledgments:
	//
	case OPERATION_INSERT_ACK:
		emigration_handle_insert_ack(src, m);
		break;
	case OPERATION_START_ACK_OK:
	case OPERATION_START_ACK_EAGAIN:
	case OPERATION_START_ACK_FAIL:
	case OPERATION_DONE_ACK:
	case OPERATION_ALL_DONE_ACK:
		emigration_handle_ctrl_ack(src, m, op);
		break;

	//--------------------------------------------
	// Immigration - handle acknowledgments:
	//
	case OPERATION_MERGE_META_ACK:
		immigration_handle_meta_batch_ack(src, m);
		break;

	default:
		cf_detail(AS_MIGRATE, "received unexpected message op %u", op);
		as_fabric_msg_put(m);
		break;
	}

	return 0;
}


//----------------------------------------------------------
// Immigration - request message handling.
//

void
immigration_handle_start_request(cf_node src, msg *m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	uint64_t cluster_key;

	if (msg_get_uint64(m, MIG_FIELD_CLUSTER_KEY, &cluster_key) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for cluster key failed");
		as_fabric_msg_put(m);
		return;
	}

	uint8_t *ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, MIG_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for namespace failed");
		as_fabric_msg_put(m);
		return;
	}

	as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_MIGRATE, "handle start: bad namespace");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t pid;

	if (msg_get_uint32(m, MIG_FIELD_PARTITION, &pid) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for pid failed");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t emig_features = 0;

	msg_get_uint32(m, MIG_FIELD_FEATURES, &emig_features);

	uint64_t emig_n_recs = 0;

	msg_get_uint64(m, MIG_FIELD_PARTITION_SIZE, &emig_n_recs);

	uint64_t incoming_ldt_version = 0;

	msg_get_uint64(m, MIG_FIELD_LDT_VERSION, &incoming_ldt_version);

	msg_preserve_fields(m, 1, MIG_FIELD_EMIG_ID);

	immigration *immig = cf_rc_alloc(sizeof(immigration));

	cf_assert(immig, AS_MIGRATE, "malloc");

	cf_atomic_int_incr(&ns->migrate_rx_instance_count);

	immig->src = src;
	immig->cluster_key = cluster_key;
	immig->pid = pid;
	immig->rx_state = AS_MIGRATE_RX_STATE_SUBRECORD; // always starts this way
	immig->incoming_ldt_version = incoming_ldt_version;
	immig->start_recv_ms = 0;
	immig->done_recv = 0;
	immig->done_recv_ms = 0;
	immig->emig_id = emig_id;
	immig_meta_q_init(&immig->meta_q);
	immig->features = MY_MIG_FEATURES;
	immig->ns = ns;
	immig->rsv.p = NULL;

	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	while (true) {
		if (cf_rchash_put_unique(g_immigration_hash, (void *)&hkey,
				sizeof(hkey), (void *)immig) == CF_RCHASH_OK) {
			cf_rc_reserve(immig); // so either put or get yields ref-count 2

			// First start request (not a retransmit) for this pid this round,
			// or we had ack'd previous start request with 'EAGAIN'.
			immig->start_result = as_partition_immigrate_start(ns, pid,
					cluster_key, src);
			break;
		}

		immigration *immig0;

		if (cf_rchash_get(g_immigration_hash, (void *)&hkey, sizeof(hkey),
				(void *)&immig0) == CF_RCHASH_OK) {
			immigration_release(immig); // free just-alloc'd immig ...

			if (immig0->start_recv_ms == 0) {
				immigration_release(immig0);
				return; // allow previous thread to respond
			}

			if (immig0->cluster_key != cluster_key) {
				immigration_release(immig0);
				return; // other node reused an immig_id, allow reaper to reap
			}

			immig = immig0; // ...  and use original
			break;
		}
	}

	switch (immig->start_result) {
	case AS_MIGRATE_OK:
		break;
	case AS_MIGRATE_FAIL:
		immig->start_recv_ms = cf_getms(); // permits reaping
		immig->done_recv_ms = immig->start_recv_ms; // permits reaping
		immigration_release(immig);
		immigration_ack_start_request(src, m, OPERATION_START_ACK_FAIL);
		return;
	case AS_MIGRATE_AGAIN:
		// Remove from hash so that the immig can be tried again.
		cf_rchash_delete(g_immigration_hash, (void *)&hkey, sizeof(hkey));
		immigration_release(immig);
		immigration_ack_start_request(src, m, OPERATION_START_ACK_EAGAIN);
		return;
	default:
		cf_crash(AS_MIGRATE, "unexpected as_partition_immigrate_start result");
		break;
	}

	if (immig->start_recv_ms == 0) {
		as_partition_reserve_migrate(ns, pid, &immig->rsv, NULL);
		cf_atomic_int_incr(&immig->rsv.ns->migrate_rx_partitions_active);

		immigration_ldt_version ldtv;

		ldtv.incoming_ldt_version = immig->incoming_ldt_version;
		ldtv.pid = immig->pid;

		shash_put(g_immigration_ldt_version_hash, &ldtv, &immig);

		if (! immigration_start_meta_sender(immig, emig_features,
				emig_n_recs)) {
			immig->features &= ~MIG_FEATURE_MERGE;
		}

		immig->start_recv_ms = cf_getms(); // permits reaping
	}

	msg_set_uint32(m, MIG_FIELD_FEATURES, immig->features);

	immigration_release(immig);
	immigration_ack_start_request(src, m, OPERATION_START_ACK_OK);
}


void
immigration_ack_start_request(cf_node src, msg *m, uint32_t op)
{
	msg_set_uint32(m, MIG_FIELD_OP, op);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_CTRL) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


void
immigration_handle_insert_request(cf_node src, msg *m)
{
	cf_digest *keyd;

	if (msg_get_buf(m, MIG_FIELD_DIGEST, (uint8_t **)&keyd, NULL,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_MIGRATE, "handle insert: msg get for digest failed");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle insert: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	immigration *immig;

	if (cf_rchash_get(g_immigration_hash, (void *)&hkey, sizeof(hkey),
			(void **)&immig) == CF_RCHASH_OK) {
		if (immig->start_result != AS_MIGRATE_OK || immig->start_recv_ms == 0) {
			// If this immigration didn't start and reserve a partition, it's
			// likely in the hash on a retransmit and this insert is for the
			// original - ignore, and let this immigration proceed.
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		cf_atomic_int_incr(&immig->rsv.ns->migrate_record_receives);

		if (immig->cluster_key != as_exchange_cluster_key()) {
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		uint32_t generation;

		if (msg_get_uint32(m, MIG_FIELD_GENERATION, &generation) != 0) {
			cf_warning(AS_MIGRATE, "handle insert: got no generation");
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		uint64_t last_update_time;

		if (msg_get_uint64(m, MIG_FIELD_LAST_UPDATE_TIME,
				&last_update_time) != 0) {
			cf_warning(AS_MIGRATE, "handle insert: got no last-update-time");
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		uint32_t void_time = 0;

		msg_get_uint32(m, MIG_FIELD_VOID_TIME, &void_time);

		void *value;
		size_t value_sz;

		if (msg_get_buf(m, MIG_FIELD_RECORD, (uint8_t **)&value, &value_sz,
				MSG_GET_DIRECT) != 0) {
			cf_warning(AS_MIGRATE, "handle insert: got no record");
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		as_record_merge_component c;

		c.record_buf = value;
		c.record_buf_sz = value_sz;
		c.generation = generation;
		c.void_time = void_time;
		c.last_update_time = last_update_time;
		as_rec_props_clear(&c.rec_props);

		uint8_t *rec_props_data = NULL;
		uint8_t *set_name = NULL;
		size_t set_name_len = 0;

		msg_get_buf(m, MIG_FIELD_SET_NAME, &set_name, &set_name_len,
				MSG_GET_DIRECT);

		uint8_t *key = NULL;
		size_t key_size = 0;

		msg_get_buf(m, MIG_FIELD_KEY, &key, &key_size, MSG_GET_DIRECT);

		uint32_t ldt_bits = 0;

		msg_get_uint32(m, MIG_FIELD_LDT_BITS, &ldt_bits);

		size_t rec_props_data_size = as_rec_props_size_all(set_name,
				set_name_len, key, key_size, ldt_bits);

		if (rec_props_data_size != 0) {
			// Use alloca() until after jump (remove new-cluster scope).
			rec_props_data = alloca(rec_props_data_size);

			as_rec_props_fill_all(&c.rec_props, rec_props_data, set_name,
					set_name_len, key, key_size, ldt_bits);
		}

		if (as_ldt_get_migrate_info(immig, &c, m)) {
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		if (immigration_ignore_pickle(c.record_buf, m)) {
			cf_warning_digest(AS_MIGRATE, keyd, "handle insert: binless pickle, dropping ");
		}
		else {
			int winner_idx  = -1;
			int rv = as_record_flatten(&immig->rsv, keyd, 1, &c, &winner_idx);

			// -3: race where we encountered a half-created/deleted record
			// -8: didn't write record because it's truncated
			if (rv != 0 && rv != -3 && rv != -8) {
				cf_warning_digest(AS_MIGRATE, keyd, "handle insert: record flatten failed %d ", rv);
				immigration_release(immig);
				as_fabric_msg_put(m);
				return;
			}
		}

		immigration_release(immig);
	}

	msg_preserve_fields(m, 2, MIG_FIELD_EMIG_INSERT_ID, MIG_FIELD_EMIG_ID);

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_INSERT_ACK);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_BULK) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
		return;
	}
}


void
immigration_handle_done_request(cf_node src, msg *m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle done: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 1, MIG_FIELD_EMIG_ID);

	// See if this migration already exists & has been notified.
	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	immigration *immig;

	if (cf_rchash_get(g_immigration_hash, (void *)&hkey, sizeof(hkey),
			(void **)&immig) == CF_RCHASH_OK) {
		if (immig->start_result != AS_MIGRATE_OK || immig->start_recv_ms == 0) {
			// If this immigration didn't start and reserve a partition, it's
			// likely in the hash on a retransmit and this DONE is for the
			// original - ignore, and let this immigration proceed.
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		if (cf_atomic32_incr(&immig->done_recv) == 1) {
			// Record the time of the first DONE received.
			immig->done_recv_ms = cf_getms();

			as_namespace *ns = immig->rsv.ns;

			if (cf_atomic_int_decr(&ns->migrate_rx_partitions_active) < 0) {
				cf_warning(AS_MIGRATE, "migrate_rx_partitions_active < 0");
				cf_atomic_int_incr(&ns->migrate_rx_partitions_active);
			}

			as_partition_immigrate_done(ns, immig->rsv.p->id,
					immig->cluster_key, immig->src);
		}
		// else - was likely a retransmitted done message.

		immigration_release(immig);
	}
	// else - garbage, or super-stale retransmitted done message.

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE_ACK);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_CTRL) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
		return;
	}
}


void
immigration_handle_all_done_request(cf_node src, msg *m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	uint64_t cluster_key;

	if (msg_get_uint64(m, MIG_FIELD_CLUSTER_KEY, &cluster_key) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for cluster key failed");
		as_fabric_msg_put(m);
		return;
	}

	uint8_t *ns_name;
	size_t ns_name_len;

	if (msg_get_buf(m, MIG_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for namespace failed");
		as_fabric_msg_put(m);
		return;
	}

	as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_MIGRATE, "handle all done: bad namespace");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t pid;

	if (msg_get_uint32(m, MIG_FIELD_PARTITION, &pid) != 0) {
		cf_warning(AS_MIGRATE, "handle all done: msg get for pid failed");
		as_fabric_msg_put(m);
		return;
	}

	msg_preserve_fields(m, 1, MIG_FIELD_EMIG_ID);

	// TODO - optionally, for replicas we might use this to remove immig objects
	// from hash and deprecate timer...

	if (as_partition_migrations_all_done(ns, pid, cluster_key) !=
			AS_MIGRATE_OK) {
		as_fabric_msg_put(m);
		return;
	}

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_ALL_DONE_ACK);

	if (as_fabric_send(src, m, AS_FABRIC_CHANNEL_CTRL) != AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
		return;
	}
}


//----------------------------------------------------------
// Emigration - acknowledgment message handling.
//

void
emigration_handle_insert_ack(cf_node src, msg *m)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "insert ack: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	emigration *emig;

	if (cf_rchash_get(g_emigration_hash, (void *)&emig_id, sizeof(emig_id),
			(void **)&emig) != CF_RCHASH_OK) {
		// Probably came from a migration prior to the latest rebalance.
		as_fabric_msg_put(m);
		return;
	}

	uint64_t insert_id;

	if (msg_get_uint64(m, MIG_FIELD_EMIG_INSERT_ID, &insert_id) != 0) {
		cf_warning(AS_MIGRATE, "insert ack: msg get for emig insert id failed");
		emigration_release(emig);
		as_fabric_msg_put(m);
		return;
	}

	emigration_reinsert_ctrl *ri_ctrl = NULL;
	pthread_mutex_t *vlock;

	if (shash_get_vlock(emig->reinsert_hash, &insert_id, (void **)&ri_ctrl,
			&vlock) == SHASH_OK) {
		if (src == emig->dest) {
			if (cf_atomic32_sub(&emig->bytes_emigrating,
					(int32_t)msg_get_wire_size(ri_ctrl->m)) < 0) {
				cf_warning(AS_MIGRATE, "bytes_emigrating less than zero");
			}

			as_fabric_msg_put(ri_ctrl->m);
			// At this point, the rt is *GONE*.
			shash_delete_lockfree(emig->reinsert_hash, &insert_id);
			ri_ctrl = NULL;
		}
		else {
			cf_warning(AS_MIGRATE, "insert ack: unexpected source %lx", src);
		}

		pthread_mutex_unlock(vlock);
	}

	emigration_release(emig);
	as_fabric_msg_put(m);
}


void
emigration_handle_ctrl_ack(cf_node src, msg *m, uint32_t op)
{
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "ctrl ack: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	uint32_t immig_features = 0;

	msg_get_uint32(m, MIG_FIELD_FEATURES, &immig_features);

	as_fabric_msg_put(m);

	emigration *emig;

	if (cf_rchash_get(g_emigration_hash, (void *)&emig_id, sizeof(emig_id),
			(void **)&emig) == CF_RCHASH_OK) {
		if (emig->dest == src) {
			if ((immig_features & MIG_FEATURE_MERGE) == 0) {
				// TODO - rethink where this should go after further refactor.
				if (op == OPERATION_START_ACK_OK && emig->meta_q) {
					emig->meta_q->is_done = true;
				}
			}

			cf_queue_push(emig->ctrl_q, &op);
		}
		else {
			cf_warning(AS_MIGRATE, "ctrl ack (%d): unexpected source %lx", op,
					src);
		}

		emigration_release(emig);
	}
	else {
		cf_detail(AS_MIGRATE, "ctrl ack (%d): can't find emig id %u", op,
				emig_id);
	}
}


//==========================================================
// Local helpers - info API helpers.
//

int
emigration_dump_reduce_fn(const void *key, uint32_t keylen, void *object,
		void *udata)
{
	uint32_t emig_id = *(const uint32_t *)key;
	emigration *emig = (emigration *)object;
	int *item_num = (int *)udata;

	cf_info(AS_MIGRATE, "[%d]: mig_id %u : id %u ; ck %lx", *item_num, emig_id,
			emig->id, emig->cluster_key);

	*item_num += 1;

	return 0;
}


int
immigration_dump_reduce_fn(const void *key, uint32_t keylen, void *object,
		void *udata)
{
	const immigration_hkey *hkey = (const immigration_hkey *)key;
	immigration *immig = (immigration *)object;
	int *item_num = (int *)udata;

	cf_info(AS_MIGRATE, "[%d]: src %016lx ; id %u : src %016lx ; done recv %u ; start recv ms %lu ; done recv ms %lu ; ck %lx",
			*item_num, hkey->src, hkey->emig_id, immig->src, immig->done_recv,
			immig->start_recv_ms, immig->done_recv_ms, immig->cluster_key);

	*item_num += 1;

	return 0;
}


//==========================================================
// Local helpers - LDT-related.
//

// Set up the LDT information.
// 1. Flag
// 2. Parent Digest
// 3. Esr Digest
// 4. Version
int
as_ldt_fill_mig_msg(const emigration *emig, msg *m, const pickled_record *pr,
		uint16_t ldt_bits, uint32_t *info)
{
	if (! emig->rsv.ns->ldt_enabled) {
		return 0;
	}

	bool is_subrecord = emig->tx_state == AS_PARTITION_MIG_TX_STATE_SUBRECORD;

	if (! is_subrecord) {
		cf_assert((emig->tx_state == AS_PARTITION_MIG_TX_STATE_RECORD),
				AS_PARTITION,
				"unexpected partition migration state at source %d:%d",
				emig->tx_state, emig->rsv.p->id);
	}

	if (is_subrecord) {
		msg_set_uint64(m, MIG_FIELD_LDT_VERSION, pr->ldt_version);

		as_index_ref r_ref;
		r_ref.skip_lock = false;

		int rv = as_record_get_live(emig->rsv.tree, (cf_digest *)&pr->pkeyd,
				&r_ref, emig->rsv.ns);

		if (rv == 0) {
			msg_set_uint32(m, MIG_FIELD_LDT_PVOID_TIME, r_ref.r->void_time);
			msg_set_uint32(m, MIG_FIELD_LDT_PGENERATION, r_ref.r->generation);
			as_record_done(&r_ref, emig->rsv.ns);
		}
		else {
			return -1;
		}

		msg_set_buf(m, MIG_FIELD_LDT_PDIGEST, (const uint8_t *)&pr->pkeyd,
				sizeof(cf_digest), MSG_SET_COPY);

		if (as_ldt_flag_has_esr(ldt_bits)) {
			*info |= MIG_INFO_LDT_ESR;
		}
		else if (as_ldt_flag_has_subrec(ldt_bits)) {
			*info |= MIG_INFO_LDT_SUBREC;
			msg_set_buf(m, MIG_FIELD_LDT_EDIGEST, (const uint8_t *)&pr->ekeyd,
					sizeof(cf_digest), MSG_SET_COPY);
		}
		else {
			cf_warning(AS_MIGRATE, "expected subrec and esr bit not found");
		}
	}
	else if (as_ldt_flag_has_parent(ldt_bits)) {
		msg_set_uint64(m, MIG_FIELD_LDT_VERSION, pr->ldt_version);

		*info |= MIG_INFO_LDT_PREC;
	}

	return 0;
}


void
as_ldt_fill_precord(pickled_record *pr, uint16_t ldt_bits, as_storage_rd *rd,
		const emigration *emig)
{
	pr->pkeyd = cf_digest_zero;
	pr->ekeyd = cf_digest_zero;
	pr->ldt_version = 0;

	if (! rd->ns->ldt_enabled) {
		return;
	}

	bool is_subrec = false;
	bool is_parent = false;

	if (as_ldt_flag_has_subrec(ldt_bits)) {
		int rv = as_ldt_subrec_storage_get_digests(rd, &pr->ekeyd, &pr->pkeyd);

		if (rv) {
			cf_warning(AS_MIGRATE, "ldt_migration: could not find parent or esr key in subrec rv=%d",
					rv);
		}

		is_subrec = true;
	}
	else if (as_ldt_flag_has_esr(ldt_bits)) {
		as_ldt_subrec_storage_get_digests(rd, NULL, &pr->pkeyd);
		is_subrec = true;
	}
	else {
		// When tree is being reduced for the record the state should already
		// be STATE_RECORD.
		cf_assert((emig->tx_state == AS_PARTITION_MIG_TX_STATE_RECORD),
				AS_PARTITION,
				"unexpected partition migration state at source %d:%d",
				emig->tx_state, emig->rsv.p->id);

		if (as_ldt_flag_has_parent(ldt_bits)) {
			is_parent = true;
		}
	}

	uint64_t new_version = emig->rsv.p->current_outgoing_ldt_version;

	if (is_parent) {
		uint64_t old_version = 0;

		as_ldt_parent_storage_get_version(rd, &old_version, true, __FILE__,
				__LINE__);

		pr->ldt_version = new_version ? new_version : old_version;
	}
	else if (is_subrec) {
		cf_assert((emig->tx_state == AS_PARTITION_MIG_TX_STATE_SUBRECORD),
				AS_PARTITION,
				"unexpected partition migration state at source %d:%d",
				emig->tx_state, emig->rsv.p->id);

		uint64_t old_version = as_ldt_subdigest_getversion(&pr->keyd);

		if (new_version) {
			as_ldt_subdigest_setversion(&pr->keyd, new_version);
			pr->ldt_version = new_version;
		}
		else {
			pr->ldt_version = old_version;
		}
	}
}


// Extracts ldt related infrom the migration messages
// return <0 in case of some sort of failure
// returns 0 for success
//
// side effect component will be filled up
int
as_ldt_get_migrate_info(immigration *immig, as_record_merge_component *c,
		msg *m)
{
	c->flag        = AS_COMPONENT_FLAG_MIG;
	c->pdigest     = cf_digest_zero;
	c->edigest     = cf_digest_zero;
	c->version     = 0;
	c->pgeneration = 0;
	c->pvoid_time  = 0;

	if (! immig->rsv.ns->ldt_enabled) {
		return 0;
	}

	uint32_t info;

	if (msg_get_uint32(m, MIG_FIELD_INFO, &info) == 0) {
		if ((info & MIG_INFO_LDT_SUBREC) != 0) {
			c->flag |= AS_COMPONENT_FLAG_LDT_SUBREC;
		}
		else if ((info & MIG_INFO_LDT_PREC) != 0) {
			c->flag |= AS_COMPONENT_FLAG_LDT_REC;
		}
		else if ((info & MIG_INFO_LDT_ESR) != 0) {
			c->flag |= AS_COMPONENT_FLAG_LDT_ESR;
		}
	}
	// else - resort to defaults.

	cf_digest *key = NULL;

	msg_get_buf(m, MIG_FIELD_LDT_PDIGEST, (uint8_t **)&key, NULL,
			MSG_GET_DIRECT);

	if (key) {
		c->pdigest = *key;
		key = NULL;
	}

	msg_get_buf(m, MIG_FIELD_LDT_EDIGEST, (uint8_t **)&key, NULL,
			MSG_GET_DIRECT);

	if (key) {
		c->edigest = *key;
	}

	msg_get_uint64(m, MIG_FIELD_LDT_VERSION, &c->version);
	msg_get_uint32(m, MIG_FIELD_LDT_PGENERATION, &c->pgeneration);
	msg_get_uint32(m, MIG_FIELD_LDT_PVOID_TIME, &c->pvoid_time);

	if (COMPONENT_IS_LDT_SUB(c)) {
		;
	}
	else if (COMPONENT_IS_LDT_DUMMY(c)) {
		cf_crash(AS_MIGRATE, "Invalid Component Type Dummy received by migration");
	}
	else {
		if (immig->rx_state == AS_MIGRATE_RX_STATE_SUBRECORD) {
			immig->rx_state = AS_MIGRATE_RX_STATE_RECORD;
		}
	}

	return 0;
}
