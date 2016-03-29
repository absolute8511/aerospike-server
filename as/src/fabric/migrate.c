/*
 * migrate.c
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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


//==============================================================================
// Includes.
//

#include "fabric/migrate.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_queue_priority.h"
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "msg.h"
#include "rchash.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "fabric/fabric.h"
#include "storage/storage.h"


//==============================================================================
// Constants and typedefs.
//

#define MIG_FIELD_OP 0
#define MIG_FIELD_EMIG_INSERT_ID 1
#define MIG_FIELD_EMIG_ID 2
#define MIG_FIELD_NAMESPACE 3
#define MIG_FIELD_PARTITION 4
#define MIG_FIELD_DIGEST 5
#define MIG_FIELD_GENERATION 6
#define MIG_FIELD_RECORD 7
#define MIG_FIELD_CLUSTER_KEY 8
#define MIG_FIELD_VINFOSET 9 // deprecated
#define MIG_FIELD_VOID_TIME 10
#define MIG_FIELD_TYPE 11
#define MIG_FIELD_REC_PROPS 12
#define MIG_FIELD_INFO 13
#define MIG_FIELD_VERSION 14
#define MIG_FIELD_PDIGEST 15
#define MIG_FIELD_EDIGEST 16
#define MIG_FIELD_PGENERATION 17
#define MIG_FIELD_PVOID_TIME 18

#define OPERATION_UNDEF 0
#define OPERATION_INSERT 1
#define OPERATION_INSERT_ACK 2
#define OPERATION_START 3
#define OPERATION_START_ACK_OK 4
#define OPERATION_START_ACK_EAGAIN 5
#define OPERATION_START_ACK_FAIL 6
#define OPERATION_START_ACK_ALREADY_DONE 7
#define OPERATION_DONE 8
#define OPERATION_DONE_ACK 9
#define OPERATION_CANCEL 10 // deprecated

const msg_template migrate_mt[] = {
		{ MIG_FIELD_OP, M_FT_UINT32 },
		{ MIG_FIELD_EMIG_INSERT_ID, M_FT_UINT32 },
		{ MIG_FIELD_EMIG_ID, M_FT_UINT32 },
		{ MIG_FIELD_NAMESPACE, M_FT_BUF },
		{ MIG_FIELD_PARTITION, M_FT_UINT32 },
		{ MIG_FIELD_DIGEST, M_FT_BUF },
		{ MIG_FIELD_GENERATION, M_FT_UINT32 },
		{ MIG_FIELD_RECORD, M_FT_BUF },
		{ MIG_FIELD_CLUSTER_KEY, M_FT_UINT64 },
		{ MIG_FIELD_VINFOSET, M_FT_BUF },
		{ MIG_FIELD_VOID_TIME, M_FT_UINT32 },
		{ MIG_FIELD_TYPE, M_FT_UINT32 }, // AS_MIGRATE_TYPE: 0 merge, 1 overwrite
		{ MIG_FIELD_REC_PROPS, M_FT_BUF },
		{ MIG_FIELD_INFO, M_FT_UINT32 },
		{ MIG_FIELD_VERSION, M_FT_UINT64 },
		{ MIG_FIELD_PDIGEST, M_FT_BUF },
		{ MIG_FIELD_EDIGEST, M_FT_BUF },
		{ MIG_FIELD_PGENERATION, M_FT_UINT32 },
		{ MIG_FIELD_PVOID_TIME, M_FT_UINT32 },
};

// If the bit is not set then it is normal record.
#define MIG_INFO_LDT_REC    0x0001
#define MIG_INFO_LDT_SUBREC 0x0002
#define MIG_INFO_LDT_ESR    0x0004

#define MIGRATE_RETRANSMIT_MS (g_config.transaction_retry_ms)
#define MIGRATE_RETRANSMIT_STARTDONE_MS (g_config.transaction_retry_ms)
#define MAX_BYTES_EMIGRATING (32 * 1024 * 1024)

typedef struct pickled_record_s {
	cf_digest     keyd;
	uint32_t      generation;
	uint32_t      void_time;
	byte          *record_buf; // pickled!
	size_t        record_len;
	as_rec_props  rec_props;

	// For LDT only:
	cf_digest     pkeyd;
	cf_digest     ekeyd;
	uint64_t      version;
} pickled_record;

typedef struct emigration_s {
	cf_node     dest;
	uint64_t    cluster_key;
	uint32_t    id;
	uint32_t    tx_flags;
	bool        aborted;
	as_partition_mig_tx_state tx_state; // really only for LDT

	cf_atomic32 bytes_emigrating;
	shash       *reinsert_hash;
	cf_queue    *ctrl_q;

	as_partition_reservation rsv;
} emigration;

typedef struct emigration_pop_info_s {
	uint32_t best_migrate_order;
	uint32_t best_tree_elements;
} emigration_pop_info;

typedef struct emigration_ctrl_s {
	uint32_t emig_id;
	int op;
} emigration_ctrl;

typedef struct emigration_reinsert_ctrl_s {
	uint64_t xmit_ms; // time of last xmit - 0 when done
	emigration *emig;
	msg *m;
} emigration_reinsert_ctrl;

typedef struct immigration_s {
	cf_node          src;
	uint64_t         cluster_key;
	as_partition_id  pid;
	as_partition_mig_rx_state rx_state; // really only for LDT
	uint64_t         incoming_ldt_version;

	cf_atomic32      done_recv;      // flag - 0 if not yet received, atomic counter for receives
	uint64_t         start_recv_ms;  // time the first START event was received
	uint64_t         done_recv_ms;   // time the first DONE event was received

	as_partition_reservation rsv;
} immigration;

typedef struct immigration_hkey_s {
	cf_node src;
	uint32_t emig_id;
} __attribute__((__packed__)) immigration_hkey;

typedef struct immigration_ldt_version_s {
	uint64_t        incoming_ldt_version;
	as_partition_id pid;
} __attribute__((__packed__)) immigration_ldt_version;


//==============================================================================
// Globals.
//

static rchash *g_emigration_hash = NULL;
static cf_atomic32 g_emigration_id = 0;
static cf_atomic32 g_emigration_insert_id = 0;
static cf_queue_priority *g_emigration_q = NULL;
static rchash *g_immigration_hash = NULL;
static shash *g_immigration_ldt_version_hash;


//==============================================================================
// Forward declarations and inlines.
//

// Emigration, immigration, & pickled record destructors.
void emigration_destroy(void *parm);
void emigration_release(emigration *emig);
void immigration_destroy(void *parm);
void immigration_release(immigration *immig);
void pickled_record_destroy(pickled_record *pr);

// Emigration.
void *run_emigration(void *arg);
void emigration_pop(emigration **emigp);
int emigration_pop_reduce_fn(void *buf, void *udata);
as_migrate_state emigrate(emigration *emig);
as_migrate_state emigrate_tree(emigration *emig);
void emigrate_tree_reduce_fn(as_index_ref *r_ref, void *udata);
bool emigrate_record(emigration *emig, msg *m);
int emigration_reinsert_reduce_fn(void *key, void *data, void *udata);
as_migrate_state emigration_send_start(emigration *emig);
as_migrate_state emigration_send_done(emigration *emig);

// Immigration.
void *run_immigration_reaper(void *unused);
int immigration_reaper_reduce_fn(void *key, uint32_t keylen, void *object, void *udata);

// Migrate fabric message handling.
int migrate_receive_msg_cb(cf_node src, msg *m, void *udata);
void immigration_handle_start_request(cf_node src, msg *m);
void immigration_handle_insert_request(cf_node src, msg *m);
void immigration_handle_done_request(cf_node src, msg *m);
void emigration_handle_insert_ack(cf_node src, msg *m);
void emigration_handle_ctrl_ack(cf_node src, msg *m, uint32_t op);

// Info API helpers.
int emigration_dump_reduce_fn(void *key, uint32_t keylen, void *object, void *udata);
int immigration_dump_reduce_fn(void *key, uint32_t keylen, void *object, void *udata);

// LDT-related.
bool as_ldt_precord_is_esr(const pickled_record *pr);
bool as_ldt_precord_is_subrec(const pickled_record *pr);
bool as_ldt_precord_is_parent(const pickled_record *pr);
int as_ldt_fill_mig_msg(const emigration *emig, msg *m, const pickled_record *pr);
void as_ldt_fill_precord(pickled_record *pr, as_storage_rd *rd, const emigration *emig);
int as_ldt_get_migrate_info(immigration *immig, as_record_merge_component *c, msg *m, cf_digest *keyd);


static inline uint32_t
emigration_hashfn(void *value, uint32_t value_len)
{
	return *(uint32_t *)value;
}

static inline uint32_t
emigration_insert_hashfn(void *key)
{
	return *(uint32_t *)key;
}

static inline uint32_t
immigration_hashfn(void *value, uint32_t value_len)
{
	return ((immigration_hkey *)value)->emig_id;
}

static inline uint32_t
immigration_ldt_version_hashfn(void *key)
{
	return *(uint32_t *)key;
}


//==============================================================================
// Public API.
//

void
as_migrate_init()
{
	g_emigration_q = cf_queue_priority_create(sizeof(void *), true);

	if (rchash_create(&g_emigration_hash, emigration_hashfn, emigration_destroy,
			sizeof(uint32_t), 64, RCHASH_CR_MT_MANYLOCK) != RCHASH_OK) {
		cf_crash(AS_MIGRATE, "couldn't create emigration hash");
	}

	if (rchash_create(&g_immigration_hash, immigration_hashfn,
			immigration_destroy, sizeof(immigration_hkey), 64,
			RCHASH_CR_MT_BIGLOCK) != RCHASH_OK) {
		cf_crash(AS_MIGRATE, "couldn't create immigration hash");
	}

	// Looks like an as_priority_thread_pool, but the reduce-pop is different.

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	for (int i = 0; i < g_config.n_migrate_threads; i++) {
		if (pthread_create(&thread, &attrs, run_emigration, NULL) != 0) {
			cf_crash(AS_MIGRATE, "failed to create emigration thread");
		}
	}

	if (pthread_create(&thread, &attrs, run_immigration_reaper, NULL) != 0) {
		cf_crash(AS_MIGRATE, "failed to create immigration reaper thread");
	}

	if (shash_create(&g_immigration_ldt_version_hash,
			immigration_ldt_version_hashfn, sizeof(immigration_ldt_version),
			sizeof(void *), 64, SHASH_CR_MT_MANYLOCK) != SHASH_OK) {
		cf_crash(AS_MIGRATE, "couldn't create immigration ldt version hash");
	}

	as_fabric_register_msg_fn(M_TYPE_MIGRATE, migrate_mt, sizeof(migrate_mt),
			migrate_receive_msg_cb, NULL);
}


// Kicks off an emigration.
void
as_migrate_emigrate(const partition_migrate_record *pmr,
		bool is_migrate_state_done)
{
	emigration *emig = cf_rc_alloc(sizeof(emigration));

	cf_assert(emig, AS_MIGRATE, CF_CRITICAL, "failed emigration malloc");

	cf_atomic_int_incr(&g_config.migrate_tx_object_count);

	emig->dest = pmr->dest;
	emig->cluster_key = pmr->cluster_key;
	emig->id = cf_atomic32_incr(&g_emigration_id);
	emig->tx_flags = pmr->tx_flags;
	emig->aborted = false;

	// Create these later only when we need them - we'll get lots at once.
	emig->bytes_emigrating = 0;
	emig->reinsert_hash = NULL;
	emig->ctrl_q = NULL;

	AS_PARTITION_RESERVATION_INIT(emig->rsv);
	as_partition_reserve_migrate(pmr->ns, pmr->pid, &emig->rsv, NULL);
	cf_atomic_int_incr(&g_config.migtx_tree_count);

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

	if (cf_queue_priority_push(g_emigration_q, &emig, CF_QUEUE_PRIORITY_HIGH) !=
			CF_QUEUE_OK) {
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
		as_partition_id partition_id, int rx_state)
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
// TODO - make thread safe for concurrent info commands?
void
as_migrate_set_num_xmit_threads(int n_threads)
{
	if (g_config.n_migrate_threads > n_threads) {
		// Decrease the number of migrate transmit threads to n_threads.
		while (g_config.n_migrate_threads > n_threads) {
			void *death_msg = NULL;

			// Send high priority terminator (NULL message).
			if (cf_queue_priority_push(g_emigration_q, &death_msg,
					CF_QUEUE_PRIORITY_HIGH) != CF_QUEUE_OK) {
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
			rchash_get_size(g_emigration_hash));
	cf_info(AS_MIGRATE, "number of requested emigrations waiting in g_emigration_q : %d",
			cf_queue_priority_sz(g_emigration_q));
	cf_info(AS_MIGRATE, "number of immigrations in g_immigration_hash: %d",
			rchash_get_size(g_immigration_hash));
	cf_info(AS_MIGRATE, "current emigration id: %d", g_emigration_id);
	cf_info(AS_MIGRATE, "current emigration insert id: %d",
			g_emigration_insert_id);

	if (verbose) {
		int item_num = 0;

		if (rchash_get_size(g_emigration_hash) > 0) {
			cf_info(AS_MIGRATE, "contents of g_emigration_hash:");
			cf_info(AS_MIGRATE, "------------------------------");

			rchash_reduce(g_emigration_hash, emigration_dump_reduce_fn,
					&item_num);
		}

		if (rchash_get_size(g_immigration_hash) > 0) {
			item_num = 0;

			cf_info(AS_MIGRATE, "contents of g_immigration_hash:");
			cf_info(AS_MIGRATE, "-------------------------------");

			rchash_reduce(g_immigration_hash, immigration_dump_reduce_fn,
					&item_num);
		}
	}
}


//==============================================================================
// Local helpers - emigration, immigration, & pickled record destructors.
//

// Destructor handed to rchash.
void
emigration_destroy(void *parm)
{
	emigration *emig = (emigration *)parm;

	if (emig->reinsert_hash) {
		shash_destroy(emig->reinsert_hash);
	}

	if (emig->ctrl_q) {
		cf_queue_destroy(emig->ctrl_q);
	}

	if (emig->rsv.p) {
		as_partition_release(&emig->rsv);
		cf_atomic_int_decr(&g_config.migtx_tree_count);
	}

	cf_atomic_int_decr(&g_config.migrate_tx_object_count);
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
		cf_atomic_int_decr(&g_config.migrx_tree_count);
	}

	shash_delete(g_immigration_ldt_version_hash, &ldtv);

	cf_atomic_int_decr(&g_config.migrate_rx_object_count);
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

	if (pr->rec_props.p_data) {
		cf_free(pr->rec_props.p_data);
	}
}


//==============================================================================
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

		// Re-queue migration from desync. TODO - does this happen? How?
		if (emig->rsv.state == AS_PARTITION_STATE_DESYNC) {
			cf_debug(AS_MIGRATE, "attempted to migrate a desync partition");

			as_partition_reserve_update_state(&emig->rsv);

			if (cf_queue_priority_push(g_emigration_q, (void *)&emig,
					CF_QUEUE_PRIORITY_LOW) != CF_QUEUE_OK) {
				cf_crash(AS_MIGRATE, "failed re-queueing desync emigration");
			}

			usleep(1000);
			continue;
		}

		cf_atomic_int_incr(&g_config.migrate_progress_send);

		as_migrate_state result = emigrate(emig);

		as_partition_migrate_tx(result, emig->rsv.ns, emig->rsv.pid,
				emig->cluster_key, emig->tx_flags);

		cf_atomic_int_decr(&g_config.migrate_progress_send);

		emig->tx_state = AS_PARTITION_MIG_TX_STATE_NONE;
		emig->rsv.p->current_outgoing_ldt_version = 0;

		rchash_delete(g_emigration_hash, (void *)&emig->id , sizeof(emig->id));
		emigration_release(emig);
	}

	return NULL;
}


void
emigration_pop(emigration **emigp)
{
	emigration_pop_info pop_info;

	pop_info.best_migrate_order = 0xFFFFffff;
	pop_info.best_tree_elements = 0;
	// 0 is a special value - means we haven't started.

	int rv = cf_queue_priority_reduce_pop(g_emigration_q, (void *)emigp,
			emigration_pop_reduce_fn, &pop_info);

	if (rv == CF_QUEUE_ERR) {
		cf_crash(AS_MIGRATE, "emigration queue reduce pop failed");
	}

	if (rv == CF_QUEUE_NOMATCH) {
		if (cf_queue_priority_pop(g_emigration_q, (void *)emigp,
				CF_QUEUE_FOREVER) != CF_QUEUE_OK) {
			cf_crash(AS_MIGRATE, "emigration queue pop failed");
		}
	}
}


int
emigration_pop_reduce_fn(void *buf, void *udata)
{
	emigration_pop_info *pop_info = (emigration_pop_info *)udata;
	emigration *emig = *(emigration **)buf;

	if (! emig || // null emig terminates thread
			emig->rsv.tree->elements == 0 ||
			emig->tx_flags == TX_FLAGS_REQUEST ||
			emig->cluster_key != as_paxos_get_cluster_key()) {
		return -1; // process immediately
	}

	uint32_t migrate_order = emig->rsv.ns->migrate_order;
	uint32_t tree_elements = emig->rsv.tree->elements;

	if (migrate_order < pop_info->best_migrate_order ||
			(migrate_order == pop_info->best_migrate_order &&
					tree_elements < pop_info->best_tree_elements)) {
		pop_info->best_migrate_order = migrate_order;
		pop_info->best_tree_elements = tree_elements;
		return -2; // candidate
	}

	return 0; // not interested
}


as_migrate_state
emigrate(emigration *emig)
{
	as_namespace *ns = emig->rsv.ns;

	if (emig->cluster_key != as_paxos_get_cluster_key()) {
		return AS_MIGRATE_STATE_ERROR;
	}

	switch (emig->rsv.state) {
	case AS_PARTITION_STATE_DESYNC:
		cf_crash(AS_MIGRATE, "can't emigrate from desync");
		break;
	case AS_PARTITION_STATE_SYNC:
	case AS_PARTITION_STATE_ZOMBIE:
		break;
	case AS_PARTITION_STATE_ABSENT:
	case AS_PARTITION_STATE_UNDEF:
	default:
		cf_warning(AS_MIGRATE, "imbalance: unexpected partition state %u",
				emig->rsv.state);
		cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
		return AS_MIGRATE_STATE_ERROR;
	}

	emig->ctrl_q = cf_queue_create(sizeof(emigration_ctrl), true);

	if (! emig->ctrl_q) {
		cf_warning(AS_MIGRATE, "imbalance: failed to allocate emig ctrl q");
		cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
		return AS_MIGRATE_STATE_ERROR;
	}

	if (shash_create(&emig->reinsert_hash, emigration_insert_hashfn,
			sizeof(uint32_t), sizeof(emigration_reinsert_ctrl),
			32 * 1024, SHASH_CR_MT_BIGLOCK) != SHASH_OK) {
		cf_warning(AS_MIGRATE, "imbalance: failed to allocate reinsert hash");
		cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
		return AS_MIGRATE_STATE_ERROR;
	}

	// Add myself to the global hash so my acks find me.
	cf_rc_reserve(emig);
	rchash_put(g_emigration_hash, (void *)&emig->id , sizeof(emig->id),
			(void *)emig);

	as_migrate_state result;

	//--------------------------------------------
	// Send START request.
	//
	if ((result = emigration_send_start(emig)) != AS_MIGRATE_STATE_START) {
		return result;
	}

	//--------------------------------------------
	// Send whole sub-tree - may block a while.
	//
	if (ns->ldt_enabled) {
		if ((result = emigrate_tree(emig)) != AS_MIGRATE_STATE_DONE) {
			return result;
		}
	}

	emig->tx_state = AS_PARTITION_MIG_TX_STATE_RECORD;

	//--------------------------------------------
	// Send whole tree - may block a while.
	//
	if ((result = emigrate_tree(emig)) != AS_MIGRATE_STATE_DONE) {
		return result;
	}

	//--------------------------------------------
	// Send DONE request.
	//
	return emigration_send_done(emig);
}


as_migrate_state
emigrate_tree(emigration *emig)
{
	bool is_subrecord = emig->tx_state == AS_PARTITION_MIG_TX_STATE_SUBRECORD;
	as_index_tree *tree = is_subrecord ? emig->rsv.sub_tree : emig->rsv.tree;

	if (as_index_tree_size(tree) == 0) {
		return AS_MIGRATE_STATE_DONE;
	}

	as_index_reduce(tree, emigrate_tree_reduce_fn, emig);

	if (emig->aborted) {
		return AS_MIGRATE_STATE_ERROR;
	}

	// Reduce over the reinsert hash until finished.
	while (true) {
		if (emig->cluster_key != as_paxos_get_cluster_key()) {
			return AS_MIGRATE_STATE_ERROR;
		}

		uint64_t now = cf_getms();

		// The only rv from this is the rv of the reduce fn, which is the
		// return value of a fabric_send.
		int rv = shash_reduce(emig->reinsert_hash,
				emigration_reinsert_reduce_fn, &now);

		switch (rv) {
		case AS_FABRIC_SUCCESS:
		case AS_FABRIC_ERR_QUEUE_FULL:
			break;
		case AS_FABRIC_ERR_NO_NODE:
			// New rebalance expected.
			return AS_MIGRATE_STATE_ERROR;
		default:
			cf_warning(AS_MIGRATE, "imbalance: failure emigrating - bad fabric send in retransmission - error %d",
					rv);
			cf_atomic_int_incr(&emig->rsv.ns->migrate_tx_partitions_imbalance);
			return AS_MIGRATE_STATE_ERROR;
		}

		if (shash_get_size(emig->reinsert_hash) == 0) {
			break;
		}

		usleep(1000 * 50);
	}

	return AS_MIGRATE_STATE_DONE;
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

	if (emig->cluster_key != as_paxos_get_cluster_key()) {
		as_record_done(r_ref, ns);
		emig->aborted = true;
		return; // no point continuing to reduce this tree
	}

	//--------------------------------------------
	// Read the record and pickle it.
	//

	as_index *r = r_ref->r;
	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd, &r->key);

	rd.n_bins = as_bin_get_n_bins(r, &rd);

	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	pickled_record pr;

	if (as_record_pickle(r, &rd, &pr.record_buf, &pr.record_len) != 0) {
		cf_warning(AS_MIGRATE, "failed migrate record pickle");
		as_storage_record_close(r, &rd);
		as_record_done(r_ref, ns);
		return;
	}

	pr.keyd = r->key;
	pr.generation = r->generation;
	pr.void_time = r->void_time;

	as_storage_record_get_key(&rd);

	as_rec_props_clear(&pr.rec_props);
	as_rec_props rec_props;

	if (as_storage_record_copy_rec_props(&rd, &rec_props) != 0) {
		pr.rec_props = rec_props;
	}

	as_ldt_fill_precord(&pr, &rd, emig);

	as_storage_record_close(r, &rd);
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
		return;
	}

	if (as_ldt_fill_mig_msg(emig, m, &pr) != 0) {
		// Skipping stale version subrecord shipping.
		as_fabric_msg_put(m);
		pickled_record_destroy(&pr);
		return;
	}

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_INSERT);
	msg_set_uint32(m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_buf(m, MIG_FIELD_NAMESPACE, (byte *)ns->name, strlen(ns->name),
			MSG_SET_COPY);
	msg_set_buf(m, MIG_FIELD_DIGEST, (void *)&pr.keyd, sizeof(cf_digest),
			MSG_SET_COPY);
	msg_set_uint32(m, MIG_FIELD_GENERATION, pr.generation);
	msg_set_uint32(m, MIG_FIELD_VOID_TIME, pr.void_time);
	// Note - older versions handle missing MIG_FIELD_VINFOSET field.

	// Note - after MSG_SET_HANDOFF_MALLOCs, no need to destroy pickled_record.

	if (pr.rec_props.p_data) {
		msg_set_buf(m, MIG_FIELD_REC_PROPS, (void *)pr.rec_props.p_data,
				pr.rec_props.size, MSG_SET_HANDOFF_MALLOC);
	}

	msg_set_buf(m, MIG_FIELD_RECORD, pr.record_buf, pr.record_len,
			MSG_SET_HANDOFF_MALLOC);

	// This might block if the queues are backed up but a failure is a
	// hard-fail - can't notify other side.
	if (! emigrate_record(emig, m)) {
		emig->aborted = true;
		return;
	}

	if (ns->migrate_sleep != 0) {
		usleep(ns->migrate_sleep);
	}

	uint32_t waits = 0;

	while (cf_atomic32_get(emig->bytes_emigrating) > MAX_BYTES_EMIGRATING &&
			emig->cluster_key == as_paxos_get_cluster_key()) {
		usleep(1000);

		// Temporary paranoia to inform us old nodes aren't acking properly.
		if (++waits % 5000 == 0) {
			cf_warning(AS_MIGRATE, "missing acks from node %lx", emig->dest);
		}
	}
}


bool
emigrate_record(emigration *emig, msg *m)
{
	uint32_t insert_id = cf_atomic32_incr(&g_emigration_insert_id);

	msg_set_uint32(m, MIG_FIELD_EMIG_INSERT_ID, insert_id);

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

	cf_atomic32_add(&emig->bytes_emigrating, m->bytes_used);

	int rv;

	while ((rv = as_fabric_send(emig->dest, m, AS_FABRIC_PRIORITY_LOW)) !=
			AS_FABRIC_SUCCESS) {
		switch (rv) {
		case AS_FABRIC_ERR_QUEUE_FULL:
			usleep(1000 * 10);
			break;
		case AS_FABRIC_ERR_NO_NODE:
			// New rebalance expected.
			as_fabric_msg_put(m);
			return false;
		default:
			cf_warning(AS_MIGRATE, "imbalance: emigrate record failed fabric send");
			cf_atomic_int_incr(&emig->rsv.ns->migrate_tx_partitions_imbalance);
			as_fabric_msg_put(m);
			return false;
		}
	}

	cf_atomic_int_incr(&g_config.migrate_msgs_sent);

	return true;
}


int
emigration_reinsert_reduce_fn(void *key, void *data, void *udata)
{
	emigration_reinsert_ctrl *ri_ctrl = (emigration_reinsert_ctrl *)data;
	uint64_t now = *(uint64_t *)udata;

	if (ri_ctrl->xmit_ms + MIGRATE_RETRANSMIT_MS < now) {
		msg_incr_ref(ri_ctrl->m);

		int rv = as_fabric_send(ri_ctrl->emig->dest, ri_ctrl->m,
				AS_FABRIC_PRIORITY_LOW);

		if (rv != AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(ri_ctrl->m);
			return rv; // this will stop the reduce
		}

		cf_atomic_int_incr(&g_config.migrate_msgs_sent);
		ri_ctrl->xmit_ms = now;
	}

	return 0;
}


as_migrate_state
emigration_send_start(emigration *emig)
{
	as_namespace *ns = emig->rsv.ns;
	msg *start_m = as_fabric_msg_get(M_TYPE_MIGRATE);

	if (! start_m) {
		cf_warning(AS_MIGRATE, "failed to get fabric msg");
		return AS_MIGRATE_STATE_ERROR;
	}

	msg_set_uint32(start_m, MIG_FIELD_OP, OPERATION_START);
	msg_set_uint32(start_m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_uint64(start_m, MIG_FIELD_CLUSTER_KEY, emig->cluster_key);
	msg_set_buf(start_m, MIG_FIELD_NAMESPACE, (byte *)ns->name,
			strlen(ns->name), MSG_SET_COPY);
	msg_set_uint32(start_m, MIG_FIELD_PARTITION, emig->rsv.pid);
	msg_set_uint32(start_m, MIG_FIELD_TYPE, 0); // not used, but older nodes expect this
	msg_set_uint64(start_m, MIG_FIELD_VERSION,
			emig->rsv.p->current_outgoing_ldt_version);

	uint64_t start_xmit_ms = 0;

	while (true) {
		if (emig->cluster_key != as_paxos_get_cluster_key()) {
			as_fabric_msg_put(start_m);
			return AS_MIGRATE_STATE_ERROR;
		}

		uint64_t now = cf_getms();

		if (start_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now) {
			cf_rc_reserve(start_m);

			int rv;

			if ((rv = as_fabric_send(emig->dest, start_m,
					AS_FABRIC_PRIORITY_MEDIUM)) != AS_FABRIC_SUCCESS) {
				// NO_NODE is expected when node drops, new rebalance imminent.
				if (rv != AS_FABRIC_ERR_NO_NODE) {
					cf_warning(AS_MIGRATE, "could not send start rv: %d", rv);
				}

				as_fabric_msg_put(start_m);
			}

			start_xmit_ms = now;
		}

		emigration_ctrl emig_ctrl;

		if (cf_queue_pop(emig->ctrl_q, &emig_ctrl,
				MIGRATE_RETRANSMIT_STARTDONE_MS) == CF_QUEUE_OK) {
			if (emig_ctrl.emig_id != emig->id) {
				cf_crash(AS_MIGRATE, "internal emig id error");
			}

			switch (emig_ctrl.op) {
			case OPERATION_START_ACK_OK:
				as_fabric_msg_put(start_m);
				return AS_MIGRATE_STATE_START;
			case OPERATION_START_ACK_ALREADY_DONE:
				as_fabric_msg_put(start_m);
				return AS_MIGRATE_STATE_DONE;
			case OPERATION_START_ACK_EAGAIN:
				usleep(1000);
				break;
			case OPERATION_START_ACK_FAIL:
				cf_warning(AS_MIGRATE, "dest refused migrate with ACK_FAIL");
				cf_atomic_int_incr(&ns->migrate_tx_partitions_imbalance);
				as_fabric_msg_put(start_m);
				return AS_MIGRATE_STATE_ERROR;
			default:
				cf_warning(AS_MIGRATE, "unexpected ctrl op %d", emig_ctrl.op);
				break;
			}
		}
	}

	// Should never get here.
	cf_crash(AS_MIGRATE, "unexpected - exited infinite while loop");

	return AS_MIGRATE_STATE_ERROR;
}


as_migrate_state
emigration_send_done(emigration *emig)
{
	msg *done_m = as_fabric_msg_get(M_TYPE_MIGRATE);

	if (! done_m) {
		cf_warning(AS_MIGRATE, "imbalance: failed to get fabric msg");
		cf_atomic_int_incr(&emig->rsv.ns->migrate_tx_partitions_imbalance);
		return -1;
	}

	msg_set_uint32(done_m, MIG_FIELD_OP, OPERATION_DONE);
	msg_set_uint32(done_m, MIG_FIELD_EMIG_ID, emig->id);
	msg_set_buf(done_m, MIG_FIELD_NAMESPACE, (byte *)emig->rsv.ns->name,
			strlen(emig->rsv.ns->name), MSG_SET_COPY);
	msg_set_uint32(done_m, MIG_FIELD_PARTITION, emig->rsv.pid);

	uint64_t done_xmit_ms = 0;

	while (true) {
		uint64_t now = cf_getms();

		if (done_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now) {
			cf_rc_reserve(done_m);

			int rv = as_fabric_send(emig->dest, done_m,
					AS_FABRIC_PRIORITY_MEDIUM);

			if (rv == AS_FABRIC_SUCCESS) {
				cf_atomic_int_incr(&g_config.migrate_msgs_sent);
			}
			else {
				as_fabric_msg_put(done_m);

				if (rv == AS_FABRIC_ERR_NO_NODE) {
					as_fabric_msg_put(done_m);
					return AS_MIGRATE_STATE_ERROR;
				}
			}

			done_xmit_ms = now;
		}

		emigration_ctrl emig_ctrl;

		if (cf_queue_pop(emig->ctrl_q, &emig_ctrl,
				MIGRATE_RETRANSMIT_STARTDONE_MS) == CF_QUEUE_OK) {
			if (emig_ctrl.emig_id == emig->id &&
					emig_ctrl.op == OPERATION_DONE_ACK) {
				as_fabric_msg_put(done_m);
				return AS_MIGRATE_STATE_DONE;
			}
		}
	}

	// Should never get here.
	cf_crash(AS_MIGRATE, "unexpected - exited infinite while loop");

	return AS_MIGRATE_STATE_ERROR;
}


//==============================================================================
// Local helpers - immigration.
//

void *
run_immigration_reaper(void *unused)
{
	while (true) {
		rchash_reduce(g_immigration_hash, immigration_reaper_reduce_fn, NULL);
		sleep(1);
	}

	return NULL;
}


int
immigration_reaper_reduce_fn(void *key, uint32_t keylen, void *object,
		void *udata)
{
	immigration *immig = (immigration *)object;

	if (immig->start_recv_ms == 0) {
		// If the start time isn't set, immigration is still being processed.
		return RCHASH_OK;
	}

	if (immig->cluster_key != as_paxos_get_cluster_key() ||
			(g_config.migrate_rx_lifetime_ms > 0 &&
					cf_atomic32_get(immig->done_recv) != 0 &&
					cf_getms() > immig->done_recv_ms +
								g_config.migrate_rx_lifetime_ms)) {

		if (cf_rc_count(immig) == 1 && cf_atomic32_get(immig->done_recv) == 0) {
			// No outstanding readers of hkey and hasn't yet completed means
			// that we haven't already decremented migrate_progress_recv.
			if (cf_atomic_int_decr(&g_config.migrate_progress_recv) < 0) {
				cf_warning(AS_MIGRATE, "migrate_progress_recv < 0");
				cf_atomic_int_incr(&g_config.migrate_progress_recv);
			}
		}

		return RCHASH_REDUCE_DELETE;
	}

	return RCHASH_OK;
}


//==============================================================================
// Local helpers - migrate fabric message handling.
//

int
migrate_receive_msg_cb(cf_node src, msg *m, void *udata)
{
	cf_atomic_int_incr(&g_config.migrate_msgs_rcvd);

	uint32_t op = OPERATION_UNDEF;

	msg_get_uint32(m, MIG_FIELD_OP, &op);

	switch (op) {
	//--------------------------------------------
	// Immigration - handle requests:
	//
	case OPERATION_START:
		immigration_handle_start_request(src, m);
		break;
	case OPERATION_INSERT:
		immigration_handle_insert_request(src, m);
		break;
	case OPERATION_CANCEL: // deprecated case
	case OPERATION_DONE:
		immigration_handle_done_request(src, m);
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
	case OPERATION_START_ACK_ALREADY_DONE:
	case OPERATION_DONE_ACK:
		emigration_handle_ctrl_ack(src, m, op);
		break;

	default:
		cf_warning(AS_MIGRATE, "received unexpected message op %u", op);
		as_fabric_msg_put(m);
		break;
	}

	return 0;
}

//----------------------------------------------------------
// Immigration - request message handling.
//

void
immigration_handle_start_request(cf_node src, msg *m) {
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	immigration *immig = cf_rc_alloc(sizeof(immigration));

	cf_assert(immig, AS_MIGRATE, CF_CRITICAL, "malloc");
	cf_atomic_int_incr(&g_config.migrate_rx_object_count);

	immig->done_recv = 0;
	immig->done_recv_ms = 0;
	immig->incoming_ldt_version = 0;
	immig->start_recv_ms = 0;
	immig->src = src;
	AS_PARTITION_RESERVATION_INIT(immig->rsv);

	if (msg_get_uint64(m, MIG_FIELD_CLUSTER_KEY, &immig->cluster_key) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for cluster key failed");
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	if (immig->cluster_key != as_paxos_get_cluster_key()) {
		immigration_release(immig);
		// Do not fail, sender may be from an advanced cluster key.
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_EAGAIN);

		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
		}

		return;
	}

	uint8_t *ns_name = NULL;
	size_t ns_name_len;

	if (msg_get_buf(m, MIG_FIELD_NAMESPACE, &ns_name, &ns_name_len,
			MSG_GET_DIRECT) != 0) {
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);

	if (! ns) {
		cf_warning(AS_MIGRATE, "handle start: bad namespace");
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	uint32_t pid;

	if (msg_get_uint32(m, MIG_FIELD_PARTITION, &pid) != 0) {
		cf_warning(AS_MIGRATE, "handle start: msg get for pid failed");
		immigration_release(immig);
		as_fabric_msg_put(m);
		return;
	}

	as_migrate_result rv = as_partition_migrate_rx(AS_MIGRATE_STATE_START, ns,
			pid, immig->cluster_key, immig->src);

	switch (rv) {
	case AS_MIGRATE_FAIL:
		immigration_release(immig);
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_FAIL);
		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
		}
		return;
	case AS_MIGRATE_AGAIN:
		immigration_release(immig);
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_EAGAIN);
		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
		}
		return;
	case AS_MIGRATE_ALREADY_DONE:
		immigration_release(immig);
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_ALREADY_DONE);
		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
		}
		return;
	case AS_MIGRATE_OK:
		break;
	default:
		cf_crash(AS_MIGRATE, "unexpected as_partition_migrate_rx result");
		break;
	}

	as_partition_reserve_migrate(ns, pid, &immig->rsv, NULL);
	cf_atomic_int_incr(&g_config.migrx_tree_count);

	if (immig->cluster_key != immig->rsv.cluster_key) {
		immigration_release(immig);
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_EAGAIN);

		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
		}

		return;
	}

	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	// This node is going to accept a migration. When a migration starts it is a
	// subrecord migration.
	immig->rx_state = AS_MIGRATE_RX_STATE_SUBRECORD;
	msg_get_uint64(m, MIG_FIELD_VERSION, &immig->incoming_ldt_version);
	immig->pid = immig->rsv.p->partition_id;

	if (rchash_put_unique(g_immigration_hash, (void *)&hkey, sizeof(hkey),
			(void *)immig) == RCHASH_OK) {
		cf_atomic_int_incr(&g_config.migrate_progress_recv);

		immigration_ldt_version ldtv;

		ldtv.incoming_ldt_version = immig->incoming_ldt_version;
		ldtv.pid = immig->pid;

		shash_put(g_immigration_ldt_version_hash, &ldtv, &immig);

		immig->start_recv_ms = cf_getms();
	}
	else {
		immigration_release(immig);
	}

	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_OK);

	if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
			AS_FABRIC_SUCCESS) {
		as_fabric_msg_put(m);
	}
}


void
immigration_handle_insert_request(cf_node src, msg *m) {
	cf_digest *keyd;
	size_t sz = 0;

	if (msg_get_buf(m, MIG_FIELD_DIGEST, (byte **)&keyd, &sz,
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

	if (rchash_get(g_immigration_hash, (void *)&hkey, sizeof(hkey),
			(void **)&immig) == RCHASH_OK) {
		if (immig->cluster_key != as_paxos_get_cluster_key()) {
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		uint32_t generation = 1;

		if (msg_get_uint32(m, MIG_FIELD_GENERATION, &generation) != 0) {
			cf_warning(AS_MIGRATE, "handle insert: no generation - making it 1");
		}

		if (generation == 0) {
			cf_warning(AS_MIGRATE, "handle insert: generation 0 - making it 1");
			generation = 1;
		}

		uint32_t void_time = 0;

		if (msg_get_uint32(m, MIG_FIELD_VOID_TIME, &void_time) != 0) {
			cf_warning(AS_MIGRATE, "handle insert: no void-time - making it 0");
		}

		void *value = NULL;
		size_t value_sz = 0;

		if (msg_get_buf(m, MIG_FIELD_RECORD, (byte **)&value, &value_sz,
				MSG_GET_DIRECT) != 0) {
			cf_warning(AS_MIGRATE, "handle insert: got no record");
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		as_rec_props rec_props;
		as_rec_props_clear(&rec_props);

		// These are optional.
		msg_get_buf(m, MIG_FIELD_REC_PROPS, &rec_props.p_data,
				(size_t *)&rec_props.size, MSG_GET_DIRECT);

		as_record_merge_component c;

		c.record_buf    = value;
		c.record_buf_sz = value_sz;
		c.generation    = generation;
		c.void_time     = void_time;
		c.rec_props     = rec_props;

		if (as_ldt_get_migrate_info(immig, &c, m, keyd)) {
			immigration_release(immig);
			as_fabric_msg_put(m);
			return;
		}

		// TODO - should have inline wrapper to peek pickled bin count.
		if (*(uint16_t *)c.record_buf == 0) {
			cf_warning_digest(AS_MIGRATE, keyd, "handle insert: binless pickle, dropping ");
		}
		else {
			int winner_idx  = -1;
			int rv = as_record_flatten(&immig->rsv, keyd, 1, &c, &winner_idx);

			if (rv != 0) {
				if (rv != -3) {
					// -3 is not a failure. It is get_create failure inside
					// as_record_flatten which is possible in case of race.
					cf_warning_digest(AS_MIGRATE, keyd, "handle insert: record flatten failed %d ", rv);
					immigration_release(immig);
					as_fabric_msg_put(m);
					return;
				}
			}
		}

		immigration_release(immig);
	}

	msg_set_unset(m, MIG_FIELD_INFO);
	msg_set_unset(m, MIG_FIELD_RECORD);
	msg_set_unset(m, MIG_FIELD_DIGEST);
	msg_set_unset(m, MIG_FIELD_NAMESPACE);
	msg_set_unset(m, MIG_FIELD_GENERATION);
	msg_set_unset(m, MIG_FIELD_VOID_TIME);
	msg_set_uint32(m, MIG_FIELD_OP, OPERATION_INSERT_ACK);
	msg_set_unset(m, MIG_FIELD_REC_PROPS);

	if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_LOW) != AS_FABRIC_SUCCESS) {
		cf_warning(AS_MIGRATE, "handle insert: ack send failed");
		as_fabric_msg_put(m);
		return;
	}

	cf_atomic_int_incr(&g_config.migrate_msgs_sent);
}


void
immigration_handle_done_request(cf_node src, msg *m) {
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "handle done: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	// See if this migration already exists & has been notified.
	immigration_hkey hkey;

	hkey.src = src;
	hkey.emig_id = emig_id;

	immigration *immig;

	if (rchash_get(g_immigration_hash, (void *)&hkey, sizeof(hkey),
			(void **)&immig) == RCHASH_OK) {
		if (cf_atomic32_incr(&immig->done_recv) == 1) {
			// Record the time of the first DONE received.
			immig->done_recv_ms = cf_getms();

			if (cf_atomic_int_decr(&g_config.migrate_progress_recv) < 0) {
				cf_warning(AS_MIGRATE, "migrate_progress_recv < 0");
				cf_atomic_int_incr(&g_config.migrate_progress_recv);
			}

			as_partition_migrate_rx(AS_MIGRATE_STATE_DONE, immig->rsv.ns,
					immig->rsv.pid, immig->cluster_key, immig->src);

			if (g_config.migrate_rx_lifetime_ms <= 0) {
				rchash_delete(g_immigration_hash, (void *)&hkey, sizeof(hkey));
			}

			// And we always need to release the extra ref-count now that we're
			// done accessing the object.
			immigration_release(immig);
		}
		// else - was likely a retransmitted done message.

		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE_ACK);

		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			as_fabric_msg_put(m);
			return;
		}

		cf_atomic_int_incr(&g_config.migrate_msgs_sent);
	}
	else {
		msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE_ACK);

		if (as_fabric_send(src, m, AS_FABRIC_PRIORITY_MEDIUM) !=
				AS_FABRIC_SUCCESS) {
			cf_warning(AS_MIGRATE, "handle done: received unknown done, could not ack");
			as_fabric_msg_put(m);
		}

		cf_detail(AS_MIGRATE, "handle done: received done message for unknown migrate, acking source %lx emig id %u",
				src, emig_id);
		cf_atomic_int_incr(&g_config.migrate_msgs_sent);
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

	if (rchash_get(g_emigration_hash, (void *)&emig_id, sizeof(emig_id),
			(void **)&emig) != RCHASH_OK) {
		// Probably came from a migration prior to the latest rebalance.
		as_fabric_msg_put(m);
		return;
	}

	uint32_t insert_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_INSERT_ID, &insert_id) != 0) {
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
			if (cf_atomic32_sub(&emig->bytes_emigrating, m->bytes_used) < 0) {
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
emigration_handle_ctrl_ack(cf_node src, msg *m, uint32_t op) {
	uint32_t emig_id;

	if (msg_get_uint32(m, MIG_FIELD_EMIG_ID, &emig_id) != 0) {
		cf_warning(AS_MIGRATE, "ctrl ack: msg get for emig id failed");
		as_fabric_msg_put(m);
		return;
	}

	emigration *emig;

	if (rchash_get(g_emigration_hash, (void *)&emig_id, sizeof(emig_id),
			(void **)&emig) == RCHASH_OK) {
		emigration_ctrl emig_ctrl;

		emig_ctrl.emig_id = emig_id;
		emig_ctrl.op = op;

		if (emig->dest == src) {
			cf_queue_push(emig->ctrl_q, &emig_ctrl);
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

	as_fabric_msg_put(m);
}


//==============================================================================
// Local helpers - info API helpers.
//

int
emigration_dump_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	uint32_t emig_id = *(uint32_t *)key;
	emigration *emig = (emigration *)object;
	int *item_num = (int *)udata;

	cf_info(AS_MIGRATE, "[%d]: mig_id %u : id %u ; ck %016lx", *item_num,
			emig_id, emig->id, emig->cluster_key);

	*item_num += 1;

	return 0;
}


int
immigration_dump_reduce_fn(void *key, uint32_t keylen, void *object,
		void *udata)
{
	immigration_hkey *hkey = (immigration_hkey *)key;
	immigration *immig = (immigration *)object;
	int *item_num = (int *)udata;

	cf_info(AS_MIGRATE, "[%d]: src %016lx ; id %u : src %016lx ; done recv %u ; start recv ms %lu ; done recv ms %lu ; ck %016lx",
			*item_num, hkey->src, hkey->emig_id, immig->src, immig->done_recv,
			immig->start_recv_ms, immig->done_recv_ms, immig->cluster_key);

	*item_num += 1;

	return 0;
}


//==============================================================================
// Local helpers - LDT-related.
//

bool
as_ldt_precord_is_esr(const pickled_record *pr)
{
	uint16_t *ldt_rectype_bits;

	if (pr->rec_props.size != 0 &&
			(as_rec_props_get_value(&pr->rec_props, CL_REC_PROPS_FIELD_LDT_TYPE,
					NULL, (uint8_t**)&ldt_rectype_bits) == 0)) {
		return as_ldt_flag_has_esr(*ldt_rectype_bits);
	}

	return false;
}


bool
as_ldt_precord_is_subrec(const pickled_record *pr)
{
	uint16_t *ldt_rectype_bits;

	if (pr->rec_props.size != 0 &&
			(as_rec_props_get_value(&pr->rec_props, CL_REC_PROPS_FIELD_LDT_TYPE,
					NULL, (uint8_t**)&ldt_rectype_bits) == 0)) {
		return as_ldt_flag_has_subrec(*ldt_rectype_bits);
	}

	return false;
}


bool
as_ldt_precord_is_parent(const pickled_record *pr)
{
	uint16_t *ldt_rectype_bits;

	if (pr->rec_props.size != 0 &&
			(as_rec_props_get_value(&pr->rec_props, CL_REC_PROPS_FIELD_LDT_TYPE,
					NULL, (uint8_t**)&ldt_rectype_bits) == 0)) {
		return as_ldt_flag_has_parent(*ldt_rectype_bits);
	}

	return false;
}


// Set up the LDT information.
// 1. Flag
// 2. Parent Digest
// 3. Esr Digest
// 4. Version
int
as_ldt_fill_mig_msg(const emigration *emig, msg *m, const pickled_record *pr)
{
	if (! emig->rsv.ns->ldt_enabled) {
		msg_set_unset(m, MIG_FIELD_VERSION);
		msg_set_unset(m, MIG_FIELD_PVOID_TIME);
		msg_set_unset(m, MIG_FIELD_PGENERATION);
		msg_set_unset(m, MIG_FIELD_PDIGEST);
		msg_set_unset(m, MIG_FIELD_EDIGEST);
		msg_set_unset(m, MIG_FIELD_INFO);

		return 0;
	}

	bool is_subrecord = emig->tx_state == AS_PARTITION_MIG_TX_STATE_SUBRECORD;

	if (! is_subrecord) {
		cf_assert((emig->tx_state == AS_PARTITION_MIG_TX_STATE_RECORD),
				AS_PARTITION, CF_CRITICAL,
				"unexpected partition migration state at source %d:%d",
				emig->tx_state, emig->rsv.p->partition_id);
	}

	msg_set_uint64(m, MIG_FIELD_VERSION, pr->version);

	uint32_t info = 0;

	if (is_subrecord) {
		as_index_ref r_ref;
		r_ref.skip_lock = false;

		int rv = as_record_get(emig->rsv.tree, (cf_digest *)&pr->pkeyd, &r_ref,
				emig->rsv.ns);

		if (rv == 0) {
			msg_set_uint32(m, MIG_FIELD_PVOID_TIME, r_ref.r->void_time);
			msg_set_uint32(m, MIG_FIELD_PGENERATION, r_ref.r->generation);
			as_record_done(&r_ref, emig->rsv.ns);
		}
		else {
			return -1;
		}

		msg_set_buf(m, MIG_FIELD_PDIGEST, (void *)&pr->pkeyd, sizeof(cf_digest),
				MSG_SET_COPY);

		if (as_ldt_precord_is_esr(pr)) {
			info |= MIG_INFO_LDT_ESR;
			msg_set_unset(m, MIG_FIELD_EDIGEST);
		}
		else if (as_ldt_precord_is_subrec(pr)) {
			info |= MIG_INFO_LDT_SUBREC;
			msg_set_buf(m, MIG_FIELD_EDIGEST, (void *)&pr->ekeyd,
					sizeof(cf_digest), MSG_SET_COPY);
		}
		else {
			cf_warning(AS_MIGRATE, "expected subrec and esr bit not found");
		}
	}
	else {
		if (as_ldt_precord_is_parent(pr)) {
			info |= MIG_INFO_LDT_REC;
		}

		msg_set_unset(m, MIG_FIELD_PVOID_TIME);
		msg_set_unset(m, MIG_FIELD_PGENERATION);
		msg_set_unset(m, MIG_FIELD_PDIGEST);
		msg_set_unset(m, MIG_FIELD_EDIGEST);
	}

	msg_set_uint32(m, MIG_FIELD_INFO, info);

	return 0;
}


void
as_ldt_fill_precord(pickled_record *pr, as_storage_rd *rd,
		const emigration *emig)
{
	pr->pkeyd = cf_digest_zero;
	pr->ekeyd = cf_digest_zero;
	pr->version = 0;

	if (! rd->ns->ldt_enabled) {
		return;
	}

	bool is_subrec = false;
	bool is_parent = false;

	if (as_ldt_precord_is_subrec(pr)) {
		int rv = as_ldt_subrec_storage_get_digests(rd, &pr->ekeyd, &pr->pkeyd);

		if (rv) {
			cf_warning(AS_MIGRATE, "ldt_migration: could not find parent or esr key in subrec rv=%d",
					rv);
		}

		is_subrec = true;
	}
	else if (as_ldt_precord_is_esr(pr)) {
		as_ldt_subrec_storage_get_digests(rd, NULL, &pr->pkeyd);
		is_subrec = true;
	}
	else {
		// When tree is being reduced for the record the state should already
		// be STATE_RECORD.
		cf_assert((emig->tx_state == AS_PARTITION_MIG_TX_STATE_RECORD),
				AS_PARTITION, CF_CRITICAL,
				"unexpected partition migration state at source %d:%d",
				emig->tx_state, emig->rsv.p->partition_id);

		if (as_ldt_precord_is_parent(pr)) {
			is_parent = true;
		}
	}

	uint64_t new_version = emig->rsv.p->current_outgoing_ldt_version;

	if (is_parent) {
		uint64_t old_version = 0;

		as_ldt_parent_storage_get_version(rd, &old_version, true, __FILE__,
				__LINE__);

		pr->version = new_version ? new_version : old_version;
	}
	else if (is_subrec) {
		cf_assert((emig->tx_state == AS_PARTITION_MIG_TX_STATE_SUBRECORD),
				AS_PARTITION, CF_CRITICAL,
				"unexpected partition migration state at source %d:%d",
				emig->tx_state, emig->rsv.p->partition_id);

		uint64_t old_version = as_ldt_subdigest_getversion(&pr->keyd);

		if (new_version) {
			as_ldt_subdigest_setversion(&pr->keyd, new_version);
			pr->version = new_version;
		}
		else {
			pr->version = old_version;
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
		msg *m, cf_digest *keyd)
{
	uint32_t info = 0;

	c->flag        = AS_COMPONENT_FLAG_MIG;
	c->pdigest     = cf_digest_zero;
	c->edigest     = cf_digest_zero;
	c->version     = 0;
	c->pgeneration = 0;
	c->pvoid_time  = 0;

	if (! immig->rsv.ns->ldt_enabled) {
		return 0;
	}

	if (msg_get_uint32(m, MIG_FIELD_INFO, &info) == 0) {
		if ((info & MIG_INFO_LDT_SUBREC) != 0) {
			c->flag |= AS_COMPONENT_FLAG_LDT_SUBREC;
		}
		else if ((info & MIG_INFO_LDT_REC) != 0) {
			c->flag |= AS_COMPONENT_FLAG_LDT_REC;
		}
		else if ((info & MIG_INFO_LDT_ESR) != 0) {
			c->flag |= AS_COMPONENT_FLAG_LDT_ESR;
		}
	}
	// else - resort to defaults

	size_t sz = 0;
	cf_digest *key;

	msg_get_buf(m, MIG_FIELD_PDIGEST, (byte **)&key, &sz, MSG_GET_DIRECT);

	if (key) {
		c->pdigest = *key;
	}

	msg_get_buf(m, MIG_FIELD_EDIGEST, (byte **)&key, &sz, MSG_GET_DIRECT);

	if (key) {
		c->edigest = *key;
	}

	msg_get_uint64(m, MIG_FIELD_VERSION, &c->version);
	msg_get_uint32(m, MIG_FIELD_PGENERATION, &c->pgeneration);
	msg_get_uint32(m, MIG_FIELD_PVOID_TIME, &c->pvoid_time);

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
