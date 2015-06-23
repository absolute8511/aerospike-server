/*
 * migrate.c
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
 * migrate.c
 * Moves a partition from one machine to another using the fabric messaging system
 *
 * Notes:
 *
 * -------------------------------------------------------------------------------
 * Main Functions
 * ==============
 *
 * as_partition_balance_new:   Main starting function
 *
 * as_partition_migrate_rx :   Called at the begining and end of receive of migration
 *                             for a partition. Does the partition state checks and
 *                             transition.
 *
 * as_partition_migrate_tx:    Called at the end of the outgoing partition migration.
 *
 *
 * migrate_msg_fn:             RPC callback function called when the migrate related
 *                             messages are received. Both at incoming and outgoing
 *                             node
 *
 * as_migrate_tree:            Workhorse function which does the tree migration.
 *                             (Needs optimization)
 *
 *
 * migrate_xmit_fn:            Function which serves all the migration requests.
 *                             Everything in g_migrate_q, migrate_msg_fn queues up
 *                             request to this thread.
 *
 * as_migrate:                 Called to initiate a new migration for the partition.
 *                             this creates requests and queues up to g_migrate_q.
 *                             Called by as_partition_balance_new. Or migrate_xmit_fn
 *                             in response to primary migration to initiate subsequent
 *                             transaction.
 * -------------------------------------------------------------------------------
 * Data Structures
 * ===============
 *
 * mig->xmit_control_q : Queue for migration related control messages per migration object
 *
 * mig->pickled_array  : Array of pickled record formed after migrate reduce used to do
 *                       record migration
 *
 * g_migrate_recv_control_hash: Hash for migrate_recv_control structure used to manage
 *                       incoming migration. Create when migration OPERATION_START is
 *                       received. This is on receiving node.
 *
 * g_migrate_hash:       Hash for mig object on the sending node.
 *
 *
 * -------------------------------------------------------------------------------
 *
 *
 * -------------------------------------------------------------------------------
 * Call Graph:
 * ==========
 *
 * SENDER
 * ------
 *
 * as_partition_balance_new  : Entry function which restarts migration
 *     |
 *     |--> partition_migrate_record_fill
 *     |
 *     |--> as_migrate  : Called per partition: Allocates migration object and
 *            |           populates values reserves partitions; sets cluster key:
 *            |
 *       g_migrate_q
 *            |
 *            |--> migrate_xmit_fn : (g_config.n_migrate_threads), create xmit_control_q
 *                      |            to manage control information per migration and
 *                      |            retransmit hash. Add mig into the global g_migrate_hash
 *                      |
 *                      |            State Machine
 *                      |            1. Send Migration Start message OPERATION_START to
 *                      |                all destination node
 *                      |            2. Wait for all control ack in the queue xmit_control_q
 *                      |
 *                      | --> as_migrate_tree : Send subtree first and parent tree
 *
 * as_partition_migrate_tx: Function called at the end of the migration in both the cases
 *                          where is succeds or fails
 *
 * RECEIVER
 * --------
 *
 * migrate_msg_fn :  Called when migration related message is received at the node where
 *                   migration is incoming.
 *
 *    State Machine
 *
 *         1. On receiving OPERATION_START
 *            --  allocate migrate_recv_control and instantiate it and get
 *                ready to receive migration.
 *            --  Call as_partition_migrate_rx with AS_MIGRATE_STATE_START
 *                which does state checks and approves the incoming migration
 *            -- Reserve the partition for the migration on receiving node.
 *            -- put migrate_recv_control in g_migrate_recv_control_hash with
 *               key as source_node and mig_id
 *            -- send OPERATION_START_ACK_OK
 *
 *            Now node is ready to receive incoming migrations
 *
*/

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

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
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "msg.h"
#include "queue.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "fabric/fabric.h"
#include "storage/storage.h"


// These must ALL be OFF in Production.

// Enable Migrate Debug Msg(s)
// #define DEBUG 1
// Turn on MESSAGE DUMP
// #define DEBUG_MSG 1
// #define EXTRA_CHECKS 1
// #define USE_NETWORK_POLICY
// #define USE_MARK_SWEEP 1

// Template for migrate messages
#define MIG_FIELD_OP 0
#define MIG_FIELD_TID 1
#define MIG_FIELD_MIG_ID 2
#define MIG_FIELD_NAMESPACE 3
#define MIG_FIELD_PARTITION 4
#define MIG_FIELD_DIGEST 5
#define MIG_FIELD_GENERATION 6
#define MIG_FIELD_RECORD 7
#define MIG_FIELD_CLUSTER_KEY 8
#define MIG_FIELD_VINFOSET 9    	// sent with INSERT
#define MIG_FIELD_VOID_TIME 10
#define MIG_FIELD_TYPE 11
#define MIG_FIELD_REC_PROPS 12
#define MIG_FIELD_INFO 13
#define MIG_FIELD_VERSION 14
#define MIG_FIELD_PDIGEST 15
#define MIG_FIELD_EDIGEST 16
#define MIG_FIELD_PGENERATION 17
#define MIG_FIELD_PVOID_TIME 18

#define OPERATION_INSERT 1
#define OPERATION_ACK 2
#define OPERATION_START 3
#define OPERATION_START_ACK_OK 4
#define OPERATION_START_ACK_EAGAIN 5
#define OPERATION_START_ACK_FAIL 6
#define OPERATION_START_ACK_ALREADY_DONE 7
#define OPERATION_DONE 8
#define OPERATION_DONE_ACK 9
#define OPERATION_CANCEL 10

msg_template migrate_mt[] = {
	{ MIG_FIELD_OP, M_FT_UINT32 },
	{ MIG_FIELD_TID, M_FT_UINT32 },
	{ MIG_FIELD_MIG_ID, M_FT_UINT32 },
	{ MIG_FIELD_NAMESPACE, M_FT_BUF },
	{ MIG_FIELD_PARTITION, M_FT_UINT32 },
	{ MIG_FIELD_DIGEST, M_FT_BUF },
	{ MIG_FIELD_GENERATION, M_FT_UINT32 },
	{ MIG_FIELD_RECORD, M_FT_BUF },
	{ MIG_FIELD_CLUSTER_KEY, M_FT_UINT64 },
	{ MIG_FIELD_VINFOSET, M_FT_BUF },
	{ MIG_FIELD_VOID_TIME, M_FT_UINT32 },
	{ MIG_FIELD_TYPE, M_FT_UINT32 }, // AS_MIGRATE_TYPE - 0 merge, 1 overwite
	{ MIG_FIELD_REC_PROPS, M_FT_BUF },
	{ MIG_FIELD_INFO, M_FT_UINT32 },
	{ MIG_FIELD_VERSION, M_FT_UINT64 },
	{ MIG_FIELD_PDIGEST, M_FT_BUF },
	{ MIG_FIELD_EDIGEST, M_FT_BUF },
	{ MIG_FIELD_PGENERATION, M_FT_UINT32 },
	{ MIG_FIELD_PVOID_TIME, M_FT_UINT32 },
};

// If the bit is not set then it is normal record
#define MIG_INFO_LDT_REC    0x0001
#define MIG_INFO_LDT_SUBREC 0x0002
#define MIG_INFO_LDT_ESR    0x0004

// Must make retransmits more clever. Until then, make retransmits really, really long
#define MIGRATE_RETRANSMIT_MS (g_config.transaction_retry_ms)

//
// Warning. Ok. So the transmit/retransmit system here is ghetto and leads to serious problems
// and needs a rethink. If you set this value too low, what can happen is the start-retransmit
// can transit after the entire migration is complete, leading to a dangingling migrate.
//

#define MIGRATE_RETRANSMIT_STARTDONE_MS (g_config.transaction_retry_ms)

typedef struct pickled_record_s {
	cf_digest				key;
	uint32_t				generation;
	uint32_t				void_time;
	byte 					*record_buf;   // pickled!
	size_t					record_len;
	as_rec_props            rec_props;
	size_t                  vinfo_buf_len;
	uint8_t                 vinfo_buf[AS_PARTITION_VINFOSET_PICKLE_MAX ];
	cf_digest				pkey;
	cf_digest				ekey;
	uint64_t                version;
} pickled_record;

typedef struct migration_t {

	cf_node		dst_nodes[AS_CLUSTER_SZ];
	uint32_t	dst_nodes_sz;

	as_partition_reservation rsv;

	// reduce will gen this up
	uint		pickled_alloc;
	uint		pickled_size;
	pickled_record *pickled_array;

	uint32_t	id;
	as_migrate_type mig_type;

	// the START and DONE messages - goes to NULL when the ack is received
	msg *start_m;
	uint64_t	start_xmit_ms;
	bool 		start_done[AS_CLUSTER_SZ];

	msg *done_m;
	uint64_t	done_xmit_ms;
	bool		done_done[AS_CLUSTER_SZ];

	shash		*retransmit_hash;
	cf_queue	*xmit_control_q;

	// Migrates with higher priority will be done first
	int         migration_sort_priority;

	uint64_t	yield_count;

	as_migrate_callback    cb;
	void				*udata;

	uint64_t cluster_key;

	as_partition_mig_tx_state txstate;
} migration;

/*
** some things are easier as globals
*/

// global hash all migrations, so an incoming ack can find its migrate
// index is the migrate_id ; value is the migration *
static rchash *g_migrate_hash;

// an id for each migration so we can keep track of everything
static cf_atomic32   g_migrate_id;

static shash *g_migrate_incoming_ldt_version_hash;

typedef struct migrate_recv_ldt_version_t {
	uint64_t        incoming_ldt_version;
	as_partition_id pid;
} __attribute__((__packed__)) migrate_recv_ldt_version;

// a per-pusher transaction id
static cf_atomic32   g_migrate_trans_id;

static as_migrate_callback g_event_cb = 0;
static void *g_udata = 0;

/*
** queue of requested migrates waiting to be serviced, one at a time
**   contains (migration *). All first looks come here.
*/
static cf_queue_priority *g_migrate_q;

/*
** queue of control information - like when a start message has been acked -
** 	used on transmit and such
*/

typedef struct migrate_xmit_control_s {
	uint32_t		mig_id;
	uint32_t		node_offset; // which node in the local migrate table did the deed
	int				op;
} migrate_xmit_control;

/*
** the threads that do all the migration transmission. A variable number of threads.
** Regrettably, this tunes the migrate/current work balance.
*/

static pthread_t g_migrate_rx_reaper_th;

/*
 *  Attribute used to make migrate xmit threads detatched.
 */
static pthread_attr_t migrate_xmit_th_attr;

/*
 *  Array of migrate xmit threads.
 */
static pthread_t g_migrate_xmit_th[MAX_NUM_MIGRATE_XMIT_THREADS];

typedef struct migrate_retransmit_s {
	uint64_t	xmit_ms;   // time of last xmit - 0 when done
	bool		done[AS_CLUSTER_SZ]; // could implement as a bitfield if we were cool
	migration	*mig;
	msg			*m;
} migrate_retransmit;

//
// migrate control methodology
//
// Need a mechanism to not duplicate-notify for control (START and DONE events)
// on the *receive* side of the control plane.
// So have an indexed rchash - index being the source node and the migration id,
// which is unique on the source side - so you can detect if a START has been
// received by the existance of the element in the hash table. You know whether
// a done has been received (and only once) by looking at the counter.
//
// The true annoyance is that debouncing the DONEs only works when the structure
// sticks around after the done has been received & acked. So you need a scavenger
// function that reaps out the dead ones. Which is why you need the MS of the last
// done received. (hey, new thought - what about if you receive a done, and it's
// NOT in the table, then you debounce? That saves the reaper.... but there's
// some chance the START and DONE could both be retransmitting... can't exactly
// imagine how that would happen

// this hash has a migrate control structure.
// it's used to keep track of the inbound migrations
static rchash *g_migrate_recv_control_hash = 0;

// NB - important to be packed, otherwise we'll have uninitalized space in the middle

typedef struct migrate_recv_control_index_t {
	cf_node		source_node;
	uint32_t	mig_id;
} __attribute__((__packed__)) migrate_recv_control_index;

// NB: arguably, the flag values could be folded into the ms values,
// but that would require 64-bit TAS across all machines, which I don't have
// at the moment. It's a small thing, because these structures are infrequently
// used

typedef struct migrate_recv_control_t {
	cf_atomic32 done_recv;		// flag - 0 if not yet received, atomic counter for receives
	uint64_t	start_recv_ms;  // time the first START event was received
	uint64_t	done_recv_ms;   // time the first DONE event was received (for purposes of clean-up
	//    via expiration, lifetime is measured from this value)
	as_partition_reservation rsv;
	uint64_t cluster_key;
	cf_node source_node;
	as_migrate_type mig_type;
	as_partition_mig_rx_state rxstate;
	as_partition_vinfoset    vinfoset;
	uint64_t                 incoming_ldt_version;
	as_partition_id          pid;
} migrate_recv_control;

void as_migrate_print_cluster_key(const char *message)
{
	cf_info(AS_MIGRATE, "%s: cluster key %"PRIx64"", message, as_paxos_get_cluster_key());
}

void as_migrate_print2_cluster_key(const char *message, uint64_t cluster_key)
{
	cf_debug(AS_MIGRATE, "%s: cluster key global %"PRIx64" recd %"PRIx64"", message, as_paxos_get_cluster_key(), cluster_key);
}

// this hash has the start-done
uint32_t
migrate_ldt_version_hashfn(void *value)
{
	return( *(uint32_t *)value);
}


uint32_t
migrate_id_hashfn(void *value, uint32_t sz)
{
	return( *(uint32_t *)value);
}

uint32_t
migrate_id_shashfn(void *value)
{
	return( *(uint32_t *)value);
}

uint32_t
migrate_recv_control_hashfn(void *value, uint32_t value_len)
{
	migrate_recv_control_index *mci = value;
	if (value_len != sizeof(migrate_recv_control_index)) {
		cf_debug(AS_MIGRATE, "internal confusion: migrate control hashfn with wrong len fix fix");
		return(0);
	}
	return( mci->mig_id );

}

void
migrate_recv_control_destroy(void *parm)
{
	migrate_recv_control *mc = (migrate_recv_control *) parm;

	migrate_recv_ldt_version mc_l;
	mc_l.incoming_ldt_version = mc->incoming_ldt_version;
	mc_l.pid                  = mc->pid;

	if (mc->rsv.p) {
		as_partition_release(&mc->rsv);
		cf_atomic_int_decr(&g_config.migrx_tree_count);
	}

	shash_delete(g_migrate_incoming_ldt_version_hash, &mc_l);
    cf_detail(AS_LDT, "Remove incoming for pid:%d",mc_l.pid); 

	cf_atomic_int_decr(&g_config.migrate_rx_object_count);
}


#define migrate_recv_control_release(__mc) 				\
    if (0 == cf_rc_release(__mc)) {  					\
        migrate_recv_control_destroy(__mc);				\
		memset(__mc, 0, sizeof(migrate_recv_control) );	\
        cf_rc_free(__mc);									\
    }

bool
as_ldt_precord_is_esr(pickled_record *pr)
{
	uint16_t *ldt_rectype_bits;
	if (pr->rec_props.size != 0 &&
			(as_rec_props_get_value(&pr->rec_props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
									(uint8_t**)&ldt_rectype_bits) == 0)) {
		if (as_ldt_flag_has_esr(*ldt_rectype_bits))
			return true;
		else
			return false;
	}
	return false;
}

bool
as_ldt_precord_is_subrec(pickled_record *pr)
{
	uint16_t *ldt_rectype_bits;
	if (pr->rec_props.size != 0 &&
			(as_rec_props_get_value(&pr->rec_props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
									(uint8_t**)&ldt_rectype_bits) == 0)) {
		if (as_ldt_flag_has_subrec(*ldt_rectype_bits))
			return true;
		else
			return false;
	}
	return false;
}

bool
as_ldt_precord_is_parent(pickled_record *pr)
{
	uint16_t *ldt_rectype_bits;
	if (pr->rec_props.size != 0 &&
			(as_rec_props_get_value(&pr->rec_props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
									(uint8_t**)&ldt_rectype_bits) == 0)) {
		if (as_ldt_flag_has_parent(*ldt_rectype_bits))
			return true;
		else
			return false;
	}
	return false;
}


// Set up the LDT information
// 1. Flag
// 2. Parent Digest
// 3. Esr Digest
// 4. Version
int
as_ldt_fill_mig_msg(migration *mig, msg *m, pickled_record *pr, bool is_subrecord)
{
	as_index_ref r_ref;
	r_ref.skip_lock = false;
	if (!mig || !m || !pr || !mig->rsv.tree || !mig->rsv.ns) {
		cf_warning(AS_LDT, "Mig not initialized properly %p:%p:%p:%p:%p", mig, m, pr, mig->rsv.tree, mig->rsv.ns);
		return -1;
	}
	if (mig->rsv.ns->ldt_enabled == false) {
		msg_set_unset (m, MIG_FIELD_VERSION);
		msg_set_unset (m, MIG_FIELD_PVOID_TIME);
		msg_set_unset (m, MIG_FIELD_PGENERATION);
		msg_set_unset (m, MIG_FIELD_PDIGEST);
		msg_set_unset (m, MIG_FIELD_EDIGEST);
		msg_set_unset (m, MIG_FIELD_INFO);
		return 0;
	}

	if (!is_subrecord) {
		cf_assert((mig->txstate == AS_PARTITION_MIG_TX_STATE_RECORD),
				  AS_PARTITION, CF_CRITICAL,
				  "Unexpected Partition Migration State at Source %d:%d", mig->txstate, mig->rsv.p->partition_id);
	}
	msg_set_uint64(m, MIG_FIELD_VERSION, pr->version);
	uint32_t info = 0;
	if (is_subrecord) {
		int rv = as_record_get(mig->rsv.tree, &pr->pkey, &r_ref, mig->rsv.ns);
		if (rv == 0) {
			msg_set_uint32(m, MIG_FIELD_PVOID_TIME, r_ref.r->void_time);
			msg_set_uint32(m, MIG_FIELD_PGENERATION, r_ref.r->generation);
			as_record_done(&r_ref, mig->rsv.ns);
		} else {
			cf_debug_digest(AS_LDT, &pr->pkey, "No parent found that would mean this is stale version ... skip rv = %d", rv);
			return -1;
		}

		msg_set_buf   (m, MIG_FIELD_PDIGEST, (void *) &pr->pkey, sizeof(cf_digest), MSG_SET_COPY);

		if (as_ldt_precord_is_esr(pr)) {
			info |= MIG_INFO_LDT_ESR;
			msg_set_unset (m, MIG_FIELD_EDIGEST);
		} else if (as_ldt_precord_is_subrec(pr)) {
			info |= MIG_INFO_LDT_SUBREC;
			msg_set_buf (m, MIG_FIELD_EDIGEST, (void *) &pr->ekey, sizeof(cf_digest), MSG_SET_COPY);
		} else {
			cf_warning(AS_MIGRATE, "Expected SUBREC and ESR bit not found !!");
		}
	} else {
		if (as_ldt_precord_is_parent(pr)) {
			info |= MIG_INFO_LDT_REC;
		}
		msg_set_unset (m, MIG_FIELD_PVOID_TIME);
		msg_set_unset (m, MIG_FIELD_PGENERATION);
		msg_set_unset (m, MIG_FIELD_PDIGEST);
		msg_set_unset (m, MIG_FIELD_EDIGEST);
	}
	msg_set_uint32(m, MIG_FIELD_INFO, info);
	cf_detail(AS_MIGRATE, "LDT_MIGRATION Sending %sRecord version %ld %d",
			  as_ldt_precord_is_esr(pr) ? "ESR"
			  : (as_ldt_precord_is_subrec(pr) ? "SUB"
				 : ((as_ldt_precord_is_parent(pr)) ? "LDT" : "")), pr->version, info);
	return 0;
}

int
as_ldt_fill_precord(pickled_record *pr, as_storage_rd *rd, migration *mig)
{
	if (!pr || !rd) {
		cf_warning(AS_MIGRATE, "Internal Migration error pr or rd unexpectedly NULL");
		return -1;
	}
	pr->pkey       = cf_digest_zero;
	pr->ekey       = cf_digest_zero;
	pr->version    = 0;
	if (rd->ns->ldt_enabled == false) {
		return 0;
	}

	bool is_subrec = false;
	bool is_parent = false;
	if (as_ldt_precord_is_subrec(pr)) {
		int rv = as_ldt_subrec_storage_get_digests(rd, &pr->ekey, &pr->pkey);
		if (rv) {
			cf_warning(AS_MIGRATE, "LDT_MIGRATION Could Not find Parent or ESR key in subrec rv=%d", rv);
		}
		is_subrec = true;
	} else if (as_ldt_precord_is_esr(pr)) {
		int rv = as_ldt_subrec_storage_get_digests(rd, NULL, &pr->pkey);
		if (rv) {
			cf_detail(AS_MIGRATE, "LDT_MIGRATION Could find PARENT key in subrec rv=%d", rv);
		}
		is_subrec = true;
	} else {
		// When tree is being reduced for the record the state should already be STATE_RECORD
		cf_assert((mig->txstate == AS_PARTITION_MIG_TX_STATE_RECORD),
				  AS_PARTITION, CF_CRITICAL,
				  "Unexpected Partition Migration State at Source %d:%d", mig->txstate, mig->rsv.p->partition_id);
		if (as_ldt_precord_is_parent(pr)) {
			is_parent = true;
		}
	}

	uint64_t new_version = mig->rsv.p->current_outgoing_ldt_version;
	if (is_parent) {
		uint64_t old_version = 0;
		if (as_ldt_parent_storage_get_version(rd, &old_version, true,__FILE__, __LINE__)) {
			cf_detail(AS_MIGRATE, "LDT_MIGRATION could not find version in parent record");
		}
		if (new_version) {
			cf_detail(AS_MIGRATE, "LDT_MIGRATION Parent Setting New Version %ld from %ld %"PRIx64"",
					  new_version, old_version);
			pr->version = new_version;
		} else {
			cf_detail(AS_MIGRATE, "LDT_MIGRATION Parent Keeping Old Version %ld intact %"PRIx64"",
					  old_version, *(uint64_t *)&pr->key);
			pr->version = old_version;
		}
	} else if (is_subrec) {
		cf_assert((mig->txstate == AS_PARTITION_MIG_TX_STATE_SUBRECORD),
				  AS_PARTITION, CF_CRITICAL,
				  "Unexpected Partition Migration State at Source %d:%d", mig->txstate, mig->rsv.p->partition_id);

		uint64_t old_version = as_ldt_subdigest_getversion(&pr->key);
		if (new_version) {
			cf_detail_digest(AS_MIGRATE, &pr->key, "LDT_MIGRATION Subrecord Setting New Version %ld from %ld %"PRIx64"",
					  new_version, old_version, *(uint64_t *)&pr->key);
			as_ldt_subdigest_setversion(&pr->key, new_version);
			pr->version = new_version;
		} else {
			cf_detail(AS_MIGRATE, "LDT_MIGRATION Subrecord Keeping Old Version %ld intact %"PRIx64"",
					  old_version, *(uint64_t *)&pr->key);
			pr->version = old_version;
		}
	}
	return 0;
}

// Extracts ldt related infrom the migration messages
// return <0 in case of some sort of failure
// returns 0 for success
//
// side effect component will be filled up
int
as_ldt_get_migrate_info(migrate_recv_control *mc, as_record_merge_component *c, msg *m, cf_digest *digest)
{
	uint32_t info   = 0;
	c->flag         = AS_COMPONENT_FLAG_MIG;
	c->pdigest      = cf_digest_zero;
	c->edigest      = cf_digest_zero;
	c->version      = 0;
	c->pgeneration  = 0;
	c->pvoid_time   = 0;
	if (mc->rsv.ns->ldt_enabled == false) {
		return 0;
	}

	if (0 == msg_get_uint32(m, MIG_FIELD_INFO, &info)) {
		if (info & MIG_INFO_LDT_SUBREC) {
			c->flag |= AS_COMPONENT_FLAG_LDT_SUBREC;
		} else if (info & MIG_INFO_LDT_REC) {
			c->flag |= AS_COMPONENT_FLAG_LDT_REC;
		} else if (info & MIG_INFO_LDT_ESR) {
			c->flag |= AS_COMPONENT_FLAG_LDT_ESR;
		}
	} else {
		cf_debug(AS_LDT, "Incomplete Migration information resorting to defaults !!!");
	}

	size_t sz = 0;
	cf_digest *key;
	msg_get_buf   (m, MIG_FIELD_PDIGEST, (byte **) &key, &sz, MSG_GET_DIRECT);
	if (key)    c->pdigest = *key;
	msg_get_buf   (m, MIG_FIELD_EDIGEST, (byte **) &key, &sz, MSG_GET_DIRECT);
	if (key)    c->edigest = *key;
	msg_get_uint64(m, MIG_FIELD_VERSION, &c->version);
	msg_get_uint32(m, MIG_FIELD_PGENERATION, &c->pgeneration);
	msg_get_uint32(m, MIG_FIELD_PVOID_TIME, &c->pvoid_time);

	if (COMPONENT_IS_LDT_SUB(c)) {
		cf_detail_digest(AS_MIGRATE, digest, "LDT_MIGRATION: Incoming version %ld Started Receiving Sub Record Migration !! %s:%d:%d:%d",
			  mc->incoming_ldt_version, mc->rsv.ns->name, mc->rsv.p->partition_id, 
			  mc->rsv.p->vp->elements, mc->rsv.p->sub_vp->elements);
		if (mc->rxstate == AS_MIGRATE_RX_STATE_RECORD) {
			cf_debug(AS_PARTITION, "Unexpected Partition Migration State %d:%d", mc->rxstate, mc->rsv.p->partition_id);
			// Consider a case where source sends sub record migration request multiple
			// times and moves on to record migration. In that case it is possible to 
			// receieve the subrecord out of order. There is no way to control .. we make
			// sure at the source that record migration does not start before all the sub
			// record migration have finished.
			//
			// So we simply continue this should be noop
		}
	} else if (COMPONENT_IS_LDT_DUMMY(c)) {
		cf_crash(AS_MIGRATE, "Invalid Component Type Dummy received by migration");
	} else {
		if (mc->rxstate == AS_MIGRATE_RX_STATE_SUBRECORD) {
			mc->rxstate = AS_MIGRATE_RX_STATE_RECORD;
			cf_debug_digest(AS_MIGRATE, digest, "LDT_MIGRATION: Incoming version %ld Started Receiving Record Migration !! %s:%d:%d:%d",
				  mc->incoming_ldt_version, mc->rsv.ns->name, mc->rsv.p->partition_id, 
			  	mc->rsv.p->vp->elements, mc->rsv.p->sub_vp->elements);
		}
	}
	cf_detail_digest(AS_MIGRATE, digest, "LDT_MIGRATION: Incoming %s version=%ld flag=%d subrec=%d state=%d",
			  (info & MIG_INFO_LDT_SUBREC) ? "MIG_INFO_LDT_SUBREC"
			  : ((info & MIG_INFO_LDT_ESR) ? "MIG_INFO_LDT_ESR"
				 : "MIG_INFO_LDT_REC"), c->version, c->flag, COMPONENT_IS_LDT_SUB(c), mc->rxstate);
	return 0;
}

// Since this is used as the destructor function for the rchash,
// it has to be typed as a void *

void
migrate_migrate_destroy(void *parm)
{

	migration *mig = (migration *) parm;
	if (mig->start_m)		as_fabric_msg_put(mig->start_m);
	if (mig->done_m)		as_fabric_msg_put(mig->done_m);
	if (mig->pickled_array)	{
		for (uint i = 0; i < mig->pickled_size; i++) {
			if (mig->pickled_array[i].record_buf) {
				cf_free(mig->pickled_array[i].record_buf);
			}
			if  (mig->pickled_array[i].rec_props.p_data) {
				cf_free(mig->pickled_array[i].rec_props.p_data);
			}
		}
		cf_free(mig->pickled_array);
	}
	if (mig->retransmit_hash) shash_destroy(mig->retransmit_hash);
	if (mig->xmit_control_q) cf_queue_destroy(mig->xmit_control_q);
	if (mig->rsv.p) {
		cf_debug(AS_MIGRATE, "{%s:%d}migrate_migrate_destroy: END MIG %p", mig->rsv.ns->name, mig->rsv.p->partition_id, mig);
		as_partition_release(&mig->rsv);
		cf_atomic_int_decr(&g_config.migtx_tree_count);
	}

	cf_atomic_int_decr(&g_config.migrate_tx_object_count);
	memset(mig, 0xff, sizeof(migration) );
}

void
migrate_migrate_release(migration *mig)
{
	if (0 == cf_rc_release(mig)) {
		migrate_migrate_destroy(mig);
		cf_rc_free(mig);
	}
//	else
//		cf_detail(AS_MIGRATE, "Migration tx object released but not destroyed %p", mig);
}

// when we realize the migration is done, then
// call this function. It sends a DONE message.s

int
migrate_done_send(migration *mig, bool cancel_migrate)
{

	cf_debug(AS_MIGRATE, "Migration complete, may send done: {%s:%d} id %d", mig->rsv.ns->name, mig->rsv.pid, mig->id);

	if (mig->done_m == 0) {

		msg *done_m = as_fabric_msg_get(M_TYPE_MIGRATE);
		if (done_m == 0)	return(-1);
		if (cancel_migrate)
			msg_set_uint32(done_m, MIG_FIELD_OP, OPERATION_CANCEL);
		else
			msg_set_uint32(done_m, MIG_FIELD_OP, OPERATION_DONE);
		msg_set_uint32(done_m, MIG_FIELD_MIG_ID, mig->id);
		msg_set_buf(done_m, MIG_FIELD_NAMESPACE, (byte *) mig->rsv.ns->name, strlen(mig->rsv.ns->name), MSG_SET_COPY);
		msg_set_uint32(done_m, MIG_FIELD_PARTITION, mig->rsv.pid);
		mig->done_m = done_m;

		for (uint i = 0; i < mig->dst_nodes_sz; i++)
			mig->done_done[i] = false;

		mig->done_xmit_ms = 0;
	}

	// Check to see that it's a good time to retransmit
	uint64_t now = cf_getms();

	if (mig->done_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now ) {

		mig->done_xmit_ms = cf_getms();

		for (uint i = 0; i < mig->dst_nodes_sz; i++) {

			if (mig->done_done[i] == false) {

				cf_debug(AS_MIGRATE, "Migration complete, actualy sending done: {%s:%d} id %d", mig->rsv.ns->name, mig->rsv.pid, mig->id);

				cf_rc_reserve(mig->done_m);
				int rv = as_fabric_send(mig->dst_nodes[i], mig->done_m, AS_FABRIC_PRIORITY_MEDIUM);
				if (rv == 0) {
					cf_atomic_int_incr(&g_config.migrate_msgs_sent);
				}
				else {
					as_fabric_msg_put(mig->done_m);
					// important, very important, that we backsignal the node going away here
					// because done is special: dones don't get cleared out on cluster key change
					// !!!
					if (rv == AS_FABRIC_ERR_NO_NODE)
						return(-1);
					cf_debug(AS_MIGRATE, "Migration complete, done send failed {%s:%d} id %d rv %d", mig->rsv.ns->name, mig->rsv.pid, mig->id, rv);
				}
			}
		}

		mig->done_xmit_ms = now;
	}

	return(0);
}

//
// Processing the ack is fairly simple. Grab the migration id to get the migration pointer
// then use the transaction id to get the retransmit structure
//. use the node to figure out if this is the last transmit
//

void
migrate_process_ack(cf_node src, msg *m)
{
	uint32_t	mig_id;
	if (0 != msg_get_uint32(m, MIG_FIELD_MIG_ID, &mig_id)) {
		cf_debug(AS_MIGRATE, " could not pull migration id from ack, weird");
		return;
	}

	migration *mig;
	if (0 != rchash_get(g_migrate_hash, (void *) &mig_id, sizeof(mig_id), (void **) &mig)) {
		cf_debug(AS_MIGRATE, "got ack with no migrate, ignoring, mig_id %d", mig_id);
		return;
	}

	uint32_t	tid = 0xFFFFFFFF;
	msg_get_uint32(m, MIG_FIELD_TID, &tid);
	migrate_retransmit *rt = 0;
	pthread_mutex_t *vlock;
	if (SHASH_OK == shash_get_vlock(mig->retransmit_hash, &tid, (void **) &rt, &vlock))
	{
		// find done field and mark it
		for (uint i = 0; i < mig->dst_nodes_sz; i++) {
			if (src == mig->dst_nodes[i]) {
				rt->done[i] = true;
			}
		}

		// See if all done
		bool done = true;
		for (uint i = 0; i < mig->dst_nodes_sz; i++) {
			if (rt->done[i] == false) {
				done = false;
				break;
			}
		}

		// if done, delete from hash table, but lockfree since we have the lock
		if (done == true) {
			as_fabric_msg_put(rt->m);
			// at this point, the rt is *GONE*
			shash_delete_lockfree(mig->retransmit_hash, &tid);
			rt = 0;
		}
		else {
			cf_debug(AS_MIGRATE, "transactions awaiting further acks");
		}

		pthread_mutex_unlock(vlock);
	}

	migrate_migrate_release(mig);

}

//
// The reliable-send will stamp a migration id and transaction id into the
// message, and will return the transaction id to the caller.
// This allows the caller to keep track of the greatest migration id used
// to determine when a set of sends is correct, and allows the ack receive
// system to find the correct migration table, without actually regaining the
// migration table lock
//


int
migrate_send_reliable(migration *mig, msg *m)
{
#ifdef EXTRA_CHECKS
	if (cf_rc_count(m) <= 0) {
		cf_debug(AS_MIGRATE, "send reliable: given bad message, no ref count %p", m);
		return(-1);
	}
#endif


	// Allocate a transaction id and insert it in the message

	msg_set_uint32(m, MIG_FIELD_MIG_ID, mig->id);
	uint32_t tid = cf_atomic32_incr(&g_migrate_trans_id);
	msg_set_uint32(m, MIG_FIELD_TID, tid);

	// insert into the hash table by tid
	// NB: it is CRITICAL to insert before sending, as the ack might come
	// before the send completes!!! (yeah, we're that fast)
	// cf_debug(AS_MIGRATE, "send reliable put mig %p hash %p", mig, mig->trans_hash);
	migrate_retransmit rt;
	msg_incr_ref(m); // the reference in the hash
	rt.m = m;
	rt.mig = mig;
	rt.xmit_ms = cf_getms();
	for (uint i = 0; i < mig->dst_nodes_sz; i++)
		rt.done[i] = false;

	if (SHASH_OK != shash_put(mig->retransmit_hash, &tid, &rt) ) {
		cf_debug(AS_MIGRATE, "send reliable put failed *SERIOUS!*");
		as_fabric_msg_put(m);
		return(-1);
	}

	// send!
	int rv;
	do {
		if (mig->dst_nodes_sz == 1)
			rv = as_fabric_send(mig->dst_nodes[0], m, AS_FABRIC_PRIORITY_LOW);
		else
			rv = as_fabric_send_list(mig->dst_nodes, mig->dst_nodes_sz, m, AS_FABRIC_PRIORITY_LOW);

		if (rv == 0) {
			cf_atomic_int_incr(&g_config.migrate_msgs_sent);
			cf_atomic_int_incr(&g_config.migrate_inserts_sent);
		}
		else if (rv == AS_FABRIC_ERR_QUEUE_FULL) {
			usleep(1000 * 10);
		}
		else {
			cf_debug(AS_MIGRATE, "can't send can't insert: error %d", rv);
			as_fabric_msg_put(m); // if the send failed, decr the ref count the send would have taken
			return(rv);
		}

	} while (rv != 0);


	return(0);

}

//
// Mark and sweep
// In order to "migrate deletes", if the migrate is a replace-migrate,
// mark all the elements currently in the tree, and sweep when the migrate is
// done and remove any elements that no longer exist.
//
// This is done on the RX side.

void
migrate_delete( migrate_recv_control *mc, cf_digest *keyd )
{
	// This is necessary to get the size of the record before deleting
	as_index_ref r_ref;
	r_ref.skip_lock = false;
	int rv = as_record_get(mc->rsv.tree, keyd, &r_ref, mc->rsv.ns);
	if (rv == 0) {
		as_record *r = r_ref.r;
		as_namespace *ns = mc->rsv.ns;

		// bookkeeping: life is smaller
		if (ns->storage_data_in_memory) {
			as_storage_rd rd;
			rd.ns = mc->rsv.ns;
			rd.n_bins = as_bin_get_n_bins(r, &rd);
			rd.bins = as_bin_get_all(r, &rd, 0);

			cf_atomic_int_sub(&mc->rsv.p->n_bytes_memory, as_storage_record_get_n_bytes_memory(&rd));
		}

		// remove from the tree
		as_index_delete(mc->rsv.tree, keyd);

		// release the record, which will call the record destructor
		as_record_done(&r_ref, mc->rsv.ns);

		cf_detail(AS_MIGRATE, " migrate rx deleted key: %"PRIx64, *(uint64_t *)keyd);

	}
	else {
		cf_info(AS_MIGRATE, " migrate rx COULD NOT delete key: %"PRIx64, *(uint64_t *)keyd);
	}
}

//
// MARK what was there first
//

void
migrate_mark_reduce_fn(as_index *r, void *udata)
{
	if (r == 0)	return;

	r->migrate_mark = 1;
	return;

}


void
migrate_mark( migrate_recv_control *mc )
{
	as_index_reduce_sync( mc->rsv.tree, migrate_mark_reduce_fn, 0);
}

//
//

typedef struct {
	int alloc_sz;
	int n_keys;
	cf_digest keyd[];
} migrate_sweep_keys;



void
migrate_sweep_reduce_fn(as_index *r, void *udata)
{
	if (r == 0)	return;

	migrate_sweep_keys **sweep_keys_p = (migrate_sweep_keys **)udata;
	migrate_sweep_keys *sweep_keys = *sweep_keys_p;

	if (r->migrate_mark == 1) {

		if (sweep_keys == 0) {
			*sweep_keys_p = (migrate_sweep_keys *) cf_malloc(sizeof(migrate_sweep_keys) + (1000 * sizeof(cf_digest)));
			sweep_keys = *sweep_keys_p;
			sweep_keys->alloc_sz = 1000;
			sweep_keys->n_keys = 0;
		}
		if (sweep_keys->alloc_sz == sweep_keys->n_keys) {
			sweep_keys->alloc_sz += 1000;
			*sweep_keys_p = cf_realloc(sweep_keys, sizeof(migrate_sweep_keys) + (sweep_keys->alloc_sz * sizeof(cf_digest) ) );
			sweep_keys = *sweep_keys_p;
		}

//		cf_debug(AS_MIGRATE, " migrate: sweep: found migrate mark 1 %"PRIx64,*(uint64_t *) key );

		memcpy( &sweep_keys->keyd[ sweep_keys->n_keys ], &r->key, sizeof(cf_digest));
		sweep_keys->n_keys ++;

	}
}




void
migrate_sweep( migrate_recv_control *mc )
{
	migrate_sweep_keys *sweep_keys = 0;

	// reduce to find list of elements that need deleting
	as_index_reduce_sync( mc->rsv.tree, migrate_sweep_reduce_fn, &sweep_keys );

	if (sweep_keys) {
		cf_info(AS_MIGRATE, " migrate_rx: sweep detected %d keys to delete", sweep_keys->n_keys);
		for (int i = 0 ; i < sweep_keys->n_keys; i++) {
//			cf_debug(AS_MIGRATE, " migrate: sweep: delete %"PRIx64,*(uint64_t *) &sweep_keys->keyd[i] );
			migrate_delete(mc, &sweep_keys->keyd[i]);
		}
		cf_free(sweep_keys);
	}
}




#ifdef USE_NETWORK_POLICY

void
migrate_start_network_policy(cf_node node)
{
	// tell the fabric to go into high-throughput mode
	uint32_t val = 500;
	as_fabric_set_node_parameter(node, AS_FABRIC_PARAMETER_GATHER_USEC, &val);
	val = 2000;
	as_fabric_set_node_parameter(node, AS_FABRIC_PARAMETER_MSG_SIZE, &val);
}


typedef struct {
	uint	n_in_progress;
	cf_node node;
} migrate_network_policy_reduce;

// this code is a little tricky - it is only looking for the first time there's a migrate in progress,
// so it can return as soon as it finds the right node.

int
migrate_done_network_policy_rch_reduce(void *key, uint32_t key_len, void *object, void *udata)
{
	migrate_network_policy_reduce *mnpr = (migrate_network_policy_reduce *) udata;
	migrate_recv_control_index *mc_i = (migrate_recv_control_index *) key;
	// migrate_recv_control *mc = (migrate_recv_control *) object;
	if (mc_i->source_node == mnpr->node) {
		mnpr->n_in_progress++;
		return(-1);
	}
	return(0);
}

int
migrate_done_network_policy_mh_reduce(void *key, uint32_t key_len, void *object, void *udata)
{
	migrate_network_policy_reduce *mnpr = (migrate_network_policy_reduce *) udata;
	migration *mig = (migration *) object;
	// migrate_recv_control *mc = (migrate_recv_control *) object;
	for (uint i = 0; i < mig->dst_nodes_sz; i++) {
		if (mig->dst_nodes[i] == mnpr->node) {
			mnpr->n_in_progress++;
			return(-1);
		}
	}
	return(0);
}


void
migrate_done_network_policy(cf_node node)
{
	// short circut
	if ((rchash_get_size(g_migrate_recv_control_hash) == 0) &&
			(rchash_get_size(g_migrate_hash) == 0) ) {
		// No more migrates,
//		cf_debug(AS_MIGRATE, "migrate_done: set fabric to low-latency %"PRIx64,node);
		as_fabric_set_node_parameter(node, AS_FABRIC_PARAMETER_GATHER_USEC, 0);
		return;
	}

	migrate_network_policy_reduce mnpr;
	mnpr.n_in_progress = 0;
	mnpr.node = node;
	rchash_reduce(g_migrate_recv_control_hash, migrate_done_network_policy_rch_reduce, &mnpr);

	if (mnpr.n_in_progress == 0)
		rchash_reduce(g_migrate_hash, migrate_done_network_policy_mh_reduce, &mnpr);

	if (mnpr.n_in_progress == 0) {
//		cf_debug(AS_MIGRATE, "migrate_done: set fabric to low-latency %"PRIx64,node);
		as_fabric_set_node_parameter(node, AS_FABRIC_PARAMETER_GATHER_USEC, 0);
	}
	else {
		// migrates still in progress - reset to be sure
//		cf_debug(AS_MIGRATE, "migrate_done: others still going, reset to high-throughput %"PRIx64,node);
		migrate_start_network_policy(node);
	}
	return;

}

#endif // USE_NETWORK_POLICY

// This thread will poll the transaction hash and retransmit anything old
// (also need to get


int
migrate_retransmit_reduce_fn(void *key, void *data, void *udata)
{
	migrate_retransmit *rt = data;
	uint64_t now = *(uint64_t *) udata;


	if (rt->xmit_ms + MIGRATE_RETRANSMIT_MS < now) {

		cf_debug(AS_MIGRATE, "migrate_retransmit: mig %d sending: now %"PRIu64" xmit_ms %"PRIu64 , rt->mig->id, now, rt->xmit_ms);

		cf_node nodes[AS_CLUSTER_SZ];
		uint nodes_sz = 0;
		for (uint i = 0; i < rt->mig->dst_nodes_sz; i++) {
			if (rt->done[i] == false) {
				nodes[nodes_sz] = rt->mig->dst_nodes[i];
				cf_debug(AS_MIGRATE, "migrate_retransmit: mig %d sending: dstnode %"PRIx64" msg %p", rt->mig->id, nodes[nodes_sz], rt->m);
				nodes_sz++;
			}
		}
		int rv = 0;
		msg_incr_ref(rt->m);
		if (nodes_sz == 1) {
			rv = as_fabric_send(nodes[0], rt->m, AS_FABRIC_PRIORITY_LOW);
		}
		else if (nodes_sz > 1) {
			rv = as_fabric_send_list(nodes, nodes_sz, rt->m, AS_FABRIC_PRIORITY_LOW);
		}
		else {
			// this is serious!
			cf_info(AS_MIGRATE, "warning- migrate retransmit reduce found nothing to send for mig %d, means a rouge", rt->mig->id);
			as_fabric_msg_put(rt->m);
			return(-1);
		}

		if (0 != rv) {
			as_fabric_msg_put(rt->m);
			return(rv); // this will bail out the reduce
		}
		else {
			cf_atomic_int_incr(&g_config.migrate_msgs_sent);
			cf_atomic_int_incr(&g_config.migrate_inserts_sent);
			rt->xmit_ms = now;
		}
	}
	return(0);
}

/*
 * Called at the migration sender for all the cases ... success fail
 * cb is reset after first call. cb setting and migrate_progress_send
 * counter go hand in hand
 */
void
migrate_send_finish(migration *mig, as_migrate_state state, char *reason)
{
	cf_debug(AS_MIGRATE, "migrate xmit failed, mig %d, %s: {%s:%d}", mig->id, reason, mig->rsv.ns->name, mig->rsv.pid);

	// TODO: What if migration is pushed to multiple nodes .. passes only first one.
	// currently only sent to one node in a migration record
	if (mig->cb) {
		(mig->cb) ( state, mig->rsv.ns, mig->rsv.pid, mig->rsv.tree, mig->dst_nodes[0], mig->udata);
		mig->cb = NULL;
		cf_atomic_int_decr( &g_config.migrate_progress_send );
	}
	return;
}

int
migrate_debug_reduce_fn(void *key, uint32_t len, void *object, void *udata)
{
//	migrate_recv_control *mc = (migrate_recv_control *)object;
	migrate_recv_control_index *mc_i = (migrate_recv_control_index *)key;

	fprintf(stderr, "recv control remain: %"PRIx64" id %d\n", mc_i->source_node, mc_i->mig_id);

	return(0);
}


int
migrate_msg_fn(cf_node id, msg *m, void *udata)
{
	cf_detail(AS_MIGRATE, "receved migrate message");

#ifdef DEBUG_MSG
	msg_dump(m);
#endif

#ifdef EXTRA_CHECKS
	if (cf_rc_count(m) <= 0) {
		cf_debug(AS_MIGRATE, "migrate msg function recieved bad ref count %p *** FAIL FAIL FAIL ***", m);
		return(0);
	}
#endif

	cf_atomic_int_incr(&g_config.migrate_msgs_rcvd);

	bool cancel_migrate = false;

	uint32_t op = 99999;
	msg_get_uint32(m, MIG_FIELD_OP, &op);

	switch (op) {
		case OPERATION_INSERT:
		{

			cf_atomic_int_incr(&g_config.migrate_inserts_rcvd);

			uint32_t i_tid;
			msg_get_uint32(m, MIG_FIELD_TID, &i_tid);

			cf_digest *key;
			size_t sz = 0;
			msg_get_buf(m, MIG_FIELD_DIGEST, (byte **) &key, &sz, MSG_GET_DIRECT);

			uint32_t mig_id;
			msg_get_uint32(m, MIG_FIELD_MIG_ID, &mig_id);

			cf_detail(AS_MIGRATE, " mig %d recved insert tid %d", mig_id, i_tid);

			// get the migrate_recv_control, it has my tree and such
			migrate_recv_control_index mc_i;
			mc_i.source_node = id;
			mc_i.mig_id = mig_id;

			migrate_recv_control *mc;
			if (RCHASH_OK == rchash_get(g_migrate_recv_control_hash, &mc_i, sizeof(mc_i), (void **) &mc))
			{

				if (mc->cluster_key != as_paxos_get_cluster_key()) {
					cf_info(AS_MIGRATE, "migration insert: cluster key mismatch can't insert %"PRIx64, *(uint64_t *)key);
					as_migrate_print2_cluster_key("INSERTION FAIL", mc->cluster_key);
					migrate_recv_control_release(mc);
					goto Done;
				}

				uint32_t generation = 0;
				if (0 != msg_get_uint32(m, MIG_FIELD_GENERATION, &generation)) {
					cf_warning(AS_MIGRATE, "received key with no generation, setting to 1 as a default %"PRIx64, *(uint64_t *)key);
					generation = 1;
				}
				uint32_t void_time = 0;
				if (0 != msg_get_uint32(m, MIG_FIELD_VOID_TIME, &void_time)) {
					cf_debug(AS_MIGRATE, "received key with no ttl, setting to 0 as a default ");
				}

				// Deal with incomplete vinfo
				as_partition_vinfoset vinfoset;

				/* extract the vinfoset */
				uint8_t *vinfo_buf = 0;
				size_t vinfo_buf_len = 0;
				if (0 != msg_get_buf(m, MIG_FIELD_VINFOSET, &vinfo_buf, &vinfo_buf_len, MSG_GET_DIRECT)) {
					cf_debug(AS_MIGRATE, "migrate: no vinfoset");
					memset(&vinfoset, 0, sizeof(vinfoset));
					// migrate_recv_control_release(mc);
					// goto Done;
				}
				else if (0 != as_partition_vinfoset_unpickle( &vinfoset, vinfo_buf, vinfo_buf_len, "MIG")) {
					cf_info(AS_MIGRATE, "migrate: could not unpickle vinfoset");
					memset(&vinfoset, 0, sizeof(vinfoset));
					// migrate_recv_control_release(mc);
					// goto Done;
				}

//				as_partition_vinfoset_dump(vinfoset, "migrate INSERT");

				void *value = NULL;
				as_rec_props rec_props;
				as_rec_props_clear(&rec_props);
				size_t value_sz = 0;
				msg_get_buf(m, MIG_FIELD_RECORD, (byte **) &value, &value_sz, MSG_GET_DIRECT);
				msg_get_buf(m, MIG_FIELD_REC_PROPS, &rec_props.p_data, (size_t*)&rec_props.size, MSG_GET_DIRECT);

				as_record_merge_component c;
				c.vinfoset      = vinfoset;  // kind of sucks. We should have copied directly into here
				c.record_buf    = value;
				c.record_buf_sz = value_sz;
				c.generation    = generation;
				c.void_time     = void_time;
				c.rec_props     = rec_props;

				if (as_ldt_get_migrate_info(mc, &c, m, key)) {
					cf_debug_digest(AS_MIGRATE, &key, "LDT_MIGRATE: sub record received Out Of Order");
					migrate_recv_control_release(mc);
					goto Done;
				}

				// Default it to local node winner
				int winner_idx  = -1;
				if (mc->mig_type == AS_MIGRATE_TYPE_OVERWRITE) {
					// cf_info(AS_MIGRATE, "migrate rx: merge replace %"PRIx64" gen %d",*(uint64_t*)key,generation);
					// NB: Blind replace is disabled, it is always flatten
					// if (0 != as_record_replace(&mc->rsv, key, 1, &c, &winner_idx)) {
					if (0 != as_record_flatten(&mc->rsv, key, 1, &c, &winner_idx)) {
						cf_warning(AS_MIGRATE, "migrate: record replace failed %"PRIx64, *(uint64_t *)key);
						migrate_recv_control_release(mc);
						goto Done;
					}
				}
				else {
					if (mc->rsv.ns->allow_versions) {
						// cf_info(AS_MIGRATE, "migrate rx: merge insert %"PRIx64" gen %d",*(uint64_t*)key,generation);
						if (0 != as_record_merge(&mc->rsv, key, 1, &c)) {
							cf_warning(AS_MIGRATE, "migrate: record migrate failed %"PRIx64, *(uint64_t *)key);
							migrate_recv_control_release(mc);
							goto Done;
						}
					}
					else {
						// cf_info(AS_MIGRATE, "migrate rx: flatten insert %"PRIx64" gen %d",*(uint64_t*)key,generation);
						int rv = as_record_flatten(&mc->rsv, key, 1, &c, &winner_idx);
						if (rv) {
							if (rv != -3) {
								// -3 is not a failure. It is get_create failure inside as_record_flatten which is 
								// possible in case of race.
								cf_warning_digest(AS_MIGRATE, key, "migrate: record flatten failed %d", rv);
								migrate_recv_control_release(mc);
								goto Done;
							} 
						}
					}
				}

				migrate_recv_control_release(mc);

			}

			// ack it
			// TODO: Potential optimization. ...  In case the first LDT
			// subrecord migrate fails indicate it to the remote to make sure
			// further shipping is aborted... because it anyways is not going
			// to win :D
			msg_set_unset(m, MIG_FIELD_INFO);
			msg_set_unset(m, MIG_FIELD_RECORD);
			msg_set_unset(m, MIG_FIELD_DIGEST);
			msg_set_unset(m, MIG_FIELD_NAMESPACE);
			msg_set_unset(m, MIG_FIELD_GENERATION);
			msg_set_unset(m, MIG_FIELD_VOID_TIME);
			msg_set_uint32(m, MIG_FIELD_OP, OPERATION_ACK);
			msg_set_unset(m, MIG_FIELD_REC_PROPS);
			if (0 != as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH)) {
				cf_info(AS_MIGRATE, "insert-ack-send-failed!!!!");
			}
			else {
				m = 0;
				cf_atomic_int_incr(&g_config.migrate_acks_sent);
				cf_atomic_int_incr(&g_config.migrate_msgs_sent);
			}

		}
		break;

		case OPERATION_ACK:

			cf_atomic_int_incr(&g_config.migrate_acks_rcvd);

			migrate_process_ack(id, m);

			break;

		case OPERATION_START:
		{

			// fetch migration id
			uint32_t mig_id = 0;
			if (0 != msg_get_uint32(m, MIG_FIELD_MIG_ID, &mig_id)) {
				goto Done;
			}

			// lookup in migration hash - see if this migration already exists & has been notified

			migrate_recv_control *mc = cf_rc_alloc(sizeof(migrate_recv_control));
			cf_assert(mc, AS_MIGRATE, CF_CRITICAL, "malloc" );
			cf_atomic_int_incr(&g_config.migrate_rx_object_count);
			mc->done_recv = 0;
			mc->done_recv_ms = 0;
			mc->incoming_ldt_version = 0;
			// Record the time fo the first START received.
			mc->start_recv_ms = cf_getms();
			mc->source_node = id;
			AS_PARTITION_RESERVATION_INIT(mc->rsv);

			/*
			 * Extract the cluster key
			 * If the key does not match the local one, then refuse migrate
			 */
			int e = 0;
			e += msg_get_uint64(m, MIG_FIELD_CLUSTER_KEY, &mc->cluster_key);
			if (0 > e) {
				cf_debug(AS_MIGRATE, "migrate start: msg get for cluster key failed, mig %d");
				migrate_recv_control_release(mc);
				goto Done;
			}

			if (mc->cluster_key != as_paxos_get_cluster_key()) {
				cf_debug(AS_MIGRATE, "migrate start: cluster key mismatch can't start, mig %d", mig_id);
				as_migrate_print2_cluster_key("START_ACK_FAIL", mc->cluster_key);
				migrate_recv_control_release(mc);
				msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_FAIL);
				if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH)) {
					m = 0;
				}
				goto Done;
			}

			/* and now some namespace */
			uint8_t *ns_name = 0;
			size_t   ns_name_len;
			if (0 != msg_get_buf(m, MIG_FIELD_NAMESPACE, &ns_name, &ns_name_len, MSG_GET_DIRECT)) {
				migrate_recv_control_release(mc);
				goto Done;
			}
			as_namespace *ns = as_namespace_get_bybuf(ns_name, ns_name_len);
			if ( ! ns ) {
				cf_debug(AS_MIGRATE, "migrate start: bad namespace, can't start mig %d", mig_id);
				migrate_recv_control_release(mc);
				goto Done;
			}

			uint32_t part_id = 0xfffffff;
			msg_get_uint32(m, MIG_FIELD_PARTITION, &part_id);

			uint32_t mig_type = 0; // default: overwrite
			msg_get_uint32(m, MIG_FIELD_TYPE, &mig_type);
			mc->mig_type = AS_MIGRATE_TYPE_MERGE;
			//if (mig_type == 0)
			//	mc->mig_type = AS_MIGRATE_TYPE_MERGE;
			//else if (mig_type == 1)
			//	mc->mig_type = AS_MIGRATE_TYPE_OVERWRITE;

			cf_debug(AS_MIGRATE, "{%s:%d} MIGRATE RECV: partition iid %"PRIx64"", ns->name, part_id, mc->cluster_key);
			// check with the underlying system whether it's OK to migrate at the moment
			//
			as_migrate_cb_return rv = AS_MIGRATE_CB_OK;
			if (g_event_cb) {
				rv = (*g_event_cb) (AS_MIGRATE_STATE_START, ns, part_id, 0, mc->source_node, g_udata);
			}
			switch(rv) {
				case AS_MIGRATE_CB_FAIL:
					// Send 'fail' ack
					cf_debug(AS_MIGRATE, "recv: partition refused migrate: mig %d {%s:%d}", mig_id, ns->name, part_id);
					migrate_recv_control_release(mc);
					msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_FAIL);
					if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH)) m = 0;
					goto Done;

				case AS_MIGRATE_CB_AGAIN:
					cf_debug(AS_MIGRATE, "recv: partition says not now, waiting for retry: mig %d {%s:%d}", mig_id, ns->name, part_id);
					migrate_recv_control_release(mc);
					msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_EAGAIN);
					if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH)) m = 0;
					goto Done;

				case AS_MIGRATE_CB_ALREADY_DONE:
					cf_debug(AS_MIGRATE, "recv: partition says already done: mig %d {%s:%d}", mig_id, ns->name, part_id);
					migrate_recv_control_release(mc);
					msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_ALREADY_DONE);
					if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH)) m = 0;
					goto Done;

				case AS_MIGRATE_CB_OK:
					break;
			}

			as_partition_reserve_migrate(ns, part_id, &mc->rsv, 0);
			cf_atomic_int_incr(&g_config.migrx_tree_count);
			ns = 0; // ns lock subsumed into the migrate reservation

#ifdef USE_MARK_SWEEP
			if (mc->mig_type == AS_MIGRATE_TYPE_OVERWRITE) {
				migrate_mark( mc );
			}
#endif


			if ((mc->rsv.p == 0) || (mc->rsv.ns == 0) ) {
				cf_warning(AS_MIGRATE, "migrate recv start: receiving bad reservation: p %p ns %p", mc->rsv.p, mc->rsv.ns);
				mc->rsv.p = 0;
				migrate_recv_control_release(mc);
				msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_FAIL);
				if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH))     m = 0;
				goto Done;
			}

			if (mc->cluster_key != mc->rsv.cluster_key) {
				cf_debug(AS_MIGRATE, "migrate start: cluster key mismatch in target partition can't start");
				as_migrate_print2_cluster_key("START_ACK_FAIL", mc->cluster_key);
				migrate_recv_control_release(mc);
				msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_FAIL);
				if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_HIGH))     m = 0;
				goto Done;
			}

			migrate_recv_control_index mc_i;
			mc_i.source_node = id;
			mc_i.mig_id = mig_id;

			// This node is going to accept migration. When migration starts
			// it is subrecord migration
			mc->rxstate = AS_MIGRATE_RX_STATE_SUBRECORD;
			msg_get_uint64(m, MIG_FIELD_VERSION, &mc->incoming_ldt_version);
			mc->pid     = mc->rsv.p->partition_id;


			if (RCHASH_OK == rchash_put_unique(g_migrate_recv_control_hash, &mc_i, sizeof(mc_i), mc)) {

				cf_debug(AS_MIGRATE, "{%s:%d} recv migrate start msg: src %"PRIx64" id %d m %p: recv control size %d", mc->rsv.ns->name, mc->rsv.pid, id, mig_id, m, rchash_get_size(g_migrate_recv_control_hash) );

				cf_atomic_int_incr( &g_config.migrate_progress_recv );

				migrate_recv_ldt_version mc_l;
				mc_l.incoming_ldt_version = mc->incoming_ldt_version;
				mc_l.pid                  = mc->pid;

				shash_put(g_migrate_incoming_ldt_version_hash, &mc_l, &mc); 
				cf_detail(AS_LDT, "LDT_MIGRATION: Incoming Version %ld, Started Receiving SubRecord Migration !! %s:%d:%d:%d",
						  mc->incoming_ldt_version,
						  mc->rsv.ns->name, mc->rsv.p->partition_id, mc->rsv.p->vp->elements, 
						  mc->rsv.p->sub_vp->elements);

				// If I'm receiving migrates, then I should be batching messages
//				cf_debug(AS_MIGRATE, "migrate_start: set fabric to high-throughtput %"PRIx64,id);

#ifdef USE_NETWORK_POLICY
				migrate_start_network_policy(id);
#endif

			}
			else {
				// can't insert, means we're already started, so free what we were trying to put in
				// and send an ack to let'em know we care

				cf_debug(AS_MIGRATE, "recv migrate start msg: duplicate, ignoring: {%s:%d} from node %"PRIx64, ns->name, part_id, id);

				migrate_recv_control_release(mc);
			}

			// perhaps already seen, but another ack doesn't hurt, and hey, I've got this message just sitting here
			msg_set_uint32(m, MIG_FIELD_OP, OPERATION_START_ACK_OK);
			if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_MEDIUM))  m = 0;

		}
		break;

		case OPERATION_START_ACK_OK:
		case OPERATION_START_ACK_EAGAIN:
		case OPERATION_START_ACK_FAIL:
		case OPERATION_START_ACK_ALREADY_DONE:
		case OPERATION_DONE_ACK:
		{

			uint32_t mig_id;
			if (0 != msg_get_uint32(m, MIG_FIELD_MIG_ID, &mig_id)) {
				cf_debug(AS_MIGRATE, " could not pull miration id from start/done ack");
				goto Done;
			}

			cf_detail(AS_MIGRATE, "received start/done ack mig_id %d op %d", mig_id, op);

			migration *mig;
			if (0 == rchash_get(g_migrate_hash, (void *) &mig_id, sizeof(mig_id), (void **) &mig)) {
				// Pop this on the queue for the migrate thread
				migrate_xmit_control mig_c;
				mig_c.mig_id = mig_id;
				mig_c.op = op;

				// lookup the id in the mig table to see which node acked me
				// and for that node, send the control message
				for (uint i = 0; i < mig->dst_nodes_sz; i++) {
					if (mig->dst_nodes[i] == id) {
						mig_c.node_offset = i;
						cf_queue_push(mig->xmit_control_q, &mig_c);
						break;
					}
				}

				migrate_migrate_release(mig);
			}
			else {
				cf_debug(AS_MIGRATE, "got start/done ack with no migrate, ignoring, mig_id %d op %d", mig_id, op);
			}

		}
		break;

		case OPERATION_CANCEL: // cancel migrate and cleanup
			cancel_migrate = true;
		case OPERATION_DONE:
		{

			// fetch migration id
			uint32_t mig_id;
			if (0 != msg_get_uint32(m, MIG_FIELD_MIG_ID, &mig_id)) {
				cf_warning(AS_MIGRATE, "FAILED: No Migration ID for recv migrate %s msg: from node %"PRIx64, cancel_migrate ? "cancel" : "done", id);
				goto Done;
			}
			else {
				cf_detail(AS_MIGRATE, "recv migrate %s msg: mig %d node %"PRIx64, cancel_migrate ? "cancel" : "done", mig_id, id);
			}

			// see if this migration already exists & has been notified
			migrate_recv_control_index mc_i;
			mc_i.source_node = id;
			mc_i.mig_id = mig_id;

#if 0
			// This debug is interesting because if you have dangling migrates, this will print the set of migrates
			// at the very end
			cf_debug(AS_MIGRATE, "recv done msg: src %"PRIx64" id %d m %p: recv control size %d", id, mig_id, m, rchash_get_size(g_migrate_recv_control_hash) );
			if (rchash_get_size(g_migrate_recv_control_hash) < 5)
				rchash_reduce(g_migrate_recv_control_hash, migrate_debug_reduce_fn, 0);
#endif
			// thought - maybe it's better to use a 'get and delete' methodology?
			// except what if the send of the ack fails? maybe send the ack first
			// because you always need to?

			migrate_recv_control *mc;
			if (RCHASH_OK == rchash_get(g_migrate_recv_control_hash, &mc_i, sizeof(mc_i), (void **) &mc)) {
				smb_mb();
				uint32_t c = cf_atomic32_incr(&mc->done_recv);
				if (c == 1 ) {
					smb_mb();

					// Record the time of the first DONE received.
					mc->done_recv_ms = cf_getms();

#ifdef USE_MARK_SWEEP
					if (mc->mig_type == AS_MIGRATE_TYPE_OVERWRITE) {
						migrate_sweep(mc);
					}
#endif

					if (cf_atomic_int_get(g_config.migrate_progress_recv)) {
						cf_atomic_int_decr(&g_config.migrate_progress_recv);
					}

					// This node is done with receiving migration. reset rxstate
					cf_detail(AS_MIGRATE, "LDT_MIGRATION: Incoming Version %ld Finished Receiving Migration !! %s:%d:%d:%d",
							  mc->incoming_ldt_version, mc->rsv.ns->name, mc->rsv.p->partition_id, mc->rsv.p->vp->elements, mc->rsv.p->sub_vp->elements);

					if (g_event_cb) {
						cf_debug(AS_MIGRATE, "recv migrate %s msg: mig %d, {%s:%d} from node %"PRIx64, cancel_migrate ? "cancel" : "done", mig_id, mc->rsv.ns->name, mc->rsv.pid, id);

						// TODO: should the callback be called with cancel in order to cleanup state (e.g., journals, etc.)
						if (!cancel_migrate) {
							(*g_event_cb) (AS_MIGRATE_STATE_DONE, mc->rsv.ns, mc->rsv.pid, mc->rsv.tree, mc->source_node, g_udata);
						}
					}

					// If RX control expiration is not enabled, delete it now.
					if (0 >= g_config.migrate_rx_lifetime_ms) {
						rchash_delete(g_migrate_recv_control_hash, &mc_i, sizeof(mc_i));
					} else {
						// Otherwise, leave the existing recv control object in the hash table as a reminder that the migrate
						//  has already been done, and it will be reaped by the reaper thread after the expiration time.
						// [XXX -- Ideally would re-insert a placeholder object smaller than the current 1,936 bytes.]
					}

					// And we always need to release the extra ref. count now that we're done accessing the object.
					migrate_recv_control_release(mc);

				} else {
					cf_debug(AS_MIGRATE, "NOTE:  Received %d DONE msgs for mig rx %d from node %"PRIx64, cf_atomic32_get(mc->done_recv), mig_id, id);
				}

				// send back an ack
				msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE_ACK);
				if (0 == as_fabric_send(id, m, AS_FABRIC_PRIORITY_MEDIUM)) {
					m = 0;
					// at least we sent an ack, so wipe out the table
					cf_atomic_int_incr(&g_config.migrate_msgs_sent);
				}
			} else {
				msg_set_uint32(m, MIG_FIELD_OP, OPERATION_DONE_ACK);
				if (0 != as_fabric_send(id, m, AS_FABRIC_PRIORITY_MEDIUM)) {
					cf_debug(AS_MIGRATE, " migrate: received unknown done, tried to ack source, weird failure");
				}
				else {
					cf_debug(AS_MIGRATE, " migrate: received done message for unknown migrate, stupidly acking source %"PRIx64" mig %d", id, mig_id);
					m = 0;
				}
			}

#ifdef USE_NETWORK_POLICY
			// Check to see the correct network policy
			migrate_done_network_policy(id);
#endif

		}
		break;

		default:
			cf_debug(AS_MIGRATE, "received unknown message");
			break;
	}
Done:
	if (m) as_fabric_msg_put(m);

	return(0);
}

int
migrate_rx_reaper_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	migrate_recv_control_index *mrc_idx = key;
	migrate_recv_control *mci = object;

	if (mci->cluster_key != as_paxos_get_cluster_key() ) {
		cf_debug(AS_MIGRATE, "reaping migrate rx after a cluster key change: mci %p id %d node %"PRIx64,
				 mci, mrc_idx->mig_id, mrc_idx->source_node);

		if (cf_atomic_int_get(g_config.migrate_progress_recv)) {
			cf_atomic_int_decr(&g_config.migrate_progress_recv);
		}

		// This node is done with migration receive. reset rxstate
		cf_detail(AS_MIGRATE, "LDT_MIGRATION: Incoming Version %ld Finished Receiving Migration !! %s:%d:%d:%d",
				  mci->incoming_ldt_version, mci->rsv.ns->name, mci->rsv.p->partition_id, mci->rsv.p->vp->elements, mci->rsv.p->sub_vp->elements);

		return(RCHASH_REDUCE_DELETE);
	} else if ((g_config.migrate_rx_lifetime_ms > 0) && mci->done_recv && (cf_getms() > (mci->done_recv_ms + g_config.migrate_rx_lifetime_ms))) {
		cf_debug(AS_MIGRATE, "reaping expired done mig rx %d (done recv count %d ; start ms %d ; done ms %d) from node %"PRIx64,
				 mrc_idx->mig_id, cf_atomic32_get(mci->done_recv), mci->start_recv_ms, mci->done_recv_ms, mci->source_node);

		return(RCHASH_REDUCE_DELETE);
	}

	return(0);
}

//
// This simple function watches for cluster state changes. Any RX migrates
// that are in progress during the

void *
migrate_rx_reaper_fn(void *unused)
{
	do {

		rchash_reduce(g_migrate_recv_control_hash, migrate_rx_reaper_reduce_fn, 0);

		sleep(1);

	} while (1);

	return(0);
}

//
// This uses the asynchronous index reduce
// which means the tree size is a guide
// and you're guarenteed that the value exists, but not that it's particularly good
// it may have been tombstone-ized or something
//

void
migrate_tree_reduce(as_index_ref *r_ref, void *udata)
{
	if ((r_ref == 0) || (r_ref->r == 0)) {
		return;
	}

	migration *mig = (migration *) udata;

	if (mig->pickled_array == 0) {
		// find the size, and malloc the pickled_array
		// size is not guarenteed to be correct now, because this
		if (mig->txstate & AS_PARTITION_MIG_TX_STATE_SUBRECORD) {
			mig->pickled_alloc = mig->rsv.sub_tree->elements + 20;
		} else {
			mig->pickled_alloc = mig->rsv.tree->elements + 20;
		}
		mig->pickled_array = cf_malloc(mig->pickled_alloc * sizeof(pickled_record));
		cf_assert(mig->pickled_array, AS_MIGRATE, CF_CRITICAL, "malloc");
		mig->pickled_size = 0;
	}
	if (mig->pickled_size >= mig->pickled_alloc) {
		cf_info(AS_MIGRATE, "Tuning parameter: rcrb-reduce hit pickled array limit: id %d pid %d alloc %d size %d growing",
				mig->id, mig->rsv.pid, mig->pickled_alloc, mig->pickled_size);
		mig->pickled_alloc += 100;
		mig->pickled_array = cf_realloc(mig->pickled_array, mig->pickled_alloc * sizeof(pickled_record) );
		cf_assert(mig->pickled_array, AS_MIGRATE, CF_CRITICAL, "malloc");
	}

	pickled_record *pr = &mig->pickled_array[mig->pickled_size];
	mig->pickled_size++;
	pr->record_buf = NULL;

	as_index *r = r_ref->r;

	pr->vinfo_buf_len = sizeof(pr->vinfo_buf);
	if (0 != as_partition_vinfoset_mask_pickle(&mig->rsv.p->vinfoset, as_index_vinfo_mask_get(r, mig->rsv.ns->allow_versions), pr->vinfo_buf, &pr->vinfo_buf_len)) {
		// this only happens if the record we have is too small. Do the best we can: send a null pickled value
		pr->vinfo_buf_len = 4;
		pr->vinfo_buf[0] = pr->vinfo_buf[1] = pr->vinfo_buf[2] = pr->vinfo_buf[3] = 0;
		cf_warning(AS_MIGRATE, "migrate: could not pickle vinfoset, internal error");
	}
	if (pr->vinfo_buf[1] || pr->vinfo_buf[2] || pr->vinfo_buf[3]) {
		cf_info(AS_MIGRATE, "migrate-reduce-vinfo very flawed! len %d : %02x %02x %02x", pr->vinfo_buf_len, pr->vinfo_buf[1], pr->vinfo_buf[2] , pr->vinfo_buf[3]);
	}

	as_storage_rd rd;
	if (0 != as_storage_record_open(mig->rsv.ns, r, &rd, &r->key)) {
		cf_debug(AS_RECORD, "pickle: couldn't open record");
		mig->pickled_size--;
		as_record_done(r_ref, mig->rsv.ns);
		return;
	}

	rd.n_bins = as_bin_get_n_bins(r, &rd);
	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	if (0 != as_record_pickle(r, &rd, &pr->record_buf, &pr->record_len)) {
		cf_info(AS_MIGRATE, "migrate could not pickle");
		mig->pickled_size--;
		as_storage_record_close(r, &rd);
		as_record_done(r_ref, mig->rsv.ns);
		return;
	}

	pr->key = r->key;
	pr->generation = r->generation;
	pr->void_time  = r->void_time;

	as_storage_record_get_key(&rd);

	as_rec_props_clear(&pr->rec_props);
	as_rec_props rec_props;
	if (0 != as_storage_record_copy_rec_props(&rd, &rec_props)) {
		pr->rec_props = rec_props;
	}

	// Always succeeds ... does it ??
	as_ldt_fill_precord(pr, &rd, mig);

	as_storage_record_close(r, &rd);
	as_record_done(r_ref, mig->rsv.ns);

	cf_atomic_int_incr(&g_config.migrate_reads);

	mig->yield_count++;
	if (g_config.migrate_read_priority && (mig->yield_count % g_config.migrate_read_priority == 0)) {
		usleep(g_config.migrate_read_sleep);
	}
}


//
// This will reduce the tree, create all the messages, call send on each migrate message,
// and set up the retransmit structures
//
// Have to insert into the migration hash first, because responses will start flowing back
// as we're reducing
//
// Todo: start_send and done_send can/should be collapsed


int
migrate_start_send(migration *mig)
{

	if (mig->start_m == 0) {
		// cons up the start message
		msg *start_m = as_fabric_msg_get(M_TYPE_MIGRATE);
		if (!start_m)	return(-1);
		msg_set_uint32(start_m, MIG_FIELD_OP, OPERATION_START);
		msg_set_uint32(start_m, MIG_FIELD_MIG_ID, mig->id);
		msg_set_uint64(start_m, MIG_FIELD_CLUSTER_KEY, mig->cluster_key);
		msg_set_buf(start_m, MIG_FIELD_NAMESPACE, (byte *) mig->rsv.ns->name, strlen(mig->rsv.ns->name), MSG_SET_COPY);
		msg_set_uint32(start_m, MIG_FIELD_PARTITION, mig->rsv.pid);
		msg_set_uint32(start_m, MIG_FIELD_TYPE, mig->mig_type);
		cf_detail(AS_MIGRATE, "Version at mig start %ld for partition-id:%d", mig->rsv.p->current_outgoing_ldt_version, mig->rsv.pid);
		msg_set_uint64(start_m, MIG_FIELD_VERSION, mig->rsv.p->current_outgoing_ldt_version);

		mig->start_m = start_m;
		for (uint i = 0; i < mig->dst_nodes_sz; i++)
			mig->start_done[i] = false;

		mig->start_xmit_ms = 0;
		cf_debug(AS_MIGRATE, "{%s:%d} MIGRATE SEND: partition iid %"PRIx64"", mig->rsv.ns->name, mig->rsv.pid, mig->cluster_key);
	}

	uint64_t now = cf_getms();
	if (mig->start_xmit_ms + MIGRATE_RETRANSMIT_STARTDONE_MS < now ) {

		for (uint i = 0; i < mig->dst_nodes_sz; i++) {
			if (mig->start_done[i] == false) {

				cf_rc_reserve(mig->start_m);
				if (0 != as_fabric_send(mig->dst_nodes[i], mig->start_m, AS_FABRIC_PRIORITY_MEDIUM)) {
					cf_debug(AS_MIGRATE, "could not send start");
					as_fabric_msg_put(mig->start_m); // put back if the send didn't
				}
			}
		}

		mig->start_xmit_ms = now;

	}

	return(0);
}


// migration_reduce_arg
// Helper struct so we can find the smallest migration and do them first

typedef struct migration_reduce_pop_arg_s {
	int       most_urgent_migration_sort_priority;
	uint32_t  most_urgent_migration_tree_elements;
} migration_reduce_pop_arg;

void
migration_reduce_pop_arg_reset(migration_reduce_pop_arg *mra)
{
	mra->most_urgent_migration_sort_priority = -1;
	// 0 is a special value - means we haven't started
	mra->most_urgent_migration_tree_elements = 0;
}

int
get_most_urgent_migration_reduce_pop_fn(void *buf, void *udata)
{
	migration_reduce_pop_arg *mra = (migration_reduce_pop_arg *) udata;
	migration *mig = *((migration **) buf);

	// if all elements are mig = 0, we'll always return 0 and pop it later
	if (0 == mig)
		return(-1);

	// if migration size = 0 OR cluster key mismatch, process immediately
	if (mig->rsv.tree->elements == 0 ||
			mig->cluster_key != as_paxos_get_cluster_key())
		return(-1);

	// Do zombies first (priority == 2), then migrate_state == DONE (priority == 1)
	// then the rest. If priority is tied, sort by smallest.
	if (mig->migration_sort_priority > mra->most_urgent_migration_sort_priority ||
			(mig->migration_sort_priority == mra->most_urgent_migration_sort_priority &&
			 mig->rsv.tree->elements < mra->most_urgent_migration_tree_elements))
	{
		mra->most_urgent_migration_sort_priority = mig->migration_sort_priority;
		mra->most_urgent_migration_tree_elements = mig->rsv.tree->elements;
		return(-2);
	}

	// found a larger migration than the smallest we've found so far
	return(0);
}



/*
 * Workhorse function which reduce passed in tree and migrates based on data in
 * migration job
 *
 * When the migration is started for the subrecord the partition is put into a
 * state SUBRECORD_MIGRATING state.
 *
 * Once the record migration starts it gets out of SUBRECORD_MIGRATING state and
 * everything moves as normal. As record keep coming in all the subrecord are
 * resolved and added/removed moved from the migrate tree to main tree.
 */
int
as_migrate_tree(migration *mig, as_index_tree *tree, bool is_subrecord)
{
	uint32_t yield_count = 0;
	cf_detail(AS_MIGRATE, "DEBUG: TREE SIZE IF %d", as_index_tree_size(tree));
	// Got sub record data
	if (as_index_tree_size(tree) != 0) {

		// reduce the hash table into an array/list
		cf_debug(AS_MIGRATE, "migration reduce read started");
		as_index_reduce(tree, migrate_tree_reduce, mig);
		cf_debug(AS_MIGRATE, "migration reduce reads finished");

		// transmit all with inserts into the retransmit hash
		for (uint p_idx = 0; p_idx < mig->pickled_size ; p_idx++) {

			// Cluster key does not match it is obselete
			if (mig->cluster_key != as_paxos_get_cluster_key()) {
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "MIGRATE_INSERT_SEND: cluster key mismatch");
				as_migrate_print2_cluster_key("MIGRATE_INSERT_SEND", mig->cluster_key);
				return -1;
			}

			pickled_record *pr = &mig->pickled_array[p_idx];

			// TODO: what happens now then ... should it not retry
			msg *m = as_fabric_msg_get(M_TYPE_MIGRATE);
			if (!m) {
				// [Note:  This can happen when the limit on number of migrate "msg" objects is reached.]
				cf_detail(AS_MIGRATE, "failed to allocate a msg of type %d ~~ bailing out of migration", M_TYPE_MIGRATE);
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "no available msgs");
				return 1;
			}

			if (as_ldt_fill_mig_msg(mig, m, pr, is_subrecord)) {
				cf_detail(AS_MIGRATE, "Skipping Stale version Subrecord Shipping");
				as_fabric_msg_put(m);
				continue;
			}
			msg_set_uint32(m, MIG_FIELD_OP,         OPERATION_INSERT);
			msg_set_buf   (m, MIG_FIELD_DIGEST,     (void *) &pr->key, sizeof(cf_digest), MSG_SET_COPY);
			msg_set_uint32(m, MIG_FIELD_GENERATION, pr->generation);
			msg_set_uint32(m, MIG_FIELD_VOID_TIME,  pr->void_time);
			msg_set_buf   (m, MIG_FIELD_NAMESPACE,  (byte *) mig->rsv.ns->name, strlen(mig->rsv.ns->name), MSG_SET_COPY);
			msg_set_buf   (m, MIG_FIELD_VINFOSET,   pr->vinfo_buf, pr->vinfo_buf_len, MSG_SET_COPY);

			if (pr->rec_props.p_data) {
				msg_set_buf(m, MIG_FIELD_REC_PROPS, (void *)pr->rec_props.p_data, pr->rec_props.size, MSG_SET_HANDOFF_MALLOC);
				as_rec_props_clear(&pr->rec_props);
			}

			msg_set_buf(m, MIG_FIELD_RECORD, pr->record_buf, pr->record_len, MSG_SET_HANDOFF_MALLOC);
			pr->record_len = 0;
			pr->record_buf = NULL;

			// This might block a bit if the queues are blocked up
			// but a failure is a hard-fail - can't notify other side
			if (0 != migrate_send_reliable(mig, m)) {
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "data send reliable fail");
				return 2;
			}

			// Don't want too many outstanding. Monitor the hash size and pause a bit if
			// it gets too full.
			if (shash_get_size(mig->retransmit_hash) > g_config.migrate_xmit_hwm) {
				int escape = 0;
				while ( shash_get_size(mig->retransmit_hash) > g_config.migrate_xmit_lwm ) {
					if (escape++ >= 300) {
						cf_debug(AS_MIGRATE, "tx transmit backoff: escaping: tid %d size %d", mig->id, shash_get_size(mig->retransmit_hash) );
						break;
					}
					usleep(1000);
				}
			}

			yield_count++;
			if (g_config.migrate_xmit_priority && (yield_count % g_config.migrate_xmit_priority == 0)) {
				usleep(g_config.migrate_xmit_sleep);
			}
		}
		cf_debug(AS_MIGRATE, "sent all, retransmitting: mig_id %d", mig->id);

		// reduce over the retransmit hash until finished
		do {

			/*
			 * Check in case the migrate is obsolete - new key has been set
			 * due to a new paxos vote.
			 */
			if (mig->cluster_key != as_paxos_get_cluster_key()) {
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "MIGRATE_RETRANSMIT_SEND: cluster key mismatch");
				as_migrate_print2_cluster_key("MIGRATE_RETRANSMIT_SEND", mig->cluster_key);
				return -2;
			}

			uint64_t now = cf_getms();

			// the only rv from this is the rv of the reduce fn,
			// which is the return value of a fabric_send
			int rv = shash_reduce( mig->retransmit_hash , migrate_retransmit_reduce_fn, &now );
			if (rv != 0) {
				if (rv != AS_FABRIC_ERR_QUEUE_FULL) {
					cf_detail(AS_MIGRATE, "failure migrating - bad fabric send in retranmission - error %d", rv);
					migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "retransmit send fail");
					return 3;
				}
				else {
					cf_detail(AS_MIGRATE, "retransmit: q full: {%s:%d} unacked sz %d", mig->rsv.ns->name, mig->rsv.pid, shash_get_size(mig->retransmit_hash) );
				}
			}

			if (shash_get_size( mig->retransmit_hash ) > 0) {

				cf_detail(AS_MIGRATE, "mig_id %d retransmit size %d", mig->id, shash_get_size(mig->retransmit_hash));
				usleep( 1000 * 50 );
			}
			else // done!
				break;
		} while(1);

		cf_detail(AS_MIGRATE, "retranmsits done %d", mig->id);

		if (mig->pickled_array)	{
			for (uint i = 0; i < mig->pickled_size; i++) {
				if (mig->pickled_array[i].record_buf) {
					cf_free(mig->pickled_array[i].record_buf);
				}
				if  (mig->pickled_array[i].rec_props.p_data) {
					cf_free(mig->pickled_array[i].rec_props.p_data);
				}
			}
			cf_free(mig->pickled_array);
			mig->pickled_array = NULL;
		}
	}
	return 0;
}

/*
 *  Request one migrate xmit thread to exit cleanly.
 */
static int
migrate_kill_xmit_thread()
{
	// Only use one, but never free it!
	static void *death_msg = NULL;

	// Send thread exit request (i.e., a NULL message) via the queue with high priority.
	return cf_queue_priority_push(g_migrate_q, &death_msg, CF_QUEUE_PRIORITY_HIGH);
}

//
// O success
// 1 goto next
int
migration_pop(migration **migp, migration_reduce_pop_arg *mrpa)
{
	// try to get smallest migration on q
	migration_reduce_pop_arg_reset(mrpa);
	int rv = cf_queue_priority_reduce_pop(g_migrate_q, (void *) migp,
										  get_most_urgent_migration_reduce_pop_fn, mrpa);

	if (CF_QUEUE_ERR == rv) {
		cf_warning(AS_MIGRATE, "priority queue reduce pop failed");
		return 1;
	}

	if (CF_QUEUE_NOMATCH == rv) {
		if (0 != cf_queue_priority_pop(g_migrate_q, (void *) migp,
									   CF_QUEUE_FOREVER))
		{
			cf_warning(AS_MIGRATE, "priority queue pop failed");
			return 1;
		}
		migration *mig = *migp;

		if (0 != mig) {
			cf_debug(AS_MIGRATE, "waited for a migrate and got one, tree size = %"PRIu32", q sz = %d", mig->rsv.tree->elements, cf_queue_priority_sz(g_migrate_q));
		} else {
			cf_debug(AS_MIGRATE, "got mig = 0, q sz = %d", cf_queue_priority_sz(g_migrate_q));
		}
	} else {
		migration *mig = *migp;

		if (!mig) {
			cf_debug(AS_MIGRATE, "TID %d received the migrate xmit thread kill message", syscall(SYS_gettid));
		} else {
			cf_debug(AS_MIGRATE, "got smallest migrate, tree size = %"PRIu32", q sz = %d", mig->rsv.tree->elements, cf_queue_priority_sz(g_migrate_q));
		}
	}
	return 0;
}

//
// Main outbound migration thread
//
// Grab a migration off the queue, reduce it quick-as-possible into an array
// then transmit all those operations (reliably)
// if there's anything left, do all the retransmits
//
// Kill the thread when a marker null pointer is inserted in the queue
//
// Timeout choice: currently we have about 40 threads. Unfortuantly they're
// going to be somewhat synchronized, but they'd end up pulling apart. The right
// answer is to have these threads kill themselves after they find they have nothing to
// do except for the last thread

void *
migrate_xmit_fn(void *arg)
{
	long my_id = (long) arg;
	migration_reduce_pop_arg mrpa;
	migration_reduce_pop_arg_reset(&mrpa);

	do {
		migration *mig;
		int rv;
		bool cancel = false;

		migration_pop(&mig, &mrpa);
		/*
		 * This is the case for intentionally stopping the migrate thread.
		 */
		if (mig == 0) {
			cf_info(AS_MIGRATE, "Migrate thread #%ld (TID %d) received termination request.", my_id, syscall(SYS_gettid));
			break; // signal of death
		}

		if (mig->cluster_key != as_paxos_get_cluster_key()) {
			migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "cluster key mismatch");
			as_migrate_print2_cluster_key("MIGRATE_XMIT_FAIL", mig->cluster_key);
			goto FinishedMigrate;
		}

		// Check to make sure the partition is valid
		// Retry on DESYNC,
		switch (mig->rsv.state) {
			case AS_PARTITION_STATE_DESYNC:
				cf_debug(AS_MIGRATE, " attempt to send-migrate a non-sync partition (%d), reinserting as low priority {%s:%d}", mig->rsv.state, mig->rsv.ns->name, (int)mig->rsv.pid);
				as_partition_reserve_update_state(&mig->rsv);
				if (0 != cf_queue_priority_push(g_migrate_q, (void *) &mig, CF_QUEUE_PRIORITY_LOW)) {
					cf_crash(AS_MIGRATE, "queue");
				}
				usleep(1000); // 1 ms
				goto NextMigrate; // no, really, go back to the queue.
			case AS_PARTITION_STATE_SYNC:
			case AS_PARTITION_STATE_ZOMBIE:
			case AS_PARTITION_STATE_WAIT:
				break;
			case AS_PARTITION_STATE_ABSENT:
			case AS_PARTITION_STATE_UNDEF:
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "tree reserve fail");
				goto FinishedMigrate;
			case AS_PARTITION_STATE_JOURNAL_APPLY:
				cf_crash(AS_MIGRATE, "migrations are not allowed to include state JOURNAL APPLY");
				break;
		}

		cf_detail(AS_MIGRATE, "migrate xmit begin: migration id %d {%s:%d}  migp %p", mig->id, mig->rsv.ns->name, mig->rsv.pid, mig);

		// a queue of control information, like whether starts and dones have been sent and received
		mig->xmit_control_q = cf_queue_create( sizeof(migrate_xmit_control), true);
		if (!mig->xmit_control_q) {
			migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "queue create fail");
			goto FinishedMigrate;
		}

		// A hash table that all xmit retransmit information can funnel through
		rv = shash_create(&mig->retransmit_hash, migrate_id_shashfn, sizeof (uint32_t), sizeof(migrate_retransmit), 512,
						  SHASH_CR_MT_BIGLOCK);
		if (rv != 0) {
			migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "shash create fail");
			goto FinishedMigrate;
		}

		// Add myself to the global hash so my acks find me
		cf_rc_reserve(mig);
		rchash_put( g_migrate_hash, (void *) &mig->id , sizeof(mig->id), (void *) mig);

#ifdef USE_NETWORK_POLICY
		// Puts the fabric into a different policy - throughput over delay
		for (uint i = 0; i < mig->dst_nodes_sz; i++)
			migrate_start_network_policy(mig->dst_nodes[i]);
#endif

		/*
		 * Check in case the migrate is obsolete - new key has been set
		 * due to a new paxos vote.
		 */
		if (mig->cluster_key != as_paxos_get_cluster_key()) {
			migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "MIGRATE_START_SEND: cluster key mismatch");
			as_migrate_print2_cluster_key("MIGRATE_START_SEND", mig->cluster_key);
			goto FinishedMigrate;
		}
		// send start messages until all nodes ack
		bool done = false;
		do {

			if (0 != migrate_start_send(mig)) {
				cf_info(AS_MIGRATE, "migrate start send failed, will retransmit (%d)", mig->id);
			}
			migrate_xmit_control mxc;
			int rv = cf_queue_pop(mig->xmit_control_q, &mxc, MIGRATE_RETRANSMIT_STARTDONE_MS);
			if (rv == 0) {
				cf_debug(AS_MIGRATE, "migrate recv start reply op %d offset %d (%d)", mxc.op, mxc.node_offset, mig->id);

				if (mxc.mig_id != mig->id)
					cf_crash(AS_MIGRATE, "internal ID error");

				switch (mxc.op) {
					case OPERATION_START_ACK_OK:

						// set the start_done flag properly
						mig->start_done[mxc.node_offset] = true;
						// check for completion
						done = true;
						for (uint i = 0; i < mig->dst_nodes_sz; i++) {
							if (mig->start_done[i] == false) {
								done = false;
								break;
							}
						}
						break;

					case OPERATION_START_ACK_ALREADY_DONE:

						// remove from the list - not doing to migrate to this chap
						// todo: do we really need to handle > 1 migrate at the same time?
						// it's hard to keep track of all the error states seperately

						// Migrate is done!
						goto CompletedMigrate;

					case OPERATION_START_ACK_EAGAIN:

						//
						// The destination of the migrate might be no longer interested
						// Check the read replica set and make sure it's still a valid destination
						//
						// TODO! this will not work with partition count > 2. In that case,
						// we need to allow the other migrations to go forward

						if (!as_paxos_succession_ismember(mig->dst_nodes[mxc.node_offset])) {
							cf_debug(AS_MIGRATE, "node no longer in cluster, failing migrate");
							migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "destination node no longer in succession");
							goto FinishedMigrate;
						}

						cf_detail(AS_MIGRATE, "received start_eagain id %d {%d}, waiting a few ticks", mig->id, mig->rsv.pid);
						usleep(1000 * 1);
						break;

					case OPERATION_START_ACK_FAIL:
						cf_detail(AS_MIGRATE, "node refused a migrate with a FAIL message");
						migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "dest node refused migrate");
						goto FinishedMigrate;

				}
			}
			else {
				cf_debug(AS_MIGRATE, "migrate start: retransmitting: mig_id %d", mig->id);
			}
			/*
			 * Check in case the migrate is obsolete - new key has been set
			 * due to a new paxos vote. Since the start migrate has completed, any cancel
			 * from here onwards needs to send the done message to the other side
			 */
			if (mig->cluster_key != as_paxos_get_cluster_key()) {
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "MIGRATE_START_SEND_DONE: cluster key mismatch");
				as_migrate_print2_cluster_key("MIGRATE_START_SEND_DONE", mig->cluster_key);
				cancel = true;
				goto DoneMigrate;
			}

		} while( done == false);

		mig->yield_count = 0;

		// There are two algorithm to do this
		//
		// Option 1: Ship the sub record tree first and the main tree. While
		//           sub record tree is getting migrated all the writes only
		//           replicate the sub record changes ... no LDT record changes
		//           are replicated. Once the sub record tree is replicate
		//           entire replication falls back to the normal scheme. Note
		//           PROS:
		//           - Logic is simple and migration layer need not know the
		//             upper layer.
		//           CONS:
		//           - Cause memory over head where in it would cause entire
		//             migrate tree to be in-memory @ the receiving node until
		//             record tree migration starts.
		//
		// Option 2: Walk through the LDT record and reduce it and migrate sub
		//           records.
		//
		//           Algorithm:
		//              For the normal write operation LDT record will be
		//              replicated once it is done, so status of LDT record has
		//              been migrated need to be maintained per record. Use bit
		//              in index marking LDT as not migrated (@ the partition
		//              rebalance i.e start of migrate) and then mark it as
		//              migrated in the end should work.
		//
		//              Entire migration then is driven through the reduce
		//              function which would do I/O and make list of SUB RECORD
		//              and also trigger migrations. So that there are never
		//              two I/O one for creating list and other for performing
		//              migration.
		//
		//           PROS:
		//           - Benefits of this scheme is because migration happens per
		//             LDT record there is need to maintain in-memory version
		//             of subrecord only of on LDT record at a given time on
		//             the recieving node
		//           - Data shipping is done structurly which can make sure the
		//             data is sent incrementally to make sure failure at any
		//             point of time make sure some valid data is replicated.
		//
		//           CONS:
		//           - The migration is no meaningful work as long as LDT record
		//             starts migrating.
		//           - For optimization of not sending losing LDT local I/O needs
		//             to be always done.
		//           - Developer needs to write Reduce UDF as he only knows the
		//             structure of data.
		//           - Migration layer has to work with UDF layer. Bad dependency.
		//
		//  Current implementation is option 1 which is easier way as it seems
		//
		if (mig->rsv.ns->ldt_enabled) {
			rv = as_migrate_tree(mig, mig->rsv.sub_tree, true);
			if (rv < 0) {
				cancel = true;
				goto DoneMigrate;
			} else if (rv > 0) {
				goto FinishedMigrate;
			}
		}

		if (shash_get_size( mig->retransmit_hash ) > 0) {
			cf_warning(AS_MIGRATE, "Unexpected Retransmit hash size");
		}
		mig->txstate = AS_PARTITION_MIG_TX_STATE_RECORD;
		if (AS_PARTITION_HAS_DATA(mig->rsv.p)) {
			cf_detail(AS_MIGRATE, "LDT_MIGRATION: Started Sending Record Migration !! %s:%d:%d:%d",
					  mig->rsv.ns->name, mig->rsv.p->partition_id, mig->rsv.p->vp->elements, mig->rsv.p->sub_vp->elements);
		}
		rv = as_migrate_tree(mig, mig->rsv.tree, false);
		if (rv < 0) {
			cancel = true;
			goto DoneMigrate;
		} else if (rv > 0) {
			goto FinishedMigrate;
		}

DoneMigrate:
		// send done messages until acked
		done = false;
		do {

			if (0 != migrate_done_send(mig, cancel)) {
				migrate_send_finish(mig, AS_MIGRATE_STATE_ERROR, "done send fail");
				goto FinishedMigrate;
			}

			migrate_xmit_control mxc;
			if (0 == cf_queue_pop(mig->xmit_control_q, &mxc, MIGRATE_RETRANSMIT_STARTDONE_MS)) {

				cf_debug(AS_MIGRATE, "migrate: received done: id %d op %d doneoffset %d", mxc.mig_id, mxc.op, mxc.node_offset);

				if ( (mxc.mig_id == mig->id) && (mxc.op == OPERATION_DONE_ACK) ) {
					// set the start_done flag properly
					mig->done_done[mxc.node_offset] = true;
					// check for completion
					done = true;
					for (uint i = 0; i < mig->dst_nodes_sz; i++) {
						if (mig->done_done[i] == false) {
							done = false;
							break;
						}
					}
				}
			}
			else {
				cf_detail(AS_MIGRATE, "migrate done retransmit: mig_id %d", mig->id);
			}
		} while( done == false );

CompletedMigrate:
		// Migrate complete - notify caller
		// TODO: should the callback be called to cleanup state with the cancel flag?
		// TODO: What if migration is pushed to multiple nodes .. passes only first one.
		// currently only sent to one node in a migration record
		if (!cancel) {
			migrate_send_finish(mig, AS_MIGRATE_STATE_DONE, "Sucessfully Sent");
		}

		cf_detail(AS_MIGRATE, "migrate done: migration id %d {%s:%d}", mig->id, mig->rsv.ns->name, mig->rsv.pid );

FinishedMigrate:

		mig->txstate = AS_PARTITION_MIG_TX_STATE_NONE;
		mig->rsv.p->current_outgoing_ldt_version = 0;
		cf_detail(AS_MIGRATE, "LDT_MIGRATION: Finished Sending Migration !! %s:%d:%d:%d",
				  mig->rsv.ns->name, mig->rsv.p->partition_id, mig->rsv.p->vp->elements, mig->rsv.p->sub_vp->elements);

		rchash_delete(g_migrate_hash, (void *) &mig->id , sizeof(mig->id));

#ifdef USE_NETWORK_POLICY
		for (uint i = 0; i < mig->dst_nodes_sz; i++)
			migrate_done_network_policy(mig->dst_nodes[i]);
#endif

		// free mig
		migrate_migrate_release(mig);
NextMigrate:
		;
	} while (1);

	// Give up my thead slot.
	g_migrate_xmit_th[my_id] = 0;

	return(0);
}


int
as_migrate_cancel(cf_node dst, as_namespace *ns, as_partition_id partition)
{
	cf_info(AS_MIGRATE, "as_migrate_cancel: stub");
	return(0);
}

/*
** Externally visible function which kicks off a migrate
**
** allocate a migration structure to keep the state we need
**
** put that structure on a queue over to the xmit function to kick off the transmission
*/

int
as_migrate(cf_node *dst_node, uint dst_sz, as_namespace *ns, as_partition_id part_id, as_migrate_type mig_type, bool is_migrate_state_done, as_migrate_callback cb, void *udata)
{

	if (dst_sz < 1 || dst_sz >= g_config.paxos_max_cluster_size) {
		cf_warning(AS_MIGRATE, "as_migrate: passed bad node length %d, fail", dst_sz);
		return(-1);
	}

#ifdef EXTRA_CHECKS
	if (!dst_node) {
		cf_info(AS_MIGRATE, "as_migrate: passed bad destination node pointer NULL, fail");
		return(-1);
	}

	// todo: the cool check would be against the fabric's list of active nodes
	for (uint i = 0; i < dst_sz; i++) {
		if (dst_node[i] == 0) {
			cf_info(AS_MIGRATE, "as_migrate: passed bad node NULL, fail");
			return(-1);
		}
		if (dst_node[i] == g_config.self_node) {
			cf_info(AS_MIGRATE, "as_migrate: passed self node, can't migrate to that, sucka");
			return(-1);
		}
	}
#endif

	migration *mig = cf_rc_alloc( sizeof(migration) );
	cf_assert(mig, AS_MIGRATE, CF_CRITICAL, "malloc");
	cf_atomic_int_incr(&g_config.migrate_tx_object_count);
	cf_debug(AS_MIGRATE, "{%s:%d}as_migrate: START MIG %p", ns->name, part_id, mig);

	memcpy(mig->dst_nodes, dst_node, dst_sz * sizeof(cf_node) );
	mig->dst_nodes_sz = dst_sz;

	AS_PARTITION_RESERVATION_INIT(mig->rsv);
	mig->pickled_alloc = 0;
	mig->pickled_size = 0;
	mig->pickled_array = 0;
	mig->id = cf_atomic32_incr(&g_migrate_id);
	mig->mig_type = mig_type;

	mig->start_m = 0;
	mig->start_xmit_ms = 0;
	memset(mig->start_done, 0, sizeof(mig->start_done));
	mig->done_m = 0;
	mig->done_xmit_ms = 0;
	memset(mig->done_done, 0, sizeof(mig->done_done));

	// mig->cluster_key = as_paxos_get_cluster_key();

	// Create these later when we need them
	// this is very important, because we might get *lots* all at once
	mig->retransmit_hash = 0;
	mig->xmit_control_q = 0;

	// It is important to reserve the tree now, because we must migrate the tree
	// that is in existance NOW
	as_partition_reserve_migrate(ns, part_id, &mig->rsv, 0);
	cf_atomic_int_incr(&g_config.migtx_tree_count);

	mig->cluster_key = mig->rsv.cluster_key; // use the partition's cluster key for this variable

	// Do zombies first (priority == 2), then migrate_state == DONE (priority == 1)
	// then the rest. If priority is tied, sort by smallest.
	mig->migration_sort_priority = (mig->rsv.state == AS_PARTITION_STATE_ZOMBIE) ? 2 :
								   (is_migrate_state_done ? 1 : 0);

	mig->cb = cb;
	mig->udata = udata;

	cf_debug(AS_MIGRATE, "migration created: %p id %d {%s:%d}", mig, mig->id, mig->rsv.ns->name, mig->rsv.pid);
	for (uint i = 0; i < mig->dst_nodes_sz; i++)
		cf_debug(AS_MIGRATE, " destination %d : %"PRIx64, i, mig->dst_nodes[i]);

//	cf_debug(AS_MIGRATE, "migration created: %p transaction hash %p",mig,mig->trans_hash);

	cf_atomic_int_incr( &g_config.migrate_progress_send );
	/*
	 * Generate new LDT version before starting the migration for a record
	 * This would mean that everytime a outgoing migration is triggerred it
	 * will actually cause the system to create new version of the data.
	 * It could possibly blow up the versions of subrec... Look at the
	 * enhancement in migration alogrithm which makes sure the migration
	 * only happens in case data is different based on the comparison of
	 * record rather than subrecord and cleans up old versions aggressively
	 *
	 * No new version if data is migrating out of master
	*/
	if (mig->rsv.ns->ldt_enabled) {
		mig->rsv.p->current_outgoing_ldt_version = as_ldt_generate_version();
		if (AS_PARTITION_HAS_DATA(mig->rsv.p))
			cf_detail(AS_LDT, "LDT_MIGRATION Generated new Version @ start of migration %d:%ld",
				  mig->rsv.p->partition_id, mig->rsv.p->current_outgoing_ldt_version);
		mig->txstate = AS_PARTITION_MIG_TX_STATE_SUBRECORD;
		cf_detail(AS_MIGRATE, "LDT_MIGRATION: OutGoing Version %ld Started Sending SubRecord Migration !! %s:%d:%d:%d",
				  mig->rsv.p->current_outgoing_ldt_version, mig->rsv.ns->name, mig->rsv.p->partition_id, 
				  mig->rsv.p->vp->elements, mig->rsv.p->sub_vp->elements);
	} else {
		mig->txstate = AS_PARTITION_MIG_TX_STATE_RECORD;
		mig->rsv.p->current_outgoing_ldt_version = 0;
	}

	if (0 != cf_queue_priority_push(g_migrate_q, &mig, CF_QUEUE_PRIORITY_HIGH))
		cf_crash(AS_MIGRATE, "queue");

	return(0);
}

static cf_atomic32 init_counter = 0;

void
as_migrate_init()
{
	if (1 != cf_atomic32_incr(&init_counter)) {
		return;
	}

	// a queue of the migrations that have been requested.
	g_migrate_q = cf_queue_priority_create(sizeof(void *), true);

	rchash_create(&g_migrate_hash, migrate_id_hashfn, migrate_migrate_destroy, sizeof(uint32_t), 64, RCHASH_CR_MT_MANYLOCK);

	rchash_create(&g_migrate_recv_control_hash, migrate_recv_control_hashfn, migrate_recv_control_destroy,
				  sizeof(migrate_recv_control_index), 64, RCHASH_CR_MT_BIGLOCK);

	// We can't quite simply create multiple threads to do multiple simultaneous migrates
	// because of the xmit_control methodology - not that hard to retrofit though
	memset(g_migrate_xmit_th, 0, MAX_NUM_MIGRATE_XMIT_THREADS * sizeof(pthread_t));

	// Create the migrate xmit threads detatched so we don't need to join with them.
	if (pthread_attr_init(&migrate_xmit_th_attr)) {
		cf_crash(AS_MIGRATE, "failed to initialize the migrate xmit thread attributes");
	}
	if (pthread_attr_setdetachstate(&migrate_xmit_th_attr, PTHREAD_CREATE_DETACHED)) {
		cf_crash(AS_MIGRATE, "failed to set the migrate xmit thread attributes to the detached state");
	}
	for (long i = 0; i < g_config.n_migrate_threads; i++) {
		pthread_create(&g_migrate_xmit_th[i], &migrate_xmit_th_attr, migrate_xmit_fn, (void *) i);
	}

	pthread_create(&g_migrate_rx_reaper_th, 0, migrate_rx_reaper_fn, 0);

	as_fabric_register_msg_fn(M_TYPE_MIGRATE, migrate_mt, sizeof(migrate_mt), migrate_msg_fn, 0 /* udata */);

	g_event_cb = as_partition_migrate_rx;
	g_udata = NULL;

	g_migrate_trans_id = 1;
	g_migrate_id = 1;

	// Migration Incoming LDT version hash
	if (SHASH_OK != shash_create(&g_migrate_incoming_ldt_version_hash, migrate_ldt_version_hashfn,
									sizeof(migrate_recv_ldt_version), 
									sizeof(void *), 64, SHASH_CR_MT_MANYLOCK)) {
		cf_crash(AS_AS, "Couldn't incoming ldt migrate hash");
	}
}

// Searches for incoming version based on passed in incoming migrate_ldt_vesion and partition_id.
// migrate rxstate match is also performed if it is passed. Check is skipped if zero.
// Return:
//     True:  If there is incoming migration
//     False: if no matching incoming migration found
bool
as_migrate_is_incoming(cf_digest *subrec_digest, uint64_t version, as_partition_id partition_id, int rxstate)
{
	migrate_recv_control *mc = NULL;
	migrate_recv_ldt_version mc_l;
	mc_l.incoming_ldt_version = version;
	mc_l.pid                  = partition_id;
	if (SHASH_OK == shash_get(g_migrate_incoming_ldt_version_hash, &mc_l, &mc)) {
		if (mc) {
			if (rxstate) {
				if (mc->rxstate == rxstate) {
					return true;
				} else {
					cf_detail(AS_LDT, "No Incoming for pid:%d mc->rxstate:%d", partition_id, mc->rxstate);
					return false;
				}
			} else {
				return true;
			}
		}
	}
	cf_detail_digest(AS_LDT, subrec_digest, "No Incoming for pid:%d, version:%ld No mig item", partition_id, version);
	return false;
}

/*
 *  Set the number of migrate xmit threads.
 */
int
as_migrate_set_num_xmit_threads(int n_threads)
{
	int rv;

	if (MAX_NUM_MIGRATE_XMIT_THREADS < n_threads) {
		cf_warning(AS_MIGRATE, "Setting number of migrate xmit threads to the maximum permissible value (%d) not %d", MAX_NUM_MIGRATE_XMIT_THREADS, n_threads);
		n_threads = MAX_NUM_MIGRATE_XMIT_THREADS;
	} else if (0 > n_threads) {
		cf_warning(AS_MIGRATE, "Setting number of migrate xmit threads to the minimum permissible value (0) not %d", n_threads);
		n_threads = 0;
	}

	while (g_config.n_migrate_threads > n_threads) {
		cf_info(AS_MIGRATE, "Killing a migrate thread (delta: %d).", g_config.n_migrate_threads - n_threads);
		if (0 > (rv = migrate_kill_xmit_thread())) {
			cf_warning(AS_MIGRATE, "Failed to send migrate xmit thread kill message (rv %d)", rv);
			return -1;
		}
		g_config.n_migrate_threads--;
	}

	while (g_config.n_migrate_threads < n_threads) {
		cf_info(AS_MIGRATE, "Starting a migrate xmit thread (delta: %d).", n_threads - g_config.n_migrate_threads);
		long i = -1;
		bool found_one = false;
		while (++i < MAX_NUM_MIGRATE_XMIT_THREADS) {
			if (!g_migrate_xmit_th[i]) {
				found_one = true;
				break;
			}
		}
		if (!found_one) {
			cf_warning(AS_MIGRATE, "Failed to find a free slot for creating a migrate xmit thread! (Num. threads %d)", g_config.n_migrate_threads);
			return -1;
		}
		if ((rv = pthread_create(&g_migrate_xmit_th[i], &migrate_xmit_th_attr, migrate_xmit_fn, (void *) i))) {
			cf_warning(AS_MIGRATE, "Failed to create migrate thread! (rv %d)", rv);
		}
		g_config.n_migrate_threads++;
	}

	return 0;
}


/* Info commands. */


/*
 *  Reduce function to dump the g_migrate_hash.
 */
static int as_migrate_dump_migrate_hash_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	uint32_t mig_id = (uint32_t) ((uint64_t) key & 0x00000000ffffffff);
	migration *mig = (migration *) object;
	int *item_num = (int *) udata;

	cf_info(AS_MIGRATE, "[%d]: mig_id %d : id %d ; type %d ; start xmit ms %ld ; done xmit ms %ld ; yc %ld ; ck %016lX", *item_num, mig_id,
			mig->id, mig->mig_type, mig->start_xmit_ms, mig->done_xmit_ms, mig->yield_count, mig->cluster_key);

	*item_num += 1;

	return 0;
}

/*
 *  Reduce function to dump the g_migrate_recv_control_hash.
 */
static int as_migrate_dump_migrate_recv_control_hash_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	migrate_recv_control_index *mc_i = (migrate_recv_control_index *) key;
	migrate_recv_control *mc = (migrate_recv_control *) object;
	int *item_num = (int *) udata;

	cf_info(AS_MIGRATE, "[%d]: src node %016lX ; id %d : done recv %d ; start recv ms %ld ; done recv ms %ld ; ck %016lX ; src node %016lX ; type %d",
			*item_num, mc_i->source_node, mc_i->mig_id, mc->done_recv, mc->start_recv_ms, mc->done_recv_ms, mc->cluster_key, mc->source_node, mc->mig_type);

	*item_num += 1;

	return 0;
}

/*
 *  Print information about migration to the log.
 */
void as_migrate_dump(bool verbose)
{
	cf_info(AS_MIGRATE, "Migration Info.:");
	cf_info(AS_MIGRATE, "----------------");
	cf_info(AS_MIGRATE, "Number of migrations in g_migrate_hash: %d", rchash_get_size(g_migrate_hash));
	cf_info(AS_MIGRATE, "Number of requested migrates waiting in g_migrate_q : %d", cf_queue_priority_sz(g_migrate_q));
	cf_info(AS_MIGRATE, "Number of inbound migrations in g_migrate_recv_control_hash: %d", rchash_get_size(g_migrate_recv_control_hash));
	cf_info(AS_MIGRATE, "Current migration ID: %d", g_migrate_id);
	cf_info(AS_MIGRATE, "Current migrate transaction ID: %d", g_migrate_trans_id);

	if (verbose) {
		int item_num = 0;
		if (rchash_get_size(g_migrate_hash) > 0) {
			cf_info(AS_MIGRATE, "Contents of g_migrate_hash:");
			cf_info(AS_MIGRATE, "---------------------------");
			rchash_reduce(g_migrate_hash, as_migrate_dump_migrate_hash_fn, &item_num);
		}

		if (rchash_get_size(g_migrate_recv_control_hash) > 0) {
			item_num = 0;
			cf_info(AS_MIGRATE, "Contents of g_migrate_recv_control_hash:");
			cf_info(AS_MIGRATE, "----------------------------------------");
			rchash_reduce(g_migrate_recv_control_hash, as_migrate_dump_migrate_recv_control_hash_fn, &item_num);
		}
	}
}
