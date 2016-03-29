/*
 * paxos.h
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
 *  Paxos consensus algorithm
 *
 */

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_queue_priority.h"

#include "msg.h"
#include "util.h"

#include "base/datamodel.h"

/* SYNOPSIS
 * Paxos
 *
 * A full discussion of the Paxos distributed consensus algorithm is outside
 * the scope of this header file; comprehension is left as an exercise to the
 * reader.  This implementation is a slightly modified variant of collapsed
 * multi-Paxos.
 *
 * Terminology
 * node
 * cohort
 * succession
 * principal
 * generation: sequence, proposal
 *
 * we have to store all outstanding transactions
 */


/* AS_PAXOS_ALPHA
 * The maximum number of commands outstanding */
#define AS_PAXOS_ALPHA 128


/* as_paxos_msg
 * An intermediate structure for message queuing: this is necessary to
 * preserve the originating node ID of a message */
typedef struct as_paxos_msg {
	cf_node id;
	msg *m;
} as_paxos_msg;


/* as_paxos_msg_template
 * The template for a Paxos fabric message
 * Note:  There are four versions of the Paxos protocol sharing this message template:
 *   Paxos protocol v1 doesn't have the succession / change list length.
 *   Paxos protocol v2 rightfully includes the length of the succession / change list
 *      so that it's possible to have peaceful coexistence and interoperability
 *      between nodes of different maximum cluster sizes.
 *   Paxos protocol v3 includes the properties of v2 as well as adding the partition size
 *      array used for Secondary Index Query Node determination.
 *   Paxos protocol v4 is identical to v3 except in the value of the version identifier.
 *      The v4 protocol is used as a tag to distinguish nodes running with Rack Aware mode
 *      enabled so that clustering will only be possible between nodes with a common
 *      interpretation of the internal bit field structure of node ID values. */
static const msg_template as_paxos_msg_template[] = {
#define AS_PAXOS_MSG_V1_IDENTIFIER 0x7078
#define AS_PAXOS_MSG_V2_IDENTIFIER 0x7079
#define AS_PAXOS_MSG_V3_IDENTIFIER 0x707A
#define AS_PAXOS_MSG_V4_IDENTIFIER 0x707B
#define AS_PAXOS_MSG_ID 0
	{ AS_PAXOS_MSG_ID, M_FT_UINT32 },
#define AS_PAXOS_MSG_COMMAND 1
	{ AS_PAXOS_MSG_COMMAND, M_FT_UINT32 },
#define AS_PAXOS_MSG_GENERATION_SEQUENCE 2
	{ AS_PAXOS_MSG_GENERATION_SEQUENCE, M_FT_UINT32 },
#define AS_PAXOS_MSG_GENERATION_PROPOSAL 3
	{ AS_PAXOS_MSG_GENERATION_PROPOSAL, M_FT_UINT32 },
#define AS_PAXOS_MSG_CHANGE 4
	{ AS_PAXOS_MSG_CHANGE, M_FT_BUF },
#define AS_PAXOS_MSG_SUCCESSION 5
	{ AS_PAXOS_MSG_SUCCESSION, M_FT_BUF},
#define AS_PAXOS_MSG_PARTITION 6
	{ AS_PAXOS_MSG_PARTITION, M_FT_ARRAY_BUF},
#define AS_PAXOS_MSG_CLUSTER_KEY 7
	{ AS_PAXOS_MSG_CLUSTER_KEY, M_FT_UINT64},
#define AS_PAXOS_MSG_HEARTBEAT_EVENTS_COUNT 8
	{ AS_PAXOS_MSG_HEARTBEAT_EVENTS_COUNT, M_FT_UINT32},
#define AS_PAXOS_MSG_HEARTBEAT_EVENTS 9
	{ AS_PAXOS_MSG_HEARTBEAT_EVENTS, M_FT_BUF},
#define AS_PAXOS_MSG_SUCCESSION_LENGTH 10
	{ AS_PAXOS_MSG_SUCCESSION_LENGTH, M_FT_UINT32 },
#define AS_PAXOS_MSG_PARTITIONSZ 11
	{ AS_PAXOS_MSG_PARTITIONSZ, M_FT_ARRAY_BUF}
};


/* Paxos command encoding
 * Representations of the possible Paxos message types; if these get
 * changed, please also fix as_paxos_state_next() */
#define AS_PAXOS_MSG_COMMAND_UNDEF 0
#define AS_PAXOS_MSG_COMMAND_PREPARE 1
#define AS_PAXOS_MSG_COMMAND_PREPARE_ACK 2
#define AS_PAXOS_MSG_COMMAND_PREPARE_NACK 3
#define AS_PAXOS_MSG_COMMAND_COMMIT 4
#define AS_PAXOS_MSG_COMMAND_COMMIT_ACK 5
#define AS_PAXOS_MSG_COMMAND_COMMIT_NACK 6
#define AS_PAXOS_MSG_COMMAND_CONFIRM 7
#define AS_PAXOS_MSG_COMMAND_SYNC_REQUEST 8
#define AS_PAXOS_MSG_COMMAND_SYNC 9
#define AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST 10
#define AS_PAXOS_MSG_COMMAND_PARTITION_SYNC 11
#define AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT 12
#define AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK 13
#define AS_PAXOS_MSG_COMMAND_SET_SUCC_LIST 14


/* as_paxos_generation
 * A generation identifier: contains a sequence number, monotonic with voted
 * changes, and a proposal number, monotonic within each sequence.  Most
 * sequence numbers will have only one proposal number */
typedef struct as_paxos_generation {
	uint32_t sequence;
} as_paxos_generation;


/* as_paxos_change
 * A specific change to be voted on and implemented
 * Enhanced to also contain the principal since we can have multiple clusters
 * coming together and more than one principal active during the time the cluster
 * merge is in progress */
typedef struct as_paxos_change_t {
#define AS_PAXOS_CHANGE_UNKNOWN 0
#define AS_PAXOS_CHANGE_NOOP 1
#define AS_PAXOS_CHANGE_SYNC 2
#define AS_PAXOS_CHANGE_SUCCESSION_ADD 3
#define AS_PAXOS_CHANGE_SUCCESSION_REMOVE 4
	cf_node p_node; // The principal node that initiated this change
	int n_change;
	uint8_t type[AS_CLUSTER_SZ];
	cf_node id[AS_CLUSTER_SZ];
} __attribute__((__packed__)) as_paxos_change;

/* as_paxos_wire_change
 * A wire-protocol-ready structure to be filled out and pushed onto the wire
 * or pulled off of the wire.
 *
 * Note:  The "payload[]" field is used to convey both the types of changes
 * as well as the changing nodes.  These are re-packed from the corresponding
 * "as_paxos_change" structure, which is sized to the compiled-in maximum
 * cluster size, down to the current Paxos maximum cluster size for consistent
 * network transmission to the rest of the cluster members.
 */
typedef struct as_paxos_wire_change_t {
	cf_node p_node;
	int n_change;
	uint8_t payload[];	// Structure of payload is:
						//   uint8_t type[CurrentPaxosMaxClusterSize];
						//   cf_node id[CurrentPaxosMaxClusterSize];
} __attribute__((__packed__)) as_paxos_wire_change;

/* as_paxos_transaction
 * An encapsulation of a Paxos transaction: the generation, the change itself,
 * and a log of who has voted */
typedef struct as_paxos_transaction_t {
	as_paxos_generation gen;
	bool retired, confirmed;
	bool votes[AS_CLUSTER_SZ];
	int election_cycle;
	as_paxos_change c;
	cf_clock establish_time;
} __attribute__((__packed__)) as_paxos_transaction;


/* as_paxos_transaction_vote_result
 * The possible states resulting from a vote */
typedef enum {
	AS_PAXOS_TRANSACTION_VOTE_ACCEPT,
	AS_PAXOS_TRANSACTION_VOTE_REJECT,
	AS_PAXOS_TRANSACTION_VOTE_QUORUM
} as_paxos_transaction_vote_result;


/* as_paxos_change_callback
 * A callback that will be triggered when a vote completes *and the partition rebalance*
 * NB: This will be called under the protection of the Paxos lock! */
typedef void (*as_paxos_change_callback) (as_paxos_generation gen, as_paxos_change *change, cf_node succession[], void *udata);

#define MAX_CHANGE_CALLBACKS 6

/* as_paxos
 * Runtime information for a Paxos instance */
typedef struct as_paxos_t {
	pthread_mutex_t lock;

	cf_queue_priority *msgq;
	bool need_to_rebalance;        // do rebalance if eq gen.sequence

	bool ready;                    // Is Paxos intialized?

	as_paxos_generation gen;
	cf_node succession[AS_CLUSTER_SZ];
	bool alive[AS_CLUSTER_SZ];
	bool partition_sync_state[AS_CLUSTER_SZ];

	cf_node principal_pro_tempore; // Node with greatest ID currently vying to become Paxos principal (or 0 if there is none.)

	int n_callbacks;
	as_paxos_change_callback cb[MAX_CHANGE_CALLBACKS];
	void *cb_udata[MAX_CHANGE_CALLBACKS];

	as_partition_vinfo *c_partition_vinfo[AS_NAMESPACE_SZ][AS_CLUSTER_SZ];
	uint64_t c_partition_size[AS_NAMESPACE_SZ][AS_CLUSTER_SZ][AS_PARTITIONS];

	as_paxos_transaction pending[AS_PAXOS_ALPHA];

	// keeps track of transactions currently in flight
	as_paxos_transaction *current[AS_CLUSTER_SZ];

	size_t cluster_size;

	bool cluster_has_integrity;    // Is true when there is no cluster integrity fault.

	bool dun_other_clusters;       // Do we automatically dun nodes in other clusters?
} as_paxos;

/* as_paxos_petition_type
 * What sorts of changes can be requested */
typedef enum {
	AS_PAXOS_PETITION_NODE_INSERT,
	AS_PAXOS_PETITION_NODE_REMOVE
} as_paxos_petition_type;


/* Function declarations */


void as_paxos_init();
void as_paxos_start();
int as_paxos_register_change_callback(as_paxos_change_callback cb, void *udata);
int as_paxos_deregister_change_callback(as_paxos_change_callback cb, void *udata);
cf_node as_paxos_succession_getprincipal();
bool as_paxos_succession_ismember(cf_node n);

// Get the head of the Paxos succession list, or zero if there is none.
cf_node as_paxos_succession_getprincipal();

// Set the Paxos protocol version.
int as_paxos_set_protocol(paxos_protocol_enum protocol);

// Set the Paxos recovery policy.
int as_paxos_set_recovery_policy(paxos_recovery_policy_enum policy);

// Get the Paxos cluster integrity state.
bool as_paxos_get_cluster_integrity(as_paxos *p);

// Set the Paxos cluster integrity state.
void as_paxos_set_cluster_integrity(as_paxos *p, bool state);

// Set whether automatic dunning of all nodes in other clusters should persist.
void as_paxos_dun_hold(bool is_dunned);

/* Paxos Info. command functions. */

/*
 * Print info. about the Paxos state to the log.
 * (Verbose true prints partition map as well.)
 */
void as_paxos_dump(bool verbose);

/*
 * Get the Paxos succession list and log it to the given "cf_dyn_buf *".
 * The first element of the list will become the Paxos principal.
 * Returns 0 if successful, -1 otherwise.
 */
int as_paxos_get_succession_list(cf_dyn_buf *db);

/*
 * Set the Paxos succession list from list of node IDs.
 * The first element of the list will become the Paxos principal.
 * Returns 0 if successful, -1 otherwise.
 */
int as_paxos_set_succession_list(/*const*/ char *nodes_str, int nodes_str_len);
