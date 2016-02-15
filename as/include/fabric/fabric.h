/*
 * fabric.h
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
 * The fabric interconnect is a set of network routines
 * to send and receive messages in between nodes in a cluster
 *
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "msg.h"
#include "queue.h"
#include "rchash.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"


// This is the maximum number of file descriptors a node may have outstanding
#define FABRIC_MAX_FDS	8

#define AS_FABRIC_ERR_UNKNOWN (-1)
#define AS_FABRIC_ERR_QUEUE_FULL (-2)
#define AS_FABRIC_ERR_NO_NODE (-3)
#define AS_FABRIC_ERR_BAD_MSG (-4)
#define AS_FABRIC_ERR_UNINITIALIZED (-5)
#define AS_FABRIC_ERR_TIMEOUT (-6)
#define AS_FABRIC_SUCCESS (0)

#define AS_FABRIC_PRIORITY_HIGH		(CF_QUEUE_PRIORITY_HIGH)	// Paxos + acks
#define AS_FABRIC_PRIORITY_MEDIUM	(CF_QUEUE_PRIORITY_MEDIUM)	// regular data requests
#define AS_FABRIC_PRIORITY_LOW		(CF_QUEUE_PRIORITY_LOW)		// migrate data


// Register for fabric notifications
typedef enum {
	FABRIC_NODE_ARRIVE,		// new node came online
	FABRIC_NODE_DEPART,		// node departed
	FABRIC_NODE_UNDUN,		// node un-dunned
	FABRIC_NODE_DUN,		// node dunned
	FABRIC_RESET		    // reset the universe of nodes
} as_fabric_event_type;

typedef struct as_fabric_event_node_t {
	as_fabric_event_type evt;
	cf_node nodeid;         // the node which has arrived or departed
	cf_node p_node; 		// principle node in the succession list, only on arrival?
} as_fabric_event_node;

#define FABRIC_ALL_NODES (0xFFFFFFFFFFFFFFFF)

// Log information about existing "msg" objects and queues.
extern void as_fabric_msg_queue_dump();

//
// Use this to allocate new messages. The reference counts on the messages
// are carefully managed
extern msg *as_fabric_msg_get(msg_type t);
extern void as_fabric_msg_put(msg *);


//
// Return the ms in the past when I last heard from a node in question
//  (add whether I've been getting connection fails, or other health check?)
//  point of this function is adding health checks.
//
// WARNING! Directly referenced without header file from hb.c to avoid circular references.
// If you change this,look there....
extern int as_fabric_get_node_lasttime(cf_node node, uint64_t *lasttime);

//
// Register for global events ARRIVE and DEPARTED
//
// FABRIC_NODE_ARRIVE - the corresponding nodeid will be set to the node that has joined, and you can send it messages
//
// FABRIC_NODE_DEPART - the corresponding nodeid will be set to the departed node, you can no longer send it messages
// The events are batched in an array of as_fabric_event_node elements
//

typedef int (*as_fabric_event_fn) (int nevents, as_fabric_event_node *events, void *udata);

//
// Register for message arrival from the fabric
//
// This is a message corresponding to the message you described in the init
// For efficiency, it is hoped you can copy out the fields you need and be done.
// Call 'fabric_msg_put' when you're done with the message.


typedef int (*as_fabric_msg_fn) (cf_node id, msg *m, void *udata);

// starts the heartbeat system, opens up connections to the remote endpoints
// Upper layer is currently allowed to define only one global message type
// that is carried across the fabric. This message should include any form of
// disambiguation the fabric should use, or versioning if necessary
extern int as_fabric_init();

//
// Register for events, such as node arrive and node depart
// at the moment, there will only be one registered handlers - a second writer
// will kick out the previous - but there's no real reason for that other than sloth
// maybe someday we'll have more messages to register for? I hope not, those would
// become messages

extern int as_fabric_register_event_fn( as_fabric_event_fn event_cb, void *udata_event );

//
// Register for incoming message types. All messages of this type will be handed to this
// one handler. Right now there is only one handlers - but it's really cheap to have multiple
// handlers, if we wanted, because of the reference counting system.

extern int as_fabric_register_msg_fn(	msg_type type, const msg_template *mt, size_t mt_sz,  as_fabric_msg_fn msg_cb, void *udata_msg);

// Call this once your register functions are all done and ready (or ready enough)

extern int as_fabric_start();

//
// Send a message
//
extern int as_fabric_send(cf_node node, msg *m, int priority );

// to send to all nodes currently known, set the nodes pointer to 0
extern int as_fabric_send_list(cf_node *nodes, int nodes_sz, msg *m, int priority);

// Reliably send a transaction/message
//
// Used to send a request, and receive a response, reliably
// this is guaranteed to NEVER return an error directly
// but might call the callback function saying that we ran out of time or had
// some other error
//
// Requires Field 0 be a uint64_t which will be used by the fabric system, an unknown
// error will be thrown if this is not true

//
// XMIT SIDE - sending (initiating) a transaction
// you will get a callback when your transaction is complete
// or not an error
// defined that there will be exactly one callback
typedef int (*as_fabric_transact_complete_fn) (msg *rsp, void *udata, int as_fabric_err);

extern void as_fabric_transact_start(cf_node node, msg *m, int timeout_ms, as_fabric_transact_complete_fn cb, void *userdata);

//
// RECV SIDE - register for incoming transaction requests
// and you send your response

typedef int (*as_fabric_transact_recv_fn) (cf_node node, msg *msg, void *transact_data, void *udata);

extern int as_fabric_transact_register(	msg_type type, const msg_template *mt, size_t mt_sz,  as_fabric_transact_recv_fn cb, void *udata);

// this is how you reply to a message you've received - pass back the same transact data
extern int as_fabric_transact_reply(msg *reply_msg, void *transact_data);

//
//

#if 0
// NOT YET
// Reliably send a message.
// All this means is the other side's fabric receives the message.
// there is no guarantee that it reaches the actual application level or that they don't throw
// it away immediately

typedef int (as_fabric_send_reliable_fn) (void *udata, int as_fabric_err);

extern void as_fabric_send_reliable(cf_node node, msg *m, int timeout_ms, as_fabric_send_reliable_fn *cb, void *userdata);
#endif

// Print useful status information about all fabric resources to the log file.
extern void as_fabric_dump(bool verbose);

//
// Get a list of all the nodes - use a dynamic array, which requires inline
//

/*
 * Fabric must support the maximum cluster size.
 */
#define MAX_NODES_LIST (AS_CLUSTER_SZ)
typedef struct as_node_list_t {
	uint	sz;
	uint    alloc_sz;
	cf_node nodes[MAX_NODES_LIST];
} as_node_list;

extern int fabric_get_node_list_fn(void *key, uint32_t keylen, void *data, void *udata);
extern rchash *g_fabric_node_element_hash;


static inline int
as_fabric_get_node_list(as_node_list *nl)
{
	nl->sz = 1;
	nl->nodes[0] = g_config.self_node;
	nl->alloc_sz = MAX_NODES_LIST;

	rchash_reduce(g_fabric_node_element_hash, fabric_get_node_list_fn, nl);

	return(nl->sz);
}

