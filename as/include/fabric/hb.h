/*
 * hb.h
 *
 * Copyright (C) 2008 Aerospike, Inc.
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
 * The heartbeat module is a set of network routines
 * to send and receive lightweight messages in the cluster
 *
 * State diagram:
 *
 * heartbeat_init starts listening for other nodes, and adds those nodes to the
 * list of active nodes without reporting them to interested parties.
 *
 * heartbeat_start finalizes the startup of the heartbeat system. It enforces
 * a time delay for startup, and after that, all new nodes will be reported.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_clock.h"

#include "socket.h"
#include "util.h"

#include "fabric/fabric.h"


typedef enum { AS_HB_NODE_ARRIVE, AS_HB_NODE_DEPART, AS_HB_NODE_UNDUN, AS_HB_NODE_DUN } as_hb_event_type;

typedef struct as_hb_event_node_s {
	as_hb_event_type evt;
	cf_node nodeid;
	cf_node p_node; // the principal node from the succession list
} as_hb_event_node;

typedef struct as_hb_host_addr_port_s {
	struct in_addr ip_addr;
	int port;
} as_hb_host_addr_port;

typedef void (*as_hb_event_fn) (int nevents, as_hb_event_node *events, void *udata);

extern void as_hb_init();
extern void as_hb_start();
extern bool as_hb_shutdown();
extern int as_hb_getaddr(cf_node node, cf_sockaddr *so);
extern int as_hb_register(as_hb_event_fn cb, void *udata);

extern void as_hb_process_fabric_heartbeat(cf_node node, int fd, cf_sockaddr socket, uint32_t addr, uint32_t port, cf_node *buf, size_t bufsz);
extern bool as_hb_get_is_node_dunned(cf_node node);
extern void as_hb_set_is_node_dunned(cf_node node, bool state, char *context);
extern int as_hb_set_are_nodes_dunned(char *node_str, int node_str_len, bool is_dunned);

// list a node as non-responsive for a certain amount of time
// 0 means un-snub
// use a very large value for 'forever'
extern int as_hb_snub(cf_node node, cf_clock ms);

// Unsnub all snubbed nodes.
extern int as_hb_unsnub_all();

// TIP the heartbeat system that there might be a cluster at a given IP address.
extern int as_hb_tip(char *host, int port);
// Clear tips for the given list of nodes, or for all nodes if the list is empty.
extern int as_hb_tip_clear(as_hb_host_addr_port *host_addr_port_list, int host_port_list_len);

// Set the heartbeat protocol version.
extern int as_hb_set_protocol(hb_protocol_enum protocol);

/*
 *  as_hb_nodes_str_to_cf_nodes
 *  Parse a string of comma-separated hexadecimal node IDs into a list
 *  If successful, return the parsed, 64-bit numeric node IDs is the caller-supplied
 *  "nodes" array, which must be at least "AS_CLUSTER_SZ" in length.
 *  Return the number of nodes in "num_nodes" (if non-NULL.)
 *  Return 0 if successful, -1 otherwise.
 */
int as_hb_nodes_str_to_cf_nodes(char *nodes_str, int nodes_str_len, cf_node *nodes, int *num_nodes);

/*
 *  as_hb_stats
 *  Return a string summarizing the number of heartbeat-related errors of each type.
 *  Use long format messages if "verbose" is true, otherwise use short format messages.
 */
const char *as_hb_stats(bool verbose);

/*
 *  as_dump_hb
 *  Log the state of the heartbeat module.
 */
void as_hb_dump(bool verbose);

/**
 * Generate events required to transform the input  succession list to a list
 * that would be consistent with the heart beat adjacency list. This means nodes
 * that are in the adjacency list but missing from the succession list will
 * generate an NODE_ARRIVE event. Nodes in the succession list but missing from
 * the adjacency list will generate a NODE_DEPART event.
 *
 * @param succession_list the succession list to correct. This should be large
 * enough to hold g_config.paxos_max_cluster_size events.
 * @param events the output events. This should be large enough to hold
 * g_config.paxos_max_cluster_size events.
 * @return the number of corrective events generated.
 */
int as_hb_get_corrective_events(cf_node *succession_list,
								as_fabric_event_node *events);
