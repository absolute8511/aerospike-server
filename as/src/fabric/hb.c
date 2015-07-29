/*
 * hb.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 * node detection
 */

#include "fabric/hb.h"

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/param.h>  // For MAX().

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_shash.h"

#include "cf_str.h"
#include "dynbuf.h"
#include "fault.h"
#include "msg.h"
#include "queue.h"
#include "socket.h"
#include "util.h"

#include "base/cfg.h"
#include "fabric/fabric.h"
#include "fabric/fb_health.h"


/* SYNOPSIS
 * Heartbeat Module:
 * System for detecting arrival and departure of cluster nodes and
 * maintaining a node's presence in a cluster via sending periodic
 * network messages to all other known nodes.
 * Heartbeat transport modes are: 1). Multicast (UDP) and 2). Mesh (TCP.)
 */


/*
 *  Size of the epoll descriptor events set.
 */
#define EPOLL_SZ (1024)

// Workaround for platforms that don't have EPOLLRDHUP yet
#ifndef EPOLLRDHUP
#define EPOLLRDHUP EPOLLHUP
#endif

/* AS_HB_PROTOCOL_IDENTIFIER
 * Select the appropriate message identifier for the active heartbeat protocol. */
#define AS_HB_PROTOCOL_IDENTIFIER() (AS_HB_PROTOCOL_V1 == g_config.hb_protocol ? AS_HB_MSG_V1_IDENTIFIER : AS_HB_MSG_V2_IDENTIFIER)

/* AS_HB_PROTOCOL_IS_V
 * Is the current heartbeat protocol version the given version number? */
#define AS_HB_PROTOCOL_IS_V(n) (AS_HB_PROTOCOL_V ## n == g_config.hb_protocol)

/* AS_HB_PROTOCOL_IS_AT_LEAST_V
 * Is the current heartbeat protocol version greater than or equal to the given version number? */
#define AS_HB_PROTOCOL_IS_AT_LEAST_V(n) (((int)(g_config.hb_protocol - AS_HB_PROTOCOL_V ## n)) >= 0)

/* AS_HB_PROTOCOL_VERSION_NUMBER
 * Return the version number for the given heartbeat protocol identifier. */
#define AS_HB_PROTOCOL_VERSION_NUMBER(n) ((n) - AS_HB_PROTOCOL_NONE)

/* AS_HB_ENABLED
 * Is this node currently sending and receiving heartbeats? */
#define AS_HB_ENABLED() ((AS_HB_PROTOCOL_NONE != g_config.hb_protocol) && g_hb.adjacencies)

/* AS_HB_PULSE_SIZE
 * Return the size of an as_hb_pulse structure relative to the current maximum cluster size. */
#define AS_HB_PULSE_SIZE() (sizeof(as_hb_pulse) + sizeof(cf_node) * g_config.paxos_max_cluster_size)

/*
 * AS_HB_MIN_INTERVAL
 * Minimum allowable heartbeat interval in milliseconds. */
#define AS_HB_MIN_INTERVAL (4)

typedef struct {
	cf_node     node;
	cf_clock    expiration;
} snub_list_element;

typedef struct mesh_host_list_element_s {
	struct mesh_host_list_element_s *next;
	char        host[128];
	int         port;
	cf_clock    next_try;
	int         try_interval;
	int         fd;     // -1 if inactive, allows nodes to come and go and be retried
} mesh_host_list_element;

#define MH_OP_REMOVE_ALL 1
#define MH_OP_ADD        2
#define MH_OP_REMOVE_FD  3

typedef struct mesh_host_queue_element_s {
	int         op;
	char        host[128];
	int         port;
	int         remove_fd;
	bool        seed;
} mesh_host_queue_element;

/* as_hb
 * Runtime information for the heartbeat system */
#define AS_HB_MAX_CALLBACKS 7
typedef struct as_hb_s {
	shash *adjacencies;
	shash *discovered_list;

	// [Note:  Ideally a hash table would be used to maintain the txlist instead of parallel arrays.]
#define AS_HB_TXLIST_SZ 1024 * 64
	bool endpoint_txlist[AS_HB_TXLIST_SZ];
	bool endpoint_txlist_isudp[AS_HB_TXLIST_SZ];
	cf_node endpoint_txlist_node_id[AS_HB_TXLIST_SZ]; // Node ID associated with a given mesh fd.

	struct epoll_event ev;
	int efd;

	union {
		cf_mcastsocket_cfg socket_mcast;
		cf_socket_cfg socket;
	};

	cf_clock time_start, time_last;

	/* Callbacks */
	int cb_sz;
	as_hb_event_fn cb[AS_HB_MAX_CALLBACKS];
	void *cb_udata[AS_HB_MAX_CALLBACKS];

	/* snub list + lock */
	pthread_mutex_t snub_lock;
	snub_list_element *snub_list; // array with 0 node terminating

	/* mesh host list + insert/delete queue */
	mesh_host_list_element *mesh_seed_host_list; // single linked list with null terminate
	mesh_host_list_element *mesh_non_seed_host_list; // list for non-seed  
	cf_queue *mesh_host_queue;

} as_hb;
as_hb g_hb;


#define AS_HB_MSG_TYPE_PULSE 0
#define AS_HB_MSG_TYPE_INFO_REQUEST 1
#define AS_HB_MSG_TYPE_INFO_REPLY 2

#define AS_HB_MSG_ID 0
#define AS_HB_MSG_TYPE 1
#define AS_HB_MSG_NODE 2
#define AS_HB_MSG_ADDR 3
#define AS_HB_MSG_PORT 4
#define AS_HB_MSG_ANV 5
#define AS_HB_MSG_ANV_LENGTH 6


/* as_hb_message
 * A message template for a heartbeat
 * Note:  There are two versions of the heartbeat protocol sharing this message template:
 *   Heartbeat protocol v1 doesn't have the ANV (Adjacent Node Vector) length.
 *   Heartbeat protocol v2 rightfully includes the length of the ANV
 *      so that it's possible to have peaceful coexistence and interoperability
 *      between nodes of different maximum cluster sizes. */
static const msg_template as_hb_msg_template[] = {
#define AS_HB_MSG_V1_IDENTIFIER 0x6862
#define AS_HB_MSG_V2_IDENTIFIER 0x6863
	{ AS_HB_MSG_ID, M_FT_UINT32 },
	{ AS_HB_MSG_TYPE, M_FT_UINT32 },
	{ AS_HB_MSG_NODE, M_FT_UINT64 },
	{ AS_HB_MSG_ADDR, M_FT_UINT32 },
	{ AS_HB_MSG_PORT, M_FT_UINT32 },
	{ AS_HB_MSG_ANV, M_FT_BUF },
	{ AS_HB_MSG_ANV_LENGTH, M_FT_UINT32 }
};

/* as_hb_pulse
 * A tracking structure for heartbeats
 * These structures are stored in the adjacency hash, indexed by node id
 */
typedef struct {
	uint64_t last;
	cf_sockaddr socket;         // if mcast, the socket we heard about the node
	uint32_t addr, port;        // if mesh, filled with the address and port
	int fd;                     // last fd we heard a pulse from, so as to avoid multiple fds between nodes
	bool new;                   // note if this is a new node
	bool updated;               // used by paxos and fabric health to detect when a node has just been dunned or undunned
	bool dunned;                // if true, node is "dunned". this will remove it from the paxos succession list, but not from the fabric's node list.
	uint64_t last_detected;     // use this for detecting nodes repeatedly until they are truly joined with the cluster
	cf_node principal;          // store the principal of the succession list that came with the pulse for this node
	cf_node anv[];              // store the succession list that came with the pulse for this node
} as_hb_pulse;


/* as_hb_monitor_reduce_udata
 * */
typedef struct {
	uint      n_delete;
	cf_node   delete[AS_CLUSTER_SZ];
	uint      n_insert;
	cf_node   insert[AS_CLUSTER_SZ];
	cf_node   insert_p_node[AS_CLUSTER_SZ];
	uint      n_dun;
	cf_node   dun[AS_CLUSTER_SZ];
	uint      n_undun;
	cf_node   undun[AS_CLUSTER_SZ];
	cf_node   undun_p_node[AS_CLUSTER_SZ];
} as_hb_monitor_reduce_udata;


//
// Forward references
//

static void as_hb_init_socket();
static void as_hb_reinit(int socket, bool isudp);
static int as_hb_endpoint_add(int socket, bool isudp, cf_node node_id);

/*
 * Enumeration of the types of heartbeat error events.
 * (Occurrence counts for these event types are incremented by calling "as_hb_error()".)
 */
typedef enum as_hb_err_type_e
{
	AS_HB_ERR_BAD_TYPE = 0,
	AS_HB_ERR_BAD_PULSE_FD,
	AS_HB_ERR_NO_TYPE,
	AS_HB_ERR_NO_ID,
	AS_HB_ERR_NO_NODE_PULSE,
	AS_HB_ERR_NO_NODE_REQ,
	AS_HB_ERR_NO_ANV_LENGTH,
	AS_HB_ERR_SENDTO_FAIL_1,
	AS_HB_ERR_SENDTO_FAIL_2,
	AS_HB_ERR_SENDTO_FAIL_3,
	AS_HB_ERR_SENDTO_FAIL_4,
	AS_HB_ERR_SENDTO_FAIL_5,
	AS_HB_ERR_SENDTO_FAIL_6,
	AS_HB_ERR_MISSING_FIELD,
	AS_HB_ERR_EXPIRE_HB,
	AS_HB_ERR_EXPIRE_FAB_DEAD,
	AS_HB_ERR_EXPIRE_FAB_ALIVE,
	AS_HB_ERR_UNPARSABLE_MSG,
	AS_HB_ERR_MESH_CONNECT_FAIL,
	AS_HB_ERR_REMOTE_CLOSE,
	AS_HB_ERR_MAX_TYPE
} as_hb_err_type;

// Message formats for logging heartbeat error event.
typedef enum as_hb_err_msg_format_e
{
	LONG_FORMAT = 0,
	SHORT_FORMAT
} as_hb_err_msg_format;

/*
 * Names of the types of heartbeat error events.
 * NOTE:  Must match the number and order of "as_hb_err_type".
 */
static char *as_hb_error_msg[AS_HB_ERR_MAX_TYPE][2] =
{
	{ "bad type", "bt", },
	{ "bad pulse fd", "bf", },
	{ "no type", "nt", },
	{ "no id", "ni", },
	{ "no node in pulse", "nn", },
	{ "no node in info request", "nnir", },
	{ "no anv length", "nal", },
	{ "sendto fail 1", "sf1", },
	{ "sendto fail 2", "sf2", },
	{ "sendto fail 3", "sf3", },
	{ "sendto fail 4", "sf4", },
	{ "sendto fail 5", "sf5", },
	{ "sendto fail 6", "sf6", },
	{ "missing required field", "mrf", },
	{ "expire hb", "eh", },
	{ "expire fab dead", "efd", },
	{ "expire fab alive", "efa", },
	{ "unparsable msg", "um" },
	{ "mesh connect fail", "mcf" },
	{ "remote close", "rc" }
};

// Occurrence counts for each type of heartbeat error detected.
static uint64_t as_hb_error_count[AS_HB_ERR_MAX_TYPE] = { 0 };

// Maxiumum line length for logging heartbeat errors.
#define MAX_LINE_LEN  (512)

// Module-global log message line for printing heartbeat errors.
static char g_line[MAX_LINE_LEN];

/*
 *  as_hb_error
 *  Increment the occurrence count for a heartbeat-related error of the given type.
 */
static void
as_hb_error(as_hb_err_type type)
{
	if ((0 <= type) && (type <= AS_HB_ERR_MAX_TYPE)) {
		as_hb_error_count[type]++;
	}
}

/*
 *  as_hb_stats
 *  Return a string summarizing the number of heartbeat-related errors of each type.
 *  Use long format messages if "verbose" is true, otherwise use short format messages.
 */
const char *
as_hb_stats(bool verbose)
{
	char msg[MAX_LINE_LEN];
	int pos = 0;
	g_line[pos] = '\0';

	for (int i = 0; i < AS_HB_ERR_MAX_TYPE; i++) {
		msg[0] = '\0';
		snprintf(msg, sizeof(msg), "%s %lu ", as_hb_error_msg[i][verbose ? LONG_FORMAT : SHORT_FORMAT], as_hb_error_count[i]);
		strncat(g_line, msg, sizeof(g_line) - pos);
	}

	return g_line;
}

/* as_hb_register
 * Register a heartbeat callback */
int
as_hb_register(as_hb_event_fn cb, void *udata)
{
	if (g_hb.cb_sz == AS_HB_MAX_CALLBACKS) {
		return(-1);
	}

	/* Insert the new callback */
	g_hb.cb[g_hb.cb_sz] = cb;
	g_hb.cb_udata[g_hb.cb_sz] = udata;
	g_hb.cb_sz++;

	return(0);
}

/* as_hb_getaddr
 * Get the socket address for a node - stashed in the adjacency hash */
int
as_hb_getaddr(cf_node node, cf_sockaddr *so)
{
	as_hb_pulse *a_p_pulse = NULL;

	if (!AS_HB_ENABLED()) {
		cf_debug(AS_HB, "heartbeat messaging disabled ~~ not returning an address");
		return(-1);
	}

	// NB: Stack allocation!!
	if (!(a_p_pulse = (as_hb_pulse *) alloca(AS_HB_PULSE_SIZE())))
		cf_crash(AS_HB, "failed to alloca() a heartbeat pulse of size %d", AS_HB_PULSE_SIZE());

	if (SHASH_ERR_NOTFOUND == shash_get(g_hb.adjacencies, &node, a_p_pulse))
		return(-1);
	if (AS_HB_MODE_MCAST == g_config.hb_mode)
		*so = a_p_pulse->socket;
	else
		*so = a_p_pulse->addr;

	/* Overwrite the port */
	unsigned short port = cf_nodeid_get_port(node);
	cf_sockaddr_setport(so, port);

	return(0);
}

// There can only be two open sockets to any other given node: one going in each direction.
#define AS_HB_MAX_OPEN_CONN_PER_NODE 2

typedef struct discovered_node_conn_s {
	int fd;
	// [TODO:  Add more parameters for debugging.]
} discovered_node_conn_t;

typedef struct discovered_node_hval_s {
	int                      num_conn;
	discovered_node_conn_t   conn[AS_HB_MAX_OPEN_CONN_PER_NODE];
} discovered_node_hval_t;


static void
init_discovered_node_conn(discovered_node_conn_t * ap_conn)
{
	if(ap_conn) {
		ap_conn->fd = -1;
	}
}

static void 
init_discovered_node_hval(discovered_node_hval_t * ap_hval)
{
	if(ap_hval) {
		ap_hval->num_conn  = 0;
		for ( int i = 0 ; i < AS_HB_MAX_OPEN_CONN_PER_NODE ; i++) {
			init_discovered_node_conn(&ap_hval->conn[i]);
		}
	}
}

static int
as_hb_nodes_discovered_hash_create()
{
	/* Create the discovered hash and zero the tx list */
	if (SHASH_OK != shash_create(&g_hb.discovered_list, cf_nodeid_shash_fn, sizeof(cf_node), sizeof(discovered_node_hval_t), 127, SHASH_CR_MT_MANYLOCK))
		cf_crash(AS_HB, "could not create nodes discovered hash table");

	return(0);
}

static bool
as_hb_nodes_discovered_hash_is_conn(cf_node node)
{

	pthread_mutex_t *vlock = NULL;
	bool ret = false;

	cf_detail(AS_HB, "as_hb_nodes_discovered_hash_is_conn  node %"PRIx64"", node);

	discovered_node_hval_t * p_hval = NULL;

	if (SHASH_ERR_NOTFOUND == shash_get_vlock(g_hb.discovered_list, &node, (void **)&p_hval, &vlock)) {
		cf_detail(AS_HB, "as_hb_nodes_discovered_hash_is_conn node %"PRIx64" not conn", node);
		return ret;
	}

	for ( int i = 0 ; i < AS_HB_MAX_OPEN_CONN_PER_NODE ; i++) {
		if (p_hval->conn[i].fd != -1) {
			cf_detail(AS_HB, "as_hb_nodes_discovered_hash_is_conn node %"PRIx64" conn[%d] fd:[%d] ", node, i, p_hval->conn[i].fd );
			ret = true;
			break;
		}
	}
	pthread_mutex_unlock(vlock);
	return ret;
}

static int
as_hb_nodes_discovered_hash_del_conn(cf_node node, int fd)
{
	pthread_mutex_t *vlock = NULL;
	int ret = -1;
	bool found = false;

	cf_detail(AS_HB, "as_hb_nodes_discovered_hash_del_conn  node %"PRIx64"", node);

	discovered_node_hval_t * p_hval = NULL;
	if (SHASH_ERR_NOTFOUND == shash_get_vlock(g_hb.discovered_list, &node, (void **)&p_hval, &vlock)) {
		cf_warning(AS_HB, "as_hb_nodes_discovered_hash_del_conn node %"PRIx64" not found", node);
	}
	else {
		cf_detail(AS_HB, "as_hb_nodes_discovered_hash_del_conn node %"PRIx64" fd:[%d] num curr conn before deleting %d", node, fd, p_hval->num_conn);
		for ( int i = 0 ; i < AS_HB_MAX_OPEN_CONN_PER_NODE ; i++ ) {
			if (p_hval->conn[i].fd == fd) {
				cf_detail(AS_HB, "as_hb_nodes_discovered_hash_del_conn node %"PRIx64" conn[%d] fd:[%d] ", node, i, fd);
				p_hval->conn[i].fd = -1;
				p_hval->num_conn--;
				found = true;
				ret = 0;
				break;
			}
		}
		cf_detail(AS_HB, "as_hb_nodes_discovered_hash_del_conn node %"PRIx64" fd:[%d] num curr conn after deleting %d", node, fd, p_hval->num_conn);
		pthread_mutex_unlock(vlock);
		if (!found) {
			cf_warning(AS_HB, "as_hb_nodes_discovered_hash_del_conn node %"PRIx64" fd:[%d] not found", node, fd);
		}
	}

	return(ret);
}

static int
as_hb_nodes_discovered_hash_shutdown_conn(cf_node node)
{
	pthread_mutex_t *vlock = NULL;
	int ret = 0;

	cf_detail(AS_HB, "as_hb_nodes_discovered_hash_shutdown_conn  node %"PRIx64"", node);

	discovered_node_hval_t * p_hval = NULL;
	if (SHASH_ERR_NOTFOUND == shash_get_vlock(g_hb.discovered_list, &node, (void **)&p_hval, &vlock)) {
		cf_warning(AS_HB, "as_hb_nodes_discovered_hash_shutdown_conn node %"PRIx64" not found", node);
	}
	else {
		cf_detail(AS_HB, "as_hb_nodes_discovered_hash_shutdown_conn node %"PRIx64" num curr conn %d", node, p_hval->num_conn);
		for ( int i = 0 ; i < AS_HB_MAX_OPEN_CONN_PER_NODE ; i++ ) {
			if (p_hval->conn[i].fd != -1) {
				cf_detail(AS_HB, "as_hb_nodes_discovered_hash_shutdown_conn node %"PRIx64" conn[%d] fd:[%d] ", node, i, p_hval->conn[i].fd);
				shutdown(p_hval->conn[i].fd, SHUT_RDWR);
			}
		}
		cf_detail(AS_HB, "as_hb_nodes_discovered_hash_shutdown_conn node %"PRIx64" num curr conn %d exiting", node, p_hval->num_conn);
		pthread_mutex_unlock(vlock);
	}
	return(ret);
}


static int
as_hb_nodes_discovered_hash_put_conn(cf_node node, int fd)
{
	pthread_mutex_t *vlock = NULL;
	int ret = 0;

	cf_detail(AS_HB, "as_hb_nodes_discovered_hash_put_conn  node %"PRIx64" fd %d", node, fd);

	discovered_node_hval_t * p_hval = NULL;

	discovered_node_hval_t l_hval;

	if (SHASH_ERR_NOTFOUND == shash_get_vlock(g_hb.discovered_list, &node, (void **)&p_hval, &vlock)) {
		cf_detail(AS_HB, "as_hb_nodes_discovered_hash_put_conn node %"PRIx64" not conn", node);
		init_discovered_node_hval(&l_hval);
		p_hval = &l_hval;
	}

	cf_detail(AS_HB, "as_hb_nodes_discovered_hash_put_conn node %"PRIx64" fd:[%d] num curr conn before adding %d", node, fd, p_hval->num_conn);

	for ( int i = 0 ; i < AS_HB_MAX_OPEN_CONN_PER_NODE ; i++) {
		if (p_hval->conn[i].fd == -1) {
			p_hval->conn[i].fd = fd;
			p_hval->num_conn++;
			cf_detail(AS_HB, "as_hb_nodes_discovered_hash_put_conn node %"PRIx64" conn[%d] fd:[%d]", node, i, p_hval->conn[i].fd );
			break;
		}
	}

	if (vlock) {
		pthread_mutex_unlock(vlock);
	} else {
		int rv = shash_put_unique(g_hb.discovered_list, &node, p_hval);
		if (rv == SHASH_ERR_FOUND) {
			cf_info(AS_HB, "as_hb_nodes_discovered_hash_put_conn node found in discovered list");
			ret = as_hb_nodes_discovered_hash_put_conn(node, fd); //calling recursively
		} else if (rv != 0) {
			cf_warning(AS_HB, "as_hb_nodes_discovered_hash_put_conn unable to update discovered_list");
			ret = -1;
		}
	}
	cf_detail(AS_HB, "as_hb_nodes_discovered_hash_put_conn node %"PRIx64" fd:[%d] num curr conn after addition %d", node, fd, p_hval->num_conn);
	return ret;
}

void
as_hb_process_fabric_heartbeat(cf_node node, int fd, cf_sockaddr socket, uint32_t addr, uint32_t port, cf_node *buf, size_t bufsz)
{
	as_hb_pulse *a_p_pulse = NULL, *p_pulse = NULL;
	pthread_mutex_t *vlock;

	cf_detail(AS_HB, "received fabric heartbeat from node %"PRIx64, node);

	if (!AS_HB_ENABLED()) {
		cf_debug(AS_HB, "heartbeat messaging disabled ~~ not processing fabric heartbeat");
		return;
	}

	// NB: Stack allocation!!
	if (!(a_p_pulse = (as_hb_pulse *) alloca(AS_HB_PULSE_SIZE())))
		cf_crash(AS_HB, "failed to alloca() a heartbeat pulse of size %d", AS_HB_PULSE_SIZE());

	if (SHASH_ERR_NOTFOUND == shash_get_vlock(g_hb.adjacencies, &node, (void **) &p_pulse, &vlock)) {
		p_pulse = a_p_pulse;
		memset(p_pulse, 0, AS_HB_PULSE_SIZE());
		vlock = 0;
		p_pulse->new = true;
	}

	p_pulse->last = cf_getms();
	cf_debug(AS_HB, "HB fabric (%"PRIx64"+%"PRIu64"): addr %08x port %d", node, p_pulse->last, addr, port);

	// Don't steal or remember the file descriptor from fabric. Fabric is using it,
	// and pain breaks out if we use it.
	// Mark Fabric-initiated heartbeat connections with a negative FD.
//	p_pulse->fd = - fd;

	// This was already 0'd in the case of new pulse, and clearing it for old pulse
	// seems dangerous.
//	memset(&p_pulse->socket, 0, sizeof(cf_sockaddr));

	// If this is a new pulse message, we will keep track of the socket and addr.
	// Not sure that keeping the socket is safe for MCAST, but going to try it for now.
	if (p_pulse->new) {
		if (AS_HB_MODE_MCAST == g_config.hb_mode) {
			p_pulse->socket = socket;
		} else if (AS_HB_MODE_MESH == g_config.hb_mode) {
			p_pulse->addr = addr;
			p_pulse->port = port;
		}
	}

	// copy the succession list into the pulse structure
	p_pulse->principal = buf[0]; // the first value is the principal-store this
	memcpy(p_pulse->anv, buf, bufsz);

	if (vlock) {
		pthread_mutex_unlock(vlock);
	} else {
		int rv = shash_put_unique(g_hb.adjacencies, &node, p_pulse);
		if (rv == SHASH_ERR_FOUND) {
			as_hb_process_fabric_heartbeat(node, fd, socket, addr, port, buf, bufsz);
		} else if (rv != 0) {
			cf_warning(AS_FABRIC, "unable to update adjacencies hash");
		}
	}
}

bool
as_hb_get_is_node_dunned(cf_node node)
{
	as_hb_pulse *a_p_pulse = NULL;

	if (node == g_config.self_node) {
		// Self is never dunned.
		return false;
	}

	if (!g_hb.adjacencies) {
		cf_warning(AS_HB, "no adjacency list ~~ considering all nodes dunned");
		return true;
	}

	// NB: Stack allocation!!
	if (!(a_p_pulse = (as_hb_pulse *) alloca(AS_HB_PULSE_SIZE())))
		cf_crash(AS_HB, "failed to alloca() a heartbeat pulse of size %d", AS_HB_PULSE_SIZE());

	if (SHASH_ERR_NOTFOUND == shash_get(g_hb.adjacencies, &node, a_p_pulse)) {
		return false;
	}

	return (a_p_pulse->dunned);
}

void
as_hb_set_is_node_dunned(cf_node node, bool is_dunned, char *context)
{
	if (node == g_config.self_node) {
		// don't dun/undun self
		return;
	}

	if (!g_hb.adjacencies) {
		cf_warning(AS_HB, "no adjacency list ~~ not setting node to %sdunned", (is_dunned ? "" : "un"));
		return;
	}

	as_hb_pulse *p_pulse;
	pthread_mutex_t *vlock;

	if (SHASH_ERR_NOTFOUND == shash_get_vlock(g_hb.adjacencies, &node, (void **) &p_pulse, &vlock)) {
		if (is_dunned) {
			cf_info(AS_HB, "could not dun node %"PRIx64" - node not found in node list", node);
		} else {
			cf_info(AS_HB, "could not undun node %"PRIx64" - node not found in node list", node);
		}
		return;
	}

	if (! p_pulse->new && p_pulse->dunned != is_dunned) {
		if (is_dunned) {
			cf_info(AS_HB, "%s dunning node %"PRIx64" (updated %d)", context, node, p_pulse->updated);
		} else {
			cf_info(AS_HB, "%s un-dunning node %"PRIx64" (updated %d)", context, node, p_pulse->updated);
		}

		p_pulse->dunned = is_dunned;

		// if p.updated was false, set p.updated to true so hb notifies the other parts of the system
		// if p.updates was true, the other parts of the system weren't first notified about the first change,
		// so don't bother updating them about the change back.
		p_pulse->updated = ! p_pulse->updated;
	}

	pthread_mutex_unlock(vlock);
}

typedef struct {
	cf_node nodes_to_dun[AS_CLUSTER_SZ];
	int n_nodes;
	cf_dyn_buf *db;
} nodes_to_dun_udata;

static int
get_nodes_to_dun(void *key, void *data, void *udata) {
	cf_node *node = (cf_node *) key;
	cf_node *node_list = (cf_node *) udata;

	int i;
	for (i = 0; node_list[i] != 0 && i < g_config.paxos_max_cluster_size; i++) {
		if (*node == node_list[i]) {
			return (0);
		}
	}

	if (i == g_config.paxos_max_cluster_size) {
		return (0);
	}

	nodes_to_dun_udata *u = (nodes_to_dun_udata *) udata;
	u->nodes_to_dun[u->n_nodes] = *node;

	if (u->n_nodes) {
		cf_dyn_buf_append_char(u->db, ',');
	}

	cf_dyn_buf_append_uint64_x(u->db, *node);
	u->n_nodes++;

	return (0);
}

/*
 *  as_hb_nodes_str_to_cf_nodes
 *  Parse a string of comma-separated hexadecimal node IDs into a list
 *  If successful, return the parsed, 64-bit numeric node IDs is the caller-supplied
 *  "nodes" array, which must be at least "AS_CLUSTER_SZ" in length.
 *  Return the number of nodes in "num_nodes" (if non-NULL.)
 *  Return 0 if successful, -1 otherwise.
 */
int
as_hb_nodes_str_to_cf_nodes(char *nodes_str, int nodes_str_len, cf_node *nodes, int *num_nodes)
{
	char *next_node = nodes_str, c;
	int n;

	for (n = 0; (nodes_str_len > 0) && (n < AS_CLUSTER_SZ - 1); n++) {
		int len = 0;
		while ('\0' != (c = next_node[len]) && isxdigit(c) && (len++ < 16))
			;

		if ('\0' != c) {
			if ((',' != c) || (0 == len)) {
				cf_info(CF_MISC, "node list not a valid format, must be a comma-separated list of 64-bit hex numbers");
				return -1;
			} else
				next_node[len++] = '\0';
		}

		if (0 != cf_str_atoi_u64_x(next_node, &nodes[n], 16)) {
			cf_info(CF_MISC, "node list not a valid format, expected 64-bit hex number, found %s", next_node);
			return -1;
		} else {
			nodes_str_len -= len;
			if (nodes_str_len > 0)
				next_node[len - 1] = ',';
			next_node += len;
		}
	}

	// Terminate the list.
	// Note: Maximum list length is AS_CLUSTER_SZ - 1.
	nodes[n + 1] = 0;

	if (num_nodes) {
		*num_nodes = n;
	}

	return 0;
}

int
as_hb_set_are_nodes_dunned(char *nodes_str, int nodes_str_len, bool is_dunned)
{
	cf_node nodes[AS_CLUSTER_SZ];
	int num_nodes = 0;
	bool all_nodes = false;

	if (!strcmp(nodes_str, "all")) {
		all_nodes = true;
		as_paxos_dun_hold(is_dunned);
	} else {
		if (as_hb_nodes_str_to_cf_nodes(nodes_str, nodes_str_len, nodes, &num_nodes)) {
			cf_warning(AS_HB, "%sdun command: failed to parse node list string (\"%s\") to nodes", (is_dunned ? "" : "un"), nodes_str);
			return -1;
		}

		cf_node *nodes_p = nodes;
		bool self_in_list = false;
		while (*nodes_p) {
			if (*nodes_p++ == g_config.self_node) {
				self_in_list = true;
				break;
			}
		}

		all_nodes = self_in_list;
	}

	if (all_nodes) {
		cf_dyn_buf_define(db);

		nodes_to_dun_udata u;
		memset(u.nodes_to_dun, 0, sizeof(u.nodes_to_dun));
		u.n_nodes = 0;
		u.db = &db;

		if (!g_hb.adjacencies) {
			cf_warning(AS_HB, "no adjacencies list ~~ not (un)dunning nodes");
			return -1;
		}

		shash_reduce(g_hb.adjacencies, get_nodes_to_dun, &u);

		if (u.n_nodes > 0) {
			cf_dyn_buf_append_char(u.db, 0);

			cf_info(AS_HB, "[self in list] %sdunning nodes: %s", (is_dunned ? "" : "un-"), u.db->buf);

			for (int i = 0; i < u.n_nodes; i++) {
				as_hb_set_is_node_dunned(u.nodes_to_dun[i], is_dunned, "info command");
			}
		} else
			cf_info(AS_HB, "[self in list] no nodes to %sdun", (is_dunned ? "" : "un-"), nodes_str);
	} else {
		if (num_nodes > 0) {
			cf_info(AS_HB, "%sdunning nodes: %s", (is_dunned ? "" : "un-"), nodes_str);

			for (int i = 0; i < num_nodes; i++)
				as_hb_set_is_node_dunned(nodes[i], is_dunned, "info command");
		} else
			cf_info(AS_HB, "no nodes to %sdun", (is_dunned ? "" : "un-"), nodes_str);
	}

	return 0;
}

int
as_hb_fb_health_cb(cf_node node, fb_health_status status, void *udata) {
	switch (status) {
		case FB_HEALTH_OK:
			if (g_config.auto_undun) {
				as_hb_set_is_node_dunned(node, false, "fabric health");
			}
			break;
		case FB_HEALTH_BAD_NODE:
		case FB_HEALTH_BAD_CLUSTER:
			if (g_config.auto_dun) {
				as_hb_set_is_node_dunned(node, true, "fabric health");
			}
			break;
	}
	return (0);
}

/*
** as_hb_snub
**
*/

//
// internal helper function removes a particular offset - hold lock to use this
//

static void
as_hb_snub_remove(int i)
{
	// find length
	snub_list_element *e = g_hb.snub_list;
	int ll = 0;
	while (e[ll].node) {
		ll++;
	}

	// validate length
	if (i >= ll) {
		cf_warning(AS_HB, "internal error managing snub list: investigate");
		return;
	}

	cf_debug(AS_HB, "remove snub : node %"PRIx64" no longer snubbed", e[i].node);

	// special case: only one to delete
	if (ll == 1) {
		cf_detail(AS_HB, " snub size one");
		cf_free(g_hb.snub_list);
		g_hb.snub_list = 0;
		return;
	}

	// overlapping copy for sure
	// this looks a little weird but is right. ll is the length minus 1 (not including
	// the null element), but you need to copy down the null element
	cf_detail(AS_HB, " snub size: ll %d i %d", ll, i);
	memmove(&e[i], &e[i + 1], sizeof(snub_list_element) * ((ll + 1) - i));
	g_hb.snub_list = cf_realloc(e, sizeof(snub_list_element) * ll);

}

bool
as_hb_is_snubbed(cf_node node)
{
	// quick cut-through for 99% of cases
	if (g_hb.snub_list == 0) {
		return(false);
	}

	bool rv = false;

	pthread_mutex_lock(&g_hb.snub_lock);

	snub_list_element *e = g_hb.snub_list;

	for (int i = 0; e[i].node ; i++) {
		if (e[i].node == node) {
			// side effect of snub-remove is possibly freeing the list,
			// so don't touch e after removal
			if (e[i].expiration < cf_getms()) {
				as_hb_snub_remove(i);
				goto Out;
			}
			rv = true;
			goto Out;
		}
	}

Out:
	pthread_mutex_unlock(&g_hb.snub_lock);
	return(rv);

}

int
as_hb_snub(cf_node node, cf_clock ms)
{
	cf_debug(AS_HB, "snub : node %"PRIx64" time %"PRIu64, node, ms);

	pthread_mutex_lock(&g_hb.snub_lock);

	snub_list_element *l = g_hb.snub_list;

	// adding node to list
	if (ms > 0) {

		snub_list_element *l = g_hb.snub_list;

		if (l == 0) {
			l = g_hb.snub_list = cf_malloc(sizeof(snub_list_element) * 2);
			l[0].node = node;
			l[0].expiration = ms + cf_getms();
			l[1].node = 0;
			l[1].expiration = 0;

			cf_detail(AS_HB, "snub : added first node %"PRIx64" for %"PRIu64" ms", node, ms);
		} else {
			// check dups + find length
			int ll = 0;
			while (l[ll].node) {
				if (l[ll].node == node) {
					l[ll].expiration = ms + cf_getms();
					goto Out;
				}
				ll++;
			}
			l = g_hb.snub_list = cf_realloc(g_hb.snub_list, sizeof(snub_list_element) * (ll + 2));
			l[ll].node = node;
			l[ll].expiration = ms + cf_getms();
			l[ll + 1].node = 0;
			l[ll + 1].expiration = 0;
			cf_detail(AS_HB, "snub : added node %"PRIx64" for %"PRIu64" ms", node, ms);
			goto Out;
		}
	} else { // removing from list
		for (int i = 0; l[i].node; i++) {
			if (l[i].node == node) {
				as_hb_snub_remove(i);
				goto Out;
			}
		}
	}

Out:
	pthread_mutex_unlock(&g_hb.snub_lock);
	return(-1);
}

//
// MESH HOST LIST CONNECTIVITY
//
//
#define MESH_RETRY_INTERVAL (2 * 1000)
void as_hb_try_connecting_remote(mesh_host_list_element * e)
{
	while (e) {
		if ( (e->fd == -1) && (e->next_try < cf_getms()) ) {

			// found one to try
			cf_socket_cfg s;
			s.addr = e->host;
			s.port = e->port;
			s.proto = SOCK_STREAM;

			cf_debug(AS_HB, "attempting to connect mesh host at %s:%d", e->host, e->port);

			if ( 0 != cf_socket_init_client(&s, ((g_config.hb_interval * g_config.hb_timeout) / 2)) ) {
				cf_debug(AS_HB, "could not create heartbeat connection to node %s:%d", e->host, e->port);
				// Retry after waiting 10x longer than timeout.
				e->next_try = cf_getms() + 10 * (g_config.hb_interval * g_config.hb_timeout);
				e = e->next;
				continue;
			}
			struct sockaddr_in addr_in;
			socklen_t addr_len = sizeof(addr_in);

			char some_addr[24];
			some_addr[0] = 0;

			struct sockaddr_in addr_in2;
			socklen_t addr_len2 = sizeof(addr_in2);

			char some_addr2[24];
			some_addr2[0] = 0;

			// Try to get the client details for better logging.
			// Otherwise, fall back to generic log message.
			if (getpeername(s.sock, (struct sockaddr*)&addr_in, &addr_len) == 0
					&& inet_ntop(AF_INET, &addr_in.sin_addr.s_addr, (char *)some_addr, sizeof(some_addr)) != NULL) {

				if (getsockname(s.sock, (struct sockaddr*)&addr_in2, &addr_len2) == 0
					&& inet_ntop(AF_INET, &addr_in2.sin_addr.s_addr, (char *)some_addr2, sizeof(some_addr2)) != NULL) {

					cf_info(AS_HB, "initiated connection to mesh host at %s:%d (%s:%d) via socket %d from %s:%d",
							some_addr, ntohs(addr_in.sin_port), e->host, e->port, s.sock, some_addr2, ntohs(addr_in2.sin_port));
				} else {
					cf_warning(AS_HB, "getsockname() failed: %s", cf_strerror(errno));
				}
			} else {
				as_hb_error(AS_HB_ERR_MESH_CONNECT_FAIL);
				cf_debug(AS_HB, "failed - initiated connection to mesh host at %s:%d socket %d from %s:%d", e->host, e->port, s.sock,some_addr, ntohs(addr_in.sin_port));
			}
			cf_atomic_int_incr(&g_config.heartbeat_connections_opened);

			// simply adds the socket to the epoll list
			// if this call fails, sock has been eaten.
			// node id will be set for this socket after 
			// receiving HB pulse as that is the place we are 
			// populating adjancency list also.
			if (0 != as_hb_endpoint_add(s.sock, false /*is not udp*/, 0 )) { 
				close(s.sock);
			} else {
				e->fd = s.sock;
			}
		}
		e = e->next;
	}
}

//
// run the mesh list and try to connect to anything we've been configured
// or TIPped to.
//
// The code as it stands is slightly problematic. It would be better to use a queue
// between the 'add' function and the 'service' function. Also not clear if service should
// be running on its own thread - can these connect calls be very hang-ish?
//

void *
mesh_list_service_fn(void *arg)
{
	cf_debug(AS_HB, "starting mesh list service");

	do {

		mesh_host_list_element *e = 0;

		if (cf_queue_sz(g_hb.mesh_host_queue) > 0) cf_debug(AS_HB, "mesh list service: servicing queue");

		// Get all elements off work queue and do them
		mesh_host_queue_element mhqe;
		if (CF_QUEUE_OK == cf_queue_pop( g_hb.mesh_host_queue, &mhqe, ((g_config.hb_interval * g_config.hb_timeout) / 2)) ) { // putting timed wait time equal to half of HB timeout

			if (mhqe.op == MH_OP_REMOVE_ALL) {

				cf_debug(AS_HB, "removing all hosts from mesh list");

				while (g_hb.mesh_seed_host_list) {
					e = g_hb.mesh_seed_host_list;
					g_hb.mesh_seed_host_list = e->next;
					if (e->fd) shutdown(e->fd, SHUT_RDWR);
					cf_free(e);
				}
			} else if (mhqe.op == MH_OP_ADD) {

				// check uniqueness
				e = g_hb.mesh_seed_host_list;
				while (e) {
					if ((0 == strcmp(mhqe.host, e->host)) && (mhqe.port == e->port)) {
						cf_debug(AS_HB, "attempt to add duplicate host to mesh seed host list, ignored");
						goto TryConnect;
					}
					e = e->next;
				}
				// duplicate check in non seed list
				e = g_hb.mesh_non_seed_host_list;
				while (e) {
					if ((0 == strcmp(mhqe.host, e->host)) && (mhqe.port == e->port)) {
						cf_debug(AS_HB, "attempt to add duplicate host to mesh non seed host list, ignored");
						goto TryConnect;
					}
					e = e->next;
				}

				cf_debug(AS_HB, "adding %s:%d to mesh host lists", mhqe.host, mhqe.port);

				// add to list
				e = cf_malloc(sizeof(mesh_host_list_element));
				if (!e) cf_crash(AS_HB, "unable to allocate memory for mesh host list");
				if (mhqe.seed) { // adding into seed list
					e->next = g_hb.mesh_seed_host_list;
					g_hb.mesh_seed_host_list = e;
				} else { // else adding into discovered list
					e->next = g_hb.mesh_non_seed_host_list;
					g_hb.mesh_non_seed_host_list = e;
				}
				strcpy(e->host, mhqe.host);
				e->port = mhqe.port;
				e->fd = -1;
				e->next_try = 0;
			} else if (mhqe.op == MH_OP_REMOVE_FD) {

				cf_debug(AS_HB, "removing fd %d from mesh host list", mhqe.remove_fd);

				//checking in seed host list
				e = g_hb.mesh_seed_host_list;
				while (e) {
					if (e->fd == mhqe.remove_fd) {
						cf_debug(AS_HB, "seed node putting it for retry fd %d",  mhqe.remove_fd);
						e->fd = -1;
						e->next_try = cf_getms() + MESH_RETRY_INTERVAL;
						goto TryConnect;
					}
					e = e->next;
				}
				//checking in non seed host list
				e = g_hb.mesh_non_seed_host_list;
				mesh_host_list_element * trailing_e = NULL;
				while (e) {
					cf_detail(AS_HB, "non seed list fd %d", e->fd);
					if (e->fd == mhqe.remove_fd) {
						cf_debug(AS_HB, "actually removing fd %d",  mhqe.remove_fd);
						if ( trailing_e == NULL ) { //e is first node
							g_hb.mesh_non_seed_host_list = e->next;
						} else {
							trailing_e->next = e->next;
						}
						cf_free(e);
						e = NULL;
						goto TryConnect;
					}
					trailing_e = e;
					e = e->next;
				}

				cf_debug(AS_HB, "did NOT find fd %d in mesh host list",  mhqe.remove_fd);

			} else {
				cf_warning(AS_HB, "recieved bad op service queue message: %d internal error", mhqe.op);
			}

TryConnect:
			;

		}

		// Try any connections that might be a good idea

		as_hb_try_connecting_remote(g_hb.mesh_non_seed_host_list);
		e = g_hb.mesh_non_seed_host_list;
		mesh_host_list_element * prev_element = NULL;
		while (e) {
			cf_debug(AS_HB, "traversing list to remove non connected fds current fd %d", e->fd);
			if (e->fd == -1) {
				if ( prev_element == NULL ) { //e is first node
					g_hb.mesh_non_seed_host_list = e->next;
					cf_free(e);
					e = g_hb.mesh_non_seed_host_list;
				} else {
					prev_element->next = e->next;
					cf_free(e);
					e = prev_element->next;
				}
			} else {
				prev_element = e;
				e = e->next;
			}
		}
		
		as_hb_try_connecting_remote(g_hb.mesh_seed_host_list);

	} while (1);

	return(0);
}

int
mesh_host_list_remove_fd(int fd)
{
	mesh_host_queue_element mhqe;
	memset(&mhqe, 0, sizeof(mhqe));
	mhqe.op = MH_OP_REMOVE_FD;
	mhqe.remove_fd = fd;
	cf_queue_push(g_hb.mesh_host_queue, (void *) &mhqe);

	return(0);
}

int
mesh_host_list_add(char *host, int port, bool is_seed)
{
	// validate input
	if (port > (1 << 16)) {
		cf_info(AS_HB, "mesh host list add: invalid input: port %d out of range", port);
		return(-1);
	}
	if (strlen(host) > 127) {
		cf_info(AS_HB, "mesh host list add: invalid input: hostname %s too big", host);
		return(-1);
	}

	cf_debug(AS_HB, "Mesh host list: queuing (not yet added) %s:%d", host, port);

	// check that it's not myself
	if ((0 == strcmp(g_config.hb_addr, host)) && (g_config.hb_port == port)) {
		cf_debug(AS_HB, "rejected tip for self: %s:%d", host, port);
		return(0);
	}
	if ((0 == strcmp("127.0.0.1", host)) && (g_config.hb_port == port)) {
		cf_debug(AS_HB, "rejected tip for self2: %s:%d", host, port);
		return(0);
	}

	// place on queue
	mesh_host_queue_element mhqe;
	memset(&mhqe, 0, sizeof(mhqe));
	mhqe.op = MH_OP_ADD;
	strcpy(mhqe.host, host);
	mhqe.port = port;
	mhqe.remove_fd = -1;
	mhqe.seed = is_seed;
	cf_queue_push(g_hb.mesh_host_queue, (void *) &mhqe);

	return(0);
}

//
// TIP is an external control function that adds an IP address to the
// list of configured MESH addresses.
// It TIPs you off to good possible
// The char * is used only during the call ---
// and the call doesn't do the connect inline (?)

int
as_hb_tip(char *host, int port)
{
	cf_debug(AS_HB, " Heartbeat: tipped about server at %s:%d", host, port);

	mesh_host_list_add(host, port, true);

	return(0);
}

int
as_hb_tip_clear()
{
	cf_debug(AS_HB, " Heartbeat: clearing tip list");

	mesh_host_queue_element mhqe;
	memset(&mhqe, 0, sizeof(mhqe));
	mhqe.op = MH_OP_REMOVE_ALL;
	cf_queue_push(g_hb.mesh_host_queue, (void *) &mhqe);
	return(0);
}

static int
as_hb_adjacencies_create()
{
	/* Create the adjacency hash and zero the tx list */
	if (SHASH_OK != shash_create(&g_hb.adjacencies, cf_nodeid_shash_fn, sizeof(cf_node), AS_HB_PULSE_SIZE(), 127, SHASH_CR_MT_MANYLOCK))
		cf_crash(AS_HB, "could not create adjacency hash table");
	memset(g_hb.endpoint_txlist, 0, sizeof(g_hb.endpoint_txlist));
	memset(g_hb.endpoint_txlist_isudp, 0, sizeof(g_hb.endpoint_txlist_isudp));
	memset(g_hb.endpoint_txlist_node_id, 0, sizeof(g_hb.endpoint_txlist_node_id));

	return(0);
}

static int
as_hb_adjacencies_destroy()
{
	shash *old_adjacencies = g_hb.adjacencies;
	g_hb.adjacencies = NULL;
	shash_destroy(old_adjacencies);

	return(0);
}

static int
as_hb_start_receiving(int socket, int was_udp, cf_node node_id)
{
	cf_debug(AS_HB, "Heartbeat: starting packet receive on socket fd %d", socket);

	if (!g_hb.adjacencies)
		as_hb_adjacencies_create();

	if (0 > epoll_ctl(g_hb.efd, EPOLL_CTL_ADD, socket, &g_hb.ev))
		cf_crash(AS_HB,  "unable to add socket %d to epoll fd list: %s", socket, cf_strerror(errno));

	g_hb.endpoint_txlist[socket] = true;
	g_hb.endpoint_txlist_isudp[socket] = was_udp;
	g_hb.endpoint_txlist_node_id[socket] = node_id; 
	if ( node_id != 0 ) { 
		//add this connection to discovered hash
		as_hb_nodes_discovered_hash_put_conn (node_id, socket);
	}

	return(0);
}

static int
as_hb_stop_receiving()
{
	int socket = g_hb.socket_mcast.s.sock;

	cf_debug(AS_HB, "Heartbeat: stopping packet receive on socket fd %d", socket);

	if (0 > epoll_ctl(g_hb.efd, EPOLL_CTL_DEL, socket, &g_hb.ev))
		cf_crash(AS_HB,  "unable to remove socket %d from epoll fd list: %s", socket, cf_strerror(errno));

	g_hb.endpoint_txlist[socket] = false;
	bool was_udp = g_hb.endpoint_txlist_isudp[socket];
	g_hb.endpoint_txlist_isudp[socket] = false;

	as_hb_adjacencies_destroy();

	return was_udp;
}

// Set the heartbeat protocol version.
// XXX -- Currently does not support the "mesh" transport correctly.
int
as_hb_set_protocol(hb_protocol_enum protocol)
{
	static bool s_was_udp = false;
	int socket = g_hb.socket_mcast.s.sock;

	if (g_config.hb_protocol == protocol) {
		cf_info(AS_HB, "no heartbeat protocol change needed");
		return(0);
	}

	if (AS_HB_MODE_MCAST != g_config.hb_mode) {
		cf_warning(AS_HB, "setting heartbeat protocol is only supported in heartbeat mode \"multicast\"");
		return(-1);
	}

	switch (protocol) {
		case AS_HB_PROTOCOL_V1:
		case AS_HB_PROTOCOL_V2:
			cf_info(AS_HB, "setting heartbeat protocol version number to %d", protocol);

			if (AS_HB_PROTOCOL_V1 == protocol && AS_CLUSTER_LEGACY_SZ != g_config.paxos_max_cluster_size) {
				cf_warning(AS_HB, "setting heartbeat protocol version v1 only allowed when paxos_max_cluster_size = %d not the current value of %d",
						   AS_CLUSTER_LEGACY_SZ, g_config.paxos_max_cluster_size);
				return(-1);
			}

			if (AS_HB_PROTOCOL_NONE != g_config.hb_protocol) {
				cf_info(AS_HB, "first disabling current heatbeat protocol (%d)", g_config.hb_protocol);
				s_was_udp = as_hb_stop_receiving();
				g_config.hb_protocol = AS_HB_PROTOCOL_NONE;
			}

			as_hb_start_receiving(socket, s_was_udp, 0 /*multicast node id not required*/);
			g_config.hb_protocol = protocol;
			break;

		case AS_HB_PROTOCOL_NONE:
			cf_info(AS_HB, "disabling heartbeat messaging");
			g_config.hb_protocol = protocol;
			s_was_udp = as_hb_stop_receiving();
			break;

		case AS_HB_PROTOCOL_RESET:
			if (AS_HB_PROTOCOL_NONE == g_config.hb_protocol) {
				cf_info(AS_HB, "heartbeat messaging disabled ~~ not resetting");
				return(-1);
			}

			// NB: "hb_protocol" is never actually set to "RESET" ~~ it is simply a trigger for the reset action.
			cf_info(AS_HB, "resetting heartbeat messaging");
			hb_protocol_enum saved_hb_protocol = g_config.hb_protocol;

			cf_info(AS_HB, "first disabling current heatbeat protocol (%d)", g_config.hb_protocol);
			g_config.hb_protocol = AS_HB_PROTOCOL_NONE;
			s_was_udp = as_hb_shutdown();

			as_hb_reinit(socket, s_was_udp);
			g_config.hb_protocol = saved_hb_protocol;
			break;

		default:
			cf_warning(AS_HB, "unknown heartbeat protocol version number: %d", protocol);
			return(-1);
	}

	return(0);
}

/* as_hb_endpoint_add
 * Add a new endpoint to listen to */
static int
as_hb_endpoint_add(int socket, bool isudp, cf_node node_id)
{
	if (socket >= AS_HB_TXLIST_SZ) {
		cf_info(AS_HB, "attempting to add heartbeat: socket fd %d too large", socket);
		return(-1);
	}

	/* Make the socket nonblocking */
	if (-1 == cf_socket_set_nonblocking(socket)) {
		cf_info(AS_HB, "unable to set client socket %d to nonblocking mode: %s", socket, cf_strerror(errno));
		cf_atomic_int_incr(&g_config.heartbeat_connections_closed);
		return(-1);
	}
	cf_socket_set_nodelay(socket);

	/* Put the socket in the event queue and update the transmit list */
	g_hb.ev.events = EPOLLIN | EPOLLERR | EPOLLRDHUP;  // level-triggered!
	g_hb.ev.data.fd = socket;

	as_hb_start_receiving(socket, isudp, node_id);

	return(0);
}


/* as_hb_tcp_close
 * closes hb tcp socket and removed it from discovered hash*/
static void
as_hb_tcp_close(int fd)
{
	cf_detail(AS_HB, "as_hb_tcp_close():  Closing HB TCP fd %d to node %"PRIx64, fd, g_hb.endpoint_txlist_node_id[fd]);
	g_hb.endpoint_txlist[fd] = false;
	// Remove node from discovered list.
	if (g_hb.endpoint_txlist_node_id[fd]) {
		as_hb_nodes_discovered_hash_del_conn(g_hb.endpoint_txlist_node_id[fd], fd);
	}
	g_hb.endpoint_txlist_node_id[fd] = 0;
	mesh_host_list_remove_fd(fd);
	shutdown(fd, SHUT_RDWR);
}

/* as_hb_tcp_send
 * send hb protocol message and retry after 100 u sec in case of EAGAIN 
 * and EWOULDBLOCK*/

static int 
as_hb_tcp_send(int fd, byte * buff, size_t msg_size) 
{  
	int ret = 0;
	int retry = 0;
	const int max_retry = 3;
	size_t orig_size = msg_size;
	cf_clock start = cf_getus();
	do {
		cf_detail(AS_HB, "cf_socket_sendto() fd %d retry count:%d msg_size:%d", fd, retry, msg_size);
		ret =  cf_socket_sendto(fd, buff, msg_size, 0, 0);
		if( ( ret < 0 ) && (( errno != EAGAIN ) || ( errno != EWOULDBLOCK ))) {
			cf_info(AS_HB, "as_hb_tcp_send cf_socket_sendto() fd %d failed", fd);
			as_hb_tcp_close(fd);
			return -1;
		} else if (ret > 0) {
			buff += ret; //incrementing buff pointer
			msg_size -= ret;//decreasing msg_size to be sent
		}

		if (msg_size > 0) {
			retry++;
			usleep(g_config.hb_mesh_rw_retry_timeout/3);
		} else {
			cf_detail(AS_HB, "cf_socket_sendto() fd %d retry count:%d msg_size:%d complete msg sent", fd, retry, msg_size);
			break;
		}
	} while ( ((start + g_config.hb_mesh_rw_retry_timeout) > cf_getus()) && (retry < max_retry) );

	if ( (msg_size != 0)  && (msg_size != orig_size) ) {
		cf_warning(AS_HB, "as_hb_tcp_send cf_socket_sendto() fd %d incomplete msg sent. %d bytes left out of %d bytes.", fd, msg_size, orig_size);
		as_hb_tcp_close(fd); //closing fd
		return -1;
	}
	return (orig_size - msg_size);
}

/* as_hb_tcp_recv
 * recv hb protocol message and retry after 100 u sec in case of EAGAIN 
 * and EWOULDBLOCK*/
static int 
as_hb_tcp_recv(int fd, byte * buff, size_t msg_size)
{
	int ret = 0;
	uint32_t len = 0;
	int flags = (MSG_NOSIGNAL | MSG_PEEK);
	if (0 >= (ret = recv(fd, buff, 4, flags))) {
		cf_warning(AS_HB, "as_hb_tcp_recv() fd %d recv peek error", fd);
		return ret;
	} 
	if (ret != 4) {
		cf_warning(AS_HB, "as_hb_tcp_recv() fd %d not even 4 bytes", fd);
		return -1;
	}
	len = ntohl(*((uint32_t *)buff))  + 6;
	if (msg_size < len) {
		cf_warning(AS_HB, "as_hb_tcp_recv() fd %d buffer size insufficient", fd);
		return -1;
	}

	int try = 0;
	const int max_try = 3;
	int read_so_far = 0;
	cf_clock start = cf_getus();
	do {
		cf_detail(AS_HB, "as_hb_tcp_recv() fd %d try %d len %d", fd, try, len);
		if (0 >= (ret = recv(fd, (buff + read_so_far), len - read_so_far, MSG_NOSIGNAL))) {
			if(errno == EAGAIN || errno == EWOULDBLOCK) {
				try++;
				usleep(g_config.hb_mesh_rw_retry_timeout/3);
				continue;
			}
			cf_info(AS_HB, "as_hb_tcp_recv() fd %d recv error try %d", fd, try);
			return ret;
		} 

		read_so_far += ret;
		if(read_so_far < len) {
			cf_detail(AS_HB, "as_hb_tcp_recv() recv success fd %d try %d read_so_far %d ", fd, try, read_so_far);
			try++;
			usleep(g_config.hb_mesh_rw_retry_timeout/3);
		} else {
			cf_detail(AS_HB, "as_hb_tcp_recv() recv success fd %d try %d len %d ", fd, try, len);
			return len;
		}
	} while ( (try < max_try) && ((start + g_config.hb_mesh_rw_retry_timeout) > cf_getus()) );
	cf_info(AS_HB, "as_hb_tcp_recv() fd %d returning error try %d len %d read_so_far %d", fd, try, len, read_so_far);
	return -1;
}

/* as_hb_rx_process
 * Process a received heartbeat */
void
as_hb_rx_process(msg *m, cf_sockaddr so, int fd)
{
	cf_node node;
	as_hb_pulse *a_p_pulse = NULL;
	uint32_t addr, port, type = 8888;
	cf_node *buf;
	size_t bufsz;

	if (!AS_HB_ENABLED()) {
		cf_debug(AS_HB, "heartbeat messaging disabled ~~ not rx processing heartbeats");
		return;
	}

	// NB: Stack allocation!!
	if (!(a_p_pulse = (as_hb_pulse *) alloca(AS_HB_PULSE_SIZE())))
		cf_crash(AS_HB, "failed to alloca() a heartbeat pulse of size %d", AS_HB_PULSE_SIZE());

	if (0 > msg_get_uint32(m, AS_HB_MSG_TYPE, &type)) {
		cf_detail(AS_HB, "unable to get type field");
		as_hb_error(AS_HB_ERR_NO_TYPE);
		return;
	}

	cf_detail(AS_HB, "received message type %d", type);

	switch (type) {
		case AS_HB_MSG_TYPE_PULSE:
			/* Ignore messages from ourselves */
			if (0 > msg_get_uint64(m, AS_HB_MSG_NODE, &node)) {
				cf_detail(AS_HB, "unable to get node ID");
				as_hb_error(AS_HB_ERR_NO_NODE_PULSE);
				return;
			}

			if (node == g_config.self_node) {
				cf_atomic_int_incr(&g_config.heartbeat_received_self);
				return;
			}

			uint64_t now = cf_getms();

			if (g_config.snub_nodes) {
				/* ignore messages from snubbed nodes */
				if (as_hb_is_snubbed(node)) {
					cf_debug(AS_HB, "HB SNUBBED (%"PRIx64"+%"PRIu64")", node, now);
					return;
				} else {
					cf_debug(AS_HB, "HB (%"PRIx64"+%"PRIu64")", node, now);
				}
			}

			/* Make sure this is actually a heartbeat message. */
			uint32_t c;
			if (0 > msg_get_uint32(m, AS_HB_MSG_ID, &c)) {
				cf_detail(AS_HB, "received heartbeat message without a valid ID");
				as_hb_error(AS_HB_ERR_NO_ID);
				return;
			}

			/* Check the protocol. */
			if (AS_HB_PROTOCOL_IDENTIFIER() != c) {
				cf_warning(AS_HB, "received heartbeat message not for the currently active protocol version (received 0x%04x ; expected 0x%04x) ~~ Ignoring message!", c, AS_HB_PROTOCOL_IDENTIFIER());
				return;
			}

			/* The heartbeat protocol v2 or greater provides a means of peaceful coexistence between nodes with different maximum cluster sizes:
			   If the adjacent node vector (ANV) of the incoming message does not agree with our maximum cluster size, simply ignore it. */
			if (AS_HB_MSG_V1_IDENTIFIER != c) {
				if (0 > msg_get_uint32(m, AS_HB_MSG_ANV_LENGTH, &c)) {
					cf_detail(AS_HB, "Received heartbeat protocol v%d message without ANV length ~~ Ignoring message!", AS_HB_PROTOCOL_VERSION_NUMBER(c));
					as_hb_error(AS_HB_ERR_NO_ANV_LENGTH);
					return;
				}
				if (c != g_config.paxos_max_cluster_size) {
					cf_warning(AS_HB, "Received heartbeat message with a different maximum cluster size (received %d ; expected %d) ~~ Ignoring message!", c, g_config.paxos_max_cluster_size);
					return;
				}
			}

			cf_atomic_int_incr(&g_config.heartbeat_received_foreign);

			// If this node encounters other nodes at startup, prevent it from
			// switching to a single-node cluster.
			as_partition_balance_init_multi_node_cluster();

			/* Update the node's entry in the adjacencies hash */
			/* COPY the data into p_pulse */

			as_hb_pulse *p_pulse;
			pthread_mutex_t *vlock = NULL;

			// Add the association between this node ID and mesh fd.
			if ((AS_HB_MODE_MESH == g_config.hb_mode) && (g_hb.endpoint_txlist_node_id[fd] == 0)) {
				g_hb.endpoint_txlist_node_id[fd] = node;
				as_hb_nodes_discovered_hash_put_conn(node, fd);
			}

			int rv = shash_get_vlock(g_hb.adjacencies, &node, (void **) &p_pulse, &vlock);
			if (rv == SHASH_ERR_NOTFOUND) {
				memset(a_p_pulse, 0, AS_HB_PULSE_SIZE());
				p_pulse = a_p_pulse;
				vlock = 0;
				p_pulse->new = true;
			} else if (rv == SHASH_OK) {
				if (p_pulse->fd != fd) {
					cf_detail(AS_HB, "received same pulse from other fd, surprising");
					if (0 > p_pulse->fd) {
						cf_detail(AS_HB, "Re-setting Fabric-opened HB fd %d to the current fd %d", - p_pulse->fd, fd);
						p_pulse->fd = fd;
					} else {
						cf_detail(AS_HB, "Bad Pulse FD: pulse says %d ; received on %d", p_pulse->fd, fd);
						as_hb_error(AS_HB_ERR_BAD_PULSE_FD);
					}
					// cf_warning(AS_HB, "closing redundant HB socket fd %d (same as addr 0x%08x port %d fd %d)", fd, p_pulse->addr, p_pulse->port, p_pulse->fd);
					// shutdown(fd, SHUT_RDWR);
					// return;
				}
			}

			p_pulse->last = now;
			// this is a really interesting print, because it shows the true smoothness of
			// receiving other's heartbeats
			p_pulse->fd = fd;
			if (AS_HB_MODE_MCAST == g_config.hb_mode) {
				p_pulse->socket = so;
				p_pulse->port = p_pulse->addr = 0;
			} else {
				msg_get_uint32(m, AS_HB_MSG_ADDR, &p_pulse->addr);
				msg_get_uint32(m, AS_HB_MSG_PORT, &p_pulse->port);
				if (p_pulse->addr) {
					char fromip[INET_ADDRSTRLEN];
					cf_detail(AS_HB, "Got heartbeat pulse from node identifying itself as %s:%d",
							  inet_ntop(AF_INET, &p_pulse->addr, fromip, INET_ADDRSTRLEN) == NULL ? "Unknown" : fromip,
							  p_pulse->port);
				}
				memset(&p_pulse->socket, 0, sizeof(cf_sockaddr));
			}

			/* Get the succession list from the pulse message */
			int retval = msg_get_buf(m, AS_HB_MSG_ANV, (byte **) &buf, &bufsz, MSG_GET_DIRECT);

			if (bufsz != (g_config.paxos_max_cluster_size * sizeof(cf_node)))
				cf_warning(AS_HB, "Corrupted data? The size of anv is inaccurate. Received: %d ; Expected: %d", bufsz, (g_config.paxos_max_cluster_size * sizeof(cf_node)));

			/* copy the succession list into the heartbeat pulse for sending over to paxos code */
			if (0 == retval) {
				p_pulse->principal = buf[0]; // the first value is the principal-store this
				memcpy(p_pulse->anv, buf, bufsz);
			} else {
				cf_warning(AS_HB, "unable to get succession list from the heartbeat pulse.");
				memset(&p_pulse->principal, 0, sizeof(cf_node));
				memset(p_pulse->anv, 0, sizeof(cf_node)*g_config.paxos_max_cluster_size);
			}

			/* cf_warning(AS_HB, "GET HEARTBEAT PRINCIPAL is %"PRIx64"", p.principal); */

			if (vlock) {
				pthread_mutex_unlock(vlock);
			} else {
				int rv = shash_put_unique(g_hb.adjacencies, &node, p_pulse);
				if (rv == SHASH_ERR_FOUND) {
					cf_warning(AS_HB, "reprocessing HB msg, ppaddr %08x ppport %d ppfd %d fd %d", p_pulse->addr, p_pulse->port, p_pulse->fd, fd);
#ifdef FAIL_FAST
					cf_crash(AS_HB, "declining to recurse");
#endif
					as_hb_rx_process(m, so, fd);
					break;
				} else if (rv != 0) {
					cf_warning(AS_HB, "unable to update adjacencies hash");
				}
			}

			/* If MESH, we'll be sent a list of node names, request an INFO message for anything new */
			if (0 == retval) {

				for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
					if (0 == buf[i])
						break;

					if (g_config.self_node == buf[i])
						continue;
					if (SHASH_ERR_NOTFOUND != shash_get(g_hb.adjacencies, &buf[i], a_p_pulse))
						continue;

					/* We don't have a connection to this node; send an info
					 * request to find out who to connect to */
					msg *mt = as_fabric_msg_get(M_TYPE_HEARTBEAT);
					byte bufm[512];

					msg_set_uint32(mt, AS_HB_MSG_ID, AS_HB_PROTOCOL_IDENTIFIER());
					msg_set_uint32(mt, AS_HB_MSG_TYPE, AS_HB_MSG_TYPE_INFO_REQUEST);
//                    fprintf(stderr, "sending message request for node %"PRIx64, buf[i]);
					msg_set_uint64(mt, AS_HB_MSG_NODE, buf[i]);

					/* Include the ANV length in all heartbeat protocol v2 and greater messages. */
					if (!AS_HB_PROTOCOL_IS_V(1))
						if (0 > msg_set_uint32(mt, AS_HB_MSG_ANV_LENGTH, g_config.paxos_max_cluster_size))
							cf_crash(AS_HB, "Failed to set ANV length in heartbeat protocol v2 message.");

					size_t n = sizeof(bufm);
					if (0 == msg_fillbuf(mt, bufm, &n)) {
						if (AS_HB_MODE_MCAST == g_config.hb_mode) {
							if (0 > cf_socket_sendto(fd, bufm, n, 0, so)) {
								cf_detail(AS_HB, "cf_socket_sendto() failed 1");
								as_hb_error(AS_HB_ERR_SENDTO_FAIL_1);
							}
						} else {
							if (0 > as_hb_tcp_send(fd, bufm, n)) {
								cf_detail(AS_HB, "as_hb_tcp_send() fd %d failed 2", fd);
								as_hb_error(AS_HB_ERR_SENDTO_FAIL_2);
							}
						}
					} else {
						cf_warning(AS_HB, "could not create heartbeat message for transmission");
					}
					as_fabric_msg_put(mt);
				}
			}

			break;

		case AS_HB_MSG_TYPE_INFO_REQUEST:
//            fprintf(stderr, "got an info request\n");
//            msg_dump(m);
			if (0 > msg_get_uint64(m, AS_HB_MSG_NODE, &node)) {
				cf_detail(AS_HB, "unable to get node ID");
				as_hb_error(AS_HB_ERR_NO_NODE_REQ);
				return;
			} else {
				msg *mt = as_fabric_msg_get(M_TYPE_HEARTBEAT);
				byte bufm[512];

				msg_set_uint32(mt, AS_HB_MSG_ID, AS_HB_PROTOCOL_IDENTIFIER());
				msg_set_uint32(mt, AS_HB_MSG_TYPE, AS_HB_MSG_TYPE_INFO_REPLY);
				msg_set_uint64(mt, AS_HB_MSG_NODE, node);

				/* Include the ANV length in all heartbeat protocol v2 and greater messages. */
				if (!AS_HB_PROTOCOL_IS_V(1))
					if (0 > msg_set_uint32(mt, AS_HB_MSG_ANV_LENGTH, g_config.paxos_max_cluster_size))
						cf_crash(AS_HB, "Failed to set ANV length in heartbeat protocol v2 message.");

				if (SHASH_ERR_NOTFOUND == shash_get(g_hb.adjacencies, &node, a_p_pulse)) {
//                    fprintf(stderr, "Request: Node %"PRIx64" not found!", node);
					msg_set_uint32(mt, AS_HB_MSG_ADDR, 0);
					msg_set_uint32(mt, AS_HB_MSG_PORT, 0);
				} else {
					msg_set_uint32(mt, AS_HB_MSG_ADDR, a_p_pulse->addr);
					msg_set_uint32(mt, AS_HB_MSG_PORT, a_p_pulse->port);
//                    fprintf(stderr, "Request: Node %"PRIx64" found at %d:%d", node, p.addr, p.port);
				}

				size_t n = sizeof(bufm);
				if (0 == msg_fillbuf(mt, bufm, &n)) {
					if (AS_HB_MODE_MCAST == g_config.hb_mode) {
						if (0 > cf_socket_sendto(fd, bufm, n, 0, so)) {
							cf_detail(AS_HB, "cf_socket_sendto() failed 3");
							as_hb_error(AS_HB_ERR_SENDTO_FAIL_3);
						}
					} else {
						if (0 > as_hb_tcp_send(fd, bufm, n)) {
							cf_detail(AS_HB, "as_hb_tcp_send() fd %d failed 4", fd);
							as_hb_error(AS_HB_ERR_SENDTO_FAIL_4);
						}
					}
				} else {
					cf_warning(AS_HB, "unable to create message for transmission");
				}
				as_fabric_msg_put(mt);
			}
			break;

		case AS_HB_MSG_TYPE_INFO_REPLY:
//            fprintf(stderr, "got an info reply\n");
//            msg_dump(m);
			if ((0 > msg_get_uint64(m, AS_HB_MSG_NODE, &node)) ||
					(0 > msg_get_uint32(m, AS_HB_MSG_ADDR, &addr)) ||
					(0 > msg_get_uint32(m, AS_HB_MSG_PORT, &port))) {
				cf_detail(AS_HB, "unable to get required field");
				as_hb_error(AS_HB_ERR_MISSING_FIELD);
				return;
			}

			// If it's already known, we don't need to connect again
			if (SHASH_OK == shash_get(g_hb.adjacencies, &node, a_p_pulse)) {
				return;
			}

			/* If the address or port are zero, just wait; we'll try again
			 * when the next heartbeat is received */
			if (0 == addr || 0 == port) {
				return;
			}

			// check discovered node data with this node id
			if(as_hb_nodes_discovered_hash_is_conn(node)) {
				cf_detail(AS_HB, "as_hb_rx_process node already connected, returning");
				return;
			} else {
				// unwind the address
				char cpaddr[24];
				if (NULL == inet_ntop(AF_INET, &addr, (char *) cpaddr, sizeof(cpaddr))) {
					cf_info(AS_HB, "heartbeat: received suspicious address %s : %s", cpaddr, cf_strerror(errno));
					return;
				}
				cf_debug(AS_HB, "connecting to remote heartbeat service: %s:%d", cpaddr, port);
				// could be both seed and non-seed but we are passing is_seed = 0, 
				// as mesh_list_service_fn will take care of duplicates
				mesh_host_list_add(cpaddr, port, false);
			}
			break;

		default:
			cf_detail(AS_HB, "incomprehensible message type %d", type);
			as_hb_error(AS_HB_ERR_BAD_TYPE);
			break;
	}

	return;
}

/* as_hb_thr
 * Heartbeat control thread */
void *
as_hb_thr(void *arg)
{
	byte buft[2048], bufr[2048];
	msg *mt, *mr;
	struct epoll_event events[EPOLL_SZ];
	int nevents, sock = -1;
	cf_clock last_fd_print = 0;

	cf_debug(AS_HB, "starting heartbeat control: mode %d", g_config.hb_mode);

	/* Register fabric heartbeat msg type with no processing function:
	   This permits getting / putting heartbeat msgs to be moderated via an idle msg queue. */
	as_fabric_register_msg_fn(M_TYPE_HEARTBEAT, as_hb_msg_template, sizeof(as_hb_msg_template), 0, 0);

	/* Create the invariant portion of the heartbeat message */
	mt = as_fabric_msg_get(M_TYPE_HEARTBEAT);

	msg_set_uint32(mt, AS_HB_MSG_ID, AS_HB_PROTOCOL_IDENTIFIER());
	msg_set_uint32(mt, AS_HB_MSG_TYPE, AS_HB_MSG_TYPE_PULSE);
	msg_set_uint64(mt, AS_HB_MSG_NODE, g_config.self_node);

	/* Set the socket descriptor and some associated properties */
	if (AS_HB_MODE_MCAST == g_config.hb_mode) {
		sock = g_hb.socket_mcast.s.sock;
		if (sock >= AS_HB_TXLIST_SZ)
			cf_crash(AS_HB, "unable to add mcast socket to txlist, too large");

		g_hb.endpoint_txlist[sock] = true;
		g_hb.endpoint_txlist_isudp[sock] = true;

	} else if (AS_HB_MODE_MESH == g_config.hb_mode) {
		sock = g_hb.socket.sock;

		// If the user specified 'any' as heartbeat address, we listen on 0.0.0.0 (all interfaces)
		// But we should send a proper IP address to the remote machine to send back heartbeat.
		// Use the node's IP address in this case.
		char *hbaddr_to_use = g_config.hb_addr;
		// Checking the first byte is enough as '0' cannot be a valid IP address other than 0.0.0.0
		if (*hbaddr_to_use == '0') {
			cf_debug(AS_HB, "Sending %s as nodes IP to return heartbeat", g_config.node_ip);
			hbaddr_to_use = g_config.node_ip;
		}

		struct in_addr self;
		if (1 != inet_pton(AF_INET, hbaddr_to_use, &self))
			cf_warning(AS_HB, "unable to call inet_pton: %s", cf_strerror(errno));
		else {
			msg_set_uint32(mt, AS_HB_MSG_ADDR, * (uint32_t *) &self);
			msg_set_uint32(mt, AS_HB_MSG_PORT, g_config.hb_port);
		}
	}

	/* Create something for inbound heartbeat messages */
	mr = as_fabric_msg_get(M_TYPE_HEARTBEAT);

	/* Configure epoll */
	if (-1 == (g_hb.efd = epoll_create(EPOLL_SZ)))
		cf_crash(AS_HB, "unable to create epoll fd: %s", cf_strerror(errno));
	g_hb.ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
	g_hb.ev.data.fd = sock;
	if (0 > epoll_ctl(g_hb.efd, EPOLL_CTL_ADD, sock, &g_hb.ev))
		cf_crash(AS_HB,  "unable to add socket %d to epoll fd list: %s", sock, cf_strerror(errno));

	/* Mesh-topology systems allow config-file bootstraping; connect to the provided node */
	if ((AS_HB_MODE_MESH == g_config.hb_mode)) {
		if (g_config.hb_init_addr) {
			cf_info(AS_HB, "connecting to remote heartbeat service at %s:%d", g_config.hb_init_addr, g_config.hb_init_port);

			if (0 != mesh_host_list_add(g_config.hb_init_addr, g_config.hb_init_port, true)) {
				cf_crash(AS_HB, "couldn't add remote heartbeat service %s:%d to mesh host list", g_config.hb_init_addr, g_config.hb_init_port);
			}
		} else {
			for (int i = 0; i < AS_CLUSTER_SZ; i++) {
				if (g_config.hb_mesh_seed_addrs[i]) {
					cf_info(AS_HB, "connecting to remote heartbeat service at %s:%d", g_config.hb_mesh_seed_addrs[i], g_config.hb_mesh_seed_ports[i]);

					if (0 != mesh_host_list_add(g_config.hb_mesh_seed_addrs[i], g_config.hb_mesh_seed_ports[i], true)) {
						cf_crash(AS_HB, "couldn't add remote heartbeat service %s:%d to mesh host list", g_config.hb_mesh_seed_addrs[i], g_config.hb_mesh_seed_ports[i]);
					}
				} else {
					break;
				}

			}

		}
	}

	/* Iterate over events */
	do {
		nevents = epoll_wait(g_hb.efd, events, EPOLL_SZ, MAX(g_config.hb_interval / 3, 1));

		if (0 > nevents)
			cf_debug(AS_HB, "epoll_wait() returned %d ; errno = %d (%s)", nevents, errno, cf_strerror(errno));

		for (int i = 0; i < nevents; i++) {
			int fd = events[i].data.fd;

			/* Accept a new connection */
			if (fd == sock && (AS_HB_MODE_MESH == g_config.hb_mode)) {
				int csock;
				struct sockaddr_in caddr;
				socklen_t clen = sizeof(caddr);
				char cpaddr[24];

				if (-1 == (csock = accept(fd, (struct sockaddr *) &caddr, &clen))) {
					if ((errno == EMFILE) || (errno == ENFILE) || (errno == ENOMEM) || (errno == ENOBUFS)) {
						if (last_fd_print != (cf_getms() / 1000L)) {
							cf_warning(AS_HB, "Failed to accept heartbeat connection due to error : %s", cf_strerror(errno));
							last_fd_print = cf_getms() / 1000L;
						}
						// We are in an extreme situation where we ran out of system resources (file/mem).
						// We should rather lie low and not do too much activity. So, sleep.
						// We should not sleep too long as this same function is supposed to send heartbeat also.
						usleep(MAX(g_config.hb_interval/2, 1) * 1000);
						continue;
					} else {
						cf_crash(AS_HB, "accept failed: %s", cf_strerror(errno));
					}
				}
				if (NULL == inet_ntop(AF_INET, &caddr.sin_addr.s_addr, (char *) cpaddr, sizeof(cpaddr)))
					cf_crash(AS_HB, "inet_ntop failed: %s", cf_strerror(errno));
				cf_debug(AS_HB, "new connection from %s:%d", cpaddr, caddr.sin_port);

				cf_atomic_int_incr(&g_config.heartbeat_connections_opened);
				if (0 != as_hb_endpoint_add(csock, false /*is not udp*/, 0 /*node id not known till pulse come*/)) {
					close(csock);
					continue;
				}
			} else {
				/* Catch remotely-closed connections */
				if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
CloseSocket:
					as_hb_error(AS_HB_ERR_REMOTE_CLOSE);
					cf_debug(AS_HB, "remote close: fd %d event %x", fd, events[i].events);
					g_hb.endpoint_txlist[fd] = false;
					// Remove node from the discovered list.
					if (g_hb.endpoint_txlist_node_id[fd]) {
						as_hb_nodes_discovered_hash_del_conn(g_hb.endpoint_txlist_node_id[fd], fd);
					}
					g_hb.endpoint_txlist_node_id[fd] = 0;
					cf_atomic_int_incr(&g_config.heartbeat_connections_closed);
					mesh_host_list_remove_fd(fd);
					if (0 > epoll_ctl(g_hb.efd, EPOLL_CTL_DEL, fd, &g_hb.ev)) {
						cf_warning(AS_HB, "unable to remove socket %d from epoll fd list: %s", fd, cf_strerror(errno));
					}
					close(fd);
					continue;
				}

				if (events[i].events & EPOLLIN) {

					cf_sockaddr from;
					int r = 0;
					memset(&from, 0, sizeof(from));

					if (AS_HB_MODE_MCAST == g_config.hb_mode) {
						r = cf_socket_recvfrom(fd, bufr, sizeof(bufr), 0, &from);
					} else {
						r = as_hb_tcp_recv(fd, bufr, sizeof(bufr));
					}
					cf_detail(AS_HB, "received %d bytes, calling msg_parse", r);
					if (r > 0) {
						if (0 > msg_parse(mr, bufr, r, false)) {
							cf_detail(AS_HB, "unable to parse heartbeat message");
							as_hb_error(AS_HB_ERR_UNPARSABLE_MSG);
						} else {
							as_hb_rx_process(mr, from, fd);
						}
						msg_reset(mr);
					} else {
						cf_warning(AS_HB, "about to goto CloseSocket....");
						goto CloseSocket;
					}
				}
			}
		}

		/* Transmit heartbeats to every fd in the txlist */
		cf_clock now = cf_getms();
		if (AS_HB_ENABLED() && (now > g_hb.time_last + (g_config.hb_interval))) {
			/* Always use the current heartbeat protocol version (which may have changed since last time.) */
			msg_set_uint32(mt, AS_HB_MSG_ID, AS_HB_PROTOCOL_IDENTIFIER());

			/* Include the ANV length in all heartbeat protocol v2 and greater messages. */
			if (!AS_HB_PROTOCOL_IS_V(1))
				if (0 > msg_set_uint32(mt, AS_HB_MSG_ANV_LENGTH, g_config.paxos_max_cluster_size))
					cf_crash(AS_HB, "Failed to set ANV length in heartbeat protocol v2 message.");

			/* Fill in the current adjacency list and bufferize the message */
			msg_set_buf(mt, AS_HB_MSG_ANV, (byte *) g_config.paxos->succession, sizeof(cf_node) * g_config.paxos_max_cluster_size, MSG_SET_COPY);
			/* cf_info(AS_HB, "PUT HEARTBEAT PULSE PRINCIPAL is %"PRIx64"", g_config.paxos->succession[0]); */
			size_t n = sizeof(buft);
			if (0 != msg_fillbuf(mt, buft, &n)) {
				cf_crash(AS_HB, "internal error: could not create heartbeat message");
			}

			for (int i = 0; i < AS_HB_TXLIST_SZ; i++) {
				if (true == g_hb.endpoint_txlist[i]) {
					if (true == g_hb.endpoint_txlist_isudp[i]) {
						cf_detail(AS_HB, "sending udp heartbeat to index %d : msg size %d", i, n);
						struct sockaddr_in so;
						cf_sockaddr dest;
						so.sin_family = AF_INET;
						inet_pton(AF_INET, g_config.hb_addr, &so.sin_addr.s_addr);
						so.sin_port = htons(g_config.hb_port);
						cf_sockaddr_convertto(&so, &dest);

						if (0 > cf_socket_sendto(i, buft, n, 0, dest)) {
							cf_detail(AS_HB, "cf_socket_sendto() failed 5");
							as_hb_error(AS_HB_ERR_SENDTO_FAIL_5);
						}
					} else { // tcp

						cf_detail(AS_HB, "sending tcp heartbeat to index %d : msg size %zu", i, n);
						if (0 > as_hb_tcp_send(i, buft, n)) {
							cf_detail(AS_HB, "as_hb_tcp_send() fd %d failed 6", i);
							as_hb_error(AS_HB_ERR_SENDTO_FAIL_6);
						}
					}
				}
			}

			// this seems safer, but means we're always further and further behind
			// g_hb.time_last = now;
			// this takes the average of where we are and where we should be, which is far more accurate
			// but has the chance of catching back up after a glitch
			g_hb.time_last = ((g_config.hb_interval / 2) + (g_hb.time_last / 2) + (now / 2));
		}
	} while (1);

	msg_destroy(mt);

	return(NULL);
}

// copy over this node's adjacency vector to Paxos
void as_hb_copy_to_paxos(cf_node id, cf_node *anv)
{

	int i = 0;
	for (i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// get node id
		cf_node curr = g_config.hb_paxos_succ_list_index[i];
		if (curr == (cf_node)0)
			break;
		if (curr == id)
			break;
	}
	if (i == g_config.paxos_max_cluster_size) {
		cf_info(AS_HB, "hb runs out of cluster size");
		return;
	}
	// i is the position for this node
	cf_detail(AS_HB, "SETTING index %d, node %"PRIx64"", i, id);
	g_config.hb_paxos_succ_list_index[i] =  id;
	// Add node's ANV to the ith hb_paxos_succ_list.
	memcpy(g_config.hb_paxos_succ_list[i], anv, sizeof(cf_node) * g_config.paxos_max_cluster_size);
}

/* as_hb_monitor_reduce
 * Reduce the adjacency hash to find expired nodes */
int
as_hb_monitor_reduce(void *key, void *data, void *udata)
{
	as_hb_monitor_reduce_udata *u = (as_hb_monitor_reduce_udata *) udata;
	as_hb_pulse *p = (as_hb_pulse *) data;
	cf_node id = * (cf_node *) key;

	cf_clock now = cf_getms();

	// Collect new nodes first
	if (p->new) {
		p->new = false;
		p->last_detected = now;

		if (u->n_insert < g_config.paxos_max_cluster_size) {
			u->insert[u->n_insert] = id;
			u->insert_p_node[u->n_insert] = p->principal;
			u->n_insert++;
		}

		//copy adjacency list for paxos checks
		as_hb_copy_to_paxos(id, p->anv);

		return (0);
	}

	bool node_expired = false;

	/* Check the time */
	if (now > p->last + (g_config.hb_interval * g_config.hb_timeout)) {
		node_expired = true;

		if (p->dunned) {
			cf_debug(AS_HB, "hb considers expiring: now %"PRIu64" last %"PRIu64, now, p->last);
		} else {
			cf_info(AS_HB, "hb considers expiring: now %"PRIu64" last %"PRIu64, now, p->last);
			as_hb_error(AS_HB_ERR_EXPIRE_HB);
		}
	}

	/* suspect node. Ask fabric what fabric thinks. */
	if (node_expired) {
		uint64_t fabric_lasttime;
		if (0 == as_fabric_get_node_lasttime(id, &fabric_lasttime)) {
			if (fabric_lasttime > (g_config.hb_interval * g_config.hb_timeout)) {
				if (p->dunned) {
					cf_debug(AS_HB, "hb expires but fabric says DEAD: node %"PRIx64, id);
				} else {
					cf_info(AS_HB, "hb expires but fabric says DEAD: node %"PRIx64, id);
					as_hb_error(AS_HB_ERR_EXPIRE_FAB_DEAD);
				}

				node_expired = true;
			} else {
				if (p->dunned) {
					cf_debug(AS_HB, "hb expires but fabric says ALIVE: lasttime %"PRIu64" node %"PRIx64, fabric_lasttime, id);
				} else {
					cf_info(AS_HB, "hb expires but fabric says ALIVE: lasttime %"PRIu64" node %"PRIx64, fabric_lasttime, id);
					as_hb_error(AS_HB_ERR_EXPIRE_FAB_ALIVE);
				}

				node_expired = false;
			}

		} else {
			cf_info(AS_HB, "possible node expiration, check of fabric returns error: node %"PRIx64, id);
		}
	}

	/*
	 * node is gone
	 */
	if (node_expired) {
		cf_debug(AS_HB, "node has actually expired according to heartbeat node %"PRIx64, id);

		//do not copy adjacency list for paxos checks - node is gone
		if (u->n_delete < g_config.paxos_max_cluster_size) {
			if (AS_HB_MODE_MESH == g_config.hb_mode) {
				as_hb_nodes_discovered_hash_shutdown_conn(id);
			}
			u->delete[u->n_delete] = id;
			u->n_delete++;
			return(SHASH_REDUCE_DELETE);
		}
	}

	if (p->updated) {
		p->updated = false;
		if (p->dunned) {
			// fabric test failed, so do not copy adjacency list for paxos checks.
			// don't delete from shash either - it's probably a
			// one-way network fault and the node should come back soon.

			if (u->n_dun < g_config.paxos_max_cluster_size) {
				u->dun[u->n_dun] = id;
				u->n_dun++;
			}

			return (0);
		} else {
			// one way network failure resolved
			p->last_detected = now;

			if (u->n_undun < g_config.paxos_max_cluster_size) {
				u->undun[u->n_undun] = id;
				u->undun_p_node[u->n_undun] = p->principal;
				u->n_undun++;
			}

			// copy adjacency list for paxos checks
			as_hb_copy_to_paxos(id, p->anv);

			return (0);
		}
	}

	//copy adjacency list for paxos checks
	as_hb_copy_to_paxos(id, p->anv);

	/*
	 * This series of checks is the place to catch a bunch of bad state transitions
	 * to verify that this alive node is healthy and part of the cluster
	 */
	/* Check the time. The period for this check is 5 times more interval for a heartbeat failure */
	if (now < p->last_detected + (g_config.hb_interval * g_config.hb_timeout * 10))
		return (0);

	p->last_detected = now;

	/*
	 * First check if this node is in its own succession list. This can actually happen if the paxos transaction
	 * does not complete properly
	 */
	bool node_in_slist = false;
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((p->anv[i] != (cf_node)0) && (p->anv[i] == id)) {
			node_in_slist = true;
		}
	}
	/*
	 * Check if this node's succession list is in sync with ours
	 */
	bool slists_match = true;
	as_paxos *px = g_config.paxos;
	char *same_diff = (px->succession[0] == p->anv[0] ? "same" : "different");

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (px->succession[i] != p->anv[i]) {
			slists_match = false;
			break;
		}
	}

	// These warnings are crucial to inform us about the cluster's Paxos health.
	if (! node_in_slist) {
		if (slists_match) { // a really weird case of one way partitioning
			cf_warning(AS_HB, "HB node %"PRIx64" in %s cluster is not in its own succession list - succession lists match", id, same_diff);
		} else { // A second weird case - should not really happen
			cf_warning(AS_HB, "HB node %"PRIx64" in %s cluster is not in its own succession list - succession lists don't match", id, same_diff);
		}
	} else {
		if (slists_match) // This is the normal case - print with debug level
			cf_debug(AS_HB, "HB node %"PRIx64" in %s cluster - succession lists match", id, same_diff);
		else { // this is ths case where a node is just going to join a cluster or two clusters are merging
			if (! p->dunned) {
				cf_info(AS_HB, "HB node %"PRIx64" in %s cluster - succession lists don't match", id, same_diff);
			} else {
				cf_info(AS_HB, "[Ignoring succession list mismatch with dunned node %"PRIx64" in %s cluster]", id, same_diff);
			}
		}
	}

	return(0);
}

/* as_hb_monitor_thr
 * Heartbeat monitoring */
void *
as_hb_monitor_thr(void *arg)
{
	cf_debug(AS_HB, "starting heartbeat monitoring");

	do {
		as_hb_monitor_reduce_udata u;
		u.n_delete = 0;
		u.n_insert = 0;
		u.n_dun = 0;
		u.n_undun = 0;

		// lock
		pthread_mutex_lock(&g_config.hb_paxos_lock);

		// clean up the global succession list structures
		memset(g_config.hb_paxos_succ_list_index, 0, sizeof(g_config.hb_paxos_succ_list_index));
		memset(g_config.hb_paxos_succ_list, 0, sizeof(g_config.hb_paxos_succ_list));

		if (g_hb.adjacencies) {
			shash_reduce_delete(g_hb.adjacencies, as_hb_monitor_reduce, &u);
		} else {
			cf_debug(AS_HB, "not processing heartbeat adjacency");
		}

		// unlock
		pthread_mutex_unlock(&g_config.hb_paxos_lock);

		// anything that's departing, put it in the snub list to create some hysteresis
		// This is a bit of a hack, but....
		if (u.n_delete) {
			for (int i = 0; i < u.n_delete; i++) {
				cf_debug(AS_HB, "internal snub: node %"PRIx64" waits %d", u.delete[i], g_config.hb_interval * g_config.hb_timeout * 10);
				as_hb_snub(u.delete[i], g_config.hb_interval * g_config.hb_timeout * 10);
			}
		}

		/* Fire callbacks; this has to be done outside the reduction
		 * above to avoid deadlocking if someone recurses back into the
		 * heartbeat system */
		if (g_hb.cb_sz) {
			/*
			 * Create a batched list of callback events in an array
			 */
			as_hb_event_node events[AS_CLUSTER_SZ];
			if (g_config.paxos_max_cluster_size < (u.n_delete + u.n_insert + u.n_dun + u.n_undun)) {
				cf_warning(AS_HB, "Number of heartbeat events (%d) exceeds cluster size", (u.n_delete + u.n_insert + u.n_dun + u.n_undun));
			}
			for (int i = 0; i < u.n_delete; i++) {
				events[i].evt = AS_HB_NODE_DEPART;
				events[i].nodeid = u.delete[i];
				cf_info(AS_HB, "removing node on heartbeat failure: %"PRIx64"", events[i].nodeid);
			}
			for (int i = u.n_delete; i < (u.n_delete + u.n_insert); i++) {
				events[i].evt = AS_HB_NODE_ARRIVE;
				events[i].nodeid = u.insert[i - u.n_delete];
				events[i].p_node = u.insert_p_node[i - u.n_delete];
				cf_info(AS_HB, "new heartbeat received: %"PRIx64" principal node is %"PRIx64"", events[i].nodeid, events[i].p_node);
			}
			for (int i = (u.n_delete + u.n_insert); i < (u.n_delete + u.n_insert + u.n_dun); i++) {
				events[i].evt = AS_HB_NODE_DUN;
				events[i].nodeid = u.dun[i - u.n_delete - u.n_insert];
				cf_info(AS_HB, "removing dunned node: %"PRIx64"", events[i].nodeid);
			}
			for (int i = (u.n_delete + u.n_insert + u.n_dun); i < (u.n_delete + u.n_insert + u.n_dun + u.n_undun); i++) {
				events[i].evt = AS_HB_NODE_UNDUN;
				events[i].nodeid = u.undun[i - u.n_delete - u.n_insert - u.n_dun];
				events[i].p_node = u.undun_p_node[i - u.n_delete - u.n_insert - u.n_dun];
				cf_info(AS_HB, "re-adding undunned node: %"PRIx64" principal node is %"PRIx64"", events[i].nodeid, events[i].p_node);
			}
			if (0 < (u.n_delete + u.n_insert + u.n_dun + u.n_undun)) {
				for (uint j = 0; j < g_hb.cb_sz; j++) {
					(g_hb.cb[j])((u.n_delete + u.n_insert + u.n_dun + u.n_undun), events, g_hb.cb_udata[j]);
				}
			}
		}

		usleep(MAX(g_config.hb_interval, 1) * 1000);

	} while (1);

	cf_warning(AS_HB, "heartbeat monitoring stopping");

	return(NULL);
}

pthread_t g_monitor_tid;
pthread_t g_mesh_list_tid;

/* as_hb_init
 * Initialization of the heartbeat subsystem */
void
as_hb_init()
{
	cf_debug(AS_HB, "heartbeat initialization");

	if (g_config.hb_interval < AS_HB_MIN_INTERVAL) {
		cf_warning(AS_HB, "Re-setting hb_interval from %d to minimum value %d", g_config.hb_interval, AS_HB_MIN_INTERVAL);
		g_config.hb_interval = AS_HB_MIN_INTERVAL;
	}
	as_hb_nodes_discovered_hash_create();
	as_hb_adjacencies_create();

	/* Start a thread to monitor the adjacency hash for failed nodes */
	if (0 != pthread_create(&g_monitor_tid, 0, as_hb_monitor_thr, &g_hb))
		cf_crash(AS_HB, "could not create hb monitor thread: %s", cf_strerror(errno));

	// and another thread to attempt to connect to mesh nodes - probably should make this contingent
	// on mesh being active? Or, in some sense, is mesh always active?
	g_hb.mesh_host_queue = cf_queue_create(sizeof(mesh_host_queue_element), true);
	g_hb.mesh_seed_host_list = 0;
	g_hb.mesh_non_seed_host_list = 0;
	if (0 != pthread_create(&g_mesh_list_tid, 0, mesh_list_service_fn, 0))
		cf_crash(AS_HB, "could not create hb monitor thread: %s", cf_strerror(errno));

	pthread_mutex_init(&g_hb.snub_lock, 0);
	pthread_mutex_init(&g_config.hb_paxos_lock, 0);
	g_hb.snub_list = 0;

	/* Continue on with the initialization actions. */
	as_hb_init_socket();
}

/* as_hb_init_socket
 * Initialize the heartbeat socket. */
static void
as_hb_init_socket()
{
	cf_info(AS_HB, "heartbeat socket initialization");

	switch (g_config.hb_mode) {
		case AS_HB_MODE_MCAST:
			cf_info(AS_HB, "initializing multicast heartbeat socket : %s:%d", g_config.hb_addr, g_config.hb_port);
			g_hb.socket_mcast.s.addr = g_config.hb_addr;
			g_hb.socket_mcast.s.port = g_config.hb_port;
			g_hb.socket_mcast.tx_addr = g_config.hb_tx_addr;
			g_hb.socket_mcast.mcast_ttl = g_config.hb_mcast_ttl;
			if (0 != cf_mcastsocket_init(&g_hb.socket_mcast))
				cf_crash(AS_HB, "couldn't initialize multicast heartbeat socket: %s", cf_strerror(errno));
			cf_debug(AS_HB, "Opened multicast socket %d", g_hb.socket_mcast.s.sock);
			break;
		case AS_HB_MODE_MESH:
			cf_info(AS_HB, "initializing mesh heartbeat socket : %s:%d", g_config.hb_addr, g_config.hb_port);
			g_hb.socket.addr = g_config.hb_addr;
			g_hb.socket.port = g_config.hb_port;
			g_hb.socket.proto = SOCK_STREAM;
			g_hb.socket.reuse_addr = (g_config.socket_reuse_addr) ? true : false;
			if (0 != cf_socket_init_svc(&g_hb.socket))
				cf_crash(AS_AS, "couldn't initialize unicast heartbeat socket: %s", cf_strerror(errno));
			break;
		case AS_HB_MODE_UNDEF:
		default:
			cf_crash(AS_HB, "invalid heartbeat mode!");
			break;
	}

	/* Note the time */
	g_hb.time_start = cf_getms();
	g_hb.time_last = 0;
}

/* as_hb_reinit
 * Re-initialize the heartbeat subsystem
 * (Open socket, but don't create threads, etc.) */
static void
as_hb_reinit(int socket, bool isudp)
{
	cf_info(AS_HB, "heartbeat re-initialization: socket %d is%s UDP", socket, (isudp ? "" : " not"));

	as_hb_init_socket();

	as_hb_start_receiving(socket, isudp, 0 /*node*/);
}

/* as_hb_start
 * Startup of the pulse subsystem */
void
as_hb_start()
{
	pthread_t tid;

	cf_debug(AS_HB, "heartbeat start");

	/* Mandatory pause for twice the timeout interval to prevent flapping */
	while (cf_getms() - g_hb.time_start < (2 * g_config.hb_interval * g_config.hb_timeout))
		usleep(50 * 1000);

	as_fb_health_register_cb_fn(as_hb_fb_health_cb, NULL);

	/* Start transmissions */
	if (0 != pthread_create(&tid, 0, as_hb_thr, &g_hb))
		cf_crash(AS_HB, "could not create hb tx thread: %s", cf_strerror(errno));
}

/* as_hb_shutdown
 * Shut dow the heartbeat subsystem. */
bool
as_hb_shutdown()
{
	cf_info(AS_HB, "heartbeat shutdown");

	bool was_udp = as_hb_stop_receiving();

	switch (g_config.hb_mode) {
		case AS_HB_MODE_MCAST:
			cf_debug(AS_HB, "Closing multicast socket %d", g_hb.socket_mcast.s.sock);
			cf_mcastsocket_close(&g_hb.socket_mcast);
			break;
		case AS_HB_MODE_MESH:
			cf_socket_close(&g_hb.socket);
			break;
		case AS_HB_MODE_UNDEF:
		default:
			cf_crash(AS_HB, "invalid heartbeat mode!");
	}

	return was_udp;
}

/*
 *  as_hb_dump_pulse
 *  Log the properties of an as_hb_pulse object.
 */
static void
as_hb_dump_pulse(as_hb_pulse *pulse)
{
	cf_info(AS_HB, " last %lu", pulse->last);
	cf_info(AS_HB, " sockaddr %"PRIx64"", pulse->socket);
	uint8_t *addr = (uint8_t *) &pulse->addr;
	cf_info(AS_HB, " addr %d.%d.%d.%d port %d", addr[0], addr[1], addr[2], addr[3], pulse->port);
	cf_info(AS_HB, " fd %d", pulse->fd);
	cf_info(AS_HB, " new %d updated %d dunned %d", pulse->new, pulse->updated, pulse->dunned);
	cf_info(AS_HB, " last detected %lu", pulse->last_detected);
	cf_info(AS_HB, " principal %"PRIx64"", pulse->principal);

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (pulse->anv[i]) {
			cf_info(AS_HB, " anv[%d] %"PRIx64"", i, pulse->anv[i]);
		}
	}
}

/*
 *  as_dump_adjacencies_entry
 *  Log the properties of a single HB adjacencies list entry, i.e.,
 *    cf_node ==> as_hb_pulse
 */
static int
as_hb_dump_adjacencies_entry(void *key, void *data, void *udata)
{
	cf_node node = * (cf_node *) key;
	as_hb_pulse *pulse = (as_hb_pulse *) data;
	int *count = (int *) udata;

	cf_info(AS_HB, "Adjacencies[%d]: node %"PRIx64"", *count, node);
	as_hb_dump_pulse(pulse);

	*count += 1;

	return 0;
}

/*
 *  as_hb_dump
 *  Log the state of the heartbeat module.
 */
void
as_hb_dump(bool verbose)
{
	cf_info(AS_HB, "Heartbeat Dump:");

	// General HB Info:

	cf_info(AS_HB, "HB Mode:  %s (%d)", (AS_HB_MODE_MCAST == g_config.hb_mode ? "multicast" :
										 (AS_HB_MODE_MESH == g_config.hb_mode ? "mesh" : "undefined")),
			g_config.hb_mode);
	cf_info(AS_HB, "HB Interval:  %d", g_config.hb_interval);
	cf_info(AS_HB, "HB Timeout:  %d", g_config.hb_timeout);
	cf_info(AS_HB, "HB Protocol:  %s (%d)", (AS_HB_PROTOCOL_V1 == g_config.hb_protocol ? "V1" :
											 (AS_HB_PROTOCOL_V2 == g_config.hb_protocol ? "V2" :
											  (AS_HB_PROTOCOL_NONE == g_config.hb_protocol ? "none" :
											   (AS_HB_PROTOCOL_RESET == g_config.hb_protocol ? "reset" : "undefined")))),
			g_config.hb_protocol);

	cf_socket_cfg *socket = (AS_HB_MODE_MCAST == g_config.hb_mode ? &g_hb.socket_mcast.s :
							 (AS_HB_MODE_MESH == g_config.hb_mode ? &g_hb.socket : NULL));

	if (socket) {
		cf_info(AS_HB, "HB Socket:  addr:port %s:%d proto %d sock %d", socket->addr, socket->port, socket->proto, socket->sock);
	}

	// Mesh Host List Info:

	mesh_host_list_element *elem = g_hb.mesh_seed_host_list;
	int i = 0;
	while (elem) {
		cf_info(AS_HB, "MeshSeedHostList[%d] %s:%d %lu %d %d", i, elem->host, elem->port, elem->next_try, elem->try_interval, elem->fd);
		i++;
		elem = elem->next;
	}

	// Snub List:

	snub_list_element *sle = g_hb.snub_list;
	i = 0;
	while (sle && sle->node) {
		cf_info(AS_HB, "SnubList[%d] node %"PRIx64" expiration %"PRIx64"", i, sle->node, sle->expiration);
		sle++;
		i++;
	}

	// TxList Info:

	int endpoint_count = 0;
	for (i = 0; i < AS_HB_TXLIST_SZ; i++) {
		if (g_hb.endpoint_txlist[i]) {
			endpoint_count++;
			cf_info(AS_HB, "TxList[%d]: fd %d isudp %d", i, i, g_hb.endpoint_txlist_isudp[i]);
		}
	}
	cf_info(AS_HB, "There are %d open HB TxList sockets.", endpoint_count);

	// Adjacencies

	if (verbose) {
		int count = 0;
		shash_reduce(g_hb.adjacencies, as_hb_dump_adjacencies_entry, &count);
	}
}
