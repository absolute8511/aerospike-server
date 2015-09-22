/*
 * fabric.c
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
 * The interconnect fabric is a means of sending messages throughout
 * the entire system, in order to heal and vote and such
 */

#include "fabric/fabric.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "msg.h"
#include "queue.h"
#include "socket.h"
#include "util.h"

#include "base/cfg.h"
#include "fabric/fb_health.h"
#include "fabric/hb.h"


// #define EXTRA_CHECKS 1

// Operation to be performed on transaction in retransmission hash.
typedef enum op_xmit_transaction_e {
	OP_TRANS_TIMEOUT = 1,               // Transaction is timed out.
	OP_TRANS_RETRANSMIT = 2,            // Retransmit the message.
} op_xmit_transaction;

// An alternative way to keep track of FBs
// #define CONNECTED_FB_HASH_USE_PUT_UNIQUE 1

#ifdef EXTRA_CHECKS
void as_fabric_dump();
#endif

// Workaround for platforms that don't have EPOLLRDHUP yet
#ifndef EPOLLRDHUP
#define EPOLLRDHUP EPOLLHUP
#endif


/*
**                                      Fabric Theory of Operation
**                                      ==========================
**
**   Overview:
**   ---------
**
**   The fabric is a "field of force", mediated by messages carried via TCP sockets, that binds a cluster
**   together.  The fabric must first be initialized by "as_fabric_init()", which creates the module-global
**   "g_fabric_args" object containing the parameters and data structures of the fabric.  After being
**   initialized, the fabric may be started up using "as_fabric_start()", which creates the worker threads
**   to send and receive messages and the accept thread to receive incoming connections.  (There is currently
**   no means to shut the fabric down.)
**
**   Fabric Message and Event Callbacks:
**   -----------------------------------
**
**   A module may register a callback function to process incoming fabric messages of a particular type using
**   "as_fabric_register_msg_fn()".  Message types are defined in "msg.h", and message handlers are registered
**   in the individual modules.  In addition, there is support for a single fabric heartbeat event handler
**   registered via "as_fabric_register_event_fn()", which is bound to the "as_paxos_event()" function of the
**   Paxos module.
**
**   Fabric Connection Setup:
**   ------------------------
**
**   When the local node sends a fabric message to a remote node, it will first try to open a new, non-blocking
**   TCP connection to the remote node using "fabric_connect()".  The number of permissible outbound connections
**   to a particular remote node is limited to being strictly lower than "FABRIC_MAX_FDS" (currently 8.)  Thus
**   a maximum of 7 outbound socket connections (each with its own FB [see below]), will generally be created to
**   each remote node as fabric messages are sent out.  Once the maximum number of outbound sockets is reached,
**   an already-existing connection will be re-used to send the message.  In addition, there will generally be
**   7 incoming connections (each with its own FB) from each remote node, for a total of 14 open sockets between
**   each pair of fabric nodes.
**
**   When a node opens a fabric connection to a remote node, the first fabric message sent will be used to
**   identify the local node by sending its 64-bit node ID (as the value of the "FS_FIELD_NODE" field) to the
**   remote node.  Correspondingly, when a new incoming connection is received via "fabric_accept_fn()", the
**   local node will determine the remote node's ID when processing the first fabric message received from the
**   remote node in "fabric_process_read_msg()".  Once the node ID has been determined, an FNE [see below] for
**   the remote node will be looked up in the FNE hash table (or else created and added to the table if not
**   found), and it will be associated with the incoming FB.  At this point, the incoming connection is set up,
**   and forthcoming messages will be parsed and dispatched to the appropriate message type handler callback
**   function.
**
**   Detailed Method of Fabric Node Connection / Disconnection:
**   ----------------------------------------------------------
**
**   For each remote node that is newly detected (either via receipt of a heartbeat or via the "weird" way
**   of first receiving an incoming fabric message from the remote node), a "fabric_node_element" (FNE) will
**   be constructed.  All known fabric nodes have a corresponding FNE in the "g_fabric_node_element_hash".
**   When a node is found to no longer be available (i.e., upon heartbeat receipt failure, which may be due
**   either to the remote node actually shutting down or else to a (potentially temporary) network outage),
**   the FNE will be destroyed.
**
**   Each active fabric connection is represented by a "fabric_buffer" (FB), with its own socket file descriptor
**   (FD), and which will be associated with an FNE.  There are two types ("polarities") of fabric connections,
**   outbound and inbound.  Outbound connections are created using "fabric_connect()" and all torn down using
**   "fabric_disconnect()".  Inbound connections are created in "fabric_accept_fn()".  Inbound connections are
**   shut down when the node departs the fabric and the remote endpoint is closed.
**
**   The FNE destruction procedure is handled in a lazy fashion via reference counts from the associated FBs.
**   In the normal case, a socket FD will be shut down cleanly, and the local node will receive an "epoll(4)"
**   event that allows the FB containing the FD to be released.  Once all of its FBs are released, the FNE
**   itself will be released.  In the abnormal case of a (possibly temporary) one-way network failure, the
**   fabric node has to be disconnected via "fabric_disconnect()", which will trigger FB cleanup by sending
**   "DELETE_FABRIC_BUFFER" messages for each FB to the associated worker thread.  Each FNE keeps a hash
**   table of its connected outbound FBs for exactly this purpose of being able to clean up when necessary.
**
**   Threading Structure:
**   --------------------
**
**   High-performance, concurrent fabric message exchange is provided via worker threads handling a particular
**   set of FBs.  (There is currently a maximum of 6 worker threads.)  Each worker thread has a Unix-domain
**   notification ("note") socket that is used to send events to the worker thread.  The main work of receiving
**   and sending fabric messages is handled by "fabric_worker_fn()", which does an "epoll_wait()" on the "note_fd"
**   and all of the worker thread's attached FBs.  Events on the "note_fd" may be either "NEW_FABRIC_BUFFER" or
**   "DELETE_FABRIC_BUFFER", received with a parameter that is the FB containing the FD to be listened to or
**   else shutdown.  Events on the FBs FDs may be either readable, writable, or errors (which result in the
**   particular fabric connection being closed.)
**
**   Debugging Utilities:
**   --------------------
**
**   The state of the fabric (all FNEs and FBs) can be logged using the "dump-fabric:" Info command.
*/


/*
** Statics
** Right now we support only one callback for arrival of events and messages
*/

// #define DEBUG 1
// #define DEBUG_VERBOSE 1


typedef struct {

	as_fabric_event_fn 	event_cb;
	void 				*event_udata;

	// Arguably, these first two should be pushed into the msg system
	const msg_template 	*mt[M_TYPE_MAX];
	size_t 				mt_sz[M_TYPE_MAX];

	as_fabric_msg_fn 		msg_cb[M_TYPE_MAX];
	void 				*msg_udata[M_TYPE_MAX];

	cf_queue    *msg_pool_queue[M_TYPE_MAX];   // A pool of unused messages, better than calling create


	int			num_workers;
	pthread_t	workers_th[MAX_FABRIC_WORKERS];
	cf_queue	*workers_queue[MAX_FABRIC_WORKERS]; // messages to workers - type worker_queue_element
	int			workers_epoll_fd[MAX_FABRIC_WORKERS]; // have workers export the epoll fd

	pthread_t	accept_th;

	char		note_sockname[108];
	int			note_server_fd;
	pthread_t	note_server_th;

	int			note_clients;
	int			note_fd[MAX_FABRIC_WORKERS];

	pthread_t   node_health_th;

} fabric_args;


//
// This hash:
// key is cf_node, value is a *pointer* to a fabric_node_element
//
rchash *g_fabric_node_element_hash;

//
// A fabric_node_element is one-per-remote-endpoint
// it is stored in the fabric_node_element_hash, keyed by the node, so when a message_send
// is called we can find the queues, and it's linked from the fabric buffer which is
// attached to the file descriptor through the epoll args
//
// A fabric buffer sunk in an epoll holds a reference count on this object

typedef struct {

	cf_node 	node;   // when coming from a fd, we want to know the source node

	cf_atomic32 fd_counter;         // Count of open outbound FDs.
	shash      *connected_fb_hash;  // All connected outbound FBs attached to this FNE:
	// Key: fabric_buffer * ; Value: 0 (Arbitrary & unused.)

	bool        live;  // set to false on shutdown to prevent confusing incoming messages

	uint64_t    good_write_counter;
	uint64_t    good_read_counter;

	cf_queue 	*xmit_buffer_queue; // queue of currently unused fabric_buffers that can be written to
	// Queue contains: fabric_buffer *
	cf_queue_priority    *xmit_msg_queue; 	// queue of messages to be sent
	// 	queue contains: msg *

	uint32_t	parameter_gather_usec;
	uint32_t	parameter_msg_size;

} fabric_node_element;



//
// When we get notification about a socket, this is the structure
// that's in the data portion
// Tells you everything about what's currently pending to read and write
// on this descriptor, so you can call read and write
//

// Current state of a fabric buffer, idle or in-flight.
typedef enum fb_status_e {
	FB_STATUS_IDLE,                 // The FB is not being used.
	FB_STATUS_READ,                 // The FB has read data.
	FB_STATUS_WRITE,                // The FB is being written.
	FB_STATUS_ERROR                 // The FB is in an error state.
} fb_status;


// Inplace size
#define FB_INPLACE_SZ (1024 * 128)

typedef struct {

	int fd;
	int worker_id;

	bool nodelay_isset;
	bool keepalive_isset;

	fabric_node_element *fne;

	int         connected;          // Is this a connected outbound FB?

	fb_status   status;             // Is this FB in flight or idle?

	// this is the write section
	size_t		w_total_len;		// total size to write
	size_t		w_len;				// current size we've written
	bool		w_in_place;
	byte		w_data[ FB_INPLACE_SZ ];
	byte		*w_buf;

	// This is the read section
	uint32_t	r_msg_size; 		// size of the incoming message
	msg_type	r_type;             // type of the incoming message
	byte		*r_append;			// read from socket to here
	byte		*r_parse;			// parse from here
	byte		*r_end;				// the end of r_buf
	byte 		r_stack_buf[ FB_INPLACE_SZ ];
	byte 		*r_buf;				// may be r_stack_buf or allocated big buffer

} fabric_buffer;

//
// Worker queue
// Notification about various things, like a new file descriptor to manage
//
enum work_type {
	NEW_FABRIC_BUFFER,
	DELETE_FABRIC_BUFFER
};


typedef struct {
	enum work_type type;
	fabric_buffer *fb;
} worker_queue_element;


// A few select forward references
void fabric_worker_add(fabric_args *fa, fabric_buffer *fb);
void fabric_worker_delete(fabric_args *fa, fabric_buffer *fb);
void fabric_buffer_set_epoll_state(fabric_buffer *fb);
static void fabric_heartbeat_event(int nevents, as_hb_event_node *events, void *udata);
void fabric_buffer_release(fabric_buffer *fb);


//
// Ideally this would not be global, but there is in reality only one fabric,
// and the alternative would be to pass this value around everywhere.
static fabric_args *g_fabric_args = 0;


//
// The start message is always sent by the connecting device to specify
// what the remote endpoint's node ID is. We could pack other info
// in here as needed too
// The MsgType is used to specify the type, it's a good idea but not required

#define FS_FIELD_NODE 	 0
#define FS_ADDR          1
#define FS_PORT          2
#define FS_ANV           3

// Special message at the front to describe my node ID
msg_template fabric_mt[] = {
	{ FS_FIELD_NODE, M_FT_UINT64 },
	{ FS_ADDR, M_FT_UINT32 },
	{ FS_PORT, M_FT_UINT32 },
	{ FS_ANV, M_FT_BUF }
};

// Hash table of all fabric buffers.
//  Key: fabric_buffer * ; Value: 0 (Arbitrary & unused.)
//  (Used to supporting logging information about the fabric resources via the "dump_fabric" command.)
static shash *g_fb_hash;

//void
//as_fabric_nodeid_get(cf_node *node)
//{
//	*node = g_config.self_node;
//}


//
// regrettably, this may be called simultaneously from multiple nodes
// Reference counting system works like this:
// 'create' takes refcount to 1, and you can have no TCP connections
// but still a refcount
// Every buffer holds a reference count, since it has a pointer to the
// fne. Even buffers in the epoll code

fabric_node_element *
fne_create(cf_node node)
{
	fabric_node_element *fne;

	fne = cf_rc_alloc(sizeof(fabric_node_element));
	if (!fne) return 0;

	memset(fne, 0, sizeof(fabric_node_element));

	fne->node = node;
	fne->live = true;
	fne->xmit_buffer_queue = cf_queue_create(sizeof(fabric_buffer *), true);
	fne->xmit_msg_queue = cf_queue_priority_create(sizeof(msg *), true);
	if (SHASH_OK != shash_create(&(fne->connected_fb_hash), ptr_hash_fn, sizeof(fabric_buffer *), sizeof(int), 100, SHASH_CR_MT_BIGLOCK)) {
		cf_crash(AS_FABRIC, "failed to create connected_fb_hash for fne %p", fne);
	}

#ifdef EXTRA_CHECKS
	as_fabric_dump();
#endif

	if (RCHASH_OK != rchash_put_unique(g_fabric_node_element_hash, &node, sizeof(node), fne))
	{
		// already have this node, not really an error, hope this kind of thing doesn't happen often though
		cf_info(AS_FABRIC, " received second notification of already extant node: %"PRIx64, node);
		cf_queue_destroy(fne->xmit_buffer_queue);
		cf_queue_priority_destroy(fne->xmit_msg_queue);
		cf_rc_releaseandfree( fne );
		return fne;
	}

	cf_debug(AS_FABRIC, "create FNE: node %"PRIx64" fne %p", node, fne);

#ifdef EXTRA_CHECKS
	as_fabric_dump();
#endif

	return fne;
}

void
fne_destructor(void *fne_o)
{
	fabric_node_element *fne = (fabric_node_element *) fne_o;

	cf_debug(AS_FABRIC, "destroy FNE: fne %p", fne);

	int rv;

	// Empty out the queue
	if (fne->xmit_buffer_queue) {
		do {
			fabric_buffer *fb;
			rv = cf_queue_pop(fne->xmit_buffer_queue, &fb, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK) {
				cf_debug(AS_FABRIC, "fne_destructor(%p): releasing fb: %p", fne, fb);
				fabric_buffer_release(fb);
			} else {
				cf_debug(AS_FABRIC, "fnd_destructor(%p): xmit buffer queue empty", fne);
			}
		} while (rv == CF_QUEUE_OK);
		cf_queue_destroy(fne->xmit_buffer_queue);
	}

	// Empty out the queue
	do {
		msg *m;
		rv = cf_queue_priority_pop(fne->xmit_msg_queue, &m, CF_QUEUE_NOWAIT);
		if (rv == CF_QUEUE_OK) {
			cf_info(AS_FABRIC, "fabric node endpoint: destroy %"PRIx64" dropping message", fne->node);
			as_fabric_msg_put(m);
		} else {
			cf_debug(AS_FABRIC, "fne_destructor(%p): xmit msg queue empty", fne);
		}
	} while (rv == CF_QUEUE_OK);

	cf_queue_priority_destroy(fne->xmit_msg_queue);

	shash_destroy(fne->connected_fb_hash);
}


inline static void
fne_release(fabric_node_element *fne)
{

	if (0 == cf_rc_release(fne)) {
		fne_destructor(fne);
		cf_rc_free(fne);
	}
}


fabric_buffer *
fabric_buffer_create(int fd)
{
	fabric_buffer *fb = cf_rc_alloc(sizeof(fabric_buffer));

	fb->fd = fd;
	fb->worker_id = -1; // no worker assigned yet
	fb->nodelay_isset = false;
	fb->keepalive_isset = false;
	fb->fne = NULL;
	fb->connected = false; // not in the connected_fb_hash yet
	fb->status = FB_STATUS_IDLE;

	fb->w_total_len = 0;
	fb->w_len = 0;
	fb->w_in_place = true;
	fb->w_buf = NULL;

	fb->r_msg_size = 0;
	fb->r_type = M_TYPE_FABRIC; // since we don't have an "invalid"
	fb->r_buf = fb->r_stack_buf;
	fb->r_append = fb->r_buf;
	fb->r_parse = fb->r_buf;
	fb->r_end = fb->r_buf + FB_INPLACE_SZ;

	int value = 0; // (Arbitrary & unused.)
#ifdef CONNECTED_FB_HASH_USE_PUT_UNIQUE
	if (SHASH_OK != shash_put_unique(g_fb_hash, &fb, &value)) {
		cf_crash(AS_FABRIC, "failed to put unique in hash fb %p", fb);
	}
#else
	if (SHASH_OK == shash_get(g_fb_hash, &fb, &value)) {
		cf_warning(AS_FABRIC, "fbc(%d): fb %p unexpectedly found in g_fb_hash", fd, fb);
	} else if (SHASH_OK != shash_put(g_fb_hash, &fb, &value)) {
		cf_crash(AS_FABRIC, "failed to shash_put() into hash fb %p", fb);
	}
#endif

	return(fb);
}

void
fabric_buffer_associate(fabric_buffer *fb, fabric_node_element *fne)
{
	cf_rc_reserve(fne);
	fb->fne = fne;

//    cf_debug(AS_FABRIC, "associate: fne %p to fb %p (fne ref now %d)",fne,fb,cf_rc_count(fne));

}

void
fabric_buffer_release(fabric_buffer *fb)
{
	fb->status = FB_STATUS_IDLE;

	if (0 == cf_rc_release(fb)) {
#if 0		// super deep debug
		if (fb->fne) {
			if (fb->fne->xmit_buffer_queue)
				cf_debug(AS_FABRIC, "fabric buffer destroy fb %p fb-fne %p fb-fne-xmitbuf %p", fb, fb->fne, fb->fne->xmit_buffer_queue);
			else
				cf_debug(AS_FABRIC, "fabric buffer destroy fb %p fb-fne %p ", fb, fb->fne);
		}
		else {
			cf_debug(AS_FABRIC, "fabric buffer destroy fb %p no attached fne ", fb);
		}
#endif

//		cf_debug(AS_FABRIC, "fabric buffer destruction! fb %p fd %d",fb,fb->fd);

		if (fb->connected) {
			cf_debug(AS_FABRIC, "removing fb %p connected %d from fne %p connected_fb_hash", fb, fb->connected, fb->fne);
			if (SHASH_OK != shash_delete(fb->fne->connected_fb_hash, &fb)) {
				cf_crash(AS_FABRIC, "fb %p is connected to fne %p but not in connected_fb_hash", fb, fb->fne);
			} else {
				cf_debug(AS_FABRIC, "removed fb %p from connected_fb_hash", fb);
			}
		}

		if (fb->fne) {
			if (fb->connected) {
				fb->connected = false;
				cf_atomic32_decr(&(fb->fne->fd_counter));
			}

//            cf_debug(AS_FABRIC, "fb delete: fne %p refcount %d",fb->fne,cf_rc_count(fb->fne) );

			fne_release(fb->fne);
			fb->fne = 0;
		} else {
			cf_debug(AS_FABRIC, "(releasing fb %p not attached to an FNE)", fb);
		}

		// According to the documentation, this isnt necessary - see epoll(4)
		// int epoll_fd = g_fabric_args->workers_epoll_fd[fb->worker_id];
		// epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fb->fd, 0);

		close(fb->fd);
		fb->fd = -1;
		cf_atomic_int_incr(&g_config.fabric_connections_closed);

		// No longer assigned to a worker.
		fb->worker_id = -1;

		if (fb->w_total_len != fb->w_len)
			cf_debug(AS_FABRIC, "dropping message: TCP connection close with writable bytes %d", fb->w_total_len - fb->w_len);

		if (fb->r_buf != fb->r_stack_buf) {
			cf_free(fb->r_buf);
		}

		if (fb->w_in_place == false && fb->w_buf) cf_free(fb->w_buf);

#ifdef EXTRA_CHECKS
		// DEBUG - this is a large memset - not good for production
		memset(fb, 0xff, sizeof(fabric_buffer));
#endif

		if (SHASH_OK != shash_delete(g_fb_hash, &fb))
			cf_crash(AS_FABRIC, "failed to fb %p delete from hash table", fb);

		cf_rc_free(fb);
	}

}

//
// Fill the write buffer from the fabric node element's stash
// return false if there's nothing in this buffer
// true if there's still data to send

bool
fabric_buffer_write_fill( fabric_buffer *fb )
{
	fabric_node_element *fne = fb->fne;

	// check for fullness
	if (fb->w_total_len >= FB_INPLACE_SZ)	return(true);

	// See if we can fit any more
	int q_rv;
	do {
		msg *m;
		q_rv = cf_queue_priority_pop(fne->xmit_msg_queue, &m, CF_QUEUE_NOWAIT);
		if (q_rv == 0)
		{
			size_t	remain = FB_INPLACE_SZ - fb->w_total_len;
			if (0 != msg_fillbuf(m, &fb->w_data[fb->w_total_len], &remain)) {

				// this is OK in normal operation. It's the way we tell there's not enough
				// data left in the buffer
				cf_detail(AS_FABRIC, "write fill: fb %p return bad, either malloc or push back", fb);

				// not enough room left. If it's the first part of a message,
				// then malloc a new buffer
				if (remain > FB_INPLACE_SZ && fb->w_len == 0) {
					cf_debug(AS_FABRIC, "msg fillbuf returned fail, allocating new size %d", remain);
					fb->w_buf = cf_malloc(remain);
					if (fb->w_buf) {
						cf_atomic_int_incr(&g_config.fabric_msgs_sent);
						msg_fillbuf(m, fb->w_buf, &remain);
						fb->w_in_place = false;
						fb->w_total_len = remain;
					}
					as_fabric_msg_put(m); // this will drop, but we're out of memory
				}
				else {
					// a partially full buffer, kick it out and let this large one hit
					// an empty case. Hope that's OK.
					// put it back on the queue - don't know the priority, make it high
					cf_queue_priority_push(fne->xmit_msg_queue, &m, CF_QUEUE_PRIORITY_HIGH);
				}

				cf_detail(AS_FABRIC, "+ tot %d len %d", fb->w_total_len, fb->w_len);

				return(true);
			}
			cf_atomic_int_incr(&g_config.fabric_msgs_sent);
			fb->w_total_len += remain;
			as_fabric_msg_put(m);
		}
	} while(q_rv == 0 );

	cf_detail(AS_FABRIC, "fabric_buffer_write_fill: fb %p inplace %d ( %d : %d )", fb, fb->w_in_place, fb->w_total_len, fb->w_len);

	return ( (fb->w_total_len == fb->w_len) ? false : true );

}


// Copies the contents of the message into the beginning of the linear store
// write buffer and returns the message to the queue for reuse

bool
fabric_buffer_set_write_msg( fabric_buffer *fb, msg *m )
{
	// statistic - doesn't really show the message went out, though
	cf_atomic_int_incr(&g_config.fabric_msgs_sent);

	// Parse out the message to the inplace buffer
	fb->w_len = 0;
	fb->w_total_len = FB_INPLACE_SZ; // set the maximum for msg_fillbuf
	if (0 != msg_fillbuf(m, &fb->w_data[0], &fb->w_total_len)) {
		// msg_fillbuf says we don't have enough data, but has graciously suggested
		// the real amount of size we need
		cf_detail(AS_FABRIC, "msg fillbuf returned long buffer: allocating not-in-place %d", fb->w_total_len);
		fb->w_buf = cf_malloc(fb->w_total_len);
		if (fb->w_buf == 0)	return(false);
		msg_fillbuf(m, fb->w_buf, &fb->w_total_len);
		fb->w_in_place = false;
	}
	else {
		fb->w_in_place = true;
		fb->w_buf = 0;
		fb->status = FB_STATUS_WRITE;
	}
	as_fabric_msg_put(m);
	return(true);
}

/*
** Send a message to the worker attached to the fabric buffer asking to release it.
*/
int
fabric_disconnect_reduce_fn(void *key, void *data, void *udata)
{
	fabric_buffer *fb = * (fabric_buffer **) key;
	fabric_args *fa = (fabric_args *) udata;

	if (fb && (FB_STATUS_IDLE != fb->status)) {
		cf_warning(AS_FABRIC, "not freeing in-flight FB %p (status %d)", fb, fb->status);
		return 0;
	}

	if (fb && (fb->worker_id != -1)) {
		fabric_worker_delete(fa, fb);
	} else {
		cf_warning(AS_FABRIC, "fb %p has no worker_id ~~ Ignoring it!", fb);
	}

	return 0;
}

/*
** Disconnect a fabric node element and release its connected outbound fabric buffers.
*/
int
fabric_disconnect(fabric_args *fa, fabric_node_element *fne)
{
	int num_fbs = shash_get_size(fne->connected_fb_hash);
	int num_fds = cf_atomic32_get(fne->fd_counter);

	if (num_fbs > num_fds) {
		cf_warning(AS_FABRIC, "number of fabric buffers (%d) > number of open file descriptors (%d) for fne %p", num_fbs, num_fds, fne);
		// Shouldn't need to halt on this case, just warn.
//		cf_crash(AS_FABRIC, "number of fabric buffers (%d) > number of open file descriptors (%d) for fne %p", num_fbs, num_fds, fne);
	} else if (num_fbs < num_fds) {
		cf_warning(AS_FABRIC, "number of fabric buffers (%d) < number of open file descriptors (%d) for fne %p", num_fbs, num_fds, fne);
	}

	// When a fabric node node is disconnected, all of its connected outbound fabric buffers must be disconnected.
	if (num_fbs)
		shash_reduce(fne->connected_fb_hash, fabric_disconnect_reduce_fn, fa);

	return(0);
}

/*
** Create a connection to the remote node. This creates a non-blocking
** connection and adds it to the worker queue only, when the socket becomes
** writable, messages can start flowing
*/
int
fabric_connect(fabric_args *fa, fabric_node_element *fne)
{

	// Get the address of the remote endpoint
	cf_sockaddr so;
	if (0 > as_hb_getaddr(fne->node, &so)) {
		cf_debug(AS_FABRIC, "fabric_connect: unknown remote endpoint %"PRIx64, fne->node);
		return(-1);
	}

	// Initiate the connect to the remote endpoint
	int fd;
	if (0 != cf_socket_connect_nb(so, &fd)) {
		cf_debug(AS_FABRIC, "fabric connect could not create connect");
		return(-1);
	}

	cf_atomic_int_incr(&g_config.fabric_connections_opened);

	// Create a fabric buffer to go along with the file descriptor
	fabric_buffer *fb = fabric_buffer_create(fd);
	fabric_buffer_associate(fb, fne);

	// Grab a start message, send it to the remote endpoint so it knows me
	msg *m = as_fabric_msg_get(M_TYPE_FABRIC);
	if (!m) {
		fabric_buffer_release(fb);
		return(-1);
	}

	struct in_addr self;
	msg_set_uint64(m, FS_FIELD_NODE, g_config.self_node); // identifies self to remote
	if (AS_HB_MODE_MESH == g_config.hb_mode) {

		// If the user specified 'any' as heartbeat address, we listen on 0.0.0.0 (all interfaces)
		// But we should send a proper IP address to the remote machine to send back heartbeat.
		// Use the node's IP address in this case.
		char *hbaddr_to_use = g_config.hb_addr;
		// Checking the first byte is enough as '0' cannot be a valid IP address other than 0.0.0.0
		if (*hbaddr_to_use == '0') {
			cf_debug(AS_HB, "Sending %s as nodes IP to return heartbeat", g_config.node_ip);
			hbaddr_to_use = g_config.node_ip;
		}

		if (1 != inet_pton(AF_INET, hbaddr_to_use, &self))
			cf_warning(AS_HB, "unable to call inet_pton: %s", cf_strerror(errno));
		else {
			msg_set_uint32(m, FS_ADDR, *(uint32_t *)&self);
			msg_set_uint32(m, FS_PORT, g_config.hb_port);
		}
	}
	msg_set_buf(m, FS_ANV, (byte *)g_config.paxos->succession, sizeof(cf_node) * g_config.paxos_max_cluster_size, MSG_SET_COPY);

	fabric_buffer_set_write_msg(fb, m);

	// pass the fabric_buffer to a worker thread
	fabric_worker_add(fa, fb);

//	cf_debug(AS_FABRIC, "fabric_connect(): adding to fne %p : fb %p w/ wid %d", fne, fb, fb->worker_id);

	int value = 0; // (Arbitrary & unused.)
	int rv = 0;
	if (SHASH_OK == shash_get(fne->connected_fb_hash, fb, &value)) {
		cf_warning(AS_FABRIC, "fb %p is already in fne %p connected_fb_hash (connected: %d)", fb, fne, fb->connected);
#ifdef CONNECTED_FB_HASH_USE_PUT_UNIQUE
	} else if (SHASH_OK != (rv = shash_put_unique(fne->connected_fb_hash, &fb, &value))) {
		cf_crash(AS_FABRIC, "failed to add unique fb %p to fne %p connected_fb_hash -- rv %d", fb, fne, rv);
#else
	} else if (SHASH_OK != (rv = shash_put(fne->connected_fb_hash, &fb, &value))) {
		cf_crash(AS_FABRIC, "failed to shash_put() fb %p into fne %p connected_fb_hash -- rv %d", fb, fne, rv);
#endif
	}

	fb->connected = true;

	return(0);
}

//
// Called when a message has written a few bytes
//
// MIGHT HAVE THE SIDE EFFECT OF FREEING THE FABRIC BUFFER!!!!

void
fabric_write_complete(fabric_buffer *fb)
{
#ifdef DEBUG_VERBOSE
	cf_debug(AS_FABRIC, "fabric_write_complete: fb %p", fb);
#endif

	// if we still have bytes to write, don't add more to the buffer
	// todo: make this cooler
	if (fb->w_len != fb->w_total_len)
		return;

	// Reset the write components
	fb->w_len = 0;
	fb->w_total_len = 0;
	fb->w_in_place = true;
	if (fb->w_buf) {
		cf_free(fb->w_buf);
		fb->w_buf = 0;
	}

	if (fb->fne->live == false) {
		fabric_buffer_release(fb);
	}
	else {
		// Is there a message waiting that I can send?
		if (false == fabric_buffer_write_fill( fb))
		{
			cf_detail(AS_FABRIC, "fabric_write_complete: no buffers extant, sleeping %p", fb);

			// patch up epoll state - no longer writable
			fabric_buffer_set_epoll_state(fb);

			cf_rc_reserve(fb);
			cf_queue_push(fb->fne->xmit_buffer_queue, &fb);
		}
	}
}

static void
fabric_set_keepalive_options(fabric_buffer *fb)
{
	if (!fb->keepalive_isset && g_config.fabric_keepalive_enabled) {
		int value = 1;

		cf_debug(AS_FABRIC, "Setting keepalive on fd %d to %s time: %d intvl: %d probes: %d",
				   fb->fd, g_config.fabric_keepalive_enabled ? "ON" : "OFF",
				   g_config.fabric_keepalive_time, g_config.fabric_keepalive_intvl,
				   g_config.fabric_keepalive_probes);

		if (0 > setsockopt(fb->fd, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value))) {
			cf_warning(AS_FABRIC, "setsockopt: SO_KEEPALIVE (fd %d; errno %d)", fb->fd, errno);
		}

		value = g_config.fabric_keepalive_time;
		if ((value > 0) && (0 > setsockopt(fb->fd, IPPROTO_TCP, TCP_KEEPIDLE, &value, sizeof(value)))) {
			cf_warning(AS_FABRIC, "setsockopt: TCP_KEEPIDLE (fd %d; errno %d)", fb->fd, errno);
		}

		value = g_config.fabric_keepalive_intvl;
		if ((value > 0) && (0 > setsockopt(fb->fd, IPPROTO_TCP, TCP_KEEPINTVL, &value, sizeof(value)))) {
			cf_warning(AS_FABRIC, "setsockopt: TCP_KEEPINTVL (fd %d; errno %d)", fb->fd, errno);
		}

		value = g_config.fabric_keepalive_probes;
		if ((value > 0) && (0 > setsockopt(fb->fd, IPPROTO_TCP, TCP_KEEPCNT, &value, sizeof(value)))) {
			cf_warning(AS_FABRIC, "setsockopt: TCP_KEEPCNT (fd %d; errno %d)", fb->fd, errno);
		}

		fb->keepalive_isset = true;
	}
}

int
fabric_process_writable(fabric_buffer *fb)
{
	int w_sz;

	// cf_detail(AS_FABRIC,"fabric_writable: fb %p fd %d ( %d : %d )",fb, fb->fd, fb->w_total_len, fb->w_len);

	// writable and nothing to write? A travesty! Fill me up, or try to
	if ((fb->w_total_len == 0) || (fb->w_total_len - fb->w_len == 0))
	{
		if (fabric_buffer_write_fill( fb ) == false) {
			// patch up epoll state - no longer writable
			fabric_buffer_set_epoll_state(fb);
			// Put fabric_buffer on waiting queue - always holds a ref while on the queue
			cf_rc_reserve(fb);
			cf_queue_push(fb->fne->xmit_buffer_queue, &fb);
			return(0);
		}
	}

	if (fb->fne->parameter_gather_usec) {
		if (fb->w_total_len - fb->w_len < fb->fne->parameter_msg_size) {
			usleep( fb->fne->parameter_gather_usec );
			fabric_buffer_write_fill( fb );
		}
	}

	uint32_t w_len = fb->w_total_len - fb->w_len;
	if (w_len < 500)
		cf_atomic_int_incr(&g_config.fabric_write_short);
	else if (w_len < (4 * 1024))
		cf_atomic_int_incr(&g_config.fabric_write_medium);
	else
		cf_atomic_int_incr(&g_config.fabric_write_long);

	if (fb->nodelay_isset == false) {
		int flag = 1;
		setsockopt(fb->fd, SOL_TCP, TCP_NODELAY, &flag, sizeof(flag));
		fb->nodelay_isset = true;
	}

	fabric_set_keepalive_options(fb);

	if (fb->w_in_place) {
		// cf_assert(fb->fd, AS_FABRIC, CF_WARNING, "attempted write to fd 0");
		if (0 > (w_sz = send(fb->fd, &fb->w_data[fb->w_len], w_len, MSG_NOSIGNAL))) {
			if (errno == EAGAIN)	return(0);
			else if (errno == EFAULT) {
				cf_debug(AS_FABRIC, "write returned efault: data %p len %d", &fb->w_data[fb->w_len], fb->w_total_len - fb->w_len);
			}
			else {
				cf_debug(AS_FABRIC, "fabric_process_writable: write return less than 0 %d", errno);
			}
			return(-1);
		}

		// cf_detail(AS_FABRIC,"fabric_writable: wrote %d",w_sz);

		// it was a very good write!
		fb->fne->good_write_counter = 0;

		fb->w_len += w_sz;
		if (fb->w_len == fb->w_total_len) {
			// side effect: fb might not be any good after here
			fabric_write_complete(fb);
		}
	}
	else {
		// cf_assert(fb->fd, AS_FABRIC, CF_WARNING, "attempted write to fd 0");
		if (0 > (w_sz = send(fb->fd, fb->w_buf + fb->w_len, w_len, MSG_NOSIGNAL))) {
			if (errno == EAGAIN) {
				return 0;
			}
			cf_debug(AS_FABRIC, "fabric_process_writable: return less than 0 %d", errno);
			return(-1);
		}

		// cf_detail(AS_FABRIC,"fabric_writable: wrote malloc: %d",w_sz);

		// it was a very good write!
		fb->fne->good_write_counter = 0;

		fb->w_len += w_sz;
		if (w_sz > 0) {
			// side effect: fb might not be any good after here
			fabric_write_complete(fb);
		}
	}

	return(0);
}

// Log information about existing "msg" objects and queues.
void
as_fabric_msg_queue_dump()
{
	cf_info(AS_FABRIC, "All currently-existing msg types:");
	int total_q_sz = 0;
	int total_alloced_msgs = 0;
	for (int i = 0; i < M_TYPE_MAX; i++) {
		int q_sz = cf_queue_sz(g_fabric_args->msg_pool_queue[i]);
		int num_of_type = cf_atomic_int_get(g_num_msgs_by_type[i]);
		total_alloced_msgs += num_of_type;
		if (q_sz || num_of_type) {
			cf_info(AS_FABRIC, "|msgq[%d]| = %d ; alloc'd = %d", i, q_sz, num_of_type);
			total_q_sz += q_sz;
		}
	}
	int num_msgs = cf_atomic_int_get(g_num_msgs);
	if (abs(num_msgs - total_alloced_msgs) > 2) {
		cf_warning(AS_FABRIC, "num msgs (%d) != total alloc'd msgs (%d)", num_msgs, total_alloced_msgs);
	}
	cf_info(AS_FABRIC, "Total num. msgs = %d ; Total num. queued = %d ; Delta = %d", num_msgs, total_q_sz, num_msgs - total_q_sz);
}

// Helper function. Pull a message off the internal queue.
msg *
as_fabric_msg_get(msg_type type)
{
	// What's coming in is actually a network value, so should be validated a bit
	if (type >= M_TYPE_MAX)
		return(0);
	if (g_fabric_args->mt[type] == 0)
		return(0);

	msg *m = 0;
	cf_queue *q = g_fabric_args->msg_pool_queue[type];

	if (0 != cf_queue_pop(q, &m, CF_QUEUE_NOWAIT)) {
		msg_create(&m, 	type, g_fabric_args->mt[type], g_fabric_args->mt_sz[type]);
	}
	else {
		msg_incr_ref(m);
	}

//	cf_debug(AS_FABRIC,"fabric_msg_get: m %p count %d",m,cf_rc_count(m));

	return(m);
}

void
as_fabric_msg_put(msg *m)
{
	// Debug: validate that there was a reference count held before decrementing
	cf_atomic_int_t d_cnt = cf_rc_count(m);
	if (d_cnt <= 0) {
		cf_debug(AS_FABRIC, "as_fabric_msg_put:  bad scene, ref count already low: m %p cnt %d", m, d_cnt);
		return;
	}

	// when it's in the queue, it has a reference count of 0
	// thus, you can pull a message off and directly free it
//	cf_debug(AS_FABRIC,"fabric_msg_put: check the use count %p %d",m,m->type);
	cf_atomic_int_t	 cnt = cf_rc_release(m);
	if (cnt == 0) {
//		cf_debug(AS_FABRIC,"fabric_msg_put: resetting %p and adding to queue type %d",m,m->type);
		msg_reset(m);
		if (cf_queue_sz(g_fabric_args->msg_pool_queue[m->type]) > 128) {
			msg_put(m);
		}
		else {
			cf_queue_push(g_fabric_args->msg_pool_queue[m->type], &m);
		}
	}
//	else {
//		cf_debug(AS_FABRIC,"fabric_msg_put: decr %p to %d",m,cnt);
//	}
}

void
fabric_buffer_shift(fabric_buffer *fb, uint32_t parsable_size)
{
	if (fb->r_parse != fb->r_buf) {
		// Can overlap. (Though r_buf < r_parse, so memcpy() would work here.)
		memmove(fb->r_buf, fb->r_parse, parsable_size);
		fb->r_parse = fb->r_buf;
		fb->r_append = fb->r_parse + parsable_size;
	}
}

//
// Helper function
// Return true if you need to be called again on this buffer
// because there might be more messages
//

bool
fabric_process_read_msg(fabric_buffer *fb)
{
	uint32_t parsable_size = (uint32_t)(fb->r_append - fb->r_parse);

	if (fb->r_msg_size == 0) {
		if (0 != msg_get_initial(&fb->r_msg_size, &fb->r_type, fb->r_parse, parsable_size)) {
			fabric_buffer_shift(fb, parsable_size);
			return false;
		}

		cf_detail(AS_FABRIC, "length required: %u", fb->r_msg_size);

		if (fb->r_msg_size > FB_INPLACE_SZ) {
			fb->r_buf = cf_malloc(fb->r_msg_size);
			fb->r_end = fb->r_buf + fb->r_msg_size;
			fabric_buffer_shift(fb, parsable_size);
			return false;
		}
	}

	// Return quick if we can't parse a full message.
	if (fb->r_msg_size > parsable_size) {
		fabric_buffer_shift(fb, parsable_size);
		return false;
	}

	if (fb->fne == 0) {
		// We have not yet associated this incoming connection with an endpoint,
		// so we need to read in the special fabric message that contains the
		// unique fabric address.
		msg *m = as_fabric_msg_get(M_TYPE_FABRIC);

		if (msg_parse(m, fb->r_parse, fb->r_msg_size, true) != 0) {
			cf_warning(AS_FABRIC, "fabric_process_read: msg_parse failed fabric msg");
			// TODO: Should never happen, because the first message in a connection should be short
			// abandon this connection is OK
			return false;
		}

		int rv;

		// create as_hb_pulse - code copied from as_hb_rx_process
		cf_node node;
		cf_sockaddr socket;
		uint32_t addr;
		uint32_t port;
		cf_node *buf;
		size_t bufsz;

		int fd = fb->fd;

		if (0 != msg_get_uint64(m, FS_FIELD_NODE, &node))
			goto Next;

		if (AS_HB_MODE_MCAST == g_config.hb_mode) {
			struct sockaddr_in addr_in;
			socklen_t addr_len = sizeof(addr_in);
			if (0 != getpeername(fd, (struct sockaddr*)&addr_in, &addr_len))
				goto Next;

			char some_addr[24];
			if (NULL == inet_ntop(AF_INET, &addr_in.sin_addr.s_addr, (char *)some_addr, sizeof(some_addr)))
				goto Next;

			cf_debug(AS_HB, "getpeername | %s:%d (node = %"PRIx64", fd = %d)", some_addr, ntohs(addr_in.sin_port), node, fd);

			cf_sockaddr_convertto(&addr_in, &socket);
		} else if (AS_HB_MODE_MESH == g_config.hb_mode) {
			if (0 != msg_get_uint32(m, FS_ADDR, &addr))
				goto Next;
			if (0 != msg_get_uint32(m, FS_PORT, &port))
				goto Next;
		}

		// Get the succession list from the pulse message
		if (0 != msg_get_buf(m, FS_ANV, (byte **)&buf, &bufsz, MSG_GET_DIRECT))
			goto Next;

		if (bufsz != (g_config.paxos_max_cluster_size * sizeof(cf_node))) {
			cf_warning(AS_HB, "Corrupted data? The size of anv is inaccurate. Received: %d ; Expected: %d", bufsz, (g_config.paxos_max_cluster_size * sizeof(cf_node)));
			goto Next;
		}

		as_hb_process_fabric_heartbeat(node, fd, socket, addr, port, buf, bufsz);

Next:
		as_fabric_msg_put(m);

		cf_debug(AS_FABRIC, "received and parse connection start message: from node %"PRIx64, node);

		// Have node, attach this read buffer to a fne
		fabric_node_element *fne;
		rv = rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **) &fne );
		if (RCHASH_OK != rv) {
			// This happens sometimes. I might get a connection request before I get a ping
			// stating that anything exists. So make, then try again
			fne = fne_create(node);

			cf_debug(AS_FABRIC, "created an FNE %p for node %p the weird way from incoming fabric msg!", fne, node);

			rv = rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **) &fne );
			if (RCHASH_OK != rv) {
				// I get a connection.
				cf_debug(AS_FABRIC, "fabric_process_read_msg: cf_node unknown, can't create new endpoint descriptor odd %d", rv);
				return false;
			}
		}

		fabric_buffer_associate(fb, fne);

		fne_release(fne);

		fb->status = FB_STATUS_IDLE;
	}
	else {
		// Standard path - parse into a regular message
		msg *m = as_fabric_msg_get(fb->r_type);

		if (! m) {
			// this can happen if the TCP stream gets horribly out of sync
			// or if we're way out of memory --- close the connection and let
			// the error paths take care of it
			cf_warning(AS_FABRIC, "msg_parse could not parse message, for type %d", fb->r_type);
			shutdown(fb->fd, SHUT_WR);
			return false;
		}

		if (msg_parse(m, fb->r_parse, fb->r_msg_size, true) != 0) {
			cf_warning(AS_FABRIC, "msg_parse failed regular message, not supposed to happen: fb %p", fb);
			shutdown(fb->fd, SHUT_WR);
			return false;
		}

		// todo: have a full message, call the callback with the message

		cf_detail(AS_FABRIC, "msg_read: received msg: type %d node %"PRIx64, m->type, fb->fne->node);

		// statistic - received a message.
		cf_atomic_int_incr(&g_config.fabric_msgs_rcvd);
		// and it was a good read
		fb->fne->good_read_counter = 0;

		// deliver to registered guy
		if (g_fabric_args->msg_cb[m->type]) {
			(*g_fabric_args->msg_cb[m->type]) (fb->fne->node, m, g_fabric_args->msg_udata[m->type]);
		}
		else {
			cf_info(AS_FABRIC, "msg_read: could not deliver message type %d", m->type);
			as_fabric_msg_put(m);
		}
	}

	fb->r_parse += fb->r_msg_size;
	fb->r_msg_size = 0;

	if (fb->r_parse < fb->r_append) {
		// This wasn't the last message in available bytes. (Note - can't get
		// here with an allocated big buffer.)
		return true;
	}

	fb->status = FB_STATUS_IDLE;

	// If we used an allocated big buffer, we're done with it here - free it and
	// restore stack buffer mode.
	if (fb->r_buf != fb->r_stack_buf) {
		cf_free(fb->r_buf);
		fb->r_buf = fb->r_stack_buf;
		fb->r_end = fb->r_buf + FB_INPLACE_SZ;
	}

	fb->r_append = fb->r_buf;
	fb->r_parse = fb->r_buf;

	return false;
}

int
fabric_process_readable(fabric_buffer *fb)
{
	int32_t	rsz;

	if (0 >= (rsz = read(fb->fd, fb->r_append, fb->r_end - fb->r_append))) {
		cf_debug(AS_FABRIC, "read returned %d, broken connection? %d %s", rsz, errno, cf_strerror(errno));
		return -1;
	}

	fb->r_append += rsz;

	while (fabric_process_read_msg(fb)) {
		;
	}

	if (rsz < 500)
		cf_atomic_int_incr(&g_config.fabric_read_short);
	else if (rsz < (4 * 1024))
		cf_atomic_int_incr(&g_config.fabric_read_medium);
	else
		cf_atomic_int_incr(&g_config.fabric_read_long);

	return 0;
}

//
// Sets the epoll mask for the related file descriptor according to what we
// need to do at the moment
// This is a little sketchy because we've got multiple threads hammering this, but
// it should be safe, maybe turning some of these into atomics would help
//
void
fabric_buffer_set_epoll_state(fabric_buffer *fb)
{
	fb->status = FB_STATUS_IDLE;

	struct epoll_event ev;
	memset(&ev, 0, sizeof(struct epoll_event));
	ev.data.ptr = fb;
	ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
	if (fb->w_total_len > fb->w_len) {
		ev.events |= EPOLLOUT;
	}

	epoll_ctl( g_fabric_args->workers_epoll_fd[ fb->worker_id ], EPOLL_CTL_MOD, fb->fd, &ev);
}

//
// Decide which worker to send to
// Put a message on that worker's queue
// send a byte to the worker over the notification FD
//

static int worker_add_index = 0;

void
fabric_worker_delete(fabric_args *fa, fabric_buffer *fb)
{
	worker_queue_element e;

	e.type = DELETE_FABRIC_BUFFER;
	e.fb = fb;

	int worker = fb->worker_id;

	// This should never happen, but if it does, try to survive.
	if (worker >= fa->num_workers) {
		cf_warning(AS_FABRIC, "when deleting fb %p, worker (%d) > num_workers (%d) ~~ ignoring request", fb, worker, fa->num_workers);
		return;
	}

	if (0 > worker) {
		cf_warning(AS_FABRIC, "fabric_worker_delete(): someone else got to fb %p first (worker: %d)", fb, worker);
		return;
	}

	cf_debug(AS_FABRIC, "sending a DELETE_FABRIC_BUFFER note: fb %p to worker %d", fb, worker);

	cf_queue_push( fa->workers_queue[worker], &e);

	// Write a byte to his file scriptor too
	byte note_byte = 1;
	cf_assert(fa->note_fd[worker], AS_FABRIC, CF_WARNING, "attempted write to fd 0");
	if (1 != send(fa->note_fd[worker], &note_byte, sizeof(note_byte), MSG_NOSIGNAL)) {
		// TODO!
		cf_info(AS_FABRIC, "can't write to notification file descriptor: will probably have to take down process");
	}
}

void
fabric_worker_add(fabric_args *fa, fabric_buffer *fb)
{
	worker_queue_element e;

	e.type = NEW_FABRIC_BUFFER;
	e.fb = fb;

	// decide which queue to add to -- try round robin for the moment
	int worker;
	do {
		worker = worker_add_index % fa->num_workers;
		worker_add_index++;
	} while(fa->note_fd[worker] == 0);

	cf_debug(AS_FABRIC, "worker_fabric_add: adding fd %d to worker id %d notefd %d", fb->fd, worker, fa->note_fd[worker]);

	fb->worker_id = worker;

	cf_queue_push( fa->workers_queue[worker], &e);

	// Write a byte to his file descriptor too
	byte note_byte = 1;
	cf_assert(fa->note_fd[worker], AS_FABRIC, CF_WARNING, "attempted write to fd 0");
	if (1 != send(fa->note_fd[worker], &note_byte, sizeof(note_byte), MSG_NOSIGNAL)) {
		// TODO!
		cf_info(AS_FABRIC, "can't write to notification file descriptor: will probably have to take down process");
	}
}

void *
fabric_worker_fn(void *argv)
{
	fabric_args *fa = (fabric_args *) argv;

	// Figure out my thread index
	pthread_t self = pthread_self();
	int       worker_id;
	for (worker_id = 0; worker_id < MAX_FABRIC_WORKERS; worker_id++) {
		if (pthread_equal(fa->workers_th[worker_id], self) != 0)
			break;
	}
	if (worker_id == MAX_FABRIC_WORKERS) {
		cf_debug(AS_FABRIC, "fabric_worker_thread: could not figure own ID, bogus, exit, fu!");
		return(0);
	}

	cf_debug(AS_FABRIC, "fabric worker created: index %d", worker_id);

	// Create my epoll fd, register in the global list
	// TODO: something fancier than the 512 here
	int epoll_fd;
	epoll_fd = epoll_create(512);
	fa->workers_epoll_fd[worker_id] = epoll_fd;

	// Connect to the notification socket
	struct sockaddr_un note_so;
	int note_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (note_fd < 0) {
		cf_debug(AS_FABRIC, "Could not create socket for notification thread");
		return(0);
	}
	note_so.sun_family = AF_UNIX;
	strcpy(note_so.sun_path, fa->note_sockname);
	int len = strlen(note_so.sun_path) + sizeof(note_so.sun_family);
	if (connect(note_fd, (struct sockaddr *) &note_so, len) == -1) {
		cf_debug(AS_FABRIC, "could not connect to notification socket");
		return(0);
	}
	// Write the one byte that is my index
	byte fd_idx = worker_id;
	cf_assert(note_fd, AS_FABRIC, CF_WARNING, "attempted write to fd 0");
	if (1 != send(note_fd, &fd_idx, sizeof(fd_idx), MSG_NOSIGNAL)) {
		// TODO
		cf_debug(AS_FABRIC, "can't write to notification fd: probably need to blow up process errno %d", errno);
	}

	// File my notification information
	struct epoll_event ev;
	memset(&ev, 0, sizeof(ev));
	ev.data.fd = note_fd;
	ev.events = EPOLLIN | EPOLLERR ; // will be receiving 1 byte reads
	epoll_ctl(epoll_fd, EPOLL_CTL_ADD, note_fd, &ev);

	/* Demarshal transactions from the socket */
	for ( ; ; ) {
		struct epoll_event events[64];
		memset(&events[0], 0, sizeof(events));
		int nevents = epoll_wait(epoll_fd, events, 64, -1);

		/* Iterate over all events */
		for (int i = 0; i < nevents; i++) {

			if (events[i].data.fd == note_fd) {

				if (events[i].events & EPOLLIN) {

					// read the notification byte out
					byte note_byte;

					read(note_fd, &note_byte, sizeof(note_byte));

					// Got some kind of notification - check my queue
					worker_queue_element wqe;
					if (0 == cf_queue_pop(fa->workers_queue[worker_id], &wqe, 0)) {

						// Got an element - probably a new FD to look after, what else?
						if (wqe.type == NEW_FABRIC_BUFFER) {

							fabric_buffer *fb = wqe.fb;

							struct epoll_event ev;
							memset(&ev, 0, sizeof(ev));
							ev.data.ptr = fb;
							ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
							if (fb->w_total_len > fb->w_len) ev.events |= EPOLLOUT;
							if (0 != epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fb->fd, &ev)) {
								cf_debug(AS_FABRIC, "fabric_worker_fn: epoll_ctl failed %s", cf_strerror(errno));
							}
						} else if (DELETE_FABRIC_BUFFER == wqe.type) {
							fabric_buffer *fb = wqe.fb;

							cf_debug(AS_FABRIC, "received DELETE_FABRIC_BUFFER note: fb %p", fb);

							if (FB_STATUS_IDLE != fb->status) {
								cf_warning(AS_FABRIC, "received a DELETE message for a non-idle FB %p (status %d) ~~ Ignorning", fb, fb->status);
							} else {
								cf_debug(AS_FABRIC, "shutting down fd %d", fb->fd);
								if (fb) {
									shutdown(fb->fd, SHUT_RDWR);
								} else {
									cf_warning(AS_FABRIC, "got a NULL fb ~~ Ignorning");
								}
							}
						}
						else {
							cf_debug(AS_FABRIC, "worker %d received unknown notification on queue", worker_id, wqe.type);
						}
					}
				}
			}
			else {

				// It's one of my worker FBs. Do something cool.

				fabric_buffer *fb = events[i].data.ptr;

				cf_detail(AS_FABRIC, "epoll trigger: fd %d events %x", fb->fd, events[i].events);

				if (fb->fne && (fb->fne->live == false)) {

					fb->status = FB_STATUS_ERROR;

					epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fb->fd, 0);
					fabric_buffer_release(fb);
					fb = 0;
					continue;
				}

				if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {

					fb->status = FB_STATUS_ERROR;

					cf_debug(AS_FABRIC, "epoll : error, will close: fb %p fd %d errno %d", fb, fb->fd, errno);
					epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fb->fd, 0);
					fabric_buffer_release(fb);
					fb = 0;
					continue;
				}
				if (events[i].events & EPOLLIN) {

					cf_detail(AS_FABRIC, "fabric_buffer_readable %p", fb);

					fb->status = FB_STATUS_READ;

					if (fabric_process_readable(fb) < 0) {
						epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fb->fd, 0);
						fabric_buffer_release(fb);
						fb = 0;
						continue;
					}
				}
				if (events[i].events & EPOLLOUT) {

					cf_detail(AS_FABRIC, "fabric_buffer_writable: %p", fb);

					fb->status = FB_STATUS_WRITE;

					if (fabric_process_writable(fb) < 0) {
						epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fb->fd, 0);
						fabric_buffer_release(fb);
						fb = 0;
						continue;
					}
				}

			}

			/* We should never be canceled externally, but just in case... */
			pthread_testcancel();
		}
	}

	return(0);
}


// Do the network accepts here, and allocate a file descriptor to
// an epoll worker thread

void *
fabric_accept_fn(void *argv)
{
	fabric_args *fa = (fabric_args *) argv;

	// Create listener socket
	cf_socket_cfg sc;
	sc.addr = "0.0.0.0";     // inaddr any!
	sc.port = g_config.fabric_port;
	sc.reuse_addr = (g_config.socket_reuse_addr) ? true : false;
	sc.proto = SOCK_STREAM;
	if (0 != cf_socket_init_svc(&sc)) {
		cf_crash(AS_FABRIC, "Could not create fabric listener socket - check configuration");
	}

	cf_debug(AS_FABRIC, "fabric_accept: creating listener");

	do {
		/* Accept new connections on the service socket */
		int csocket;
		struct sockaddr_in caddr;
		socklen_t clen = sizeof(caddr);
		char cpaddr[24];

		if (-1 == (csocket = accept(sc.sock, (struct sockaddr *)&caddr, &clen))) {
			if (errno == EMFILE) {
				cf_info(AS_FABRIC, " warning : low on file descriptors ");
				continue;
			}
			else {
				cf_crash(AS_FABRIC, "accept: %d %s", errno, cf_strerror(errno));
			}
		}
		if (NULL == inet_ntop(AF_INET, &caddr.sin_addr.s_addr, (char *)cpaddr, sizeof(cpaddr)))
			cf_crash(AS_FABRIC, "inet_ntop(): %s", cf_strerror(errno));

		cf_debug(AS_FABRIC, "fabric_accept: accepting new sock %d", csocket);

		/* Set the socket to nonblocking */
		if (-1 == cf_socket_set_nonblocking(csocket)) {
			cf_info(AS_FABRIC, "unable to set client socket to nonblocking mode");
			close(csocket);
			continue;
		}

		cf_atomic_int_incr(&g_config.fabric_connections_opened);

		/* Create new fabric buffer, but don't yet know
		   the remote endpoint, so the FNE is not associated yet */
		fabric_buffer *fb = fabric_buffer_create(csocket);

		fabric_worker_add(fa, fb);

	} while(1);

	return(0);
}

// Accept connections to the notification function here


void *
fabric_note_server_fn(void *argv)
{
	fabric_args *fa = (fabric_args *) argv;

	do {
		int fd;
		struct sockaddr_un caddr;
		socklen_t clen = sizeof(caddr);

		/* Accept new connections on the notification socket */
		if (-1 == (fd = accept(fa->note_server_fd, (struct sockaddr *)&caddr, &clen))) {
			cf_debug(AS_FABRIC, "note_server: accept failed: fd %d clen %d : %s", fa->note_server_fd, clen,
					 cf_strerror(errno));
			return(0);
		}

		// Wait for the single byte which tell me which index this is
		byte fd_idx;
		if (1 != read(fd, &fd_idx, sizeof(fd_idx))) {
			cf_debug(AS_FABRIC, "note_server: could not read index of fd");
			close(fd);
			continue;
		}

		/* Set the socket to nonblocking */
		if (-1 == cf_socket_set_nonblocking(fd)) {
			cf_info(AS_FABRIC, "unable to set client socket to nonblocking mode");
			close(fd);
			continue;
		}

		cf_atomic_int_incr(&g_config.fabric_connections_opened);

		cf_debug(AS_FABRIC, "Notification server: received connect from index %d", fd_idx);

		/* file this worker in the list */
		fa->note_fd[fd_idx] = fd;
		fa->note_clients++;

	} while(1);

	return(0);
}


int
as_fabric_get_node_lasttime(cf_node node, uint64_t *lasttime)
{
	// Look up the node's FNE
	fabric_node_element *fne;
	if (RCHASH_OK != rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **) &fne)) {
		return(-1);
	}

	// get the last time read
	*lasttime = fne->good_read_counter;

	cf_debug(AS_FABRIC, "asking about node %"PRIx64" good read %u good write %u",
			 node, (uint)fne->good_read_counter, (uint)fne->good_write_counter);

	fne_release(fne);

	return(0);

}

//
// The node health function:
// Every N millisecond, it increases the 'good read counter' and 'good write counter'
// on each node element.
// Every read and write stamps those values back to 0. Thus, you can easily pick up
// a node and see how healthy it is, but only when you suspect trouble. With a capital T!
//

static int
fabric_node_health_reduce_fn(void *key, uint32_t keylen, void *data, void *udata)
{
	fabric_node_element *fne = (fabric_node_element *) data;
	uint32_t  incr = *(uint32_t *) udata;

	fne->good_write_counter += incr;
	fne->good_read_counter += incr;
	return(0);
}

#define FABRIC_HEALTH_INTERVAL 40   // ms

void *
fabric_node_health_fn( void *argv )
{
	do {
		uint64_t start = cf_getms();

		usleep( FABRIC_HEALTH_INTERVAL * 1000);

		uint32_t ms = cf_getms() - start;

		rchash_reduce ( g_fabric_node_element_hash  , fabric_node_health_reduce_fn, &ms);

	} while (1);

	return(0);
}

static void
fabric_node_disconnect(cf_node node)
{
	fabric_node_element *fne;

	if (RCHASH_OK == rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **)&fne)) {

		cf_info(AS_FABRIC, "fabric disconnecting node: %"PRIx64, node);

		fne->live = false;

		if (RCHASH_OK != rchash_delete(g_fabric_node_element_hash, &node, sizeof(node) ) ) {

			cf_info(AS_FABRIC, "fabric disconnecting FAIL rchash delete: node %"PRIx64, node);

		};

		// DEBUG - it should be GONE
		if (RCHASH_OK == rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **) &fne)) {
			cf_info(AS_FABRIC, "fabric disconnecting: deleted from hash, but still there SUPER FAIL");
			fne_release(fne);
		}

		// drain the queues
		int rv;
		do {
			fabric_buffer *fb;
			rv = cf_queue_pop(fne->xmit_buffer_queue, &fb, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK) {
				fabric_buffer_release(fb);
			} else {
				cf_debug(AS_FABRIC, "fabric_node_disconnect(%p): fne: %p : xmit buffer queue empty", node, fne);
			}
		} while (rv == CF_QUEUE_OK);

		// Empty out the queue
		do {
			msg *m;
			rv = cf_queue_priority_pop(fne->xmit_msg_queue, &m, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK) {
				cf_debug(AS_FABRIC, "fabric: dropping message to now-gone (heartbeat fail) node %"PRIx64, node);
				as_fabric_msg_put(m);
			} else {
				cf_debug(AS_FABRIC, "fabric_node_disconnect(%p): fne: %p : xmit msg queue empty", node, fne);
			}
		} while (rv == CF_QUEUE_OK);

		// Clean up all connected outgoing fabric buffers attached to this FNE.
		fabric_disconnect(g_fabric_args, fne);

		fne_release(fne);
	}
	else {
		cf_warning(AS_FABRIC, "fabric disconnect node: node %"PRIx64" not connected, PROBLEM", node);
	}
}

// Function is called when a new node created or destroyed on the heartbeat system.
// This will insert a new element in the hashtable that keeps track of all TCP connections
static void
fabric_heartbeat_event(int nevents, as_hb_event_node *events, void *udata)
{
	fabric_args *fa = udata;

	if ((nevents < 1) || (nevents > g_config.paxos_max_cluster_size) || !events)
	{
		cf_warning(AS_FABRIC, "fabric: received event count of %d", nevents);
		return;
	}

	as_fabric_event_node fabric_events[AS_CLUSTER_SZ];
	memset(fabric_events, 0, sizeof(as_fabric_event_node) * g_config.paxos_max_cluster_size);

	for (int i = 0; i < nevents; i++)
	{
		fabric_events[i].nodeid = events[i].nodeid;
		switch (events[i].evt)
		{
			case AS_HB_NODE_ARRIVE:
			{
				fabric_node_element *fne;

				// create corresponding fabric node element - add it to the hash table
				if (RCHASH_OK != rchash_get(g_fabric_node_element_hash, &(events[i].nodeid), sizeof(cf_node), (void **) &fne)) {
					fne = fne_create(events[i].nodeid);
					cf_debug(AS_FABRIC, "fhe(): created an FNE %p for node %p from HB_NODE_ARRIVE", fne, events[i].nodeid);
				} else {
					cf_debug(AS_FABRIC, "fhe(): found an already-existing FNE %p for node %p from HB_NODE_ARRIVE", fne, events[i].nodeid);
					cf_debug(AS_FABRIC, "fhe(): need to let go of it ~~ before fne_release(%p) count:%d", fne, cf_rc_count(fne));
					fne_release(fne);
				}
				cf_info(AS_FABRIC, "fabric: node %"PRIx64" arrived", events[i].nodeid);
				fabric_events[i].evt = FABRIC_NODE_ARRIVE;
				fabric_events[i].p_node = events[i].p_node;
				break;
			}
			case AS_HB_NODE_DEPART:
				cf_info(AS_FABRIC, "fabric: node %"PRIx64" departed", events[i].nodeid);
				fabric_node_disconnect(events[i].nodeid);
				fabric_events[i].evt = FABRIC_NODE_DEPART;
				break;
			case AS_HB_NODE_UNDUN:
				cf_info(AS_FABRIC, "fabric: node %"PRIx64" undunned", events[i].nodeid);
				fabric_events[i].evt = FABRIC_NODE_UNDUN;
				fabric_events[i].p_node = events[i].p_node;
				break;
			case AS_HB_NODE_DUN:
				cf_info(AS_FABRIC, "fabric: node %"PRIx64" dunned", events[i].nodeid);
				fabric_events[i].evt = FABRIC_NODE_DUN;
				break;
			default:
				cf_warning(AS_FABRIC, "fabric: received unknown event type %d %"PRIx64"", i, events[i].nodeid);
				break;
		}
	}

#ifdef EXTRA_CHECKS
	as_fabric_dump();
#endif

	// Notify whoever above who might care
	if (fa->event_cb)
	{
		(*fa->event_cb) (nevents, fabric_events, fa->event_udata);
	}
	else
		cf_warning(AS_FABRIC, "fabric_heartbeat_event: got %d events from heartbeat system, no handler registered", nevents);

}

int
as_fabric_register_event_fn( as_fabric_event_fn event_cb, void *event_udata )
{
	g_fabric_args->event_udata = event_udata;
	g_fabric_args->event_cb = event_cb;
	return(0);
}

int
as_fabric_register_msg_fn ( msg_type type, const msg_template *mt, size_t mt_sz,  as_fabric_msg_fn msg_cb, void *msg_udata)
{
	if (type >= M_TYPE_MAX)
		return(-1);
	g_fabric_args->mt_sz[type] = mt_sz;
	g_fabric_args->mt[type] = mt;
	g_fabric_args->msg_cb[type] = msg_cb;
	g_fabric_args->msg_udata[type] = msg_udata;
	return(0);
}

//
// print out all the known nodes, connections, queue states
//

static int
fabric_status_node_reduce_fn(void *key, uint32_t keylen, void *data, void *udata)
{
	fabric_node_element *fne = (fabric_node_element *) data;
	cf_node *keyd = (cf_node *) key;

	cf_info(AS_FABRIC, "fabric status: fne %p node %"PRIx64" refcount %d", fne, *(uint64_t *)keyd, cf_rc_count(fne));

	return(0);
}

void *
fabric_status_ticker_fn(void *i_hate_gcc)
{
	do {

		cf_info(AS_FABRIC, "fabric status ticker: %d nodes", rchash_get_size(g_fabric_node_element_hash));

		rchash_reduce( g_fabric_node_element_hash, fabric_status_node_reduce_fn, 0);

		sleep(7);

	} while (1);
	return(0);
}


static int as_fabric_transact_init(void);

int
as_fabric_init()
{
	if (SHASH_OK != shash_create(&g_fb_hash, ptr_hash_fn, sizeof(void *), sizeof(int), 1000, SHASH_CR_MT_BIGLOCK))
		cf_crash(AS_FABRIC, "Failed to allocate FB hash");

	fabric_args *fa = cf_malloc(sizeof(fabric_args));
	memset(fa, 0, sizeof(fabric_args));
	g_fabric_args = fa;

	fa->num_workers = g_config.n_fabric_workers;

	// register my little fabric message type, so I can create 'em
	as_fabric_register_msg_fn(M_TYPE_FABRIC, fabric_mt, sizeof(fabric_mt), 0 /* arrival function!*/, 0);

	// Create the cf_node hash table
	rchash_create( &g_fabric_node_element_hash, cf_nodeid_rchash_fn, fne_destructor,
				   sizeof(cf_node), 64, RCHASH_CR_MT_MANYLOCK);

	// Create a global queue for the stashing of wayward messages for reuse
	for (int i = 0; i < M_TYPE_MAX; i++) {
		fa->msg_pool_queue[i] = cf_queue_create( sizeof(msg *), true);
	}

	// Create a thread for monitoring the health of nodes
	pthread_create( &(fa->node_health_th), 0, fabric_node_health_fn, 0);

	as_fabric_transact_init();

	// Create a thread for monitoring general status of fabric
	// pthread_create( &fabric_status_ticker_th, 0, fabric_status_ticker_fn, 0);

	// TODO - decide if we really want to keep the fabric health subsystem.
	as_fb_health_ack_other_nodes(true);

	return(0);
}


int
as_fabric_start()
{
	fabric_args *fa = g_fabric_args;

	// Create a unix domain socket that all workers can connect to, then be written to
	// in order to wakeup from epoll wait
	// Create a listener for the notification socket
	if (0 > (fa->note_server_fd = socket(AF_UNIX, SOCK_STREAM, 0))) {
		cf_crash(AS_FABRIC,
				 "could not create note server fd: %d %s", errno, cf_strerror(errno));
	}
	struct sockaddr_un ns_so;
	ns_so.sun_family = AF_UNIX;
	snprintf(&fa->note_sockname[0], sizeof(ns_so.sun_path), "/tmp/wn-%d", getpid());
	strcpy(ns_so.sun_path, fa->note_sockname);
	unlink(ns_so.sun_path); // not sure why this is necessary
	int ns_so_len = strlen(ns_so.sun_path) + sizeof(ns_so.sun_family) + 1;
	if (0 > bind(fa->note_server_fd, (struct sockaddr *)&ns_so, ns_so_len)) {
		cf_crash(AS_FABRIC,
				 "could not bind note server name %s: %d %s", ns_so.sun_path, errno, cf_strerror(errno));
	}
	if (0 > listen(fa->note_server_fd, 5)) {
		cf_crash(AS_FABRIC, "listen: %s", cf_strerror(errno));
	}

	// todo: handle errors
	// Create thread for accepting and filing notification fds
	pthread_create( &(fa->note_server_th), 0, fabric_note_server_fn, fa);

	// Create a set of workers for data motion
	for (uint i = 0; i < fa->num_workers; i++) {
		// todo: handle errors
		fa->workers_queue[i] = cf_queue_create( sizeof(worker_queue_element), true);
		pthread_create( &(fa->workers_th[i]), 0, fabric_worker_fn, fa);
	}

	// Create the Accept thread
	if ( 0 != pthread_create( &(fa->accept_th), 0, fabric_accept_fn, fa)) {
		cf_crash(AS_FABRIC, "could not create thread to receive heartbeat");
		return(0);
	}

	// Register a callback with the heartbeat mechanism
	as_hb_register(fabric_heartbeat_event, fa);

	// TODO - decide if we really want to keep the fabric health subsystem.
	as_fb_health_create();

	return(0);

}


int
as_fabric_set_node_parameter(cf_node node, int parameter, void *value)
{
	// Look up the node's FNE
	fabric_node_element *fne;
	int rv;
	rv = rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **) &fne);
	if (rv != RCHASH_OK) 		return(-1);

	// set the parameter
	if (parameter == AS_FABRIC_PARAMETER_GATHER_USEC) {
		uint32_t *ui_val = (uint32_t *) value;
		if (ui_val)
			fne->parameter_gather_usec = *ui_val;
		else
			fne->parameter_gather_usec = 0;
		cf_debug(AS_FABRIC, "node parameter GATHER USEC set to %d node %"PRIx64,
				 fne->parameter_gather_usec, node);

	}
	else if (parameter == AS_FABRIC_PARAMETER_MSG_SIZE) {
		uint32_t *ui_val = (uint32_t *) value;
		if (ui_val)
			fne->parameter_msg_size = *ui_val;
		else
			fne->parameter_msg_size = 2000;
		cf_debug(AS_FABRIC, "node parameter MSG SIZE set to %d node %"PRIx64,
				 fne->parameter_msg_size, node);
	}

	fne_release(fne);

	return(0);

}


uint g_qs_counter = 0;

int
as_fabric_send(cf_node node, msg *m, int priority )
{
	if (g_fabric_args == 0) {
		cf_debug(AS_FABRIC, "fabric send without initilaized fabric, BOO!");
		return(AS_FABRIC_ERR_UNINITIALIZED);
	}

#ifdef EXTRA_CHECKS
// this is debug and a little expensive but has been seriously useful so far
	if (cf_rc_count(m) <= 0) {
		cf_info(AS_FABRIC, "fabric send with bad ref count: m %p", m);
		return(-1);
	}
#endif

	cf_detail(AS_FABRIC, "fabric send: m %p to node %"PRIx64, m, node);

	// Short circut for self!
	if (g_config.self_node == node) {

		// statistic!
		cf_atomic_int_incr(&g_config.fabric_msgs_selfsend);

		// if not, just deliver raw message
		if (g_fabric_args->msg_cb[m->type]) {
			(*g_fabric_args->msg_cb[m->type]) (node, m, g_fabric_args->msg_udata[m->type]);
		}
		else {
			cf_debug(AS_FABRIC, "msg: self send to unregistered type: %d", m->type);
			return(AS_FABRIC_ERR_BAD_MSG);
		}
		return(0);
	}

	// Look up the cf_node in the hash table - get the fne
	fabric_node_element *fne;
	int rv;
	rv = rchash_get(g_fabric_node_element_hash, &node, sizeof(node), (void **) &fne);
	if (RCHASH_OK != rv ) {
		if (rv == RCHASH_ERR_NOTFOUND) {
			cf_debug(AS_FABRIC, "fabric send to unknown node %"PRIx64, node);
			return(AS_FABRIC_ERR_NO_NODE);
		}
		else
			return(AS_FABRIC_ERR_UNKNOWN);
	}

	// The FNE has a pool of file descriptors / buffers just hanging out - grab one
	// the one we grab may have gone bad for some reason, in which case we decr the reference
	// count and move on
	fabric_buffer *fb = 0;
	do {
		rv = cf_queue_pop(fne->xmit_buffer_queue, &fb, CF_QUEUE_NOWAIT);
		if ((CF_QUEUE_OK == rv) && (fb->fd == -1)) {
			cf_warning(AS_FABRIC, "releasing fb: %p with fne: %p and fd: -1", fb, fb->fne);
			fabric_buffer_release(fb);
			fb = 0;
		}
	} while ((CF_QUEUE_OK == rv) && (fb == 0));

	if (fb == 0) {

		// Queue the message, and consider creating a new connection to the endpoint
		cf_detail(AS_FABRIC, "fabric_send: no connection, queueing message fne %p q %p m %p",
				  fne, fne->xmit_msg_queue, m);

		// Queue it:
		// check whether we've really got enough space on the xmit queue
		if ( (priority == AS_FABRIC_PRIORITY_LOW) &&
				(cf_queue_priority_sz(fne->xmit_msg_queue) > 50000) ) {
//			cf_debug(AS_FABRIC,"queue full for low priority: sz %d",qs);
			fne_release(fne);
			return(AS_FABRIC_ERR_QUEUE_FULL);
		}

		cf_queue_priority_push(fne->xmit_msg_queue, &m, priority);
		if (g_qs_counter++ % 50000 == 0)
			cf_debug(AS_FABRIC, "xmit-msg-queue: %d -- node %"PRIx64, cf_queue_priority_sz(fne->xmit_msg_queue), node);

		// Consider creating a new connection - don't create too many conns,
		// though, because you'll just get small packets with too many conns.
		// It's good to have a few for multithreading/multihoming though.
		uint32_t fds = cf_atomic32_incr( &(fne->fd_counter) );
		if (fds < FABRIC_MAX_FDS ) {
			// Room for more connections!
//			cf_debug(AS_FABRIC," creating new connection: fd %d",fds);
			if (0 != fabric_connect(g_fabric_args, fne)) {
				// error! uncount the file descriptor
				cf_atomic32_decr( &(fne->fd_counter) );
			}
		}
		else {
			// Decide no new conn - decrement the counter we had incremented
			cf_atomic32_decr( &(fne->fd_counter) );

			// cf_debug(AS_FABRIC,"fabric: no connection free, too many connections %d currently, queue",fds);
		}

	}
	// Got an xmit buffer - can do an immediate send (or close anyway)
	else {

		// write the message into the buffer
		fabric_buffer_set_write_msg(fb, m);

		// try to pack more in - taking from my own queue
		fabric_buffer_write_fill(fb);

		// This function will patch up the epoll state (but somewhat expensive call,
		// only call it if required)
		if (fb->w_total_len > fb->w_len)
			fabric_buffer_set_epoll_state(fb);

		// since the fabric buffer is no longer on the queue, decrease its ref count
		fabric_buffer_release(fb);
	}

	fne_release(fne);

	return(0);
}

int
fabric_get_node_list_fn(void *key, uint32_t keylen, void *data, void *udata)
{
//	cf_debug(AS_FABRIC,"get_node_list_fn");
	as_node_list *nl = udata;
	if (nl->sz == nl->alloc_sz)		return(0);
	nl->nodes[nl->sz] = *(cf_node *)key;
//	cf_debug(AS_FABRIC,"nl_nodes %d : %"PRIx64,nl->sz,nl->nodes[nl->sz]);
	nl->sz++;
	return(0);
}

//
//
//
//
//

rchash *g_fabric_transact_xmit_hash = 0;
rchash *g_fabric_transact_recv_hash = 0;

typedef struct {
	uint64_t	tid;
	cf_node 	node;
} __attribute__ ((__packed__)) transact_recv_key;

uint32_t
fabric_tranact_xmit_hash_fn (void *value, uint32_t value_len)
{
	// Todo: the input is a transaction id which really is just used directly,
	// but is depends on the size and whether we've got "masking bits"
	if (value_len != sizeof(uint64_t)) {
		cf_warning(AS_FABRIC, "transact hash fn received wrong size");
		return(0);
	}

	return((uint32_t) ( *(uint64_t *)value) );
}

uint32_t
fabric_tranact_recv_hash_fn (void *value, uint32_t value_len)
{
	// Todo: the input is a transaction id which really is just used directly,
	// but is depends on the size and whether we've got "masking bits"
	if (value_len != sizeof(transact_recv_key)) {
		cf_warning(AS_FABRIC, "transact hash fn received wrong size");
		return(0);
	}
	transact_recv_key *trk = (transact_recv_key *) value;

	return((uint32_t) trk->tid );
}

cf_atomic64 g_fabric_transact_tid = 0;

typedef struct {

	// allocated tid, without the 'code'
	uint64_t   			tid;

	cf_node 			node;

	msg *				m;

	pthread_mutex_t 	LOCK;

	uint64_t			deadline_ms;  // absolute time according to cf_getms of expiration

	uint64_t			retransmit_ms; // when we retransmit (absolute deadline)

	int 				retransmit_period; // next time

	as_fabric_transact_complete_fn cb;

	void *				udata;

} fabric_transact_xmit;

typedef struct {
	cf_ll_element ll_e;
	int op;
	uint64_t tid;
} ll_fabric_transact_xmit_element;

typedef struct {

	cf_node 	node; // where it came from

	uint64_t	tid;  // inbound tid

} fabric_transact_recv;

static inline int tid_code_get(uint64_t tid) {
	return( tid >> 56 );
}

static inline uint64_t tid_code_set(uint64_t tid, int code) {
	return( tid | ((((uint64_t) code)) << 56));
}

static inline uint64_t tid_code_clear(uint64_t tid) {
	return( tid & 0xffffffffffffff );
}

#define TRANSACT_CODE_REQUEST 1
#define TRANSACT_CODE_RESPONSE 2

//
// Given the requirement to signal exactly once, we we need to call the callback or
// free the message?
//
// NO -- but it's OK to check and make sure someone signaled before the destructor
// WHY - because you need to be able to call the destructor from anywhere
//
// rchash destructors always free the internals but not the thing itself: thus to
// release, always call the _release function below, don't call this directly
//
void fabric_transact_xmit_destructor (void *object) {

	fabric_transact_xmit *ft = object;

	as_fabric_msg_put(ft->m);

}

void fabric_transact_xmit_release(fabric_transact_xmit *ft) {

	if (0 == cf_rc_release(ft)) {
		fabric_transact_xmit_destructor(ft);
		cf_rc_free(ft);
	}
}

void fabric_transact_recv_destructor (void *object) {

	fabric_transact_recv *ft = object;
	// Suppress warning
	(void)ft;

}

void fabric_transact_recv_release(fabric_transact_recv *ft) {

	if (0 == cf_rc_release(ft)) {
		fabric_transact_recv_destructor(ft);
		cf_rc_free(ft);
	}
}

int
as_fabric_transact_reply(msg *m, void *transact_data) {

	fabric_transact_recv *ftr = (fabric_transact_recv *) transact_data;

	// cf_info(AS_FABRIC, "fabric: transact: sending reply tid %"PRIu64,ftr->tid);

	// this is a response - overwrite tid with response code etc
	uint64_t xmit_tid = tid_code_set(ftr->tid, TRANSACT_CODE_RESPONSE);
	msg_set_uint64(m, 0, xmit_tid);

	// TODO: make sure it's in the outbound hash?

	// send the response for the first time
	as_fabric_send(ftr->node, m, AS_FABRIC_PRIORITY_MEDIUM);

	return(0);
}

void
as_fabric_transact_start(cf_node dest, msg *m, int timeout_ms, as_fabric_transact_complete_fn cb, void *udata)
{
	// Todo: could check it against the list of global message ids

	if (m->f[0].type != M_FT_UINT64) {
		// error
		cf_warning(AS_FABRIC, "as_fabric_transact: first field must be int64");
		(*cb) (0, udata, AS_FABRIC_ERR_UNKNOWN);
		return;
	}

	fabric_transact_xmit *ft = cf_rc_alloc(sizeof(fabric_transact_xmit));
	if (!ft) {
		cf_warning(AS_FABRIC, "as_fabric_transact: can't malloc");
		(*cb) (0, udata, AS_FABRIC_ERR_UNKNOWN);
		return;
	}

	ft->tid = cf_atomic64_incr(&g_fabric_transact_tid);
	ft->node = dest;
	ft->m = m;
	uint64_t now = cf_getms();
	pthread_mutex_init(&ft->LOCK, 0);
	ft->deadline_ms = now + timeout_ms;
	ft->retransmit_period = 10; // 10 ms start
	ft->retransmit_ms = now + ft->retransmit_period; // hard start at 10 milliseconds
	ft->cb = cb;
	ft->udata = udata;

	uint64_t xmit_tid = tid_code_set(ft->tid, TRANSACT_CODE_REQUEST);

	// set message tid
	msg_set_uint64(m, 0, xmit_tid);

	// put will take the reference, need to keep one around for the send
	cf_rc_reserve(ft);
	if (0 != rchash_put(g_fabric_transact_xmit_hash, &ft->tid, sizeof(ft->tid), ft)) {
		cf_warning(AS_FABRIC, "as_fabric_transact: can't put in hash");
		cf_rc_release(ft);
		fabric_transact_xmit_release(ft);
		return;
	}

	// transmit the initial message
	msg_incr_ref(m);
	int rv = as_fabric_send(ft->node, ft->m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {

		// need to release the ref I just took with the incr_ref
		as_fabric_msg_put(m);

		if (rv == -2) {
			// no destination node: callback & remove from hash
			;
		}

	}
	fabric_transact_xmit_release(ft);

	return;

}

static as_fabric_transact_recv_fn   fabric_transact_recv_cb[M_TYPE_MAX] = { 0 };
static void *					  fabric_transact_recv_udata[M_TYPE_MAX] = { 0 };

// received a message. Could be a response to an outgoing message,
// or a new incoming transaction message
//

int
fabric_transact_msg_fn(cf_node node, msg *m, void *udata)
{
	// could check type against max, but msg should have already done that on creation

	// received a message, make sure we have a registered callback
	if (fabric_transact_recv_cb[m->type] == 0) {
		cf_warning(AS_FABRIC, "transact: received message for transact with bad type %d, internal error", m->type);
		as_fabric_msg_put(m); // return to pool unexamined
		return(0);
	}

	// check to see that we have an outstanding request (only cb once!)
	uint64_t tid = 0;
	if (0 != msg_get_uint64(m, 0 /*field_id*/, &tid)) {
		cf_warning(AS_FABRIC, "transact: received message with no tid");
		as_fabric_msg_put(m);
		return(0);
	}

	int code = tid_code_get(tid);
	tid = tid_code_clear(tid);

	// if it's a response, check against what you sent
	if (code == TRANSACT_CODE_RESPONSE) {

		// cf_info(AS_FABRIC, "transact: received response");

		fabric_transact_xmit *ft;
		int rv = rchash_get(g_fabric_transact_xmit_hash, &tid, sizeof(tid), (void **) &ft);
		if (rv != 0) {
			cf_warning(AS_FABRIC, "No fabric transmit structure in global hash for fabric transaction-id %"PRIu64"", tid);
			as_fabric_msg_put(m);
			return(0);
		}

		pthread_mutex_lock(&ft->LOCK);

		// make sure we haven't notified some other way, then notify caller
		if (ft->cb) {

			(*ft->cb) (m, ft->udata, AS_FABRIC_SUCCESS);

			ft->cb = 0;
		}

		pthread_mutex_unlock(&ft->LOCK);

		// ok if this happens twice....
		rchash_delete(g_fabric_transact_xmit_hash, &tid, sizeof(tid) );

		// this will often be the final release
		fabric_transact_xmit_release(ft);

	}
	// if it's a request, start a mew
	else if (code == TRANSACT_CODE_REQUEST) {

		fabric_transact_recv *ftr = cf_malloc( sizeof(fabric_transact_recv) );

		ftr->tid = tid; // has already been cleared
		ftr->node = node;

		// notify caller - they will likely respond inline
		(*fabric_transact_recv_cb[m->type]) (node, m, ftr, fabric_transact_recv_udata[m->type]);
		cf_free(ftr);

	}
	else {
		cf_warning(AS_FABRIC, "transact: bad code on incoming message: %d", code);
		as_fabric_msg_put(m);
	}

	return(0);
}


// registers all of this message type as a
// transaction type message, which means the main message
int
as_fabric_transact_register( msg_type type, const msg_template *mt, size_t mt_sz,
							 as_fabric_transact_recv_fn cb, void *udata)
{

	// put details in the global structure
	fabric_transact_recv_cb[type] = cb;
	fabric_transact_recv_udata[type] = udata;

	// register my internal callback with the main message callback
	as_fabric_register_msg_fn( type, mt, mt_sz,  fabric_transact_msg_fn, 0);

	return(0);
}

//
// Transaction maintenance threads
// this thread watches the hash table, and finds transactions that
// have completed and need to be signaled
//

pthread_t	fabric_transact_th;

static int
fabric_transact_xmit_reduce_fn(void *key, uint32_t keylen, void *o, void *udata)
{
	fabric_transact_xmit *ftx = (fabric_transact_xmit *) o;
	int op = 0;

	cf_detail(AS_FABRIC, "transact: xmit reduce");

	uint64_t now = cf_getms();

	pthread_mutex_lock(&ftx->LOCK);

	if ( now > ftx->deadline_ms ) {
		// Expire and remove transactions that are timed out
		// Need to call application: we've timed out
		op = OP_TRANS_TIMEOUT;
	}
	else if ( now > ftx->retransmit_ms ) {
		// retransmit, update time counters, etc
		ftx->retransmit_ms = now + ftx->retransmit_period;
		ftx->retransmit_period *= 2;
		op = OP_TRANS_RETRANSMIT;
	}

	if (op > 0)
	{
		// Add the transaction in linked list of transactions to be processed.
		// Process such transactions outside retransmit hash lock.
		// Why ?
		// Fabric short circuit the message to self.
		// It short circuits it by directly calling receiver function of corresponding module.
		// Receiver of module construct "reply" and hand over to fabric to deliver.
		// On receiving "reply", fabric removes original message, for which this is a reply, from retransmit hash
		//
		// "fabric_transact_xmit_reduce_fn" function is invoked by reduce_delete.
		// reduce_delete holds the lock over corrsponding hash (here "retransmit hash").
		// If the message, sent by this function, is short circuited by fabric,
		// the same thread will again try to get lock over "retransmit hash".
		// It is self dead lock.
		//
		// To avoid this process it outside retransmit hash lock.

		cf_ll *ll_fabric_transact_xmit  = (cf_ll *)udata;

		// Create new node for list
		ll_fabric_transact_xmit_element *ll_ftx_ele = (ll_fabric_transact_xmit_element *)cf_malloc(sizeof(ll_fabric_transact_xmit_element));
		ll_ftx_ele->tid = ftx->tid;
		ll_ftx_ele->op = op;
		// Append into list
		cf_ll_append(ll_fabric_transact_xmit, (cf_ll_element *)ll_ftx_ele);
	}
	pthread_mutex_unlock(&ftx->LOCK);
	return(0);
}

int
ll_ftx_reduce_fn(cf_ll_element *le, void *udata)
{
	ll_fabric_transact_xmit_element *ll_ftx_ele = (ll_fabric_transact_xmit_element *)le;

	fabric_transact_xmit *ftx;
	uint64_t tid;
	int rv;

	msg *m = 0;
	cf_node node;

	tid = ll_ftx_ele->tid;
	// rchash_get increment ref count on transaction ftx.
	rv = rchash_get(g_fabric_transact_xmit_hash, &tid, sizeof(tid), (void **) &ftx);
	if (rv != 0) {
		cf_warning(AS_FABRIC, "No fabric transmit structure in global hash for fabric transaction-id %"PRIu64"", tid);
		return (CF_LL_REDUCE_DELETE);
	}

	if (ll_ftx_ele->op == OP_TRANS_TIMEOUT) {

		// call application: we've timed out
		if (ftx->cb) {
			(*ftx->cb) ( 0, ftx->udata, AS_FABRIC_ERR_TIMEOUT);
			ftx->cb = 0;
		}
		cf_debug(AS_FABRIC, "fabric transact: %"PRIu64" timed out", tid);
		// rchash_delete removes ftx from hash and decrement ref count on it.
		rchash_delete(g_fabric_transact_xmit_hash, &tid, sizeof(tid));
		// It should be final release of transaction ftx.
		// On final release, it also decrements message ref count, taken by initial fabric_send.
		fabric_transact_xmit_release(ftx);
	}
	else if (ll_ftx_ele->op == OP_TRANS_RETRANSMIT) {
		//msg_incr_ref(ftx->m);
		if (ftx->m) {
			msg_incr_ref(ftx->m);
			m = ftx->m;
			node = ftx->node;
			if (0 != as_fabric_send(node, m, AS_FABRIC_PRIORITY_MEDIUM)) {
				cf_debug(AS_FABRIC, "fabric: transact: %"PRIu64" retransmit send failed", tid);
				as_fabric_msg_put(m);
			}
			else {
				cf_debug(AS_FABRIC, "fabric: transact: %"PRIu64" retransmit send success", tid);
			}
		}
		// Decrement ref count, incremented by rchash_get
		cf_rc_release(ftx);
	}
	// Remove it from link list
	return (CF_LL_REDUCE_DELETE);
}

// Function to delete node in linked list
void
ll_ftx_destructor_fn(cf_ll_element *e)
{
	cf_free(e);
}

//
// long running thread for tranaction maintance
//
void *
fabric_transact_fn( void *argv )
{
	// Create a list of transactions to be processed in each pass.
	cf_ll ll_fabric_transact_xmit;
	// Initialize list to empty list.
	// This list is processed by single thread. No need of a lock.
	cf_ll_init(&ll_fabric_transact_xmit, &ll_ftx_destructor_fn, false);
	do {

		usleep( 10000); // 10 ms for now

		// Visit each entry in g_fabric_transact_xmit_hash and select entries to be retransmitted or timed out.
		// Add that transaction id (tid) in the link list 'll_fabric_transact_xmit'
		rchash_reduce(g_fabric_transact_xmit_hash, fabric_transact_xmit_reduce_fn, (void *)&ll_fabric_transact_xmit);

		if (cf_ll_size(&ll_fabric_transact_xmit))
		{
			// There are transactions to be processed
			// Process each transaction in list.
			cf_ll_reduce(&ll_fabric_transact_xmit, true /*forward*/, ll_ftx_reduce_fn, NULL);
		}
	} while (1);

	return(0);
}

//
// Transaction init -- hooked from the main fabric init,
// sets up the hash table and threads
//

static int
as_fabric_transact_init()
{

	// Create the transaction hash table
	rchash_create( &g_fabric_transact_xmit_hash, fabric_tranact_xmit_hash_fn , fabric_transact_xmit_destructor,
				   sizeof(uint64_t), 64 /* n_buckets */, RCHASH_CR_MT_MANYLOCK);

	rchash_create( &g_fabric_transact_recv_hash, fabric_tranact_recv_hash_fn , fabric_transact_recv_destructor,
				   sizeof(uint64_t), 64 /* n_buckets */, RCHASH_CR_MT_MANYLOCK);

	// Create a thread for monitoring transactions
	pthread_create( &fabric_transact_th, 0, fabric_transact_fn, 0);

	return(0);
}

// To send to all nodes, simply set the nodes pointer to 0 (and we'll just fetch
// the list internally)
// If an element in the array is 0, then there's no destination

int
as_fabric_send_list(cf_node *nodes, int nodes_sz, msg *m, int priority)
{
	as_node_list nl;
	if (nodes == 0) {
		as_fabric_get_node_list(&nl);
		nodes = &nl.nodes[0];
		nodes_sz = nl.sz;
	}

	if (nodes_sz == 1)
		return(as_fabric_send(nodes[0], m, priority));

	cf_debug(AS_FABRIC, "as_fabric_send_list sending: m %p", m);
	for (uint j = 0; j < nodes_sz; j++) {
		cf_debug(AS_FABRIC, "  destination: %"PRIx64"\n", nodes[j]);
	}

	// careful with the ref count here: need to increment before every
	// send except the last
	int rv = 0;
	for (int i = 0; i < nodes_sz ; i++) {
		if (i != nodes_sz - 1) {
			msg_incr_ref(m);
		}
		rv = as_fabric_send(nodes[i], m, priority);
		if (0 != rv) goto Cleanup;
	}
	return(0);
Cleanup:
	as_fabric_msg_put(m);
	return(rv);
}

static int
fb_hash_dump_reduce_fn(void *key, void *data, void *udata)
{
	fabric_buffer *fb = *(fabric_buffer **) key;
	int *item_num = (int *) udata;

	int count = cf_rc_count(fb);

	cf_info(AS_FABRIC, "\tFB[%d] fb(%p): fne: %p (node %p: %s); fd: %d ; wid: %d ; rc: %d ; polarity: %s", *item_num, fb, fb->fne, fb->fne->node, (fb->fne->live ? "live" : "dead"), fb->fd, fb->worker_id, count, (fb->connected ? "outbound" : "inbound"));

	*item_num += 1;

	return 0;
}

//
// Debug routine that dumps out everything known by fabric, like what the node lists really are
//   and all the fabric buffers.
//   Add more info as you need it....
void
as_fabric_dump(bool verbose)
{
	as_node_list nl;
	as_fabric_get_node_list(&nl);

	cf_info(AS_FABRIC, " Fabric Dump: %d nodes known", nl.sz);
	for (int i = 0; i < nl.sz; i++) {
		if (nl.nodes[i] == g_config.self_node) {
			cf_info(AS_FABRIC, "    %"PRIx64" node is self", nl.nodes[i]);
			continue;
		}

		// not self, check it out
		fabric_node_element *fne;
		int rv = rchash_get(g_fabric_node_element_hash, &nl.nodes[i], sizeof(cf_node), (void **)&fne);
		if (rv != RCHASH_OK) {
			cf_info(AS_FABRIC, "   %"PRIx64" node not found in hash although reported available", nl.nodes[i]);
		}
		else {
			cf_info(AS_FABRIC, "    %"PRIx64" fds %d live %d goodwrite %d goodread %d q %d", fne->node,
					fne->fd_counter, fne->live, fne->good_write_counter, fne->good_read_counter, cf_queue_priority_sz(fne->xmit_msg_queue));
			fne_release(fne);
		}
	}

	if (verbose) {
		cf_info(AS_FABRIC, " All Fabric Buffers in the FB hash:");
		int item_num = 0;
		shash_reduce(g_fb_hash, fb_hash_dump_reduce_fn, &item_num);
	}
}
