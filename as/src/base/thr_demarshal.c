/*
 * thr_demarshal.c
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

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/param.h>	// for MIN()
#include <sys/socket.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"
#include "jem.h"
#include "hist.h"
#include "queue.h"
#include "socket.h"

#include "base/as_stap.h"
#include "base/batch.h"
#include "base/cfg.h"
#include "base/packet_compression.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/thr_info.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"

#ifdef USE_JEM
#include "base/datamodel.h"
#endif

#define EPOLL_SZ	1024
// Workaround for platforms that don't have EPOLLRDHUP yet.
#ifndef EPOLLRDHUP
#define EPOLLRDHUP EPOLLHUP
#endif


extern void *thr_demarshal(void *arg);

typedef struct {
	unsigned int	epoll_fd[MAX_DEMARSHAL_THREADS];
	unsigned int	num_threads;
	pthread_t	dm_th[MAX_DEMARSHAL_THREADS];
} demarshal_args;

static demarshal_args *g_demarshal_args = 0;


//
// File handle reaper.
//

pthread_mutex_t	g_file_handle_a_LOCK = PTHREAD_MUTEX_INITIALIZER;
as_file_handle	**g_file_handle_a = 0;
uint			g_file_handle_a_sz;
pthread_t		g_demarshal_reaper_th;

void *thr_demarshal_reaper_fn(void *arg);
static cf_queue *g_freeslot = 0;

void
demarshal_file_handle_init()
{
	struct rlimit rl;

	pthread_mutex_lock(&g_file_handle_a_LOCK);

	if (g_file_handle_a == 0) {
		if (-1 == getrlimit(RLIMIT_NOFILE, &rl)) {
			cf_crash(AS_DEMARSHAL, "getrlimit: %s", cf_strerror(errno));
		}

		// Initialize the message pointer array and the unread byte counters.
		g_file_handle_a = cf_calloc(rl.rlim_cur, sizeof(as_proto *));
		cf_assert(g_file_handle_a, AS_DEMARSHAL, CF_CRITICAL, "allocation: %s", cf_strerror(errno));
		g_file_handle_a_sz = rl.rlim_cur;

		for (int i = 0; i < g_file_handle_a_sz; i++) {
			cf_queue_push(g_freeslot, &i);
		}

		pthread_create(&g_demarshal_reaper_th, 0, thr_demarshal_reaper_fn, 0);

		// If config value is 0, set a maximum proto size based on the RLIMIT.
		if (g_config.n_proto_fd_max == 0) {
			g_config.n_proto_fd_max = rl.rlim_cur / 2;
			cf_info(AS_DEMARSHAL, "setting default client file descriptors to %d", g_config.n_proto_fd_max);
		}
	}

	pthread_mutex_unlock(&g_file_handle_a_LOCK);
}

// Keep track of the connections, since they're precious. Kill anything that
// hasn't been used in a while. The file handle array keeps a reference count,
// and allows a reaper to run through and find the ones to reap. The table is
// only written by the demarshal threads, and only read by the reaper thread.
void *
thr_demarshal_reaper_fn(void *arg)
{
	uint64_t last = cf_getms();

	while (true) {
		uint64_t now = cf_getms();
		uint inuse_cnt = 0;
		uint64_t kill_ms = g_config.proto_fd_idle_ms;
		bool refresh = false;

		if (now - last > (uint64_t)(g_config.sec_cfg.privilege_refresh_period * 1000)) {
			refresh = true;
			last = now;
		}

		pthread_mutex_lock(&g_file_handle_a_LOCK);

		for (int i = 0; i < g_file_handle_a_sz; i++) {
			if (g_file_handle_a[i]) {
				as_file_handle *fd_h = g_file_handle_a[i];

				if (refresh) {
					as_security_refresh(fd_h);
				}

				// Reap if not obviously in use.
				if (fd_h->inuse == false) {
					g_file_handle_a[i] = 0;
					cf_queue_push(g_freeslot, &i);
					AS_RELEASE_FILE_HANDLE(fd_h);
				}
				// Reap if past kill time.
				else if ((0 != kill_ms) && (fd_h->last_used + kill_ms < now)) {
					if (fd_h->fh_info & FH_INFO_DONOT_REAP) {
						cf_debug(AS_DEMARSHAL, "Not reaping the fd %d as it has the protection bit set", fd_h->fd);
						inuse_cnt++;
						continue;
					}

					shutdown(fd_h->fd, SHUT_RDWR); // will trigger epoll errors
					cf_debug(AS_DEMARSHAL, "remove unused connection, fd %d", fd_h->fd);
					g_file_handle_a[i] = 0;
					cf_queue_push(g_freeslot, &i);
					AS_RELEASE_FILE_HANDLE(fd_h);
					cf_atomic_int_incr(&g_config.reaper_count);
				}
				else {
					inuse_cnt++;
				}
			}
		}

		pthread_mutex_unlock(&g_file_handle_a_LOCK);

		if ((g_file_handle_a_sz / 10) > (g_file_handle_a_sz - inuse_cnt)) {
			cf_warning(AS_DEMARSHAL, "less than ten percent file handles remaining: %d max %d inuse",
					g_file_handle_a_sz, inuse_cnt);
		}

		// Validate the system statistics.
		if (g_config.proto_connections_opened - g_config.proto_connections_closed != inuse_cnt) {
			cf_debug(AS_DEMARSHAL, "reaper: mismatched connection count: %d in stats vs %d calculated",
					g_config.proto_connections_opened - g_config.proto_connections_closed,
					inuse_cnt);
		}

		sleep(1);
	}

	return NULL;
}

// Get the remote IP address and port connected to a socket file descriptor.
// Return 0 if successful, and populate the caller-provided IP address and port.
// Return -1 otherwise.
static int
get_fd_ip_addr_and_port(int fd, char *ip_addr, size_t ip_addr_sz, int *port)
{
	struct sockaddr_in addr_in;
	socklen_t addr_len = sizeof(addr_in);
	int retval = -1;

	// Try to get the client details for better logging.
	// Otherwise, fall back to generic log message.
	if (getpeername(fd, (struct sockaddr *) &addr_in, &addr_len) == 0
		&& ip_addr
		&& inet_ntop(AF_INET, &addr_in.sin_addr.s_addr, (char *) ip_addr, ip_addr_sz) != NULL) {
		retval = 0;
		if (port) {
			*port = ntohs(addr_in.sin_port);
		}
	}

	return retval;
}


// Log information about a suspicious incoming transaction.
static void
log_as_proto_and_peeked_data(as_proto *proto, uint8_t *peekbuf, size_t peeked_data_sz)
{
	cf_warning(AS_DEMARSHAL, "as_proto {version = %d ; type = %d ; sz = %zu (0x%x)}", proto->version, proto->type, proto->sz, proto->sz);
	cf_warning(AS_DEMARSHAL, "peeked_data_sz = %ld (0x%x)", peeked_data_sz, peeked_data_sz);
	cf_warning_binary(AS_DEMARSHAL, peekbuf, peeked_data_sz, CF_DISPLAY_HEX_SPACED, "peekbuf");
}

// Set of threads which talk to client over the connection for doing the needful
// processing. Note that once fd is assigned to a thread all the work on that fd
// is done by that thread. Fair fd usage is expected of the client. First thread
// is special - also does accept [listens for new connections]. It is the only
// thread which does it.
void *
thr_demarshal(void *arg)
{
	cf_socket_cfg *s;
	// Create my epoll fd, register in the global list.
	static struct epoll_event ev;
	int nevents, i, n, epoll_fd;
	cf_clock last_fd_print = 0;

#if defined(USE_SYSTEMTAP)
	uint64_t nodeid = g_config.self_node;
#endif

	// Early stage aborts; these will cause faults in process scope.
	cf_assert(arg, AS_DEMARSHAL, CF_CRITICAL, "invalid argument");
	s = &g_config.socket;

#ifdef USE_JEM
	int orig_arena;
	if (0 > (orig_arena = jem_get_arena())) {
		cf_crash(AS_DEMARSHAL, "Failed to get original arena for thr_demarshal()!");
	} else {
		cf_info(AS_DEMARSHAL, "Saved original JEMalloc arena #%d for thr_demarshal()", orig_arena);
	}
#endif

	// Figure out my thread index.
	pthread_t self = pthread_self();
	int thr_id;
	for (thr_id = 0; thr_id < MAX_DEMARSHAL_THREADS; thr_id++) {
		if (0 != pthread_equal(g_demarshal_args->dm_th[thr_id], self))
			break;
	}

	if (thr_id == MAX_DEMARSHAL_THREADS) {
		cf_debug(AS_FABRIC, "Demarshal thread could not figure own ID, bogus, exit, fu!");
		return(0);
	}

	// First thread accepts new connection at interface socket.
	if (thr_id == 0) {
		demarshal_file_handle_init();
		epoll_fd = epoll_create(EPOLL_SZ);
		if (epoll_fd == -1)
			cf_crash(AS_DEMARSHAL, "epoll_create(): %s", cf_strerror(errno));

		ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
		ev.data.fd = s->sock;
		if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_ADD, s->sock, &ev))
			cf_crash(AS_DEMARSHAL, "epoll_ctl(): %s", cf_strerror(errno));
		cf_info(AS_DEMARSHAL, "Service started: socket %d", s->port);
	}
	else {
		epoll_fd = epoll_create(EPOLL_SZ);
		if (epoll_fd == -1)
			cf_crash(AS_DEMARSHAL, "epoll_create(): %s", cf_strerror(errno));
	}

	g_demarshal_args->epoll_fd[thr_id] = epoll_fd;
	cf_detail(AS_DEMARSHAL, "demarshal thread started: id %d", thr_id);

	int id_cntr = 0;

	// Demarshal transactions from the socket.
	for ( ; ; ) {
		struct epoll_event events[EPOLL_SZ];

		cf_detail(AS_DEMARSHAL, "calling epoll");

		nevents = epoll_wait(epoll_fd, events, EPOLL_SZ, -1);

		if (0 > nevents) {
			cf_debug(AS_DEMARSHAL, "epoll_wait() returned %d ; errno = %d (%s)", nevents, errno, cf_strerror(errno));
		}

		cf_detail(AS_DEMARSHAL, "epoll event received: nevents %d", nevents);

		uint64_t now_ns = cf_getns();
		uint64_t now_ms = now_ns / 1000000;

		// Iterate over all events.
		for (i = 0; i < nevents; i++) {

			if (s->sock == events[i].data.fd) {

				// Accept new connections on the service socket.
				int csocket = -1;
				struct sockaddr_in caddr;
				socklen_t clen = sizeof(caddr);
				char cpaddr[24];

				if (-1 == (csocket = accept(s->sock, (struct sockaddr *)&caddr, &clen))) {
					// This means we're out of file descriptors - could be a SYN
					// flood attack or misbehaving client. Eventually we'd like
					// to make the reaper fairer, but for now we'll just have to
					// ignore the accept error and move on.
					if ((errno == EMFILE) || (errno == ENFILE)) {
						if (last_fd_print != (cf_getms() / 1000L)) {
							cf_info(AS_DEMARSHAL, " warning: hit OS file descript limit (EMFILE on accept), consider raising limit");
							last_fd_print = cf_getms() / 1000L;
						}
						continue;
					}
					cf_crash(AS_DEMARSHAL, "accept: %s (errno %d)", cf_strerror(errno), errno);
				}

				if (NULL == inet_ntop(AF_INET, &caddr.sin_addr.s_addr, (char *)cpaddr, sizeof(cpaddr))) {
					cf_crash(AS_DEMARSHAL, "inet_ntop(): %s (errno %d)", cf_strerror(errno), errno);
				}

				cf_detail(AS_DEMARSHAL, "new connection: %s", cpaddr);

				// Validate the limit of protocol connections we allow.
				uint32_t conns_open = g_config.proto_connections_opened - g_config.proto_connections_closed;
				if (conns_open > g_config.n_proto_fd_max) {
					if ((last_fd_print + 5000L) < cf_getms()) { // no more than 5 secs
						cf_warning(AS_DEMARSHAL, "dropping incoming client connection: hit limit %d connections", conns_open);
						last_fd_print = cf_getms();
					}
					shutdown(csocket, SHUT_RDWR);
					close(csocket);
					csocket = -1;
					continue;
				}

				// Set the socket to nonblocking.
				if (-1 == cf_socket_set_nonblocking(csocket)) {
					cf_info(AS_DEMARSHAL, "unable to set client socket to nonblocking mode");
					shutdown(csocket, SHUT_RDWR);
					close(csocket);
					csocket = -1;
					continue;
				}

				// Create as_file_handle and queue it up in epoll_fd for further
				// communication on one of the demarshal threads.
				as_file_handle *fd_h = cf_rc_alloc(sizeof(as_file_handle));
				if (!fd_h) {
					cf_crash(AS_DEMARSHAL, "malloc");
				}

				fd_h->fd = csocket;

				fd_h->last_used = cf_getms();
				fd_h->inuse = true;
				fd_h->t_inprogress = false;
				fd_h->proto = 0;
				fd_h->proto_unread = 0;
				fd_h->fh_info = 0;
				fd_h->security_filter = as_security_filter_create();

				// Insert into the global table so the reaper can manage it. Do
				// this before queueing it up for demarshal threads - once
				// EPOLL_CTL_ADD is done it's difficult to back out (if insert
				// into global table fails) because fd state could be anything.
				cf_rc_reserve(fd_h);

				pthread_mutex_lock(&g_file_handle_a_LOCK);

				int j;
				bool inserted = true;

				if (0 != cf_queue_pop(g_freeslot, &j, CF_QUEUE_NOWAIT)) {
					inserted = false;
				}
				else {
					g_file_handle_a[j] = fd_h;
				}

				pthread_mutex_unlock(&g_file_handle_a_LOCK);

				if (!inserted) {
					cf_info(AS_DEMARSHAL, "unable to add socket to file handle table");
					shutdown(csocket, SHUT_RDWR);
					close(csocket);
					csocket = -1;
					cf_rc_free(fd_h); // will free even with ref-count of 2
				}
				else {
					// Place the client socket in the event queue.
					memset(&ev, 0, sizeof(ev));
					ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP ;
					ev.data.ptr = fd_h;

					// Round-robin pick up demarshal thread epoll_fd and add
					// this new connection to epoll.
					int id;
					while (true) {
						id = (id_cntr++) % g_demarshal_args->num_threads;
						if (g_demarshal_args->epoll_fd[id] != 0) {
							break;
						}
					}

					if (0 > (n = epoll_ctl(g_demarshal_args->epoll_fd[id], EPOLL_CTL_ADD, csocket, &ev))) {
						cf_info(AS_DEMARSHAL, "unable to add socket to event queue of demarshal thread %d %d", id, g_demarshal_args->num_threads);
						pthread_mutex_lock(&g_file_handle_a_LOCK);
						fd_h->inuse = false;
						AS_RELEASE_FILE_HANDLE(fd_h);
						fd_h = 0;
						pthread_mutex_unlock(&g_file_handle_a_LOCK);
					}
					else {
						cf_atomic_int_incr(&g_config.proto_connections_opened);
					}
				}
			}
			else {
				bool has_extra_ref   = false;
				as_file_handle *fd_h = events[i].data.ptr;
				if (fd_h == 0) {
					cf_info(AS_DEMARSHAL, "event with null handle, continuing");
					goto NextEvent;
				}

				// Process data on an existing connection: this might be more
				// activity on an already existing transaction, so we have some
				// state to manage.
				as_proto *proto_p = 0;
				int fd = fd_h->fd;

				if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
					cf_detail(AS_DEMARSHAL, "proto socket: remote close: fd %d event %x", fd, events[i].events);
					// no longer in use: out of epoll etc
					goto NextEvent_FD_Cleanup;
				}

				// If pointer is NULL, then we need to create a transaction and
				// store it in the buffer.
				if (fd_h->proto == NULL) {
					as_proto proto;
					int sz;

					if (fd_h->t_inprogress) {
						cf_debug(AS_DEMARSHAL, "receiving pipelined request");
					}

					/* Get the number of available bytes */
					if (-1 == ioctl(fd, FIONREAD, &sz)) {
						cf_info(AS_DEMARSHAL, "unable to get number of available bytes");
						goto NextEvent_FD_Cleanup;
					}

					// If we don't have enough data to fill the message buffer,
					// just wait and we'll come back to this one. However, we'll
					// let messages with zero size through, since they are
					// likely errors. We don't cleanup the FD in this case since
					// we'll get more data on it.
					if (sz < sizeof(as_proto) && sz != 0) {
						goto NextEvent;
					}

					// Do a preliminary read of the header into a stack-
					// allocated structure, so that later on we can allocate the
					// entire message buffer.
					if (0 >= (n = cf_socket_recv(fd, &proto, sizeof(as_proto), MSG_WAITALL))) {
						cf_detail(AS_DEMARSHAL, "proto socket: read header fail: error: rv %d sz was %d errno %d", n, sz, errno);
						goto NextEvent_FD_Cleanup;
					}

					// Swap the necessary elements of the as_proto.
					as_proto_swap(&proto);

					// (For storing the returned values when getting the client info.)
					char ip_addr[24];
					ip_addr[0] = 0;
					int port = 0;

					if (proto.sz > PROTO_SIZE_MAX) {
						if (!get_fd_ip_addr_and_port(fd, ip_addr, sizeof(ip_addr), &port)) {
							cf_warning(AS_DEMARSHAL, "proto input from %s:%d: msg greater than %d, likely request from non-Aerospike client, rejecting: sz %"PRIu64,
									   ip_addr, port, PROTO_SIZE_MAX, proto.sz);
						} else {
							cf_warning(AS_DEMARSHAL, "proto input: msg greater than %d, likely request from non-Aerospike client, rejecting: sz %"PRIu64,
									   PROTO_SIZE_MAX, proto.sz);
						}
						goto NextEvent_FD_Cleanup;
					}

#ifdef USE_JEM
					// Attempt to peek the namespace and set the JEMalloc arena accordingly.
					size_t peeked_data_sz = 0;
					size_t min_field_sz = sizeof(uint32_t) + sizeof(char);
					size_t min_as_msg_sz = sizeof(as_msg) + min_field_sz;
					size_t peekbuf_sz = 2048; // (Arbitrary "large enough" size for peeking the fields of "most" AS_MSGs.)
					uint8_t peekbuf[peekbuf_sz];
					if (PROTO_TYPE_AS_MSG == proto.type) {
						size_t offset = sizeof(as_msg);
						// Number of bytes to peek from the socket.
						size_t peek_sz = peekbuf_sz;                 // Peak up to the size of the peek buffer.
//						size_t peek_sz = MIN(proto.sz, peekbuf_sz);  // Peek only up to the minimum necessary number of bytes.
						if (!(peeked_data_sz = cf_socket_recv(fd, peekbuf, peek_sz, 0))) {
							cf_warning(AS_DEMARSHAL, "could not peek the as_msg header");
							goto NextEvent_FD_Cleanup;
						} else if (peeked_data_sz > min_as_msg_sz) {
//							cf_debug(AS_DEMARSHAL, "(Peeked %zu bytes.)", peeked_data_sz);
							if (peeked_data_sz > proto.sz) {
								char ip_port_str[64];
								ip_port_str[0] = '\0';
								if (!get_fd_ip_addr_and_port(fd, ip_addr, sizeof(ip_addr), &port)) {
									snprintf(ip_port_str, sizeof(ip_port_str), "%s:%d ", ip_addr, port);
								}
								cf_warning(AS_DEMARSHAL, "Received unexpected extra data from client %ssocket %d when peeking as_proto!", ip_port_str, fd);
								log_as_proto_and_peeked_data(&proto, peekbuf, peeked_data_sz);
								goto NextEvent_FD_Cleanup;
							}

							if (((as_msg*)peekbuf)->info1 & AS_MSG_INFO1_BATCH) {
								jem_set_arena(orig_arena);
							} else {
								uint16_t n_fields = ntohs(((as_msg *) peekbuf)->n_fields), field_num = 0;
								bool found = false;
	//							cf_debug(AS_DEMARSHAL, "Found %d AS_MSG fields", n_fields);
								while (!found && (field_num < n_fields)) {
									as_msg_field *field = (as_msg_field *) (&peekbuf[offset]);
									uint32_t value_sz = ntohl(field->field_sz) - 1;
	//								cf_debug(AS_DEMARSHAL, "Field #%d offset: %lu", field_num, offset);
	//								cf_debug(AS_DEMARSHAL, "\tvalue_sz %u", value_sz);
	//								cf_debug(AS_DEMARSHAL, "\ttype %d", field->type);
									if (AS_MSG_FIELD_TYPE_NAMESPACE == field->type) {
										if (value_sz >= AS_ID_NAMESPACE_SZ) {
											cf_warning(AS_DEMARSHAL, "namespace too long (%u) in as_msg", value_sz);
											goto NextEvent_FD_Cleanup;
										}
										char ns[AS_ID_NAMESPACE_SZ];
										found = true;
										memcpy(ns, field->data, value_sz);
										ns[value_sz] = '\0';
	//									cf_debug(AS_DEMARSHAL, "Found ns \"%s\" in field #%d.", ns, field_num);
										jem_set_arena(as_namespace_get_jem_arena(ns));
									} else {
	//									cf_debug(AS_DEMARSHAL, "Message field %d is not namespace (type %d) ~~ Reading next field", field_num, field->type);
										field_num++;
										offset += sizeof(as_msg_field) + value_sz;
										if (offset >= peekbuf_sz) {
											break;
										}
									}
								}
								if (!found) {
									cf_warning(AS_DEMARSHAL, "Can't get namespace from AS_MSG (peeked %zu bytes) ~~ Using default thr_demarshal arena.", peeked_data_sz);
									jem_set_arena(orig_arena);
								}
							}
						} else {
							jem_set_arena(orig_arena);
						}
					} else {
						jem_set_arena(orig_arena);
					}
#endif

					// Allocate the complete message buffer.
					proto_p = cf_malloc(sizeof(as_proto) + proto.sz);

					cf_assert(proto_p, AS_DEMARSHAL, CF_CRITICAL, "allocation: %zu %s", (sizeof(as_proto) + proto.sz), cf_strerror(errno));
					memcpy(proto_p, &proto, sizeof(as_proto));

#ifdef USE_JEM
					// Jam in the peeked data.
					if (peeked_data_sz) {
						memcpy(proto_p->data, &peekbuf, peeked_data_sz);
					}
					fd_h->proto_unread = proto_p->sz - peeked_data_sz;
#else
					fd_h->proto_unread = proto_p->sz;
#endif
					fd_h->proto = (void *) proto_p;
				}
				else {
					proto_p = fd_h->proto;
				}

				if (fd_h->proto_unread > 0) {

					// Read the data.
					n = cf_socket_recv(fd, proto_p->data + (proto_p->sz - fd_h->proto_unread), fd_h->proto_unread, 0);
					if (0 >= n) {
						if (errno == EAGAIN) {
							continue;
						}
						cf_info(AS_DEMARSHAL, "receive socket: fail? n %d errno %d %s closing connection.", n, errno, cf_strerror(errno));
						goto NextEvent_FD_Cleanup;
					}

					// Decrement bytes-unread counter.
					cf_detail(AS_DEMARSHAL, "read fd %d (%d %d)", fd, n, fd_h->proto_unread);
					fd_h->proto_unread -= n;
				}

				// Check for a finished read.
				if (0 == fd_h->proto_unread) {

					// It's only really live if it's injecting a transaction.
					fd_h->last_used = now_ms;

					fd_h->t_inprogress = true; // disallow and/or detect pipelining
					fd_h->proto = 0;
					fd_h->proto_unread = 0;

					// INIT_TR
					as_transaction tr;
					as_transaction_init(&tr, NULL, (cl_msg *)proto_p);

					cf_rc_reserve(fd_h);
					has_extra_ref   = true;
					tr.proto_fd_h   = fd_h;
					tr.start_time   = now_ns; // set transaction start time
					tr.preprocessed = false;

					if (g_config.microbenchmarks) {
						histogram_insert_data_point(g_config.demarshal_hist, now_ns);
						tr.microbenchmark_time = cf_getns();
					}

					// Check if it's compressed.
					if (tr.msgp->proto.type == PROTO_TYPE_AS_MSG_COMPRESSED) {
						// Decompress it - allocate buffer to hold decompressed
						// packet.
						uint8_t *decompressed_buf = NULL;
						size_t decompressed_buf_size = 0;
						int rv = 0;
						if ((rv = as_packet_decompression((uint8_t *)proto_p, &decompressed_buf, &decompressed_buf_size))) {
							cf_warning(AS_DEMARSHAL, "as_proto decompression failed! (rv %d)", rv);
							cf_warning_binary(AS_DEMARSHAL, proto_p, sizeof(as_proto) + proto_p->sz, CF_DISPLAY_HEX_SPACED, "compressed proto_p");
							goto NextEvent_FD_Cleanup;
						}
						// Count the packets.
						cf_atomic_int_add(&g_config.stat_compressed_pkts_received, 1);
						// Free the compressed packet since we'll be using the
						// decompressed packet from now on.
						cf_free(proto_p);
						proto_p = NULL;
						// Get original packet.
						tr.msgp = (cl_msg *)decompressed_buf;
						as_proto_swap(&(tr.msgp->proto));

						if (tr.msgp->proto.sz > PROTO_SIZE_MAX) {
							cf_warning(AS_DEMARSHAL, "as_proto decompressed packet sz %lu > %lu (MAX) %d", tr.msgp->proto.sz, PROTO_SIZE_MAX, (tr.msgp->proto.sz > PROTO_SIZE_MAX));
							cf_free(decompressed_buf);
							goto NextEvent_FD_Cleanup;
						}

						if (tr.msgp->proto.sz != decompressed_buf_size - sizeof(as_proto)) {
							cf_warning(AS_DEMARSHAL, "as_proto decompressed packet sz %lu != decompressed buffer size %lu", tr.msgp->proto.sz, decompressed_buf_size - sizeof(as_proto));
							cf_free(decompressed_buf);
							goto NextEvent_FD_Cleanup;
						}

						if (tr.msgp->proto.type == PROTO_TYPE_AS_MSG_COMPRESSED) {
							cf_warning(AS_DEMARSHAL, "as_proto decompressed packet cannot contain another compressed packet.");
							cf_free(decompressed_buf);
							goto NextEvent_FD_Cleanup;
						}
					}

					// Security protocol transactions.
					if (tr.msgp->proto.type == PROTO_TYPE_SECURITY) {
						as_security_transact(&tr);
						cf_atomic_int_incr(&g_config.proto_transactions);
						goto NextEvent;
					}

					// Fast path for info protocol requests.
					if ((tr.msgp->proto.type == PROTO_TYPE_INFO) && g_config.info_fastpath_enabled) {
						cf_debug(AS_DEMARSHAL, "[Sending Info request via fast path.]");
						if (as_info(&tr)) {
							cf_warning(AS_DEMARSHAL, "Info request failed to be enqueued ~~ Freeing protocol buffer");
							goto NextEvent_FD_Cleanup;
						}
						cf_atomic_int_incr(&g_config.proto_transactions);
						goto NextEvent;
					}

					ASD_TRANS_DEMARSHAL(nodeid, (uint64_t) tr.msgp);

					// Fast path for batch requests.
					if (tr.msgp->msg.info1 & AS_MSG_INFO1_BATCH) {
						if (as_batch_queue_task(&tr)) {
							goto NextEvent_FD_Cleanup;
						}
						cf_atomic_int_incr(&g_config.proto_transactions);
						goto NextEvent;
					}

					// Either process the transaction directly in this thread,
					// or queue it for processing by another thread (tsvc/info).
					if (0 != thr_tsvc_process_or_enqueue(&tr)) {
						cf_warning(AS_DEMARSHAL, "Failed to queue transaction to the service thread");
						goto NextEvent_FD_Cleanup;
					}
					else {
						cf_atomic_int_incr(&g_config.proto_transactions);
					}
				}

				// Jump the proto message free & FD cleanup. If we get here, the
				// above operations went smoothly. The message free & FD cleanup
				// job is handled elsewhere as directed by
				// thr_tsvc_process_or_enqueue().
				goto NextEvent;

NextEvent_FD_Cleanup:
				// If we allocated memory for the incoming message, free it.
				if (proto_p) {
					cf_free(proto_p);
					fd_h->proto = 0;
				}
				// If fd has extra reference for transaction, release it.
				if (has_extra_ref) {
					cf_rc_release(fd_h);
				}
				// Remove the fd from the events list.
				epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, 0);
				pthread_mutex_lock(&g_file_handle_a_LOCK);
				fd_h->inuse = false;
				AS_RELEASE_FILE_HANDLE(fd_h);
				fd_h = 0;
				pthread_mutex_unlock(&g_file_handle_a_LOCK);
NextEvent:
				;
			}

			// We should never be canceled externally, but just in case...
			pthread_testcancel();
		}
	}

	return NULL;
}

// Initialize the demarshal service, start demarshal threads.
int
as_demarshal_start()
{
	demarshal_args *dm = cf_malloc(sizeof(demarshal_args));
	memset(dm, 0, sizeof(demarshal_args));
	g_demarshal_args = dm;

	dm->num_threads = g_config.n_service_threads;

	g_freeslot = cf_queue_create(sizeof(int), true);
	if (!g_freeslot) {
		cf_crash(AS_DEMARSHAL, " Couldn't create reaper free list ");
	}

	// Start the listener socket: note that because this is done after privilege
	// de-escalation, we can't use privileged ports.
	g_config.socket.reuse_addr = g_config.socket_reuse_addr;
	if (0 != cf_socket_init_svc(&g_config.socket)) {
		cf_crash(AS_DEMARSHAL, "couldn't initialize service socket: %s", cf_strerror(errno));
	}
	if (-1 == cf_socket_set_nonblocking(g_config.socket.sock)) {
		cf_crash(AS_DEMARSHAL, "couldn't set socket nonblocking: %s", cf_strerror(errno));
	}

	// Create first thread which is the listener, and wait for it to come up
	// before others are spawned.
	if (0 != pthread_create(&(dm->dm_th[0]), 0, thr_demarshal, &g_config.socket)) {
		cf_crash(AS_DEMARSHAL, "Can't create demarshal threads");
	}
	while (dm->epoll_fd[0] == 0) {
		sleep(1);
	}

	// Create all the epoll_fds and wait for all the threads to come up.
	int i;
	for (i = 1; i < dm->num_threads; i++) {
		if (0 != pthread_create(&(dm->dm_th[i]), 0, thr_demarshal, &g_config.socket)) {
			cf_crash(AS_DEMARSHAL, "Can't create demarshal threads");
		}
	}

	for (i = 1; i < dm->num_threads; i++) {
		while (dm->epoll_fd[i] == 0) {
			sleep(1);
			cf_info(AS_DEMARSHAL, "Waiting to spawn demarshal threads ...");
		}
	}
	cf_info(AS_DEMARSHAL, "Started %d Demarshal Threads", dm->num_threads);

	return 0;
}
