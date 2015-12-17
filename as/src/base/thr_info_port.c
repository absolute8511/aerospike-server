/*
 * thr_info_port.c
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
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"

#include "cf_str.h"
#include "dynbuf.h"
#include "fault.h"
#include "socket.h"

#include "base/cfg.h"
#include "base/thr_info.h"


#define EPOLL_SZ	64
// Workaround for platforms that don't have EPOLLRDHUP yet.
#ifndef EPOLLRDHUP
#define EPOLLRDHUP EPOLLHUP
#endif


// State for any open info port.
typedef struct {
	int			recv_pos;
	int			recv_alloc;
	uint8_t		*recv_buf;

	int			xmit_pos;    // where we're currently writing
	int			xmit_limit;  // the end of the write buffer
	int			xmit_alloc;
	uint8_t		*xmit_buf;

	int			fd;

} info_port_state;


void
info_port_state_free(info_port_state *ips)
{
	if (ips->recv_buf) cf_free(ips->recv_buf);
	if (ips->xmit_buf) cf_free(ips->xmit_buf);
	if (ips->fd != -1) {
		close(ips->fd);
		cf_atomic_int_incr(&g_config.info_connections_closed);
	}
	memset(ips, -1, sizeof(info_port_state));
	cf_free(ips);
}


int
thr_info_port_readable(info_port_state *ips)
{
	int sz;
	int fd = ips->fd;

	// Get the number of available bytes.
	if (-1 == ioctl(fd, FIONREAD, &sz)) {
		cf_info(AS_INFO_PORT, "unable to get number of available bytes");
		return -1;
	}

	if (sz == 0) {
		return 0;
	}

	// Make sure we've got some reasonable space in the read buffer.
	if (ips->recv_alloc - ips->recv_pos < sz) {
		int new_sz = sz + ips->recv_pos + 100;
		ips->recv_buf = cf_realloc(ips->recv_buf, new_sz);
		if (!ips->recv_buf) {
			return -1;
		}
		ips->recv_alloc = new_sz;
	}

	int n = cf_socket_recv(fd, ips->recv_buf + ips->recv_pos, ips->recv_alloc - ips->recv_pos, 0);
	if (n < 0) {
		if (errno != EAGAIN) {
			cf_detail(AS_INFO_PORT, "info socket: read fail: error: rv %d sz was %d errno %d", n, ips->recv_alloc - ips->recv_pos, errno);
		}
		return -1;
	}
	ips->recv_pos += n;

	// What about a control-c?
	if (-1 != cf_str_strnchr(ips->recv_buf, ips->recv_pos, 0xFF)) {
		cf_debug(AS_INFO_PORT, "recived a control c, aborting");
		return -1;
	}

	// See if we've got a CR or LF in the buf yet.
	int cr = cf_str_strnchr(ips->recv_buf, ips->recv_pos, '\r');
	int lf = cf_str_strnchr(ips->recv_buf, ips->recv_pos, '\n');
	if ((cr >= 0) || (lf >= 0)) {
		size_t len;
		// Take the closest of cr or lf.
		if (-1 == lf) {
			len = cr;
		}
		else if (-1 == cr) {
			len = lf;
		}
		else {
			len = lf < cr ? lf : cr;
		}

		// We have a message. Process it.
		cf_dyn_buf_define(db);

		ips->recv_buf[len] = '\n';
		len++;

		// Fill out the db buffer with the response (always returns 0).
		as_info_buffer(ips->recv_buf, len, &db);
		if (db.used_sz == 0)   			cf_dyn_buf_append_char(&db, '\n');

		// See if it has a tab, get that location. It probably does.
		int tab = cf_str_strnchr(db.buf, db.used_sz , '\t');
		tab++;

		while (len < ips->recv_pos &&
				((ips->recv_buf[len] == '\r') || (ips->recv_buf[len] == '\n'))) {

			len ++ ;
		}

		// Move transmit buffer forward.
		if (ips->recv_pos - len > 0) {
			memmove(ips->recv_buf, ips->recv_buf + len, ips->recv_pos - len);
			ips->recv_pos -= len;
		}
		else {
			ips->recv_pos = 0;
		}

		// Queue the response - set to the xmit buf.
		if (ips->xmit_alloc - ips->xmit_limit < db.used_sz) {
			ips->xmit_buf = cf_realloc(ips->xmit_buf, db.used_sz + ips->xmit_limit);
			ips->xmit_alloc = db.used_sz + ips->xmit_limit;
		}
		memcpy(ips->xmit_buf + ips->xmit_limit, db.buf + tab, db.used_sz - tab);
		ips->xmit_limit += db.used_sz - tab;

		cf_dyn_buf_free(&db);
	}

	return 0;
}


int
thr_info_port_writable(info_port_state *ips)
{
	// Do we have bytes to write?
	if (ips->xmit_limit > 0) {

		// Write them!
		int rv = send(ips->fd, ips->xmit_buf + ips->xmit_pos, ips->xmit_limit - ips->xmit_pos , MSG_NOSIGNAL);
		if (rv < 0) {
			if (errno != EAGAIN) {
				return -1;
			}
		}
		else if (rv == 0) {
			cf_debug(AS_INFO_PORT, "send with return value 0");
			return 0;
		}
		else {
			ips->xmit_pos += rv;
			if (ips->xmit_pos == ips->xmit_limit) {
				ips->xmit_pos = ips->xmit_limit = 0;
			}
		}
	}

	return 0;
}


// Demarshal info socket connections.
void *
thr_info_port_fn(void *arg)
{
	cf_socket_cfg *s;
	static struct epoll_event ev;
	int nevents, i, n, epoll_fd;
	int err_count = 0;

	cf_debug(AS_INFO_PORT, "info-port process started");

	// Start the listener socket: note that because this is done after privilege
	// de-escalation, we can't use privileged ports.
	cf_socket_cfg info_socket;
	info_socket.addr = cf_strdup("0.0.0.0");
	info_socket.proto = SOCK_STREAM;
	info_socket.port = g_config.info_port;
	info_socket.reuse_addr = g_config.socket_reuse_addr ? true : false;
	// Listen happens here.
	if (0 != cf_socket_init_svc(&info_socket)) {
		cf_crash(AS_AS, "couldn't initialize service socket");
	}
	if (-1 == cf_socket_set_nonblocking(info_socket.sock)) {
		cf_crash(AS_AS, "couldn't set socket nonblocking");
	}

	s = &info_socket;

	// Create the epoll file descriptor and set up the epoll structure to listen
	// to the socket.
	if (-1 == (epoll_fd = epoll_create(EPOLL_SZ))) {
		cf_crash(AS_INFO_PORT, "epoll_create(): %s", cf_strerror(errno));
	}
	ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
	ev.data.fd = s->sock;
	if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_ADD, s->sock, &ev)) {
		cf_crash(AS_INFO_PORT, "epoll_ctl(): %s", cf_strerror(errno));
	}

	// Demarshal transactions from the socket.
	for ( ; ; ) {
		struct epoll_event events[EPOLL_SZ];

		cf_debug(AS_INFO_PORT, "calling epoll");

		nevents = epoll_wait(epoll_fd, events, EPOLL_SZ, -1);

		cf_debug(AS_INFO_PORT, "epoll event received: nevents %d", nevents);

		// Iterate over all events.
		for (i = 0; i < nevents; i++) {

			if (s->sock == events[i].data.fd) {

				// Accept new connections on the service socket.
				int csocket;
				struct sockaddr_in caddr;
				socklen_t clen = sizeof(caddr);
				char cpaddr[24];

				if (-1 == (csocket = accept(s->sock, (struct sockaddr *)&caddr, &clen))) {
					// This means we're out of file descriptors - could be a SYN
					// flood attack or misbehaving client. Eventually we'd like
					// to make the reaper fairer, but for now we'll just have to
					// ignore the accept error and move on.
					if (errno == EMFILE) {
						cf_debug(AS_INFO_PORT, " warning: too many file descriptors in use, consider raising limit");
						continue;
					}
					if (EINVAL == errno) {
						if (!(err_count++ % 1000)) {
							cf_warning(AS_INFO_PORT, "accept(%d, %p, %d) returned EINVAL ~~ Ignoring (err_count: %d)", s->sock, caddr, clen, err_count);
						}
						continue;
					}
					cf_crash(AS_INFO_PORT, "accept: %s (errno %d)", cf_strerror(errno), errno);
				}
				if (NULL == inet_ntop(AF_INET, &caddr.sin_addr.s_addr, (char *)cpaddr, sizeof(cpaddr))) {
					cf_crash(AS_INFO_PORT, "inet_ntop(): %s (errno %d)", cf_strerror(errno), errno);
				}

				cf_detail(AS_INFO_PORT, "new connection: %s", cpaddr);

				// Set the socket to nonblocking.
				if (-1 == cf_socket_set_nonblocking(csocket)) {
					cf_debug(AS_INFO_PORT, "unable to set client socket to nonblocking mode");
					close(csocket);
					continue;
				}

				info_port_state *ips = cf_malloc(sizeof(info_port_state));
				if (!ips) {
					cf_crash(AS_INFO_PORT, "malloc");
				}

				ips->recv_pos = 0;
				ips->recv_alloc = 100;
				ips->recv_buf = cf_malloc(100);
				ips->xmit_limit = ips->xmit_pos = 0;
				ips->xmit_alloc = 100;
				ips->xmit_buf = cf_malloc(100);

				ips->fd = csocket;

				// Place the client socket in the event queue.
				ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP ;
				ev.data.ptr = ips;
				if (0 > (n = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, csocket, &ev))) {
					cf_debug(AS_INFO_PORT, "unable to add socket to event queue");
					cf_free(ips->recv_buf);
					cf_free(ips->xmit_buf);
					cf_free(ips);
				}
				cf_atomic_int_incr(&g_config.info_connections_opened);

			}
			else {
				// A regular working socket.

				info_port_state *ips = events[i].data.ptr;
				if (ips == 0) {
					cf_debug(AS_INFO_PORT, "event with null handle, continuing");
					continue;
				}

				cf_detail(AS_INFO_PORT, "event: %x fd: %d", events[i].events, ips->fd);

				if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
					cf_detail(AS_INFO_PORT, "proto socket: remote close: fd %d event %x", ips->fd, events[i].events);
					// no longer in use: out of epoll etc
					epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ips->fd, 0);
					info_port_state_free(ips);
					continue;
				}


				if (events[i].events & EPOLLIN) {
					if (0 != thr_info_port_readable(ips)) {
						epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ips->fd, 0);
						info_port_state_free(ips);
						goto NextEvent;
					}

				}

				if (events[i].events & EPOLLOUT) {
					if (0 != thr_info_port_writable(ips)) {
						epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ips->fd, 0);
						info_port_state_free(ips);
						goto NextEvent;
					}
				}

NextEvent:
				;
			}

			// We should never be canceled externally, but just in case...
			pthread_testcancel();
		}
	}

	return NULL;
}

pthread_t g_info_port_th;

int
as_info_port_start()
{
	if (g_config.info_port == 0) {
		return 0;
	}

	if (0 != pthread_create(&g_info_port_th, 0, thr_info_port_fn, 0)) {
		cf_crash(AS_AS, "pthread_create: %s", cf_strerror(errno));
	}

	return 0;
}
