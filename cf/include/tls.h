/*
 * tls.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#pragma once

#include "socket.h"
#include "tls_mode.h"

void tls_check_init();

void tls_cleanup();

void tls_thread_cleanup();

void tls_socket_init(cf_socket *sock);

void tls_socket_term(cf_socket *sock);

void tls_config_context(cf_serv_spec *spec);

int tls_socket_shutdown(cf_socket *sock);

void tls_socket_close(cf_socket *sock);

void tls_socket_prepare(const cf_serv_spec *spec, cf_socket *sock, cf_sock_addr *sa);

static inline bool tls_socket_needs_handshake(cf_socket *sock)
{
	return sock->state == CF_SOCKET_STATE_TLS_HANDSHAKE;
}

int tls_socket_accept(cf_socket *sock);

int tls_socket_connect(cf_socket *sock);

int tls_socket_recv(cf_socket *sock, void *buf, size_t sz, int32_t flags,
					uint64_t timeout_msec);

int tls_socket_send(cf_socket *sock, void const *buf, size_t sz, int32_t flags,
					uint64_t timeout_msec);

int tls_socket_pending(cf_socket *sock);
