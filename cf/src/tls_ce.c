/*
 * tls.c
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
#include <openssl/ssl.h>

#include "fault.h"
#include "socket.h"
#include "tls.h"

void
tls_check_init()
{
}

void
tls_cleanup()
{
}

void
tls_thread_cleanup()
{
}

void
tls_socket_init(cf_socket *sock)
{
	sock->ssl = NULL;
}

void
tls_socket_term(cf_socket *sock)
{
	if (sock->ssl) {
		cf_crash(CF_TLS, "unexpected TLS state");
	}
}

void
tls_config_context(cf_serv_spec *spec)
{
}

int
tls_socket_shutdown(cf_socket *sock)
{
	if (sock->ssl) {
		cf_crash(CF_TLS, "unexpected TLS state");
	}
	return -1;
}

void
tls_socket_close(cf_socket *sock)
{
	if (sock->ssl) {
		cf_crash(CF_TLS, "unexpected TLS state");
	}
}

void
tls_socket_prepare(const cf_serv_spec *spec, cf_socket *sock, cf_sock_addr *sa)
{
	cf_crash(CF_TLS, "unexpected TLS state");
}

int
tls_socket_accept(cf_socket *sock)
{
	cf_crash(CF_TLS, "unexpected TLS state");
	return 1;
}

int
tls_socket_connect(cf_socket *sock)
{
	cf_crash(CF_TLS, "unexpected TLS state");
	return 1;
}

int
tls_socket_recv(cf_socket *sock, void *buf, size_t sz, int32_t flags,
				uint64_t deadline_msec)
{
	cf_crash(CF_TLS, "unexpected TLS state");
	return 1;
}

int
tls_socket_send(cf_socket *sock, void const *buf, size_t sz, int32_t flags,
				uint64_t deadline_msec)
{
	cf_crash(CF_TLS, "unexpected TLS state");
	return 1;
}

int
tls_socket_pending(cf_socket *sock)
{
	return 0;
}

