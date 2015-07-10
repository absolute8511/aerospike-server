/*
 * thr_info.h
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

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "socket.h"
#include "util.h"

#include "base/transaction.h"
#include "fabric/paxos.h"


extern void as_info_paxos_event(as_paxos_generation gen,
		as_paxos_change *change, cf_node succession[], void *udata);

// Starting to calculate more and more stats in thr_info. Perhaps this should be
// elsewhere?
extern uint64_t thr_info_get_object_count();
extern uint64_t thr_info_get_subobject_count();

// Processes an info request that comes in from the network, sends the response.
extern int as_info(as_transaction *tr);

// Processes a pure puffer request without any info header stuff.
extern int as_info_buffer(uint8_t *req_buf, size_t req_buf_len, cf_dyn_buf *rsp);

extern void info_debug_ticker_start();

// The info unit uses the fabric to communicate with the other members of the
// cluster so it needs to register for different messages and create listener
// threads, etc.
extern int as_info_init();

// The info port is used by more basic monitoring services.
extern int as_info_port_start();

extern void build_service_list(cf_ifaddr *ifaddr, int ifaddr_sz, cf_dyn_buf *db);
