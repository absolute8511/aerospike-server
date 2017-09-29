/*
 * proxy.h
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

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "dynbuf.h"
#include "node.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

void as_proxy_init();

uint32_t as_proxy_hash_count();

bool as_proxy_divert(cf_node dst, as_transaction* tr, as_namespace* ns,
		uint64_t cluster_key);
void as_proxy_return_to_sender(const as_transaction* tr, as_namespace* ns);

void as_proxy_send_response(cf_node dst, uint32_t proxy_tid,
		uint32_t result_code, uint32_t generation, uint32_t void_time,
		as_msg_op** ops, as_bin** bins, uint16_t bin_count, as_namespace* ns,
		uint64_t trid, const char* set_name);
void as_proxy_send_ops_response(cf_node dst, uint32_t proxy_tid,
		cf_dyn_buf* db);

// LDT-related.
void as_proxy_shipop(cf_node dst, rw_request* rw);
