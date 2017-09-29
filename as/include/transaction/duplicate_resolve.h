/*
 * duplicate_resolve.h
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

#include "msg.h"
#include "node.h"

#include "base/transaction.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

bool dup_res_make_message(rw_request* rw, as_transaction* tr);
void dup_res_setup_rw(rw_request* rw, as_transaction* tr, dup_res_done_cb dup_res_cb, timeout_done_cb timeout_cb);
void dup_res_handle_request(cf_node node, msg* m);
void dup_res_handle_ack(cf_node node, msg* m);
