/*
 * replica_write.h
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
#include <stddef.h>
#include <stdint.h>

#include "msg.h"
#include "node.h"

#include "base/datamodel.h"
#include "base/rec_props.h"
#include "base/transaction.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

bool repl_write_make_message(rw_request* rw, as_transaction* tr);
void repl_write_setup_rw(rw_request* rw, as_transaction* tr, repl_write_done_cb repl_write_cb, timeout_done_cb timeout_cb);
void repl_write_reset_rw(rw_request* rw, as_transaction* tr, repl_write_done_cb cb);
void repl_write_handle_op(cf_node node, msg* m);
void repl_write_handle_ack(cf_node node, msg* m);

// For LDTs only:
void repl_write_ldt_make_message(msg* m, as_transaction* tr,
		uint8_t** p_pickled_buf, size_t pickled_sz,
		as_rec_props* p_pickled_rec_props, bool is_subrec);
void repl_write_handle_multiop(cf_node node, msg* m);
void repl_write_handle_multiop_ack(cf_node node, msg* m);
