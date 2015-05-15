/*
 * thr_proxy.h
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
 * proxy function declarations
 *
 */

#pragma once

#include <stdint.h>

#include "dynbuf.h"
#include "msg.h"
#include "util.h"

#include "base/transaction.h"
#include "base/write_request.h"


extern void as_proxy_init();
extern int as_proxy_divert(cf_node dst, as_transaction *tr, as_namespace *ns,
		uint64_t cluster_key);
extern int as_proxy_shipop(cf_node dst, write_request *wr);
extern int as_proxy_send_response(cf_node dst, msg *m, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_bin **bins,
		uint16_t bin_count, as_namespace *ns, uint64_t trid,
		const char *setname);
extern int as_proxy_send_ops_response(cf_node dst, msg *m, cf_dyn_buf *db);
extern int as_proxy_send_redirect(cf_node dst, msg *m, cf_node rdst);

// Get a rough estimate of the in progress size for statistics.
extern uint32_t as_proxy_inprogress();

// Compare the tids and return 0 if same. Must compare the nodes they came from
// separately.
extern int as_proxy_msg_compare(msg *m1, msg *m2);


