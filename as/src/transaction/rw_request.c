/*
 * rw_request.c
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

//==========================================================
// Includes.
//

#include "transaction/rw_request.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/rec_props.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"


//==========================================================
// Globals.
//

static cf_atomic32 g_rw_tid = 0;


//==========================================================
// Public API.
//

rw_request*
rw_request_create(cf_digest* keyd)
{
	rw_request* rw = cf_rc_alloc(sizeof(rw_request));
	cf_assert(rw, AS_RW, "alloc rw_request");

	// as_transaction look-alike:
	rw->msgp				= NULL;
	rw->msg_fields			= 0;
	rw->origin				= 0;
	rw->from_flags			= 0;
	rw->from.any			= NULL;
	rw->from_data.any		= 0;
	rw->keyd				= *keyd;
	rw->start_time			= 0;
	rw->benchmark_time		= 0;

	AS_PARTITION_RESERVATION_INIT(rw->rsv);

	rw->end_time			= 0;
	rw->generation			= 0;
	rw->void_time			= 0;
	// End of as_transaction look-alike.

	pthread_mutex_init(&rw->lock, NULL);

	rw->wait_queue_head = NULL;

	rw->is_set_up = false;
	rw->has_udf = false;
	rw->is_multiop = false;
	rw->respond_client_on_master_completion = false;

	rw->pickled_buf = NULL;
	rw->pickled_sz = 0;
	as_rec_props_clear(&rw->pickled_rec_props);

	rw->response_db.buf = NULL;
	rw->response_db.is_stack = false;
	rw->response_db.alloc_sz = 0;
	rw->response_db.used_sz = 0;

	rw->tid = cf_atomic32_incr(&g_rw_tid);
	rw->dup_res_complete = false;
	rw->dup_res_cb = NULL;
	rw->repl_write_cb = NULL;
	rw->timeout_cb = NULL;

	rw->dest_msg = NULL;
	rw->xmit_ms = 0;
	rw->retry_interval_ms = 0;

	rw->n_dest_nodes = 0;

	return rw;
}


void
rw_request_destroy(rw_request* rw)
{
	// Paranoia:
	if (rw->from.any) {
		cf_crash(AS_RW, "rw_request_destroy: origin %d has non-null 'from'",
				rw->origin);
	}

	if (rw->msgp && rw->origin != FROM_BATCH) {
		cf_free(rw->msgp);
	}

	if (rw->pickled_buf) {
		cf_free(rw->pickled_buf);
	}

	if (rw->pickled_rec_props.p_data) {
		cf_free(rw->pickled_rec_props.p_data);
	}

	cf_dyn_buf_free(&rw->response_db);

	if (rw->is_set_up) {
		if (rw->dest_msg) {
			as_fabric_msg_put(rw->dest_msg);
		}

		// Can't use rw->n_dest_nodes - might now count replica-write nodes.
		for (int i = 0; i < rw->rsv.n_dupl; i++) {
			if (rw->dup_msg[i]) {
				as_fabric_msg_put(rw->dup_msg[i]);
			}
		}

		as_partition_release(&rw->rsv);
	}

	pthread_mutex_destroy(&rw->lock);

	rw_wait_ele* e = rw->wait_queue_head;

	while (e) {
		rw_wait_ele* next = e->next;

		e->tr.from_flags |= FROM_FLAG_RESTART;
		as_tsvc_enqueue(&e->tr);

		cf_free(e);
		e = next;
	}
}
