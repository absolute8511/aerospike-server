/*
 * write_request.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "dynbuf.h"
#include "msg.h"
#include "util.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"


// Need a key digest that's unique over all namespaces.
typedef struct {
	as_namespace_id     ns_id;
	cf_digest           keyd;
} global_keyd;

typedef struct wreq_tr_element_s {
	as_transaction            tr;
	struct wreq_tr_element_s *next;
} wreq_tr_element;

// We have to keep track of the first time that all the writes come back, and
// it's a little harsh. There's an atomic for each outstanding sub-transaction
// and an atomic for all of them. The first thing an incoming ACK does is
// increment the 'complete' atomic. If this transaction was the first one to
// complete, then atomic-increment the number of complete transactions.

typedef struct write_request_s {
	pthread_mutex_t      lock;
	wreq_tr_element    * wait_queue_head;
	bool                 ready; // set to true when fully initialized
	uint32_t             tid;

	// Where initial write request comes from.
	as_file_handle     * proto_fd_h;
	cf_node              proxy_node;
	msg                * proxy_msg;

	// The incoming msgp from the transaction that created this request.
	cl_msg             * msgp;

	cf_clock             xmit_ms; // time of next retransmit
	uint32_t             retry_interval_ms; // interval to add for next retransmit
	cf_clock             start_time;

	// Be carefully atomic, since this is written in 2 places (constructor and
	// when starting the transaction), as well as read in a number of places
	// (including on the reaper thread.)
	cf_atomic_clock      end_time;

	cf_clock             microbenchmark_time;

	// The request we're making, so we can retransmit if necessary. Will be the
	// duplicate request if we're in dup phase, or the op (write) if we're in
	// the second phase but it's always the message we're sending out to the
	// nodes in the dest array.
	msg                * dest_msg;

	// After a merge-and-write is done, we need to have the real generation and
	// data so we can reflect it out to the secondaries. Who knows what data
	// they might or might not have.
	uint8_t            * pickled_buf;
	size_t               pickled_sz;
	as_rec_props         pickled_rec_props;
	uint32_t             generation;
	uint32_t             void_time;

	// Store ops' responses here.
	cf_dyn_buf           response_db;

	cf_atomic32          trans_complete; // make sure transaction gets processed only once
	cf_atomic32          dupl_trans_complete; // if 0, we are in 'dup' phase (and use atomic to only-once

	bool                 is_read;
	bool                 rsv_valid;
	// If set, this transaction should respond back to client after write on master.
	bool                 respond_client_on_master_completion;
	bool                 replication_fire_and_forget;

	// Transaction consistency guarantees:
	as_policy_consistency_level   read_consistency_level;
	as_policy_commit_level        write_commit_level;

	cf_digest            keyd;
	// udf request data
	ureq_data            udata;
	bool                 shipped_op;
	bool                 shipped_op_initiator;
	bool                 has_udf;

	as_partition_reservation rsv;
	// These three elements are used both for the duplicate resolution phase
	//  the "operation" (usually write) phase.
	int                  dest_sz;
	cf_node              dest_nodes[AS_CLUSTER_SZ];
	bool                 dest_complete[AS_CLUSTER_SZ];

	// These elements are only used in the duplicate phase, and represent the
	// response that comes back from a given node
	msg                * dup_msg[AS_CLUSTER_SZ];
	int                  dup_result_code[AS_CLUSTER_SZ];

	// Batch common data.
	struct as_batch_shared* batch_shared;
	uint32_t batch_index;
} write_request; // this is really an rw_request, but the old name looks pretty

void write_request_destructor (void *object);

#define WR_RELEASE( __wr ) \
	if (0 == cf_rc_release(__wr)) { \
		write_request_destructor(__wr); \
		cf_rc_free(__wr); \
	}

#define RW_TR_WR_MISMATCH(tr, wr) \
		(((wr)->msgp != (tr)->msgp) || !(wr)->rsv_valid)

#ifdef TRACK_WR
static int wr_fd;
#include <sys/stat.h>
#include <fcntl.h>
void wr_track_create(write_request *wr)
{
	char buf[100];
	if (0 > write( wr_fd, buf, sprintf(buf, "C %p\n", wr) ) ) return;
}

void wr_track_destroy(write_request *wr)
{
	char buf[100];
	if (0 > write( wr_fd, buf, sprintf(buf, "D %p\n", wr) ) ) return;
}

void wr_track_info(write_request *wr, char *msg)
{
	char buf[40 + strlen(msg)];
	if (0 > write( wr_fd, buf, sprintf(buf, "I %p %s\n", wr, msg) ) ) return;
}

#define WR_TRACK_INFO(__wr, __msg) wr_track_info(__wr, __msg)

void wr_track_init()
{
	char filename[100];
	strcpy(filename, "/tmp/wr_track_XXXXXX");
	wr_fd = open( mktemp(filename),
				  O_APPEND | O_WRONLY | O_CREAT,
				  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH );
	cf_info(AS_RW, "Writing write request object log to %s", filename);
}

#else

#define WR_TRACK_INFO(__wr, __msg)

#endif

void 			g_write_hash_delete(global_keyd *gk);
write_request * write_request_create(void);
int             write_request_init_tr(as_transaction *tr, void *wreq);
bool            finish_rw_process_ack(write_request *wr, uint32_t result_code, bool is_repl_write);
int             write_request_process_ack(int ns_id, cf_digest *keyd);
void            write_request_finish(as_transaction *tr, write_request *wr, bool must_delete);
int             write_request_start(as_transaction *tr, write_request **wrp, bool is_read);
int             write_request_setup(write_request *wr, as_transaction *tr, int op);
