/*
 * thr_tsvc.h
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
 * thr_tsvc function declarations
 *
 */

#pragma once

#include <stdint.h>

#include "base/transaction.h"

// Per-Transaction Consistency Guarantees:
//   The client-request consistency guarantee level is respected
//   unless the corresponding server's namespace override is enabled.

// Extract the read consistency level from an as_msg.
// [Note:  Not a strict check:  Both bits == 0 means the default, anything else means the alternative.]
#define PROTO_CONSISTENCY_LEVEL(asmsg)									\
	((!(asmsg.info1 & AS_MSG_INFO1_CONSISTENCY_LEVEL_B0)				\
	  && !(asmsg.info1 & AS_MSG_INFO1_CONSISTENCY_LEVEL_B1))			\
	 ? AS_POLICY_CONSISTENCY_LEVEL_ONE : AS_POLICY_CONSISTENCY_LEVEL_ALL)

// Extract the write commit level from an as_msg.
// [Note:  Not a strict check:  Both bits == 0 means the default, anything else means the alternative.]
#define PROTO_COMMIT_LEVEL(asmsg)										\
	((!(asmsg.info3 & AS_MSG_INFO3_COMMIT_LEVEL_B0)						\
	  && !(asmsg.info3 & AS_MSG_INFO3_COMMIT_LEVEL_B1))					\
	 ? AS_POLICY_COMMIT_LEVEL_ALL : AS_POLICY_COMMIT_LEVEL_MASTER)

// Determine the read consistency level for this transaction based upon the server's namespace and client policy settings.
#define TRANSACTION_CONSISTENCY_LEVEL(tr)								\
	(tr->rsv.ns->read_consistency_level_override						\
	 ? tr->rsv.ns->read_consistency_level : PROTO_CONSISTENCY_LEVEL(tr->msgp->msg))

// Determine the write commit level for this transaction based upon the server's namespace and client policy settings.
#define TRANSACTION_COMMIT_LEVEL(tr)									\
	(tr->rsv.ns->write_commit_level_override							\
	 ? tr->rsv.ns->write_commit_level : PROTO_COMMIT_LEVEL(tr->msgp->msg))


// A rather heavyweight way to get a record generation during the initial
// phase of a write request. Does a tree lookup.
extern int thr_tsvc_get_generation(as_transaction *tr, uint32_t *generation);

// Sometimes I stamp the file descriptors bad in waiting queues. Don't just set
// to 0, make sure I know it's a stompy thing.
#define TSVC_RECLAIM_FD (-2)

int thr_tsvc_process_or_enqueue(as_transaction *tr);
int thr_tsvc_enqueue(as_transaction *tr);
void process_transaction(as_transaction *tr);

// Statistics function for monitoring server load.
extern int thr_tsvc_queue_get_size();

// Initialize the queues and start the handler threads.
extern void as_tsvc_init();

typedef struct {
	int n_sz;
	char n_name[AS_ID_NAMESPACE_SZ];
	int queue_offset;
	int n_devices;
} tsvc_namespace_devices;

extern tsvc_namespace_devices *g_tsvc_devices_a;

extern int g_tsvc_n_namespaces;

