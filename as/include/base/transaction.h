/*
 * transaction.h
 *
 * Copyright (C) 2008-2015 Aerospike, Inc.
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

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "msg.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "storage/storage.h"


//==========================================================
// Microbenchmark macros.
//

#define MICROBENCHMARK_SET_TO_START() \
{ \
	if (g_config.microbenchmarks) { \
		tr.microbenchmark_time = tr.start_time; \
	} \
}

#define MICROBENCHMARK_SET_TO_START_P() \
{ \
	if (g_config.microbenchmarks) { \
		tr->microbenchmark_time = tr->start_time; \
	} \
}

#define MICROBENCHMARK_HIST_INSERT(__hist_name) \
{ \
	if (g_config.microbenchmarks && tr.microbenchmark_time != 0) { \
		histogram_insert_data_point(g_config.__hist_name, tr.microbenchmark_time); \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_P(__hist_name) \
{ \
	if (g_config.microbenchmarks && tr->microbenchmark_time != 0) { \
		histogram_insert_data_point(g_config.__hist_name, tr->microbenchmark_time); \
	} \
}

#define MICROBENCHMARK_RESET() \
{ \
	if (g_config.microbenchmarks) { \
		tr.microbenchmark_time = cf_getns(); \
	} \
}

#define MICROBENCHMARK_RESET_P() \
{ \
	if (g_config.microbenchmarks) { \
		tr->microbenchmark_time = cf_getns(); \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_AND_RESET(__hist_name) \
{ \
	if (g_config.microbenchmarks) { \
		if (tr.microbenchmark_time != 0) { \
			tr.microbenchmark_time = histogram_insert_data_point(g_config.__hist_name, tr.microbenchmark_time); \
		} \
		else { \
			tr.microbenchmark_time = cf_getns(); \
		} \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_AND_RESET_P(__hist_name) \
{ \
	if (g_config.microbenchmarks) { \
		if (tr->microbenchmark_time != 0) { \
			tr->microbenchmark_time = histogram_insert_data_point(g_config.__hist_name, tr->microbenchmark_time); \
		} \
		else { \
			tr->microbenchmark_time = cf_getns(); \
		} \
	} \
}


//==========================================================
// Transaction.
//

typedef struct as_file_handle_s {
	char		client[64];		// client identifier (currently ip-addr:port)
	uint64_t	last_used;		// last ms we read or wrote
	int			fd;
	int			epoll_fd;		// the file descriptor of our epoll instance
	bool		reap_me;		// tells the reaper to come and get us
	bool		trans_active;	// a transaction is running on this connection
	uint32_t	fh_info;		// bitmap containing status info of this file handle
	as_proto	*proto;
	uint64_t	proto_unread;
	void		*security_filter;
} as_file_handle;

#define FH_INFO_DONOT_REAP	0x00000001	// this bit indicates that this file handle should not be reaped

// Helpers to release transaction file handles.
void as_release_file_handle(as_file_handle *proto_fd_h);
void as_end_of_transaction(as_file_handle *proto_fd_h, bool force_close);
void as_end_of_transaction_ok(as_file_handle *proto_fd_h);
void as_end_of_transaction_force_close(as_file_handle *proto_fd_h);

#define AS_TRANSACTION_FLAG_NSUP_DELETE     0x0001
#define AS_TRANSACTION_FLAG_BATCH_SUB       0x0002 // was INTERNAL
#define AS_TRANSACTION_FLAG_SHIPPED_OP      0x0004
#define AS_TRANSACTION_FLAG_UNUSED_8        0x0008 // deprecated LDT_SUB
#define AS_TRANSACTION_FLAG_SINDEX_TOUCHED  0x0010

struct iudf_origin_s;
struct as_batch_shared;

/* as_transaction
 * The basic unit of work
 *
 * NB: Fields which are frequently accessed together are laid out
 *     to be single cache line. DO NOT REORGANIZE unless you know
 *     what you are doing ..and tr.rsv starts at 64byte aligned
 *     address
 */
typedef struct as_transaction_s {

	/************ Frequently accessed fields *************/
	/* The message describing the action */
	cl_msg          * msgp;
	/* Bit field showing which message fields are present */
	uint32_t		  msg_fields;
	/* and the digest to apply it to */
	cf_digest 	      keyd;

	uint16_t          generation;
	// set to true in duplicate resolution phase
	bool              microbenchmark_is_resolve;
	// By default we store the key if the client sends it. However some older
	// clients send the key without a digest, without intent to store the key.
	// In such cases, we set this flag false and don't store the key.
	uint8_t           flag;
	uint8_t           result_code;

	// INTERNAL INTERNAL INTERNAL
	/* start time of the transaction at the running node */
	uint64_t          start_time;
	uint64_t          end_time; // client requested end time, same time base as start

	// to collect microbenchmarks
	uint64_t          microbenchmark_time;

	/******************** 64 bytes *****************/
	// Make sure rsv starts aligned at the cacheline
	/* the reservation of the partition (and thus tree) I'm acting against */
	as_partition_reservation rsv;

	/******* Infrequently or conditionally accessed fields ************/
	/* The origin of the transaction: either a file descriptor for a socket
	 * or a node ID */
	as_file_handle	* proto_fd_h;
	cf_node 	      proxy_node;
	msg 		    * proxy_msg;

	/* User data corresponding to the internally created transaction
	   first user is Scan UDF */
	struct iudf_origin_s * iudf_orig;

	// Batch
	struct as_batch_shared* batch_shared;
	uint32_t batch_index;

	// TODO - another re-org of this structure...
	uint32_t void_time;

} as_transaction;

typedef struct as_query_transaction_s as_query_transaction;

extern void as_transaction_init(as_transaction *tr, cf_digest *, cl_msg *);

bool as_transaction_set_msg_field_flag(as_transaction *tr, uint8_t type);
bool as_transaction_demarshal_prepare(as_transaction *tr);
void as_transaction_proxyee_prepare(as_transaction *tr);

static inline bool
as_transaction_is_batch_sub(const as_transaction *tr)
{
	return (tr->flag & AS_TRANSACTION_FLAG_BATCH_SUB) != 0;
}

static inline bool
as_transaction_has_set(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SET) != 0;
}

static inline bool
as_transaction_has_key(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_KEY) != 0;
}

static inline bool
as_transaction_has_digest(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_DIGEST_RIPE) != 0;
}

static inline bool
as_transaction_has_no_key_or_digest(const as_transaction *tr)
{
	return (tr->msg_fields & (AS_MSG_FIELD_BIT_KEY | AS_MSG_FIELD_BIT_DIGEST_RIPE)) == 0;
}

static inline bool
as_transaction_is_multi_record(const as_transaction *tr)
{
	return	(tr->msg_fields & (AS_MSG_FIELD_BIT_KEY | AS_MSG_FIELD_BIT_DIGEST_RIPE)) == 0 &&
			(tr->flag & AS_TRANSACTION_FLAG_BATCH_SUB) == 0;
}

static inline bool
as_transaction_is_batch_direct(const as_transaction *tr)
{
	// Assumes we're already multi-record.
	return (tr->msg_fields & AS_MSG_FIELD_BIT_DIGEST_RIPE_ARRAY) != 0;
}

static inline bool
as_transaction_is_query(const as_transaction *tr)
{
	// Assumes we're already multi-record.
	return (tr->msg_fields & AS_MSG_FIELD_BIT_INDEX_RANGE) != 0;
}

static inline bool
as_transaction_is_udf(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_UDF_FILENAME) != 0;
}

static inline bool
as_transaction_has_udf_op(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_UDF_OP) != 0;
}

static inline bool
as_transaction_has_scan_options(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SCAN_OPTIONS) != 0;
}

// For now it's not worth storing the trid in the as_transaction struct since we
// only parse it from the msg once per transaction anyway.
static inline uint64_t
as_transaction_trid(const as_transaction *tr)
{
	if ((tr->msg_fields & AS_MSG_FIELD_BIT_TRID) == 0) {
		return 0;
	}

	as_msg_field *f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_TRID);

	return cf_swap_from_be64(*(uint64_t*)f->data);
}

int as_transaction_init_iudf(as_transaction *tr, as_namespace *ns, cf_digest *keyd);

void as_transaction_demarshal_error(as_transaction* tr, uint32_t error_code);
void as_transaction_error(as_transaction* tr, uint32_t error_code);
