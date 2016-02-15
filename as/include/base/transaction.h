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

#define MICROBENCHMARK_HIST_INSERT(__hist_name) \
{ \
	if (g_config.microbenchmarks && tr.microbenchmark_time) { \
		histogram_insert_data_point(g_config.__hist_name, tr.microbenchmark_time); \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_P(__hist_name) \
{ \
	if (g_config.microbenchmarks && tr->microbenchmark_time) { \
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
		if (tr.microbenchmark_time) { \
			histogram_insert_data_point(g_config.__hist_name, tr.microbenchmark_time); \
		} \
		tr.microbenchmark_time = cf_getns(); \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_AND_RESET_P(__hist_name) \
{ \
	if (g_config.microbenchmarks) { \
		if (tr->microbenchmark_time) { \
			histogram_insert_data_point(g_config.__hist_name, tr->microbenchmark_time); \
		} \
		tr->microbenchmark_time = cf_getns(); \
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

struct as_transaction_s;
typedef int (*ureq_cb)(struct as_transaction_s *tr, int retcode);
typedef int (*ures_cb)(struct as_transaction_s *tr, int retcode);

typedef enum {
	UDF_UNDEF_REQUEST = -1,
	UDF_SCAN_REQUEST  = 0,
	UDF_QUERY_REQUEST = 1
} udf_request_type;

typedef struct udf_request_data {
	void *				req_udata;
	ureq_cb				req_cb;		// callback called at completion with request structure
	void *				res_udata;
	ures_cb				res_cb;		// callback called at completion with response structure
	udf_request_type	req_type;
} ureq_data;

#define UREQ_DATA_INIT(ureq)	\
	(ureq)->req_cb    = NULL;	\
	(ureq)->req_udata = NULL;	\
	(ureq)->res_cb    = NULL;	\
	(ureq)->res_udata = NULL;   \
	(ureq)->req_type  = UDF_UNDEF_REQUEST;

#define UREQ_DATA_RESET UREQ_DATA_INIT

#define UREQ_DATA_COPY(dest, src)			\
	(dest)->req_cb    = (src)->req_cb;		\
	(dest)->req_udata = (src)->req_udata;	\
	(dest)->res_cb    = (src)->res_cb;		\
	(dest)->res_udata = (src)->res_udata;   \
	(dest)->req_type  = (src)->req_type;

#define AS_TRANSACTION_FLAG_NSUP_DELETE     0x0001
#define AS_TRANSACTION_FLAG_INTERNAL        0x0002
#define AS_TRANSACTION_FLAG_SHIPPED_OP      0x0004
#define AS_TRANSACTION_FLAG_UNUSED_8        0x0008 // deprecated LDT_SUB
#define AS_TRANSACTION_FLAG_SINDEX_TOUCHED  0x0010

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
	/* and the digest to apply it to */
	cf_digest 	      keyd;
	/* generation to send to the user */
	uint32_t          generation;

	/* result code to send to user */
	int               result_code;
	// set to true in duplicate resolution phase
	bool              microbenchmark_is_resolve;
	/* has the transaction been 'prepared' by as_prepare_transaction? This
	   means that the incoming msg has been translated and the corresponding
	   transaction structure has been set up */
	bool              preprocessed;
	// By default we store the key if the client sends it. However some older
	// clients send the key without a digest, without intent to store the key.
	// In such cases, we set this flag false and don't store the key.
	uint8_t           flag;

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
	ureq_data         udata;

	// Batch
	struct as_batch_shared* batch_shared;
	uint32_t batch_index;

	// TODO - another re-org of this structure...
	uint32_t void_time;

} as_transaction;

typedef struct as_query_transaction_s as_query_transaction;

extern int as_transaction_prepare(as_transaction *tr);
extern void as_transaction_init(as_transaction *tr, cf_digest *, cl_msg *);
extern int as_transaction_digest_validate(as_transaction *tr);

// For now it's not worth storing the trid in the as_transaction struct since we
// only parse it from the msg once per transaction anyway.
static inline uint64_t
as_transaction_trid(const as_transaction *tr)
{
	as_msg_field *f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_TRID);

	return f ? cf_swap_from_be64(*(uint64_t*)f->data) : 0;
}

struct udf_call_s; // forward declaration for udf_call, defined in udf_rw.h

// Data needed for creation of a transaction, add more fields here later.
typedef struct tr_create_data {
	cf_digest			digest;
	as_namespace *		ns;
	char				set[AS_SET_NAME_MAX_SIZE];
	struct udf_call_s *	call;
	uint				msg_type;	/* Which type of msg is it -- maybe make it default? */
	as_file_handle *	fd_h;		/* fd of the parent scan job */
	uint64_t			trid;		/* transaction id of the parent job -- if any */
	void *				udata;		/* udata to be passed on to the new transaction */
} tr_create_data;

extern int   as_transaction_create_internal(as_transaction *tr, tr_create_data * data);

void as_transaction_demarshal_error(as_transaction* tr, uint32_t error_code);
void as_transaction_error_unswapped(as_transaction* tr, uint32_t error_code);
void as_transaction_error(as_transaction* tr, uint32_t error_code);
