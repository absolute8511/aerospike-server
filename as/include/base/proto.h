/*
 * proto.h
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
 * wire protocol definition
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <netinet/in.h>

#include <aerospike/as_val.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_vector.h>

#include "dynbuf.h"


// Forward declarations.
struct as_bin_s;
struct as_index_s;
struct as_storage_rd_s;
struct as_namespace_s;
struct as_file_handle_s;

// These numbers match with cl_types.h on the client

#define AS_PROTO_RESULT_OK 0
#define AS_PROTO_RESULT_FAIL_UNKNOWN 1 // unknown failure - consider retry
#define AS_PROTO_RESULT_FAIL_NOTFOUND 2
#define AS_PROTO_RESULT_FAIL_GENERATION 3
#define AS_PROTO_RESULT_FAIL_PARAMETER 4
#define AS_PROTO_RESULT_FAIL_RECORD_EXISTS 5 // if 'WRITE_ADD', could fail because already exists
#define AS_PROTO_RESULT_FAIL_BIN_EXISTS 6
#define AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH 7
#define AS_PROTO_RESULT_FAIL_PARTITION_OUT_OF_SPACE 8
#define AS_PROTO_RESULT_FAIL_TIMEOUT 9
#define AS_PROTO_RESULT_FAIL_NOXDR 10
#define AS_PROTO_RESULT_FAIL_UNAVAILABLE 11 // error returned during node down and partition isn't available
#define AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE 12 // op and bin type incompatibility
#define AS_PROTO_RESULT_FAIL_RECORD_TOO_BIG 13
#define AS_PROTO_RESULT_FAIL_KEY_BUSY 14
#define AS_PROTO_RESULT_FAIL_SCAN_ABORT 15
#define AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE 16 // asked to do something we don't yet do (like scan+udf).
#define AS_PROTO_RESULT_FAIL_BIN_NOT_FOUND 17
#define AS_PROTO_RESULT_FAIL_DEVICE_OVERLOAD 18
#define AS_PROTO_RESULT_FAIL_KEY_MISMATCH 19
#define AS_PROTO_RESULT_FAIL_NAMESPACE 20
#define AS_PROTO_RESULT_FAIL_BIN_NAME 21
#define AS_PROTO_RESULT_FAIL_FORBIDDEN 22 // operation (perhaps temporarily) not possible

// Security result codes. Must be <= 255, to fit in one byte. Defined here to
// ensure no overlap with other result codes.
#define AS_SEC_RESULT_OK_LAST			50	// the last message
	// Security message errors.
#define AS_SEC_ERR_NOT_SUPPORTED		51	// security features not supported
#define AS_SEC_ERR_NOT_ENABLED			52	// security features not enabled
#define AS_SEC_ERR_SCHEME				53	// security scheme not supported
#define AS_SEC_ERR_COMMAND				54	// unrecognized command
#define AS_SEC_ERR_FIELD				55	// can't parse field
#define AS_SEC_ERR_STATE				56	// e.g. unexpected command
	// Security procedure errors.
#define AS_SEC_ERR_USER					60	// no user or unknown user
#define AS_SEC_ERR_USER_EXISTS			61	// user already exists
#define AS_SEC_ERR_PASSWORD				62	// no password or bad password
#define AS_SEC_ERR_EXPIRED_PASSWORD		63	// expired password
#define AS_SEC_ERR_FORBIDDEN_PASSWORD	64	// forbidden password (e.g. recently used)
#define AS_SEC_ERR_CREDENTIAL			65	// no credential or bad credential
	// ... room for more ...
#define AS_SEC_ERR_ROLE					70	// no role(s) or unknown role(s)
#define AS_SEC_ERR_ROLE_EXISTS			71	// role already exists
#define AS_SEC_ERR_PRIVILEGE			72	// no privileges or unknown privileges
	// Permission errors.
#define AS_SEC_ERR_NOT_AUTHENTICATED	80	// socket not authenticated
#define AS_SEC_ERR_ROLE_VIOLATION		81	// role (privilege) violation

// UDF Errors (100 - 120)
#define AS_PROTO_RESULT_FAIL_UDF_EXECUTION     100

// LDT (and general collection) Errors (125 - 140)
#define AS_PROTO_RESULT_FAIL_COLLECTION_ITEM_NOT_FOUND 125 // Item not found

// Secondary Index Query Failure Codes 200 - 230
#define AS_PROTO_RESULT_FAIL_INDEX_FOUND       200
#define AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND    201
#define AS_PROTO_RESULT_FAIL_INDEX_OOM         202
#define AS_PROTO_RESULT_FAIL_INDEX_NOTREADABLE 203
#define AS_PROTO_RESULT_FAIL_INDEX_GENERIC     204
#define AS_PROTO_RESULT_FAIL_INDEX_NAME_MAXLEN 205
#define AS_PROTO_RESULT_FAIL_INDEX_MAXCOUNT    206

#define AS_PROTO_RESULT_FAIL_QUERY_USERABORT   210
#define AS_PROTO_RESULT_FAIL_QUERY_QUEUEFULL   211
#define AS_PROTO_RESULT_FAIL_QUERY_TIMEOUT     212
#define AS_PROTO_RESULT_FAIL_QUERY_CBERROR     213
#define AS_PROTO_RESULT_FAIL_QUERY_NETIO_ERR   214


/* SYNOPSIS
 * Aerospike wire protocol
 *
 * Version 2
 *
 * Aerospike uses a message-oriented wire protocol to transfer information.
 * Each message consists of a header, which determines the type and the length
 * to follow. This is called the 'proto_msg'.
 *
 * these messages are vectored out to the correct handler. Over TCP, they can be
 * pipelined (but not out of order). If we wish to support out of order responses,
 * we should upgrade the protocol.
 *
 * the most common type of message is the as_msg, a message which reads or writes
 * a single row to the data store.
 *
 */

#define PROTO_VERSION					2

#define PROTO_TYPE_INFO					1 // ascii-format message for determining server info
#define PROTO_TYPE_SECURITY				2
#define PROTO_TYPE_AS_MSG				3
#define PROTO_TYPE_AS_MSG_COMPRESSED	4
#define PROTO_TYPE_MAX					5 // if you see 5, it's illegal

#define PROTO_SIZE_MAX (128 * 1024 * 1024) // used simply for validation, as we've been corrupting msgp's

#define PROTO_NFIELDS_MAX_WARNING 32

#define PROTO_FIELD_LENGTH_MAX	1024
#define PROTO_OP_LENGTH_MAX		131072

typedef struct as_proto_s {
	uint8_t		version;
	uint8_t		type;
	uint64_t	sz: 48;
	uint8_t		data[];
} __attribute__ ((__packed__)) as_proto;

/*
 * zlib decompression API needs original size of the compressed data.
 * So we need to transfer it to another end.
 * This structure packs together -
 * header + original size of data + compressed data
 */
typedef struct as_comp_proto_s {
	as_proto    proto;     // Protocol header
	uint64_t    org_sz;    // Original size of compressed data hold in 'data'
	uint8_t data[];        // Compressed data
}  as_comp_proto;

/* as_msg_field
* Aerospike message field */
typedef struct as_msg_field_s {
#define AS_MSG_FIELD_TYPE_NAMESPACE 0 	// UTF8 string
#define AS_MSG_FIELD_TYPE_SET 1
#define AS_MSG_FIELD_TYPE_KEY 2 		// contains a key value
#define AS_MSG_FIELD_TYPE_BIN 3    		// used for secondary key access - contains a bin, thus a name and value
#define AS_MSG_FIELD_TYPE_DIGEST_RIPE 4 // Key digest computed with RIPE160
#define AS_MSG_FIELD_TYPE_GU_TID 5
#define AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY 6
#define AS_MSG_FIELD_TYPE_TRID 7
#define AS_MSG_FIELD_TYPE_SCAN_OPTIONS 8

#define AS_MSG_FIELD_TYPE_SECONDARY_INDEX_SINGLE          9
#define AS_MSG_FIELD_TYPE_SECONDARY_INDEX_LIMIT          10
#define AS_MSG_FIELD_TYPE_SECONDARY_INDEX_RANGE          11
#define AS_MSG_FIELD_TYPE_COMPOUND_SECONDARY_INDEX       12
#define AS_MSG_FIELD_TYPE_LUA_MAP_FUNCTION_REGISTER      13
#define AS_MSG_FIELD_TYPE_LUA_REDUCE_FUNCTION_REGISTER   14
#define AS_MSG_FIELD_TYPE_LUA_FINALIZE_FUNCTION_REGISTER 15
#define AS_MSG_FIELD_TYPE_MAP_REDUCE_JOB_ID              16
#define AS_MSG_FIELD_TYPE_SECONDARY_INDEX_ID             17
#define AS_MSG_FIELD_TYPE_MAP_REDUCE_ARG                 18
#define AS_MSG_FIELD_TYPE_MAP_REDUCE_ID                  19
#define AS_MSG_FIELD_TYPE_CREATE_SECONDARY_INDEX         20

#define AS_MSG_FIELD_TYPE_INDEX_NAME			21
#define	AS_MSG_FIELD_TYPE_INDEX_RANGE			22
#define AS_MSG_FIELD_TYPE_INDEX_FILTER			23
#define AS_MSG_FIELD_TYPE_INDEX_LIMIT			24
#define AS_MSG_FIELD_TYPE_INDEX_ORDER_BY		25
#define AS_MSG_FIELD_TYPE_INDEX_TYPE  			26

// UDF RANGE: 30-39
#define AS_MSG_FIELD_TYPE_UDF_FILENAME			30
#define AS_MSG_FIELD_TYPE_UDF_FUNCTION			31
#define AS_MSG_FIELD_TYPE_UDF_ARGLIST			32
#define AS_MSG_FIELD_TYPE_UDF_OP				33

// #define AS_MSG_FIELD_TYPE_SPROC_PACKAGE			30
// #define AS_MSG_FIELD_TYPE_SPROC_PACKAGE_GEN		31

// #define AS_MSG_FIELD_TYPE_SPROC_MAP				32
// #define AS_MSG_FIELD_TYPE_SPROC_REDUCE			33
// #define AS_MSG_FIELD_TYPE_SPROC_FINALIZE			34
// #define AS_MSG_FIELD_TYPE_SPROC_MAP_PARAMS		35
// #define AS_MSG_FIELD_TYPE_SPROC_REDUCE_PARAMS	36
// #define AS_MSG_FIELD_TYPE_SPROC_FINALIZE_PARAMS	37

// #define AS_MSG_FIELD_TYPE_SPROC_RECORD			38
// #define AS_MSG_FIELD_TYPE_SPROC_RECORD_PARAMS	39

#define AS_MSG_FIELD_TYPE_QUERY_BINLIST			40

	/* NB: field_sz is sizeof(type) + sizeof(data) */
	uint32_t field_sz; // get the data size through the accessor function, don't worry, it's a small macro
	uint8_t type;   // ordering matters :-( see as_transaction_prepare
	uint8_t data[];
} __attribute__((__packed__)) as_msg_field;

typedef struct map_args_t {
	int    argc;
	char **kargv;
	char **vargv;
} map_args_t;

typedef struct index_metadata_t {
	char     *iname;
	int       ilen;
	char     *bname;
	int       blen;
	char     *type;
	int       tlen;
	int       msg_sz;
	uint8_t   isuniq;
	uint8_t   istime;
} index_metadata_t;

#define AS_MSG_OP_READ 1			// read the value in question
#define AS_MSG_OP_WRITE 2			// write the value in question

// Prospective CDT top-level ops:
#define AS_MSG_OP_CDT_READ 3
#define AS_MSG_OP_CDT_MODIFY 4

#define AS_MSG_OP_INCR 5			// arithmetically add a value to an existing value, works only on integers
// Unused - 6
// Unused - 7
// Unused - 8
#define AS_MSG_OP_APPEND 9			// append a value to an existing value, works on strings and blobs
#define AS_MSG_OP_PREPEND 10		// prepend a value to an existing value, works on strings and blobs
#define AS_MSG_OP_TOUCH 11			// touch a value without doing anything else to it - will increment the generation

#define AS_MSG_OP_MC_INCR 129		// Memcache-compatible version of the increment command
#define AS_MSG_OP_MC_APPEND 130		// append the value to an existing value, works only strings for now
#define AS_MSG_OP_MC_PREPEND 131	// prepend a value to an existing value, works only strings for now
#define AS_MSG_OP_MC_TOUCH 132		// Memcache-compatible touch - does not change generation

#define OP_IS_MODIFY(op) ( \
	   (op) == AS_MSG_OP_INCR \
	|| (op) == AS_MSG_OP_APPEND \
	|| (op) == AS_MSG_OP_PREPEND \
	|| (op) == AS_MSG_OP_MC_INCR \
    || (op) == AS_MSG_OP_MC_APPEND \
    || (op) == AS_MSG_OP_MC_PREPEND \
    )

#define OP_IS_TOUCH(op) ((op) == AS_MSG_OP_TOUCH || (op) == AS_MSG_OP_MC_TOUCH)

typedef struct as_msg_op_s {
	uint32_t op_sz;
	uint8_t  op;
	uint8_t  particle_type;
	uint8_t  version;
	uint8_t  name_sz;
	uint8_t	 name[]; // UTF-8
	// there's also a value here but you can't have two variable size arrays
} __attribute__((__packed__)) as_msg_op;

static inline uint8_t * as_msg_op_get_value_p(as_msg_op *op)
{
	return (uint8_t*)op + sizeof(as_msg_op) + op->name_sz;
}

static inline uint32_t as_msg_op_get_value_sz(const as_msg_op *op)
{
	return op->op_sz - (4 + op->name_sz);
}

static inline uint32_t as_msg_field_get_value_sz(as_msg_field *f)
{
	return f->field_sz - 1;
}

static inline uint32_t as_msg_op_get_value_sz_unswap(as_msg_op *op)
{
	uint32_t sz = ntohl(op->op_sz);
	return sz - (4 + op->name_sz);
}

static inline uint32_t as_msg_field_get_value_sz_unswap(as_msg_field *f)
{
	uint32_t sz = ntohl(f->field_sz);
	return sz - 1;
}

static inline uint32_t as_msg_field_get_strncpy(as_msg_field *f, char *dst, int sz)
{
	int fsz = f->field_sz - 1;
	if (sz > fsz) {
		memcpy(dst, f->data, fsz);
		dst[fsz] = 0;
		return fsz;
	}
	else {
		memcpy(dst, f->data, sz - 1);
		dst[sz - 1] = 0;
		return sz - 1;
	}
}

typedef struct as_msg_key_s {
	as_msg_field	f;
	uint8_t			key[];
} __attribute__ ((__packed__)) as_msg_key;

typedef struct as_msg_number_s {
	as_msg_field	f;
	uint32_t		number;
} __attribute__ ((__packed__)) as_msg_number;

typedef struct as_msg_s {
	/*00 [x00] (08) */	uint8_t		header_sz;	// number of bytes in this header - 22
	/*01 [x01] (09) */	uint8_t		info1;		// bitfield about this request
	/*02 [x02] (10) */	uint8_t		info2;		// filled up, need another
	/*03 [x03] (11) */	uint8_t		info3;		// nice extra space. Mmm, tasty extra space.
	/*04 [x04] (12) */	uint8_t		unused;
	/*05 [x05] (13) */	uint8_t		result_code;
	/*06 [x06] (14) */	uint32_t	generation;
	/*10 [x0A] (18) */	uint32_t	record_ttl;
	/*14 [x10] (22) */	uint32_t	transaction_ttl;
	/*18 [x12] (26) */	uint16_t	n_fields;	// number of fields
	/*20 [x14] (28) */	uint16_t	n_ops;		// number of operations
	/*22 [x16] (30) */	uint8_t		data[];		// data contains first the fields, then the ops
} __attribute__((__packed__)) as_msg;

/* as_ms
 * Aerospike message
 * sz: size of the payload, not including the header */
typedef struct cl_msg_s {
	as_proto  	proto;
	as_msg		msg;
} __attribute__((__packed__)) cl_msg;

#define AS_MSG_INFO1_READ				(1 << 0) // contains a read operation
#define AS_MSG_INFO1_GET_ALL			(1 << 1) // get all bins, period
#define AS_MSG_INFO1_GET_ALL_NODATA		(1 << 2) // get all bins WITHOUT data (currently unimplemented)
// (Note:  Bit 3 is unused.)
#define AS_MSG_INFO1_XDR				(1 << 4) // operation is being performed by XDR
#define AS_MSG_INFO1_GET_NOBINDATA		(1 << 5) // Do not get information about bins and its data
#define AS_MSG_INFO1_CONSISTENCY_LEVEL_B0	(1 << 6) // read consistency level - bit 0
#define AS_MSG_INFO1_CONSISTENCY_LEVEL_B1	(1 << 7) // read consistency level - bit 1

#define AS_MSG_INFO2_WRITE				(1 << 0) // contains a write semantic
#define AS_MSG_INFO2_DELETE				(1 << 1) // delete record
#define AS_MSG_INFO2_GENERATION			(1 << 2) // pay attention to the generation
#define AS_MSG_INFO2_GENERATION_GT		(1 << 3) // apply write if new generation >= old, good for restore
#define AS_MSG_INFO2_GENERATION_DUP		(1 << 4) // if a generation collision, create a duplicate
#define AS_MSG_INFO2_CREATE_ONLY		(1 << 5) // write record only if it doesn't exist
#define AS_MSG_INFO2_BIN_CREATE_ONLY	(1 << 6) // write bin only if it doesn't exist
#define AS_MSG_INFO2_ORDERED_OPS		(1 << 7) // bin ops (read, write, or modify) that require an ordered response

#define AS_MSG_INFO3_LAST				(1 << 0) // this is the last of a multi-part message
#define AS_MSG_INFO3_COMMIT_LEVEL_B0  	(1 << 1) // write commit level - bit 0
#define AS_MSG_INFO3_COMMIT_LEVEL_B1  	(1 << 2) // write commit level - bit 1
#define AS_MSG_INFO3_UPDATE_ONLY		(1 << 3) // update existing record only, do not create new record
#define AS_MSG_INFO3_CREATE_OR_REPLACE	(1 << 4) // completely replace existing record, or create new record
#define AS_MSG_INFO3_REPLACE_ONLY		(1 << 5) // completely replace existing record, do not create new record
#define AS_MSG_INFO3_BIN_REPLACE_ONLY	(1 << 6) // replace existing bin, do not create new bin
// (Note:  Bit 7 is unused.)

#define AS_MSG_FIELD_SCAN_DISCONNECTED_JOB			(0x04) // for sproc jobs that won't be sending results back to the client
#define AS_MSG_FIELD_SCAN_FAIL_ON_CLUSTER_CHANGE	(0x08) // if we should fail when cluster is migrating or cluster changes
#define AS_MSG_FIELD_SCAN_PRIORITY(__cl_byte)		((0xF0 & __cl_byte)>>4) // 4 bit value indicating the scan priority

static inline as_msg_field *
as_msg_field_get_next(as_msg_field *mf)
{
	return (as_msg_field*)(((uint8_t*)mf) + sizeof(mf->field_sz) + mf->field_sz);
}

static inline as_msg_field *
as_msg_field_get_next_unswap(as_msg_field *mf)
{
	return (as_msg_field*)(((uint8_t*)mf) + sizeof(mf->field_sz) + ntohl(mf->field_sz));
}

/* as_msg_field_get
 * Retrieve a specific field from a message */
static inline as_msg_field *
as_msg_field_get(as_msg *msg, uint8_t type)
{
	uint16_t n;
	as_msg_field *fp = NULL;

	fp = (as_msg_field*)msg->data;

	for (n = 0; n < msg->n_fields; n++) {

		if (fp->type == type) {
			break;
		}

		fp = as_msg_field_get_next(fp);
	}

	if (n == msg->n_fields) {
		return NULL;
	}
	else {
		return fp;
	}
}

static inline as_msg_op *
as_msg_op_get_next(as_msg_op *op)
{
	return (as_msg_op*)(((uint8_t*)op) + sizeof(uint32_t) + op->op_sz);
}

/* as_msg_field_getnext
 * Iterator for all fields of a particular type.
 * First time through: pass 0 as current, you'll get a field.
 * Next time through: pass the current as current, you'll get null when there
 * are no more.
 */
static inline as_msg_op *
as_msg_op_iterate(as_msg *msg, as_msg_op *current, int *n)
{
	// Skip over the fields the first time.
	if (! current) {
		if (msg->n_ops == 0) {
			return 0; // short cut
		}

		as_msg_field *mf = (as_msg_field*)msg->data;

		for (uint i = 0; i < msg->n_fields; i++) {
			mf = as_msg_field_get_next(mf);
		}

		current = (as_msg_op*)mf;
		*n = 0;

		return current;
	}

	(*n)++;

	if (*n >= msg->n_ops) {
		return 0;
	}

	return as_msg_op_get_next(current);
}

static inline size_t
as_proto_size_get(as_proto *proto)
{
	return sizeof(as_proto) + proto->sz;
}


extern void as_proto_swap(as_proto *m);
extern void as_msg_swap_header(as_msg *m);
extern void as_msg_swap_field(as_msg_field *mf);
extern int as_msg_swap_fields(as_msg *m, void *limit);
extern int as_msg_swap_ops(as_msg *m, void *limit);
extern int as_msg_swap_fields_and_ops(as_msg *m, void *limit);
extern int as_msg_send_reply(struct as_file_handle_s *fd_h, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op **ops,
		struct as_bin_s **bins, uint16_t bin_count, struct as_namespace_s *ns,
		uint *written_sz, uint64_t trid, const char *setname);
extern int as_msg_send_ops_reply(struct as_file_handle_s *fd_h, cf_dyn_buf *db);

extern cl_msg *as_msg_make_response_msg(uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op **ops,
		struct as_bin_s **bins, uint16_t bin_count, struct as_namespace_s *ns,
		cl_msg *msgp_in, size_t *msg_sz_in, uint64_t trid, const char *setname);
extern int as_msg_make_response_bufbuilder(struct as_index_s *r, struct as_storage_rd_s *rd,
		cf_buf_builder **bb_r, bool nobindata, char *nsname, bool use_sets, bool include_key, bool skip_empty_records, cf_vector *);
extern int as_msg_make_error_response_bufbuilder(cf_digest *keyd, int result_code,
		cf_buf_builder **bb_r, char *nsname);
extern size_t as_msg_get_bufbuilder_newsize(struct as_index_s *r, struct as_storage_rd_s *rd,
		cf_buf_builder **bb_r, bool nobindata, char *nsname, bool use_sets, cf_vector *);
extern size_t as_msg_response_msgsize(struct as_index_s *r, struct as_storage_rd_s *rd,
		bool nobindata, char *nsname, bool use_sets, cf_vector *binlist);
extern int as_msg_make_val_response_bufbuilder(const as_val *val, cf_buf_builder **bb_r, int val_sz, bool);

extern int as_msg_send_response(int fd, uint8_t* buf, size_t len, int flags);
extern int as_msg_send_fin(int fd, uint32_t result_code);

extern bool as_msg_peek_data_in_memory(cl_msg *msgp);

// To find out key things about the message before actually reading it.
typedef struct {
	int       info1;
	int       info2;
	cf_digest keyd;
	int       ns_queue_offset;
	int       ns_n_devices;
} proto_peek;

// Always succeeds, sometimes finds nothing.
extern void as_msg_peek(cl_msg *m, proto_peek *peek);

extern uint8_t * as_msg_write_fields(uint8_t *buf, const char *ns, int ns_len,
		const char *set, int set_len, const cf_digest *d, cf_digest *d_ret,
		uint64_t trid, as_msg_field *scan_param_field, void * call);

extern uint8_t * as_msg_write_header(uint8_t *buf, size_t msg_sz, uint info1,
		uint info2, uint info3, uint32_t generation, uint32_t record_ttl,
		uint32_t transaction_ttl, uint32_t n_fields, uint32_t n_ops);

// Async IO 
typedef int (* as_netio_finish_cb) (void *udata, int retcode);
typedef int (* as_netio_start_cb) (void *udata, int seq);
typedef struct as_netio_s {
	as_netio_finish_cb         finish_cb;
	as_netio_start_cb          start_cb;	
	void                     * data;
	// fd and buffer
	struct as_file_handle_s  * fd_h;
	cf_buf_builder           * bb_r;
	uint32_t                   offset;
	uint32_t                   seq;
	bool                       slow;
} as_netio;

void as_netio_init();
int as_netio_send(as_netio *io, void *q, bool);

#define AS_NETIO_OK        0
#define AS_NETIO_CONTINUE  1
#define AS_NETIO_ERR       2 
