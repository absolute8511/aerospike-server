/*
 * thr_rw_internal.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 *  internal functions used by thr_rw.c
 *
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <citrusleaf/cf_digest.h>

#include "msg.h"
#include "util.h"

#include "base/datamodel.h"
#include "base/rec_props.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "base/write_request.h"


#define RW_FIELD_OP             0
#define RW_FIELD_RESULT         1
#define RW_FIELD_NAMESPACE      2
// WARNING! This is only the NS_ID of the initiator - can't be used by client,
// as IDs are not stable between nodes. The NS_ID + DIGEST is transmitter key.
#define RW_FIELD_NS_ID          3
#define RW_FIELD_GENERATION     4
#define RW_FIELD_DIGEST         5
#define RW_FIELD_VINFOSET       6
#define RW_FIELD_AS_MSG         7   // +request+ as_msg (used in RW phase)
#define RW_FIELD_CLUSTER_KEY    8
#define RW_FIELD_RECORD         9   // +PICKLE+ record format (used in 'dup' phase)
#define RW_FIELD_TID            10
#define RW_FIELD_VOID_TIME      11
#define RW_FIELD_INFO           12  // Bitmap to convey extra info
#define RW_FIELD_REC_PROPS      13  // additional metadata for sets and secondary indices
// Field to have single message sent to do multiple operations over fabric.
// First two use cases:
// 1. LDT, to send operation on record and sub-record in single message.
// 2. Secondary index, to send record operation and secondary index operation in
//    single message.
#define RW_FIELD_MULTIOP        14
#define RW_FIELD_LDT_VERSION    15

#define RW_OP_WRITE 1
#define RW_OP_WRITE_ACK 2
#define RW_OP_DUP 3
#define RW_OP_DUP_ACK 4
#define RW_OP_MULTI 5
#define RW_OP_MULTI_ACK 6

#define RW_RESULT_OK 0 // write completed
#define RW_RESULT_NOT_FOUND 1  // a real valid "yo there's no data at this key"
#define RW_RESULT_RETRY 2 // a "yo, that's not my partition beeeeyotch

#define RW_INFO_XDR            0x0001
#define RW_INFO_MIGRATION      0x0002
#define RW_INFO_NSUP_DELETE	   0x0004
#define RW_INFO_LDT_DUMMY      0x0008 // Indicating dummy (no data)
#define RW_INFO_LDT_REC        0x0010 // Indicating LDT REC
#define RW_INFO_LDT_SUBREC     0x0020 // Indicating LDT SUB
#define RW_INFO_LDT_ESR        0x0040 // Indicating LDT ESR
#define RW_INFO_SINDEX_TOUCHED 0x0080 // Indicating the SINDEX was touched
#define RW_INFO_LDT            0x0100 // Indicating LDT Multi Op Message
#define RW_INFO_UDF_WRITE      0x0200 // Indicating the write is done from inside UDF

// Define the various TTL milestone limits in terms of seconds.
#define TTL_ONE_YEAR     31536000
#define TTL_FIVE_YEARS  157680000
#define TTL_TEN YEARS   315360000

/* TTL values set above this value, when max_ttl is NOT set, will generate an
 * error on the server. */
#define MAX_TTL_WARNING 315360000 // Set to ten years for now.

typedef struct write_local_generation {
	bool use_gen_check;
	as_generation gen_check;
	bool use_gen_set;
	as_generation gen_set;
	bool use_msg_gen;
} write_local_generation;

typedef struct ldt_prole_info_s {
	bool        replication_partition_version_match;
	uint64_t    ldt_source_version;
	uint64_t    ldt_prole_version;
	bool        ldt_prole_version_set;
} ldt_prole_info;

int write_local_pickled(
	cf_digest *,
	as_partition_reservation *,
	uint8_t *,
	size_t,
	const as_rec_props *,
	as_generation,
	uint32_t,
	cf_node,
	uint32_t,
	ldt_prole_info *
	);

extern bool msg_has_key(as_msg* m);
extern bool check_msg_key(as_msg* m, as_storage_rd* rd);
extern bool get_msg_key(as_msg* m, as_storage_rd* rd);

extern void update_metadata_in_index(as_transaction *tr, bool increment_generation, as_index *r);

typedef struct pickle_info_s {
	uint32_t	void_time;
	uint8_t*	rec_props_data;
	uint32_t	rec_props_size;
	uint8_t*	buf;
	size_t		buf_size;
} pickle_info;

extern bool pickle_all(as_storage_rd *rd, pickle_info *pickle);

extern void account_memory(as_transaction *tr, as_storage_rd *rd, uint64_t start_bytes);

extern int rw_udf_replicate(udf_record *urecord);

extern int
rw_msg_setup(
	msg *m,
	as_transaction *tr,
	cf_digest *keyd,
	uint8_t ** p_pickled_buf,
	size_t pickled_sz,
	uint32_t pickled_void_time,
	as_rec_props * p_pickled_rec_props,
	int op,
	uint16_t ldt_rectype_bits,
	bool has_udf
	);
