/*
 * rw_request_hash.h
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

#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "base/datamodel.h"
#include "base/transaction.h"
#include "transaction/rw_request.h"


//==========================================================
// Typedefs.
//

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	RW_FIELD_OP,
	RW_FIELD_RESULT,
	RW_FIELD_NAMESPACE,
	RW_FIELD_NS_ID,
	RW_FIELD_GENERATION,
	RW_FIELD_DIGEST,
	RW_FIELD_VINFOSET, // now used only by LDT
	RW_FIELD_UNUSED_7,
	RW_FIELD_CLUSTER_KEY,
	RW_FIELD_RECORD,
	RW_FIELD_TID,
	RW_FIELD_VOID_TIME,
	RW_FIELD_INFO,
	RW_FIELD_UNUSED_13,
	RW_FIELD_MULTIOP, // single msg for multiple ops - LDT (& secondary index?)
	RW_FIELD_LDT_VERSION,
	RW_FIELD_LAST_UPDATE_TIME,
	RW_FIELD_SET_NAME,
	RW_FIELD_KEY,
	RW_FIELD_LDT_BITS,

	NUM_RW_FIELDS
} rw_msg_field;

#define RW_OP_WRITE 1
#define RW_OP_WRITE_ACK 2
#define RW_OP_DUP 3
#define RW_OP_DUP_ACK 4
#define RW_OP_MULTI 5
#define RW_OP_MULTI_ACK 6

#define RW_INFO_XDR				0x0001
#define RW_INFO_UNUSED_2		0x0002 // was RW_INFO_MIGRATE
#define RW_INFO_NSUP_DELETE		0x0004
#define RW_INFO_LDT_DUMMY		0x0008 // dummy (no data)
#define RW_INFO_LDT_PARENTREC	0x0010 // LDT parent record
#define RW_INFO_LDT_SUBREC		0x0020 // LDT subrecord
#define RW_INFO_UNUSED_40		0x0040 // was LDT ESR
#define RW_INFO_SINDEX_TOUCHED	0x0080 // sindex was touched
#define RW_INFO_LDT				0x0100 // LDT multi-op message
#define RW_INFO_UDF_WRITE		0x0200 // write is done from inside UDF
#define RW_INFO_TOMBSTONE		0x0400 // enterprise only

typedef struct rw_request_hkey_s {
	as_namespace_id	ns_id;
	cf_digest		keyd;
} rw_request_hkey;


//==========================================================
// Public API.
//

void as_rw_init();

uint32_t rw_request_hash_count();
transaction_status rw_request_hash_insert(rw_request_hkey* hkey, rw_request* rw, as_transaction* tr);
void rw_request_hash_delete(rw_request_hkey* hkey, rw_request* rw);
rw_request* rw_request_hash_get(rw_request_hkey* hkey);

void rw_request_hash_dump();
