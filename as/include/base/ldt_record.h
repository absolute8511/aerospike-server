/*
 * ldt_record.h
 *
 * Copyright (C) 2013-2016 Aerospike, Inc.
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
 * as_record interface implementation for Large Stack Objects
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_rec.h"

#include "base/index.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "storage/storage.h"


/*
 * Large Data Type (LDT)
 *
 * The General LDT value comprises a "Top Record" (a regular Aerospike record)
 * which contains a bin that is a Large Data Type.  In that LDT Bin there is
 * a map that contains pointers to some number of "Sub Records" (aka. "Chunks",
 * aka "child records", that are linked to the Top Rec).
 * Currently we limit the number of open sub (esr + subrec) to 20, but this
 * will change as the LDT structures get more sophisticated.  Over time, we'll
 * likely manage the open structures dynamically and thus allow large numbers
 * of open subs, should the situation require it.
 */

/* A Large Data Type (LDT) "Slot" refers to a single record that is a
 * child to an Aerospike "Top Record".
 */
typedef struct ldt_slot_s {
	as_rec            * c_urec_p;
	udf_record          c_urecord;   // Currently open chunk
	as_transaction      tr;
	as_storage_rd       rd;
	as_index_ref        r_ref;
	bool                inuse;
} ldt_slot;

/*
 * This structure represents an open record that contains an LDT Object.
 * "ldt_chunk" represents LDT_SLOT_CHUNK_SIZE number of slots 
 */
#define LDT_SLOT_CHUNK_SIZE 10 
typedef struct ldt_slot_chunk_s {
	ldt_slot      * slots;
} ldt_slot_chunk;

// Can add more context. LDT needs to be set this to 1 to indicate
// to server side of the context. This gets set in ldt_record
#define UDF_CONTEXT_LDT   0x0001

struct ldt_record_s {
	as_rec             * h_urec;
	uint64_t             max_chunks;
	uint64_t             num_slots_used;
	ldt_slot_chunk     * chunk;
	as_aerospike       * as;       // To operate on ldt_record_chunk
	uint64_t             version;  // this is version key used to open/close/search
	                               // for the sub_record digest
	// stats
	int                 subrec_io; 
	uint32_t            udf_context;
};

#define FOR_EACH_SUBRECORD(i, j, lrecord)           \
	for (int i = 0; i < lrecord->max_chunks; i++)   \
    for (int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++)   \
	if (lrecord->chunk[i].slots[j].inuse) 

extern const as_rec_hooks ldt_record_hooks;
