/*
 * udf_record.h
 *
 * Copyright (C) 2013-2015 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_rec.h"
#include "aerospike/as_hashmap.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_atomic.h"

#include "base/datamodel.h"
#include "base/rec_props.h"
#include "base/transaction.h"
#include "base/xdr_serverside.h"
#include "storage/storage.h"


// Maximum number of bins that can be updated in a single UDF.
#define UDF_RECORD_BIN_ULIMIT 512
#define UDF_BIN_NONAME " "

typedef struct ldt_record_s ldt_record;

typedef struct udf_record_bin_s {
	char				name[AS_ID_BIN_SZ];
	as_val *			value;
	as_val *			oldvalue; // keeps track of old value in case rollback is required
	bool				dirty;
	bool				ishidden;
	bool				washidden;
	void                *particle_buf;
} udf_record_bin;

typedef struct udf_record_s {

	// STORAGE
	as_index_ref 		*r_ref;
	as_transaction 		*tr;
	as_storage_rd 		*rd;
	xdr_dirty_bins		*dirty;
	cf_digest			keyd;
	as_bin				stack_bins[UDF_RECORD_BIN_ULIMIT]; // TODO increase bin limit?

	// UDF CHANGE CACHE
	int8_t				ldt_rectype_bit_update; // ESR  / LDT / PARENT LDT / NOTHING
	udf_record_bin		updates[UDF_RECORD_BIN_ULIMIT]; // stores cache bin value
                                                        // if dirty flag is set the bin is being modified
	uint32_t			nupdates; // reset after every cache free, incremented in every cache set

	// RUNTIME ACCOUNTING
	uint8_t				*particle_data; // non-null for data-on-ssd, and lazy allocated on first bin write
	uint8_t				*cur_particle_data; // where the pointer is
	uint8_t				*end_particle_data;
	uint32_t			starting_memory_bytes;
	cf_atomic_int		udf_runtime_memory_used;

	// INTERNAL UTILITY
	ldt_record 			*lrecord; // Parent lrecord
	uint16_t			flag;

	// FABRIC MESSAGE
	uint8_t             op;
	uint8_t				*pickled_buf;
	size_t				pickled_sz;
	as_rec_props		pickled_rec_props;
} udf_record;

#define UDF_RECORD_FLAG_ALLOW_UPDATES		0x0001   // Write/Updates Allowed
#define UDF_RECORD_FLAG_TOO_MANY_BINS		0x0002   // UDF exceeds the bin limit
#define UDF_RECORD_FLAG_IS_SUBRECORD		0x0004   // Is udf_record for SubRecord
#define UDF_RECORD_FLAG_OPEN				0x0008   // as_record_open done
#define UDF_RECORD_FLAG_STORAGE_OPEN		0x0010   // as_storage_record_open done
#define UDF_RECORD_FLAG_HAS_UPDATES			0x0020   // Write/Update done
#define UDF_RECORD_FLAG_PREEXISTS			0x0040   // Record preexisted not created
#define UDF_RECORD_FLAG_ISVALID				0x0080   // Udf is setup and in use
#define UDF_RECORD_FLAG_METADATA_UPDATED	0x0100   // Write/Update metadata done

extern const as_rec_hooks udf_record_hooks;

//------------------------------------------------
// Utility functions for all the wrapper as_record implementation
// which use udf_record under the hood
extern void     udf_record_cache_free   (udf_record *);
extern int      udf_record_open         (udf_record *);
extern int      udf_storage_record_open (udf_record *);
extern void     udf_record_close        (udf_record *);
extern int      udf_storage_record_close(udf_record *);
extern void     udf_record_init         (udf_record *, bool);
extern void     udf_record_cleanup      (udf_record *, bool);
extern as_val * udf_record_storage_get  (const udf_record *, const char *);
extern bool     udf_record_bin_ishidden (const udf_record *urecord, const char *name);
extern bool     udf_record_ldt_enabled  (const as_rec * rec);

#define UDF_ERR_INTERNAL_PARAMETER   2
#define UDF_ERR_RECORD_NOT_VALID     3
#define UDF_ERR_PARAMETER            4
extern int      udf_record_param_check(const as_rec *rec, const char *bname, char *fname, int lineno);
extern bool     udf_record_destroy(as_rec *rec);

//------------------------------------------------
// Note that the main interface routines do NOT get declared here.
// extern int      udf_record_set_flags(const as_rec *, const char *, uint8_t);
// extern int      udf_record_set_type(const as_rec *,  int8_t);
