/*
 * ldt.h
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
 * Large (Linked) Data Type module
 *
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "aerospike/as_bytes.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"

#include "base/index.h"
#include "base/ldt_record.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"

struct as_namespace_s;
struct as_partition_reservation_s;

#define LDT_SUB_GC_MAX_RATE         100000 // Do not allow more than 100,000 subrecord GC per second

// Here are the fields (the contents) of the Property Maps.  We've annotated
// the fields that are used by TopRecords and SubRecords (and both).
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#define PM_ItemCount             'I' // (Top): Count of all items in LDT
#define PM_Version               'V' // (Top): Code Version
#define PM_LdtType               'T' // (Top): Type: stack, set, map, list
#define PM_BinName               'B' // (Top): LDT Bin Name
#define PM_Magic                 'Z' // (All): Special Sauce
#define PM_EsrDigest             'E' // (All): Digest of ESR
#define PM_RecType               'R' // (All): Type of Rec:Top,Ldr,Esr,CDir
#define PM_LogInfo               'L' // (All): Log Info (currently unused)
#define PM_ParentDigest          'P' // (Subrec): Digest of TopRec
#define PM_SelfDigest            'D' // (Subrec): Digest of THIS Record

// Here are the fields that are found in the SINGLE "Hidden" LDT Control Map.
//-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//-- Record Level Property Map (RPM) Fields: One RPM per record
//-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#define RPM_LdtCount             'C'  // Number of LDTs in this rec
#define RPM_Version              'V'  // Partition Version Info (6 bytes)
#define RPM_Magic                'Z'  // Special Sauce
#define RPM_SelfDigest           'D'  // Digest of this record

#define LF_NextPage              'N'  // Digest of Next (right) Leaf Page

// Here are the fields for Tree Meta Data

#define LS_StoreState            'S'  // Compact or Regular Storage

#define LS_RootKeyList         	 'K'  // Root Key List, when in List Mode
#define LS_RootDigestList        'D'  // Digest List, when in List Mode
#define LS_CompactList           'Q'  // Simple Compact List -- before "tree mode"
#define LS_LeftLeafDigest        'A'  // Record Ptr of Left-most leaf
#define LS_RightLeafDigest       'Z'  // Record Ptr of Right-most leaf

// Define the LDT Hidden Bin Name -- for any record that contains LDTs
#define REC_LDT_CTRL_BIN         "LDTCONTROLBIN"

// Define the Property Map Bin Name for Sub Records
#define SUBREC_PROP_BIN          "SR_PROP_BIN"
#define LDT_VERSION_SZ           6


// Use these flags to designate various LDT bin types -- but they are all
// HIDDEN BINS.
#define LDT_FLAG_LDT_BIN 1
#define LDT_FLAG_HIDDEN_BIN 2
#define LDT_FLAG_CONTROL_BIN 4

extern cf_clock cf_clock_getabsoluteus();

#define ERR_TOP_REC_NOT_FOUND    2
#define ERR_NOT_FOUND            125
#define ERR_INTERNAL             1400
#define ERR_UNIQUE_KEY           1402
#define ERR_INSERT               1403
#define ERR_SEARCH               1404
#define ERR_DELETE               1405
#define ERR_VERSION              1406

#define ERR_CAPACITY_EXCEEDED    1408
#define ERR_INPUT_PARM           1409

#define ERR_TYPE_MISMATCH        1410
#define ERR_NULL_BIN_NAME        1411
#define ERR_BIN_NAME_NOT_STRING  1412
#define ERR_BIN_NAME_TOO_LONG    1413
#define ERR_TOO_MANY_OPEN_SUBRECS 1414
#define ERR_SUB_REC_NOT_FOUND    1416
#define ERR_BIN_DOES_NOT_EXIST   1417
#define ERR_BIN_ALREADY_EXISTS   1418
#define ERR_BIN_DAMAGED          1419

#define ERR_SUBREC_POOL_DAMAGED  1420
#define ERR_SUBREC_DAMAGED       1421
#define ERR_SUBREC_OPEN          1422
#define ERR_SUBREC_UPDATE        1423
#define ERR_SUBREC_CREATE        1424
#define ERR_SUBREC_DELETE        1425
#define ERR_SUBREC_CLOSE         1426
#define ERR_TOPREC_UPDATE        1427
#define ERR_TOPREC_CREATE        1428

#define ERR_FILTER_BAD           1430
#define ERR_FILTER_NOT_FOUND     1431
#define ERR_KEY_BAD              1432
#define ERR_KEY_FIELD_NOT_FOUND  1433
#define ERR_INPUT_USER_MODULE_NOT_FOUND 1439
#define ERR_INPUT_CREATESPEC     1442
#define ERR_INPUT_TOO_LARGE      1443
#define ERR_NS_LDT_NOT_ENABLED   1500


typedef struct ldt_sub_gc_info_s {
	struct as_namespace_s	*ns;
	uint32_t				num_gc;
} ldt_sub_gc_info;


#define LDT_READ_OP		0
#define LDT_WRITE_OP	1

extern int		as_ldt_package_index(const char *package_name);
extern int		as_ldt_op_type(int package_index, const char *op_name);

extern bool     as_ldt_set_flag            (uint16_t flag);
extern bool     as_ldt_flag_has_parent     (uint16_t flag);
extern bool     as_ldt_flag_has_sub        (uint16_t flag);
extern bool     as_ldt_flag_has_subrec     (uint16_t flag);
extern bool     as_ldt_flag_has_esr        (uint16_t flag);

extern void     as_ldt_sub_gc_fn           (as_index_ref *r_ref, void *udata);
extern int      as_ldt_shipop              (rw_request *rw, cf_node dest_node);

extern int      as_ldt_parent_storage_set_version (as_storage_rd *rd, uint64_t, uint8_t *, char *fname, int lineno);
extern int      as_ldt_parent_storage_get_version (as_storage_rd *rd, uint64_t *, bool, char *fname, int lineno);
extern int      as_ldt_subrec_storage_get_digests (as_storage_rd *rd, cf_digest *edigest, cf_digest *pdigest);
extern void     as_ldt_subrec_storage_validate    (as_storage_rd *rd, char *op);

extern void     as_ldt_digest_randomizer           (cf_digest *dig);
extern bool     as_ldt_merge_component_is_candidate(struct as_partition_reservation_s *rsv, as_record_merge_component *c);

extern void     as_ldt_record_set_rectype_bits    (as_record *r, const as_rec_props *props);
extern int      as_ldt_record_pickle              (ldt_record *lrecord, uint8_t **pickled_buf, size_t *pickled_sz);
extern void     as_ldt_get_property               (as_rec_props *props, bool *is_ldt_parent, bool *is_ldt_sub);

// Version related functions
extern uint64_t as_ldt_generate_version();
extern void     as_ldt_subdigest_setversion   (cf_digest *dig, uint64_t version);
extern uint64_t as_ldt_subdigest_getversion   (cf_digest *dig);
extern void     as_ldt_subdigest_resetversion (cf_digest *dig);

/*
 * Returns true if passed in record an LDT Parent (top record).
 * NOTE: Record is expected to be properly initialized and locked and
 * partition reserved
 */
static inline bool
as_ldt_record_is_parent(as_record *r)
{
	return ((!r)
			? false
			: as_index_is_flag_set( r, AS_INDEX_FLAG_SPECIAL_BINS ));
}

static inline bool
as_ldt_record_is_sub(as_record *r)
{
	return ((!r)
			? false
			: (as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_ESR ) ||
			   as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_REC )));
}

// Return true if this record is ANY type of LDT record (parent, child, esr)
static inline bool
as_ldt_record_is_ldt(as_index *r)
{
	return ((!r)
			? false
			: (as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_ESR ) ||
			   as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_REC ) ||
			   as_index_is_flag_set( r, AS_INDEX_FLAG_SPECIAL_BINS )));
}

// Return true if this record is an LDT subrecord; Child or ESR subrec.
static inline bool
as_ldt_record_is_subrec(as_record *r)
{
	return ((!r)
			? false
			: (as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_REC )));
}

// Return true if this LDT subrecord is of type ESR (Existence Sub Record).
static inline bool
as_ldt_record_is_esr(as_record *r)
{
	return ((!r)
			? false
			: as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_ESR ));
}

/*
 * Create 16 bit property field for storing on disk
 * Notes:
 * (1) All this property is some way or the other also in index so that is
 * pattern used.
 * (2) Must not add anything which is not in index as record
 * property at this point!
 */
static inline uint16_t
as_ldt_record_get_rectype_bits(as_record *r)
{
	return (as_index_get_flags(r) & 
		(AS_INDEX_FLAG_CHILD_ESR | AS_INDEX_FLAG_CHILD_REC | AS_INDEX_FLAG_SPECIAL_BINS));
}

static inline int
as_ldt_bytes_todigest(as_bytes *bytes, cf_digest *keyd)
{
	if (as_bytes_size(bytes) < CF_DIGEST_KEY_SZ) {
		cf_warning(AS_LDT, "ldt_string_todigest Invalid digest size %d", as_bytes_size(bytes));
		return -1;
	}

	int i = 0;
	for(;;) {
		uint8_t val;
		as_bytes_get_byte(bytes, i, &val);
		//cf_detail(AS_UDF, "dig %d", val);
		keyd->digest[i++] = val;
		if (i == CF_DIGEST_KEY_SZ) break;
	}
	return 0;
}

// Note: If the string form bdig ever changes this function will break.
// TODO: change the whole thing into as_bytes.
static inline int
as_ldt_string_todigest(const char *bdig, cf_digest *keyd)
{
	if (strlen(bdig) < ((CF_DIGEST_KEY_SZ * 3) - 1)) {
		cf_warning(AS_LDT, "ldt_string_todigest Invalid digest %s:%zu", bdig, strlen(bdig));
		return -1;
	}
	// bdig looks like
	// "06 89 8C 53 4A 51 AC F7 70 29 8D 71 FE FF 00 00 00 00 00 00"
	//
	// start at 1 and at every byte shift three positions
	int j = 0;
	for (int i = 0; ; i++) {
		char val[3];
		val[0] = bdig[i++];
		val[1] = bdig[i++];
		val[2] = '\0';
		int intval = strtol(val, NULL, 16);
		cf_detail(AS_UDF, "dig %s->%d", val, intval);
		keyd->digest[j++] = intval;
		if (j == CF_DIGEST_KEY_SZ) break;
	}
	return 0;
}

as_val * as_llist_scan(struct as_namespace_s *ns, as_index_tree *sub_tree, as_storage_rd  *rd, as_bin *binp);

void ldt_update_err_stats(struct as_namespace_s *ns, as_val *val);
long ldt_get_error_code(void *val, size_t vlen); 
