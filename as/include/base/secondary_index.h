/*
 * secondary_index.h
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
 *  SYNOPSIS
 *  Abstraction to support secondary indexes with multiple implementations.
 */

#pragma once

#include "base/datamodel.h"
#include "base/monitor.h"
#include "base/proto.h"
#include "base/system_metadata.h"
#include "base/transaction.h"

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"

#include "dynbuf.h"
#include "hist.h"
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "storage/storage.h"


/*
 * HARD LIMIT ON SIZES
 */
// **************************************************************************************************
#define AS_SINDEX_MAX_STRING_KSIZE 2048
#define SINDEX_SMD_KEY_SIZE        AS_ID_INAME_SZ + AS_ID_NAMESPACE_SZ 
#define SINDEX_SMD_VALUE_SIZE      (AS_SMD_MAJORITY_CONSENSUS_KEYSIZE)
#define SINDEX_MODULE              "sindex_module"
#define AS_SINDEX_MAX_PATH_LENGTH  256
#define AS_SINDEX_MAX_DEPTH        10
#define AS_SINDEX_TYPE_STR_SIZE    20
#define AS_INDEX_KEYS_ARRAY_QUEUE_HIGHWATER  512
#define AS_INDEX_KEYS_PER_ARR      51
// **************************************************************************************************

/* 
 * Return status codes for index object functions.
 *
 * NB: When adding error code add the string in the as_sindex_err_str 
 * in secondary_index.c 
 *
 * Negative > 10 are the ones which show up and goes till client
 *
 * Positive are < 10 are something which are internal
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_ERR_MAXCOUNT         = -16,
	AS_SINDEX_ERR_SET_MISMATCH     = -15,
	AS_SINDEX_ERR_UNKNOWN_KEYTYPE  = -14,
	AS_SINDEX_ERR_BIN_NOTFOUND     = -13,
	AS_SINDEX_ERR_TYPE_MISMATCH    = -11,
	

	// Needed when attemping index create
	AS_SINDEX_ERR_FOUND            = -6,
	AS_SINDEX_ERR_NOTFOUND         = -5,
	AS_SINDEX_ERR_NO_MEMORY        = -4,
	AS_SINDEX_ERR_PARAM            = -3,
	AS_SINDEX_ERR_NOT_READABLE     = -2,
	AS_SINDEX_ERR                  = -1,
	AS_SINDEX_OK                   =  0,

	// Internal Not needed
	AS_SINDEX_CONTINUE             = 1,
	AS_SINDEX_DONE                 = 2,
	// Needed when inserting object in the btree.
	AS_SINDEX_KEY_FOUND            = 3,
	AS_SINDEX_KEY_NOTFOUND         = 4
} as_sindex_status;
// **************************************************************************************************

/*
 * SINDEX OP TYPES.
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_OP_UPDATE = 0,
	AS_SINDEX_OP_DELETE = 1,
	AS_SINDEX_OP_INSERT = 2,
	AS_SINDEX_OP_READ = 3
} as_sindex_op;
// **************************************************************************************************

/*
 * SINDEX GC RETURN ENUMS
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_GC_OK             = 0,
	AS_SINDEX_GC_ERROR          = 1,
	AS_SINDEX_GC_SKIP_ITERATION = 2
} as_sindex_gc_status;
// **************************************************************************************************

/*
 * SECONDARY INDEX KEY TYPES
 * NB: DO NOT CHANGE NUMBERING, it matches COL_TYPE* to track down weird 
 * behavior if it does not match.
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_KTYPE_NONE   = 0,
	AS_SINDEX_KTYPE_LONG   = 2, //Particle type INT
	AS_SINDEX_KTYPE_FLOAT  = 4, //Particle type INT
	AS_SINDEX_KTYPE_DIGEST = 10
} as_sindex_ktype;

typedef enum {
	AS_SINDEX_KEY_TYPE_LONG   = 0,
	AS_SINDEX_KEY_TYPE_DIGEST = 1,
	AS_SINDEX_KEY_TYPE_MAX    = 2
} as_sindex_key_type;
// **************************************************************************************************

/*
 * SINDEX TYPES.
 * THEY WOULD BE IN SYNC WITH THE CLIENTS.
 * Do not change the order of this enum
 */
// **************************************************************************************************
typedef enum {
	AS_SINDEX_ITYPE_DEFAULT   = 0,
	AS_SINDEX_ITYPE_LIST      = 1,
	AS_SINDEX_ITYPE_MAPKEYS   = 2,
	AS_SINDEX_ITYPE_MAPVALUES = 3,
	AS_SINDEX_ITYPE_MAX       = 4
} as_sindex_type;
// **************************************************************************************************

/* 
 * Configuration parameter and control variable for secondary indexes
 */
// **************************************************************************************************
#define AS_SINDEX_CONFIG_IGNORE_ON_DESYNC     0x01
// First byte
#define IMD_FLAG_NO_RANGE_QUERY      0x0001
// Fourth byte
#define IMD_FLAG_LOCKSET             0x0200
// **************************************************************************************************

/* 
 * STRUCTURES FROM ALCHEMY
 */
// **************************************************************************************************
struct btree;
// **************************************************************************************************

/*
 * STATS AND CONFIG STRUCTURE
 * Stats are collected about memory utilization based on simple index
 * overhead. Any insert delete from the secondary index would update
 * this number and the memory management folks has to use this info.
 */
// **************************************************************************************************
typedef struct as_sindex_stat_s {
	cf_atomic64        n_objects;
	int                n_keys;
	int                mem_used;

	cf_atomic64        n_reads;
	cf_atomic64        read_errs;

	cf_atomic64        n_writes;
	cf_atomic64        write_errs;
	histogram *        _write_hist;         // Histogram to track time spend writing to the sindex
	histogram *        _si_prep_hist;

	cf_atomic64        n_deletes;
	cf_atomic64        delete_errs;
	histogram *        _delete_hist;        // Histogram to track time spend deleting from sindex

	// Background thread stats
	cf_atomic64        loadtime;
	cf_atomic64        recs_pending;

	cf_atomic64        n_defrag_records;
	cf_atomic64        defrag_time;
	
	// Query Stats
	histogram *       _query_hist;            // Histogram to track query latency
	histogram *       _query_batch_lookup;    // Histogram to track latency of batch request from sindex tree.
	histogram *       _query_batch_io;        // Histogram to track time spend doing I/O per batch
	//	--aggregation stats
	cf_atomic64        n_aggregation;
	cf_atomic64        agg_response_size;
	cf_atomic64        agg_num_records;
	cf_atomic64        agg_errs;
	//	--lookup stats
	cf_atomic64        n_lookup;
	cf_atomic64        lookup_response_size;
	cf_atomic64        lookup_num_records;
	cf_atomic64        lookup_errs;

	histogram *       _query_rcnt_hist;       // Histogram to track record counts from queries
	histogram *       _query_diff_hist;       // Histogram to track the false positives found by queries
} as_sindex_stat;

typedef struct as_sindex_config_var_s {
	char 		name[AS_ID_INAME_SZ];
	uint64_t    defrag_period;
	uint32_t    defrag_max_units;
	uint64_t    data_max_memory;
	uint16_t    trace_flag;  // tracing flags
	bool        enable_histogram; // default false;
	uint16_t    ignore_not_sync_flag;
	bool 		conf_valid_flag;
}as_sindex_config_var;

typedef struct as_sindex_config_s {
	uint64_t    defrag_period;
	uint32_t    defrag_max_units;
	uint64_t    data_max_memory;
	uint16_t    flag;
} as_sindex_config;

// **************************************************************************************************


/*
 * SINDEX METADATAS
 */
// **************************************************************************************************
typedef struct as_sindex_physical_metadata_s {
	pthread_rwlock_t    slock;
	int                 tmatch;
	int                 imatch;  // Aerospike Index Number
	struct btree       *ibtr;    // Aerospike Index pointer
} as_sindex_pmetadata;


typedef struct as_sindex_path_s {
	as_particle_type type;  // MAP/LIST
	union {
		int      index;     // For index of lists.
		char   * key_str;   // For string type keys in maps.
		uint64_t key_int;   // For integer type keys in maps.
	} value;
	as_particle_type mapkey_type;  // This could be either string or integer type
} as_sindex_path;

typedef struct as_sindex_metadata_s {
	// Run Time Data
	pthread_rwlock_t      slock;
	int                   bimatch;
	int                   tmatch;  // Aerospike Index to table(tmatch)
	int                   nprts;   // Aerospike Index Number of Index partitions
	struct as_sindex_s  * si;
	as_sindex_pmetadata * pimd;
	unsigned char         dtype;   // Aerospike Index type
	uint32_t              binid[AS_SINDEX_BINMAX]; // Redundant info to aid search
	byte                  mfd_slot; // slot on the persistent file

	// Index Static Data (part persisted)
	char                * ns_name;
	char                * set;
	char                * iname;
	char                * bnames[AS_SINDEX_BINMAX];
	as_sindex_ktype       btype[AS_SINDEX_BINMAX]; // Same as Aerospike Index type
	as_sindex_type        itype;
	int                   num_bins;
	uint8_t               oindx;
	uint32_t              flag;
	int 				  post_op;
	as_sindex_path        path[AS_SINDEX_MAX_DEPTH];
	int                   path_length;
	char                * path_str;
} as_sindex_metadata;

/*
 * This structure right now hangs from the namespace structure for the
 * Aerospike Index B-tree.
 */
typedef struct as_sindex_s {
	int                          simatch; //self, shash match by name
	byte                         state;
	uint64_t                     flag;
	struct as_sindex_metadata_s *imd;
	struct as_sindex_metadata_s *new_imd;
	as_namespace                *ns;
	as_sindex_stat               stats;
	// clean-up needed for sindex-config
	as_sindex_config             config; // Secondary index configuration
	// memory management
	cf_atomic_int                data_memory_used;
	uint16_t                     trace_flag;  // tracing flags
	bool                         enable_histogram; // default false;
	cf_atomic_int                desync_cnt;
} as_sindex;

// Opaque type definition.
struct as_config_s;

// **************************************************************************************************
/*
 * SBINS STRUCTURES
 */
typedef struct sbin_value_pool_s{
	uint32_t used_sz;
	uint8_t  *value;
} sbin_value_pool;

#define AS_SINDEX_VALUESZ_ON_STACK 16 * 1000
#define SINDEX_BINS_SETUP(skey_bin, size)                      \
	sbin_value_pool value_pool;                                    \
	value_pool.value   = alloca(AS_SINDEX_VALUESZ_ON_STACK);       \
	value_pool.used_sz = 0;                    \
	as_sindex_bin skey_bin[(size)];                            \
	for (int id = 0; id < (size); id++) {         \
			skey_bin[id].si = NULL;         \
			skey_bin[id].stack_buf = &value_pool; \
	}

/*
 * Used as structure to call into secondary indexes sindex_* interface
 * TODO: as_sindex_bin is not appropriate name for this structure.
 * maybe as_sindex_transaction 
 */
typedef struct as_sindex_bin_s {
	union {                       // we use this if we need to store only one value inside sbin.
		int64_t       int_val;    // accessing this is much faster than accessing any other value
		cf_digest     str_val;    // value on the stack.
	} value;
	uint64_t          num_values; 
	void            * values;     // If there are more than 1 value in the sbin, we use this to
	as_particle_type  type;       // point to them. the type of data which is going to get indexed
	as_sindex_op      op;         // (STRING or INTEGER). Should we delete or insert this values
	bool              to_free;    // from/into the secondary index tree. If the values are malloced.
	as_sindex       * si;         // simatch of the si this bin is pointing to.
	sbin_value_pool * stack_buf;
	uint32_t          heap_capacity;
} as_sindex_bin;

// TODO: Reorganise this structure.
// No need of union.
typedef struct as_sindex_bin_data_s {
	uint32_t          id;
	as_particle_type  type; // this type is citrusleaf type
	// Union is to support sindex for other datatypes in future.
	// Currently sindex is supported for only int64 and string.
	union {
		int64_t  i64;
	} u;
	cf_digest         digest;
} as_sindex_bin_data;

// Caution: Using this will waste 12 bytes per long type skey 
typedef struct as_sindex_key_s {
	union {
		cf_digest str_key;
		uint64_t  int_key;
	} key;
} as_sindex_key;
// **************************************************************************************************


// **************************************************************************************************

/*
 * STRUCTUES FOR QUERY MODULE
 */
// **************************************************************************************************
struct ai_obj;
typedef struct as_sindex_query_context_s {
	uint64_t         bsize;
	cf_ll            *recl;
	uint64_t         n_bdigs;
		
	// Physical Tree offset
	bool             new_ibtr;		  // If new tree
	int              pimd_idx;

	// IBTR offset
	bool             nbtr_done;       // If nbtr was finished
								      // next iteration starts
							          // from key next to bkey
	struct ai_obj   *bkey;     	      // offset in ibtr

	// NBTR offset
	cf_digest        bdig;

	// If true all qnodes will be reserved before processing the query
	bool             qnodes_pre_reserved; 
	// Qnode map
	bool             is_partition_qnode[AS_PARTITIONS];
} as_sindex_qctx;

/*
 * The range structure used to define the lower and upper limit
 * along with the key types. 
 *
 *  [0, endl]
 *  [startl, -1(inf)]
 *  [startl, endl]
 */
typedef struct as_sindex_range_s {
	byte                num_binval;
	bool                isrange;
	as_sindex_bin_data  start;
	as_sindex_bin_data  end;
	as_sindex_type      itype;
	char                bin_path[AS_SINDEX_MAX_PATH_LENGTH];
} as_sindex_range;

/*
 * sindex_keys  are used by Secondary index queries to validate the keys against
 * the values of bins
 * ALl the jobs which runs over these queries also uses them
 * Like - Aggregation Query
 */
typedef struct as_index_keys_arr_s { 
	uint32_t      num;
	cf_digest     pindex_digs[AS_INDEX_KEYS_PER_ARR];	
	as_sindex_key sindex_keys[AS_INDEX_KEYS_PER_ARR];
} __attribute__ ((packed)) as_index_keys_arr;

typedef struct as_index_keys_ll_element_s {
	cf_ll_element       ele;
	as_index_keys_arr * keys_arr;
} as_index_keys_ll_element;


// **************************************************************************************************


// APIs exposed to other modules
// TODO return values is actually enum. 

/*
 * MODULE INIT AND SHUTDOWN
 */
// **************************************************************************************************

/* Index abstraction layer functions. */
/*
 * Initialize an instantiation of the index abstraction layer
 * using the array of index type-specific parameters passed in.
 *
 * All indexes created during this instantiation will use these type-specific
 * parameters (e.g., maximum data structure sizes, allocation policies, and any
 * other tuning parameters.)
 *
 * Call once before creating any type of index object.
 */
extern  int as_sindex_init(as_namespace *ns);

/*
 * Terminate an instantiation of the index abstraction layer.
 *
 * Do not use any "sindex" functions after calling this function, so free your indexes beforehand.
 */
extern void as_sindex_shutdown(as_namespace *ns);
extern int  as_sindex_reinit(char *name, char *params, cf_dyn_buf *db);
// **************************************************************************************************

/*
 * INDEX BOOT
 */
// **************************************************************************************************
extern int  as_sindex_populate_done(as_sindex *si);
extern int  as_sindex_boot_populateall_done(as_namespace *ns);
extern int  as_sindex_boot_populateall();
// **************************************************************************************************

/* 
 * DDL AND METADATA QUERY
 * 
*/
// **************************************************************************************************
extern int  as_sindex_create(as_namespace *ns, as_sindex_metadata *imd, bool user_create);
extern int  as_sindex_destroy(as_namespace *ns, as_sindex_metadata *imd);
extern int  as_sindex_update(as_sindex_metadata *imd);
extern void as_sindex_destroy_pmetadata(as_sindex *si);
// **************************************************************************************************


/*
 * CREATION AND UPDATION OF SINDEX BIN 
 */
// **************************************************************************************************
extern int  as_sindex_sbins_from_rd(as_storage_rd *rd, uint16_t from_bin, uint16_t to_bin, 
			as_sindex_bin sbins[], as_sindex_op op);
extern int  as_sindex_sbins_from_bin(as_namespace *ns, const char *set, as_bin *b, 
			as_sindex_bin * start_sbin, as_sindex_op op);
extern int  as_sindex_diff_sbins_from_buf(as_namespace * ns, const char * set, as_bin * b, 
			byte * buf, uint32_t buf_sz, as_particle_type type, as_sindex_bin * start_sbin);
extern int  as_sindex_sbins_from_buf(as_namespace *ns, const char *set, as_bin *b, 
			as_sindex_bin * start_sbin, byte * buf, uint32_t buf_sz, as_particle_type type, 
			as_sindex_op op);
extern int  as_sindex_update_by_sbin(as_namespace *ns, const char *set, as_sindex_bin *start_sbin, 
			int num_sbins, cf_digest * pkey);
// **************************************************************************************************

/*
 * DMLs USING RECORDS
 */
// **************************************************************************************************
extern int as_sindex_put_rd(as_sindex *si, as_storage_rd *rd);
extern int as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd);
// **************************************************************************************************


/* 
 * UTILS
 */
// **************************************************************************************************
extern int                  as_sindex_ns_has_sindex(as_namespace *ns);
extern const char         * as_sindex_err_str(int err_code);
extern int                  as_sindex_err_to_clienterr(int err, char *fname, int lineno);
extern bool                 as_sindex_isactive(as_sindex *si);
extern int                  as_sindex_get_err(int op_code, char *filename, int lineno);
extern uint64_t             as_sindex_get_ns_memory_used(as_namespace *ns);
extern as_sindex_status     as_sindex__delete_from_set_binid_hash(as_namespace * ns, 
							as_sindex_metadata * imd);
extern as_val             * as_sindex_extract_val_from_path(as_sindex_metadata * imd, as_val * v);
extern as_sindex_gc_status  as_sindex_can_defrag_record(as_namespace *ns, cf_digest *keyd);
extern as_sindex_status     as_sindex_extract_bin_path(as_sindex_metadata * imd, char * path_str);
int                         as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata* imd);
bool                        as_sindex_delete_checker(as_namespace *ns, as_sindex_metadata *imd);
as_particle_type            as_sindex_pktype(as_sindex_metadata * imd);
// **************************************************************************************************

/*
 * INFO AND CONFIGS
 */
// **************************************************************************************************
extern int  as_sindex_list_str(as_namespace *ns, cf_dyn_buf *db);
extern int  as_sindex_stats_str(as_namespace *ns, char * iname, cf_dyn_buf *db);
extern int  as_sindex_repair(as_namespace *ns, char * iname);
extern int  as_sindex_set_config(as_namespace *ns, as_sindex_metadata *imd, char *params);
extern void as_sindex_gconfig_default(struct as_config_s *c);
extern int  as_info_parse_params_to_sindex_imd(char* params, as_sindex_metadata *imd, cf_dyn_buf* db,
			bool is_create, bool *is_smd_op);
void        as_sindex_config_var_default(as_sindex_config_var *si_cfg);
int         as_sindex_cfg_var_hash_reduce_fn(void *key, void *data, void *udata);
void        as_sindex__config_default(as_sindex *si);
void        as_sindex_ticker_start(as_namespace * ns, as_sindex * si);
void        as_sindex_ticker(as_namespace * ns, as_sindex * si, uint64_t n_obj_scanned, uint64_t start_time);
void        as_sindex_ticker_done(as_namespace * ns, as_sindex * si, uint64_t start_time);
// **************************************************************************************************

/*
 * HISTOGRAMS
 */
// **************************************************************************************************
extern int as_sindex_histogram_enable(as_namespace *ns, char * iname, bool enable);
extern int as_sindex_histogram_dumpall(as_namespace *ns);
#define SINDEX_HIST_INSERT_DATA_POINT(si, type, start_time_ns)                          \
do {                                                                                    \
	if (si->enable_histogram && start_time_ns != 0) {                                   \
		if (si->stats._ ##type) {                                                       \
			histogram_insert_data_point(si->stats._ ##type, start_time_ns);             \
		}                                                                               \
	}                                                                                   \
} while(0);

#define SINDEX_HIST_INSERT_RAW(si, type, value)                                         \
do {                                                                                    \
	if (si->enable_histogram) {                                                         \
		if (si->stats._ ##type) {                                                       \
			histogram_insert_raw(si->stats._ ##type, value);                            \
		}                                                                               \
	}                                                                                   \
} while(0);


// **************************************************************************************************

/* 
 * UTILS FOR QUERIES
*/
// **************************************************************************************************
extern int         as_sindex_query(as_sindex *si, as_sindex_range *range, as_sindex_qctx *qctx);
extern int         as_sindex_range_free(as_sindex_range **srange);
extern int         as_sindex_rangep_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range **srange);
extern int         as_sindex_range_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range *srange);
extern int         as_sindex_assert_query(as_sindex *si, as_sindex_range *srange);
extern as_sindex * as_sindex_from_msg(as_namespace *ns, as_msg *msgp); 
extern as_sindex * as_sindex_from_range(as_namespace *ns, char *set, as_sindex_range *srange);
extern bool        as_sindex_partition_isqnode(as_namespace *ns, cf_digest *digest);
extern int         as_index_keys_reduce_fn(cf_ll_element *ele, void *udata);
extern void        as_index_keys_destroy_fn(cf_ll_element *ele);
// **************************************************************************************************


/*
 * RESERVE, RELEASE AND FREE
 */
// **************************************************************************************************
#define AS_SINDEX_RESERVE(si) \
	as_sindex_reserve((si), __FILE__, __LINE__);
#define AS_SINDEX_RELEASE(si) \
	as_sindex_release((si), __FILE__, __LINE__);
extern int  as_sindex_reserve(as_sindex *si, char *fname, int lineno);
extern int  as_sindex_release(as_sindex *si, char *fname, int lineno);
extern int  as_sindex_imd_free(as_sindex_metadata *imd);
extern int  as_sindex_sbin_free(as_sindex_bin *sbin);
extern int  as_sindex_sbin_freeall(as_sindex_bin *sbin, int numval);
extern bool as_sindex_reserve_data_memory(as_sindex_metadata *imd, uint64_t bytes);
extern bool as_sindex_release_data_memory(as_sindex_metadata *imd, uint64_t bytes);
// **************************************************************************************************

/*
 * SINDEX LOCKS
 */
// **************************************************************************************************
extern pthread_rwlock_t g_sindex_rwlock;
#define SINDEX_GRLOCK()         \
do { \
	int ret = pthread_rwlock_rdlock(&g_sindex_rwlock); \
	if (ret) cf_warning(AS_SINDEX, "GRLOCK(%d) %s:%d",ret, __FILE__, __LINE__); \
} while (0);

#define SINDEX_GWLOCK()         \
do { \
	int ret = pthread_rwlock_wrlock(&g_sindex_rwlock); \
	if (ret) cf_warning(AS_SINDEX, "GWLOCK(%d) %s:%d", ret, __FILE__, __LINE__); \
} while (0);

#define SINDEX_GUNLOCK()        \
do { \
	int ret = pthread_rwlock_unlock(&g_sindex_rwlock); \
	if (ret) cf_warning(AS_SINDEX, "GUNLOCK (%d) %s:%d",ret,  __FILE__, __LINE__); \
} while (0);

#define SINDEX_RLOCK(l)          \
do {                                            \
	int ret = pthread_rwlock_rdlock((l));        \
	if (ret) cf_warning(AS_SINDEX, "RLOCK_ONLY (%d) %s:%d", ret, __FILE__, __LINE__); \
} while(0);

#define SINDEX_WLOCK(l)                       \
do {                                            \
	int ret = pthread_rwlock_wrlock((l));        \
	if (ret) cf_warning(AS_SINDEX, "WLOCK_ONLY (%d) %s:%d",ret, __FILE__, __LINE__); \
} while(0);
#define SINDEX_UNLOCK(l)                      \
do {                                            \
	int ret = pthread_rwlock_unlock((l));        \
	if (ret) cf_warning(AS_SINDEX, "UNLOCK_ONLY (%d) %s:%d",ret, __FILE__, __LINE__); \
} while(0);
// **************************************************************************************************


/*
 * INLINES
 *
 * Hash function that takes a sindex-name and returns a uint64_t hash, 
 * meant for hashing name to a as_sindex_config_var structure. 
 */
// **************************************************************************************************
static inline uint32_t
as_sindex_config_var_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, strlen((const char *)p_key));
}
// **************************************************************************************************


/*
 * APIs for SMD
 */
// **************************************************************************************************
extern bool g_sindex_smd_restored;
extern int  as_sindex_smd_can_accept_cb(char* module, as_smd_item_t *item, void *udata);
extern int  as_sindex_smd_accept_cb(char *module, as_smd_item_list_t *items, void *udata, 
						uint32_t accept_opt);
// **************************************************************************************************

/*
 * QUERY MACROS
 */
// **************************************************************************************************
#define AS_QUERY_OK        AS_SINDEX_OK
#define AS_QUERY_ERR       AS_SINDEX_ERR
#define AS_QUERY_CONTINUE  AS_SINDEX_CONTINUE
#define AS_QUERY_DONE      AS_SINDEX_DONE
// **************************************************************************************************

/*
 * QUERY APIs exposed to other modules
 */
// **************************************************************************************************
extern void                 as_query_init();
extern int                  as_query(as_transaction *tr);
extern int                  as_query_reinit(int set_size, int *actual_size);
extern int                  as_query_worker_reinit(int set_size, int *actual_size);
extern int                  as_query_stat(char *name, cf_dyn_buf *db);
extern int                  as_query_list(char *name, cf_dyn_buf *db);
extern int                  as_query_kill(uint64_t trid);
extern void                 as_query_gconfig_default(struct as_config_s *c);
extern as_mon_jobstat     * as_query_get_jobstat(uint64_t trid);
extern as_mon_jobstat     * as_query_get_jobstat_all(int * size);
extern int                  as_query_set_priority(uint64_t trid, uint32_t priority);
extern void                 as_query_histogram_dumpall();
extern as_index_keys_arr  * as_index_get_keys_arr();
extern void                 as_index_keys_release_arr_to_queue(as_index_keys_arr *v);
extern int                  as_index_keys_ll_reduce_fn(cf_ll_element *ele, void *udata);
extern void                 as_index_keys_ll_destroy_fn(cf_ll_element *ele);
// **************************************************************************************************



