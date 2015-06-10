/*
 * datamodel.h
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
 * core data model structures and definitions
 */

#pragma once

#include "base/feature.h" // turn new AS Features on/off (must be first in line)

#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_shash.h>

#include "arenax.h"
#include "dynbuf.h"
#include "hist.h"
#include "util.h"
#include "vmapx.h"

#include "base/proto.h"
#include "base/rec_props.h"
#include "base/transaction_policy.h"


/* AS_CLUSTER_SZ, AS_CLUSTER_SZ_MASK[P,N]
 * The instantaneous maximum number of cluster participants, represented as
 * an integer and as a positive and negative mask */
#define AS_CLUSTER_SZ 128
#define AS_CLUSTER_SZ_MASKP ((uint64_t)(1 - (AS_CLUSTER_SZ + 1)))
#define AS_CLUSTER_SZ_MASKN ((uint64_t)(AS_CLUSTER_SZ - 1))
#define UNUSED 	-1

#define SINDEX 1

/*
 * AS_CLUSTER_LEGACY_SZ:  Historical hard-code maximum cluster size.
 * [Note:  This was the value of AS_CLUSTER_SZ in previous releases.]
 */
#define AS_CLUSTER_LEGACY_SZ 32

/*
 * AS_CLUSTER_DEFAULT_SZ:  Default maximum cluster size if not specified in the configuration.
 * [Note:  The legacy size is used for backward-compatibility with previous releases.]
 */
#define AS_CLUSTER_DEFAULT_SZ (AS_CLUSTER_LEGACY_SZ)

/* AS_NAMESPACE_SZ
 * The maximum number of namespaces that can exist at any one moment
 */
#define AS_NAMESPACE_SZ 32

#define AS_STORAGE_MAX_DEVICES 32 // maximum devices per namespace
#define AS_STORAGE_MAX_FILES 32 // maximum files per namespace
#define AS_STORAGE_MAX_DEVICE_SIZE (2L * 1024L * 1024L * 1024L * 1024L) // 2Tb, due to rblock_id in as_index

#define OBJ_SIZE_HIST_NUM_BUCKETS 100
#define EVICTION_HIST_NUM_BUCKETS 100

/*
 * Subrecord Digest Scramble Position
 */
// [0-1] For Partitionid
// [2-3] For the Lock
// [4-6] Scrambled bytes
#define DIGEST_SCRAMBLE_BYTE1       4
#define DIGEST_SCRAMBLE_BYTE2       5
#define DIGEST_SCRAMBLE_BYTE3       6
// [8]   SSD device hash
//       DO NOT CHANGE THIS 2.0 STORAGE uses it
//       Needed for backward compatibility
#define DIGEST_STORAGE_BYTE			8

// [7] [9-13]  // 6 byte clock
#define DIGEST_CLOCK_ZERO_BYTE      7
#define DIGEST_CLOCK_START_BYTE     9 // upto 13  

// [14-19]  // 6 byte version
#define DIGEST_VERSION_START_POS   14 // upto 19
// Define the size of the Version Info that we'll write into the LDT control Map
#define LDT_VERSION_SIZE  6

/* SYNOPSIS
 * Data model
 *
 * Objects are stored in a hierarchy: namespace:record:bin:particle.
 * The records in a namespace are further partitioned for distribution
 * amongst the participating nodes in the cluster.
 */



/* Forward declarations */
typedef struct as_namespace_s as_namespace;
typedef struct as_partition_s as_partition;
typedef struct as_partition_vinfo_s as_partition_vinfo;
typedef struct as_partition_reservation_s as_partition_reservation;
typedef struct as_index_s as_record;
typedef struct as_bin_s as_bin;
typedef struct as_index_ref_s as_index_ref;
typedef struct as_set_s as_set;
typedef struct as_treex_s as_treex;

struct as_index_tree_s;


// TODO - We have a #include loop - datamodel.h and storage.h include each
// other. I'd love to untangle this mess, but can't right now. So this needs to
// be here to allow compilation for now:
#include "storage/storage.h"


/* AS_ID_[NAMESPACE,SET,BIN,INAME]_SZ
 * The maximum length, in bytes, of an identification field; by convention,
 * these values are null-terminated UTF-8 */
#define AS_ID_NAMESPACE_SZ 32
#define AS_ID_BIN_SZ 15 // size used in storage format
#define AS_ID_INAME_SZ 256
#define BIN_NAME_MAX_SZ ((AS_ID_BIN_SZ + 3) & ~3) // round up to multiple of 4
#define MAX_BIN_NAMES 0x10000 // no need for more - numeric ID is 16 bits
#define BIN_NAMES_QUOTA (MAX_BIN_NAMES / 2) // don't add more names than this via client transactions


// now dynamic
// #define AS_OBJECT_INDEX_OVERHEAD_BYTES 80
// #define AS_OBJECT_INDEX_OVERHEAD_BYTES 230

/* as_generation
 * A generation ID */
typedef uint32_t as_generation;


/* as_particle_type
 * Particles are typed, which reflects their contents:
 *    NULL: no associated content (not sure I really need this internally?)
 *    INTEGER: a signed, 64-bit integer
 *    FLOAT: a floating point
 *    STRING: a null-terminated UTF-8 string
 *    BLOB: arbitrary-length binary data
 *    TIMESTAMP: milliseconds since 1 January 1970, 00:00:00 GMT
 *    DIGEST: an internal Aerospike key digest */
typedef enum {
	AS_PARTICLE_TYPE_NULL = 0,
	AS_PARTICLE_TYPE_INTEGER = 1,
	AS_PARTICLE_TYPE_FLOAT = 2,
	AS_PARTICLE_TYPE_STRING = 3,
	AS_PARTICLE_TYPE_BLOB = 4,
	AS_PARTICLE_TYPE_TIMESTAMP = 5,
	AS_PARTICLE_TYPE_UNUSED_6 = 6,
	AS_PARTICLE_TYPE_JAVA_BLOB = 7,
	AS_PARTICLE_TYPE_CSHARP_BLOB = 8,
	AS_PARTICLE_TYPE_PYTHON_BLOB = 9,
	AS_PARTICLE_TYPE_RUBY_BLOB = 10,
	AS_PARTICLE_TYPE_PHP_BLOB = 11,
	AS_PARTICLE_TYPE_ERLANG_BLOB = 12,
	AS_PARTICLE_TYPE_MAP = 19,
	AS_PARTICLE_TYPE_LIST = 20,
	AS_PARTICLE_TYPE_HIDDEN_LIST = 21,
	AS_PARTICLE_TYPE_HIDDEN_MAP = 22, // hidden map/list - can only be manipulated by system UDF
	AS_PARTICLE_TYPE_MAX = 23
} as_particle_type;

/* as_particle
 * The common part of a particle
 * this is poor man's subclassing - IE, how to do a subclassed interface in C
 * Go look in particle.c to see all the subclass implementation and structure */
typedef struct as_particle_s {
	uint8_t		metadata;		// used by the iparticle for is_integer and inuse, as well as version in multi bin mode only
								// used by *particle for type
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle;

// Bit Flag constants used for the particle state value (2 bits, 4 values)
#define AS_BIN_STATE_UNUSED			0
#define AS_BIN_STATE_INUSE_INTEGER	1
#define AS_BIN_STATE_INUSE_HIDDEN	2 // Denotes a server-side, hidden bin
#define AS_BIN_STATE_INUSE_OTHER	3

typedef struct as_particle_iparticle_s {
	uint8_t		version: 4;		// can only be used in multi bin
	uint8_t		unused: 2;		// can only be used in multi bin
	uint8_t		state: 2;		// IF 0: unused, IF 1: integer, IF 2: HIDDEN bin, IF 3: inuse, other bin type
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle_iparticle;

/* Particle function declarations */

static inline bool
is_embedded_particle_type(as_particle_type type)
{
	return type == AS_PARTICLE_TYPE_INTEGER || type == AS_PARTICLE_TYPE_FLOAT;
}

extern int32_t as_particle_size_from_client(const as_msg_op *op); // TODO - will we ever need this?
extern int32_t as_particle_size_from_pickled(uint8_t **p_pickled);
extern uint32_t as_particle_size_from_mem(as_particle_type type, const uint8_t *value, uint32_t value_size);
extern int32_t as_particle_size_from_flat(const uint8_t *flat, uint32_t flat_size); // TODO - will we ever need this?

extern as_particle_type as_particle_type_convert(as_particle_type type);
extern as_particle_type as_particle_type_convert_to_hidden(as_particle_type type);
extern bool as_particle_type_hidden(as_particle_type type);

// as_bin particle function declarations

extern void as_bin_particle_destroy(as_bin *b, bool free_particle);
extern uint32_t as_bin_particle_size(as_bin *b);
extern uint32_t as_bin_particle_ptr(as_bin *b, uint8_t **p_value);

// wire:
extern int32_t as_bin_particle_size_modify_from_client(as_bin *b, const as_msg_op *op); // TODO - will we ever need this?
extern int as_bin_particle_alloc_modify_from_client(as_bin *b, const as_msg_op *op);
extern int as_bin_particle_stack_modify_from_client(as_bin *b, cf_dyn_buf *particles_db, const as_msg_op *op);
extern int as_bin_particle_alloc_from_client(as_bin *b, const as_msg_op *op);
extern int as_bin_particle_stack_from_client(as_bin *b, cf_dyn_buf *particles_db, const as_msg_op *op);
extern int as_bin_particle_replace_from_pickled(as_bin *b, uint8_t **p_pickled);
extern int32_t as_bin_particle_stack_from_pickled(as_bin *b, uint8_t* stack, uint8_t **p_pickled);
extern int as_bin_particle_compare_from_pickled(const as_bin *b, uint8_t **p_pickled);
extern uint32_t as_bin_particle_client_value_size(as_bin *b);
extern uint32_t as_bin_particle_to_client(const as_bin *b, as_msg_op *op);
extern uint32_t as_bin_particle_pickled_size(as_bin *b);
extern uint32_t as_bin_particle_to_pickled(const as_bin *b, uint8_t *pickled);

// Different for LDTs - an LDT's as_list is expensive to generate, so we return
// it from the sizing method, and cache it for later use by the packing method:
extern uint32_t as_ldt_particle_client_value_size(as_storage_rd *rd, as_bin *b, as_val **p_val);
extern uint32_t as_ldt_particle_to_client(const as_val *val, as_msg_op *op);

// mem: TODO - replace with as_val family.
extern int as_bin_particle_replace_from_mem(as_bin *b, as_particle_type type, const uint8_t *value, uint32_t value_size);
extern uint32_t as_bin_particle_stack_from_mem(as_bin *b, uint8_t* stack, as_particle_type type, const uint8_t *value, uint32_t value_size);
extern uint32_t as_bin_particle_mem_size(as_bin *b);
extern uint32_t as_bin_particle_to_mem(const as_bin *b, uint8_t *value);

// flat:
extern int as_bin_particle_cast_from_flat(as_bin *b, uint8_t *flat, uint32_t flat_size);
extern int as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, uint32_t flat_size);
extern uint32_t as_bin_particle_flat_size(as_bin *b);
extern uint32_t as_bin_particle_to_flat(const as_bin *b, uint8_t *flat);


#define BIN_VERSION_MAX 15 // the largest number we can place in the version

/* as_bin
 * A bin container - null name means unused */
struct as_bin_s {
	as_particle iparticle;			// DO NOT USE THE TYPE FROM THIS STRUCTURE! THIS WILL OVERWRITE DATA IN THE SINGLE BIN CASE!
									// Only use the is_integer and inuse fields.
	union {
		uint64_t ivalue;			// this field should be never used directly. always use the pointer to the iparticle;
		as_particle *particle;
	};
	/*
	 *  The above is used as an as_particle_int subtype embedded inside the bin
	 *  The length to which we go to save bytes !
	 */
	uint16_t	id;			// ID of bin name (bytes 10 and 11 of this struct)
	uint8_t		unused;		// pad to 12 bytes (multiple of 4) for thread safety
} __attribute__ ((__packed__)) ;

// For data-in-memory namespaces in multi-bin mode, we keep an array of as_bin
// structs in memory, accessed via this struct.
typedef struct as_bin_space_s {
	uint16_t	n_bins;
	as_bin		bins[];
} __attribute__ ((__packed__)) as_bin_space;

// TODO - Do we really need to pad as_bin to 12 bytes for thread safety?
// Do we ever write & read adjacent as_bin structures in a bins array from
// different threads when not under the record lock? And if we're worried about
// 4-byte alignment for that or any other reason, wouldn't we also have to pad
// after n_bins in as_bin_space?

// For data-in-memory namespaces in multi-bin mode, if we're storing extra
// record metadata, we access it via this struct. In this case the index points
// here instead of directly to an as_bin_space.
typedef struct as_rec_space_s {
	as_bin_space*	bin_space;

	// So far the key is the only extra record metadata we store in memory.
	uint32_t		key_size;
	uint8_t			key[];
} __attribute__ ((__packed__)) as_rec_space;

static inline bool
as_bin_inuse(const as_bin *b)
{
	return (((as_particle_iparticle *)b)->state);
}

static inline uint8_t
as_bin_state(const as_bin *b)
{
	return ((as_particle_iparticle *)b)->state;
}

static inline void
as_bin_state_set(as_bin *b, uint8_t val)
{
	((as_particle_iparticle *)b)->state = val;
	((as_particle_iparticle *)b)->unused = 0;
}

static inline void
as_bin_state_set_from_type(as_bin *b, as_particle_type type)
{
	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_UNUSED;
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_INTEGER;
		break;
	case AS_PARTICLE_TYPE_FLOAT:
	case AS_PARTICLE_TYPE_TIMESTAMP:
		// TODO - unsupported
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_UNUSED;
		break;
	case AS_PARTICLE_TYPE_HIDDEN_LIST:
	case AS_PARTICLE_TYPE_HIDDEN_MAP:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_HIDDEN;
		break;
	default:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_OTHER;
		break;
	}

	((as_particle_iparticle *)b)->unused = 0;
}

static inline bool
as_bin_inuse_has(as_storage_rd *rd)
{
	// In-use bins are at the beginning - only need to check the first bin.
	return (rd->n_bins && as_bin_inuse(rd->bins));
}

static inline void
as_bin_set_empty(as_bin *b)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
}

static inline void
as_bin_set_empty_shift(as_storage_rd *rd, uint32_t i)
{
	// Shift the bins over, so there's no space between used bins.
	// This can overwrite the "emptied" bin, and that's fine.

	uint16_t j;

	for (j = i + 1; j < rd->n_bins; j++) {
		if (! as_bin_inuse(&rd->bins[j])) {
			break;
		}
	}

	uint16_t n = j - (i + 1);

	if (n) {
		memmove(&rd->bins[i], &rd->bins[i + 1], n * sizeof(as_bin));
	}

	// Mark the last bin that was *formerly* in use as null.
	as_bin_set_empty(&rd->bins[j - 1]);
}

static inline void
as_bin_set_empty_from(as_storage_rd *rd, uint16_t from) {
	for (uint16_t i = from; i < rd->n_bins; i++) {
		as_bin_set_empty(&rd->bins[i]);
	}
}

static inline void
as_bin_set_all_empty(as_storage_rd *rd) {
	as_bin_set_empty_from(rd, 0);
}

static inline bool
as_bin_is_embedded_particle(const as_bin *b) {
	return ((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_INTEGER;
}

static inline as_particle *
as_bin_get_particle(as_bin *b) {
	return as_bin_is_embedded_particle(b) ? &b->iparticle : b->particle;
}

/**
 * Quick test to show if this bin is one of the HIDDEN bins.
 */
static inline bool
as_bin_is_hidden(const as_bin *b) {
	return  (((as_particle_iparticle *)b)->state) == AS_BIN_STATE_INUSE_HIDDEN;
}

/*
 * Return the type of the particle.  Integers are stored directly, but the other
 * bin types ("other" or "hidden") must follow an indirection to get the
 * actual type.
 */
static inline uint8_t
as_bin_get_particle_type(const as_bin *b) {
	switch (((as_particle_iparticle *)b)->state) {
		case AS_BIN_STATE_INUSE_INTEGER:
			return (AS_PARTICLE_TYPE_INTEGER);
		case AS_BIN_STATE_INUSE_OTHER:
			return (b->particle->metadata);
		case AS_BIN_STATE_INUSE_HIDDEN:
			return (b->particle->metadata);
		default:
			return (AS_PARTICLE_TYPE_NULL);
	}
}

static inline uint8_t
as_bin_get_version(const as_bin *b, bool single_bin) {
	return (single_bin ? 0 : ((as_particle_iparticle *)b)->version);
}

static inline void
as_bin_set_version(as_bin *b, uint8_t version, bool single_bin) {
	if (! single_bin) {
		((as_particle_iparticle *)b)->version = version;
	}
}

/* AS_INITIAL_BINS_PER_RECORD
 * How many bin slots to preallocate when we instantiate a new record */
#define AS_INITIAL_BINS_PER_RECORD 1

/* Bin function declarations */
extern int16_t as_bin_get_id(as_namespace *ns, const char *name);
extern uint16_t as_bin_get_or_assign_id(as_namespace *ns, const char *name);
extern const char* as_bin_get_name_from_id(as_namespace *ns, uint16_t id);
extern bool as_bin_name_within_quota(as_namespace *ns, byte *buf, size_t len);
extern uint16_t as_bin_get_n_bins(as_record *r, as_storage_rd *rd);
extern as_bin *as_bin_get_all(as_record *r, as_storage_rd *rd, as_bin *stack_bins);
extern int as_storage_rd_load_bins(as_storage_rd *rd, as_bin *stack_bins);
extern void as_bin_get_all_p(as_storage_rd *rd, as_bin **bin_ptrs);
extern as_bin *as_bin_create(as_record *r, as_storage_rd *rd, uint8_t *name, size_t namesz, uint version);
extern as_bin *as_bin_get(as_storage_rd *rd, uint8_t *name, size_t namesz);
extern int32_t as_bin_get_index(as_storage_rd *rd, uint8_t *name, size_t namesz);
extern int as_bin_get_all_versions(as_storage_rd *rd, uint8_t *name, size_t namesz, as_bin **curr_bins);
extern void as_bin_allocate_bin_space(as_record *r, as_storage_rd *rd, int32_t delta);
extern void as_bin_destroy(as_storage_rd *rd, uint16_t i);
extern void as_bin_destroy_from(as_storage_rd *rd, uint16_t i);
extern void as_bin_destroy_all(as_storage_rd *rd);
extern uint16_t as_bin_inuse_count(as_storage_rd *rd);
extern void as_bin_all_dump(as_storage_rd *rd, char *msg);

extern void as_bin_init(as_namespace *ns, as_bin *b, byte *name, size_t namesz, uint version);

#define AS_PARTITION_MAX_VERSION 16

/* as_partition_vinfo
 * A partition's version information */
struct as_partition_vinfo_s {
	uint64_t iid;								// iid is the identifier of the cluster at the time the partition was created
	uint8_t vtp[AS_PARTITION_MAX_VERSION];      // vtp is the version string of the partition with the cluster's split-reforms
};


#define AS_PARTITION_VINFOSET_SIZE 32

typedef struct as_partition_vinfoset_s {
	uint				sz;
	as_partition_vinfo 	vinfo_a[AS_PARTITION_VINFOSET_SIZE];
} as_partition_vinfoset;

typedef uint32_t as_partition_vinfo_mask;

// vinfo related calls
#define AS_PARTITION_VINFOSET_PICKLE_MAX ( 4 + ( AS_PARTITION_VINFOSET_SIZE * ( sizeof(as_partition_vinfo) + 1 ) ) )

extern int as_partition_vinfoset_mask_pickle( as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask, uint8_t *buf, size_t *sz_r);
extern int as_partition_vinfoset_mask_pickle_getsz( as_partition_vinfo_mask mask, size_t *sz_r);
extern int as_partition_vinfoset_pickle( as_partition_vinfoset *vinfoset, uint8_t *buf, size_t *sz_r);
extern as_partition_vinfo_mask as_partition_vinfoset_mask_unpickle( as_partition *p, uint8_t *buf, size_t buf_sz);
extern int as_partition_vinfoset_unpickle( as_partition_vinfoset *vinfoset, uint8_t *buf, size_t buf_sz, char *msg);
extern bool as_partition_vinfo_contains(as_partition_vinfo *v1, as_partition_vinfo *v2);
extern bool as_partition_vinfoset_contains_vinfoset(as_partition_vinfoset *vs1, as_partition_vinfo_mask mask1, as_partition_vinfoset *vs2, as_partition_vinfo_mask mask2, bool debug );
extern bool as_partition_vinfoset_superset_vinfoset(as_partition_vinfoset *vs1, as_partition_vinfo_mask mask1, as_partition_vinfoset *vs2);
extern void as_partition_vinfo_dump(as_partition_vinfo *vinfo, char *msg);
extern void as_partition_vinfoset_dump(as_partition_vinfoset *vinfoset, char *msg);
extern void as_partition_vinfoset_mask_dump(as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask, char *msg);

static inline bool
as_partition_vinfo_same(as_partition_vinfo *v1, as_partition_vinfo *v2) {
	if (v1->iid != v2->iid)		return (false);
	if ( 0 != memcmp( v1->vtp, v2->vtp, AS_PARTITION_MAX_VERSION ) ) return (false);
	return (true);
}

static inline bool
as_partition_vinfo_different(as_partition_vinfo *v1, as_partition_vinfo *v2) {
	if (v1->iid != v2->iid)	return (true);
	if ( 0 != memcmp( v1->vtp, v2->vtp, AS_PARTITION_MAX_VERSION ) ) return (true);
	return (false);
}


/* Record function declarations */
// special - get_create returns 1 if created, 0 if just gotten, -1 if fail
extern int as_record_get_create(struct as_index_tree_s *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns, bool);
extern int as_record_get(struct as_index_tree_s *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns);
extern int as_record_exists(struct as_index_tree_s *tree, cf_digest *keyd, as_namespace *ns);
// initialize as_record
extern void as_record_initialize(as_index_ref *r_ref, as_namespace *ns);

extern void as_record_clean_bins_from(as_storage_rd *rd, uint16_t from);
extern void as_record_clean_bins(as_storage_rd *rd);

extern void as_record_destroy(as_record *r, as_namespace *ns);
extern void as_record_done(as_index_ref *r_ref, as_namespace *ns);

extern void as_record_allocate_key(as_record* r, const uint8_t* key, uint32_t key_size);
extern void as_record_remove_key(as_record* r);

extern int as_record_pickle(as_record *r, as_storage_rd *rd, uint8_t **buf_r, size_t *len_r);
extern int as_record_pickle_a_delete(byte **buf_r, size_t *len_r);
extern uint32_t as_record_buf_get_stack_particles_sz(uint8_t *buf);
extern int as_record_unpickle_replace(as_record *r, as_storage_rd *rd, uint8_t *buf, size_t bufsz, uint8_t **stack_particles, bool has_sindex);
extern int as_record_unpickle_merge(as_record *r, as_storage_rd *rd, uint8_t *buf, size_t bufsz, uint8_t **stack_particles, bool *record_written);
extern int as_record_unused_version_get(as_storage_rd *rd);
extern void as_record_apply_properties(as_record *r, as_namespace *ns, const as_rec_props *p_rec_props);
extern void as_record_clear_properties(as_record *r, as_namespace *ns);
extern void as_record_set_properties(as_storage_rd *rd, const as_rec_props *rec_props);
extern int as_record_set_set_from_msg(as_record *r, as_namespace *ns, as_msg *m);

// Set in component if it is dummy (no data). This in
// conjunction with LDT_REC is used to determine if merge
// can be done or not. If this flag is not set then it is
// normal record
#define AS_COMPONENT_FLAG_LDT_DUMMY       0x01
#define AS_COMPONENT_FLAG_LDT_REC         0x02
#define AS_COMPONENT_FLAG_LDT_SUBREC   	  0x04
#define AS_COMPONENT_FLAG_LDT_ESR         0x08
#define AS_COMPONENT_FLAG_MIG             0x10
#define AS_COMPONENT_FLAG_DUP             0x20
#define AS_COMPONENT_FLAG_UNUSED3         0x40
#define AS_COMPONENT_FLAG_UNUSED4         0x80

#define COMPONENT_IS_MIG(c) \
	((c)->flag & AS_COMPONENT_FLAG_MIG)

#define COMPONENT_IS_DUP(c) \
	((c)->flag & AS_COMPONENT_FLAG_DUP)

#define COMPONENT_IS_LDT_PARENT(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_REC)

#define COMPONENT_IS_LDT_DUMMY(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_DUMMY)

#define COMPONENT_IS_LDT_SUBREC(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_SUBREC)

#define COMPONENT_IS_LDT_ESR(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_ESR)

#define COMPONENT_IS_LDT_SUB(c) \
	(((c)->flag & AS_COMPONENT_FLAG_LDT_ESR)        \
		|| ((c)->flag & AS_COMPONENT_FLAG_LDT_SUBREC))

#define COMPONENT_IS_LDT(c) \
	COMPONENT_IS_LDT_PARENT((c)) \
		|| COMPONENT_IS_LDT_SUB((c))

typedef struct {
	as_partition_vinfoset   vinfoset; // entire description of versions
	uint8_t					*record_buf;
	size_t					record_buf_sz;
	uint32_t				generation;
	uint32_t				void_time;
	as_rec_props			rec_props;
	char					flag;
	cf_digest               pdigest;
	cf_digest               edigest;
	uint32_t                pgeneration;
	uint32_t                pvoid_time;
	uint64_t                version;
} as_record_merge_component;

extern int as_record_merge(as_partition_reservation *rsv, cf_digest *keyd,
		uint16_t n_components, as_record_merge_component *components);


extern int as_record_flatten(as_partition_reservation *rsv, cf_digest *keyd,
		uint16_t n_components, as_record_merge_component *components, int *winner_idx);

// this function can be called with only one component, the one to replace the record
extern int as_record_replace(as_partition_reservation *rsv, cf_digest *keyd,
		uint16_t n_components, as_record_merge_component *components);


// vinfo routines

// get the mask, used for the in-memory representation
extern as_partition_vinfo_mask as_record_vinfo_mask_get(as_partition *p, as_partition_vinfo *vinfo);
extern as_partition_vinfo_mask as_record_vinfoset_mask_get( as_partition *p, as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask);
extern bool as_record_vinfoset_mask_validate(as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask);

// a simpler call that gives seconds in the right epoch
extern uint32_t as_record_void_time_get();


/* as_partition_id
 * A generic type for partition identifiers */
typedef uint16_t as_partition_id;
#define AS_PARTITION_ID_UNDEF ((uint16_t)0xFFFF)

/* AS_PARTITIONS
 * The number of partitions in the system (and a mask for convenience) */
#define AS_PARTITIONS 4096
// #define AS_PARTITIONS 1024
//#define AS_PARTITIONS 256
// #define AS_PARTITIONS 64
// #define AS_PARTITIONS 32
// #define AS_PARTITIONS 16
// #define AS_PARTITIONS 8
#define AS_PARTITION_MASK (AS_PARTITIONS - 1)



/* as_partition_state
 * The state of a partition
 *    SYNC: fully synchronized
 *    DESYNC: unsynchronized, but moving towards synchronization
 *    ZOMBIE: sync, but moving towards absent
 *    WAIT: waiting for pending writes to flush out
 *    ABSENT: empty
 */
#define AS_PARTITION_STATE_UNDEF 0
#define AS_PARTITION_STATE_SYNC  1
#define AS_PARTITION_STATE_DESYNC  2
#define AS_PARTITION_STATE_ZOMBIE  3
#define AS_PARTITION_STATE_WAIT 4
#define AS_PARTITION_STATE_ABSENT 5
#define AS_PARTITION_STATE_JOURNAL_APPLY 6 // used in faked reservations
typedef uint8_t as_partition_state;

#define AS_PARTITION_MIG_TX_STATE_NONE  0
#define AS_PARTITION_MIG_TX_STATE_SUBRECORD 1
#define AS_PARTITION_MIG_TX_STATE_RECORD 2
typedef uint8_t as_partition_mig_tx_state;

/* as_partition_getid
 * A brief utility function to derive the partition ID from a digest */
static inline as_partition_id
as_partition_getid(cf_digest d)
{
	return( (as_partition_id) cf_digest_gethash( &d, AS_PARTITION_MASK ) );
//     return((as_partition_id)((*(as_partition_id *)&d.digest[0]) & AS_PARTITION_MASK));
}





/* as_partition
 * A partition */
struct as_partition_s {
	pthread_mutex_t lock;

	cf_node replica[AS_CLUSTER_SZ];
	/* origin: the node that is replicating to us. For master, origin could be "acting master" during migration.
	 * target: an actual master that we're migrating to */
	cf_node origin, target;
	as_partition_state state;  // used to be consistency
	int pending_writes;  // one thread polls on this going to 0
	int pending_migrate_tx, pending_migrate_rx;
	bool replica_tx_onsync[AS_CLUSTER_SZ];

	size_t n_dupl;
	cf_node  dupl_nodes[AS_CLUSTER_SZ];
	bool reject_writes;
	bool waiting_for_master;
	cf_node  qnode; 	// point to the node which serves the query at the moment
	as_partition_vinfo primary_version_info; // the version of the primary partition in the cluster
	as_partition_vinfo version_info;         // the version of my partition here and now
	pthread_mutex_t        vinfoset_lock;
	as_partition_vinfoset vinfoset;

	cf_node old_sl[AS_CLUSTER_SZ];

	uint64_t cluster_key;

	// the number of bytes in the tree below
	cf_atomic_int	n_bytes_memory; // memory bytes
	// the maximum void time of all records in the tree below
	cf_atomic_int max_void_time;

	// the actual data
	struct as_index_tree_s *vp;
	struct as_index_tree_s *sub_vp;
	as_partition_id partition_id;
	uint p_repl_factor;

	// Track ldt version in transit currently
	uint64_t current_outgoing_ldt_version;
	uint64_t current_incoming_ldt_version;
};

#define AS_PARTITION_HAS_DATA(p)  ((p)->vp->elements || (p)->sub_vp->elements)

/* as_partition_reservation
 * A structure to hold state on a reserved partition
 * NB: Structure elements are organized to make sure access to most
 *     common field is a single cache line access ... DO NOT DISTURB
 *     unless you what you are doing
 */
struct as_partition_reservation_s {
	as_namespace          *ns;
	bool                   is_write;
	bool                   reject_writes;
	as_partition_state     state;
	uint8_t                n_dupl;
	as_partition_id        pid;
	uint8_t                spare[2];
	/************* 16 byte ******/
	as_partition           *p;
	struct as_index_tree_s *tree;
	uint64_t                cluster_key;
	as_partition_vinfo      vinfo;

	/************* 64 byte *****/
	struct as_index_tree_s *sub_tree;
	cf_node                 dupl_nodes[AS_CLUSTER_SZ];
};


#define AS_PARTITION_RESERVATION_INIT(__rsv)   \
	__rsv.ns = NULL; \
	__rsv.is_write = false; \
	__rsv.pid = AS_PARTITION_ID_UNDEF; \
	__rsv.p = 0; \
	__rsv.state = AS_PARTITION_STATE_UNDEF; \
	__rsv.tree = 0; \
	__rsv.n_dupl = 0; \
	__rsv.reject_writes = false; \
	__rsv.cluster_key = 0;

#define AS_PARTITION_RESERVATION_INITP(__rsv)   \
	__rsv->ns = NULL; \
	__rsv->is_write = false; \
	__rsv->pid = AS_PARTITION_ID_UNDEF; \
	__rsv->p = 0; \
	__rsv->state = AS_PARTITION_STATE_UNDEF; \
	__rsv->tree = 0; \
	__rsv->n_dupl = 0; \
	__rsv->reject_writes = false; \
	__rsv->cluster_key = 0;


// This is a statistics function
typedef struct as_partition_states_s {
	int		sync_actual;
	int		sync_replica;
	int		desync;
	int		zombie;
	int 	wait;
	int		absent;
	int		undef;
	int     n_objects;
	int     n_ref_count;
	int     n_sub_objects;
	int     n_sub_ref_count;
} as_partition_states;

/* Partition function declarations */
extern void as_partition_init(as_partition *p, as_namespace *ns, int pid);
extern void as_partition_reinit(as_partition *p, as_namespace *ns, int pid);
extern void as_partition_bless(as_partition *p);
extern bool is_partition_null(as_partition_vinfo *vinfo);
extern cf_node as_partition_getreplica_read(as_namespace *ns, as_partition_id p);
extern int as_partition_getreplica_readall(as_namespace *ns, as_partition_id p, cf_node *nv);
extern cf_node as_partition_getreplica_write(as_namespace *ns, as_partition_id p);
#define as_partition_isconsistent(_n, _p) (SYNC == ((_n)->consistency[(_p)]))

// reserve_qnode - *consumes* the ns reservation if success
extern int as_partition_reserve_qnode(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv);
extern void as_partition_prereserve_qnodes(as_namespace * ns, bool is_partition_qnode[], as_partition_reservation rsv[]);
// reserve_write - *consumes* the ns reservation if success
extern int as_partition_reserve_write(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv, cf_node *node, uint64_t *cluster_key);
// reserve_migrate - *consumes* the ns reservation if success
extern void as_partition_reserve_migrate(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv, cf_node *node);
extern int as_partition_reserve_migrate_timeout(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv, cf_node *node, int timeout_ms );

// reserve_read - *consumes* the ns reservation if success
extern int as_partition_reserve_read(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv, cf_node *node, uint64_t *cluster_key);
extern int as_partition_reserve_replica_list(as_namespace *ns, as_partition_id pid, as_partition_reservation *rsv);

// moves the reservation -
extern void as_partition_reservation_move(as_partition_reservation *dst, as_partition_reservation *src);
extern void as_partition_reservation_copy(as_partition_reservation *dst, as_partition_reservation *src);
extern void as_partition_reserve_update_state(as_partition_reservation *rsv);
extern void as_partition_release(as_partition_reservation *rsv);

extern int as_partition_tree_release(struct as_index_tree_s *p);

extern void as_partition_getreplica_read_str(cf_dyn_buf *db);
extern void as_partition_getreplica_prole_str(cf_dyn_buf *db);
extern void as_partition_getreplica_write_str(cf_dyn_buf *db);
extern void as_partition_getreplica_master_str(cf_dyn_buf *db);
extern void as_partition_get_replicas_all_str(cf_dyn_buf *db);
extern void as_partition_getinfo_str(cf_dyn_buf *db);
extern void as_partition_getstates(as_partition_states *ps);

extern void as_partition_getreplica_write_node(as_namespace *ns, cf_node *node_a);

extern void as_partition_balance();
extern void as_partition_balance_init();
extern void as_partition_balance_init_multi_node_cluster();
extern void as_partition_balance_init_single_node_cluster();
extern bool as_partition_balance_is_init_resolved();
extern bool as_partition_balance_is_multi_node_cluster();

typedef struct as_master_prole_stats_s {
	uint64_t n_master_records;
	uint64_t n_prole_records;
	uint64_t n_master_sub_records;
	uint64_t n_prole_sub_records;
} as_master_prole_stats;

extern void as_partition_get_master_prole_stats(as_namespace* ns, as_master_prole_stats* p_stats);

extern void as_partition_allow_migrations(void);
extern void as_partition_disallow_migrations(void);
extern bool as_partition_get_migration_flag(void);

// return number of partitions found in storage
extern int  as_partition_get_state_from_storage(as_namespace *ns, bool *partition_states);
extern char as_partition_getstate_str(int state);
extern bool as_partition_is_query_active(as_namespace *ns, size_t pid, as_partition *p);

// Print info. about the partition map to the log.
void as_partition_map_dump();

//#define NS_RWLOCK	 1   /* use a reader-writer lock */
#define NS_RWLOCK    0   /* use a standard mutex */

#define AS_SINDEX_BINMAX	4
#define AS_SINDEX_MAX		256

// as_sindex structure which hangs from the ns.
#define AS_SINDEX_INACTIVE			1 // On init, pre-loading
#define AS_SINDEX_ACTIVE			2 // On creation and afterwards
#define AS_SINDEX_DESTROY			3 // On destroy
// dummy sindex state when ai_btree_create() returns error this
// sindex is not available for any of the DML operations
#define AS_SINDEX_NOTCREATED		4 // Un-used flag
#define AS_SINDEX_FLAG_WACTIVE			0x01 // On ai btree create of sindex, never reset
#define AS_SINDEX_FLAG_RACTIVE			0x02 // When sindex scan of database is completed
#define AS_SINDEX_FLAG_DESTROY_CLEANUP 	0x04 // Called for AI clean-up during si deletion
#define AS_SINDEX_FLAG_MIGRATE_CLEANUP  0x08 // Un-used
#define AS_SINDEX_FLAG_POPULATING		0x10 // Indicates current si scan job, reset when scan is done.

struct as_sindex_s;
struct as_sindex_config_s;

typedef enum {
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF = 0,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION = 1,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_TTL = 2
} conflict_resolution_policy;

#define AS_SET_MAX_COUNT 0x3FF	// ID's 10 bits worth minus 1 (ID 0 means no set)
#define AS_BINID_HAS_SINDEX_SIZE  MAX_BIN_NAMES / ( sizeof(uint32_t) * CHAR_BIT )

#define  NS_READ_CONSISTENCY_LEVEL_NAME()								\
	(ns->read_consistency_level_override ?								\
	 (AS_POLICY_CONSISTENCY_LEVEL_ALL == ns->read_consistency_level ? "all" : "one") \
	 : "off")

#define NS_WRITE_COMMIT_LEVEL_NAME()									\
	(ns->write_commit_level_override ?									\
	 (AS_POLICY_COMMIT_LEVEL_ALL == ns->write_commit_level ? "all" : "master") \
	 : "off")

/* as_namespace[_id]
 * A namespace container */
typedef int32_t as_namespace_id; // signed to denote -1 bad namespace id

typedef struct ns_ldt_stats_s {

	/* LDT Operational Statistics */
	cf_atomic_int	ldt_write_reqs;
	cf_atomic_int	ldt_write_success;

	cf_atomic_int	ldt_read_reqs;
	cf_atomic_int	ldt_read_success;

	cf_atomic_int	ldt_delete_reqs;
	cf_atomic_int	ldt_delete_success;

	cf_atomic_int	ldt_update_reqs;

	cf_atomic_int	ldt_errs;
	cf_atomic_int   ldt_err_unknown;
	cf_atomic_int	ldt_err_toprec_not_found;
	cf_atomic_int	ldt_err_item_not_found;
	cf_atomic_int	ldt_err_internal;
	cf_atomic_int	ldt_err_unique_key_violation;

	cf_atomic_int	ldt_err_insert_fail;
	cf_atomic_int	ldt_err_search_fail;
	cf_atomic_int	ldt_err_delete_fail;
	cf_atomic_int	ldt_err_version_mismatch;

	cf_atomic_int	ldt_err_capacity_exceeded;
	cf_atomic_int	ldt_err_param;

	cf_atomic_int	ldt_err_op_bintype_mismatch;
	cf_atomic_int	ldt_err_too_many_open_subrec;
	cf_atomic_int	ldt_err_subrec_not_found;

	cf_atomic_int	ldt_err_bin_does_not_exist;
	cf_atomic_int	ldt_err_bin_exits;
	cf_atomic_int	ldt_err_bin_damaged;

	cf_atomic_int	ldt_err_subrec_internal;
	cf_atomic_int	ldt_err_toprec_internal;
	cf_atomic_int   ldt_err_filter;
	cf_atomic_int	ldt_err_key;
	cf_atomic_int	ldt_err_createspec;
	cf_atomic_int	ldt_err_usermodule;
	cf_atomic_int	ldt_err_input_too_large;
	cf_atomic_int	ldt_err_ldt_not_enabled;

	cf_atomic_int   ldt_gc_io;
	cf_atomic_int   ldt_gc_cnt;
	cf_atomic_int   ldt_gc_no_esr_cnt;
	cf_atomic_int   ldt_gc_no_parent_cnt;
	cf_atomic_int   ldt_gc_parent_version_mismatch_cnt;
	cf_atomic_int   ldt_gc_processed;

	cf_atomic_int	ldt_randomizer_retry;

} ns_ldt_stats;

struct as_namespace_s {
	/* Namespaces are internally assigned monotonic identifiers, but these
	 * are not portable across node boundaries; to identify a namespace
	 * canonically, you need to use the namespace name */
	char name[AS_ID_NAMESPACE_SZ];
	as_namespace_id id;

	// If true, read storage devices to build index at startup.
	bool cold_start;

	// Pointer to the persistent memory "base" block.
	uint8_t* p_xmem_base;

	// Pointer to array of partition tree info in persistent memory base block.
	as_treex* tree_roots;
	as_treex* sub_tree_roots;

	// Pointer to arena structure (not stages) in persistent memory base block.
	cf_arenax* arena;

#ifdef USE_JEM
	// JEMalloc arena to be used for long-term storage in this namespace (-1 if nonexistent.)
	int jem_arena;
#endif

	/* Replication management */
	uint16_t					replication_factor;
	uint16_t					cfg_replication_factor;
	conflict_resolution_policy	conflict_resolution_policy;
	bool						allow_versions;	// allow consistancy errors to create duplicate versions
	bool						single_bin;		// restrict the namespace to objects with exactly one bin
	bool						data_in_index;	// with single-bin, allows warm restart for data-in-memory (with storage-engine device)
	bool 						disallow_null_setname;
	bool                        ldt_enabled;
	uint32_t                    ldt_page_size;
	uint32_t					ldt_gc_sleep_us;

	/* XDR */
	bool						enable_xdr;
	bool 						sets_enable_xdr; // namespace-level flag to enable set-based xdr shipping.
	bool 						ns_forward_xdr_writes; // namespace-level flag to enable forwarding of xdr writes
	bool 						ns_allow_nonxdr_writes; // namespace-level flag to allow nonxdr writes or not
	bool 						ns_allow_xdr_writes; // namespace-level flag to allow xdr writes or not

	/* The server default read consistency level for this namespace. */
	as_policy_consistency_level read_consistency_level;

	/* Should the optional client-supplied, per-transaction read consistency level
	   be overriden by the server default on this namespace? */
	bool read_consistency_level_override;

	/* The server default write commit level for this namespace. */
	as_policy_commit_level write_commit_level;

	/* Should the optional client-supplied, per-transaction write commit level
	   be overriden by the server default on this namespace? */
	bool write_commit_level_override;

	/* Storage engine configuration - and per storage engine variables -
	** 'private' is managed by the storage engine in question */
	as_storage_type storage_type;
	char *storage_path;
	char *storage_devices[AS_STORAGE_MAX_DEVICES];
	char *storage_shadows[AS_STORAGE_MAX_DEVICES];
	char *storage_files[AS_STORAGE_MAX_FILES];
	char *storage_scheduler_mode; // relevant for devices only, not files
	off_t		storage_filesize;
	uint32_t	storage_blocksize;
	uint32_t	storage_write_threads;
	uint64_t	storage_max_write_cache;
	uint32_t	storage_read_block_size;
	uint32_t	storage_write_block_size;
	uint32_t	storage_num_write_blocks;
	bool		storage_data_in_memory;    // true if the DRAM copy is always kept
	bool    	storage_signature;
	bool		storage_cold_start_empty;
	bool		storage_disable_odirect;
	bool		storage_enable_osync;
	uint32_t	storage_defrag_lwm_pct;
	uint32_t	storage_defrag_queue_min;
	uint32_t	storage_defrag_sleep;
	int			storage_defrag_startup_minimum;
	uint64_t	storage_flush_max_us;
	uint64_t	storage_fsync_max_us;
	uint32_t	storage_min_avail_pct;
	uint32_t	storage_write_smoothing_period;

	// For data-not-in-memory, optionally cache swbs after writing to device.
	cf_atomic32 storage_post_write_queue; // number of swbs/device held after writing to device
	// To track fraction of reads from cache:
	cf_atomic32 n_reads_from_cache;
	cf_atomic32 n_reads_from_device;
	float cache_read_pct;

	int demo_read_multiplier;
	int demo_write_multiplier;

	void *storage_private;

	/* data store management */
	uint64_t	memory_size;
	uint64_t	ssd_size;
	uint64_t	kv_size;
	bool		cond_write;  // true if writing uniqueness is to be enforced by the KV store.
	float		hwm_disk, hwm_memory;
	float   	stop_writes_pct;
	uint32_t	evict_tenths_pct;
	uint64_t	default_ttl;
	uint64_t	max_ttl;
	int			auto_hwm_last_free;
	int			storage_min_free_wblocks; // the number of wblocks per device to "reserve"
	int			storage_last_avail_pct; // most recently calculated available percent
	int			storage_max_write_q; // storage_max_write_cache is converted to this
	uint32_t	saved_defrag_sleep; // restore after defrag at startup is done
	uint32_t	saved_write_smoothing_period; // restore after defrag at startup is done
	uint32_t	defrag_lwm_size; // storage_defrag_lwm_pct % of storage_write_block_size

	/* very interesting counters */
	cf_atomic_int	n_objects;
	cf_atomic_int	n_sub_objects;
	cf_atomic_int	n_bytes_memory;
	cf_atomic_int	n_absent_partitions;
	cf_atomic_int	n_actual_partitions;
	cf_atomic_int	n_expired_objects;
	cf_atomic_int	n_evicted_objects;
	cf_atomic_int	n_deleted_set_objects;
	cf_atomic_int	n_evicted_set_objects;

	// the maximum void time of all records in the namespace
	cf_atomic_int max_void_time;

	// Number of 0-void-time objects. TODO - should be atomic.
	uint64_t non_expirable_objects;

	uint32_t	nsup_cycle_duration; // seconds taken for most recent nsup cycle
	uint32_t	nsup_cycle_sleep_pct; // fraction of most recent nsup cycle that was spent sleeping

	// Pointer to bin name vmap in persistent memory.
	cf_vmapx		*p_bin_name_vmap;

	// Pointer to set information vmap in persistent memory.
	cf_vmapx		*p_sets_vmap;

	// Temporary array of sets to hold config values until sets vmap is ready.
	as_set			*sets_cfg_array;
	uint32_t		sets_cfg_count;

	// Temporary structure to hold si config values until smd-bootup is done.
	// shash entry for si name comparison btwn cfg and smd data
	shash *sindex_cfg_var_hash;

	// SINDEX
	int					sindex_cnt;
	struct as_sindex_s	*sindex;  // array with AS_MAX_SINDEX meta data
	uint64_t			sindex_data_max_memory;
	cf_atomic_int		sindex_data_memory_used;
	shash               *sindex_set_binid_hash;
	shash				*sindex_iname_hash;
	uint32_t             binid_has_sindex[AS_BINID_HAS_SINDEX_SIZE];

	// Current state of threshold breaches.
	cf_atomic32		hwm_breached;
	cf_atomic32		stop_writes;

	// Flag for cold-start ticker and eviction threshold check.
	bool			cold_start_loading;

	// For cold-start eviction.
	pthread_mutex_t	cold_start_evict_lock;
	uint32_t		cold_start_record_add_count;
	uint32_t		cold_start_evict_ttl;
	cf_atomic32		cold_start_threshold_void_time;
	uint32_t		cold_start_max_void_time;

	// Histogram of all master object storage sizes. (Meaningful for drive-backed namespaces only.)
	linear_histogram 	*obj_size_hist;
	cf_atomic32			obj_size_hist_max;

	// Histograms used for general eviction and expiration.
	linear_histogram 	*evict_hist;
	linear_histogram 	*ttl_hist;

	// Histograms used for set eviction.
	// (If AS_SET_MAX_COUNT ever gets too big, malloc based on vmap count.)
	linear_histogram 	*set_evict_hists[AS_SET_MAX_COUNT + 1];
	linear_histogram 	*set_ttl_hists[AS_SET_MAX_COUNT + 1];

	as_partition partitions[AS_PARTITIONS];

	ns_ldt_stats        lstats;
};

#define AS_SET_NAME_MAX_SIZE	64		// includes space for null-terminator

#define AS_SINDEX_PROP_KEY_SIZE ( AS_SET_NAME_MAX_SIZE + 20) // setname_binid_typeid
#define INVALID_SET_ID 0
#define AS_NAMESPACE_SET_THRESHOLD_EXCEEDED -2

// Set state bit-field:
//#define AS_SET_STOP_WRITES	0x00000001	// not using this so far
#define AS_SET_EVICT_RECORDS	0x00000002	// may soon be deprecated
#define AS_SET_DELETE 			0x00000004	// Delete this set

#define IS_SET_DELETED(p_set)	(cf_atomic32_get(p_set->state) & AS_SET_DELETE)

#define SET_DELETED_ON(p_set)	(cf_atomic32_set(&p_set->state, cf_atomic32_get(p_set->state) |  AS_SET_DELETE))
#define SET_DELETED_OFF(p_set)	(cf_atomic32_set(&p_set->state, cf_atomic32_get(p_set->state) &  ~AS_SET_DELETE))

typedef enum {
	AS_SET_ENABLE_XDR_DEFAULT = 0,
	AS_SET_ENABLE_XDR_TRUE = 1,
	AS_SET_ENABLE_XDR_FALSE = 2
} as_set_enable_xdr_flag;

struct as_set_s {
	char			name[AS_SET_NAME_MAX_SIZE];
	cf_atomic64		num_elements;
	cf_atomic64		stop_write_count;	// Stop writes in the set after this count is reached.
	cf_atomic32		unused;				// Stub variable to be reclaimed for future needs.
	cf_atomic64		evict_hwm_count;	// Evict records from set after this count is reached.
	cf_atomic32     enable_xdr;			// White or black-list a set-name for XDR replication for true/false of this set-level flag.
	cf_atomic32		state;				// Current state of the set.
};

// These bin functions must be below definition of struct as_namespace_s:

static inline void
as_bin_set_id_from_name_buf(as_namespace *ns, as_bin *b, byte *buf, int len) {
	if (! ns->single_bin) {
		char name[len + 1];

		memcpy(name, buf, len);
		name[len] = 0;
		b->id = as_bin_get_or_assign_id(ns, name);
	}
}

static inline void
as_bin_set_id_from_name(as_namespace *ns, as_bin *b, char *name) {
	if (! ns->single_bin) {
		b->id = as_bin_get_or_assign_id(ns, name);
	}
}

static inline size_t
as_bin_memcpy_name(as_namespace *ns, byte *buf, as_bin *b) {
	size_t len = 0;

	if (! ns->single_bin) {
		const char *name = as_bin_get_name_from_id(ns, b->id);

		len = strlen(name);
		memcpy(buf, name, len);
	}

	return len;
}

// forward ref
struct as_msg_field_s;

/* Namespace function declarations */
extern as_namespace *as_namespace_create(char *name, uint16_t replication_factor);
extern void as_namespaces_init(bool cold_start_cmd, uint32_t instance);
extern void as_namespace_setup(as_namespace* ns, uint32_t instance, uint32_t stage_capacity);
extern bool as_namespace_configure_sets(as_namespace *ns);
extern as_namespace *as_namespace_get_byname(char *name);
extern as_namespace *as_namespace_get_byid(uint id);
extern as_namespace *as_namespace_get_bymsgfield(struct as_msg_field_s *fp);
extern as_namespace *as_namespace_get_bymsgfield_unswap(struct as_msg_field_s *fp);
extern as_namespace *as_namespace_get_bybuf(uint8_t *name, size_t len);
extern as_namespace_id as_namespace_getid_bymsgfield(struct as_msg_field_s *fp);
extern void as_namespace_eval_write_state(as_namespace *ns, bool *hwm_breached, bool *stop_writes);
extern void as_namespace_bless(as_namespace *ns);
extern int as_namespace_get_create_set(as_namespace *ns, const char *set_name, uint16_t *p_set_id, bool check_threshold);
extern as_set * as_namespace_init_set(as_namespace *ns, const char *set_name);
extern const char *as_namespace_get_set_name(as_namespace *ns, uint16_t set_id);
extern uint16_t as_namespace_get_set_id(as_namespace *ns, const char *set_name);
extern uint16_t as_namespace_get_create_set_id(as_namespace *ns, const char *set_name);
extern void as_namespace_get_set_info(as_namespace *ns, const char *set_name, cf_dyn_buf *db);
extern void as_namespace_release_set_id(as_namespace *ns, uint16_t set_id);
extern void as_namespace_get_bins_info(as_namespace *ns, cf_dyn_buf *db, bool show_ns);
extern void as_namespace_get_hist_info(as_namespace *ns, char *set_name, char *hist_name,
		cf_dyn_buf *db, bool show_ns);
extern int as_namespace_check_set_limits(as_set * p_set, as_namespace * ns);

#ifdef USE_JEM
int as_namespace_set_jem_arena(char *ns, int arena);
int as_namespace_get_jem_arena(char *ns);
#endif

// Persistent Memory Management

struct as_treex_s {
	cf_arenax_handle sentinel_h;
	cf_arenax_handle root_h;
};

void as_namespace_xmem_trusted(as_namespace *ns);
void as_namespace_xmem_release(as_namespace* ns);

// Not namespace class functions, but they live in namespace.c:
void as_xmem_scheme_check();
uint32_t as_mem_check();

/* Cluster Key */
// Set the cluster key
extern void as_paxos_set_cluster_key(uint64_t cluster_key);
// Get the cluster key
extern uint64_t as_paxos_get_cluster_key();
