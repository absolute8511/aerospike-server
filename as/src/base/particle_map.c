/*
 * particle_map.c
 *
 * Copyright (C) 2015-2016 Aerospike, Inc.
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


#include <alloca.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/cdt.h"
#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"


//==========================================================
// MAP particle interface - function declarations.
//

// Destructor, etc.
void map_destruct(as_particle *p);
uint32_t map_size(const as_particle *p);

// Handle "wire" format.
int32_t map_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t map_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int map_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int map_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
uint32_t map_wire_size(const as_particle *p);
uint32_t map_to_wire(const as_particle *p, uint8_t *wire);

// Handle as_val translation.
uint32_t map_size_from_asval(const as_val *val);
void map_from_asval(const as_val *val, as_particle **pp);
as_val *map_to_asval(const as_particle *p);
uint32_t map_asval_wire_size(const as_val *val);
uint32_t map_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
uint32_t map_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
void map_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);

// Handle on-device "flat" format.
int32_t map_size_from_flat(const uint8_t *flat, uint32_t flat_size);
int map_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp);
int map_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp);
uint32_t map_flat_size(const as_particle *p);
uint32_t map_to_flat(const as_particle *p, uint8_t *flat);


//==========================================================
// MAP particle interface - vtable.
//

const as_particle_vtable map_vtable = {
		map_destruct,
		map_size,

		map_concat_size_from_wire,
		map_append_from_wire,
		map_prepend_from_wire,
		map_incr_from_wire,
		map_size_from_wire,
		map_from_wire,
		map_compare_from_wire,
		map_wire_size,
		map_to_wire,

		map_size_from_asval,
		map_from_asval,
		map_to_asval,
		map_asval_wire_size,
		map_asval_to_wire,

		map_size_from_msgpack,
		map_from_msgpack,

		map_size_from_flat,
		map_cast_from_flat,
		map_from_flat,
		map_flat_size,
		map_to_flat
};


//==========================================================
// Typedefs & constants.
//

//#define MAP_DEBUG_VERIFY

#define LINEAR_FIND_RANK_MAX_COUNT	16 // switch to linear search when the count drops to this number

#define AS_PACKED_MAP_INVALID	1
#define AS_PACKED_MAP_FAILED	2

#define AS_PACKED_MAP_FLAG_RESERVED_0		0x04 // placeholder for multimap
#define AS_PACKED_MAP_FLAG_OFF_IDX			0x10 // has list offset index
#define AS_PACKED_MAP_FLAG_ORD_IDX			0x20 // has value order index

struct order_index_adjust_s;
struct packed_map_op_s;

typedef uint32_t (*order_index_adjust_func)(const struct order_index_adjust_s *via, uint32_t src);
typedef bool (*packed_map_op_get_by_idx_func)(const struct packed_map_op_s *op, cdt_payload *packed, uint32_t index);
typedef msgpack_compare_t (*packed_map_op_compare_func)(const struct packed_map_op_s *op, uint32_t index1, uint32_t index2);

typedef struct msgpacked_index_s {
	uint8_t *ptr;
	size_t ele_size;
	size_t ele_count;
} msgpacked_index;

// Value order index.
typedef struct order_index_s {
	msgpacked_index _;
} order_index;

// Value order heap.
typedef struct order_heap_s {
	order_index _;

	size_t filled;
	size_t heap_size;

	const struct packed_map_op_s *op;
	packed_map_op_compare_func cmp_func;
	msgpack_compare_t cmp;
} order_heap;

// Offset index.
typedef struct offset_index_s {
	msgpacked_index _;

	const uint8_t *ele_start;
	size_t tot_ele_sz;
} offset_index;

typedef struct offidx_op_s {
	offset_index *dest;
	const offset_index *src;
	uint32_t d_i;
	uint32_t s_i;
	int delta;
} offidx_op;

typedef struct order_index_adjust_s {
	order_index_adjust_func f;
	uint32_t upper;
	uint32_t lower;
	int32_t delta;
} order_index_adjust;

typedef struct index_pack24_s {
	uint32_t value:24;
} __attribute__ ((__packed__)) index_pack24;

typedef struct as_packed_map_index_s {
	// Mutable state member.
	// Is considered mutable in const objects.
	offset_index offset_idx; // offset start at ele_start (excluding ext metadata pair)

    order_index	value_idx;
	uint8_t flags;
} as_packed_map_index;

typedef struct packed_map_op_s {
	const uint8_t *packed;
	uint32_t packed_sz;
	as_packed_map_index pmi;

	uint32_t ele_count;	// excludes ext pair
	uint32_t new_ele_count;

	uint32_t ele_removed;
	uint32_t ele_start; // offset where elements start
	uint32_t seg1_size;
	uint32_t seg2_offset;
	uint32_t seg2_size;

	uint32_t key1_offset;
	uint32_t key1_size;
	uint32_t key2_offset;
	uint32_t key2_size;
} packed_map_op;

typedef struct map_packer_s {
	uint8_t *write_ptr;
	uint8_t *ele_start_ptr;

	offset_index offset_idx;	// offset start at ele_start (excluding ext metadata pair)
	order_index value_idx;

	uint32_t ele_count;
	uint32_t content_size;		// does not include map header or ext
	uint32_t index_size;

	uint32_t ext_size;
	uint32_t ext_header_size;

	uint8_t flags;
} map_packer;

typedef struct map_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) map_mem;

typedef struct map_flat_s {
    uint8_t     type;
    uint32_t    size;
    uint8_t     data[];
} __attribute__ ((__packed__)) map_flat;

typedef struct map_mem_empty_flagged_s {
	uint8_t		map_hdr;
	uint8_t		ext_hdr;
	uint8_t		ext_sz;
	uint8_t		ext_flags;
	uint8_t		nil;
} __attribute__ ((__packed__)) map_mem_empty_flagged;

static const map_mem_empty_flagged msgpack_empty_flagged_map = {
		.map_hdr = 0x81,
		.ext_hdr = 0xC7,
		.ext_sz = 0,
		.ext_flags = 0,
		.nil = 0xC0
};
static const map_mem map_mem_empty = {
		.type = AS_PARTICLE_TYPE_MAP,
		.sz = 1,
		.data = {0x80},
};
static const cdt_payload nil_segment = {
		.ptr = msgpack_nil,
		.size = 1
};

typedef enum sort_by_e {
	SORT_BY_KEY,
	SORT_BY_VALUE,
	SORT_BY_IDX,
} sort_by_t;

typedef struct index_sort_userdata_s {
	const offset_index *offsets;
	order_index *order;
	const uint8_t *packed;
	uint32_t packed_sz;
	bool error;
	sort_by_t sort_by;
} index_sort_userdata;

typedef struct map_add_control_s {
	bool allow_overwrite;			// if key exists and map is unique-keyed - may overwrite
	bool allow_create;				// if key does not exist - may create
} map_add_control;

typedef struct map_ele_find_s {
	bool found_key;
	bool found_value;

	uint32_t idx;
	uint32_t rank;

	uint32_t key_offset;	// offset start at map header
	uint32_t value_offset;	// offset start at map header
	uint32_t size;

	uint32_t upper;
	uint32_t lower;
} map_ele_find;

typedef struct result_data_s {
	as_bin *result;
	rollback_alloc *alloc;
	result_type_t type;
	bool is_multi;
} cdt_result_data;

#define as_bin_create_temp_packed_flagged_map_if_notinuse(__b, __flags) { \
	if (__flags == 0) { \
		as_bin_create_temp_packed_map_if_notinuse(__b); \
	} \
	else if (! as_bin_inuse(b)) { \
		__b->particle = alloca(sizeof(map_mem) + sizeof(msgpack_empty_flagged_map)); \
		as_particle_set_empty_flagged_map(__b->particle, __flags); \
		as_bin_state_set_from_type(__b, AS_PARTICLE_TYPE_MAP); \
	} \
}

#define order_index_inita(__idx_ptr, __ele_count) \
		order_index_init(__idx_ptr, NULL, __ele_count); \
		(__idx_ptr)->_.ptr = alloca(order_index_size(__idx_ptr));

#define order_index_inita2(__idx_ptr, __ele_count, __ele_alloc) \
		order_index_init(__idx_ptr, NULL, __ele_count); \
		(__idx_ptr)->_.ptr = alloca((__idx_ptr)->_.ele_size * __ele_alloc); \
		(__idx_ptr)->_.ele_count = __ele_alloc

#define order_index_inita_copy(__idx_ptr, __src_ptr) { \
		uint32_t ele_count = (__src_ptr)->_.ele_count; \
		(__idx_ptr)->_.ele_size = (__src_ptr)->_.ele_size; \
		size_t alloc_size = (__idx_ptr)->_.ele_size * ele_count; \
		(__idx_ptr)->_.ptr = alloca(alloc_size);	\
		(__idx_ptr)->_.ele_count = ele_count; \
		memcpy((__idx_ptr)->_.ptr, order_index_get_mem(__src_ptr, 0), alloc_size);	\
}

#define order_heap_inita(__idx_ptr, __ele_count, __op_ptr, __cmp, __is_key) \
		order_heap_init(__idx_ptr, NULL, __ele_count, __op_ptr, __cmp, __is_key);	\
		(__idx_ptr)->_._.ptr = alloca(order_heap_size(__idx_ptr));

#define offset_index_inita(__idx_ptr, __ele_start_ptr, __tot_ele_sz, __ele_count) \
		offset_index_init(__idx_ptr, NULL, __ele_count, __tot_ele_sz); \
		(__idx_ptr)->_.ptr = alloca(offset_index_size(__idx_ptr));	\
		offset_index_set_filled((__idx_ptr), 1);	\
		(__idx_ptr)->ele_start = __ele_start_ptr;

#define offset_index_inita_if_invalid(__idx_ptr, __packed_ptr, __packed_sz, __ele_count) \
		if (offset_index_is_null(__idx_ptr)) {	\
			offset_index_inita(__idx_ptr, __packed_ptr, __packed_sz, __ele_count);	\
		}

#define offset_index_inita_from_op_if_invalid(__idx_ptr, __op_ptr) \
		offset_index_inita_if_invalid(__idx_ptr, (__op_ptr)->packed + (__op_ptr)->ele_start, (__op_ptr)->packed_sz - (__op_ptr)->ele_start, (__op_ptr)->ele_count);


//==========================================================
// Forward declarations.
//

static inline bool is_map_type(uint8_t type);
static inline bool is_k_ordered(uint8_t flags);
static inline bool is_kv_ordered(uint8_t flags);
static size_t map_calc_ext_content_sz(uint8_t flags, uint32_t ele_count, uint32_t content_size);
static uint8_t map_adjust_incoming_flags(uint8_t flags);

static size_t op_map_ext_content_sz(const packed_map_op *op);
static inline bool op_is_k_ordered(const packed_map_op *op);
static inline bool op_is_kv_ordered(const packed_map_op *op);
static inline bool has_offidx(const packed_map_op *op);
static inline bool has_full_offidx(const packed_map_op *op);

static void as_pack_init_indexes(as_packer *pk, const packed_map_op *op);
static inline bool skip_map_pair(as_unpacker *pk);
static int qsort_r_compare32(const void *a, const void *b, void *arg);

// as_bin
static inline void as_bin_set_empty_packed_map(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags);
static inline void as_bin_create_temp_packed_map_if_notinuse(as_bin *b);
static inline bool as_bin_is_temp_packed_map(const as_bin *b);

// as_particle
static void as_particle_set_empty_flagged_map(as_particle *p, uint64_t flags);

// cdt_payload
static void cdt_payload_pack_val(cdt_payload *packed, const as_val *val);
static inline void cdt_payload_pack_int(cdt_payload *packed, int64_t value);
static inline void cdt_payload_pack_double(cdt_payload *packed, double value);

// as_packed_map_index
static void as_packed_map_index_init(as_packed_map_index *pmi, uint32_t ele_count, uint32_t content_size);

// map_packer
static as_particle *map_packer_create_particle(map_packer *pk, rollback_alloc *alloc_buf);
static void map_packer_init(map_packer *pk, uint32_t ele_count, uint8_t flags, uint32_t content_size);
static bool map_packer_setup_bin(map_packer *pk, as_bin *b, rollback_alloc *alloc_buf);
static void map_packer_write_hdridx(map_packer *pk);
static bool map_packer_fill_offset_index(map_packer *mpk);
static int map_packer_fill_index_sort_compare(const void *x, const void *y, void *p);
static bool map_packer_fill_v_index(map_packer *mpk, const uint8_t *ele_start_ptr, uint32_t tot_ele_sz);
static bool map_packer_copy_index(map_packer *pk, const packed_map_op *op, map_ele_find *remove_info, const map_ele_find *add_info, uint32_t kv_size);
static inline void map_packer_write_seg1(map_packer *pk, const packed_map_op *op);
static inline void map_packer_write_seg2(map_packer *pk, const packed_map_op *op);
static inline void map_packer_write_msgpack_seg(map_packer *pk, const cdt_payload *seg);

// packed_map
static int packed_map_set_flags(as_bin *b, rollback_alloc *alloc_buf, as_bin *result, uint8_t set_flags);
static int packed_map_increment(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, const cdt_payload *delta_value, as_bin *result, bool is_decrement);
static int packed_map_add(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, const cdt_payload *value, as_bin *result, const map_add_control *control);
static int packed_map_add_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, as_bin *result, const map_add_control *control);

static int packed_map_remove_idxs(as_bin *b, const packed_map_op *op, rollback_alloc *alloc_buf, const order_index *remove_idxs, uint32_t count, uint32_t *removed);

static int packed_map_remove_by_key(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, cdt_result_data *result);
static int packed_map_remove_by_key_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result);
static int packed_map_remove_by_index_range(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, cdt_result_data *result);
static int packed_map_remove_by_value_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int packed_map_remove_by_rank_range(as_bin *b, rollback_alloc *alloc_buf, int64_t rank, uint64_t count, cdt_result_data *result);

static int packed_map_remove_all_key_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, cdt_result_data *result);
static int packed_map_remove_all_value_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, cdt_result_data *result);

static int packed_map_clear(as_bin *b, rollback_alloc *alloc_buf, as_bin *result);

static int packed_map_get_by_key(const as_bin *b, const cdt_payload *key, cdt_result_data *result);
static int packed_map_get_by_key_interval(const as_bin *b, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result);
static int packed_map_get_by_index_range(const as_bin *b, int64_t index, uint64_t count, cdt_result_data *result);
static int packed_map_get_by_value_interval(const as_bin *b, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int packed_map_get_by_rank_range(const as_bin *b, int64_t rank, uint64_t count, cdt_result_data *result);

// packed_map_op
static void packed_map_op_init(packed_map_op *op, const uint8_t *buf, uint32_t size);
static inline bool packed_map_op_init_from_particle(packed_map_op *op, const as_particle *p);
static bool packed_map_op_init_from_bin(packed_map_op *op, const as_bin *b);
static bool packed_map_op_unpack_hdridx(packed_map_op *op);

static inline void packed_map_op_init_unpacker(const packed_map_op *op, as_unpacker *pk);
static bool packed_map_op_ensure_ordidx_filled(const packed_map_op *op);

static uint32_t packed_map_op_find_index_by_idx_unordered(const packed_map_op *op, uint32_t idx);
static uint32_t packed_map_op_find_index_by_key_unordered(const packed_map_op *op, const cdt_payload *key);

static void packed_map_op_find_rank_indexed_linear(const packed_map_op *op, map_ele_find *find, uint32_t start, uint32_t len);
static bool packed_map_op_find_rank_indexed(const packed_map_op *op, map_ele_find *find);
static bool packed_map_op_find_rank_by_value_indexed(const packed_map_op *op, map_ele_find *find, const cdt_payload *value);
static bool packed_map_op_find_rank_range_by_value_interval_indexed(const packed_map_op *op, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, bool is_multi);
static bool packed_map_op_find_rank_range_by_value_interval_unordered(const packed_map_op *op, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, order_index *ordidx);
static bool packed_map_op_find_key_indexed(const packed_map_op *op, map_ele_find *find, const cdt_payload *key, const cdt_payload *value);
static bool packed_map_op_find_key(const packed_map_op *op, map_ele_find *find, const cdt_payload *key, const cdt_payload *value);

static int32_t packed_map_op_add(packed_map_op *op, const map_ele_find *found);
static int32_t packed_map_op_remove(packed_map_op *op, const map_ele_find *found, uint32_t count, uint32_t remove_sz);

static int packed_map_op_get_remove_by_key(packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, cdt_result_data *result);
static int packed_map_op_get_remove_by_key_interval(packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result);
static int packed_map_op_get_remove_by_index_range(const packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, uint32_t index, uint32_t count, cdt_result_data *result);

static int packed_map_op_get_remove_by_value_interval(const packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result);
static int packed_map_op_get_remove_by_rank_range(const packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, uint32_t rank, uint32_t count, cdt_result_data *result);

static bool packed_map_op_get_range_by_key_interval_unordered(packed_map_op *op, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count, order_index *ranks);
static bool packed_map_op_get_range_by_key_interval_ordered(packed_map_op *op, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count);
static int packed_map_op_build_rank_result_by_index_range(const packed_map_op *op, uint32_t index, uint32_t count, const order_index *ele_idx, uint32_t start, cdt_result_data *result);

static bool packed_map_op_get_key_by_idx(const packed_map_op *op, cdt_payload *key, uint32_t index);
static bool packed_map_op_get_value_by_idx(const packed_map_op *op, cdt_payload *value, uint32_t idx);
static bool packed_map_op_get_pair_by_idx(const packed_map_op *op, cdt_payload *value, uint32_t index);

static int packed_map_op_build_index_result_by_ele_idx(const packed_map_op *op, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result);
static bool packed_map_op_build_ele_result_by_idx_range(const packed_map_op *op, uint32_t ele_idx, uint32_t count, cdt_result_data *result);
static bool packed_map_op_build_ele_result_by_ele_idx(const packed_map_op *op, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result);
static int packed_map_op_build_result_by_key(const packed_map_op *op, const cdt_payload *key, uint32_t idx, uint32_t count, cdt_result_data *result);

static int64_t packed_map_op_get_rank_by_idx(const packed_map_op *op, uint32_t idx);
static int packed_map_op_build_rank_result_by_idx(const packed_map_op *op, uint32_t idx, cdt_result_data *result);
static int packed_map_op_build_rank_result_by_idx_range(const packed_map_op *op, uint32_t idx, uint32_t count, cdt_result_data *result);

static uint8_t *packed_map_op_write_seg1(const packed_map_op *op, uint8_t *buf);
static uint8_t *packed_map_op_write_seg2(const packed_map_op *op, uint8_t *buf);
static bool packed_map_op_write_dk_index(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, offset_index *offset_idx, uint32_t kv_size);
static bool packed_map_op_write_dv_index(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, order_index *value_idx);

static msgpack_compare_t packed_map_op_compare_key_by_idx(const packed_map_op *op, uint32_t idx1, uint32_t idx2);
static msgpack_compare_t packed_map_compare_values(as_unpacker *pk1, as_unpacker *pk2);
static msgpack_compare_t packed_map_op_compare_value_by_idx(const packed_map_op *op, uint32_t idx1, uint32_t idx2);

static bool packed_map_op_write_k_ordered(packed_map_op *op, uint8_t *write_ptr, offset_index *offsets_new);

// packed_map create
static as_particle *packed_map_create(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t content_size, uint8_t flags);

static int64_t packed_map_strip_indexes(uint8_t *dest, const as_particle *p, bool remove_flags);

// map_ele_find
static void map_ele_find_init(map_ele_find *find, const packed_map_op *op);
static void map_ele_find_continue_from_lower(map_ele_find *find, const map_ele_find *found, uint32_t ele_count);
static void map_ele_find_init_from_idx(map_ele_find *find, const packed_map_op *op, uint32_t idx);

// msgpacked_index
static inline void msgpacked_index_set(msgpacked_index *idxs, size_t index, uint32_t value);
static inline void msgpacked_index_set_ptr(msgpacked_index *idxs, uint8_t *ptr);
static inline void *msgpacked_index_get_mem(const msgpacked_index *idxs, size_t index);
static inline size_t msgpacked_index_size(const msgpacked_index *idxs);
static inline uint32_t msgpacked_index_ptr2value(const msgpacked_index *idxs, const void *ptr);
static inline uint32_t msgpacked_index_get(const msgpacked_index *idxs, size_t index);
static void msgpacked_index_print(const msgpacked_index *idxs, const char *name);
static bool msgpacked_index_find_index_sorted(const msgpacked_index *sorted_indexes, uint32_t find_index, uint32_t count, uint32_t *where);

// offset_index
static inline void offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr, uint32_t ele_count, uint32_t content_size);
static inline void offset_index_set(offset_index *offidx, size_t index, uint32_t value);
static bool offset_index_set_next(offset_index *offidx, size_t index, uint32_t value);
static inline void offset_index_set_filled(offset_index *offidx, uint32_t ele_filled);
static inline void offset_index_set_ptr(offset_index *offidx, uint8_t *idx_mem, const uint8_t *packed_mem);
static inline void offset_index_copy(offset_index *dest, const offset_index *src, size_t d_start, size_t s_start, size_t count, int delta);
static bool offset_index_fill(offset_index *offidx, size_t index);
static inline int64_t offset_index_get(offset_index *offidx, size_t index);
static inline int64_t offset_index_get_delta(offset_index *offidx, size_t index);
static void offset_index_append_size(offset_index *offidx, uint32_t delta);

static inline void *offset_index_get_mem(const offset_index *offidx, size_t index);
static inline size_t offset_index_size(const offset_index *offidx);
static inline bool offset_index_is_null(const offset_index *offidx);
static inline bool offset_index_is_valid(const offset_index *offidx);
static inline bool offset_index_is_full(const offset_index *offidx);
static inline uint32_t offset_index_get_const(const offset_index *offidx, size_t idx);
static inline uint32_t offset_index_get_delta_const(const offset_index *offidx, size_t index);
static inline uint32_t offset_index_get_filled(const offset_index *offidx);
void offset_index_print(const offset_index *offidx, const char *name);
void offset_index_delta_print(const offset_index *offidx, const char *name);

// offidx_op
static void offidx_op_init(offidx_op *op, offset_index *dest, const offset_index *src);
static void offidx_op_add(offidx_op *op, uint32_t index, uint32_t mem_sz);
static void offidx_op_remove(offidx_op *op, uint32_t index);
static void offidx_op_remove_range(offidx_op *op, uint32_t index, uint32_t count);
static void offidx_op_end(offidx_op *op);

// value_heap
static inline void order_heap_init(order_heap *heap, uint8_t *ptr, uint32_t ele_count, const packed_map_op *op, msgpack_compare_t cmp, bool cmp_key);
static inline void order_heap_set(order_heap *heap, size_t index, uint32_t value);
static inline size_t order_heap_size(const order_heap *heap);
static inline uint32_t order_heap_get(const order_heap *heap, size_t index);

static void order_heap_swap(order_heap *heap, uint32_t index1, uint32_t index2);
static bool order_heap_remove_top(order_heap *heap);
static bool order_heap_replace_top(order_heap *heap, uint32_t value);
bool order_heap_add(order_heap *heap, uint32_t value);
static bool order_heap_heapify(order_heap *heap, uint32_t index);
static bool order_heap_build(order_heap *heap, bool init);
static bool order_heap_order_at_end(order_heap *heap, uint32_t count);
static void order_heap_reverse_end(order_heap *heap, uint32_t count);
static uint32_t order_heap_get_ordered(const order_heap *heap, uint32_t index);
void order_heap_print(const order_heap *heap);

// order_index
static inline void order_index_init(order_index *ordidx, uint8_t *ptr, uint32_t ele_count);
static inline void order_index_set(order_index *ordidx, size_t index, uint32_t value);
static inline void order_index_set_ptr(order_index *ordidx, uint8_t *ptr);
static inline void order_index_copy(order_index *dest, const order_index *src, size_t d_start, size_t s_start, size_t count, const order_index_adjust *adjust);
static bool order_index_sort(order_index *ordidx, const offset_index *offsets, const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by);
static inline bool order_index_set_sorted(order_index *ordidx, const offset_index *offsets, const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by);
static bool order_index_set_sorted_with_offsets(order_index *ordidx, const offset_index *offsets, sort_by_t sort_by);
static void order_index_remove_dup_idx(order_index *ordidx, uint32_t x);
static uint32_t order_index_sorted_remove_dups(order_index *ordidx);
static bool order_index_remove_dups(order_index *ordidx, const order_index *sorted_hint);

static uint32_t order_index_find_idx(const order_index *ordidx, uint32_t idx, uint32_t start, uint32_t len);
static bool order_index_sorted_has_dups(const order_index *ordidx);
static inline void *order_index_get_mem(const order_index *ordidx, size_t index);
static inline size_t order_index_size(const order_index *ordidx);
static inline bool order_index_is_null(const order_index *ordidx);
static inline bool order_index_is_valid(const order_index *ordidx);
static inline bool order_index_is_filled(const order_index *ordidx);
static inline uint32_t order_index_ptr2value(const order_index *ordidx, const void *ptr);
static inline uint32_t order_index_get(const order_index *ordidx, size_t index);
static void order_index_print(const order_index *ordidx, const char *name);

// order_index_adjust
static inline uint32_t order_index_adjust_value(const order_index_adjust *via, uint32_t src);
static uint32_t order_index_adjust_lower(const order_index_adjust *via, uint32_t src);
static uint32_t order_index_adjust_mid(const order_index_adjust *via, uint32_t src);

// order_index_op
static inline void order_index_op_add(order_index *dest, const order_index *src, uint32_t add_idx, int64_t add_rank);
static bool order_index_op_remove_or_replace_internal(order_index *dest, const order_index *src, uint32_t add_rank, uint32_t remove_rank, uint32_t remove_count);
static inline void order_index_op_replace1_internal(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank, const order_index_adjust *adjust);
static inline void order_index_op_replace1(order_index *dest, const order_index *src, uint32_t add_rank, uint32_t remove_rank);
static inline void order_index_op_replace1_idx(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank);
static inline bool order_index_op_replace(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank, uint32_t remove_count);
static inline bool order_index_op_remove(order_index *dest, const order_index *src, uint32_t remove_rank, uint32_t remove_count);
static void order_index_op_remove_indexes(order_index *dest, const order_index *src, const order_index *sorted_indexes, uint32_t count);

// result_data
static int result_data_set_range(cdt_result_data *result, uint32_t start, uint32_t count, uint32_t ele_count);
static int result_data_set_index_rank_count(cdt_result_data *rd, uint32_t start, uint32_t count, uint32_t ele_count);
static bool result_data_set_list_int2x(cdt_result_data *rd, int64_t i1, int64_t i2);

static bool result_data_set_not_found(cdt_result_data *rd, int64_t index);
static bool result_data_set_key_not_found(cdt_result_data *rd, int64_t index);
static bool result_data_set_value_not_found(cdt_result_data *rd, int64_t rank);
static bool result_data_set_ordered_list(cdt_result_data *rd, order_index *ordidx, uint32_t count);

static inline bool result_data_is_return_elements(const cdt_result_data *rd);
static inline bool result_data_is_return_index(const cdt_result_data *rd);
static inline bool result_data_is_return_index_range(const cdt_result_data *rd);
static inline bool result_data_is_return_rank(const cdt_result_data *rd);
static inline bool result_data_is_return_rank_range(const cdt_result_data *rd);

// Debugging support
void print_index32(const uint32_t *index, uint32_t ele_count, const char *name);
void print_vindex(const order_index *index, const char *name);
bool as_bin_verify(const as_bin *b);


//==========================================================
// MAP particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
map_destruct(as_particle *p)
{
	cf_free(p);
}

uint32_t
map_size(const as_particle *p)
{
	const map_mem *p_map_mem = (const map_mem *)p;
	return (uint32_t)sizeof(map_mem) + p_map_mem->sz;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
map_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for map");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
map_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to map");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
map_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to map");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
map_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "increment of map");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
map_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// TODO - CDT can't determine in memory or not.
	packed_map_op op;
	packed_map_op_init(&op, wire_value, value_size);

	if (! packed_map_op_unpack_hdridx(&op)) {
		return -1;
	}

	if (op.pmi.flags == 0) {
		return (int32_t)(sizeof(map_mem) + value_size);
	}

	size_t extra_sz = op_map_ext_content_sz(&op);

	// 1 byte for header, 1 byte for type, 1 byte for length for existing ext.
	extra_sz += as_pack_ext_header_get_size(extra_sz) - 3;

	return (int32_t)(sizeof(map_mem) + value_size + extra_sz);
}

int
map_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// TODO - CDT can't determine in memory or not.
	// It works for data-not-in-memory but we'll incur a memcpy that could be eliminated.
	packed_map_op op;
	packed_map_op_init(&op, wire_value, value_size);

	if (! packed_map_op_unpack_hdridx(&op)) {
		return -1;
	}

	map_mem *p_map_mem = (map_mem *)*pp;

	p_map_mem->type = wire_type;

	if (op.pmi.flags == 0) {
		p_map_mem->sz = value_size;
		memcpy(p_map_mem->data, wire_value, value_size);
		return 0;
	}

	// TODO - May want to check key order here but for now we'll trust the client/other node.
	size_t ext_content_sz = op_map_ext_content_sz(&op);
	// 1 byte for header, 1 byte for type, 1 byte for length for existing ext.
	size_t extra_sz = as_pack_ext_header_get_size((uint32_t)ext_content_sz) - 3;

	as_packer pk = {
			.head = NULL,
			.tail = NULL,
			.buffer = p_map_mem->data,
			.offset = 0,
			.capacity = value_size + extra_sz,
	};

	as_pack_map_header(&pk, op.ele_count + 1);
	as_pack_ext_header(&pk, ext_content_sz, map_adjust_incoming_flags(op.pmi.flags));
	as_pack_init_indexes(&pk, &op);
	as_pack_val(&pk, (const as_val *)&as_nil);
	memcpy(pk.buffer + pk.offset, op.packed + op.ele_start, op.packed_sz - op.ele_start);
	p_map_mem->sz = value_size + ext_content_sz + extra_sz;

#ifdef MAP_DEBUG_VERIFY
	{
		as_bin b;
		b.particle = *pp;
		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);

		if (! as_bin_verify(&b)) {
			offset_index_print(&op.pmi.offset_idx, "verify");
			cf_warning(AS_PARTICLE, "map_from_wire: pp=%p wire_value=%p", pp, wire_value);
		}
	}
#endif

	return 0;
}

int
map_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
	cf_warning(AS_PARTICLE, "map_compare_from_wire() not implemented");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

uint32_t
map_wire_size(const as_particle *p)
{
	const map_mem *p_map_mem = (const map_mem *)p;

	packed_map_op op;
	packed_map_op_init_from_particle(&op, p);

	if (op.pmi.flags == 0) {
		return p_map_mem->sz;
	}

	uint32_t sz = p_map_mem->sz - op.ele_start;
	sz += as_pack_list_header_get_size(op.ele_count + 1);
	sz += 3 + 1;	// 3 for min ext hdr and 1 for nil pair

	return sz;
}

uint32_t
map_to_wire(const as_particle *p, uint8_t *wire)
{
	int64_t ret = packed_map_strip_indexes(wire, p, false);

	if (ret < 0) {
		cf_crash(AS_PARTICLE, "map_to_wire() strip failed with ret=%ld", ret);
	}

	return (uint32_t)ret;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
map_size_from_asval(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	const as_map *map = (const as_map *)val;

	if (map->flags == 0) {
		return (uint32_t)sizeof(map_mem) + size;
	}

	uint32_t ele_count = as_map_size(map);
	uint32_t map_hdr_sz = as_pack_list_header_get_size(ele_count);
	uint32_t content_size = size - map_hdr_sz;
	uint32_t ext_content_sz = map_calc_ext_content_sz(map->flags, ele_count, content_size);

	size = (uint32_t)sizeof(map_mem);
	size += as_pack_list_header_get_size(ele_count + 1) + content_size;
	size += as_pack_ext_header_get_size(ext_content_sz);	// ext header and length field
	size += ext_content_sz;									// ext content
	size++;													// nil pair

	return (uint32_t)sizeof(map_mem) + size;
}

void
map_from_asval(const as_val *val, as_particle **pp)
{
	map_mem *p_map_mem = (map_mem *)*pp;
	const as_map *map = (const as_map *)val;

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;

	as_serializer s;
	as_msgpack_init(&s);

	int32_t size = as_serializer_serialize_presized(&s, val, p_map_mem->data);

	if (size < 0) {
		// TODO - really crash or something else?
		cf_crash(AS_PARTICLE, "map_from_asval() failed to presize");
	}

	as_serializer_destroy(&s);

	if (map->flags == 0) {
		p_map_mem->sz = (uint32_t)size;
		return;
	}

	uint8_t buf[size];

	memcpy(buf, p_map_mem->data, size);

	packed_map_op op;
	packed_map_op_init(&op, buf, size);

	if (! packed_map_op_unpack_hdridx(&op)) {
		cf_crash(AS_PARTICLE, "map_from_asval() failed to unpack header");
	}

	map_packer mpk;
	uint32_t ele_count = op.ele_count;
	uint8_t map_flags = map_adjust_incoming_flags(map->flags);
	uint32_t content_length = op.packed_sz - op.ele_start;

	map_packer_init(&mpk, (uint32_t)ele_count, map_flags, content_length);
	mpk.write_ptr = p_map_mem->data;
	map_packer_write_hdridx(&mpk);

	if (! packed_map_op_write_k_ordered(&op, mpk.write_ptr, &mpk.offset_idx)) {
		cf_crash(AS_PARTICLE, "map_from_asval() sort on key failed");
	}

	p_map_mem->sz = (uint32_t)(mpk.ele_start_ptr - p_map_mem->data + content_length);

	if (order_index_is_valid(&mpk.value_idx)) {
		order_index_set(&mpk.value_idx, 0, ele_count);
	}

#ifdef MAP_DEBUG_VERIFY
	{
		as_bin b;
		b.particle = (as_particle *)p_map_mem;
		as_bin_state_set_from_type(&b, AS_PARTICLE_TYPE_MAP);
		if (! as_bin_verify(&b)) {
			const map_mem *p = p_map_mem;
			cf_warning(AS_PARTICLE, "map_from_asval(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
			char buf[4096];
			print_hex(p->data, p->sz, buf, 4096);
			cf_warning(AS_PARTICLE, "map_from_asval(): buf=%s", buf);
		}
	}
#endif
}

as_val *
map_to_asval(const as_particle *p)
{
	as_buffer buf;
	as_buffer_init(&buf);

	map_mem *p_map_mem = (map_mem *)p;

	buf.data = p_map_mem->data;
	buf.capacity = p_map_mem->sz;
	buf.size = p_map_mem->sz;

	as_serializer s;
	as_msgpack_init(&s);

	as_val *val = NULL;

	as_serializer_deserialize(&s, &buf, &val);
	as_serializer_destroy(&s);

	if (val) {
		packed_map_op op;

		packed_map_op_init_from_particle(&op, p);
		((as_map *)val)->flags = (uint32_t)op.pmi.flags;
	}

	return val;
}

uint32_t
map_asval_wire_size(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	return size;
}

uint32_t
map_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_presized(&s, val, wire);

	as_serializer_destroy(&s);

	return size;
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
map_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	return (uint32_t)sizeof(map_mem) + packed_size;
}

void
map_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	map_mem *p_map_mem = (map_mem *)*pp;

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = packed_size;
	memcpy(p_map_mem->data, packed, p_map_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
map_size_from_flat(const uint8_t *flat, uint32_t flat_size)
{
	// TODO - maybe never used
	return -1;
}

int
map_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Cast temp buffer from disk to data-not-in-memory.
	map_flat *p_map_flat = (map_flat *)flat;

	// This assumes map_flat is the same as map_mem.
	*pp = (as_particle *)p_map_flat;

	return 0;
}

int
map_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	const map_flat *p_map_flat = (const map_flat *)flat;
	packed_map_op op;

	packed_map_op_init(&op, p_map_flat->data, p_map_flat->size);

	if (! packed_map_op_unpack_hdridx(&op)) {
		return -1;
	}

	if (op.pmi.flags == 0) {
		// Convert temp buffer from disk to data-in-memory.
		map_mem *p_map_mem = cf_malloc_ns(sizeof(map_mem) + p_map_flat->size);

		p_map_mem->type = p_map_flat->type;
		p_map_mem->sz = p_map_flat->size;
		memcpy(p_map_mem->data, p_map_flat->data, p_map_mem->sz);

		*pp = (as_particle *)p_map_mem;

		return 0;
	}

	const uint8_t *content_ptr = op.packed + op.ele_start;
	uint32_t content_sz = op.packed_sz - op.ele_start;
	uint8_t flags = map_adjust_incoming_flags(op.pmi.flags);
	map_packer mpk;

	map_packer_init(&mpk, op.ele_count, flags, content_sz);

	as_particle *p = map_packer_create_particle(&mpk, NULL);

	if (! p) {
		return -1;
	}

	map_packer_write_hdridx(&mpk);
	memcpy(mpk.write_ptr, content_ptr, content_sz);

	if (offset_index_is_valid(&mpk.offset_idx)) {
		offset_index_set_filled(&mpk.offset_idx, 1);

		if (! offset_index_fill(&mpk.offset_idx, op.ele_count)) {
			cf_free(p);
			return -1;
		}
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (! order_index_set_sorted(&mpk.value_idx, &op.pmi.offset_idx, content_ptr, content_sz, SORT_BY_VALUE)) {
			cf_free(p);
			return -1;
		}
	}

	*pp = p;

	return 0;
}

uint32_t
map_flat_size(const as_particle *p)
{
	const map_mem *p_map_mem = (const map_mem *)p;

	packed_map_op op;
	packed_map_op_init_from_particle(&op, p);

	if (op.pmi.flags == 0) {
		return sizeof(map_flat) + p_map_mem->sz;
	}

	uint32_t sz = p_map_mem->sz - op.ele_start;
	sz += as_pack_list_header_get_size(op.ele_count + 1);
	sz += 3 + 1;	// 3 for min ext hdr and 1 for nil pair

	return (uint32_t)sizeof(map_flat) + sz;
}

uint32_t
map_to_flat(const as_particle *p, uint8_t *flat)
{
	map_flat *p_map_flat = (map_flat *)flat;

	int64_t ret = packed_map_strip_indexes(p_map_flat->data, p, true);

	if (ret < 0) {
		cf_crash(AS_PARTICLE, "map_to_flat() strip indexes failed with ret=%ld", ret);
	}

	p_map_flat->size = (uint32_t)ret;

	// Already wrote the type.

	return sizeof(map_flat) + p_map_flat->size;
}


//==========================================================
// as_bin particle functions specific to MAP.
//

void
as_bin_particle_map_set_hidden(as_bin *b)
{
	// Caller must ensure this is called only for MAP particles.
	map_mem *p_map_mem = (map_mem *)b->particle;

	p_map_mem->type = AS_PARTICLE_TYPE_HIDDEN_MAP;

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_HIDDEN_MAP);
}


//==========================================================
// Local helpers.
//

static inline bool
is_map_type(uint8_t type)
{
	return type == AS_PARTICLE_TYPE_MAP;
}

static inline bool
is_k_ordered(uint8_t flags)
{
	return flags & AS_PACKED_MAP_FLAG_K_ORDERED;
}

static inline bool
is_kv_ordered(uint8_t flags)
{
	return (flags & AS_PACKED_MAP_FLAG_KV_ORDERED) == AS_PACKED_MAP_FLAG_KV_ORDERED;
}

static size_t
map_calc_ext_content_sz(uint8_t flags, uint32_t ele_count, uint32_t content_size)
{
	size_t size = 0;

	if (is_k_ordered(flags)) {
		offset_index offidx;

		offset_index_init(&offidx, NULL, ele_count, content_size);
		size += offset_index_size(&offidx);
	}

	if (is_kv_ordered(flags)) {
		order_index ordidx;

		order_index_init(&ordidx, NULL, ele_count);
		size += order_index_size(&ordidx);
	}

	return size;
}

static uint8_t
map_adjust_incoming_flags(uint8_t flags)
{
	static const uint8_t mask = AS_PACKED_MAP_FLAG_KV_ORDERED | AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX;

	if (is_k_ordered(flags)) {
		flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
	}

	if (is_kv_ordered(flags)) {
		flags |= AS_PACKED_MAP_FLAG_ORD_IDX;
	}

	return flags & mask;
}

static size_t
op_map_ext_content_sz(const packed_map_op *op)
{
	return map_calc_ext_content_sz(op->pmi.flags, op->ele_count, op->packed_sz - op->ele_start);
}

static inline bool
op_is_k_ordered(const packed_map_op *op)
{
	return is_k_ordered(op->pmi.flags);
}

static inline bool
op_is_kv_ordered(const packed_map_op *op)
{
	return is_kv_ordered(op->pmi.flags);
}

static inline bool
has_offidx(const packed_map_op *op)
{
	return offset_index_is_valid(&op->pmi.offset_idx);
}

static inline bool
has_full_offidx(const packed_map_op *op)
{
	return offset_index_is_full(&op->pmi.offset_idx);
}

static void
as_pack_init_indexes(as_packer *pk, const packed_map_op *op)
{
	offset_index *offidx = (offset_index *)&op->pmi.offset_idx;
	order_index *ordidx = (order_index *)&op->pmi.value_idx;
	uint8_t *ptr = pk->buffer + pk->offset;

	if (op_is_k_ordered(op)) {
		uint32_t content_size = op->packed_sz - op->ele_start;

		offset_index_init(offidx, ptr, op->ele_count, content_size);

		size_t offidx_sz = offset_index_size(offidx);

		ptr += offidx_sz;
		offset_index_set_filled(offidx, 1);
		pk->offset += (int)offidx_sz;
	}

	if (op_is_kv_ordered(op)) {
		order_index_init(ordidx, ptr, op->ele_count);
		order_index_set(ordidx, 0, op->ele_count);
		pk->offset += (int)order_index_size(ordidx);
	}
}

static inline bool
skip_map_pair(as_unpacker *pk)
{
	if (as_unpack_size(pk) < 0) {
		return false;
	}

	if (as_unpack_size(pk) < 0) {
		return false;
	}

	return true;
}

static int
qsort_r_compare32(const void *a, const void *b, void *arg)
{
    uint32_t *p = (uint32_t *)a;
    uint32_t *q = (uint32_t *)b;

    return *p - *q;
}

//------------------------------------------------
// as_bin
//

static inline void
as_bin_set_empty_packed_map(as_bin *b, rollback_alloc *alloc_buf, uint8_t flags)
{
	b->particle = packed_map_create(alloc_buf, 0, NULL, 0, AS_PACKED_MAP_FLAG_PRESERVE_ORDER);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_MAP);
}

static inline void
as_bin_create_temp_packed_map_if_notinuse(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		b->particle = (as_particle *)&map_mem_empty;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_MAP);
	}
}

static inline bool
as_bin_is_temp_packed_map(const as_bin *b)
{
	return b->particle == (const as_particle *)&map_mem_empty;
}

//------------------------------------------------
// as_particle
//

static void
as_particle_set_empty_flagged_map(as_particle *p, uint64_t flags)
{
	map_mem *p_map_mem = (map_mem *)p;

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = sizeof(msgpack_empty_flagged_map);
	memcpy(p_map_mem->data, &msgpack_empty_flagged_map, sizeof(msgpack_empty_flagged_map));

	map_mem_empty_flagged *p_map_mem_empty_flagged = (map_mem_empty_flagged *)p_map_mem->data;
	uint8_t all_flags = AS_PACKED_MAP_FLAG_KV_ORDERED;

	p_map_mem_empty_flagged->ext_flags = (uint8_t) flags & all_flags;

	if (is_k_ordered(p_map_mem_empty_flagged->ext_flags)) {
		p_map_mem_empty_flagged->ext_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;

		if (p_map_mem_empty_flagged->ext_flags & AS_PACKED_MAP_FLAG_V_ORDERED) {
			p_map_mem_empty_flagged->ext_flags |= AS_PACKED_MAP_FLAG_ORD_IDX;
		}
	}
}

//------------------------------------------------
// cdt_payload
//

static void
cdt_payload_pack_val(cdt_payload *value, const as_val *val)
{
	as_serializer ser;
	as_msgpack_init(&ser);

	value->size = as_serializer_serialize_presized(&ser, val, (uint8_t *)value->ptr);

	as_serializer_destroy(&ser);
}

static inline void
cdt_payload_pack_int(cdt_payload *packed, int64_t value)
{
	as_integer val;
	as_integer_init(&val, value);

	return cdt_payload_pack_val(packed, (as_val *)&val);
}

static inline void
cdt_payload_pack_double(cdt_payload *packed, double value)
{
	as_double val;
	as_double_init(&val, value);

	return cdt_payload_pack_val(packed, (as_val *)&val);
}

//------------------------------------------------
// as_packed_map_index

static void
as_packed_map_index_init(as_packed_map_index *pmi, uint32_t ele_count, uint32_t content_size)
{
	offset_index_init(&pmi->offset_idx, NULL, ele_count, content_size);
	order_index_init(&pmi->value_idx, NULL, ele_count);
	pmi->flags = AS_PACKED_MAP_FLAG_NONE;
}

//------------------------------------------------
// map_packer

static as_particle *
map_packer_create_particle(map_packer *pk, rollback_alloc *alloc_buf)
{
	uint32_t size = pk->ext_size + pk->content_size + as_pack_map_header_get_size(pk->ele_count + (pk->flags ? 1 : 0));
	map_mem *p_map_mem = (map_mem *)(alloc_buf
			? rollback_alloc_reserve(alloc_buf, sizeof(map_mem) + size)
			: cf_malloc(sizeof(map_mem) + size)); // response, so not cf_malloc_ns()

	if (! p_map_mem) {
		return NULL;
	}

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = size;
	pk->write_ptr = p_map_mem->data;

	return (as_particle *)p_map_mem;
}

static void
map_packer_init(map_packer *pk, uint32_t ele_count, uint8_t flags, uint32_t content_size)
{
	pk->ele_count = ele_count;
	pk->content_size = content_size;
	pk->index_size = 0;

	offset_index_init(&pk->offset_idx, NULL, ele_count, content_size);

	if (flags & AS_PACKED_MAP_FLAG_OFF_IDX) {
		pk->index_size += offset_index_size(&pk->offset_idx);
	}

	order_index_init(&pk->value_idx, NULL, ele_count);

	if (flags & AS_PACKED_MAP_FLAG_ORD_IDX) {
		pk->index_size += order_index_size(&pk->value_idx);
	}

	pk->flags = flags;

	if (flags == AS_PACKED_MAP_FLAG_NONE) {
		pk->ext_header_size = 0;
		pk->ext_size = 0;
	}
	else {
		pk->ext_header_size = as_pack_ext_header_get_size(pk->index_size);
		pk->ext_size = pk->ext_header_size + pk->index_size + 1;	// +1 for packed nil
	}

	pk->write_ptr = NULL;
	pk->ele_start_ptr = NULL;
}

static bool
map_packer_setup_bin(map_packer *pk, as_bin *b, rollback_alloc *alloc_buf)
{
	as_particle *p = map_packer_create_particle(pk, alloc_buf);

	if (! p) {
		return false;
	}

	b->particle = p;

	return true;
}

static void
map_packer_write_hdridx(map_packer *pk)
{
	as_packer write = {
			.head = NULL,
			.tail = NULL,
			.buffer = pk->write_ptr,
			.offset = 0,
			.capacity = INT_MAX
	};

	as_pack_map_header(&write, pk->ele_count + (pk->flags == AS_PACKED_MAP_FLAG_NONE ? 0 : 1));

	if (pk->flags == AS_PACKED_MAP_FLAG_NONE) {
		pk->write_ptr += write.offset;
		pk->ele_start_ptr = pk->write_ptr;

		return;
	}

	as_pack_ext_header(&write, pk->index_size, pk->flags);

	if (pk->index_size > 0) {
		uint8_t *ptr = pk->write_ptr + write.offset;
		size_t index_size_left = pk->index_size;
		size_t size = offset_index_size(&pk->offset_idx);

		if ((pk->flags & AS_PACKED_MAP_FLAG_OFF_IDX) && index_size_left >= size) {
			offset_index_set_ptr(&pk->offset_idx, ptr, ptr + pk->index_size + 1);	// +1 for nil pair
			ptr += size;
			index_size_left -= size;
		}

		size = order_index_size(&pk->value_idx);

		if ((pk->flags & AS_PACKED_MAP_FLAG_ORD_IDX) && index_size_left >= size) {
			order_index_set_ptr(&pk->value_idx, ptr);
		}
	}

	// Pack nil.
	write.offset += pk->index_size;
	write.buffer[write.offset++] = msgpack_nil[0];

	pk->write_ptr += write.offset;
	pk->ele_start_ptr = pk->write_ptr;
}

static bool
map_packer_fill_offset_index(map_packer *mpk)
{
	if (offset_index_is_null(&mpk->offset_idx)) {
		return true;
	}

	offset_index_set_filled(&mpk->offset_idx, 1);

	return offset_index_fill(&mpk->offset_idx, mpk->ele_count);
}

// qsort_r callback function.
static int
map_packer_fill_index_sort_compare(const void *x, const void *y, void *p)
{
	index_sort_userdata *udata = (index_sort_userdata *)p;

	if (udata->error) {
		return 0;
	}

	order_index *order = udata->order;
	uint32_t a = order_index_ptr2value(order, x);
	uint32_t b = order_index_ptr2value(order, y);

	if (udata->sort_by == SORT_BY_IDX) {
		if (a < b) {
			return -1;
		}

		if (a == b) {
			return 0;
		}

		return 1;
	}

	const offset_index *offsets = udata->offsets;
	const uint8_t *buf = udata->packed;
	uint32_t len = udata->packed_sz;
	uint32_t x_off = offset_index_get_const(offsets, a);
	uint32_t y_off = offset_index_get_const(offsets, b);

	as_unpacker x_pk = {
			.buffer = buf + x_off,
			.offset = 0,
			.length = len - x_off
	};

	as_unpacker y_pk = {
			.buffer = buf + y_off,
			.offset = 0,
			.length = len - y_off
	};

	if (udata->sort_by == SORT_BY_VALUE) {
		// Skip keys.
		if (as_unpack_size(&x_pk) < 0) {
			udata->error = true;
			return 0;
		}

		if (as_unpack_size(&y_pk) < 0) {
			udata->error = true;
			return 0;
		}
	}

	msgpack_compare_t cmp = as_unpack_compare(&x_pk, &y_pk);

	if (cmp == MSGPACK_COMPARE_EQUAL) {
		if (udata->sort_by == SORT_BY_KEY) {
			if ((cmp = as_unpack_compare(&x_pk, &y_pk)) == MSGPACK_COMPARE_EQUAL) {
				return 0;
			}
		}
		else {
			return 0;
		}
	}

	if (cmp == MSGPACK_COMPARE_LESS) {
		return -1;
	}

	if (cmp == MSGPACK_COMPARE_GREATER) {
		return 1;
	}

	udata->error = true;
	return 0;
}

static bool
map_packer_fill_v_index(map_packer *mpk, const uint8_t *ele_start_ptr, uint32_t tot_ele_sz)
{
	if (order_index_is_null(&mpk->value_idx)) {
		return true;
	}

	return order_index_set_sorted(&mpk->value_idx, &mpk->offset_idx, ele_start_ptr, tot_ele_sz, SORT_BY_VALUE);
}

static bool
map_packer_copy_index(map_packer *pk, const packed_map_op *op, map_ele_find *remove_info, const map_ele_find *add_info, uint32_t kv_size)
{
	// No elements left.
	if (op->new_ele_count == 0) {
		return true;
	}

	if (offset_index_is_valid(&pk->offset_idx)) {
		if (! packed_map_op_write_dk_index(op, remove_info, add_info, &pk->offset_idx, kv_size)
				&& ! map_packer_fill_offset_index(pk)) {
			return false;
		}
	}

	if (order_index_is_valid(&pk->value_idx)) {
		if (remove_info->found_key && order_index_is_filled(&op->pmi.value_idx)) {
			if (! packed_map_op_find_rank_indexed(op, remove_info)) {
				cf_warning(AS_PARTICLE, "map_packer_copy_index() remove_info find rank failed");
				return false;
			}

			if (! remove_info->found_value) {
				cf_warning(AS_PARTICLE, "map_packer_copy_index() remove_info rank not found: idx=%u found=%d ele_count=%u", remove_info->idx, remove_info->found_key, op->ele_count);
				return false;
			}
		}

		if (! packed_map_op_write_dv_index(op, remove_info, add_info, &pk->value_idx)
				&& ! map_packer_fill_v_index(pk, pk->ele_start_ptr, pk->content_size)) {
			return false;
		}
	}

	return true;
}

static inline void
map_packer_write_seg1(map_packer *pk, const packed_map_op *op)
{
	pk->write_ptr = packed_map_op_write_seg1(op, pk->write_ptr);
}

static inline void
map_packer_write_seg2(map_packer *pk, const packed_map_op *op)
{
	pk->write_ptr = packed_map_op_write_seg2(op, pk->write_ptr);
}

static inline void
map_packer_write_msgpack_seg(map_packer *pk, const cdt_payload *seg)
{
	memcpy(pk->write_ptr, seg->ptr, seg->size);
	pk->write_ptr += seg->size;
}

//------------------------------------------------
// packed_map

static int
packed_map_set_flags(as_bin *b, rollback_alloc *alloc_buf, as_bin *result, uint8_t set_flags)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint8_t map_flags = op.pmi.flags;
	uint32_t ele_count = op.ele_count;
	bool reorder = false;

	if ((set_flags & AS_PACKED_MAP_FLAG_KV_ORDERED) == AS_PACKED_MAP_FLAG_V_ORDERED) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() invalid flags 0x%x", set_flags);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (is_kv_ordered(set_flags)) {
		if (! is_kv_ordered(map_flags)) {
			if (ele_count > 1 && ! is_k_ordered(map_flags)) {
				reorder = true;
			}

			map_flags |= AS_PACKED_MAP_FLAG_KV_ORDERED;
			map_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
			map_flags |= AS_PACKED_MAP_FLAG_ORD_IDX;
		}
	}
	else if (is_k_ordered(set_flags)) {
		if (is_kv_ordered(map_flags)) {
			map_flags &= ~AS_PACKED_MAP_FLAG_V_ORDERED;
			map_flags &= ~AS_PACKED_MAP_FLAG_ORD_IDX;
		}
		else if (! is_k_ordered(map_flags)) {
			if (ele_count > 1) {
				reorder = true;
			}

			map_flags |= AS_PACKED_MAP_FLAG_K_ORDERED;
			map_flags |= AS_PACKED_MAP_FLAG_OFF_IDX;
		}
	}
	else if ((set_flags & AS_PACKED_MAP_FLAG_KV_ORDERED) == 0) {
		map_flags &= ~AS_PACKED_MAP_FLAG_KV_ORDERED;
		map_flags &= ~AS_PACKED_MAP_FLAG_OFF_IDX;
		map_flags &= ~AS_PACKED_MAP_FLAG_ORD_IDX;
	}

	map_packer mpk;
	uint32_t content_length = op.packed_sz - op.ele_start;

	map_packer_init(&mpk, (uint32_t)ele_count, map_flags, content_length);

	if (! map_packer_setup_bin(&mpk, b, alloc_buf)) {
		cf_warning(AS_PARTICLE, "packed_map_set_flags() failed to alloc map particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	map_packer_write_hdridx(&mpk);

	if (reorder) {
		offset_index_inita_from_op_if_invalid(&op.pmi.offset_idx, &op);

		if (! packed_map_op_write_k_ordered(&op, mpk.write_ptr, &mpk.offset_idx)) {
			cf_warning(AS_PARTICLE, "packed_map_set_flags() sort on key failed, set_flags = 0x%x", set_flags);
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
	}
	else {
		memcpy(mpk.write_ptr, op.packed + op.ele_start, content_length);

		if (offset_index_is_valid(&mpk.offset_idx)) {
			if (offset_index_is_full(&op.pmi.offset_idx)) {
				offset_index_copy(&mpk.offset_idx, &op.pmi.offset_idx, 0, 0, ele_count, 0);
			}
			else if (! map_packer_fill_offset_index(&mpk)) {
				cf_warning(AS_PARTICLE, "packed_map_set_flags() fill index failed");
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (order_index_is_filled(&op.pmi.value_idx)) {
			order_index_copy(&mpk.value_idx, &op.pmi.value_idx, 0, 0, ele_count, NULL);
		}
		else {
			map_packer_fill_v_index(&mpk, mpk.ele_start_ptr, mpk.content_size);
		}
	}

#ifdef MAP_DEBUG_VERIFY
	if (! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_set_flags(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_set_flags(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_increment(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, const cdt_payload *delta_value, as_bin *result, bool is_decrement)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	map_ele_find find_key;
	map_ele_find_init(&find_key, &op);

	if (! packed_map_op_find_key(&op, &find_key, key, NULL)) {
		cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	int64_t incr_int;
	double incr_double;
	as_val_t delta_value_type;

	if (delta_value) {
		as_unpacker pk_delta_value = {
				.buffer = delta_value->ptr,
				.offset = 0,
				.length = (int)delta_value->size
		};

		delta_value_type = as_unpack_peek_type(&pk_delta_value);

		if (delta_value_type == AS_INTEGER) {
			if (as_unpack_int64(&pk_delta_value, &incr_int) != 0) {
				cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed delta value");
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}
		else if (delta_value_type == AS_DOUBLE) {
			if (as_unpack_double(&pk_delta_value, &incr_double) != 0) {
				cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed delta value");
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}
		else {
			cf_warning(AS_PARTICLE, "packed_map_increment() delta is not int/double");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
	}
	else {
		delta_value_type = AS_UNDEF;
		incr_int = 1;
		incr_double = 1;
	}

	if (is_decrement) {
		incr_int = -incr_int;
		incr_double = -incr_double;
	}

	int64_t map_int = 0;
	double map_double = 0;
	as_val_t map_value_type;
	uint8_t value_buf[sizeof(uint64_t) + 1];	// enough space for largest packed int/double
	cdt_payload value = {
			.ptr = value_buf,
			.size = 0
	};

	if (find_key.found_key) {
		as_unpacker pk_map_value;

		packed_map_op_init_unpacker(&op, &pk_map_value);
		pk_map_value.offset = find_key.value_offset;
		map_value_type = as_unpack_peek_type(&pk_map_value);

		if (map_value_type == AS_INTEGER) {
			if (as_unpack_int64(&pk_map_value, &map_int) != 0) {
				cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed int");
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (delta_value_type == AS_DOUBLE) {
				map_int += (int64_t)incr_double;
			}
			else {
				map_int += incr_int;
			}

			cdt_payload_pack_int(&value, map_int);
			as_bin_set_int(result, map_int);
		}
		else if (map_value_type == AS_DOUBLE) {
			if (as_unpack_double(&pk_map_value, &map_double) != 0) {
				cf_warning(AS_PARTICLE, "packed_map_increment() invalid packed double");
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (delta_value_type == AS_DOUBLE) {
				map_double += incr_double;
			}
			else {
				map_double += (double)incr_int;
			}

			cdt_payload_pack_double(&value, map_double);
			as_bin_set_double(result, map_double);
		}
		else {
			cf_warning(AS_PARTICLE, "packed_map_increment() only valid for int/double");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
	}
	else {
		if (delta_value_type == AS_DOUBLE) {
			map_double += incr_double;
			cdt_payload_pack_double(&value, map_double);
			as_bin_set_double(result, map_double);
		}
		else {
			map_int += incr_int;
			cdt_payload_pack_int(&value, map_int);
			as_bin_set_int(result, map_int);
		}
	}

	map_add_control control = {
			.allow_overwrite = true,
			.allow_create = true,
	};

	return packed_map_add(b, alloc_buf, key, &value, NULL, &control);
}

static int
packed_map_add(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, const cdt_payload *value, as_bin *result, const map_add_control *control)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_add() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (has_offidx(&op)) {
		offset_index_fill(&op.pmi.offset_idx, op.ele_count);
	}

	const cdt_payload *use_value = NULL;

	map_ele_find find_key_to_remove;
	map_ele_find_init(&find_key_to_remove, &op);

	if (! packed_map_op_find_key(&op, &find_key_to_remove, key, use_value)) {
		cf_warning(AS_PARTICLE, "packed_map_add() find key failed, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (find_key_to_remove.found_key) {
		// ADD for [unique] & [key exist].
		if (! control->allow_overwrite) {
			return -AS_PROTO_RESULT_FAIL_ELEMENT_EXISTS;
		}
	}
	else {
		// REPLACE for ![key exist].
		if (! control->allow_create) {
			return -AS_PROTO_RESULT_FAIL_ELEMENT_NOT_FOUND;
		}

		// Normal cases handled by packed_map_op_add():
		//  ADD for (![unique] & [key exist]) or ![key exist]
		//  PUT for all cases
		//  REPLACE for ([unique] & [key exist])
		//  UPDATE for ([unique] & [key exist]) or ![key exist]
	}

	int32_t new_size = packed_map_op_add(&op, &find_key_to_remove);

	if (new_size < 0) {
		cf_warning(AS_PARTICLE, "packed_map_add() failed with ret=%d, ele_count=%u", new_size, op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t content_size = (uint32_t)new_size + key->size + value->size;
	int32_t new_ele_count = op.new_ele_count;
	map_packer mpk;

	map_packer_init(&mpk, new_ele_count, op.pmi.flags, content_size);

	if (! map_packer_setup_bin(&mpk, b, alloc_buf)) {
		cf_warning(AS_PARTICLE, "packed_map_add() failed to alloc map particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	map_packer_write_hdridx(&mpk);

	map_ele_find find_value_to_add;

	map_ele_find_init(&find_value_to_add, &op);
	find_value_to_add.idx = find_key_to_remove.idx;	// Find closest matching position for multiple same values.

	if (order_index_is_valid(&mpk.value_idx) && order_index_is_filled(&op.pmi.value_idx)) {
		if (! packed_map_op_find_rank_by_value_indexed(&op, &find_value_to_add, value)) {
			cf_warning(AS_PARTICLE, "packed_map_add() find_value_to_add rank failed");
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	map_packer_write_seg1(&mpk, &op);
	map_packer_write_msgpack_seg(&mpk, key);
	map_packer_write_msgpack_seg(&mpk, value);
	map_packer_write_seg2(&mpk, &op);

	if (! map_packer_copy_index(&mpk, &op, &find_key_to_remove, &find_value_to_add, key->size + value->size)) {
		cf_warning(AS_PARTICLE, "packed_map_add() copy index failed");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (result) {
		as_bin_set_int(result, op.new_ele_count);
	}

#ifdef MAP_DEBUG_VERIFY
	if (! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_add(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_add(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_add_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, as_bin *result, const map_add_control *control)
{
	as_unpacker pk = {
			.buffer = items->ptr,
			.offset = 0,
			.length = items->size
	};

	int64_t items_count = as_unpack_map_header_element_count(&pk);

	if (items_count < 0) {
		cf_warning(AS_PARTICLE, "packed_map_add_items() invalid parameter, expected packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (items_count > 0 && as_unpack_peek_is_ext(&pk)) {
		if (! skip_map_pair(&pk)) {
			cf_warning(AS_PARTICLE, "packed_map_add_items() invalid parameter");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		items_count--;
	}

	rollback_alloc_inita(alloc_0, NULL, 1, false);
	rollback_alloc_inita(alloc_1, NULL, 1, false);

	int ret = AS_PROTO_RESULT_OK;

	for (int64_t i = 0; i < items_count; i++) {
		cdt_payload key = {
				.ptr = pk.buffer + pk.offset,
				.size = (uint32_t)pk.offset
		};

		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_add_items() invalid parameter");
			ret = -AS_PROTO_RESULT_FAIL_PARAMETER;
			break;
		}

		key.size = (uint32_t)pk.offset - key.size;

		cdt_payload value = {
				.ptr = pk.buffer + pk.offset,
				.size = (uint32_t)pk.offset
		};

		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_add_items() invalid parameter");
			ret = -AS_PROTO_RESULT_FAIL_PARAMETER;
			break;
		}

		value.size = (uint32_t)pk.offset - value.size;

		rollback_alloc *alloc_ptr;
		as_particle *old_particle = b->particle;

		if (i == items_count - 1) {
			alloc_ptr = alloc_buf;
		}
		else {
			alloc_ptr = alloc_0;
		}

		if ((ret = packed_map_add(b, alloc_ptr, &key, &value, result, control)) < 0) {
			break;
		}

		// Check for no-op.
		if (old_particle == b->particle) {
			if (i == items_count - 1) {
				// Must copy to non-temp alloc memory.
				map_mem *p_map_mem = (map_mem *)b->particle;
				size_t size = sizeof(map_mem) + p_map_mem->sz;
				b->particle = (as_particle *)rollback_alloc_reserve(alloc_buf, size);
				memcpy(b->particle, p_map_mem, size);
			}

			continue;
		}

		rollback_alloc_rollback(alloc_1);

		rollback_alloc *alloc_temp = alloc_0;

		alloc_0 = alloc_1;
		alloc_1 = alloc_temp;
	}

	rollback_alloc_rollback(alloc_0);
	rollback_alloc_rollback(alloc_1);

	return ret;
}

// Assumes remove_indexes ordered by idx.
static int
packed_map_remove_idxs(as_bin *b, const packed_map_op *op, rollback_alloc *alloc_buf, const order_index *remove_idxs, uint32_t count, uint32_t *removed)
{
	if (count == 0) {
		if (removed) {
			*removed = 0;
		}

		return AS_PROTO_RESULT_OK;
	}

	uint32_t ele_count = op->ele_count;
	uint32_t new_ele_count = ele_count;
	uint32_t remove_size = 0;
	const offset_index *offidx = &op->pmi.offset_idx;
	uint32_t prev_idx = ele_count;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(remove_idxs, i);

		if (idx == prev_idx) {
			cf_crash(AS_PARTICLE, "packed_map_remove_idxs requires non duplicate remove_idxs");
		}

		remove_size += offset_index_get_delta_const(offidx, idx);
		new_ele_count--;
		prev_idx = idx;
	}

	if (removed) {
		*removed = ele_count - new_ele_count;
	}

	uint32_t content_size = op->packed_sz - op->ele_start - remove_size;

	map_packer mpk;

	map_packer_init(&mpk, new_ele_count, op->pmi.flags, content_size);

	if (! map_packer_setup_bin(&mpk, b, alloc_buf)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_indexes() failed to alloc map particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	map_packer_write_hdridx(&mpk);

	uint32_t idx = order_index_get(remove_idxs, 0);
	uint32_t offset = offset_index_get_const(offidx, idx);
	const uint8_t *read_ptr = op->packed + op->ele_start;
	uint32_t size = offset;
	uint32_t delta = offset_index_get_delta_const(offidx, idx);

	memcpy(mpk.write_ptr, read_ptr, size);
	mpk.write_ptr += size;
	read_ptr += size + delta;

	for (uint32_t i = 1; i < count; i++) {
		idx = order_index_get(remove_idxs, i);

		uint32_t new_offset = offset_index_get_const(offidx, idx);

		size = new_offset - offset - delta;
		offset = new_offset;
		delta = offset_index_get_delta_const(offidx, idx);

		memcpy(mpk.write_ptr, read_ptr, (size_t)size);
		mpk.write_ptr += size;
		read_ptr += size + delta;
	}

	if (idx != ele_count - 1) {
		size = op->packed_sz - op->ele_start - offset - delta;
		memcpy(mpk.write_ptr, read_ptr, size);
		mpk.write_ptr += size;
	}

	if (offset_index_is_valid(&mpk.offset_idx)) {
		if (offset_index_is_full(offidx)) {
			offidx_op off_op;
			offidx_op_init(&off_op, &mpk.offset_idx, offidx);

			for (uint32_t i = 0; i < count; i ++) {
				uint32_t rem_idx = order_index_get(remove_idxs, i);
				offidx_op_remove(&off_op, rem_idx);
			}

			offidx_op_end(&off_op);
		}
		else {
			offset_index_set_filled(&mpk.offset_idx, new_ele_count);
		}
	}

	if (order_index_is_valid(&mpk.value_idx)) {
		if (order_index_is_filled(&op->pmi.value_idx)) {
			order_index_op_remove_indexes(&mpk.value_idx, &op->pmi.value_idx, remove_idxs, count);
		}
		else if (! order_index_set_sorted(&mpk.value_idx, &mpk.offset_idx, mpk.ele_start_ptr, mpk.content_size, SORT_BY_VALUE)) {
			cf_warning(AS_PARTICLE, "packed_map_remove_indexes() failed to sort new value_idex");
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_remove_by_key(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_key() invalid packed map, ele_count=%d", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_map_op_get_remove_by_key(&op, b, alloc_buf, key, result);
}

static int
packed_map_remove_by_key_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_key_interval() invalid packed map, ele_count=%d", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index_inita_from_op_if_invalid(&op.pmi.offset_idx, &op);

	return packed_map_op_get_remove_by_key_interval(&op, b, alloc_buf, key_start, key_end, result);
}

static int
packed_map_remove_by_index_range(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() invalid packed map index, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t uindex;
	uint32_t count32;

	if (! calc_index_count(index, count, op.ele_count, &uindex, &count32, result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() index %ld out of bounds for ele_count %u", index, op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index *offidx = &op.pmi.offset_idx;

	offset_index_inita_from_op_if_invalid(offidx, &op);

	return packed_map_op_get_remove_by_index_range(&op, b, alloc_buf, uindex, count32, result);
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
packed_map_remove_by_value_interval(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_value_interval() invalid packed map, ele_count=%d", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index *offidx = &op.pmi.offset_idx;
	offset_index_inita_from_op_if_invalid(offidx, &op);

	// Pre-populate index.
	if (! offset_index_fill(offidx, op.ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_value_interval() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_map_op_get_remove_by_value_interval(&op, b, alloc_buf, value_start, value_end, result);
}

static int
packed_map_remove_by_rank_range(as_bin *b, rollback_alloc *alloc_buf, int64_t rank, uint64_t count, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_index_range() invalid packed map index, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t urank;
	uint32_t count32;

	if (! calc_index_count(rank, count, op.ele_count, &urank, &count32, result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_rank_range() rank %ld out of bounds for ele_count %u", rank, op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index *offidx = &op.pmi.offset_idx;

	offset_index_inita_from_op_if_invalid(offidx, &op);

	if (! offset_index_fill(offidx, op.ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_by_rank_range() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_map_op_get_remove_by_rank_range(&op, b, alloc_buf, urank, count32, result);
}

static int
packed_map_remove_all_key_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items() invalid packed map, ele_count=%d", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	as_unpacker pk = {
			.buffer = items->ptr,
			.offset = 0,
			.length = items->size
	};
	int64_t items_count = as_unpack_list_header_element_count(&pk);

	if (items_count < 0) {
		cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items() invalid parameter, expected packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index_inita_from_op_if_invalid(&op.pmi.offset_idx, &op);

	if (op_is_k_ordered(&op)) {
		offset_index_fill(&op.pmi.offset_idx, op.ele_count);
	}

	uint32_t remove_array[items_count * 2];
	const cdt_payload *use_value = NULL;
	uint32_t ele_found = 0;

	for (int64_t i = 0; i < items_count; i++) {
		cdt_payload key = {
				.ptr = pk.buffer + pk.offset,
				.size = (uint32_t)pk.offset
		};

		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items() invalid parameter");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		key.size = (uint32_t)pk.offset - key.size;

		map_ele_find find_key;
		map_ele_find_init(&find_key, &op);

		if (! packed_map_op_find_key(&op, &find_key, &key, use_value)) {
			if (cdt_payload_is_int(&key)) {
				cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items() find key=%ld failed, ele_count=%d", cdt_payload_get_int64(&key), op.ele_count);
			}
			else {
				cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items() find key failed, ele_count=%d", op.ele_count);
			}

			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (! find_key.found_key) {
			continue;
		}

		remove_array[2 * ele_found] = find_key.idx;
		remove_array[2 * ele_found + 1] = 1;
		ele_found++;
	}

	uint32_t *return_array = NULL;
	size_t array_sz = sizeof(uint32_t) * 2 * ele_found;
	bool return_need_array = result_data_is_return_elements(result) || result_data_is_return_index(result);

	// Make a copy for return order.
	if (return_need_array) {
		return_array = alloca(array_sz);
		memcpy(return_array, remove_array, array_sz);
	}

	qsort_r(remove_array, ele_found, sizeof(uint32_t) * 2, qsort_r_compare32, NULL);

	order_index rem_idx;
	uint32_t ele_removed = 0;
	uint32_t prev = op.ele_count;
	bool is_prev = false;

	order_index_inita(&rem_idx, op.ele_count);

	for (int64_t i = 0; i < ele_found; i++) {
		uint32_t idx = remove_array[2 * i];

		if (idx == prev) {
			if (return_need_array && ! is_prev) {
				uint32_t j = 0;

				for (; j < ele_found; j++) {
					if (return_array[2 * j] == idx) {
						break;
					}
				}

				for (j++; j < ele_found; j++) {
					if (return_array[2 * j] == idx) {
						return_array[2 * j] = op.ele_count;
					}
				}
			}

			is_prev = true;
			continue;
		}

		uint32_t count = remove_array[2 * i + 1];

		for (uint32_t j = 0; j < count; j++) {
			order_index_set(&rem_idx, ele_removed++, idx + j);
		}

		prev = idx;
		is_prev = false;
	}

	offset_index_fill(&op.pmi.offset_idx, op.ele_count); // ensure fill for unordered case

	int ret = packed_map_remove_idxs(b, &op, alloc_buf, &rem_idx, ele_removed, NULL);

	if (ret < 0) {
		return ret;
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP: {
		order_index ret_idx;
		uint32_t ret_idx_count = 0;

		order_index_inita2(&ret_idx, op.ele_count, ele_removed);

		for (int64_t i = 0; i < ele_found; i++) {
			uint32_t idx = return_array[2 * i];

			if (idx >= op.ele_count) {
				continue;
			}

			uint32_t count = return_array[2 * i + 1];

			for (uint32_t j = 0; j < count; j++) {
				order_index_set(&ret_idx, ret_idx_count++, idx + j);
			}
		}

		if (! packed_map_op_build_ele_result_by_ele_idx(&op, &ret_idx, 0, ret_idx_count, result)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		break;
	}
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_INDEX: {
		order_index ret_index;
		uint32_t ret_index_count = 0;

		order_index_inita2(&ret_index, op.ele_count, ele_removed);

		for (int64_t i = 0; i < ele_found; i++) {
			uint32_t index = return_array[2 * i];

			if (index >= op.ele_count) {
				continue;
			}

			uint32_t count = return_array[2 * i + 1];

			if (! op_is_k_ordered(&op)) {
				cdt_payload key = {
						.ptr = op.packed + op.ele_start + offset_index_get_const(&op.pmi.offset_idx, index),
						.size = INT_MAX,
				};

				index = packed_map_op_find_index_by_key_unordered(&op, &key);
			}

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = op.ele_count - index - count;
			}

			for (uint32_t j = 0; j < count; j++) {
				order_index_set(&ret_index, ret_index_count++, index + j);
			}
		}

		result_data_set_ordered_list(result, &ret_index, ret_index_count);
		break;
	}
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, ele_removed);
		break;
	case RESULT_TYPE_REVRANK:	// not supported
	case RESULT_TYPE_RANK:		// not supported
	default:
		cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items() invalid return type %d", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_remove_all_key_items(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_remove_all_value_items(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *items, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items() invalid packed map, ele_count=%d", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	as_unpacker pk = {
			.buffer = items->ptr,
			.offset = 0,
			.length = items->size
	};
	int64_t items_count = as_unpack_list_header_element_count(&pk);

	if (items_count < 0) {
		cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items() invalid parameter, expected packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index_inita_from_op_if_invalid(&op.pmi.offset_idx, &op);

	uint32_t rem_idx_count = 0;
	bool return_rank = result_data_is_return_rank(result);
	order_index rem_idx;
	order_index *rem_ranks = NULL;
	order_index *rem_rank_counts = NULL;

	// Over allocate array to deal with possible dup parameters.
	order_index_inita2(&rem_idx, op.ele_count, 2 * op.ele_count);

	if (return_rank) {
		rem_ranks = (order_index *)alloca(sizeof(order_index));
		order_index_inita2(rem_ranks, op.ele_count, items_count);
		rem_rank_counts = (order_index *)alloca(sizeof(order_index));
		order_index_inita2(rem_rank_counts, op.ele_count, items_count);
	}

	order_index find_idx;

	if (! order_index_is_valid(&op.pmi.value_idx)) {
		order_index_inita(&find_idx, op.ele_count);
	}

	bool has_dups = false;

	for (int64_t i = 0; i < items_count; i++) {
		cdt_payload value = {
				.ptr = pk.buffer + pk.offset,
				.size = (uint32_t)pk.offset
		};

		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items() invalid parameter");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		value.size = (uint32_t)pk.offset - value.size;

		uint32_t rank = 0;
		uint32_t count = 0;

		if (order_index_is_valid(&op.pmi.value_idx)) {
			if (! packed_map_op_ensure_ordidx_filled(&op)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (! packed_map_op_find_rank_range_by_value_interval_indexed(&op, &value, &value, &rank, &count, result->is_multi)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			for (uint32_t j = 0; j < count; j++) {
				uint32_t idx = order_index_get(&op.pmi.value_idx, rank + j);
				order_index_set(&rem_idx, rem_idx_count++, idx);
			}
		}
		else {
			if (! packed_map_op_find_rank_range_by_value_interval_unordered(&op, &value, &value, &rank, &count, &find_idx)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			for (uint32_t j = 0; j < count; j++) {
				uint32_t idx = order_index_get(&find_idx, j);
				order_index_set(&rem_idx, rem_idx_count++, idx);
			}
		}

		// Must have encountered dups when rem_idx grows beyond op.ele_count.
		if (rem_idx_count > op.ele_count) {
			cf_detail(AS_PARTICLE, "packed_map_remove_all_value_items() dup list items reduces performance, i=%ld rem_idx_count=%u ele_count=%u", i, rem_idx_count, op.ele_count);
			rem_idx._.ele_count = rem_idx_count;

			if (! order_index_remove_dups(&rem_idx, NULL)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			rem_idx_count = rem_idx._.ele_count;
			has_dups = true;
		}

		if (return_rank) {
			order_index_set(rem_ranks, (size_t)i, rank);
			order_index_set(rem_rank_counts, (size_t)i, count);
		}
	}

	rem_idx._.ele_count = rem_idx_count;

	order_index sorted;
	order_index_inita_copy(&sorted, &rem_idx);

	if (! order_index_sort(&sorted, NULL, NULL, 0, SORT_BY_IDX)) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (order_index_sorted_has_dups(&sorted)) {
		// Remove duplicates.
		if (result_data_is_return_elements(result)) {
			order_index_remove_dups(&rem_idx, &sorted);
		}

		rem_idx_count = order_index_sorted_remove_dups(&sorted);
		has_dups = true;
	}

	uint32_t removed_count;
	int ret = packed_map_remove_idxs(b, &op, alloc_buf, &sorted, sorted._.ele_count, &removed_count);

	if (ret < 0) {
		return ret;
	}

	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP: {
		if (! packed_map_op_build_ele_result_by_ele_idx(&op, &rem_idx, 0, rem_idx_count, result)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		break;
	}
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_RANK: {
		if (has_dups) {
			for (uint32_t i = 0; i < items_count - 1; i++) {
				uint32_t rank = order_index_get(rem_ranks, i);

				if (rank == op.ele_count) {
					continue;
				}

				for (uint32_t j = i + 1; j < items_count; j++) {
					if (rank == order_index_get(rem_ranks, j)) {
						order_index_set(rem_ranks, j, op.ele_count);
						order_index_set(rem_rank_counts, j, 0);
					}
				}
			}
		}

		uint32_t rank_count_total = 0;

		for (uint32_t i = 0; i < items_count; i++) {
			rank_count_total += order_index_get(rem_rank_counts, i);
		}

		order_index rem_rank_out;
		uint32_t rem_rank_out_count = 0;

		order_index_inita2(&rem_rank_out, op.ele_count, rank_count_total);

		for (uint32_t i = 0; i < items_count; i++) {
			uint32_t rank = order_index_get(rem_ranks, i);

			if (rank == op.ele_count) {
				continue;
			}

			uint32_t count = order_index_get(rem_rank_counts, i);

			if (result->type == RESULT_TYPE_REVRANK) {
				rank = op.ele_count - rank - count;
			}

			for (uint32_t j = 0; j < count; j++) {
				order_index_set(&rem_rank_out, rem_rank_out_count++, rank + j);
			}
		}

		if (! result_data_set_ordered_list(result, &rem_rank_out, rank_count_total)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		break;
	}
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, removed_count);
		break;
	case RESULT_TYPE_REVINDEX:	// not supported
	case RESULT_TYPE_INDEX:		// not supported
	default:
		cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items() invalid return type %d", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

#ifdef MAP_DEBUG_VERIFY
	if (! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_remove_all_value_items(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_clear(as_bin *b, rollback_alloc *alloc_buf, as_bin *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_clear() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint8_t map_flags = op.pmi.flags;
	map_packer mpk;

	map_packer_init(&mpk, 0, map_flags, 0);

	if (! map_packer_setup_bin(&mpk, b, alloc_buf)) {
		cf_warning(AS_PARTICLE, "packed_map_clear() failed to alloc map particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	map_packer_write_hdridx(&mpk);

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_get_by_key(const as_bin *b, const cdt_payload *key, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_key() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_map_op_get_remove_by_key(&op, NULL, NULL, key, result);
}

static int
packed_map_get_by_key_interval(const as_bin *b, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_key_interval() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index_inita_from_op_if_invalid(&op.pmi.offset_idx, &op);

	return packed_map_op_get_remove_by_key_interval(&op, NULL, NULL, key_start, key_end, result);
}

static int
packed_map_get_by_index_range(const as_bin *b, int64_t index, uint64_t count, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_index_range() invalid packed map index, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t uindex;
	uint32_t count32;

	if (! calc_index_count(index, count, op.ele_count, &uindex, &count32, result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_index_range() index %ld out of bounds for ele_count %u", index, op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index *offidx = &op.pmi.offset_idx;

	offset_index_inita_from_op_if_invalid(offidx, &op);

	return packed_map_op_get_remove_by_index_range(&op, NULL, NULL, uindex, count32, result);
}

static int
packed_map_get_by_value_interval(const as_bin *b, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_value_interval() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index *offidx = &op.pmi.offset_idx;
	offset_index_inita_from_op_if_invalid(offidx, &op);

	// Pre-populate index.
	if (! offset_index_fill(offidx, op.ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_value_interval() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_map_op_get_remove_by_value_interval(&op, NULL, NULL, value_start, value_end, result);
}

// count == 0 means missing count.
// get_keys == true if getting keys rather than values.
static int
packed_map_get_by_rank_range(const as_bin *b, int64_t rank, uint64_t count, cdt_result_data *result)
{
	packed_map_op op;

	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_rank_range() invalid packed map, ele_count=%u", op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t urank;
	uint32_t count32;

	if (! calc_index_count(rank, count, op.ele_count, &urank, &count32, result->is_multi)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_rank_range() rank %ld out of bounds for ele_count %u", rank, op.ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	offset_index *offidx = &op.pmi.offset_idx;

	offset_index_inita_from_op_if_invalid(offidx, &op);

	if (! offset_index_fill(offidx, op.ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_get_by_rank_range() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_map_op_get_remove_by_rank_range(&op, NULL, NULL, urank, count32, result);
}

//------------------------------------------------
// packed_map_op

static void
packed_map_op_init(packed_map_op *op, const uint8_t *buf, uint32_t size)
{
	op->packed = buf;
	op->packed_sz = size;

	op->ele_count = 0;
	op->new_ele_count = 0;
	op->ele_removed = 0;

	op->ele_start = 0;
	op->seg1_size = 0;
	op->seg2_offset = 0;
	op->seg2_size = 0;

	op->key1_offset = 0;
	op->key1_size = 0;
	op->key2_offset = 0;
	op->key2_size = 0;
}

static inline bool
packed_map_op_init_from_particle(packed_map_op *op, const as_particle *p)
{
	const map_mem *p_map_mem = (const map_mem *)p;
	packed_map_op_init(op, p_map_mem->data, p_map_mem->sz);

	return packed_map_op_unpack_hdridx(op);
}

static bool
packed_map_op_init_from_bin(packed_map_op *op, const as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	if (! is_map_type(type)) {
		cf_crash(AS_PARTICLE, "as_packed_map_init_from_bin() invalid type %d", type);
	}

	return packed_map_op_init_from_particle(op, b->particle);
}

static bool
packed_map_op_unpack_hdridx(packed_map_op *op)
{
	as_unpacker pk = {
			.buffer = op->packed,
			.offset = 0,
			.length = (int)op->packed_sz
	};

	if (op->packed_sz == 0) {
		op->pmi.flags = 0;
		return false;
	}

	int64_t ele_count = as_unpack_map_header_element_count(&pk);

	if (ele_count < 0) {
		return false;
	}

	op->ele_count = (uint32_t)ele_count;

	as_packed_map_index *pmi = &op->pmi;

	if (ele_count > 0 && as_unpack_peek_is_ext(&pk)) {
		as_msgpack_ext ext;

		if (as_unpack_ext(&pk, &ext) != 0) {
			return false;
		}

		if (as_unpack_size(&pk) < 0) {	// skip the packed nil
			return false;
		}

		pmi->flags = ext.type;
		op->ele_count--;

		uint32_t ele_count = op->ele_count;
		uint32_t content_size = op->packed_sz - (uint32_t)pk.offset;

		offset_index_init(&pmi->offset_idx, NULL, ele_count, content_size);
		order_index_init(&pmi->value_idx, NULL, ele_count);

		size_t index_size_left = (size_t)ext.size;
		uint8_t *ptr = (uint8_t *)ext.data;
		size_t size = offset_index_size(&pmi->offset_idx);

		if ((pmi->flags & AS_PACKED_MAP_FLAG_OFF_IDX) && index_size_left >= size) {
			offset_index_set_ptr(&pmi->offset_idx, ptr, op->packed + pk.offset);
			ptr += size;
			index_size_left -= size;
		}

		size = order_index_size(&pmi->value_idx);

		if ((pmi->flags & AS_PACKED_MAP_FLAG_ORD_IDX) && index_size_left >= size) {
			order_index_set_ptr(&pmi->value_idx, ptr);
		}
	}
	else {
		uint32_t content_size = op->packed_sz - (uint32_t)pk.offset;

		as_packed_map_index_init(pmi, op->ele_count, content_size);
	}

	op->ele_start = (uint32_t)pk.offset;

	return true;
}

static inline void
packed_map_op_init_unpacker(const packed_map_op *op, as_unpacker *pk)
{
	pk->buffer = op->packed + op->ele_start;
	pk->offset = 0;
	pk->length = (int)op->packed_sz - op->ele_start;
}

static bool
packed_map_op_ensure_ordidx_filled(const packed_map_op *op)
{
	order_index *ordidx = (order_index *)&op->pmi.value_idx;

	if (! order_index_is_filled(ordidx)) {
		offset_index *offidx = (offset_index *)&op->pmi.offset_idx;

		if (! offset_index_fill(offidx, op->ele_count)) {
			cf_warning(AS_PARTICLE, "packed_map_op_ensure_ordidx_filled() failed to fill offset_idx");
			return false;
		}

		return order_index_set_sorted(ordidx, &op->pmi.offset_idx, op->packed + op->ele_start, op->packed_sz - op->ele_start, SORT_BY_VALUE);
	}

	return true;
}

static uint32_t
packed_map_op_find_index_by_idx_unordered(const packed_map_op *op, uint32_t idx)
{
	uint32_t pk_offset = op->ele_start + offset_index_get_const(&op->pmi.offset_idx, idx);
	cdt_payload key = {
			.ptr = op->packed + pk_offset,
			.size = op->packed_sz - pk_offset
	};

	return packed_map_op_find_index_by_key_unordered(op, &key);
}

static uint32_t
packed_map_op_find_index_by_key_unordered(const packed_map_op *op, const cdt_payload *key)
{
	as_unpacker pk_key = {
			.buffer = key->ptr,
			.offset = 0,
			.length = (int)key->size
	};
	as_unpacker pk;
	uint32_t index = 0;

	packed_map_op_init_unpacker(op, &pk);

	for (uint32_t i = 0; i < op->ele_count; i++) {
		pk_key.offset = 0;
		msgpack_compare_t cmp = as_unpack_compare(&pk, &pk_key);

		if (cmp == MSGPACK_COMPARE_ERROR) {
			return op->ele_count;
		}

		if (cmp == MSGPACK_COMPARE_LESS) {
			index++;
		}

		if (as_unpack_size(&pk) < 0) {
			return op->ele_count;
		}
	}

	return index;
}

static void
packed_map_op_find_rank_indexed_linear(const packed_map_op *op, map_ele_find *find, uint32_t start, uint32_t len)
{
	uint32_t rank = order_index_find_idx(&op->pmi.value_idx, find->idx, start, len);

	if (rank < start + len) {
		find->found_value = true;
		find->rank = rank;
	}
}

// Find rank given index (find->idx).
// Return true on success.
static bool
packed_map_op_find_rank_indexed(const packed_map_op *op, map_ele_find *find)
{
	uint32_t ele_count = op->ele_count;

	if (ele_count == 0) {
		return true;
	}

	if (find->idx >= ele_count) {
		find->found_value = false;
		return true;
	}

	const offset_index *offset_idx = &op->pmi.offset_idx;
	const order_index *value_idx = &op->pmi.value_idx;

	uint32_t rank = ele_count / 2;
	uint32_t upper = ele_count;
	uint32_t lower = 0;
	as_unpacker pk_value = {
			.buffer = op->packed + op->ele_start + find->value_offset,
			.offset = 0,
			.length = (int)(find->key_offset + find->size - find->value_offset)
	};

	find->found_value = false;

	while (true) {
		if (upper - lower < LINEAR_FIND_RANK_MAX_COUNT) {
			packed_map_op_find_rank_indexed_linear(op, find, lower, upper - lower);
			return true;
		}

		uint32_t idx = order_index_get(value_idx, rank);

		if (find->idx == idx) {
			find->found_value = true;
			find->rank = rank;
			break;
		}

		as_unpacker pk_buf = {
				.buffer = op->packed,
				.offset = (int)offset_index_get_const(offset_idx, idx) + op->ele_start,
				.length = (int)op->packed_sz
		};

		if (as_unpack_size(&pk_buf) < 0) {	// skip key
			cf_warning(AS_PARTICLE, "packed_map_op_find_rank_indexed() unpack key failed at rank=%u", rank);
			return false;
		}

		pk_value.offset = 0; // reset

		msgpack_compare_t cmp = as_unpack_compare(&pk_value, &pk_buf);

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			if (find->idx < idx) {
				cmp = MSGPACK_COMPARE_LESS;
			}
			else if (find->idx > idx) {
				cmp = MSGPACK_COMPARE_GREATER;
			}

			find->found_value = true;
		}

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			find->rank = rank;
			break;
		}

		if (cmp == MSGPACK_COMPARE_GREATER) {
			if (rank >= upper - 1) {
				find->rank = rank + 1;
				break;
			}

			lower = rank + 1;
			rank += upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			if (rank == lower) {
				find->rank = rank;
				break;
			}

			upper = rank;
			rank += lower;
			rank /= 2;
		}
		else {
			cf_warning(AS_PARTICLE, "packed_map_op_find_rank_indexed() error=%d lower=%u rank=%u upper=%u", (int)cmp, lower, rank, upper);
			return false;
		}
	}

	return true;
}

// Find (closest) rank given value.
// Find closest rank for find->idx (0 means first instance of value).
// Return true on success.
static bool
packed_map_op_find_rank_by_value_indexed(const packed_map_op *op, map_ele_find *find, const cdt_payload *value)
{
	const offset_index *offset_idx = &op->pmi.offset_idx;
	const order_index *value_idx = &op->pmi.value_idx;
	uint32_t ele_count = op->ele_count;

	find->found_value = false;

	if (ele_count == 0) {
		return true;
	}

	uint32_t rank = ele_count / 2;
	as_unpacker pk_value = {
			.buffer = value->ptr,
			.offset = 0,
			.length = (int)value->size
	};

	while (true) {
		uint32_t idx = order_index_get(value_idx, rank);
		uint32_t pk_offset = offset_index_get_const(offset_idx, idx) + op->ele_start;
		uint32_t len = op->packed_sz - pk_offset;
		as_unpacker pk_buf = {
				.buffer = op->packed + pk_offset,
				.offset = 0,
				.length = (int)len
		};

		if (as_unpack_size(&pk_buf) < 0) {	// skip key
			return false;
		}

		pk_value.offset = 0; // reset

		msgpack_compare_t cmp = as_unpack_compare(&pk_value, &pk_buf);

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			if (find->idx < idx) {
				cmp = MSGPACK_COMPARE_LESS;
			}
			else if (find->idx > idx) {
				cmp = MSGPACK_COMPARE_GREATER;
			}

			find->found_value = true;
		}

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			find->found_value = true;
			find->rank = rank;
			break;
		}

		if (cmp == MSGPACK_COMPARE_GREATER) {
			if (rank >= find->upper - 1) {
				find->rank = rank + 1;
				break;
			}

			find->lower = rank + 1;
			rank += find->upper;
			rank /= 2;
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			if (rank == find->lower) {
				find->rank = rank;
				break;
			}

			find->upper = rank;
			rank += find->lower;
			rank /= 2;
		}
		else {
			return false;
		}
	}

	return true;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static bool
packed_map_op_find_rank_range_by_value_interval_indexed(const packed_map_op *op, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, bool is_multi)
{
	if (! has_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_indexed() offset_index needs to be valid");
	}

	map_ele_find find_start;

	map_ele_find_init(&find_start, op);
	find_start.idx = 0;	// find least ranked entry with value == value_start

	if (! packed_map_op_find_rank_by_value_indexed(op, &find_start, value_start)) {
		cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_indexed() invalid packed map");
		return false;
	}

	*rank = find_start.rank;
	*count = 1;

	if (! value_end) {
		*count = op->ele_count - *rank;
	}
	else {
		map_ele_find find_end;

		map_ele_find_init(&find_end, op);

		if (value_end != value_start) {
			find_end.idx = 0;

			if (! packed_map_op_find_rank_by_value_indexed(op, &find_end, value_end)) {
				cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_indexed() invalid packed map");
				return false;
			}

			*count = (find_end.rank > find_start.rank) ? find_end.rank - find_start.rank : 0;
		}
		else {
			if (! find_start.found_value) {
				*count = 0;
			}
			else if (is_multi) {
				find_end.idx = op->ele_count;	// find highest ranked entry with value == value_start

				if (! packed_map_op_find_rank_by_value_indexed(op, &find_end, value_start)) {
					cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_indexed() invalid packed map");
					return false;
				}

				*count = find_end.rank - find_start.rank;
			}
		}
	}

	return true;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static bool
packed_map_op_find_rank_range_by_value_interval_unordered(const packed_map_op *op, const cdt_payload *value_start, const cdt_payload *value_end, uint32_t *rank, uint32_t *count, order_index *ordidx)
{
	if (! has_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_unordered() offset_index needs to be valid");
	}

	as_unpacker pk_start = {
			.buffer = value_start->ptr,
			.offset = 0,
			.length = (int)value_start->size
	};
	as_unpacker pk_end = {
			.buffer = value_end ? value_end->ptr : NULL,
			.offset = 0,
			.length = value_end ? (int)value_end->size : 0,
	};

	// Pre-check parameters.
	if (as_unpack_size(&pk_start) < 0) {
		cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_unordered() invalid start value");
		return false;
	}

	if (value_end != value_start) {
		// Pre-check parameters.
		if (value_end && as_unpack_size(&pk_end) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_unordered() invalid end value");
			return false;
		}
	}

	*rank = 0;
	*count = 0;

	offset_index *offidx = (offset_index *)&op->pmi.offset_idx;
	as_unpacker pk;
	packed_map_op_init_unpacker(op, &pk);

	for (uint32_t i = 0; i < op->ele_count; i++) {
		offset_index_set(offidx, i, (uint32_t)pk.offset);

		if (as_unpack_size(&pk) < 0) {	// skip key
			cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_unordered() invalid packed map at index %u", i);
			return false;
		}

		int value_offset = pk.offset;	// save for pk_end

		pk_start.offset = 0;	// reset

		msgpack_compare_t cmp_start = as_unpack_compare(&pk, &pk_start);

		if (cmp_start == MSGPACK_COMPARE_ERROR) {
			cf_warning(AS_PARTICLE, "packed_map_op_find_rank_range_by_value_interval_unordered() invalid packed map at index %u", i);
			return false;
		}

		if (cmp_start == MSGPACK_COMPARE_LESS) {
			(*rank)++;
		}
		else if (value_start != value_end) {
			msgpack_compare_t cmp_end = MSGPACK_COMPARE_LESS;

			// NULL value_end means largest possible value.
			if (value_end) {
				pk.offset = value_offset;
				pk_end.offset = 0;
				cmp_end = as_unpack_compare(&pk, &pk_end);
			}

			if (cmp_end == MSGPACK_COMPARE_LESS) {
				if (ordidx) {
					order_index_set(ordidx, *count, i);
				}

				(*count)++;
			}
		}
		// Single value case.
		else if (cmp_start == MSGPACK_COMPARE_EQUAL) {
			if (ordidx) {
				order_index_set(ordidx, *count, i);
			}

			(*count)++;
		}
	}

	offset_index_set_filled(offidx, op->ele_count);

	return true;
}

// Find key given list index.
// Return true on success.
static bool
packed_map_op_find_key_indexed(const packed_map_op *op, map_ele_find *find, const cdt_payload *key, const cdt_payload *value)
{
	const offset_index *offidx = &op->pmi.offset_idx;
	uint32_t ele_count = op->ele_count;

	find->lower = 0;
	find->upper = ele_count;

	uint32_t idx = (find->lower + find->upper) / 2;
	as_unpacker pk_key = {
		.buffer = key->ptr,
		.offset = 0,
		.length = (int)key->size
	};

	find->found_key = false;

	while (true) {
		uint32_t offset = offset_index_get_const(offidx, idx);
		uint32_t content_sz = op->packed_sz - op->ele_start;
		uint32_t len = content_sz - offset;
		as_unpacker pk_buf = {
			.buffer = op->packed + op->ele_start + offset,
			.offset = 0,
			.length = (int)len
		};

		pk_key.offset = 0; // reset

		msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk_buf);
		uint32_t key_size = (uint32_t)pk_buf.offset;

		if (value && cmp == MSGPACK_COMPARE_EQUAL) {
			as_unpacker pk_value = {
					.buffer = value->ptr,
					.offset = 0,
					.length = (int)value->size
			};

			cmp = as_unpack_compare(&pk_value, &pk_buf);

			find->found_key = true;
			find->key_offset = offset;
			find->value_offset = offset + key_size;
			find->idx = idx;
			find->size = (uint32_t)pk_buf.offset;
		}

		if (cmp == MSGPACK_COMPARE_EQUAL) {
			if (! find->found_key) {
				find->found_key = true;
				find->key_offset = offset;
				find->value_offset = offset + key_size;
				find->idx = idx++;
				find->size = (idx >= ele_count) ? len : offset_index_get_const(offidx, idx) - offset;
			}

			break;
		}

		if (cmp == MSGPACK_COMPARE_GREATER) {
			if (idx >= find->upper - 1) {
				if (++idx >= ele_count) {
					find->key_offset = content_sz;
					find->value_offset = content_sz;
					find->idx = idx;
					find->size = 0;
					break;
				}

				if (! find->found_key) {
					uint32_t offset = offset_index_get_const(offidx, idx);
					uint32_t content_sz = op->packed_sz - op->ele_start;
					uint32_t len = content_sz - offset;
					as_unpacker pk = {
							.buffer = op->packed + op->ele_start + offset,
							.offset = 0,
							.length = (int)len
					};

					if (as_unpack_size(&pk) < 0) {
						cf_warning(AS_PARTICLE, "packed_map_op_find_key_indexed() invalid packed map");
						return false;
					}

					find->key_offset = offset;
					find->value_offset = offset + pk.offset;
					find->idx = idx++;
					find->size = (idx >= ele_count) ? len : offset_index_get_const(offidx, idx) - offset;
				}

				break;
			}

			find->lower = idx + 1;
			idx += find->upper;
			idx /= 2;
		}
		else if (cmp == MSGPACK_COMPARE_LESS) {
			if (idx == find->lower) {
				find->key_offset = offset;
				find->value_offset = offset + key_size;
				find->idx = idx++;
				find->size = (idx >= ele_count) ? len : offset_index_get_const(offidx, idx) - offset;
				break;
			}

			find->upper = idx;
			idx += find->lower;
			idx /= 2;
		}
		else {
			cf_warning(AS_PARTICLE, "packed_map_op_find_key_indexed() compare error=%d", (int)cmp);
			return false;
		}
	}

	return true;
}

static bool
packed_map_op_find_key(const packed_map_op *op, map_ele_find *find, const cdt_payload *key, const cdt_payload *value)
{
	uint32_t ele_count = op->ele_count;
	offset_index *offidx = (offset_index *)&op->pmi.offset_idx;

	if (ele_count == 0) {
		return true;
	}

	if (op_is_k_ordered(op) && offset_index_is_full(offidx)) {
		if (! packed_map_op_find_key_indexed(op, find, key, value)) {
			cf_warning(AS_PARTICLE, "packed_map_op_find_key() packed_map_op_find_key_indexed failed");
			return false;
		}

		return true;
	}

	as_unpacker pk_key = {
			.buffer = key->ptr,
			.offset = 0,
			.length = (int)key->size
	};

	find->found_key = false;

	as_unpacker pk;

	packed_map_op_init_unpacker(op, &pk);

	uint32_t content_sz = (uint32_t)pk.length;
	bool has_index = offset_index_is_valid(offidx);

	if (op_is_k_ordered(op)) {
		// Ordered compare.

		// Allows for continuation from last search.
		if (find->lower > 0) {
			pk.offset = find->key_offset;
		}

		for (uint32_t i = find->lower; i < find->upper; i++) {
			uint32_t key_offset = (uint32_t)pk.offset;
			uint32_t size;

			pk_key.offset = 0;	// reset

			msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}

			find->value_offset = (uint32_t)pk.offset;

			if (value && cmp == MSGPACK_COMPARE_EQUAL) {
				as_unpacker pk_value = {
						.buffer = value->ptr,
						.offset = 0,
						.length = (int)value->size
				};

				cmp = as_unpack_compare(&pk_value, &pk);

				find->found_key = true;
				size = (uint32_t)pk.offset - key_offset;

				if (has_index && ! offset_index_set_next(offidx, i + 1, (uint32_t)pk.offset)) {
					cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%d offidx_offset=%u",
							i + 1, pk.offset, offset_index_get_const(offidx, i + 1));
				}
			}
			else if (has_index) {
				int64_t ret = offset_index_get_delta(offidx, i);

				if (ret < 0) {
					return false;
				}

				pk.offset = (int)offset_index_get(offidx, i + 1);
				size = (uint32_t)ret;
			}
			else {
				// Skip value.
				if (as_unpack_size(&pk) < 0) {
					return false;
				}

				size = (uint32_t)pk.offset - key_offset;
			}

			if (cmp != MSGPACK_COMPARE_GREATER) {
				if (cmp == MSGPACK_COMPARE_EQUAL) {
					find->found_key = true;
				}

				find->idx = i;
				find->key_offset = key_offset;
				find->size = size;

				return true;
			}
		}

		if (find->upper == ele_count) {
			find->key_offset = content_sz;
			find->value_offset = content_sz;
			find->size = 0;
		}
		else {
			if (has_index && ! offset_index_set_next(offidx, find->upper, (uint32_t)pk.offset)) {
				cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%d offidx_offset=%u",
						find->upper, pk.offset, offset_index_get_const(offidx, find->upper));
			}

			find->key_offset = (uint32_t)pk.offset;

			// Skip key.
			if (as_unpack_size(&pk) < 0) {
				return false;
			}

			find->value_offset = (uint32_t)pk.offset;

			// Skip value.
			if (as_unpack_size(&pk) < 0) {
				return false;
			}

			find->size = (uint32_t)pk.offset - find->key_offset;
		}

		find->idx = find->upper;
	}
	else {
		// Unordered compare.
		// Assumes same keys are clustered.
		for (uint32_t i = 0; i < ele_count; i++) {
			uint32_t offset = (uint32_t)pk.offset;

			pk_key.offset = 0;	// reset

			msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}

			uint32_t value_offset = (uint32_t)pk.offset;

			if (cmp == MSGPACK_COMPARE_EQUAL) {
				if (value) {
					as_unpacker pk_value = {
							.buffer = value->ptr,
							.offset = 0,
							.length = (int)value->size
					};

					if ((cmp = as_unpack_compare(&pk_value, &pk)) == MSGPACK_COMPARE_ERROR) {
						return false;
					}
				}
				else {
					// Skip value.
					if (as_unpack_size(&pk) < 0) {
						return false;
					}
				}

				if (! find->found_key) {
					find->found_key = true;
					find->idx = i;
					find->key_offset = offset;
					find->value_offset = value_offset;
					find->size = (uint32_t)pk.offset - offset;
				}

				if (has_index && ! offset_index_set_next(offidx, i + 1, (uint32_t)pk.offset)) {
					cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%d offidx_offset=%u",
							i + 1, pk.offset, offset_index_get_const(offidx, i + 1));
				}

				if (value) {
					if (cmp == MSGPACK_COMPARE_EQUAL) {
						find->found_key = true;
						find->idx = i;
						find->key_offset = offset;
						find->value_offset = value_offset;
						find->size = (uint32_t)pk.offset - offset;

						return true;
					}

					continue;
				}
				else {
					return true;
				}
			}
			else if (find->found_key) {
				return true;
			}
			// Skip value.
			else if (as_unpack_size(&pk) < 0) {
				return false;
			}

			if (has_index && ! offset_index_set_next(offidx, i + 1, (uint32_t)pk.offset)) {
				cf_warning(AS_PARTICLE, "offset mismatch at i=%u offset=%d offidx_offset=%u",
						i + 1, pk.offset, offset_index_get_const(offidx, i + 1));
			}
		}

		find->key_offset = content_sz;
		find->value_offset = content_sz;
		find->size = 0;
		find->idx = ele_count;
	}

	return true;
}

// Return new size of map elements.
static int32_t
packed_map_op_add(packed_map_op *op, const map_ele_find *found)
{
	uint32_t ele_count = op->ele_count;

	// Replace at offset.
	if (found->found_key) {
		op->new_ele_count = ele_count;
		op->seg2_offset = found->key_offset + found->size;
	}
	// Insert at offset.
	else {
		op->new_ele_count = ele_count + 1;
		op->seg2_offset = found->key_offset;
	}

	op->seg1_size = found->key_offset;
	op->seg2_size = op->packed_sz - op->seg2_offset;

	// seg2_size does not include header.
	op->seg2_size -= op->ele_start;

	return (int32_t)(op->seg1_size + op->seg2_size);
}

static int32_t
packed_map_op_remove(packed_map_op *op, const map_ele_find *found, uint32_t count, uint32_t remove_sz)
{
	uint32_t ele_count = op->ele_count;

	op->new_ele_count = ele_count - count;
	op->seg1_size = found->key_offset;
	op->seg2_offset = found->key_offset + remove_sz;
	op->seg2_size = op->packed_sz - op->ele_start - op->seg2_offset;

	op->ele_removed = count;

	return (int32_t)(op->seg1_size + op->seg2_size);
}

static int
packed_map_op_get_remove_by_key(packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key, cdt_result_data *result)
{
	offset_index_inita_from_op_if_invalid(&op->pmi.offset_idx, op);

	if (op_is_k_ordered(op)) {
		offset_index_fill(&op->pmi.offset_idx, op->ele_count);
	}

	map_ele_find find_key;
	map_ele_find_init(&find_key, op);

	const cdt_payload *use_value = NULL;

	if (! packed_map_op_find_key(op, &find_key, key, use_value)) {
		if (cdt_payload_is_int(key)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key() find key=%ld failed, ele_count=%d", cdt_payload_get_int64(key), op->ele_count);
		}
		else {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key() find key failed, ele_count=%d", op->ele_count);
		}

		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (! find_key.found_key) {
		if (! result_data_set_key_not_found(result, -1)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key() invalid result_type %d", result->type);
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		return AS_PROTO_RESULT_OK;
	}

	offset_index_fill(&op->pmi.offset_idx, op->ele_count); // ensure fill for unordered case

	uint32_t count = 1;
	uint32_t remove_sz = find_key.size;

	if (b) {
		int32_t new_size = packed_map_op_remove(op, &find_key, count, remove_sz);

		if (new_size < 0) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key() packed_map_transform_remove_key failed with ret=%d, ele_count=%d", new_size, op->ele_count);
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		map_packer mpk;
		map_packer_init(&mpk, op->new_ele_count, op->pmi.flags, (uint32_t)new_size);

		if (! map_packer_setup_bin(&mpk, b, alloc_buf)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key() failed to alloc map particle");
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		map_packer_write_hdridx(&mpk);
		map_packer_write_seg1(&mpk, op);
		map_packer_write_seg2(&mpk, op);

		if (! map_packer_copy_index(&mpk, op, &find_key, NULL, 0)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key() copy index failed");
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key(): buf=%s", buf);
	}
#endif

	return packed_map_op_build_result_by_key(op, key, find_key.idx, count, result);
}

static int
packed_map_op_get_remove_by_key_interval(packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *key_start, const cdt_payload *key_end, cdt_result_data *result)
{
	if (! has_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_get_remove_by_key_interval() offset_index needs to be valid");
	}

	if (result_data_is_return_rank_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key_interval() result_type %d not supported", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t index = 0;
	uint32_t count = 0;

	if (op_is_k_ordered(op)) {
		if (! packed_map_op_get_range_by_key_interval_ordered(op, key_start, key_end, &index, &count)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		return packed_map_op_get_remove_by_index_range(op, b, alloc_buf, index, count, result);
	}

	order_index idxs;

	order_index_inita(&idxs, op->ele_count);

	if (! packed_map_op_get_range_by_key_interval_unordered(op, key_start, key_end, &index, &count, &idxs)) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}
	// NOTE: idxs already sorted.

	int ret = 0;

	if (b) {
		if ((ret = packed_map_remove_idxs(b, op, alloc_buf, &idxs, count, NULL)) < 0) {
			return ret;
		}
	}

	if (result_data_is_return_elements(result)) {
		if (! packed_map_op_build_ele_result_by_ele_idx(op, &idxs, 0, count, result)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}
	else if (result_data_is_return_rank(result)) {
		ret = packed_map_op_build_rank_result_by_index_range(op, index, count, &idxs, 0, result);
	}
	else {
		ret = result_data_set_range(result, index, count, op->ele_count);
	}

	if (ret < 0) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key_interval(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_key_interval(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

// Set b = NULL for get_by_index_range operation.
static int
packed_map_op_get_remove_by_index_range(const packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, uint32_t index, uint32_t count, cdt_result_data *result)
{
	if (result_data_is_return_rank_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_index_range() result_type %d not supported", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (count == 0) {
		if (! result_data_set_key_not_found(result, index)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		return AS_PROTO_RESULT_OK;
	}

	if (! has_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_get_remove_by_index_range() offset_index needs to be valid");
	}

	offset_index *offidx = (offset_index *)&op->pmi.offset_idx;
	int ret = 0;

	if (op_is_k_ordered(op)) {
		// Pre-fill index.
		if (! offset_index_fill(offidx, index + count)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_index_range() invalid packed map");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (b) {
			order_index rem_idx;

			order_index_init(&rem_idx, NULL, index + count);
			rem_idx._.ptr = alloca(rem_idx._.ele_size * count);
			rem_idx._.ele_count = count;

			for (uint32_t i = 0; i < count; i++) {
				order_index_set(&rem_idx, i, i + index);
			}

			int ret = packed_map_remove_idxs(b, op, alloc_buf, &rem_idx, count, NULL);

			if (ret < 0) {
				return ret;
			}
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_op_build_ele_result_by_idx_range(op, index, count, result)) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
		else if (result_data_is_return_rank(result)) {
			ret = packed_map_op_build_rank_result_by_index_range(op, index, count, NULL, 0, result);
		}
		else {
			ret = result_data_set_range(result, index, count, op->ele_count);
		}
	}
	else {
		// Pre-fill index.
		if (! offset_index_fill(offidx, op->ele_count)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_index_range() invalid packed map");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		uint32_t tail_distance = op->ele_count - index - count;
		uint32_t discard;
		msgpack_compare_t cmp;

		if (index <= tail_distance) {
			cmp = MSGPACK_COMPARE_LESS;		// min k
			discard = index;
		}
		else {
			cmp = MSGPACK_COMPARE_GREATER;	// max k
			discard = tail_distance;
		}

		order_heap heap;

		order_heap_inita(&heap, op->ele_count, op, cmp, true);
		order_heap_build(&heap, true);

		for (uint32_t i = 0; i < discard; i++) {
			if (! order_heap_remove_top(&heap)) {
				cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_index_range() invalid packed map");
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}

		// Reorder results in key order.
		order_heap_order_at_end(&heap, count);

		// Make sure order is from lowest to highest order
		if (cmp == MSGPACK_COMPARE_LESS) {
			order_heap_reverse_end(&heap, count);
		}

		if (b) {
			order_index sorted_rem_idx;

			sorted_rem_idx._.ele_size = heap._._.ele_size;
			sorted_rem_idx._.ptr = alloca(sorted_rem_idx._.ele_size * count);
			sorted_rem_idx._.ele_count = count;

			for (uint32_t i = 0; i < count; i++) {
				uint32_t idx = order_heap_get_ordered(&heap, i);
				order_index_set(&sorted_rem_idx, i, idx);
			}

			if (! order_index_sort(&sorted_rem_idx, offidx, op->packed + op->ele_start, op->packed_sz - op->ele_start, SORT_BY_IDX)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if ((ret = packed_map_remove_idxs(b, op, alloc_buf, &sorted_rem_idx, count, NULL)) < 0) {
				return ret;
			}
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_op_build_ele_result_by_ele_idx(op, &heap._, heap.filled, count, result)) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
		else if (result_data_is_return_rank(result)) {
			ret = packed_map_op_build_rank_result_by_index_range(op, index, count, &heap._, heap.filled, result);
		}
		else {
			ret = result_data_set_range(result, index, count, op->ele_count);
		}
	}

	if (ret < 0) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_index_range(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_index_range(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

// value_end == NULL means looking for: [value_start, largest possible value].
// value_start == value_end means looking for a single value: [value_start, value_start].
static int
packed_map_op_get_remove_by_value_interval(const packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *value_start, const cdt_payload *value_end, cdt_result_data *result)
{
	if (! has_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_get_remove_by_value_interval() offset_index needs to be valid");
	}

	if (result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_value_interval() result_type %d not supported", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t rank = 0;
	uint32_t count = 0;
	int ret = AS_PROTO_RESULT_OK;

	if (order_index_is_valid(&op->pmi.value_idx)) {
		if (! packed_map_op_ensure_ordidx_filled(op)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (! packed_map_op_find_rank_range_by_value_interval_indexed(op, value_start, value_end, &rank, &count, result->is_multi)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (b) {
			const order_index *idx = &op->pmi.value_idx;
			order_index sorted;

			sorted._.ele_size = idx->_.ele_size;

			size_t sorted_size = sorted._.ele_size * count;

			sorted._.ptr = alloca(sorted_size);
			sorted._.ele_count = count;
			memcpy(sorted._.ptr, order_index_get_mem(idx, rank), sorted_size);

			if (! order_index_sort(&sorted, NULL, NULL, 0, SORT_BY_IDX)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			int ret = packed_map_remove_idxs(b, op, alloc_buf, &sorted, count, NULL);

			if (ret < 0) {
				return ret;
			}
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_op_build_ele_result_by_ele_idx(op, &op->pmi.value_idx, rank, count, result)) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
		else if (result_data_is_return_index(result)) {
			ret = packed_map_op_build_index_result_by_ele_idx(op, &op->pmi.value_idx, rank, count, result);
		}
		else {
			ret = result_data_set_range(result, rank, count, op->ele_count);
		}
	}
	else {
		order_heap heap;
		order_heap_inita(&heap, op->ele_count, op, MSGPACK_COMPARE_GREATER, false);

		if (! packed_map_op_find_rank_range_by_value_interval_unordered(op, value_start, value_end, &rank, &count, &heap._)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (count == 0) {
			if (! result_data_set_value_not_found(result, rank)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}
		else {
			if (! result->is_multi) {
				count = 1;
			}

			if (b) {
				packed_map_remove_idxs(b, op, alloc_buf, &heap._, count, NULL);
			}

			if (result_data_is_return_elements(result)) {
				// Make them value ordered for return.
				heap.filled = count;
				heap._._.ele_count = count;
				order_heap_build(&heap, false);
				order_heap_order_at_end(&heap, count);

				if (! packed_map_op_build_ele_result_by_ele_idx(op, &heap._, 0, count, result)) {
					return -AS_PROTO_RESULT_FAIL_UNKNOWN;
				}
			}
			else if (result_data_is_return_index(result)) {
				ret = packed_map_op_build_index_result_by_ele_idx(op, &heap._, 0, count, result);
			}
			else {
				ret = result_data_set_range(result, rank, count, op->ele_count);
			}
		}
	}

	if (ret < 0) {
		return ret;
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_value_interval(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_value_interval(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_op_get_remove_by_rank_range(const packed_map_op *op, as_bin *b, rollback_alloc *alloc_buf, uint32_t rank, uint32_t count, cdt_result_data *result)
{
	if (! has_full_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_get_remove_by_rank_range() offset_index needs to be valid and filled");
	}

	if (result_data_is_return_index_range(result)) {
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_rank_range() result_type %d not supported", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	const order_index *ordidx = &op->pmi.value_idx;
	size_t idx_list_sz = ordidx->_.ele_size * count;
	order_index idx_list;
	int ret = 0;

	idx_list._.ele_size = ordidx->_.ele_size;
	idx_list._.ele_count = count;

	if (order_index_is_valid(ordidx)) {
		if (! packed_map_op_ensure_ordidx_filled(op)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_op_build_ele_result_by_ele_idx(op, ordidx, rank, count, result)) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
		else if (result_data_is_return_index(result)) {
			ret = packed_map_op_build_index_result_by_ele_idx(op, ordidx, rank, count, result);
		}
		else {
			ret = result_data_set_range(result, rank, count, op->ele_count);
		}

		if (ret < 0) {
			return ret;
		}

		if (b) {
			idx_list._.ptr = alloca(idx_list_sz);
			memcpy(idx_list._.ptr, order_index_get_mem(ordidx, rank), idx_list_sz);

			if (! order_index_sort(&idx_list, NULL, NULL, 0, SORT_BY_IDX)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if ((ret = packed_map_remove_idxs(b, op, alloc_buf, &idx_list, count, NULL)) < 0) {
				return ret;
			}
		}
	}
	else {
		uint32_t tail_distance = op->ele_count - rank - count;
		uint32_t discard;
		msgpack_compare_t cmp;

		if (rank <= tail_distance) {
			cmp = MSGPACK_COMPARE_LESS;		// min k
			discard = rank;
		}
		else {
			cmp = MSGPACK_COMPARE_GREATER;	// max k
			discard = tail_distance;
		}

		// Make a heap ordered by value.
		order_heap heap;

		order_heap_inita(&heap, op->ele_count, op, cmp, false);
		order_heap_build(&heap, true);

		for (uint32_t i = 0; i < discard; i++) {
			if (! order_heap_remove_top(&heap)) {
				cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_rank_range() invalid packed map");
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}

		// Reorder results from lowest to highest order.
		order_heap_order_at_end(&heap, count);

		if (cmp == MSGPACK_COMPARE_LESS) {
			order_heap_reverse_end(&heap, count);
		}

		if (result_data_is_return_elements(result)) {
			if (! packed_map_op_build_ele_result_by_ele_idx(op, &heap._, heap.filled, count, result)) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
		else if (result_data_is_return_index(result)) {
			ret = packed_map_op_build_index_result_by_ele_idx(op, &heap._, heap.filled, count, result);
		}
		else {
			ret = result_data_set_range(result, rank, count, op->ele_count);
		}

		if (ret < 0) {
			return ret;
		}

		if (b) {
			idx_list._.ptr = alloca(idx_list_sz);
			memcpy(idx_list._.ptr, order_index_get_mem(&heap._, heap.filled), idx_list_sz);

			if (! order_index_sort(&idx_list, NULL, NULL, 0, SORT_BY_IDX)) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if ((ret = packed_map_remove_idxs(b, op, alloc_buf, &idx_list, count, NULL)) < 0) {
				return ret;
			}
		}
	}

#ifdef MAP_DEBUG_VERIFY
	if (b && ! as_bin_verify(b)) {
		const map_mem *p = (const map_mem *)b->particle;
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_rank_range(): data=%p sz=%u type=%d", p->data, p->sz, p->type);
		char buf[4096];
		print_hex(p->data, p->sz, buf, 4096);
		cf_warning(AS_PARTICLE, "packed_map_op_get_remove_by_rank_range(): buf=%s", buf);
	}
#endif

	return AS_PROTO_RESULT_OK;
}

static bool
packed_map_op_get_range_by_key_interval_unordered(packed_map_op *op, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count, order_index *ranks)
{
	as_unpacker pk_start = {
			.buffer = key_start->ptr,
			.offset = 0,
			.length = (int)key_start->size
	};
	as_unpacker pk_end = {
			.buffer = key_end ? key_end->ptr : NULL,
			.offset = 0,
			.length = key_end? (int)key_end->size : 0
	};

	// Pre-check parameters.
	if (as_unpack_size(&pk_start) < 0) {
		cf_warning(AS_PARTICLE, "packed_map_op_get_range_by_key_interval_unordered() invalid start key");
		return false;
	}

	if (key_end) {
		// Pre-check parameters.
		if (key_end && as_unpack_size(&pk_end) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_range_by_key_interval_unordered() invalid end key");
			return false;
		}
	}

	*index = 0;
	*count = 0;

	offset_index *offidx = &op->pmi.offset_idx;
	as_unpacker pk;
	packed_map_op_init_unpacker(op, &pk);

	for (uint32_t i = 0; i < op->ele_count; i++) {
		int key_offset = pk.offset;		// start of key

		offset_index_set(offidx, i, (uint32_t)key_offset);

		pk_start.offset = 0;

		msgpack_compare_t cmp_start = as_unpack_compare(&pk, &pk_start);

		if (cmp_start == MSGPACK_COMPARE_ERROR) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_range_by_key_interval_unordered() invalid packed map at index %u", i);
			return false;
		}

		if (cmp_start == MSGPACK_COMPARE_LESS) {
			(*index)++;
		}
		else {
			msgpack_compare_t cmp_end = MSGPACK_COMPARE_LESS;

			// NULL key_end means largest possible value.
			if (key_end) {
				pk.offset = key_offset;
				pk_end.offset = 0;
				cmp_end = as_unpack_compare(&pk, &pk_end);
			}

			if (cmp_end == MSGPACK_COMPARE_LESS) {
				order_index_set(ranks, *count, i);
				(*count)++;
			}
		}

		// Skip value.
		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_range_by_key_interval_unordered() invalid packed map at index %u", i);
			return false;
		}
	}

	offset_index_set_filled(offidx, op->ele_count);

	return true;
}

static bool
packed_map_op_get_range_by_key_interval_ordered(packed_map_op *op, const cdt_payload *key_start, const cdt_payload *key_end, uint32_t *index, uint32_t *count)
{
	map_ele_find find_key_start;
	map_ele_find_init(&find_key_start, op);

	if (! packed_map_op_find_key(op, &find_key_start, key_start, &nil_segment)) {
		cf_warning(AS_PARTICLE, "packed_map_get_key_interval_ordered() find key failed, ele_count=%d", op->ele_count);
		return false;
	}

	*index = find_key_start.idx;

	if (key_end) {
		map_ele_find find_key_end;

		map_ele_find_continue_from_lower(&find_key_end, &find_key_start, op->ele_count);

		if (! packed_map_op_find_key(op, &find_key_end, key_end, &nil_segment)) {
			cf_warning(AS_PARTICLE, "packed_map_get_key_interval_ordered() find key failed, ele_count=%d", op->ele_count);
			return false;
		}

		if (find_key_end.idx <= find_key_start.idx) {
			*count = 0;
		}
		else {
			*count = find_key_end.idx - find_key_start.idx;
		}
	}
	else {
		*count = op->ele_count - find_key_start.idx;
	}

	return true;
}

static int
packed_map_op_build_rank_result_by_index_range(const packed_map_op *op, uint32_t index, uint32_t count, const order_index *ele_idx, uint32_t start, cdt_result_data *result)
{
	offset_index *offidx = (offset_index *)&op->pmi.offset_idx;
	order_index *ordidx = (order_index *)&op->pmi.value_idx;

	if (! result->is_multi) {
		uint32_t idx = index;

		if (ele_idx) {
			idx = order_index_get(ele_idx, start);
		}

		return packed_map_op_build_rank_result_by_idx(op, idx, result);
	}

	cdt_container_builder builder;

	if (! cdt_list_builder_start(&builder, result->alloc, count, (sizeof(uint64_t) + 1) * count)) {
		return false;
	}

	// Preset offsets if necessary.
	if (! offset_index_fill(offidx, op->ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_op_build_rank_range_result_by_index_range() invalid packed map");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (order_index_is_valid(ordidx)) {
		if (! packed_map_op_ensure_ordidx_filled(op)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
	}
	else {
		order_index_inita(ordidx, op->ele_count);
		order_index_set_sorted(ordidx, offidx, op->packed + op->ele_start, op->packed_sz - op->ele_start, SORT_BY_VALUE);
	}

	if (op_is_k_ordered(op)) {
		for (uint32_t i = 0; i < count; i++) {
			map_ele_find find;

			map_ele_find_init_from_idx(&find, op, index + i);
			packed_map_op_find_rank_indexed(op, &find);

			if (! find.found_value) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			uint32_t rank = find.rank;

			if (result->type == RESULT_TYPE_REVRANK) {
				rank = op->ele_count - rank - 1;
			}

			cdt_container_builder_add_int64(&builder, rank);
		}
	}
	else {
		if (! ele_idx) {
			cf_crash(AS_PARTICLE, "packed_map_op_build_rank_range_result_by_index_range() require ele_idx != NULL for unindexed");
		}

		for (uint32_t i = 0; i < count; i++) {
			uint32_t idx = order_index_get(ele_idx, start + i);
			map_ele_find find;

			map_ele_find_init_from_idx(&find, op, idx);
			packed_map_op_find_rank_indexed(op, &find);

			if (! find.found_value) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			uint32_t rank = find.rank;

			if (result->type == RESULT_TYPE_REVRANK) {
				rank = op->ele_count - rank - 1;
			}

			cdt_container_builder_add_int64(&builder, rank);
		}
	}

	result->result->particle = builder.particle;
	as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);

	return AS_PROTO_RESULT_OK;
}

static bool
packed_map_op_get_key_by_idx(const packed_map_op *op, cdt_payload *key, uint32_t index)
{
	uint32_t pk_offset = offset_index_get_const(&op->pmi.offset_idx, index) + op->ele_start;
	as_unpacker pk = {
			.buffer = op->packed + pk_offset,
			.offset = 0,
			.length = (int)(op->packed_sz - pk_offset)
	};

	if (as_unpack_size(&pk) < 0) { // read key
		cf_warning(AS_PARTICLE, "packed_map_op_get_key_by_idx() read key failed");
		return false;
	}

	key->ptr = pk.buffer;
	key->size = (uint32_t)pk.offset;

	return true;
}

static bool
packed_map_op_get_value_by_idx(const packed_map_op *op, cdt_payload *value, uint32_t idx)
{
	uint32_t pk_offset = (uint32_t)offset_index_get_const(&op->pmi.offset_idx, idx) + op->ele_start;
	uint32_t size = offset_index_get_delta_const(&op->pmi.offset_idx, idx);
	as_unpacker pk = {
			.buffer = op->packed + pk_offset,
			.offset = 0,
			.length = (int)(op->packed_sz - pk_offset)
	};

	if (as_unpack_size(&pk) < 0) { // read key
		cf_warning(AS_PARTICLE, "packed_map_op_get_value_by_idx() read key failed");
		return false;
	}

	uint32_t key_size = (uint32_t)pk.offset;

	value->ptr = pk.buffer + key_size;
	value->size = size - key_size;

	return true;
}

static bool
packed_map_op_get_pair_by_idx(const packed_map_op *op, cdt_payload *value, uint32_t index)
{
	uint32_t pk_offset = offset_index_get_const(&op->pmi.offset_idx, index) + op->ele_start;
	uint32_t size = offset_index_get_delta_const(&op->pmi.offset_idx, index);

	value->ptr = op->packed + pk_offset;
	value->size = size;

	return true;
}

static int
packed_map_op_build_index_result_by_ele_idx(const packed_map_op *op, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result)
{
	if (count == 0) {
		if (! result_data_set_not_found(result, start)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		return AS_PROTO_RESULT_OK;
	}

	if (! result->is_multi) {
		uint32_t index = order_index_get(ele_idx, start);

		if (! op_is_k_ordered(op)) {
			index = packed_map_op_find_index_by_idx_unordered(op, index);
		}

		if (result->type == RESULT_TYPE_REVINDEX) {
			index = op->ele_count - index - 1;
		}

		as_bin_set_int(result->result, index);

		return AS_PROTO_RESULT_OK;
	}

	cdt_container_builder builder;

	if (! cdt_list_builder_start(&builder, result->alloc, count, (sizeof(uint64_t) + 1) * count)) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (op_is_k_ordered(op)) {
		for (uint32_t i = 0; i < count; i++) {
			uint32_t index = order_index_get(ele_idx, start + i);

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = op->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
		}
	}
	else {
		offset_index *offidx = (offset_index *)&op->pmi.offset_idx;
		order_index keyordidx;

		// Preset offsets if necessary.
		if (offset_index_get(offidx, op->ele_count) < 0) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_rank_range_by_index_range() invalid packed map");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		// Make order index on stack.
		order_index_inita(&keyordidx, op->ele_count);
		order_index_set_sorted(&keyordidx, offidx, op->packed + op->ele_start, op->packed_sz - op->ele_start, SORT_BY_KEY);

		for (uint32_t i = 0; i < count; i++) {
			uint32_t idx = order_index_get(ele_idx, start + i);
			uint32_t index = order_index_find_idx(&keyordidx, idx, 0, op->ele_count);

			if (index >= op->ele_count) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (result->type == RESULT_TYPE_REVINDEX) {
				index = op->ele_count - index - 1;
			}

			cdt_container_builder_add_int64(&builder, index);
		}
	}

	result->result->particle = builder.particle;
	as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);

	return AS_PROTO_RESULT_OK;
}

// Build by map ele_idx range.
static bool
packed_map_op_build_ele_result_by_idx_range(const packed_map_op *op, uint32_t ele_idx, uint32_t count, cdt_result_data *result)
{
	packed_map_op_get_by_idx_func get_by_idx_func;
	cdt_container_builder builder;
	uint32_t content_size = (count > 0 ? op->packed_sz - op->ele_start : 0);

	if (result->type == RESULT_TYPE_MAP) {
		get_by_idx_func = packed_map_op_get_pair_by_idx;

		if (! cdt_map_builder_start(&builder, result->alloc, count, content_size, AS_PACKED_MAP_FLAG_PRESERVE_ORDER)) {
			return false;
		}
	}
	else {
		if (result->type == RESULT_TYPE_KEY) {
			get_by_idx_func = packed_map_op_get_key_by_idx;
		}
		else {
			get_by_idx_func = packed_map_op_get_value_by_idx;
		}

		if (result->is_multi) {
			if (! cdt_list_builder_start(&builder, result->alloc, count, content_size)) {
				return false;
			}
		}
		else {
			cdt_payload packed;

			if (! get_by_idx_func(op, &packed, ele_idx)) {
				return false;
			}

			return rollback_alloc_from_msgpack(result->alloc, result->result, &packed);
		}
	}

	for (size_t i = 0; i < count; i++) {
		cdt_payload packed;

		if (! get_by_idx_func(op, &packed, ele_idx + i)) {
			return false;
		}

		cdt_container_builder_add(&builder, packed.ptr, (size_t)packed.size);
	}

	result->result->particle = builder.particle;

	if (result->type == RESULT_TYPE_MAP) {
		as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_MAP);
	}
	else {
		as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);
	}

	return true;
}

static bool
packed_map_op_build_ele_result_by_ele_idx(const packed_map_op *op, const order_index *ele_idx, uint32_t start, uint32_t count, cdt_result_data *result)
{
	packed_map_op_get_by_idx_func get_by_index_func;
	cdt_container_builder builder;
	uint32_t content_size = (count > 0 ? op->packed_sz - op->ele_start : 0);

	if (result->type == RESULT_TYPE_MAP) {
		get_by_index_func = packed_map_op_get_pair_by_idx;

		if (! cdt_map_builder_start(&builder, result->alloc, count, content_size, AS_PACKED_MAP_FLAG_PRESERVE_ORDER)) {
			return false;
		}
	}
	else {
		if (result->type == RESULT_TYPE_KEY) {
			get_by_index_func = packed_map_op_get_key_by_idx;
		}
		else {
			get_by_index_func = packed_map_op_get_value_by_idx;
		}

		if (result->is_multi) {
			if (! cdt_list_builder_start(&builder, result->alloc, count, content_size)) {
				return false;
			}
		}
		else if (count == 0) {
			return true;
		}
		else {
			uint32_t index = order_index_get(ele_idx, start);
			cdt_payload packed;

			if (! get_by_index_func(op, &packed, index)) {
				return false;
			}

			return rollback_alloc_from_msgpack(result->alloc, result->result, &packed);
		}
	}

	for (size_t i = 0; i < count; i++) {
		uint32_t index = order_index_get(ele_idx, i + start);
		cdt_payload packed;

		if (! get_by_index_func(op, &packed, index)) {
			return false;
		}

		cdt_container_builder_add(&builder, packed.ptr, (size_t)packed.size);
	}

	result->result->particle = builder.particle;

	if (result->type == RESULT_TYPE_MAP) {
		as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_MAP);
	}
	else {
		as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);
	}

	return true;
}

static int
packed_map_op_build_result_by_key(const packed_map_op *op, const cdt_payload *key, uint32_t idx, uint32_t count, cdt_result_data *result)
{
	switch (result->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX: {
		uint32_t index = idx;

		if (! op_is_k_ordered(op)) {
			index = packed_map_op_find_index_by_key_unordered(op, key);
		}

		if (result_data_is_return_index_range(result)) {
			if (result->type == RESULT_TYPE_REVINDEX_RANGE) {
				index = op->ele_count - index - count;
			}

			if (! result_data_set_list_int2x(result, index, count)) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}
		else {
			if (result->type == RESULT_TYPE_REVINDEX) {
				index = op->ele_count - index - count;
			}

			as_bin_set_int(result->result, index);
		}

		break;
	}
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		if (result->is_multi) {
			return packed_map_op_build_rank_result_by_idx_range(op, idx, count, result);
		}

		return packed_map_op_build_rank_result_by_idx(op, idx, result);
	case RESULT_TYPE_COUNT:
		as_bin_set_int(result->result, count);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
	case RESULT_TYPE_MAP:
		if (! packed_map_op_build_ele_result_by_idx_range(op, idx, count, result)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		break;
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
	default:
		cf_warning(AS_PARTICLE, "packed_map_op_build_result_by_key() invalid result_type %d", result->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return AS_PROTO_RESULT_OK;
}

// Return negative codes on error.
static int64_t
packed_map_op_get_rank_by_idx(const packed_map_op *op, uint32_t idx)
{
	if (! has_offidx(op)) {
		cf_crash(AS_PARTICLE, "packed_map_op_get_rank_by_idx() offset_index needs to be valid");
	}

	uint32_t rank;

	if (order_index_is_valid(&op->pmi.value_idx)) {
		if (! packed_map_op_ensure_ordidx_filled(op)) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		map_ele_find find_key;
		map_ele_find_init_from_idx(&find_key, op, idx);

		if (! packed_map_op_find_rank_indexed(op, &find_key)) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_rank_by_idx() packed_map_op_find_rank_indexed failed");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (! find_key.found_value) {
			cf_warning(AS_PARTICLE, "packed_map_op_get_rank_by_idx() rank not found, idx=%u rank=%u", find_key.idx, find_key.rank);
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		rank = find_key.rank;
	}
	else {
		const offset_index *offidx = &op->pmi.offset_idx;
		uint32_t pk_offset = op->ele_start + offset_index_get_const(offidx, idx);
		as_unpacker pk_entry = {
				.buffer = op->packed + pk_offset,
				.offset = 0,
				.length = (int)op->packed_sz - pk_offset
		};
		as_unpacker pk;

		packed_map_op_init_unpacker(op, &pk);
		rank = 0;

		for (uint32_t i = 0; i < op->ele_count; i++) {
			pk_entry.offset = 0;

			msgpack_compare_t cmp = packed_map_compare_values(&pk, &pk_entry);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (cmp == MSGPACK_COMPARE_LESS) {
				rank++;
			}
		}
	}

	return (int64_t)rank;
}

static int
packed_map_op_build_rank_result_by_idx(const packed_map_op *op, uint32_t idx, cdt_result_data *result)
{
	int64_t rank = packed_map_op_get_rank_by_idx(op, idx);

	if (rank < 0) {
		return rank;
	}

	if (result->type == RESULT_TYPE_REVRANK) {
		as_bin_set_int(result->result, (int64_t)op->ele_count - rank - 1);
	}
	else {
		as_bin_set_int(result->result, rank);
	}

	return AS_PROTO_RESULT_OK;
}

static int
packed_map_op_build_rank_result_by_idx_range(const packed_map_op *op, uint32_t idx, uint32_t count, cdt_result_data *result)
{
	cdt_container_builder builder;

	if (! cdt_list_builder_start(&builder, result->alloc, count, count * (sizeof(int64_t) + 1))) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	for (uint32_t i = 0; i < count; i++) {
		int64_t rank = packed_map_op_get_rank_by_idx(op, idx);

		if (rank < 0) {
			return rank;
		}

		if (result->type == RESULT_TYPE_REVRANK) {
			rank = (int64_t)op->ele_count - rank - 1;
		}

		cdt_container_builder_add_int64(&builder, rank);
	}

	result->result->particle = builder.particle;
	as_bin_state_set_from_type(result->result, AS_PARTICLE_TYPE_LIST);

	return AS_PROTO_RESULT_OK;
}

static uint8_t *
packed_map_op_write_seg1(const packed_map_op *op, uint8_t *buf)
{
	const uint8_t *src = op->packed + op->ele_start;

	memcpy(buf, src, op->seg1_size);
	memcpy(buf + op->seg1_size, src + op->key1_offset, op->key1_size);

	return buf + op->seg1_size + op->key1_size;
}

static uint8_t *
packed_map_op_write_seg2(const packed_map_op *op, uint8_t *buf)
{
	const uint8_t *src = op->packed + op->ele_start;

	memcpy(buf, src + op->key2_offset, op->key2_size);
	memcpy(buf + op->key2_size, src + op->seg2_offset, op->seg2_size);

	return buf + op->key2_size + op->seg2_size;
}

static bool
packed_map_op_write_dk_index(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, offset_index *offset_idx, uint32_t kv_size)
{
	if (offset_index_is_null(&op->pmi.offset_idx)) {
		return false;
	}

	int ele_delta = op->new_ele_count - op->ele_count;

	// Add
	if (ele_delta > 0) {
		// Insert at end.
		if (remove_info->idx == op->ele_count) {
			offset_index_copy(offset_idx, &op->pmi.offset_idx, 0, 0, op->ele_count, 0);
			offset_index_set(offset_idx, op->ele_count, op->seg1_size + op->seg2_size);
		}
		// Insert at offset.
		else {
			offset_index_copy(offset_idx, &op->pmi.offset_idx, 0, 0, remove_info->idx + 1, 0);
			offset_index_copy(offset_idx, &op->pmi.offset_idx,
					remove_info->idx + 1, remove_info->idx,
					(op->ele_count - remove_info->idx), kv_size);
		}
	}
	// Replace 1
	else if (ele_delta == 0) {
		// Multimap replace with different remove/add indexes.
		if (remove_info->idx != add_info->idx) {
			if (remove_info->idx > add_info->idx) {
				offset_index_copy(offset_idx, &op->pmi.offset_idx, 0, 0, add_info->idx + 1, 0);
				offset_index_set(offset_idx, add_info->idx + 1,
						offset_index_get_const(offset_idx, add_info->idx) + kv_size);
				offset_index_copy(offset_idx, &op->pmi.offset_idx,
						add_info->idx + 2, add_info->idx + 1,
						remove_info->idx - add_info->idx - 1, (int32_t)kv_size);

				int delta = (int)kv_size - offset_index_get_delta_const(&op->pmi.offset_idx, add_info->idx);

				offset_index_copy(offset_idx, &op->pmi.offset_idx,
						remove_info->idx + 1, remove_info->idx + 1,
						op->ele_count - remove_info->idx, delta);
			}
			else {
				offset_index_copy(offset_idx, &op->pmi.offset_idx, 0, 0, remove_info->idx + 1, 0);

				int delta = -(int)offset_index_get_delta_const(&op->pmi.offset_idx, remove_info->idx);

				offset_index_copy(offset_idx, &op->pmi.offset_idx,
						remove_info->idx + 1, remove_info->idx + 2,
						add_info->idx - remove_info->idx - 1, delta);
				offset_index_set(offset_idx, add_info->idx,
						offset_index_get_const(offset_idx, add_info->idx - 1) + kv_size);
				delta += (int32_t)kv_size;
				offset_index_copy(offset_idx, &op->pmi.offset_idx,
						add_info->idx + 1, add_info->idx + 1,
						op->ele_count - add_info->idx, delta);
			}
		}
		// Replace on same index.
		else {
			offset_index_copy(offset_idx, &op->pmi.offset_idx, 0, 0, remove_info->idx, 0);
			offset_index_set(offset_idx, remove_info->idx, remove_info->key_offset);

			int delta = (int)kv_size - (int)remove_info->size;

			offset_index_copy(offset_idx, &op->pmi.offset_idx,
					remove_info->idx + 1, remove_info->idx + 1,
					op->ele_count - remove_info->idx - 1, delta);
		}
	}
	else if (op->ele_removed > 0) {
		uint32_t index = remove_info->idx;
		offidx_op offop;

		offidx_op_init(&offop, offset_idx, &op->pmi.offset_idx);

		// Replace many -> 1
		if (op->ele_removed + ele_delta != 0) {
			offidx_op_add(&offop, index, kv_size);
		}

		offidx_op_remove_range(&offop, index, op->ele_removed);
		offidx_op_end(&offop);
	}

	offset_index_set_filled(offset_idx, op->new_ele_count);

	return true;
}

static bool
packed_map_op_write_dv_index(const packed_map_op *op, const map_ele_find *remove_info, const map_ele_find *add_info, order_index *value_idx)
{
	if (order_index_is_null(&op->pmi.value_idx)) {
		return false;
	}

	int ele_delta = op->new_ele_count - op->ele_count;

	// Add
	if (ele_delta > 0) {
		order_index_op_add(value_idx, &op->pmi.value_idx, add_info->idx, (int64_t)add_info->rank);
	}
	// Replace 1
	else if (ele_delta == 0) {
		if (add_info->idx == remove_info->idx) {
			order_index_op_replace1(value_idx, &op->pmi.value_idx, add_info->rank, remove_info->rank);
		}
		else {
			order_index_op_replace1_idx(value_idx, &op->pmi.value_idx, add_info->idx, add_info->rank, remove_info->rank);
		}
	}
	else if (op->ele_removed > 0) {
		// Remove
		if (op->ele_removed + ele_delta == 0) {
			return order_index_op_remove(value_idx, &op->pmi.value_idx,
					remove_info->rank, op->ele_removed);
		}
		// Replace all -> 1
		return order_index_op_replace(value_idx, &op->pmi.value_idx,
				add_info->idx, add_info->rank,
				remove_info->rank, op->ele_removed);
	}

	return true;
}

static msgpack_compare_t
packed_map_op_compare_key_by_idx(const packed_map_op *op, uint32_t idx1, uint32_t idx2)
{
	const offset_index *offidx = &op->pmi.offset_idx;
	as_unpacker pk1 = {
			.buffer = op->packed + op->ele_start,
			.offset = offset_index_get_const(offidx, idx1),
			.length = (int)op->packed_sz
	};
	as_unpacker pk2 = {
			.buffer = op->packed + op->ele_start,
			.offset = offset_index_get_const(offidx, idx2),
			.length = (int)op->packed_sz
	};

	msgpack_compare_t ret = as_unpack_compare(&pk1, &pk2);

	if (ret == MSGPACK_COMPARE_EQUAL) {
		ret = as_unpack_compare(&pk1, &pk2);
	}

	return ret;
}

static msgpack_compare_t
packed_map_compare_values(as_unpacker *pk1, as_unpacker *pk2)
{
	msgpack_compare_t keycmp = as_unpack_compare(pk1, pk2);

	if (keycmp == MSGPACK_COMPARE_ERROR) {
		return MSGPACK_COMPARE_ERROR;
	}

	msgpack_compare_t ret = as_unpack_compare(pk1, pk2);

	if (ret == MSGPACK_COMPARE_EQUAL) {
		return keycmp;
	}

	return ret;
}

static msgpack_compare_t
packed_map_op_compare_value_by_idx(const packed_map_op *op, uint32_t idx1, uint32_t idx2)
{
	const offset_index *offidx = &op->pmi.offset_idx;
	as_unpacker pk1 = {
			.buffer = op->packed + op->ele_start,
			.offset = offset_index_get_const(offidx, idx1),
			.length = (int)op->packed_sz
	};
	as_unpacker pk2 = {
			.buffer = op->packed + op->ele_start,
			.offset = offset_index_get_const(offidx, idx2),
			.length = (int)op->packed_sz
	};

	return packed_map_compare_values(&pk1, &pk2);
}

static bool
packed_map_op_write_k_ordered(packed_map_op *op, uint8_t *write_ptr, offset_index *offsets_new)
{
	uint32_t ele_count = op->ele_count;
	order_index temp_key_order;

	order_index_inita(&temp_key_order, ele_count);
	offset_index_inita_from_op_if_invalid(&op->pmi.offset_idx, op);

	if (! offset_index_fill(&op->pmi.offset_idx, ele_count)) {
		cf_warning(AS_PARTICLE, "packed_map_op_write_k_ordered() offset fill failed");
		return false;
	}

	const offset_index *offsets_old = &op->pmi.offset_idx;

	if (! order_index_set_sorted_with_offsets(&temp_key_order, offsets_old, SORT_BY_KEY)) {
		return false;
	}

	const uint8_t *ptr = offsets_old->ele_start;

	offset_index_set_filled(offsets_new, 1);

	for (uint32_t i = 0; i < ele_count; i++) {
		uint32_t index = order_index_get(&temp_key_order, i);
		uint32_t offset = offset_index_get_const(offsets_old, index);
		uint32_t size = offset_index_get_delta_const(offsets_old, index);

		memcpy(write_ptr, ptr + offset, size);
		write_ptr += size;
		offset_index_append_size(offsets_new, size);
	}

	return true;
}

//------------------------------------------------
// packed_map create

static as_particle *
packed_map_create(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t content_size, uint8_t flags)
{
	map_packer mpk;
	map_packer_init(&mpk, ele_count, flags, content_size);

	map_mem *p_map_mem = (map_mem *)map_packer_create_particle(&mpk, alloc_buf);

	if (! p_map_mem) {
		return NULL;
	}

	map_packer_write_hdridx(&mpk);

	if (buf) {
		memcpy(mpk.write_ptr, buf, content_size);
	}

	return (as_particle *)p_map_mem;
}

static int64_t
packed_map_strip_indexes(uint8_t *dest, const as_particle *p, bool remove_flags)
{
	const map_mem *p_map_mem = (const map_mem *)p;

	if (p_map_mem->sz == 0) {
		return 0;
	}

	as_unpacker upk = {
			.buffer = p_map_mem->data,
			.offset = 0,
			.length = (int)p_map_mem->sz
	};

	int64_t ele_count = as_unpack_map_header_element_count(&upk);

	if (ele_count < 0) {
		return -1;
	}

	as_packer pk = {
			.head = NULL,
			.tail = NULL,
			.buffer = dest,
			.offset = 0,
			.capacity = INT_MAX
	};

	if (ele_count > 0 && as_unpack_peek_is_ext(&upk)) {
		as_msgpack_ext ext;

		if (as_unpack_ext(&upk, &ext) != 0) {
			return -2;
		}

		// Skip nil val.
		if (as_unpack_size(&upk) < 0) {
			return -3;
		}

		uint8_t flags = ext.type;

		if (flags != AS_PACKED_MAP_FLAG_NONE) {
			ele_count--;
		}

		flags &= ~(AS_PACKED_MAP_FLAG_OFF_IDX | AS_PACKED_MAP_FLAG_ORD_IDX);

		if (flags != AS_PACKED_MAP_FLAG_NONE) {
			as_pack_map_header(&pk, (uint32_t)ele_count + 1);
			as_pack_ext_header(&pk, 0, flags);
			pk.buffer[pk.offset++] = msgpack_nil[0];
		}
		else {
			as_pack_map_header(&pk, (uint32_t)ele_count);
		}
	}
	else {
		// Copy header.
		as_pack_map_header(&pk, (uint32_t)ele_count);
	}

	// Copy elements.
	size_t ele_size = (size_t)(upk.length - upk.offset);

	memcpy(pk.buffer + pk.offset, upk.buffer + upk.offset, ele_size);

	return (int64_t)pk.offset + (int64_t)ele_size;
}

//------------------------------------------------
// map_ele_find

static void
map_ele_find_init(map_ele_find *find, const packed_map_op *op)
{
	find->found_key = false;
	find->found_value = false;
	find->idx = op->ele_count;
	find->rank = op->ele_count;

	find->key_offset = 0;
	find->value_offset = 0;
	find->size = 0;

	find->lower = 0;
	find->upper = op->ele_count;
}

static void
map_ele_find_continue_from_lower(map_ele_find *find, const map_ele_find *found, uint32_t ele_count)
{
	find->found_key = false;
	find->found_value = false;

	find->idx = ele_count + found->idx;
	find->idx /= 2;
	find->rank = find->idx;

	find->key_offset = found->key_offset;
	find->value_offset = found->value_offset;
	find->size = found->size;

	find->lower = found->idx;
	find->upper = ele_count;
}

static void
map_ele_find_init_from_idx(map_ele_find *find, const packed_map_op *op, uint32_t idx)
{
	map_ele_find_init(find, op);
	find->found_key = true;
	find->idx = idx;
	find->key_offset = offset_index_get_const(&op->pmi.offset_idx, idx);

	as_unpacker pk = {
			.buffer = op->packed + op->ele_start,
			.offset = find->key_offset,
			.length = op->packed_sz - op->ele_start
	};

	as_unpack_size(&pk);
	find->value_offset = pk.offset;
	find->size = offset_index_get_const(&op->pmi.offset_idx, idx + 1) - find->key_offset;
}

//------------------------------------------------
// msgpacked_index

static inline void
msgpacked_index_set(msgpacked_index *idxs, size_t index, uint32_t value)
{
	switch (idxs->ele_size) {
	case 1:
		idxs->ptr[index] = (uint8_t)value;
		break;
	case 2:
		((uint16_t *)idxs->ptr)[index] = (uint16_t)value;
		break;
	case 3:
		((index_pack24 *)idxs->ptr)[index].value = value;
		break;
	default:
		((uint32_t *)idxs->ptr)[index] = value;
		break;
	}
}

static inline void
msgpacked_index_set_ptr(msgpacked_index *idxs, uint8_t *ptr)
{
	idxs->ptr = ptr;
}

// Get pointer at index.
static inline void *
msgpacked_index_get_mem(const msgpacked_index *idxs, size_t index)
{
	return (void *)(idxs->ptr + idxs->ele_size * index);
}

static inline size_t
msgpacked_index_size(const msgpacked_index *idxs)
{
	return idxs->ele_size * idxs->ele_count;
}

static inline uint32_t
msgpacked_index_ptr2value(const msgpacked_index *idxs, const void *ptr)
{
	switch (idxs->ele_size) {
	case 1:
		return *((const uint8_t *)ptr);
	case 2:
		return *((const uint16_t *)ptr);
	case 3:
		return ((const index_pack24 *)ptr)->value;
	default:
		break;
	}

	return *((const uint32_t *)ptr);
}

static inline uint32_t
msgpacked_index_get(const msgpacked_index *idxs, size_t index)
{
	switch (idxs->ele_size) {
	case 1:
		return idxs->ptr[index];
	case 2:
		return ((const uint16_t *)idxs->ptr)[index];
	case 3:
		return ((const index_pack24 *)idxs->ptr)[index].value;
	default:
		break;
	}

	return ((const uint32_t *)idxs->ptr)[index];
}

// Find find_index in a list of sorted_indexes.
// *where will be the location where find_index is (if exist) or is suppose to be (if not exist).
// Return true if find_index is in sorted_indexes.
static bool
msgpacked_index_find_index_sorted(const msgpacked_index *sorted_indexes, uint32_t find_index, uint32_t count, uint32_t *where)
{
	if (count == 0) {
		*where = 0;
		return false;
	}

	uint32_t upper = count;
	uint32_t lower = 0;
	uint32_t i = count / 2;

	while (true) {
		uint32_t index = msgpacked_index_get(sorted_indexes, i);

		if (find_index == index) {
			*where = i;
			return true;
		}

		if (find_index > index) {
			if (i >= upper - 1) {
				*where = i + 1;
				break;
			}

			lower = i + 1;
			i += upper;
			i /= 2;
		}
		else {
			if (i <= lower) {
				*where = i;
				break;
			}

			upper = i;
			i += lower;
			i /= 2;
		}
	}

	return false;
}

static void
msgpacked_index_print(const msgpacked_index *idxs, const char *name)
{
	size_t ele_count = idxs->ele_count;
	char buf[1024];
	char *ptr = buf;

	if (idxs->ptr) {
		for (size_t i = 0; i < ele_count; i++) {
			if (buf + 1024 - ptr < 12) {
				break;
			}

			ptr += sprintf(ptr, "%u, ", msgpacked_index_get(idxs, i));
		}

		if (ele_count > 0) {
			ptr -= 2;
		}

		*ptr = '\0';
	}
	else {
		strcpy(buf, "(null)");
	}
	cf_warning(AS_PARTICLE, "%s: index[%zu]={%s}", name, ele_count, buf);
}

//------------------------------------------------
// offset_index

static inline void
offset_index_init(offset_index *offidx, uint8_t *idx_mem_ptr, uint32_t ele_count, uint32_t content_size)
{
	offidx->_.ele_count = ele_count;
	offidx->tot_ele_sz = content_size;

	if (content_size < (1 << 8)) {
		offidx->_.ele_size = 1;
	}
	else if (content_size < (1 << 16)) {
		offidx->_.ele_size = 2;
	}
	else if (content_size < (1 << 24)) {
		offidx->_.ele_size = 3;
	}
	else {
		offidx->_.ele_size = 4;
	}

	offidx->_.ptr = idx_mem_ptr;
	offidx->ele_start = NULL;
}

static inline void
offset_index_set(offset_index *offidx, size_t index, uint32_t value)
{
	if (index == 0 || index == offidx->_.ele_count) {
		return;
	}

	msgpacked_index_set((msgpacked_index *)offidx, index, value);
}

static bool
offset_index_set_next(offset_index *offidx, size_t index, uint32_t value)
{
	if (index >= offidx->_.ele_count) {
		return true;
	}

	uint32_t filled = offset_index_get_filled(offidx);

	if (index == filled) {
		offset_index_set(offidx, index, value);
		offset_index_set_filled(offidx, filled + 1);

		return true;
	}

	if (index < filled) {
		return value == offset_index_get_const(offidx, index);
	}

	return false;
}

static inline void
offset_index_set_filled(offset_index *offidx, uint32_t ele_filled)
{
	if (offidx->_.ele_count == 0) {
		return;
	}

	msgpacked_index_set((msgpacked_index *)offidx, 0, ele_filled);
}

static inline void
offset_index_set_ptr(offset_index *offidx, uint8_t *idx_mem, const uint8_t *packed_mem)
{
	msgpacked_index_set_ptr((msgpacked_index *)offidx, idx_mem);
	offidx->ele_start = packed_mem;
}

static inline void
offset_index_copy(offset_index *dest, const offset_index *src, size_t d_start, size_t s_start, size_t count, int delta)
{
	if (count > dest->_.ele_count) {
		count = dest->_.ele_count;
	}

	if (dest->_.ele_size == src->_.ele_size && delta == 0) {
		memcpy(offset_index_get_mem(dest, d_start),
				offset_index_get_mem(src, s_start),
				dest->_.ele_size * count);
	}
	else {
		for (size_t i = 0; i < count; i++) {
			uint32_t value = offset_index_get_const(src, s_start + i);

			value += delta;
			offset_index_set(dest, d_start + i, value);
		}
	}
}

static bool
offset_index_fill(offset_index *offidx, size_t index)
{
	uint32_t ele_filled = offset_index_get_filled(offidx);

	if (index < ele_filled || offidx->_.ele_count == ele_filled) {
		return true;
	}

	as_unpacker pk = {
			.buffer = offidx->ele_start,
			.offset = 0,
			.length = (int)offidx->tot_ele_sz
	};

	pk.offset = (int)offset_index_get_const(offidx, ele_filled - 1);

	for (size_t i = ele_filled; i < index; i++) {
		if (as_unpack_size(&pk) < 0) {
			return false;
		}

		if (as_unpack_size(&pk) < 0) {
			return false;
		}

		offset_index_set(offidx, i, (uint32_t)pk.offset);
	}

	if (as_unpack_size(&pk) < 0) {
		return false;
	}

	if (as_unpack_size(&pk) < 0) {
		return false;
	}

	// Make sure last iteration is in range for set.
	if (index < offidx->_.ele_count) {
		offset_index_set(offidx, index, (uint32_t)pk.offset);
		offset_index_set_filled(offidx, index + 1);
	}
	// Check if sizes match.
	else if (pk.offset != offidx->tot_ele_sz) {
		cf_warning(AS_PARTICLE, "offset_index_fill() offset mismatch %d, expected %zu", pk.offset, offidx->tot_ele_sz);
		return false;
	}
	else {
		offset_index_set_filled(offidx, offidx->_.ele_count);
	}

	return true;
}

static inline int64_t
offset_index_get(offset_index *offidx, size_t index)
{
	if (index > offidx->_.ele_count) {
		index = offidx->_.ele_count;
	}

	if (! offset_index_fill(offidx, index)) {
		return -1;
	}

	return (int64_t)offset_index_get_const(offidx, index);
}

static inline int64_t
offset_index_get_delta(offset_index *offidx, size_t index)
{
	int64_t offset = offset_index_get(offidx, index);

	if (offset < 0) {
		return offset;
	}

	if (index == offidx->_.ele_count - 1) {
		return offidx->tot_ele_sz - offset;
	}

	return offset_index_get(offidx, index + 1) - offset;
}

static void
offset_index_append_size(offset_index *offidx, uint32_t delta)
{
	uint32_t filled = offset_index_get_filled(offidx);

	if (filled == offidx->_.ele_count) {
		return;
	}

	uint32_t last = offset_index_get_const(offidx, filled - 1);

	offset_index_set_filled(offidx, filled + 1);
	offset_index_set(offidx, filled, last + delta);
}

static inline void *
offset_index_get_mem(const offset_index *offidx, size_t index)
{
	return msgpacked_index_get_mem((msgpacked_index *)offidx, index);
}

static inline size_t
offset_index_size(const offset_index *offidx)
{
	return msgpacked_index_size((const msgpacked_index *)offidx);
}

static inline bool
offset_index_is_null(const offset_index *offidx)
{
	return offidx->_.ptr == NULL;
}

static inline bool
offset_index_is_valid(const offset_index *offidx)
{
	return offidx->_.ptr != NULL;
}

static inline bool
offset_index_is_full(const offset_index *offidx)
{
	if (offset_index_is_null(offidx)) {
		return false;
	}

	if (offidx->_.ele_count == 0) {
		return true;
	}

	uint32_t filled = offset_index_get_filled(offidx);

	if (filled > offidx->_.ele_count) {
		cf_crash(AS_PARTICLE, "filled(%u) > ele_count(%zu)", filled, offidx->_.ele_count);
	}

	if (filled == offidx->_.ele_count) {
		return true;
	}

	return false;
}

static inline uint32_t
offset_index_get_const(const offset_index *offidx, size_t idx)
{
	if (idx == 0) {
		return 0;
	}

	if (idx == offidx->_.ele_count) {
		return offidx->tot_ele_sz;
	}

	if (idx >= offset_index_get_filled(offidx)) {
		offset_index_print(offidx, "offset_index_get_const() offidx");
		print_packed(offidx->ele_start, offidx->tot_ele_sz, "offset_index_get_const() offidx->ele_start");
		cf_crash(AS_PARTICLE, "offset_index_get_const() idx=%zu >= filled=%u ele_count=%zu", idx, offset_index_get_filled(offidx), offidx->_.ele_count);
	}

	return msgpacked_index_get((const msgpacked_index *)offidx, idx);
}

static inline uint32_t
offset_index_get_delta_const(const offset_index *offidx, size_t index)
{
	uint32_t offset = offset_index_get_const(offidx, index);

	if (index == offidx->_.ele_count - 1) {
		return offidx->tot_ele_sz - offset;
	}

	return offset_index_get_const(offidx, index + 1) - offset;
}

static inline uint32_t
offset_index_get_filled(const offset_index *offidx)
{
	if (offidx->_.ele_count == 0) {
		return 1;
	}

	return msgpacked_index_get((const msgpacked_index *)offidx, 0);
}

void
offset_index_print(const offset_index *offidx, const char *name)
{
	if (! name) {
		name = "offset";
	}

	msgpacked_index_print((msgpacked_index *)offidx, name);
}

void
offset_index_delta_print(const offset_index *offidx, const char *name)
{
	size_t ele_count = offidx->_.ele_count;
	char buf[1024];
	char *ptr = buf;

	if (offidx->_.ptr) {
		for (size_t i = 0; i < ele_count; i++) {
			if (buf + 1024 - ptr < 12) {
				break;
			}

			ptr += sprintf(ptr, "%u, ", offset_index_get_delta_const(offidx, i));
		}

		if (ele_count > 0) {
			ptr -= 2;
		}

		*ptr = '\0';
	}
	else {
		strcpy(buf, "(null)");
	}
	cf_warning(AS_PARTICLE, "%s: delta_off[%zu]={%s} %zu", name, ele_count, buf, offidx->tot_ele_sz);
}

//------------------------------------------------
// offidx_op

static void
offidx_op_init(offidx_op *op, offset_index *dest, const offset_index *src)
{
	op->dest = dest;
	op->src = src;
	op->d_i = 0;
	op->s_i = 0;
	op->delta = 0;
}

static void
offidx_op_add(offidx_op *op, uint32_t index, uint32_t mem_sz)
{
	uint32_t ele_count = op->src->_.ele_count;
	uint32_t count = index - op->s_i;

	if (op->s_i + count == ele_count) {
		offset_index_copy(op->dest, op->src, op->d_i, op->s_i, count, op->delta);
		offset_index_set(op->dest, op->d_i + count, op->src->tot_ele_sz + op->delta);
	}
	else {
		offset_index_copy(op->dest, op->src, op->d_i, op->s_i, count + 1, op->delta);
	}

	op->delta += mem_sz;
	op->d_i += count + 1;
	op->s_i += count;
}

static void
offidx_op_remove(offidx_op *op, uint32_t index)
{
	uint32_t count = index - op->s_i;
	uint32_t mem_sz = offset_index_get_delta_const(op->src, index);

	offset_index_copy(op->dest, op->src, op->d_i, op->s_i, count, op->delta);

	op->delta -= mem_sz;
	op->d_i += count;
	op->s_i += count + 1;
}

static void
offidx_op_remove_range(offidx_op *op, uint32_t index, uint32_t count)
{
	uint32_t ele_count = op->src->_.ele_count;
	uint32_t delta_count = index - op->s_i;
	uint32_t offset = offset_index_get_const(op->src, index);
	uint32_t mem_sz;

	if (index + count == ele_count) {
		mem_sz = op->src->tot_ele_sz - offset;
	}
	else {
		mem_sz = offset_index_get_const(op->src, index + count) - offset;
	}

	offset_index_copy(op->dest, op->src, op->d_i, op->s_i, delta_count, op->delta);

	op->delta -= mem_sz;
	op->d_i += delta_count;
	op->s_i += delta_count + count;
}

static void
offidx_op_end(offidx_op *op)
{
	uint32_t ele_count = op->src->_.ele_count;
	uint32_t count = ele_count - op->s_i;

	offset_index_copy(op->dest, op->src, op->d_i, op->s_i, count, op->delta);
	op->d_i += count;
	offset_index_set_filled(op->dest, op->d_i);
}

//------------------------------------------------
// order_heap

// Set cmp_key to true if comparing keys rather than values.
static inline void
order_heap_init(order_heap *heap, uint8_t *ptr, uint32_t ele_count, const packed_map_op *op, msgpack_compare_t cmp, bool cmp_key)
{
	order_index_init((order_index *)heap, ptr, op->ele_count);
	heap->filled = 0;
	heap->op = op;
	heap->heap_size = ele_count;
	heap->cmp = cmp;

	if (cmp_key) {
		heap->cmp_func = packed_map_op_compare_key_by_idx;
	}
	else {
		heap->cmp_func = packed_map_op_compare_value_by_idx;
	}
}

static inline void
order_heap_set(order_heap *heap, size_t index, uint32_t value)
{
	order_index_set((order_index *)heap, index, value);
}

static inline size_t
order_heap_size(const order_heap *heap)
{
	return heap->_._.ele_size * heap->heap_size;
}

static inline uint32_t
order_heap_get(const order_heap *heap, size_t index)
{
	return order_index_get((const order_index *)heap, index);
}

static void
order_heap_swap(order_heap *heap, uint32_t index1, uint32_t index2)
{
	uint32_t temp = order_heap_get(heap, index1);
	order_heap_set(heap, index1, order_heap_get(heap, index2));
	order_heap_set(heap, index2, temp);
}

static bool
order_heap_remove_top(order_heap *heap)
{
	if (heap->filled == 0) {
		return true;
	}

	uint32_t index = order_heap_get(heap, (heap->filled--) - 1);

	return order_heap_replace_top(heap, index);
}

static bool
order_heap_replace_top(order_heap *heap, uint32_t value)
{
	order_heap_set(heap, 0, value);

	return order_heap_heapify(heap, 0);
}

bool
order_heap_add(order_heap *heap, uint32_t value)
{
	const packed_map_op *op = heap->op;

	if (heap->filled >= heap->heap_size) {
		msgpack_compare_t cmp = heap->cmp_func(op,
				value,
				order_heap_get(heap, 0));

		if (cmp == MSGPACK_COMPARE_ERROR) {
			return false;
		}

		if (cmp == heap->cmp || cmp == MSGPACK_COMPARE_EQUAL) {
			return true;	// ignore
		}

		return order_heap_replace_top(heap, value);
	}

	uint32_t index = heap->filled++;

	order_heap_set(heap, index, value);

	while (index != 0) {
		uint32_t parent = (index - 1) / 2;
		msgpack_compare_t cmp = heap->cmp_func(op,
				order_heap_get(heap, index),
				order_heap_get(heap, parent));

		if (cmp != heap->cmp) {
			break;
		}

		order_heap_swap(heap, index, parent);
		index = parent;
	}

	return true;
}

static bool
order_heap_heapify(order_heap *heap, uint32_t index)
{
	const packed_map_op *op = heap->op;

	while (true) {
		uint32_t child1 = 2 * index + 1;
		uint32_t child2 = 2 * index + 2;
		uint32_t child;

		if (child1 >= heap->filled) {
			break;
		}

		if (child2 >= heap->filled) {
			child = child1;
		}
		else {
			msgpack_compare_t cmp = heap->cmp_func(op,
					order_heap_get(heap, child1),
					order_heap_get(heap, child2));

			if (cmp == MSGPACK_COMPARE_ERROR) {
				return false;
			}

			if (cmp == heap->cmp || cmp == MSGPACK_COMPARE_EQUAL) {
				child = child1;
			}
			else {
				child = child2;
			}
		}

		msgpack_compare_t cmp = heap->cmp_func(op,
				order_heap_get(heap, child),
				order_heap_get(heap, index));

		if (cmp == MSGPACK_COMPARE_ERROR) {
			return false;
		}

		if (cmp == heap->cmp) {
			order_heap_swap(heap, index, child);
			index = child;
		}
		else {
			break;
		}
	}

	return true;
}

// O(n)
static bool
order_heap_build(order_heap *heap, bool init)
{
	if (init) {
		heap->filled = heap->_._.ele_count;

		for (size_t i = 0; i < heap->filled; i++) {
			order_heap_set(heap, i, i);
		}
	}

	int64_t start = (int64_t)heap->filled / 2 - 1;

	for (int64_t i = start; i >= 0; i--) {
		if (! order_heap_heapify(heap, (uint32_t)i)) {
			return false;
		}
	}

	return true;
}

static bool
order_heap_order_at_end(order_heap *heap, uint32_t count)
{
	uint32_t end_index = heap->filled - 1;

	for (uint32_t i = 0; i < count; i++) {
		uint32_t value = order_heap_get(heap, 0);

		if (! order_heap_remove_top(heap)) {
			return false;
		}

		order_heap_set(heap, end_index--, value);
	}

	heap->filled = end_index + 1;

	return true;
}

// Reverse order of end indexes.
static void
order_heap_reverse_end(order_heap *heap, uint32_t count)
{
	uint32_t start = heap->filled;
	uint32_t end = start + count;
	uint32_t stop = (start + end) / 2;

	end--;

	for (uint32_t i = start; i < stop; i++) {
		uint32_t left = order_heap_get(heap, i);
		uint32_t right = order_heap_get(heap, end);

		order_heap_set(heap, end--, left);
		order_heap_set(heap, i, right);
	}
}

static uint32_t
order_heap_get_ordered(const order_heap *heap, uint32_t index)
{
	return order_heap_get(heap, heap->filled + index);
}

void
order_heap_print(const order_heap *heap)
{
	order_index_print(&heap->_, "heap");
}

//------------------------------------------------
// value_index

static inline void
order_index_init(order_index *ordidx, uint8_t *ptr, uint32_t ele_count)
{
	ordidx->_.ele_count = ele_count;

	if (ele_count < (1 << 8)) {
		ordidx->_.ele_size = 1;
	}
	else if (ele_count < (1 << 16)) {
		ordidx->_.ele_size = 2;
	}
	else if (ele_count < (1 << 24)) {
		ordidx->_.ele_size = 3;
	}
	else {
		ordidx->_.ele_size = 4;
	}

	ordidx->_.ptr = ptr;
}

static inline void
order_index_set(order_index *ordidx, size_t index, uint32_t value)
{
	msgpacked_index_set((msgpacked_index *)ordidx, index, value);
}

static inline void
order_index_set_ptr(order_index *ordidx, uint8_t *ptr)
{
	msgpacked_index_set_ptr((msgpacked_index *)ordidx, ptr);
}

static inline void
order_index_copy(order_index *dest, const order_index *src, size_t d_start, size_t s_start, size_t count, const order_index_adjust *adjust)
{
	if (dest->_.ele_size == src->_.ele_size && ! adjust) {
		memcpy(order_index_get_mem(dest, d_start),
				order_index_get_mem(src, s_start),
				src->_.ele_size * count);
	}
	else {
		for (size_t i = 0; i < count; i++) {
			uint32_t value = order_index_get(src, s_start + i);

			value = order_index_adjust_value(adjust, value);
			order_index_set(dest, d_start + i, value);
		}
	}
}

static bool
order_index_sort(order_index *ordidx, const offset_index *offsets, const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by)
{
	uint32_t ele_count = ordidx->_.ele_count;
	index_sort_userdata udata = {
			.order = ordidx,
			.offsets = offsets,
			.packed = ele_start,
			.packed_sz = tot_ele_sz,
			.error = false,
			.sort_by = sort_by
	};

	if (sort_by != SORT_BY_IDX) {
		if (! offsets || offset_index_is_null(offsets)) {
			offset_index temp_index;

			offset_index_inita(&temp_index, ele_start, tot_ele_sz, ele_count);
			offset_index_fill(&temp_index, ele_count);
			udata.offsets = &temp_index;
		}
	}

	qsort_r(order_index_get_mem(ordidx, 0), ele_count, ordidx->_.ele_size, map_packer_fill_index_sort_compare, (void *)&udata);

	if (udata.error) {
		return false;
	}

	return true;
}

static inline bool
order_index_set_sorted(order_index *ordidx, const offset_index *offsets, const uint8_t *ele_start, uint32_t tot_ele_sz, sort_by_t sort_by)
{
	uint32_t ele_count = ordidx->_.ele_count;

	for (uint32_t i = 0; i < ele_count; i++) {
		order_index_set(ordidx, i, i);
	}

	return order_index_sort(ordidx, offsets, ele_start, tot_ele_sz, sort_by);
}

static bool
order_index_set_sorted_with_offsets(order_index *ordidx, const offset_index *offsets, sort_by_t sort_by)
{
	return order_index_set_sorted(ordidx, offsets, offsets->ele_start, offsets->tot_ele_sz, sort_by);
}

static void
order_index_remove_dup_idx(order_index *ordidx, uint32_t x)
{
	uint32_t ele_count = ordidx->_.ele_count;
	uint32_t i = 0;

	for (; i < ele_count; i++) {
		if (order_index_get(ordidx, i) == x) {
			break;
		}
	}

	i++;

	while (i < ele_count) {
		if (order_index_get(ordidx, i) == x) {
			ele_count--;

			for (uint32_t j = i; j < ele_count; j++) {
				uint32_t temp = order_index_get(ordidx, j + 1);

				order_index_set(ordidx, j, temp);
			}
		}
		else {
			i++;
		}
	}

	ordidx->_.ele_count = ele_count;
}

// Remove dups in a sorted order_index.
static uint32_t
order_index_sorted_remove_dups(order_index *ordidx)
{
	uint32_t count = (uint32_t)ordidx->_.ele_count;

	if (count <= 1) {
		return count;
	}

	uint32_t prev_idx = order_index_get(ordidx, 0);
	uint32_t d_i = 1;
	uint32_t ret_count = count;

	for (uint32_t i = d_i; i < count; i++) {
		uint32_t idx = order_index_get(ordidx, i);

		if (prev_idx == idx) {
			ret_count--;
			continue;
		}

		if (d_i != i) {
			order_index_set(ordidx, d_i, idx);
		}

		d_i++;
		prev_idx = idx;
	}

	ordidx->_.ele_count = ret_count;

	return ret_count;
}

// Remove dups while keeping the order intact.
static bool
order_index_remove_dups(order_index *ordidx, const order_index *sorted_hint)
{
	order_index sorted_temp;

	if (! sorted_hint) {
		order_index_inita_copy(&sorted_temp, ordidx);

		if (! order_index_sort(&sorted_temp, NULL, NULL, 0, SORT_BY_IDX)) {
			return false;
		}

		sorted_hint = &sorted_temp;
	}

	uint32_t ele_count = ordidx->_.ele_count;
	uint32_t prev = ele_count;
	bool is_prev = false;

	for (uint32_t i = 0; i < ele_count; i++) {
		uint32_t idx = order_index_get(sorted_hint, i);

		if (idx == prev) {
			if (! is_prev) {
				order_index_remove_dup_idx(ordidx, idx);
			}

			is_prev = true;
			continue;
		}

		prev = idx;
		is_prev = false;
	}

	return true;
}

static uint32_t
order_index_find_idx(const order_index *ordidx, uint32_t idx, uint32_t start, uint32_t len)
{
	for (uint32_t i = start; i < start + len; i++) {
		if (order_index_get(ordidx, i) == idx) {
			return i;
		}
	}

	return start + len;
}

static bool
order_index_sorted_has_dups(const order_index *ordidx)
{
	uint32_t count = (uint32_t)ordidx->_.ele_count;

	if (count <= 1) {
		return false;
	}

	uint32_t idx = order_index_get(ordidx, 0);

	for (uint32_t i = 1; i < count; i++) {
		uint32_t prev_idx = idx;

		idx = order_index_get(ordidx, i);

		if (prev_idx == idx) {
			return true;
		}
	}

	return false;
}

// Get pointer at index.
static inline void *
order_index_get_mem(const order_index *ordidx, size_t index)
{
	return msgpacked_index_get_mem((const msgpacked_index *)ordidx, index);
}

static inline size_t
order_index_size(const order_index *ordidx)
{
	return msgpacked_index_size((const msgpacked_index *)ordidx);
}

static inline bool
order_index_is_null(const order_index *ordidx)
{
	return ordidx->_.ptr == NULL;
}

static inline bool
order_index_is_valid(const order_index *ordidx)
{
	return ordidx->_.ptr != NULL;
}

static inline bool
order_index_is_filled(const order_index *ordidx)
{
	if (! order_index_is_valid(ordidx)) {
		return false;
	}

	if (ordidx->_.ele_count > 0 && order_index_get(ordidx, 0) >= ordidx->_.ele_count) {
		return false;
	}

	return true;
}

static inline uint32_t
order_index_ptr2value(const order_index *ordidx, const void *ptr)
{
	return msgpacked_index_ptr2value((const msgpacked_index *)ordidx, ptr);
}

static inline uint32_t
order_index_get(const order_index *ordidx, size_t index)
{
	return msgpacked_index_get((const msgpacked_index *)ordidx, index);
}

static void order_index_print(const order_index *ordidx, const char *name)
{
	if (! name) {
		name = "value";
	}

	msgpacked_index_print(&ordidx->_, name);
}

//------------------------------------------------
// order_index_adjust

static inline uint32_t
order_index_adjust_value(const order_index_adjust *via, uint32_t src)
{
	if (via) {
		return via->f(via, src);
	}

	return src;
}

static uint32_t
order_index_adjust_lower(const order_index_adjust *via, uint32_t src)
{
	if (src >= via->lower) {
		return src + via->delta;
	}

	return src;
}

static uint32_t
order_index_adjust_mid(const order_index_adjust *via, uint32_t src)
{
	if (src >= via->lower && src < via->upper) {
		return src + via->delta;
	}

	return src;
}

//------------------------------------------------
// order_index_op

static inline void
order_index_op_add(order_index *dest, const order_index *src, uint32_t add_idx, int64_t add_rank)
{
	uint32_t ele_count = src->_.ele_count;
	order_index_adjust adjust = {
			.f = order_index_adjust_lower,
			.lower = add_idx,
			.upper = 0,
			.delta = 1
	};

	order_index_copy(dest, src, 0, 0, add_rank, &adjust);
	order_index_set(dest, add_rank, add_idx);
	order_index_copy(dest, src, add_rank + 1, add_rank, ele_count - add_rank, &adjust);
}

static bool
order_index_op_remove_or_replace_internal(order_index *dest, const order_index *src, uint32_t add_rank, uint32_t remove_rank, uint32_t remove_count)
{
	uint32_t ele_count = src->_.ele_count;
	uint32_t remove_idx = order_index_get(src, remove_rank);
	uint32_t add_idx = remove_idx;	// Same for now
	uint32_t src_stop = ele_count;
	uint32_t find_idx = remove_idx;
	uint32_t si = remove_rank;
	uint32_t di = remove_rank;

	order_index_adjust adjust = {
			.f = order_index_adjust_lower,
			.lower = remove_idx + remove_count,
			.upper = 0,
			.delta = -remove_count
	};

	// Replace mode.
	if (src->_.ele_count - dest->_.ele_count != remove_count) {
		adjust.delta++;

		if (add_rank > remove_rank) {
			src_stop = add_rank;
		}
		else {
			di++;
		}
	}

	for (uint32_t i = 0; i < remove_count; i++) {
		bool found = false;

		while (si < src_stop) {
			if (order_index_get(src, si) == find_idx) {
				found = true;
				si++;
				break;
			}

			uint32_t value = order_index_adjust_value(&adjust, order_index_get(src, si++));

			order_index_set(dest, di++, value);
		}

		if (! found) {
			if (src_stop < ele_count) {
				src_stop = ele_count;
				i--;
				order_index_set(dest, di++, add_idx);
				continue;
			}

			return false;
		}

		find_idx++;
	}

	if (add_rank >= si) {
		uint32_t size = (uint32_t)add_rank - si;

		order_index_copy(dest, src, di, si, size, &adjust);
		order_index_set(dest, add_rank - remove_count, add_idx);
		di += size + 1;
		si += size;
		order_index_copy(dest, src, di, si, ele_count - add_rank, &adjust);
	}
	else {
		order_index_copy(dest, src, di, si, ele_count - si, &adjust);
	}

	return true;
}

static inline void
order_index_op_replace1_internal(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank, const order_index_adjust *adjust)
{
	uint32_t ele_count = src->_.ele_count;

	if (add_rank == remove_rank) {
		order_index_copy(dest, src, 0, 0, ele_count, NULL);
	}
	else if (add_rank > remove_rank) {
		order_index_copy(dest, src, 0, 0, remove_rank, adjust);
		order_index_copy(dest, src, remove_rank, remove_rank + 1, add_rank - remove_rank - 1, adjust);
		order_index_set(dest, add_rank - 1, add_idx);
		order_index_copy(dest, src, add_rank, add_rank, ele_count - add_rank, adjust);
	}
	else {
		order_index_copy(dest, src, 0, 0, add_rank, adjust);
		order_index_set(dest, add_rank, add_idx);
		order_index_copy(dest, src, add_rank + 1, add_rank, remove_rank - add_rank, adjust);
		order_index_copy(dest, src, remove_rank + 1, remove_rank + 1, ele_count - remove_rank - 1, adjust);
	}
}

// Replace remove_rank with add_rank in dest.
static inline void
order_index_op_replace1(order_index *dest, const order_index *src, uint32_t add_rank, uint32_t remove_rank)
{
	uint32_t add_idx = order_index_get(src, remove_rank);

	order_index_op_replace1_internal(dest, src, add_idx, add_rank, remove_rank, NULL);
}

// Replace remove_rank with add_rank in dest with specific add_idx.
static inline void
order_index_op_replace1_idx(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank)
{
	uint32_t remove_idx = order_index_get(src, remove_rank);
	order_index_adjust adjust = {
			.f = order_index_adjust_mid,
	};

	if (add_rank == remove_rank) {
		adjust.delta = 0;
	}
	else if (add_rank > remove_rank) {
		adjust.lower = remove_idx + 1;
		adjust.upper = add_idx;
		adjust.delta = -1;
		add_idx--;
	}
	else {
		adjust.lower = add_idx;
		adjust.upper = remove_idx + 1;
		adjust.delta = 1;
	}

	order_index_op_replace1_internal(dest, src, add_idx, add_rank, remove_rank, &adjust);
}

// Replace rank range (remove_rank, remove_count) with add_rank in dest index array.
//  add_rank Set to -1 if no add op.
// Return true on success.
static inline bool
order_index_op_replace(order_index *dest, const order_index *src, uint32_t add_idx, uint32_t add_rank, uint32_t remove_rank, uint32_t remove_count)
{
	uint32_t remove_idx = order_index_get(src, remove_rank);
	order_index_adjust adjust = {
			.f = order_index_adjust_lower,
			.lower = remove_idx + remove_count,
			.upper = 0,
			.delta = -remove_count + 1
	};

	if (add_rank > remove_rank) {
		order_index_copy(dest, src, 0, 0, remove_rank, &adjust);
	}
	else {
		order_index_copy(dest, src, 0, 0, add_rank, &adjust);
		order_index_set(dest, add_rank, add_idx);
		order_index_copy(dest, src, add_rank + 1, add_rank, remove_rank - add_rank, &adjust);
	}

	return order_index_op_remove_or_replace_internal(dest, src,
			add_rank,
			remove_rank, remove_count);
}

static inline bool
order_index_op_remove(order_index *dest, const order_index *src, uint32_t remove_rank, uint32_t remove_count)
{
	uint32_t remove_idx = order_index_get(src, remove_rank);
	order_index_adjust adjust = {
			.f = order_index_adjust_lower,
			.lower = remove_idx + remove_count,
			.upper = 0,
			.delta = -remove_count
	};

	order_index_copy(dest, src, 0, 0, remove_rank, &adjust);

	return order_index_op_remove_or_replace_internal(dest, src,
			0,
			remove_rank, remove_count);
}

static void
order_index_op_remove_indexes(order_index *dest, const order_index *src, const order_index *sorted_indexes, uint32_t count)
{
	uint32_t di = 0;

	for (uint32_t i = 0; i < src->_.ele_count; i++) {
		uint32_t index = order_index_get(src, i);
		uint32_t where = 0;

		if (! msgpacked_index_find_index_sorted(&sorted_indexes->_, index, count, &where)) {
			index -= where;
			order_index_set(dest, di++, index);
		}
	}
}


//==========================================================
// result_data

static int
result_data_set_index_rank_count(cdt_result_data *rd, uint32_t start, uint32_t count, uint32_t ele_count)
{
	bool is_reverse = false;

	switch (rd->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(rd->result, count);
		break;
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_REVRANK:
		is_reverse = true;
		/* no break */
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_RANK: {
		if (! rd->is_multi) {
			if (count == 0) {
				as_bin_set_int(rd->result, -1);
				break;
			}

			if (is_reverse) {
				start = ele_count - start - 1;
			}

			as_bin_set_int(rd->result, start);
		}
		else {
			cdt_container_builder builder;

			if (! cdt_list_builder_start(&builder, rd->alloc, count, count * (sizeof(int64_t) + 1))) {
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}

			for (uint32_t i = 0; i < count; i++) {
				int64_t n = start + i;

				if (is_reverse) {
					n = ele_count - n - 1;
				}

				cdt_container_builder_add_int64(&builder, n);
			}

			rd->result->particle = builder.particle;
			as_bin_state_set_from_type(rd->result, AS_PARTICLE_TYPE_LIST);
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "result_data_set_index_rank_count() invalid return type %d", rd->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return AS_PROTO_RESULT_OK;
}

static int
result_data_set_range(cdt_result_data *rd, uint32_t start, uint32_t count, uint32_t ele_count)
{
	switch (rd->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_COUNT:
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_REVRANK:
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_RANK:
		return result_data_set_index_rank_count(rd, start, count, ele_count);
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		start = ele_count - start - count;
		/* no break */
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_RANK_RANGE: {
		if (! result_data_set_list_int2x(rd, start, count)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "result_data_set_range() invalid return type %d", rd->type);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return AS_PROTO_RESULT_OK;
}

static bool
result_data_set_list_int2x(cdt_result_data *rd, int64_t i1, int64_t i2)
{
	cdt_container_builder builder;

	if (! cdt_list_builder_start(&builder, rd->alloc, 2, 2 * (sizeof(int64_t) + 1))) {
		return false;
	}

	cdt_container_builder_add_int64(&builder, i1);
	cdt_container_builder_add_int64(&builder, i2);

	rd->result->particle = builder.particle;
	as_bin_state_set_from_type(rd->result, AS_PARTICLE_TYPE_LIST);

	return true;
}

static bool
result_data_set_not_found(cdt_result_data *rd, int64_t index)
{
	switch (rd->type) {
	case RESULT_TYPE_NONE:
		break;
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_INDEX_RANGE:
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		return result_data_set_list_int2x(rd, index, 0);
	case RESULT_TYPE_INDEX:
	case RESULT_TYPE_REVINDEX:
	case RESULT_TYPE_RANK:
	case RESULT_TYPE_REVRANK:
		if (rd->is_multi) {
			rd->result->particle = packed_list_simple_create_empty(rd->alloc);
			as_bin_state_set_from_type(rd->result, AS_PARTICLE_TYPE_LIST);

			break;
		}

		as_bin_set_int(rd->result, -1);
		break;
	case RESULT_TYPE_COUNT:
		as_bin_set_int(rd->result, 0);
		break;
	case RESULT_TYPE_KEY:
	case RESULT_TYPE_VALUE:
		if (rd->is_multi) {
			rd->result->particle = packed_list_simple_create_empty(rd->alloc);
			as_bin_state_set_from_type(rd->result, AS_PARTICLE_TYPE_LIST);
		}
		break;
	case RESULT_TYPE_MAP:
		as_bin_set_empty_packed_map(rd->result, rd->alloc, AS_PACKED_MAP_FLAG_PRESERVE_ORDER);
		break;
	default:
		return false;
	}

	return true;
}

static bool
result_data_set_key_not_found(cdt_result_data *rd, int64_t index)
{
	switch (rd->type) {
	case RESULT_TYPE_RANK_RANGE:
	case RESULT_TYPE_REVRANK_RANGE:
		return false;
	default:
		return result_data_set_not_found(rd, index);
	}

	return true;
}

static bool
result_data_set_value_not_found(cdt_result_data *rd, int64_t rank)
{
	switch (rd->type) {
	case RESULT_TYPE_REVINDEX_RANGE:
	case RESULT_TYPE_INDEX_RANGE:
		return false;
	default:
		return result_data_set_not_found(rd, rank);
	}

	return true;
}

static bool
result_data_set_ordered_list(cdt_result_data *rd, order_index *ordidx, uint32_t count)
{
	cdt_container_builder builder;

	if (! cdt_list_builder_start(&builder, rd->alloc, count, (sizeof(uint64_t) + 1) * count)) {
		return false;
	}

	for (uint32_t i = 0; i < count; i++) {
		uint32_t idx = order_index_get(ordidx, i);

		cdt_container_builder_add_int64(&builder, (int64_t)idx);
	}

	rd->result->particle = builder.particle;
	as_bin_state_set_from_type(rd->result, AS_PARTICLE_TYPE_LIST);

	return true;
}

static inline bool
result_data_is_return_elements(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_KEY	|| rd->type == RESULT_TYPE_VALUE || rd->type == RESULT_TYPE_MAP);
}

static inline bool
result_data_is_return_index(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_INDEX || rd->type == RESULT_TYPE_REVINDEX);
}

static inline bool
result_data_is_return_index_range(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_INDEX_RANGE || rd->type == RESULT_TYPE_REVINDEX_RANGE);
}

static inline bool
result_data_is_return_rank(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_REVRANK	|| rd->type == RESULT_TYPE_RANK);
}

static inline bool
result_data_is_return_rank_range(const cdt_result_data *rd)
{
	return (rd->type == RESULT_TYPE_REVRANK_RANGE || rd->type == RESULT_TYPE_RANK_RANGE);
}


//==========================================================
// cdt_map_builder
//

bool
cdt_map_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t max_size, uint8_t flags)
{
	uint32_t new_size = sizeof(map_mem) + sizeof(uint64_t) + 1 + 3 + max_size;
	map_mem *p_map_mem = (map_mem *)rollback_alloc_reserve(alloc_buf, new_size);

	if (! p_map_mem) {
		return false;
	}

	as_packer pk = {
			.head = NULL,
			.tail = NULL,
			.buffer = p_map_mem->data,
			.offset = 0,
			.capacity = INT_MAX
	};

	if (flags != AS_PACKED_MAP_FLAG_NONE) {
		as_pack_map_header(&pk, ele_count + 1);
		as_pack_ext_header(&pk, 0, flags);
		pk.buffer[pk.offset++] = msgpack_nil[0];
	}
	else {
		as_pack_map_header(&pk, ele_count);
	}

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = (uint32_t)pk.offset;

	builder->particle = (as_particle *)p_map_mem;
	builder->write_ptr = p_map_mem->data + p_map_mem->sz;
	builder->ele_count = 0;
	builder->header_ele_count = ele_count;
	builder->size = &p_map_mem->sz;

	return true;
}

void
cdt_map_builder_finalize(cdt_container_builder *builder)
{
	if (builder->ele_count == builder->header_ele_count) {
		return;
	}

	uint32_t hdr_size = as_pack_list_header_get_size(builder->ele_count);
	map_mem *p_map_mem = (map_mem *)builder->particle;
	uint32_t current_hdr_size = as_pack_list_header_get_size(builder->header_ele_count);

	if (hdr_size != current_hdr_size) {
		int64_t delta = (int64_t)hdr_size - current_hdr_size;
		size_t size = 0;

		memmove(p_map_mem->data + delta + current_hdr_size, p_map_mem->data + current_hdr_size, size);
		p_map_mem->sz += delta;
	}

	as_packer pk = {
			.head = NULL,
			.tail = NULL,
			.buffer = p_map_mem->data,
			.offset = 0,
			.capacity = INT_MAX
	};

	as_pack_map_header(&pk, builder->ele_count);
}


//==========================================================
// cdt_process_state_packed_map
//

bool
cdt_process_state_packed_map_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata)
{
	as_bin *b = cdt_udata->b;
	as_bin *result = cdt_udata->result;
	as_cdt_optype optype = state->type;

	if (! is_map_type(as_bin_get_particle_type(b)) && as_bin_inuse(b)) {
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() invalid type %d", as_bin_get_particle_type(b));
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		return false;
	}

	rollback_alloc_inita(alloc_buf, cdt_udata->alloc_buf, 1, true);
	// Results always on the heap.
	rollback_alloc_inita(alloc_result, NULL, 1, false);

	cdt_result_data result_data = {
			.result = result,
			.alloc = alloc_result,
	};

	switch (optype) {
	case AS_CDT_OP_MAP_SET_TYPE: {
		uint64_t create_type_flags;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &create_type_flags)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_map_if_notinuse(b);

		int ret = packed_map_set_flags(b, alloc_buf, result, (uint8_t)create_type_flags);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "AS_CDT_OP_MAP_SET_TYPE: failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_ADD: {
		cdt_payload key;
		cdt_payload value;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value, &flags)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_flagged_map_if_notinuse(b, flags);

		map_add_control control = {
				.allow_overwrite = false,
				.allow_create = true,
		};

		int ret = packed_map_add(b, alloc_buf, &key, &value, result, &control);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() ADD failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_ADD_ITEMS: {
		cdt_payload items;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &flags)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_flagged_map_if_notinuse(b, flags);

		map_add_control control = {
				.allow_overwrite = false,
				.allow_create = true,
		};

		int ret = packed_map_add_items(b, alloc_buf, &items, result, &control);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() ADD_ITEMS failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_PUT: {
		cdt_payload key;
		cdt_payload value;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value, &flags)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_flagged_map_if_notinuse(b, flags);

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = true,
		};

		int ret = packed_map_add(b, alloc_buf, &key, &value, result, &control);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() PUT failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_PUT_ITEMS: {
		cdt_payload items;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items, &flags)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_flagged_map_if_notinuse(b, flags);

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = true,
		};

		int ret = packed_map_add_items(b, alloc_buf, &items, result, &control);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() PUT_ITEMS failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REPLACE: {
		cdt_payload key;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &value)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_map_if_notinuse(b);

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = false,
		};

		int ret = packed_map_add(b, alloc_buf, &key, &value, result, &control);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() PUT failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REPLACE_ITEMS: {
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &items)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_ELEMENT_NOT_FOUND;
			return false;
		}

		map_add_control control = {
				.allow_overwrite = true,
				.allow_create = false,
		};

		int ret = packed_map_add_items(b, alloc_buf, &items, result, &control);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REPLACE_ITEMS failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_INCREMENT:
	case AS_CDT_OP_MAP_DECREMENT: {
		cdt_payload key;
		cdt_payload delta_value;
		uint64_t flags = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &key, &delta_value, &flags)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_flagged_map_if_notinuse(b, flags);

		int ret = packed_map_increment(b, alloc_buf, &key, state->ele_count >= 2 ? &delta_value : NULL, result, optype == AS_CDT_OP_MAP_DECREMENT);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() INCREMENT/DECREMENT failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload key;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_remove_by_key(b, alloc_buf, &key, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_KEY failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_INDEX: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_remove_by_index_range(b, alloc_buf, index, 1, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_INDEX failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_remove_by_value_interval(b, alloc_buf, &value, &value, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_VALUE failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_RANK: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_remove_by_rank_range(b, alloc_buf, index, 1, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_RANK failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_remove_all_key_items(b, alloc_buf, &items, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_KEY_LIST failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_remove_by_value_interval(b, alloc_buf, &value, &value, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_ALL_BY_VALUE failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload items;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &items)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_remove_all_value_items(b, alloc_buf, &items, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_VALUE_LIST failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload key_start;
		cdt_payload key_end;
		cdt_payload *p_key_end = NULL;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key_start, &key_end)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (state->ele_count > 2) {
			p_key_end = &key_end;
		}

		result_data.type = result_type;
		result_data.is_multi = true;

		int ret = packed_map_remove_by_key_interval(b, alloc_buf, &key_start, p_key_end, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_KEY_INTERVAL failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		int64_t index;
		uint64_t count = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		// User specifically asked for 0 count.
		if (state->ele_count == 3 && count == 0) {
			if (! result_data_set_key_not_found(&result_data, index)) {
				cf_warning(AS_PARTICLE, "REMOVE_BY_INDEX_RANGE: result_type %d not supported", result_data.type);
				cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
				return false;
			}
			break;
		}

		int ret = packed_map_remove_by_index_range(b, alloc_buf, index, count, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_INDEX_RANGE failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end;
		cdt_payload *p_value_end = NULL;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start, &value_end)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (state->ele_count > 2) {
			p_value_end = &value_end;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_remove_by_value_interval(b, alloc_buf, &value_start, p_value_end, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_VALUE_INTERVAL failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		uint64_t result_type;
		int64_t rank;
		uint64_t count = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		// User specifically asked for 0 count.
		if (state->ele_count == 3 && count == 0) {
			if (! result_data_set_value_not_found(&result_data, rank)) {
				cf_warning(AS_PARTICLE, "REMOVE_BY_RANK_RANGE: result_type %d not supported", result_data.type);
				cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
				return false;
			}

			break;
		}

		int ret = packed_map_remove_by_rank_range(b, alloc_buf, rank, count, &result_data);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() REMOVE_BY_RANK_RANGE failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_CLEAR: {
		if (! as_bin_inuse(b)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		int ret = packed_map_clear(b, alloc_buf, result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "AS_CDT_OP_LIST_CLEAR: failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_modify_optype() invalid cdt op: %d", optype);
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
		return false;
	}

	if (as_bin_is_temp_packed_map(b)) {
		as_bin_set_empty(b);
	}

	return true;
}

bool
cdt_process_state_packed_map_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata)
{
	const as_bin *b = cdt_udata->b;
	as_bin *result = cdt_udata->result;
	as_cdt_optype optype = state->type;

	if (! is_map_type(as_bin_get_particle_type(b))) {
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		return false;
	}

	// Just one entry needed for results bin.
	rollback_alloc_inita(alloc_result, NULL, 1, false);

	cdt_result_data result_data = {
			.result = result,
			.alloc = alloc_result,
	};

	switch (optype) {
	case AS_CDT_OP_MAP_SIZE: {
		packed_map_op op;

		if (! packed_map_op_init_from_bin(&op, b)) {
			cf_warning(AS_PARTICLE, "AS_CDT_OP_MAP_GET: invalid packed map, ele_count=%u", op.ele_count);
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_set_int(result, op.ele_count);

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY: {
		uint64_t result_type;
		cdt_payload key;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_get_by_key(b, &key, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_get_by_value_interval(b, &value, &value, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_INDEX: {
		uint64_t result_type;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_get_by_index_range(b, index, 1, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_RANK: {
		uint64_t result_type;
		int64_t rank;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = false;

		int ret = packed_map_get_by_rank_range(b, rank, 1, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_ALL_BY_VALUE: {
		uint64_t result_type;
		cdt_payload value;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_get_by_value_interval(b, &value, &value, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL: {
		uint64_t result_type;
		cdt_payload key_start;
		cdt_payload key_end;
		cdt_payload *p_key_end = NULL;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &key_start, &key_end)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (state->ele_count > 2) {
			p_key_end = &key_end;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_get_by_key_interval(b, &key_start, p_key_end, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL: {
		uint64_t result_type;
		cdt_payload value_start;
		cdt_payload value_end;
		cdt_payload *p_value_end = NULL;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &value_start, &value_end)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (state->ele_count > 2) {
			p_value_end = &value_end;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		int ret = packed_map_get_by_value_interval(b, &value_start, p_value_end, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_INDEX_RANGE: {
		uint64_t result_type;
		int64_t index;
		uint64_t count = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &index, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		// User specifically asked for 0 count.
		if (state->ele_count == 3 && count == 0) {
			if (! result_data_set_key_not_found(&result_data, index)) {
				cf_warning(AS_PARTICLE, "GET_BY_INDEX_RANGE: result_type %d not supported", result_data.type);
				cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
				return false;
			}
			break;
		}

		int ret = packed_map_get_by_index_range(b, index, count, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	case AS_CDT_OP_MAP_GET_BY_RANK_RANGE: {
		uint64_t result_type;
		int64_t rank;
		uint64_t count = 0;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &result_type, &rank, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		result_data.type = (result_type_t)result_type;
		result_data.is_multi = true;

		// User specifically asked for 0 count.
		if (state->ele_count == 3 && count == 0) {
			if (! result_data_set_value_not_found(&result_data, rank)) {
				cf_warning(AS_PARTICLE, "AS_CDT_OP_MAP_GET_BY_RANK_RANGE: result_type %d not supported", result_data.type);
				cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
				return false;
			}

			break;
		}

		int ret = packed_map_get_by_rank_range(b, rank, count, &result_data);

		if (ret < 0) {
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			return false;
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_map_read_optype() invalid cdt op: %d", optype);
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
		return false;
	}

	return true;
}


//==========================================================
// Debugging support.
//

void
print_index32(const uint32_t *index, uint32_t ele_count, const char *name)
{
	char buf[1024];
	char *ptr = buf;
	for (uint32_t i = 0; i < ele_count; i++) {
		if (buf + 1024 - ptr < 10) {
			break;
		}
		ptr += sprintf(ptr, "%u, ", index[i]);
	}
	if (ele_count > 0) {
		ptr -= 2;
	}
	*ptr = '\0';
	cf_warning(AS_PARTICLE, "%s: index32[%u]={%s}", name, ele_count, buf);
}

void
print_vindex(const order_index *index, const char *name)
{
	if (! index || order_index_is_null(index)) {
		return;
	}

	uint32_t ele_count = index->_.ele_count + 2;
	char buf[1024];
	char *ptr = buf;
	for (uint32_t i = 0; i < ele_count; i++) {
		if (buf + 1024 - ptr < 10) {
			break;
		}
		ptr += sprintf(ptr, "%u, ", order_index_get(index, i));
	}
	if (ele_count > 0) {
		ptr -= 2;
	}
	*ptr = '\0';
	cf_warning(AS_PARTICLE, "%s: vindex[%u]={%s}", name, ele_count, buf);
}

bool
as_bin_verify(const as_bin *b)
{
	packed_map_op op;

	uint8_t type = as_bin_get_particle_type(b);

	if (type != AS_PARTICLE_TYPE_MAP) {
		cf_warning(AS_PARTICLE, "as_bin_verify() non-map type: %u", type);
		return false;
	}

	// Check header.
	if (! packed_map_op_init_from_bin(&op, b)) {
		cf_warning(AS_PARTICLE, "as_bin_verify() invalid packed map");
		return false;
	}

	if (op.pmi.flags != 0) {
		const uint8_t *byte = op.packed + op.ele_start - 1;

		if (*byte != 0xC0) {
			cf_warning(AS_PARTICLE, "as_bin_verify() invalid ext header, expected C0 for pair.2");
		}
	}

	offset_index *offidx = &op.pmi.offset_idx;
	const order_index *ordidx = &op.pmi.value_idx;
	bool check_offidx = has_offidx(&op);

	as_unpacker pk;
	packed_map_op_init_unpacker(&op, &pk);

	offset_index_inita_from_op_if_invalid(offidx, &op);

	uint32_t filled = offset_index_get_filled(offidx);
	offset_index temp_offidx;

	offset_index_inita(&temp_offidx, NULL, offidx->tot_ele_sz, offidx->_.ele_count);
	offset_index_copy(&temp_offidx, offidx, 0, 0, filled, 0);

	// Check offsets.
	for (uint32_t i = 0; i < op.ele_count; i++) {
		uint32_t offset;

		if (check_offidx) {
			if (i < filled) {
				offset = offset_index_get_const(offidx, i);

				if (pk.offset != offset) {
					cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%u expected=%d", i, offset, pk.offset);
					return false;
				}
			}
			else {
				offset_index_set(&temp_offidx, i, pk.offset);
			}
		}
		else {
			offset_index_set(offidx, i, pk.offset);
		}

		offset = (uint32_t)pk.offset;

		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%u pk.offset=%d invalid key", i, offset, pk.offset);
			return false;
		}

		offset = (uint32_t)pk.offset;

		if (as_unpack_size(&pk) < 0) {
			cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%u pk.offset=%d invalid value", i, offset, pk.offset);
			return false;
		}
	}

	if (check_offidx && filled < op.ele_count) {
		offidx->_.ptr = temp_offidx._.ptr;
	}

	// Check packed size.
	if (op.packed_sz - op.ele_start != pk.offset) {
		cf_warning(AS_PARTICLE, "as_bin_verify() content_size=%u expected=%d", op.packed_sz - op.ele_start, pk.offset);
		return false;
	}

	// Check key orders.
	if (op_is_k_ordered(&op) && op.ele_count > 0) {
		packed_map_op_init_unpacker(&op, &pk);

		as_unpacker pk_key;
		packed_map_op_init_unpacker(&op, &pk_key);

		for (uint32_t i = 1; i < op.ele_count; i++) {
			int offset = pk.offset;
			msgpack_compare_t cmp = as_unpack_compare(&pk_key, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%d pk.offset=%d invalid key", i, offset, pk.offset);
				return false;
			}

			if (cmp == MSGPACK_COMPARE_GREATER) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%d pk.offset=%d keys not in order", i, offset, pk.offset);
				return false;
			}

			pk_key.offset = offset;

			if (as_unpack_size(&pk) < 0) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%u pk.offset=%d invalid value", i, offset, pk.offset);
				return false;
			}
		}
	}

	// Check value orders.
	if (order_index_is_filled(ordidx) && op.ele_count > 0) {
		// Compare with freshly sorted.
		order_index cmp_order;

		order_index_inita(&cmp_order, op.ele_count);
		order_index_set_sorted(&cmp_order, offidx, op.packed + op.ele_start, op.packed_sz - op.ele_start, SORT_BY_VALUE);

		for (uint32_t i = 0; i < op.ele_count; i++) {
			uint32_t expected = order_index_get(&cmp_order, i);
			uint32_t index = order_index_get(ordidx, i);

			if (index != expected) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u index=%u expected=%u invalid order index", i, index, expected);
				return false;
			}
		}

		// Walk index and check value order.
		packed_map_op_init_unpacker(&op, &pk);

		as_unpacker prev_value;
		packed_map_op_init_unpacker(&op, &prev_value);

		uint32_t index = order_index_get(ordidx, 0);

		prev_value.offset = offset_index_get_const(offidx, index);

		if (as_unpack_size(&prev_value) < 0) {
			cf_warning(AS_PARTICLE, "as_bin_verify() index=%u pk.offset=%d invalid key", index, pk.offset);
			return false;
		}

		for (uint32_t i = 1; i < op.ele_count; i++) {
			index = order_index_get(ordidx, i);
			pk.offset = offset_index_get_const(offidx, index);

			if (as_unpack_size(&pk) < 0) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u index=%u pk.offset=%d invalid key", i, index, pk.offset);
				return false;
			}

			int offset = pk.offset;
			msgpack_compare_t cmp = as_unpack_compare(&prev_value, &pk);

			if (cmp == MSGPACK_COMPARE_ERROR) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%d pk.offset=%d invalid value", i, offset, pk.offset);
				return false;
			}

			if (cmp == MSGPACK_COMPARE_GREATER) {
				cf_warning(AS_PARTICLE, "as_bin_verify() i=%u offset=%d pk.offset=%d value index not in order", i, offset, pk.offset);
				return false;
			}

			prev_value.offset = offset;
		}
	}

	return true;
}
