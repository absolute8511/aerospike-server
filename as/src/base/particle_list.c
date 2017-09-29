/*
 * particle_list.c
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

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/cdt.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"


//==========================================================
// LIST particle interface - function declarations.
//

// Destructor, etc.
void list_destruct(as_particle *p);
uint32_t list_size(const as_particle *p);

// Handle "wire" format.
int32_t list_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t list_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int list_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
uint32_t list_wire_size(const as_particle *p);
uint32_t list_to_wire(const as_particle *p, uint8_t *wire);

// Handle as_val translation.
uint32_t list_size_from_asval(const as_val *val);
void list_from_asval(const as_val *val, as_particle **pp);
as_val *list_to_asval(const as_particle *p);
uint32_t list_asval_wire_size(const as_val *val);
uint32_t list_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
uint32_t list_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
void list_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);

// Handle on-device "flat" format.
int32_t list_size_from_flat(const uint8_t *flat, uint32_t flat_size);
int list_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp);
int list_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp);
uint32_t list_flat_size(const as_particle *p);
uint32_t list_to_flat(const as_particle *p, uint8_t *flat);


//==========================================================
// LIST particle interface - vtable.
//

const as_particle_vtable list_vtable = {
		list_destruct,
		list_size,

		list_concat_size_from_wire,
		list_append_from_wire,
		list_prepend_from_wire,
		list_incr_from_wire,
		list_size_from_wire,
		list_from_wire,
		list_compare_from_wire,
		list_wire_size,
		list_to_wire,

		list_size_from_asval,
		list_from_asval,
		list_to_asval,
		list_asval_wire_size,
		list_asval_to_wire,

		list_size_from_msgpack,
		list_from_msgpack,

		list_size_from_flat,
		list_cast_from_flat,
		list_from_flat,
		list_flat_size,
		list_to_flat
};


//==========================================================
// Typedefs & constants.
//

#define AS_PACKED_LIST_INVALID	1
#define AS_PACKED_LIST_FAILED	2

#define AS_PACKED_LIST_INDEX_STEP	128

typedef struct as_packed_list_index_s {
	uint32_t count;
	uint32_t cap;
	uint32_t indexes[];
} as_packed_list_index;

typedef struct as_packed_list_s {
	as_unpacker upk;

	int ele_count;
	int new_ele_count;

	uint32_t header_size;
	uint32_t seg1_size;
	uint32_t seg2_index;
	uint32_t seg2_size;
	uint32_t nil_ele_size;	// number of nils we need to insert
} as_packed_list;

#define CDT_FLAG_PACKED_NEED_FREE	1

typedef struct list_wrapper_s {
	uint8_t			type;
	uint32_t		packed_sz;

	uint8_t			magic;
	uint8_t			flags;

	uint8_t 		*packed;

	// Mutable state members.
	// Is considered mutable in const objects.
	as_packed_list_index index;
} __attribute__ ((__packed__)) list_wrapper;

typedef struct list_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) list_mem;

typedef struct list_flat_s {
	uint8_t		type;
	uint32_t	size; // host order on device and in memory
	uint8_t		data[];
} __attribute__ ((__packed__)) list_flat;

static const uint8_t msgpack_empty_list[1] = {0x90};
static const list_wrapper list_wrapper_empty = {
	.type = AS_PARTICLE_TYPE_LIST,
	.magic = CDT_MAGIC,
	.flags = 0,
	.packed_sz = 1,
	.packed = (uint8_t *)msgpack_empty_list,
	.index = {
			.count = 0,
			.cap = 0
	}
};


//==========================================================
// Forward declarations.
//

static inline bool is_list_type(uint8_t type);

// as_bin
static inline void as_bin_set_empty_packed_list(as_bin *b, rollback_alloc *alloc_buf);
static inline as_packed_list_index *as_bin_get_packed_list_index(const as_bin *b);
static inline void as_bin_create_temp_packed_list_if_notinuse(as_bin *b);
static inline bool as_bin_is_temp_packed_list(const as_bin *b);

// as_packed_list
static int32_t as_packed_list_get_new_element_count(as_packed_list *pl);
static int32_t as_packed_list_header_element_count(as_packed_list *pl);
static void as_packed_list_init(as_packed_list *pl, const uint8_t *buf, uint32_t size);
static void as_packed_list_init_from_bin(as_packed_list *pl, const as_bin *b);
static int64_t as_packed_list_insert(as_packed_list *pl, uint32_t index, uint32_t count, uint32_t insert_size, as_packed_list_index *pli);
static int32_t as_packed_list_remove(as_packed_list *pl, uint32_t index, uint32_t count, as_packed_list_index *pli);
static int32_t as_packed_list_write_hdrseg1(as_packed_list *pl, uint8_t *buf);
static int32_t as_packed_list_write_header(uint8_t *buf, uint32_t ele_count);
static int32_t as_packed_list_write_header_new(as_packed_list *pl, uint8_t *buf);
static int32_t as_packed_list_write_header_old(as_packed_list *pl, uint8_t *buf);
static uint32_t as_packed_list_write_seg1(as_packed_list *pl, uint8_t *buf);
static uint32_t as_packed_list_write_seg2(as_packed_list *pl, uint8_t *buf);

// as_packed_list_index
static void as_packed_list_index_cpy(as_packed_list_index *dst, as_packed_list_index *src);
static void as_packed_list_index_init(as_packed_list_index *pli, uint32_t ele_max);
static void as_packed_list_index_truncate(as_packed_list_index *pli, uint32_t index);
static const uint8_t *as_unpack_list_elements_find_index(as_unpacker *pk, uint32_t index, as_packed_list_index *pli);

// list_wrapper
static inline bool list_is_wrapped(const as_particle *p);
static inline list_wrapper *list_wrapper_create(rollback_alloc *alloc_buf, uint32_t ele_count, as_packed_list_index *pli_old, uint32_t packed_size);
static void list_wrapper_destroy(list_wrapper *p_list_wrapped);
static int32_t list_wrapper_from_buf(list_wrapper *p_list_wrapped, const uint8_t *buf, uint32_t size);
static int32_t list_wrapper_from_buf_size(const uint8_t *buf, uint32_t size);
static void list_wrapper_init(list_wrapper *p_list_wrapped, uint32_t ele_count);

// packed_list create
static as_particle *packed_list_create(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t size, bool wrapped);

// packed_list ops
static int packed_list_append(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, bool payload_is_container, as_bin *result);
static int packed_list_insert(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, bool payload_is_container, int64_t index, as_bin *result);
static int packed_list_remove(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, as_bin *result, bool result_is_count, bool result_is_list, rollback_alloc *alloc_result);
static int packed_list_set(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, int64_t index);
static int packed_list_trim(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, as_bin *result);
static uint8_t *packed_list_setup_bin(as_bin *b, rollback_alloc *alloc_buf, uint32_t new_size, uint32_t new_ele_count, uint32_t index, as_packed_list_index *pli);

// Debugging support
static void print_cdt_list_particle(const as_particle *p);
void print_cdt_list_bin(const as_bin *b);
void print_as_packed_list(const as_packed_list *pl);


//==========================================================
// LIST particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
list_destruct(as_particle *p)
{
	if (list_is_wrapped(p)) {
		list_wrapper_destroy((list_wrapper *)p);
	}
	else {
		cf_free(p);
	}
}

uint32_t
list_size(const as_particle *p)
{
	if (list_is_wrapped(p)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)p;
		return sizeof(list_wrapper) + p_list_wrapped->packed_sz + (sizeof(uint32_t) * p_list_wrapped->index.cap);
	}

	const list_mem *p_list_mem = (const list_mem *)p;
	return (uint32_t)sizeof(list_mem) + p_list_mem->sz;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
list_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
list_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
list_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
list_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "increment of list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
list_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// TODO - CDT can't determine in memory or not.
	return list_wrapper_from_buf_size(wire_value, value_size);
}

int
list_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// TODO - CDT can't determine in memory or not.
	// It works for data-not-in-memory but we'll incur a memcpy that could be eliminated.
	return list_wrapper_from_buf((list_wrapper *)*pp, wire_value, value_size);
}

int
list_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
	cf_warning(AS_PARTICLE, "list_compare_from_wire() not implemented");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

uint32_t
list_wire_size(const as_particle *p)
{
	if (list_is_wrapped(p)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)p;
		return p_list_wrapped->packed_sz;
	}

	const list_mem *p_list_mem = (const list_mem *)p;
	return p_list_mem->sz;
}

uint32_t
list_to_wire(const as_particle *p, uint8_t *wire)
{
	if (list_is_wrapped(p)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)p;
		memcpy(wire, p_list_wrapped->packed, p_list_wrapped->packed_sz);

		return p_list_wrapped->packed_sz;
	}

	const list_mem *p_list_mem = (const list_mem *)p;
	memcpy(wire, p_list_mem->data, p_list_mem->sz);

	return p_list_mem->sz;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
list_size_from_asval(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_getsize(&s, (as_val *)val);
	uint32_t index_count = as_list_size((as_list *)val) / AS_PACKED_LIST_INDEX_STEP;

	as_serializer_destroy(&s);

	return sizeof(list_wrapper) + (sizeof(uint32_t) * index_count) + size;
}

void
list_from_asval(const as_val *val, as_particle **pp)
{
	list_wrapper *p_list_wrapped = (list_wrapper *)*pp;

	list_wrapper_init(p_list_wrapped, as_list_size((as_list *)val));

	p_list_wrapped->packed = (uint8_t *)p_list_wrapped + sizeof(list_wrapper) + (sizeof(uint32_t) * p_list_wrapped->index.cap);

	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_presized(&s, val, p_list_wrapped->packed);

	p_list_wrapped->packed_sz = size;

	as_serializer_destroy(&s);
}

as_val *
list_to_asval(const as_particle *p)
{
	as_buffer buf;
	as_buffer_init(&buf);

	if (list_is_wrapped(p)) {
		list_wrapper *p_list_wrapped = (list_wrapper *)p;

		buf.data = p_list_wrapped->packed;
		buf.capacity = p_list_wrapped->packed_sz;
		buf.size = p_list_wrapped->packed_sz;
	}
	else {
		list_mem *p_list_mem = (list_mem *)p;

		buf.data = p_list_mem->data;
		buf.capacity = p_list_mem->sz;
		buf.size = p_list_mem->sz;
	}

	as_serializer s;
	as_msgpack_init(&s);

	as_val *val = NULL;

	as_serializer_deserialize(&s, &buf, &val);
	as_serializer_destroy(&s);

	return val;
}

uint32_t
list_asval_wire_size(const as_val *val)
{
	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_getsize(&s, (as_val *)val);

	as_serializer_destroy(&s);

	return size;
}

uint32_t
list_asval_to_wire(const as_val *val, uint8_t *wire)
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
list_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	return (uint32_t)sizeof(list_mem) + packed_size;
}

void
list_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	list_mem *p_list_mem = (list_mem *)*pp;

	p_list_mem->type = AS_PARTICLE_TYPE_LIST;
	p_list_mem->sz = packed_size;
	memcpy(p_list_mem->data, packed, p_list_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

// This is never used currently.
int32_t
list_size_from_flat(const uint8_t *flat, uint32_t flat_size)
{
	const list_flat *p_list_flat = (const list_flat *)flat;

	return list_wrapper_from_buf_size(p_list_flat->data, p_list_flat->size);
}

int
list_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Cast temp buffer from disk to data-not-in-memory.
	list_flat *p_list_flat = (list_flat *)flat;

	// This assumes list_flat is the same as list_mem.
	*pp = (as_particle *)p_list_flat;

	return 0;
}

int
list_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Convert temp buffer from disk to data-in-memory.
	const list_flat *p_list_flat = (const list_flat *)flat;

	int32_t new_size = list_wrapper_from_buf_size(p_list_flat->data, p_list_flat->size);

	if (new_size < 0) {
		return (int)new_size;
	}

	list_wrapper *p_list_wrapped = cf_malloc_ns(new_size);

	if (! p_list_wrapped) {
		cf_warning(AS_PARTICLE, "failed malloc for list wrapper (%d)", new_size);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	list_wrapper_from_buf(p_list_wrapped, p_list_flat->data, p_list_flat->size);

	*pp = (as_particle *)p_list_wrapped;

	return 0;
}

uint32_t
list_flat_size(const as_particle *p)
{
	if (list_is_wrapped(p)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)p;
		return sizeof(list_flat) + p_list_wrapped->packed_sz;
	}

	const list_mem *p_list_mem = (const list_mem *)p;
	return sizeof(list_flat) + p_list_mem->sz;
}

uint32_t
list_to_flat(const as_particle *p, uint8_t *flat)
{
	list_flat *p_list_flat = (list_flat *)flat;

	if (list_is_wrapped(p)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)p;

		p_list_flat->size = p_list_wrapped->packed_sz;
		memcpy(p_list_flat->data, p_list_wrapped->packed, p_list_wrapped->packed_sz);
	}
	else {
		const list_mem *p_list_mem = (const list_mem *)p;

		p_list_flat->size = p_list_mem->sz;
		memcpy(p_list_flat->data, p_list_mem->data, p_list_mem->sz);
	}

	// Already wrote the type.

	return sizeof(list_flat) + p_list_flat->size;
}


//==========================================================
// as_bin particle functions specific to LIST.
//

void
as_bin_particle_list_set_hidden(as_bin *b)
{
	// Caller must ensure this is called only for LIST particles.
	list_wrapper *p_list_wrapped = (list_wrapper *)b->particle;

	p_list_wrapped->type = AS_PARTICLE_TYPE_HIDDEN_LIST;

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_HIDDEN_LIST);
}

void
as_bin_particle_list_get_packed_val(const as_bin *b, cdt_payload *packed)
{
	if (list_is_wrapped(b->particle)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)b->particle;

		packed->ptr = p_list_wrapped->packed;
		packed->size = p_list_wrapped->packed_sz;

		return;
	}

	const list_mem *p_list_mem = (const list_mem *)b->particle;

	packed->ptr = (uint8_t *)p_list_mem->data;
	packed->size = p_list_mem->sz;
}


//==========================================================
// Local helpers.
//

static inline bool
is_list_type(uint8_t type)
{
	return type == AS_PARTICLE_TYPE_LIST;
}

//------------------------------------------------
// as_bin
//

static inline void
as_bin_set_empty_packed_list(as_bin *b, rollback_alloc *alloc_buf)
{
#if defined(CDT_LIST_DISALLOW_EMPTY)
	as_bin_set_empty(b);
#else
	b->particle = packed_list_simple_create_empty(alloc_buf);
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
#endif
}

static inline as_packed_list_index *
as_bin_get_packed_list_index(const as_bin *b)
{
	if (list_is_wrapped(b->particle)) {
		list_wrapper *p_list_wrapped = (list_wrapper *)b->particle;
		return &p_list_wrapped->index;
	}

	return NULL;
}

static inline void
as_bin_create_temp_packed_list_if_notinuse(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		b->particle = (as_particle *)&list_wrapper_empty;
		as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
	}
}

static inline bool
as_bin_is_temp_packed_list(const as_bin *b)
{
	return b->particle == (const as_particle *)&list_wrapper_empty;
}

//----------------------------------------------------------
// as_packed_list
//

static int32_t
as_packed_list_get_new_element_count(as_packed_list *pl)
{
	return pl->new_ele_count;
}

// Return negative int on failure, number of elements in list.
static int32_t
as_packed_list_header_element_count(as_packed_list *pl)
{
	if (pl->ele_count >= 0) {
		return pl->ele_count;
	}

	if (pl->ele_count == -AS_PACKED_LIST_FAILED) {
		return -AS_PACKED_LIST_FAILED;
	}

	if ((pl->ele_count = as_unpack_list_header_element_count(&pl->upk)) < 0) {
		pl->ele_count = -AS_PACKED_LIST_FAILED;
	}

	return pl->ele_count;
}

static void
as_packed_list_init(as_packed_list *pl, const uint8_t *buf, uint32_t size)
{
	pl->upk.buffer = buf;
	pl->upk.length = size;
	pl->upk.offset = 0;

	pl->ele_count = -AS_PACKED_LIST_INVALID;
	pl->new_ele_count = -AS_PACKED_LIST_INVALID;

	pl->header_size = 0;
	pl->seg1_size = 0;
	pl->seg2_index = 0;
	pl->seg2_size = 0;
	pl->nil_ele_size = 0;
}

static void
as_packed_list_init_from_bin(as_packed_list *pl, const as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	if (! is_list_type(type)) {
		cf_crash(AS_PARTICLE, "as_packed_list_init_from_bin() invalid type %d", type);
	}

	if (list_is_wrapped(b->particle)) {
		const list_wrapper *p_list_wrapped = (const list_wrapper *)b->particle;
		as_packed_list_init(pl, p_list_wrapped->packed, p_list_wrapped->packed_sz);
	}
	else {
		const list_mem *p_list_mem = (const list_mem *)b->particle;
		as_packed_list_init(pl, p_list_mem->data, p_list_mem->sz);
	}
}

// Calculate a packed list split via insert op.
// Return negative int on failure, new size of packed buffer.
static int64_t
as_packed_list_insert(as_packed_list *pl, uint32_t index, uint32_t count, uint32_t insert_size, as_packed_list_index *pli)
{
	int32_t ele_count = as_packed_list_header_element_count(pl);

	if (ele_count < 0) {
		return -1;
	}

	if (pl->new_ele_count < 0) {
		pl->header_size = (uint32_t)pl->upk.offset;

		if (index >= ele_count) {
			if (index + count >= INT32_MAX) {
				cf_warning(AS_PARTICLE, "as_packed_list_insert() index %u + count %u overflow", index, count);
				return -2;
			}

			pl->new_ele_count = (int)(index + count);
			pl->nil_ele_size = index - (uint32_t)ele_count;

			pl->seg1_size = (uint32_t)pl->upk.length;
			pl->seg2_size = 0;
		}
		else {
			pl->new_ele_count = ele_count + (int)count;
			pl->nil_ele_size = 0;

			if (! as_unpack_list_elements_find_index(&pl->upk, index, pli)) {
				return -3;
			}

			pl->seg1_size = pl->upk.offset;
			pl->seg2_index = pl->seg1_size;
			pl->seg2_size = pl->upk.length - pl->seg1_size;
		}

		// seg1_size does not include header.
		pl->seg1_size -= pl->header_size;
	}

	return (int64_t)as_pack_list_header_get_size(pl->new_ele_count)
			+ pl->seg1_size
			+ pl->nil_ele_size
			+ insert_size
			+ pl->seg2_size;
}

// Calculate a packed list split via remove op.
// Return -1 on failure, new size of packed buffer.
static int32_t
as_packed_list_remove(as_packed_list *pl, uint32_t index, uint32_t count, as_packed_list_index *pli)
{
	int32_t ele_count = as_packed_list_header_element_count(pl);

	if (ele_count < 0) {
		return -1;
	}

	if (pl->new_ele_count < 0) {
		pl->header_size = (uint32_t)pl->upk.offset;

		// Nothing to remove.
		if (index >= (uint32_t)ele_count) {
			pl->seg1_size = (uint32_t)pl->upk.length - pl->header_size;
			pl->seg2_size = 0;
			pl->new_ele_count = ele_count;

			return pl->upk.length;
		}

		if (count >= (uint32_t)ele_count - index) {
			pl->new_ele_count = (int)index;

			if (! as_unpack_list_elements_find_index(&pl->upk, index, pli)) {
				return -2;
			}

			pl->seg1_size = (uint32_t)pl->upk.offset;
			pl->seg2_index = 0;
			pl->seg2_size = 0;
		}
		else {
			pl->new_ele_count = ele_count - (int)count;

			if (! as_unpack_list_elements_find_index(&pl->upk, index, pli)) {
				return -3;
			}

			pl->seg1_size = (uint32_t)pl->upk.offset;

			for (uint32_t i = 0; i < count; i++) {
				if (as_unpack_size(&pl->upk) < 0) {
					return -4;
				}
			}

			pl->seg2_index = (uint32_t)pl->upk.offset;
			pl->seg2_size = (uint32_t)(pl->upk.length - pl->upk.offset);
		}

		// seg1_size does not include header.
		pl->seg1_size -= pl->header_size;
	}

	return (int32_t)(as_pack_list_header_get_size(pl->new_ele_count)
			+ pl->seg1_size
			+ pl->seg2_size);
}

// Write header and segment 1 and trailing nils if any.
// Return -1 on failure, number of bytes written.
static int32_t
as_packed_list_write_hdrseg1(as_packed_list *pl, uint8_t *buf)
{
	int32_t header_size = as_packed_list_write_header_new(pl, buf);

	if (header_size < 0) {
		return -1;
	}

	buf += header_size;

	return header_size + (int32_t)as_packed_list_write_seg1(pl, buf);
}

// Write header with ele_count elements.
// Return -1 on failure, number of bytes written.
static int32_t
as_packed_list_write_header(uint8_t *buf, uint32_t ele_count)
{
	// Just a wrapping of as_pack_list_header().
	as_packer pk = {
			.head = NULL,
			.tail = NULL,
			.buffer = buf,
			.offset = 0,
			.capacity = INT_MAX,
	};

	if (as_pack_list_header(&pk, ele_count) != 0) {
		return -1;
	}

	return pk.offset;
}

// Write new header.
// Return -1 on failure, number of bytes written.
static int32_t
as_packed_list_write_header_new(as_packed_list *pl, uint8_t *buf)
{
	if (pl->new_ele_count < 0) {
		return -1;
	}

	return as_packed_list_write_header(buf, pl->new_ele_count);
}

// Write original header.
// Return -1 on failure, number of bytes written.
static int32_t
as_packed_list_write_header_old(as_packed_list *pl, uint8_t *buf)
{
	int32_t ele_count = as_packed_list_header_element_count(pl);

	if (ele_count < 0) {
		return -1;
	}

	return as_packed_list_write_header(buf, ele_count);
}

// Write segment 1 and trailing nils if any.
// Assumes pl->new_ele_count is valid.
// Return number of bytes written.
static uint32_t
as_packed_list_write_seg1(as_packed_list *pl, uint8_t *buf)
{
	memcpy(buf, pl->upk.buffer + pl->header_size, pl->seg1_size);

	if (pl->nil_ele_size == 0) {
		return pl->seg1_size;
	}

	buf += pl->seg1_size;
	// 0xC0 is the encoding for an AS_NIL.
	memset(buf, 0xC0, pl->nil_ele_size);

	return pl->seg1_size + pl->nil_ele_size;
}

// Write segment 2 if any.
// Assumes pl->new_ele_count is valid.
// Return -1 on failure, number of bytes written.
static uint32_t
as_packed_list_write_seg2(as_packed_list *pl, uint8_t *buf)
{
	if (pl->seg2_size == 0) {
		return 0;
	}

	memcpy(buf, pl->upk.buffer + pl->seg2_index, pl->seg2_size);

	return pl->seg2_size;
}

//----------------------------------------------------------
// as_packed_list_index
//

static void
as_packed_list_index_cpy(as_packed_list_index *dst, as_packed_list_index *src)
{
	uint32_t ncpy = dst->cap;

	if (ncpy > src->count) {
		ncpy = src->count;
	}

	memcpy(dst->indexes, src->indexes, sizeof(uint32_t) * ncpy);
	dst->count = ncpy;
}

static void
as_packed_list_index_init(as_packed_list_index *pli, uint32_t ele_max)
{
	pli->cap = ele_max / AS_PACKED_LIST_INDEX_STEP;
	pli->count = 0;
}

// Throw out all index info above index.
static void
as_packed_list_index_truncate(as_packed_list_index *pli, uint32_t index)
{
	uint32_t new_count = index / AS_PACKED_LIST_INDEX_STEP;

	if (pli->count > new_count) {
		pli->count = new_count;
	}
}

// Assumes element count has already been extracted.
// Offset must be at start of list data when this is called.
// Return ptr to element at index.
static const uint8_t *
as_unpack_list_elements_find_index(as_unpacker *pk, uint32_t index, as_packed_list_index *pli)
{
	if (pli) {
		uint32_t start_offset = pk->offset;

		if (index >= AS_PACKED_LIST_INDEX_STEP) {
			if (index < pli->count * AS_PACKED_LIST_INDEX_STEP) {
				uint32_t pli_index = index / AS_PACKED_LIST_INDEX_STEP;

				index -= pli_index * AS_PACKED_LIST_INDEX_STEP;
				pk->offset += pli->indexes[pli_index - 1];
			}
			else if (pli->count > 0) {
				index -= pli->count * AS_PACKED_LIST_INDEX_STEP;
				pk->offset += pli->indexes[pli->count - 1];
			}
		}

		uint32_t count_down = AS_PACKED_LIST_INDEX_STEP;

		for (uint32_t i = 0; i < index; i++) {
			if (as_unpack_size(pk) < 0) {
				return NULL;
			}

			if (--count_down == 0) {
				if (pli->count < pli->cap) {
					pli->indexes[pli->count] = pk->offset - start_offset;
					pli->count++;
				}

				count_down = AS_PACKED_LIST_INDEX_STEP;
			}
		}
	}
	else {
		for (uint32_t i = 0; i < index; i++) {
			if (as_unpack_size(pk) < 0) {
				return NULL;
			}
		}
	}

	return pk->buffer + pk->offset;
}

//----------------------------------------------------------
// list_wrapper
//

static inline bool
list_is_wrapped(const as_particle *p)
{
	const list_wrapper *p_list_wrapped = (const list_wrapper *)p;

	return p_list_wrapped->magic == CDT_MAGIC;
}

// alloc_buf -	allocation method
// ele_count -	element count of new list
// pli_old -	old index to copy over
static inline list_wrapper *
list_wrapper_create(rollback_alloc *alloc_buf, uint32_t ele_count, as_packed_list_index *pli_old, uint32_t packed_size)
{
	size_t wrap_size = sizeof(list_wrapper) + (sizeof(uint32_t) * (ele_count / AS_PACKED_LIST_INDEX_STEP));
	list_wrapper *p_list_wrapped = (list_wrapper *)rollback_alloc_reserve(alloc_buf, wrap_size + packed_size);

	if (! p_list_wrapped) {
		return NULL;
	}

	list_wrapper_init(p_list_wrapped, ele_count);

	if (pli_old) {
		as_packed_list_index_cpy(&p_list_wrapped->index, pli_old);
	}

	if (packed_size > 0) {
		p_list_wrapped->packed = (uint8_t *)p_list_wrapped;
		p_list_wrapped->packed += wrap_size;
		p_list_wrapped->packed_sz = packed_size;
	}

	return p_list_wrapped;
}

static void
list_wrapper_destroy(list_wrapper *p_list_wrapped)
{
	if ((p_list_wrapped->flags & CDT_FLAG_PACKED_NEED_FREE) != 0) {
		cf_free(p_list_wrapped->packed);
	}

	cf_free(p_list_wrapped);
}

static int32_t
list_wrapper_from_buf(list_wrapper *p_list_wrapped, const uint8_t *buf, uint32_t size)
{
	int ele_count = as_unpack_buf_list_element_count(buf, size);

	if (ele_count < 0) {
		cf_warning(AS_PARTICLE, "invalid list parameter");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	list_wrapper_init(p_list_wrapped, ele_count);

	uint8_t *new_buf = (uint8_t *)p_list_wrapped + sizeof(list_wrapper) + (sizeof(uint32_t) * p_list_wrapped->index.cap);

	memcpy(new_buf, buf, size);

	p_list_wrapped->packed = new_buf;
	p_list_wrapped->packed_sz = size;

	return 0;
}

static int32_t
list_wrapper_from_buf_size(const uint8_t *buf, uint32_t size)
{
	int index_count = as_unpack_buf_list_element_count(buf, size);

	if (index_count < 0) {
		cf_warning(AS_PARTICLE, "invalid list parameter");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	index_count /= AS_PACKED_LIST_INDEX_STEP;

	return (int32_t)(sizeof(list_wrapper) + (sizeof(uint32_t) * index_count) + size);
}

static void
list_wrapper_init(list_wrapper *p_list_wrapped, uint32_t ele_count)
{
	p_list_wrapped->type = AS_PARTICLE_TYPE_LIST;
	p_list_wrapped->magic = CDT_MAGIC;
	p_list_wrapped->flags = 0;
	p_list_wrapped->packed_sz = 0;
	p_list_wrapped->packed = NULL;
	as_packed_list_index_init(&p_list_wrapped->index, ele_count);
}

//----------------------------------------------------------
// packed_list create
//

// Create packed or non-indexed wrapped list.
// If alloc_buf is NULL, memory is reserved using cf_malloc.
static as_particle *
packed_list_create(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t size, bool wrapped)
{
	uint32_t result_header_size = as_pack_list_header_get_size(ele_count);
	uint32_t new_size = result_header_size + size;
	uint8_t *data;
	as_particle *particle;

	if (wrapped) {
		list_wrapper *p_list_wrapped = NULL;

		if (! (p_list_wrapped = list_wrapper_create(alloc_buf, 0, NULL, new_size))) {
			return NULL;
		}

		p_list_wrapped->packed_sz = new_size;

		data = p_list_wrapped->packed;
		particle = (as_particle *)p_list_wrapped;
	}
	else {
		new_size += sizeof(list_mem);

		uint8_t *pack_buf = rollback_alloc_reserve(alloc_buf, new_size);

		if (! pack_buf) {
			rollback_alloc_rollback(alloc_buf);
			return NULL;
		}

		list_mem *p_list_mem = (list_mem *)pack_buf;

		p_list_mem->sz = result_header_size + size;
		p_list_mem->type = AS_PARTICLE_TYPE_LIST;

		data = p_list_mem->data;
		particle = (as_particle *)p_list_mem;
	}

	if (as_packed_list_write_header(data, ele_count) < 0) {
		rollback_alloc_rollback(alloc_buf);
		return NULL;
	}

	if (size > 0 && buf) {
		memcpy(data + result_header_size, buf, size);
	}

	return particle;
}

as_particle *
packed_list_simple_create_from_buf(rollback_alloc *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t size)
{
	return packed_list_create(alloc_buf, ele_count, buf, size, false);
}

as_particle *
packed_list_simple_create_empty(rollback_alloc *alloc_buf)
{
	return packed_list_simple_create_from_buf(alloc_buf, 0, NULL, 0);
}

//----------------------------------------------------------
// packed_list ops
//

static int
packed_list_append(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, bool payload_is_container, as_bin *result)
{
	as_packed_list pl;
	as_packed_list_init_from_bin(&pl, b);

	int32_t ele_count = as_packed_list_header_element_count(&pl);

	if (ele_count < 0) {
		cf_warning(AS_PARTICLE, "packed_list_append() invalid packed list");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return packed_list_insert(b, alloc_buf, payload, payload_is_container, (int64_t)ele_count, result);
}

static int
packed_list_insert(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, bool payload_is_container, int64_t index, as_bin *result)
{
	uint32_t count = 1;
	uint32_t payload_hdr_sz = 0;

	if (payload_is_container) {
		int payload_ele_count = as_unpack_buf_list_element_count(payload->ptr, payload->size);

		if (payload_ele_count < 0) {
			cf_warning(AS_PARTICLE, "packed_list_insert() invalid payload, expected a list");
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		if (payload_ele_count == 0) {
			return AS_PROTO_RESULT_OK;
		}

		count = (uint32_t)payload_ele_count;
		payload_hdr_sz = as_pack_list_header_get_size(payload_ele_count);
	}

	as_packed_list pl;
	as_packed_list_init_from_bin(&pl, b);

	int32_t ele_count = as_packed_list_header_element_count(&pl);

	if (ele_count < 0) {
		cf_warning(AS_PARTICLE, "packed_list_insert() invalid list");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (index > INT32_MAX || (index = calc_index(index, ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "packed_list_insert() index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t uindex = (uint32_t)index;

	as_packed_list_index *pli = as_bin_get_packed_list_index(b);
	int64_t new_size = as_packed_list_insert(&pl, uindex, count, payload->size - payload_hdr_sz, pli);

	if (new_size < 0) {
		cf_warning(AS_PARTICLE, "packed_list_insert() as_packed_list_insert failed with ret=%ld, index.cap=%u", new_size, pli->cap);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (new_size > (int64_t)UINT32_MAX) {
		cf_warning(AS_PARTICLE, "packed_list_insert() mem size overflow with new_size=%ld", new_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	int32_t new_ele_count = as_packed_list_get_new_element_count(&pl);
	uint8_t *ptr = packed_list_setup_bin(b, alloc_buf, (uint32_t)new_size, (uint32_t)new_ele_count, uindex, pli);

	if (! ptr) {
		cf_warning(AS_PARTICLE, "packed_list_insert() failed to alloc list particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	int32_t ret = as_packed_list_write_hdrseg1(&pl, ptr);

	if (ret < 0) {
		cf_warning(AS_PARTICLE, "packed_list_insert() write hdr+seg1 failed with ret=%d", ret);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	ptr += ret;

	if (payload_is_container) {
		if (payload_hdr_sz > payload->size) {
			cf_warning(AS_PARTICLE, "packed_list_insert() invalid list header: payload->size=%d", payload->size);
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}

		const uint8_t *p = payload->ptr + payload_hdr_sz;
		uint32_t sz = payload->size - payload_hdr_sz;

		memcpy(ptr, p, sz);
		ptr += sz;
	}
	else {
		memcpy(ptr, payload->ptr, payload->size);
		ptr += payload->size;
	}

	as_packed_list_write_seg2(&pl, ptr);

	if (result) {
		as_bin_set_int(result, pl.new_ele_count);
	}

	return AS_PROTO_RESULT_OK;
}

// count == 0 means missing count.
static int
packed_list_remove(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, as_bin *result, bool result_is_count, bool result_is_list, rollback_alloc *alloc_result)
{
	as_packed_list pl;
	as_packed_list_init_from_bin(&pl, b);

	int32_t ele_count = as_packed_list_header_element_count(&pl);

	if (ele_count < 0) {
		cf_warning(AS_PARTICLE, "packed_list_remove() invalid list header");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (index >= ele_count || (index = calc_index(index, ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "packed_list_remove() index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t uindex = (uint32_t)index;

	count = calc_count((uint64_t)index, count, ele_count);

	as_packed_list_index *pli = as_bin_get_packed_list_index(b);
	int32_t new_size = as_packed_list_remove(&pl, uindex, (uint32_t)count, pli);

	if (new_size < 0) {
		cf_warning(AS_PARTICLE, "packed_list_remove() as_packed_list_remove failed with ret=%d", new_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (as_packed_list_get_new_element_count(&pl) == 0) {
		as_bin_set_empty_packed_list(b, alloc_buf);
	}
	else {
		int32_t new_ele_count = as_packed_list_get_new_element_count(&pl);
		uint8_t *ptr = packed_list_setup_bin(b, alloc_buf, (uint32_t)new_size, (uint32_t)new_ele_count, uindex, pli);

		if (! ptr) {
			cf_warning(AS_PARTICLE, "packed_list_remove() failed to alloc list particle");
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		int32_t ret = as_packed_list_write_hdrseg1(&pl, ptr);

		if (ret < 0) {
			cf_crash(AS_PARTICLE, "packed_list_remove() write hdr+seg1 failed with ret=%d", ret);
		}

		ptr += ret;

		as_packed_list_write_seg2(&pl, ptr);
	}

	if (result) {
		uint32_t result_count = (uint32_t)(pl.ele_count - as_packed_list_get_new_element_count(&pl));

		if (result_is_count) {
			as_bin_set_int(result, result_count);
		}
		else {
			uint32_t result_start = pl.header_size + pl.seg1_size;
			const uint8_t *result_ptr = pl.upk.buffer + result_start;
			uint32_t result_end = (pl.seg2_size > 0) ? pl.seg2_index : (uint32_t)pl.upk.length;
			uint32_t result_size = result_end - result_start;

			if (result_is_list) {
				result->particle = packed_list_simple_create_from_buf(alloc_result, result_count, result_ptr, result_size);

				if (! result->particle) {
					return -AS_PROTO_RESULT_FAIL_UNKNOWN;
				}

				as_bin_state_set_from_type(result, AS_PARTICLE_TYPE_LIST);
			}
			else if (result_size > 0) {
				if (count > 1) {
					cf_crash(AS_PARTICLE, "packed_list_remove() result must be list for count > 1");
				}

				as_bin_particle_alloc_from_msgpack(result, result_ptr, result_size);
			}
			// else - leave result bin empty because result_size is 0.
		}
	}

	return AS_PROTO_RESULT_OK;
}

static int
packed_list_set(as_bin *b, rollback_alloc *alloc_buf, const cdt_payload *payload, int64_t index)
{
	as_packed_list pl;
	as_packed_list_init_from_bin(&pl, b);

	int32_t ele_count = as_packed_list_header_element_count(&pl);

	if (ele_count < 0) {
		cf_warning(AS_PARTICLE, "packed_list_set() invalid list");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (index >= ele_count) {
		return packed_list_insert(b, alloc_buf, payload, false, index, NULL);
	}

	if (index > UINT32_MAX || (index = calc_index(index, ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "packed_list_set() index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t uindex = (uint32_t)index;

	as_packed_list_index *pli = as_bin_get_packed_list_index(b);
	int32_t new_size = as_packed_list_remove(&pl, uindex, 1, pli);

	if (new_size < 0) {
		cf_warning(AS_PARTICLE, "packed_list_set() as_packed_list_remove failed with ret=%d", new_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	int32_t new_ele_count = as_packed_list_get_new_element_count(&pl);

	// Add difference in header size and payload size.
	new_size += as_pack_list_header_get_size(ele_count) - as_pack_list_header_get_size(new_ele_count);
	new_size += payload->size;

	uint8_t *ptr = packed_list_setup_bin(b, alloc_buf, (uint32_t)new_size, (uint32_t)ele_count, uindex, pli);

	if (! ptr) {
		cf_warning(AS_PARTICLE, "packed_list_set() failed to alloc list particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	int32_t ret = as_packed_list_write_header_old(&pl, ptr);

	if (ret < 0) {
		cf_warning(AS_PARTICLE, "packed_list_set() write header failed with ret=%d", ret);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	ptr += ret;
	ptr += as_packed_list_write_seg1(&pl, ptr);

	memcpy(ptr, payload->ptr, payload->size);
	ptr += payload->size;

	as_packed_list_write_seg2(&pl, ptr);

	return AS_PROTO_RESULT_OK;
}

static int
packed_list_trim(as_bin *b, rollback_alloc *alloc_buf, int64_t index, uint64_t count, as_bin *result)
{
	// Remove head section.
	as_packed_list pl;
	as_packed_list_init_from_bin(&pl, b);

	int32_t original_ele_count = as_packed_list_header_element_count(&pl);

	if (original_ele_count < 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() invalid list");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (count == 0) {
		// Remove everything.
		as_bin_set_int(result, original_ele_count);
		as_bin_set_empty_packed_list(b, alloc_buf);

		return AS_PROTO_RESULT_OK;
	}

	if (index >= original_ele_count || (index = calc_index(index, original_ele_count)) < 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() index %ld out of bounds for ele_count %d", index > 0 ? index : index - original_ele_count, original_ele_count);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t uindex = (uint32_t)index;

	count = calc_count((uint64_t)index, count, original_ele_count);

	as_packed_list_index *pli = as_bin_get_packed_list_index(b);
	int32_t new_size = as_packed_list_remove(&pl, 0, uindex, pli);

	if (new_size < 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() as_packed_list_remove failed with ret=%d", new_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (as_packed_list_get_new_element_count(&pl) == 0) {
		as_bin_set_int(result, original_ele_count);
		as_bin_set_empty_packed_list(b, alloc_buf);

		return AS_PROTO_RESULT_OK;
	}

	uint8_t temp_buf[new_size];
	uint8_t *ptr = temp_buf;

	int32_t ret = as_packed_list_write_hdrseg1(&pl, ptr);

	if (ret < 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() write hdr+seg1 failed with ret=%d", ret);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	ptr += ret;

	as_packed_list_write_seg2(&pl, ptr);

	// Remove tail section.
	as_packed_list_init(&pl, temp_buf, (uint32_t)new_size);

	int32_t ele_count = as_packed_list_header_element_count(&pl);
	new_size = as_packed_list_remove(&pl, (uint32_t)count, (uint32_t)ele_count - (uint32_t)count, NULL);

	if (new_size < 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() invalid list");
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	int32_t new_ele_count = as_packed_list_get_new_element_count(&pl);

	if (new_ele_count == 0) {
		as_bin_set_int(result, original_ele_count);
		as_bin_set_empty_packed_list(b, alloc_buf);

		return AS_PROTO_RESULT_OK;
	}

	ptr = packed_list_setup_bin(b, alloc_buf, (uint32_t)new_size, (uint32_t)new_ele_count, uindex, pli);

	if (! ptr) {
		cf_warning(AS_PARTICLE, "packed_list_trim() failed to alloc list particle");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	ret = as_packed_list_write_hdrseg1(&pl, ptr);

	if (ret < 0) {
		cf_warning(AS_PARTICLE, "packed_list_trim() write hdr+seg1 failed with ret=%d", ret);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	ptr += ret;

	as_packed_list_write_seg2(&pl, ptr);

	as_bin_set_int(result, original_ele_count - pl.new_ele_count);

	return AS_PROTO_RESULT_OK;
}

static uint8_t *
packed_list_setup_bin(as_bin *b, rollback_alloc *alloc_buf, uint32_t new_size, uint32_t new_ele_count, uint32_t index, as_packed_list_index *pli)
{
	list_wrapper *p_list_wrapped = list_wrapper_create(alloc_buf, new_ele_count, pli, new_size);

	if (! p_list_wrapped) {
		return NULL;
	}

	as_packed_list_index_truncate(&p_list_wrapped->index, index);
	b->particle = (as_particle *)p_list_wrapped;

	return p_list_wrapped->packed;
}


//==========================================================
// cdt_list_builder
//

bool
cdt_list_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t max_size)
{
	uint32_t new_size = sizeof(list_mem) + sizeof(uint64_t) + 1 + max_size;
	list_mem *p_list_mem = (list_mem *)rollback_alloc_reserve(alloc_buf, new_size);

	if (! p_list_mem) {
		return false;
	}

	p_list_mem->type = AS_PARTICLE_TYPE_LIST;
	p_list_mem->sz = as_packed_list_write_header(p_list_mem->data, ele_count);

	builder->particle = (as_particle *)p_list_mem;
	builder->write_ptr = p_list_mem->data + p_list_mem->sz;
	builder->ele_count = 0;
	builder->header_ele_count = ele_count;
	builder->size = &p_list_mem->sz;

	return true;
}

void
cdt_list_builder_finalize(cdt_container_builder *builder)
{
	if (builder->ele_count == builder->header_ele_count) {
		return;
	}

	uint32_t hdr_size = as_pack_list_header_get_size(builder->ele_count);
	list_mem *p_list_mem = (list_mem *)builder->particle;
	uint32_t current_hdr_size = as_pack_list_header_get_size(builder->header_ele_count);

	if (hdr_size != current_hdr_size) {
		int64_t delta = (int64_t)hdr_size - current_hdr_size;
		size_t size = 0;

		memmove(p_list_mem->data + delta + current_hdr_size, p_list_mem->data + current_hdr_size, size);
		p_list_mem->sz += delta;
	}

	as_packed_list_write_header(p_list_mem->data, builder->ele_count);
}


//==========================================================
// cdt_process_state_packed_list
//

bool
cdt_process_state_packed_list_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata)
{
	as_bin *b = cdt_udata->b;
	as_bin *result = cdt_udata->result;
	as_cdt_optype optype = state->type;

	if (! is_list_type(as_bin_get_particle_type(b)) && as_bin_inuse(b)) {
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() invalid type %d", as_bin_get_particle_type(b));
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		return false;
	}

	rollback_alloc_inita(alloc_buf, cdt_udata->alloc_buf, 5, true);
	// Results always on the heap.
	rollback_alloc_inita(alloc_result, NULL, 1, false);

	switch (optype) {
	// Add to list.
	case AS_CDT_OP_LIST_APPEND: {
		cdt_payload payload;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &payload)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_list_if_notinuse(b);

		int ret = packed_list_append(b, alloc_buf, &payload, false, result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() APPEND failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_LIST_APPEND_ITEMS: {
		cdt_payload payload;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &payload)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_list_if_notinuse(b);

		int ret = packed_list_append(b, alloc_buf, &payload, true, result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() APPEND_ITEMS failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		if (as_bin_is_temp_packed_list(b)) {
			b->particle = packed_list_simple_create_empty(alloc_buf);
			as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
		}

		break;
	}
	case AS_CDT_OP_LIST_INSERT: {
		int64_t index;
		cdt_payload payload;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &payload)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_list_if_notinuse(b);

		int ret = packed_list_insert(b, alloc_buf, &payload, false, index, result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() INSERT failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_LIST_INSERT_ITEMS: {
		const cdt_payload payload;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &payload)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_list_if_notinuse(b);

		int ret = packed_list_insert(b, alloc_buf, &payload, true, index, result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() INSERT_ITEMS failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		if (as_bin_is_temp_packed_list(b)) {
			b->particle = packed_list_simple_create_empty(alloc_buf);
			as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_LIST);
		}

		break;
	}
	case AS_CDT_OP_LIST_SET: {
		cdt_payload payload;
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &payload)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_bin_create_temp_packed_list_if_notinuse(b);

		int ret = packed_list_set(b, alloc_buf, &payload, index);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() SET failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}

	// OP by Index
	case AS_CDT_OP_LIST_REMOVE:
	case AS_CDT_OP_LIST_POP: {
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (! is_list_type(as_bin_get_particle_type(b))) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		int ret = packed_list_remove(b, alloc_buf, index, 1, result, optype == AS_CDT_OP_LIST_REMOVE, false, alloc_result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() POP/REMOVE failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_LIST_REMOVE_RANGE:
	case AS_CDT_OP_LIST_POP_RANGE: {
		int64_t index;
		uint64_t count;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (! is_list_type(as_bin_get_particle_type(b))) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		// User specifically asked for 0 count.
		if (state->ele_count == 2 && count == 0) {
			if (optype == AS_CDT_OP_LIST_POP_RANGE) {
				result->particle = packed_list_simple_create_empty(alloc_result);
				as_bin_state_set_from_type(result, AS_PARTICLE_TYPE_LIST);
			}
			else {
				as_bin_set_int(result, 0);
			}

			break;
		}

		int ret = packed_list_remove(b,
				alloc_buf,
				index,
				state->ele_count == 1 ? 0 : count,
				result,
				optype == AS_CDT_OP_LIST_REMOVE_RANGE,	// result_is_count
				optype == AS_CDT_OP_LIST_POP_RANGE,		// result_is_list
				alloc_result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() POP_RANGE/REMOVE_RANGE failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_result);
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	case AS_CDT_OP_LIST_INCREMENT_BY:
		// TODO - Support or remove these.
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_UNSUPPORTED_FEATURE;
		return false;

	// Misc
	case AS_CDT_OP_LIST_CLEAR: {
		if (as_bin_get_particle_type(b) != AS_PARTICLE_TYPE_LIST) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		as_bin_set_empty_packed_list(b, alloc_buf);

		break;
	}
	case AS_CDT_OP_LIST_TRIM: {
		int64_t index;
		uint64_t count;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (! is_list_type(as_bin_get_particle_type(b))) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
			return false;
		}

		int ret = packed_list_trim(b, alloc_buf, index, count, result);

		if (ret < 0) {
			cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() TRIM failed");
			cdt_udata->ret_code = ret;
			rollback_alloc_rollback(alloc_buf);
			return false;
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_modify_optype() invalid cdt op: %d", optype);
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
		return false;
	}

	return true;
}

bool
cdt_process_state_packed_list_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata)
{
	const as_bin *b = cdt_udata->b;
	as_bin *result = cdt_udata->result;
	as_cdt_optype optype = state->type;

	if (! is_list_type(as_bin_get_particle_type(b))) {
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		return false;
	}

	// Just one entry needed for results bin.
	rollback_alloc_inita(packed_alloc, NULL, 1, false);

	switch (optype) {
	case AS_CDT_OP_LIST_GET: {
		int64_t index;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_packed_list pl;
		as_packed_list_init_from_bin(&pl, b);

		int32_t ele_count = as_packed_list_header_element_count(&pl);

		if (ele_count < 0) {
			cf_warning(AS_PARTICLE, "OP_LIST_GET: invalid list header");
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (index >= ele_count || (index = calc_index(index, ele_count)) < 0) {
			cf_warning(AS_PARTICLE, "OP_LIST_GET: index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		uint32_t uindex = (uint32_t)index;

		as_packed_list_index *pli = as_bin_get_packed_list_index(b);
		const uint8_t *ele_ptr = as_unpack_list_elements_find_index(&pl.upk, uindex, pli);
		int ele_size = as_unpack_size(&pl.upk);

		if (ele_size < 0) {
			cf_warning(AS_PARTICLE, "OP_LIST_GET: unable to unpack element at %u", uindex);
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_UNKNOWN;
			return false;
		}

		if (ele_size > 0) {
			as_bin_particle_alloc_from_msgpack(result, ele_ptr, (uint32_t)ele_size);
		}
		// else - leave result bin empty because ele_size is 0.

		break;
	}
	case AS_CDT_OP_LIST_GET_RANGE: {
		int64_t index;
		uint64_t count;

		if (! CDT_OP_TABLE_GET_PARAMS(state, &index, &count)) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		as_packed_list pl;
		as_packed_list_init_from_bin(&pl, b);

		int32_t ele_count = as_packed_list_header_element_count(&pl);

		if (ele_count < 0) {
			cf_warning(AS_PARTICLE, "OP_LIST_GET_RANGE: invalid list header");
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		if (index >= ele_count || (index = calc_index(index, ele_count)) < 0) {
			cf_warning(AS_PARTICLE, "OP_LIST_GET_RANGE: index %ld out of bounds for ele_count %d", index > 0 ? index : index - ele_count, ele_count);
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		// User specifically asked for 0 count.
		if (state->ele_count == 2 && count == 0) {
			result->particle = packed_list_simple_create_empty(packed_alloc);
			as_bin_state_set_from_type(result, AS_PARTICLE_TYPE_LIST);

			break;
		}

		// If missing count, take the rest of the list.
		count = calc_count((uint64_t)index, state->ele_count == 1 ? 0 : count, ele_count);

		uint32_t uindex = (uint32_t)index;

		as_packed_list_index *pli = as_bin_get_packed_list_index(b);
		const uint8_t *ele_ptr = as_unpack_list_elements_find_index(&pl.upk, uindex, pli);
		uint32_t ele_size = 0;

		if (! ele_ptr) {
			cf_warning(AS_PARTICLE, "OP_LIST_GET_RANGE: invalid list element with index <= %u", uindex);
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
			return false;
		}

		for (uint64_t i = 0; i < count; i++) {
			int64_t i_size = as_unpack_size(&pl.upk);

			if (i_size < 0) {
				cf_warning(AS_PARTICLE, "OP_LIST_GET_RANGE: invalid list element at index %u", uindex + (uint32_t)i);
				cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
				return false;
			}

			ele_size += (uint32_t)i_size;
		}

		result->particle = packed_list_simple_create_from_buf(packed_alloc, count, ele_ptr, ele_size);

		if (! result->particle) {
			cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_UNKNOWN;
			return false;
		}

		as_bin_state_set_from_type(result, AS_PARTICLE_TYPE_LIST);

		break;
	}
	case AS_CDT_OP_LIST_SIZE: {
		as_packed_list pl;
		as_packed_list_init_from_bin(&pl, b);

		int32_t ele_count = as_packed_list_header_element_count(&pl);

		if (ele_count < 0) {
			// TODO - is this the right policy?
			as_bin_set_int(result, 0);
		}
		else {
			as_bin_set_int(result, ele_count);
		}

		break;
	}
	default:
		cf_warning(AS_PARTICLE, "cdt_process_state_packed_list_read_optype() invalid cdt op: %d", optype);
		cdt_udata->ret_code = -AS_PROTO_RESULT_FAIL_PARAMETER;
		return false;
	}

	return true;
}


//==========================================================
// Debugging support.
//

static void
print_cdt_list_particle(const as_particle *p)
{
	list_wrapper *p_list_wrapped = (list_wrapper *)p;
	cf_warning(AS_PARTICLE, "print_cdt_list_particle: type=%d", p_list_wrapped->type);

	if (p_list_wrapped->magic == CDT_MAGIC) {
		cf_warning(AS_PARTICLE, "  wrapped");
		cf_warning(AS_PARTICLE, "    flags=%x", p_list_wrapped->flags);
		cf_warning(AS_PARTICLE, "    magic=%X", p_list_wrapped->magic);
		cf_warning(AS_PARTICLE, "    packed_sz=%d", p_list_wrapped->packed_sz);
		char buf[1024];
		print_hex(p_list_wrapped->packed, p_list_wrapped->packed_sz, buf, 1024);
		cf_warning(AS_PARTICLE, "    packed=%s", buf);
	}
	else {
		list_mem *flat = (list_mem *)p;
		cf_warning(AS_PARTICLE, "  flat");
		cf_warning(AS_PARTICLE, "    packed_sz=%d", flat->sz);
		char buf[1024];
		print_hex(flat->data, flat->sz, buf, 1024);
		cf_warning(AS_PARTICLE, "    packed=%s", buf);
	}
}

void
print_cdt_list_bin(const as_bin *b)
{
	int8_t type = as_bin_get_particle_type(b);
	cf_warning(AS_PARTICLE, "print_cdt_list_bin: type=%d", type);

	if (type != AS_PARTICLE_TYPE_LIST) {
		return;
	}

	print_cdt_list_particle(b->particle);
}

void
print_as_packed_list(const as_packed_list *pl)
{
	char buf[1024];

	print_hex(pl->upk.buffer, pl->upk.length, buf, 1024);
	cf_warning(AS_PARTICLE, "as_packed_list: buf='%s' buf_sz=%d offset=%d",
			buf, pl->upk.length, pl->upk.offset);
}
