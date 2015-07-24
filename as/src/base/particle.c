/*
 * particle.c
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
 * particle operations
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "storage/storage.h"


//==========================================================
// Local utilities.
//

//------------------------------------------------
// Particle type check.
//

static inline as_particle_type
safe_particle_type(uint8_t type)
{
	switch ((as_particle_type)type) {
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_TIMESTAMP:
	case AS_PARTICLE_TYPE_JAVA_BLOB:
	case AS_PARTICLE_TYPE_CSHARP_BLOB:
	case AS_PARTICLE_TYPE_PYTHON_BLOB:
	case AS_PARTICLE_TYPE_RUBY_BLOB:
	case AS_PARTICLE_TYPE_PHP_BLOB:
	case AS_PARTICLE_TYPE_ERLANG_BLOB:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_HIDDEN_LIST:
	case AS_PARTICLE_TYPE_HIDDEN_MAP:
		return (as_particle_type)type;
	// Note - AS_PARTICLE_TYPE_NULL is considered bad here.
	default:
		cf_warning(AS_PARTICLE, "encountered bad particle type %u", type);
		return -1;
	}
}


//==========================================================
// NULL particle.
//

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_null(as_particle *p)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_destruct_null()");
}

uint32_t
as_particle_size_null(const as_particle *p)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_size_null()");
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
as_particle_concat_size_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_concat_size_from_wire_null()");
	return -1;
}

int
as_particle_append_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_append_from_wire_null()");
	return -1;
}

int
as_particle_prepend_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_prepend_from_wire_null()");
	return -1;
}

int
as_particle_incr_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_incr_from_wire_null()");
	return -1;
}

int32_t
as_particle_size_from_wire_null(const uint8_t *wire_value, uint32_t value_size)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_size_from_wire_null()");
	return -1;
}

int
as_particle_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_from_wire_null()");
	return -1;
}

int
as_particle_compare_from_wire_null(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_compare_from_wire_null()");
	return -1;
}

uint32_t
as_particle_wire_size_null(const as_particle *p)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_wire_size_null()");
	return 0;
}

uint32_t
as_particle_to_wire_null(const as_particle *p, uint8_t *wire)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_to_wire_null()");
	return 0;
}

//------------------------------------------------
// Handle in-memory format.
//

uint32_t
as_particle_size_from_mem_null(as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_size_from_mem_null()");
	return 0;
}

void
as_particle_from_mem_null(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_from_mem_null()");
}

uint32_t
as_particle_mem_size_null(const as_particle *p)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_mem_size_null()");
	return 0;
}

uint32_t
as_particle_to_mem_null(const as_particle *p, uint8_t *value)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_to_mem_null()");
	return 0;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
as_particle_size_from_flat_null(const uint8_t *flat, uint32_t flat_size)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_size_from_flat_null()");
	return -1;
}

int
as_particle_cast_from_flat_null(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_cast_from_flat_null()");
	return -1;
}

int
as_particle_from_flat_null(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_from_flat_null()");
	return -1;
}

uint32_t
as_particle_flat_size_null(const as_particle *p)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_flat_size_null()");
	return 0;
}

uint32_t
as_particle_to_flat_null(const as_particle *p, uint8_t *flat)
{
	cf_warning(AS_PARTICLE, "unexpected - called as_particle_to_flat_null()");
	return 0;
}


//==========================================================
// INTEGER particle.
//

typedef struct as_particle_int_mem_s {
	uint8_t		do_not_use;	// already know it's an int type
	uint64_t	i;
} __attribute__ ((__packed__)) as_particle_int_mem;

typedef struct as_particle_int_flat_s {
	uint8_t		type;
	uint8_t		size;
	uint64_t	i;
} __attribute__ ((__packed__)) as_particle_int_flat;

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_int(as_particle *p)
{
	// Nothing to do - integer values live in the as_bin.
}

uint32_t
as_particle_size_int(const as_particle *p)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
as_particle_concat_size_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for integer/float");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
as_particle_append_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to integer/float");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
as_particle_prepend_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to integer/float");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
as_particle_incr_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	if (wire_type != AS_PARTICLE_TYPE_INTEGER) {
		cf_warning(AS_PARTICLE, "increment with non integer type %u", wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		i = (uint64_t)cf_swap_from_be32(*(uint32_t *)wire_value);
		break;
	case 2:
		i = (uint64_t)cf_swap_from_be16(*(uint16_t *)wire_value);
		break;
	case 1:
		i = (uint64_t)*wire_value;
		break;
	case 16: // memcache increment - it's special
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		// For memcache, decrements floor at 0.
		if ((int64_t)i < 0 && *(uint64_t *)pp + i > *(uint64_t *)pp) {
			*pp = 0;
			return 0;
		}
		break;
	default:
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	(*(uint64_t *)pp) += i;

	return 0;
}

int32_t
as_particle_size_from_wire_int(const uint8_t *wire_value, uint32_t value_size)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

int
as_particle_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		i = (uint64_t)cf_swap_from_be32(*(uint32_t *)wire_value);
		break;
	case 2:
		i = (uint64_t)cf_swap_from_be16(*(uint16_t *)wire_value);
		break;
	case 1:
		i = (uint64_t)*wire_value;
		break;
	default:
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	*pp = (as_particle *)i;

	return 0;
}

int
as_particle_compare_from_wire_int(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	if (wire_type != AS_PARTICLE_TYPE_INTEGER) {
		return 1;
	}

	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		i = (uint64_t)cf_swap_from_be32(*(uint32_t *)wire_value);
		break;
	case 2:
		i = (uint64_t)cf_swap_from_be16(*(uint16_t *)wire_value);
		break;
	case 1:
		i = (uint64_t)*wire_value;
		break;
	default:
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	return (uint64_t)p == i ? 0 : 1;
}

uint32_t
as_particle_wire_size_int(const as_particle *p)
{
	return (uint32_t)sizeof(uint64_t);
}

uint32_t
as_particle_to_wire_int(const as_particle *p, uint8_t *wire)
{
	*(uint64_t *)wire = cf_swap_to_be64((uint64_t)p);

	return (uint32_t)sizeof(uint64_t);
}

//------------------------------------------------
// Handle in-memory format.
//

uint32_t
as_particle_size_from_mem_int(as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

void
as_particle_from_mem_int(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp)
{
	if (value_size != 8) {
		cf_crash(AS_PARTICLE, "unexpected value size %u", value_size);
	}

	uint64_t i = *(uint64_t *)mem_value;

	*pp = (as_particle *)i;
}

uint32_t
as_particle_mem_size_int(const as_particle *p)
{
	return sizeof(uint64_t);
}

uint32_t
as_particle_to_mem_int(const as_particle *p, uint8_t *value)
{
	*(uint64_t *)value = (uint64_t)p;

	return sizeof(uint64_t);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
as_particle_size_from_flat_int(const uint8_t *flat, uint32_t flat_size)
{
	// Integer values live in the as_bin instead of a pointer.
	return 0;
}

int
as_particle_cast_from_flat_int(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	as_particle_int_flat *p_int_flat = (as_particle_int_flat *)flat;
	// Assume type is correct, since we got here.

	// Sanity check lengths.
	if (p_int_flat->size != 8 || flat_size != sizeof(as_particle_int_flat)) {
		cf_warning(AS_PARTICLE, "unexpected flat integer: flat_size %u, len %u",
				flat_size, p_int_flat->size);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Integer values live in an as_bin instead of a pointer. Also, flat
	// integers are host order, so no byte swap.
	*pp = (as_particle *)p_int_flat->i;

	return 0;
}

int
as_particle_from_flat_int(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	const as_particle_int_flat *p_int_flat = (const as_particle_int_flat *)flat;
	// Assume type is correct, since we got here.

	// Sanity check lengths.
	if (p_int_flat->size != 8 || flat_size != sizeof(as_particle_int_flat)) {
		cf_warning(AS_PARTICLE, "unexpected flat integer: flat_size %u, len %u",
				flat_size, p_int_flat->size);
		return -1; // TODO - AS_PROTO error code seems inappropriate?
	}

	// Integer values live in an as_bin instead of a pointer. Also, flat
	// integers are host order, so no byte swap.
	*pp = (as_particle *)p_int_flat->i;

	return 0;
}

uint32_t
as_particle_flat_size_int(const as_particle *p)
{
	return sizeof(as_particle_int_flat);
}

uint32_t
as_particle_to_flat_int(const as_particle *p, uint8_t *flat)
{
	as_particle_int_flat *p_int_flat = (as_particle_int_flat *)flat;

	// Already wrote the type.
	p_int_flat->size = 8;
	p_int_flat->i = (uint64_t)p;

	return as_particle_flat_size_int(p);
}


//==========================================================
// FLOAT particle.
//

// Most FLOAT particle table functions just use the equivalent INTEGER
// particle functions. Here are the differences...

int
as_particle_incr_from_wire_float(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// For now we won't allow adding integers (or anything else) to floats.
	if (wire_type != AS_PARTICLE_TYPE_FLOAT) {
		cf_warning(AS_PARTICLE, "increment with non float type %u", wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	uint64_t i;

	switch (value_size) {
	case 8:
		i = cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	default:
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	(*(double *)pp) += *(double *)&i;

	return 0;
}

int
as_particle_from_wire_float(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	if (value_size != 8) {
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return as_particle_from_wire_int(wire_type, wire_value, value_size, pp);
}

int
as_particle_compare_from_wire_float(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	if (wire_type != AS_PARTICLE_TYPE_FLOAT) {
		return 1;
	}

	if (value_size != 8) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	return as_particle_compare_from_wire_int(p, AS_PARTICLE_TYPE_INTEGER, wire_value, value_size);
}


//==========================================================
// BLOB particle.
//

typedef struct as_particle_blob_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle_blob_mem;

typedef struct as_particle_blob_flat_s {
	uint8_t		type;
	uint32_t	size; // host order on device
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle_blob_flat;

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_blob(as_particle *p)
{
	cf_free(p);
}

uint32_t
as_particle_size_blob(const as_particle *p)
{
	return (uint32_t)(sizeof(as_particle_blob_mem) + ((as_particle_blob_mem *)p)->sz);
}

uint32_t
as_particle_ptr_blob(as_particle *p, uint8_t **p_value)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;

	*p_value = p_blob_mem->data;

	return p_blob_mem->sz;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
as_particle_concat_size_from_wire_blob(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch concat sizing blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	return (int32_t)(sizeof(as_particle_blob_mem) + p_blob_mem->sz + value_size);
}

int
as_particle_append_from_wire_blob(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch appending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	memcpy(p_blob_mem->data + p_blob_mem->sz, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
as_particle_prepend_from_wire_blob(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch prepending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	memmove(p_blob_mem->data + value_size, p_blob_mem->data, p_blob_mem->sz);
	memcpy(p_blob_mem->data, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
as_particle_incr_from_wire_blob(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected increment of blob/string");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
as_particle_size_from_wire_blob(const uint8_t *wire_value, uint32_t value_size)
{
	// Wire value is same as in-memory value.
	return (int32_t)(sizeof(as_particle_blob_mem) + value_size);
}

int
as_particle_from_wire_blob(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)*pp;

	p_blob_mem->type = wire_type;
	p_blob_mem->sz = value_size;
	memcpy(p_blob_mem->data, wire_value, p_blob_mem->sz);

	return 0;
}

int
as_particle_compare_from_wire_blob(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;

	return (wire_type == p_blob_mem->type &&
			value_size == p_blob_mem->sz &&
			memcmp(wire_value, p_blob_mem->data, value_size) == 0) ? 0 : 1;
}

uint32_t
as_particle_wire_size_blob(const as_particle *p)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;

	return p_blob_mem->sz;
}

uint32_t
as_particle_to_wire_blob(const as_particle *p, uint8_t *wire)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;

	memcpy(wire, p_blob_mem->data, p_blob_mem->sz);

	return p_blob_mem->sz;
}

//------------------------------------------------
// Handle in-memory format.
//

uint32_t
as_particle_size_from_mem_blob(as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	return (uint32_t)sizeof(as_particle_blob_mem) + value_size;
}

void
as_particle_from_mem_blob(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)*pp;

	p_blob_mem->type = type;
	p_blob_mem->sz = value_size;
	memcpy(p_blob_mem->data, mem_value, p_blob_mem->sz);
}

uint32_t
as_particle_mem_size_blob(const as_particle *p)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;

	return p_blob_mem->sz;
}

uint32_t
as_particle_to_mem_blob(const as_particle *p, uint8_t *value)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;

	memcpy(value, p_blob_mem->data, p_blob_mem->sz);

	return p_blob_mem->sz;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
as_particle_size_from_flat_blob(const uint8_t *flat, uint32_t flat_size)
{
	as_particle_blob_flat *p_blob_flat = (as_particle_blob_flat *)flat;
	// Assume type is correct, since we got here.

	// Sanity check length.
	if (p_blob_flat->size != flat_size - sizeof(as_particle_blob_flat)) {
		cf_warning(AS_PARTICLE, "unexpected flat blob/string: flat size %u, len %u",
				flat_size, p_blob_flat->size);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Flat value is same as in-memory value.
	return (int32_t)(sizeof(as_particle_blob_mem) + p_blob_flat->size);
}

int
as_particle_cast_from_flat_blob(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Sizing is only a sanity check.
	int32_t mem_size = as_particle_size_from_flat_blob(flat, flat_size);

	if (mem_size < 0) {
		return mem_size;
	}

	// We can do this only because the flat and in-memory formats are identical.
	*pp = (as_particle *)flat;

	return 0;
}

int
as_particle_from_flat_blob(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	int32_t mem_size = as_particle_size_from_flat_blob(flat, flat_size);

	if (mem_size < 0) {
		return mem_size;
	}

	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)cf_malloc((size_t)mem_size);

	if (! p_blob_mem) {
		cf_warning(AS_PARTICLE, "failed malloc for blob/string (%d)", mem_size);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	const as_particle_blob_flat *p_blob_flat = (const as_particle_blob_flat *)flat;

	p_blob_mem->type = p_blob_flat->type;
	p_blob_mem->sz = p_blob_flat->size;
	memcpy(p_blob_mem->data, p_blob_flat->data, p_blob_mem->sz);

	*pp = (as_particle *)p_blob_mem;

	return 0;
}

uint32_t
as_particle_flat_size_blob(const as_particle *p)
{
	return (uint32_t)(sizeof(as_particle_blob_flat) + ((as_particle_blob_mem *)p)->sz);
}

uint32_t
as_particle_to_flat_blob(const as_particle *p, uint8_t *flat)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)p;
	as_particle_blob_flat *p_blob_flat = (as_particle_blob_flat *)flat;

	// Already wrote the type.
	p_blob_flat->size = p_blob_mem->sz;
	memcpy(p_blob_flat->data, p_blob_mem->data, p_blob_flat->size);

	return as_particle_flat_size_blob(p);
}


//==========================================================
// STRING particle.
//

// So far, all STRING particle table functions just use the equivalent BLOB
// particle functions. If they ever differ, add the differing STRING functions
// here...


//==========================================================
// Particle function tables.
//

//------------------------------------------------
// Destructor, etc.
//

typedef void (*as_particle_destructor_fn) (as_particle *p);

as_particle_destructor_fn g_particle_destructor_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_destruct_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_destruct_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_destruct_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_destruct_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_destruct_blob,
};

typedef uint32_t (*as_particle_size_fn) (const as_particle *p);

as_particle_size_fn g_particle_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_size_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_size_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_size_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_size_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_size_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_size_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_size_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_size_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_size_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_size_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_size_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_size_blob,
};

typedef uint32_t (*as_particle_ptr_fn) (as_particle *p, uint8_t **p_value);

as_particle_ptr_fn g_particle_ptr_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= NULL,
	[AS_PARTICLE_TYPE_INTEGER]			= NULL,
	[AS_PARTICLE_TYPE_FLOAT]			= NULL,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= NULL,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_ptr_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_ptr_blob,
};

//------------------------------------------------
// Handle "wire" format.
//

typedef int32_t (*as_particle_concat_size_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

as_particle_concat_size_from_wire_fn g_particle_concat_size_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_concat_size_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_concat_size_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_concat_size_from_wire_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_concat_size_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_concat_size_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_concat_size_from_wire_blob,
};

typedef int (*as_particle_append_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

as_particle_append_from_wire_fn g_particle_append_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_append_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_append_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_append_from_wire_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_append_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_append_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_append_from_wire_blob,
};

typedef int (*as_particle_prepend_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

as_particle_prepend_from_wire_fn g_particle_prepend_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_prepend_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_prepend_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_prepend_from_wire_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_prepend_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_prepend_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_prepend_from_wire_blob,
};

typedef int (*as_particle_incr_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

as_particle_incr_from_wire_fn g_particle_incr_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_incr_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_incr_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_incr_from_wire_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_incr_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_incr_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_incr_from_wire_blob,
};

typedef int32_t (*as_particle_size_from_wire_fn) (const uint8_t *wire_value, uint32_t value_size);

as_particle_size_from_wire_fn g_particle_size_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_size_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_size_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_size_from_wire_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_size_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_size_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_size_from_wire_blob,
};

typedef int (*as_particle_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

as_particle_from_wire_fn g_particle_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_from_wire_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_from_wire_blob,
};

typedef int (*as_particle_compare_from_wire_fn) (const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);

as_particle_compare_from_wire_fn g_particle_compare_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_compare_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_compare_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_compare_from_wire_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_compare_from_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_compare_from_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_compare_from_wire_blob,
};

typedef uint32_t (*as_particle_wire_size_fn) (const as_particle *p);

as_particle_wire_size_fn g_particle_wire_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_wire_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_wire_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_wire_size_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_wire_size_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_wire_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_wire_size_blob,
};

typedef uint32_t (*as_particle_to_wire_fn) (const as_particle *p, uint8_t *wire);

as_particle_to_wire_fn g_particle_to_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_to_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_to_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_to_wire_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_to_wire_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_to_wire_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_to_wire_blob,
};

//------------------------------------------------
// Handle in-memory format.
//

typedef uint32_t (*as_particle_size_from_mem_fn) (as_particle_type type, const uint8_t *value, uint32_t value_size);

as_particle_size_from_mem_fn g_particle_size_from_mem_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_size_from_mem_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_size_from_mem_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_size_from_mem_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_size_from_mem_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_size_from_mem_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_size_from_mem_blob,
};

typedef void (*as_particle_from_mem_fn) (as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp);

as_particle_from_mem_fn g_particle_from_mem_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_from_mem_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_from_mem_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_from_mem_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_from_mem_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_from_mem_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_from_mem_blob,
};

typedef uint32_t (*as_particle_mem_size_fn) (const as_particle *p);

as_particle_mem_size_fn g_particle_mem_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_mem_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_mem_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_mem_size_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_mem_size_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_mem_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_mem_size_blob,
};

typedef uint32_t (*as_particle_to_mem_fn) (const as_particle *p, uint8_t *value);

as_particle_to_mem_fn g_particle_to_mem_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_to_mem_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_to_mem_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_to_mem_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_to_mem_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_to_mem_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_to_mem_blob,
};

//------------------------------------------------
// Handle on-device "flat" format.
//

typedef int32_t (*as_particle_size_from_flat_fn) (const uint8_t *flat, uint32_t flat_size);

as_particle_size_from_flat_fn g_particle_size_from_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_size_from_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_size_from_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_size_from_flat_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_size_from_flat_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_size_from_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_size_from_flat_blob,
};

typedef int (*as_particle_cast_from_flat_fn) (uint8_t *flat, uint32_t flat_size, as_particle **pp);

as_particle_cast_from_flat_fn g_particle_cast_from_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_cast_from_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_cast_from_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_cast_from_flat_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_cast_from_flat_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_cast_from_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_cast_from_flat_blob,
};

typedef int (*as_particle_from_flat_fn) (const uint8_t *flat, uint32_t flat_size, as_particle **pp);

as_particle_from_flat_fn g_particle_from_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_from_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_from_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_from_flat_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_from_flat_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_from_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_from_flat_blob,
};

typedef uint32_t (*as_particle_flat_size_fn) (const as_particle *p);

as_particle_flat_size_fn g_particle_flat_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_flat_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_flat_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_flat_size_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_flat_size_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_flat_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_flat_size_blob,
};

typedef uint32_t (*as_particle_to_flat_fn) (const as_particle *p, uint8_t *flat);

as_particle_to_flat_fn g_particle_to_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_to_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_to_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_to_flat_int,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_to_flat_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_to_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_to_flat_blob,
};


//==========================================================
// Particle "class static" functions.
//

// TODO - will we ever need this?
int32_t
as_particle_size_from_client(const as_msg_op *op)
{
	uint8_t type = op->particle_type;
	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);

	return g_particle_size_from_wire_table[type](value, value_size);
}

int32_t
as_particle_size_from_pickled(uint8_t **p_pickled)
{
	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	uint8_t type = *pickled++;
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;

	*p_pickled = (uint8_t *)value + value_size;

	return g_particle_size_from_wire_table[type](value, value_size);
}

uint32_t
as_particle_size_from_mem(as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	return g_particle_size_from_mem_table[type](type, value, value_size);
}

// TODO - will we ever need this?
int32_t
as_particle_size_from_flat(const uint8_t *flat, uint32_t flat_size)
{
	uint8_t type = *flat;

	return g_particle_size_from_flat_table[type](flat, flat_size);
}

as_particle_type
as_particle_type_convert(as_particle_type type)
{
	switch (type) {
	case AS_PARTICLE_TYPE_HIDDEN_MAP:
		return AS_PARTICLE_TYPE_MAP;
	case AS_PARTICLE_TYPE_HIDDEN_LIST:
		return AS_PARTICLE_TYPE_LIST;
	default:
		return type;
	}
}

as_particle_type
as_particle_type_convert_to_hidden(as_particle_type type)
{
	switch (type) {
	case AS_PARTICLE_TYPE_MAP:
		return AS_PARTICLE_TYPE_HIDDEN_MAP;
	case AS_PARTICLE_TYPE_LIST:
		return AS_PARTICLE_TYPE_HIDDEN_LIST;
	default:
		return type;
	}
}

bool
as_particle_type_hidden(as_particle_type type)
{
	return	type == AS_PARTICLE_TYPE_HIDDEN_MAP ||
			type == AS_PARTICLE_TYPE_HIDDEN_LIST;
}


//==========================================================
// as_bin particle functions - maybe should be elsewhere.
//

//------------------------------------------------
// Destructor, etc.
//

// TODO - shouldn't this set the bin state to UNUSED?
void
as_bin_particle_destroy(as_bin *b, bool free_particle)
{
	if (as_bin_is_embedded_particle(b)) {
		b->particle = 0;
	}
	else if (b->particle) {
		if (free_particle) {
			g_particle_destructor_table[as_bin_get_particle_type(b)](b->particle);
		}

		b->particle = 0;
	}
}

uint32_t
as_bin_particle_size(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		cf_warning(AS_PARTICLE, "sizing unused bin");
		return 0;
	}

	return g_particle_size_table[as_bin_get_particle_type(b)](b->particle);
}

uint32_t
as_bin_particle_ptr(as_bin *b, uint8_t **p_value)
{
	return g_particle_ptr_table[as_bin_get_particle_type(b)](b->particle, p_value);
}

//------------------------------------------------
// Handle "wire" format.
//

// TODO - will we ever need this?
// Unlike the other "size_from" methods, this one needs the existing bin. And
// unlike the "modify" methods, this can be called with a null bin pointer.
int32_t
as_bin_particle_size_modify_from_client(as_bin *b, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type < 0) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	uint8_t *op_value = as_msg_op_get_value_p((as_msg_op *)op);

	// Currently all operations become creates if there's no existing particle.
	if (! (b && as_bin_inuse(b))) {
		// Memcache increment is weird - manipulate to create integer.
		if (operation == AS_MSG_OP_MC_INCR) {
			op_type = AS_PARTICLE_TYPE_INTEGER;
		}

		return g_particle_size_from_wire_table[op_type](op_value, op_value_size);
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
	case AS_MSG_OP_INCR:
		// Currently only embedded types can be incremented.
		return 0;
	case AS_MSG_OP_MC_APPEND:
	case AS_MSG_OP_APPEND:
	case AS_MSG_OP_MC_PREPEND:
	case AS_MSG_OP_PREPEND:
		return g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}
}

int
as_bin_particle_alloc_modify_from_client(as_bin *b, const as_msg_op *op)
{
	// This method does not destroy the existing particle, if any. We assume
	// there is a copy of this bin (and particle reference) elsewhere, and that
	// the copy will be responsible for the existing particle. Therefore it's
	// important on failure to leave the existing particle intact.

	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type < 0) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	uint8_t *op_value = as_msg_op_get_value_p((as_msg_op *)op);

	// Currently all operations become creates if there's no existing particle.
	if (! as_bin_inuse(b)) {
		// Memcache increment is weird - manipulate to create integer.
		if (operation == AS_MSG_OP_MC_INCR) {
			if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			op_type = AS_PARTICLE_TYPE_INTEGER;
			op_value_size = sizeof(uint64_t);
			op_value += sizeof(uint64_t);
		}

		int32_t mem_size = g_particle_size_from_wire_table[op_type](op_value, op_value_size);

		if (mem_size < 0) {
			return (int)mem_size;
		}

		as_particle *old_particle = b->particle;

		if (mem_size != 0) {
			b->particle = cf_malloc((size_t)mem_size);

			if (! b->particle) {
				b->particle = old_particle;
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}

		// Load the new particle into the bin.
		int result = g_particle_from_wire_table[op_type](op_type, op_value, op_value_size, &b->particle);

		// Set the bin's iparticle metadata.
		if (result == 0) {
			as_bin_state_set_from_type(b, op_type);
		}
		else {
			if (mem_size != 0) {
				cf_free(b->particle);
			}

			b->particle = old_particle;
		}

		return result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;
	as_particle *new_particle = NULL;

	as_particle *old_particle = b->particle;
	int result = 0;

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
		if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
		op_type = AS_PARTICLE_TYPE_INTEGER;
		// op_value_size of 16 will flag operation as memcache increment...
		// no break
	case AS_MSG_OP_INCR:
		result = g_particle_incr_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_APPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_APPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return new_mem_size;
		}
		if (! (new_particle = cf_malloc((size_t)new_mem_size))) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(new_particle, b->particle, g_particle_size_table[existing_type](b->particle));
		b->particle = new_particle;
		result = g_particle_append_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_PREPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_PREPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return new_mem_size;
		}
		if (! (new_particle = cf_malloc((size_t)new_mem_size))) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(new_particle, b->particle, g_particle_size_table[existing_type](b->particle));
		b->particle = new_particle;
		result = g_particle_prepend_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		break;
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (result < 0) {
		if (new_mem_size != 0) {
			cf_free(b->particle);
		}

		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_stack_modify_from_client(as_bin *b, cf_dyn_buf *particles_db, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type < 0) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	uint8_t *op_value = as_msg_op_get_value_p((as_msg_op *)op);

	// Currently all operations become creates if there's no existing particle.
	if (! as_bin_inuse(b)) {
		// Memcache increment is weird - manipulate to create integer.
		if (operation == AS_MSG_OP_MC_INCR) {
			if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			op_type = AS_PARTICLE_TYPE_INTEGER;
			op_value_size = sizeof(uint64_t);
			op_value += sizeof(uint64_t);
		}

		int32_t mem_size = g_particle_size_from_wire_table[op_type](op_value, op_value_size);

		if (mem_size < 0) {
			return (int)mem_size;
		}

		as_particle *old_particle = b->particle;

		// Instead of allocating, we use the stack buffer provided. (Note that
		// embedded types like integer will overwrite this with the value.)
		if (0 > cf_dyn_buf_reserve(particles_db, (size_t)mem_size, (uint8_t **)&b->particle)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		// Load the new particle into the bin.
		int result = g_particle_from_wire_table[op_type](op_type, op_value, op_value_size, &b->particle);

		// Set the bin's iparticle metadata.
		if (result == 0) {
			as_bin_state_set_from_type(b, op_type);
		}
		else {
			b->particle = old_particle;
		}

		return result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;

	as_particle *old_particle = b->particle;
	int result = 0;

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
		if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
		op_type = AS_PARTICLE_TYPE_INTEGER;
		// op_value_size of 16 will flag operation as memcache increment...
		// no break
	case AS_MSG_OP_INCR:
		result = g_particle_incr_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_APPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_APPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return (int)new_mem_size;
		}
		if (0 > cf_dyn_buf_reserve(particles_db, (size_t)new_mem_size, (uint8_t **)&b->particle)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(b->particle, old_particle, g_particle_size_table[existing_type](old_particle));
		result = g_particle_append_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_PREPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_PREPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return (int)new_mem_size;
		}
		if (0 > cf_dyn_buf_reserve(particles_db, (size_t)new_mem_size, (uint8_t **)&b->particle)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(b->particle, old_particle, g_particle_size_table[existing_type](old_particle));
		result = g_particle_prepend_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		break;
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (result < 0) {
		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_alloc_from_client(as_bin *b, const as_msg_op *op)
{
	// This method does not destroy the existing particle, if any. We assume
	// there is a copy of this bin (and particle reference) elsewhere, and that
	// the copy will be responsible for the existing particle. Therefore it's
	// important on failure to leave the existing particle intact.

	as_particle_type type = safe_particle_type(op->particle_type);

	if (type < 0) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);
	int32_t mem_size = g_particle_size_from_wire_table[type](value, value_size);

	if (mem_size < 0) {
		return (int)mem_size;
	}

	as_particle *old_particle = b->particle;

	if (mem_size != 0) {
		b->particle = cf_malloc((size_t)mem_size);

		if (! b->particle) {
			b->particle = old_particle;
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	// Load the new particle into the bin.
	int result = g_particle_from_wire_table[type](type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		if (mem_size != 0) {
			cf_free(b->particle);
		}

		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_stack_from_client(as_bin *b, cf_dyn_buf *particles_db, const as_msg_op *op)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	as_particle_type type = safe_particle_type(op->particle_type);

	if (type < 0) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);
	int32_t mem_size = g_particle_size_from_wire_table[type](value, value_size);

	if (mem_size < 0) {
		return (int)mem_size;
	}

	as_particle *old_particle = b->particle;

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	if (0 > cf_dyn_buf_reserve(particles_db, (size_t)mem_size, (uint8_t **)&b->particle)) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Load the new particle into the bin.
	int result = g_particle_from_wire_table[type](type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		b->particle = old_particle;
	}

	return result;
}

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_replace_from_pickled(as_bin *b, uint8_t **p_pickled)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	uint32_t old_mem_size = as_bin_inuse(b) ? g_particle_size_table[old_type](b->particle) : 0;

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type new_type = safe_particle_type(*pickled++);
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t new_value_size = cf_swap_from_be32(*p32++);
	const uint8_t *new_value = (const uint8_t *)p32;
	int32_t new_mem_size = g_particle_size_from_wire_table[new_type](new_value, new_value_size);

	*p_pickled = (uint8_t *)new_value + new_value_size;

	if (new_type < 0) {
		return (int)new_type;
	}

	if (new_mem_size < 0) {
		// Leave existing particle intact.
		return (int)new_mem_size;
	}

	if ((uint32_t)new_mem_size != old_mem_size) {
		if (as_bin_inuse(b)) {
			// Destroy the old particle.
			g_particle_destructor_table[old_type](b->particle);
		}

		b->particle = NULL;
	}

	if (new_mem_size != 0 && ! b->particle) {
		b->particle = cf_malloc((size_t)new_mem_size);

		if (! b->particle) {
			as_bin_set_empty(b);
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	// Load the new particle into the bin.
	int result = g_particle_from_wire_table[new_type](new_type, new_value, new_value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, new_type);
	}
	else {
		if (as_bin_inuse(b)) {
			// Destroy the old particle.
			g_particle_destructor_table[old_type](b->particle);
		}

		b->particle = NULL;
		as_bin_set_empty(b);
	}

	return result;
}

// TODO - re-do to leave original intact on failure.
int32_t
as_bin_particle_stack_from_pickled(as_bin *b, uint8_t *stack, uint8_t **p_pickled)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type type = safe_particle_type(*pickled++);
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;
	int32_t mem_size = g_particle_size_from_wire_table[type](value, value_size);

	*p_pickled = (uint8_t *)value + value_size;

	if (type < 0) {
		return (int)type;
	}

	if (mem_size < 0) {
		// Leave existing particle intact.
		return mem_size;
	}

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	b->particle = (as_particle *)stack;

	// Load the new particle into the bin.
	int result = g_particle_from_wire_table[type](type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		b->particle = NULL;
		as_bin_set_empty(b);
	}

	return result == 0 ? mem_size : (int32_t)result;
}

int
as_bin_particle_compare_from_pickled(const as_bin *b, uint8_t **p_pickled)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "comparing to unused bin");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type type = (as_particle_type)*pickled++;
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;

	*p_pickled = (uint8_t *)value + value_size;

	return g_particle_compare_from_wire_table[as_bin_get_particle_type(b)](b->particle, type, value, value_size);
}

uint32_t
as_bin_particle_client_value_size(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		// UDF result bin (bin name "SUCCESS" or "FAILURE") will get here.
		return 0;
	}

	if (as_bin_is_hidden(b)) {
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	return g_particle_wire_size_table[type](b->particle);
}

uint32_t
as_bin_particle_to_client(const as_bin *b, as_msg_op *op)
{
	if (! (b && as_bin_inuse(b))) {
		// UDF result bin (bin name "SUCCESS" or "FAILURE") will get here.
		// Ordered ops that find no bin will get here.
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	if (as_bin_is_hidden(b)) {
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	op->particle_type = type;

	uint8_t *value = (uint8_t *)op + sizeof(as_msg_op) + op->name_sz;
	uint32_t added_size = g_particle_to_wire_table[type](b->particle, value);

	op->op_sz += added_size;

	return added_size;
}

uint32_t
as_bin_particle_pickled_size(as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	// Always a type byte and a 32-bit size.
	return 1 + 4 + g_particle_wire_size_table[type](b->particle);
}

uint32_t
as_bin_particle_to_pickled(const as_bin *b, uint8_t *pickled)
{
	uint8_t type = as_bin_get_particle_type(b);

	*pickled++ = type;

	uint32_t *p32 = (uint32_t *)pickled;

	*p32++ = cf_swap_to_be32(g_particle_wire_size_table[type](b->particle));

	uint8_t *value = (uint8_t *)p32;

	return 1 + 4 + g_particle_to_wire_table[type](b->particle, value);
}

//
// LDTs are special.
//

uint32_t
as_ldt_particle_client_value_size(as_storage_rd *rd, as_bin *b, as_val **p_val)
{
	*p_val = as_llist_scan(rd->ns, rd->ns->partitions[as_partition_getid(rd->keyd)].sub_vp, rd, b);

	if (! *p_val) {
		return 0;
	}

	as_serializer s;
	as_msgpack_init(&s);

	uint32_t added_size = as_serializer_serialize_getsize(&s, *p_val);

	as_serializer_destroy(&s);

	return added_size;
}

uint32_t
as_ldt_particle_to_client(const as_val *val, as_msg_op *op)
{
	if (! val) {
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	op->particle_type = AS_PARTICLE_TYPE_HIDDEN_LIST;

	uint8_t *value = (uint8_t *)op + sizeof(as_msg_op) + op->name_sz;

	as_buffer abuf;
	as_buffer_init(&abuf);

	as_serializer s;
	as_msgpack_init(&s);
	as_serializer_serialize(&s, (as_val *)val, &abuf);

	uint32_t added_size = abuf.size;

	memcpy(value, abuf.data, abuf.size);

	as_serializer_destroy(&s);
	as_buffer_destroy(&abuf);
	as_val_destroy(val);

	op->op_sz += added_size;

	return added_size;
}

//------------------------------------------------
// Handle in-memory format.
//

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_replace_from_mem(as_bin *b, as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	uint32_t old_mem_size = as_bin_inuse(b) ? g_particle_size_table[old_type](b->particle) : 0;

	uint32_t new_mem_size = g_particle_size_from_mem_table[type](type, value, value_size);

	if (new_mem_size != old_mem_size) {
		if (as_bin_inuse(b)) {
			// Destroy the old particle.
			g_particle_destructor_table[old_type](b->particle);
		}

		b->particle = NULL;
	}

	if (new_mem_size != 0 && ! b->particle) {
		b->particle = cf_malloc(new_mem_size);

		if (! b->particle) {
			as_bin_set_empty(b);
			return -1; // TODO - AS_PROTO error code seems inappropriate?
		}
	}

	// Load the new particle into the bin.
	g_particle_from_mem_table[type](type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, type);

	return 0;
}

uint32_t
as_bin_particle_stack_from_mem(as_bin *b, uint8_t* stack, as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	uint32_t mem_size = g_particle_size_from_mem_table[type](type, value, value_size);

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	b->particle = (as_particle *)stack;

	// Load the new particle into the bin.
	g_particle_from_mem_table[type](type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, type);

	return mem_size;
}

uint32_t
as_bin_particle_mem_size(as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	return g_particle_mem_size_table[type](b->particle);
}

uint32_t
as_bin_particle_to_mem(const as_bin *b, uint8_t *value)
{
	uint8_t type = as_bin_get_particle_type(b);

	return g_particle_to_mem_table[type](b->particle, value);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_cast_from_flat(as_bin *b, uint8_t *flat, uint32_t flat_size)
{
	if (as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "cast from flat into used bin");
		return -1;
	}

	as_particle_type type = safe_particle_type(*flat);

	if (type < 0) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Cast the new particle into the bin.
	int result = g_particle_cast_from_flat_table[type](flat, flat_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		as_bin_set_empty(b);
	}

	return result;
}

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, uint32_t flat_size)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	as_particle_type new_type = safe_particle_type(*flat);

	if (new_type < 0) {
		return (int)new_type;
	}

	// Just destroy the old particle, if any - we're replacing it.
	g_particle_destructor_table[old_type](b->particle);

	// Load the new particle into the bin.
	int result = g_particle_from_flat_table[new_type](flat, flat_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, new_type);
	}
	else {
		as_bin_set_empty(b);
	}

	return result;
}

uint32_t
as_bin_particle_flat_size(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "flat sizing unused bin");
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	return g_particle_flat_size_table[type](b->particle);
}

uint32_t
as_bin_particle_to_flat(const as_bin *b, uint8_t *flat)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "flattening unused bin");
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	*flat = type;

	return g_particle_to_flat_table[type](b->particle, flat);
}
