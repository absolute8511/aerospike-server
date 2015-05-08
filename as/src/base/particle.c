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

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/proto.h"


//==========================================================
// NULL particle.
//

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_null(as_particle *p)
{
	cf_free(p); // TODO - really?
}

uint32_t
as_particle_size_null(const as_particle *p)
{
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
as_particle_concat_size_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	return -1;
}

int32_t
as_particle_append_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	return -1;
}

int32_t
as_particle_prepend_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	return -1;
}

int32_t
as_particle_incr_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	return -1;
}

int32_t
as_particle_size_from_wire_null(const uint8_t *wire_value, uint32_t value_size)
{
	return -1;
}

int
as_particle_from_wire_null(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	return -1;
}

uint32_t
as_particle_wire_size_null(const as_particle *p)
{
	return 0;
}

uint32_t
as_particle_to_wire_null(const as_particle *p, uint8_t *wire)
{
	return 0;
}

//------------------------------------------------
// Handle in-memory format.
//

uint32_t
as_particle_size_from_mem_null(as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	return 0;
}

void
as_particle_from_mem_null(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp)
{
}

uint32_t
as_particle_mem_size_null(const as_particle *p)
{
	return 0;
}

uint32_t
as_particle_to_mem_null(const as_particle *p, uint8_t *value)
{
	return 0;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
as_particle_size_from_flat_null(const uint8_t *flat, uint32_t flat_size)
{
	return -1;
}

int
as_particle_cast_from_flat_null(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	return -1;
}

int
as_particle_from_flat_null(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	return -1;
}

uint32_t
as_particle_flat_size_null(const as_particle *p)
{
	return 1;
}

uint32_t
as_particle_to_flat_null(const as_particle *p, uint8_t *flat)
{
	cf_warning(AS_PARTICLE, "unexpected - stored NULL particle");
	return as_particle_flat_size_null(p);
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

int32_t
as_particle_append_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to integer/float");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
as_particle_prepend_from_wire_int(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to integer/float");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
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
		if ((int64_t)i < 0 && *(uint64_t*)pp + i > *(uint64_t*)pp) {
			*pp = 0;
			return 0;
		}
		break;
	default:
		*pp = 0;
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	(*(uint64_t*)pp) += i;

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
		*pp = 0;
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	*pp = (as_particle *)i;

	return 0;
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

int32_t
as_particle_incr_from_wire_float(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// For now we won't allow adding integers (or anything else) to floats.
	if (wire_type != AS_PARTICLE_TYPE_FLOAT) {
		cf_warning(AS_PARTICLE, "increment with non float type %u", wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	double x;

	switch (value_size) {
	case 8:
		x = (double)cf_swap_from_be64(*(uint64_t *)wire_value);
		break;
	case 4:
		x = (double)cf_swap_from_be32(*(uint64_t *)wire_value);
		break;
	default:
		*pp = 0;
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	(*(double*)pp) += x;

	return 0;
}

int
as_particle_from_wire_float(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	if (! (value_size == 8 || value_size == 4)) {
		*pp = 0;
		cf_warning(AS_PARTICLE, "unexpected value size %u", value_size);
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	return as_particle_from_wire_int(wire_type, wire_value, value_size, pp);
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

int32_t
as_particle_append_from_wire_blob(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch appending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	memcpy(p_blob_mem->data + p_blob_mem->sz, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return (int32_t)(sizeof(as_particle_blob_mem) + p_blob_mem->sz);
}

int32_t
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

	return (int32_t)(sizeof(as_particle_blob_mem) + p_blob_mem->sz);
}

int32_t
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

typedef int32_t (*as_particle_append_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

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

typedef int32_t (*as_particle_prepend_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

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

typedef int32_t (*as_particle_incr_from_wire_fn) (as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);

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

// TODO - deprecate - for from-wire flat sizing.
uint32_t
as_particle_flat_size(uint8_t type, uint32_t value_size)
{
	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		return 0;
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
		return (uint32_t)sizeof(as_particle_int_flat);
	default:
		return sizeof(as_particle_blob_mem) + value_size;
	}
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

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

	return g_particle_size_table[type](p);
}

uint32_t
as_bin_particle_ptr(as_bin *b, uint8_t **p_value)
{
	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

	return g_particle_ptr_table[type](p, p_value);
}

//------------------------------------------------
// Operations (arithmetic, append, prepend).
//

/*
// guaranteed to work, any architecture, any swap, any alignment
uint64_t int_convert(void *data, uint32_t sz)
{
	uint8_t *b = (uint8_t *)data;
	uint64_t r = 0;
	while (sz) {
		r <<= 8;
		r |= *b++;
		sz--;
	}
	return(r);
}

static int
as_particle_add_int(as_particle *p, void *data, uint32_t sz, bool mc_compliant)
{
	if (!p || !data)
		return(-1);

	uint64_t i;
	if (sz == 8) {
		i = __be64_to_cpup(data);
	}
	else if (sz == 4) {
		i = __be32_to_cpup(data);
	}
	else {
		i = int_convert(data, sz);
	}

// The integer particle is never allocated anymore
	if (!p) {
		cf_info(AS_PARTICLE, "as_particle_add_int: null particle passed inf or integer. Error ");
		return (-1);
	}

	as_particle_int_mem *pi = (as_particle_int_mem *)p;

	// memcache has wrap requirement that decrements cannot take the value
	// below 0. The value is unsigned, so that really means don't wrap from 0 to
	// 0xFFFF... It also means you can't use (int64_t)(pi->i + i) < 0 since
	// all unsigned numbers > 0x8000... typecast to signed will be < 0.
	if (mc_compliant && ((int64_t)i < 0) && ((pi->i + i) > pi->i)) {
		pi->i = 0;
	} else {
		pi->i += i;
	}

	return(0);
}

int
as_particle_increment(as_bin *b, as_particle_type type, byte *buf, uint32_t sz, bool mc_compliant)
{
	if (type != AS_PARTICLE_TYPE_INTEGER) {
		cf_info(AS_PARTICLE, "attempt to add using non-integer");
		return(-1);
	}

	if (as_bin_is_integer(b)) {
		// standard case
		if (0 != as_particle_add_int(&b->iparticle, buf, sz, mc_compliant)) {
			return(-1);
		}
	}
	else {
		cf_info(AS_PARTICLE, "attempt to add to a non-integer");
		return(-2);
	}

	return( 0 );
}
*/

/*
 * Utility function for memcache compatibility, where we may
 * need to change a blob to an int...
 * Given a buffer of data, determine if it can be represented
 * as a signed 64 bit integer. If it can, return true and set
 * the output value. If it cannot, return false.
 */
/*
static bool
cl_strtoll(char *p_str, uint32_t len, int64_t *o_int64)
{
	errno = 0;
	char *endptr;
	int64_t rv = strtoll(p_str, &endptr, 10);
	if (errno != 0) {
		return false;
	}
	// Make sure we use *all* of the characters in the buffer to make a legal
	// integer - strtoll() allows legal padding at the beginning.
	if (endptr != p_str + len) {
		return false;
	}

	*o_int64 =  rv;

	return true;
}

static void
particle_append_prepend_data_impl(as_bin *b, as_particle_type existing_type, as_particle_type incoming_type,
		as_particle_type *new_type, void *data, uint32_t data_len, bool data_in_memory, bool is_append, bool mc_compliant)
{
	// particle better currently exist...
	if (!b)
		return;

	if (!mc_compliant && incoming_type != existing_type) {
		cf_warning(AS_PARTICLE, "Invalid type on append");
		return;
	}

	uint8_t  *p_new_data = NULL;
	as_particle *p_particle_ret = NULL;

	switch(existing_type) {
		case AS_PARTICLE_TYPE_STRING:
		{
			as_particle_blob_mem *ps = (as_particle_blob_mem *)(b->particle);
			if (data_in_memory) {
				as_particle_blob_mem *new_ps = (as_particle_blob_mem *)cf_malloc(sizeof(as_particle_blob_mem) + data_len + ps->sz);

				// copy header, then data
				memcpy(new_ps, ps, sizeof(as_particle_blob_mem));
				memcpy(new_ps->data + (is_append ? 0 : data_len), ps->data, ps->sz);

				cf_free(ps);
				ps = new_ps;
			} else {
				// if data not in memory, we had to have allocated space before
				// but still need to copy over the previous data in case it's a prepend
				if (!is_append) {
					memmove(ps->data + data_len, ps->data, ps->sz);
				}
			}

			p_new_data = is_append ? ps->data + ps->sz : ps->data;
			ps->sz += data_len;
			p_particle_ret = (as_particle *)ps;
			*new_type = AS_PARTICLE_TYPE_STRING;
			break;
		}
		case AS_PARTICLE_TYPE_BLOB:
		{
			as_particle_blob_mem *pb = (as_particle_blob_mem *)(b->particle);
			if (data_in_memory) {
				as_particle_blob_mem *new_pb = (as_particle_blob_mem *)cf_malloc(sizeof(as_particle_blob_mem) + data_len + pb->sz);

				// copy header, then data
				memcpy(new_pb, pb, sizeof(as_particle_blob_mem));
				memcpy(new_pb->data + (is_append ? 0 : data_len), pb->data, pb->sz);

				cf_free(pb);
				pb = new_pb;
			} else {
				// if data not in memory, we had to have allocated space before
				// but still need to copy over the previous data in case it's a prepend
				if (!is_append) {
					memmove(pb->data + data_len, pb->data, pb->sz);
				}
			}

			p_new_data = is_append ? pb->data + pb->sz : pb->data;
			pb->sz += data_len;
			p_particle_ret = (as_particle *)pb;
			*new_type = AS_PARTICLE_TYPE_BLOB;
			break;
		}
		case AS_PARTICLE_TYPE_INTEGER:
		{
			char buf[64 + data_len];
			as_particle_int_mem *pi = (as_particle_int_mem *)&b->iparticle;

			// write integer into buffer
			char *p_buf;
			if (is_append) {
				p_buf = buf;
			} else {
				p_buf = buf + data_len;
			}
			sprintf(p_buf, "%"PRIi64"", pi->i);

			uint32_t int_len = strlen(p_buf);

			// now copy incoming into buffer...
			if (is_append) {
				p_new_data = (uint8_t *)buf + int_len;
				*(p_new_data + data_len) = '\0'; // null terminate, if we're putting a blob of data at the end.
			} else {
				p_new_data = (uint8_t *)buf;
			}
			memcpy(p_new_data, data, data_len);

			int64_t new_int;
			if (cl_strtoll(buf, data_len + int_len, &new_int)) {
				// it's an integer.
				pi->i = (uint64_t)new_int;
				*new_type = AS_PARTICLE_TYPE_INTEGER;
			} else {
				if (incoming_type == AS_PARTICLE_TYPE_STRING) {
					uint32_t new_data_len = strlen(buf);
					as_particle_blob_mem *ps = (as_particle_blob_mem *)cf_malloc(sizeof(as_particle_blob_mem) +  new_data_len);
					ps->sz = new_data_len;
					memcpy(ps->data, buf, new_data_len);
					ps->type = AS_PARTICLE_TYPE_STRING;
					b->particle = (as_particle *)ps;
					*new_type = AS_PARTICLE_TYPE_STRING;
				} else { // it's a blob.
					uint32_t new_data_len = data_len + int_len;
					as_particle_blob_mem *pb = (as_particle_blob_mem *)cf_malloc(sizeof(as_particle_blob_mem) + new_data_len);
					pb->sz = new_data_len;
					memcpy(pb->data, buf, new_data_len);
					pb->type = AS_PARTICLE_TYPE_BLOB;
					b->particle = (as_particle *)pb;
					*new_type = AS_PARTICLE_TYPE_BLOB;
				}
			}
			break;
		}
		default:
			break;
	}

	// append/prepend new data. In the integer case, this has been done specially
	if (existing_type == AS_PARTICLE_TYPE_STRING || existing_type == AS_PARTICLE_TYPE_BLOB) {
		memcpy(p_new_data, data, data_len);
		b->particle = p_particle_ret;
		//cf_warning(AS_PARTICLE, "copied %d to [%s] from [%s]", data_len, p_new_data,data);
	}
}

//In case of memcache append/prepend, appending string to a string is allowed currently.
//We have the capability to append Integers and Blobs,but we have restricted it currently,
//to avoid confusions in use cases.
int
as_particle_append_prepend_data(as_bin *b, as_particle_type type, byte *data, uint32_t data_len, bool data_in_memory, bool is_append, bool memcache_compliant)
{
	as_particle_type old_type = as_bin_get_particle_type(b);
	as_particle_type new_type;

	switch(old_type) {
		case AS_PARTICLE_TYPE_STRING:
			particle_append_prepend_data_impl(b, old_type, type, &new_type, data, data_len, data_in_memory, is_append, memcache_compliant);
			break;
		case AS_PARTICLE_TYPE_BLOB:
			if(!memcache_compliant) {
				particle_append_prepend_data_impl(b, old_type, type, &new_type, data, data_len, data_in_memory, is_append, memcache_compliant);
			}
			else {
				return -1;
			}
			break;

		case AS_PARTICLE_TYPE_INTEGER:
			return -1;
			break;
		default:
			return -1;
	}

	if (new_type == AS_PARTICLE_TYPE_INTEGER) {
		as_bin_state_set(b, AS_BIN_STATE_INUSE_INTEGER);
	} else {
		// We do not support append prepend to hidden bins
		as_bin_state_set(b, AS_BIN_STATE_INUSE_OTHER);
	}

	return 0;
}
*/

//------------------------------------------------
// Handle "wire" format.
//

// Unlike the other "size_from" methods, this one needs the existing bin. And
// unlike the "modify" methods, this can be called with a null bin pointer.
int32_t
as_bin_particle_size_modify_from_client(as_bin *b, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = (as_particle_type)op->particle_type;
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
as_bin_particle_replace_modify_from_client(as_bin *b, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = (as_particle_type)op->particle_type;
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
			// Leave existing particle intact.
			return (int)mem_size;
		}

		if (mem_size != 0) {
			b->particle = cf_malloc((size_t)mem_size);

			if (! b->particle) {
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
			b->particle = NULL;
		}

		return result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;
	as_particle *new_particle = NULL;

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
		if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
		op_type = AS_PARTICLE_TYPE_INTEGER;
		// op_value_size of 16 will flag operation as memcache increment...
		// no break
	case AS_MSG_OP_INCR:
		return g_particle_incr_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	case AS_MSG_OP_MC_APPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_APPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			// Leave existing particle intact.
			return new_mem_size;
		}
		if (! (new_particle = cf_realloc(b->particle, (size_t)new_mem_size))) {
			// Leave existing particle intact.
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		b->particle = new_particle;
		return g_particle_append_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	case AS_MSG_OP_MC_PREPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_PREPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			// Leave existing particle intact.
			return new_mem_size;
		}
		if (! (new_particle = cf_realloc(b->particle, (size_t)new_mem_size))) {
			// Leave existing particle intact.
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		b->particle = new_particle;
		return g_particle_prepend_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}
}

int32_t
as_bin_particle_stack_modify_from_client(as_bin *b, uint8_t* stack, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = (as_particle_type)op->particle_type;
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
			// Leave existing particle intact.
			return mem_size;
		}

		// Instead of allocating, we use the stack buffer provided. (Note that
		// embedded types like integer will overwrite this with the value.)
		b->particle = (as_particle *)stack;

		// Load the new particle into the bin.
		int result = g_particle_from_wire_table[op_type](op_type, op_value, op_value_size, &b->particle);

		// Set the bin's iparticle metadata.
		if (result == 0) {
			as_bin_state_set_from_type(b, op_type);
		}
		else {
			b->particle = NULL;
		}

		return result == 0 ? mem_size : (int32_t)result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
		if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
		op_type = AS_PARTICLE_TYPE_INTEGER;
		// op_value_size of 16 will flag operation as memcache increment...
		// no break
	case AS_MSG_OP_INCR:
		return g_particle_incr_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	case AS_MSG_OP_MC_APPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_APPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			// Leave existing particle intact.
			return new_mem_size;
		}
		memcpy(stack, b->particle, g_particle_size_table[existing_type](b->particle));
		b->particle = (as_particle *)stack;
		return g_particle_append_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	case AS_MSG_OP_MC_PREPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_PREPEND:
		new_mem_size = g_particle_concat_size_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			// Leave existing particle intact.
			return new_mem_size;
		}
		memcpy(stack, b->particle, g_particle_size_table[existing_type](b->particle));
		b->particle = (as_particle *)stack;
		return g_particle_prepend_from_wire_table[existing_type](op_type, op_value, op_value_size, &b->particle);
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}
}

int
as_bin_particle_replace_from_client(as_bin *b, const as_msg_op *op)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	uint32_t old_mem_size = as_bin_inuse(b) ?
			g_particle_size_table[old_type](as_bin_get_particle(b)) : 0;

	as_particle_type new_type = (as_particle_type)op->particle_type;
	uint32_t new_value_size = as_msg_op_get_value_sz(op);
	uint8_t *new_value = as_msg_op_get_value_p((as_msg_op *)op);
	int32_t new_mem_size = g_particle_size_from_wire_table[new_type](new_value, new_value_size);

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

int
as_bin_particle_replace_from_pickled(as_bin *b, uint8_t **p_pickled)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	uint32_t old_mem_size = as_bin_inuse(b) ?
			g_particle_size_table[old_type](as_bin_get_particle(b)) : 0;

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type new_type = (as_particle_type)*pickled++;
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t new_value_size = cf_swap_from_be32(*p32++);
	const uint8_t *new_value = (const uint8_t *)p32;
	int32_t new_mem_size = g_particle_size_from_wire_table[new_type](new_value, new_value_size);

	*p_pickled = (uint8_t *)new_value + new_value_size;

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

int32_t
as_bin_particle_stack_from_client(as_bin *b, uint8_t *stack, const as_msg_op *op)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	as_particle_type type = (as_particle_type)op->particle_type;
	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);
	int32_t mem_size = g_particle_size_from_wire_table[type](value, value_size);

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

int32_t
as_bin_particle_stack_from_pickled(as_bin *b, uint8_t *stack, uint8_t **p_pickled)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type type = (as_particle_type)*pickled++;
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;
	int32_t mem_size = g_particle_size_from_wire_table[type](value, value_size);

	*p_pickled = (uint8_t *)value + value_size;

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

uint32_t
as_bin_particle_client_value_size(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "sizing unused bin");
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
		// Currently, 'select' transactions that don't find a bin get here.
		// TODO - consider not returning the bin? (Like scans, etc.)
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

/*
// NOTE: tojson is IGNORED
int
_as_particle_towire(as_bin *b, byte *buf, uint32_t *sz, bool tojson) {
	if (!b)
		return (-1);

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle towire: bad particle type %d, error", (int)type);
		return(-1);
	}
#endif

	int rv = g_particle_to_wire_table[type](p, buf, sz);

	return(rv);

}

int
as_particle_towire(as_bin *b, byte *buf, uint32_t *sz) {
	return _as_particle_towire(b, buf, sz, 0);
}
*/

//------------------------------------------------
// Handle in-memory format.
//

/*
as_particle *
as_particle_frommem(as_bin *b, as_particle_type type, byte *buf, uint32_t sz, uint8_t *stack_particle, bool data_in_memory)
{

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle frommem: bad particle type %d, error", (int)type);
		return(NULL);
	}
#endif
	as_particle *retval = 0;

	if (data_in_memory) {
		// we have to deal with these cases
		// current type is integer, new type is integer
		// current type is not integer, new type is integer
		// current type is integer, new type is not integer
		// current type is not integer, new type is not integer
		if (as_bin_is_integer(b)) {
			if (type == AS_PARTICLE_TYPE_INTEGER) {
				// current type is integer, new type is integer
				// just copy the new integer over the existing one.
				return (g_particle_from_mem_table[type](&b->iparticle, type, buf, sz, data_in_memory));
			}
			else {
				// current type is integer, new type is not integer
				// make this the same case as current type is not integer, new type is not integer
				// cleanup the integer and allocate a pointer.
				b->particle = 0;
			}
		}
		else if (as_bin_inuse(b)) {
			// if it's a completely new type, destruct the old one and create a new one
			uint8_t bin_particle_type = as_bin_get_particle_type(b);
			if (type != bin_particle_type) {
				g_particle_destructor_table[bin_particle_type](b->particle);
				b->particle = 0;
			}
		}
		else {
			b->particle = 0;
		}
	}

	switch (type) {
		case AS_PARTICLE_TYPE_INTEGER:
			// current type is not integer, new type is integer
			as_bin_state_set(b, AS_BIN_STATE_INUSE_INTEGER);
			// use the iparticle embedded in the bin
			retval = g_particle_from_mem_table[type](&b->iparticle, type, buf, sz, data_in_memory);
			break;
		case AS_PARTICLE_TYPE_NULL:
			// special case, used to free old particle w/o setting new one
			break;
		default:
			// current type is not integer, new type is not integer
			if (! data_in_memory) {
				b->particle = (as_particle *)stack_particle;
			}

			if (as_particle_type_hidden(type)) {
				as_bin_state_set(b, AS_BIN_STATE_INUSE_HIDDEN);
			} else {
				as_bin_state_set(b, AS_BIN_STATE_INUSE_OTHER);
			}
			b->particle = g_particle_from_mem_table[type](b->particle, type, buf, sz, data_in_memory);
			retval = b->particle;
			break;
	}

	return(retval);
}
*/

int
as_bin_particle_replace_from_mem(as_bin *b, as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	uint32_t old_mem_size = as_bin_inuse(b) ?
			g_particle_size_table[old_type](as_bin_get_particle(b)) : 0;

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

/*
// NOTE: tojson is IGNORED
int
_as_particle_tomem(as_bin *b, byte *buf, uint32_t *sz, bool tojson) {
	if (!b)
		return (-1);

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle tomem: bad particle type %d, error", (int)type);
		return(-1);
	}
#endif

	int rv = g_particle_to_mem_table[type](p, buf, sz);

	return(rv);

}

int
as_particle_tomem(as_bin *b, byte *buf, uint32_t *sz) {
	return _as_particle_tomem(b, buf, sz, 0);
}
*/

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

int
as_bin_particle_cast_from_flat(as_bin *b, uint8_t *flat, uint32_t flat_size)
{
	if (as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "cast from flat into used bin");
		return -1;
	}

	as_particle_type type = (as_particle_type)*flat;

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

int
as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, uint32_t flat_size)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	as_particle_type new_type = (as_particle_type)*flat;

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
