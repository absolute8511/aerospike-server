/*
 * particle_map.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
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


#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h" // TODO - eventually separate from BLOB
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

// TODO - eventually none will use BLOB.
const as_particle_vtable map_vtable = {
		blob_destruct,
		blob_size,

		map_concat_size_from_wire,
		map_append_from_wire,
		map_prepend_from_wire,
		map_incr_from_wire,
		blob_size_from_wire,
		blob_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		blob_to_wire,

		map_size_from_asval,
		map_from_asval,
		map_to_asval,
		map_asval_wire_size,
		map_asval_to_wire,

		map_size_from_msgpack,
		map_from_msgpack,

		blob_size_from_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

// For now, same as related BLOB struct. TODO - eventually separate from BLOB.

typedef struct map_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) map_mem;


//==========================================================
// Forward declarations.
//


//==========================================================
// MAP particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
map_destruct(as_particle *p)
{
	// TODO
}

uint32_t
map_size(const as_particle *p)
{
	// TODO
	return 0;
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
	// TODO
	return -1;
}

int
map_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// TODO
	return -1;
}

int
map_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
	return -1;
}

uint32_t
map_wire_size(const as_particle *p)
{
	// TODO
	return 0;
}

uint32_t
map_to_wire(const as_particle *p, uint8_t *wire)
{
	// TODO
	return 0;
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

	return (uint32_t)sizeof(map_mem) + size;
}

void
map_from_asval(const as_val *val, as_particle **pp)
{
	map_mem *p_map_mem = (map_mem *)*pp;

	as_serializer s;
	as_msgpack_init(&s);

	uint32_t size = as_serializer_serialize_presized(&s, val, p_map_mem->data);

	p_map_mem->type = AS_PARTICLE_TYPE_MAP;
	p_map_mem->sz = size;

	as_serializer_destroy(&s);
}

as_val *
map_to_asval(const as_particle *p)
{
	map_mem *p_map_mem = (map_mem *)p;

	as_buffer buf;
	as_buffer_init(&buf);

	buf.data = p_map_mem->data;
	buf.capacity = p_map_mem->sz;
	buf.size = p_map_mem->sz;

	as_serializer s;
	as_msgpack_init(&s);

	as_val *val;

	as_serializer_deserialize(&s, &buf, &val);
	as_serializer_destroy(&s);

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
	// TODO
	return -1;
}

int
map_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// TODO
	return -1;
}

int
map_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// TODO
	return -1;
}

uint32_t
map_flat_size(const as_particle *p)
{
	// TODO
	return 0;
}

uint32_t
map_to_flat(const as_particle *p, uint8_t *flat)
{
	// TODO
	return 0;
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
