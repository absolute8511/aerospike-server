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

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "fault.h"

#include "base/cfg.h"

// #define EXTRA_CHECKS 1


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
as_particle_get_base_size_null(uint8_t particle_type)
{
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int
as_particle_compare_from_wire_null(as_particle *p, void *data, uint32_t sz)
{
	return 0;
}

as_particle *
as_particle_from_wire_null(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	return NULL;
}

int
as_particle_to_wire_null(as_particle *p, void *data, uint32_t *sz)
{
	// TODO - what?
	if (sz) {
		*sz = 0;
	}

	return 0;
}

//------------------------------------------------
// Handle in-memory format.
//

as_particle *
as_particle_from_mem_null(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	return NULL;
}

int
as_particle_to_mem_null(as_particle *p, void *data, uint32_t *sz)
{
	// TODO - what?
	if (sz) {
		*sz = 0;
	}

	return 0;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

uint32_t
as_particle_flat_size_null(const as_particle *p)
{
	return 1;
}

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

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_int(as_particle *p)
{
	// Nothing to do - integer values live in the as_bin.
}

uint32_t
as_particle_get_base_size_int(uint8_t particle_type)
{
	// this is firing now that we're using stored procedures
	// commenting it out, but not sure if that's a good idea
	// cf_warning(AS_PARTICLE, "unexpected request for base size of integer");
	return 8;
}

//------------------------------------------------
// Handle "wire" format.
//

int
as_particle_compare_from_wire_int(as_particle *p, void *data, uint32_t sz)
{
	if (!p || !data)
		return (-1);

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

	as_particle_int_mem *pi = (as_particle_int_mem *) p;

	if (pi->i < i)
		return (-1);
	else if (pi->i > i)
		return (1);
	else
		return (0);
}

as_particle *
as_particle_from_wire_int(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	// convert the incoming buffer to a uint64_t

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
		cf_info(AS_PARTICLE, "as_particle_fromwire_int: null particle passed inf or integer. Error ");
		return (p);
	}
	as_particle_int_mem *pi = (as_particle_int_mem *)p;
	pi->i = i;
	return (p);
}

int
as_particle_to_wire_int(as_particle *p, void *data, uint32_t *sz)
{
	// attempt to get size
	if (!data) {
		*sz = 8;
		return(0);
	}

	if (*sz < 8) {
		*sz = 8;
		return(-1);
	}

	as_particle_int_mem *pi = (as_particle_int_mem *)p;
	*(uint64_t *) data = __cpu_to_be64(pi->i);
	cf_detail(AS_PARTICLE, "READING value %"PRIx64"", pi->i);

	*sz = 8;
	return(0);
}

//------------------------------------------------
// Handle in-memory format.
//

as_particle *
as_particle_from_mem_int(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	// TODO - should checks really be down at this level? See also to_mem calls...
	if (! p) {
		return NULL;
	}

	// TODO - properly!
	as_particle_from_wire_int(p, type, data, sz, data_in_memory);

	((as_particle_int_mem *)p)->i = __cpu_to_be64(((as_particle_int_mem *)p)->i);

	return p;
}

int
as_particle_to_mem_int(as_particle *p, void *data, uint32_t *sz)
{
	// TODO - properly!
	int result = as_particle_to_wire_int(p, data, sz);

	*(uint64_t *)data = __cpu_to_be64p((__u64 *)data);

	return result;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

uint32_t
as_particle_flat_size_int(const as_particle *p)
{
	return sizeof(as_particle_int_flat);
}

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
		return -1;
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
		return -1;
	}

	// Integer values live in an as_bin instead of a pointer. Also, flat
	// integers are host order, so no byte swap.
	*pp = (as_particle *)p_int_flat->i;

	return 0;
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

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_float(as_particle *p)
{
}

uint32_t
as_particle_get_base_size_float(uint8_t particle_type)
{
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int
as_particle_compare_from_wire_float(as_particle *p, void *data, uint32_t sz)
{
	return -1;
}

as_particle *
as_particle_from_wire_float(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	return NULL;
}

int
as_particle_to_wire_float(as_particle *p, void *data, uint32_t *sz)
{
	return -1;
}

//------------------------------------------------
// Handle in-memory format.
//

as_particle *
as_particle_from_mem_float(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	return NULL;
}

int
as_particle_to_mem_float(as_particle *p, void *data, uint32_t *sz)
{
	return -1;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

uint32_t
as_particle_flat_size_float(const as_particle *p)
{
	return 0;
}

int32_t
as_particle_size_from_flat_float(const uint8_t *flat, uint32_t flat_size)
{
	// Float values live in an as_bin instead of a pointer.
	return 0;
}

int
as_particle_cast_from_flat_float(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	return -1;
}

int
as_particle_from_flat_float(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	return -1;
}

uint32_t
as_particle_to_flat_float(const as_particle *p, uint8_t *flat)
{
	return as_particle_flat_size_float(p);
}


//==========================================================
// STRING particle.
//

typedef struct as_particle_string_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle_string_mem;

typedef struct as_particle_string_flat_s {
	uint8_t		type;
	uint32_t	size; // host order on device
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle_string_flat;

//------------------------------------------------
// Destructor, etc.
//

void
as_particle_destruct_string(as_particle *p)
{
	cf_free(p);
}

uint32_t
as_particle_get_base_size_string(uint8_t particle_type)
{
	return sizeof(as_particle_string_mem);
}

int
as_particle_get_p_string(as_particle *p, void **data, uint32_t *sz)
{
	as_particle_string_mem *ps = (as_particle_string_mem *)p;
	if (! data) {
		*sz = ps->sz;
		return 0;
	}
	*sz = ps->sz;
	*data = ps->data;

	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int
as_particle_compare_from_wire_string(as_particle *p, void *data, uint32_t sz)
{
	as_particle_string_mem *ps = (as_particle_string_mem *)p;
	if (!ps || !data)
		return -1;

	return memcmp(ps->data, data, (ps->sz > sz) ? ps->sz : sz);
}

as_particle *
as_particle_from_wire_string(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	as_particle_string_mem *ps = (as_particle_string_mem *)p;

	if (data_in_memory) {
		if (ps && (sz != ps->sz)) {
			if (sz > ps->sz) {
				cf_free(ps);
				ps = 0;
			}
			else {
				ps = cf_realloc(ps, sizeof(as_particle_string_mem) + sz);
			}
		}

		if (! ps) {
			ps = cf_malloc(sizeof(as_particle_string_mem) + sz);
		}
	}

	ps->type = AS_PARTICLE_TYPE_STRING;
	ps->sz = sz;

	memcpy(ps->data, data, sz);

	return((as_particle *)ps);
}

int
as_particle_to_wire_string(as_particle *p, void *data, uint32_t *sz)
{
	as_particle_string_mem *ps = (as_particle_string_mem *)p;
	if (! data) {
		*sz = ps->sz;
		return(0);
	}
	if (*sz < ps->sz) {
		*sz = ps->sz;
		return(-1);
	}

	*sz = ps->sz;
	memcpy(data, ps->data, ps->sz);

	return(0);
}

//------------------------------------------------
// Handle in-memory format.
//

as_particle *
as_particle_from_mem_string(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	// TODO - other way around?
	return as_particle_from_wire_string(p, type, data, sz, data_in_memory);
}

int
as_particle_to_mem_string(as_particle *p, void *data, uint32_t *sz)
{
	// TODO - other way around?
	return as_particle_to_wire_string(p, data, sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

uint32_t
as_particle_flat_size_string(const as_particle *p)
{
	return (uint32_t)(sizeof(as_particle_string_flat) + ((as_particle_string_mem *)p)->sz);
}

int32_t
as_particle_size_from_flat_string(const uint8_t *flat, uint32_t flat_size)
{
	as_particle_string_flat *p_string_flat = (as_particle_string_flat *)flat;
	// Assume type is correct, since we got here.

	// Sanity check length.
	if (p_string_flat->size != flat_size - sizeof(as_particle_string_flat)) {
		cf_warning(AS_PARTICLE, "unexpected flat string: flat size %u, len %u",
				flat_size, p_string_flat->size);
		return -1;
	}

	// Flat value is same as in-memory value.
	return (int32_t)(sizeof(as_particle_string_mem) + p_string_flat->size);
}

int
as_particle_cast_from_flat_string(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Sizing is only a sanity check.
	if (as_particle_size_from_flat_string(flat, flat_size) == -1) {
		return -1;
	}

	// We can do this only because the flat and in-memory formats are identical.
	*pp = (as_particle *)flat;

	return 0;
}

int
as_particle_from_flat_string(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	int32_t mem_size = as_particle_size_from_flat_string(flat, flat_size);

	if (mem_size == -1) {
		return -1;
	}

	as_particle_string_mem *p_string_mem = (as_particle_string_mem *)cf_malloc((size_t)mem_size);

	if (! p_string_mem) {
		cf_warning(AS_PARTICLE, "failed malloc");
		return -1;
	}

	const as_particle_string_flat *p_string_flat = (const as_particle_string_flat *)flat;

	p_string_mem->type = p_string_flat->type;
	p_string_mem->sz = p_string_flat->size;
	memcpy(p_string_mem->data, p_string_flat->data, p_string_mem->sz);

	*pp = (as_particle *)p_string_mem;

	return 0;
}

uint32_t
as_particle_to_flat_string(const as_particle *p, uint8_t *flat)
{
	as_particle_string_mem *p_string_mem = (as_particle_string_mem *)p;
	as_particle_string_flat *p_string_flat = (as_particle_string_flat *)flat;

	// Already wrote the type.
	p_string_flat->size = p_string_mem->sz;
	memcpy(p_string_flat->data, p_string_mem->data, p_string_flat->size);

	return as_particle_flat_size_string(p);
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
as_particle_get_base_size_blob(uint8_t particle_type)
{
	return sizeof(as_particle_blob_mem);
}

int
as_particle_get_p_blob(as_particle *p, void **data, uint32_t *sz)
{
	as_particle_blob_mem *pb = (as_particle_blob_mem *)p;
	if (!data) {
		*sz = pb->sz;
		return 0;
	}
	*sz = pb->sz;
	*data = pb->data;

	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int
as_particle_compare_from_wire_blob(as_particle *p, void *data, uint32_t sz)
{
	as_particle_blob_mem *pb = (as_particle_blob_mem *)p;
	if (!pb || !data || (pb->sz != sz))
		return -1;

	return memcmp(pb->data, data, sz);
}

as_particle *
as_particle_from_wire_blob(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	as_particle_blob_mem *pb = (as_particle_blob_mem *)p;
	if (data_in_memory) {
		if (pb && (sz != pb->sz)) {
			if (sz > pb->sz) {
				cf_free(pb);
				pb = 0;
			}
			else {
				pb = cf_realloc(pb, sizeof(as_particle_blob_mem) + sz);
			}
		}

		if (! pb) {
			pb = cf_malloc(sizeof(as_particle_blob_mem) + sz);
		}
	}

	pb->type = type;
	pb->sz = sz;

	memcpy(pb->data, data, sz);

	return((as_particle *)pb);
}

int
as_particle_to_wire_blob(as_particle *p, void *data, uint32_t *sz)
{
	as_particle_blob_mem *pb = (as_particle_blob_mem *)p;
	if (!data) {
		*sz = pb->sz;
		return(0);
	}
	if (*sz < pb->sz) {
		*sz = pb->sz;
		return(-1);
	}

	*sz = pb->sz;
	memcpy(data, pb->data, pb->sz);

	return(0);
}

//------------------------------------------------
// Handle in-memory format.
//

as_particle *
as_particle_from_mem_blob(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	// TODO - other way around?
	return as_particle_from_wire_blob(p, type, data, sz, data_in_memory);
}

int
as_particle_to_mem_blob(as_particle *p, void *data, uint32_t *sz)
{
	// TODO - other way around?
	return as_particle_to_wire_blob(p, data, sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

uint32_t
as_particle_flat_size_blob(const as_particle *p)
{
	return (uint32_t)(sizeof(as_particle_blob_flat) + ((as_particle_blob_mem *)p)->sz);
}

int32_t
as_particle_size_from_flat_blob(const uint8_t *flat, uint32_t flat_size)
{
	as_particle_blob_flat *p_blob_flat = (as_particle_blob_flat *)flat;
	// Assume type is correct, since we got here.

	// Sanity check length.
	if (p_blob_flat->size != flat_size - sizeof(as_particle_blob_flat)) {
		cf_warning(AS_PARTICLE, "unexpected flat blob: flat size %u, len %u",
				flat_size, p_blob_flat->size);
		return -1;
	}

	// Flat value is same as in-memory value.
	return (int32_t)(sizeof(as_particle_blob_mem) + p_blob_flat->size);
}

int
as_particle_cast_from_flat_blob(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Sizing is only a sanity check.
	if (as_particle_size_from_flat_blob(flat, flat_size) == -1) {
		return -1;
	}

	// We can do this only because the flat and in-memory formats are identical.
	*pp = (as_particle *)flat;

	return 0;
}

int
as_particle_from_flat_blob(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	int32_t mem_size = as_particle_size_from_flat_blob(flat, flat_size);

	if (mem_size == -1) {
		return -1;
	}

	as_particle_blob_mem *p_blob_mem = (as_particle_blob_mem *)cf_malloc((size_t)mem_size);

	if (! p_blob_mem) {
		cf_warning(AS_PARTICLE, "failed malloc");
		return -1;
	}

	const as_particle_blob_flat *p_blob_flat = (const as_particle_blob_flat *)flat;

	p_blob_mem->type = p_blob_flat->type;
	p_blob_mem->sz = p_blob_flat->size;
	memcpy(p_blob_mem->data, p_blob_flat->data, p_blob_mem->sz);

	*pp = (as_particle *)p_blob_mem;

	return 0;
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
// Particle function tables.
//

//------------------------------------------------
// Destructor, etc.
//

typedef void (*as_particle_destructor_fn) (as_particle *p);

as_particle_destructor_fn g_particle_destructor_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_destruct_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_destruct_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_destruct_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_destruct_string,
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

typedef uint32_t (*as_particle_get_base_size_fn) (uint8_t particle_type);

as_particle_get_base_size_fn g_particle_get_base_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_get_base_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_get_base_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_get_base_size_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_get_base_size_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_get_base_size_int,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_get_base_size_blob,
};

typedef int (*as_particle_getter_p) (as_particle *p, void **data, uint32_t *sz);

as_particle_getter_p g_particle_getter_p_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= 0,
	[AS_PARTICLE_TYPE_INTEGER]			= 0,
	[AS_PARTICLE_TYPE_FLOAT]			= 0,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_get_p_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= 0,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_get_p_blob,
};

//------------------------------------------------
// Handle "wire" format.
//

typedef int (*as_particle_compare_from_wire_fn) (as_particle *p, void *data, uint32_t sz);

as_particle_compare_from_wire_fn g_particle_compare_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_compare_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_compare_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_compare_from_wire_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_compare_from_wire_string,
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

typedef as_particle * (*as_particle_from_wire_fn) (as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory);

as_particle_from_wire_fn g_particle_from_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_from_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_from_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_from_wire_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_from_wire_string,
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

typedef int (*as_particle_to_wire_fn) (as_particle *p, void *data, uint32_t *sz);

as_particle_to_wire_fn g_particle_to_wire_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_to_wire_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_to_wire_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_to_wire_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_to_wire_string,
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

typedef as_particle * (*as_particle_from_mem_fn) (as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory);

as_particle_from_mem_fn g_particle_from_mem_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_from_mem_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_from_mem_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_from_mem_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_from_mem_string,
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

typedef int (*as_particle_to_mem_fn) (as_particle *p, void *data, uint32_t *sz);

as_particle_to_mem_fn g_particle_to_mem_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_to_mem_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_to_mem_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_to_mem_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_to_mem_string,
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

typedef uint32_t (*as_particle_flat_size_fn) (const as_particle *p);

as_particle_flat_size_fn g_particle_flat_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_flat_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_flat_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_flat_size_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_flat_size_string,
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

typedef int32_t (*as_particle_size_from_flat_fn) (const uint8_t *flat, uint32_t flat_size);

as_particle_size_from_flat_fn g_particle_size_from_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_size_from_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_size_from_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_size_from_flat_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_size_from_flat_string,
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
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_cast_from_flat_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_cast_from_flat_string,
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
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_from_flat_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_from_flat_string,
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

typedef uint32_t (*as_particle_to_flat_fn) (const as_particle *p, uint8_t *flat);

as_particle_to_flat_fn g_particle_to_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_to_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_to_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_to_flat_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_to_flat_string,
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

uint32_t
as_particle_memory_size(uint8_t type, uint32_t value_size)
{
	switch (type) {
		case AS_PARTICLE_TYPE_NULL:
		case AS_PARTICLE_TYPE_INTEGER:
			return 0;
		default:
			return as_particle_get_base_size(type) + value_size;
	}
}

uint32_t
as_particle_flat_size(uint8_t type, uint32_t value_size)
{
	switch (type) {
		case AS_PARTICLE_TYPE_NULL:
			return 0;
		case AS_PARTICLE_TYPE_INTEGER:
			return (uint32_t)sizeof(as_particle_int_flat);
		default:
			return as_particle_get_base_size(type) + value_size;
	}
}

uint32_t
as_particle_get_base_size(uint8_t type)
{
	return g_particle_get_base_size_table[type](type);
}

as_particle_type
as_particle_type_convert(as_particle_type type)
{
	if (type == AS_PARTICLE_TYPE_HIDDEN_MAP) {
		return AS_PARTICLE_TYPE_MAP;
	} else if (type == AS_PARTICLE_TYPE_HIDDEN_LIST) {
		return AS_PARTICLE_TYPE_LIST;
	}
	return type;
}

as_particle_type
as_particle_type_convert_to_hidden(as_particle_type type)
{
	if (type == AS_PARTICLE_TYPE_MAP) {
		return AS_PARTICLE_TYPE_HIDDEN_MAP;
	} else if (type == AS_PARTICLE_TYPE_LIST) {
		return AS_PARTICLE_TYPE_HIDDEN_LIST;
	}
	return type;
}

bool
as_particle_type_hidden(as_particle_type type)
{
	if ( (type == AS_PARTICLE_TYPE_HIDDEN_MAP)
			|| (type == AS_PARTICLE_TYPE_HIDDEN_LIST)) {
		return true;
	} else {
		return false;
	}
}


//==========================================================
// as_bin particle functions - maybe should be elsewhere.
//

//------------------------------------------------
// Operations (arithmetic, append, prepend).
// TODO - should be 'from_wire' functions!
//

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

/*
 * Utility function for memcache compatibility, where we may
 * need to change a blob to an int...
 * Given a buffer of data, determine if it can be represented
 * as a signed 64 bit integer. If it can, return true and set
 * the output value. If it cannot, return false.
 */
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
			as_particle_string_mem *ps = (as_particle_string_mem *)(b->particle);
			if (data_in_memory) {
				as_particle_string_mem *new_ps = (as_particle_string_mem *)cf_malloc(sizeof(as_particle_string_mem) + data_len + ps->sz);

				// copy header, then data
				memcpy(new_ps, ps, sizeof(as_particle_string_mem));
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
					as_particle_string_mem *ps = (as_particle_string_mem *)cf_malloc(sizeof(as_particle_string_mem) +  new_data_len);
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

//------------------------------------------------
// Miscellaneous sizing.
//

// TODO - WTF?
uint32_t
as_particle_get_size_in_memory(as_bin *b, as_particle *particle)
{
	uint8_t type = as_bin_get_particle_type(b);
	switch(type) {
		case AS_PARTICLE_TYPE_INTEGER:
		case AS_PARTICLE_TYPE_NULL:
		case AS_PARTICLE_TYPE_FLOAT:
			return 0;
		case AS_PARTICLE_TYPE_STRING:
			return sizeof(as_particle_string_mem) + ((as_particle_string_mem *)particle)->sz;
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_JAVA_BLOB:
		case AS_PARTICLE_TYPE_CSHARP_BLOB:
		case AS_PARTICLE_TYPE_PYTHON_BLOB:
		case AS_PARTICLE_TYPE_RUBY_BLOB:
		case AS_PARTICLE_TYPE_PHP_BLOB:
		case AS_PARTICLE_TYPE_ERLANG_BLOB:
			return sizeof(as_particle_blob_mem) + ((as_particle_blob_mem *)particle)->sz;
		default:
			break;
	}

	return 0;
}

// TODO - WTF?
uint32_t
as_bin_get_particle_size(as_bin *b)
{
	if (! as_bin_inuse(b))
		return (0);

	as_particle *p = as_bin_get_particle(b);

	uint8_t type = as_bin_get_particle_type(b);
	uint32_t sz = 0;
	(void)g_particle_to_wire_table[type](p, 0, &sz);

	return(sz);
}

//------------------------------------------------
// Destructor, etc.
//

// TODO - rename and clean this up!
void
as_particle_destroy(as_bin *b, bool data_in_memory)
{
	if (as_bin_is_integer(b)) {
		b->particle = 0; // this is the same field as the integer in the union
	}
	else if (b->particle) {
		if (data_in_memory) {
			g_particle_destructor_table[as_bin_get_particle_type(b)](b->particle);
		}
		b->particle = 0;
	}
}

// TODO - rename and clean this up!
int
as_particle_p_get(as_bin *b, byte **buf, uint32_t *sz) {
	if (!b)
		return (-1);

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

	int rv = g_particle_getter_p_table[type](p, (void **)buf, sz);

	return(rv);
}

//------------------------------------------------
// Handle "wire" format.
//

// TODO - rename and clean this up!
// TODO - do we only need wire comparator?
int
as_particle_compare_fromwire(as_bin *b, as_particle_type type, byte *buf, uint32_t sz)
{
#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle compare: bad particle type %d, error", (int)type);
		return(-1);
	}
#endif

	// check if the types match
	if (as_bin_is_integer(b)) {
		if (type == AS_PARTICLE_TYPE_INTEGER)
			return (g_particle_compare_from_wire_table[type](&b->iparticle, buf, sz));
		else
			return (-1);
	}
	else if (b->particle && type == as_bin_get_particle_type(b))
		return (g_particle_compare_from_wire_table[type](b->particle, buf, sz));
	else
		return (-1);
}

// TODO - rename and clean this up!
as_particle *
as_particle_fromwire(as_bin *b, as_particle_type type, byte *buf, uint32_t sz, uint8_t *stack_particle, bool data_in_memory)
{

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle fromwire: bad particle type %d, error", (int)type);
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
				return (g_particle_from_wire_table[type](&b->iparticle, type, buf, sz, data_in_memory));
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
			retval = g_particle_from_wire_table[type](&b->iparticle, type, buf, sz, data_in_memory);
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
			b->particle = g_particle_from_wire_table[type](b->particle, type, buf, sz, data_in_memory);
			retval = b->particle;
			break;
	}

	return(retval);
}

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

// TODO - rename and clean this up!
int
as_particle_towire(as_bin *b, byte *buf, uint32_t *sz) {
	return _as_particle_towire(b, buf, sz, 0);
}

//------------------------------------------------
// Handle in-memory format.
//

// TODO - rename and clean this up!
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

// TODO - rename and clean this up!
int
as_particle_tomem(as_bin *b, byte *buf, uint32_t *sz) {
	return _as_particle_tomem(b, buf, sz, 0);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

// TODO - clean this up!
int
as_bin_particle_flat_size(as_bin *b, size_t *flat_size)
{
	if (!b)
		return (-1);

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

	if (type == AS_PARTICLE_TYPE_NULL)
		return (-2);

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle get flat size: bad particle type %d, error", (int)type);
		return(-3);
	}
#endif

	uint32_t size = g_particle_flat_size_table[type](p);
	if (size == 0)	return(-4);
	*flat_size = size;
	return(0);
}

int
as_bin_particle_size_from_flat(as_bin *b, uint32_t *p_flat_size)
{
	// TODO
	return 0;
}

int
as_bin_particle_cast_from_flat(as_bin *b, uint8_t *flat, uint32_t flat_size)
{
	if (as_bin_inuse(b)) {
		cf_warning(AS_PARTICLE, "cast from flat into used bin");
		return -1;
	}

	as_particle_type new_type = (as_particle_type)*flat;

	// Cast the new particle into the bin.
	int result = g_particle_cast_from_flat_table[new_type](flat, flat_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, new_type);
	}
	else {
		as_bin_set_empty(b);
	}

	return result;
}

int
as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, uint32_t flat_size)
{
	as_particle_type old_type = (as_particle_type)as_bin_get_particle_type(b);
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
as_bin_particle_to_flat(const as_bin *b, uint8_t *flat)
{
	if (! as_bin_inuse(b)) {
		cf_crash(AS_PARTICLE, "flattening unused bin");
	}

	as_particle_type type = (as_particle_type)as_bin_get_particle_type(b);

	*flat = (uint8_t)type;

	return g_particle_to_flat_table[type](b->particle, flat);
}
