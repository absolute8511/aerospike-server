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

// NULL particle type
as_particle *as_particle_set_null(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	cf_warning(AS_PARTICLE, "trying to set a particle to null via the setter table");
	return (0);
}

int as_particle_get_null(as_particle *p, void *data, uint32_t *sz)
{
	// attempt to get size
	if (sz) *sz = 0;
	return(0);
}

int as_particle_compare_null(as_particle *p, void *data, uint32_t sz)
{
	return(0); // two null objects are always equal
}

// arguably null is very flat
uint32_t as_particle_get_flat_null(as_particle *p)
{
	return(0);
}

void as_particle_destruct_null(as_particle *p)
{
	cf_free(p);
}

uint32_t as_particle_get_base_size_null(uint8_t particle_type)
{
	return(0);
}

//
// INT particle type
//

typedef struct as_particle_int_s {
	uint8_t 		do_not_use;	// already know it's an int type
	uint64_t		i;
} __attribute__ ((__packed__)) as_particle_int;

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

as_particle *as_particle_set_int(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
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
		cf_info(AS_PARTICLE, "as_particle_set_int: null particle passed inf or integer. Error ");
		return (p);
	}
	as_particle_int *pi = (as_particle_int *)p;
	pi->i = i;
	return (p);
}

int as_particle_get_int(as_particle *p, void *data, uint32_t *sz)
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

	as_particle_int *pi = (as_particle_int *)p;
	*(uint64_t *) data = __cpu_to_be64(pi->i);
	cf_detail(AS_PARTICLE, "READING value %"PRIx64"", pi->i);

	*sz = 8;
	return(0);
}

int as_particle_compare_int(as_particle *p, void *data, uint32_t sz)
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

	as_particle_int *pi = (as_particle_int *) p;

	if (pi->i < i)
		return (-1);
	else if (pi->i > i)
		return (1);
	else
		return (0);
}

uint32_t as_particle_get_flat_int(as_particle *p)
{
	return (sizeof(as_particle_int_on_device));
}

uint32_t as_particle_get_base_size_int(uint8_t particle_type)
{
	// this is firing now that we're using stored procedures
	// commenting it out, but not sure if that's a good idea
	// cf_warning(AS_PARTICLE, "unexpected request for base size of integer");
	return(8);
}

void as_particle_destruct_int(as_particle *p)
{
// This is a no op. integers are never allocated and are always part of the bin structure
//	cf_free(p);
}

static
int as_particle_add_int(as_particle *p, void *data, uint32_t sz, bool mc_compliant)
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

	as_particle_int *pi = (as_particle_int *)p;

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

//
// BIGNUM particle type
//

as_particle *as_particle_set_float(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	cf_info(AS_PARTICLE, "particle set float stub");
	return(NULL);
}

int as_particle_get_float(as_particle *p, void *data, uint32_t *sz)
{
	cf_info(AS_PARTICLE, "particle get float stub");
	return(-1);
}

int as_particle_compare_float(as_particle *p, void *data, uint32_t sz)
{
	cf_info(AS_PARTICLE, "particle compare float stub");
	return(-1);
}

uint32_t as_particle_get_flat_float(as_particle *p)
{
	cf_info(AS_PARTICLE, "particle get flat float stub");
	return(0);
}

uint32_t as_particle_get_base_size_float(uint8_t particle_type)
{
	cf_info(AS_PARTICLE, "particle get base size float stub");
	return(0);
}

void as_particle_destruct_float(as_particle *p)
{
	cf_info(AS_PARTICLE, "particle destruct float stub");
}

//
// STRING particle type
//

typedef struct as_particle_string_s {
	uint8_t type;			// must start with type!
	uint32_t		sz;
	uint8_t			data[];
} __attribute__ ((__packed__)) as_particle_string;


as_particle *as_particle_set_string(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	as_particle_string *ps = (as_particle_string *)p;

	if (data_in_memory) {
		if (ps && (sz != ps->sz)) {
			if (sz > ps->sz) {
				cf_free(ps);
				ps = 0;
			}
			else {
				ps = cf_realloc(ps, sizeof(as_particle_string) + sz);
			}
		}

		if (! ps) {
			ps = cf_malloc(sizeof(as_particle_string) + sz);
		}
	}

	ps->type = AS_PARTICLE_TYPE_STRING;
	ps->sz = sz;

	memcpy(ps->data, data, sz);

	return((as_particle *)ps);
}

int as_particle_get_string(as_particle *p, void *data, uint32_t *sz)
{
	as_particle_string *ps = (as_particle_string *)p;
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

int as_particle_get_p_string(as_particle *p, void **data, uint32_t *sz)
{
	as_particle_string *ps = (as_particle_string *)p;
	if (! data) {
		*sz = ps->sz;
		return(0);
	}
	*sz = ps->sz;
	*data = ps->data;

	return(0);
}


int as_particle_compare_string(as_particle *p, void *data, uint32_t sz)
{
	as_particle_string *ps = (as_particle_string *)p;
	if (!ps || !data)
		return (-1);

	return (memcmp(ps->data, data, (ps->sz > sz) ? ps->sz : sz));
}

uint32_t as_particle_get_flat_string(as_particle *p)
{
	return ( sizeof(as_particle_string) + ((as_particle_string *)p)->sz );
}

uint32_t as_particle_get_base_size_string(uint8_t particle_type)
{
	return(sizeof(as_particle_string));
}

void as_particle_destruct_string(as_particle *p)
{
	cf_free(p);
}


//
// BLOB particle type
//

typedef struct as_particle_blob_s {
	uint8_t			 type;			// must start with type!
	uint32_t		sz;
	uint8_t			data[];
} __attribute__ ((__packed__)) as_particle_blob;


as_particle *as_particle_set_blob(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	as_particle_blob *pb = (as_particle_blob *)p;
	if (data_in_memory) {
		if (pb && (sz != pb->sz)) {
			if (sz > pb->sz) {
				cf_free(pb);
				pb = 0;
			}
			else {
				pb = cf_realloc(pb, sizeof(as_particle_blob) + sz);
			}
		}

		if (! pb) {
			pb = cf_malloc(sizeof(as_particle_blob) + sz);
		}
	}

	pb->type = type;
	pb->sz = sz;

	memcpy(pb->data, data, sz);

	return((as_particle *)pb);
}

int as_particle_get_blob(as_particle *p, void *data, uint32_t *sz)
{
	as_particle_blob *pb = (as_particle_blob *)p;
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

int as_particle_get_p_blob(as_particle *p, void **data, uint32_t *sz)
{
	as_particle_blob *pb = (as_particle_blob *)p;
	if (!data) {
		*sz = pb->sz;
		return(0);
	}
	*sz = pb->sz;
	*data = pb->data;

	return(0);
}

int as_particle_compare_blob(as_particle *p, void *data, uint32_t sz)
{
	as_particle_blob *pb = (as_particle_blob *)p;
	if (!pb || !data || (pb->sz != sz))
		return (-1);

	return(memcmp(pb->data, data, sz));
}

uint32_t as_particle_get_flat_blob(as_particle *p)
{
	return ( sizeof(as_particle_blob) + ((as_particle_blob *)p)->sz );
}

uint32_t as_particle_get_base_size_blob(uint8_t particle_type)
{
	return(sizeof(as_particle_blob));
}

void as_particle_destruct_blob(as_particle *p)
{
	cf_free(p);
}



//
// TIMESTAMP particle type
//

as_particle *as_particle_set_timestamp(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	cf_info(AS_PARTICLE, "particle set timestamp stub");
	return(NULL);
}

int as_particle_get_timestamp(as_particle *p, void *data, uint32_t *sz)
{
	cf_info(AS_PARTICLE, "particle get timestamp stub");
	return(-1);
}

int as_particle_compare_timestamp(as_particle *p, void *data, uint32_t sz)
{
	cf_info(AS_PARTICLE, "particle compare timestamp stub");
	return(-1);
}

uint32_t as_particle_get_flat_timestamp(as_particle *p)
{
	cf_info(AS_PARTICLE, "particle get flat timestamp stub");
	return(0);
}

uint32_t as_particle_get_base_size_timestamp(uint8_t particle_type)
{
	cf_info(AS_PARTICLE, "particle get base size timestamp stub");
	return(0);
}

void as_particle_destruct_timestamp(as_particle *p)
{
	cf_info(AS_PARTICLE, "particle destruct timestamp stub");
}

//
// DIGEST particle type
//

as_particle *as_particle_set_digest(as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory)
{
	cf_info(AS_PARTICLE, "particle set digest stub");
	return(NULL);
}

int as_particle_get_digest(as_particle *p, void *data, uint32_t *sz)
{
	cf_info(AS_PARTICLE, "particle get digest stub");
	return(-1);
}

int as_particle_compare_digest(as_particle *p, void *data, uint32_t sz)
{
	cf_info(AS_PARTICLE, "particle compare digest stub");
	return(-1);
}

uint32_t as_particle_get_flat_digest(as_particle *p)
{
	cf_info(AS_PARTICLE, "particle get flat digest stub");
	return( 0 );
}

uint32_t as_particle_get_base_size_digest(uint8_t particle_type)
{
	cf_info(AS_PARTICLE, "particle get base size digest stub");
	return(0);
}

void as_particle_destruct_digest(as_particle *p)
{
	cf_info(AS_PARTICLE, "particle destruct digest stub");
}


//
// Definitions
//
// NB - using the MAX to set the size of the array helps the compiler make sure
// that the guard in the define is the right size and all

typedef as_particle * (*as_particle_setter) (as_particle *p, as_particle_type type, void *data, uint32_t sz, bool data_in_memory);

as_particle_setter g_particle_setter_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_set_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_set_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_set_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_set_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_set_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_set_timestamp,
	[AS_PARTICLE_TYPE_DIGEST]			= as_particle_set_digest,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_set_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_set_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_set_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_set_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_set_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_set_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_set_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_set_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_set_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_set_blob,
};

typedef int (*as_particle_getter) (as_particle *p, void *data, uint32_t *sz);

as_particle_getter g_particle_getter_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_get_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_get_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_get_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_get_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_get_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_get_timestamp,
	[AS_PARTICLE_TYPE_DIGEST]			= as_particle_get_digest,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_get_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_get_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_get_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_get_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_get_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_get_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_get_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_get_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_get_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_get_blob,
};


typedef int (*as_particle_getter_p) (as_particle *p, void **data, uint32_t *sz);
as_particle_getter_p g_particle_getter_p_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= 0,
	[AS_PARTICLE_TYPE_INTEGER]			= 0,
	[AS_PARTICLE_TYPE_FLOAT]			= 0,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_get_p_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_get_p_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= 0,
	[AS_PARTICLE_TYPE_DIGEST]			= 0,
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

typedef void (*as_particle_destructor) (as_particle *p);

as_particle_destructor g_particle_destructor_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_destruct_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_destruct_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_destruct_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_destruct_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_destruct_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_destruct_timestamp,
	[AS_PARTICLE_TYPE_DIGEST]			= as_particle_destruct_digest,
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

typedef uint32_t (*as_particle_get_flat) (as_particle *p);

as_particle_get_flat g_particle_get_flat_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_get_flat_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_get_flat_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_get_flat_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_get_flat_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_get_flat_timestamp,
	[AS_PARTICLE_TYPE_DIGEST]			= as_particle_get_flat_digest,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_get_flat_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_get_flat_blob,
};

typedef int (*as_particle_compare) (as_particle *p, void *data, uint32_t sz);

as_particle_compare g_particle_compare_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_compare_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_compare_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_compare_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_compare_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_compare_timestamp,
	[AS_PARTICLE_TYPE_DIGEST]			= as_particle_compare_digest,
	[AS_PARTICLE_TYPE_JAVA_BLOB]		= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_CSHARP_BLOB]		= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_PYTHON_BLOB]		= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_RUBY_BLOB]		= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_PHP_BLOB]			= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_ERLANG_BLOB]		= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_MAP]				= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_LIST]				= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_HIDDEN_LIST]		= as_particle_compare_blob,
	[AS_PARTICLE_TYPE_HIDDEN_MAP]		= as_particle_compare_blob,
};

typedef uint32_t (*as_particle_get_base) (uint8_t particle_type);

as_particle_get_base g_particle_get_base_size_table[AS_PARTICLE_TYPE_MAX] = {
	[AS_PARTICLE_TYPE_NULL]				= as_particle_get_base_size_null,
	[AS_PARTICLE_TYPE_INTEGER]			= as_particle_get_base_size_int,
	[AS_PARTICLE_TYPE_FLOAT]			= as_particle_get_base_size_float,
	[AS_PARTICLE_TYPE_STRING]			= as_particle_get_base_size_string,
	[AS_PARTICLE_TYPE_BLOB]				= as_particle_get_base_size_blob,
	[AS_PARTICLE_TYPE_TIMESTAMP]		= as_particle_get_base_size_timestamp,
	[AS_PARTICLE_TYPE_DIGEST]			= as_particle_get_base_size_digest,
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


/* as_particle_set
 * Set the contents of a particle, which safely destroys the old particle
 */
as_particle *
as_particle_frombuf(as_bin *b, as_particle_type type, byte *buf, uint32_t sz, uint8_t *stack_particle, bool data_in_memory)
{

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle set: bad particle type %d, error", (int)type);
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
				return (g_particle_setter_table[type](&b->iparticle, type, buf, sz, data_in_memory));
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
			retval = g_particle_setter_table[type](&b->iparticle, type, buf, sz, data_in_memory);
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
			b->particle = g_particle_setter_table[type](b->particle, type, buf, sz, data_in_memory);
			retval = b->particle;
			break;
	}

	return(retval);
}

/* as_particle_compare_frombuf
 * Compares two particles and returns equal or not
 */
int
as_particle_compare_frombuf(as_bin *b, as_particle_type type, byte *buf, uint32_t sz)
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
			return (g_particle_compare_table[type](&b->iparticle, buf, sz));
		else
			return (-1);
	}
	else if (b->particle && type == as_bin_get_particle_type(b))
		return (g_particle_compare_table[type](b->particle, buf, sz));
	else
		return (-1);
}

/*
** as_particle_add
** takes a local buffer, and a wire-format particle, and does the 'add' operation
**
** first test: just integers and integers let the strings and doubles go for a minute
** doing the entire sparse matrix of all possible additions,, and it's very sparse,
** so let it ride for a minute.
**
** 0 is success as usual
*/


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


static
void particle_append_prepend_data_impl(as_bin *b, as_particle_type existing_type, as_particle_type incoming_type,
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
			as_particle_string *ps = (as_particle_string *)(b->particle);
			if (data_in_memory) {
				as_particle_string *new_ps = (as_particle_string *)cf_malloc(sizeof(as_particle_string) + data_len + ps->sz);

				// copy header, then data
				memcpy(new_ps, ps, sizeof(as_particle_string));
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
			as_particle_blob *pb = (as_particle_blob *)(b->particle);
			if (data_in_memory) {
				as_particle_blob *new_pb = (as_particle_blob *)cf_malloc(sizeof(as_particle_blob) + data_len + pb->sz);

				// copy header, then data
				memcpy(new_pb, pb, sizeof(as_particle_blob));
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
			as_particle_int *pi = (as_particle_int *)&b->iparticle;

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
					as_particle_string *ps = (as_particle_string *)cf_malloc(sizeof(as_particle_string) +  new_data_len);
					ps->sz = new_data_len;
					memcpy(ps->data, buf, new_data_len);
					ps->type = AS_PARTICLE_TYPE_STRING;
					b->particle = (as_particle *)ps;
					*new_type = AS_PARTICLE_TYPE_STRING;
				} else { // it's a blob.
					uint32_t new_data_len = data_len + int_len;
					as_particle_blob *pb = (as_particle_blob *)cf_malloc(sizeof(as_particle_blob) + new_data_len);
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


uint32_t
as_particle_get_size_in_memory(as_bin *b, as_particle *particle)
{
	uint8_t type = as_bin_get_particle_type(b);
	switch(type) {
		case AS_PARTICLE_TYPE_INTEGER:
		case AS_PARTICLE_TYPE_NULL:
		case AS_PARTICLE_TYPE_FLOAT:
		case AS_PARTICLE_TYPE_DIGEST:
			return 0;
		case AS_PARTICLE_TYPE_STRING:
			return sizeof(as_particle_string) + ((as_particle_string *)particle)->sz;
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_JAVA_BLOB:
		case AS_PARTICLE_TYPE_CSHARP_BLOB:
		case AS_PARTICLE_TYPE_PYTHON_BLOB:
		case AS_PARTICLE_TYPE_RUBY_BLOB:
		case AS_PARTICLE_TYPE_PHP_BLOB:
		case AS_PARTICLE_TYPE_ERLANG_BLOB:
			return sizeof(as_particle_blob) + ((as_particle_blob *)particle)->sz;
		default:
			break;
	}

	return 0;
}


uint32_t
as_particle_get_base_size(uint8_t type)
{
	return (g_particle_get_base_size_table[type](type));
}

/*
** as_bin_get_particle_size - uses the generic 'tobuf', but with a null parameter
** to avoid doing the copy. Useful for deciding if your namespace is too big
*/

uint32_t
as_bin_get_particle_size(as_bin *b)
{
	if (! as_bin_inuse(b))
		return (0);

	as_particle *p = as_bin_get_particle(b);

	uint8_t type = as_bin_get_particle_type(b);
	uint32_t sz = 0;
	(void)g_particle_getter_table[type](p, 0, &sz);

	return(sz);
}

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
			return (uint32_t)sizeof(as_particle_int_on_device);
		default:
			return as_particle_get_base_size(type) + value_size;
	}
}

/*
** as_particle_get_flat_size
** In most cases, the particle is 'flat', thus, it has no extra pointers
** in those cases, return the exact number of bytes this particle has
** be aware byteswapping and such mean this data can only be used on this machine!
*/

int
as_particle_get_flat_size(as_bin *b, size_t *flat_size)
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
		cf_info(AS_PARTICLE, "particle set: bad particle type %d, error", (int)type);
		return(-3);
	}
#endif

	uint32_t size = g_particle_get_flat_table[type](p);
	if (size == 0)	return(-4);
	*flat_size = size;
	return(0);
}

/*
** as_particle_tobuf
** reduces the particle to a platform-neutral, serial entity, through a buffer
** copy
*/
// NOTE: tojson is IGNORED
int _as_particle_tobuf(as_bin *b, byte *buf, uint32_t *sz, bool tojson) {
	if (!b)
		return (-1);

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

#ifdef EXTRA_CHECKS
	// check the incoming type
	if (type < AS_PARTICLE_TYPE_NULL || type >= AS_PARTICLE_TYPE_MAX) {
		cf_info(AS_PARTICLE, "particle set: bad particle type %d, error", (int)type);
		return(-1);
	}
#endif

	int rv = g_particle_getter_table[type](p, buf, sz);

	return(rv);

}
int as_particle_tobuf(as_bin *b, byte *buf, uint32_t *sz) {
	return _as_particle_tobuf(b, buf, sz, 0);
}

/*
** as_particle_pointer
** get the pointer being held by the particle
** no ref count. don't do anything to the memory
*/
int as_particle_p_get(as_bin *b, byte **buf, uint32_t *sz) {
	if (!b)
		return (-1);

	as_particle *p = as_bin_get_particle(b);
	uint8_t type = as_bin_get_particle_type(b);

	int rv = g_particle_getter_p_table[type](p, (void **)buf, sz);

	return(rv);
}

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
