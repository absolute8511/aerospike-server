/*
 * particle_geojson.c
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

#include "aerospike/as_geojson.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"
#include "base/proto.h"
#include "geospatial/geospatial.h"


//==========================================================
// GEOJSON particle interface - function declarations.
//

// Most GEOJSON particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

// Handle "wire" format.
int32_t geojson_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int geojson_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int geojson_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int geojson_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t geojson_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int geojson_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
uint32_t geojson_to_wire(const as_particle *p, uint8_t *wire);

// Handle as_val translation.
uint32_t geojson_size_from_asval(const as_val *val);
void geojson_from_asval(const as_val *val, as_particle **pp);
as_val *geojson_to_asval(const as_particle *p);
uint32_t geojson_asval_wire_size(const as_val *val);
uint32_t geojson_asval_to_wire(const as_val *val, uint8_t *wire);

// Handle msgpack translation.
uint32_t geojson_size_from_msgpack(const uint8_t *packed, uint32_t packed_size);
void geojson_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp);


//==========================================================
// GEOJSON particle interface - vtable.
//

const as_particle_vtable geojson_vtable = {
		blob_destruct,
		blob_size,

		geojson_concat_size_from_wire,
		geojson_append_from_wire,
		geojson_prepend_from_wire,
		geojson_incr_from_wire,
		geojson_size_from_wire,
		geojson_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		geojson_to_wire,

		geojson_size_from_asval,
		geojson_from_asval,
		geojson_to_asval,
		geojson_asval_wire_size,
		geojson_asval_to_wire,

		geojson_size_from_msgpack,
		geojson_from_msgpack,

		blob_size_from_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

// GEOJSON particle flag bit-fields.
#define GEOJSON_ISREGION	0x1

// The GEOJSON particle structs overlay the related BLOB structs.

typedef struct geojson_mem_s {
	uint8_t		type;	// IMPORTANT: overlay blob_mem!
	uint32_t	sz;		// IMPORTANT: overlay blob_mem!
	uint8_t		flags;
	uint16_t	ncells;
	uint8_t		data[];	// (ncells * uint64_t) + jsonstr
} __attribute__ ((__packed__)) geojson_mem;

typedef struct geojson_flat_s {
	uint8_t		type;	// IMPORTANT: overlay blob_flat!
	uint32_t	size;	// IMPORTANT: overlay blob_flat!
	uint8_t		flags;
	uint16_t	ncells;
	uint8_t		data[];	// (ncells * uint64_t) + jsonstr
} __attribute__ ((__packed__)) geojson_flat;


//==========================================================
// Forward declarations.
//

static char const *geojson_mem_jsonstr(geojson_mem *p_geojson_mem, size_t *p_jsonsz);
static inline uint32_t geojson_size(uint32_t n_cells, size_t string_size);


//==========================================================
// GEOJSON particle interface - function definitions.
//

// Most GEOJSON particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

//------------------------------------------------
// Handle "wire" format.
//

int32_t
geojson_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "invalid operation on geojson particle");
	return -1;
}

int32_t
geojson_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// NOTE - Unfortunately we would need to run the JSON parser and region
	// coverer to find out exactly how many cells we need to allocate for this
	// particle.
	//
	// For now we always allocate the maximum number of cells (MAX_REGION_CELLS)
	// for the in-memory particle.
	//
	// For now also ignore any incoming cells entirely.

	uint8_t const *incp = (uint8_t const *)wire_value + 1;
	uint16_t incells = cf_swap_from_be16(*(uint16_t const *)incp);
	size_t incellsz = incells * sizeof(uint64_t);
	size_t injsonsz = value_size - sizeof(uint8_t) - sizeof(uint16_t) - incellsz;

	return (int32_t)(sizeof(geojson_mem) + (MAX_REGION_CELLS * sizeof(uint64_t)) + injsonsz);
}

int
geojson_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	uint8_t const *incp = (uint8_t const *)wire_value + 1;
	uint16_t incells = cf_swap_from_be16(*(uint16_t const *)incp);
	size_t incellsz = incells * sizeof(uint64_t);
	char const *injsonptr = (char const *)incp + sizeof(uint16_t) + incellsz;
	size_t injsonsz = value_size - sizeof(uint8_t) - sizeof(uint16_t) - incellsz;

	// We ignore any incoming cells entirely.

	uint64_t cellid = 0;
	geo_region_t region = NULL;

	if (! geo_parse(NULL, injsonptr, injsonsz, &cellid, &region)) {
		cf_warning(AS_PARTICLE, "geo_parse failed");
		return -AS_PROTO_RESULT_FAIL_GEO_INVALID_GEOJSON;
	}

	if (cellid && region) {
		geo_region_destroy(region);
		cf_warning(AS_PARTICLE, "geo_parse found both point and region");
		return -AS_PROTO_RESULT_FAIL_GEO_INVALID_GEOJSON;
	}

	if (! cellid && ! region) {
		cf_warning(AS_PARTICLE, "geo_parse found neither point nor region");
		return -AS_PROTO_RESULT_FAIL_GEO_INVALID_GEOJSON;
	}

	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;

	p_geojson_mem->type = wire_type;

	// We'll come back and set the size at the end.
	uint64_t *p_outcells = (uint64_t *)p_geojson_mem->data;

	p_geojson_mem->flags = 0;

	if (cellid) {
		// POINT
		p_geojson_mem->flags &= ~GEOJSON_ISREGION;
		p_geojson_mem->ncells = 1;
		p_outcells[0] = cellid;
	}
	else {
		// REGION
		p_geojson_mem->flags |= GEOJSON_ISREGION;

		int numcells;

		if (! geo_region_cover(NULL, region, MAX_REGION_CELLS, p_outcells, NULL, NULL, &numcells)) {
			geo_region_destroy(region);
			cf_warning(AS_PARTICLE, "geo_region_cover failed");
			return -AS_PROTO_RESULT_FAIL_GEO_INVALID_GEOJSON;
		}

		p_geojson_mem->ncells = numcells;
	}

	if (region) {
		geo_region_destroy(region);
	}

	// Copy the JSON into place.
	char *p_outjson = (char *)&p_outcells[p_geojson_mem->ncells];

	memcpy(p_outjson, injsonptr, injsonsz);

	// Set the actual size; we will waste some space at the end of the allocated
	// particle.
	p_geojson_mem->sz = sizeof(uint8_t) + sizeof(uint16_t) + (p_geojson_mem->ncells * sizeof(uint64_t)) + injsonsz;

	return AS_PROTO_RESULT_OK;
}

uint32_t
geojson_to_wire(const as_particle *p, uint8_t *wire)
{
	// Use blob routine first.
	uint32_t sz = blob_to_wire(p, wire);

	// Swap ncells.
	uint16_t *p_ncells = (uint16_t *)(wire + sizeof(uint8_t));
	uint16_t ncells = *p_ncells;

	*p_ncells = cf_swap_to_be16(*p_ncells);
	++p_ncells;

	// Swap the cells.
	uint64_t *p_cell_begin = (uint64_t *)p_ncells;
	uint64_t *p_cell_end = p_cell_begin + ncells;

	for (uint64_t *p_cell = p_cell_begin; p_cell < p_cell_end; ++p_cell) {
		*p_cell = cf_swap_to_be64(*p_cell);
	}

	return sz;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
geojson_size_from_asval(const as_val *val)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jsz = as_geojson_len(pg);

	// Compute the size; we won't be writing any cellids ...
	return geojson_size(0, jsz);
}

void
geojson_from_asval(const as_val *val, as_particle **pp)
{
	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;

	as_geojson *pg = as_geojson_fromval(val);
	size_t jsz = as_geojson_len(pg);

	p_geojson_mem->type = AS_PARTICLE_TYPE_GEOJSON;
	p_geojson_mem->sz = geojson_size(0, jsz);
	p_geojson_mem->flags = 0;
	p_geojson_mem->ncells = 0;

	uint8_t *p8 = (uint8_t *)p_geojson_mem->data;
	memcpy(p8, as_geojson_get(pg), jsz);
}

as_val *
geojson_to_asval(const as_particle *p)
{
	geojson_mem *p_geojson_mem = (geojson_mem *)p;

	size_t jsonsz;
	char const *jsonptr = geojson_mem_jsonstr(p_geojson_mem, &jsonsz);
	char *buf = cf_malloc(jsonsz + 1);

	if (! buf) {
		return NULL;
	}

	memcpy(buf, jsonptr, jsonsz);
	buf[jsonsz] = '\0';

	return (as_val *)as_geojson_new_wlen(buf, jsonsz, true);
}

uint32_t
geojson_asval_wire_size(const as_val *val)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jsz = as_geojson_len(pg);

	// We won't be writing any cellids ...
	return geojson_size(0, jsz);
}

uint32_t
geojson_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_geojson *pg = as_geojson_fromval(val);
	size_t jsz = as_geojson_len(pg);

	uint8_t *p8 = wire;

	*p8++ = 0;						// flags

	uint16_t *p16 = (uint16_t *)p8;

	*p16++ = cf_swap_to_be16(0);	// no cells on output to client
	p8 = (uint8_t *)p16;
	memcpy(p8, as_geojson_get(pg), jsz);

	return geojson_size(0, jsz);
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
geojson_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	// Oversize by a few bytes doing the easy thing.
	size_t jsz = (size_t)packed_size;

	// Compute the size; we won't be writing any cellids ...
	return geojson_size(0, jsz);
}

void
geojson_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	geojson_mem *p_geojson_mem = (geojson_mem *)*pp;

	as_unpacker pk = {
			.buffer = packed,
			.offset = 0,
			.length = packed_size
	};

	int64_t blob_size = as_unpack_blob_size(&pk);
	const uint8_t *ptr = pk.buffer + pk.offset;

	// *ptr should be AS_BYTES_GEOJSON at this point.

	// Adjust for type (1 byte).
	ptr++;
	blob_size--;

	size_t jsz = (size_t)blob_size;

	p_geojson_mem->type = AS_PARTICLE_TYPE_GEOJSON;
	p_geojson_mem->sz = geojson_size(0, jsz);
	p_geojson_mem->flags = 0;
	p_geojson_mem->ncells = 0;

	uint8_t *p8 = (uint8_t *)p_geojson_mem->data;
	memcpy(p8, ptr, jsz);
}


//==========================================================
// as_bin particle functions specific to GEOJSON.
// TODO - will change once as_val family is implemented.
//

// TODO - will we ever need this?
size_t
as_bin_particle_geojson_cellids(as_bin *b, uint64_t **ppcells)
{
	geojson_mem *gp = (geojson_mem *)b->particle;

	*ppcells = (uint64_t *)gp->data;

	return (size_t)gp->ncells;
}

bool
as_bin_particle_geojson_match(as_bin *candidate_bin, uint64_t query_cellid, geo_region_t query_region, bool is_strict)
{
	// Determine whether the candidate bin geometry is a match for the
	// query geometry.
	//
	// If query_cellid is non-zero this is a regions-containing-point query.
	//
	// If query_region is non-null this is a points-in-region query.
	//
	// Candidate geometry can either be a point or a region.  Regions
	// will have the GEOJSON_ISREGION flag set.

	geojson_mem *gp = (geojson_mem *)candidate_bin->particle;
	
	// Is this a REGIONS-CONTAINING-POINT query?
	//
	if (query_cellid != 0) {

		if ((gp->flags & GEOJSON_ISREGION) != 0) {
			// Candidate is a REGION.

			// Shortcut, if we aren't strict just return true.
			if (! is_strict) {
				return true;
			}
			
			size_t jsonsz;
			char const *jsonptr = geojson_mem_jsonstr(gp, &jsonsz);
			uint64_t parsed_cellid = 0;
			geo_region_t parsed_region = NULL;

			if (! geo_parse(NULL, jsonptr, jsonsz, &parsed_cellid, &parsed_region)) {
				cf_warning(AS_PARTICLE, "geo_parse() failed - unexpected");
				geo_region_destroy(parsed_region);
				return false;
			}

			bool iswithin = geo_point_within(query_cellid, parsed_region);

			geo_region_destroy(parsed_region);
			return iswithin;
		}
		else {
			// Candidate is a POINT, skip it.
			return false;
		}
	}

	// Is this a POINTS-IN-REGION query?
	//
	if (query_region) {

		uint64_t *cells = (uint64_t *)gp->data;

		// Sanity check, make sure this geometry has been processed.
		if (cells[0] == 0) {
			cf_warning(AS_PARTICLE, "first cellid has no value");
			return false;
		}

		if ((gp->flags & GEOJSON_ISREGION) != 0) {
			// Candidate is a REGION, skip it.
			return false;
		}
		else {
			// Candidate is a POINT.
			if (is_strict) {
				return geo_point_within(cells[0], query_region);
			}
			else {
				return true;
			}
		}
	}

	return false;
}


//==========================================================
// Local helpers.
//

static char const *
geojson_mem_jsonstr(geojson_mem *p_geojson_mem, size_t *p_jsonsz)
{
	// Map the point.
	size_t cellsz = p_geojson_mem->ncells * sizeof(uint64_t);

	*p_jsonsz = p_geojson_mem->sz - sizeof(uint8_t) - sizeof(uint16_t) - cellsz;

	return (char const *)p_geojson_mem->data + cellsz;
}

static inline uint32_t
geojson_size(uint32_t n_cells, size_t string_size)
{
	return (uint32_t)(
			sizeof(uint8_t) +				// flags
			sizeof(uint16_t) +				// ncells (always 0 here)
			(n_cells * sizeof(uint64_t)) +	// cell array
			string_size);					// json string
}
