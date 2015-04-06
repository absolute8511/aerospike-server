/*
 * bin.c
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
 *  bin operations
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "fault.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "storage/storage.h"


bool
as_bin_get_id_from_name_buf(as_namespace *ns, byte *buf, size_t len, uint32_t *p_id)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_or_assign_id()");
	}

	char name[len + 1];
	memcpy(name, buf, len);
	name[len] = 0;

	return cf_vmapx_get_index(ns->p_bin_name_vmap, name, p_id) == CF_VMAPX_OK;
}

// caller-beware, name cannot be null
int16_t
as_bin_get_id(as_namespace *ns, const char *name)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_or_assign_id()");
	}
	uint32_t idx;
	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, &idx) == CF_VMAPX_OK) {
		return (uint16_t)idx;
	}
	return -1;
}

uint16_t
as_bin_get_or_assign_id(as_namespace *ns, const char *name)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_or_assign_id()");
	}

	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, &idx) == CF_VMAPX_OK) {
		return (uint16_t)idx;
	}

	// Verify length just once here at insertion.
	if (strlen(name) >= BIN_NAME_MAX_SZ) {
		cf_crash(AS_BIN, "bin name %s too long", name);
	}

	cf_vmapx_err result = cf_vmapx_put_unique(ns->p_bin_name_vmap, name, &idx);

	if (! (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS)) {
		// Tedious to handle safely for all usage paths, so for now...
		cf_crash(AS_BIN, "could not add bin name %s, vmap err %d", name, result);
	}

	return (uint16_t)idx;
}

const char *
as_bin_get_name_from_id(as_namespace *ns, uint16_t id)
{
	if (ns->single_bin) {
		cf_crash(AS_BIN, "single-bin call of as_bin_get_name_from_id()");
	}

	const char* name = NULL;

	if (cf_vmapx_get_by_index(ns->p_bin_name_vmap, id, (void**)&name) != CF_VMAPX_OK) {
		// TODO - Fail softly by returning forbidden bin name? (Empty string?)
		cf_crash(AS_BIN, "no bin name for id %u", id);
	}

	return name;
}

bool
as_bin_name_within_quota(as_namespace *ns, byte *buf, size_t len)
{
	// Won't exceed quota if single-bin or currently below quota.
	if (ns->single_bin || cf_vmapx_count(ns->p_bin_name_vmap) < BIN_NAMES_QUOTA) {
		return true;
	}

	char name[len + 1];

	memcpy(name, buf, len);
	name[len] = 0;

	// Won't exceed quota if name is found (and so would NOT be added to vmap).
	if (cf_vmapx_get_index(ns->p_bin_name_vmap, name, NULL) == CF_VMAPX_OK) {
		return true;
	}

	cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s", ns->name, name);

	return false;
}

void
as_bin_all_dump(as_storage_rd *rd, char *msg)
{
	cf_info(AS_BIN, "bin dump: %s: new nbins %d", msg, rd->n_bins);
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];
		cf_info(AS_BIN, "bin %s: %d: bin %p inuse %d particle %p", msg, i, b, as_bin_inuse(b), b->particle);
	}
}

void
as_bin_init(as_namespace *ns, as_bin *b, byte *name, size_t namesz, uint version)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
	as_bin_set_version(b, version, ns->single_bin);

	b->particle = 0;

	as_bin_set_id_from_name_buf(ns, b, name, namesz);
	// Don't touch b->unused - like b->id, it's past the end of its enclosing
	// as_index if single-bin, data-in-memory.
}

static inline
as_bin_space* safe_bin_space(const as_record *r) {
	return r->dim ? as_index_get_bin_space(r) : NULL;
}

static inline
uint16_t safe_n_bins(const as_record *r) {
	as_bin_space* bin_space = safe_bin_space(r);
	return bin_space ? bin_space->n_bins : 0;
}

static inline
as_bin* safe_bins(const as_record *r) {
	as_bin_space* bin_space = safe_bin_space(r);
	return bin_space ? bin_space->bins : NULL;
}

uint16_t
as_bin_get_n_bins(as_record *r, as_storage_rd *rd)
{
	if (rd->ns->single_bin) {
		return (1);
	}

	if (rd->ns->storage_data_in_memory) {
		return safe_n_bins(r);
	}

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		if (! rd->have_device_block) {
			as_storage_record_read(rd);
		}

		return (as_storage_record_get_n_bins(rd));
	}

	return (0);
}

as_bin *
as_bin_get_all(as_record *r, as_storage_rd *rd, as_bin *stack_bins)
{
	if (rd->ns->storage_data_in_memory) {
		return rd->ns->single_bin ? as_index_get_single_bin(r) : safe_bins(r);
	}

	rd->bins = stack_bins;
	as_bin_set_all_empty(rd);

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		as_storage_particle_read_all(rd);
	}

	return (stack_bins);
}

// - Seems like an as_storage_record method, but leaving it here for now.
// - sets rd->bins!
// - for data-in-memory, assumes rd->n_bins is already set!
bool
as_bin_get_and_size_all(as_storage_rd *rd, as_bin *stack_bins)
{
	if (rd->ns->storage_data_in_memory) {
		rd->bins = rd->ns->single_bin ? as_index_get_single_bin(rd->r) :
				safe_bins(rd->r);

		if (! rd->bins) {
			// i.e. multi-bin when record just created.
			return true;
		}

		if (rd->ns->storage_type != AS_STORAGE_ENGINE_SSD) {
			// Not interested in calculating flat-sizing info.
			return true;
		}

		// Calculate starting values for rd->n_bins_to_write and
		// rd->particles_flat_size.

		uint16_t i;

		for (i = 0; i < rd->n_bins; i++) {
			as_bin *b = &rd->bins[i];

			if (! as_bin_inuse(b)) {
				// i.e. single-bin when record just created.
				break;
			}

			size_t flat_size;

			if (0 != as_bin_particle_flat_size(b, &flat_size)) {
				return false;
			}

			rd->particles_flat_size += flat_size;
		}

		rd->n_bins_to_write = (uint32_t)i;

		return true;
	}

	// Data NOT in-memory.

	rd->bins = stack_bins;
	as_bin_set_all_empty(rd);

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		// Sets rd->n_bins_to_write and rd->particles_flat_size:
		if (0 != as_storage_particle_read_and_size_all_ssd(rd)) {
			return false;
		}
	}

	return true;
}

// utility function to convert a pointer to the bin space to an array of pointers to each (used) bin

void
as_bin_get_all_p(as_storage_rd *rd, as_bin **bin_ptrs)
{
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		bin_ptrs[i] = &rd->bins[i];
	}
}

/* as_bin_create
 * Create a new bin with the specified name in a record
 * NB: You must be holding the value lock for the record! */
as_bin *
as_bin_create(as_record *r, as_storage_rd *rd, byte *name, size_t namesz, uint version)
{
	cf_detail(AS_BIN, "as_bin_create: %s %zu", name, namesz);

	// what policy should we make for too-large bins passed in?

	if (namesz > (AS_ID_BIN_SZ - 1)) {
		cf_warning(AS_RW, "WARNING: too large bin name %d passed in, internal error", namesz);
		return(NULL);
	}

	as_bin *b = 0;

	if (rd->ns->single_bin) {
		if (as_bin_inuse(rd->bins)) {
			cf_warning(AS_RW, "WARNING: cannot allocate more than 1 bin in a single bin namespace");
			return (NULL);
		}
		// do not store bin name
		byte c = 0;
		as_bin_init(rd->ns, rd->bins, &c, 0, version);
		return (rd->bins);
	}

	// seek for an empty one
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			b = &rd->bins[i];
			break;
		}
	}

	if (b) {
		as_bin_init(rd->ns, b, name, namesz, version);
	}

	return (b);
}

as_bin *
as_bin_get(as_storage_rd *rd, byte *name, size_t namesz)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? rd->bins : NULL;
	}

	uint32_t id;

	if (! as_bin_get_id_from_name_buf(rd->ns, name, namesz, &id)) {
		return NULL;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return b;
		}
	}

	return NULL;
}

as_bin *
as_bin_get_and_reserve_name(as_storage_rd *rd, byte *name, size_t namesz,
		bool *p_reserved, uint32_t *p_idx)
{
	*p_reserved = true;

	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? rd->bins : NULL;
	}

	char zname[namesz + 1];

	memcpy(zname, name, namesz);
	zname[namesz] = 0;

	if (cf_vmapx_get_index(rd->ns->p_bin_name_vmap, zname, p_idx) != CF_VMAPX_OK) {
		if (cf_vmapx_count(rd->ns->p_bin_name_vmap) >= BIN_NAMES_QUOTA) {
			cf_warning(AS_BIN, "{%s} bin-name quota full - can't add new bin-name %s", rd->ns->name, zname);
			*p_reserved = false;
		}
		else {
			cf_vmapx_err result = cf_vmapx_put_unique(rd->ns->p_bin_name_vmap, zname, p_idx);

			if (! (result == CF_VMAPX_OK || result == CF_VMAPX_ERR_NAME_EXISTS)) {
				cf_warning(AS_BIN, "{%s} can't add new bin name %s, vmap err %d", rd->ns->name, zname, result);
				*p_reserved = false;
			}
		}

		return NULL;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == *p_idx) {
			return b;
		}
	}

	return NULL;
}

int32_t
as_bin_get_index(as_storage_rd *rd, byte *name, size_t namesz)
{
	if (rd->ns->single_bin) {
		return as_bin_inuse_has(rd) ? 0 : -1;
	}

	uint32_t id;

	if (! as_bin_get_id_from_name_buf(rd->ns, name, namesz, &id)) {
		return -1;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (! as_bin_inuse(b)) {
			break;
		}

		if ((uint32_t)b->id == id) {
			return (int32_t)i;
		}
	}

	return -1;
}

/* as_bin_get_all_versions
 * Get all versions of a bin from the record
 * returns the number of bins found and also the bin pointers in the array
 * NB: You must be holding the value lock for the record! */
int
as_bin_get_all_versions(as_storage_rd *rd, byte *name, size_t namesz, as_bin **curr_bins)
{
	int n_curr_bins = 0;
	if (!curr_bins)
		return (0);

	if (rd->ns->single_bin) {
		// no name comparison for single bin namespaces
		as_bin *b = rd->bins;
		if (rd->n_bins == 1 && as_bin_inuse(b)) {
			curr_bins[n_curr_bins] = b;
			n_curr_bins++;
		}
	}
	else {
		uint32_t id;

		if (! as_bin_get_id_from_name_buf(rd->ns, name, namesz, &id)) {
			return 0;
		}

		for (uint16_t i = 0; i < rd->n_bins; i++) {
			/* Skip empty bins */
			as_bin *b = &rd->bins[i];
			if (! as_bin_inuse(b)) {
				break;
			}

			if ((uint32_t)b->id == id) {
				curr_bins[n_curr_bins] = b;
				n_curr_bins++;
				if (n_curr_bins >= BIN_VERSION_MAX) {
					cf_debug(AS_RECORD, "bin: somehow got maximum versions: problem");
					break;
				}
			}
		}
	}

	return (n_curr_bins);
}

void
as_bin_allocate_bin_space(as_record *r, as_storage_rd *rd, int32_t delta) {
	if (rd->n_bins == 0) {
		rd->n_bins = (uint16_t)delta;

		as_bin_space* bin_space = (as_bin_space*)
				cf_malloc(sizeof(as_bin_space) + (rd->n_bins * sizeof(as_bin)));

		rd->bins = bin_space->bins;
		as_bin_set_all_empty(rd);

		bin_space->n_bins = rd->n_bins;
		as_index_set_bin_space(r, bin_space);
	}
	else {
		uint16_t new_n_bins = (uint16_t)((int32_t)rd->n_bins + delta);

		if (delta < 0) {
			as_record_clean_bins_from(rd, new_n_bins);
		}

		uint16_t old_n_bins = rd->n_bins;

		rd->n_bins = new_n_bins;

		if (new_n_bins != 0) {
			as_bin_space* bin_space = (as_bin_space*)
					cf_realloc((void*)as_index_get_bin_space(r), sizeof(as_bin_space) + (rd->n_bins * sizeof(as_bin)));

			rd->bins = bin_space->bins;

			if (delta > 0) {
				as_bin_set_empty_from(rd, old_n_bins);
			}

			bin_space->n_bins = rd->n_bins;
			as_index_set_bin_space(r, bin_space);
		}
		else {
			cf_free((void*)as_index_get_bin_space(r));
			as_index_set_bin_space(r, NULL);
			rd->bins = NULL;
		}
	}
}

void
as_bin_destroy(as_storage_rd *rd, uint16_t i)
{
	as_particle_destroy(&rd->bins[i], rd->ns->storage_data_in_memory);
	as_bin_set_empty_shift(rd, i);
}

void
as_bin_destroy_from(as_storage_rd *rd, uint16_t from)
{
	for (uint16_t i = from; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			break;
		}

		as_particle_destroy(&rd->bins[i], rd->ns->storage_data_in_memory);
	}

	as_bin_set_empty_from(rd, from);
}

void
as_bin_destroy_all(as_storage_rd *rd)
{
	as_bin_destroy_from(rd, 0);
}

uint16_t
as_bin_inuse_count(as_storage_rd *rd)
{
	uint16_t i;
	// TODO: rd NULL is an error condition, 0 is a valid value, change function semantics
	if (!rd) {
		return 0;
	}
	for (i = 0; i < rd->n_bins; i++) {
		if (! as_bin_inuse(&rd->bins[i])) {
			break;
		}
	}

	return (i);
}
