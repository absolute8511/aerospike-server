/*
 * record.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
 * Record operations
 */

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/secondary_index.h"
#include "base/stats.h"
#include "base/transaction.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/rw_utils.h"


void
as_record_rescue(as_index_ref *r_ref, as_namespace *ns)
{
	record_delete_adjust_sindex(r_ref->r, ns);
	as_record_destroy(r_ref->r, ns);
	as_index_clear_record_info(r_ref->r);
	cf_atomic64_incr(&ns->n_objects);
}

// Returns:
//  1 - created new record
//  0 - found existing record
// -1 - failure - found "half created" or deleted record
// -2 - failure - could not allocate arena stage
int
as_record_get_create(as_index_tree *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns, bool is_subrec)
{
	int rv = as_index_get_insert_vlock(tree, keyd, r_ref);

	if (rv == 0) {
		cf_detail(AS_RECORD, "record get_create: digest %"PRIx64" found record %p", *(uint64_t *)keyd , r_ref->r);
	}
	else if (rv == 1) {
		cf_detail(AS_RECORD, "record get_create: digest %"PRIx64" new record %p", *(uint64_t *)keyd, r_ref->r);

		// this is decremented by the destructor here, so best tracked on the constructor
		if (is_subrec) {
			cf_atomic64_incr(&ns->n_sub_objects);
		}
		else {
			cf_atomic64_incr(&ns->n_objects);
		}
	}

	return rv;
}

void
as_record_clean_bins_from(as_storage_rd *rd, uint16_t from)
{
	for (uint16_t i = from; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (as_bin_inuse(b)) {
			as_bin_particle_destroy(b, rd->ns->storage_data_in_memory);
			as_bin_set_empty(b);
		}
	}
}

void
as_record_clean_bins(as_storage_rd *rd)
{
	as_record_clean_bins_from(rd, 0);
}

void
as_record_free_bin_space(as_record *r)
{
	as_bin_space *bin_space = as_index_get_bin_space(r);

	if (bin_space) {
		cf_free((void*)bin_space);
		as_index_set_bin_space(r, NULL);
	}
}

/* as_record_destroy
 * Destroy a record, when the refcount has gone to zero */
void
as_record_destroy(as_record *r, as_namespace *ns)
{
	cf_detail(AS_RECORD, "destroying record %p", r);

	// cleanup statistic at the ns level
	if (ns->storage_data_in_memory) {
		as_storage_rd rd;
		rd.r = r;
		rd.ns = ns;
		as_storage_rd_load_n_bins(&rd);
		as_storage_rd_load_bins(&rd, NULL);

		as_storage_record_drop_from_mem_stats(&rd);

		as_record_clean_bins(&rd);

		if (! ns->single_bin) {
			as_record_free_bin_space(r);

			if (r->dim) {
				cf_free(r->dim); // frees the key
			}
		}
	}

	as_record_drop_stats(r, ns);

	/* Destroy the storage and then free the memory-resident parts */
	as_storage_record_destroy(ns, r);

	return;
}

/* as_record_get
 * Get a record from a tree
 * 0 if success
 * -1 if searched tree and record does not exist
 */
int
as_record_get(as_index_tree *tree, cf_digest *keyd, as_index_ref *r_ref)
{
	return as_index_get_vlock(tree, keyd, r_ref);
}

/* as_record_exists
 * Get a record from a tree
 * 0 if success
 * -1 if searched tree and record does not exist
 */
int
as_record_exists(as_index_tree *tree, cf_digest *keyd)
{
	return as_index_exists(tree, keyd);
}

/* Done with this record - release and unlock
 * Release the locks associated with a record */
void
as_record_done(as_index_ref *r_ref, as_namespace *ns)
{
	if (! r_ref->skip_lock) {
		pthread_mutex_unlock(r_ref->olock);
	}

	int rc = as_index_release(r_ref->r);

	if (rc > 0) {
		return;
	}

	cf_assert(rc == 0, AS_RECORD, "index ref-count %d", rc);

	as_record_destroy(r_ref->r, ns);
	cf_arenax_free(ns->arena, r_ref->r_h);
}

// Called only for data-in-memory multi-bin, with no key currently stored.
// Note - have to modify if/when other metadata joins key in as_rec_space.
void
as_record_allocate_key(as_record* r, const uint8_t* key, uint32_t key_size)
{
	as_rec_space* rec_space = (as_rec_space*)
			cf_malloc_ns(sizeof(as_rec_space) + key_size);

	rec_space->bin_space = (as_bin_space*)r->dim;
	rec_space->key_size = key_size;
	memcpy((void*)rec_space->key, (const void*)key, key_size);

	r->dim = (void*)rec_space;
}

// Called only for data-in-memory multi-bin, with a key currently stored.
// Note - have to modify if/when other metadata joins key in as_rec_space.
void
as_record_remove_key(as_record* r)
{
	as_bin_space* p_bin_space = ((as_rec_space*)r->dim)->bin_space;

	cf_free(r->dim);
	r->dim = (void*)p_bin_space;
}

// AS RECORD serializes as such:
//  N BINS-16
//    BINNAME-LEN-8
//    BINNAME
//    BINTYPE-8
//    LEN-32   DATA

//
//

int
as_record_pickle(as_record *r, as_storage_rd *rd, uint8_t **buf_r, size_t *len_r)
{
	// Determine size
	uint32_t sz = 2;

	// only pickle the n_bins in use
	uint16_t n_bins_inuse = as_bin_inuse_count(rd);

	for (uint16_t i = 0; i < n_bins_inuse; i++) {
		as_bin *b = &rd->bins[i];

		sz += 1; // binname-len field
		sz += rd->ns->single_bin ? 0 : strlen(as_bin_get_name_from_id(rd->ns, b->id)); // number of bytes in the name
		sz += 1; // version

		sz += as_bin_particle_pickled_size(b);
	}

	uint8_t *buf = cf_malloc(sz);
	if (!buf) {
		*buf_r = 0;
		*len_r = 0;
		return(-1);
	}

	uint8_t *buf_lim = buf + sz; // debug
	*len_r = sz;
	*buf_r = buf;

	(*(uint16_t *)buf) = htons(n_bins_inuse); // number of bins
	buf += 2;

	for (uint16_t i = 0; i < n_bins_inuse; i++) {
		as_bin *b = &rd->bins[i];

		uint8_t namelen = (uint8_t)as_bin_memcpy_name(rd->ns, buf + 1, b);
		*buf++ = namelen;
		buf += namelen;
		*buf++ = 0; // was bin version

		buf += as_bin_particle_to_pickled(b, buf);
	}

	if (buf > buf_lim) {
		cf_crash(AS_RECORD, "pickle record overwriting data");
	}

	return(0);
}

int32_t
as_record_buf_get_stack_particles_sz(uint8_t *buf) {
	int32_t stack_particles_sz = 0;

	uint16_t newbins = ntohs( *(uint16_t *) buf );
	buf += 2;

	for (uint16_t i = 0; i < newbins; i++) {
		uint8_t name_sz = *buf;
		buf += name_sz + 2;

		int32_t result = as_particle_size_from_pickled(&buf);

		if (result < 0) {
			return result;
		}

		stack_particles_sz += result;
	}

	return stack_particles_sz;
}

int
as_record_unpickle_replace(as_record *r, as_storage_rd *rd, uint8_t *buf, size_t sz, uint8_t **stack_particles, bool has_sindex)
{
	as_namespace *ns = rd->ns;

	uint8_t *buf_lim = buf + sz;

	uint16_t newbins = ntohs( *(uint16_t *) buf );
	buf += 2;

	if (newbins > BIN_NAMES_QUOTA) {
		cf_warning(AS_RECORD, "as_record_unpickle_replace: received record with too many bins (%d), illegal", newbins);
		return -2;
	}

	// Remember that rd->n_bins may not be the number of existing bins.
	uint16_t old_n_bins =  (ns->storage_data_in_memory || ns->single_bin) ?
			rd->n_bins : as_bin_inuse_count(rd);

	int32_t  delta_bins      = (int32_t)newbins - (int32_t)old_n_bins;
	int      sindex_ret      = AS_SINDEX_OK;
	int      sbins_populated = 0;

	if (has_sindex) {
		SINDEX_GRLOCK();
	}

	// To read the algorithm of upating sindex in bins check notes in ssd_record_add function.
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);
	as_sindex * si_arr[2 * ns->sindex_cnt];
	int si_arr_index = 0;
	const char* set_name = as_index_get_set_name(rd->r, ns);

	// RESERVE SIs for old bins
	// Cannot reserve SIs for new bins as we do not know the bin-id yet
	if (has_sindex) {
		for (int i=0; i<old_n_bins; i++) {
			si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, rd->bins[i].id, &si_arr[si_arr_index]);
		}
	}

	if ((delta_bins < 0) && has_sindex) {
		sbins_populated += as_sindex_sbins_from_rd(rd, newbins, old_n_bins, &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
	}

	if (ns->storage_data_in_memory && ! ns->single_bin) {
		if (delta_bins) {
			// If sizing down, this does destroy the excess particles.
			as_bin_allocate_bin_space(r, rd, delta_bins);
		}
	}
	else if (delta_bins < 0) {
		// Either single-bin data-in-memory where we deleted the (only) bin, or
		// data-not-in-memory where we read existing bins for sindex purposes.
		as_bin_destroy_from(rd, newbins);
	}

	int ret = 0;
	for (uint16_t i = 0; i < newbins; i++) {
		if (buf >= buf_lim) {
			cf_warning(AS_RECORD, "as_record_unpickle_replace: bad format: on bin %d of %d, %p >= %p (diff: %lu) newbins: %d", i, newbins, buf, buf_lim, buf - buf_lim, newbins);
			ret = -4;
			break;
		}

		uint8_t name_sz  = *buf++;
		uint8_t *name    = buf;
		buf             += name_sz;
		buf++; // skipped byte was bin version
		as_bin *b;
		if (i < old_n_bins) {
			b = &rd->bins[i];
			if (has_sindex) {
				sbins_populated      += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
			}
			as_bin_set_id_from_name_buf(ns, b, name, name_sz);
		}
		else {
			// TODO - what if this fails?
			b = as_bin_create_from_buf(rd, name, name_sz);
		}

		if (ns->storage_data_in_memory) {
			// TODO - what if this fails?
			as_bin_particle_replace_from_pickled(b, &buf);
		}
		else {
			// TODO - what if this fails?
			*stack_particles += as_bin_particle_stack_from_pickled(b, *stack_particles, &buf);
		}

		if (has_sindex) {
			si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, b->id, &si_arr[si_arr_index]);
			sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
		}
	}

	if (buf > buf_lim) {
		cf_warning(AS_RECORD, "unpickle record ran beyond input: %p > %p (diff: %lu) newbins: %d", buf, buf_lim, buf - buf_lim, newbins);
		ret = -5;
	}

	if (has_sindex) {
		SINDEX_GRUNLOCK();
	}
	if (ret == 0) {
		if (has_sindex && sbins_populated) {
			sindex_ret = as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, &rd->r->keyd);
			if (sindex_ret != AS_SINDEX_OK) {
				cf_warning(AS_RECORD, "Failed: %s", as_sindex_err_str(sindex_ret));
			}
		}
	}

	if (has_sindex) {
		as_sindex_sbin_freeall(sbins, sbins_populated);
		as_sindex_release_arr(si_arr, si_arr_index);
	}

	return ret;
}

void
as_record_apply_properties(as_record *r, as_namespace *ns, const as_rec_props *p_rec_props)
{
	// Set the record's set-id if it doesn't already have one. (If it does,
	// we assume they're the same.)
	if (! as_index_has_set(r)) {
		const char* set_name;

		if (as_rec_props_get_value(p_rec_props, CL_REC_PROPS_FIELD_SET_NAME,
				NULL, (uint8_t**)&set_name) == 0) {
			as_index_set_set(r, ns, set_name, false);
		}
	}

	uint32_t key_size;
	uint8_t* key;
	int result = as_rec_props_get_value(p_rec_props, CL_REC_PROPS_FIELD_KEY,
					&key_size, &key);

	// If a key wasn't stored, and we got one, accommodate it.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED)) {
		if (result == 0) {
			if (ns->storage_data_in_memory) {
				as_record_allocate_key(r, key, key_size);
			}

			as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
		}
	}
	// If a key was stored, but we didn't get one, remove the key.
	else if (result != 0) {
		if (ns->storage_data_in_memory) {
			as_record_remove_key(r);
		}

		as_index_clear_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	if (ns->ldt_enabled) {
		as_index_clear_flags(r, AS_INDEX_FLAG_SPECIAL_BINS | AS_INDEX_FLAG_CHILD_REC | AS_INDEX_FLAG_CHILD_ESR);
		as_ldt_record_set_rectype_bits(r, p_rec_props);
	}
}

void
as_record_clear_properties(as_record *r, as_namespace *ns)
{
	// If we didn't get a set-id, assume the existing record isn't in a set - if
	// it was, we wouldn't change that anyway, so don't even check.

	// If a key was stored, and we didn't get one, remove the key.
	if (as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED)) {
		if (ns->storage_data_in_memory) {
			as_record_remove_key(r);
		}

		as_index_clear_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	if (ns->ldt_enabled) {
		as_index_clear_flags(r, AS_INDEX_FLAG_SPECIAL_BINS | AS_INDEX_FLAG_CHILD_REC | AS_INDEX_FLAG_CHILD_ESR);
	}
}

void
as_record_set_properties(as_storage_rd *rd, const as_rec_props *p_rec_props)
{
	if (p_rec_props->p_data && p_rec_props->size != 0) {
		// Copy rec-props into rd so the metadata gets written to device.
		rd->rec_props = *p_rec_props;

		// Apply the metadata in rec-props to the as_record.
		as_record_apply_properties(rd->r, rd->ns, p_rec_props);
	}
	// It's possible to get empty rec-props.
	else {
		// Clear the rec-props related metadata in the as_record.
		as_record_clear_properties(rd->r, rd->ns);
	}
}

int
as_record_flatten_component(as_storage_rd *rd, as_index_ref *r_ref,
		as_record_merge_component *c, bool is_create)
{
	as_index *r = r_ref->r;

	rd->ignore_record_on_device = true; // TODO - set to ! has_sindex
	as_storage_rd_load_n_bins(rd); // TODO - handle error returned

	uint16_t newbins = ntohs(*(uint16_t *)c->record_buf); // already checked that newbins can't be 0 here

	if (! rd->ns->storage_data_in_memory && ! rd->ns->single_bin && newbins > rd->n_bins) {
		rd->n_bins = newbins;
	}

	as_bin stack_bins[rd->ns->storage_data_in_memory ? 0 : rd->n_bins];

	as_storage_rd_load_bins(rd, stack_bins); // TODO - handle error returned

	uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	int32_t stack_particles_sz = 0;

	if (! rd->ns->storage_data_in_memory) {
		stack_particles_sz = as_record_buf_get_stack_particles_sz(c->record_buf);

		if (stack_particles_sz < 0) {
			cf_warning_digest(AS_RECORD, &rd->r->keyd, "stack particle size failed");
			as_storage_record_close(rd);
			return -1;
		}
	}

	// 256 as upper bound on the LDT control bin, we may write version below
	uint8_t stack_particles[stack_particles_sz + 256]; // stack allocate space for new particles when data on device
	uint8_t *p_stack_particles = stack_particles;

	// Cleanup old info and put new info
	as_record_set_properties(rd, &c->rec_props);

	if (is_create) {
		r->last_update_time = c->last_update_time;

		if (as_truncate_record_is_truncated(r, rd->ns)) {
			as_storage_record_close(rd);
			return -8; // yes, another special return value
		}
	}

	// Check after applying set-id from rec-props, in case r just created.
	bool has_sindex = record_has_sindex(r, rd->ns);

	int rv = as_record_unpickle_replace(r, rd, c->record_buf, c->record_buf_sz, &p_stack_particles, has_sindex);
	if (0 != rv) {
		cf_warning_digest(AS_LDT, &rd->r->keyd, "Unpickled replace failed rv=%d",rv);
		as_storage_record_close(rd);
		return rv;
	}

	r->void_time = truncate_void_time(rd->ns, c->void_time);
	r->last_update_time  = c->last_update_time;
	r->generation = c->generation;
	// Update the version in the parent. In case it is incoming migration
	//
	// Should it be done only in case of migration ?? for LDT currently
	// flatten gets called only for migration .. because there is no duplicate
	// resolution .. there is only winner resolution
	if (COMPONENT_IS_MIG(c) && as_ldt_record_is_parent(rd->r)) {
		int ldt_rv = as_ldt_parent_storage_set_version(rd, c->version, p_stack_particles, __FILE__, __LINE__);
		if (ldt_rv < 0) {
			cf_warning_digest(AS_LDT, &rd->r->keyd, "LDT_MERGE Failed to write version in rv=%d", ldt_rv);
		}
	}

	as_record_apply_pickle(rd);
	as_storage_record_adjust_mem_stats(rd, memory_bytes);
	as_storage_record_close(rd);

	return 0;
}

// Returns -1 if left wins, 1 if right wins, and 0 for tie.

static inline int
resolve_generation_direct(uint16_t left, uint16_t right)
{
	return left == right ? 0 : (right > left  ? 1 : -1);
}

static inline int
resolve_generation(uint16_t left, uint16_t right)
{
	return left == right ? 0 : (as_gen_less_than(left, right) ? 1 : -1);
}

static inline int
resolve_last_update_time(uint64_t left, uint64_t right)
{
	return left == right ? 0 : (right > left ? 1 : -1);
}

int
as_record_resolve_conflict(conflict_resolution_pol policy, uint16_t left_gen,
		uint64_t left_lut, uint16_t right_gen, uint64_t right_lut)
{
	int result = 0;

	switch (policy) {
	case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION:
		// Doesn't use resolve_generation() - direct comparison gives much
		// better odds of picking the record with more history after a split
		// brain where one side starts the record from scratch.
		result = resolve_generation_direct(left_gen, right_gen);
		if (result == 0) {
			result = resolve_last_update_time(left_lut, right_lut);
		}
		break;

	case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME:
		result = resolve_last_update_time(left_lut, right_lut);
		if (result == 0) {
			result = resolve_generation(left_gen, right_gen);
		}
		break;

	default:
		cf_crash(AS_RECORD, "invalid conflict resolution policy");
		break;
	}

	return result;
}

int
as_record_component_winner(conflict_resolution_pol conflict_resolution_policy,
		uint32_t n_components, const as_record_merge_component *components,
		const as_record *r)
{
	int winner_idx;
	uint32_t start;
	uint32_t max_generation;
	uint64_t max_last_update_time;

	if (r) {
		winner_idx = -1; // existing record is best so far
		start = 0; // compare all components to existing record
		max_generation = r->generation;
		max_last_update_time = r->last_update_time;
	}
	else {
		winner_idx = 0; // first component is best so far
		start = 1; // compare other components to first component
		max_generation = components[0].generation;
		max_last_update_time = components[0].last_update_time;
	}

	for (uint32_t i = start; i < n_components; i++) {
		const as_record_merge_component *c = &components[i];

		if (as_record_resolve_conflict(conflict_resolution_policy,
				c->generation, c->last_update_time, max_generation,
				max_last_update_time) == -1) {
			winner_idx = (int)i;
			max_generation = c->generation;
			max_last_update_time = c->last_update_time;
		}
	}

	return winner_idx;
}

int
as_record_flatten(as_partition_reservation *rsv, cf_digest *keyd,
		uint32_t n_components, as_record_merge_component *components,
		int *winner_idx)
{
	as_namespace *ns = rsv->ns;

	if (COMPONENT_IS_LDT(&components[0]) && ! ns->ldt_enabled) {
		// Ignore LDT migrations. (And if LDT is disabled, LDT dummies won't get
		// here via duplicate resolution.)
		return 0;
	}

	if (! as_storage_has_space(ns)) {
		cf_warning(AS_RECORD, "{%s}: record_flatten: drives full", ns->name);
		return -1;
	}

	CF_ALLOC_SET_NS_ARENA(ns);

	bool is_subrec = false;

	// LDT subrecords (which get here only via migration) have their own
	// conflict resolution method.
	if (COMPONENT_IS_MIG(&components[0]) &&
			COMPONENT_IS_LDT_SUB(&components[0])) {
		if (! as_ldt_merge_component_is_candidate(rsv, &components[0])) {
			// Remote subrecord loses comparison - ignore it.
			return 0;
		}
		// else - remote subrecord wins - apply it.

		is_subrec = true;
		*winner_idx = 0;
	}

	as_index_tree *tree = is_subrec ? rsv->sub_tree : rsv->tree;

	as_index_ref r_ref;
	r_ref.skip_lock = false;

	int rv = as_record_get_create(tree, keyd, &r_ref, ns, is_subrec);

	if (rv < 0) {
		cf_debug_digest(AS_RECORD, keyd, "{%s} record flatten: could not get-create record %d", ns->name, is_subrec);
		return -3;
	}

	bool is_create = rv == 1;
	as_index *r = r_ref.r;

	if (! is_subrec) {
		*winner_idx = as_record_component_winner(ns->conflict_resolution_policy,
				n_components, components, is_create ? NULL : r);
	}

	// If the winner is the local copy, nothing to do.
	if (*winner_idx == -1) {
		as_record_done(&r_ref, ns);
		return 0;
	}
	// else - remote winner - apply it (unless it's an LDT dummy).

	int flatten_rv;
	as_record_merge_component *c = &components[*winner_idx];

	if (COMPONENT_IS_LDT_DUMMY(c)) {
		// LDT dummy won - don't apply locally, trigger a ship-op.
		flatten_rv = -7; // -7 is special - do not change!
	}
	else {
		// Normal remote winner.
		as_storage_rd rd;

		if (is_create) {
			as_storage_record_create(ns, r, &rd);
		}
		else {
			as_storage_record_open(ns, r, &rd);
		}

		// Apply remote winner locally. (Yes, call closes as_storage_rd.)
		flatten_rv = as_record_flatten_component(&rd, &r_ref, c, is_create);
	}

	// On failure or ship-op, delete index element if created above.
	if (flatten_rv != 0 && is_create) {
		as_index_delete(rsv->tree, keyd);
	}

	as_record_done(&r_ref, ns);

	return flatten_rv;
}

// TODO - inline this, if/when we unravel header files.
bool
as_record_is_expired(const as_record *r)
{
	return r->void_time != 0 && r->void_time < as_record_void_time_get();
}
