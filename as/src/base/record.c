/*
 * record.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/param.h>

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
#include "base/transaction.h"
#include "storage/storage.h"


// #define EXTRA_CHECKS
// #define BREAK_VTP_ERROR

#ifdef EXTRA_CHECKS
#include <signal.h>
#endif

/* Used for debugging/tracing */
static char * MOD = "partition.c::06/28/13";


/* as_record_initialize
 * Initialize the record.
 * Called from as_record_get_create() and write_local()
 * record lock needs to be held before calling this function.
 */
void as_record_initialize(as_index_ref *r_ref, as_namespace *ns)
{
	if (!r_ref || !ns) {
		cf_warning(AS_RECORD, "as_record_reinitialize: illegal params");
		return;
	}

	as_index *r = r_ref->r;

	as_index_clear_flags(r, AS_INDEX_ALL_FLAGS);

	if (ns->single_bin) {
		as_bin *b = as_index_get_single_bin(r);
		as_bin_state_set(b, AS_BIN_STATE_UNUSED);
		b->particle = 0;

	}
	else {
		r->dim = NULL;
	}

	// clear everything owned by record
	r->generation = 0;
	r->void_time = 0;

	// layer violation, refactor sometime
	if (AS_STORAGE_ENGINE_SSD == ns->storage_type) {
		r->storage_key.ssd.file_id = STORAGE_INVALID_FILE_ID;
		r->storage_key.ssd.rblock_id = STORAGE_INVALID_RBLOCK;
		r->storage_key.ssd.n_rblocks = 0;
	}
	else if (AS_STORAGE_ENGINE_KV == ns->storage_type) {
		r->storage_key.kv.file_id = STORAGE_INVALID_FILE_ID;
	}
	else if (AS_STORAGE_ENGINE_MEMORY == ns->storage_type) {
		// The storage_key struct shouldn't be used, but for now is accessed
		// when making the (useless for memory-only) object size histogram.
		*(uint64_t*)&r->storage_key.ssd = 0;
		r->storage_key.ssd.rblock_id = STORAGE_INVALID_RBLOCK;
	}
	else {
		cf_crash(AS_RECORD, "unknown storage engine type: %d", ns->storage_type);
	}

	as_index_set_set_id(r, 0);
}

/* as_record_get_create
 * Instantiate a new as_record in a namespace (no bins though)
 * AND CREATE IF IT DOESN"T EXIST
 * returns -1 if fail
 * 0 if successful find
 * 1 if successful but CREATE
 */
int
as_record_get_create(as_index_tree *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns, bool is_subrec)
{
	// Only create the in-memory index tree when not using KV store.
	int rv = (as_storage_has_index(ns) ?
			as_index_ref_initialize(tree, keyd, r_ref, true, ns) :
			as_index_get_insert_vlock(tree, keyd, r_ref));

	if (rv == 0) {
		cf_detail(AS_RECORD, "record get_create: digest %"PRIx64" found record %p", *(uint64_t *)keyd , r_ref->r);

		if (r_ref->r->storage_key.ssd.rblock_id == 0) {
			cf_debug_digest(AS_RECORD, keyd, "fail as_record_get_create(): rblock_id 0 ");
			as_record_done(r_ref, ns);
			rv = -1;
		}
	}
	else if (rv == 1) {
		cf_detail(AS_RECORD, "record get_create: digest %"PRIx64" new record %p", *(uint64_t *)keyd, r_ref->r);

		// new record, have to initialize bits
		as_record_initialize(r_ref, ns);

		// this is decremented by the destructor here, so best tracked on the constructor
		if (is_subrec) {
			cf_atomic_int_incr(&ns->n_sub_objects);
		}
		else {
			cf_atomic_int_incr(&ns->n_objects);
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
		rd.n_bins = as_bin_get_n_bins(r, &rd);
		rd.bins = as_bin_get_all(r, &rd, 0);

		as_storage_record_drop_from_mem_stats(&rd);

		as_record_clean_bins(&rd);
		if (! ns->single_bin) {
			if (rd.n_bins) {
				cf_free((void*)as_index_get_bin_space(r));
				as_index_set_bin_space(r, NULL);
			}

			if (r->dim) {
				cf_free(r->dim); // frees the key
			}
		}
	}

	// release from set
	as_namespace_release_set_id(ns, as_index_get_set_id(r));

	// TODO: LDT what if flag is not set ??
	if (as_ldt_record_is_sub(r)) {
		cf_atomic_int_decr(&ns->n_sub_objects);
	}
	else {
		cf_atomic_int_decr(&ns->n_objects);
	}

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
as_record_get(as_index_tree *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns)
{
	// index search takes the refcount and releases the treelock, the opposite of the
	// get_insert call above

	int rv = (as_storage_has_index(ns)
			  ? (!as_index_ref_initialize(tree, keyd, r_ref, false, ns) ? 0 : -1)
			  : as_index_get_vlock(tree, keyd, r_ref));

	if (rv == 0) {
		cf_detail(AS_RECORD, "record get: digest %"PRIx64" found record %p", *(uint64_t *)keyd, r_ref->r);

		if (r_ref->r->storage_key.ssd.rblock_id == 0) {
			cf_debug_digest(AS_RECORD, keyd, "fail as_record_get(): rblock_id 0 ");
			as_record_done(r_ref, ns);
			rv = -1; // masquerade as a not-found, which is handled everywhere
		}
	}
	else if (rv == -1) {
		cf_detail(AS_RECORD, "record get: digest %"PRIx64" not found", *(uint64_t *)keyd);
	}

	return rv;
}

/* as_record_exists
 * Get a record from a tree
 * 0 if success
 * -1 if searched tree and record does not exist
 */
int
as_record_exists(as_index_tree *tree, cf_digest *keyd, as_namespace *ns)
{
	// index search takes the refcount and releases the treelock, the opposite of the
	// get_insert call above

	int rv = (as_storage_has_index(ns)
			  ? -1
			  : as_index_exists(tree, keyd));

	if (rv == -1) {
		cf_detail(AS_RECORD, "record get: digest %"PRIx64" not found", *(uint64_t *)keyd);

		return(-1);
	}
	return(0);
}

/* Done with this record - release and unlock
 * Release the locks associated with a record */
void
as_record_done(as_index_ref *r_ref, as_namespace *ns)
{
	if ((!r_ref->skip_lock)
			&& (r_ref->olock == 0)) {
		cf_crash(AS_RECORD, "calling done with null lock, illegal");
	}

	int rv = 0;
	if (!r_ref->skip_lock) {
		rv = pthread_mutex_unlock(r_ref->olock);
		cf_atomic_int_decr(&g_config.global_record_lock_count);
	}
	if (0 != rv)
		cf_crash(AS_RECORD, "couldn't release lock: %s", cf_strerror(rv));

	if (0 == as_index_release(r_ref->r)) {
		// cf_info(AS_RECORD, "index destroy 4 %p %x",r_ref->r,r_ref->r_h);
		as_record_destroy(r_ref->r, ns);
		cf_arenax_free(ns->arena, r_ref->r_h);
	}
	cf_atomic_int_decr(&g_config.global_record_ref_count);

	return;
}

// Called only for data-in-memory multi-bin, with no key currently stored.
// Note - have to modify if/when other metadata joins key in as_rec_space.
void
as_record_allocate_key(as_record* r, const uint8_t* key, uint32_t key_size)
{
	as_rec_space* rec_space = (as_rec_space*)
			cf_malloc(sizeof(as_rec_space) + key_size);

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
as_record_pickle(as_record *r, as_storage_rd *rd, byte **buf_r, size_t *len_r)
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

	byte *buf = cf_malloc(sz);
	if (!buf) {
		*buf_r = 0;
		*len_r = 0;
		return(-1);
	}

	byte *buf_lim = buf + sz; // debug
	*len_r = sz;
	*buf_r = buf;

	(*(uint16_t *)buf) = htons(n_bins_inuse); // number of bins
	buf += 2;

	for (uint16_t i = 0; i < n_bins_inuse; i++) {
		as_bin *b = &rd->bins[i];

		byte namelen = (byte)as_bin_memcpy_name(rd->ns, buf + 1, b);
		*buf++ = namelen;
		buf += namelen;
		*buf++ = 0; // was bin version

		buf += as_bin_particle_to_pickled(b, buf);
	}

	if (buf > buf_lim)
		cf_crash(AS_RECORD, "pickle record overwriting data");

	return(0);
}

int
as_record_pickle_a_delete(byte **buf_r, size_t *len_r)
{
	// Determine size
	uint32_t sz = 2;

	// only pickle the n_bins in use
	uint16_t n_bins_inuse = 0;

	byte *buf = cf_malloc(sz);
	if (!buf) {
		*buf_r = 0;
		*len_r = 0;
		return(-1);
	}

	*len_r = sz;
	*buf_r = buf;

	(*(uint16_t *)buf) = htons(n_bins_inuse); // number of bins
	return(0);
}

uint32_t
as_record_buf_get_stack_particles_sz(uint8_t *buf) {
	uint32_t stack_particles_sz = 0;

	uint16_t newbins = ntohs( *(uint16_t *) buf );
	buf += 2;

	for (uint16_t i = 0; i < newbins; i++) {
		byte name_sz = *buf;
		buf += name_sz + 2;

		stack_particles_sz += as_particle_size_from_pickled(&buf);
	}

	return (stack_particles_sz);
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

	const char* set_name = NULL;
	if (has_sindex) {
		set_name = as_index_get_set_name(rd->r, ns);
	}

	int ret = 0;
	for (uint16_t i = 0; i < newbins; i++) {
		if (buf >= buf_lim) {
			cf_warning(AS_RECORD, "as_record_unpickle_replace: bad format: on bin %d of %d, %p >= %p (diff: %lu) newbins: %d", i, newbins, buf, buf_lim, buf - buf_lim, newbins);
			ret = -3;
			break;
		}

		byte name_sz     = *buf++;
		byte *name       = buf;
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
			as_bin_particle_replace_from_pickled(b, &buf);
		}
		else {
			*stack_particles += as_bin_particle_stack_from_pickled(b, *stack_particles, &buf);
		}

		if (has_sindex) {
			sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
		}
	}

	if (buf > buf_lim) {
		cf_warning(AS_RECORD, "unpickle record ran beyond input: %p > %p (diff: %lu) newbins: %d", buf, buf_lim, buf - buf_lim, newbins);
		ret = -5;
	}

	if (has_sindex) {
		SINDEX_GUNLOCK();
	}
	if (ret == 0) {
		if (has_sindex && sbins_populated) {
			sindex_ret = as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, &rd->keyd);
			if (sindex_ret != AS_SINDEX_OK) {
				cf_warning(AS_RECORD, "Failed: %s", as_sindex_err_str(sindex_ret));
			}
		}
		rd->write_to_device = true;
	}


	if (has_sindex && sbins_populated) {
		as_sindex_sbin_freeall(sbins, sbins_populated);
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
as_record_flatten_component(as_partition_reservation *rsv, as_storage_rd *rd,
		as_index_ref *r_ref, as_record_merge_component *c, bool *delete_record)
{
	as_index *r = r_ref->r;
	bool has_sindex = as_sindex_ns_has_sindex(rd->ns);
	rd->ignore_record_on_device = true; // TODO - set to ! has_sindex
	rd->n_bins = as_bin_get_n_bins(r, rd);
	uint16_t newbins = ntohs(*(uint16_t *) c->record_buf);

	if (! rd->ns->storage_data_in_memory && ! rd->ns->single_bin && newbins > rd->n_bins) {
		rd->n_bins = newbins;
	}

	as_bin stack_bins[rd->ns->storage_data_in_memory ? 0 : rd->n_bins];

	rd->bins = as_bin_get_all(r, rd, stack_bins);

	uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	uint32_t stack_particles_sz = 0;
	if (! rd->ns->storage_data_in_memory) {
		stack_particles_sz = as_record_buf_get_stack_particles_sz(c->record_buf);
	}

	// 256 as upper bound on the LDT control bin, we may write version below
	uint8_t stack_particles[stack_particles_sz + 256]; // stack allocate space for new particles when data on device
	uint8_t *p_stack_particles = stack_particles;

	// Cleanup old info and put new info
	as_record_set_properties(rd, &c->rec_props);
	int rv = as_record_unpickle_replace(r, rd, c->record_buf, c->record_buf_sz, &p_stack_particles, has_sindex);
	if (0 != rv) {
		cf_warning_digest(AS_LDT, &rd->keyd, "Unpickled replace failed rv=%d",rv);
		as_storage_record_close(r, rd);
		return rv;
    }

	r->void_time  = c->void_time;
	r->generation = c->generation;
	// Update the version in the parent. In case it is incoming migration
	//
	// Should it be done only in case of migration ?? for LDT currently
	// flatten gets called only for migration .. because there is no duplicate
	// resolution .. there is only winner resolution
	if (COMPONENT_IS_MIG(c) && as_ldt_record_is_parent(rd->r)) {
		int pbytes = as_ldt_parent_storage_set_version(rd, c->version, p_stack_particles, __FILE__, __LINE__);
		if (pbytes < 0) {
			cf_warning_digest(AS_LDT, &rd->keyd, "LDT_MERGE Failed to write version in rv=%d", pbytes);
		} else {
			p_stack_particles += pbytes;			
		}
	}

	// cf_info(AS_RECORD, "flatten: key %"PRIx64" used incoming component %d generation %d",*(uint64_t *)keyd, idx,r->generation);

#ifdef EXTRA_CHECKS
	// an EXTRA CHECK - should have some bins
	uint16_t n_bins_check = 0;
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		if (as_bin_inuse(&rd->bins[i])) n_bins_check++;
	}
	if (n_bins_check == 0) cf_info(AS_RECORD, "merge: extra check: after write, no bins. peculiar.");
#endif

	if (!as_bin_inuse_has(rd)) {
		*delete_record = true;
	}

	as_storage_record_adjust_mem_stats(rd, memory_bytes);

	rd->write_to_device = true;

	// write record to device
	as_storage_record_close(r, rd);

	return (0);
}


int
as_record_component_winner(as_partition_reservation *rsv, int n_components,
		as_record_merge_component *components, as_index *r)
{
	uint32_t max_void_time, max_generation, start, winner_idx;

	// if local record is there set its as starting value other
	// was set initial value to be of component[0]
	if (r) {
		max_void_time  = r->void_time;
		max_generation = r->generation;
		start          = 0;
		winner_idx     = -1;
	} else {
		max_void_time  = components[0].void_time;
		max_generation = components[0].generation;
		start          = 1;
		winner_idx     = 0;
	}
	// cf_detail(AS_RECORD, "merge: new generation %d",r->generation);
	for (uint16_t i = start; i < n_components; i++) {
		as_record_merge_component *c = &components[i];
		switch (rsv->ns->conflict_resolution_policy) {
			case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION:
				if (c->generation > max_generation || (c->generation == max_generation && c->void_time > max_void_time)) {
					max_void_time  = c->void_time;
					max_generation = c->generation;
					winner_idx = (int32_t)i;
				}
				break;
			case AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_TTL:
				if (c->void_time > max_void_time || (c->void_time == max_void_time && c->generation > max_generation)) {
					max_void_time = c->void_time;
					max_generation = c->generation;
					winner_idx = (int32_t)i;
				}
				break;
			default:
				cf_crash(AS_RECORD, "invalid conflict resolution policy");
				break;
		}
	}
	return winner_idx;
}

int
as_record_flatten(as_partition_reservation *rsv, cf_digest *keyd,
		uint16_t n_components, as_record_merge_component *components,
		int *winner_idx)
{
	static char * meth = "as_record_flatten()";
	cf_debug(AS_RECORD, "flatten start: ");

	if (! as_storage_has_space(rsv->ns)) {
		cf_warning(AS_RECORD, "{%s}: record_flatten: drives full", rsv->ns->name);
		return -1;
	}

	// Validate the reservation. This is a WORKAROUND for a known crash
	if ((rsv->tree == 0) || (rsv->ns == 0) || (rsv->p == 0)) {
		cf_info(AS_RECORD, "record merge: bad reservation. tree %p ns %p part %p", rsv->tree, rsv->ns, rsv->p);
		return(-1);
	}

	// look up base record
	as_index_ref r_ref;
	r_ref.skip_lock     = false;
	as_index_tree *tree = rsv->tree;
	bool is_subrec      = false;


	// If the incoming component is the SUBRECORD it should have come as
	// part of MIGRATION... and there will be only 1 component currently.
	// assert the fact
	if (rsv->ns->ldt_enabled) {
		if (COMPONENT_IS_MIG(&components[0])) {
			// Currently the migration is single record at a time merge
			if (n_components > 1) {
				cf_warning(AS_RECORD, "Unexpected function call parameter ... n_components = %d", n_components);
				return (-1);
			}

			if (COMPONENT_IS_LDT_SUB(&components[0])) {

				cf_detail_digest(AS_LDT, keyd, "LDT_MERGE merge component is LDT_SUB %d", components[0].flag);

				if (as_ldt_merge_component_is_candidate(rsv, &components[0]) == false) {
					cf_debug_digest(AS_LDT, keyd, "LDT subrec is not a merge candidate");
					return 0;
				}

				if ((rsv->sub_tree == 0)) {
					cf_warning(AS_RECORD, "[LDT]<%s:%s>record merge: bad reservation. sub tree %p",
							MOD, meth, rsv->sub_tree);
					return(-1);
				}
				tree        = rsv->sub_tree;
				is_subrec   = true;
				*winner_idx = 0;
			} else {
				cf_detail_digest(AS_RECORD, keyd, "LDT_MERGE merge component is NON LDT_SUB %d", components[0].flag);
			}
		} else {
			// In non-migration i.e duplicate resolution code path digest being
			// operated on at current node is is always for non ldt record or 
			// ldt parent record. is_subrec should always be false
			is_subrec = false;
		}
	}

	bool has_local_copy = false;
	as_index  *r        = NULL;
	int ret             = as_record_get_create(tree, keyd, &r_ref, rsv->ns, is_subrec);
	if (-1 == ret) {
		cf_debug_digest(AS_RECORD, keyd, "{%s} record flatten: could not get-create record %b", rsv->ns->name, is_subrec);
		return(-3);
	} else if (ret) {
		has_local_copy  = false;
		r               = r_ref.r;
	} else {
		has_local_copy  = true;
		r               = r_ref.r;
	}
	// DO NOT check for subrecord generation. If the parent generation wins
	// (check above in *_merge_candidate) we should should have winner_idx
	// set. 
	// Note: In all likelihood the incoming SUBRECORD will not have a local
	// copy because the digest comes with the migrate_ldt_version already stamped
	// in it. Even if it matches just go ahead and write it down.
	if (!is_subrec) {
		if (has_local_copy) {
			*winner_idx = as_record_component_winner(rsv, n_components, components, r);
		} else {
			*winner_idx = as_record_component_winner(rsv, n_components, components, NULL);
		}
	}
	
	// Remote Winner
	int  rv              = 0;
	bool delete_record = false;
	if (*winner_idx != -1) {
		cf_detail(AS_LDT, "Flatten Record Remote LDT Winner @ %d", *winner_idx);
		as_record_merge_component *c = &components[*winner_idx];

		if (COMPONENT_IS_LDT_DUMMY(c)) {
			// Case 1:
			// In case the winning component is remote and is dummy (ofcourse flatten
			// is called under reply to duplicate resolution request) return -2. Caller
			// would ship operation to the winning node!!
			if (COMPONENT_IS_MIG(c)) {
				cf_warning(AS_RECORD, "DUMMY LDT Component in Non Duplicate Resolution Code");
				rv = -1;
			} else {
				cf_detail(AS_LDT, "Ship Operation");
				// NB: DO NOT CHANGE THIS RETURN. IT MEANS A SPECIAL THING TO THE CALLER
				rv = -2;
			}
		} else {
			// Case 2:
			// In case the winning component is remote then write it locally. Create record
			// in case there is no local copy of record.
			cf_detail_digest(AS_RECORD, keyd, "is_subrec (%d) Local (%d:%d) Remote (%d:%d)", is_subrec, r->generation, r->void_time, c->generation, c->void_time);

			as_storage_rd rd;
			if (has_local_copy) {
				as_storage_record_open(rsv->ns, r_ref.r, &rd, keyd);
			} else {
				as_storage_record_create(rsv->ns, r_ref.r, &rd, keyd);
			}

			// NB: Side effect of this function is this closes the record
			rv = as_record_flatten_component(rsv, &rd, &r_ref, c, &delete_record);
		}

		// delete newly created index above if there is no local copy
		if (rv && !has_local_copy) {
			as_index_delete(rsv->tree, keyd);
		}
	} else {
		cf_assert(has_local_copy, AS_RECORD, CF_CRITICAL,
				"Local Copy Won when there is no local copy");
		cf_detail_digest(AS_LDT, keyd, "Local Copy Win [%d %d] %d winner_idx=%d", r->generation, components[0].generation, r->void_time, winner_idx);
	}

	// our reservation must still be valid here. Check it.
	if ((rsv->tree == 0) || (rsv->ns == 0) || (rsv->p == 0)) {
		cf_info(AS_RECORD, "record merge: bad reservation. tree %p ns %p part %p", rsv->tree, rsv->ns, rsv->p);
		return(-1);
	}

	// and after here it's GONE
	as_record_done(&r_ref, rsv->ns);

	if (delete_record) {
		as_transaction tr;
		as_transaction_init(&tr, keyd, NULL);
		tr.rsv = *rsv;
		write_delete_local(&tr, false, 0, false);
	}
	return rv;
}

// TODO - inline this, if/when we unravel header files.
bool
as_record_is_expired(as_record *r)
{
	return r->void_time != 0 && r->void_time < as_record_void_time_get();
}
