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
#include <time.h> // for as_record_void_time_get() - TODO - replace with clock function
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

	if (ns->allow_versions) {
		as_index_vinfo_mask_set(r, 0, ns->allow_versions);
	}

	// clear everything owned by record
	r->migrate_mark = 0;
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

		uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(&rd);
		cf_atomic_int_sub(&ns->n_bytes_memory, memory_bytes);

		cf_debug(AS_RECORD, " record destroy: %s subtracting: memory %"PRIu64,
				ns->name, memory_bytes);

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
		*buf++ = as_bin_get_version(b, rd->ns->single_bin);

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

// the unpickle merge
// takes an existing record, and lays in the incoming data
// This code works only when allow version is true. Behaviour of sindex in that scenario is not well defined.
int
as_record_unpickle_merge(as_record *r, as_storage_rd *rd, uint8_t *buf, size_t sz, uint8_t **stack_particles, bool *record_written)
{
	cf_crash(AS_RECORD, "as_record_unpickle_merge() should be unreachable");
	return(0);
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

	int32_t  delta_bins   = (int32_t)newbins - (int32_t)old_n_bins;
	int      sindex_ret   = AS_SINDEX_OK;
	int      sindex_found = 0;

	if (has_sindex) {
		SINDEX_GRLOCK();
	}
	
	// To read the algorithm of upating sindex in bins check notes in ssd_record_add function.
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);

	if ((delta_bins < 0) && has_sindex) {
		 sindex_found += as_sindex_sbins_from_rd(rd, newbins, old_n_bins, &sbins[sindex_found], AS_SINDEX_OP_DELETE);
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
		uint8_t version  = *buf++;
		as_bin *b;
		if (i < old_n_bins) {
			b = &rd->bins[i];
			if (has_sindex) {
				sindex_found      += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sindex_found], AS_SINDEX_OP_DELETE);
			}
			as_bin_set_version(b, version, ns->single_bin);
			as_bin_set_id_from_name_buf(ns, b, name, name_sz);
		}
		else {
			b = as_bin_create(r, rd, name, name_sz, version);
		}

		if (ns->storage_data_in_memory) {
			as_bin_particle_replace_from_pickled(b, &buf);
		}
		else {
			*stack_particles += as_bin_particle_stack_from_pickled(b, *stack_particles, &buf);
		}

		if (has_sindex) {
			sindex_found += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sindex_found], AS_SINDEX_OP_INSERT);
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
		if (has_sindex && sindex_found) {
			sindex_ret = as_sindex_update_by_sbin(ns, set_name, sbins, sindex_found, &rd->keyd);
			if (sindex_ret != AS_SINDEX_OK) {
				cf_warning(AS_RECORD, "Failed: %s", as_sindex_err_str(sindex_ret));
			}
		}
		rd->write_to_device = true;
	}


	if (has_sindex && sindex_found) {
		as_sindex_sbin_freeall(sbins, sindex_found);
	}

	return ret;
}


//
// AS RECORD VINFO
//
//
// This function gets the mask of a particular vinfo from the partition's vinfoset.
// Many other functions here are deferred time, this on is in the main data path
// of every transaction and should be considered carefully.
//
// SIDE EFFECT: if the version did not exist in the table, adds it.
// If no space, will return 0.


as_partition_vinfo_mask
as_record_vinfo_mask_get_lockfree( as_partition_vinfoset *vinfoset, as_partition_vinfo *vinfo  )
{
	uint sz = vinfoset->sz;

	// validate that the incoming vinfo is correct
	if ((vinfo->iid == 0) || (vinfo->vtp[0] == 0)) {
		cf_info(AS_RECORD, "mask get: can't get mask for invalid VTP, internal error");
		return(0);
	}

	for (int i = 0 ; i < sz ; i++) {
		as_partition_vinfo *v = &vinfoset->vinfo_a[i];
		if ( (v->iid == vinfo->iid) && (memcmp(v->vtp, vinfo->vtp, sizeof(v->vtp)) == 0) ) {
			return ( 1 << i );
		}
	}

	// not yet set. See if there's an empty slot inside we can use
	uint use_slot = sz;
	for (int i = 0; i < sz; i++) {
		if (vinfoset->vinfo_a[i].iid == 0) {
			use_slot = i;
			break;
		}
	}

	if (use_slot == AS_PARTITION_VINFOSET_SIZE) {
		cf_debug(AS_RECORD, "set vinfo mask: no slot left for new record");
		return(0);
	}

	// update the table first
	as_partition_vinfo *v = &vinfoset->vinfo_a[use_slot];
	v->iid = vinfo->iid;
	memcpy(v->vtp, vinfo->vtp, AS_PARTITION_MAX_VERSION);


	// then the size
	if (use_slot >= vinfoset->sz) vinfoset->sz = use_slot + 1;

	as_partition_vinfo_mask mask = 1 << use_slot;

	if (use_slot >= vinfoset->sz) {
		cf_info(AS_RECORD, "mask-get-lockfree: illegal size: use_slot %d sz %d", use_slot, vinfoset->sz);
	}

	// as_partition_vinfoset_dump(vinfoset, "mask_get_lockfree: after insert");

	return ( mask );

}

as_partition_vinfo_mask
as_record_vinfo_mask_get( as_partition *p, as_partition_vinfo *vinfo  )
{
	// cf_detail(AS_RECORD, "write path getting mask for partition %p",p);


	if (0 != pthread_mutex_lock(&p->vinfoset_lock))
		cf_info(AS_RECORD, "vinfo mask get: mutex fail");

	as_partition_vinfo_mask m = as_record_vinfo_mask_get_lockfree(&p->vinfoset, vinfo);

	if (0 != pthread_mutex_unlock(&p->vinfoset_lock))
		cf_info(AS_RECORD, "vinfo mask get mutex unlock fail");

	return(m);
}

//
// Get the entire mask that matches this vinfoset, within the partition in question
//
// SIDE EFFECT: any vinfo entry that didn't exist will get created
//

as_partition_vinfo_mask
as_record_vinfoset_mask_get( as_partition *p, as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask)
{
	pthread_mutex_lock(&p->vinfoset_lock);

	if (mask == 0) mask = (1 << vinfoset->sz) - 1;

	as_partition_vinfo_mask omask = mask;

	as_partition_vinfo_mask accum = 0;
	do {
		int idx = ffsll(mask);
		if (idx == 0) break;
		idx--;
		as_partition_vinfo *v = &vinfoset->vinfo_a[idx];
		if (v->iid == 0) {
			cf_info(AS_RECORD, "vinfoset_mask_get: mask to no vinfo, INTERNAL ERROR omask %x idx %d", omask, idx);
			as_partition_vinfoset_dump(vinfoset, "INTERNAL ERROR");
			break;
		}
		accum |= as_record_vinfo_mask_get_lockfree(&p->vinfoset, v );
		mask  &= ~(1 << idx);
	} while(1);

	pthread_mutex_unlock(&p->vinfoset_lock);

	as_record_vinfoset_mask_validate(&p->vinfoset, accum);

	return ( accum );
}

bool
as_record_vinfoset_mask_validate(as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask)
{
#ifdef EXTRA_CHECKS
	as_partition_vinfo_mask omask = mask;
#endif
	do {
		int idx = ffsll(mask);
		if (idx == 0) break;
		idx--;
		as_partition_vinfo *v = &vinfoset->vinfo_a[idx];
		if (v->iid == 0) {
#ifdef EXTRA_CHECKS
			cf_warning(AS_RECORD, "vinfoset_mask_get: invalid, position %d is empty (originally %x)", idx, omask);
			as_partition_vinfoset_dump(vinfoset, "   invalid: set is");
#endif
			return(false);
		}
		if (v->vtp[0] == 0) {
#ifdef EXTRA_CHECKS
			cf_warning(AS_RECORD, "vinfoset_mask_get: invalid, position %d has bad vtp (originally %x)", idx, omask);
			as_partition_vinfoset_dump(vinfoset, "   invalid: set is");
#endif
			return(false);
		}
		mask &= ~(1 << idx);
	} while(1);

	return(true);
}

// loop through the current bins, collecting
// the versions in a bitmask, then use 'find first set'
// to find the first cleared one

int
as_record_unused_version_get(as_storage_rd *rd)
{
	if (rd->ns->single_bin)
		return (0);

	uint vmask = 0;
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];
		if (as_bin_inuse(b)) {
			vmask |= (1 << as_bin_get_version(b, rd->ns->single_bin));
		}
	}

	int rv = ffsll( ~vmask );
	if (rv == 0) {
		cf_info(AS_RECORD, "unused version get: no available versions. serious error");
		return(-1);
	}
	return( rv - 1 );
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

//
// merge incoming data into whatever is in the local store
//
// returns
// -1 : In case record cannot be created
// -2 : In case components have LDT and record merge cannot
//      be done
//
int
as_record_merge(as_partition_reservation *rsv, cf_digest *keyd, uint16_t n_components,
		as_record_merge_component *components)
{
	cf_debug(AS_RECORD, "merge start: ");

	if (! as_storage_has_space(rsv->ns)) {
		cf_warning(AS_RECORD, "{%s}: record_merge: drives full", rsv->ns->name);
		return -1;
	}

	// Validate the reservation. This is a WORKAROUND for a known crash
	if ((rsv->tree == 0) || (rsv->ns == 0) || (rsv->p == 0)) {
		cf_info(AS_RECORD, "record merge: bad reservation. tree %p ns %p part %p", rsv->tree, rsv->ns, rsv->p);
		return(-1);
	}

	as_index_ref r_ref;
	r_ref.skip_lock     = false;
	as_index_tree *tree = rsv->tree;

	if (rsv->ns->ldt_enabled) {
		cf_warning(AS_LDT, "Merge not allowed on the namespace %s with ldt enabled.. Merge Aborted !!", rsv->ns->name);
		return -1;
	}

	// NB: LDT subrecord cannot be merged ... it is unknown either
	//     one wins or other ... Also LDT SUB currently won't deal
	//     with merge. Need to deal with a given binname winning
	//     in one version and losing in the other version. Assert if
	//     we come here.
	for (uint16_t i_c = 0; i_c < n_components; i_c++) {
		if (COMPONENT_IS_LDT(&components[i_c])) {
			cf_warning(AS_LDT, "LDT components cannot be merged ... "
					"possibly namespace configured related to (allow-version / ldt-enabled) "
					"have mismatch in cluster.. falling back to flatten");
			// may be resort back to flatten/replace
			return -2;
		}
	}

	int rv = as_record_get_create(tree, keyd, &r_ref, rsv->ns, false);
	if (rv == -1) {
		cf_warning_digest(AS_RECORD, keyd, "{%s} record merge: could not get-create record ", rsv->ns->name);
		return(-1);
	}
	as_record *r = r_ref.r;

	as_storage_rd rd;
	uint64_t memory_bytes = 0;

	if (rv == 1) {
		as_storage_record_create(rsv->ns, r, &rd, keyd);
	}
	else {
		as_storage_record_open(rsv->ns, r, &rd, keyd);
	}

	int      n_generations = (r->generation == 0) ? 0 : 1; // trying
	uint32_t generation = r->generation;
	uint32_t void_time  = r->void_time;

	bool has_sindex = as_sindex_ns_has_sindex(rd.ns);

	rd.ignore_record_on_device = rd.ns->single_bin; // TODO - set to ! has_sindex
	rd.n_bins = as_bin_get_n_bins(r, &rd);

	uint16_t newbins = 0;
	for (uint16_t i_c = 0 ; i_c < n_components ; i_c++) {
		newbins += ntohs( *(uint16_t *) components[i_c].record_buf );
	}

	if (! rd.ns->storage_data_in_memory && ! rd.ns->single_bin) {
		rd.n_bins += newbins;
	}

	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	if (rv != 1 && rd.ns->storage_data_in_memory) {
		memory_bytes = as_storage_record_get_n_bytes_memory(&rd);
	}

	uint32_t stack_particles_sz = 0;
	if (! rd.ns->storage_data_in_memory) {
		for (uint16_t i_c = 0; i_c < n_components; i_c++) {
			as_record_merge_component *c = &components[i_c];
			stack_particles_sz += as_record_buf_get_stack_particles_sz(c->record_buf);
		}
	}

	uint8_t stack_particles[stack_particles_sz]; // stack allocate space for new particles when data on device
	uint8_t *p_stack_particles = stack_particles;

	// for each duplicate,
	for (uint16_t i_c = 0; i_c < n_components; i_c++) {

		cf_detail(AS_RECORD, "merging component %d %"PRIx64, i_c, *(uint64_t *)keyd);

		as_record_merge_component *c = &components[i_c];

		// Overwrite properties upfront before pickling. Code downstream uses it to update
		// secondary index.
		as_record_set_properties(&rd, &c->rec_props);
		//
		// If the incoming vinfo set is empty, then simply compare the values of the incoming record with the existing record
		//
		if (c->vinfoset.sz == 0) {
			// this is a little unusual in scoping because I want to use stack
			// allocated structures here - the number of bins won't be large
			// (although I should protect myself....)

			bool record_written = false;

			as_record_unpickle_merge(r, &rd, c->record_buf, c->record_buf_sz, &p_stack_particles, &record_written);

			if (record_written) {
				cf_debug(AS_RECORD, " wrote a record with vinfo set missing during migrate");
				as_index_vinfo_mask_union( r, as_record_vinfo_mask_get(rsv->p, &rsv->p->version_info ), rd.ns->allow_versions);
				if (c->generation) {
					generation = MAX(generation, c->generation);
					n_generations++;
				}
				void_time = MAX(void_time, c->void_time);
			}
		}
		// If the incoming record is a superset of the current, allow it to overwrite
		// overwrites never create duplicates, so prefer such
		else if (as_partition_vinfoset_superset_vinfoset(&rsv->p->vinfoset,
					as_index_vinfo_mask_get(r, rd.ns->allow_versions), &c->vinfoset)) {

			// cf_detail(AS_RECORD, " superset: overlay: %"PRIx64,*(uint64_t *)keyd);

			// this section of unpickle will overwite because the new info is a superset

			as_record_unpickle_replace(r, &rd, c->record_buf, c->record_buf_sz, &p_stack_particles, has_sindex);

			as_index_vinfo_mask_union( r, as_record_vinfoset_mask_get( rsv->p, &c->vinfoset, 0), rd.ns->allow_versions);

			if (c->generation) {
				generation = MAX(generation, c->generation);
				n_generations++;
			}
			void_time = MAX(void_time, c->void_time);
		}
		// decide whether to merge in
		else if (! as_partition_vinfoset_contains_vinfoset(&rsv->p->vinfoset,
					as_index_vinfo_mask_get(r, rd.ns->allow_versions), &c->vinfoset, 0, false/*debug*/)) {

			//            cf_info(AS_RECORD, "merge has work to do: %"PRIx64,*(uint64_t *)keyd);

			// merge in!
			bool record_written = false;
			as_record_unpickle_merge(r, &rd, c->record_buf, c->record_buf_sz, &p_stack_particles, &record_written);

			// update mask
			as_index_vinfo_mask_union(r, as_record_vinfoset_mask_get( rsv->p, &c->vinfoset, 0), rd.ns->allow_versions);

			if (! as_record_vinfoset_mask_validate(&rsv->p->vinfoset,
						as_index_vinfo_mask_get(r, rd.ns->allow_versions))) {
				cf_debug(AS_RECORD, "vinfoset mask merge: invalid!");
			}

			// continue to calculate generation
			if (c->generation) {
				generation = MAX(generation, c->generation);
				n_generations++;
			}
			void_time = MAX(void_time, c->void_time);
			cf_detail(AS_RECORD, "merge: updated vinfo mask %x %"PRIx64,
					as_index_vinfo_mask_get(r, rd.ns->allow_versions), *(uint64_t *)keyd);
		}
		else {
			cf_detail(AS_RECORD, "merge has NO work to do %"PRIx64, *(uint64_t *)keyd);
		}
	}

	// stamp in generation
	r->generation = generation;

	if (n_generations > 1) {
		r->generation++;
	}

	// The generation might wrap - 0 is reserved as "uninitialized". (Also, this
	// fixes legacy deployments' generation 0.)
	if (r->generation == 0) {
		r->generation = 1;
	}

	r->void_time = void_time;
	r->migrate_mark = 0;

#ifdef EXTRA_CHECKS
	// an EXTRA CHECK - should have some bins
	uint16_t n_bins_check = 0;
	for (uint16_t i = 0; i < rd.n_bins; i++) {
		if (as_bin_inuse(&rd.bins[i])) n_bins_check++;
	}
	if (n_bins_check == 0) cf_info(AS_RECORD, "merge: extra check: after write, no bins. peculiar.");
#endif

	if (rd.ns->storage_data_in_memory) {
		// I think only at this point is the value newly-correct. Not sure this could ever
		// shrink...
		uint64_t end_memory_bytes = as_storage_record_get_n_bytes_memory(&rd);

		int64_t delta_bytes = end_memory_bytes - memory_bytes;
		if (delta_bytes) {
			cf_atomic_int_add(&rsv->ns->n_bytes_memory, delta_bytes);
			cf_atomic_int_add(&rsv->p->n_bytes_memory, delta_bytes);
		}
	}

	// cf_info(AS_RECORD, "merge: new generation %d",r->generation);

	// write record to device
	as_storage_record_close(r, &rd);

	// our reservation must still be valid here. Check it.
	if ((rsv->tree == 0) || (rsv->ns == 0) || (rsv->p == 0)) {
		cf_info(AS_RECORD, "record merge: bad reservation. tree %p ns %p part %p", rsv->tree, rsv->ns, rsv->p);
		return(-1);
	}

	// and after here it's GONE
	as_record_done(&r_ref, rsv->ns);

	// NB: rd->n_bins is intact as rd is in intact
	//     rd->bins is intact as stack_bins above is intact
	if (! as_bin_inuse_has(&rd)) {
		// INIT_TR
		as_transaction tr;
		as_transaction_init(&tr, keyd, NULL);
		tr.rsv = *rsv;
		write_delete_local(&tr, false, 0, false);
	}

	return(0);
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

	uint64_t memory_bytes = 0;
	if (rd->ns->storage_data_in_memory) {
		memory_bytes = as_storage_record_get_n_bytes_memory(rd);
	}

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

	if (rd->ns->storage_data_in_memory) {
		uint64_t end_memory_bytes = as_storage_record_get_n_bytes_memory(rd);
		int64_t delta_bytes = end_memory_bytes - memory_bytes;
		if (delta_bytes) {
			cf_atomic_int_add(&rsv->ns->n_bytes_memory, delta_bytes);
			cf_atomic_int_add(&rsv->p->n_bytes_memory, delta_bytes);
		}
	}
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
		cf_warning_digest(AS_RECORD, keyd, "{%s} record flatten: could not get-create record %b", rsv->ns->name, is_subrec);
		return(-1);
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

	r->migrate_mark = 0;

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

//
// Version info comparision
//  returns TRUE if vs2 is contained in vs1
//
bool
as_partition_vinfo_contains(as_partition_vinfo *v1, as_partition_vinfo *v2)
{
	if (v1->iid != v2->iid) return(false);

	for (uint i = 0 ; i < AS_PARTITION_MAX_VERSION ; i++) {

		if (v2->vtp[i] == 0) return(true); // reached the end of v2 first, is contained

		if (v2->vtp[i] != v1->vtp[i]) return(false); // just don't match at this length
	}

	return(true); // worrisome, means we ran out of versions
}

//
// Determine whether vs2 is contained in fs1
//

bool
as_partition_vinfoset_contains_vinfoset(as_partition_vinfoset *vs1,
		as_partition_vinfo_mask mask1, as_partition_vinfoset *vs2,
		as_partition_vinfo_mask mask2, bool debug)
{
	if (debug) {
		cf_info(AS_RECORD, "vinfoset contains vinfoset: mask1 %x mask2 %x", mask1, mask2);
		//    as_partition_vinfoset_dump(vs1, "vinfoset contains vinfoset: vs1");
		//    as_partition_vinfoset_dump(vs2, "vinfoset contains vinfoset: vs2");
	}

	if (mask2 == 0) {
		mask2 = (1 << vs2->sz) - 1;
		if(debug) cf_info(AS_RECORD, "vinfoset contains: setting mask2 to %x", mask2);
	}

	int idx2;
	while ((idx2 = ffsl(mask2))) {

		idx2--;
		as_partition_vinfo *v2 = &vs2->vinfo_a[idx2];

		int idx1;
		while ((idx1 = ffsll(mask1))) {

			idx1--;

			as_partition_vinfo *v1 = &vs1->vinfo_a[idx1];

			if (true == as_partition_vinfo_contains(v1, v2))
				return(true);

			if (debug) {
				cf_info(AS_RECORD, "  checked idx1 %d against idx2 %d: no contains", idx1, idx2);
			}

			mask1 &= ~(1 << idx1);

			if (debug) cf_info(AS_RECORD, "mask1 changed: now %d", mask1);

		}
		mask2 &= ~(1 << idx2);

		if (debug) cf_info(AS_RECORD, "mask2 changed: now %d", mask2);

	}
	return(false);
}


//
// Determine whether vs2 is a superset of v1 (must not be equal)
//

bool
as_partition_vinfoset_superset_vinfoset(as_partition_vinfoset *vs1,
		as_partition_vinfo_mask mask1, as_partition_vinfoset *vs2)
{
	// cf_info(AS_RECORD, "vinfoset superset vinfoset: mask1 %x",mask1);
	// as_partition_vinfoset_dump(vs1, "vinfoset superset vinfoset: vs1");
	// as_partition_vinfoset_dump(vs2, "vinfoset superset vinfoset: vs2");

	// check some trivial cases
	if (mask1 == 0) {
		if (vs2->sz > 0)  {
			return(true);
		}
		return(false);
	}

	int msize = 0;
	int idx1;
	while ((idx1 = ffsl(mask1))) {

		idx1--;
		as_partition_vinfo *v1 = &vs1->vinfo_a[idx1];

		bool found = false;
		for (int i = 0; i < vs2->sz; i++) {

			as_partition_vinfo *v2 = &vs2->vinfo_a[i];

			if (as_partition_vinfo_contains(v2, v1)) {
				found = true;
				break;
			}

		}

		if (found == false) {
			return(false);
		}

		mask1 &= ~(1 << idx1);
		msize++;
	}

	// need an extra check - they might be equal
	if (msize == vs2->sz) {
		return(false);
	}

	return(true);
}

//
// Pickle a set in its entirety, allowing for subsequent masks to work
//

int
as_partition_vinfoset_pickle( as_partition_vinfoset *vinfoset, uint8_t *buf, size_t *sz_r)
{
	cf_detail(AS_RECORD, "vinfoset_pickle: size %d", vinfoset->sz);

	int buf_sz = sizeof(uint32_t) + (vinfoset->sz * (sizeof(uint64_t) + AS_PARTITION_MAX_VERSION + 1));
	if (buf_sz > *sz_r) {
		cf_debug(AS_RECORD, "vinfoset_pickle: too small input buffer: need %d have %zd", buf_sz, *sz_r);
		*sz_r = buf_sz;
		return(-1);
	}
	*sz_r = buf_sz;
	uint8_t *buf_lim = buf + buf_sz; // debug
	*(uint32_t *)buf = vinfoset->sz;
	buf += sizeof(uint32_t);

	for (int i = 0; i < vinfoset->sz; i++) {
		as_partition_vinfo *v = &vinfoset->vinfo_a[i];
		(*(uint64_t *) buf) = v->iid;
		buf += sizeof(uint64_t);
		*buf++ = AS_PARTITION_MAX_VERSION;
		memcpy(buf, v->vtp, AS_PARTITION_MAX_VERSION);
		buf += AS_PARTITION_MAX_VERSION;
	}

	if (buf > buf_lim) {
		cf_crash(AS_RECORD, "pickle record overwriting data");
	}

	return(0);
}

//
// pickles a set and a mask into the minimal format that represents this particular state
//
int
as_partition_vinfoset_mask_pickle( as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask, uint8_t *buf, size_t *sz_r)
{
	// a zero mask is easy
	if (mask == 0) {
		if (*sz_r < 4) {
			*sz_r = 4;
			return(-1);
		}
		buf[0] = buf[1] = buf[2] = buf[3] = 0;
		*sz_r = 4;
		return(0);
	}

	as_partition_vinfo_mask mask_sav = mask;
	int i;

	// determine length
	int sz = 0;
	while ((i = ffsll(mask))) { // TODO: bit counter instead of a loop?
		i--;
		sz++;
		mask &= ~(1 << i);
	}
	mask = mask_sav;

	if (sz == 0) {
		cf_debug(AS_RECORD, "attempting to pickle info mask of NO SIZE mask %x", mask);
		buf[0] = buf[1] = buf[2] = buf[3] = 0;
		*sz_r = 4;
		return(0);
	}

	if (sz > AS_PARTITION_VINFOSET_SIZE || sz < 0) {
		cf_debug(AS_RECORD, "vinfo set size greater than max allowed size %d - send empty mask", sz);
		buf[0] = buf[1] = buf[2] = buf[3] = 0;
		*sz_r = 4;
		return(0);
	}

	// check length of incoming buf
	size_t buf_sz = sizeof(uint32_t) + (sz * (sizeof(uint64_t) + AS_PARTITION_MAX_VERSION + 1));
	if (buf_sz > *sz_r) {
		cf_warning(AS_RECORD, "attempting to pickle vinfo mask - not enough size: need %d got %zd", buf_sz, *sz_r);
		*sz_r = buf_sz;
		return(-1);
	}
	*sz_r = buf_sz;

	// lay down the pickled version!
	uint8_t *obuf = buf; // DEBUG
	(*(uint32_t *)buf) = sz;
	buf += sizeof(uint32_t);

	while ((i = ffsll(mask))) {
		i--;
		as_partition_vinfo *v = &vinfoset->vinfo_a[i];
		(*(uint64_t *) buf) = v->iid;
		buf += sizeof(uint64_t);
		*buf++ = AS_PARTITION_MAX_VERSION;
		memcpy(buf, v->vtp, AS_PARTITION_MAX_VERSION);
		buf += AS_PARTITION_MAX_VERSION;

#ifdef EXTRA_CHECKS
		if (v->iid == 0) {
			cf_warning(AS_RECORD, "pickling invalid vinfoset: 0 iid");
#ifdef BREAK_VTP_ERROR
			raise(SIGINT);
#endif
		}
		if (v->vtp[0] == 0) {
			cf_warning(AS_RECORD, "pickling invalid vinfoset: 0 vtp");
#ifdef EXTRA_CHECKS
			raise(SIGINT);
#endif
		}
#endif

		mask &= ~ ( 1 << i );
	}

	// check reputed length against actual laid down
	if (buf_sz != buf - obuf) {
		cf_info(AS_RECORD, " vinfoset_mask_pickle: internal error, wrong length");
	}

	return(0);
}

int
as_partition_vinfoset_mask_pickle_getsz( as_partition_vinfo_mask mask, size_t *sz_r)
{
	as_partition_vinfo_mask mask_sav = mask;
	int i;

	// determine length
	int sz = 0;
	while ((i = ffsll(mask))) { // TODO: bit counter instead of a loop?
		i--;
		sz++;
		mask &= ~(1 << i);
	}
	mask = mask_sav;

	if (sz == 0) {
		cf_debug(AS_RECORD, "attempting to pickle info mask of NO SIZE mask %x", mask);
		*sz_r = 4;
		return(0);
	}

	// check length of incoming buff
	*sz_r = sizeof(uint32_t) + (sz * (sizeof(uint64_t) + AS_PARTITION_MAX_VERSION + 1));

	return(0);

}


//
// Unpickles a set
// Whether it can be used with a 'mask' depends on how it was pickled
//
as_partition_vinfo_mask
as_partition_vinfoset_mask_unpickle( as_partition *p, uint8_t *buf, size_t buf_sz)
{
	uint8_t *lim = buf + buf_sz;

	if (buf_sz < sizeof(uint32_t)) {
		cf_debug(AS_RECORD, "received vinfoset for unpickling with too little size");
		return(0);
	}

	uint32_t n_vinfo = *(uint32_t *)buf;
	if (n_vinfo == 0) {
		cf_debug(AS_RECORD, "received vinfo set with no records, denotes unknown state, ok\n");
		return(0);
	}

	if (n_vinfo >= AS_PARTITION_VINFOSET_SIZE) {
		cf_debug(AS_RECORD, "received vinfo set with too many elements: %d, max allowed %d", n_vinfo, AS_PARTITION_VINFOSET_SIZE);
		goto Error;
	}
	buf += sizeof(uint32_t);

	as_partition_vinfo_mask mask = 0;

	for (uint i = 0; i < n_vinfo; i++) {
		as_partition_vinfo v;

		// todo: should check against buf_sz to make sure there's no overflow
		if (lim <= buf) {
			cf_info(AS_RECORD, "ran off the end unpickling");
			goto Error;
		}

		v.iid = *(uint64_t *)buf;
		buf += sizeof(uint64_t);
		uint8_t sz = *buf++;
		if (sz == AS_PARTITION_MAX_VERSION) {
			memcpy(v.vtp, buf, sz);
			buf += sz;
		}
		else if (sz == 0) {
			// if we receive an illegal, return whatever we've got
			cf_info(AS_RECORD, "received vinfo set with 0 length in vtp, illegal");
			goto Error;
		}
		else if (sz == AS_PARTITION_MAX_VERSION) {
			memcpy(v.vtp, buf, sz);
			buf += sz;
		}
		else if (sz < AS_PARTITION_MAX_VERSION) {
			memcpy(v.vtp, buf, sz);
			memset(&v.vtp[sz], 0, AS_PARTITION_MAX_VERSION - sz);
			buf += sz;
		}
		else { // longer than we have, corruption
			cf_info(AS_RECORD, "received vinfo set with too-large vtp table size %d, failing", sz);
			goto Error;
		}

#ifdef EXTRA_CHECKS
		// some more validation
		if (v.iid == 0) {
			cf_info(AS_RECORD, "received vinfoset with 0 iid, illegal: index %d");
#ifdef BREAK_VTP_ERROR
			raise(SIGINT);
#endif
			goto Error;
		}
		if (v.vtp[0] == 0) {
			cf_info(AS_RECORD, "received vinfoset with 0 iid, illegal: index %d");
#ifdef BREAK_VTP_ERROR
			raise(SIGINT);
#endif
			goto Error;
		}
#endif

		// as_partition_vinfo_dump(&v, "vinfoset mask unpickle: ");

		mask |= as_record_vinfo_mask_get(p, &v );

	}
	return(mask);

Error:
	// we've received some kind of illegal entry, our best effort is a zero mask, meaning unknown
	// vinfoset state
	return(0);
}

//
// Unpickles a set
// Whether it can be used with a 'mask' depends on how it was pickled
//
int
as_partition_vinfoset_unpickle( as_partition_vinfoset *vinfoset, uint8_t *buf, size_t buf_sz, char *msg)
{
	uint8_t *lim = buf + buf_sz;

	if (buf_sz < sizeof(uint32_t)) {
		cf_debug(AS_RECORD, "received vinfoset for unpickling with too little size : %zu (need 4) %s", buf_sz, msg);
		return(-1);
	}

	uint32_t n_vinfo = *(uint32_t *)buf;
	if (n_vinfo == 0) {
		memset(vinfoset, 0, sizeof(as_partition_vinfoset));
		cf_debug(AS_RECORD, "received vinfo set with 0 size, unusual %s", msg);
		return(0);
	}

	if (n_vinfo >= AS_PARTITION_VINFOSET_SIZE) {
		cf_warning(AS_RECORD, "received vinfo set with too many elements: %d, max allowed %d %s", n_vinfo, AS_PARTITION_VINFOSET_SIZE, msg);
		goto Error;
	}
	buf += sizeof(uint32_t);


	// super safe
	memset(vinfoset, 0, sizeof(as_partition_vinfoset));
	// for (uint i=n_vinfo;i<AS_PARTITION_VINFOSET_SIZE;i++) // 0 out what's not there
	// 		vinfoset->vinfo_a[i].iid = 0;

	vinfoset->sz = n_vinfo;
	for (uint i = 0; i < n_vinfo; i++) {
		if (buf >= lim) {
			cf_warning(AS_RECORD, "ran off end of vinfoset during unpickle, serious internal error %s", msg);
			goto Error;
		}
		// todo: should check against buf_sz to make sure there's no overflow
		as_partition_vinfo *v = &vinfoset->vinfo_a[i];
		v->iid = *(uint64_t *)buf;
		buf += sizeof(uint64_t);
		uint8_t sz = *buf++;
		if (sz == AS_PARTITION_MAX_VERSION) {
			memcpy(v->vtp, buf, sz);
			buf += sz;
		}
		else if (sz == 0) {
			cf_debug(AS_RECORD, "received vinfo set with 0 length in vtp, illegal %s", msg);
			goto Error;
		}
		else if (sz < AS_PARTITION_MAX_VERSION) {
			memcpy(v->vtp, buf, sz);
			memset(&v->vtp[sz], 0, AS_PARTITION_MAX_VERSION - sz);
			buf += sz;
		}
		else { // longer than we have, fail
			cf_debug(AS_RECORD, "received vinfo set with too-large vtp table size %d, failing %s", sz, msg);
			goto Error;
		}

		// see if there's anything interesting to validate
		if (v->iid == 0) {
			cf_info(AS_RECORD, "received and unpickling illegal vinfoset: 0 iid in position %d %s", i, msg);
			goto Error;
		}

	}

	return(0);

Error:
	// if we've just received a fully illegal item, our best effort is to set up a vinfoset
	// with 0 length, denoting an unknown partition set state.

	memset(vinfoset, 0, sizeof(as_partition_vinfoset));
	return(0);
}



void
as_partition_vinfoset_dump(as_partition_vinfoset *vinfoset, char *msg)
{
	cf_info(AS_RECORD, "%s: dump vinfoset: sz %d", msg, vinfoset->sz);
	for (int i = 0 ; i < vinfoset->sz ; i++) {
		cf_info(AS_RECORD, "  idx %d : iid %"PRIx64, i, vinfoset->vinfo_a[i].iid);
		cf_info(AS_RECORD, "  vtp %d : %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x", i,
				vinfoset->vinfo_a[i].vtp[0], vinfoset->vinfo_a[i].vtp[1], vinfoset->vinfo_a[i].vtp[2],
				vinfoset->vinfo_a[i].vtp[3], vinfoset->vinfo_a[i].vtp[4], vinfoset->vinfo_a[i].vtp[5],
				vinfoset->vinfo_a[i].vtp[6], vinfoset->vinfo_a[i].vtp[7], vinfoset->vinfo_a[i].vtp[8],
				vinfoset->vinfo_a[i].vtp[9], vinfoset->vinfo_a[i].vtp[10], vinfoset->vinfo_a[i].vtp[11] );
	}
}

void
as_partition_vinfoset_mask_dump(as_partition_vinfoset *vinfoset, as_partition_vinfo_mask mask, char *msg)
{
	cf_info(AS_RECORD, "%s: dump vinfoset: mask %x sz %d", msg, mask, vinfoset->sz);
	for (int i = 0 ; i < vinfoset->sz ; i++) {
		cf_info(AS_RECORD, "  idx %d : %s : iid %"PRIx64, i, ((1 << i)& mask) ? "VALID" : "UNUSED", vinfoset->vinfo_a[i].iid);
		cf_info(AS_RECORD, "  vtp %d : %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x", i,
				vinfoset->vinfo_a[i].vtp[0], vinfoset->vinfo_a[i].vtp[1], vinfoset->vinfo_a[i].vtp[2],
				vinfoset->vinfo_a[i].vtp[3], vinfoset->vinfo_a[i].vtp[4], vinfoset->vinfo_a[i].vtp[5],
				vinfoset->vinfo_a[i].vtp[6], vinfoset->vinfo_a[i].vtp[7], vinfoset->vinfo_a[i].vtp[8],
				vinfoset->vinfo_a[i].vtp[9], vinfoset->vinfo_a[i].vtp[10], vinfoset->vinfo_a[i].vtp[11] );
	}
}


void
as_partition_vinfo_dump(as_partition_vinfo *vinfo, char *msg)
{
	cf_info(AS_RECORD, "vinfo dump: %s: iid %"PRIx64, msg, vinfo->iid);
	cf_info(AS_RECORD, "  vtp : %02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x %02x %02x",
			vinfo->vtp[0], vinfo->vtp[1], vinfo->vtp[2], vinfo->vtp[3], vinfo->vtp[4], vinfo->vtp[5],
			vinfo->vtp[6], vinfo->vtp[7], vinfo->vtp[8], vinfo->vtp[9], vinfo->vtp[10], vinfo->vtp[11] );
}


// TOD0 - seems redundant, should use clock function.
uint32_t
as_record_void_time_get()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return ts.tv_sec - CITRUSLEAF_EPOCH;
}
