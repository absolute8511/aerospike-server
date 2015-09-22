/*
 * index.h
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

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"

#include "base/datamodel.h"


//==========================================================
// Index tree node - as_index, also known as as_record.
//
// There's one for every record. Contains metadata, and
// points to record data in memory and/or on storage device.
//

typedef struct as_index_s {

	// offset: 0
	cf_atomic32 rc;		// must be on 4-byte boundary

	// offset: 4
	cf_digest key;		// 20 bytes

	// offset: 24
	cf_arenax_handle right_h;
	cf_arenax_handle left_h;
	cf_arenax_handle parent_h;

	// offset: 36
	// Don't use the free bits here for record info - this is accessed outside
	// the record lock.
	uint32_t color: 1; // one bit
	uint32_t unused_but_unsafe: 31;

	// Everything below here is used under the record lock.

	// offset: 40
	uint32_t void_time;

	// offset: 44
	uint16_t generation;

	// offset: 46
	// Used by the storage engines.
	union {
		struct {
			uint64_t rblock_id: 34;		// can address 2^34 * 128b = 2Tb drive
			uint64_t n_rblocks: 14;		// is enough for 1Mb/128b = 8K rblocks
			uint64_t file_id: 6;		// can spec 2^6 = 64 drives
			uint64_t set_id_bits: 10;	// do not use directly, used for set-ID
		} ssd;
		struct {
			uint32_t file_id: 6;
		} kv;
	} storage_key;

	// offset: 54
	// This byte is currently unused.
	uint8_t flex_bits_1;

	// offset: 55
	// In single-bin mode for data-in-memory namespaces, this is cast to an
	// as_bin, though only the last 2 bits get used (for the iparticle state).
	// The next-to-last 2 bits, or in multi-bin mode the last 4 bits, are used
	// for index flags. Do not use flex_bits_2 directly - use access functions!
	uint8_t flex_bits_2;

	// offset: 56
	// For data-not-in-memory namespaces, these 8 bytes are currently unused.
	// For data-in-memory namespaces: in single-bin mode the as_bin is embedded
	// here (these 8 bytes plus the last 2 bits in flex_bits_2 above), but in
	// multi-bin mode this is a pointer to either of:
	// - an as_bin_space containing n_bins and an array of as_bin structs
	// - an as_rec_space containing an as_bin_space pointer and other metadata
	void* dim;

	// final size: 64

} __attribute__ ((__packed__)) as_index;


//==========================================================
// Accessor functions for bits in as_index.
//

// Size in bytes of as_index, currently the same for all namespaces.
static inline
uint32_t as_index_size_get(as_namespace *ns)
{
	return (uint32_t)sizeof(as_index);
}

// Clear the record portion of as_index - excluding variable part, for speed.
// Note - relies on current layout and size of as_index!
static inline
void as_index_clear_record_info(as_index *index) {
	uint64_t *p_clear = (uint64_t*)&index->void_time;

	*p_clear++	= 0;
	*p_clear++	= 0;
	*p_clear	= 0;
}


//------------------------------------------------
// Flex bits - flags.
//

typedef struct as_index_flag_bits_s {
	uint8_t flag_bits: 4;
	uint8_t do_not_use: 4; // These are used in single-bin mode.
} as_index_flag_bits;

typedef enum {
	AS_INDEX_FLAG_SPECIAL_BINS	= 0x01, // first user of this is LDT (to denote sub-records)
	AS_INDEX_FLAG_CHILD_REC		= 0x02, // child record of a regular record (LDT)
	AS_INDEX_FLAG_CHILD_ESR		= 0x04, // special child existence sub-record (ESR)
	AS_INDEX_FLAG_KEY_STORED	= 0x08, // for data-in-memory, dim points to as_rec_space

	// Combinations:
	AS_INDEX_ALL_FLAGS			= 0x0F
} as_index_flag;

static inline
bool as_index_is_flag_set(const as_index* index, as_index_flag flag) {
	return (((as_index_flag_bits*)&index->flex_bits_2)->flag_bits & flag) != 0;
}

static inline
uint8_t as_index_get_flags(const as_index* index) {
	return ((as_index_flag_bits*)&index->flex_bits_2)->flag_bits;
}

static inline
void as_index_set_flags(as_index* index, as_index_flag flags) {
	((as_index_flag_bits*)&index->flex_bits_2)->flag_bits |= flags;
}

static inline
void as_index_clear_flags(as_index* index, as_index_flag flags) {
	((as_index_flag_bits*)&index->flex_bits_2)->flag_bits &= ~flags;
}


//------------------------------------------------
// Flex bits - bins, as_bin_space & as_rec_space.
//

static inline
as_bin *as_index_get_single_bin(const as_index *index) {
	// We're only allowed to use the last 2 bits for the bin state.
	return (as_bin*)&index->flex_bits_2;
}

static inline
as_bin_space* as_index_get_bin_space(const as_index *index) {
	return as_index_is_flag_set(index, AS_INDEX_FLAG_KEY_STORED) ?
		   ((as_rec_space*)index->dim)->bin_space : (as_bin_space*)index->dim;
}

static inline
void as_index_set_bin_space(as_index* index, as_bin_space* bin_space) {
	if (as_index_is_flag_set(index, AS_INDEX_FLAG_KEY_STORED)) {
		((as_rec_space*)index->dim)->bin_space = bin_space;
	}
	else {
		index->dim = (void*)bin_space;
	}
}


//------------------------------------------------
// Set-ID bits.
//

static inline
uint16_t as_index_get_set_id(as_index *index) {
	return index->storage_key.ssd.set_id_bits;
}

static inline
void as_index_set_set_id(as_index *index, uint16_t set_id) {
	// TODO - check that it fits in the 10 bits ???
	index->storage_key.ssd.set_id_bits = set_id;
}

static inline
bool as_index_has_set(as_index *index) {
	return index->storage_key.ssd.set_id_bits != 0;
}


//------------------------------------------------
// Set-ID helpers.
//

static inline
int as_index_set_set(as_index *index, as_namespace *ns, const char *set_name,
		bool fail_if_deleted) {
	uint16_t set_id;
	int rv = as_namespace_get_create_set(ns, set_name, &set_id,
			fail_if_deleted);

	if (rv != 0) {
		return rv;
	}

	as_index_set_set_id(index, set_id);
	return 0;
}

static inline
const char *as_index_get_set_name(as_index *index, as_namespace *ns) {
	// TODO - don't really need this check - remove?
	if (! as_index_has_set(index)) {
		return NULL;
	}

	return as_namespace_get_set_name(ns, as_index_get_set_id(index));
}


//==========================================================
// Handling as_index objects.
//

// Container for as_index pointer with lock and location.
struct as_index_ref_s {
	bool				skip_lock;
	as_index			*r;
	cf_arenax_handle	r_h;
	pthread_mutex_t		*olock;
};

// Callback invoked when as_index is destroyed.
typedef void (*as_index_value_destructor) (as_index *v, void *udata);


//==========================================================
// Index tree.
//

typedef struct as_index_tree_s {
	// Note: reduce_lock's scope is always inside of lock's scope.
	pthread_mutex_t		lock;        // insert, delete vs. insert, delete, get
	pthread_mutex_t		reduce_lock; // insert, delete vs. reduce

	as_index			*root;
	cf_arenax_handle	root_h;

	as_index			*sentinel;
	cf_arenax_handle	sentinel_h;

	as_index_value_destructor destructor;
	void				*destructor_udata;

	cf_arenax			*arena; // where we allocate and free to

	uint32_t			elements; // not making this atomic, it's not very exact
	uint16_t			data_inmemory;
} as_index_tree;


//------------------------------------------------
// as_index_tree public API.
//

// When inserting, the reference count of the value is *consumed* by the insert
// if it succeeds.
//
// Except for get-insert, which is a little weird. If a value is returned (thus
// a different value is gotten), its ref-count is increased. If the value is
// inserted (and not returned), the ref-count is consumed.

extern int as_index_ref_initialize(as_index_tree *tree, cf_digest *key, as_index_ref *index_ref, bool create_p, as_namespace *ns);

// 1 = new, 0 = found, -1 = error.
extern int as_index_get_insert_vlock(as_index_tree *tree, cf_digest *key, as_index_ref *index_ref);

// This call is more unusual. It does not take the object lock but does take the
// ref-count. Thus, the caller (as_record_get) must take the o_lock.
extern int as_index_get_vlock(as_index_tree *tree, cf_digest *key, as_index_ref *index_ref);

extern int as_index_exists(as_index_tree *tree, cf_digest *key);

// 0 is ok, -1 is fail, -2 is key not found.
extern int as_index_delete(as_index_tree *tree, cf_digest *key);

extern as_index_tree *as_index_tree_create(cf_arenax *arena, as_index_value_destructor destructor, void *destructor_udata, as_treex *p_treex);
extern as_index_tree *as_index_tree_resume(cf_arenax *arena, as_index_value_destructor destructor, void *destructor_udata, as_treex *p_treex);

#define as_index_tree_reserve(_t) cf_atomic32_incr(&(_t)->rc)
extern int as_index_tree_release(as_index_tree *tree, void *destructor_udata);

#define as_index_reserve(_r) cf_atomic32_incr(&(_r->rc))
#define as_index_release(_r) cf_atomic32_decr(&(_r->rc))

// Number of elements in the tree.
extern uint32_t as_index_tree_size(as_index_tree *tree);

// These reduce functions give a reference count of the value: you must release
// it and it contains, internally, code to not block the tree lock - so you can
// spend as much time in the reduce function as you want.
typedef void (*as_index_reduce_fn) (as_index_ref *value, void *udata);
extern void as_index_reduce(as_index_tree *tree, as_index_reduce_fn cb, void *udata);
extern void as_index_reduce_partial(as_index_tree *tree, uint32_t sample_count, as_index_reduce_fn cb, void *udata);

// as_index_reduce_sync() doesn't take a reference, and holds the tree lock.
typedef void (*as_index_reduce_sync_fn) (as_index *value, void *udata);
extern void as_index_reduce_sync(as_index_tree *tree, as_index_reduce_sync_fn cb, void *udata);

