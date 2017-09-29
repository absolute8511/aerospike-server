/*
 * vmapx.c
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


//==========================================================
// Includes.
//

#include "vmapx.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_hash_math.h"


//==========================================================
// Forward declarations.
//

bool vhash_get(const vhash* h, const char* key, size_t key_len, uint32_t* p_value);


//==========================================================
// Public API.
//

//------------------------------------------------
// Return persistent memory size needed. Includes
// cf_vmap struct plus values vector.
//
size_t
cf_vmapx_sizeof(uint32_t value_size, uint32_t max_count)
{
	return sizeof(cf_vmapx) + ((size_t)value_size * (size_t)max_count);
}

//------------------------------------------------
// Create a cf_vmapx object in persistent memory.
//
cf_vmapx_err
cf_vmapx_create(cf_vmapx* this, uint32_t value_size, uint32_t max_count,
		uint32_t hash_size, uint32_t max_name_size)
{
	// Value-size needs to be a multiple of 4 bytes for thread safety.
	if ((value_size & 3) != 0 || max_count == 0 || hash_size == 0 ||
			max_name_size == 0 || max_name_size > value_size) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	this->value_size = value_size;
	this->max_count = max_count;
	this->count = 0;

	if (! (this->p_hash = vhash_create(max_name_size, hash_size))) {
		return CF_VMAPX_ERR_UNKNOWN;
	}

	this->key_size = max_name_size;

	pthread_mutex_init(&this->write_lock, 0);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Free internal resources of a cf_vmapx object.
// Don't call after failed cf_vmapx_create() or
// cf_vmapx_resume() call - those functions clean
// up on failure.
//
void
cf_vmapx_release(cf_vmapx* this)
{
	// Helps in handling bins vmap, which doesn't exist in single-bin mode.
	if (! this) {
		return;
	}

	pthread_mutex_destroy(&this->write_lock);

	vhash_destroy(this->p_hash);
}

//------------------------------------------------
// Return count.
//
uint32_t
cf_vmapx_count(const cf_vmapx* this)
{
	return this->count;
}

//------------------------------------------------
// Get value by index.
//
cf_vmapx_err
cf_vmapx_get_by_index(const cf_vmapx* this, uint32_t index, void** pp_value)
{
	// This check is commented out for now to avoid the volatile access.
	// TODO - ultimately, caller code can be simplified. (Especially if this
	// just returned the value pointer.) And if necessary, we could make a
	// "safe" version of this that does the check.

//	if (index >= this->count) {
//		return CF_VMAPX_ERR_BAD_PARAM;
//	}

	*pp_value = cf_vmapx_value_ptr(this, index);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get value by null-terminated name.
//
cf_vmapx_err
cf_vmapx_get_by_name(const cf_vmapx* this, const char* name, void** pp_value)
{
	size_t name_len = strlen(name);

	if (name_len >= this->key_size) {
		return CF_VMAPX_ERR_NAME_NOT_FOUND;
	}

	uint32_t index;

	if (! vhash_get(this->p_hash, name, name_len, &index)) {
		return CF_VMAPX_ERR_NAME_NOT_FOUND;
	}

	*pp_value = cf_vmapx_value_ptr(this, index);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get index by null-terminated name. May pass
// null p_index to just check existence.
//
cf_vmapx_err
cf_vmapx_get_index(const cf_vmapx* this, const char* name, uint32_t* p_index)
{
	size_t name_len = strlen(name);

	if (name_len >= this->key_size) {
		return CF_VMAPX_ERR_NAME_NOT_FOUND;
	}

	return vhash_get(this->p_hash, name, name_len, p_index) ?
			CF_VMAPX_OK : CF_VMAPX_ERR_NAME_NOT_FOUND;
}

//------------------------------------------------
// Same as above, but non-null-terminated name.
//
cf_vmapx_err
cf_vmapx_get_index_w_len(const cf_vmapx* this, const char* name,
		size_t name_len, uint32_t* p_index)
{
	if (name_len >= this->key_size) {
		return CF_VMAPX_ERR_NAME_NOT_FOUND;
	}

	return vhash_get(this->p_hash, name, name_len, p_index) ?
			CF_VMAPX_OK : CF_VMAPX_ERR_NAME_NOT_FOUND;
}

//------------------------------------------------
// The value must begin with a string which is its
// name. (The hash map is not stored in persistent
// memory, so names must be in the vector to
// enable us to rebuild the hash map on warm
// restart.)
//
// If name is not found, add new name, clear rest
// of value in vector, and return newly assigned
// index (and CF_VMAPX_OK). If name is found,
// return index for existing value (with
// CF_VMAPX_ERR_NAME_EXISTS). May pass null
// p_index.
//
cf_vmapx_err
cf_vmapx_put_unique(cf_vmapx* this, const char* name, uint32_t* p_index)
{
	return cf_vmapx_put_unique_w_len(this, name, strlen(name), p_index);
}

//------------------------------------------------
// Same as above, but with known name length.
//
cf_vmapx_err
cf_vmapx_put_unique_w_len(cf_vmapx* this, const char* name, size_t name_len,
		uint32_t* p_index)
{
	// Make sure name fits in key's allocated size.
	if (name_len >= this->key_size) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	pthread_mutex_lock(&this->write_lock);

	// If name is found, return existing name's index, ignore p_value.
	if (vhash_get(this->p_hash, name, name_len, p_index)) {
		pthread_mutex_unlock(&this->write_lock);
		return CF_VMAPX_ERR_NAME_EXISTS;
	}

	// Make sure name has no illegal premature null-terminator.
	for (uint32_t i = 0; i < name_len; i++) {
		if (name[i] == 0) {
			pthread_mutex_unlock(&this->write_lock);
			return CF_VMAPX_ERR_BAD_PARAM;
		}
	}

	uint32_t count = this->count;

	// If vmap is full, can't add more.
	if (count >= this->max_count) {
		pthread_mutex_unlock(&this->write_lock);
		return CF_VMAPX_ERR_FULL;
	}

	// Add name to vector (and clear rest of value).
	char* value_ptr = (char*)cf_vmapx_value_ptr(this, count);

	memset((void*)value_ptr, 0, this->value_size);
	memcpy((void*)value_ptr, name, name_len);

	// Increment count here so indexes returned by other public API calls (just
	// after adding to hash below) are guaranteed to be valid.
	this->count++;

	// Add to hash.
	if (! vhash_put(this->p_hash, value_ptr, name_len, count)) {
		this->count--;

		pthread_mutex_unlock(&this->write_lock);
		return CF_VMAPX_ERR_UNKNOWN;
	}

	pthread_mutex_unlock(&this->write_lock);

	if (p_index) {
		*p_index = count;
	}

	return CF_VMAPX_OK;
}


//==========================================================
// Private functions.
//

//------------------------------------------------
// Return value pointer at trusted index.
//
void*
cf_vmapx_value_ptr(const cf_vmapx* this, uint32_t index)
{
	return (void*)(this->values + (this->value_size * index));
}


//==========================================================
// vhash "scoped class".
//

// Custom hashmap for cf_vmapx usage.
// - Elements are added but never removed.
// - It's thread safe yet lockless. (Relies on cf_vmapx's write_lock.)
// - Element keys are null-terminated strings.
// - Element values are uint32_t's.

struct vhash_s {
	uint32_t key_size;
	uint32_t ele_size;
	uint32_t n_rows;
	uint8_t* table;
	uint32_t row_counts[];
};

typedef struct vhash_ele_s {
	struct vhash_ele_s* next;
	uint8_t data[]; // key_size bytes of key, 4 bytes of value
} vhash_ele;

#define VHASH_ELE_KEY_PTR(_e)		((char*)_e->data)
#define VHASH_ELE_VALUE_PTR(_h, _e)	((uint32_t*)(_e->data + _h->key_size))

// Copy null-terminated key into hash, then pad with non-null characters.
// Padding ensures quicker compare in vhash_get() when key in hash is shorter,
// and prevents accidental match if key param has illegal null character(s).
static inline void
vhash_set_ele_key(char* ele_key, size_t key_size, const char* zkey,
		size_t zkey_size)
{
	memcpy((void*)ele_key, (const void*)zkey, zkey_size);
	memset((void*)(ele_key + zkey_size), 'x', key_size - zkey_size);
}

//------------------------------------------------
// Create vhash with specified key size (max) and
// number or rows.
//
vhash*
vhash_create(uint32_t key_size, uint32_t n_rows)
{
	size_t row_counts_size = n_rows * sizeof(uint32_t);
	vhash* h = (vhash*)cf_malloc(sizeof(vhash) + row_counts_size);

	if (! h) {
		return NULL;
	}

	h->key_size = key_size;
	h->ele_size = sizeof(vhash_ele) + key_size + sizeof(uint32_t);
	h->n_rows = n_rows;

	size_t table_size = n_rows * h->ele_size;

	if (! (h->table = (uint8_t*)cf_malloc(table_size))) {
		cf_free(h);
		return NULL;
	}

	memset((void*)h->row_counts, 0, row_counts_size);
	memset((void*)h->table, 0, table_size);

	return h;
}

//------------------------------------------------
// Destroy vhash. (Assumes it was fully created.)
//
void
vhash_destroy(vhash* h)
{
	vhash_ele* e_table = (vhash_ele*)h->table;

	for (uint32_t i = 0; i < h->n_rows; i++) {
		if (e_table->next) {
			vhash_ele* e = e_table->next;

			while (e) {
				vhash_ele* t = e->next;

				cf_free(e);
				e = t;
			}
		}

		e_table = (vhash_ele*)((uint8_t*)e_table + h->ele_size);
	}

	cf_free(h->table);
	cf_free(h);
}

//------------------------------------------------
// Add element. Key must be null-terminated,
// although its length is known.
//
bool
vhash_put(vhash* h, const char* zkey, size_t key_len, uint32_t value)
{
	uint64_t hashed_key = cf_hash_fnv32((const uint8_t*)zkey, key_len);
	uint32_t row_i = (uint32_t)(hashed_key % h->n_rows);

	vhash_ele* e = (vhash_ele*)(h->table + (h->ele_size * row_i));

	if (h->row_counts[row_i] == 0) {
		vhash_set_ele_key(VHASH_ELE_KEY_PTR(e), h->key_size, zkey, key_len + 1);
		*VHASH_ELE_VALUE_PTR(h, e) = value;
		h->row_counts[row_i]++;

		return true;
	}

	vhash_ele* e_head = e;

	// This function is always called under write lock, after get, so we'll
	// never encounter the key - don't bother checking it.
	while (e) {
		e = e->next;
	}

	e = (vhash_ele*)cf_malloc(h->ele_size);

	if (! e) {
		return false;
	}

	e->next = e_head->next;
	e_head->next = e;

	vhash_set_ele_key(VHASH_ELE_KEY_PTR(e), h->key_size, zkey, key_len + 1);
	*VHASH_ELE_VALUE_PTR(h, e) = value;
	h->row_counts[row_i]++;

	return true;
}

//------------------------------------------------
// Get element value. Key may or may not be
// null-terminated.
//
bool
vhash_get(const vhash* h, const char* key, size_t key_len, uint32_t* p_value)
{
	uint64_t hashed_key = cf_hash_fnv32((const uint8_t*)key, key_len);
	uint32_t row_i = (uint32_t)(hashed_key % h->n_rows);
	uint32_t row_count = h->row_counts[row_i];

	if (row_count == 0) {
		return false;
	}

	vhash_ele* e = (vhash_ele*)(h->table + (h->ele_size * row_i));

	// Use row count instead of following pointers to the end, for thread
	// safety with concurrent put.
	for (uint32_t j = 0; j < row_count; j++) {
		if (VHASH_ELE_KEY_PTR(e)[key_len] == 0 &&
				memcmp(VHASH_ELE_KEY_PTR(e), key, key_len) == 0) {
			if (p_value) {
				*p_value = *VHASH_ELE_VALUE_PTR(h, e);
			}

			return true;
		}

		e = e->next;
	}

	return false;
}
