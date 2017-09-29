/*
 * vmapx.c
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


//==========================================================
// Includes
//

#include "vmapx.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_shash.h>

#include "util.h"


//==========================================================
// Forward Declarations
//

static bool get_index(cf_vmapx* this, const char* name, uint32_t* p_index);


//==========================================================
// Public API
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
	if ((value_size & 3) || ! max_count || ! hash_size || ! max_name_size) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	this->value_size = value_size;
	this->max_count = max_count;
	this->count = 0;

	if (shash_create(&this->p_hash, cf_vmapx_hash_fn, max_name_size,
			sizeof(uint32_t), hash_size, SHASH_CR_MT_MANYLOCK) != SHASH_OK) {
		return CF_VMAPX_ERR_UNKNOWN;
	}

	this->key_size = max_name_size;

	if (pthread_mutex_init(&this->write_lock, 0) != 0) {
		shash_destroy(this->p_hash);

		return CF_VMAPX_ERR_UNKNOWN;
	}

	return CF_VMAPX_OK;
}

// Resume a cf_vmapx object from persistent memory.
//
cf_vmapx_err
cf_vmapx_resume(cf_vmapx* this,  uint32_t hash_size, uint32_t max_name_size)
{
	uint32_t value_size;
	uint32_t max_count;
	char key[this->key_size];

	value_size = this->value_size;
	max_count = this->max_count;

	// Value-size needs to be a multiple of 4 bytes for thread safety.
	if ((value_size & 3) || ! max_count || ! hash_size || ! max_name_size) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	if (shash_create(&this->p_hash, cf_vmapx_hash_fn, max_name_size,
			sizeof(uint32_t), hash_size, SHASH_CR_MT_MANYLOCK) != SHASH_OK) {
		return CF_VMAPX_ERR_UNKNOWN;
	}

	// resume the hash struct
	for (int i=0; i<this->count; i++) {
		strncpy(key, (const char*)cf_vmapx_value_ptr(this, i), this->key_size);

		if (shash_put(this->p_hash, key, &i) != SHASH_OK) {
			return CF_VMAPX_ERR_UNKNOWN;
		}
	}

	this->key_size = max_name_size;

	if (pthread_mutex_init(&this->write_lock, 0) != 0) {
		shash_destroy(this->p_hash);

		return CF_VMAPX_ERR_UNKNOWN;
	}

	return CF_VMAPX_OK;
}


//------------------------------------------------

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

	shash_destroy(this->p_hash);
}

//------------------------------------------------
// Return count.
//
uint32_t
cf_vmapx_count(cf_vmapx* this)
{
	return cf_atomic32_get(this->count);
}

//------------------------------------------------
// Get value by index.
//
cf_vmapx_err
cf_vmapx_get_by_index(cf_vmapx* this, uint32_t index, void** pp_value)
{
	if (index >= cf_atomic32_get(this->count)) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	*pp_value = cf_vmapx_value_ptr(this, index);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get value by name.
//
cf_vmapx_err
cf_vmapx_get_by_name(cf_vmapx* this, const char* name, void** pp_value)
{
	uint32_t index;

	if (! get_index(this, name, &index)) {
		return CF_VMAPX_ERR_NAME_NOT_FOUND;
	}

	*pp_value = cf_vmapx_value_ptr(this, index);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get index by name. May pass null p_index to
// just check existence.
//
cf_vmapx_err
cf_vmapx_get_index(cf_vmapx* this, const char* name, uint32_t* p_index)
{
	return get_index(this, name, p_index) ?
			CF_VMAPX_OK : CF_VMAPX_ERR_NAME_NOT_FOUND;
}

//------------------------------------------------
// The value must begin with a null-terminated
// string which is its name. (The hash map is not
// stored in persistent memory, so names must be
// in the vector to enable us to rebuild the hash
// map on warm restart.)
//
// If name is not found, add new value and return
// newly assigned index (and CF_VMAPX_OK). If name
// is found, return index for existing name (with
// CF_VMAPX_ERR_NAME_EXISTS) but ignore new value.
// May pass null p_index.
//
cf_vmapx_err
cf_vmapx_put_unique(cf_vmapx* this, const void* p_value, uint32_t* p_index)
{
	// Not using get_index() since we may need key for shash_put() call.
	char key[this->key_size];

	// Pad with nulls to achieve consistent key.
	strncpy(key, (const char*)p_value, this->key_size);

	pthread_mutex_lock(&this->write_lock);

	// If name is found, return existing name's index, ignore p_value.
	if (shash_get(this->p_hash, key, p_index) == SHASH_OK) {
		pthread_mutex_unlock(&this->write_lock);

		return CF_VMAPX_ERR_NAME_EXISTS;
	}

	uint32_t count = cf_atomic32_get(this->count);

	// Not allowed to add more.
	if (count >= this->max_count) {
		pthread_mutex_unlock(&this->write_lock);

		return CF_VMAPX_ERR_FULL;
	}

	// Add to vector.
	memcpy(cf_vmapx_value_ptr(this, count), p_value, this->value_size);

	// Increment count here so indexes returned by other public API calls (just
	// after adding to hash below) are guaranteed to be valid.
	cf_atomic32_incr(&this->count);

	// Add to hash.
	if (shash_put(this->p_hash, key, &count) != SHASH_OK) {
		cf_atomic32_decr(&this->count);

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
// Private Functions
//

//------------------------------------------------
// Return value pointer at trusted index.
//
void*
cf_vmapx_value_ptr(cf_vmapx* this, uint32_t index)
{
	return (void*)(this->values + (this->value_size * index));
}

//------------------------------------------------
// Hash a name string.
//
inline uint32_t
cf_vmapx_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, strlen((const char*)p_key));
}

//------------------------------------------------
// Get index by trusted name.
//
static bool
get_index(cf_vmapx* this, const char* name, uint32_t* p_index)
{
	char key[this->key_size];

	// Pad with nulls to achieve consistent key.
	strncpy(key, name, this->key_size);

	return shash_get(this->p_hash, key, p_index) == SHASH_OK;
}
