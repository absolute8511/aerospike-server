/*
 * arenax.c
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
 * An arena that uses persistent memory.
 */


//==========================================================
// Includes
//

#include "arenax.h"
 
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "fault.h"


//==========================================================
// Constants
//

// Limit so stage_size fits in 32 bits.
// (Probably unnecessary - size_t is 64 bits on our systems.)
const uint64_t MAX_STAGE_SIZE = 0xFFFFffff;

// Must be in-sync with cf_arenax_err:
const char* ARENAX_ERR_STRINGS[] = {
	"ok",
	"bad parameter",
	"error creating stage",
	"error attaching stage",
	"error detaching stage",
	"unknown error"
};


//==========================================================
// Public API
//

//------------------------------------------------
// Return persistent memory size needed. Excludes
// stages, which cf_arenax handles internally.
//
size_t
cf_arenax_sizeof()
{
	return sizeof(cf_arenax);
}

//------------------------------------------------
// Convert cf_arenax_err to meaningful string.
//
const char*
cf_arenax_errstr(cf_arenax_err err)
{
	if (err < 0 || err > CF_ARENAX_ERR_UNKNOWN) {
		err = CF_ARENAX_ERR_UNKNOWN;
	}

	return ARENAX_ERR_STRINGS[err];
}

//------------------------------------------------
// Create a cf_arenax object in persistent memory.
// Also create and attach the first arena stage in
// persistent memory.
//
cf_arenax_err
cf_arenax_create(cf_arenax* this, key_t key_base, uint32_t element_size,
		uint32_t stage_capacity, uint32_t max_stages, uint32_t flags)
{
	if (stage_capacity == 0) {
		stage_capacity = MAX_STAGE_CAPACITY;
	}
	else if (stage_capacity > MAX_STAGE_CAPACITY) {
		cf_warning(CF_ARENAX, "stage capacity %u too large", stage_capacity);
		return CF_ARENAX_ERR_BAD_PARAM;
	}

	if (max_stages == 0) {
		max_stages = CF_ARENAX_MAX_STAGES;
	}
	else if (max_stages > CF_ARENAX_MAX_STAGES) {
		cf_warning(CF_ARENAX, "max stages %u too large", max_stages);
		return CF_ARENAX_ERR_BAD_PARAM;
	}

	uint64_t stage_size = (uint64_t)stage_capacity * (uint64_t)element_size;

	if (stage_size > MAX_STAGE_SIZE) {
		cf_warning(CF_ARENAX, "stage size %lu too large", stage_size);
		return CF_ARENAX_ERR_BAD_PARAM;
	}

	this->key_base = key_base;
	this->element_size = element_size;
	this->stage_capacity = stage_capacity;
	this->max_stages = max_stages;
	this->flags = flags;

	this->stage_size = (size_t)stage_size;

	this->free_h = 0;

	// Skip 0:0 so null handle is never used.
	this->at_stage_id = 0;
	this->at_element_id = 1;

	if ((flags & CF_ARENAX_BIGLOCK) &&
			pthread_mutex_init(&this->lock, 0) != 0) {
		return CF_ARENAX_ERR_UNKNOWN;
	}

	this->stage_count = 0;
	memset(this->stages, 0, sizeof(this->stages));

	// Add first stage.
	cf_arenax_err result = cf_arenax_add_stage(this);

	if (result == CF_ARENAX_OK) {
		// Clear the null element - allocation bypasses it, but it may be read.
		memset(cf_arenax_resolve(this, 0), 0, element_size);
	}
	// No need to detach - add_stage() won't fail and leave attached stage.
	else if (this->flags & CF_ARENAX_BIGLOCK) {
		pthread_mutex_destroy(&this->lock);
	}

	return result;
}

//------------------------------------------------
// Allocate an element within the arena.
//
cf_arenax_handle
cf_arenax_alloc(cf_arenax* this)
{
	if ((this->flags & CF_ARENAX_BIGLOCK) &&
			pthread_mutex_lock(&this->lock) != 0) {
		return 0;
	}

	cf_arenax_handle h;

	// Check free list first.
	if (this->free_h != 0) {
		h = this->free_h;

		free_element* p_free_element = cf_arenax_resolve(this, h);

		this->free_h = p_free_element->next_h;
	}
	// Otherwise keep end-allocating.
	else {
		if (this->at_element_id >= this->stage_capacity) {
			if (cf_arenax_add_stage(this) != CF_ARENAX_OK) {
				if (this->flags & CF_ARENAX_BIGLOCK) {
					pthread_mutex_unlock(&this->lock);
				}

				return 0;
			}

			this->at_stage_id++;
			this->at_element_id = 0;
		}

		cf_arenax_set_handle(&h, this->at_stage_id, this->at_element_id);

		this->at_element_id++;
	}

	if (this->flags & CF_ARENAX_BIGLOCK) {
		pthread_mutex_unlock(&this->lock);
	}

	if (this->flags & CF_ARENAX_CALLOC) {
		memset(cf_arenax_resolve(this, h), 0, this->element_size);
	}

	return h;
}

//------------------------------------------------
// Free an element.
//
void
cf_arenax_free(cf_arenax* this, cf_arenax_handle h)
{
	free_element* p_free_element = cf_arenax_resolve(this, h);

	if ((this->flags & CF_ARENAX_BIGLOCK) &&
			pthread_mutex_lock(&this->lock) != 0) {
		// TODO - function doesn't return failure - just press on?
		return;
	}

	p_free_element->magic = FREE_MAGIC;
	p_free_element->next_h = this->free_h;
	this->free_h = h;

	if (this->flags & CF_ARENAX_BIGLOCK) {
		pthread_mutex_unlock(&this->lock);
	}
}

//------------------------------------------------
// Convert cf_arenax_handle to memory address.
//
void*
cf_arenax_resolve(cf_arenax* this, cf_arenax_handle h)
{
	return this->stages[h >> ELEMENT_ID_NUM_BITS] +
			((h & ELEMENT_ID_MASK) * this->element_size);
}
