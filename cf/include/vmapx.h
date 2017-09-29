/*
 * vmapx.h
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
 * A vector of fixed-size values, also accessible by name, which operates in
 * persistent memory.
 *
 */

#pragma once


//==========================================================
// Includes
//

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_shash.h>


//==========================================================
// Typedefs
//

// DO NOT access this member data directly - use the API!
typedef struct cf_vmapx_s {
	// Vector-related
	uint32_t		value_size;
	uint32_t		max_count;
	cf_atomic32		count;

	// Hash-related
	shash*			p_hash;
	uint32_t		key_size;

	// Generic
	pthread_mutex_t	write_lock;

	// Vector Data
	uint8_t			values[];
} cf_vmapx;

typedef enum {
	CF_VMAPX_OK = 0,
	CF_VMAPX_ERR_BAD_PARAM,
	CF_VMAPX_ERR_FULL,
	CF_VMAPX_ERR_NAME_EXISTS,
	CF_VMAPX_ERR_NAME_NOT_FOUND,
	CF_VMAPX_ERR_UNKNOWN
} cf_vmapx_err;


//==========================================================
// Public API
//

//------------------------------------------------
// Persistent Size (cf_vmapx struct + values)
//
size_t cf_vmapx_sizeof(uint32_t value_size, uint32_t max_count);

//------------------------------------------------
// Constructor
//
cf_vmapx_err cf_vmapx_create(cf_vmapx* _this, uint32_t value_size,
		uint32_t max_count, uint32_t hash_size, uint32_t max_name_size);

//------------------------------------------------
// Destructor
//
void cf_vmapx_release(cf_vmapx* _this);


//------------------------------------------------
// Number of Values
//
uint32_t cf_vmapx_count(cf_vmapx* _this);

//------------------------------------------------
// Get a Value
//
cf_vmapx_err cf_vmapx_get_by_index(cf_vmapx* _this, uint32_t index,
		void** pp_value);
cf_vmapx_err cf_vmapx_get_by_name(cf_vmapx* _this, const char* name,
		void** pp_value);

//------------------------------------------------
// Get Index from Name
//
cf_vmapx_err cf_vmapx_get_index(cf_vmapx* _this, const char* name,
		uint32_t* p_index);

//------------------------------------------------
// Add a Value (if name is unique)
//
cf_vmapx_err cf_vmapx_put_unique(cf_vmapx* _this, const void* p_value,
		uint32_t* p_index);


//==========================================================
// Private API - for enterprise separation only
//
cf_vmapx_err cf_vmapx_resume(cf_vmapx* _this, uint32_t hash_size, uint32_t max_name_size);

uint32_t cf_vmapx_hash_fn(void* p_key);
void* cf_vmapx_value_ptr(cf_vmapx* _this, uint32_t index);
