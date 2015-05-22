/*
 * rchash.h
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

/**
 * A general purpose hashtable implementation
 * Uses locks, so only moderately fast
 * Just, hopefully, the last hash table you'll ever need
 * And you can keep adding cool things to it
 */

#pragma once
#include <stdint.h>
#include <citrusleaf/cf_rchash.h>

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define RCHASH_ERR_FOUND		CF_RCHASH_ERR_FOUND
#define RCHASH_ERR_NOTFOUND		CF_RCHASH_ERR_NOTFOUND
#define RCHASH_ERR_BUFSZ		CF_RCHASH_ERR_BUFSZ
#define RCHASH_ERR				CF_RCHASH_ERR
#define RCHASH_OK				CF_RCHASH_OK
#define RCHASH_REDUCE_DELETE	CF_RCHASH_REDUCE_DELETE

#define RCHASH_CR_RESIZE 0x01   // support resizes (will sometimes hang for long periods)
#define RCHASH_CR_GRAB   0x02   // support 'grab' call (requires more memory)
#define RCHASH_CR_MT_BIGLOCK 0x04 // support multithreaded access with a single big lock
#define RCHASH_CR_MT_MANYLOCK 0x08 // support multithreaded access with a pool of object loccks
#define RCHASH_CR_NOSIZE 0x10 // don't calculate the size on every call, which makes 'getsize' expensive if you ever call it

#define RCHASH_CR_RESIZE 0x01   // support resizes (will sometimes hang for long periods)
#define RCHASH_CR_MT_BIGLOCK 0x04 // support multithreaded access with a single big lock
#define RCHASH_CR_MT_LOCKPOOL 0x08 // support multithreaded access with a pool of object loccks



/******************************************************************************
 * TYPE ALIASES
 ******************************************************************************/

typedef struct cf_rchash_s rchash;
typedef struct cf_rchash_elem_v_s rchash_elem_v;
typedef struct cf_rchash_elem_f_s rchash_elem_f;
typedef uint32_t (*rchash_hash_fn) (void *value, uint32_t value_len);
typedef int (*rchash_reduce_fn) (void *key, uint32_t keylen, void *object, void *udata);
typedef void (*rchash_destructor_fn) (void *object);

/******************************************************************************
 * FUNCTION ALIASES
 ******************************************************************************/

#define rchash_create cf_rchash_create
#define rchash_set_nlocks cf_rchash_set_nlocks
#define rchash_put cf_rchash_put
#define rchash_put_unique cf_rchash_put_unique
#define rchash_get cf_rchash_get
#define rchash_delete cf_rchash_delete
#define rchash_get_size cf_rchash_get_size
#define rchash_reduce cf_rchash_reduce
#define rchash_destroy cf_rchash_destroy
#define rchash_dump cf_rchash_dump
