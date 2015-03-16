/*
 * dynbuf.c
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

#include "dynbuf.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <asm/byteorder.h>
#include <netinet/in.h>

#include <citrusleaf/alloc.h>

#include "cf_str.h"


size_t
cf_dyn_buf_get_newsize(int alloc, int used, int requested)
{
	if (alloc - used > requested)  return alloc;

	size_t new_sz = alloc + requested + sizeof(cf_buf_builder);
	int backoff;
	if (new_sz < (1024 * 8))
		backoff = 1024;
	else if (new_sz < (1024 * 32))
		backoff = (1024 * 4);
	else if (new_sz < (1024 * 128))
		backoff = (1024 * 32);
	else
		backoff = (1024 * 256);
	new_sz = new_sz + (backoff - (new_sz % backoff));
	return new_sz;
}

//
// Make sure the buf has enough bytes for whatever you're up to.
int
cf_dyn_buf_reserve(cf_dyn_buf *db, size_t sz)
{
	// see if we need more space
	size_t new_sz = cf_dyn_buf_get_newsize(db->alloc_sz, db->used_sz, sz);
	if (new_sz > db->alloc_sz) {
		uint8_t	*_t;
		if (db->is_stack) {
			_t = cf_malloc(new_sz);
			if (!_t)	return(-1);
			memcpy(_t, db->buf, db->used_sz);
			db->is_stack = false;
			db->buf = _t;
		}
		else {
			_t = cf_realloc(db->buf, new_sz);
			if (!_t)	return(-1);
			db->buf = _t;
		}
		db->alloc_sz = new_sz;
	}
	return(0);
}

#define DB_RESERVE(_n) \
	if ( db->alloc_sz - db->used_sz < _n ) { \
		if (0 != cf_dyn_buf_reserve(db, _n)) \
			return(-1); \
	}

int
cf_dyn_buf_append_buf(cf_dyn_buf *db, uint8_t *buf, size_t sz)
{
	DB_RESERVE(sz);
	memcpy(&db->buf[db->used_sz], buf, sz);
	db->used_sz += sz;
	return(0);
}

int
cf_dyn_buf_append_string(cf_dyn_buf *db, const char *s)
{
	size_t	len = strlen(s);
	DB_RESERVE(len);
	memcpy(&db->buf[db->used_sz], s, len);
	db->used_sz += len;
	return ( 0 );
}

int
cf_dyn_buf_append_char (cf_dyn_buf *db, char c)
{
	DB_RESERVE(1);
	db->buf[db->used_sz] = (uint8_t) c;
	db->used_sz++;
	return( 0 );
}

int
cf_dyn_buf_append_int (cf_dyn_buf *db, int i)
{
	// overreserving isn't a crime
	DB_RESERVE(12);
	db->used_sz += cf_str_itoa(i, (char *) &db->buf[db->used_sz], 10);
	return( 0 );
}

int
cf_dyn_buf_append_uint64_x (cf_dyn_buf *db, uint64_t i)
{
	DB_RESERVE(18);
	db->used_sz += cf_str_itoa_u64(i, (char *) &db->buf[db->used_sz], 16);
	return( 0 );
}

int
cf_dyn_buf_append_uint64 (cf_dyn_buf *db, uint64_t i)
{
	DB_RESERVE(22);
	db->used_sz += cf_str_itoa_u64(i, (char *) &db->buf[db->used_sz], 10);
	return( 0 );
}

int
cf_dyn_buf_append_uint32 (cf_dyn_buf *db, uint32_t i)
{
	DB_RESERVE(12);
	db->used_sz += cf_str_itoa_u32(i, (char *) &db->buf[db->used_sz], 10);
	return( 0 );
}


int
cf_dyn_buf_chomp(cf_dyn_buf *db)
{
	if (db->used_sz > 0)
		db->used_sz--;
	return(0);
}

char *
cf_dyn_buf_strdup(cf_dyn_buf *db)
{
	if (db->used_sz == 0)	return(0);
	char *r = cf_malloc(db->used_sz+1);
	if (!r)	return(0);
	memcpy(r, db->buf, db->used_sz);
	r[db->used_sz] = 0;
	return(r);
}

void
cf_dyn_buf_free(cf_dyn_buf *db)
{
	if (db->is_stack == false)
		cf_free(db->buf);
}

//
// Make sure the buf has enough bytes for whatever you're up to.
int
cf_buf_builder_reserve_internal(cf_buf_builder **bb_r, size_t sz)
{
	cf_buf_builder *bb = *bb_r;

	// see if we need more space
	size_t new_sz = cf_dyn_buf_get_newsize(bb->alloc_sz, bb->used_sz, sz);
	if (new_sz > bb->alloc_sz) {
		bb = cf_realloc(bb, new_sz);
		if (!bb)	return(-1);
		bb->alloc_sz = new_sz - sizeof(cf_buf_builder);
		*bb_r = bb;
	}
	return(0);
}

#define BB_RESERVE(_n) \
	if ( (*bb_r)->alloc_sz - (*bb_r)->used_sz < _n ) { \
		if (0 != cf_buf_builder_reserve_internal(bb_r, _n)) \
			return(-1); \
	}

int
cf_buf_builder_append_buf(cf_buf_builder **bb_r, uint8_t *buf, size_t sz)
{
	BB_RESERVE(sz);
	cf_buf_builder *bb = *bb_r;
	memcpy(&bb->buf[bb->used_sz], buf, sz);
	bb->used_sz += sz;
	return(0);
}

int
cf_buf_builder_append_string(cf_buf_builder **bb_r, const char *s)
{
	size_t	len = strlen(s);
	BB_RESERVE(len);
	cf_buf_builder *bb = *bb_r;
	memcpy(&bb->buf[bb->used_sz], s, len);
	bb->used_sz += len;
	return ( 0 );
}

int
cf_buf_builder_append_char (cf_buf_builder **bb_r, char c)
{
	BB_RESERVE(1);
	cf_buf_builder *bb = *bb_r;
	bb->buf[bb->used_sz] = (uint8_t) c;
	bb->used_sz++;
	return( 0 );
}

int
cf_buf_builder_append_ascii_int (cf_buf_builder **bb_r, int i)
{
	BB_RESERVE(12);
	cf_buf_builder *bb = *bb_r;
	bb->used_sz += cf_str_itoa(i, (char *) &bb->buf[bb->used_sz], 10);
	return( 0 );
}

int
cf_buf_builder_append_ascii_uint64_x (cf_buf_builder **bb_r, uint64_t i)
{
	BB_RESERVE(18);
	cf_buf_builder *bb = *bb_r;
	bb->used_sz += cf_str_itoa_u64(i, (char *) &bb->buf[bb->used_sz], 16);
	return( 0 );
}

int
cf_buf_builder_append_ascii_uint64 (cf_buf_builder **bb_r, uint64_t i)
{
	BB_RESERVE(12);
	cf_buf_builder *bb = *bb_r;
	bb->used_sz += cf_str_itoa_u64(i, (char *) &bb->buf[bb->used_sz], 10);
	return( 0 );
}

int
cf_buf_builder_append_ascii_uint32 (cf_buf_builder **bb_r, uint32_t i)
{
	BB_RESERVE(12);
	cf_buf_builder *bb = *bb_r;
	bb->used_sz += cf_str_itoa_u32(i, (char *) &bb->buf[bb->used_sz], 10);
	return( 0 );
}

int
cf_buf_builder_append_uint64 (cf_buf_builder **bb_r, uint64_t i)
{
	BB_RESERVE(8);
	cf_buf_builder *bb = *bb_r;
	uint64_t *i_p = (uint64_t *) &bb->buf[bb->used_sz];
	*i_p = __swab64(i);
	bb->used_sz += 8;
	return( 0 );
}

int
cf_buf_builder_append_uint32 (cf_buf_builder **bb_r, uint32_t i)
{
	BB_RESERVE(4);
	cf_buf_builder *bb = *bb_r;
	uint32_t *i_p = (uint32_t *) &bb->buf[bb->used_sz];
	*i_p = htonl(i);
	bb->used_sz += 4;
	return( 0 );
}

int
cf_buf_builder_append_uint16(cf_buf_builder **bb_r, uint16_t i)
{
	BB_RESERVE(2);
	cf_buf_builder *bb = *bb_r;
	uint16_t *i_p = (uint16_t *) &bb->buf[bb->used_sz];
	*i_p = htons(i);
	bb->used_sz += 2;
	return( 0 );
}

int
cf_buf_builder_append_uint8(cf_buf_builder **bb_r, uint8_t i)
{
	BB_RESERVE(1);
	cf_buf_builder *bb = *bb_r;
	bb->buf[bb->used_sz] = i;
	bb->used_sz ++;
	return( 0 );
}

int
cf_buf_builder_reserve(	cf_buf_builder **bb_r, int sz, uint8_t **buf)
{
	BB_RESERVE(sz);
	cf_buf_builder *bb = *bb_r;
    if (buf) {
		*buf = &bb->buf[bb->used_sz];
	}
	bb->used_sz += sz;
	return( 0 );
}

int
cf_buf_builder_size( cf_buf_builder *bb)
{
	return bb->alloc_sz + sizeof(cf_buf_builder);
}

int
cf_buf_builder_chomp(cf_buf_builder *bb)
{
	if (bb->used_sz > 0)
		bb->used_sz--;
	return(0);
}

char *
cf_buf_builder_strdup(cf_buf_builder *bb)
{
	if (bb->used_sz == 0)	return(0);
	char *r = cf_malloc(bb->used_sz+1);
	if (!r)	return(0);
	memcpy(r, bb->buf, bb->used_sz);
	r[bb->used_sz] = 0;
	return(r);
}

cf_buf_builder *
cf_buf_builder_create()
{
	cf_buf_builder *bb = cf_malloc(1024);
	if (!bb)	return(0);
	bb->alloc_sz = 1024 - sizeof(cf_buf_builder);
	bb->used_sz = 0;
	return(bb);
}

cf_buf_builder *
cf_buf_builder_create_size(size_t sz)
{
	size_t malloc_sz = (sz < 1024) ? 1024 : sz;
	cf_buf_builder *bb = cf_malloc(malloc_sz);
	if (! bb) {
		return 0;
	}
	bb->alloc_sz = malloc_sz - sizeof(cf_buf_builder);
	bb->used_sz = 0;
	return bb;
}

void
cf_buf_builder_free(cf_buf_builder *bb)
{
	cf_free(bb);
}
