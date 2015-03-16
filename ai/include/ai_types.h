/*
 * ai_types.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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
 *  SYNOPSIS
 *    This file provides common declarations and definitions for
 *    the Aerospike Index module.
 */

#pragma once

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>

#include <citrusleaf/cf_shash.h>
#include <citrusleaf/cf_ll.h>
#include <base/secondary_index.h>

#define uchar    unsigned char
#define ushort16 unsigned short
#define uint32   unsigned int
#define lolo     long long
#define ull      unsigned long long
#define uint128  __uint128_t
#define dbl      double

#define AS_DIGEST_KEY_SZ 20
typedef struct uint160 {
	char digest[AS_DIGEST_KEY_SZ];
} uint160;

/*
 *  Maximum length for table / column / index name strings.
 */
#define NAME_STR_LEN               128

#define INIT_MAX_NUM_TABLES        256
#define INIT_MAX_NUM_INDICES       256

#define MAX_PARTITIONS_PER_INDEX   256

#define MAX_JOIN_COLS              128
#define MAX_ORDER_BY_COLS          16

#define COL_TYPE_NONE         0
#define COL_TYPE_INT          1
#define COL_TYPE_LONG         2
#define COL_TYPE_STRING       3
#define COL_TYPE_FLOAT        4
#define COL_TYPE_U128         5
#define COL_TYPE_LUAO         6
#define COL_TYPE_FUNC         7
#define COL_TYPE_BOOL         8
#define COL_TYPE_CNAME        9
#define COL_TYPE_U160        10
#define COL_TYPE_ERR         11

#define C_IS_N(ctype)    (ctype == COL_TYPE_NONE)
#define C_IS_I(ctype)    (ctype == COL_TYPE_INT)
#define C_IS_L(ctype)    (ctype == COL_TYPE_LONG)
#define C_IS_S(ctype)    (ctype == COL_TYPE_STRING)
#define C_IS_F(ctype)    (ctype == COL_TYPE_FLOAT)
#define C_IS_X(ctype)    (ctype == COL_TYPE_U128)
#define C_IS_Y(ctype)    (ctype == COL_TYPE_U160)
#define C_IS_P(ctype)    (ctype == COL_TYPE_FUNC)
#define C_IS_O(ctype)    (ctype == COL_TYPE_LUAO)
#define C_IS_B(ctype)    (ctype == COL_TYPE_BOOL)
#define C_IS_C(ctype)    (ctype == COL_TYPE_CNAME)
#define C_IS_E(ctype)    (ctype == COL_TYPE_ERR)
#define C_IS_NUM(ctype)  (C_IS_I(ctype) || C_IS_L(ctype) || C_IS_X(ctype))

#define CONSTRAINT_NONE   0
#define CONSTRAINT_UNIQUE 1
#define UNIQ(cnstr) (cnstr == CONSTRAINT_UNIQUE)

#define VOIDINT (void *) (long)
#define INTVOID (uint32) (ulong)

enum OP {NONE, EQ, NE, GT, GE, LT, LE, RQ, IN, LFUNC};

#define ASSERT_OK(x) assert((x) == DICT_OK)

#define SPLICE_128(num)											\
	uint128 bu = num; char *pbu = (char *) &bu; ull ubl, ubh;	\
	memcpy(&ubh, pbu + 8, 8);									\
	memcpy(&ubl, pbu,     8);

#define DEBUG_U128(fp, num)										\
	{															\
		SPLICE_128(num);										\
		fprintf(fp, "DEBUG_U128: high: %llu low: %llu", ubh, ubl); \
	}

#define SPLICE_160(num)											\
	ull ubh, ubm; uint32 u;										\
	char *pbu = (char *) &num;									\
	memcpy(&ubh, pbu + 12, 8);									\
	memcpy(&ubm, pbu + 4,  8);									\
	memcpy(&u,   pbu,      4);

#define DEBUG_U160(fp, num)										\
	{															\
		SPLICE_160(num);										\
		fprintf(fp, "DEBUG_U160: high: %llu mid: %llu low: %u", ubh, ubm, u); \
	}

#define CDICT_HASH_KEY_SIZE AS_SINDEX_MAX_PATH_LENGTH + 2*AS_SINDEX_TYPE_STR_SIZE
#define TBLD_HASH_KEY_SIZE AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE + 1
#define INDD_HASH_KEY_SIZE TBLD_HASH_KEY_SIZE

/***************** Opaque Forward Type Declarations *****************/

/*
 *  B-Tree Object [Implementation defined in "btreepriv.h".]
 */
typedef struct btree bt;


/***************** Type Declarations *****************/


/*
 *  Column Object.
 */
typedef struct icol_t {
	int cmatch;
	int fimatch;
	uint32 nlo;
	char **lo;
} icol_t;

typedef struct ci_t {
	int    cmatch;
	cf_ll *ilist;
} ci_t;

typedef struct r_col {
	char *name;
	uchar type;
	bool indxd;
	int imatch;
} r_col_t;

/*
 *  AI Object.
 */
typedef struct ai_obj {
	char    *s;
	uint32   len;
	uint32   i;
	ulong    l;
	uint128  x;
	uint160  y;
	float    f;
	bool     b;
	bool     err;
	icol_t  *ic;
	uchar    type;
	uchar    enc;
	uchar    freeme;
	uchar    empty;
} ai_obj;

typedef struct r_tbl {
	char    *name;
	bt      *btr;
	int      col_count;
	r_col_t *col;
	cf_ll    *ilist;      // USAGE: list of this table's imatch's
	shash   *cdict;      // USAGE: maps cname to ci_t
	uint32   tcols;      // HASH: on INSERT num new columns

	uint32   lrud;       // LRU: timestamp & bool
	int      lruc;       // LRU: column containing LRU
	bool     lfu;        // LFU: indexing on/off
	int      lfuc;       // LFU: column containing LFU

	bool     dirty;      // ALTER TABLE [UN]SET DIRTY
} r_tbl_t;

typedef struct r_ind {
	bt     *btr;         // Btree of Index
	int     nprts;       // Number or Partitions

	char   *name;        // Name of index

	int     tmatch;      // table index is ON
	icol_t *icol;        // single column OR 1st MCI column

	uchar   cnstr;       // CONSTRAINTS: [UNIQUE,,,]

	bool    done;        // CREATE INDEX OFFSET -> not done until finished
	long    ofst;        // CREATE INDEX OFFSET partial indexes current offset

	bool    iposon;      // Index Position On (i.e. SELECT "index.pos()"
	uint32  cipos;       // Current Index position, when iposon

	uchar   dtype;       // DotNotation Index Type (e.g. luatbl.x.y.z -> INT)
	char   *fname;       // LuaFunctionIndex: functionname

	int     simatch;     // AEROSPIKE Secondary Index Array[slot]
} r_ind_t;


typedef struct filter {
	int      jan;        // JoinAliasNumber filter runs on (for JOINS)
	int      imatch;     // index  filter runs on (for JOINS)
	int      tmatch;     // table  filter runs on (for JOINS)
	icol_t  *ic;         // column filter runs on (JOINS & RQ & SNGL)
	enum OP  op;         // operation filter applies [>,<,=,!=]

	bool     iss;        // is string, WHERE fk = 'fk' (1st iss=0, 2nd iss=1)
	char    *key;        // RHS of filter (e.g. AND x < 7 ... key=7)
	ai_obj   akey;       // value of KEY [string="7",int=7,float=7.0]

	char    *low;        // LHS of Range filter (AND x BETWEEN 3 AND 4 -> 3)
	ai_obj   alow;       // value of LOW  [string="3",int=3,float=3.0]
	char    *high;       // RHS of Range filter (AND x BETWEEN 3 AND 4 -> 4)
	ai_obj   ahigh;      // value of HIGH [string="4",int=4,float=4.0]
} f_t;


typedef struct check_sql_where_clause {
	uchar   wtype;
	char   *token;
	char   *lvr;         // Leftover AFTER parse
	f_t     wf;          // WhereClause Filter (i.e. i,c,t,low,inl)
} cswc_t;

typedef struct ll_ai_match_element_s {
	cf_ll_element ele;
	long match;
} ll_ai_match_element;

typedef struct ll_ai_names_element_s {
	cf_ll_element ele;
	char *name;
} ll_ai_names_element;

typedef struct ll_ai_types_element_s {
	cf_ll_element ele;
	long type;
} ll_ai_types_element;

/* Function declarations */

// needed for Aerospike Index lists (cf_ll)
void ll_ai_match_destroy_fn(cf_ll_element * ele);

int ll_ai_match_reduce_fn(cf_ll_element *ele1, void * ele2);

int ll_ai_reduce_fn(cf_ll_element *ele, void *udata);
