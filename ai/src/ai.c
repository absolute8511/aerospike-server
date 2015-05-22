/*
 * ai.c
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

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_shash.h>
#include <citrusleaf/cf_ll.h>

#include "fault.h"
#include "util.h"

#include "base/datamodel.h"

#include "ai.h"
#include "ai_globals.h"
#include "bt.h"
#include "find.h"
#include "stream.h"

/***************** Define Global Variables *****************/

/*
 *  [Note:  Ideally this module's state will eventually be encapsulated within
 *           a functional interface, rather than accessed via global variables.]
 */

/*
 *  Column type definitions.
 */
char *Col_type_defs[] =
{	"NONE",     "INT SIGNED", "BIGINT UNSIGNED", "TEXT", "FLOAT", "U128",
	"LUATABLE", "FUNCTION",     "BOOL",            "COLUMN_NAME",   "U160"
};

/*
 *  Array of tables.
 */
r_tbl_t *Tbl = NULL;

/*
 *  Number of tables.
 */
int Num_tbls = 0;

/*
 *  Table High Watermark = Number of tables currently allocated.
 */
uint32 Tbl_HW = 0;

/*
 *  Dictionary mapping table names to their definitions.
 */
shash  *TblD = NULL;
/*
 *  List of empty table IDs.
 */
cf_ll *DropT = NULL;

/*
 *  Array of indexes.
 */
r_ind_t *Index = NULL;

/*
 *  Number of indexes.
 */
int Num_indx = 0;

/*
 *  Index High Watermark = Number of indexes currently allocated.
 */
uint32 Ind_HW = 0;

/*
 *  Dictionary mapping index names to their definitions.
 */
shash  *IndD = NULL;

/*
 *  List of empty index IDs.
 */
cf_ll *DropI = NULL;


/***************** Define Module-Global Variables *****************/


/*
 *  Has Aerospike Index module been initialized?
 */
static bool g_ai_initialized = false;

/*
 * shash hash function
 */
static inline uint32_t
as_sindex__dict_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, sizeof(uint32_t));
}

/*
 * Functions needed for linked lists
 */
void
ll_ai_match_destroy_fn(cf_ll_element * ele)
{
	cf_free((ll_ai_match_element *)ele);
}

int
ll_ai_match_reduce_fn(cf_ll_element *ele1, void * ele2)
{
	if (((ll_ai_match_element *)ele1)->match == ((ll_ai_match_element *)ele2)->match) {
		return CF_LL_REDUCE_MATCHED;
	}
	else {
		return CF_LL_REDUCE_NOT_MATCHED;
	}
}

int
ll_ai_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

/*
 *  Initialize Aerospike Index.
 */
void ai_init()
{
	if (g_ai_initialized) {
		return;
	}

	int ntbl = INIT_MAX_NUM_TABLES;
	size_t sz = sizeof(r_tbl_t) * ntbl;
	if (!(Tbl = cf_malloc(sz))) {
		cf_crash(AS_SINDEX, "Failed to allocate tables arrray (sz %zu)", sz);
	}
	bzero(Tbl, sz);
	Num_tbls = 0;
	Tbl_HW = ntbl;

	if (SHASH_OK != shash_create(&TblD, as_sindex__dict_hash_fn, TBLD_HASH_KEY_SIZE,
								 sizeof(long), AS_SINDEX_MAX, 0)) {
		cf_crash(AS_SINDEX, "Failed to allocate tables dictionary");
	}

	if (!(DropT = cf_malloc(sizeof(cf_ll)))) {
		cf_crash(AS_SINDEX, "Failed to create dropped table IDs list");
	}
	cf_ll_init(DropT, ll_ai_match_destroy_fn, false /*no lock*/);

	int nindx = INIT_MAX_NUM_INDICES;
	sz = sizeof(r_ind_t) * nindx;
	if (!(Index = cf_malloc(sz))) {
		cf_crash(AS_SINDEX, "Failed to allocate indexes array (sz %zu)", sz);
	}
	bzero(Index, sz);
	Num_indx = 0;
	Ind_HW = nindx;

	if (SHASH_OK != shash_create(&IndD, as_sindex__dict_hash_fn, TBLD_HASH_KEY_SIZE,
								 sizeof(long), AS_SINDEX_MAX, 0)) {
		cf_crash(AS_SINDEX, "Failed to allocate tables dictionary");
	}

	if (!(DropI = cf_malloc(sizeof(cf_ll)))) {
		cf_crash(AS_SINDEX, "Failed to create dropped index IDs list");
	}
	cf_ll_init(DropI, ll_ai_match_destroy_fn, false /*no lock*/);

	g_ai_initialized = true;
}

static void destroy_index(bt *ibtr, bt_n *n, int imatch)
{
	r_ind_t *ri = &Index[imatch];

	if (UNIQ(ri->cnstr)) {
		bt_destroy(ibtr);
		return;
	}

	if (!n->leaf) {
		for (int i = 0; i <= n->n; i++) {
			destroy_index(ibtr, NODES(ibtr, n)[i], imatch);
		}
	}

	for (int i = 0; i < n->n; i++) {
		void *be = KEYS(ibtr, n, i);
		ai_nbtr *anbtr = (ai_nbtr *) parseStream(be, ibtr);
		if (anbtr) {
			if (anbtr->is_btree) {
				bt_destroy(anbtr->u.nbtr);
			} else {
				cf_free(anbtr);
			}
		}
	}
}

static void emptyIndex(int imatch, bool is_part)
{
	r_ind_t *ri = &Index[imatch];
	if (!ri->name) {
		return; // previously deleted
	}
	r_tbl_t *rt = &Tbl[ri->tmatch];

	char tmp_name[TBLD_HASH_KEY_SIZE];
	memset(tmp_name, 0, TBLD_HASH_KEY_SIZE);
	memcpy(tmp_name, ri->name, strlen(ri->name));

	if(SHASH_OK != shash_delete(IndD, tmp_name) ) {
		cf_warning(AS_SINDEX, "Deletion from Aerospike Index Table failed. %s", ri->name);
	}
	cf_free(ri->name);
	ri->name = NULL;

	if (0 <= ri->icol->cmatch) {
		ll_ai_match_element * node = cf_malloc(sizeof(ll_ai_match_element));
		node->match = imatch;
		cf_ll_element * ele = cf_ll_search(rt->ilist, (cf_ll_element* )node, true, ll_ai_match_reduce_fn);
		if (ele) {
			cf_ll_delete(rt->ilist, ele);
		}
	}
	if ((is_part || 0 <= ri->icol->cmatch) && ri->btr) {
		destroy_index(ri->btr, ri->btr->root, imatch);
	}
	if (ri->icol) {
		cf_free(ri->icol);
		ri->icol = NULL;
	}
	bzero(ri, sizeof(r_ind_t));
	ri->tmatch = -1;
	ri->cnstr = CONSTRAINT_NONE;
	if (imatch == (Num_indx - 1)) {
		Num_indx--;                       // if last -> reuse
	} else {                              // else put on DropI for reuse
		if (!DropI) {
			if (!(DropI = cf_malloc(sizeof(cf_ll)))) {
				cf_crash(AS_SINDEX, "Failed to create dropped index IDs list");
			}
			cf_ll_init(DropI, ll_ai_match_destroy_fn, false /*no lock*/);
		}
		ll_ai_match_element * node = cf_malloc(sizeof(ll_ai_match_element));
		node->match = imatch;
		cf_ll_append(DropI, (cf_ll_element *)node);
	}
}

static int validateCreateTableCnames(cf_ll *cnames)
{
	cf_ll_element * ele;
	cf_ll_element * elej;
	int i = 0;
	cf_ll_iterator * iter = cf_ll_getIterator(cnames, true/*forward*/);

	while ((ele = cf_ll_getNext(iter))) {
		char *cnamei = ((ll_ai_names_element*)ele)->name;
		int j = 0;
		cf_ll_iterator * iterj = cf_ll_getIterator(cnames, true);
		while((elej = cf_ll_getNext(iterj))) {
			char *cnamej = ((ll_ai_names_element * )elej)->name;
			if (i == j) {
				continue;
			}
			if (!strcasecmp(cnamei, cnamej)) {
				return -1;
			}
			j++;
		}
		cf_ll_releaseIterator(iterj);
		i++;
	}
	cf_ll_releaseIterator(iter);
	return 0;
}

static int newIndex(char *iname, int tmatch, icol_t *ic, uchar cnstr, uchar dtype, int nprts)
{
	if (ic->nlo > 1) {
		return -1;
	}

	if (cf_ll_size(DropI) == 0 && (Num_indx >= (int) Ind_HW)) {
		Ind_HW++;
		r_ind_t *indxs = cf_malloc(sizeof(r_ind_t) * Ind_HW);
		bzero(indxs, sizeof(r_ind_t) * Ind_HW);
		for (int i = 0; i < Num_indx; i++) {
			memcpy(&indxs[i], &Index[i], sizeof(r_ind_t)); // copy index metadata
		}
		cf_free(Index);
		Index = indxs;
	}

	int imatch;
	cf_ll_element *ele = DropI->head;
	if (ele) {
		imatch = ((ll_ai_match_element *)ele)->match;
		cf_ll_delete(DropI, ele);
	} else {
		imatch = Num_indx;
		Num_indx++;
	}

//	printf("newIndex: iname: %s imatch: %d\n", iname, imatch);

	r_tbl_t *rt = &Tbl[tmatch];
	r_ind_t *ri = &Index[imatch];
	bzero(ri, sizeof(r_ind_t));
	ri->name = cf_strdup(iname);
	ri->tmatch = tmatch;
	if (!(ri->icol = (icol_t *) cf_malloc(sizeof(icol_t)))) {
		cf_warning(AS_SINDEX, "Failed to create index column");
		return -1;
	}
	cloneIC(ri->icol, ic);
	ri->cnstr = cnstr;
	ri->dtype = (0 > ic->cmatch || dtype) ? dtype : rt->col[ic->cmatch].type;
	ri->simatch = -1;
	ri->nprts = nprts; // AEROSPIKE Variables
	if (!rt->ilist) {
		if (!(rt->ilist = cf_malloc(sizeof(cf_ll)))) {
			cf_warning(AS_SINDEX, "Failed to malloc index list.");
			return -1;
		}
		cf_ll_init(rt->ilist, ll_ai_match_destroy_fn, false /*no lock*/);
	}
	ll_ai_match_element * node;
	node        = cf_malloc(sizeof(ll_ai_match_element));
	node->match = imatch;
	cf_ll_append(rt->ilist, (cf_ll_element *) node);
	if (0 <= ri->icol->cmatch) {
		rt->col[ri->icol->cmatch].imatch = imatch;
		ci_t *ci;
		// Put the cname into col dict
		char tmp_cname[CDICT_HASH_KEY_SIZE];
		memset(tmp_cname, 0, CDICT_HASH_KEY_SIZE);
		memcpy(tmp_cname, rt->col[ri->icol->cmatch].name, strlen(rt->col[ri->icol->cmatch].name));
		if(SHASH_OK != shash_get(rt->cdict, tmp_cname, (void**)&ci)) {
			cf_warning(AS_SINDEX, "shash get failed on %s", rt->col[ri->icol->cmatch].name);
			return -1;
		}

		if (!ci->ilist) {
			if (!(ci->ilist = cf_malloc(sizeof(cf_ll)))) {
				cf_warning(AS_SINDEX, "Failed to malloc index list.");
				return -1;
			}
			cf_ll_init(ci->ilist, ll_ai_match_destroy_fn, false /*no lock*/);
		}

		ll_ai_match_element * node;
		node        = cf_malloc(sizeof(ll_ai_match_element));
		node->match = imatch;
		cf_ll_append(ci->ilist, (cf_ll_element *) node);

	}

	if (0 <= ri->icol->cmatch) {
		rt->col[ri->icol->cmatch].indxd = 1;
	}

	ri->btr = createIndexBT(ri->dtype, imatch);

	// Put the iname into index dict
	char tmp_name[TBLD_HASH_KEY_SIZE];
	memset(tmp_name, 0, TBLD_HASH_KEY_SIZE);
	memcpy(tmp_name, ri->name, strlen(ri->name));
	long tmp_imatch = imatch + 1;
	if (SHASH_OK != shash_put_unique(IndD, tmp_name, (void*) & (tmp_imatch))) {
		cf_warning(AS_SINDEX, "shash put unique failed for IndD key - %s", ri->name);
	}

	return imatch;
}

static int newTable(cf_ll *ctypes, cf_ll *cnames, int ccount, char *tname)
{
	if (ccount < 2) {
		return -1;
	}

	if (0 > validateCreateTableCnames(cnames)) {
		return -1;
	}

	if (cf_ll_size(DropT) == 0 && Num_tbls >= (int) Tbl_HW) {
		Tbl_HW++;
		r_tbl_t *tbls = cf_malloc(sizeof(r_tbl_t) * Tbl_HW);
		bzero(tbls, sizeof(r_tbl_t) * Tbl_HW);
		for (int i = 0; i < Num_tbls; i++) {
			memcpy(&tbls[i], &Tbl[i], sizeof(r_tbl_t)); // copy table metadata
		}
		cf_free(Tbl);
		Tbl = tbls;
	}

	int tmatch;
	cf_ll_element * ele = DropT->head;
	if (ele) {
		tmatch = ((ll_ai_match_element *) ele)->match;
		cf_ll_delete(DropT, ele);
	} else {
		tmatch = Num_tbls;
		Num_tbls++;
	}

//	printf("newTable: tmatch: %d\n", tmatch);

	r_tbl_t *rt = &Tbl[tmatch];
	bzero(rt, sizeof(r_tbl_t));
	rt->name = cf_strdup(tname);
	rt->col_count = ccount;
	rt->col = cf_malloc(sizeof(r_col_t) * rt->col_count);
	bzero(rt->col, sizeof(r_col_t) * rt->col_count);
	rt->cdict  = NULL;
	// NUMERIC - 8 characters
	// STRING  - 7 characters (we will assume max characters used by bin type to be 10 )
	// key - bin_name.bin_type (AS_ID_BIN_SZ)
	if (SHASH_OK != shash_create(&rt->cdict, as_sindex__dict_hash_fn, CDICT_HASH_KEY_SIZE,
								 sizeof(ci_t *), AS_SINDEX_MAX, 0)) {
		cf_crash(AS_SINDEX, "Failed to allocate tables dictionary");
	}
	for (int i = 0; i < rt->col_count; i++) {
		cf_ll_element * ele_n = cf_ll_index(cnames, i);
		char *cname = ((ll_ai_names_element *) ele_n)->name;
		rt->col[i].name = cf_strdup(cname);
		ci_t *ci = cf_malloc(sizeof(ci_t));
		bzero(ci, sizeof(ci_t));
		ci->cmatch = i;

		// Put the cname into col dict
		
		char tmp_cname[CDICT_HASH_KEY_SIZE];
		memset(tmp_cname, 0, CDICT_HASH_KEY_SIZE);
		memcpy(tmp_cname, cname, strlen(cname));
		if (SHASH_OK != shash_put_unique(rt->cdict, tmp_cname, (void**)&ci)) {
			cf_warning(AS_SINDEX, "shash put unique failed for columne table. key - %s", cname);
		}
		cf_ll_element * ele_t = cf_ll_index(ctypes, i);
		rt->col[i].type = ((ll_ai_types_element * )ele_t)->type;
		rt->col[i].imatch = -1;
	}

	rt->btr = createDBT(rt->col[0].type, tmatch);

	// Put the tname into table dict
	char tmp_tname[TBLD_HASH_KEY_SIZE];
	memset(tmp_tname, 0, TBLD_HASH_KEY_SIZE);
	memcpy(tmp_tname, rt->name, strlen(rt->name));
	long tmp_tmatch = tmatch + 1;
	if (SHASH_OK != shash_put_unique(TblD, tmp_tname, (void*) & (tmp_tmatch))) {
		cf_warning(AS_SINDEX, "shash put unique failed for TblD. key - %s", rt->name);
	}
	/* BTREE implies an index on "tbl_pk_index" -> autogenerate */
	char *pkname = rt->col[0].name;
	char iname[NAME_STR_LEN];
	snprintf(iname, sizeof(iname), "%s_%s_%s", rt->name, pkname, "index");
	icol_t *pkic;
	if (!(pkic = (icol_t *) cf_malloc(sizeof(icol_t)))) {
		cf_warning(AS_SINDEX, "Failed to create PK column");
		return -1;
	}
	bzero(pkic, sizeof(icol_t));
	pkic->fimatch = -1;
	pkic->cmatch = 0;
	newIndex(iname, tmatch, pkic, CONSTRAINT_NONE, 0, 0);

	return 0;
}

/*
 *  Create a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_create_table(char *tname)
{
	int tmatch;
	if (-1 != (tmatch = find_table(tname))) {
		// If a table with this name already exists, return success.
		return 0;
	}

	/*
	 *  Create a table with 2 columns.  The DDL is effectively:
	 *
	 *       CREATE TABLE <tname> (pk U160, __dummy TEXT)
	 */

	int ccount = 2;

	cf_ll cnames;
	cf_ll_init(&cnames, NULL, false /*no lock*/);

	ll_ai_names_element cnode1, cnode2;
	cnode1.name = "pk";
	cf_ll_append(&cnames, (cf_ll_element *)&cnode1);
	cnode2.name = "__dummy";
	cf_ll_append(&cnames, (cf_ll_element *)&cnode2);

	cf_ll ctypes;
	cf_ll_init(&ctypes, NULL, false);
	// Add COL_TYPE_U160
	ll_ai_types_element node1, node2;
	node1.type = COL_TYPE_U160;
	cf_ll_append(&ctypes, (cf_ll_element * )&node1);
	// Add COL_TYPE_STRING
	node2.type = COL_TYPE_STRING;
	cf_ll_append(&ctypes, (cf_ll_element * )&node2);

	int rv = newTable(&ctypes, &cnames, ccount, tname);

	return rv;
}

/*
 *  Create an index.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_create_index(char *iname, char *tname, char *cname, int col_type, int num_partitions)
{
	int tmatch = find_table(tname);
	if (-1 == tmatch) {
		// Table does not exist.
		return -1;
	}
	r_tbl_t *rt = &Tbl[tmatch];

	icol_t *ic = find_column(tmatch, cname);
	if (!ic) {
		// Column does not exist.
		return -1;
	}

	if (-1 != match_index_name(iname)) {
		// Index already exists.
		return -1;
	}

	if ((2 > num_partitions) || (MAX_PARTITIONS_PER_INDEX < num_partitions)) {
		// Unacceptible number of partitions.
		return -1;
	}

	// Actually create the index.
	uchar cnstr = CONSTRAINT_NONE;
	int rv = newIndex(iname, tmatch, ic, cnstr, col_type, num_partitions);

	if (num_partitions) {
		if (0 > ic->cmatch) {
			cf_crash(AS_SINDEX, "ic->cmatch (%d) is less than 0", ic->cmatch);
		}
		int dtype = rt->col[ic->cmatch].type;
		ic->cmatch = -1;
		for (int i = 1; i < num_partitions; i++) {
			char piname[NAME_STR_LEN];
			snprintf(piname, sizeof(piname), "%s.%d", iname, i);
			if (0 > newIndex(piname, tmatch, ic, cnstr, dtype, -1)) {
				return -1;
			}
		}
	}

	return rv;
}

/*
 *  Add a column to a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_add_column(char *tname, char *cname, int col_type)
{
	int tmatch;
	if (-1 == (tmatch = find_table(tname))) {
		return -1;
	}

	r_tbl_t *rt = &Tbl[tmatch];
	int new_col_count = rt->col_count + 1;

	r_col_t *tcol = cf_malloc(sizeof(r_col_t) * new_col_count);
	if (!tcol) {
		return -1;
	}
	bzero(tcol, sizeof(r_col_t) * new_col_count);

	ci_t *ci = cf_malloc(sizeof(ci_t));
	if (!ci) {
		cf_free(tcol);
		return -1;
	}
	bzero(ci, sizeof(ci_t));

	for (int i = 0; i < rt->col_count; i++) {
		memcpy(&tcol[i], &rt->col[i], sizeof(r_col_t)); // copy column metadata
	}

	r_col_t *new_col = &tcol[rt->col_count];
	new_col->name = cf_strdup(cname);                  // duplicate the column name
	new_col->type = col_type;
	new_col->imatch = -1;

	cf_free(rt->col);
	rt->col = tcol;
	rt->col_count = new_col_count;

	ci->cmatch = new_col_count - 1;

	// Put the cname into col dict
	char tmp_cname[CDICT_HASH_KEY_SIZE];
	memset(tmp_cname, 0, CDICT_HASH_KEY_SIZE);
	memcpy(tmp_cname, cname, strlen(cname));
	if (SHASH_OK != shash_put_unique(rt->cdict, tmp_cname, (void**)&ci)) {
		cf_warning(AS_SINDEX, "shash put unique failed for coumn table. key - %s", cname);
	}
	return 0;
}

/*
 *  Drop a column from a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_drop_column(char *tname, char *cname)
{
	int tmatch;
	if (-1 == (tmatch = find_table(tname))) {
		return -1;
	}

	r_tbl_t *rt = &Tbl[tmatch];
	int new_col_count = rt->col_count - 1;

	r_col_t *tcol = cf_malloc(sizeof(r_col_t) * new_col_count);
	if (!tcol) {
		return -1;
	}
	bzero(tcol, sizeof(r_col_t) * new_col_count);

	int to = 0;
	for (int from = 0; from < rt->col_count; from++) {
		if (!strcmp(rt->col[from].name, cname)) {
			cf_free(rt->col[from].name);                      // release the column name
			continue;                                         // skip the one being deleted
		}
		memcpy(&tcol[to++], &rt->col[from], sizeof(r_col_t)); // copy column metadata
	}
	cf_free(rt->col);
	rt->col = tcol;
	rt->col_count = new_col_count;

	char tmp_cname[CDICT_HASH_KEY_SIZE];	
	memset(tmp_cname, 0, CDICT_HASH_KEY_SIZE);
	memcpy(tmp_cname, cname, strlen(cname));
	if(SHASH_OK != shash_delete(rt->cdict, tmp_cname) ) {
		cf_warning(AS_SINDEX, "Deletion from Aerospike Column Table failed. %s", cname);
	}

	return 0;
}

/*
 *  Drop a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_drop_table(char *tname)
{
	int tmatch = find_table(tname);
	if (-1 == tmatch) {
		return -1;
	}
	r_tbl_t *rt = &Tbl[tmatch];

	if (!rt->name) {
		return 0;                               // already deleted
	}

	MATCH_INDICES(tmatch);
	if (matches) {                              // delete indices first
		for (int i = 0; i < matches; i++) {
			emptyIndex(inds[i], 0);
		}
	}

	char tmp_tname[TBLD_HASH_KEY_SIZE];
	memset(tmp_tname, 0, TBLD_HASH_KEY_SIZE);
	memcpy(tmp_tname, rt->name, strlen(rt->name));
	if(SHASH_OK != shash_delete(TblD, tmp_tname) ) {
		cf_warning(AS_SINDEX, "Deletion from Aerospike Table failed. %s", rt->name);
	}
	cf_free(rt->name);
	rt->name = NULL;
	for (int j = 0; j < rt->col_count; j++) {
		cf_free(rt->col[j].name);
	}
	cf_free(rt->col);
	bt_destroy(rt->btr);
	cf_ll_reduce(rt->ilist, true, ll_ai_reduce_fn, NULL);
	if (rt->ilist) {
		cf_free(rt->ilist);
	}
	shash_destroy(rt->cdict);
	bzero(rt, sizeof(r_tbl_t));
	if ((Num_tbls - 1) == tmatch) {
		Num_tbls--;                             // if last -> reuse
	} else { // else put on DropT for reuse
		ll_ai_match_element * node = cf_malloc(sizeof(ll_ai_match_element));
		node->match = tmatch;
		cf_ll_append(DropT, (cf_ll_element *)node);
	}

	return 0;
}

/*
 *  Drop an index.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_drop_index(char *iname)
{
	int imatch = match_partial_index_name(iname);
	if (-1 == imatch) {
		return -1;
	}
	r_ind_t *ri = &Index[imatch];
	int nprts = ri->nprts;

	emptyIndex(imatch, 0);
	for (int i = 1; i < nprts; i++) {
		char piname[NAME_STR_LEN];
		snprintf(piname, sizeof(piname), "%s.%d", iname, i);
		int pimatch = match_partial_index_name(piname);
		emptyIndex(pimatch, 1);
	}

	return 0;
}

/*
 *  Shut Down the Aerospike Index Module.
 */
void ai_shutdown(void)
{
	if (!g_ai_initialized) {
		return;
	}

	if (Tbl) {
		cf_free(Tbl);
		Tbl = NULL;
	}
	Num_tbls = Tbl_HW = 0;

	if (TblD) {
		shash_destroy(TblD);
		TblD = NULL;
	}

	if (DropT) {
		cf_ll_reduce(DropT, true, ll_ai_reduce_fn, NULL);
		cf_free(DropT);
		DropT = NULL;
	}

	if (Index) {
		cf_free(Index);
		Index = NULL;
	}
	Num_indx = Ind_HW = 0;

	if (IndD) {
		shash_destroy(IndD);
		IndD = NULL;
	}

	if (DropI) {
		cf_ll_reduce(DropI, true, ll_ai_reduce_fn, NULL);
		cf_free(DropI);
		DropI = NULL;
	}

	g_ai_initialized = false;
}
