/*
 * find.c
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
 * This file implements find_table, index, column logic.
 */

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "ai_globals.h"
#include "bt.h"
#include "find.h"

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_shash.h>

#include "base/datamodel.h"
#include "fault.h"

#define SPECIAL_COL_IMATCH_ADD_NUM 100

static inline int setOCmatchFromImatch(int imatch)
{
	if (!SIMP_UNIQ(Index[imatch].btr)) {
		return -1;
	}
	Index[imatch].iposon = 1;
	Index[imatch].cipos = 1;

	return (imatch * -1) - 2 - SPECIAL_COL_IMATCH_ADD_NUM;
}

static inline int getImatchFromOCmatch(int cmatch)
{
	return (cmatch * -1) - 2 - SPECIAL_COL_IMATCH_ADD_NUM;
}

// [Note:  Originally from: "range.c".]
static bool icol_cmp(icol_t *ic1, icol_t *ic2)
{
	bool ret =  (ic1->cmatch == ic2->cmatch ? 0 :
				 ic1->cmatch >  ic2->cmatch ? 1 : -1);

	if (!ic1->nlo && !ic1->nlo) {
		return ret;
	} else if (ic1->nlo && ic1->nlo) { // Compare "lo"
		if (ic1->nlo != ic2->nlo) {
			return ic1->nlo > ic2->nlo ? 1 : -1;
		} else {
			for (uint32 i = 0; i < ic1->nlo; i++) {
				bool r = strcmp(ic1->lo[i], ic2->lo[i]);
				if (r) {
					return r;
				}
			}
			return 0;
		}
	} else {
		return ic1->nlo ? 1 : -1;
	}
}

static int _find_index(int tmatch, icol_t *ic, bool prtl)
{
	if (ic->cmatch < -1) {
		return getImatchFromOCmatch(ic->cmatch);
	}

	int imatch = -1;
	r_tbl_t *rt = &Tbl[tmatch];
	if (!C_IS_O(rt->col[ic->cmatch].type)) {
		imatch = rt->col[ic->cmatch].imatch;
	} else {
		ci_t *ci = NULL;

		char tmp_cname[CDICT_HASH_KEY_SIZE];
		memset(tmp_cname, 0, CDICT_HASH_KEY_SIZE);
		memcpy(tmp_cname, rt->col[ic->cmatch].name, strlen(rt->col[ic->cmatch].name));
		if(SHASH_OK != shash_get(rt->cdict, tmp_cname, (void **) &ci)) {
			cf_debug(AS_SINDEX, "shash get failed on %s", rt->col[ic->cmatch].name);
		}
		if (!ci || !ci->ilist) {
			return -1;
		}
		cf_ll_iterator * iter = cf_ll_getIterator(ci->ilist, true);
		cf_ll_element  * ele;
		while ((ele = cf_ll_getNext(iter))) {
			int im = ((ll_ai_match_element *)ele)->match;
			r_ind_t *ri = &Index[im];
			if (!icol_cmp(ic, ri->icol)) {
				imatch = im;
				break;
			}
		}
		cf_ll_releaseIterator(iter);
	}
	if (imatch == -1) {
		return -1;
	} else if (!prtl) {
		return Index[imatch].done ? imatch : - 1;
	} else {
		return imatch;
	}
}

int find_index(int tmatch, icol_t *ic)
{
	return _find_index(tmatch, ic, 0);
}

int find_partial_index(int tmatch, icol_t *ic)
{
	return _find_index(tmatch, ic, 1);
}

int _match_index(int tmatch, cf_ll *indl, bool prtl)
{
	cf_ll_element * ele;
	r_tbl_t *rt = &Tbl[tmatch];
	if (!rt->ilist) {
		return 0;
	}
	int matches =  0;
	cf_ll_iterator * iter = cf_ll_getIterator(rt->ilist, true);
	while (( ele = cf_ll_getNext(iter))) {
		int imatch = ((ll_ai_match_element *)ele)->match;
		r_ind_t *ri = &Index[imatch];
		if (!prtl && !ri->done) {
			continue;
		}
		ll_ai_match_element * node = cf_malloc(sizeof(ll_ai_match_element));
		node->match = imatch;
		if (prtl) {
			cf_ll_append(indl, (cf_ll_element *)node);
		} else { // \/ UNIQ can fail, must be 1st
			if (UNIQ(ri->cnstr)) {
				cf_ll_prepend(indl, (cf_ll_element *)node);
			} else {
				cf_ll_append(indl, (cf_ll_element *)node);
			}
		}
		matches++;
	}
	cf_ll_releaseIterator(iter);

	return matches;
}

int match_index(int tmatch, cf_ll *indl)
{
	return _match_index(tmatch, indl, 0);
}

int match_partial_index(int tmatch, cf_ll *indl)
{
	return _match_index(tmatch, indl, 1);
}

int match_partial_index_name(char *iname)
{
	char tmp_name[INDD_HASH_KEY_SIZE];
	memset(tmp_name, 0, INDD_HASH_KEY_SIZE);
	memcpy(tmp_name, iname, strlen(iname));

	long val = -1;
	if(SHASH_OK != shash_get(IndD, (void*)tmp_name, (void*)&val)) {
		cf_debug(AS_SINDEX, "shash get failed on %s", iname);
	}

	return val != -1 ? val - 1 : -1;
}

int match_index_name(char *iname)
{
	int imatch = match_partial_index_name(iname);

	if (imatch == -1) {
		return -1;
	} else {
		return Index[imatch].done ? imatch : -1; // completed?
	}
}

int find_table(char *tname)
{
	long val = -1;

	char tmp_tname[TBLD_HASH_KEY_SIZE];
	memset(tmp_tname, 0, TBLD_HASH_KEY_SIZE);
	memcpy(tmp_tname, tname, strlen(tname));
	if(SHASH_OK != shash_get(TblD, (void *)tmp_tname, (void *)&val)) {
		cf_detail(AS_SINDEX, "shash get failed on %s", tname);
	}

	return val != -1 ? val - 1 : -1;
}

icol_t *find_column(int tmatch, char *cname)
{
	char tmp_cname[CDICT_HASH_KEY_SIZE];	
	memset(tmp_cname, 0, CDICT_HASH_KEY_SIZE);
	memcpy(tmp_cname, cname, strlen(cname));

	r_tbl_t *rt = &Tbl[tmatch];
	ci_t *ci = NULL;
	if(SHASH_OK != shash_get(rt->cdict, (void *)tmp_cname, (void **)&ci)) {
		cf_debug(AS_SINDEX, "shash get failed on %s", cname);
	}
	icol_t *ic = NULL;

	if (ci && (ic = (icol_t *) cf_malloc(sizeof(icol_t)))) {
		bzero(ic, sizeof(icol_t));
		ic->fimatch = -1;
		// icmatch should point to cmatch in ci
		ic->cmatch = ci->cmatch;
	}

	return ic;
}
