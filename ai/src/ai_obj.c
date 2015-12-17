/*
 * ai_obj.h
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
 *  Aerospike Index Object Implementation.
 */

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/param.h>  // For MIN().

#include "ai_obj.h"
#include "stream.h"

#include <citrusleaf/alloc.h>

void init_ai_obj(ai_obj *a)
{
	bzero(a, sizeof(ai_obj));
	a->type = COL_TYPE_NONE;
	a->empty = 1;
}

void init_ai_objLong(ai_obj *a, ulong l)
{
	init_ai_obj(a);
	a->l = l;
	a->type = a->enc = COL_TYPE_LONG;
	a->empty = 0;
}

void init_ai_objU160(ai_obj *a, uint160 y) {
	a->enc = COL_TYPE_U160;
	a->type = COL_TYPE_U160;
	a->y = y;
	a->empty = 0;
}

void cloneIC(icol_t *dic, icol_t *sic)
{
	bzero(dic, sizeof(icol_t));
	dic->cmatch = sic->cmatch;
	dic->nlo    = sic->nlo;
	//printf("cloneIC: cmatch: %d nlo: %d\n", dic->cmatch, dic->nlo);
	if (dic->nlo) {
		dic->lo = cf_malloc(sizeof(char *) * dic->nlo);
		for (uint32 j = 0; j < dic->nlo; j++) {
			dic->lo[j] = cf_strdup(sic->lo[j]);
		}
	}
}

void ai_objClone(ai_obj *dest, ai_obj *src)
{
	memcpy(dest, src, sizeof(ai_obj));
	if (src->freeme) {
		dest->s = cf_malloc(src->len);
		memcpy(dest->s, src->s, src->len);
		dest->freeme = 1;
	}
	if (src->ic) {
		dest->ic = (icol_t *) cf_malloc(sizeof(icol_t));
		cloneIC(dest->ic, src->ic);
	}
}

static int ai_objCmp(ai_obj *a, ai_obj *b)
{
	if (C_IS_S(a->type)) {
		return strncmp(a->s, b->s, a->len);
	} else if (C_IS_F(a->type)) {
		float f = a->f - b->f;
		return (f == 0.0)     ? 0 : ((f > 0.0)     ? 1 : -1);
	} else if (C_IS_L(a->type) || C_IS_G(a->type)) {
		return (a->l == b->l) ? 0 : ((a->l > b->l) ? 1 : -1);
	} else if (C_IS_X(a->type)) {
		return (a->x == b->x) ? 0 : ((a->x > b->x) ? 1 : -1);
	} else if (C_IS_Y(a->type)) {
		return u160Cmp(&a->y, &b->y);
	} else if (C_IS_I(a->type) || C_IS_P(a->type)) {
		return (long) (a->i - b->i);
	} else {
		assert(!"ai_objCmp ERROR");
	}
}

bool ai_objEQ(ai_obj *a, ai_obj *b)
{
	return !ai_objCmp(a, b);
}

static void dumpIC(FILE *fp, icol_t *ic)
{
	fprintf(fp, "IC: cmatch: %d nlo: %u\n", ic->cmatch, ic->nlo);
	if (ic->nlo) {
		for (uint32 i = 0; i < ic->nlo; i++) {
			fprintf(fp, "\t\tlo[%d]: %s\n", i, ic->lo[i]);
		}
	}
}

static void memcpy_ai_objStoDumpBuf(char *dumpbuf, ai_obj *a)
{
	int len = MIN(a->len, 1023);
	if (a->s) {
		memcpy(dumpbuf, a->s, len);
	}
	dumpbuf[len] = '\0';
}

void dump_ai_obj(FILE *fp, ai_obj *a)
{
	char dumpbuf[1024];

	if (C_IS_S(a->type) || C_IS_O(a->type)) {
		if (a->empty) {
			fprintf(fp, "\tSTRING ai_obj: EMPTY\n");
			return;
		}
		memcpy_ai_objStoDumpBuf(dumpbuf, a);
		fprintf(fp, "\tSTRING ai_obj: mt: %d len: %d -> (%s) type: %d\n", a->empty, a->len, dumpbuf, a->type);
	} else if (C_IS_C(a->type)) {
		if (a->empty) {
			fprintf(fp, "\tCNAME ai_obj: EMPTY\n");
			return;
		}
		memcpy_ai_objStoDumpBuf(dumpbuf, a);
		fprintf(fp, "\tCNAME ai_obj: mt: %d -> (%s) cmatch: %d", a->empty, dumpbuf, a->i);
		if (a->ic) {
			fprintf(fp, " ic: ");
			dumpIC(fp, a->ic);
		} else {
			fprintf(fp, "\n");
		}
	} else if (C_IS_I(a->type) || C_IS_P(a->type)) {
		char *name = C_IS_I(a->type) ? "INT" : "FUNC";
		if (a->enc == COL_TYPE_INT || a->enc == COL_TYPE_FUNC) {
			fprintf(fp, "\t%s ai_obj: mt: %d val: %u\n", name, a->empty, a->i);
		} else {
			memcpy_ai_objStoDumpBuf(dumpbuf, a);
			fprintf(fp, "\t%s(S) ai_obj: mt: %d val: %s\n", name, a->empty, dumpbuf);
		}
	} else if (C_IS_L(a->type) || C_IS_G(a->type)) {
		if (a->enc == COL_TYPE_LONG) {
			fprintf(fp, "\tLONG ai_obj: mt: %d val: %lu\n", a->empty, a->l);
		} else {
			memcpy_ai_objStoDumpBuf(dumpbuf, a);
			fprintf(fp, "\tLONG(S) ai_obj: mt: %d val: %s\n", a->empty, dumpbuf);
		}
	} else if (C_IS_X(a->type)) {
		fprintf(fp, "\tU128 ai_obj: mt: %d val: ", a->empty);
		DEBUG_U128(fp, a->x);
		fprintf(fp, "\n");
	} else if (C_IS_Y(a->type)) {
		fprintf(fp, "\tU160 ai_obj: mt: %d val: ", a->empty);
		DEBUG_U160(fp, a->y);
		fprintf(fp, "\n");
	} else if (C_IS_F(a->type)) {
		if (a->enc == COL_TYPE_INT) {
			fprintf(fp, "\tFLOAT ai_obj: mt: %d val: %f\n", a->empty, a->f);
		} else {
			memcpy_ai_objStoDumpBuf(dumpbuf, a);
			fprintf(fp, "\tFLOAT(S) ai_obj: mt: %d val: %s\n", a->empty, dumpbuf);
		}
	} else {
		fprintf(fp, "\tUNINITIALISED ai_obj mt: %d\n", a->empty);
	}
}
