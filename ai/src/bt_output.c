/*
 * Copyright 1997-1998, 2001 John-Mark Gurney.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 */

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "ai_globals.h"
#include "bt_output.h"
#include "bt_iterator.h"
#include "find.h"
#include "stream.h"

#define getBtr(tmatch)  Tbl[tmatch].btr
#define getIBtr(imatch) Index[imatch].btr

#define PRINT_EVICTED_KEYS

void printKey(bt *btr, bt_n *x, int i)
{
	if (i < 0 || i >= x->n) {
		printf(" NO KEY\n");
	} else {
		ai_obj akey;
		void *be = KEYS(btr, x, i);
		//printf("btr: %p x: %p i: %d be: %p\n", btr, x, i, be);
		convertStream2Key(be, &akey, btr);
		dump_ai_obj(stdout, &akey);
	}
}

void printDWD(dwd_t dwd, bt *btr)
{
	ai_obj akey;
	void *be = dwd.k;
	convertStream2Key(be, &akey, btr);
	dump_ai_obj(stdout, &akey);
}
void printKeyFromPtr(bt *btr, void *be) {
	ai_obj akey;
	convertStream2Key(be, &akey, btr);
	dump_ai_obj(stdout, &akey);
}

#define DEBUG_BT_TYPE(fp, btr)										\
	fprintf(fp, "btr: %p INODE: %d NORM: %d "						\
			"UU: %d UL: %d UX: %d UY: %d "							\
			"LU: %d LL: %d LX: %d LY: %d "							\
			"XU: %d XL: %d XX: %d XY: %d "							\
			"YU: %d YL: %d YX: %d YY: %d "							\
			" [UP: %d LUP: %d LLP: %d XUP: %d XLP: %d XXP: %d] "	\
			" [YLP: %d YYP: %d] OBYI: %d BIG: %d ksize: %d\n",		\
			btr, INODE(btr), NORM_BT(btr),							\
			UU(btr), UL(btr), UX(btr), UY(btr),						\
			LU(btr), LL(btr), LX(btr), LY(btr),						\
			XU(btr), XL(btr), XX(btr), XY(btr),						\
			YU(btr), YL(btr), YX(btr), YY(btr),						\
			UP(btr),  LUP(btr), LLP(btr),							\
			XUP(btr), XLP(btr), XXP(btr),							\
			YLP(btr), YYP(btr), OBYI(btr), BIG_BT(btr), btr->s.ksize);

static int treeheight(bt *btr)
{
	bt_n *x = btr->root;
	if (!x) {
		return 0;
	}

	int ret = 0;
	while (!x->leaf) {
		x = NODES(btr, x)[0];
		ret++;
	}

	return ++ret;
}

void bt_dump_info(FILE *fp, bt *btr)
{
	fprintf(fp, "BT: %p t: %d nbits: %d nbyte: %d kbyte: %d "
			"ksize: %d koff: %d noff: %d numkeys: %d numnodes: %d "
			"height: %d btr: %p btype: %d ktype: %d bflag: %d "
			"num: %d root: %p dirty_left: %u msize: %ld dsize: %ld "
			"dirty: %u\n",
			btr, btr->t, btr->nbits, btr->nbyte, btr->kbyte, btr->s.ksize,
			btr->keyofst, btr->nodeofst, btr->numkeys, btr->numnodes,
			treeheight(btr), (void *)btr, btr->s.btype, btr->s.ktype,
			btr->s.bflag, btr->s.num, btr->root,
			btr->dirty_left, btr->msize, btr->dsize, btr->dirty);
	DEBUG_BT_TYPE(fp, btr);
}

static void dump_tree_node(FILE *fp, bt *btr, bt_n *x, int depth, bool is_index, int slot, bool is_inode)
{
	if (!x->leaf) {
#ifdef BTREE_DEBUG
		fprintf(fp, "%d: NODE[%d]: ", depth, x->num);
#else
		fprintf(fp, "%d: NODE: ",     depth);
#endif
		if (x->dirty > 0) {
			GET_BTN_SIZE(x->leaf);
			void *ds = GET_DS(x, nsize);
			fprintf(fp, "slot: %d n: %d scion: %d -> "
					"(%p) ds: %p dirty: %u ndirty: %d\n",
					slot, x->n, x->scion, (void *)x, ds, x->dirty,
#ifdef NO_NUM_DIRTY_BUILD
					0);
#else
					x->ndirty);
#endif
		} else {
			fprintf(fp, "slot: %d n: %d scion: %d -> (%p)\n",
					slot, x->n, x->scion, (void *) x);
		}
	} else {
		if (x->dirty > 0) {
			GET_BTN_SIZE(x->leaf) void *ds = GET_DS(x, nsize);
			fprintf(fp, "%d: LEAF: slot: %d n: %d scion: %d -> " \
					"(%p) ds: %p dirty: %u ndirty: %d\n",
					depth, slot, x->n, x->scion, (void *)x, ds,
					x->dirty,
#ifdef NO_NUM_DIRTY_BUILD
					0);
#else
					x->ndirty);
#endif
		} else {
			fprintf(fp, "%d: LEAF: slot: %d n: %d scion: %d -> (%p)\n",
					depth, slot, x->n, x->scion, (void *)x);
		}
		if (btr->dirty_left) {
			if (findminnode(btr, btr->root) == x) {
#ifdef PRINT_EVICTED_KEYS
				if (is_inode)
					fprintf(fp, "\t\tDL: %u\n", btr->dirty_left);
				else {
					for (uint32 i = 1; i <= btr->dirty_left; i++) {
						fprintf(fp, "\t\t\t\t\tEVICTED KEY:\t\t\t%u\n", i);
					}
				}
#else
				fprintf(fp, "\t\tDL: %u\n", btr->dirty_left);
#endif
			}
		}
	}

	for (int i = 0; i < x->n; i++) {
		void *be  = KEYS(btr, x, i);
		ai_obj  akey;
		convertStream2Key(be, &akey, btr);
		void *rrow = parseStream(be, btr);
		if (is_index) {
			fprintf(fp, "\tINDEX-KEY: ");
			dump_ai_obj(fp, &akey);
			if (!SIMP_UNIQ(btr)) {
				if (!rrow) fprintf(fp, "\t\tTOTAL EVICTION\n");
				else       bt_dumptree(fp, (bt *)rrow, 0, 0);
			}
		} else {
			bool key_printed = 0;
			if        UU(btr) {
				key_printed = 1;
				ulong uu = (ulong)rrow;
				fprintf(fp, "\t\tUU[%d]: KEY: %lu VAL: %lu\n",
						i, (uu / UINT_MAX), (uu % UINT_MAX));
			} else if UL(btr) {
				if UP(btr) fprintf(fp, "\t\tUL: PTR: %p\t", rrow);
				else {
					key_printed = 1;
					ulk *ul = (ulk *)rrow;
					fprintf(fp, "\t\tUL[%d]: KEY: %u VAL: %lu\n",
							i, ul->key, ul->val);
				}
			} else if LU(btr) {
				if LUP(btr) fprintf(fp, "\t\tLU: PTR: %p\t", rrow);
				else {
					key_printed = 1;
					luk *lu = (luk *)rrow;
					fprintf(fp, "\t\tLU[%d]: KEY: %lu VAL: %u\n",
							i, lu->key, lu->val);
				}
			} else if LL(btr) {
				if LLP(btr) fprintf(fp, "\t\tLL: PTR: %p\t", rrow);
				else {
					key_printed = 1;
					llk *ll = (llk *)rrow;
					fprintf(fp, "\t\tLL[%d]: KEY: %lu VAL: %lu\n",
							i, ll->key, ll->val);
				}
			} else if UX(btr) {
				key_printed = 1;
				uxk *ux = (uxk *)rrow;
				fprintf(fp, "\t\tUX[%d]: KEY: %u ", i, ux->key);
				fprintf(fp, " VAL: ");
				DEBUG_U128(fp, ux->val);
				fprintf(fp, "\n");
			} else if XU(btr) {
				key_printed = 1;
				xuk *xu = (xuk *)rrow;
				fprintf(fp, "\t\tXU[%d]: KEY: ", i);
				DEBUG_U128(fp, xu->key);
				fprintf(fp, " VAL: %u\n", xu->val);
			} else if LX(btr) {
				key_printed = 1;
				lxk *lx = (lxk *)rrow;
				fprintf(fp, "\t\tLX[%d]: KEY: %lu ", i, lx->key);
				fprintf(fp, " VAL: ");
				DEBUG_U128(fp, lx->val);
				fprintf(fp, "\n");
			} else if XL(btr) {
				if XLP(btr) fprintf(fp, "\t\tXL: PTR: %p\t", rrow);
				else {
					key_printed = 1;
					xlk *xl = (xlk *)rrow;
					fprintf(fp, "\t\tXL[%d]: KEY: ", i);
					DEBUG_U128(fp, xl->key);
					fprintf(fp, " VAL: %lu\n", xl->val);
				}
			} else if XX(btr) {
				key_printed = 1;
				xxk *xx = (xxk *)rrow;
				fprintf(fp, "\t\tXX[%d]: KEY: ", i);
				DEBUG_U128(fp, xx->key);
				fprintf(fp, " VAL: ");
				DEBUG_U128(fp, xx->val);
				fprintf(fp, "\n");
			} else {
				bool gost = IS_GHOST(btr, rrow);
				if (gost) fprintf(fp, "\t\tROW [%d]: %p \tGHOST-", i, rrow);
				else      fprintf(fp, "\t\tROW [%d]: %p\t",        i, rrow);
			}
			if (!key_printed) {
				fprintf(fp, "KEY: ");
				dump_ai_obj(fp, &akey);
			}
			if (x->dirty > 0) {
#ifdef PRINT_EVICTED_KEYS
				uint32 dr = getDR(btr, x, i);
				if (is_inode && dr) fprintf(fp, "\t\t\t\tDR: %d\n", dr);
				else {
					ulong beg = C_IS_I(btr->s.ktype) ? akey.i : akey.l;
					for (ulong j = 1; j <= (ulong)dr; j++) {
						fprintf(fp, "\t\t\t\t\tEVICTED KEY:\t\t\t%lu\n", beg + j);
					}
				}
#else
				fprintf(fp, "\t\t\t\tDR: %d\n", getDR(btr, x, i));
#endif
			}
		}
	}
	if (!x->leaf) {
		depth++;
		for (int i = 0; i <= x->n; i++) {
			fprintf(fp, "\t\tNPTR[%d]: %p\n", i, NODES(btr, x)[i]);
		}
		for (int i = 0; i <= x->n; i++) {
			dump_tree_node(fp, btr, NODES(btr, x)[i], depth, is_index, i, is_inode);
		}
	}
}

void bt_dumptree(FILE *fp, bt *btr, bool is_index, bool is_inode)
{
	bt_dump_info(fp, btr);
	if (btr->root && btr->numkeys > 0) {
		dump_tree_node(fp, btr, btr->root, 0, is_index, 0, is_inode);
	}
	fprintf(fp, "\n");
}

/*
 *  Print out the structure and contents of a B-Tree.
 *  Return 0 if successful, -1 otherwise.
 */
int dump_btree(char *tbl_name, char *filename)
{
	int tmatch = -1;

	if (-1 == (tmatch = find_table(tbl_name))) {
		return -1;
	}

	FILE *fp = NULL;
	if (!(fp = fopen(filename, "w"))) {
		return -1;
	}

	r_tbl_t *rt = &Tbl[tmatch];
	fprintf(fp, "Table: %s columns\n", rt->name);
	for (int j = 0; j < rt->col_count; j++) {
		fprintf(fp, "\t%s | %s\n", rt->col[j].name,
                as_sindex_ktype_str((as_sindex_ktype) rt->col[j].type));
	}
	MATCH_INDICES(tmatch);
	bt *btr = getBtr(tmatch);

	bt_dumptree(fp, btr, 0, 0);

	if (matches) {
		for (int i = 0; i < matches; i++) {
			int j = inds[i];
			bt *ibtr = getIBtr(j);
			fprintf(fp, "INDEX: name: %s imatch: %d (%p)\n", Index[j].name, inds[i], (void *) ibtr);
			if (ibtr) {
				bt_dumptree(fp, ibtr, 1, 0);
			}
		}
	}

	fclose(fp);

	return 0;
}
