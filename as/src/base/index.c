/*
 * index.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
// Includes.
//

#include "base/index.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "arenax.h"
#include "fault.h"
#include "olock.h"

#include "base/cfg.h"
#include "base/datamodel.h"



//==========================================================
// Typedefs and macros.
//

#define RESOLVE_H(__h) ((as_index*)cf_arenax_resolve(tree->arena, __h))

typedef enum {
	AS_BLACK	= 0,
	AS_RED		= 1
} as_index_color;

// Flag to indicate full index reduce.
#define AS_REDUCE_ALL (-1)

typedef struct as_index_ph_s {
	as_index			*r;
	cf_arenax_handle	r_h;
} as_index_ph;

typedef struct as_index_ph_array_s {
	uint32_t	alloc_sz;
	uint32_t	pos;
	as_index_ph	indexes[];
} as_index_ph_array;

typedef struct as_index_ele_s {
	struct as_index_ele_s	*parent;
	cf_arenax_handle		me_h;
	as_index				*me;
} as_index_ele;



//==========================================================
// Forward declarations.
//

void as_index_tree_purge(as_index_tree *tree, as_index *r, cf_arenax_handle r_h);
void as_index_reduce_traverse(as_index_tree *tree, cf_arenax_handle r_h, cf_arenax_handle sentinel_h, as_index_ph_array *v_a);
void as_index_reduce_sync_traverse(as_index_tree *tree, as_index *r, cf_arenax_handle sentinel_h, as_index_reduce_sync_fn cb, void *udata);
int as_index_search_lockless(as_index_tree *tree, cf_digest *keyd, as_index **ret, cf_arenax_handle *ret_h);
void as_index_insert_rebalance(as_index_tree *tree, as_index_ele *ele);
void as_index_delete_rebalance(as_index_tree *tree, as_index_ele *ele);
void as_index_rotate_left(as_index_tree *tree, as_index_ele *a, as_index_ele *b);
void as_index_rotate_right(as_index_tree *tree, as_index_ele *a, as_index_ele *b);



//==========================================================
// Public API - create/resume/destroy/size a tree.
//

// Create a new red-black tree.
as_index_tree *
as_index_tree_create(cf_arenax *arena, as_index_value_destructor destructor,
		void *destructor_udata, as_treex *p_treex)
{
	as_index_tree *tree = cf_rc_alloc(sizeof(as_index_tree));

	if (! tree) {
		return NULL;
	}

	pthread_mutex_init(&tree->lock, NULL);
	pthread_mutex_init(&tree->reduce_lock, NULL);

	tree->arena = arena;

	// Make the sentinel element.
	tree->sentinel_h = cf_arenax_alloc(arena);

	if (tree->sentinel_h == 0) {
		cf_rc_free(tree);
		return NULL;
	}

	as_index *sentinel = RESOLVE_H(tree->sentinel_h);

	memset(sentinel, 0, sizeof(as_index));
	sentinel->left_h = sentinel->right_h = tree->sentinel_h;
	sentinel->color = AS_BLACK;

	// Make the fixed root element.
	tree->root_h = cf_arenax_alloc(arena);

	if (tree->root_h == 0) {
		cf_arenax_free(arena, tree->sentinel_h);
		cf_rc_free(tree);
		return NULL;
	}

	tree->root = RESOLVE_H(tree->root_h);
	memset(tree->root, 0, sizeof(as_index));
	tree->root->left_h = tree->root->right_h = tree->sentinel_h;
	tree->root->color = AS_BLACK;

	tree->destructor = destructor;
	tree->destructor_udata = destructor_udata;

	tree->elements = 0;

	if (p_treex) {
		// Update the tree information in persistent memory.
		p_treex->sentinel_h = tree->sentinel_h;
		p_treex->root_h = tree->root_h;
	}

	return tree;
}


// Resume a red-black tree in persistent memory.
// TODO - should really hide this in an EE version of as_index.c.
as_index_tree *
as_index_tree_resume(cf_arenax *arena, as_index_value_destructor destructor,
		void *destructor_udata, as_treex *p_treex)
{
	as_index_tree *tree = cf_rc_alloc(sizeof(as_index_tree));

	if (! tree) {
		return NULL;
	}

	pthread_mutex_init(&tree->lock, NULL);
	pthread_mutex_init(&tree->reduce_lock, NULL);

	tree->arena = arena;

	// Resume the sentinel.
	tree->sentinel_h = p_treex->sentinel_h;

	if (tree->sentinel_h == 0) {
		cf_rc_free(tree);
		return NULL;
	}

	// Resume the fixed root.
	tree->root_h = p_treex->root_h;

	if (tree->root_h == 0) {
		cf_rc_free(tree);
		return NULL;
	}

	tree->root = RESOLVE_H(tree->root_h);

	tree->destructor = destructor;
	tree->destructor_udata = destructor_udata;

	// We'll soon update this to its proper value by reducing the tree.
	tree->elements = 0;

	return tree;
}


// Destroy a red-black tree; return 0 if the tree was destroyed or 1 otherwise.
// TODO - nobody cares about the return value, make it void?
int
as_index_tree_release(as_index_tree *tree, void *destructor_udata)
{
	if (0 != cf_rc_release(tree)) {
		return 1;
	}

	as_index_tree_purge(tree, RESOLVE_H(tree->root->left_h),
			tree->root->left_h);

	cf_arenax_free(tree->arena, tree->root_h);
	cf_arenax_free(tree->arena, tree->sentinel_h);

	pthread_mutex_destroy(&tree->lock);
	pthread_mutex_destroy(&tree->reduce_lock);

	memset(tree, 0, sizeof(as_index_tree)); // paranoia - for debugging only
	cf_rc_free(tree);

	return 0;
}


// Get the number of elements in the tree.
uint32_t
as_index_tree_size(as_index_tree *tree)
{
	pthread_mutex_lock(&tree->lock);

	uint32_t sz = tree->elements;

	pthread_mutex_unlock(&tree->lock);

	return sz;
}



//==========================================================
// Public API - reduce a tree.
//

// Make a callback for every element in the tree, from outside the tree lock.
void
as_index_reduce(as_index_tree *tree, as_index_reduce_fn cb, void *udata)
{
	as_index_reduce_partial(tree, AS_REDUCE_ALL, cb, udata);
}


// Make a callback for a specified number of elements in the tree, from outside
// the tree lock.
void
as_index_reduce_partial(as_index_tree *tree, uint32_t sample_count,
		as_index_reduce_fn cb, void *udata)
{
	pthread_mutex_lock(&tree->reduce_lock);

	// For full reduce, get the number of elements inside the tree lock.
	if (sample_count == AS_REDUCE_ALL) {
		sample_count = tree->elements;
	}

	if (sample_count == 0) {
		pthread_mutex_unlock(&tree->reduce_lock);
		return;
	}

	size_t sz = sizeof(as_index_ph_array) +
			(sizeof(as_index_ph) * sample_count);
	as_index_ph_array *v_a;
	uint8_t buf[64 * 1024];

	if (sz > 64 * 1024) {
		v_a = cf_malloc(sz);

		if (! v_a) {
			pthread_mutex_unlock(&tree->reduce_lock);
			return;
		}
	}
	else {
		v_a = (as_index_ph_array*)buf;
	}

	v_a->alloc_sz = sample_count;
	v_a->pos = 0;

	uint64_t start_ms = cf_getms();

	// Recursively, fetch all the value pointers into this array, so we can make
	// all the callbacks outside the big lock.
	if (tree->root->left_h != tree->sentinel_h) {
		as_index_reduce_traverse(tree, tree->root->left_h, tree->sentinel_h,
				v_a);
	}

	cf_debug(AS_INDEX, "as_index_reduce_traverse took %"PRIu64" ms",
			cf_getms() - start_ms);

	pthread_mutex_unlock(&tree->reduce_lock);

	for (uint32_t i = 0; i < v_a->pos; i++) {
		as_index_ref r_ref;

		r_ref.skip_lock = false;
		r_ref.r = v_a->indexes[i].r;
		r_ref.r_h = v_a->indexes[i].r_h;

		olock_vlock(g_config.record_locks, &r_ref.r->key, &r_ref.olock);
		cf_atomic_int_incr(&g_config.global_record_lock_count);

		// Callback MUST call as_record_done() to unlock and release record.
		cb(&r_ref, udata);
	}

	if (v_a != (as_index_ph_array*)buf) {
		cf_free(v_a);
	}
}


// Make a callback for every element in the tree, from under the tree lock.
void
as_index_reduce_sync(as_index_tree *tree, as_index_reduce_sync_fn cb,
		void *udata)
{
	pthread_mutex_lock(&tree->reduce_lock);

	if (tree->root->left_h != tree->sentinel_h) {
		as_index_reduce_sync_traverse(tree, RESOLVE_H(tree->root->left_h),
				tree->sentinel_h, cb, udata);
	}

	pthread_mutex_unlock(&tree->reduce_lock);
}



//==========================================================
// Public API - get/insert/delete an element in a tree.
//

// Is there an element with specified digest in the tree?
//
// Returns:
//		 0 - found (yes)
//		-1 - not found (no)
int
as_index_exists(as_index_tree *tree, cf_digest *keyd)
{
	pthread_mutex_lock(&tree->lock);

	int rv = as_index_search_lockless(tree, keyd, NULL, NULL);

	pthread_mutex_unlock(&tree->lock);

	return rv;
}


// If there's an element with specified digest in the tree, return a locked
// and reserved reference to it in index_ref.
//
// Returns:
//		 0 - found (reference returned in index_ref)
//		-1 - not found (index_ref untouched)
int
as_index_get_vlock(as_index_tree *tree, cf_digest *keyd,
		as_index_ref *index_ref)
{
	pthread_mutex_lock(&tree->lock);

	int rv = as_index_search_lockless(tree, keyd, &index_ref->r,
			&index_ref->r_h);

	if (rv != 0) {
		pthread_mutex_unlock(&tree->lock);
		return rv;
	}

	as_index_reserve(index_ref->r);
	cf_atomic_int_incr(&g_config.global_record_ref_count);

	pthread_mutex_unlock(&tree->lock);

	if (! index_ref->skip_lock) {
		olock_vlock(g_config.record_locks, keyd, &index_ref->olock);
		cf_atomic_int_incr(&g_config.global_record_lock_count);
	}

	return 0;
}


// If there's an element with specified digest in the tree, return a locked
// and reserved reference to it in index_ref. If not, create an element with
// this digest, insert it into the tree, and return a locked and reserved
// reference to it in index_ref.
//
// Returns:
//		 1 - created and inserted (reference returned in index_ref)
//		 0 - found already existing (reference returned in index_ref)
//		-1 - error (index_ref untouched)
int
as_index_get_insert_vlock(as_index_tree *tree, cf_digest *keyd,
		as_index_ref *index_ref)
{
	int cmp = 0;
	bool retry;

	// Save parents as we search for the specified element's insertion point.
	as_index_ele eles[64];
	as_index_ele *ele;

	do {
		ele = eles;

		pthread_mutex_lock(&tree->lock);

		// Search for the specified element, or a parent to insert it under.

		ele->parent = NULL; // we'll never look this far up
		ele->me_h = tree->root_h;
		ele->me = tree->root;

		cf_arenax_handle t_h = tree->root->left_h;
		as_index *t = RESOLVE_H(t_h);

		while (t_h != tree->sentinel_h) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = t_h;
			ele->me = t;

			if ((cmp = cf_digest_compare(keyd, &t->key)) == 0) {
				// The element already exists, simply return it.

				as_index_reserve(t);
				cf_atomic_int_incr(&g_config.global_record_ref_count);

				pthread_mutex_unlock(&tree->lock);

				if (! index_ref->skip_lock) {
					olock_vlock(g_config.record_locks, keyd, &index_ref->olock);
					cf_atomic_int_incr(&g_config.global_record_lock_count);
				}

				index_ref->r = t;
				index_ref->r_h = t_h;

				return 0;
			}

			t_h = cmp > 0 ? t->left_h : t->right_h;
			t = RESOLVE_H(t_h);
		}

		// We didn't find the tree element, so we'll be inserting it.

		retry = false;

		if (EBUSY == pthread_mutex_trylock(&tree->reduce_lock)) {
			// The tree is being reduced - could take long, unlock so reads and
			// overwrites aren't blocked.
			pthread_mutex_unlock(&tree->lock);

			// Wait until the tree reduce is done...
			pthread_mutex_lock(&tree->reduce_lock);
			pthread_mutex_unlock(&tree->reduce_lock);

			// ... and start over - we unlocked, so the tree may have changed.
			retry = true;
		}
	} while (retry);

	// Create a new element and insert it.

	// Make the new element.
	cf_arenax_handle n_h = cf_arenax_alloc(tree->arena);

	if (n_h == 0) {
		cf_warning(AS_INDEX, "arenax alloc failed");
		pthread_mutex_unlock(&tree->reduce_lock);
		pthread_mutex_unlock(&tree->lock);
		return -1;
	}

	as_index *n = RESOLVE_H(n_h);

	n->rc = 2; // one for create (eventually balanced by delete), one for caller
	cf_atomic_int_add(&g_config.global_record_ref_count, 2);

	n->key = *keyd;

	n->left_h = n->right_h = tree->sentinel_h; // n starts as a leaf element
	n->color = AS_RED; // n's color starts as red

	// Make sure we can detect that the record isn't initialized.
	as_index_clear_record_info(n);

	// Insert the new element n under parent ele.
	if (ele->me == tree->root || 0 < cmp) {
		ele->me->left_h = n_h;
	}
	else {
		ele->me->right_h = n_h;
	}

	ele++;
	ele->parent = ele - 1;
	ele->me_h = n_h;
	ele->me = n;

	// Rebalance the tree as needed.
	as_index_insert_rebalance(tree, ele);

	tree->elements++;

	pthread_mutex_unlock(&tree->reduce_lock);
	pthread_mutex_unlock(&tree->lock);

	if (! index_ref->skip_lock) {
		olock_vlock(g_config.record_locks, keyd, &index_ref->olock);
		cf_atomic_int_incr(&g_config.global_record_lock_count);
	}

	index_ref->r = n;
	index_ref->r_h = n_h;

	return 1;
}


// If there's an element with specified digest in the tree, delete it.
//
// Returns:
//		 0 - found and deleted
//		-1 - not found
// TODO - nobody cares about the return value, make it void?
int
as_index_delete(as_index_tree *tree, cf_digest *keyd)
{
	as_index *r;
	cf_arenax_handle r_h;
	bool retry;

	// Save parents as we search for the specified element (or its successor).
	as_index_ele eles[(64 * 2) + 3];
	as_index_ele *ele;

	do {
		ele = eles;

		pthread_mutex_lock(&tree->lock);

		ele->parent = NULL; // we'll never look this far up
		ele->me_h = tree->root_h;
		ele->me = tree->root;

		r_h = tree->root->left_h;
		r = RESOLVE_H(r_h);

		while (r_h != tree->sentinel_h) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = r_h;
			ele->me = r;

			int cmp = cf_digest_compare(keyd, &r->key);

			if (cmp == 0) {
				break; // found, we'll be deleting it
			}

			r_h = cmp > 0 ? r->left_h : r->right_h;
			r = RESOLVE_H(r_h);
		}

		if (r_h == tree->sentinel_h) {
			pthread_mutex_unlock(&tree->lock);
			return -1; // not found, nothing to delete
		}

		// We found the tree element, so we'll be deleting it.

		retry = false;

		if (EBUSY == pthread_mutex_trylock(&tree->reduce_lock)) {
			// The tree is being reduced - could take long, unlock so reads and
			// overwrites aren't blocked.
			pthread_mutex_unlock(&tree->lock);

			// Wait until the tree reduce is done...
			pthread_mutex_lock(&tree->reduce_lock);
			pthread_mutex_unlock(&tree->reduce_lock);

			// ... and start over - we unlocked, so the tree may have changed.
			retry = true;
		}
	} while (retry);

	// Delete the element.

	// Snapshot the element to delete, r. (Already have r_h and r shortcuts.)
	as_index_ele *r_e = ele;

	if (r->left_h != tree->sentinel_h && r->right_h != tree->sentinel_h) {
		// Search down for a "successor"...

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r->right_h;
		ele->me = RESOLVE_H(ele->me_h);

		while (ele->me->left_h != tree->sentinel_h) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = ele->parent->me->left_h;
			ele->me = RESOLVE_H(ele->me_h);
		}
	}
	// else ele is left at r, i.e. s == r

	// Snapshot the successor, s. (Note - s could be r.)
	as_index_ele *s_e = ele;
	cf_arenax_handle s_h = s_e->me_h;
	as_index *s = s_e->me;

	// Get the appropriate child of s. (Note - child could be sentinel.)
	ele++;

	if (s->left_h == tree->sentinel_h) {
		ele->me_h = s->right_h;
	}
	else {
		ele->me_h = s->left_h;
	}

	ele->me = RESOLVE_H(ele->me_h);

	// Cut s (remember, it could be r) out of the tree.
	ele->parent = s_e->parent;

	if (s_h == s_e->parent->me->left_h) {
		s_e->parent->me->left_h = ele->me_h;
	}
	else {
		s_e->parent->me->right_h = ele->me_h;
	}

	// Rebalance at ele if necessary. (Note - if r != s, r is in the tree, and
	// its parent may change during rebalancing.)
	if (s->color == AS_BLACK) {
		as_index_delete_rebalance(tree, ele);
	}

	if (s != r) {
		// s was a successor distinct from r, put it in r's place in the tree.
		s->left_h = r->left_h;
		s->right_h = r->right_h;
		s->color = r->color;

		if (r_h == r_e->parent->me->left_h) {
			r_e->parent->me->left_h = s_h;
		}
		else {
			r_e->parent->me->right_h = s_h;
		}
	}

	// We may now destroy r, which is no longer in the tree.
	if (0 == as_index_release(r)) {
		if (tree->destructor) {
			tree->destructor(r, tree->destructor_udata);
		}

		cf_arenax_free(tree->arena, r_h);
	}

	cf_atomic_int_decr(&g_config.global_record_ref_count);

	tree->elements--;

	pthread_mutex_unlock(&tree->reduce_lock);
	pthread_mutex_unlock(&tree->lock);

	return 0;
}



//==========================================================
// Local helpers.
//

void
as_index_tree_purge(as_index_tree *tree, as_index *r, cf_arenax_handle r_h)
{
	// Don't purge the sentinel.
	if (r_h == tree->sentinel_h) {
		return;
	}

	as_index_tree_purge(tree, RESOLVE_H(r->left_h), r->left_h);
	as_index_tree_purge(tree, RESOLVE_H(r->right_h), r->right_h);

	if (0 == as_index_release(r)) {
		if (tree->destructor) {
			tree->destructor(r, tree->destructor_udata);
		}

		cf_arenax_free(tree->arena, r_h);
	}

	cf_atomic_int_decr(&g_config.global_record_ref_count);
}


void
as_index_reduce_traverse(as_index_tree *tree, cf_arenax_handle r_h,
		cf_arenax_handle sentinel_h, as_index_ph_array *v_a)
{
	if (v_a->pos >= v_a->alloc_sz) {
		return;
	}

	as_index *r = RESOLVE_H(r_h);

	as_index_reserve(r);
	cf_atomic_int_incr(&g_config.global_record_ref_count);

	v_a->indexes[v_a->pos].r = r;
	v_a->indexes[v_a->pos].r_h = r_h;
	v_a->pos++;

	if (r->left_h != sentinel_h) {
		as_index_reduce_traverse(tree, r->left_h, sentinel_h, v_a);
	}

	if (r->right_h != sentinel_h) {
		as_index_reduce_traverse(tree, r->right_h, sentinel_h, v_a);
	}
}


void
as_index_reduce_sync_traverse(as_index_tree *tree, as_index *r,
		cf_arenax_handle sentinel_h, as_index_reduce_sync_fn cb, void *udata)
{
	cb(r, udata);

	if (r->left_h != sentinel_h) {
		as_index_reduce_sync_traverse(tree, RESOLVE_H(r->left_h), sentinel_h,
				cb, udata);
	}

	if (r->right_h != sentinel_h) {
		as_index_reduce_sync_traverse(tree, RESOLVE_H(r->right_h), sentinel_h,
				cb, udata);
	}
}


int
as_index_search_lockless(as_index_tree *tree, cf_digest *keyd, as_index **ret,
		cf_arenax_handle *ret_h)
{
	cf_arenax_handle r_h = tree->root->left_h;
	as_index *r = RESOLVE_H(r_h);

	while (r_h != tree->sentinel_h) {
		int cmp = cf_digest_compare(keyd, &r->key);

		if (cmp == 0) {
			if (ret_h) {
				*ret_h = r_h;
			}

			if (ret) {
				*ret = r;
			}

			return 0; // found
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
		r = RESOLVE_H(r_h);
	}

	return -1; // not found
}


void
as_index_insert_rebalance(as_index_tree *tree, as_index_ele *ele)
{
	// Entering here, ele is the last element on the stack. It turns out during
	// insert rebalancing we won't ever need new elements on the stack, but make
	// this resemble delete rebalance - define r_e to go back up the tree.
	as_index_ele *r_e = ele;
	as_index_ele *parent_e = r_e->parent;

	while (parent_e->me->color == AS_RED) {
		as_index_ele *grandparent_e = parent_e->parent;

		if (r_e->parent->me_h == grandparent_e->me->left_h) {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->right_h;
			as_index *u = RESOLVE_H(u_h);

			if (u->color == AS_RED) {
				u->color = AS_BLACK;
				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->right_h) {
					// Save original r, which will become new r's parent.
					as_index_ele *r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_left(tree, r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_right(tree, grandparent_e, parent_e);
			}
		}
		else {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->left_h;
			as_index *u = RESOLVE_H(u_h);

			if (u->color == AS_RED) {
				u->color = AS_BLACK;
				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->left_h) {
					// Save original r, which will become new r's parent.
					as_index_ele *r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_right(tree, r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = AS_BLACK;
				grandparent_e->me->color = AS_RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_left(tree, grandparent_e, parent_e);
			}
		}
	}

	RESOLVE_H(tree->root->left_h)->color = AS_BLACK;
}


void
as_index_delete_rebalance(as_index_tree *tree, as_index_ele *ele)
{
	// Entering here, ele is the last element on the stack. It's possible as r_e
	// crawls up the tree, we'll need new elements on the stack, in which case
	// ele keeps building the stack down while r_e goes up.
	as_index_ele *r_e = ele;

	while (r_e->me->color == AS_BLACK && r_e->me_h != tree->root->left_h) {
		as_index *r_parent = r_e->parent->me;

		if (r_e->me_h == r_parent->left_h) {
			cf_arenax_handle s_h = r_parent->right_h;
			as_index *s = RESOLVE_H(s_h);

			if (s->color == AS_RED) {
				s->color = AS_BLACK;
				r_parent->color = AS_RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(tree, r_e->parent, ele);

				s_h = r_parent->right_h;
				s = RESOLVE_H(s_h);
			}

			as_index *s_left = RESOLVE_H(s->left_h);
			as_index *s_right = RESOLVE_H(s->right_h);

			if (s_left->color == AS_BLACK && s_right->color == AS_BLACK) {
				s->color = AS_RED;

				r_e = r_e->parent;
			}
			else {
				if (s_right->color == AS_BLACK) {
					s_left->color = AS_BLACK;
					s->color = AS_RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					as_index_ele *s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->left_h;
					ele->me = s_left;

					as_index_rotate_right(tree, s_e, ele);

					s_h = r_parent->right_h;
					s = s_left; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = AS_BLACK;
				RESOLVE_H(s->right_h)->color = AS_BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(tree, r_e->parent, ele);

				RESOLVE_H(tree->root->left_h)->color = AS_BLACK;

				return;
			}
		}
		else {
			cf_arenax_handle s_h = r_parent->left_h;
			as_index *s = RESOLVE_H(s_h);

			if (s->color == AS_RED) {
				s->color = AS_BLACK;
				r_parent->color = AS_RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(tree, r_e->parent, ele);

				s_h = r_parent->left_h;
				s = RESOLVE_H(s_h);
			}

			as_index *s_left = RESOLVE_H(s->left_h);
			as_index *s_right = RESOLVE_H(s->right_h);

			if (s_left->color == AS_BLACK && s_right->color == AS_BLACK) {
				s->color = AS_RED;

				r_e = r_e->parent;
			}
			else {
				if (s_left->color == AS_BLACK) {
					s_right->color = AS_BLACK;
					s->color = AS_RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					as_index_ele *s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->right_h;
					ele->me = s_right;

					as_index_rotate_left(tree, s_e, ele);

					s_h = r_parent->left_h;
					s = s_right; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = AS_BLACK;
				RESOLVE_H(s->left_h)->color = AS_BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(tree, r_e->parent, ele);

				RESOLVE_H(tree->root->left_h)->color = AS_BLACK;

				return;
			}
		}
	}

	r_e->me->color = AS_BLACK;
}


void
as_index_rotate_left(as_index_tree *tree, as_index_ele *a, as_index_ele *b)
{
	// Element b is element a's right child - a will become b's left child.

	/*        p      -->      p
	 *        |               |
	 *        a               b
	 *       / \             / \
	 *     [x]  b           a  [y]
	 *         / \         / \
	 *        c  [y]     [x]  c
	 */

	// Set a's right child to c, b's former left child.
	a->me->right_h = b->me->left_h;

	// Set p's left or right child (whichever a was) to b.
	if (a->me_h == a->parent->me->left_h) {
		a->parent->me->left_h = b->me_h;
	}
	else {
		a->parent->me->right_h = b->me_h;
	}

	// Set b's parent to p, a's old parent.
	b->parent = a->parent;

	// Set b's left child to a, and a's parent to b.
	b->me->left_h = a->me_h;
	a->parent = b;
}


void
as_index_rotate_right(as_index_tree *tree, as_index_ele *a, as_index_ele *b)
{
	// Element b is element a's left child - a will become b's right child.

	/*        p      -->      p
	 *        |               |
	 *        a               b
	 *       / \             / \
	 *      b  [x]         [y]  a
	 *     / \                 / \
	 *   [y]  c               c  [x]
	 */

	// Set a's left child to c, b's former right child.
	a->me->left_h = b->me->right_h;

	// Set p's left or right child (whichever a was) to b.
	if (a->me_h == a->parent->me->left_h) {
		a->parent->me->left_h = b->me_h;
	}
	else {
		a->parent->me->right_h = b->me_h;
	}

	// Set b's parent to p, a's old parent.
	b->parent = a->parent;

	// Set b's right child to a, and a's parent to b.
	b->me->right_h = a->me_h;
	a->parent = b;
}



//==========================================================
// KV API - currently unmaintained.
//

#ifdef USE_KV
/*
 * Create a tree "stub" for the storage has index case.
 * Returns:  1 = new
 *           0 = success (found)
 *          -1 = fail
 */
int
as_index_ref_initialize(as_index_tree *tree, cf_digest *key, as_index_ref *index_ref, bool create_p, as_namespace *ns)
{
	/* Allocate memory for the new node and set the node parameters */
	cf_arenax_handle n_h = cf_arenax_alloc(tree->arena);
	if (0 == n_h) {
		// cf_debug(AS_INDEX," malloc failed ");
		return(-1);
	}
	as_index *n = RESOLVE_H(n_h);
	n->key = *key;
	n->rc = 1;
	n->left_h = n->right_h = tree->sentinel_h;
	n->color = AS_RED;
	n->parent_h = tree->sentinel_h;

	if (AS_STORAGE_ENGINE_KV == ns->storage_type)
		n->storage_key.kv.file_id = STORAGE_INVALID_FILE_ID; // careful here - this is now unsigned
	else
		cf_crash(AS_INDEX, "non-KV storage type ns %s key %p", ns->name, key);

	index_ref->r = n;
	index_ref->r_h = n_h;
	if (!index_ref->skip_lock) {
		olock_vlock(g_config.record_locks, key, &(index_ref->olock));
		cf_atomic_int_incr(&g_config.global_record_lock_count);
	}
	as_index_reserve(n);
	cf_atomic_int_add(&g_config.global_record_ref_count, 2);

	int rv = !as_storage_record_exists(ns, key);

	// Unlock if not found and we're not creating it.
	if (rv && !create_p) {
		if (!index_ref->skip_lock) {
			pthread_mutex_unlock(index_ref->olock);
			cf_atomic_int_decr(&g_config.global_record_lock_count);
		}
		as_index_release(n);
		cf_atomic_int_decr(&g_config.global_record_ref_count);
		cf_arenax_free(tree->arena, n_h);
		index_ref->r = 0;
		index_ref->r_h = 0;
	}

	return(rv);
}
#endif // USE_KV
