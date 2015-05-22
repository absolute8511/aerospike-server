/*
 * index.c
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
 * A special purpose red-black tree which holds the information about a 'record'
 */

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"

#include "base/index.h"
#include "base/cfg.h"


#define RESOLVE_H( __h ) ((as_index *) cf_arenax_resolve(tree->arena, __h))

enum {
	CF_RCRB_BLACK,
	CF_RCRB_RED
};

/* as_indexrotate_left
 * Rotate a tree left - r's parent might change */
void
as_index_rotate_left(as_index_tree *tree, as_index *r, cf_arenax_handle r_h)
{
	cf_arenax_handle s_h = r->right_h;
	as_index *s = RESOLVE_H(s_h);

	/* Establish r->right */
	r->right_h = s->left_h;
	if (s->left_h != tree->sentinel_h)
		RESOLVE_H(s->left_h)->parent_h = r_h;

	/* Establish the new parent */
	s->parent_h = r->parent_h;
	as_index *r_parent = RESOLVE_H(r->parent_h);
	if (r_h == r_parent->left_h)
		r_parent->left_h = s_h;
	else
		r_parent->right_h = s_h;

	/* Tidy up the pointers */
	s->left_h = r_h;
	r->parent_h = s_h;
}


/* as_index_rotate_right
 * Rotate a tree right - r's parent might change */
void
as_index_rotate_right(as_index_tree *tree, as_index *r, cf_arenax_handle r_h)
{
	cf_arenax_handle s_h = r->left_h;
	as_index *s = RESOLVE_H(s_h);

	/* Establish r->left */
	r->left_h = s->right_h;
	if (s->right_h != tree->sentinel_h)
		RESOLVE_H(s->right_h)->parent_h = r_h;

	/* Establish the new parent */
	s->parent_h = r->parent_h;
	as_index *r_parent = RESOLVE_H(r->parent_h);
	if (r_h == r_parent->left_h)
		r_parent->left_h = s_h;
	else
		r_parent->right_h = s_h;

	/* Tidy up the pointers */
	s->right_h = r_h;
	r->parent_h = s_h;
}

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
	n->color = CF_RCRB_RED;
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
		cf_arenax_free(tree->arena, n_h);
		index_ref->r = 0;
		index_ref->r_h = 0;
	}

	return(rv);
}

/* as_index_get_insert
 * Get or insert a node with a given tree into a red-black tree.
 *
 * The purpose of this admittadly strange API is it allows the caller to insert a new value after
 * determinging whether the item existed in the first place.
 *
 * 1 = new
 * 0 = success (found)
 * -1 = fail
 * */
int
as_index_get_insert_vlock(as_index_tree *tree, cf_digest *key, as_index_ref *index_ref)
{
	as_index 		*n, *s, *t;
	cf_arenax_handle n_h, s_h, t_h;
	bool retry;

	do {
		/* Lock the tree */
		pthread_mutex_lock(&tree->lock);

		/* Insert the node directly into the tree, via the typical method of
		 * binary tree insertion */
		s_h = tree->root_h;
		s = tree->root;

		t_h = tree->root->left_h;
		t = RESOLVE_H(t_h);

		// cf_debug(AS_INDEX,"get-insert: key %"PRIx64" sentinal %p",*(uint64_t *)key, tree->sentinel);

		while (t_h != tree->sentinel_h) {

			s = t;
			s_h = t_h;
	//		cf_debug(AS_INDEX,"  at %p: key %"PRIx64": right %p left %p",t,*(uint64_t *)&t->key,t->right,t->left);

			int c = cf_digest_compare(key, &t->key);
			if (c) {
				t_h = (c > 0) ? t->left_h : t->right_h;
				t = RESOLVE_H(t_h);
			}
			else
				break;
		}

		/* If the node already exists, simply return it */
		if ((s != tree->root) && (0 == cf_digest_compare(key, &s->key))) {
			as_index_reserve(s);
			cf_atomic_int_incr(&g_config.global_record_ref_count);
			pthread_mutex_unlock(&tree->lock);
			if (!index_ref->skip_lock) {
				olock_vlock(g_config.record_locks, key, &(index_ref->olock) );
				cf_atomic_int_incr(&g_config.global_record_lock_count);
			}
			index_ref->r = s;
			index_ref->r_h = s_h;
			return(0);
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

//	cf_debug(AS_INDEX,"get-insert: not found");

	/* Allocate memory for the new node and set the node parameters */
	n_h = cf_arenax_alloc(tree->arena);
	if (0 == n_h) {
		cf_warning(AS_INDEX, "arenax alloc failed");
		pthread_mutex_unlock(&tree->reduce_lock);
		pthread_mutex_unlock(&tree->lock);
		return(-1);
	}
	n = RESOLVE_H(n_h);
	n->key = *key;
	n->rc = 1;
	n->left_h = n->right_h = tree->sentinel_h;
	n->color = CF_RCRB_RED;
	n->parent_h = s_h;

	// Make sure we can detect that the record isn't initialized.
	as_index_clear_record_info(n);

	// bookkeeping the index
	index_ref->r = n;
	index_ref->r_h = n_h;
	as_index_reserve(n);
	cf_atomic_int_add(&g_config.global_record_ref_count, 2);

	/* Insert the node */
	if ((s == tree->root) || (0 < cf_digest_compare(&n->key, &s->key)))
		s->left_h = n_h;
	else
		s->right_h = n_h;

	/* Rebalance the tree */
	as_index *n_parent = RESOLVE_H(n->parent_h);
	while (CF_RCRB_RED == n_parent->color) {
		n_parent = RESOLVE_H(n->parent_h);
		as_index *n_parent_parent = RESOLVE_H(n_parent->parent_h);
		if (n->parent_h == n_parent_parent->left_h) {
			s_h = n_parent_parent->right_h;
			s = RESOLVE_H(s_h);
			if (CF_RCRB_RED == s->color) {
				n_parent->color = CF_RCRB_BLACK;
				s->color = CF_RCRB_BLACK;
				n_parent_parent->color = CF_RCRB_RED;
				n_h = n_parent->parent_h;
				n = RESOLVE_H(n_h);
				n_parent = RESOLVE_H(n->parent_h);
			} else {
				if (n_h == n_parent->right_h) {
					n_h = n->parent_h;
					n = n_parent;
					as_index_rotate_left(tree, n, n_h);
				}
				n_parent = RESOLVE_H(n->parent_h);
				n_parent->color = CF_RCRB_BLACK;
				n_parent_parent = RESOLVE_H(n_parent->parent_h);
				n_parent_parent->color = CF_RCRB_RED;
				as_index_rotate_right(tree, n_parent_parent, n_parent->parent_h);
			}
		} else {
			s_h = n_parent_parent->left_h;
			s = RESOLVE_H(s_h);
			if (CF_RCRB_RED == s->color) {
				n_parent->color = CF_RCRB_BLACK;
				s->color = CF_RCRB_BLACK;
				n_parent_parent->color = CF_RCRB_RED;
				n_h = n_parent->parent_h;
				n = RESOLVE_H(n_h);
				n_parent = RESOLVE_H(n->parent_h);
			} else {
				if (n_h == n_parent->left_h) {
					n_h = n->parent_h;
					n = n_parent;
					as_index_rotate_right(tree, n, n_h);
				}
				n_parent = RESOLVE_H(n->parent_h);
				n_parent->color = CF_RCRB_BLACK;
				n_parent_parent = RESOLVE_H(n_parent->parent_h);
				n_parent_parent->color = CF_RCRB_RED;
				as_index_rotate_left(tree, n_parent_parent, n_parent->parent_h);
			}
		}
	}
	RESOLVE_H(tree->root->left_h)->color = CF_RCRB_BLACK;
	tree->elements++;

	pthread_mutex_unlock(&tree->reduce_lock);
	pthread_mutex_unlock(&tree->lock);

	if (!index_ref->skip_lock) {
		olock_vlock(g_config.record_locks, key, &(index_ref->olock) );
		cf_atomic_int_incr(&g_config.global_record_lock_count);
	}

	return(1);
}




/* as_index_successor
 * Find the successor to a given node */
int
as_index_successor(as_index_tree *tree, as_index *n, cf_arenax_handle n_h, as_index **r, cf_arenax_handle *r_h)
{
	as_index *s;
	cf_arenax_handle s_h;

	s_h = n->right_h;
	s = RESOLVE_H(s_h);

	if (tree->sentinel != s) {
		while (tree->sentinel_h != s->left_h) {
			s_h = s->left_h;
			s = RESOLVE_H(s_h);
		}
		*r_h = s_h;
		*r = s;
		return(0);
	} else {

		s_h = n->parent_h;
		s = RESOLVE_H(s_h);

		while (n_h == s->right_h) {
			n = s;
			n_h = s_h;

			s_h = s->parent_h;
			s = RESOLVE_H(s_h);
		}

		if (tree->root == s) {
			*r_h = tree->sentinel_h;
			*r = tree->sentinel;
			return(0);
		}

		*r_h = s_h;
		*r = s;
		return(0);
	}
}


/* as_index_deleterebalance
 * Rebalance a red-black tree after removing a node */
void
as_index_deleterebalance(as_index_tree *tree, as_index *r, cf_arenax_handle r_h)
{
	cf_arenax_handle s_h;
	as_index *s;

	while ((CF_RCRB_BLACK == r->color) && (tree->root->left_h != r_h)) {

		as_index *r_parent = RESOLVE_H(r->parent_h);

		if (r_h == r_parent->left_h) {

			s_h = r_parent->right_h;
			s = RESOLVE_H(s_h);

			if (CF_RCRB_RED == s->color) {
				s->color = CF_RCRB_BLACK;
				r_parent->color = CF_RCRB_RED;
				as_index_rotate_left(tree, r_parent, r->parent_h);
				s_h = r_parent->right_h;
				s = RESOLVE_H(s_h);
			}

			if ((CF_RCRB_RED != RESOLVE_H(s->right_h)->color) &&
					(CF_RCRB_RED != RESOLVE_H(s->left_h)->color)) {
				s->color = CF_RCRB_RED;
				r_h = r->parent_h;
				r = r_parent;
			} else {
				if (CF_RCRB_RED != RESOLVE_H(s->right_h)->color) {
					RESOLVE_H(s->left_h)->color = CF_RCRB_BLACK;
					s->color = CF_RCRB_RED;
					as_index_rotate_right(tree, s, s_h);
					s_h = r_parent->right_h;
					s = RESOLVE_H(s_h);
				}
				s->color = r_parent->color;
				r_parent->color = CF_RCRB_BLACK;
				RESOLVE_H(s->right_h)->color = CF_RCRB_BLACK;
				as_index_rotate_left(tree, r_parent, r->parent_h);
				r_h = tree->root->left_h;
				r = RESOLVE_H(r_h);
			}

		} else {

			/* This is a mirror image of the code above */
			s_h = r_parent->left_h;
			s = RESOLVE_H(s_h);

			if (CF_RCRB_RED == s->color) {
				s->color = CF_RCRB_BLACK;
				r_parent->color = CF_RCRB_RED;
				as_index_rotate_right(tree, r_parent, r->parent_h);
				s_h = r_parent->left_h;
				s = RESOLVE_H(s_h);
			}

			if ((CF_RCRB_RED != RESOLVE_H(s->right_h)->color) &&
					(CF_RCRB_RED != RESOLVE_H(s->left_h)->color)) {
				s->color = CF_RCRB_RED;
				r_h = r->parent_h;
				r = r_parent;
			} else {
				if (CF_RCRB_RED != RESOLVE_H(s->left_h)->color) {
					RESOLVE_H(s->right_h)->color = CF_RCRB_BLACK;
					s->color = CF_RCRB_RED;
					as_index_rotate_left(tree, s, s_h);
					s_h = r_parent->left_h;
					s = RESOLVE_H(s_h);
				}
				s->color = r_parent->color;
				r_parent->color = CF_RCRB_BLACK;
				RESOLVE_H(s->left_h)->color = CF_RCRB_BLACK;
				as_index_rotate_right(tree, r_parent, r->parent_h);
				r_h = tree->root->left_h;
				r = RESOLVE_H(r_h);
			}
		}
	}
	r->color = CF_RCRB_BLACK;

	return;
}


/* as_index_search_lockless
 * Perform a lockless search for a node in a red-black tree */
as_index *
as_index_search_lockless(as_index_tree *tree, cf_digest *key)
{
	/* If there are no entries in the tree, we're done */
	if (tree->root->left_h == tree->sentinel_h)
		goto miss;

	cf_arenax_handle r_h = tree->root->left_h;
	as_index *r = RESOLVE_H(r_h);
	int c;

	// cf_debug(AS_INDEX,"search: key %"PRIx64" sentinal %p",*(uint64_t *)&key, tree->sentinel);

	while (r != tree->sentinel) {

//		cf_debug(AS_INDEX,"  at %p: key %"PRIx64": right %p left %p",s,*(uint64_t *)&s->key,s->right,s->left);

		c = cf_digest_compare(key, &r->key);
		if (c) {
			r_h = (c > 0) ? r->left_h : r->right_h;
			r = RESOLVE_H(r_h);
		}
		else
			return(r);
	}

	/* No matches found */
miss:
	return(NULL);
}

/* as_index_search_lockless
 * Perform a lockless search for a node in a red-black tree */
int
as_index_search_h_lockless(as_index_tree *tree, cf_digest *key, as_index **ret, cf_arenax_handle *ret_h)
{
	/* If there are no entries in the tree, we're done */
	if (tree->root->left_h == tree->sentinel_h)
		goto miss;

	cf_arenax_handle r_h = tree->root->left_h;
	as_index *r = RESOLVE_H(r_h);
	int c;

	// cf_debug(AS_INDEX,"search: key %"PRIx64" sentinal %p",*(uint64_t *)&key, tree->sentinel);

	while (r != tree->sentinel) {

//		cf_debug(AS_INDEX,"  at %p: key %"PRIx64": right %p left %p",s,*(uint64_t *)&s->key,s->right,s->left);

		c = cf_digest_compare(key, &r->key);
		if (c) {
			r_h = (c > 0) ? r->left_h : r->right_h;
			r = RESOLVE_H(r_h);
		}
		else {
			if (ret_h) *ret_h = r_h;
			if (ret) *ret = r;
			return(0);
		}
	}

	/* No matches found */
miss:
	return(-1);
}

/* as_index_search
 * Search a red-black tree for a node with a particular key
 *
 * 0 success (found)
 * -1 fail (not found)
 */
int
as_index_exists(as_index_tree *tree, cf_digest *key)
{
	/* Lock the tree */
	pthread_mutex_lock(&tree->lock);

	/* If there are no entries in the tree, we're done */
	if (tree->root->left_h == tree->sentinel_h) {
		pthread_mutex_unlock(&tree->lock);
		return -1;
	}

	int rv = as_index_search_h_lockless(tree, key, NULL, NULL);
	pthread_mutex_unlock(&tree->lock);
	return rv;
}


/* as_index_search
 * Search a red-black tree for a node with a particular key */
/* the contract of this is different from the others. It holds and releases the
** tree itself, so the caller doesn't have to. It also increments the refcount
** of the object, so the caller doesn't have to
**
** 0 success (found)
** -1 fail (not found)
*/
int
as_index_get_vlock(as_index_tree *tree, cf_digest *key, as_index_ref *index_ref)
{
	/* Lock the tree */
	pthread_mutex_lock(&tree->lock);

	int rv = as_index_search_h_lockless(tree, key, &(index_ref->r), &(index_ref->r_h));
	if (rv == 0) {
		as_index_reserve(index_ref->r);
		cf_atomic_int_incr(&g_config.global_record_ref_count);
		pthread_mutex_unlock(&tree->lock);
		if (!index_ref->skip_lock) {
			olock_vlock(g_config.record_locks, key, &(index_ref->olock) );
			cf_atomic_int_incr(&g_config.global_record_lock_count);
		}
	}
	else {
		pthread_mutex_unlock(&tree->lock);
	}

	return(rv);
}




/* as_index_delete
 * Remove a node from a red-black tree,
 * returning 0 or any return value from the provided value destructor function
 * return value:
 *   0 means success
 *   -1 means internal failure
 *   -2 means value not found
 */
int
as_index_delete(as_index_tree *tree, cf_digest *key)
{
	as_index *r, *s, *t;
	cf_arenax_handle r_h, s_h, t_h;
	bool retry;

	do {
		/* Lock the tree */
		if (0 != pthread_mutex_lock(&tree->lock)) {
			cf_warning(AS_INDEX, "unable to acquire tree lock: %s", cf_strerror(errno));
			return(-1);
		}

		/* Find a node with the matching key; if none exists, eject immediately */
		if (-1 == as_index_search_h_lockless(tree, key, &r, &r_h)) {
			pthread_mutex_unlock(&tree->lock);
			return -2;
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

	if ((tree->sentinel_h == r->left_h) || (tree->sentinel_h == r->right_h)) {
		s = r;
		s_h = r_h;
	}
	else {
		// changes S to the successor
		as_index_successor(tree, r, r_h, &s, &s_h);
	}

	if (tree->sentinel_h == s->left_h) {
		t_h = s->right_h;
	}
	else {
		t_h = s->left_h;
	}
	t = RESOLVE_H(t_h);

	t->parent_h = s->parent_h;
	if (tree->root_h == t->parent_h) {
		tree->root->left_h = t_h;
	}
	else {
		as_index *s_parent = RESOLVE_H(s->parent_h);
		if (s_h == s_parent->left_h)
			s_parent->left_h = t_h;
		else
			s_parent->right_h = t_h;
	}

	/* s is the node to splice out, and t is its child */
	if (s != r) {

		if (CF_RCRB_BLACK == s->color)
			as_index_deleterebalance(tree, t, t_h);

		/* Reassign pointers and coloration */
		s->left_h = r->left_h;
		s->right_h = r->right_h;
		s->parent_h = r->parent_h;
		s->color = r->color;
		RESOLVE_H(r->left_h)->parent_h = s_h;
		RESOLVE_H(r->right_h)->parent_h = s_h;

		as_index *r_parent = RESOLVE_H(r->parent_h);
		if (r_h == r_parent->left_h)
			r_parent->left_h = s_h;
		else
			r_parent->right_h = s_h;

		/* Consume the node - R IS DEAD AFTER HERE */
		// cf_detail(AS_RECORD, "as_index_delete REFERENCE RELEASED:  %p", r);
		if (0 == as_index_release(r)) {
			// cf_info(AS_INDEX, "index destroy 1 %p %x",r,r_h);
			if (tree->destructor) tree->destructor(r, tree->destructor_udata);
			cf_arenax_free(tree->arena, r_h);
			r_h = 0;
			r = 0;
		}
		cf_atomic_int_decr(&g_config.global_record_ref_count);

	} else {

		if (CF_RCRB_BLACK == s->color)
			as_index_deleterebalance(tree, t, t_h);

		// cf_detail(AS_RECORD, "as_index_delete REFERENCE RELEASED:  %p", s);
		/* Destroy the node contents - S IS DEAD AFTER HERE */
		if (0 == as_index_release(s)) {
			// cf_info(AS_INDEX, "index destroy 2 %p %x",r,r_h);
			tree->destructor(s, tree->destructor_udata);
			cf_arenax_free(tree->arena, r_h);
			r_h = 0;
			r = 0;
		}
		cf_atomic_int_decr(&g_config.global_record_ref_count);

	}
	tree->elements--;

	pthread_mutex_unlock(&tree->reduce_lock);
	pthread_mutex_unlock(&tree->lock);

	return 0;
}


/* rb_create
 * Create a new red-black tree */
as_index_tree *
as_index_tree_create(cf_arenax *arena, as_index_value_destructor destructor, void *destructor_udata, as_treex *p_treex) {

	as_index_tree *tree;

	/* Allocate memory for the tree and initialize the tree lock */
	if (NULL == (tree = cf_rc_alloc(sizeof(as_index_tree))))
		return(NULL);

	pthread_mutex_init(&tree->lock, NULL);
	pthread_mutex_init(&tree->reduce_lock, NULL);

	tree->arena = arena;

	/* Allocate memory for the sentinel; note that it's pointers are all set
	 * to itself */
	tree->sentinel_h = cf_arenax_alloc(arena);
	if (tree->sentinel_h == 0) {
		cf_rc_free(tree);
		return(NULL);
	}
	tree->sentinel = RESOLVE_H(tree->sentinel_h);

	// this is OK, we only need to blank the 'normal' part
	memset(tree->sentinel, 0, sizeof(as_index));
	tree->sentinel->parent_h = tree->sentinel->left_h = tree->sentinel->right_h = tree->sentinel_h;
	tree->sentinel->color = CF_RCRB_BLACK;

	/* Allocate memory for the root node, and set things up */
	tree->root_h = cf_arenax_alloc(arena);
	if (0 == tree->root_h) {
		cf_arenax_free(arena, tree->sentinel_h);
		cf_rc_free(tree);
		return(NULL);
	}
	tree->root = RESOLVE_H(tree->root_h);

	memset(tree->root, 0, sizeof(as_index));
	tree->root->parent_h = tree->root->left_h = tree->root->right_h = tree->sentinel_h;
	tree->root->color = CF_RCRB_BLACK;

	tree->destructor = destructor;
	tree->destructor_udata = destructor_udata;

	tree->elements = 0;

	if (p_treex) {
		// Update the tree information in persistent memory.
		p_treex->sentinel_h = tree->sentinel_h;
		p_treex->root_h = tree->root_h;
	}

	// cf_debug(AS_RECORD, "as_index_create CREATING TREE :  %p", tree);
	/* Return a pointer to the new tree */
	return(tree);
}


/* rb_create
 * Resume a red-black tree in persistent memory */
as_index_tree *
as_index_tree_resume(cf_arenax *arena, as_index_value_destructor destructor, void *destructor_udata, as_treex *p_treex) {

	as_index_tree *tree;

	/* Allocate memory for the tree and initialize the tree lock */
	if (NULL == (tree = cf_rc_alloc(sizeof(as_index_tree)))) {
		return(NULL);
	}

	pthread_mutex_init(&tree->lock, NULL);
	pthread_mutex_init(&tree->reduce_lock, NULL);

	tree->arena = arena;

	/* Resume the sentinel */
	tree->sentinel_h = p_treex->sentinel_h;
	if (tree->sentinel_h == 0) {
		cf_rc_free(tree);
		return(NULL);
	}
	tree->sentinel = RESOLVE_H(tree->sentinel_h);

	/* Resume the root */
	tree->root_h = p_treex->root_h;
	if (tree->root_h == 0) {
		cf_rc_free(tree);
		return(NULL);
	}
	tree->root = RESOLVE_H(tree->root_h);

	tree->destructor = destructor;
	tree->destructor_udata = destructor_udata;

	// We'll soon update this to its proper value by reducing the tree.
	tree->elements = 0;

	// cf_debug(AS_RECORD, "as_index_create RESUMING TREE :  %p", tree);
	/* Return a pointer to the new tree */
	return(tree);
}



/* as_index_purge
 * Purge a node and, recursively, its children, from a red-black tree */
void
as_index_tree_purge_h(as_index_tree *tree, as_index *r, cf_arenax_handle r_h)
{
	/* Don't purge the sentinel */
	if (r == tree->sentinel)
		return;

	/* Purge the children */
	as_index_tree_purge_h(tree, RESOLVE_H(r->left_h), r->left_h);
	as_index_tree_purge_h(tree, RESOLVE_H(r->right_h), r->right_h);

	// cf_detail(AS_RECORD, "as_index_purge REFERENCE RELEASED:  %p", r);
	if (0 == as_index_release(r)) {
		// cf_info(AS_INDEX, "index destroy 3 %p %x",r,r_h);
		tree->destructor(r, tree->destructor_udata);
		cf_arenax_free(tree->arena, r_h);
	}
	cf_atomic_int_decr(&g_config.global_record_ref_count);

	// debug thing
	// memset(r, 0xff, sizeof(as_index));
	// cf_free(r);

	return;
}

uint32_t
as_index_tree_size(as_index_tree *tree)
{
	uint32_t	sz;
	pthread_mutex_lock(&tree->lock);
	sz = tree->elements;
	pthread_mutex_unlock(&tree->lock);
	return(sz);
}

typedef struct {
	as_index 		*r;
	cf_arenax_handle r_h;
} as_index_value;

typedef struct {
	uint alloc_sz;
	uint pos;
	as_index_value indexes[];
} as_index_value_array;


/*
** call a function on all the nodes in the tree
*/
void
as_index_reduce_traverse( as_index_tree *tree, cf_arenax_handle r_h, cf_arenax_handle sentinel_h, as_index_value_array *v_a)
{

	if (v_a->pos >= v_a->alloc_sz)	return;

	as_index *r = RESOLVE_H(r_h);
	as_index_reserve(r);
	cf_atomic_int_incr(&g_config.global_record_ref_count);

	// cf_detail(AS_RECORD, "as_index_reduce_traverse EXISTING RECORD REFERENCE ACQUIRED:  %p", r);
	v_a->indexes[v_a->pos].r = r;
	v_a->indexes[v_a->pos].r_h = r_h;
	v_a->pos++;

	if (r->left_h != sentinel_h)
		as_index_reduce_traverse(tree, r->left_h, sentinel_h, v_a);

	if (r->right_h != sentinel_h)
		as_index_reduce_traverse(tree, r->right_h, sentinel_h, v_a);

}


// Flag to indicate full index reduce.
#define AS_REDUCE_ALL (-1)

/* Make a callback for every element in the tree.
 */
void
as_index_reduce(as_index_tree* tree, as_index_reduce_fn cb, void* udata)
{
	as_index_reduce_partial(tree, AS_REDUCE_ALL, cb, udata);
}

/* Make a callback for a specified number of elements in the tree.
 */
void
as_index_reduce_partial(as_index_tree* tree, uint32_t sample_count, as_index_reduce_fn cb, void* udata)
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

	size_t sz = sizeof(as_index_value_array) + (sizeof(as_index_value) * sample_count);
	as_index_value_array* v_a;
	uint8_t buf[64 * 1024];

	if (sz > 64 * 1024) {
		v_a = cf_malloc(sz);

		if (! v_a) {
			pthread_mutex_unlock(&tree->reduce_lock);
			return;
		}
	}
	else {
		v_a = (as_index_value_array*)buf;
	}

	v_a->alloc_sz = sample_count;
	v_a->pos = 0;

	uint64_t start_ms = cf_getms();

	// Recursively, fetch all the value pointers into this array, so we can make
	// all the callbacks outside the big lock.
	if (tree->root &&
		tree->root->left_h &&
		tree->root->left_h != tree->sentinel_h) {

		as_index_reduce_traverse(tree, tree->root->left_h, tree->sentinel_h, v_a);
	}

	cf_debug(AS_INDEX, "as_index_reduce_traverse took %"PRIu64" ms", cf_getms() - start_ms);

	pthread_mutex_unlock(&tree->reduce_lock);

	for (uint i = 0; i < v_a->pos; i++) {
		as_index_ref r_ref;

		r_ref.skip_lock = false;
		r_ref.r = v_a->indexes[i].r;
		r_ref.r_h = v_a->indexes[i].r_h;

		olock_vlock(g_config.record_locks, &(r_ref.r->key), &(r_ref.olock));
		cf_detail(AS_INDEX, "reduce partial - RECORD LOCK ACQUIRED: %p", r_ref);
		cf_atomic_int_incr(&g_config.global_record_lock_count);

		// Callback MUST call as_record_done() to unlock and release record.
		cb(&r_ref, udata);
	}

	if (v_a != (as_index_value_array*)buf) {
		cf_free(v_a);
	}
}


/*
** call a function on all the nodes in the tree
*/
void
as_index_reduce_sync_traverse(as_index_tree *tree, as_index *r, cf_arenax_handle sentinel_h, as_index_reduce_sync_fn cb, void *udata)
{
	cb ( r , udata);

	if (r->left_h != sentinel_h)
		as_index_reduce_sync_traverse(tree, RESOLVE_H(r->left_h), sentinel_h, cb, udata);

	if (r->right_h != sentinel_h)
		as_index_reduce_sync_traverse(tree, RESOLVE_H(r->right_h), sentinel_h, cb, udata);
}


void
as_index_reduce_sync(as_index_tree *tree, as_index_reduce_sync_fn cb, void *udata)
{
	/* Lock the tree */
	pthread_mutex_lock(&tree->reduce_lock);

	if ( (tree->root) &&
			(tree->root->left_h) &&
			(tree->root->left_h != tree->sentinel_h) ) {

		as_index_reduce_sync_traverse(tree, RESOLVE_H(tree->root->left_h), tree->sentinel_h, cb, udata);

	}

	pthread_mutex_unlock(&tree->reduce_lock);
}


/* as_index_release
 * Destroy a red-black tree; return 0 if the tree was destroyed or 1
 * otherwise */
int
as_index_tree_release(as_index_tree *tree, void *destructor_udata)
{
	if (0 != cf_rc_release(tree))
		return(1);
	// cf_debug(AS_RECORD, "as_index_release FREEING TREE :  %p", tree);

	/* Purge the tree and all its ilk */
	as_index_tree_purge_h(tree, RESOLVE_H(tree->root->left_h), tree->root->left_h);

	/* Release the tree's memory */
	cf_arenax_free(tree->arena, tree->root_h);
	cf_arenax_free(tree->arena, tree->sentinel_h);

	pthread_mutex_destroy(&tree->lock);
	pthread_mutex_destroy(&tree->reduce_lock);

	memset(tree, 0, sizeof(as_index_tree)); // a little debug
	cf_rc_free(tree);

	return(0);
}

// returns the number of bytes required to hold this index for allocation

int
as_index_size_get(as_namespace *ns)
{
	int sz = sizeof(struct as_index_s);

	if (ns->allow_versions) sz += 4;

//	cf_info(AS_RECORD, " INDEX SIZE IS %d",sz);

	return(sz);
}
