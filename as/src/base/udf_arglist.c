/*
 * udf_arglist.c
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

#include <aerospike/as_list.h>
#include <aerospike/as_list_iterator.h>
#include <aerospike/as_msgpack.h>

#include "base/udf_arglist.h"
#include "base/proto.h"

/******************************************************************************
 * STATIC FUNCTIONS
 ******************************************************************************/

static bool udf_arglist_foreach(const as_list *, as_list_foreach_callback, void *);
static as_val *udf_arglist_get(const as_list *, const uint32_t idx);

/******************************************************************************
 * VARIABLES
 ******************************************************************************/

const as_list_hooks udf_arglist_hooks = {
	.destroy		= NULL,
	.hashcode		= NULL,
	.size			= NULL,
	.append			= NULL,
	.prepend		= NULL,
	.get			= udf_arglist_get,
	.set			= NULL,
	.head			= NULL,
	.tail			= NULL,
	.drop			= NULL,
	.take			= NULL,
	.foreach		= udf_arglist_foreach,
	.iterator_init	= NULL,
	.iterator_new	= NULL
};

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

static bool udf_arglist_foreach(const as_list * l, as_list_foreach_callback callback, void * context) {
	as_msg_field * field = (as_msg_field *) l->data;

	if ( field != NULL ) {
		as_unpacker unpacker;
		unpacker.buffer = (const unsigned char*)field->data;
		unpacker.length = as_msg_field_get_value_sz(field);
		unpacker.offset = 0;
		if ( unpacker.length == 0 )
			return true;

		as_val* val = 0;
		int ret = as_unpack_val(&unpacker, &val);

		if (ret == 0 && as_val_type(val) == AS_LIST) {
			as_list_iterator list_iter;
			as_iterator* iter = (as_iterator*) &list_iter;
			as_list_iterator_init(&list_iter, (as_list*)val);

			while (as_iterator_has_next(iter)) {
				const as_val* v = as_iterator_next(iter);
				callback((as_val *) v, context);
			}
			as_iterator_destroy(iter);
		}
		as_val_destroy(val);
		return ret == 0;
	}
	return true;
}

static as_val *udf_arglist_get(const as_list * l, const uint32_t idx) {
	as_msg_field * field = (as_msg_field *) l->data;

	if ( field != NULL ) {
		as_unpacker unpacker;
		unpacker.buffer = (const unsigned char*)field->data;
		unpacker.length = as_msg_field_get_value_sz(field);
		unpacker.offset = 0;
		if ( unpacker.length == 0 )
			return NULL;

		as_val* item = 0;
		as_val* val = 0;
		int ret = as_unpack_val(&unpacker, &val);

		if (ret == 0 && as_val_type(val) == AS_LIST) {
			item = as_list_get((as_list*)val, idx);
		}
		as_val_destroy(val);
		return item;
	}
	return NULL;
}

