/*
 * record_ce.c
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "storage/storage.h"


//==========================================================
// Public API.
//

bool
as_record_is_live(const as_record* r)
{
	return true;
}


int
as_record_get_live(as_index_tree* tree, cf_digest* keyd, as_index_ref* r_ref,
		as_namespace* ns)
{
	return as_index_get_vlock(tree, keyd, r_ref);
}


int
as_record_exists_live(as_index_tree* tree, cf_digest* keyd, as_namespace* ns)
{
	return as_record_exists(tree, keyd);
}


void
as_record_drop_stats(as_record* r, as_namespace* ns)
{
	as_namespace_release_set_id(ns, as_index_get_set_id(r));

	if (as_ldt_record_is_sub(r)) {
		cf_atomic64_decr(&ns->n_sub_objects);
	}
	else {
		cf_atomic64_decr(&ns->n_objects);
	}
}


void
as_record_apply_pickle(as_storage_rd* rd)
{
	cf_assert(as_bin_inuse_has(rd), AS_RECORD, "unexpected binless pickle");

	as_storage_record_write(rd);
}


bool
as_record_apply_replica(as_storage_rd* rd, uint32_t info, as_index_tree* tree)
{
	// Should already have handled drop.
	cf_assert(as_bin_inuse_has(rd), AS_RECORD, "unexpected binless pickle");

	as_storage_record_write(rd);

	return false;
}
