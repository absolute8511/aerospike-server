/*
 * read.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>

#include "base/cfg.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"


//==========================================================
// Public API.
//

transaction_status as_read_start(as_transaction* tr);

static inline bool
as_read_must_duplicate_resolve(const as_transaction* tr)
{
	return tr->rsv.n_dupl != 0 &&
			(g_config.transaction_repeatable_read ||
					TRANSACTION_CONSISTENCY_LEVEL(tr) ==
							AS_POLICY_CONSISTENCY_LEVEL_ALL);
}
