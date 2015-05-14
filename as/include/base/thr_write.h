/*
 * thr_write.h
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
 * write service function declarations
 */

#pragma once

#include <stdint.h>

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"


extern void as_write_init();
extern int as_write_start(as_transaction *t);
extern int as_read_start(as_transaction *t);
extern int as_write_journal_apply(as_partition_reservation *prsv);
extern int as_write_journal_start(as_namespace *ns, as_partition_id pid);

/* rough guess of writes in progress for stats */
extern uint32_t as_write_inprogress();

/* Dump information about the transaction hash table. */
extern void as_dump_wr();

extern void single_transaction_response(as_transaction *tr, as_namespace *ns,
		as_bin **response_bins, uint16_t n_bins, uint32_t generation,
		uint32_t void_time, uint *written_sz, char *setname);
