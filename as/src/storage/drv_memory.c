/*
 * drv_memory.c
 *
 * Copyright (C) 2009-2014 Aerospike, Inc.
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
 * in-memory storage engine driver
 *
 */

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_queue.h"

#include "base/datamodel.h"
#include "storage/storage.h"


/* SYNOPSIS
 * In-memory storage driver
 *
 * This code almost entirely performs no-ops, because all the in-memory state
 * is correct already.
 * Note that this code is mostly for the NON-PERSISTENT main memory namespace.
 * The File-backed (persistent) main memory namespace is NOT type 1 (MM) for
 * some calls, but is instead treated as type 2 (SSD);  hence in some cases
 * the SSD functions, like as_storage_bin_can_fit(), are applied with an SSD
 * context rather than a transient main memory context.  (tjl)
 */

int
as_storage_namespace_init_memory(as_namespace *ns, cf_queue *complete_q, void *udata)
{
	cf_queue_push(complete_q, &udata);
	return(0);
}

int
as_storage_namespace_destroy_memory(as_namespace *ns)
{
	return(0);
}

int
as_storage_namespace_attributes_get_memory(as_namespace *ns, as_storage_attributes *attr)
{
    attr->n_devices = 0; // put all requests in the standard group
    return(0);
}

int
as_storage_stats_memory(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes)
{
	if (available_pct) {
		*available_pct = 100;
	}
	if (used_disk_bytes) {
		*used_disk_bytes = 0;
	}
	return(0);
}
