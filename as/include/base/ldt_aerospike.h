/*
 * ldt_aerospike.h
 *
 * Copyright (C) 2013-2016 Aerospike, Inc.
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

#include "base/udf_record.h"
#include "aerospike/as_aerospike.h"

extern         as_aerospike g_ldt_aerospike;
extern const   as_aerospike_hooks ldt_aerospike_hooks;

as_aerospike * ldt_aerospike_new();
as_aerospike * ldt_aerospike_init(as_aerospike *);
int            ldt_init(void);
void           ldt_record_init(ldt_record *lrecord);
int            ldt_record_destroy(ldt_record *lrecord);
