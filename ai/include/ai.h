/*
 * ai.h
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
/* SYNOPSIS
 * This file provides declarations for the Aerospike Index API.
 */

#pragma once

#include "ai_types.h"


/***************** Declare Aerospike Index API Functions *****************/


/*
 *  Initialize the Aerospike Index Module.
 */
void ai_init(void);

/*
 *  Create a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_create_table(char *tname);

/*
 *  Create an index.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_create_index(char *iname, char *tname, char *cname, int col_type, int num_partitions);

/*
 *  Add a column to a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_add_column(char *tname, char *cname, int col_type);

/*
 *  Drop a column from a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_drop_column(char *tname, char *cname);

/*
 *  Drop a table.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_drop_table(char *tname);

/*
 *  Drop an index.
 *  Return 0 if successful, -1 otherwise.
 */
int ai_drop_index(char *iname);
