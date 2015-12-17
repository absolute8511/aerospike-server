/*
 * cdt.h
 *
 * Copyright (C) 2015 Aerospike, Inc.
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

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_msgpack.h"

#include "base/datamodel.h"
#include "base/proto.h"

#include "dynbuf.h"


//==========================================================
// Typedefs & constants.
//

#define CDT_DO_TEMP_HISTO	1

typedef struct rollback_alloc_s {
	cf_ll_buf *ll_buf;
	size_t malloc_list_sz;
	size_t malloc_list_cap;
	void *malloc_list[];
} rollback_alloc;

#define rollback_alloc_inita(__name, __alloc_buf, __rollback_size) \
		uint8_t cdt_packed_alloc_stage##__name[sizeof(rollback_alloc) + sizeof(void *)*(__alloc_buf ? 0 : __rollback_size)]; \
		rollback_alloc *__name = (rollback_alloc *)cdt_packed_alloc_stage##__name; \
		__name->ll_buf = __alloc_buf; \
		__name->malloc_list_sz = 0; \
		__name->malloc_list_cap = (__alloc_buf ? 0 : __rollback_size);

typedef struct cdt_process_state_s {
	as_cdt_optype type;
	as_unpacker pk;
	uint32_t ele_count;
} cdt_process_state;

typedef struct cdt_payload_s {
	const uint8_t *ptr;
	uint32_t size;
} cdt_payload;

typedef struct cdt_modify_data_s {
	as_bin *b;
	as_bin *result;
	cf_ll_buf *alloc_buf;

	int ret_code;
} cdt_modify_data;

typedef struct cdt_read_data_s {
	const as_bin *b;
	as_bin *result;

	int ret_code;
} cdt_read_data;

typedef struct cdt_op_table_entry_s {
	int count;
	int opt_args;
	const as_cdt_paramtype *args;
} cdt_op_table_entry;

#define VA_NARGS_SEQ 9, 8, 7, 6, 5, 4, 3, 2, 1, 0
#define VA_NARGS_EXTRACT_N(_9, _8, _7, _6, _5, _4, _3, _2, _1, _0, N, ...) N
#define VA_NARGS_SEQ2N(...) VA_NARGS_EXTRACT_N(__VA_ARGS__)
#define VA_NARGS(...) VA_NARGS_SEQ2N(_, ##__VA_ARGS__, VA_NARGS_SEQ)

// Get around needing to pass last named arg to va_start().
#define CDT_OP_TABLE_GET_PARAMS(state, ...) cdt_process_state_get_params(state, VA_NARGS(__VA_ARGS__), __VA_ARGS__)


//==========================================================
// Function declarations.
//

// as_bin
void as_bin_set_int(as_bin *b, int64_t value);
bool as_bin_set_packed_val(as_bin *b, cdt_payload *packed);

// cdt_process_state
bool cdt_process_state_init(cdt_process_state *cdt_state, const as_msg_op *op);
bool cdt_process_state_get_params(cdt_process_state *state, size_t n, ...);
size_t cdt_process_state_op_param_count(as_cdt_optype op);

// cdt_process_state_packed_list
bool cdt_process_state_packed_list_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata);
bool cdt_process_state_packed_list_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata);

// rollback_alloc
void rollback_alloc_push(rollback_alloc *packed_alloc, void *ptr);
uint8_t *rollback_alloc_reserve(rollback_alloc *alloc_buf, size_t size);
void rollback_alloc_rollback(rollback_alloc *alloc_buf);
