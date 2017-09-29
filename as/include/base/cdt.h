/*
 * cdt.h
 *
 * Copyright (C) 2015-2016 Aerospike, Inc.
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

typedef struct rollback_alloc_s {
	cf_ll_buf *ll_buf;
	size_t malloc_list_sz;
	size_t malloc_list_cap;
	bool malloc_ns;
	void *malloc_list[];
} rollback_alloc;

#define rollback_alloc_inita(__name, __alloc_buf, __rollback_size, __malloc_ns) \
		rollback_alloc *__name = (rollback_alloc *)alloca(sizeof(rollback_alloc) + sizeof(void *)*(__alloc_buf ? 0 : __rollback_size)); \
		__name->ll_buf = __alloc_buf; \
		__name->malloc_list_sz = 0; \
		__name->malloc_list_cap = (__alloc_buf ? 0 : __rollback_size); \
		__name->malloc_ns = __malloc_ns;

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

typedef struct cdt_container_builder_s {
	as_particle *particle;
	uint8_t *write_ptr;
	uint32_t ele_count;
	uint32_t header_ele_count;
	uint32_t *size;
	uint8_t type;
} cdt_container_builder;

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

static const uint8_t msgpack_nil[1] = {0xC0};


//==========================================================
// Function declarations.
//

// Calculate index given index and max_index.
static inline int64_t
calc_index(int64_t index, int max_index)
{
	return index < 0 ? (int64_t)max_index + index : index;
}
uint64_t calc_count(uint64_t index, uint64_t input_count, int max_index);
void calc_index_count_multi(int64_t in_index, uint64_t in_count, uint32_t ele_count, uint32_t *out_index, uint32_t *out_count);
bool calc_index_count(int64_t in_index, uint64_t in_count, uint32_t ele_count, uint32_t *out_index, uint32_t *out_count, bool is_multi);

// as_bin
bool as_bin_get_int(const as_bin *b, int64_t *value);
void as_bin_set_int(as_bin *b, int64_t value);
void as_bin_set_double(as_bin *b, double value);

// cdt_payload
bool cdt_payload_is_int(const cdt_payload *payload);
int64_t cdt_payload_get_int64(const cdt_payload *payload);

// cdt_process_state
bool cdt_process_state_init(cdt_process_state *cdt_state, const as_msg_op *op);
bool cdt_process_state_get_params(cdt_process_state *state, size_t n, ...);
size_t cdt_process_state_op_param_count(as_cdt_optype op);

// cdt_process_state_packed_list
bool cdt_process_state_packed_list_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata);
bool cdt_process_state_packed_list_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata);

void cdt_container_builder_add(cdt_container_builder *builder, const uint8_t *buf, size_t size);
void cdt_container_builder_add_int64(cdt_container_builder *builder, int64_t value);
void cdt_container_builder_finalize(cdt_container_builder *builder);

bool cdt_list_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_max_sz);
void cdt_list_builder_finalize(cdt_container_builder *builder);

bool cdt_map_builder_start(cdt_container_builder *builder, rollback_alloc *alloc_buf, uint32_t ele_count, uint32_t content_max_sz, uint8_t flags);
void cdt_map_builder_finalize(cdt_container_builder *builder);

// cdt_process_state_packed_map
bool cdt_process_state_packed_map_modify_optype(cdt_process_state *state, cdt_modify_data *cdt_udata);
bool cdt_process_state_packed_map_read_optype(cdt_process_state *state, cdt_read_data *cdt_udata);

// rollback_alloc
void rollback_alloc_push(rollback_alloc *packed_alloc, void *ptr);
uint8_t *rollback_alloc_reserve(rollback_alloc *alloc_buf, size_t size);
void rollback_alloc_rollback(rollback_alloc *alloc_buf);
bool rollback_alloc_from_msgpack(rollback_alloc *alloc_buf, as_bin *b, const cdt_payload *seg);

// Debugging support
void print_hex(const uint8_t *packed, uint32_t packed_sz, char *buf, uint32_t buf_sz);
void print_packed(const uint8_t *packed, uint32_t size, const char *name);
