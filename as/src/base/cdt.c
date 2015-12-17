/*
 * cdt.c
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

#include "base/cdt.h"

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"


//==========================================================
// Typedefs & constants.
//

#define VA_FIRST(first, ...)	first
#define VA_REST(first, ...)		__VA_ARGS__

#define CDT_OP_ENTRY(op, type, ...) [op].args = (const as_cdt_paramtype[]){VA_REST(__VA_ARGS__, 0)}, [op].count = VA_NARGS(__VA_ARGS__) - 1, [op].opt_args = VA_FIRST(__VA_ARGS__)

const cdt_op_table_entry cdt_op_table[] = {
	//--------------------------------------------
	// Modify OPs

	// Add to list
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND_ITEMS,	CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT_ITEMS,	CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	// Remove from list
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP_RANGE,		CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_RANGE,	CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	// Other list modifies
	CDT_OP_ENTRY(AS_CDT_OP_LIST_SET,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_TRIM,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_CLEAR,			CDT_RW_TYPE_MODIFY, 0),

	//--------------------------------------------
	// Read OPs

	// Read from list
	CDT_OP_ENTRY(AS_CDT_OP_LIST_SIZE,			CDT_RW_TYPE_READ, 0),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET,			CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_RANGE,		CDT_RW_TYPE_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
};

static const size_t cdt_op_table_size = sizeof(cdt_op_table) / sizeof(cdt_op_table_entry);

//==========================================================
// as_bin functions.
//

void
as_bin_set_int(as_bin *b, int64_t value)
{
	b->particle = (as_particle *)value;
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_INTEGER);
}


//==========================================================
// cdt_process_state functions.
//

bool
cdt_process_state_init(cdt_process_state *cdt_state, const as_msg_op *op)
{
	const uint8_t *data = op->name + op->name_sz;
	uint32_t size = op->op_sz - 4 - op->name_sz;

	if (size < sizeof(uint16_t)) {
		cf_warning(AS_PARTICLE, "cdt_parse_state_init() as_msg_op data too small to be valid: size=%u", size);
		return false;
	}

	const uint16_t *type_ptr = (const uint16_t *)data;
	cdt_state->type = cf_swap_from_be16(*type_ptr);

	cdt_state->pk.buffer = data + sizeof(uint16_t);
	cdt_state->pk.length = size - sizeof(uint16_t);
	cdt_state->pk.offset = 0;

	int ele_count = (cdt_state->pk.length == 0) ? 0 : as_unpack_list_header_element_count(&cdt_state->pk);

	if (ele_count < 0) {
		cf_warning(AS_PARTICLE, "cdt_parse_state_init() unpack list header failed: size=%u type=%u", size, cdt_state->type);
		return false;
	}

	cdt_state->ele_count = ele_count;

	return true;
}

bool
cdt_process_state_get_params(cdt_process_state *state, size_t n, ...)
{
	as_cdt_optype op = state->type;

	if (op >= cdt_op_table_size) {
		return false;
	}

	const cdt_op_table_entry *entry = &cdt_op_table[op];
	int required_count = entry->count - entry->opt_args;

	if (n < (size_t)required_count) {
		cf_crash(AS_PARTICLE, "cdt_process_state_get_params() called with %zu params, require at least %d - %d = %d params", n, entry->count, entry->opt_args, required_count);
	}

	if (n == 0 || entry->args[0] == 0) {
		return true;
	}

	if (state->ele_count < (uint32_t)required_count) {
		cf_warning(AS_PARTICLE, "cdt_process_state_get_params() count mismatch: got %u from client < expected %d", state->ele_count, required_count);
		return false;
	}

	if (state->ele_count > (uint32_t)entry->count) {
		cf_warning(AS_PARTICLE, "cdt_process_state_get_params() count mismatch: got %u from client > expected %u", state->ele_count, entry->count);
		return false;
	}

	va_list vl;
	va_start(vl, n);

	for (size_t i = 0; i < state->ele_count; i++) {
		switch (entry->args[i]) {
		case AS_CDT_PARAM_PAYLOAD: {
			cdt_payload *arg = va_arg(vl, cdt_payload *);

			arg->ptr = state->pk.buffer + state->pk.offset;

			int size = as_unpack_size(&state->pk);

			if (size < 0) {
				va_end(vl);
				return false;
			}

			arg->size = size;

			break;
		}
		case AS_CDT_PARAM_COUNT: {
			uint64_t *arg = va_arg(vl, uint64_t *);

			if (as_unpack_uint64(&state->pk, arg) != 0) {
				va_end(vl);
				return false;
			}

			break;
		}
		case AS_CDT_PARAM_INDEX: {
			int64_t *arg = va_arg(vl, int64_t *);

			if (as_unpack_int64(&state->pk, arg) != 0) {
				va_end(vl);
				return false;
			}

			break;
		}
		default:
			va_end(vl);
			return false;
		}
	}

	va_end(vl);

	return true;
}

size_t
cdt_process_state_op_param_count(as_cdt_optype op)
{
	if (op >= cdt_op_table_size) {
		return 0;
	}

	const cdt_op_table_entry *entry = &cdt_op_table[op];

	if (entry->args[0] == 0) {
		return 0;
	}

	return entry->count;
}


//==========================================================
// rollback_alloc functions.
//

void
rollback_alloc_push(rollback_alloc *packed_alloc, void *ptr)
{
	if (packed_alloc->malloc_list_sz == packed_alloc->malloc_list_cap) {
		cf_crash(AS_PARTICLE, "rollback_alloc_push() need to make rollback list larger: cap=%zu", packed_alloc->malloc_list_cap);
	}

	packed_alloc->malloc_list[packed_alloc->malloc_list_sz++] = ptr;
}

uint8_t *
rollback_alloc_reserve(rollback_alloc *alloc_buf, size_t size)
{
	if (! alloc_buf) {
		return NULL;
	}

	uint8_t *ptr;

	if (alloc_buf->ll_buf) {
		ptr = NULL;
		cf_ll_buf_reserve(alloc_buf->ll_buf, size, &ptr);
	}
	else {
		ptr = cf_malloc(size);
		rollback_alloc_push(alloc_buf, ptr);
	}

	return ptr;
}

void
rollback_alloc_rollback(rollback_alloc *alloc_buf)
{
	if (alloc_buf->ll_buf) {
		return;
	}

	for (size_t i = 0; i < alloc_buf->malloc_list_sz; i++) {
		cf_free(alloc_buf->malloc_list[i]);
	}

	alloc_buf->malloc_list_sz = 0;
}


//==========================================================
// as_bin_cdt_packed functions.
//

int
as_bin_cdt_packed_modify(as_bin *b, as_msg_op *op, as_bin *result, cf_ll_buf *particles_llb)
{
	cdt_process_state state;

	if (! cdt_process_state_init(&state, op)) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	cdt_modify_data udata = {
		.b = b,
		.result = result,
		.alloc_buf = particles_llb,
		.ret_code = AS_PROTO_RESULT_OK,
	};

	cdt_process_state_packed_list_modify_optype(&state, &udata);

	return udata.ret_code;
}

int
as_bin_cdt_packed_read(const as_bin *b, as_msg_op *op, as_bin *result)
{
	cdt_process_state state;

	if (! cdt_process_state_init(&state, op)) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	cdt_read_data udata = {
		.b = b,
		.result = result,
		.ret_code = AS_PROTO_RESULT_OK,
	};

	cdt_process_state_packed_list_read_optype(&state, &udata);

	return udata.ret_code;
}
