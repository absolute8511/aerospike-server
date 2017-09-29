/*
 * cdt.c
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

#include "base/cdt.h"

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/particle.h"


//==========================================================
// Typedefs & constants.
//

#define VA_FIRST(first, ...)	first
#define VA_REST(first, ...)		__VA_ARGS__

#define CDT_OP_ENTRY(op, type, ...) [op].args = (const as_cdt_paramtype[]){VA_REST(__VA_ARGS__, 0)}, [op].count = VA_NARGS(__VA_ARGS__) - 1, [op].opt_args = VA_FIRST(__VA_ARGS__)

const cdt_op_table_entry cdt_op_table[] = {

	//============================================
	// LIST

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

	//============================================
	// MAP

	//--------------------------------------------
	// Create and flags

	CDT_OP_ENTRY(AS_CDT_OP_MAP_SET_TYPE,				CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_FLAGS),

	//--------------------------------------------
	// Modify OPs

	CDT_OP_ENTRY(AS_CDT_OP_MAP_ADD,						CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_ADD_ITEMS,				CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_PUT,						CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_PUT_ITEMS,				CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REPLACE,					CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REPLACE_ITEMS,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_INCREMENT,				CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_DECREMENT,				CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_FLAGS),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_CLEAR,					CDT_RW_TYPE_MODIFY, 0),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_INDEX,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_RANK,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST,	CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE,		CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST,CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL,	CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL,CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE,	CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE,	CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	//--------------------------------------------
	// Read OPs

	CDT_OP_ENTRY(AS_CDT_OP_MAP_SIZE,					CDT_RW_TYPE_READ, 0),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_KEY,				CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_INDEX,			CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_VALUE,			CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_RANK,				CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_ALL_BY_VALUE,		CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL,		CDT_RW_TYPE_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_INDEX_RANGE,		CDT_RW_TYPE_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL,	CDT_RW_TYPE_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_MAP_GET_BY_RANK_RANGE,		CDT_RW_TYPE_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

};

static const size_t cdt_op_table_size = sizeof(cdt_op_table) / sizeof(cdt_op_table_entry);

extern const as_particle_vtable *particle_vtable[];


//==========================================================
// CDT helpers.
//

// Calculate count given index and max_index.
// input_count == 0 implies until end of list.
// Assumes index < ele_count.
uint64_t
calc_count(uint64_t index, uint64_t input_count, int max_index)
{
	uint64_t max = (uint64_t)max_index;

	// Since we assume index < ele_count, max - index will never overflow.
	if (input_count == 0 || input_count >= max - index) {
		return max - index;
	}

	return input_count;
}

void
calc_index_count_multi(int64_t in_index, uint64_t in_count, uint32_t ele_count, uint32_t *out_index, uint32_t *out_count)
{
	if (in_index >= ele_count) {
		*out_index = ele_count;
		*out_count = 0;
	}
	else if ((in_index = calc_index(in_index, (int)ele_count)) < 0) {
		if ((uint64_t)(-in_index) < in_count) {
			*out_count = in_count + in_index;

			if (*out_count > ele_count) {
				*out_count = ele_count;
			}
		}
		else {
			*out_count = 0;
		}

		*out_index = 0;
	}
	else {
		*out_index = (uint32_t)in_index;
		*out_count = calc_count((uint64_t)in_index, in_count, (int)ele_count);
	}
}

bool
calc_index_count(int64_t in_index, uint64_t in_count, uint32_t ele_count, uint32_t *out_index, uint32_t *out_count, bool is_multi)
{
	if (is_multi) {
		calc_index_count_multi(in_index, in_count, ele_count, out_index, out_count);
		return true;
	}

	if (in_index >= ele_count || (in_index = calc_index(in_index, ele_count)) < 0) {
		return false;
	}

	*out_index = (uint32_t)in_index;
	*out_count = (uint32_t)calc_count((uint64_t)in_index, in_count, (int)ele_count);

	return true;
}


//==========================================================
// as_bin functions.
//

bool
as_bin_get_int(const as_bin *b, int64_t *value)
{
	if (as_bin_get_particle_type(b) != AS_PARTICLE_TYPE_INTEGER) {
		return false;
	}

	*value = (int64_t)b->particle;

	return true;
}

void
as_bin_set_int(as_bin *b, int64_t value)
{
	b->particle = (as_particle *)value;
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_INTEGER);
}

void
as_bin_set_double(as_bin *b, double value)
{
	*((double *)(&b->particle)) = value;
	as_bin_state_set_from_type(b, AS_PARTICLE_TYPE_FLOAT);
}


//==========================================================
// cdt_payload functions.
//

bool
cdt_payload_is_int(const cdt_payload *payload)
{
	return as_unpack_buf_peek_type(payload->ptr, payload->size) == AS_INTEGER;
}

int64_t
cdt_payload_get_int64(const cdt_payload *payload)
{
	int64_t ret = 0;
	as_unpacker pk = {
			.buffer = payload->ptr,
			.offset = 0,
			.length = payload->size
	};

	as_unpack_int64(&pk, &ret);

	return ret;
}


//==========================================================
// cdt_container_builder functions.
//

void
cdt_container_builder_add(cdt_container_builder *builder, const uint8_t *buf, size_t size)
{
//	list_mem *p_list_mem = (list_mem *)builder->particle;
	memcpy(builder->write_ptr, buf, size);
	builder->write_ptr += size;
	(*builder->size) += (uint32_t)size;
//	p_list_mem->sz +=
	builder->ele_count++;
}

void
cdt_container_builder_add_int64(cdt_container_builder *builder, int64_t value)
{
//	list_mem *p_list_mem = (list_mem *)builder->particle;
	as_integer val64;
	as_packer pk = {
			.head = NULL,
			.tail = NULL,
			.buffer = builder->write_ptr,
			.offset = 0,
			.capacity = INT_MAX
	};

	as_integer_init(&val64, value);
	as_pack_val(&pk, (const as_val *)&val64);
	builder->write_ptr += pk.offset;
	(*builder->size) += (uint32_t)pk.offset;
	builder->ele_count++;
}

void
cdt_container_builder_finalize(cdt_container_builder *builder)
{
	if (builder->type == AS_PARTICLE_TYPE_LIST) {
		cdt_list_builder_finalize(builder);
	}
	else if (builder->type == AS_PARTICLE_TYPE_MAP) {
		cdt_map_builder_finalize(builder);
	}
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
		case AS_CDT_PARAM_FLAGS:
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
	if (packed_alloc->malloc_list_sz >= packed_alloc->malloc_list_cap) {
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
		ptr = alloc_buf->malloc_ns ? cf_malloc_ns(size) : cf_malloc(size);
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

bool
rollback_alloc_from_msgpack(rollback_alloc *alloc_buf, as_bin *b, const cdt_payload *seg)
{
	// We assume the bin is empty.

	as_particle_type type = as_particle_type_from_msgpack(seg->ptr, seg->size);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return false;
	}

	if (type == AS_PARTICLE_TYPE_NULL) {
		return true;
	}

	uint32_t mem_size = particle_vtable[type]->size_from_msgpack_fn(seg->ptr, seg->size);

	if (mem_size != 0) {
		b->particle = (as_particle *)rollback_alloc_reserve(alloc_buf, mem_size);

		if (! b->particle) {
			return false;
		}
	}

	particle_vtable[type]->from_msgpack_fn(seg->ptr, seg->size, &b->particle);

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, type);

	return true;
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

	bool success;

	if ((int)state.type <= (int)AS_CDT_OP_LIST_LAST) {
		success = cdt_process_state_packed_list_modify_optype(&state, &udata);
	}
	else {
		success = cdt_process_state_packed_map_modify_optype(&state, &udata);
	}

	if (! success) {
		as_bin_set_empty(b);
	}

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

	if ((int)state.type <= AS_CDT_OP_LIST_LAST) {
		cdt_process_state_packed_list_read_optype(&state, &udata);
	}
	else {
		cdt_process_state_packed_map_read_optype(&state, &udata);
	}

	return udata.ret_code;
}


//==========================================================
// Debugging support.
//

void
print_hex(const uint8_t *packed, uint32_t packed_sz, char *buf, uint32_t buf_sz)
{
	uint32_t n = (buf_sz - 3) / 2;

	if (n > packed_sz) {
		n = packed_sz;
		buf[buf_sz - 3] = '.';
		buf[buf_sz - 2] = '.';
		buf[buf_sz - 1] = '\0';
	}

	char *ptr = (char *)buf;

	for (int i = 0; i < n; i++) {
		sprintf(ptr, "%02X", packed[i]);
		ptr += 2;
	}
}

void
print_packed(const uint8_t *packed, uint32_t size, const char *name)
{
	char buf[4096];
	print_hex(packed, size, buf, 4096);
	cf_warning(AS_PARTICLE, "%s: buf[%u]='%s'", name, size, buf);
}
