/*
 * arenax_cold.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include "arenax.h"

#include <stdint.h>
#include "citrusleaf/alloc.h"
#include "fault.h"


//------------------------------------------------
// Create and attach a persistent memory block,
// and store its pointer in the stages array.
//
cf_arenax_err
cf_arenax_add_stage(cf_arenax* this)
{
	if (this->stage_count >= this->max_stages) {
		cf_warning(CF_ARENAX, "can't allocate more than %u arena stages",
				this->max_stages);
		return CF_ARENAX_ERR_STAGE_CREATE;
	}

	uint8_t* p_stage = (uint8_t*)cf_malloc(this->stage_size);

	if (! p_stage) {
		cf_warning(CF_ARENAX, "could not allocate %lu-byte arena stage %u",
				this->stage_size, this->stage_count);
		return CF_ARENAX_ERR_STAGE_CREATE;
	}

	this->stages[this->stage_count++] = p_stage;

	return CF_ARENAX_OK;
}
