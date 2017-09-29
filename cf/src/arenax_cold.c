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
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include "citrusleaf/alloc.h"
#include "fault.h"


//------------------------------------------------
// Create and attach a persistent memory block,
// and store its pointer in the stages array.
//
cf_arenax_err
cf_arenax_add_stage(cf_arenax* this)
{
	key_t key;
	int shmid;
	void* p_stage;

	if (this->stage_count >= this->max_stages) {
		return CF_ARENAX_ERR_STAGE_CREATE;
	}

	// key_base = 0 means this arenax don't use persistent memory
	if (this->key_base == 0) {
		p_stage = cf_malloc(this->stage_size);

		if (! p_stage) {
			cf_warning(CF_ARENAX, "could not allocate %lu-byte arena stage %u",
					this->stage_size, this->stage_count);
			return CF_ARENAX_ERR_STAGE_CREATE;
		}
	} else {
		key = this->key_base + 0x100 + this->stage_count;

		// get persistent memory, create if not exist
		shmid = shmget(key, this->stage_size, S_IRUSR | S_IWUSR);
		if (shmid == -1 && errno == ENOENT) {
			shmid = shmget(key, this->stage_size, IPC_CREAT | S_IRUSR | S_IWUSR);
		}
		if (shmid == -1) {
			cf_warning(CF_ARENAX, "could not allocate %lu-byte arena stage %u",
					this->stage_size, this->stage_count);
			return CF_ARENAX_ERR_STAGE_CREATE;
		}

		// load persistent memory, for warm restart
		p_stage = shmat(shmid, NULL, SHM_RND);
		if (p_stage == (void*)-1) {
			cf_warning(CF_ARENAX, "shmat error %lu-byte arena stage %u",
					this->stage_size, this->stage_count);
			return CF_ARENAX_ERR_STAGE_CREATE;
		}

		p_stage = shmat(shmid, NULL, SHM_RND);
		if (p_stage == (void*)-1) {
			cf_warning(CF_ARENAX, "could not allocate %lu-byte arena stage %u",
					this->stage_size, this->stage_count);
			return CF_ARENAX_ERR_STAGE_CREATE;
		}
	}

	this->stages[this->stage_count++] = (uint8_t*)p_stage;

	return CF_ARENAX_OK;
}

extern const uint64_t MAX_STAGE_SIZE;

cf_arenax_err
cf_arenax_resume(cf_arenax* this, key_t key_base, uint32_t element_size,
		uint32_t stage_capacity, uint32_t max_stages, uint32_t flags)
{
	uint32_t stage_count;
	uint32_t at_element_id;
	uint32_t at_stage_id;

	if (stage_capacity == 0) {
		stage_capacity = MAX_STAGE_CAPACITY;
	}
	else if (stage_capacity > MAX_STAGE_CAPACITY) {
		cf_warning(CF_ARENAX, "stage capacity %u too large", stage_capacity);
		return CF_ARENAX_ERR_BAD_PARAM;
	}

	if (max_stages == 0) {
		max_stages = CF_ARENAX_MAX_STAGES;
	}
	else if (max_stages > CF_ARENAX_MAX_STAGES) {
		cf_warning(CF_ARENAX, "max stages %u too large", max_stages);
		return CF_ARENAX_ERR_BAD_PARAM;
	}

	uint64_t stage_size = (uint64_t)stage_capacity * (uint64_t)element_size;

	if (stage_size > MAX_STAGE_SIZE) {
		cf_warning(CF_ARENAX, "stage size %lu too large", stage_size);
		return CF_ARENAX_ERR_BAD_PARAM;
	}

	// TODO don't panic?
	if (this->key_base != key_base ||
		this->element_size != element_size ||
		this->stage_capacity != stage_capacity ||
		this->max_stages != max_stages ||
		this->flags != flags ||
		this->stage_size != (size_t)stage_size
	) {
		cf_crash(CF_ARENAX, "persistent memory data not match!");
	}

	if ((flags & CF_ARENAX_BIGLOCK) &&
			pthread_mutex_init(&this->lock, 0) != 0) {
		return CF_ARENAX_ERR_UNKNOWN;
	}

	// recover stage data of this aernax
	stage_count = this->stage_count;
	at_element_id = this->at_element_id;
	at_stage_id = this->at_stage_id;
	memset(this->stages, 0, sizeof(this->stages));
	for (int i=0; i<stage_count; i++) {
		this->stage_count = i;
		cf_arenax_err result = cf_arenax_add_stage(this);

		// No need to detach - add_stage() won't fail and leave attached stage.
		if (result != CF_ARENAX_OK && (this->flags & CF_ARENAX_BIGLOCK)) {
			pthread_mutex_destroy(&this->lock);
		}
	}
	this->at_stage_id = at_stage_id;
	this->at_element_id = at_element_id;

	return CF_ARENAX_OK;
}
