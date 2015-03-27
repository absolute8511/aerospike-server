/*
 * storage.c
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
 * method-agnostic storage engine code
 */

// TODO - We have a #include loop - datamodel.h and storage.h include each
// other. I'd love to untangle this mess, but can't right now. So this needs to
// be here to allow compilation for now:
#include "base/datamodel.h"

#include "storage/storage.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_digest.h"

#include "fault.h"
#include "olock.h"
#include "queue.h"

#include "base/datamodel.h"
#include "base/cfg.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/thr_info.h"


//==========================================================
// Generic "base class" functions that call through
// storage-engine "v-tables".
//

//--------------------------------------
// as_storage_init
//

typedef int (*as_storage_namespace_init_fn)(as_namespace *ns, cf_queue *complete_q, void *udata);
static const as_storage_namespace_init_fn as_storage_namespace_init_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no init
	as_storage_namespace_init_ssd,
	as_storage_namespace_init_kv
};

void
as_storage_init()
{
	cf_queue *complete_q = cf_queue_create(sizeof(void*), true);

	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (as_storage_namespace_init_table[ns->storage_type]) {
			if (0 != as_storage_namespace_init_table[ns->storage_type](ns, complete_q, NULL)) {
				cf_crash(AS_STORAGE, "could not initialize storage for namespace %s", ns->name);
			}
		}
		else {
			void *_t;

			cf_queue_push(complete_q, &_t);
		}
	}

	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		void *_t;

		while (CF_QUEUE_OK != cf_queue_pop(complete_q, &_t, 2000)) {
			as_storage_cold_start_ticker_ssd();
		}
	}

	cf_queue_destroy(complete_q);
}

//--------------------------------------
// as_storage_namespace_destroy
//

typedef int (*as_storage_namespace_destroy_fn)(as_namespace *ns);
static const as_storage_namespace_destroy_fn as_storage_namespace_destroy_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no destroy
	as_storage_namespace_destroy_ssd,
	as_storage_namespace_destroy_kv

};

int
as_storage_namespace_destroy(as_namespace *ns)
{
	if (as_storage_namespace_destroy_table[ns->storage_type]) {
		return as_storage_namespace_destroy_table[ns->storage_type](ns);
	}

	return 0;
}

//--------------------------------------
// as_storage_namespace_attributes_get
//

typedef int (*as_storage_namespace_attributes_get_fn)(as_namespace *ns, as_storage_attributes *attr);
static const as_storage_namespace_attributes_get_fn as_storage_namespace_attributes_get_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	as_storage_namespace_attributes_get_memory,
	as_storage_namespace_attributes_get_ssd,
	as_storage_namespace_attributes_get_kv
};

int
as_storage_namespace_attributes_get(as_namespace *ns, as_storage_attributes *attr)
{
	if (as_storage_namespace_attributes_get_table[ns->storage_type]) {
		return as_storage_namespace_attributes_get_table[ns->storage_type](ns, attr);
	}

	cf_warning(AS_STORAGE, "could not get storage attributes for namespace %s: internal error", ns->name);
	return -1;
}

//--------------------------------------
// as_storage_has_index
//

typedef int (*as_storage_has_index_fn)(as_namespace *ns);
static const as_storage_has_index_fn as_storage_has_index_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no index
	0, // ssd has no index
	as_storage_has_index_kv
};

int
as_storage_has_index(as_namespace *ns)
{
	if (as_storage_has_index_table[ns->storage_type]) {
	  return as_storage_has_index_table[ns->storage_type](ns);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_exists
//

typedef int (*as_storage_record_exists_fn)(as_namespace *ns, cf_digest *keyd);
static const as_storage_record_exists_fn as_storage_record_exists_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record exists
	0, // ssd has no record exists
	as_storage_record_exists_kv
};

int as_storage_record_exists(as_namespace *ns, cf_digest *keyd)
{
	if (as_storage_record_exists_table[ns->storage_type]) {
	  return as_storage_record_exists_table[ns->storage_type](ns, keyd);
	}

	cf_warning(AS_STORAGE, "record existence check not supported by storage type %d on namespace %s", ns->storage_type, ns->name);
	return 0;
}

//--------------------------------------
// as_storage_record_destroy
//

typedef int (*as_storage_record_destroy_fn)(as_namespace *ns, as_record *r);
static const as_storage_record_destroy_fn as_storage_record_destroy_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record destroy
	as_storage_record_destroy_ssd,
	0  // kv has no record destroy
};

int
as_storage_record_destroy(as_namespace *ns, as_record *r)
{
	if (as_storage_record_destroy_table[ns->storage_type]) {
		return as_storage_record_destroy_table[ns->storage_type](ns, r);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_create
//

typedef int (*as_storage_record_create_fn)(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
static const as_storage_record_create_fn as_storage_record_create_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record create
	as_storage_record_create_ssd,
	as_storage_record_create_kv,
};

int
as_storage_record_create(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd)
{
	rd->storage_type = ns->storage_type;
	rd->r = r;
	rd->ns = ns;
	as_rec_props_clear(&rd->rec_props);
	rd->keyd = *keyd;
	rd->bins = 0;
	rd->n_bins = 0;
	rd->record_on_device = false;
	rd->ignore_record_on_device = false;
	rd->have_device_block = false;
	rd->write_to_device = false;
	rd->key_size = 0;
	rd->key = NULL;
	rd->n_bins_to_write = 0;
	rd->particles_flat_size = 0;
	rd->flat_size = 0;

	if (as_storage_record_create_table[ns->storage_type]) {
		return as_storage_record_create_table[ns->storage_type](ns, r, rd, keyd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_open
//

typedef int (*as_storage_record_open_fn)(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
static const as_storage_record_open_fn as_storage_record_open_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record open
	as_storage_record_open_ssd,
	as_storage_record_open_kv,
};

int
as_storage_record_open(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd)
{
	rd->storage_type = ns->storage_type;
	rd->r = r;
	rd->ns = ns;
	as_rec_props_clear(&rd->rec_props);
	rd->keyd = *keyd;
	rd->bins = 0;
	rd->n_bins = 0;
	rd->record_on_device = true;
	rd->ignore_record_on_device = false;
	rd->have_device_block = false;
	rd->write_to_device = false;
	rd->key_size = 0;
	rd->key = NULL;
	rd->n_bins_to_write = 0;
	rd->particles_flat_size = 0;
	rd->flat_size = 0;

	if (as_storage_record_open_table[ns->storage_type]) {
		return as_storage_record_open_table[ns->storage_type](ns, r, rd, keyd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_close
//

typedef void (*as_storage_record_close_fn)(as_record *ns, as_storage_rd *rd);
static const as_storage_record_close_fn as_storage_record_close_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record close
	as_storage_record_close_ssd,
	as_storage_record_close_kv
};

void
as_storage_record_close(as_record *r, as_storage_rd *rd)
{
	if (as_storage_record_close_table[rd->storage_type]) {
		as_storage_record_close_table[rd->storage_type](r, rd);
	}
}

//--------------------------------------
// as_storage_record_get_n_bins
//

typedef uint16_t (*as_storage_record_get_n_bins_fn)(as_storage_rd *rd);
static const as_storage_record_get_n_bins_fn as_storage_record_get_n_bins_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record get n bins
	as_storage_record_get_n_bins_ssd,
	as_storage_record_get_n_bins_kv,
};

uint16_t
as_storage_record_get_n_bins(as_storage_rd *rd)
{
	if (as_storage_record_get_n_bins_table[rd->storage_type]) {
		return as_storage_record_get_n_bins_table[rd->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_read
//

typedef int (*as_storage_record_read_fn)(as_storage_rd *rd);
static const as_storage_record_read_fn as_storage_record_read_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record read
	as_storage_record_read_ssd,
	as_storage_record_read_kv
};

int
as_storage_record_read(as_storage_rd *rd)
{
	if (as_storage_record_read_table[rd->storage_type]) {
		return as_storage_record_read_table[rd->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_particle_read_all
//

typedef int (*as_storage_particle_read_all_fn)(as_storage_rd *rd);
static const as_storage_particle_read_all_fn as_storage_particle_read_all_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no record read all
	as_storage_particle_read_all_ssd,
	as_storage_particle_read_all_kv
};

int
as_storage_particle_read_all(as_storage_rd *rd)
{
	if (as_storage_particle_read_all_table[rd->storage_type]) {
		return as_storage_particle_read_all_table[rd->storage_type](rd);
	}

	return 0;
}

//--------------------------------------
// as_storage_record_can_fit
//

typedef bool (*as_storage_record_can_fit_fn)(as_storage_rd *rd);
static const as_storage_record_can_fit_fn as_storage_record_can_fit_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // no limit if no persistent storage - flat size is irrelevant
	as_storage_record_can_fit_ssd,
	0
};

bool
as_storage_record_can_fit(as_storage_rd *rd)
{
	if (as_storage_record_can_fit_table[rd->ns->storage_type]) {
		return as_storage_record_can_fit_table[rd->ns->storage_type](rd);
	}

	return true;
}

//--------------------------------------
// as_storage_record_size_and_check
//

typedef bool (*as_storage_record_size_and_check_fn)(as_storage_rd *rd);
static const as_storage_record_size_and_check_fn as_storage_record_size_and_check_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // no limit if no persistent storage - flat size is irrelevant
	as_storage_record_size_and_check_ssd,
	0
};

bool
as_storage_record_size_and_check(as_storage_rd *rd)
{
	if (as_storage_record_size_and_check_table[rd->ns->storage_type]) {
		return as_storage_record_size_and_check_table[rd->ns->storage_type](rd);
	}

	return true;
}

//--------------------------------------
// as_storage_wait_for_defrag
//

typedef void (*as_storage_wait_for_defrag_fn)(as_namespace *ns);
static const as_storage_wait_for_defrag_fn as_storage_wait_for_defrag_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't do defrag
	as_storage_wait_for_defrag_ssd,
	0  // kv doesn't do defrag
};

void
as_storage_wait_for_defrag()
{
	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (as_storage_wait_for_defrag_table[ns->storage_type]) {
			as_storage_wait_for_defrag_table[ns->storage_type](ns);
		}
	}
}

//--------------------------------------
// as_storage_overloaded
//

typedef bool (*as_storage_overloaded_fn)(as_namespace *ns);
static const as_storage_overloaded_fn as_storage_overloaded_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no overload check
	as_storage_overloaded_ssd,
	0  // kv has no overload check
};

bool
as_storage_overloaded(as_namespace *ns)
{
	if (as_storage_overloaded_table[ns->storage_type]) {
		return as_storage_overloaded_table[ns->storage_type](ns);
	}

	return false;
}

//--------------------------------------
// as_storage_has_space
//

typedef bool (*as_storage_has_space_fn)(as_namespace *ns);
static const as_storage_has_space_fn as_storage_has_space_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory has no space check
	as_storage_has_space_ssd,
	0  // kv has no space check
};

bool
as_storage_has_space(as_namespace *ns)
{
	if (as_storage_has_space_table[ns->storage_type]) {
		return as_storage_has_space_table[ns->storage_type](ns);
	}

	return true;
}

//--------------------------------------
// as_storage_defrag_sweep
//

typedef void (*as_storage_defrag_sweep_fn)(as_namespace *ns);
static const as_storage_defrag_sweep_fn as_storage_defrag_sweep_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't do defrag
	as_storage_defrag_sweep_ssd,
	0  // kv doesn't do defrag
};

void
as_storage_defrag_sweep(as_namespace *ns)
{
	if (as_storage_defrag_sweep_table[ns->storage_type]) {
		as_storage_defrag_sweep_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_info_set
//

typedef int (*as_storage_info_set_fn)(as_namespace *ns, uint idx, uint8_t *buf, size_t len);
static const as_storage_info_set_fn as_storage_info_set_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't support info
	as_storage_info_set_ssd,
	0  // kv doesn't support info
};

int
as_storage_info_set(as_namespace *ns, uint idx, uint8_t *buf, size_t len)
{
	if (as_storage_info_set_table[ns->storage_type]) {
		return as_storage_info_set_table[ns->storage_type](ns, idx, buf, len);
	}

	return 0;
}

//--------------------------------------
// as_storage_info_get
//

typedef int (*as_storage_info_get_fn)(as_namespace *ns, uint idx, uint8_t *buf, size_t *len);
static const as_storage_info_get_fn as_storage_info_get_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't support info
	as_storage_info_get_ssd,
	0  // kv doesn't support info
};

int
as_storage_info_get(as_namespace *ns, uint idx, uint8_t *buf, size_t *len)
{
	if (as_storage_info_get_table[ns->storage_type]) {
		return as_storage_info_get_table[ns->storage_type](ns, idx, buf, len);
	}

	return -1; // buf not filled - safer to return error
}

//--------------------------------------
// as_storage_info_flush
//

typedef int (*as_storage_info_flush_fn)(as_namespace *ns);
static const as_storage_info_flush_fn as_storage_info_flush_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't support info
	as_storage_info_flush_ssd,
	0  // kv doesn't support info
};

int
as_storage_info_flush(as_namespace *ns)
{
	if (as_storage_info_flush_table[ns->storage_type]) {
		return as_storage_info_flush_table[ns->storage_type](ns);
	}

	return 0;
}

//--------------------------------------
// as_storage_save_evict_void_time
//

typedef void (*as_storage_save_evict_void_time_fn)(as_namespace *ns, uint32_t evict_void_time);
static const as_storage_save_evict_void_time_fn as_storage_save_evict_void_time_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't store info
	as_storage_save_evict_void_time_ssd,
	0  // kv doesn't store info
};

void
as_storage_save_evict_void_time(as_namespace *ns, uint32_t evict_void_time)
{
	if (as_storage_save_evict_void_time_table[ns->storage_type]) {
		as_storage_save_evict_void_time_table[ns->storage_type](ns, evict_void_time);
	}
}

//--------------------------------------
// as_storage_stats
//

typedef int (*as_storage_stats_fn)(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes);
static const as_storage_stats_fn as_storage_stats_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	as_storage_stats_memory,
	as_storage_stats_ssd,
	as_storage_stats_kv,
};

int
as_storage_stats(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes)
{
	if (as_storage_stats_table[ns->storage_type]) {
		return as_storage_stats_table[ns->storage_type](ns, available_pct, used_disk_bytes);
	}

	return 0;
}

//--------------------------------------
// as_storage_ticker_stats
//

typedef int (*as_storage_ticker_stats_fn)(as_namespace *ns);
static const as_storage_ticker_stats_fn as_storage_ticker_stats_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't support per-disk histograms... for now.
	as_storage_ticker_stats_ssd,
	0  // kv doesn't support per-disk histograms... for now.
};

int
as_storage_ticker_stats()
{
	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (as_storage_ticker_stats_table[ns->storage_type]) {
			as_storage_ticker_stats_table[ns->storage_type](ns);
		}
	}

	return 0;
}

//--------------------------------------
// as_storage_histogram_clear_all
//

typedef int (*as_storage_histogram_clear_fn)(as_namespace *ns);
static const as_storage_histogram_clear_fn as_storage_histogram_clear_table[AS_STORAGE_ENGINE_TYPES] = {
	NULL,
	0, // memory doesn't support per-disk histograms... for now.
	as_storage_histogram_clear_ssd,
	0  // kv doesn't support per-disk histograms... for now.
};

int
as_storage_histogram_clear_all()
{
	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (as_storage_histogram_clear_table[ns->storage_type]) {
			as_storage_histogram_clear_table[ns->storage_type](ns);
		}
	}

	return 0;
}


//==========================================================
// Generic functions that don't use "v-tables".
//

// Get size of record's in-memory data - everything except index bytes.
uint64_t
as_storage_record_get_n_bytes_memory(as_storage_rd *rd)
{
	if (! rd->ns->storage_data_in_memory) {
		return 0;
	}

	if (rd->n_bins == 0) {
		// Don't count overhead in this case.
		return 0;
	}

	uint64_t n_bytes_memory = 0;

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *b = &rd->bins[i];

		if (as_bin_inuse(b) && ! as_bin_is_integer(b)) {
			n_bytes_memory +=
					as_particle_get_base_size(as_bin_get_particle_type(b)) +
					as_bin_get_particle_size(b);
		}
	}

	if (! rd->ns->single_bin) {
		if (as_index_is_flag_set(rd->r, AS_INDEX_FLAG_KEY_STORED)) {
			n_bytes_memory += sizeof(as_rec_space) +
					((as_rec_space*)rd->r->dim)->key_size;
		}

		n_bytes_memory += sizeof(as_bin_space) + (sizeof(as_bin) * rd->n_bins);
	}

	return n_bytes_memory;
}

bool
as_storage_record_get_key(as_storage_rd *rd)
{
	if (! as_index_is_flag_set(rd->r, AS_INDEX_FLAG_KEY_STORED)) {
		return false;
	}

	if (rd->ns->storage_data_in_memory) {
		rd->key_size = ((as_rec_space*)rd->r->dim)->key_size;
		rd->key = ((as_rec_space*)rd->r->dim)->key;
		return true;
	}

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_get_key_ssd(rd);
	}

	return false;
}

size_t
as_storage_record_rec_props_size(as_storage_rd *rd)
{
	size_t rec_props_data_size = as_ldt_record_get_rectype_bits(rd->r) != 0 ?
			as_rec_props_sizeof_field(sizeof(uint16_t)) : 0;

	const char *set_name = as_index_get_set_name(rd->r, rd->ns);

	if (set_name) {
		rec_props_data_size += as_rec_props_sizeof_field(strlen(set_name) + 1);
	}

	if (rd->key) {
		rec_props_data_size += as_rec_props_sizeof_field(rd->key_size);
	}

	return rec_props_data_size;
}

// Populates rec_props struct in rd, using index info where possible. Assumes
// relevant information is ready:
// - set name
// - LDT flags
// - record key
// Relies on caller's properly allocated rec_props_data.
void
as_storage_record_set_rec_props(as_storage_rd *rd, uint8_t* rec_props_data)
{
	as_rec_props_init(&(rd->rec_props), rec_props_data);

	uint16_t ldt_rectype_bits = as_ldt_record_get_rectype_bits(rd->r);

	if (ldt_rectype_bits != 0) {
		as_rec_props_add_field(&(rd->rec_props), CL_REC_PROPS_FIELD_LDT_TYPE,
				sizeof(uint16_t), (uint8_t *)&ldt_rectype_bits);
	}

	if (as_index_has_set(rd->r)) {
		const char *set_name = as_index_get_set_name(rd->r, rd->ns);
		as_rec_props_add_field(&(rd->rec_props), CL_REC_PROPS_FIELD_SET_NAME,
				strlen(set_name) + 1, (uint8_t *)set_name);
	}

	if (rd->key) {
		as_rec_props_add_field(&(rd->rec_props), CL_REC_PROPS_FIELD_KEY,
				rd->key_size, rd->key);
	}
}

// Populates p_rec_props, doing a malloc for data, using index info where
// possible (gets key from rd parameters).
uint32_t
as_storage_record_copy_rec_props(as_storage_rd *rd, as_rec_props *p_rec_props)
{
	uint32_t malloc_size = (uint32_t)as_storage_record_rec_props_size(rd);

	if (malloc_size == 0) {
		return 0;
	}

	as_rec_props_init_malloc(p_rec_props, malloc_size);

	uint64_t ldt_rectype_bits = as_ldt_record_get_rectype_bits(rd->r);

	if (ldt_rectype_bits != 0) {
		as_rec_props_add_field(p_rec_props, CL_REC_PROPS_FIELD_LDT_TYPE,
				sizeof(uint16_t), (uint8_t *)&ldt_rectype_bits);
	}

	if (as_index_has_set(rd->r)) {
		const char *set_name = as_index_get_set_name(rd->r, rd->ns);
		as_rec_props_add_field(p_rec_props, CL_REC_PROPS_FIELD_SET_NAME,
				strlen(set_name) + 1, (uint8_t *)set_name);
	}

	if (rd->key) {
		as_rec_props_add_field(p_rec_props, CL_REC_PROPS_FIELD_KEY,
				rd->key_size, rd->key);
	}

	return malloc_size;
}

void
as_storage_shutdown(void)
{
	cf_info(AS_STORAGE, "initiating storage shutdown ...");

	// Pull all record locks - stops everything writing to current swbs such
	// that each write's record lock scope is either completed or never entered.

	olock* p_olock = g_config.record_locks;

	for (uint32_t n = 0; n < p_olock->n_locks; n++) {
		pthread_mutex_lock(&p_olock->locks[n]);
	}

	// Now flush everything outstanding to storage devices.

 	cf_info(AS_STORAGE, "flushing data to storage ...");

	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {
			as_storage_shutdown_ssd(ns);
			as_namespace_xmem_trusted(ns);
		}
	}

  	cf_info(AS_STORAGE, "completed flushing to storage");
};
