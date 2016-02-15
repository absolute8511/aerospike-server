/*
 * storage.h
 *
 * Copyright (C) 2009-2015 Aerospike, Inc.
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
 * Core storage structures and definitions
 *
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "queue.h"

#include "base/datamodel.h"
#include "base/rec_props.h"


/* SYNOPSIS
 * Data storage
 *
 * These functions are called by the main 'as' engine while reading and writing to store things
 * The abstraction is built so that 'malloc' is one of the storage engines
 * When creating a new record, pass the keyd in. Storage engines are helped by having such a unique
 * key that's easily hashable and such
 *
 * The code mates with as_record. There is a storage_key union which the storage engine is allowed
 * to use for its own nefarious purposes. It should be a union of whatever information all the currently
 * defined storage engines might need, thus is a HUGE UNPLEASANT HACK
 *
 * In use, the caller will allocate a stack record for the as_storage_rd (record descriptor) and
 * pass it in every time.
 * The caller will also stack allocate the as_storage_pd (particle descriptior) and pass it in.
 * The "flush" occurs when the record is closed.
 *
 * The particle descriptor contains a pointer to data the storage system just brought into memory.
 * There is no length required at this level (like Malloc, if in use). When the data is written,
 * the entire length of the particle is given to the storage engine.
 *
 * When a write occurs, it might be on the same pointer that
 */


// The type of backing storage configured.
typedef enum {
	AS_STORAGE_ENGINE_UNDEF		= 0,
	AS_STORAGE_ENGINE_MEMORY	= 1,
	AS_STORAGE_ENGINE_SSD		= 2,
	AS_STORAGE_ENGINE_KV		= 3
} as_storage_type;

#define NAMESPACE_HAS_PERSISTENCE(ns) \
	(ns->storage_type != AS_STORAGE_ENGINE_MEMORY) 
// For sizing the storage API "v-tables".
#define AS_STORAGE_ENGINE_TYPES 4

// For invalidating the ssd.file_id and ssd.rblock_id bits in as_index.
#define STORAGE_INVALID_FILE_ID	0x3F // 6 bits
#define STORAGE_INVALID_RBLOCK	0x3FFFFffff // 34 bits

// Forward declarations.
struct drv_ssd_s;
struct drv_ssd_block_s;
struct drv_kv_s;
struct drv_kv_block_s;

// A record descriptor.
typedef struct as_storage_rd_s {
	as_record		*r;
	as_namespace	*ns;						// the namespace, which contain all files
	as_rec_props	rec_props;					// list of metadata name-value pairs, e.g. name of set
	as_bin			*bins;						// pointer to the appropriate bin_space, which is either
												// part of the record (single bin, data_in_memory == true),
												// memalloc'd (multi-bin, data_in_memory == true), or
												// temporary (data_in_memory == false)
												// enables this record's data to be written to drive
	uint16_t		n_bins;
	bool			record_on_device;			// if true, record exists on device
	bool			ignore_record_on_device;	// if true, never read record off device (such as in replace case)
	bool			have_device_block;			// if true, we have a storage block as part of the rd. if false, we must get one.
	cf_digest		keyd;						// when doing a write, we'll need to stash this to do the "callback"

	// Parameters used when handling key storage:
	uint32_t		key_size;
	uint8_t			*key;

	as_storage_type storage_type;

	union {
		struct {
			struct drv_ssd_block_s	*block;				// data that was read in at one point
			uint8_t					*must_free_block;	// if not null, must free this pointer - may be different to block pointer
														// if null, part of a bigger block that will be freed elsewhere
			struct drv_ssd_s		*ssd;				// the particular ssd object we're using
		} ssd;
		struct {
			struct drv_kv_block_s	*block;				// data that was read in at one point
			uint8_t					*must_free_block;	// if not null, must free this pointer - may be different to block pointer
														// if null, part of a bigger block that will be freed elsewhere
			struct drv_kv_s			*kv;				// the particular kv object we're using
		} kv;
	} u;
} as_storage_rd;

// Information about a namespace's storage.
typedef struct {
    int		n_devices;
} as_storage_attributes;


//------------------------------------------------
// Generic "base class" functions that call
// through storage-engine "v-tables".
//

extern void as_storage_init();
extern int as_storage_namespace_destroy(as_namespace *ns);
extern int as_storage_namespace_attributes_get(as_namespace *ns, as_storage_attributes *attr);

extern int as_storage_has_index(as_namespace *ns);
extern int as_storage_record_exists(as_namespace *ns, cf_digest *keyd);
extern int as_storage_record_destroy(as_namespace *ns, as_record *r); // not the counterpart of as_storage_record_create()

// Start and finish an as_storage_rd usage cycle.
extern int as_storage_record_create(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_open(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_close(as_record *r, as_storage_rd *rd);

// Called within as_storage_rd usage cycle.
extern uint16_t as_storage_record_get_n_bins(as_storage_rd *rd);
extern int as_storage_record_read(as_storage_rd *rd);
extern int as_storage_particle_read_all(as_storage_rd *rd);
extern bool as_storage_record_size_and_check(as_storage_rd *rd);
extern int as_storage_record_write(as_record *r, as_storage_rd *rd);

// Storage capacity monitoring.
extern void as_storage_wait_for_defrag();
extern bool as_storage_overloaded(as_namespace *ns); // returns true if write queue is too backed up
extern bool as_storage_has_space(as_namespace *ns);
extern void as_storage_defrag_sweep(as_namespace *ns);

// Storage of generic data into device headers.
extern int as_storage_info_set(as_namespace *ns, uint idx, uint8_t *buf, size_t len);
extern int as_storage_info_get(as_namespace *ns, uint idx, uint8_t *buf, size_t *len);
extern int as_storage_info_flush(as_namespace *ns);
extern void as_storage_save_evict_void_time(as_namespace *ns, uint32_t evict_void_time);

// Statistics.
extern int as_storage_stats(as_namespace *ns, int *available_pct, uint64_t *inuse_disk_bytes); // available percent is that of worst device
extern int as_storage_ticker_stats(); // prints SSD histograms to the info ticker
extern int as_storage_histogram_clear_all(); // clears all SSD histograms


//------------------------------------------------
// Generic functions that don't use "v-tables".
//

// Called within as_storage_rd usage cycle.
extern uint64_t as_storage_record_get_n_bytes_memory(as_storage_rd *rd);
extern void as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint64_t start_bytes);
extern void as_storage_record_drop_from_mem_stats(as_storage_rd *rd);
extern bool as_storage_record_get_key(as_storage_rd *rd);
extern size_t as_storage_record_rec_props_size(as_storage_rd *rd);
extern void as_storage_record_set_rec_props(as_storage_rd *rd, uint8_t* rec_props_data);
extern uint32_t as_storage_record_copy_rec_props(as_storage_rd *rd, as_rec_props *p_rec_props);

// Called only at shutdown to flush all device write-queues.
extern void as_storage_shutdown();


//------------------------------------------------
// AS_STORAGE_ENGINE_MEMORY functions.
//

extern int as_storage_namespace_init_memory(as_namespace *ns, cf_queue *complete_q, void *udata);
extern int as_storage_namespace_destroy_memory(as_namespace *ns);
extern int as_storage_namespace_attributes_get_memory(as_namespace *ns, as_storage_attributes *attr);

extern int as_storage_stats_memory(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes);


//------------------------------------------------
// AS_STORAGE_ENGINE_SSD functions.
//

extern int as_storage_namespace_init_ssd(as_namespace *ns, cf_queue *complete_q, void *udata);
extern void as_storage_cold_start_ticker_ssd(); // called directly by as_storage_init()
extern int as_storage_namespace_destroy_ssd(as_namespace *ns);
extern int as_storage_namespace_attributes_get_ssd(as_namespace *ns, as_storage_attributes *attr);

extern int as_storage_record_destroy_ssd(as_namespace *ns, as_record *r);

extern int as_storage_record_create_ssd(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_open_ssd(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_close_ssd(as_record *r, as_storage_rd *rd);

extern uint16_t as_storage_record_get_n_bins_ssd(as_storage_rd *rd);
extern int as_storage_record_read_ssd(as_storage_rd *rd);
extern int as_storage_particle_read_all_ssd(as_storage_rd *rd);
extern bool as_storage_record_size_and_check_ssd(as_storage_rd *rd);
extern int as_storage_record_write_ssd(as_record *r, as_storage_rd *rd);

extern void as_storage_wait_for_defrag_ssd(as_namespace *ns);
extern bool as_storage_overloaded_ssd(as_namespace *ns);
extern bool as_storage_has_space_ssd(as_namespace *ns);
extern void as_storage_defrag_sweep_ssd(as_namespace *ns);

extern int as_storage_info_set_ssd(as_namespace *ns, uint idx, uint8_t *buf, size_t len);
extern int as_storage_info_get_ssd(as_namespace *ns, uint idx, uint8_t *buf, size_t *len);
extern int as_storage_info_flush_ssd(as_namespace *ns);
extern void as_storage_save_evict_void_time_ssd(as_namespace *ns, uint32_t evict_void_time);

extern int as_storage_stats_ssd(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes);
extern int as_storage_ticker_stats_ssd(as_namespace *ns);
extern int as_storage_histogram_clear_ssd(as_namespace *ns);

// Called by "base class" functions but not via table.
extern bool as_storage_record_get_key_ssd(as_storage_rd *rd);
extern void as_storage_shutdown_ssd(as_namespace *ns);


//------------------------------------------------
// AS_STORAGE_ENGINE_KV functions.
//

extern int as_storage_namespace_init_kv(as_namespace *ns, cf_queue *complete_q, void *udata);
extern int as_storage_namespace_destroy_kv(as_namespace *ns);
extern int as_storage_namespace_attributes_get_kv(as_namespace *ns, as_storage_attributes *attr);

extern int as_storage_has_index_kv(as_namespace *ns);
extern int as_storage_record_exists_kv(as_namespace *ns, cf_digest *keyd);

extern int as_storage_record_create_kv(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_open_kv(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_close_kv(as_record *r, as_storage_rd *rd);

extern uint16_t as_storage_record_get_n_bins_kv(as_storage_rd *rd);
extern int as_storage_record_read_kv(as_storage_rd *rd);
extern int as_storage_particle_read_all_kv(as_storage_rd *rd);
extern int as_storage_record_write_kv(as_record *r, as_storage_rd *rd);

extern int as_storage_stats_kv(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes);
