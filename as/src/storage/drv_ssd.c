/*
 * drv_ssd.c
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

/* SYNOPSIS
 * "file" based storage driver, which applies to both SSD namespaces and, in
 * some cases, to file-backed main-memory namespaces.
 */

#include "base/feature.h" // turn new AS Features on/off (must be first in line)

#include "storage/drv_ssd.h"

// TODO - We have a #include loop - datamodel.h and storage.h include each
// other. I'd love to untangle this mess, but can't right now. So this needs to
// be here to allow compilation for now:
#include "base/datamodel.h"

#include "storage/storage.h"

#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <linux/fs.h> // for BLKGETSIZE64
#include <sys/ioctl.h>
#include <sys/param.h> // for MAX()

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_random.h"

#include "fault.h"
#include "hist.h"
#include "jem.h"
#include "queue.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/cfg.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/secondary_index.h"


//==========================================================
// Forward declarations.
//

// Defined in thr_nsup.c, for historical reasons.
extern bool as_cold_start_evict_if_needed(as_namespace* ns);


//==========================================================
// Constants.
//

#define MAX_WRITE_BLOCK_SIZE	(1024 * 1024)
#define LOAD_BUF_SIZE			MAX_WRITE_BLOCK_SIZE // must be multiple of MAX_WRITE_BLOCK_SIZE

// We round usable device/file size down to SSD_DEFAULT_HEADER_LENGTH plus a
// multiple of LOAD_BUF_SIZE. If we ever change SSD_DEFAULT_HEADER_LENGTH we
// may break backward compatibility since an old header with different size
// could render the rounding ineffective. (There are backward compatibility
// issues anyway if we think we need to change SSD_DEFAULT_HEADER_LENGTH...)
#define SSD_DEFAULT_HEADER_LENGTH	(1024 * 1024)
#define SSD_DEFAULT_INFO_NUMBER		(1024 * 4)
#define SSD_DEFAULT_INFO_LENGTH		(128)

#define SSD_BLOCK_MAGIC		0x037AF200
#define SIGNATURE_OFFSET	offsetof(struct drv_ssd_block_s, keyd)

// Write-smoothing constants.
#define MIN_SECONDS_OF_WRITE_SMOOTHING_DATA		5
#define NUM_ELEMS_IN_LBW_CATCH_UP_CALC			5

#define DEFRAG_STARTUP_RESERVE	4
#define DEFRAG_RUNTIME_RESERVE	4


//==========================================================
// Typedefs.
//

// Info slice in device header block.
typedef struct {
	uint32_t	len;
	uint8_t		data[];
} info_buf;


//------------------------------------------------
// Per-record metadata on device.
//
typedef struct drv_ssd_block_s {
	cf_signature	sig;			// digest of this entire block, 64 bits
	uint32_t		magic;
	uint32_t		length;			// total under signature - starts after this field - pointer + 16
	cf_digest		keyd;
	as_generation	generation;
	cf_clock		void_time;
	uint32_t		bins_offset;	// offset to bins from data
	uint32_t		n_bins;
	uint32_t		vinfo_offset;
	uint32_t		vinfo_length;
	uint8_t			data[];
} __attribute__ ((__packed__)) drv_ssd_block;


//------------------------------------------------
// Per-bin metadata on device.
//
typedef struct drv_ssd_bin_s {
	char		name[AS_ID_BIN_SZ];	// 15 aligns well
	uint8_t		version;
	uint32_t	offset;				// offset of bin data within block
	uint32_t	len;				// size of bin data
	uint32_t	next;				// location of next bin: block offset
} __attribute__ ((__packed__)) drv_ssd_bin;


//==========================================================
// Miscellaneous utility functions.
//

// Get an open file descriptor from the pool, or a fresh one if necessary.
int
ssd_fd_get(drv_ssd *ssd)
{
	int fd = -1;
	int rv = cf_queue_pop(ssd->fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}
	}

	return fd;
}


int
ssd_shadow_fd_get(drv_ssd *ssd)
{
	int fd = -1;
	int rv = cf_queue_pop(ssd->shadow_fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(ssd->shadow_name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->shadow_name, errno, cf_strerror(errno));
		}
	}

	return fd;
}


// Save an open file descriptor in the pool
static inline void
ssd_fd_put(drv_ssd *ssd, int fd)
{
	cf_queue_push(ssd->fd_q, (void*)&fd);
}


static inline void
ssd_shadow_fd_put(drv_ssd *ssd, int fd)
{
	cf_queue_push(ssd->shadow_fd_q, (void*)&fd);
}


// Decide which device a record belongs on.
static inline int
ssd_get_file_id(drv_ssds *ssds, cf_digest *keyd)
{
	return keyd->digest[DIGEST_STORAGE_BYTE] % ssds->n_ssds;
}


// Put a wblock on the free queue for reuse.
void
push_wblock_to_free_q(drv_ssd *ssd, uint32_t wblock_id, e_free_to free_to)
{
	if (! ssd->free_wblock_q) { // null until devices are loaded at startup
		return;
	}

	// temp debugging:
	if (wblock_id >= ssd->alloc_table->n_wblocks) {
		cf_warning(AS_DRV_SSD, "pushing invalid wblock_id %d to free_wblock_q",
				(int32_t)wblock_id);
		return;
	}

	if (free_to == FREE_TO_HEAD) {
		cf_queue_push_head(ssd->free_wblock_q, &wblock_id);
	}
	else {
		cf_queue_push(ssd->free_wblock_q, &wblock_id);
	}
}


// Put a wblock on the defrag queue.
static inline void
push_wblock_to_defrag_q(drv_ssd *ssd, uint32_t wblock_id)
{
	if (ssd->defrag_wblock_q) { // null until devices are loaded at startup
		ssd->alloc_table->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(ssd->defrag_wblock_q, &wblock_id);
		cf_atomic_int_incr(&ssd->defrag_wblock_counter);
	}
}


// Available contiguous size.
static inline uint64_t
available_size(drv_ssd *ssd)
{
	return ssd->free_wblock_q ? // null until devices are loaded at startup
			(uint64_t)cf_queue_sz(ssd->free_wblock_q) * ssd->write_block_size :
			ssd->file_size;

	// Note - returns 100% available during cold start, to make it irrelevant in
	// cold-start eviction threshold check.
}


//------------------------------------------------
// ssd_write_buf "swb" methods.
//

static inline ssd_write_buf*
swb_create(drv_ssd *ssd)
{
	ssd_write_buf *swb = (ssd_write_buf*)cf_malloc(sizeof(ssd_write_buf));

	if (! swb) {
		cf_warning(AS_DRV_SSD, "device %s - swb malloc failed", ssd->name);
		return NULL;
	}

	swb->buf = cf_valloc(ssd->write_block_size);

	if (! swb->buf) {
		cf_warning(AS_DRV_SSD, "device %s - swb buf valloc failed", ssd->name);
		cf_free(swb);
		return NULL;
	}

	return swb;
}

static inline void
swb_destroy(ssd_write_buf *swb)
{
	cf_free(swb->buf);
	cf_free(swb);
}

static inline void
swb_reset(ssd_write_buf *swb)
{
	swb->wblock_id = STORAGE_INVALID_WBLOCK;
	swb->pos = 0;
}

#define swb_reserve(_swb) cf_atomic32_incr(&(_swb)->rc)

static inline void
swb_check_and_reserve(ssd_wblock_state *wblock_state, ssd_write_buf **p_swb)
{
	pthread_mutex_lock(&wblock_state->LOCK);

	if (wblock_state->swb) {
		*p_swb = wblock_state->swb;
		swb_reserve(*p_swb);
	}

	pthread_mutex_unlock(&wblock_state->LOCK);
}

static inline void
swb_release(ssd_write_buf *swb)
{
	if (0 == cf_atomic32_decr(&swb->rc)) {
		swb_reset(swb);

		// Put the swb back on the free queue for reuse.
		cf_queue_push(swb->ssd->swb_free_q, &swb);
	}
}

static inline void
swb_dereference_and_release(drv_ssd *ssd, uint32_t wblock_id,
		ssd_write_buf *swb)
{
	ssd_wblock_state *wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	pthread_mutex_lock(&wblock_state->LOCK);

	if (swb != wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "releasing wrong swb! %p (%d) != %p (%d), thread %d",
			swb, (int32_t)swb->wblock_id, wblock_state->swb,
			(int32_t)wblock_state->swb->wblock_id, pthread_self());
	}

	swb_release(wblock_state->swb);
	wblock_state->swb = 0;

	if (wblock_state->state != WBLOCK_STATE_DEFRAG) {
		uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

		// Free wblock if all three gating conditions hold.
		if (inuse_sz == 0) {
			push_wblock_to_free_q(ssd, wblock_id, FREE_TO_HEAD);
		}
		// Queue wblock for defrag if applicable.
		else if (inuse_sz < ssd->ns->defrag_lwm_size) {
			push_wblock_to_defrag_q(ssd, wblock_id);
		}
	}

	pthread_mutex_unlock(&wblock_state->LOCK);
}

ssd_write_buf *
swb_get(drv_ssd *ssd)
{
	ssd_write_buf *swb;

	if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
		if (! (swb = swb_create(ssd))) {
			return NULL;
		}

		swb->ssd = ssd;
		swb->wblock_id = STORAGE_INVALID_WBLOCK;
		swb->pos = 0;
		swb->rc = 0;
	}

	// Find a device block to write to.
	if (CF_QUEUE_OK != cf_queue_pop(ssd->free_wblock_q, &swb->wblock_id,
			CF_QUEUE_NOWAIT)) {
		cf_queue_push(ssd->swb_free_q, &swb);
		return NULL;
	}

	ssd_wblock_state* p_wblock_state =
			&ssd->alloc_table->wblock_state[swb->wblock_id];

	// Sanity checks.
	if (cf_atomic32_get(p_wblock_state->inuse_sz) != 0) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u inuse-size %u off free-q",
				ssd->name, swb->wblock_id,
				cf_atomic32_get(p_wblock_state->inuse_sz));
	}
	if (p_wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not null off free-q",
				ssd->name, swb->wblock_id);
	}
	if (p_wblock_state->state != WBLOCK_STATE_NONE) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not NONE off free-q",
				ssd->name, swb->wblock_id);
	}

	pthread_mutex_lock(&p_wblock_state->LOCK);

	swb_reserve(swb);
	p_wblock_state->swb = swb;

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	return swb;
}

//
// END - ssd_write_buf "swb" methods.
//------------------------------------------------


// Reduce wblock's used size, if result is 0 put it in the "free" pool, if it's
// below the defrag threshold put it in the defrag queue.
void
ssd_block_free(drv_ssd *ssd, uint64_t rblock_id, uint64_t n_rblocks, char *msg)
{
	if (n_rblocks == 0) {
		cf_warning(AS_DRV_SSD, "%s: %s: freeing 0 rblocks, rblock_id %lu",
				ssd->name, msg, rblock_id);
		return;
	}

	// Determine which wblock we're reducing used size in.
	uint64_t start_byte = RBLOCKS_TO_BYTES(rblock_id);
	uint64_t size = RBLOCKS_TO_BYTES(n_rblocks);
	uint32_t wblock_id = BYTES_TO_WBLOCK_ID(ssd, start_byte);
	uint32_t end_wblock_id = BYTES_TO_WBLOCK_ID(ssd, start_byte + size - 1);
	ssd_alloc_table *at = ssd->alloc_table;

	// Sanity-checks.
	if (! (start_byte >= ssd->header_size && wblock_id < at->n_wblocks &&
			wblock_id == end_wblock_id)) {
		cf_warning(AS_DRV_SSD, "%s: %s: invalid range to free, rblock_id %lu, n_rblocks %lu",
				ssd->name, msg, rblock_id, n_rblocks);
		return;
	}

	cf_atomic64_sub(&ssd->inuse_size, size);

	ssd_wblock_state *p_wblock_state = &at->wblock_state[wblock_id];

	pthread_mutex_lock(&p_wblock_state->LOCK);

	int64_t resulting_inuse_sz = cf_atomic32_sub(&p_wblock_state->inuse_sz,
			(int32_t)size);

	if (resulting_inuse_sz < 0 ||
			resulting_inuse_sz >= (int64_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "%s: %s: wblock %d %s, subtracted %d now %ld",
				ssd->name, msg, wblock_id,
				resulting_inuse_sz < 0 ? "over-freed" : "has crazy inuse_sz",
				(int32_t)size, resulting_inuse_sz);

		// TODO - really?
		cf_atomic32_set(&p_wblock_state->inuse_sz, ssd->write_block_size);
	}
	else if (! p_wblock_state->swb &&
			p_wblock_state->state != WBLOCK_STATE_DEFRAG) {
		// Free wblock if all three gating conditions hold.
		if (resulting_inuse_sz == 0) {
			push_wblock_to_free_q(ssd, wblock_id, FREE_TO_HEAD);
		}
		// Queue wblock for defrag if appropriate.
		else if (resulting_inuse_sz < ssd->ns->defrag_lwm_size) {
			push_wblock_to_defrag_q(ssd, wblock_id);
		}
	}

	pthread_mutex_unlock(&p_wblock_state->LOCK);
}


static void
log_bad_record(const char* ns_name, uint32_t n_bins, uint32_t block_bins,
		const drv_ssd_bin* ssd_bin, const char* tag)
{
	cf_info(AS_DRV_SSD, "untrustworthy data from disk [%s], ignoring record", tag);
	cf_info(AS_DRV_SSD, "   ns->name = %s", ns_name);
	cf_info(AS_DRV_SSD, "   bin %" PRIu32 " [of %" PRIu32 "]", (block_bins - n_bins) + 1, block_bins);

	if (ssd_bin) {
		cf_info(AS_DRV_SSD, "   ssd_bin->version = %" PRIu32, (uint32_t)ssd_bin->version);
		cf_info(AS_DRV_SSD, "   ssd_bin->offset = %" PRIu32, ssd_bin->offset);
		cf_info(AS_DRV_SSD, "   ssd_bin->len = %" PRIu32, ssd_bin->len);
		cf_info(AS_DRV_SSD, "   ssd_bin->next = %" PRIu32, ssd_bin->next);
	}
}


static bool
is_valid_record(const drv_ssd_block* block, const char* ns_name)
{
	uint8_t* block_head = (uint8_t*)block;
	uint64_t size = (uint64_t)(block->length + SIGNATURE_OFFSET);
	drv_ssd_bin* ssd_bin_end = (drv_ssd_bin*)(block_head + size - sizeof(drv_ssd_bin));
	drv_ssd_bin* ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);
	uint32_t n_bins = block->n_bins;

	if (n_bins == 0 || n_bins > BIN_NAMES_QUOTA) {
		log_bad_record(ns_name, n_bins, n_bins, NULL, "bins");
		return false;
	}

	while (n_bins > 0) {
		if (ssd_bin > ssd_bin_end) {
			log_bad_record(ns_name, n_bins, block->n_bins, NULL, "bin ptr");
			return false;
		}

		if (ssd_bin->version >= 16) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "version");
			return false;
		}

		uint64_t data_offset = (uint64_t)((uint8_t*)(ssd_bin + 1) - block_head);

		if ((uint64_t)ssd_bin->offset != data_offset) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "offset");
			return false;
		}

		uint64_t bin_end_offset = data_offset + (uint64_t)ssd_bin->len;

		if (bin_end_offset > size) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "length");
			return false;
		}

		if (n_bins > 1) {
			if ((uint64_t)ssd_bin->next != bin_end_offset) {
				log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "next ptr");
				return false;
			}

			ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
		}

		n_bins--;
	}

	return true;
}


int
ssd_record_defrag(drv_ssd *ssd, drv_ssd_block *block, uint64_t rblock_id,
		uint32_t n_rblocks, uint64_t filepos)
{
	as_namespace *ns = ssd->ns;
	as_partition_reservation rsv;
	as_partition_id pid = as_partition_getid(block->keyd);

	if (! is_valid_record(block, ns->name)) {
		return -3;
	}

	as_partition_reserve_migrate(ns, pid, &rsv, 0);
	cf_atomic_int_incr(&g_config.ssdr_tree_count);

	int rv;
	as_index_ref r_ref;
	r_ref.skip_lock = false;

	bool found = 0 == as_record_get(rsv.tree, &block->keyd, &r_ref, ns);
	bool is_subrec = false;

	if (ns->ldt_enabled && ! found) {
		found = 0 == as_record_get(rsv.sub_tree, &block->keyd, &r_ref, ns);
		is_subrec = true;
	}

	if (found) {
		as_index *r = r_ref.r;

		if (r->storage_key.ssd.file_id == ssd->file_id &&
				r->storage_key.ssd.rblock_id == rblock_id) {
			if (r->generation != block->generation) {
				cf_warning_digest(AS_DRV_SSD, &r->key, "device %s defrag: rblock_id %lu generation mismatch (%u:%u)%s ",
						ssd->name, rblock_id, r->generation, block->generation,
						is_subrec ? " subrec" : "");
			}

			if (r->storage_key.ssd.n_rblocks != n_rblocks) {
				cf_warning_digest(AS_DRV_SSD, &r->key, "device %s defrag: rblock_id %lu n_blocks mismatch (%u:%u)%s ",
						ssd->name, rblock_id, r->storage_key.ssd.n_rblocks,
						n_rblocks, is_subrec ? " subrec" : "");
			}

			as_storage_rd rd;
			as_storage_record_open(ns, r, &rd, &block->keyd);

			as_index_vinfo_mask_set(r,
					as_partition_vinfoset_mask_unpickle(rsv.p,
							block->data + block->vinfo_offset,
							block->vinfo_length),
					ns->allow_versions);

			rd.u.ssd.block = block;
			rd.have_device_block = true;

			rd.n_bins = as_bin_get_n_bins(r, &rd);
			as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

			rd.bins = as_bin_get_all(r, &rd, stack_bins);

			as_storage_record_get_key(&rd);

			size_t rec_props_data_size = as_storage_record_rec_props_size(&rd);
			uint8_t rec_props_data[rec_props_data_size];

			if (rec_props_data_size > 0) {
				as_storage_record_set_rec_props(&rd, rec_props_data);
			}

			rd.write_to_device = true;

			uint64_t start_ns = g_config.microbenchmarks ? cf_getns() : 0;

			as_storage_record_close(r, &rd);

			if (start_ns != 0) {
				histogram_insert_data_point(g_config.defrag_storage_close_hist, start_ns);
			}

			rv = 0; // record was in index tree and current - moved it
		}
		else {
			rv = -1; // record was in index tree - presumably was overwritten
		}

		as_record_done(&r_ref, ns);
	}
	else {
		rv = -2; // record was not in index tree - presumably was deleted
	}

	as_partition_release(&rsv);
	cf_atomic_int_decr(&g_config.ssdr_tree_count);

	return rv;
}


bool
ssd_is_full(drv_ssd *ssd, uint32_t wblock_id)
{
	if (cf_queue_sz(ssd->free_wblock_q) > DEFRAG_STARTUP_RESERVE) {
		return false;
	}

	ssd_wblock_state* p_wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	pthread_mutex_lock(&p_wblock_state->LOCK);

	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0) {
		// Lucky - wblock is empty, let ssd_defrag_wblock() free it.
		pthread_mutex_unlock(&p_wblock_state->LOCK);

		return false;
	}

	cf_warning(AS_DRV_SSD, "{%s}: defrag: drive %s totally full, re-queuing wblock %u",
			ssd->ns->name, ssd->name, wblock_id);

	// Not using push_wblock_to_defrag_q() - state is already DEFRAG, we
	// definitely have a queue, and it's better to push back to head.
	cf_queue_push_head(ssd->defrag_wblock_q, &wblock_id);

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	// If we got here, we used all our runtime reserve wblocks, but the wblocks
	// we defragged must still have non-zero inuse_sz. Must wait for those to
	// become free. Sleep prevents retries from overwhelming the log.
	sleep(1);

	return true;
}


int
ssd_defrag_wblock(drv_ssd *ssd, uint32_t wblock_id, uint8_t *read_buf)
{
	if (ssd_is_full(ssd, wblock_id)) {
		return 0;
	}

	int record_count = 0;
	int num_old_records = 0;
	int num_deleted_records = 0;
	int record_err_count = 0;

	ssd_wblock_state* p_wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0) {
		goto Finished;
	}

	int fd = ssd_fd_get(ssd);
	uint64_t file_offset = WBLOCK_ID_TO_BYTES(ssd, wblock_id);

	uint64_t start_ns = g_config.storage_benchmarks ? cf_getns() : 0;

	if (lseek(fd, (off_t)file_offset, SEEK_SET) != (off_t)file_offset) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: offset %lu: errno %d (%s)",
				ssd->name, file_offset, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Finished;
	}

	ssize_t rlen = read(fd, read_buf, ssd->write_block_size);

	if (rlen != (ssize_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd->name, rlen, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Finished;
	}

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_large_block_read, start_ns);
	}

	ssd_fd_put(ssd, fd);

	size_t wblock_offset = 0; // current offset within the wblock, in bytes

	while (wblock_offset < ssd->write_block_size &&
			cf_atomic32_get(p_wblock_state->inuse_sz) != 0) {
		drv_ssd_block *block = (drv_ssd_block*)&read_buf[wblock_offset];

		if (block->magic != SSD_BLOCK_MAGIC) {
			// First block must have magic.
			if (wblock_offset == 0) {
				cf_warning(AS_DRV_SSD, "BLOCK CORRUPTED: device %s has bad data on wblock %d",
						ssd->name, wblock_id);
				break;
			}

			// Later blocks may have no magic, just skip to next block.
			wblock_offset += RBLOCK_SIZE;
			continue;
		}

		// Note - if block->length is sane, we don't need to round up to a
		// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
		size_t next_wblock_offset = wblock_offset +
				BYTES_TO_RBLOCK_BYTES(block->length + SIGNATURE_OFFSET);

		if (next_wblock_offset > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "error: block extends over read size: foff %"PRIu64" boff %"PRIu64" blen %"PRIu64,
				file_offset, wblock_offset, (uint64_t)block->length);
			break;
		}

		if (ssd->use_signature && block->sig) {
			cf_signature sig;

			cf_signature_compute(((uint8_t*)block) + SIGNATURE_OFFSET,
					block->length, &sig);

			if (sig != block->sig) {
				wblock_offset += RBLOCK_SIZE;
				ssd->record_add_sigfail_counter++;
				continue;
			}
		}

		// Found a good record, move it if it's current.
		int rv = ssd_record_defrag(ssd, block,
				BYTES_TO_RBLOCKS(file_offset + wblock_offset),
				(uint32_t)BYTES_TO_RBLOCKS(next_wblock_offset - wblock_offset),
				file_offset + wblock_offset);

		if (rv == 0) {
			record_count++;
		}
		else if (rv == -1) {
			num_old_records++;
		}
		else if (rv == -2) {
			num_deleted_records++;
		}
		else if (rv == -3) {
			cf_atomic_int_incr(&g_config.err_storage_defrag_corrupt_record);
			record_err_count++;
		}

		wblock_offset = next_wblock_offset;
	}

Finished:

	// Note - usually wblock's inuse_sz is 0 here, but may legitimately be non-0
	// e.g. if a dropped partition's tree is not done purging. In this case, we
	// may have found deleted records in the wblock whose used-size contribution
	// has not yet been subtracted.

	if (record_err_count > 0) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u defragged, final in-use-sz %d records (%d:%d:%d:%d)",
				ssd->name, wblock_id, cf_atomic32_get(p_wblock_state->inuse_sz),
				record_count, num_old_records, num_deleted_records,
				record_err_count);
	}
	else {
		cf_detail(AS_DRV_SSD, "device %s: wblock-id %u defragged, final in-use-sz %d records (%d:%d:%d)",
				ssd->name, wblock_id, cf_atomic32_get(p_wblock_state->inuse_sz),
				record_count, num_old_records, num_deleted_records);
	}

	// Sanity checks.
	if (p_wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not null while defragging",
				ssd->name, wblock_id);
	}
	if (p_wblock_state->state != WBLOCK_STATE_DEFRAG) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not DEFRAG while defragging",
				ssd->name, wblock_id);
	}

	pthread_mutex_lock(&p_wblock_state->LOCK);

	p_wblock_state->state = WBLOCK_STATE_NONE;

	// Free the wblock if it's empty.
	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0 &&
			! p_wblock_state->swb) {
		push_wblock_to_free_q(ssd, wblock_id, FREE_TO_HEAD);
	}

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	return record_count;
}


// Thread "run" function to service a device's defrag queue.
void*
run_defrag(void *pv_data)
{
	drv_ssd *ssd = (drv_ssd*)pv_data;
	uint32_t wblock_id;
	uint8_t *read_buf = cf_valloc(ssd->write_block_size);

	if (! read_buf) {
		cf_crash(AS_DRV_SSD, "device %s: defrag valloc failed", ssd->name);
	}

	while (true) {
		uint32_t q_min = ssd->ns->storage_defrag_queue_min;

		if (q_min != 0) {
			if (cf_queue_sz(ssd->defrag_wblock_q) > q_min) {
				if (CF_QUEUE_OK !=
						cf_queue_pop(ssd->defrag_wblock_q, &wblock_id,
								CF_QUEUE_NOWAIT)) {
					// Should never get here!
					break;
				}
			}
			else {
				usleep(1000 * 50);
				continue;
			}
		}
		else {
			if (CF_QUEUE_OK !=
					cf_queue_pop(ssd->defrag_wblock_q, &wblock_id,
							CF_QUEUE_FOREVER)) {
				// Should never get here!
				break;
			}
		}

		ssd_defrag_wblock(ssd, wblock_id, read_buf);

		uint32_t sleep_us = ssd->ns->storage_defrag_sleep;

		if (sleep_us != 0) {
			usleep(sleep_us);
		}
	}

	// Although we ever expect to get here...
	cf_free(read_buf);
	cf_warning(AS_DRV_SSD, "device %s: quit defrag - queue error", ssd->name);

	return NULL;
}


void
ssd_start_defrag_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "ns %s starting defrag threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		if (pthread_create(&ssd->defrag_thread, NULL, run_defrag,
				(void*)ssd) != 0) {
			cf_crash(AS_DRV_SSD, "%s defrag thread failed", ssd->name);
		}
	}
}


//------------------------------------------------
// defrag_pen class.
//

#define DEFRAG_PEN_INIT_CAPACITY (8 * 1024)

typedef struct defrag_pen_s {
	uint32_t n_ids;
	uint32_t capacity;
	uint32_t *ids;
	uint32_t stack_ids[DEFRAG_PEN_INIT_CAPACITY];
} defrag_pen;

static void
defrag_pen_init(defrag_pen *pen)
{
	pen->n_ids = 0;
	pen->capacity = DEFRAG_PEN_INIT_CAPACITY;
	pen->ids = pen->stack_ids;
}

static void
defrag_pen_destroy(defrag_pen *pen)
{
	if (pen->ids != pen->stack_ids) {
		cf_free(pen->ids);
	}
}

static void
defrag_pen_add(defrag_pen *pen, uint32_t wblock_id)
{
	if (pen->n_ids == pen->capacity) {
		if (pen->capacity == DEFRAG_PEN_INIT_CAPACITY) {
			pen->capacity <<= 2;
			pen->ids = cf_malloc(pen->capacity * sizeof(uint32_t));
			memcpy(pen->ids, pen->stack_ids, sizeof(pen->stack_ids));
		}
		else {
			pen->capacity <<= 1;
			pen->ids = cf_realloc(pen->ids, pen->capacity * sizeof(uint32_t));
		}
	}

	pen->ids[pen->n_ids++] = wblock_id;
}

static void
defrag_pen_transfer(defrag_pen *pen, drv_ssd *ssd)
{
	// For speed, "customize" instead of using push_wblock_to_defrag_q()...
	for (uint32_t i = 0; i < pen->n_ids; i++) {
		uint32_t wblock_id = pen->ids[i];

		ssd->alloc_table->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(ssd->defrag_wblock_q, &wblock_id);
	}
}

static void
defrag_pens_dump(defrag_pen pens[], uint32_t n_pens, const char* ssd_name)
{
	char buf[2048];
	uint32_t n = 0;
	int pos = sprintf(buf, "%u", pens[n++].n_ids);

	while (n < n_pens) {
		pos += sprintf(buf + pos, ",%u", pens[n++].n_ids);
	}

	cf_info(AS_DRV_SSD, "%s init defrag profile: %s", ssd_name, buf);
}

//
// END - defrag_pen class.
//------------------------------------------------


// Thread "run" function to create and load a device's (wblock) free & defrag
// queues at startup. Sorts defrag-eligible wblocks so the most depleted ones
// are at the head of the defrag queue.
void*
run_load_queues(void *pv_data)
{
	drv_ssd *ssd = (drv_ssd*)pv_data;

	// TODO - would be nice to have a queue create of specified capacity.
	if (! (ssd->free_wblock_q = cf_queue_create(sizeof(uint32_t), true))) {
		cf_crash(AS_DRV_SSD, "%s free wblock queue create failed", ssd->name);
	}

	if (! (ssd->defrag_wblock_q = cf_queue_create(sizeof(uint32_t), true))) {
		cf_crash(AS_DRV_SSD, "%s defrag queue create failed", ssd->name);
	}

	as_namespace *ns = ssd->ns;
	uint32_t lwm_pct = ns->storage_defrag_lwm_pct;
	uint32_t lwm_size = ns->defrag_lwm_size;
	defrag_pen pens[lwm_pct];

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_init(&pens[n]);
	}

	ssd_alloc_table* at = ssd->alloc_table;
	uint32_t first_id = BYTES_TO_WBLOCK_ID(ssd, ssd->header_size);
	uint32_t last_id = at->n_wblocks;

	for (uint32_t wblock_id = first_id; wblock_id < last_id; wblock_id++) {
		uint32_t inuse_sz = at->wblock_state[wblock_id].inuse_sz;

		if (inuse_sz == 0) {
			// Faster than using push_wblock_to_free_q() here...
			cf_queue_push(ssd->free_wblock_q, &wblock_id);
		}
		else if (inuse_sz < lwm_size) {
			defrag_pen_add(&pens[(inuse_sz * lwm_pct) / lwm_size], wblock_id);
		}
	}

	defrag_pens_dump(pens, lwm_pct, ssd->name);

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_transfer(&pens[n], ssd);
		defrag_pen_destroy(&pens[n]);
	}

	ssd->defrag_wblock_counter = (uint64_t)cf_queue_sz(ssd->defrag_wblock_q);

	return NULL;
}


void
ssd_load_wblock_queues(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "ns %s loading free & defrag queues", ssds->ns->name);

	// Split this task across multiple threads.
	pthread_t q_load_threads[ssds->n_ssds];

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		if (pthread_create(&q_load_threads[i], NULL, run_load_queues,
				(void*)ssd) != 0) {
			cf_crash(AS_DRV_SSD, "%s load queues thread failed", ssd->name);
		}
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		pthread_join(q_load_threads[i], NULL);
	}
	// Now we're single-threaded again.

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		cf_info(AS_DRV_SSD, "%s init wblock free-q %d, defrag-q %d", ssd->name,
				cf_queue_sz(ssd->free_wblock_q),
				cf_queue_sz(ssd->defrag_wblock_q));
	}
}


void
ssd_wblock_init(drv_ssd *ssd)
{
	uint32_t n_wblocks = ssd->file_size / ssd->write_block_size;

	cf_info(AS_DRV_SSD, "%s has %u wblocks of size %u", ssd->name, n_wblocks,
			ssd->write_block_size);

	ssd_alloc_table *at = cf_malloc(sizeof(ssd_alloc_table) + (n_wblocks * sizeof(ssd_wblock_state)));

	at->n_wblocks = n_wblocks;

	// Device header wblocks' inuse_sz will (also) be 0 but that doesn't matter.
	for (uint32_t i = 0; i < n_wblocks; i++) {
		pthread_mutex_init(&at->wblock_state[i].LOCK, 0);
		at->wblock_state[i].state = WBLOCK_STATE_NONE;
		cf_atomic32_set(&at->wblock_state[i].inuse_sz, 0);
		at->wblock_state[i].swb = 0;
	}

	ssd->alloc_table = at;
}


static int
ssd_populate_bin(as_bin *bin, drv_ssd_bin *ssd_bin, uint8_t *block_head,
		bool single_bin, bool allocate_memory)
{
	as_particle *p = (as_particle*)(block_head + ssd_bin->offset);

	if (p->metadata == AS_PARTICLE_TYPE_INTEGER) {
		if (ssd_bin->len != 10) {
			cf_warning(AS_DRV_SSD, "Possible memory corruption: integer value overflows: expected 10 bytes got %d bytes",
					ssd_bin->len);
		}

		as_particle_int_on_device *pi = (as_particle_int_on_device*)p;
		if (pi->len != 8) {
			return -1;
		}

		// Destroy old particle.
		as_particle_frombuf(bin, AS_PARTICLE_TYPE_NULL, 0, 0, 0, true);

		// Copy the integer particle in-place to the bin.
		bin->ivalue = pi->i;
		as_bin_state_set(bin, AS_BIN_STATE_INUSE_INTEGER);
	}
	else {
		if (allocate_memory) {
			uint32_t base_size = as_particle_get_base_size(p->metadata);

			as_particle_frombuf(bin, p->metadata, (uint8_t*)p + base_size,
					ssd_bin->len - base_size, 0, true);
		}
		else {
			bin->particle = p;

			if (as_particle_type_hidden(p->metadata)) {
				as_bin_state_set(bin, AS_BIN_STATE_INUSE_HIDDEN);
			}
			else {
				as_bin_state_set(bin, AS_BIN_STATE_INUSE_OTHER);
			}
		}
	}

	as_bin_set_version(bin, ssd_bin->version, single_bin);

	return 0;
}


//==========================================================
// Storage API implementation: reading records.
//

inline uint16_t
as_storage_record_get_n_bins_ssd(as_storage_rd *rd)
{
	return rd->u.ssd.block ? rd->u.ssd.block->n_bins : 0;
}


int
as_storage_record_read_ssd(as_storage_rd *rd)
{
	as_record *r = rd->r;

	if (STORAGE_RBLOCK_IS_INVALID(r->storage_key.ssd.rblock_id)) {
		cf_warning_digest(AS_DRV_SSD, &rd->keyd, "{%s} read_ssd: invalid rblock_id ",
				rd->ns->name);
		return -1;
	}

	uint64_t record_offset = RBLOCKS_TO_BYTES(r->storage_key.ssd.rblock_id);
	uint64_t record_size = RBLOCKS_TO_BYTES(r->storage_key.ssd.n_rblocks);

	uint8_t *read_buf = NULL;
	drv_ssd_block *block = NULL;

	drv_ssd *ssd = rd->u.ssd.ssd;
	ssd_write_buf *swb = 0;
	uint32_t wblock = RBLOCK_ID_TO_WBLOCK_ID(ssd, r->storage_key.ssd.rblock_id);

	swb_check_and_reserve(&ssd->alloc_table->wblock_state[wblock], &swb);

	if (swb) {
		// Data is in write buffer, so read it from there.
		cf_atomic32_incr(&rd->ns->n_reads_from_cache);

		read_buf = cf_malloc(record_size);

		if (! read_buf) {
			return -1;
		}

		block = (drv_ssd_block*)read_buf;

		int swb_offset = record_offset - WBLOCK_ID_TO_BYTES(ssd, wblock);
		memcpy(read_buf, swb->buf + swb_offset, record_size);
		swb_release(swb);
	}
	else {
		// Normal case - data is read from device.
		cf_atomic32_incr(&rd->ns->n_reads_from_device);

		uint64_t record_end_offset = record_offset + record_size;
		uint64_t read_offset = BYTES_DOWN_TO_IO_MIN(ssd, record_offset);
		uint64_t read_end_offset = BYTES_UP_TO_IO_MIN(ssd, record_end_offset);
		size_t read_size = read_end_offset - read_offset;
		uint64_t record_buf_indent = record_offset - read_offset;

		read_buf = cf_valloc(read_size);

		if (! read_buf) {
			return -1;
		}

		int fd = ssd_fd_get(ssd);

		uint64_t start_ns = g_config.storage_benchmarks ? cf_getns() : 0;

		if (lseek(fd, (off_t)read_offset, SEEK_SET) != (off_t)read_offset) {
			cf_warning(AS_DRV_SSD, "%s: seek failed: offset %lu: errno %d (%s)",
					ssd->name, read_offset, errno, cf_strerror(errno));
			cf_free(read_buf);
			close(fd);
			return -1;
		}

		ssize_t rv = read(fd, read_buf, read_size);

		if (rv != (ssize_t)read_size) {
			cf_warning(AS_DRV_SSD, "%s: read failed (%ld): size %lu: errno %d (%s)",
					ssd->name, rv, read_size, errno, cf_strerror(errno));
			cf_free(read_buf);
			close(fd);
			return -1;
		}

		if (start_ns != 0) {
			histogram_insert_data_point(ssd->hist_read, start_ns);
		}

		ssd_fd_put(ssd, fd);

		block = (drv_ssd_block*)(read_buf + record_buf_indent);

		// Sanity checks.
		if (block->magic != SSD_BLOCK_MAGIC) {
			cf_warning(AS_DRV_SSD, "read: bad block magic offset %"PRIu64,
					read_offset);
			cf_free(read_buf);
			return -1;
		}
		if (0 != cf_digest_compare(&block->keyd, &rd->keyd)) {
			cf_warning(AS_DRV_SSD, "read: read wrong key: expecting %"PRIx64" got %"PRIx64,
				*(uint64_t*)&rd->keyd, *(uint64_t*)&block->keyd);
			cf_free(read_buf);
			return -1;
		}
	}

	rd->u.ssd.block = block;
	rd->u.ssd.must_free_block = read_buf;
	rd->have_device_block = true;

	return 0;
}


int
as_storage_particle_read_all_ssd(as_storage_rd *rd)
{
	// If the record hasn't been read, read it.
	if (rd->u.ssd.block == 0) {
		if (0 != as_storage_record_read_ssd(rd)) {
			cf_info(AS_DRV_SSD, "read_all: failed as_storage_record_read_ssd()");
			return -1;
		}
	}

	drv_ssd_block *block = rd->u.ssd.block;
	uint8_t *block_head = (uint8_t*)rd->u.ssd.block;

	drv_ssd_bin *ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);

	for (uint16_t i = 0; i < block->n_bins; i++) {
		as_bin_set_version(&rd->bins[i], ssd_bin->version, rd->ns->single_bin);
		as_bin_set_id_from_name(rd->ns, &rd->bins[i], ssd_bin->name);

		int rv = ssd_populate_bin(&rd->bins[i], ssd_bin, block_head,
				rd->ns->single_bin, false);

		if (0 != rv) {
			return rv;
		}

		ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
	}

	return 0;
}


// TODO: Eventually conflate this with above method.
int
as_storage_particle_read_and_size_all_ssd(as_storage_rd *rd)
{
	if (! rd->u.ssd.block) {
		if (0 != as_storage_record_read_ssd(rd)) {
			cf_warning(AS_DRV_SSD, "read_and_size_all: failed as_storage_record_read_ssd()");
			return -1;
		}
	}

	drv_ssd_block *block = rd->u.ssd.block;
	uint8_t *block_head = (uint8_t*)rd->u.ssd.block;

	uint8_t *bins_start = block->data + block->bins_offset;
	drv_ssd_bin *ssd_bin = (drv_ssd_bin*)bins_start;

	for (uint16_t i = 0; i < block->n_bins; i++) {
		as_bin_set_version(&rd->bins[i], ssd_bin->version, rd->ns->single_bin);
		as_bin_set_id_from_name(rd->ns, &rd->bins[i], ssd_bin->name);

		int rv = ssd_populate_bin(&rd->bins[i], ssd_bin, block_head,
				rd->ns->single_bin, false);

		if (0 != rv) {
			return rv;
		}

		ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
	}

	rd->n_bins_to_write = block->n_bins;
	rd->particles_flat_size = ((uint8_t*)ssd_bin - bins_start) -
			(block->n_bins * sizeof(drv_ssd_bin));

	return 0;
}


bool
as_storage_record_get_key_ssd(as_storage_rd *rd)
{
	if (! rd->u.ssd.block) {
		if (0 != as_storage_record_read_ssd(rd)) {
			cf_warning(AS_DRV_SSD, "get_key: failed as_storage_record_read_ssd()");
			return false;
		}
	}

	drv_ssd_block *block = rd->u.ssd.block;
	uint32_t properties_offset = block->vinfo_offset + block->vinfo_length;
	as_rec_props props;

	props.size = block->bins_offset - properties_offset;

	if (props.size == 0) {
		return false;
	}

	props.p_data = block->data + properties_offset;

	return as_rec_props_get_value(&props, CL_REC_PROPS_FIELD_KEY,
			&rd->key_size, &rd->key) == 0;
}


//==========================================================
// Record writing utilities.
//

//------------------------------------------------
// Write-smoother.
//

typedef struct as_write_smoothing_arg_s {
	drv_ssd *ssd;
	as_namespace *ns;

	int budget_usecs_for_this_second;

	// lbw means large block writes
	cf_queue *lbw_to_process_per_second_q;
	int lbw_to_process_per_second_sum;
	uint64_t last_lbw_request_cnt; // last value of ssd_write_buf_counter

	int lbw_to_process_per_second_recent_max; // max from last 5 seconds
	int lbw_catchup_calculation_cnt; // counter used to help calculate recent max

	uint32_t last_smoothing_period;

	uint64_t last_time_ms;
	uint64_t last_second;
} as_write_smoothing_arg;

int
as_write_smoothing_arg_initialize(as_write_smoothing_arg *awsa, drv_ssd *ssd)
{
	awsa->ssd = ssd;
	awsa->ns = ssd->ns;

	awsa->budget_usecs_for_this_second = 0;

	// lbw means large block writes
	awsa->lbw_to_process_per_second_q = cf_queue_create(sizeof(int), false);
	awsa->lbw_to_process_per_second_sum = 0;
	awsa->last_lbw_request_cnt = 0;

	awsa->lbw_to_process_per_second_recent_max = 0;
	awsa->lbw_catchup_calculation_cnt = 0;

	awsa->last_smoothing_period = awsa->ns->storage_write_smoothing_period;

	awsa->last_time_ms = cf_getms();
	awsa->last_second = awsa->last_time_ms / 1000;

	return 0;
}

void
as_write_smoothing_arg_destroy(as_write_smoothing_arg *awsa)
{
	cf_queue_destroy(awsa->lbw_to_process_per_second_q);
}

void
as_write_smoothing_arg_reset(as_write_smoothing_arg *awsa)
{
	cf_queue_delete_all(awsa->lbw_to_process_per_second_q);
	awsa->lbw_to_process_per_second_sum = 0;

	awsa->budget_usecs_for_this_second = 0;
}

// This function takes a queue (optionally with a max queue size), a pointer to
// a sum of the elements in the q, and the new data point to push on the q.
// This will "expire" the oldest q element if max_q_sz is hit, then will push
// the new element and update the sum.
//
// This is very useful for updating a rolling N second average.
void
as_write_smoothing_push_to_queue_and_update_queue_sum(cf_queue *q, int max_q_sz,
		int *p_sum, int new_data)
{
	if (max_q_sz != 0 && cf_queue_sz(q) == max_q_sz) {
		int old_data;

		if (0 != cf_queue_pop(q, &old_data, CF_QUEUE_NOWAIT)) {
			// this should never happen
			cf_assert(false, AS_DRV_SSD, CF_CRITICAL,
				"could not pop element off queue");
		}

		*p_sum -= old_data;
	}

	*p_sum += new_data;

	cf_queue_push(q, &new_data);
}

int
as_write_smoothing_get_lbw_to_process_per_second_recent_max_reduce_fn(void *buf,
		void *udata)
{
	as_write_smoothing_arg *awsa = (as_write_smoothing_arg*)udata;

	if (awsa->lbw_catchup_calculation_cnt == NUM_ELEMS_IN_LBW_CATCH_UP_CALC) {
		return -1;
	}

	int *p_lbw_to_process_per_second = (int*)buf;

	if (*p_lbw_to_process_per_second >
			awsa->lbw_to_process_per_second_recent_max) {
		awsa->lbw_to_process_per_second_recent_max =
			*p_lbw_to_process_per_second;
	}

	awsa->lbw_catchup_calculation_cnt++;

	return 0;
}

// This function dictates when to sleep after large block writes.
void
as_write_smoothing_fn(as_write_smoothing_arg *awsa)
{
	// Write throttling algorithm - look at smoothing_period seconds back of
	// data and slow writes accordingly.

	uint32_t smoothing_period = awsa->ns->storage_write_smoothing_period;

	if (smoothing_period != awsa->last_smoothing_period) {
		// We've changed the smoothing period, so shorten the
		// lbw_to_process_per_second_q if we need to.
		while (cf_queue_sz(awsa->lbw_to_process_per_second_q) >
				smoothing_period) {
			int old_lbw_to_process_per_second_data;

			if (0 != cf_queue_pop(awsa->lbw_to_process_per_second_q,
					&old_lbw_to_process_per_second_data, CF_QUEUE_NOWAIT)) {
				cf_assert(false, AS_DRV_SSD, CF_CRITICAL,
					"could not pop smoothing_data element off queue");
			}

			awsa->lbw_to_process_per_second_sum -=
				old_lbw_to_process_per_second_data;
		}

		if (awsa->last_smoothing_period == 0) {
			awsa->last_lbw_request_cnt =
				cf_atomic_int_get(awsa->ssd->ssd_write_buf_counter);
		}

		awsa->last_smoothing_period = smoothing_period;
	}

	if (smoothing_period == 0) {
		return;
	}

	uint64_t cur_time_ms = cf_getms();
	uint64_t cur_second = cur_time_ms / 1000;

	if ((cur_time_ms - awsa->last_time_ms) > 1000) {
		// It's been more than 1000 ms since our last write, so let's reset our
		// rolling averages.
		as_write_smoothing_arg_reset(awsa);
	}

	// Every *new* second, update the state of the world.
	if (cur_second != awsa->last_second) {
		uint64_t lbw_request_cnt =
			cf_atomic_int_get(awsa->ssd->ssd_write_buf_counter);

		int lbw_requests_this_second = (int)
				(lbw_request_cnt - awsa->last_lbw_request_cnt);

		awsa->last_lbw_request_cnt = lbw_request_cnt;

		if (lbw_requests_this_second == 0) {
			// We received no new lbw requests this second, so let's reset our
			// rolling averages.
			as_write_smoothing_arg_reset(awsa);
		}
		else {
			as_write_smoothing_push_to_queue_and_update_queue_sum(
					awsa->lbw_to_process_per_second_q, smoothing_period,
					&(awsa->lbw_to_process_per_second_sum),
					lbw_requests_this_second);

			// Calculate lbw process per second *recent* max (last 5s) used when
			// write q 80%+ full.
			awsa->lbw_to_process_per_second_recent_max = 0;
			awsa->lbw_catchup_calculation_cnt = 0;

			cf_queue_reduce_reverse(awsa->lbw_to_process_per_second_q,
					as_write_smoothing_get_lbw_to_process_per_second_recent_max_reduce_fn,
					awsa);

			int lbw_to_process_per_second_q_sz =
					cf_queue_sz(awsa->lbw_to_process_per_second_q);

			if (lbw_to_process_per_second_q_sz >=
					MIN_SECONDS_OF_WRITE_SMOOTHING_DATA) {
				int swb_write_q_sz = cf_queue_sz(awsa->ssd->swb_write_q);

				int est_lbw_to_do =
					((awsa->lbw_to_process_per_second_sum * smoothing_period) /
						lbw_to_process_per_second_q_sz) + swb_write_q_sz;

				if (est_lbw_to_do > 0) {
					awsa->budget_usecs_for_this_second =
						(smoothing_period * 1000 * 1000) *
							awsa->ns->storage_write_threads / est_lbw_to_do;
				}

				cf_detail(AS_DRV_SSD, "budget usecs = %d, lbw_to_process_per_second_q_sz = %d, lbw_to_process_per_second_sum = %d, write_q_depth = %d",
						awsa->budget_usecs_for_this_second,
						lbw_to_process_per_second_q_sz,
						awsa->lbw_to_process_per_second_sum, swb_write_q_sz);
			}
			else {
				// Not enough seconds of data to set a budget.
				awsa->budget_usecs_for_this_second = 0;
			}
		}
	}

	int swb_write_q_sz = cf_queue_sz(awsa->ssd->swb_write_q);
	int max_write_q_sz = awsa->ns->storage_max_write_q;

	// Set to 0 so I don't have to set to 0 in a bunch of special cases.
	int budget_usecs = 0;

	if (awsa->budget_usecs_for_this_second > 0) {
		// If write q is 90%+ full, don't smooth this transaction.
		if ((10 * swb_write_q_sz) < (9 * max_write_q_sz)) {
			// If write q is 80%+ full, calculate new budget, which tries to
			// offset the acceleration of write q growth.
			if ((10 * swb_write_q_sz) > (8 * max_write_q_sz)) {
				int est_lbw_to_do =
					(awsa->lbw_to_process_per_second_recent_max *
						smoothing_period) + swb_write_q_sz;

				if (est_lbw_to_do > 0) {
					budget_usecs = (smoothing_period * 1000 * 1000) *
						awsa->ns->storage_write_threads / est_lbw_to_do;
				}

				if (awsa->budget_usecs_for_this_second < budget_usecs) {
					budget_usecs = awsa->budget_usecs_for_this_second;
				}

				cf_detail(AS_DRV_SSD, "write_q > 80 pct | budget_usecs = %d, est_lbw_to_do = %d",
					budget_usecs, est_lbw_to_do);
			}
			else {
				// Standard case.
				budget_usecs = awsa->budget_usecs_for_this_second;
			}
		}
	}

	cf_detail(AS_DRV_SSD, "budget_usecs = %d, trans time = %"PRIu64", sleep_usecs = %d",
			budget_usecs, cur_time_ms - awsa->last_time_ms,
			budget_usecs - (((int)(cur_time_ms - awsa->last_time_ms)) * 1000));

	if (budget_usecs > 0) {
		// Don't budget for less than 1 lbw/sec.
		if (budget_usecs > 1000000) {
			budget_usecs = 1000000;
		}

		int sleep_usecs = budget_usecs -
			(((int)(cur_time_ms - awsa->last_time_ms)) * 1000);

		if (sleep_usecs > 0) {
			usleep(sleep_usecs);
			cur_time_ms += (sleep_usecs / 1000); // estimate new cur_time_ms
		}
	}

	awsa->last_time_ms = cur_time_ms;
	awsa->last_second = cur_second;
}

//
// END - Write-smoother.
//------------------------------------------------


void
ssd_flush_swb(drv_ssd *ssd, ssd_write_buf *swb)
{
	int fd = ssd_fd_get(ssd);
	off_t write_offset = (off_t)WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id);

	uint64_t start_ns = g_config.storage_benchmarks ? cf_getns() : 0;

	if (lseek(fd, write_offset, SEEK_SET) != write_offset) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: offset %ld: errno %d (%s)",
				ssd->name, write_offset, errno, cf_strerror(errno));
	}

	ssize_t rv_s = write(fd, swb->buf, ssd->write_block_size);

	if (rv_s != (ssize_t)ssd->write_block_size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_write, start_ns);
	}

	ssd_fd_put(ssd, fd);

	if (! ssd->shadow_name) {
		return;
	}

	fd = ssd_shadow_fd_get(ssd);

	start_ns = g_config.storage_benchmarks ? cf_getns() : 0;

	if (lseek(fd, write_offset, SEEK_SET) != write_offset) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: offset %ld: errno %d (%s)",
				ssd->shadow_name, write_offset, errno, cf_strerror(errno));
	}

	rv_s = write(fd, swb->buf, ssd->write_block_size);

	if (rv_s != (ssize_t)ssd->write_block_size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_shadow_write, start_ns);
	}

	ssd_shadow_fd_put(ssd, fd);
}


// Thread "run" function that flushes write buffers to device.
void *
ssd_write_worker(void *arg)
{
	drv_ssd *ssd = (drv_ssd*)arg;

	as_write_smoothing_arg awsa;
	as_write_smoothing_arg_initialize(&awsa, ssd);

	while (ssd->running) {
		ssd_write_buf *swb;

		if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_write_q, &swb, 100)) {
			continue;
		}

		ssd_wblock_state* p_wblock_state =
				&ssd->alloc_table->wblock_state[swb->wblock_id];

		// Sanity checks.
		if (p_wblock_state->swb != swb) {
			cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not consistent while writing",
					ssd->name, swb->wblock_id);
		}
		if (p_wblock_state->state != WBLOCK_STATE_NONE) {
			cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not NONE while writing",
					ssd->name, swb->wblock_id);
		}

		// Flush to the device.
		ssd_flush_swb(ssd, swb);

		if (cf_atomic32_get(ssd->ns->storage_post_write_queue) == 0) {
			swb_dereference_and_release(ssd, swb->wblock_id, swb);
		}
		else {
			// Transfer swb to post-write queue.
			cf_queue_push(ssd->post_write_q, &swb);
		}

		if (ssd->post_write_q) {
			// Release post-write queue swbs if we're over the limit.
			while ((uint32_t)cf_queue_sz(ssd->post_write_q) >
					cf_atomic32_get(ssd->ns->storage_post_write_queue)) {
				ssd_write_buf* cached_swb;

				if (CF_QUEUE_OK != cf_queue_pop(ssd->post_write_q, &cached_swb,
						CF_QUEUE_NOWAIT)) {
					// Should never happen.
					cf_warning(AS_DRV_SSD, "device %s: post-write queue pop failed",
							ssd->name);
					break;
				}

				swb_dereference_and_release(ssd, cached_swb->wblock_id,
						cached_swb);
			}
		}

		as_write_smoothing_fn(&awsa);
	} // infinite event loop waiting for block to write

	as_write_smoothing_arg_destroy(&awsa);

	return NULL;
}


void
ssd_start_write_worker_threads(drv_ssds *ssds)
{
	if (ssds->ns->storage_write_threads > MAX_SSD_THREADS) {
		cf_warning(AS_DRV_SSD, "configured number of write threads %s greater than max, using %d instead",
				ssds->ns->storage_write_threads, MAX_SSD_THREADS);
		ssds->ns->storage_write_threads = MAX_SSD_THREADS;
	}

	cf_info(AS_DRV_SSD, "ns %s starting write worker threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		for (uint32_t j = 0; j < ssds->ns->storage_write_threads; j++) {
			pthread_create(&ssd->write_worker_thread[j], 0, ssd_write_worker,
					(void*)ssd);
		}
	}
}


uint32_t
as_storage_record_overhead_size(as_storage_rd *rd)
{
	size_t size = 0;

	// Start with pickled size of vinfo, if any.
	if (rd->ns->allow_versions &&
			0 != as_partition_vinfoset_mask_pickle_getsz(
					as_index_vinfo_mask_get(rd->r, true), &size)) {
		cf_crash(AS_DRV_SSD, "unable to pickle vinfoset");
	}

	// Add size of record header struct.
	size += sizeof(drv_ssd_block);

	// Add size of any record properties.
	if (rd->rec_props.p_data) {
		size += rd->rec_props.size;
	}

	return (uint32_t)size;
}


uint32_t
as_storage_record_size(as_storage_rd *rd)
{
	// Start with the record storage overhead, including vinfo and rec-props.
	uint32_t write_size = as_storage_record_overhead_size(rd);

	if (! rd->bins) {
		// Should never get here.
		cf_warning(AS_DRV_SSD, "cannot calculate write size, no bins pointer");
		return 0;
	}

	// Add the bins' sizes, including bin overhead.
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *bin = &rd->bins[i];

		if (! as_bin_inuse(bin)) {
			break;
		}

		size_t particle_flat_sz;
		int rv = as_particle_get_flat_size(bin, &particle_flat_sz);

		if (rv != 0) {
			// Should never get here.
			cf_warning(AS_DRV_SSD, "can't get particle flat size for bin %s, rv %d",
					as_bin_get_name_from_id(rd->ns, bin->id), rv);
			return 0;
		}

		// TODO: could factor out sizeof(drv_ssd_bin) and multiply by i, but
		// for now let's favor the low bin-count case and leave it this way.
		write_size += sizeof(drv_ssd_bin) + (uint32_t)particle_flat_sz;
	}

	return write_size;
}


uint32_t
ssd_write_calculate_size(as_record *r, as_storage_rd *rd)
{
	// Note - this function is the only place where rounding size (up to a
	// multiple of RBLOCK_SIZE) is really necessary.

	// TODO - use when we're confident duplicate bins were the only issue:
#ifdef HANDLE_DUPLICATE_BINS
	// If we already know the flat size, just need to round it.
	if (rd->flat_size != 0) {
		return BYTES_TO_RBLOCK_BYTES(rd->flat_size);
	}
#endif

	uint32_t write_size = as_storage_record_size(rd);

	// TODO - remove when we're confident duplicate bins were the only issue:
	if (rd->flat_size != 0 && rd->flat_size != write_size) {
		cf_warning(AS_DRV_SSD, "flat_size %u != write_size %u - duplicate bins?",
				rd->flat_size, write_size);
	}

	return BYTES_TO_RBLOCK_BYTES(write_size);
}


int
ssd_write_bins(as_record *r, as_storage_rd *rd)
{
	uint32_t write_size = ssd_write_calculate_size(r, rd);
	drv_ssd *ssd = rd->u.ssd.ssd;

	if (write_size == 0 || write_size > ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "write: rejecting %"PRIx64" write size: %u",
				*(uint64_t*)&rd->keyd, write_size);
		return -1;
	}

	// Reserve the portion of the current swb where this record will be written.
	pthread_mutex_lock(&ssd->LOCK);

	ssd_write_buf *swb = ssd->current_swb;

	if (! swb) {
		swb = swb_get(ssd);
		ssd->current_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "write bins: couldn't get swb");
			pthread_mutex_unlock(&ssd->LOCK);
			return -1;
		}
	}

	// Check if there's enough space in current buffer - if not, free and zero
	// any remaining unused space, enqueue it to be flushed to device, and grab
	// a new buffer.
	if (write_size > ssd->write_block_size - swb->pos) {
		if (ssd->write_block_size != swb->pos) {
			// Clean the end of the buffer before pushing to write queue.
			memset(&swb->buf[swb->pos], 0, ssd->write_block_size - swb->pos);
		}

		// Wait for all writers to finish.
		while (cf_atomic32_get(ssd->n_writers) != 0) {
			;
		}

		// Enqueue the buffer, to be flushed to device.
		cf_queue_push(ssd->swb_write_q, &swb);
		cf_atomic_int_incr(&ssd->ssd_write_buf_counter); // for write smoothing

		// Get the new buffer.
		swb = swb_get(ssd);
		ssd->current_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "write bins: couldn't get swb");
			pthread_mutex_unlock(&ssd->LOCK);
			return -1;
		}
	}

	// There's enough space - save the position where this record will be
	// written, and advance swb->pos for the next writer.
	uint32_t swb_pos = swb->pos;

	swb->pos += write_size;
	cf_atomic32_incr(&ssd->n_writers);

	pthread_mutex_unlock(&ssd->LOCK);
	// May now write this record concurrently with others in this swb.

	// Flatten data into the block.

	uint8_t *buf = &swb->buf[swb_pos];
	uint8_t *buf_start = buf;

	drv_ssd_block *block = (drv_ssd_block*)buf;

	buf += sizeof(drv_ssd_block);

	// Pickle and write vinfo, if any.
	size_t vinfo_buf_length = 0;

	if (rd->ns->allow_versions) {
		uint8_t vinfo_buf[AS_PARTITION_VINFOSET_PICKLE_MAX];
		as_partition_id pid = as_partition_getid(rd->keyd);
		as_partition_vinfoset *vinfoset = &rd->ns->partitions[pid].vinfoset;

		vinfo_buf_length = AS_PARTITION_VINFOSET_PICKLE_MAX;

		if (0 != as_partition_vinfoset_mask_pickle(vinfoset,
				as_index_vinfo_mask_get(r, true), vinfo_buf,
				&vinfo_buf_length)) {
			cf_crash(AS_DRV_SSD, "unable to pickle vinfoset");
		}

		if (vinfo_buf_length != 0) {
			memcpy(buf, vinfo_buf, vinfo_buf_length);
			buf += vinfo_buf_length;
		}
	}

	// Properties list goes just before bins.
	if (rd->rec_props.p_data) {
		memcpy(buf, rd->rec_props.p_data, rd->rec_props.size);
		buf += rd->rec_props.size;
	}

	drv_ssd_bin *ssd_bin = 0;
	uint32_t write_nbins = 0;

	if (0 == rd->bins) {
		cf_warning(AS_DRV_SSD, "write bins: no bins array");
		cf_atomic32_decr(&ssd->n_writers);
		return -1;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *bin = &rd->bins[i];

		if (as_bin_inuse(bin)) {
			ssd_bin = (drv_ssd_bin*)buf;
			buf += sizeof(drv_ssd_bin);

			ssd_bin->version = as_bin_get_version(bin, rd->ns->single_bin);

			if (! rd->ns->single_bin) {
				strcpy(ssd_bin->name, as_bin_get_name_from_id(rd->ns, bin->id));
			}
			else {
				ssd_bin->name[0] = 0;
			}

			ssd_bin->offset = buf - buf_start;

			size_t particle_flat_size;

			if (0 == as_particle_get_flat_size(bin, &particle_flat_size)) {
				if (as_bin_is_integer(bin)) {
					as_particle_int_on_device *p =
							(as_particle_int_on_device*)buf;
					p->type = AS_PARTICLE_TYPE_INTEGER;
					p->len = sizeof(uint64_t);
					p->i = bin->ivalue;
				}
				else {
					memcpy(buf, as_bin_get_particle(bin), particle_flat_size);
				}
			}
			else {
				cf_warning(AS_DRV_SSD, "can't get flat particle size - writing empty bin");
				particle_flat_size = 0;
			}

			buf += particle_flat_size;
			ssd_bin->len = particle_flat_size;
			ssd_bin->next = buf - buf_start;

			write_nbins++;
		}
	}

	block->length = write_size - SIGNATURE_OFFSET;
	block->magic = SSD_BLOCK_MAGIC;
	block->sig = 0;
	block->keyd = rd->keyd;
	block->generation = r->generation;
	block->void_time = r->void_time;
	block->bins_offset = vinfo_buf_length + (rd->rec_props.p_data ? rd->rec_props.size : 0);
	block->n_bins = write_nbins;
	block->vinfo_offset = 0;
	block->vinfo_length = vinfo_buf_length;

	if (ssd->use_signature) {
		cf_signature_compute(((uint8_t*)block) + SIGNATURE_OFFSET,
				block->length, &block->sig);
	}
	else {
		block->sig = 0;
	}

	r->storage_key.ssd.file_id = ssd->file_id;
	r->storage_key.ssd.rblock_id = BYTES_TO_RBLOCKS(WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id) + swb_pos);
	r->storage_key.ssd.n_rblocks = BYTES_TO_RBLOCKS(write_size);

	cf_atomic64_add(&ssd->inuse_size, (int64_t)write_size);
	cf_atomic32_add(&ssd->alloc_table->wblock_state[swb->wblock_id].inuse_sz, (int32_t)write_size);

	// We are finished writing to the buffer.
	cf_atomic32_decr(&ssd->n_writers);

	return 0;
}


int
ssd_write(as_record *r, as_storage_rd *rd)
{
	drv_ssd *old_ssd = NULL;
	uint64_t old_rblock_id = 0;
	uint16_t old_n_rblocks = 0;

	if (STORAGE_RBLOCK_IS_VALID(r->storage_key.ssd.rblock_id)) {
		// Replacing an old record.
		old_ssd = rd->u.ssd.ssd;
		old_rblock_id = r->storage_key.ssd.rblock_id;
		old_n_rblocks = r->storage_key.ssd.n_rblocks;
	}

	drv_ssds *ssds = (drv_ssds*)rd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	rd->u.ssd.ssd = &ssds->ssds[ssd_get_file_id(ssds, &rd->keyd)];

	drv_ssd *ssd = rd->u.ssd.ssd;

	if (! ssd) {
		cf_warning(AS_DRV_SSD, "{%s} ssd_write: no drv_ssd for file_id %d",
				rd->ns->name, ssd_get_file_id(ssds, &rd->keyd));
		return -1;
	}

	int rv = ssd_write_bins(r, rd);

	if (rv == 0 && old_ssd) {
		ssd_block_free(old_ssd, old_rblock_id, old_n_rblocks, "ssd-write");
	}

	return rv;
}


//==========================================================
// Storage statistics utilities.
//

void
as_storage_show_wblock_stats(as_namespace *ns)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return;
	}

	if (ns->storage_private) {
		drv_ssds *ssds = ns->storage_private;

		for (int d = 0; d < ssds->n_ssds; d++) {
			int num_free_blocks = 0;
			int num_full_blocks = 0;
			int num_full_swb = 0;
			int num_above_wm = 0;
			int num_defraggable = 0;

			drv_ssd *ssd = &ssds->ssds[d];
			ssd_alloc_table *at = ssd->alloc_table;
			uint32_t lwm_size = ns->defrag_lwm_size;

			for (uint32_t i = 0; i < at->n_wblocks; i++) {
				ssd_wblock_state *wblock_state = &at->wblock_state[i];
				uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

				if (inuse_sz == 0) {
					num_free_blocks++;
				}
				else if (inuse_sz == ssd->write_block_size) {
					if (wblock_state->swb) {
						num_full_swb++;
					}
					else {
						num_full_blocks++;
					}
				}
				else {
					if (inuse_sz > ssd->write_block_size || inuse_sz < lwm_size) {
						cf_info(AS_DRV_SSD, "dev %d, wblock %"PRIu32", inuse_sz %"PRIu32", %s swb",
								d, i, inuse_sz, wblock_state->swb ? "has" : "no");

						num_defraggable++;
					}
					else {
						num_above_wm++;
					}
				}
			}

			cf_info(AS_DRV_SSD, "device %s free %d full %d fullswb %d pfull %d defrag %d freeq %d",
				ssd->name, num_free_blocks, num_full_blocks, num_full_swb,
				num_above_wm, num_defraggable, cf_queue_sz(ssd->free_wblock_q));
		}
	}
	else {
		cf_info(AS_DRV_SSD, "no devices");
	}
}


void
as_storage_summarize_wblock_stats(as_namespace *ns)
{
	int num_free_blocks = 0;
	int num_full_blocks = 0;
	int num_full_swb = 0;
	int num_above_wm = 0;
	int num_defraggable = 0;
	uint64_t defraggable_sz = 0;
	uint64_t non_defraggable_sz = 0;

	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return;
	}

	if (ns->storage_private) {
		drv_ssds *ssds = ns->storage_private;

		// Note: This is a sparse array that could be more efficiently stored.
		// (In addition, ranges of block sizes could be binned together to
		// compress the histogram, rather than using one bin per block size.)
		int wb_hist[MAX_WRITE_BLOCK_SIZE + 1];

		memset(wb_hist, 0, MAX_WRITE_BLOCK_SIZE * sizeof(int));

		for (int d=0; d <ssds->n_ssds; d++) {
			drv_ssd *ssd = &ssds->ssds[d];
			ssd_alloc_table *at = ssd->alloc_table;
			uint32_t lwm_size = ns->defrag_lwm_size;

			for (uint32_t i = 0; i < at->n_wblocks; i++) {
				ssd_wblock_state *wblock_state = &at->wblock_state[i];
				uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

				if (inuse_sz >= MAX_WRITE_BLOCK_SIZE) {
					cf_warning(AS_DRV_SSD, "wblock size (%d >= %d) too large ~~ not counting in histogram",
							inuse_sz, MAX_WRITE_BLOCK_SIZE);
				}
				else {
					wb_hist[inuse_sz]++;
				}

				if (inuse_sz == 0) {
					num_free_blocks++;
				}
				else if (inuse_sz == ssd->write_block_size) {
					if (wblock_state->swb) {
						num_full_swb++;
					}
					else {
						num_full_blocks++;
					}
				}
				else {
					if (inuse_sz > ssd->write_block_size || inuse_sz < lwm_size) {
						defraggable_sz += inuse_sz;
						num_defraggable++;
					}
					else {
						non_defraggable_sz += inuse_sz;
						num_above_wm++;
					}
				}
			}

			cf_info(AS_DRV_SSD, "device %s free %d full %d fullswb %d pfull %d defrag %d freeq %d",
				ssd->name, num_free_blocks, num_full_blocks, num_full_swb,
				num_above_wm, num_defraggable, cf_queue_sz(ssd->free_wblock_q));
		}

		// Dump histogram.
		cf_info(AS_DRV_SSD, "WBH: Storage histogram for namespace \"%s\":", ns->name);
		cf_info(AS_DRV_SSD, "WBH: Average wblock size of: defraggable blocks: %lu bytes; nondefraggable blocks: %lu bytes; all blocks: %lu bytes",
				(defraggable_sz / MAX(1, num_defraggable)),
				(non_defraggable_sz / MAX(1, num_above_wm)),
				((defraggable_sz + non_defraggable_sz) / MAX(1, (num_defraggable + num_above_wm))));

		for (int i = 0; i < MAX_WRITE_BLOCK_SIZE; i++) {
			if (wb_hist[i] > 0) {
				cf_info(AS_DRV_SSD, "WBH: %d block%s of size %lu bytes",
						wb_hist[i], (wb_hist[i] != 1 ? "s" : ""), i);
			}
		}
	}
	else {
		cf_info(AS_DRV_SSD,"no devices");
	}
}


// TODO - do something more useful with this info command.
int
as_storage_analyze_wblock(as_namespace* ns, int device_index,
		uint32_t wblock_id)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return -1;
	}

	cf_info(AS_DRV_SSD, "analyze wblock: ns %s, device-index %d, wblock-id %u",
			ns->name, device_index, wblock_id);

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: no devices");
		return -1;
	}

	if (device_index < 0 || device_index >= ssds->n_ssds) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: bad device-index");
		return -1;
	}

	drv_ssd* ssd = &ssds->ssds[device_index];
	uint8_t* read_buf = cf_valloc(ssd->write_block_size);

	if (! read_buf) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: fail valloc");
		return -1;
	}

	int fd = ssd_fd_get(ssd);
	uint64_t file_offset = WBLOCK_ID_TO_BYTES(ssd, wblock_id);

	if (lseek(fd, (off_t)file_offset, SEEK_SET) != (off_t)file_offset) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: offset %lu: errno %d (%s)",
				ssd->name, file_offset, errno, cf_strerror(errno));
		cf_free(read_buf);
		close(fd);
		return -1;
	}

	ssize_t rlen = read(fd, read_buf, ssd->write_block_size);

	if (rlen != (ssize_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd->name, rlen, errno, cf_strerror(errno));
		cf_free(read_buf);
		close(fd);
		return -1;
	}

	ssd_fd_put(ssd, fd);

	uint32_t living_populations[AS_PARTITIONS];
	uint32_t zombie_populations[AS_PARTITIONS];

	memset(living_populations, 0, sizeof(living_populations));
	memset(zombie_populations, 0, sizeof(zombie_populations));

	uint32_t inuse_sz_start =
			cf_atomic32_get(ssd->alloc_table->wblock_state[wblock_id].inuse_sz);
	uint32_t offset = 0;

	while (offset < ssd->write_block_size) {
		drv_ssd_block* p_block = (drv_ssd_block*)&read_buf[offset];

		if (p_block->magic != SSD_BLOCK_MAGIC) {
			if (offset == 0) {
				// First block must have magic.
				cf_warning(AS_DRV_SSD, "analyze wblock ERROR: 1st block has no magic");
				cf_free(read_buf);
				return -1;
			}

			// Later blocks may have no magic, just skip to next block.
			offset += RBLOCK_SIZE;
			continue;
		}

		// Note - if block->length is sane, we don't need to round up to a
		// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
		uint32_t next_offset = offset +
				BYTES_TO_RBLOCK_BYTES(p_block->length + SIGNATURE_OFFSET);

		if (next_offset > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "analyze wblock ERROR: record overflows wblock");
			cf_free(read_buf);
			return -1;
		}

		// Check signature.
		if (ssd->use_signature && p_block->sig) {
			cf_signature sig;

			cf_signature_compute(((uint8_t*)p_block) + SIGNATURE_OFFSET,
					p_block->length, &sig);

			if (sig != p_block->sig) {
				// Look for next block with magic.
				offset += RBLOCK_SIZE;
				continue;
			}
		}

		uint64_t rblock_id = BYTES_TO_RBLOCKS(file_offset + offset);
		uint32_t n_rblocks = (uint32_t)BYTES_TO_RBLOCKS(next_offset - offset);

		bool living = false;
		as_partition_id pid = as_partition_getid(p_block->keyd);
		as_partition_reservation rsv;

		as_partition_reserve_migrate(ns, pid, &rsv, 0);
		cf_atomic_int_incr(&g_config.ssdr_tree_count);

		as_index_ref r_ref;
		r_ref.skip_lock = false;

		if (0 == as_record_get(rsv.tree, &p_block->keyd, &r_ref, ns)) {
			as_index* r = r_ref.r;

			if (r->storage_key.ssd.rblock_id == rblock_id &&
					r->storage_key.ssd.n_rblocks == n_rblocks) {
				living = true;
			}

			as_record_done(&r_ref, ns);
		}
		else if (ns->ldt_enabled &&
				0 == as_record_get(rsv.sub_tree, &p_block->keyd, &r_ref, ns)) {
			as_index* r = r_ref.r;

			if (r->storage_key.ssd.rblock_id == rblock_id &&
					r->storage_key.ssd.n_rblocks == n_rblocks) {
				living = true;
			}

			as_record_done(&r_ref, ns);
		}
		// else it was deleted (?) so call it a zombie...

		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.ssdr_tree_count);

		if (living) {
			living_populations[pid]++;
		}
		else {
			zombie_populations[pid]++;
		}

		offset = next_offset;
	}

	cf_free(read_buf);

	uint32_t inuse_sz_end =
			cf_atomic32_get(ssd->alloc_table->wblock_state[wblock_id].inuse_sz);

	cf_info(AS_DRV_SSD, "analyze wblock: inuse_sz %u (before) -> %u (after)",
			inuse_sz_start, inuse_sz_end);

	for (int i = 0; i < AS_PARTITIONS; i++) {
		if (living_populations[i] > 0 || zombie_populations[i] > 0) {
			cf_info(AS_DRV_SSD, "analyze wblock: pid %4d - live %u, dead %u",
					i, living_populations[i], zombie_populations[i]);
		}
	}

	return 0;
}


//==========================================================
// Per-device background jobs.
//

#define LOG_STATS_INTERVAL_sec 20

void
ssd_log_stats(drv_ssd *ssd, uint64_t *p_prev_n_writes,
		uint64_t *p_prev_n_defrags)
{
	uint64_t n_writes = cf_atomic_int_get(ssd->ssd_write_buf_counter);
	uint64_t n_defrags = cf_atomic_int_get(ssd->defrag_wblock_counter);

	float write_rate = (float)(n_writes - *p_prev_n_writes) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_rate = (float)(n_defrags - *p_prev_n_defrags) /
			(float)LOG_STATS_INTERVAL_sec;

	cf_info(AS_DRV_SSD, "device %s: used %lu, contig-free %luM (%d wblocks), swb-free %d, n-w %u, w-q %d w-tot %lu (%.1f/s), defrag-q %d defrag-tot %lu (%.1f/s)",
			ssd->name, ssd->inuse_size,
			available_size(ssd) >> 20,
			cf_queue_sz(ssd->free_wblock_q),
			cf_queue_sz(ssd->swb_free_q),
			cf_atomic32_get(ssd->n_writers),
			cf_queue_sz(ssd->swb_write_q), n_writes, write_rate,
			cf_queue_sz(ssd->defrag_wblock_q), n_defrags, defrag_rate);

	*p_prev_n_writes = n_writes;
	*p_prev_n_defrags = n_defrags;

	if (cf_queue_sz(ssd->free_wblock_q) == 0) {
		cf_warning(AS_DRV_SSD, "device %s: out of storage space", ssd->name);
	}
}


void
ssd_free_swbs(drv_ssd *ssd)
{
	// Try to recover swbs, 16 at a time, down to 16.
	for (int i = 0; i < 16 && cf_queue_sz(ssd->swb_free_q) > 16; i++) {
		ssd_write_buf* swb;

		if (CF_QUEUE_OK !=
				cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
			break;
		}

		swb_destroy(swb);
	}
}


void
ssd_flush_current_swb(drv_ssd *ssd, uint64_t *p_prev_n_writes,
		uint32_t *p_prev_size)
{
	uint64_t n_writes = cf_atomic_int_get(ssd->ssd_write_buf_counter);

	// If there's an active write load, we don't need to flush.
	if (n_writes != *p_prev_n_writes) {
		*p_prev_n_writes = n_writes;
		*p_prev_size = 0;
		return;
	}

	pthread_mutex_lock(&ssd->LOCK);

	n_writes = cf_atomic_int_get(ssd->ssd_write_buf_counter);

	// Must check under the lock, could be racing a current swb just queued.
	if (n_writes != *p_prev_n_writes) {

		pthread_mutex_unlock(&ssd->LOCK);

		*p_prev_n_writes = n_writes;
		*p_prev_size = 0;
		return;
	}

	// Flush the current swb if it isn't empty, and has been written to since
	// last flushed.

	ssd_write_buf *swb = ssd->current_swb;

	if (swb && swb->pos != *p_prev_size) {
		*p_prev_size = swb->pos;

		// Clean the end of the buffer before flushing.
		if (ssd->write_block_size != swb->pos) {
			memset(&swb->buf[swb->pos], 0, ssd->write_block_size - swb->pos);
		}

		// Wait for all writers to finish. (Arguably we should bail out instead,
		// since current writers indicate we might soon flush this buffer, but
		// let's opt for certainty.)
		while (cf_atomic32_get(ssd->n_writers) != 0) {
			;
		}

		// Flush it.
		ssd_flush_swb(ssd, swb);
	}

	pthread_mutex_unlock(&ssd->LOCK);
}


void
ssd_fsync(drv_ssd *ssd)
{
	int fd = ssd_fd_get(ssd);

	uint64_t start_ns = g_config.storage_benchmarks ? cf_getns() : 0;

	fsync(fd);

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_fsync, start_ns);
	}

	ssd_fd_put(ssd, fd);
}


// Check all wblocks to load a device's defrag queue at runtime. Triggered only
// when defrag-lwm-pct is increased by manual intervention.
void
ssd_defrag_sweep(drv_ssd *ssd)
{
	ssd_alloc_table* at = ssd->alloc_table;
	uint32_t first_id = BYTES_TO_WBLOCK_ID(ssd, ssd->header_size);
	uint32_t last_id = at->n_wblocks;
	uint32_t n_queued = 0;

	for (uint32_t wblock_id = first_id; wblock_id < last_id; wblock_id++) {
		ssd_wblock_state *p_wblock_state = &at->wblock_state[wblock_id];

		pthread_mutex_lock(&p_wblock_state->LOCK);

		uint32_t inuse_sz = cf_atomic32_get(p_wblock_state->inuse_sz);

		if (! p_wblock_state->swb &&
				p_wblock_state->state != WBLOCK_STATE_DEFRAG &&
					inuse_sz != 0 &&
						inuse_sz < ssd->ns->defrag_lwm_size) {
			push_wblock_to_defrag_q(ssd, wblock_id);
			n_queued++;
		}

		pthread_mutex_unlock(&p_wblock_state->LOCK);
	}

	cf_info(AS_DRV_SSD, "... %s sweep queued %u wblocks for defrag", ssd->name,
			n_queued);
}


static inline uint64_t
next_time(uint64_t now, uint64_t job_interval, uint64_t next)
{
	uint64_t next_job = now + job_interval;

	return next_job < next ? next_job : next;
}


// All in microseconds since we're using usleep().
#define MAX_INTERVAL		(1000 * 1000)
#define LOG_STATS_INTERVAL	(1000 * 1000 * LOG_STATS_INTERVAL_sec)
#define FREE_SWBS_INTERVAL	(1000 * 1000 * 20)

// Thread "run" function to perform various background jobs per device.
void *
run_ssd_maintenance(void *udata)
{
	drv_ssd *ssd = (drv_ssd*)udata;
	as_namespace *ns = ssd->ns;

	uint64_t prev_n_writes = 0;
	uint64_t prev_n_defrags = 0;

	uint64_t prev_n_writes_flush = 0;
	uint32_t prev_size_flush = 0;

	uint64_t now = cf_getus();
	uint64_t next = now + MAX_INTERVAL;

	uint64_t prev_log_stats = now;
	uint64_t prev_free_swbs = now;
	uint64_t prev_flush = now;
	uint64_t prev_fsync = now;

	// If any job's (initial) interval is less than MAX_INTERVAL and we want it
	// done on its interval the first time through, add a next_time() call for
	// that job here to adjust 'next'. (No such jobs for now.)

	uint64_t sleep_us = next - now;

	while (true) {
		usleep((uint32_t)sleep_us);

		now = cf_getus();
		next = now + MAX_INTERVAL;

		if (now >= prev_log_stats + LOG_STATS_INTERVAL) {
			ssd_log_stats(ssd, &prev_n_writes, &prev_n_defrags);
			prev_log_stats = now;
			next = next_time(now, LOG_STATS_INTERVAL, next);
		}

		if (now >= prev_free_swbs + FREE_SWBS_INTERVAL) {
			ssd_free_swbs(ssd);
			prev_free_swbs = now;
			next = next_time(now, FREE_SWBS_INTERVAL, next);
		}

		uint64_t flush_max_us = ns->storage_flush_max_us;

		if (flush_max_us != 0 && now >= prev_flush + flush_max_us) {
			ssd_flush_current_swb(ssd, &prev_n_writes_flush, &prev_size_flush);
			prev_flush = now;
			next = next_time(now, flush_max_us, next);
		}

		uint64_t fsync_max_us = ns->storage_fsync_max_us;

		if (fsync_max_us != 0 && now >= prev_fsync + fsync_max_us) {
			ssd_fsync(ssd);
			prev_fsync = now;
			next = next_time(now, fsync_max_us, next);
		}

		if (cf_atomic32_get(ssd->defrag_sweep) != 0) {
			// May take long enough to mess up other jobs' schedules, but it's a
			// very rare manually-triggered intervention.
			ssd_defrag_sweep(ssd);
			cf_atomic32_decr(&ssd->defrag_sweep);
		}

		now = cf_getus(); // refresh in case jobs took significant time
		sleep_us = next > now ? next - now : 1;
	}

	return NULL;
}


static void
ssd_start_maintenance_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "ns %s starting device maintenance threads",
			ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		pthread_create(&ssd->maintenance_thread, 0, run_ssd_maintenance, ssd);
	}
}


//==========================================================
// Device header utilities.
//

// -1 means unrecoverable error
// -2 means not formatted, please overwrite me
int
as_storage_read_header(drv_ssd *ssd, as_namespace *ns,
		ssd_device_header **header_r)
{
	*header_r = 0;

	int rv = -1;

	bool use_shadow = ns->cold_start && ssd->shadow_name;
	const char *ssd_name = use_shadow ? ssd->shadow_name : ssd->name;
	int fd = use_shadow ? ssd_shadow_fd_get(ssd) : ssd_fd_get(ssd);

	size_t peek_size = BYTES_UP_TO_IO_MIN(ssd, sizeof(ssd_device_header));
	ssd_device_header *header = cf_valloc(peek_size);

	if (! header) {
		goto Fail;
	}

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: errno %d (%s)", ssd_name,
				errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	ssize_t sz = read(fd, (void*)header, peek_size);

	if (sz != (ssize_t)peek_size) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd_name, sz, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	// Make sure all following checks that return -1 or -2 are also done in
	// peek_devices() in the enterprise repo.

	if (header->magic != SSD_HEADER_MAGIC) { // normal path for a fresh drive
		cf_detail(AS_DRV_SSD, "read_header: device %s no magic, not a Citrusleaf drive",
				ssd_name);
		rv = -2;
		goto Fail;
	}

	if (header->version != SSD_VERSION) {
		if (can_convert_storage_version(header->version)) {
			cf_info(AS_DRV_SSD, "read_header: device %s converting storage version %u to %u",
					ssd_name, header->version, SSD_VERSION);
		}
		else {
			cf_warning(AS_DRV_SSD, "read_header: device %s bad version %u, not a current Citrusleaf drive",
					ssd_name, header->version);
			goto Fail;
		}
	}

	if (header->write_block_size != 0 &&
			ns->storage_write_block_size % header->write_block_size != 0) {
		cf_warning(AS_DRV_SSD, "read header: device %s can't change write-block-size from %u to %u",
				ssd_name, header->write_block_size,
				ns->storage_write_block_size);
		goto Fail;
	}

	if (header->devices_n > 100) {
		cf_warning(AS_DRV_SSD, "read header: device %s don't support %u devices, corrupt read",
				ssd_name, header->devices_n);
		goto Fail;
	}

	if (strcmp(header->namespace, ns->name) != 0) {
		cf_warning(AS_DRV_SSD, "read header: device %s previous namespace %s now %s, check config or erase device",
				ssd_name, header->namespace, ns->name);
		goto Fail;
	}

	size_t h_len = header->header_length;

	cf_free(header);

	header = cf_valloc(h_len);

	if (! header) {
		goto Fail;
	}

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: errno %d (%s)", ssd_name,
				errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	sz = read(fd, (void*)header, h_len);

	if (sz != (ssize_t)header->header_length) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd_name, sz, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	cf_detail(AS_DRV_SSD, "device %s: header read success: version %d devices %d random %"PRIu64,
			ssd_name, header->version, header->devices_n, header->random);

	// In case we're bumping the version - ensure the new version gets written.
	header->version = SSD_VERSION;

	// In case we're increasing write-block-size - ensure new value is recorded.
	header->write_block_size = ns->storage_write_block_size;

	*header_r = header;

	use_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);

	return 0;

Fail:

	if (header) {
		cf_free(header);
	}

	if (fd != -1) {
		use_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);
	}

	return rv;
}


ssd_device_header *
as_storage_init_header(as_namespace *ns)
{
	ssd_device_header *h = cf_valloc(SSD_DEFAULT_HEADER_LENGTH);

	if (! h) {
		return 0;
	}

	memset(h, 0, SSD_DEFAULT_HEADER_LENGTH);

	h->magic = SSD_HEADER_MAGIC;
	h->random = 0;
	h->write_block_size = ns->storage_write_block_size;
	h->last_evict_void_time = 0;
	h->version = SSD_VERSION;
	h->devices_n = 0;
	h->header_length = SSD_DEFAULT_HEADER_LENGTH;
	memset(h->namespace, 0, sizeof(h->namespace));
	strcpy(h->namespace, ns->name);
	h->info_n = 4096;
	h->info_stride = 128;

	return h;
}


bool
as_storage_empty_header(int fd, const char* device_name)
{
	void *h = cf_valloc(SSD_DEFAULT_HEADER_LENGTH);

	if (! h) {
		cf_warning(AS_DRV_SSD, "device %s: empty header: valloc failed",
				device_name);
		return false;
	}

	memset(h, 0, SSD_DEFAULT_HEADER_LENGTH);

	if (0 != lseek(fd, 0, SEEK_SET)) {
		cf_warning(AS_DRV_SSD, "device %s: empty header: seek error: %s",
				device_name, cf_strerror(errno));
		cf_free(h);
		return false;
	}

	if (SSD_DEFAULT_HEADER_LENGTH != write(fd, h, SSD_DEFAULT_HEADER_LENGTH)) {
		cf_warning(AS_DRV_SSD, "device %s: empty header: write error: %s",
				device_name, cf_strerror(errno));
		cf_free(h);
		return false;
	}

	cf_free(h);
	fsync(fd);

	return true;
}


void
as_storage_write_header(drv_ssd *ssd, ssd_device_header *header, size_t size)
{
	int fd = ssd_fd_get(ssd);

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	ssize_t sz = write(fd, (void*)header, size);

	if (sz != (ssize_t)size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	ssd_fd_put(ssd, fd);

	if (! ssd->shadow_name) {
		return;
	}

	fd = ssd_shadow_fd_get(ssd);

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	sz = write(fd, (void*)header, size);

	if (sz != (ssize_t)size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	ssd_shadow_fd_put(ssd, fd);
}


//==========================================================
// Cold start utilities.
//

void
ssd_record_get_ldt_property(as_rec_props *props, bool *is_ldt_parent,
		bool *is_ldt_sub)
{
	uint16_t * ldt_rectype_bits;

	*is_ldt_sub	= false;
	*is_ldt_parent = false;

	if (props->size != 0 &&
			(as_rec_props_get_value(props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
					(uint8_t**)&ldt_rectype_bits) == 0)) {
		if (as_ldt_flag_has_sub(*ldt_rectype_bits)) {
			*is_ldt_sub = true;
		}
		else if (as_ldt_flag_has_parent(*ldt_rectype_bits)) {
			*is_ldt_parent = true;
		}
	}

	cf_detail(AS_LDT, "LDT_LOAD ssd_record_get_ldt_property: Parent=%d Subrec=%d",
			*is_ldt_parent, *is_ldt_sub);
}


// Add a record just read from drive to the index, if all is well.
// Return values:
//  0 - success, record added or updated
// -1 - skipped or deleted this record for a "normal" reason
// -2 - serious limit encountered, caller won't continue
// -3 - couldn't parse this record, but caller will continue
int
ssd_record_add(drv_ssds* ssds, drv_ssd* ssd, drv_ssd_block* block,
		uint64_t rblock_id, uint32_t n_rblocks)
{
	as_partition_id pid = as_partition_getid(block->keyd);

	// If this isn't a partition we're interested in, skip this record.
	if (! ssds->get_state_from_storage[pid]) {
		return -1;
	}

	as_namespace* ns = ssds->ns;

	// If eviction is necessary, evict previously added records closest to
	// expiration. (If evicting, this call will block for a long time.) This
	// call also updates the cold-start threshold void-time.
	if (! as_cold_start_evict_if_needed(ns)) {
		cf_warning(AS_DRV_SSD, "device %s: record-add halting read", ssd->name);
		return -2;
	}

	// Sanity-check the record.
	if (! is_valid_record(block, ns->name)) {
		return -3;
	}

	// Don't bother with reservations - partition trees aren't going anywhere.
	as_partition* p_partition = &ns->partitions[pid];

	// Get or create the record.
	as_index_ref r_ref;
	r_ref.skip_lock = false;

	// Read LDT rec-prop.
	uint32_t properties_offset = block->vinfo_offset + block->vinfo_length;
	as_rec_props props;

	props.p_data = block->data + properties_offset;
	props.size = block->bins_offset - properties_offset;

	bool is_ldt_sub;
	bool is_ldt_parent;

	ssd_record_get_ldt_property(&props, &is_ldt_parent, &is_ldt_sub);

	if (ssd->sub_sweep) {
		if (! is_ldt_sub) {
			cf_detail(AS_DRV_SSD, "LDT_LOAD Skipping parent records in the subrecord sweep %d %d",
					is_ldt_parent, is_ldt_sub);
			return 0;
		}
	}
	else {
		if (is_ldt_sub) {
			cf_detail(AS_DRV_SSD, "LDT_LOAD Skipping subrecord records in the non subrecord sweep %d %d",
					is_ldt_parent, is_ldt_sub);
			return 0;
		}
	}

	// Get/create the record from/in the appropriate index tree.
	int rv = as_record_get_create(
			is_ldt_sub ? p_partition->sub_vp : p_partition->vp,
					&block->keyd, &r_ref, ns, is_ldt_sub);

	if (rv < 0) {
		cf_warning_digest(AS_DRV_SSD, &block->keyd, "record-add as_record_get_create() failed ");
		return -1;
	}

	// Fix 0 generations coming off device.
	if (block->generation == 0) {
		block->generation = 1;
		cf_warning_digest(AS_DRV_SSD, &block->keyd, "record-add found generation 0 - changed to 1 ");
	}

	// Set 0 void-time to default, if there is one.
	if (block->void_time == 0 && ns->default_ttl != 0) {
		cf_debug(AS_DRV_SSD, "record-add changing 0 void-time to default");
		block->void_time = as_record_void_time_get() + ns->default_ttl;
	}

	as_index* r = r_ref.r;

	if (rv == 0) {
		// Record already existed. Perform generation check first, and ignore
		// this record if existing record is newer. Use void-times as tie-break
		// if generations are equal.
		if (block->generation < r->generation ||
				(block->generation == r->generation &&
						block->void_time <= r->void_time)) {
			cf_detail(AS_DRV_SSD, "record-add skipping generation %u <= existing %u",
					block->generation, r->generation);

			as_record_done(&r_ref, ns);
			ssd->record_add_generation_counter++;
			return -1;
		}
	}
	// The record we're now reading is the latest version (so far) ...

	// Set/reset the record's void-time and generation.
	r->void_time = block->void_time;
	r->generation = block->generation;

	// No expiry for ldt_sub based on the TTL. LDT sub are expired based on the 
	// parent record expiry. 
	if (r->void_time != 0 && !is_ldt_sub) {
		// The threshold may be ~ now, or it may be in the future if eviction
		// has been happening.
		uint32_t threshold_void_time =
				cf_atomic32_get(ns->cold_start_threshold_void_time);

		// If the record is set to expire before the threshold, delete it.
		// (Note that if a record is skipped here, then later we encounter a
		// version with older generation but bigger (not expired) void-time,
		// that older version gets resurrected.)
		if (r->void_time < threshold_void_time) {
			cf_detail(AS_DRV_SSD, "record-add deleting void-time %u < threshold %u",
					r->void_time, threshold_void_time);

			as_index_delete(p_partition->vp, &block->keyd);
			as_record_done(&r_ref, ns);
			ssd->record_add_expired_counter++;
			return -1;
		}

		// If the record is beyond max-ttl, either it's rogue data (from
		// improperly coded clients) or it's data the users don't want anymore
		// (user decreased the max-ttl setting). No such check is needed for
		// the subrecords ...
		if (ns->max_ttl != 0) {
			if (r->void_time > ns->cold_start_max_void_time) {
				cf_debug(AS_DRV_SSD, "record-add deleting void-time %u > max %u",
						r->void_time, ns->cold_start_max_void_time);

				as_index_delete(p_partition->vp, &block->keyd);
				as_record_done(&r_ref, ns);
				ssd->record_add_max_ttl_counter++;
				return -1;
			}
		}
	}

	// We'll keep the record we're now reading ...

	// Update maximum void-times.
	cf_atomic_int_setmax(&p_partition->max_void_time, r->void_time);
	cf_atomic_int_setmax(&ns->max_void_time, r->void_time);

	if (props.size != 0) {
		// Do this early since set-id is needed for the secondary index update.
		as_record_apply_properties(r, ns, &props);
	}
	else {
		as_record_clear_properties(r, ns);
	}

	cf_detail(AS_RW, "TO INDEX FROM DISK	Digest=%"PRIx64" bits %d",
			*(uint64_t*)&block->keyd.digest[8],
			as_ldt_record_get_rectype_bits(r));

	// If data is in memory, load bins and particles.
	if (ns->storage_data_in_memory) {
		as_index_vinfo_mask_set(r,
				as_partition_vinfoset_mask_unpickle(p_partition,
						block->data + block->vinfo_offset, block->vinfo_length),
				ns->allow_versions);

		uint8_t* block_head = (uint8_t*)block;
		drv_ssd_bin* ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);
		as_storage_rd rd;

		if (rv == 1) {
			as_storage_record_create(ns, r, &rd, &block->keyd);
		}
		else {
			as_storage_record_open(ns, r, &rd, &block->keyd);
		}

		rd.u.ssd.block = block;
		rd.have_device_block = true;
		rd.n_bins = as_bin_get_n_bins(r, &rd);
		rd.bins = as_bin_get_all(r, &rd, 0);

		uint64_t bytes_memory = as_storage_record_get_n_bytes_memory(&rd);
		uint16_t old_n_bins = rd.n_bins;

		/*
		 * A bin having sindex can be updated in this process.
		 * oldbins = bins from record
		 * newbins = bins from disk
		 * First part of the algorithm tries to gather the sbins of bins whose index > number of newbins.
		 * Note- We can make sbin of a bin only when there is a sindex on it.
		 * Second half of the algorithm iterates in newbins and oldbins and tries to create
		 * and match the sbins of the bins of same index.
		 * If they dont match, we put the sbin from oldbins into the oldbin array and sbin from newbins into the newbin array.
		 * In the end we delete the sbins from oldbin array and insert the sbins from newbin array.
		 *
		 * Example 1:
		 * 		Assume all bins have sindex on it.
		 * 		oldbins - bin1:a, bin2:c, bin3:d, bin4:e
		 * 		newbins - bin1:a, bin2:cc
		 * In this case bin3 and bin4 is deleted. Value of bin2 is changed.
		 * Here index of bin3 and bin4 is > number of newbins (2). Thus we make sbin of bin3 and bin4 and put it in the oldbin array
		 * Size of oldbin array is now 2.
		 * Iteration1:
		 * 		sbin1 of bin1 from oldbins.
		 * 		sbin2 of bin1 from newbins
		 *		Match sbin1 and sbin2. They will match. Do nothing
		 *		Size of oldbin array = 2 and newbin array = 0
		 *
		 * Iteration2:
		 * 		sbin1 of bin2 from oldbins
		 * 		sbin2 of bin2 from newbins
		 *		Match sbin1 and sbin2. They will not match. Add them to their respective array.
		 *		Size of oldbin array = 3 and newbin array = 1
		 * End:
		 * 		Deletes all the sbins of oldbin array from secondary index trees
		 * 		Inesert all the sbins of newbin array from secondary index trees
		 *
		 * Example 2:
		 * 		oldbins - bin1:a, bin2:b
		 * 		newbins - bin1:a, bin2:bb, bin3:c, bin4:d
		 * Value of bin2 is changed.
		 * Here the number of oldbins (2) < number of newbins (4).
		 * Thus we will add nothing in the oldbin array.
		 * Size of oldbin array is now 0.
		 * Iteration1:
		 * 		sbin1 of bin1 from oldbins.
		 * 		sbin2 of bin1 from newbins
		 *		Match sbin1 and sbin2. They will match. Do nothing
		 *		Size of oldbin array = 0 and newbin array = 0
		 *
		 * Iteration2:
		 * 		sbin1 of bin2 from oldbins
		 * 		sbin2 of bin2 from newbins
		 *		Match sbin1 and sbin2. They will not match. Add them to their respective array.
		 *		Size of oldbin array = 1 and newbin array = 1
		 * Iteration3 and Iteration4:
		 *  	Create sbins of bin3 and bin4 and put them in newbin array.
		 *  	Size of oldbin array = 1 and newbin array = 3
		 * End:
		 * 		Deletes all the sbins of oldbin array from secondary index trees
		 * 		Inesert all the sbins of newbin array from secondary index trees
		 */

		bool has_sindex   = as_sindex_ns_has_sindex(ns);
		int sindex_found  = 0;

		if (has_sindex) {
			SINDEX_GRLOCK();
		}
		SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);

		if (! rd.ns->single_bin) {
			int32_t delta_bins = (int32_t)block->n_bins - (int32_t)rd.n_bins;

			if (delta_bins) {
				uint16_t new_size = (uint16_t)block->n_bins;
				if ((delta_bins < 0) && has_sindex) {
					sindex_found += as_sindex_sbins_from_rd(&rd, new_size, old_n_bins, sbins, AS_SINDEX_OP_DELETE);
				}
				as_bin_allocate_bin_space(r, &rd, delta_bins);
			}
		}
		const char * set_name = as_index_get_set_name(r, ns);
		for (uint16_t i = 0; i < block->n_bins; i++) {
			as_bin* b;
			if (i < old_n_bins) {
				b = &rd.bins[i];
				if (has_sindex) {	
					sindex_found += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sindex_found], AS_SINDEX_OP_DELETE);
				}
				as_bin_set_version(b, ssd_bin->version, ns->single_bin);
				as_bin_set_id_from_name(ns, b, ssd_bin->name);
			}
			else {
				b = as_bin_create(r, &rd, (uint8_t*)ssd_bin->name,
						strlen(ssd_bin->name), ssd_bin->version);
			}

			ssd_populate_bin(b, ssd_bin, block_head, ns->single_bin, true);
			ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);

			if (has_sindex) {
				sindex_found += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sindex_found], AS_SINDEX_OP_INSERT);
			}
		}

		if (has_sindex) {
			SINDEX_GUNLOCK();
			// Delete should precede insert.
			if (sindex_found > 0) {
				as_sindex_update_by_sbin(ns, as_index_get_set_name(r, ns), sbins, sindex_found, &rd.keyd);
				as_sindex_sbin_freeall(sbins, sindex_found);
			}
		}

		uint64_t end_bytes_memory = as_storage_record_get_n_bytes_memory(&rd);
		int64_t delta_bytes = end_bytes_memory - bytes_memory;

		if (delta_bytes) {
			cf_atomic_int_add(&ns->n_bytes_memory, delta_bytes);
			cf_atomic_int_add(&p_partition->n_bytes_memory, delta_bytes);
		}

		as_storage_record_close(r, &rd);
	}

	// If replacing an existing record, undo its previous storage accounting.
	if (STORAGE_RBLOCK_IS_VALID(r->storage_key.ssd.rblock_id)) {
		ssd_block_free(&ssds->ssds[r->storage_key.ssd.file_id],
				r->storage_key.ssd.rblock_id, r->storage_key.ssd.n_rblocks,
				"record-add");
		ssd->record_add_replace_counter++;
	}
	else {
		ssd->record_add_unique_counter++;
	}

	// Update storage accounting to include this record.
	// TODO - pass in wblock_id when we do boundary check in sweep.
	uint32_t wblock_id = RBLOCK_ID_TO_WBLOCK_ID(ssd, rblock_id);
	// TODO - pass in size instead of n_rblocks.
	uint32_t size = (uint32_t)RBLOCKS_TO_BYTES(n_rblocks);

	ssd->inuse_size += size;
	ssd->alloc_table->wblock_state[wblock_id].inuse_sz += size;

	// Set/reset the record's storage information.
	r->storage_key.ssd.file_id = ssd->file_id;
	r->storage_key.ssd.rblock_id = rblock_id;
	r->storage_key.ssd.n_rblocks = n_rblocks;

	// Make sure subrecord sweep happens.
	if (is_ldt_parent) {
		ssd->has_ldt = true;
	}

	as_record_done(&r_ref, ns);

	return 0;
}


// Sweep through storage devices and rebuild the index.
//
// If there are LDT records the sweep is done twice, once for LDT parent records
// and then again for LDT subrecords.
int
ssd_load_device_sweep(drv_ssds *ssds, drv_ssd *ssd)
{
	uint8_t *buf = cf_valloc(LOAD_BUF_SIZE);

	bool read_shadow = ssd->shadow_name && ! ssd->sub_sweep;
	char *read_ssd_name = read_shadow ? ssd->shadow_name : ssd->name;
	int fd = read_shadow ? ssd_shadow_fd_get(ssd) : ssd_fd_get(ssd);
	int write_fd = read_shadow ? ssd_fd_get(ssd) : -1;

	// Seek past the header.
	off_t file_offset = ssds->header->header_length;

	if (lseek(fd, file_offset, SEEK_SET) != file_offset) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: offset %ld: errno %d (%s)",
				read_ssd_name, file_offset, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Finished;
	}

	if (read_shadow && lseek(write_fd, file_offset, SEEK_SET) != file_offset) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: offset %ld: errno %d (%s)",
				ssd->name, file_offset, errno, cf_strerror(errno));
	}

	int error_count = 0;

	ssd->cold_start_block_counter = file_offset / LOAD_BUF_SIZE;

	// Loop over all blocks in device.
	while (true) {
		ssize_t rlen = read(fd, buf, LOAD_BUF_SIZE);

		if (rlen != LOAD_BUF_SIZE) {
			cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
					read_ssd_name, rlen, errno, cf_strerror(errno));
			close(fd);
			fd = -1;
			goto Finished;
		}

		if (read_shadow) {
			// TODO - ok to always write 1Mb blocks?
			ssize_t sz = write(write_fd, (void*)buf, LOAD_BUF_SIZE);

			if (sz != LOAD_BUF_SIZE) {
				cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
						ssd->name, errno, cf_strerror(errno));
			}
		}

		size_t block_offset = 0; // current offset within the 1M block, in bytes

		while (block_offset < LOAD_BUF_SIZE) {
			drv_ssd_block *block = (drv_ssd_block*)&buf[block_offset];

			// Look for record magic.
			if (block->magic != SSD_BLOCK_MAGIC) {
				// No record found here.
				// (Includes normal case of nothing ever written here).

				block_offset += RBLOCK_SIZE;

				// We always write some at the start of a 1M block.
				if (block_offset == RBLOCK_SIZE) {
					error_count++;
					goto NextBlock;
				}
				else {
					// Otherwise check the next rblock, looking for magic.
					continue;
				}
			}

			// Note - if block->length is sane, we don't need to round up to a
			// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
			size_t next_block_offset = block_offset +
					BYTES_TO_RBLOCK_BYTES(block->length + SIGNATURE_OFFSET);

			// Sanity-check for 1M block overruns.
			// TODO - check write_block_size boundaries!
			if (next_block_offset > LOAD_BUF_SIZE) {
				cf_warning(AS_DRV_SSD, "error: block extends over read size: foff %"PRIu64" boff %"PRIu64" blen %"PRIu64,
					file_offset, block_offset, (uint64_t)block->length);

				error_count++;
				goto NextBlock;
			}

			// Check signature.
			if (ssd->use_signature && block->sig) {
				cf_signature sig;

				cf_signature_compute(((uint8_t*)block) + SIGNATURE_OFFSET,
						block->length, &sig);

				if (sig != block->sig) {
					block_offset += RBLOCK_SIZE;
					ssd->record_add_sigfail_counter++;

					// Check the next rblock, looking for magic.
					continue;
				}
			}

			// Found a record - try to add it to the index.
			int add_rv = ssd_record_add(ssds, ssd, block,
					BYTES_TO_RBLOCKS(file_offset + block_offset),
					(uint32_t)BYTES_TO_RBLOCKS(next_block_offset - block_offset));

			if (add_rv == -2) {
				cf_warning(AS_DRV_SSD, "disk restore: hit high water limit before disk entirely loaded.");
				goto Finished;
			}

			if (add_rv == -3) {
				error_count++;
				goto NextBlock;
			}

			error_count = 0;
			block_offset = next_block_offset;
		}

NextBlock:

		// If we encounter enough 1M blocks that have no records, assume we've
		// read all our data and we're done.
		if (error_count > 10) {
			goto Finished;
		}

		file_offset += LOAD_BUF_SIZE;
		ssd->cold_start_block_counter++;
	}

Finished:

	ssd->cold_start_block_counter = ssd->file_size / LOAD_BUF_SIZE;

	if (fd != -1) {
		read_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);
	}

	if (write_fd != -1) {
		ssd_fd_put(ssd, write_fd);
	}

	cf_free(buf);

	return 0;
}


#ifdef USE_JEM
#include <sys/syscall.h>
#include <unistd.h>
#endif

typedef struct {
	drv_ssds *ssds;
	drv_ssd *ssd;
	cf_queue *complete_q;
	void *complete_udata;
	void *complete_rc;
} ssd_load_devices_data;

// Thread "run" function to read a device and rebuild the index.
void *
ssd_load_devices_fn(void *udata)
{
	ssd_load_devices_data *ldd = (ssd_load_devices_data*)udata;
	drv_ssd *ssd = ldd->ssd;
	drv_ssds *ssds = ldd->ssds;
	cf_queue *complete_q = ldd->complete_q;
	void *complete_udata = ldd->complete_udata;
	void *complete_rc = ldd->complete_rc;

	cf_free(ldd);
	ldd = 0;

	cf_info(AS_DRV_SSD, "device %s: reading device to load index", ssd->name);

#ifdef USE_JEM
	int tid = syscall(SYS_gettid);
	cf_info(AS_DRV_SSD, "In TID %d: Using arena #%d for loading data for namespace \"%s\"",
			tid, ssds->ns->jem_arena, ssds->ns->name);

	// Allocate long-term storage in this namespace's JEMalloc arena.
	jem_set_arena(ssds->ns->jem_arena);
#endif

	ssd->sub_sweep	= false;
	ssd->has_ldt	= false;

	ssd_load_device_sweep(ssds, ssd);

	if (ssds->ns->ldt_enabled && ssd->has_ldt) {
		cf_info(AS_DRV_SSD, "device %s: reading device again to load subrecords",
				ssd->name);
		ssd->sub_sweep = true;
		ssd_load_device_sweep(ssds, ssd);
	}

	cf_info(AS_DRV_SSD, "device %s: read complete: UNIQUE %"PRIu64" (REPLACED %"PRIu64") (GEN %"PRIu64") (EXPIRED %"PRIu64") (MAX-TTL %"PRIu64") records",
		ssd->name, ssd->record_add_unique_counter,
		ssd->record_add_replace_counter, ssd->record_add_generation_counter,
		ssd->record_add_expired_counter, ssd->record_add_max_ttl_counter);

	if (ssd->record_add_sigfail_counter) {
		cf_warning(AS_DRV_SSD, "devices %s: WARNING: %"PRIu64" elements could not be read due to signature failure. Possible hardware errors.",
			ssd->record_add_sigfail_counter);
	}

	if (0 == cf_rc_release(complete_rc)) {
		// All drives are done reading.

		ssds->ns->cold_start_loading = false;
		ssd_load_wblock_queues(ssds);

		pthread_mutex_destroy(&ssds->ns->cold_start_evict_lock);

		cf_queue_push(complete_q, &complete_udata);
		cf_rc_free(complete_rc);

		ssd_start_maintenance_threads(ssds);
		ssd_start_write_worker_threads(ssds);
		ssd_start_defrag_threads(ssds);
	}

	return 0;
}


void
ssd_load_devices_load(drv_ssds *ssds, cf_queue *complete_q, void *udata)
{
	ssds->ns->cold_start_loading = true;

	void *p = cf_rc_alloc(1);

	for (int i = 1; i < ssds->n_ssds; i++) {
		cf_rc_reserve(p);
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		ssd_load_devices_data *ldd = cf_malloc(sizeof(ssd_load_devices_data));

		if (! ldd) {
			cf_crash(AS_DRV_SSD, "memory allocation in device load");
		}

		ldd->ssds = ssds;
		ldd->ssd = ssd;
		ldd->complete_q = complete_q;
		ldd->complete_udata = udata;
		ldd->complete_rc = p;

		pthread_create(&ssd->load_device_thread, 0, ssd_load_devices_fn, ldd);
	}
}


void
ssd_load_devices_init_header_length(drv_ssds *ssds)
{
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->header_size = ssds->header->header_length;
	}
}


//==========================================================
// Generic startup utilities.
//

static int
first_used_device(ssd_device_header *headers[], int n_ssds)
{
	for (int i = 0; i < n_ssds; i++) {
		if (headers[i]->random != 0) {
			return i;
		}
	}

	return -1;
}


bool
ssd_load_devices(drv_ssds *ssds, cf_queue *complete_q, void *udata)
{
	uint64_t random = cf_get_rand64();

	int n_ssds = ssds->n_ssds;
	as_namespace *ns = ssds->ns;

	ssd_device_header *headers[n_ssds];

	// Check all the headers. Pick one as the representative.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		int rvh = as_storage_read_header(ssd, ns, &headers[i]);

		if (rvh == -1) {
			cf_crash(AS_DRV_SSD, "unable to read disk header %s: %s",
					ssd->name, cf_strerror(errno));
		}

		if (rvh == -2) {
			headers[i] = as_storage_init_header(ns);
		}
	}

	int first_used = first_used_device(headers, n_ssds);

	if (first_used == -1) {
		// Shouldn't find all fresh headers here during warm restart.
		if (! ns->cold_start) {
			// There's no going back to cold start now - do so the harsh way.
			cf_crash(AS_DRV_SSD, "ns %s: found all %d devices fresh during warm restart",
					ns->name, n_ssds);
		}

		cf_info(AS_DRV_SSD, "namespace %s: found all %d devices fresh, initializing to random %"PRIu64,
				ns->name, n_ssds, random);

		ssds->header = headers[0];

		for (int i = 1; i < n_ssds; i++) {
			cf_free(headers[i]);
		}

		ssds->header->random = random;
		ssds->header->devices_n = n_ssds;
		as_storage_info_flush_ssd(ns);

		ssd_load_devices_init_header_length(ssds);

		return true;
	}

	// At least one device is not fresh. Check that all non-fresh devices match.

	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		// Skip fresh devices.
		if (headers[i]->random == 0) {
			ssd->started_fresh = true; // warm restart needs to know
			continue;
		}

		if (headers[first_used]->random != headers[i]->random) {
			cf_crash(AS_DRV_SSD, "namespace %s: drive set with unmatched headers - devices %s & %s have different signatures",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (headers[first_used]->devices_n != headers[i]->devices_n) {
			cf_crash(AS_DRV_SSD, "namespace %s: drive set with unmatched headers - devices %s & %s have different device counts",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (headers[first_used]->last_evict_void_time !=
				headers[i]->last_evict_void_time) {
			cf_warning(AS_DRV_SSD, "namespace %s: devices have inconsistent evict-void-times - ignoring",
					ns->name);
			headers[first_used]->last_evict_void_time = 0;
		}
	}

	// Drive set OK - fix up header set.
	ssds->header = headers[first_used];
	headers[first_used] = 0;

	for (int i = 0; i < n_ssds; i++) {
		if (headers[i]) {
			cf_free(headers[i]);
			headers[i] = 0;
		}
	}

	ssds->header->random = random;
	ssds->header->devices_n = n_ssds; // may have added fresh drives
	as_storage_info_flush_ssd(ns);

	as_partition_get_state_from_storage(ssds->ns, ssds->get_state_from_storage);

	ssd_load_devices_init_header_length(ssds);

	// Warm restart - imitate device loading by reducing resumed index.
	if (! ns->cold_start) {
		ssd_resume_devices(ssds);

		return true;
	}

	// Initialize the cold-start eviction machinery.

	if (0 != pthread_mutex_init(&ns->cold_start_evict_lock, NULL)) {
		cf_crash(AS_DRV_SSD, "failed cold-start eviction mutex init");
	}

	uint32_t now = as_record_void_time_get();

	if (ns->cold_start_evict_ttl == 0xFFFFffff) {
		// Config file did NOT specify cold-start-evict-ttl.
		ns->cold_start_threshold_void_time = ssds->header->last_evict_void_time;

		// Check that it's not already in the past. (Note - includes 0.)
		if (ns->cold_start_threshold_void_time < now) {
			ns->cold_start_threshold_void_time = now;
		}
		else {
			cf_info(AS_DRV_SSD, "namespace %s: using saved cold start evict-ttl %u",
					ns->name, ns->cold_start_threshold_void_time - now);
		}
	}
	else {
		// Config file specified cold-start-evict-ttl. (0 is a valid value.)
		ns->cold_start_threshold_void_time = now + ns->cold_start_evict_ttl;

		cf_info(AS_DRV_SSD, "namespace %s: using config-specified cold start evict-ttl %u",
				ns->name, ns->cold_start_evict_ttl);
	}

	ns->cold_start_max_void_time = now + (uint32_t)ns->max_ttl;

	// Fire off threads to load in data - will signal completion when threads
	// are all done.
	ssd_load_devices_load(ssds, complete_q, udata);

	// Make sure caller doesn't signal completion.
	return false;
}


// Set a device's system block scheduler mode.
static int
ssd_set_scheduler_mode(const char* device_name, const char* mode)
{
	if (strncmp(device_name, "/dev/", 5)) {
		cf_warning(AS_DRV_SSD, "storage: invalid device name %s, did not set scheduler mode",
				device_name);
		return -1;
	}

	char device_tag[(strlen(device_name) - 5) + 1];

	strcpy(device_tag, device_name + 5);

	// Replace any slashes in the device tag with '!' - this is the naming
	// convention in /sys/block.
	char* p_char = device_tag;

	while (*p_char) {
		if (*p_char == '/') {
			*p_char = '!';
		}

		p_char++;
	}

	// If the device name ends with a number, assume it's a partition name and
	// removing the number gives the raw device name.
	if (p_char != device_tag) {
		p_char--;

		if (*p_char >= '0' && *p_char <= '9') {
			*p_char = 0;
		}
	}

	char scheduler_file_name[11 + strlen(device_tag) + 16 + 1];

	strcpy(scheduler_file_name, "/sys/block/");
	strcat(scheduler_file_name, device_tag);
	strcat(scheduler_file_name, "/queue/scheduler");

	FILE* scheduler_file = fopen(scheduler_file_name, "w");

	if (! scheduler_file) {
		cf_warning(AS_DRV_SSD, "storage: couldn't open %s, did not set scheduler mode: %s",
				scheduler_file_name, cf_strerror(errno));
		return -1;
	}

	if (fwrite(mode, strlen(mode), 1, scheduler_file) != 1) {
		fclose(scheduler_file);

		cf_warning(AS_DRV_SSD, "storage: couldn't write %s to %s, did not set scheduler mode",
				mode, scheduler_file_name);
		return -1;
	}

	fclose(scheduler_file);

	cf_info(AS_DRV_SSD, "storage: set device %s scheduler mode to %s",
			device_name, mode);

	return 0;
}


static void
check_write_block_size(uint32_t write_block_size)
{
	if (write_block_size > MAX_WRITE_BLOCK_SIZE) {
		cf_crash(AS_DRV_SSD, "attempted to configure write block size in excess of %u",
				MAX_WRITE_BLOCK_SIZE);
	}

	if (LOAD_BUF_SIZE % write_block_size != 0 ||
			SSD_DEFAULT_HEADER_LENGTH % write_block_size != 0) {
		cf_crash(AS_DRV_SSD, "attempted to configure non-round write block size %u",
				write_block_size);
	}
}


static off_t
check_file_size(off_t file_size, const char *tag)
{
	if (sizeof(off_t) <= 4) {
		cf_warning(AS_DRV_SSD, "this OS supports only 32-bit (4g) files - compile with 64 bit offsets");
	}

	if (file_size > SSD_DEFAULT_HEADER_LENGTH) {
		off_t unusable_size =
				(file_size - SSD_DEFAULT_HEADER_LENGTH) % LOAD_BUF_SIZE;

		if (unusable_size != 0) {
			cf_info(AS_DRV_SSD, "%s size must be header size %u + multiple of %u, rounding down",
					tag, SSD_DEFAULT_HEADER_LENGTH, LOAD_BUF_SIZE);
			file_size -= unusable_size;
		}
	}

	if (file_size <= SSD_DEFAULT_HEADER_LENGTH) {
		cf_crash(AS_DRV_SSD, "%s size %"PRIu64" must be greater than header size %d",
				tag, file_size, SSD_DEFAULT_HEADER_LENGTH);
	}

	return file_size;
}


static uint64_t
find_io_min_size(int fd, const char *ssd_name)
{
	off_t off = lseek(fd, 0, SEEK_SET);

	if (off != 0) {
		cf_crash(AS_DRV_SSD, "%s: seek error %s", ssd_name, cf_strerror(errno));
	}

	uint8_t *buf = cf_valloc(HI_IO_MIN_SIZE);
	size_t read_sz = LO_IO_MIN_SIZE;

	while (read_sz <= HI_IO_MIN_SIZE) {
		if (read(fd, (void*)buf, read_sz) == (ssize_t)read_sz) {
			cf_free(buf);
			return read_sz;
		}

		read_sz <<= 1; // LO_IO_MIN_SIZE and HI_IO_MIN_SIZE are powers of 2
	}

	cf_crash(AS_DRV_SSD, "%s: read failed at all sizes from %u to %u bytes",
			ssd_name, LO_IO_MIN_SIZE, HI_IO_MIN_SIZE);

	return 0;
}


int
ssd_init_devices(as_namespace *ns, drv_ssds **ssds_p)
{
	int n_ssds;

	for (n_ssds = 0; n_ssds < AS_STORAGE_MAX_DEVICES; n_ssds++) {
		if (! ns->storage_devices[n_ssds]) {
			break;
		}
	}

	size_t ssds_size = sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd));
	drv_ssds *ssds = cf_malloc(ssds_size);

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "failed drv_ssds malloc");
		return -1;
	}

	memset(ssds, 0, ssds_size);
	ssds->n_ssds = n_ssds;
	ssds->ns = ns;

	// Raw device-specific initialization of drv_ssd structures.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->name = ns->storage_devices[i];

		ssd->open_flag = O_RDWR |
				(ns->storage_disable_odirect ? 0 : O_DIRECT) |
				(ns->storage_enable_osync ? O_SYNC : 0);

		int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open device %s: %s", ssd->name,
					cf_strerror(errno));
			return -1;
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		ssd->file_size = check_file_size((off_t)size, "usable device");
		ssd->io_min_size = find_io_min_size(fd, ssd->name);

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (! as_storage_empty_header(fd, ssd->name)) {
				close(fd);
				return -1;
			}

			cf_info(AS_DRV_SSD, "cold-start-empty - erased header of %s",
					ssd->name);
		}

		close(fd);

		ns->ssd_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "opened device %s: usable size %lu, io-min-size %lu",
				ssd->name, ssd->file_size, ssd->io_min_size);

		if (ns->storage_scheduler_mode) {
			// Set scheduler mode specified in config file.
			ssd_set_scheduler_mode(ssd->name, ns->storage_scheduler_mode);
		}
	}

	*ssds_p = ssds;

	return 0;
}


int
ssd_init_shadows(as_namespace *ns, drv_ssds *ssds)
{
	int n_shadows = 0;

	for (int n = 0; n < ssds->n_ssds; n++) {
		if (ns->storage_shadows[n]) {
			n_shadows++;
		}
	}

	if (n_shadows == 0) {
		// No shadows - a normal deployment.
		return 0;
	}

	if (n_shadows != ssds->n_ssds) {
		cf_warning(AS_DRV_SSD, "configured %d devices but only %d shadows",
				ssds->n_ssds, n_shadows);
		return -1;
	}

	// Check shadow devices.
	for (int i = 0; i < n_shadows; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->shadow_name = ns->storage_shadows[i];

		int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open shadow device %s: %s",
					ssd->name, cf_strerror(errno));
			return -1;
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		if ((off_t)size < ssd->file_size) {
			cf_warning(AS_DRV_SSD, "shadow device %s is smaller than main device - %lu < %lu",
					ssd->shadow_name, size, ssd->file_size);
			close(fd);
			return -1;
		}

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (! as_storage_empty_header(fd, ssd->shadow_name)) {
				close(fd);
				return -1;
			}

			cf_info(AS_DRV_SSD, "cold-start-empty - erased header of %s",
					ssd->shadow_name);
		}

		close(fd);

		cf_info(AS_DRV_SSD, "shadow device %s is compatible with main device");

		if (ns->storage_scheduler_mode) {
			// Set scheduler mode specified in config file.
			ssd_set_scheduler_mode(ssd->shadow_name,
					ns->storage_scheduler_mode);
		}
	}

	return 0;
}


int
ssd_init_files(as_namespace *ns, drv_ssds **ssds_p)
{
	int n_ssds;

	for (n_ssds = 0; n_ssds < AS_STORAGE_MAX_FILES; n_ssds++) {
		if (! ns->storage_files[n_ssds]) {
			break;
		}
	}

	size_t ssds_size = sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd));
	drv_ssds *ssds = cf_malloc(ssds_size);

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "failed drv_ssds malloc");
		return -1;
	}

	memset(ssds, 0, ssds_size);
	ssds->n_ssds = n_ssds;
	ssds->ns = ns;

	// File-specific initialization of drv_ssd structures.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->name = ns->storage_files[i];

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (0 == remove(ssd->name)) {
				cf_info(AS_DRV_SSD, "cold-start-empty - removed %s", ssd->name);
			}
			else if (errno == ENOENT) {
				cf_info(AS_DRV_SSD, "cold-start-empty - no file %s", ssd->name);
			}
			else {
				cf_warning(AS_DRV_SSD, "failed remove: errno %d", errno);
				return -1;
			}
		}

		ssd->open_flag = O_RDWR;

		// Validate that file can be opened, create it if it doesn't exist.
		int fd = open(ssd->name, ssd->open_flag | O_CREAT, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open file %s: %s", ssd->name,
					cf_strerror(errno));
			return -1;
		}

		ssd->file_size = check_file_size(ns->storage_filesize, "file");
		ssd->io_min_size = LO_IO_MIN_SIZE;

		// Truncate will grow or shrink the file to the correct size.
		if (0 != ftruncate(fd, ssd->file_size)) {
			cf_warning(AS_DRV_SSD, "unable to truncate file: errno %d", errno);
			close(fd);
			return -1;
		}

		close(fd);

		ns->ssd_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "opened file %s: usable size %lu", ssd->name,
				ssd->file_size);
	}

	*ssds_p = ssds;

	return 0;
}


//==========================================================
// Storage API implementation: startup, shutdown, etc.
//

int
as_storage_namespace_init_ssd(as_namespace *ns, cf_queue *complete_q,
		void *udata)
{
	drv_ssds *ssds;

	if (ns->storage_devices[0]) {
		if (0 != ssd_init_devices(ns, &ssds)) {
			cf_warning(AS_DRV_SSD, "ns %s can't initialize devices", ns->name);
			return -1;
		}

		if (0 != ssd_init_shadows(ns, ssds)) {
			cf_warning(AS_DRV_SSD, "ns %s can't initialize shadows", ns->name);
			return -1;
		}
	}
	else if (ns->storage_files[0]) {
		if (0 != ssd_init_files(ns, &ssds)) {
			cf_warning(AS_DRV_SSD, "ns %s can't initialize files", ns->name);
			return -1;
		}
	}
	else {
		cf_warning(AS_DRV_SSD, "ns %s has no devices or files", ns->name);
		return -1;
	}

	// Allow defrag to go full speed during startup - restore the configured
	// settings when startup is done.
	ns->saved_defrag_sleep = ns->storage_defrag_sleep;
	ns->saved_write_smoothing_period = ns->storage_write_smoothing_period;
	ns->storage_defrag_sleep = 0;
	ns->storage_write_smoothing_period = 0;

	check_write_block_size(ns->storage_write_block_size);

	// The queue limit is more efficient to work with.
	ns->storage_max_write_q = (int)
			(ns->storage_max_write_cache / ns->storage_write_block_size);

	// Minimize how often we recalculate this.
	ns->defrag_lwm_size =
			(ns->storage_write_block_size * ns->storage_defrag_lwm_pct) / 100;

	ns->storage_private = (void*)ssds;

	// Finish initializing drv_ssd structures (non-zero-value members).
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->ns = ns;
		ssd->file_id = i;

		pthread_mutex_init(&ssd->LOCK, 0);

		ssd->running = true;

		ssd->use_signature = ns->storage_signature;
		ssd->data_in_memory = ns->storage_data_in_memory;
		ssd->write_block_size = ns->storage_write_block_size;

		ssd_wblock_init(ssd);

		// Note: free_wblock_q, defrag_wblock_q created after loading devices.

		if (! (ssd->fd_q = cf_queue_create(sizeof(int), true))) {
			cf_crash(AS_DRV_SSD, "can't create fd queue");
		}

		if (ssd->shadow_name &&
				! (ssd->shadow_fd_q = cf_queue_create(sizeof(int), true))) {
			cf_crash(AS_DRV_SSD, "can't create shadow fd queue");
		}

		if (! (ssd->swb_write_q = cf_queue_create(sizeof(void*), true))) {
			cf_crash(AS_DRV_SSD, "can't create swb-write queue");
		}

		if (! (ssd->swb_free_q = cf_queue_create(sizeof(void*), true))) {
			cf_crash(AS_DRV_SSD, "can't create swb-free queue");
		}

		if (! ns->storage_data_in_memory) {
			if (! (ssd->post_write_q = cf_queue_create(sizeof(void*), false))) {
				cf_crash(AS_DRV_SSD, "can't create post-write queue");
			}
		}

		char histname[HISTOGRAM_NAME_SIZE];

		snprintf(histname, sizeof(histname), "SSD_READ_%d %s", i, ssd->name);

		if (! (ssd->hist_read = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
		}

		snprintf(histname, sizeof(histname), "SSD_LARGE_BLOCK_READ_%d %s", i, ssd->name);

		if (! (ssd->hist_large_block_read = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD,"cannot create histogram %s", histname);
		}

		snprintf(histname, sizeof(histname), "SSD_WRITE_%d %s", i, ssd->name);

		if (! (ssd->hist_write = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
		}

		if (ssd->shadow_name) {
			snprintf(histname, sizeof(histname), "SSD_SHADOW_WRITE_%d %s", i, ssd->name);

			if (! (ssd->hist_shadow_write = histogram_create(histname, HIST_MILLISECONDS))) {
				cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
			}
		}

		snprintf(histname, sizeof(histname), "SSD_FSYNC_%d %s", i, ssd->name);

		if (! (ssd->hist_fsync = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
		}
	}

	// Attempt to load the data.
	//
	// Return value 'false' means it's going to take a long time and will later
	// asynchronously signal completion via the complete_q, 'true' means it's
	// finished, signal here.

	if (ssd_load_devices(ssds, complete_q, udata)) {
		ssd_load_wblock_queues(ssds);

		cf_queue_push(complete_q, &udata);

		ssd_start_maintenance_threads(ssds);
		ssd_start_write_worker_threads(ssds);
		ssd_start_defrag_threads(ssds);
	}

	return 0;
}


void
as_storage_cold_start_ticker_ssd()
{
	for (uint32_t i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (ns->cold_start_loading) {
			char buf[2048];
			int pos = 0;
			drv_ssds *ssds = (drv_ssds*)ns->storage_private;

			for (int j = 0; j < ssds->n_ssds; j++) {
				drv_ssd *ssd = &ssds->ssds[j];
				uint32_t pct = (ssd->cold_start_block_counter * 100) /
						(ssd->file_size / LOAD_BUF_SIZE);

				pos += sprintf(buf + pos, ", %s %u%%", ssd->name, pct);
			}

			cf_info(AS_DRV_SSD, "{%s} loaded %lu records, %lu subrecords%s",
					ns->name, ns->n_objects, ns->n_sub_objects, buf);
		}
	}
}


int
as_storage_namespace_destroy_ssd(as_namespace *ns)
{
	// This is not called - for now we don't bother unwinding.
	return 0;
}


int
as_storage_namespace_attributes_get_ssd(as_namespace *ns,
		as_storage_attributes *attr)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	attr->n_devices = ssds->n_ssds;

	return 0;
}


// Note that this is *NOT* the counterpart to as_storage_record_create_ssd()!
// That would be as_storage_record_close_ssd(). This is what gets called when a
// record is destroyed, to dereference storage.
int
as_storage_record_destroy_ssd(as_namespace *ns, as_record *r)
{
	if (STORAGE_RBLOCK_IS_VALID(r->storage_key.ssd.rblock_id) &&
			r->storage_key.ssd.n_rblocks != 0) {
		drv_ssds *ssds = (drv_ssds*)ns->storage_private;
		drv_ssd *ssd = &ssds->ssds[r->storage_key.ssd.file_id];

		ssd_block_free(ssd, r->storage_key.ssd.rblock_id,
				r->storage_key.ssd.n_rblocks, "destroy");

		r->storage_key.ssd.rblock_id = STORAGE_INVALID_RBLOCK;
		r->storage_key.ssd.n_rblocks = 0;
	}

	return 0;
}


//==========================================================
// Storage API implementation: as_storage_rd cycle.
//

int
as_storage_record_create_ssd(as_namespace *ns, as_record *r, as_storage_rd *rd,
		cf_digest *keyd)
{
	rd->u.ssd.block = 0;
	rd->u.ssd.must_free_block = NULL;
	rd->u.ssd.ssd = 0;

	// Should already look like this, but ...
	r->storage_key.ssd.file_id = STORAGE_INVALID_FILE_ID;
	r->storage_key.ssd.rblock_id = STORAGE_INVALID_RBLOCK;
	r->storage_key.ssd.n_rblocks = 0;

	return 0;
}


int
as_storage_record_open_ssd(as_namespace *ns, as_record *r, as_storage_rd *rd,
		cf_digest *keyd)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	rd->u.ssd.block = 0;
	rd->u.ssd.must_free_block = NULL;
	rd->u.ssd.ssd = &ssds->ssds[r->storage_key.ssd.file_id];

	return 0;
}


void
as_storage_record_close_ssd(as_record *r, as_storage_rd *rd)
{
	// All record writes come through here!
	if (rd->write_to_device && as_bin_inuse_has(rd)) {
		ssd_write(r, rd);
	}

	if (rd->u.ssd.must_free_block) {
		cf_free(rd->u.ssd.must_free_block);
	}
}


// These are near the top of this file:
//		as_storage_record_get_n_bins_ssd()
//		as_storage_record_read_ssd()
//		as_storage_particle_read_all_ssd()
//		as_storage_particle_read_and_size_all_ssd()


bool
as_storage_record_can_fit_ssd(as_storage_rd *rd)
{
	rd->flat_size =
			as_storage_record_overhead_size(rd) +
			(rd->n_bins_to_write * sizeof(drv_ssd_bin)) +
			rd->particles_flat_size;

	return rd->ns->storage_write_block_size >= rd->flat_size;
}


bool
as_storage_record_size_and_check_ssd(as_storage_rd *rd)
{
	rd->flat_size = as_storage_record_size(rd);

	return rd->ns->storage_write_block_size >= rd->flat_size;
}


//==========================================================
// Storage API implementation: storage capacity monitoring.
//

void
as_storage_wait_for_defrag_ssd(as_namespace *ns)
{
	if (ns->storage_defrag_startup_minimum > 0) {
		while (true) {
			int avail_pct;

			if (0 != as_storage_stats_ssd(ns, &avail_pct, 0)) {
				cf_crash(AS_DRV_SSD, "namespace %s storage stats failed",
						ns->name);
			}

			if (avail_pct >= ns->storage_defrag_startup_minimum) {
				break;
			}

			cf_info(AS_DRV_SSD, "namespace %s waiting for defrag: %d pct available, waiting for %d ...",
					ns->name, avail_pct, ns->storage_defrag_startup_minimum);

			sleep(2);
		}
	}

	// Restore configured defrag throttling values.
	ns->storage_defrag_sleep = ns->saved_defrag_sleep;
	ns->storage_write_smoothing_period = ns->saved_write_smoothing_period;

	// Set the "floor" for wblock usage. Must come after startup defrag so it
	// doesn't prevent defrag from resurrecting a drive that hit the floor.

	// Data-in-memory namespaces process transactions in service threads.
	int n_service_threads = ns->storage_data_in_memory ?
			g_config.n_service_threads : 0;

	int n_transaction_threads = g_config.use_queue_per_device ?
			g_config.n_transaction_threads_per_queue :
			g_config.n_transaction_queues * g_config.n_transaction_threads_per_queue;

	ns->storage_min_free_wblocks =
			n_service_threads +			// client writes
			n_transaction_threads +		// client writes
			g_config.n_fabric_workers +	// migration and prole writes
			1 +							// always 1 defrag thread
			DEFRAG_RUNTIME_RESERVE +	// reserve for defrag at runtime
			DEFRAG_STARTUP_RESERVE;		// reserve for defrag at startup
	// TODO - what about UDFs?

	cf_info(AS_DRV_SSD, "{%s} floor set at %u wblocks per device", ns->name,
			ns->storage_min_free_wblocks);
}


bool
as_storage_overloaded_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	int max_write_q = ns->storage_max_write_q;

	// TODO - would be nice to not do this loop every single write transaction!
	for (int i = 0; i < ssds->n_ssds; i++) {
		int qsz = cf_queue_sz(ssds->ssds[i].swb_write_q);

		if (qsz > max_write_q) {
			cf_atomic_int_incr(&g_config.err_storage_queue_full);
			cf_warning(AS_DRV_SSD, "{%s} write fail: queue too deep: q %d, max %d",
					ns->name, qsz, max_write_q);
			return true;
		}
	}

	return false;
}


bool
as_storage_has_space_ssd(as_namespace *ns)
{
	// Shortcut - assume we can't go from 5% to 0% in 1 ticker interval.
	if (ns->storage_last_avail_pct > 5) {
		return true;
	}
	// else - running low on available percent, check rigorously...

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		if (cf_queue_sz(ssds->ssds[i].free_wblock_q) <
				ns->storage_min_free_wblocks) {
			return false;
		}
	}

	return true;
}


void
as_storage_defrag_sweep_ssd(as_namespace *ns)
{
	cf_info(AS_DRV_SSD, "{%s} sweeping all devices for wblocks to defrag ...", ns->name);

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		cf_atomic32_incr(&ssds->ssds[i].defrag_sweep);
	}
}


//==========================================================
// Storage API implementation: data in device headers.
//

int
as_storage_info_set_ssd(as_namespace *ns, uint idx, uint8_t *buf, size_t len)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (ssds == 0 || ssds->header == 0 || ssds->header->info_data == 0) {
		cf_info(AS_DRV_SSD, "illegal ssd header in namespace %s", ns->name);
		return -1;
	}

	if (idx > ssds->header->info_n)	{
		cf_info(AS_DRV_SSD, "storage info set failed: idx %d", idx);
		return -1;
	}
	if (len > ssds->header->info_stride - sizeof(info_buf)) {
		cf_info(AS_DRV_SSD, "storage info set failed: bad length %d", len);
		return -1;
	}

	info_buf *b = (info_buf*)
			(ssds->header->info_data + (ssds->header->info_stride * idx));

	b->len = len;
	memcpy(b->data, buf, len);

	return 0;
}


int
as_storage_info_get_ssd(as_namespace *ns, uint idx, uint8_t *buf, size_t *len)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (ssds == 0 || ssds->header == 0 || ssds->header->info_data == 0) {
		cf_info(AS_DRV_SSD, "illegal ssd header in namespace %s", ns->name);
		return -1;
	}

	if (idx > ssds->header->info_n)	{
		cf_info(AS_DRV_SSD, "storage info get ssd: failed: idx %d too large",
				idx);
		return -1;
	}

	info_buf *b = (info_buf*)
			(ssds->header->info_data + (ssds->header->info_stride * idx));

	if (b->len > *len || b->len > ssds->header->info_stride) {
		cf_info(AS_DRV_SSD, "storage info get ssd: bad length: from disk %d input %d stride %d",
			b->len, *len, ssds->header->info_stride);
		return -1;
	}

	*len = b->len;

	if (b->len) {
		memcpy(buf, b->data, b->len);
	}

	return 0;
}


int
as_storage_info_flush_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		as_storage_write_header(ssd, ssds->header, ssds->header->header_length);
	}

	return 0;
}


void
as_storage_save_evict_void_time_ssd(as_namespace *ns, uint32_t evict_void_time)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	ssds->header->last_evict_void_time = evict_void_time;

	// Customized write instead of using as_storage_info_flush_ssd() so we can
	// write 512-4096b instead of 1Mb (and not interfere with potentially
	// concurrent writes for partition info).

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];
		size_t peek_size = BYTES_UP_TO_IO_MIN(ssd, sizeof(ssd_device_header));

		as_storage_write_header(ssd, ssds->header, peek_size);
	}
}


//==========================================================
// Storage API implementation: statistics.
//

int
as_storage_stats_ssd(as_namespace *ns, int *available_pct,
		uint64_t *used_disk_bytes)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (available_pct) {
		*available_pct = 100;

		// Find the device with the lowest available percent.
		for (int i = 0; i < ssds->n_ssds; i++) {
			drv_ssd *ssd = &ssds->ssds[i];
			uint64_t pct = (available_size(ssd) * 100) / ssd->file_size;

			if (pct < (uint64_t)*available_pct) {
				*available_pct = pct;
			}
		}

		// Used for shortcut in as_storage_has_space_ssd(), which is done on a
		// per-transaction basis:
		ns->storage_last_avail_pct = *available_pct;
	}

	if (used_disk_bytes) {
		uint64_t sz = 0;

		for (int i = 0; i < ssds->n_ssds; i++) {
			sz += ssds->ssds[i].inuse_size;
		}

		*used_disk_bytes = sz;
	}

	return 0;
}


int
as_storage_ticker_stats_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_dump(ssd->hist_read);
		histogram_dump(ssd->hist_large_block_read);
		histogram_dump(ssd->hist_write);

		if (ssd->hist_shadow_write) {
			histogram_dump(ssd->hist_shadow_write);
		}

		histogram_dump(ssd->hist_fsync);
	}

	return 0;
}


int
as_storage_histogram_clear_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_clear(ssd->hist_read);
		histogram_clear(ssd->hist_large_block_read);
		histogram_clear(ssd->hist_write);

		if (ssd->hist_shadow_write) {
			histogram_clear(ssd->hist_shadow_write);
		}

		histogram_clear(ssd->hist_fsync);
	}

	return 0;
}


//==========================================================
// Shutdown.
//

void
as_storage_shutdown_ssd(as_namespace *ns)
{
	ns->storage_write_smoothing_period = 0;

	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		// Stop the maintenance thread from (also) flushing the current swb.
		pthread_mutex_lock(&ssd->LOCK);

		// Flush current swb by pushing it to write-q.
		if (ssd->current_swb) {
			// Clean the end of the buffer before pushing to write-q.
			if (ssd->write_block_size > ssd->current_swb->pos) {
				memset(&ssd->current_swb->buf[ssd->current_swb->pos], 0,
						ssd->write_block_size - ssd->current_swb->pos);
			}

			cf_queue_push(ssd->swb_write_q, &ssd->current_swb);
			ssd->current_swb = NULL;
		}
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		while (cf_queue_sz(ssd->swb_write_q)) {
			usleep(1000);
		}

		ssd->running = false;
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		for (uint32_t j = 0; j < ssds->ns->storage_write_threads; j++) {
			void *p_void;

			pthread_join(ssd->write_worker_thread[j], &p_void);
		}
	}
}
