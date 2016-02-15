/*
 * drv_kv.c
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 * Persistent Key-Value (KV) Store storage engine driver
 *
 */

#ifdef USE_KV
#include <kv.h>

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "queue.h"
#endif

#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "fault.h"

#include "base/datamodel.h"
#include "storage/storage.h"


#ifdef USE_KV

/* SYNOPSIS
 * Persistent Key-Value (KV) Store (e.g., FusionIO card) storage driver
 *
 * This code, like the memory storage driver, almost entirely performs no-ops,
 * because KV store state is correct already.
 */

/*
 *  Open Questions:
 *
 *    1). Do we actually need a queue of FDs and/or KV handles, or is 1 sufficient for any number of threads?
 *
 *    2). What is the current maximum size of data that can be written as a value on a FusionIO card?
 *
 *            Answer:  1MB - 512B
 *
 *    3). Under what circumstances do we need to do re-try calling the FusionIO card APIs?
 *         (e.g., when the read/written byte count is not what was expected?)
 *
 *    4). How should delete be handled?  It's possible to do a true delete using the KV API, which
 *         is more absolute than simply doing an index delete when using device storage engine.
 *         Would this interfere with any other logic at the higher level (i.e., that might make
 *         assumptions about how delete works in those other cases)?
 */


// Define to print debug messages:
//#define DEBUG

// Define to use an FD queue:
//#define USE_FD_Q

// Define to use a KV handle queue:
//#define USE_KV_H_Q

// Define to use the KV batch put API:
//#define USE_KV_BATCH_PUT

// Define to use the KV exists API:
#define USE_KV_EXISTS

// Define to perform extra read verification checks:
//#define EXTRA_CHECKS

// Define to halt when the extra checks fail:
//#define SIGINT_ON_CORRUPTION
#ifdef SIGINT_ON_CORRUPTION
#include <signal.h>
#endif

// Define the following to issue multiple, redundant read and write requests to simulate client request pipelining:
//#define DEMO2012


/*
 *  Define Constants.
 */


/*
 *  Version of the FusionIO KV API to use.
 *   (Note: Presently hard-coded.  May need to be updated for future library versions.)
 */
const uint32_t KV_API_VERSION = 1;

/*
 *  Initial number of IO vectors to allocate for batch KV puts.
 *   (Will be realloc'ed as needed.)
 */
const uint32_t KV_INITIAL_NUM_IO_VECTORS = 10000;


/*
 *  Define Types.
 */


#ifdef USE_KV_BATCH_PUT
/*
 * Type representing a batch of puts destined for a particular KV store.
 */
typedef struct kv_write_block_s {
	pthread_mutex_t LOCK;          // Lock for accessing this write block.

	/* Each IO vector represents a single put to be witten to the KV store. */
	int num_iovs_alloc;            // Number of IO vectors allocated.
	int num_iovs_used;             // Number of IO vectors currently in use.
	kv_iovec_t *iovs;              // Pointer to array of IO vectors.

	/* The data array is a slab storing the key and value data pointed to from within each IO vector. */
	size_t data_alloc;             // Bytes of data allocated.
	size_t data_used;              // Bytes of data used.
	uint8_t data[];                // The data itself.
} kv_write_block;
#endif

/*
 *  Type representing a single KV store.
 */
typedef struct drv_kv_s {
#ifdef USE_FD_Q
	cf_queue       *fd_q;          // Queue of fd's commonly used to access this KV device.
#else
	int             fd;            // Single FD used to access this KV device.
#endif
#ifdef USE_KV_H_Q
	cf_queue       *h_q;           // Queue of KV handles commonly used to access this KV device.
#else
	int             kv_h;          // Handle to the KV store on this KV device.
#endif
	int             file_id;       // Designates what subset of digests will be stored on this KV device.
	uint32_t        open_flag;     // "open(2)" flags for this KV device.
	off_t           file_size;     // Size in bytes of this KV device.
	char            name[512];     // Name of this KV device.
#ifdef USE_KV_BATCH_PUT
	int             blocks_alloc;  // Number of write blocks allocated.
	int             curr_block;    // Currently active write block index.
	int             prev_block;    // Next block to be batch put.
	kv_write_block *write_block[]; // Batch of puts destined for this KV device.
#endif
} drv_kv;

/*
 *  Type holding all of the KV stores associated with a given namespace.
 */
typedef struct drv_kvs_s {
	as_namespace   *ns;            // Namespace associated with these KVs.
	int             n_kvs;         // Number of KVs in this namespace.
	drv_kv          kvs[];         // Array of KVs in this namespace.
} drv_kvs;


// currently, bins are never discontiguous --- worry about
// long "streaming bins" later
// the blocks on disk are a set of these
//
// The offsets are from the beginning of the *BLOCK*

typedef struct drv_kv_bin_s {
	char name[AS_ID_BIN_SZ]; // 15 aligns right
	uint8_t     version;
	uint32_t    offset;     // offset of data within block
	uint32_t    len;        // length of data in bin
	uint32_t    next;       // location of next bin: block offset
} __attribute__ ((__packed__)) drv_kv_bin;

//
// a 'block' corresponds to a record.
//

#define KV_BLOCK_MAGIC 0xcdab3412

typedef struct drv_kv_block_s {
	cf_signature    sig;          // digest of this entire block, 64bits
	uint32_t        magic;
	uint32_t        length;       // total under signature - starts after this field - pointer + 16
	cf_digest       keyd;
	as_generation   generation;
	cf_clock        void_time;
	uint32_t        bins_offset;  // byte offset to bins from data
	uint32_t        n_bins;
	uint8_t         data[]; 
} __attribute__ ((__packed__)) drv_kv_block;

#define SIGNATURE_OFFSET offsetof(struct drv_kv_block_s, keyd)


/*
 *  Define internal helper functions.
 */


#ifdef DEBUG

#ifdef cf_detail
#undef cf_detail
#endif
#define cf_detail cf_info

#include <sys/param.h>  // For "MIN()".

static size_t print_buf_max = 30;

static void
print_buf(char *name, void *buf, size_t buf_len)
{
	uint8_t *char_buf = (uint8_t *) buf;
	bool printed_newline = false;
	size_t print_len = MIN(print_buf_max, buf_len);

	for (int i = 0; i < print_len; i++) {
		if (!(i % 10)) {
			fprintf(stderr, "*** %s[%04d] = ", name, i);
			printed_newline = false;
		}
		fprintf(stderr, "0x%02x ", char_buf[i]);
		if (!((i + 1) % 10)) {
		  fprintf(stderr, "***\n");
		  printed_newline = true;
		}
	}
	if (!printed_newline)
	  fprintf(stderr, "***\n");
}
#else
#define print_buf(x, y, z)
#endif

static int
kv_fd_get(drv_kv *kv)
{
#ifdef USE_FD_Q
	int rv, fd = -1;
	
	// todo: count the number of fds, wait forever if we already have a lot

	// Note:  Currently waiting forever, since NOWAIT fails with queue size 1.
	if (CF_QUEUE_OK != (rv = cf_queue_pop(kv->fd_q, (void *) &fd, CF_QUEUE_FOREVER)))
	  cf_crash(AS_DRV_KV, "cf_queue_pop() returned %d", rv);

	return(fd);
#else
	return(kv->fd);
#endif
}

static void
kv_fd_put(drv_kv *kv, int fd)
{
#ifdef USE_FD_Q
	cf_queue_push(kv->fd_q, (void *) &fd);
#else
	kv->fd = fd;
#endif
}

static int
kv_h_get(drv_kv *kv)
{
#ifdef USE_KV_H_Q
	int rv, kv_h = -1;
	
	// todo: count the number of KV handles, wait forever if we already have a lot

	// Note:  Currently waiting forever, since NOWAIT fails with queue size 1.
	if (CF_QUEUE_OK != (rv = cf_queue_pop(kv->h_q, (void *) &kv_h, CF_QUEUE_FOREVER)))
	  cf_crash(AS_DRV_KV, "cf_queue_pop() returned %d", rv);

	return(kv_h);
#else
	return(kv->kv_h);
#endif
}

static void
kv_h_put(drv_kv *kv, int kv_h)
{
#ifdef USE_KV_H_Q
	cf_queue_push(kv->h_q, (void *) &kv_h);
#else
	kv->kv_h = kv_h;
#endif
}

static inline int
kv_get_file_id(drv_kvs *kvs, cf_digest *keyd)
{
	return(keyd->digest[8] % kvs->n_kvs);
}

#ifdef USE_KV_BATCH_PUT
/*
 *  Send a batch of puts to the KV device.
 */
static int
kv_batch_flush(drv_kv *kv)
{
	int rv = 0;
	kv_write_block *wb = kv->write_block[kv->prev_block];

	// XXX -- Make sure this is multithread safe as well as efficient!!
	pthread_mutex_lock(&wb->LOCK);

	if (wb->num_iovs_used) {
		// Get the KV handle from the queue.
		int kv_h = kv_h_get(kv);

		if (0 > (rv = kv_batch_put(kv_h, wb->iovs, wb->num_iovs_used)))
		  cf_crash(AS_DRV_KV, "kv_batch_put() returned %d with %d iovectors %d bytes total data", rv, wb->num_iovs_used, wb->data_used);
		else
		  cf_detail(AS_DRV_KV, "kv_batch_put() succeeded with %d iovectors %d bytes total data", wb->num_iovs_used, wb->data_used);

		cf_info(AS_DRV_KV, "kv_batch_flush(): num. iovs: %d ; data used: %d\n", wb->num_iovs_used, wb->data_used);

		// Zero-out the batch write block.
		wb->num_iovs_used = 0;
		wb->data_used = 0;

#ifdef USE_KV_H_Q
		// Return the KV handle to the queue.
		kv_h_put(kv, kv_h);
#endif
	}

	pthread_mutex_unlock(&wb->LOCK);

	return(rv);
}
#endif

/*
 *  Note:  This function intentionally has the same arguments as "kv_put()" (plus the KV object as first argument),
 *          even though the batch put API "kv_batch_put()" currently ignores the metadata arguments.
 *          This effectively gives us plug-compatibility with the non-vector version for easy substitution.
 */
static int
kv_put_batch(drv_kv *kv, int vsl_fd, kv_key_t *key, uint32_t key_len, void *value, uint32_t value_len,
             uint32_t pool_id, uint64_t expiry, bool replace, uint32_t gen_count)
{
#ifdef USE_KV_BATCH_PUT
	int rv = 0;
	kv_write_block *wb = kv->write_block[kv->curr_block];

	// XXX -- Make sure this is multithread safe as well as efficient!!
	pthread_mutex_lock(&wb->LOCK);

	// Is there enough space left in the block for this request?
	if ((wb->data_alloc - wb->data_used) < (key_len + value_len))
	  // If not, we need to actually put the batch first.
	  rv = kv_batch_flush(kv);

	// Now queue up the incoming put request.

	memcpy(&(wb->data[wb->data_used]), key, key_len);
	wb->iovs[wb->num_iovs_used].key = &(wb->data[wb->data_used]);
	wb->data_used += key_len;

	wb->iovs[wb->num_iovs_used].key_len = key_len;

	memcpy(&(wb->data[wb->data_used]), value, value_len);
	wb->iovs[wb->num_iovs_used].value = &(wb->data[wb->data_used]);
	wb->data_used += value_len;

	rv = wb->iovs[wb->num_iovs_used].value_len = value_len;

	wb->num_iovs_used++;

	// Do we need more IO vector space?
	if (wb->num_iovs_used >= wb->num_iovs_alloc) {
		// If so, double the space.
		int new_num_iovs_alloc = wb->num_iovs_alloc * 2;
		size_t new_size = sizeof(kv_iovec_t) * new_num_iovs_alloc;
		if (!cf_realloc(wb->iovs, new_size))
		  cf_crash(AS_DRV_KV, "failed to cf_realloc() to size %d", new_size);
		wb->num_iovs_alloc = new_num_iovs_alloc;
	}

	pthread_mutex_unlock(&wb->LOCK);

	return(rv);
#else
	// Do a single put ~~ Effectively use a batch of size 1.
	return(kv_put(vsl_fd, key, key_len, value, value_len, pool_id, expiry, replace, gen_count));
#endif
}

static int
kv_write(as_record *r, as_storage_rd *rd)
{
	// copy the flattened data into the block
	uint8_t stack_buf[1024 * 16];
	int buf_len = sizeof(stack_buf);
	uint8_t *stack_buf_lim = stack_buf + buf_len;
	uint8_t *buf_start = stack_buf;
	uint8_t *buf = stack_buf;

	buf += sizeof(drv_kv_block);

	drv_kv_bin *kv_bin = 0;
	uint	write_nbins = 0;

	if (0 == rd->bins) {
		cf_warning(AS_DRV_KV, "write bins: no bin array to write from, aborting.");
		return (-1);
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *bin = &rd->bins[i];

		if (as_bin_inuse(bin)) {
			kv_bin = (drv_kv_bin *) buf;
			buf += sizeof(drv_kv_bin);

			kv_bin->version = as_bin_get_version(bin, rd->ns->single_bin);
			if (! rd->ns->single_bin) {
				strcpy(kv_bin->name, bin->name);
			}
			else {
				kv_bin->name[0] = 0;
			}

			kv_bin->offset = buf - buf_start;

			size_t p_size;
			if (0 == as_particle_get_flat_size(bin, &p_size)) {
				if (buf + p_size >= stack_buf_lim) {
					if (buf_start == stack_buf) {
						buf_len *= 4;
						
						uint8_t *buf_start_tmp = cf_malloc(buf_len);
						memcpy(buf_start_tmp, buf_start, buf_len / 4);
						buf = buf_start_tmp + (buf - buf_start);
						buf_start = buf_start_tmp;
					} else {
						uint8_t *buf_start_tmp = cf_realloc(buf_start, buf_len * 4);
						buf = buf_start_tmp + (buf - buf_start);
						buf_start = buf_start_tmp;
						buf_len *= 4;
					}
				}
				
				if (as_bin_is_integer(bin)) {
					as_particle_int_on_device *p = (as_particle_int_on_device *)buf;
					p->type = AS_PARTICLE_TYPE_INTEGER;
					p->len = sizeof(uint64_t);
					p->i = bin->ivalue;
				} else {
					memcpy(buf, as_bin_get_particle(bin), p_size);
				}
			}
			else {
				cf_warning(AS_DRV_KV, "internal error - only flat particles correct for storage - failure");
				p_size = 0;
			}

			buf += p_size;
			kv_bin->len = p_size;
			kv_bin->next = buf - buf_start;
			
			write_nbins++;
		}
	}

	// stomp in the block header
	drv_kv_block *block = (drv_kv_block *) buf_start;
	int data_size = buf - buf_start;

	block->length = data_size - SIGNATURE_OFFSET; 
	block->magic = KV_BLOCK_MAGIC;
	block->sig = 0;
	block->keyd = rd->keyd;
	block->generation = r->generation;
	block->void_time = r->void_time;
	block->bins_offset = 0;
	block->n_bins = write_nbins;

	int rv = 0;
	int num_written;

	kv_key_t *key = (kv_key_t *) &(rd->keyd);
	uint32_t key_len = sizeof(cf_digest);

	uint32_t pool_id = 0;	 // N.B.:  Will be the partition ID in the future.	For now, must always be 0.

	uint64_t expiry = r->void_time;	 // XXX -- 32-bit to 64-bit conversion!?!
	bool replace = !(rd->ns->cond_write);
	uint32_t gen_count = r->generation;	 // XXX -- 32-bit to 16-bit conversion!?!

	if (AS_STORAGE_ENGINE_KV != rd->ns->storage_type)
	  cf_crash(AS_DRV_KV, "kv_write() called on rd with invalid storage type", rd->ns->storage_type);

	if (!(rd->u.kv.kv)) {
		drv_kvs *kvs = (drv_kvs *) rd->ns->storage_private;
		r->storage_key.kv.file_id = kv_get_file_id(kvs, &rd->keyd);
		rd->u.kv.kv = &kvs->kvs[r->storage_key.kv.file_id];
		cf_detail(AS_DRV_KV, "kv_write(): chosing file id %d ptr %p", r->storage_key.kv.file_id, rd->u.kv.kv);
	}
	drv_kv *kv = rd->u.kv.kv;

	// Get the KV handle from the queue.
	int kv_h = kv_h_get(kv);

	if (-1 == kv_h)
	  cf_crash(AS_DRV_KV, "kv_h_get() returned -1 for device: \"%s\"", kv->name);

#ifdef DEBUG
	fprintf(stderr, "kv_write(): About to write %d bytes....\n", data_size);
	print_buf("block", block, data_size);
#endif

#ifdef DEMO2012
	if (rd->ns->demo_write_multiplier > 1) {
		int write_multiplier = rd->ns->demo_write_multiplier - 1;
		for (int i=0;i<write_multiplier;i++) {
		    kv_put_batch(kv, kv_h, key, key_len, block, data_size, pool_id, expiry, replace, gen_count);
		}
	}
#endif

	if (0 > (num_written = kv_put_batch(kv, kv_h, key, key_len, block, data_size, pool_id, expiry, replace, gen_count)))
		cf_crash(AS_DRV_KV, "kv_put_batch() on device: \"%s\" returned %d, errno = %d", kv->name, num_written, errno);
	else if (num_written != data_size)
	  // XXX -- Do we need to re-try in this case?!?
	  cf_warning(AS_DRV_KV, "kv_put_batch() on device: \"%s\" wrote only %d of %d bytes", kv->name, num_written, data_size);

	cf_detail(AS_DRV_KV, "kv_put_batch() on device: \"%s\" wrote %d bytes", kv->name, num_written);
	
#ifdef USE_KV_H_Q
	// Return the KV handle to the queue.
	kv_h_put(kv, kv_h);
#endif

	return(rv);
}

static ssize_t
kv_read(as_storage_rd *rd, uint8_t *read_buf, uint32_t read_size)
{
	int num_read;

	kv_key_t *key = (kv_key_t *) &(rd->keyd);
	uint32_t key_len = sizeof(cf_digest);

	uint32_t pool_id = 0;    // N.B.:  Will be the partition ID in the future.  For now, must always be 0.

	kv_key_info_t key_info;

	if (AS_STORAGE_ENGINE_KV != rd->ns->storage_type)
	  cf_crash(AS_DRV_KV, "kv_read() called on rd with invalid storage type", rd->ns->storage_type);

	if (!(rd->u.kv.kv)) {
		drv_kvs *kvs = (drv_kvs *) rd->ns->storage_private;
		rd->u.kv.kv = &kvs->kvs[rd->r->storage_key.kv.file_id];
		cf_detail(AS_DRV_KV, "kv_read(): chosing file id %d ptr %p", rd->r->storage_key.kv.file_id, rd->u.kv.kv);
	}
	drv_kv *kv = rd->u.kv.kv;

	// Get the KV handle from the queue.
	int kv_h = kv_h_get(kv);

	if (-1 == kv_h)
	  cf_crash(AS_DRV_KV, "kv_h_get() returned -1 for device: \"%s\"", kv->name);

	if (0 > (num_read = kv_get(kv_h, key, key_len, read_buf, read_size, pool_id, &key_info))) {
		cf_detail(AS_DRV_KV,"kv_get() on device: \"%s\" returned %d, errno = %d", kv->name, num_read, errno);
		if (FIO_ERR_OBJECT_NOT_FOUND == - num_read)
		  cf_detail(AS_DRV_KV, "kv_read():  Key does not exist ~~ No problem-o.\n");
		else
		  cf_crash(AS_DRV_KV, "kv_get() on device: \"%s\" returned %d, errno = %d", kv->name, num_read, errno);
	} else /* if (num_read != read_size) */  	  // XXX -- Do we need to re-try in this case?!?
	  cf_detail(AS_DRV_KV, "kv_get() on device: \"%s\" read %d of %d bytes", kv->name, num_read, read_size);

#ifdef DEBUG
	if (num_read > 0) {
		fprintf(stderr, "kv_read(): Just read %d bytes....\n", num_read);
		print_buf("buf", read_buf, num_read);
	}
#endif

#ifdef USE_KV_H_Q
	// Return the KV handle to the queue.
	kv_h_put(kv, kv_h);
#endif

	return((ssize_t) num_read);
}

/*
 *  Note:  Nothing currently calls this function.
 *   (It's here for future reference when deletion actually is implemented.)
 */
static int
kv_delete_it(as_storage_rd *rd)
{
	int rv = 0;

	kv_key_t *key = (kv_key_t *) &(rd->keyd);
	uint32_t key_len = sizeof(cf_digest);
	uint32_t pool_id = 0;    // N.B.:  Will be the partition ID in the future.  For now, must always be 0.

	if (AS_STORAGE_ENGINE_KV != rd->ns->storage_type)
	  cf_crash(AS_DRV_KV, "kv_delete_it() called on rd with invalid storage type", rd->ns->storage_type);

	if (!(rd->u.kv.kv)) {
		drv_kvs *kvs = (drv_kvs *) rd->ns->storage_private;
		rd->r->storage_key.kv.file_id = kv_get_file_id(kvs, &rd->keyd);
		rd->u.kv.kv = &kvs->kvs[rd->r->storage_key.kv.file_id];
		cf_detail(AS_DRV_KV, "kv_delete_it(): chosing file id %d ptr %p", rd->r->storage_key.kv.file_id, rd->u.kv.kv);
	}
	drv_kv *kv = rd->u.kv.kv;

	// Get the KV handle from the queue.
	int kv_h = kv_h_get(kv);

	if (-1 == kv_h)
	  cf_crash(AS_DRV_KV, "kv_h_get() returned -1 for device: \"%s\"", kv->name);

	if (0 > (rv = kv_delete(kv_h, key, key_len, pool_id)))
	  cf_crash(AS_DRV_KV, "kv_delete() on device: \"%s\" returned %d, errno = %d", kv->name, rv, errno);

	cf_detail(AS_DRV_KV, "kv_delete() on device: \"%s\" deleted key %p", kv->name, key);

#ifdef USE_KV_H_Q
	// Return the KV handle to the queue.
	kv_h_put(kv, kv_h);
#endif

	return(rv);
}


/*
 *  KV Storage Engine Methods.
 */


static int
kv_init_devices(as_namespace *ns, drv_kvs **kvs_p)
{
	int i;
	drv_kvs *kvs;

	for (i = 0; i < AS_STORAGE_MAX_DEVICES; i++)
	  if (!(ns->storage_devices[i]))
	    break;

	int n_kvs = i;
	if (!(kvs = cf_malloc(sizeof(drv_kvs) + (n_kvs * sizeof(drv_kv)))))
	  return(-1);
	memset(kvs, 0, sizeof(sizeof(drv_kvs) + (n_kvs * sizeof(drv_kv))));
	kvs->n_kvs = n_kvs;

	cf_rc_reserve(ns);
	kvs->ns = ns;

	ns->kv_size = 0;

	// Initialize the KV devices.
	for (int i = 0; i < kvs->n_kvs; i++) {
		drv_kv *kv = &(kvs->kvs[i]);

		strcpy(kv->name, ns->storage_devices[i]);

		// XXX -- This hard-coded KV device size comes from the config. file.
		//        (The right way to do this would be to use some currently-unknown
		//         API to get the size from the device.)
		kv->file_size = ns->storage_filesize;

		ns->kv_size += kv->file_size;

#ifdef USE_KV_BATCH_PUT
		// Allocate and initialize the write block for doing batch KV puts.
		size_t size;

		for (int i = 0; i < ns->storage_num_write_blocks; i++) {
			if (!(kv->write_block[i] = (kv_write_block *) cf_malloc(size = sizeof(kv_write_block) + ns->storage_write_block_size)))
			  cf_crash(AS_DRV_KV, "failed to cf_malloc() a kv_write_block of size %d", size);

			kv_write_block *wb = kv->write_block[i];
			memset(wb, 0, size);

			if (!(wb->iovs = (kv_iovec_t *) cf_malloc(size = sizeof(kv_iovec_t) * KV_INITIAL_NUM_IO_VECTORS)))
			  cf_crash(AS_DRV_KV, "failed to cf_malloc() an IO Vectors array of size %d", size);

			memset(wb->iovs, 0, size);

			wb->num_iovs_alloc = KV_INITIAL_NUM_IO_VECTORS;
			wb->num_iovs_used = 0;
			wb->data_alloc = ns->storage_write_block_size;
			wb->data_used = 0;
			pthread_mutexattr_t mutex_attr;
			pthread_mutexattr_init(&mutex_attr);
			pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE);
			pthread_mutex_init(&wb->LOCK, &mutex_attr);
			pthread_mutexattr_destroy(&mutex_attr);
		}
#endif
	}
	*kvs_p = kvs;

	return(0);
}

int
as_storage_namespace_init_kv(as_namespace *ns, cf_queue *complete_q, void *udata)
{
	drv_kvs *kvs;

	if (0 != ns->storage_devices[0]) {
		if (0 != kv_init_devices(ns, &kvs)) {
			cf_warning(AS_DRV_KV, "can't initialize devices");
			return(-1);
		}
	} else {
		cf_warning(AS_DRV_KV, "namespace %s: no device device configured, fatal error", ns->name);
		return(-1);
	}

	ns->storage_private = (void *) kvs;

	// Create files, open file descriptors, etc.
	for (int i = 0; i < kvs->n_kvs; i++) {
		drv_kv *kv = &(kvs->kvs[i]);

		kv->open_flag = O_RDWR;

#ifdef USE_FD_Q
		// Initialize the file descriptor queue.
		if (!(kv->fd_q = cf_queue_create(sizeof(int), true)))
		  cf_crash(AS_DRV_KV, "unable to allocate FD queue for KV device: \"%s\"", kv->name);
#endif
#ifdef USE_KV_H_Q
		// Initialize the KV handle queue.
		if (!(kv->h_q = cf_queue_create(sizeof(int), true)))
		  cf_crash(AS_DRV_KV, "unable to allocate KV handle queue for KV device: \"%s\"", kv->name);
#endif

		int fd;
		if (-1 == (fd = open(kv->name, kv->open_flag, S_IRUSR | S_IWUSR)))
		  cf_crash(AS_DRV_KV, "unable to open file \"%s\": %s", kv->name, cf_strerror(errno));
		else
		  kv_fd_put(kv, fd);

		cf_info(AS_DRV_KV, "Opened device: \"%s\" bytes %"PRIu64" read buffer %"PRIu32" bytes",
				kv->name, kv->file_size, ns->storage_read_block_size);

		cf_info(AS_DRV_KV, "Calling kv_create() on KV device: \"%s\" in namespace: \"%s\"....", kv->name, ns->name);

		int kv_h;
		if (0 > (kv_h = kv_create(fd, KV_API_VERSION, ns->cond_write)))
		  cf_crash(AS_DRV_KV, "kv_create(%d, %d, %d) on KV device: \"%s\" returned %d, errno = %d",
				   fd, KV_API_VERSION, ns->cond_write, kv->name, kv_h, errno);
		else
		  kv_h_put(kv, kv_h);

		cf_info(AS_DRV_KV, "Yay!! kv_create() succeeded on KV device: \"%s\" in namespace: \"%s\"! Returned kv_h: %d",
				kv->name, ns->name, kv_h);
	}

	cf_queue_push(complete_q, &udata);
	return(0);
}

int
as_storage_namespace_destroy_kv(as_namespace *ns)
{
	cf_debug(AS_DRV_KV, "kv storage destroy: namespace %s", ns->name);
	drv_kvs *kvs = (drv_kvs *) ns->storage_private;

	for (int i = 0; i < kvs->n_kvs; i++) {
		drv_kv *kv = &kvs->kvs[i];

#ifdef USE_KV_BATCH_PUT
		// First, flush out any pending batch data.
		// XXX -- Make sure this is multithread safe!!  What thread would call this function anyway??
		kv_batch_flush(kv);
#endif
		int fd;
		do {
			if (-1 != (fd = kv_fd_get(kv)))
			  close(fd);
			else
			  break;
		} while (1);
#ifdef USE_FD_Q
		cf_queue_destroy(kv->fd_q);
#endif
#ifdef USE_KV_H_Q
		cf_queue_destroy(kv->h_q);
#endif
#ifdef USE_KV_BATCH_PUT
		for (int i = 0; i < ns->storage_num_write_blocks; i++) {
			pthread_mutex_destroy(&kv->write_block[i]->LOCK);
			cf_free(kv->write_block[i]->iovs);
			cf_free(kv->write_block[i]);
		}
#endif
	}
	as_namespace_release(kvs->ns);
	cf_free(kvs);

	return(0);
}

int
as_storage_namespace_attributes_get_kv(as_namespace *ns, as_storage_attributes *attr)
{
	attr->load_at_startup = false;

	drv_kvs *kvs = (drv_kvs *) ns->storage_private;
	attr->n_devices = kvs->n_kvs;

	return(0);
}

int
as_storage_has_index_kv(as_namespace *ns)
{
	return(true);
}

/*
 * Determine whether the record exists or not.
 *  (Ideally this would be done via an inexpensive record exists KV API.)
 *
 *  XXX -- Ideally, we should cache the returned data as an optimization for back-to-back fetches.
 */
int
as_storage_record_exists_kv(as_namespace *ns, cf_digest *keyd)
{
#ifdef USE_KV_EXISTS
	int rv = 0;
	drv_kvs *kvs = (drv_kvs *) (ns->storage_private);

	int file_id = kv_get_file_id(kvs, keyd);
	drv_kv *kv = &kvs->kvs[file_id];

	// Get the KV handle from the queue.
	int kv_h = kv_h_get(kv);

	kv_key_t *key = (kv_key_t *) keyd;
	uint32_t key_len = sizeof(cf_digest);
	uint32_t pool_id = 0;    // N.B.:  Will be the partition ID in the future.  For now, must always be 0.
	
	if (0 > (rv = kv_exists(kv_h, key, key_len, pool_id)))
	  cf_detail(AS_DRV_KV, "key does not exist ~~ OK.");
	else
	  cf_detail(AS_DRV_KV, "key exists!");

#ifdef USE_KV_H_Q
	// Return the KV handle to the queue.
	kv_h_put(kv, kv_h);
#endif

	// Note:  KV exists API returns 0 upon successfully finding a key.
	return(!rv);
#else
	drv_kvs *kvs = (drv_kvs *) (ns->storage_private);

	as_storage_rd rd;
	rd.ns = ns;
	rd.keyd = *keyd;

	as_record r;
	rd.r = &r;

	int file_id = kv_get_file_id(kvs, keyd);
	rd.u.kv.kv = &kvs->kvs[file_id];

	uint32_t read_size = ns->storage_read_block_size;
	
	// Note:   Performance suffers if this buffer is too large, but serious correctness
	//          problems would occur if serialized values don't fit within this size!
	// 
	//          Fortunately, however, we're throwing this data away, so it's OK to use
	//          the minimum buffer size of 512 bytes.
	static uint8_t read_buf[512];  // Was: 1024 * 1024  // Note:  Size must be >= read_size!

	// XXX -- This data is currently just thrown away....
	ssize_t rv = kv_read(&rd, read_buf, read_size);

	cf_detail(AS_DRV_KV, "key_exists: kv_read() with read_size %d returned %zd", read_size, rv);

	// If any error, return false.
	return (rv >= 0);
#endif
}

/*
 * Lazy-create on close, so just initialize the data structures.
 */
int
as_storage_record_create_kv(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd)
{
	cf_detail(AS_DRV_KV, "record create: ns %s record %p rd %p key %"PRIx64, ns->name, r, rd, *(uint64_t *)keyd);

	rd->u.kv.block = 0;
	rd->u.kv.must_free_block = false;
	rd->u.kv.kv = 0;

	r->storage_key.kv.file_id = STORAGE_INVALID_FILE_ID; // careful here - this is now unsigned

    return(0);
}

int
as_storage_record_open_kv(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd)
{
	cf_detail(AS_DRV_KV, "record open: ns %s record %p rd %p key %"PRIx64, ns->name, r, rd, *(uint64_t *)keyd);

	drv_kvs *kvs = (drv_kvs *) ns->storage_private;

	if (r->storage_key.kv.file_id == STORAGE_INVALID_FILE_ID)
	  r->storage_key.kv.file_id = kv_get_file_id(kvs, keyd);

	rd->u.kv.block = 0;
	rd->u.kv.must_free_block = false;

	rd->u.kv.kv = &kvs->kvs[r->storage_key.kv.file_id];
#ifdef EXTRA_CHECKS
	if (r->storage_key.kv.file_id != kv_get_file_id(kvs, keyd)) {
		cf_debug(AS_DRV_KV, "record open: using incorrect file id should be %d is %d",
			kv_get_file_id(kvs, keyd), r->storage_key.kv.file_id);
	}
#endif

	return(0);
}

int
as_storage_record_write_kv(as_record *r, as_storage_rd *rd)
{
	cf_detail(AS_DRV_KV, "record write: r %p rd %p", r, rd);

	if (as_bin_inuse_has(rd)) {
		kv_write(r, rd);
	}

	return 0;
}

int
as_storage_record_close_kv(as_record *r, as_storage_rd *rd)
{
	cf_detail(AS_DRV_KV, "record close: r %p rd %p", r, rd);

	if (rd->u.kv.block && rd->u.kv.must_free_block) {
		cf_free(rd->u.kv.block);
	}

	return 0;
}

static int
kv_populate_bin(as_bin *bin, drv_kv_bin *kv_bin, uint8_t *block_head, bool single_bin, bool allocate_memory)
{
	as_particle *p = (as_particle *) (block_head + kv_bin->offset);

	if (p->metadata == AS_PARTICLE_TYPE_INTEGER) {
		if (kv_bin->len != 10)
			cf_warning(AS_DRV_KV, "Possible memory corruption: integer value overflows: expected 10 bytes got %d bytes", kv_bin->len);

		as_particle_int_on_device *pi = (as_particle_int_on_device *) p;
		if (pi->len != 8) {
			return (-1);
		}

		// destroy old particle
		as_particle_fromflat(bin, AS_PARTICLE_TYPE_NULL, 0, 0, 0, true);

		// copy the integer particle in-place to the bin.
		bin->ivalue = pi->i;
		as_bin_state_set(bin, AS_BIN_STATE_INUSE_INTEGER);
	}
	else {
		if (allocate_memory) {
			uint32_t base_size = as_particle_get_base_size(p->metadata);
			as_particle_fromflat(bin, p->metadata, (uint8_t *)p + base_size, kv_bin->len - base_size, 0, true);
		}
		else {
			bin->particle = p;
			if (as_particle_type_hidden(p->metadata)) {
				as_bin_state_set(bin, AS_BIN_STATE_INUSE_HIDDEN);
			} else {
				as_bin_state_set(bin, AS_BIN_STATE_INUSE_OTHER);
			}
		}
	}

	as_bin_set_version(bin, kv_bin->version, single_bin);

	return (0);
}

inline uint16_t
as_storage_record_get_n_bins_kv(as_storage_rd *rd)
{
	return (rd->u.kv.block ? rd->u.kv.block->n_bins : 0);
}

int
as_storage_record_read_kv(as_storage_rd *rd)
{
	uint32_t read_size = rd->ns->storage_read_block_size;

	uint8_t *read_buf = cf_malloc(read_size);
	if (!read_buf)	return(-1);

	ssize_t rv;

#ifdef DEMO2012
	if (rd->ns->demo_read_multiplier > 1 ) {
		int read_multiplier = rd->ns->demo_read_multiplier - 1;
		for (int i=0;i<read_multiplier;i++) {
			rv = kv_read(rd, read_buf, read_size);
		}
	}
#endif

	rv = kv_read(rd, read_buf, read_size);

	cf_detail(AS_DRV_KV, "kv_read() with read_size %d returned %zd", read_size, rv);

	// Just return any error code, such as record not found.
	if (0 > rv)
	  return(rv);

	// two good checks now that we have index records independent of the data records
	drv_kv_block *block = (drv_kv_block *)read_buf;

	if (block->magic != KV_BLOCK_MAGIC) {
		cf_warning(AS_DRV_KV, " read: bad block magic 0x%08x", block->magic);
		cf_free(read_buf);
		return(-1);
	}

	if (0 != cf_digest_compare(&block->keyd, &rd->keyd)) {
		cf_warning(AS_DRV_KV, " read: read wrong key: expecting %"PRIx64" got %"PRIx64,
				*(uint64_t *) &rd->keyd, *(uint64_t *) &block->keyd );
		cf_free(read_buf);
		return(-1);
	}

#ifdef EXTRA_CHECKS	
	// do some validation of the block
	uint8_t *block_head = read_buf;

	// walk the bin list
	uint32_t n_bins = block->n_bins;
	drv_kv_bin *kv_bin = (drv_kv_bin *) (block->data + block->bins_offset);
	if (n_bins > 100) {
		cf_warning(AS_DRV_KV, " read: unlikely so many bins: %d found",n_bins);
#ifdef SIGINT_ON_CORRUPTION
		raise(SIGINT);
#endif		
		cf_free(read_buf);
		return(-1);
	}

	while (n_bins > 0) {
		if (strlen(kv_bin->name) > AS_ID_BIN_SZ) { 
			cf_warning(AS_DRV_KV, " read: unlikely bin size: offset %d sz %d",n_bins,strlen(kv_bin->name));
#ifdef SIGINT_ON_CORRUPTION			
			raise(SIGINT);
#endif			
			cf_free(read_buf);
			return(-1);
		}

		// move to the next bin
		kv_bin = (drv_kv_bin *) (block_head + kv_bin->next);
		n_bins--;		
	};
#endif

	// the block is a particular modulus up from the pointer
	rd->u.kv.block = (drv_kv_block *) read_buf;
	rd->u.kv.must_free_block = true;
	rd->u.kv.block_size = read_size;
	rd->have_device_block = true;

	return(0);
}

//
// patch up bin pointer
//

int
as_storage_particle_read_all_kv(as_storage_rd *rd)
{
	// if the first hasn't been read, read it
	if (rd->u.kv.block == 0) {
		if (0 != as_storage_record_read_kv(rd)) {
			cf_warning(AS_DRV_KV, "read_bin: could not read first");
			return(-1);
		}
	}
	
	// load up all the pointers through the first block
	drv_kv_block *block = rd->u.kv.block;
	uint8_t *block_head = (uint8_t *) rd->u.kv.block;

	// walk block list until bin is found, patch up pointer
	drv_kv_bin *kv_bin = (drv_kv_bin *) (block->data + block->bins_offset);

	for (uint16_t i = 0; i < block->n_bins; i++) {
		as_bin_set_version(&rd->bins[i], kv_bin->version, rd->ns->single_bin);
		as_bin_strcpy_name(&rd->bins[i], kv_bin->name, rd->ns->single_bin);

		int rv = kv_populate_bin(&rd->bins[i], kv_bin, block_head, rd->ns->single_bin, false);
		if (0 != rv) {
			return (rv);
		}

		kv_bin = (drv_kv_bin *) (block_head + kv_bin->next);
	}

	return(0);
}

int
as_storage_stats_kv(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes)
{
	if (available_pct) {
		uint64_t memory_sz = cf_atomic_int_get(ns->n_objects) * as_index_size_get(ns);   // CHANGE ME WHEN OBJECTS ARE CHEAPER AND MULTIFLEX
		memory_sz += cf_atomic_int_get(ns->n_bytes_memory);
		int mem_free_pct;
		if (memory_sz == 0)
		  mem_free_pct = 100;
		else if (memory_sz > ns->memory_size)
		  mem_free_pct = 0;
		else mem_free_pct = ((ns->memory_size - memory_sz) * 100L) / ns->memory_size;

		*available_pct = mem_free_pct;
	}
	if (used_disk_bytes) {
		*used_disk_bytes = 0;
	}

	return(0);
}

#else

// Define stubs for building without the KV API.


static void
error_out()
{
	cf_crash(AS_DRV_KV, "No support for the Key-Value Store Storage Engine compiled into build!");
}

int
as_storage_namespace_init_kv(as_namespace *ns, cf_queue *complete_q, void *udata)
{
	error_out();

	return 0;
}

int
as_storage_namespace_destroy_kv(as_namespace *ns)
{
	error_out();

	return 0;
}

int
as_storage_namespace_attributes_get_kv(as_namespace *ns, as_storage_attributes *attr)
{
	error_out();

	return 0;
}

int
as_storage_has_index_kv(as_namespace *ns)
{
	error_out();

	return 0;
}

int
as_storage_record_exists_kv(as_namespace *ns, cf_digest *keyd)
{
	error_out();

	return 0;
}

int
as_storage_record_create_kv(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd)
{
	error_out();

	return 0;
}

int
as_storage_record_open_kv(as_namespace *ns, as_record *r, as_storage_rd *rd, cf_digest *keyd)
{
	error_out();

	return 0;
}

int
as_storage_record_write_kv(as_record *r, as_storage_rd *rd)
{
	error_out();

	return 0;
}

int
as_storage_record_close_kv(as_record *r, as_storage_rd *rd)
{
	error_out();

	return 0;
}

uint16_t
as_storage_record_get_n_bins_kv(as_storage_rd *rd)
{
	error_out();

	return 0;
}

int
as_storage_record_read_kv(as_storage_rd *rd)
{
	error_out();

	return 0;
}

int
as_storage_particle_read_all_kv(as_storage_rd *rd) 
{
	error_out();

	return 0;
}

int
as_storage_stats_kv(as_namespace *ns, int *available_pct, uint64_t *used_disk_bytes)
{
	error_out();

	return 0;
}

#endif // defined(USE_KV)
