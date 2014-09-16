/*
 * ldt_aerospike.c
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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
 *  This function implements functional interface and corresponding internal
 *  function for as_aerospike interface for Large Data Type. None of the code
 *  here is thread safe. The calling thread which initiates the UDF needs to
 *  hold object locks and partition & namespace reservations.
 *
 *  Entire ldt_aerospike is some part wrapper over the udf_aerospike and
 *  some part its own logic
 */

#include "base/feature.h" // Turn new AS Features on/off

#include "base/ldt_aerospike.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_rec.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"
#include "msg.h"
#include "util.h"

#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/ldt_record.h"
#include "base/thr_rw_internal.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "fabric/fabric.h"

/* GLOBALS */
as_aerospike g_ldt_aerospike; // Only instantiation is enough

/* INIT */
int
ldt_init(void)
{
	as_aerospike_init(&g_ldt_aerospike, NULL, &ldt_aerospike_hooks);
	return (0);
}

as_aerospike *
ldt_aerospike_new()
{
	return as_aerospike_new(NULL, &ldt_aerospike_hooks);
}

as_aerospike *
ldt_aerospike_init(as_aerospike * as)
{
	return as_aerospike_init(as, NULL, &ldt_aerospike_hooks);
}

int
ldt_chunk_print(ldt_record *lrecord, int slot)
{
	ldt_chunk  *lchunk    = &lrecord->chunk[slot];
	udf_record *c_urecord = &lrecord->chunk[slot].c_urecord;

	cf_detail(AS_LDT, "LSO CHUNK: slot = %d lchunk [%p,%p,%p,%p] ", slot,
				lchunk->c_urecord, &lchunk->tr, &lchunk->rd, &lchunk->r_ref);
	cf_detail(AS_LDT, "LSO CHUNK: slot = %d urecord   [%p,%p,%p,%p] ", slot,
				c_urecord, c_urecord->tr, c_urecord->rd, c_urecord->r_ref);
	return 0;
}

/*
 * Internal Function: Search if the digest is already opened.
 *
 * Parameters:
 * 		lr      - Parent ldt_record
 * 		digest  - Digest to be searched for
 *
 * Return value:
 * 		>=0  slot if found
 * 		-1   in case not found
 *
 * Description:
 * 		The function walks through the lchunk array searching for
 * 		the request digest.
 *
 * Callers:
 *      ldt_aerospike_crec_open when UDF requests opening
 *      of new record
 */
int
ldt_crec_find_digest(ldt_record *lrecord, cf_digest *keyd)
{
	int slot = -1;
	FOR_EACH_SUBRECORD(i, lrecord) {
		if (!memcmp(&lrecord->chunk[i].rd.keyd, keyd, 20)) {
			slot = i;
			break;
		}
	}
	return slot;
}

/*
 * Internal Function: Search for the freeslot in the sub record array
 *
 * Parameters:
 * 		lr   - Parent ldt_record
 *
 * Return value:
 * 		>=0  if empty slot found
 * 		-1   in case not found
 *
 * Description:
 * 		The function walks through the lchunk array searching for
 * 		the request digest.
 *
 * Callers:
 *      ldt_aerospike_crec_open
 *      ldt_aerospike_crec_create
 */
int
ldt_crec_find_freeslot(ldt_record *lrecord)
{
	int slot = -1;
	if (lrecord->num_slots_used == lrecord->max_chunks) {
		int new_size    = lrecord->max_chunks + LDT_SLOT_CHUNK_SIZE;
		void *old_chunk = lrecord->chunk;

		lrecord->chunk = cf_realloc(lrecord->chunk, sizeof(ldt_chunk) * new_size);
		
		if (lrecord->chunk == NULL) {
			lrecord->chunk = old_chunk;
			goto out;
		}

		for (int i = lrecord->max_chunks; i < new_size; i++) {
			lrecord->chunk[i].slot = -1;
		}

		cf_detail(AS_LDT, "Bumping up chunks from %d to %d", lrecord->max_chunks, new_size);
		lrecord->max_chunks = new_size;
	}

	for (int i = 0; i < lrecord->max_chunks; i++) {
		if (lrecord->chunk[i].slot == -1) {
			lrecord->num_slots_used++;
			return i;
		}
	}

out:
	if (slot == -1) {
		cf_warning(AS_LDT, "ldt_crec_create: Cannot open more than (%d) records in a single UDF", lrecord->max_chunks);
	}
	return slot;
}

/*
 * Internal Function: Which initializes ldt chunk array element.
 *
 * Parameters:
 * 		lchunk - ldt_chunk to be initialized
 *
 * Return value : nothing
 *
 * Description:
 * 		1. Sets up udf_record
 * 		2. Zeroes out stuff
 * 		3. Setups as_rec in the lchunk
 *
 * Callers:
 *      ldt_aerospike_crec_open
 *      ldt_aerospike_crec_create
 */
void
ldt_chunk_init(ldt_chunk *lchunk, ldt_record *lrecord)
{
	// It is just a stub fill the proper values in
	udf_record *c_urecord   = &lchunk->c_urecord;
	udf_record_init(c_urecord);
	// note: crec cannot be destroyed from inside lua
	c_urecord->flag        |= UDF_RECORD_FLAG_IS_SUBRECORD;
	c_urecord->lrecord      = (void *)lrecord;
	c_urecord->tr           = &lchunk->tr; // set up tr properly
	c_urecord->rd           = &lchunk->rd;
	c_urecord->r_ref        = &lchunk->r_ref;
	lchunk->r_ref.skip_lock = true;
	lchunk->c_urec_p = as_rec_new(c_urecord, &udf_record_hooks);
}

void ldt_chunk_destroy(ldt_record *lrecord, int slot)
{
	ldt_chunk *lchunk = &lrecord->chunk[slot];
	if (lchunk->slot != -1) {
		as_rec_destroy(lchunk->c_urec_p);
		lchunk->slot = -1;
		lrecord->num_slots_used--;
	}
}

/*
 * Internal Function: Which sets up ldt chunk array element.
 *
 * Parameters:
 * 		lchunk  - ldt_chunk to be setup
 * 		h_urec    - initialized
 * 		keyd    - digest of the subrecord
 *
 * Return value : nothing
 *
 * Description:
 * 		1. Sets up transaction and digest
 * 		2. Sets up the partition reservation (same as parent)
 *
 * Callers:
 *      ldt_aerospike_crec_open
 *      ldt_aerospike_crec_create
 */
void
ldt_chunk_setup(ldt_chunk *lchunk, as_rec *h_urec, cf_digest *keyd)
{
	udf_record     * h_urecord = (udf_record *)as_rec_source(h_urec);
	as_transaction * h_tr      = h_urecord->tr;
	as_transaction * c_tr      = &lchunk->tr;

	c_tr->incoming_cluster_key = h_tr->incoming_cluster_key;

	// Chunk Record Does not respond for proxy request
	c_tr->proto_fd_h           = NULL;       // Need not reply
	c_tr->proxy_node           = 0;          // ??
	c_tr->proxy_msg            = NULL;       // ??

	// Chunk Record Does not respond back to the client
	c_tr->result_code          = 0;
	c_tr->generation           = 0;
	// Set this to grab some info from the msg from client like
	// set name etc ... we do not set it in wr..
	c_tr->msgp                 = h_tr->msgp;

	// We do not track microbenchmark or time for chunk today
	c_tr->microbenchmark_time  = 0;
	c_tr->microbenchmark_is_resolve = false;
	c_tr->start_time           = h_tr->start_time;
	c_tr->end_time             = h_tr->end_time;
	c_tr->trid                 = h_tr->trid;

	// Chunk transaction is always preprocessed
	c_tr->preprocessed         = true;       // keyd is hence preprocessed
	c_tr->flag                 = 0;

	// Parent reservation cannot go away as long as Chunck needs reservation.
	memcpy(&c_tr->rsv, &h_tr->rsv, sizeof(as_partition_reservation));
	c_tr->keyd                 = *keyd;
	udf_record *c_urecord      = (udf_record *)as_rec_source(lchunk->c_urec_p);
	c_urecord->keyd            = *keyd;

	// There are 4 place digest is
	// 1. tr->keyd
	// 2. r_ref->r->key
	// 3. rd->keyd
	// 4. urecord->keyd
	//
	// First three are always equal. At the start tr->keyd is setup which then
	// sets or gets r_ref / rd as normal work goes ...
	//
	// urecord->keyd is the digest which gets exposed to lua world. In this
	// version bits are always set to zero.
	cf_detail(AS_LDT, "LDT_VERSION Resetting @ create LDT version %p", *(uint64_t *)&c_urecord->keyd);
	as_ldt_subdigest_resetversion(&c_urecord->keyd);
}

/*
 * Internal Function: Function to open chunk record
 *
 * Parameters:
 * 		lrd  : Parent ldt record
 * 		keyd : Key digest for the record to be opened
 * 		slot(out): Filled with slot in case of success
 *
 * Return value :
 * 		 0  in case of success returns positive slot value
 * 		-1   in case record is already open
 * 		-2   in case free slot cannot be found
 * 		-3   in case record cannot be opened
 *
 * Description:
 * 		1. Get the empty chunk slot.
 * 		2. Read the record into it
 *
 * Callers:
 *		ldt_aerospike_crec_open
 */
int
ldt_crec_open(ldt_record *lrecord, cf_digest *keyd, int *slotp)
{
	cf_debug_digest(AS_LDT, keyd, "[ENTER] ldt_crec_open(): Digest: ");

	// 1. Search in opened record
	int slot = ldt_crec_find_digest(lrecord, keyd);
	if (slot != -1) {
		cf_detail(AS_LDT, "ldt_aerospike_rec_open : Found already open");
		*slotp = slot;
		return 0;
	}

	// 2. Find free slot and setup chunk
	slot     = ldt_crec_find_freeslot(lrecord);
	if (slot == -1) {
		cf_warning(AS_LDT, "Cannot open more than (%d) records in a single UDF",
				lrecord->max_chunks);
		return -2;
	}
	cf_detail(AS_LDT, "ldt_crec_open popped slot %d", slot);
	lrecord->chunk[slot].slot = slot;
	ldt_chunk *lchunk         = &lrecord->chunk[slot];
	ldt_chunk_init(lchunk, lrecord);
	ldt_chunk_setup(lchunk, lrecord->h_urec, keyd);
	//ldt_chunk_print(lrecord, slot);

	// Open Record
	int rv = udf_record_open((udf_record *)as_rec_source(lchunk->c_urec_p));
	if (rv) {
		// Open the slot for reuse
		ldt_chunk_destroy(lrecord, slot);
		return -3;
	}
	*slotp = slot;
	return 0;
}


/*
 * Internal Function: To create new chunk record
 *
 * Parameters:
 * 		lr    : Parent ldt record
 *
 * Return value :
 * 		crec  (as_val) in case of success
 * 		NULL  in case of failure
 *
 * Description:
 * 		1. Search for empty chunk slot.
 *		2. Read the record into it
 *
 * Callers:
 *		ldt_aerospike_crec_create
 */
as_rec *
ldt_crec_create(ldt_record *lrecord)
{
	// Generate Key Digest
	udf_record *h_urecord = (udf_record *) as_rec_source(lrecord->h_urec);
	cf_digest keyd        = h_urecord->r_ref->r->key;
	cf_detail(AS_LDT, "ldt_aerospike_crec_create %"PRIx64"", *(uint64_t *)&keyd);
	as_ldt_digest_randomizer(&keyd);
	as_ldt_subdigest_setversion(&keyd, lrecord->version);

	// Setup Chunk
	int slot     = ldt_crec_find_freeslot(lrecord);
	if (slot == -1) {
		return NULL;
	}
	cf_detail(AS_LDT, "ldt_crec_create: Popped slot %d", slot);
	lrecord->chunk[slot].slot = slot;
	ldt_chunk *lchunk    = &lrecord->chunk[slot];
	ldt_chunk_init (lchunk, lrecord);
	ldt_chunk_setup(lchunk, lrecord->h_urec, &keyd);

	// Create Record
	int rv = as_aerospike_rec_create(lrecord->as, lchunk->c_urec_p);
	if (rv < 0) {
		// Mark Slot as free
		ldt_chunk_destroy(lrecord, slot);
		cf_warning(AS_LDT, "ldt_crec_create: Record Create Failed rv=%d ... ", rv);
		return NULL;
	}

	cf_detail_digest(AS_LDT, &(lchunk->c_urecord.keyd), "Crec Create:Ptr(%p) Digest: version %ld", lchunk->c_urec_p, lrecord->version);

	as_val_reserve(lchunk->c_urec_p);
	return lchunk->c_urec_p;
}

bool
ldt_record_destroy(as_rec * rec)
{
	static const char * meth = "ldt_record_destroy()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p", meth, rec);
		return false;
	}

	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return false;
	}
	as_rec *h_urec      = lrecord->h_urec;

	// Note: destroy of udf_record today is no-op because all the closing
	// of record happens after UDF has executed.
	// TODO: validate chunk handling here.
	FOR_EACH_SUBRECORD(i, lrecord) {
		ldt_chunk_destroy(lrecord, i);
	}

	// Free up allocated chunks
	cf_free(lrecord->chunk);
	// Dir destroy should release partition reservation and
	// namespace reservation.
	as_rec_destroy(h_urec);
	return true;
}


/*********************************************************************
 * INTERFACE FUNCTIONS                                               *
 *																	 *
 * See the as_aerospike for the API definition						 *
 ********************************************************************/
static int
ldt_aerospike_rec_create(const as_aerospike * as, const as_rec * rec)
{
	static char * meth = "ldt_aerospike_rec_create()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s Invalid Paramters: as=%p, record=%p", meth, as, rec);
		return 2;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	as_rec *h_urec       = lrecord->h_urec;
	as_aerospike *las    = lrecord->as;
	int rv = as_aerospike_rec_create(las, h_urec);
	if (rv) {
		return rv;
	}

	// If record is newly created and were created by LDT lua then it
	// would have already set starting version ... read that into the
	// lrecord->version for quick reference.
	udf_record   * h_urecord = (udf_record *)as_rec_source(h_urec);
	rv = as_ldt_parent_storage_get_version(h_urecord->rd, &lrecord->version);
	cf_detail_digest(AS_LDT, &h_urecord->keyd, "LDT_VERSION At Create %ld rv=%d", lrecord->version, rv);
	return 0;
}

static as_rec *
ldt_aerospike_crec_create(const as_aerospike * as, const as_rec *rec)
{
	static char * meth = "ldt_aerospike_crec_create()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s Invalid Paramters: as=%p, record=%p", meth, as, rec);
		return NULL;
	}
	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return NULL;
	}
	if (!udf_record_ldt_enabled(lrecord->h_urec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled");
		return NULL;
	}
	cf_detail(AS_LDT, "ldt_aerospike_crec_create");
	return ldt_crec_create(lrecord);
}

static int
ldt_aerospike_crec_remove(const as_aerospike * as, const as_rec * crec)
{
	if (!as || !crec) {
		cf_warning(AS_LDT, "Invalid Paramters: as=%p, record=%p", as, crec);
		return 2;
	}
	if (!udf_record_ldt_enabled(crec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled");
		return -3;
	}

	udf_record   * c_urecord = (udf_record *)as_rec_source(crec);
	if (!c_urecord) {
		cf_warning(AS_LDT, "subrecord_update: Internal Error !! Malformed Sub Record !!... Fail");
		return -1;
	}
	ldt_record   * lrecord  = (ldt_record *)c_urecord->lrecord;
	if (!lrecord) {
		cf_warning(AS_LDT, "subrecord_update: Internal Error !! Invalid Head Record Reference in Sub Record !!... Fail");
		return -1;
	}
	as_aerospike * las  = lrecord->as;
	cf_debug(AS_LDT, "Calling as_aerospike_rec_update() ldt_aerospike_crec_update" );
	return as_aerospike_rec_remove(las, crec);
}

static int
ldt_aerospike_crec_update(const as_aerospike * as, const as_rec *crec)
{
	cf_detail(AS_LDT, "[ENTER] as(%p) subrec(%p)", as, crec );
	if (!as || !crec) {
		cf_warning(AS_LDT, "Invalid Paramters: as=%p, record=%p subrecord=%p", as, crec);
		return 2;
	}
	if (!udf_record_ldt_enabled(crec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled");
		return 3;
	}

	udf_record   * c_urecord = (udf_record *)as_rec_source(crec);
	if (!c_urecord) {
		cf_warning(AS_LDT, "subrecord_update: Internal Error !! Malformed Sub Record !!... Fail");
		return -1;
	}
	ldt_record   * lrecord  = (ldt_record *)c_urecord->lrecord;
	if (!lrecord) {
		cf_warning(AS_LDT, "subrecord_update: Internal Error !! Invalid Head Record Reference in Sub Record !!... Fail");
		return -1;
	}
	as_aerospike * las  = lrecord->as;
	cf_detail(AS_LDT, "Calling as_aerospike_rec_update() ldt_aerospike_crec_update");
	return as_aerospike_rec_update(las, crec);
}

int
ldt_aerospike_crec_close(const as_aerospike * as, const as_rec *crec_p)
{
	cf_detail(AS_LDT, "[ENTER] as(%p) subrec(%p)", as, crec_p );
	if (!as || !crec_p) {
		cf_warning(AS_LDT, " %s Invalid Paramters: as=%p, subrecord=%p",
				"ldt_aerospike_crec_close", as, crec_p);
		return 2;
	}

	// Close of the record is only allowed if the user has not updated
	// it. Other wise it is a group commit
	udf_record *c_urecord = (udf_record *)as_rec_source(crec_p);
	if (!c_urecord) {
		cf_warning(AS_LDT, "subrecord_close: Internal Error !! Malformed Sub Record !!... Fail");
		return -1;
	}
	ldt_record  *lrecord  = (ldt_record *)c_urecord->lrecord;
	if (!lrecord) {
		cf_warning(AS_LDT, "subrecord_close: Internal Error !! Invalid Head Record Reference in Sub Record !!... Fail");
		return -1;
	}
	int slot              = -1;
	for (int i = 0; i < lrecord->max_chunks; i++) {
		if (lrecord->chunk[i].c_urec_p == crec_p) {
			slot = i;
		}
	}
	if (slot == -1) {
		cf_warning(AS_LDT, "subrecord_close called for the record which is not open");
		return -1;
	}
	cf_detail(AS_LDT, "ldt_aerospike_crec_close");
	if (c_urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) {
		cf_detail(AS_LDT, "Cannot close record with update ... it needs group commit");
		return -2;
	}
	udf_record_close(c_urecord, false);
	udf_record_cache_free(c_urecord);
	ldt_chunk_destroy(lrecord, slot);
	c_urecord->flag &= ~UDF_RECORD_FLAG_ISVALID;
	return 0;
}

static as_rec *
ldt_aerospike_crec_open(const as_aerospike * as, const as_rec *rec, const char *bdig)
{
	static char * meth = "ldt_aerospike_crec_open()";
	if (!as || !rec || !bdig) {
		cf_warning(AS_LDT, "%s Invalid Paramters: as=%p, record=%p digest=%p", meth, as, rec, bdig);
		return NULL;
	}
	cf_digest keyd;
	if (as_ldt_string_todigest(bdig, &keyd)) {
		return NULL;
	}
	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return NULL;
	}
	if (!udf_record_ldt_enabled(lrecord->h_urec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled");
		return NULL;
	}
	as_ldt_subdigest_setversion(&keyd, lrecord->version);
	int slot            = -1;
	int rv              = ldt_crec_open(lrecord, &keyd, &slot);
	if (rv) {
		// This basically means the record is not found.
		// Do we need to propagate error message rv
		// back somehow
		cf_info_digest(AS_LDT, &keyd, "Failed to open Sub Record rv=%d %ld", rv, lrecord->version);
		return NULL;
	} else {
		as_val_reserve(lrecord->chunk[slot].c_urec_p);
		return lrecord->chunk[slot].c_urec_p;
	}
}

static int
ldt_aerospike_rec_update(const as_aerospike * as, const as_rec * rec)
{
	static const char * meth = "ldt_aerospike_rec_update()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s Invalid Paramters: as=%p, record=%p", meth, as, rec);
		return 2;
	}
	cf_detail(AS_LDT, "[ENTER]<%s> as(%p) rec(%p)", meth, as, rec );

	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	as_rec *h_urec      = lrecord->h_urec;
	as_aerospike *las   = lrecord->as;
	int ret = as_aerospike_rec_update(las, h_urec);
	if (0 == ret) {
		cf_debug(AS_LDT, "<%s> ZERO return(%d) from as_aero_rec_update()", meth, ret );
	} else if (ret == -1) {
		// execution error return as it is
		cf_debug(AS_LDT, "<%s> Exec Error(%d) from as_aero_rec_update()", meth, ret );
	} else if (ret == -2) {
		cf_warning(AS_LDT, "<%s> Unexpected return(%d) from as_aero_rec_update()", meth, ret );
		// Record is not open. Unexpected.  Should not reach here.
	}
	return ret;
}

static int
ldt_aerospike_rec_exists(const as_aerospike * as, const as_rec * rec)
{
	static const char * meth = "ldt_aerospike_rec_exists()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s Invalid Paramters: as=%p, record=%p", meth, as, rec);
		return 2;
	}
	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}

	as_rec *h_urec      = lrecord->h_urec;
	as_aerospike *las   = lrecord->as;
	int ret = as_aerospike_rec_exists(las, h_urec);
	if (ret) {
		cf_detail(AS_LDT, "ldt_aerospike_rec_exists true");
	} else {
		cf_detail(AS_LDT, "ldt_aerospike_rec_exists false");
	}
	return ret;
}

static int
ldt_aerospike_rec_remove(const as_aerospike * as, const as_rec * rec)
{
	static const char * meth = "ldt_aerospike_rec_remove()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s Invalid Paramters: as=%p, record=%p", meth, as, rec);
		return 2;
	}
	// Delete needs propagation
	cf_detail(AS_LDT, "ldt_aerospike_rec_remove");
	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	as_rec *h_urec      = lrecord->h_urec;
	as_aerospike *las   = lrecord->as;

	FOR_EACH_SUBRECORD(i, lrecord) {
		as_aerospike_rec_remove(las, lrecord->chunk[i].c_urec_p);
		ldt_chunk_destroy(lrecord, i);
	}
	return as_aerospike_rec_remove(las, h_urec);
}

static int
ldt_aerospike_log(const as_aerospike * a, const char * file,
				  const int line, const int lvl, const char * msg)
{
	a = a;
	// Logging for Lua Files (UDFs) should be labeled as "UDF", not "LDT".
	// If we want to distinguish between LDT and general UDF calls, then we
	// need to create a separate context for LDT.
	cf_fault_event(AS_UDF, lvl, file, NULL, line, (char *) msg);
	return 0;
}

static void
ldt_aerospike_destroy(as_aerospike * as)
{
	// Destruction flows back into udf_aerospike and implemented as NULL
	// today
	as_aerospike_destroy(as);
}

/**
 * Provide Lua UDFs with the ability to get the current system time.  We'll
 * take the current time value (expressed as a cf_clock object) and plug it into
 * a lua value.
 */
static cf_clock
ldt_aerospike_get_current_time(const as_aerospike * as)
{
	as = as;
	// Does anyone really know what time it is?
	return cf_clock_getabsolute();

} // end ldt_aerospike_get_current_time()

const as_aerospike_hooks ldt_aerospike_hooks = {
	.rec_create       = ldt_aerospike_rec_create,
	.rec_update       = ldt_aerospike_rec_update,
	.rec_remove       = ldt_aerospike_rec_remove,
	.rec_exists       = ldt_aerospike_rec_exists,
	.log              = ldt_aerospike_log,
	.destroy          = ldt_aerospike_destroy,
	.get_current_time = ldt_aerospike_get_current_time,
	.remove_subrec    = ldt_aerospike_crec_remove,
	.create_subrec    = ldt_aerospike_crec_create,
	.close_subrec     = ldt_aerospike_crec_close,
	.open_subrec      = ldt_aerospike_crec_open,
	.update_subrec    = ldt_aerospike_crec_update
};
