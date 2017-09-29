/*
 * ldt_aerospike.c
 *
 * Copyright (C) 2013-2016 Aerospike, Inc.
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

#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/ldt_record.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "fabric/fabric.h"
#include "transaction/udf.h"


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



/*
 * Slot and Chunk create destroy
 */
// **************************************************************************************************

/*
 * create_slot  : Creates slot and initializes variable
 * create_chunk : Creates chunk and initializes
 * expand_chunk : Expands chunk by single unit
 *
 * Note: The idea of having a extra level indirection for chunk and slot instead
 *       of single array is because the address values of tr/rd/r_ref extra is
 *       stored and used. Realloc may end up moving these to different memory
 *       location, invalidating stored values.
 *
 * Return value:
 * 		Slot functions
 * 			valid slot pointer in case of success
 * 			NULL in case of failure
 * 		Chunk functions
 * 			0 in case of success
 * 			-1 in case of failure
 */
ldt_slot *
create_slot()
{
	ldt_slot   *slots = cf_malloc(sizeof(ldt_slot) * LDT_SLOT_CHUNK_SIZE);
	if (!slots) {
		return NULL;
	}
	return slots;
}

int
create_chunk(ldt_record *lrecord)
{
	lrecord->chunk   = cf_malloc(sizeof(ldt_slot_chunk));
	if (!lrecord->chunk) {
		return -1;
	}
	lrecord->chunk[0].slots = create_slot();
	if (lrecord->chunk[0].slots == NULL) {
		cf_free(lrecord->chunk);
		return -1;
	}

	for(int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++) {
		lrecord->chunk[0].slots[j].inuse = false;
	}

	return 0;
}

int
expand_chunk(ldt_record *lrecord)
{
	uint64_t   new_size  = lrecord->max_chunks + 1;
	void *old_chunk      = lrecord->chunk;

	if (lrecord->max_chunks) {
		lrecord->chunk = cf_realloc(lrecord->chunk, sizeof(ldt_slot_chunk) * new_size);
	} else {
		lrecord->chunk = cf_malloc(sizeof(ldt_slot_chunk) * new_size);
	}

	if (lrecord->chunk == NULL) {
		cf_warning(AS_LDT, "expand_chunk: Allocation Error !! [Chunk cannot be allocated ]... Fail");
		lrecord->chunk = old_chunk;
		return -1;
	}

	lrecord->chunk[lrecord->max_chunks].slots = create_slot();
	if (lrecord->chunk[lrecord->max_chunks].slots == NULL) {
		cf_warning(AS_LDT, "expand_chunk: Allocation Error !! [Slot cannot be allocated ]... Fail");
		cf_free(lrecord->chunk);
		lrecord->chunk = old_chunk;
		return -1;
	}

	for (int i = lrecord->max_chunks; i < new_size; i++) {
		for(int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++) {
			lrecord->chunk[i].slots[j].inuse = false;
		}
	}

	cf_detail(AS_LDT, "Bumping up chunks from %"PRIu64" to %"PRIu64"", lrecord->max_chunks, new_size);
	lrecord->max_chunks = new_size;
	return 0;
}

int
chunk_print(ldt_slot *lslotp)
{
	udf_record *c_urecord = &lslotp->c_urecord;

	cf_detail(AS_LDT, "LSO CHUNK: slotp = %p lchunk [%p,%p,%p,%p] ", lslotp,
				&lslotp->c_urecord, &lslotp->tr, &lslotp->rd, &lslotp->r_ref);
	cf_detail(AS_LDT, "LSO CHUNK: slotp = %p urecord   [%p,%p,%p,%p] ", lslotp,
				c_urecord, c_urecord->tr, c_urecord->rd, c_urecord->r_ref);
	return 0;
}
// **************************************************************************************************



/*
 * Slot Lookup Functions
 */
// **************************************************************************************************
/*
 * Search if the sub record is already open by digest/passed as_rec pointer.
 *
 * Return value:
 * 		>=0  slot if found
 * 		-1   in case not found
 */
ldt_slot *
slot_lookup_by_digest(ldt_record *lrecord, cf_digest *keyd)
{
	for (int i = 0; i < lrecord->max_chunks; i++) {
		ldt_slot_chunk *lchunk = &lrecord->chunk[i];
		for (int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++) {
			if (lchunk->slots[j].inuse &&
				(0 == cf_digest_compare(&lchunk->slots[j].rd.r->keyd, keyd))) {
				return &lchunk->slots[j];
			}
		}
	}
	return NULL;
}

ldt_slot *
slot_lookup_by_urec(ldt_record *lrecord, const as_rec *c_urec_p)
{
	for (int i = 0; i < lrecord->max_chunks; i++) {
		ldt_slot_chunk *lchunk = &lrecord->chunk[i];
		for (int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++) {
			if (lchunk->slots[j].inuse && (lchunk->slots[j].c_urec_p == c_urec_p)) {
				return &lchunk->slots[j];
			}
		}
	}
	return NULL;
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
ldt_slot *
slot_lookup_free(ldt_record *lrecord, char *func)
{
	if (lrecord->num_slots_used == (lrecord->max_chunks * LDT_SLOT_CHUNK_SIZE)) {
		if (expand_chunk(lrecord)) {
			goto Out;
		}
	}

	for (int i = 0; i < lrecord->max_chunks; i++) {
		ldt_slot_chunk *chunk = &lrecord->chunk[i];
		for (int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++) {
			if (!chunk->slots[j].inuse) {
				lrecord->num_slots_used++;
				chunk->slots[j].inuse = true;
				cf_detail(AS_LDT, "%s Popped slot %p %"PRIu64"", func, &chunk->slots[j], lrecord->num_slots_used);
				return &chunk->slots[j];
			}
		}
	}
Out:
	cf_warning(AS_LDT, "%s: Allocation Error [Cannot open more than (%"PRIu64") Sub-Records in a single UDF]... Fail",
				func, lrecord->num_slots_used);
	return NULL;
}
// **************************************************************************************************


/*
 * Slot setup/init/cleanup functions
 */
// **************************************************************************************************

/*
 * Slot
 *    init:
 *    setup:
 *    destroy:
 */

/*
 *  Sets up udf_record
 * 	Zeroes out stuff
 * 	Setups as_rec in the lchunk
 * 	Sets up transaction and digest
 * 	Sets up the partition reservation (same as parent)
 */
void
slot_init(ldt_slot *lslotp, ldt_record *lrecord)
{
	// It is just a stub fill the proper values in
	udf_record *c_urecord   = &lslotp->c_urecord;
	udf_record_init(c_urecord, true);
	// note: crec cannot be destroyed from inside lua
	c_urecord->flag        |= UDF_RECORD_FLAG_IS_SUBRECORD;
	c_urecord->lrecord      = (void *)lrecord;
	c_urecord->tr           = &lslotp->tr; // set up tr properly
	c_urecord->rd           = &lslotp->rd;
	c_urecord->r_ref        = &lslotp->r_ref;
	lslotp->r_ref.skip_lock = true;
	lslotp->c_urec_p = as_rec_new(c_urecord, &udf_record_hooks);

	udf_record     * h_urecord = (udf_record *)as_rec_source(lrecord->h_urec);
	as_transaction * h_tr      = h_urecord->tr;
	as_transaction * c_tr      = &lslotp->tr;

	// Chunk Record Does not respond for proxy request
	c_tr->from.any             = NULL;       // Need not reply
	c_tr->from_data.any        = 0;          // ??

	// Chunk Record Does not respond back to the client
	c_tr->result_code          = 0;
	c_tr->generation           = 0;
	c_tr->void_time            = 0;
	// Set this to grab some info from the msg from client like
	// set name etc ... we do not set it in wr..
	c_tr->msgp                 = h_tr->msgp;

	// We do not track microbenchmark or time for chunk today
	c_tr->benchmark_time  = 0;
	c_tr->start_time           = h_tr->start_time;
	c_tr->end_time             = h_tr->end_time;

	// Chunk transaction is always preprocessed
	c_tr->msg_fields           = h_tr->msg_fields;
	c_tr->from_flags           = 0;
	c_tr->flags                = 0;

	// Parent reservation cannot go away as long as Chunck needs reservation.
	memcpy(&c_tr->rsv, &h_tr->rsv, sizeof(as_partition_reservation));
	//ldt_slot_print(lslotp);
}

void
slot_setup_digest(ldt_slot *lslotp, cf_digest *keyd)
{
	as_transaction * c_tr      = &lslotp->tr;
	c_tr->keyd                 = *keyd;
	udf_record *c_urecord      = (udf_record *)as_rec_source(lslotp->c_urec_p);
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
	as_ldt_subdigest_resetversion(&c_urecord->keyd);

}

/**
 * Remove the slot entry, but decrement the count only if the destroy() op
 * was successful.
 */
static void
slot_destroy(ldt_slot *lslotp, ldt_record *lrecord)
{
	if (lslotp->c_urec_p) {
		udf_record_destroy(lslotp->c_urec_p);
		as_rec_destroy(lslotp->c_urec_p);
		lslotp->c_urec_p = NULL;
		lrecord->num_slots_used--;
		lslotp->inuse = false;
	} else {
		cf_warning(AS_LDT, "slot_destroy: Internal Error [Attempt to free invalid slot] ... Skipped");
	}
}

void
chunk_destroy(ldt_record *lrecord, ldt_slot_chunk *lchunk)
{
	for (int j = 0; j < LDT_SLOT_CHUNK_SIZE; j++) {
		if (lchunk->slots[j].inuse) {
			ldt_slot *lslotp      = &lchunk->slots[j];
			slot_destroy(lslotp, lrecord);
		}
	}
	cf_free(lchunk->slots);
	lchunk->slots         = NULL;
}
// **************************************************************************************************


/*
 * WorkHorse CREC Management Functions
 */
// **************************************************************************************************

/*
 * Function to open chunk record
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
crec_open(ldt_record *lrecord, cf_digest *keyd, ldt_slot **lslotp)
{
	cf_detail_digest(AS_LDT, keyd, "[ENTER] crec_open(): Digest: ");

	// 1. Search in opened record
	*lslotp = slot_lookup_by_digest(lrecord, keyd);
	if (*lslotp) {
		cf_debug(AS_LDT, "ldt_aerospike_crec_open : Found already open");
		return 0;
	}

	// 2. Find free slot and setup chunk
	*lslotp     = slot_lookup_free(lrecord, "crec_open");
	if (!*lslotp) {
		return -2;
	}
	slot_init(*lslotp, lrecord);
	slot_setup_digest(*lslotp, keyd);

	// 3. Open Record
	int rv = udf_record_open((udf_record *)as_rec_source((*lslotp)->c_urec_p));
	if (rv) {
		// free the slot for reuse
		slot_destroy(*lslotp, lrecord);
		*lslotp = NULL;
		return -3;
	}
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
#define LDT_SUBRECORD_RANDOMIZER_MAX_RETRIES 2
as_rec *
crec_create(ldt_record *lrecord)
{
	// Generate Key Digest
	udf_record *h_urecord = (udf_record *) as_rec_source(lrecord->h_urec);
	cf_digest keyd        = h_urecord->r_ref->r->keyd;
	as_namespace *ns      = h_urecord->tr->rsv.ns;
	int retry_cnt         = 0;
	ldt_slot *lslotp      = slot_lookup_free(lrecord, "crec_create");

	if (!lslotp) {
		cf_crash(AS_LDT, "Allocation error !!!");
	}
	slot_init(lslotp, lrecord);

	while (retry_cnt++ < LDT_SUBRECORD_RANDOMIZER_MAX_RETRIES) {

		as_ldt_digest_randomizer(&keyd);
		as_ldt_subdigest_setversion(&keyd, lrecord->version);
		slot_setup_digest(lslotp, &keyd);

		int rv = as_aerospike_rec_create(lrecord->as, lslotp->c_urec_p);

		// rv == 0 if successful
		// rv == 1 if record is already found retry
		// other wise failure
		if (rv == 0) {
			cf_detail_digest(AS_LDT, &keyd, "Crec Create:Ptr(%p) Digest: version %ld", lslotp->c_urec_p, lrecord->version);
			as_val_reserve(lslotp->c_urec_p);
			return lslotp->c_urec_p;
		}

		if (rv != 1) {
			cf_warning(AS_LDT, "crec_create: LDT Sub-Record Create Error [rv=%d]... Fail", rv);
			break;
		}
		cf_atomic64_incr(&ns->lstats.ldt_randomizer_retry);
	}

	slot_destroy(lslotp, lrecord);
	cf_warning_digest(AS_LDT, &keyd, "ldt_aerospike_crec_create : Create failed after %d retries", retry_cnt);
	return NULL;
}


/*
 * lrecord init and cleanup funtions
 */
// **************************************************************************************************
void
ldt_record_init(ldt_record *lrecord)
{
	// h_urec is setup in udf_rw.c which point to the main record
	lrecord->h_urec         = 0;
	lrecord->as             = &g_as_aerospike;
	lrecord->max_chunks     = 0;
	lrecord->num_slots_used = 0;
	lrecord->version        = 0;
	lrecord->subrec_io      = 0;
	// Default is normal UDF
	lrecord->udf_context    = 0;
}

int
ldt_record_destroy(ldt_record *lrecord)
{
	if (!lrecord) {
		return -1;
	}
	as_rec *h_urec      = lrecord->h_urec;

	// Note: destroy of udf_record today is no-op because all the closing
	// of record happens after UDF has executed.
	for (uint64_t i = 0; i < lrecord->max_chunks; i++) {
		ldt_slot_chunk *lchunk = &lrecord->chunk[i];
		chunk_destroy(lrecord, lchunk);
	}

	if (lrecord->max_chunks) {
		// Free up allocated chunks
		cf_free(lrecord->chunk);
	}
	// Dir destroy should release partition reservation and
	// namespace reservation.
	udf_record_destroy(h_urec);
	return 0;
}
// **************************************************************************************************


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
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p]... Fail", meth, as, rec);
		return 2;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	as_rec *h_urec       = lrecord->h_urec;
	as_aerospike *las    = lrecord->as;

	// If record is newly created and were created by LDT lua then it
	// would have already set starting version ... read that into the
	// lrecord->version for quick reference.
	lrecord->version         = as_ldt_generate_version();

	int rv = as_aerospike_rec_create(las, h_urec);
	if (rv) {
		return rv;
	}

	udf_record   * h_urecord = (udf_record *)as_rec_source(h_urec);
	cf_detail_digest(AS_LDT, &h_urecord->keyd, "LDT_VERSION At Create %ld rv=%d", lrecord->version, rv);
	return 0;
}

static as_rec *
ldt_aerospike_crec_create(const as_aerospike * as, const as_rec *rec)
{
	static char * meth = "ldt_aerospike_crec_create()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p]... Fail", meth, as, rec);
		return NULL;
	}
	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return NULL;
	}
	if (!udf_record_ldt_enabled(lrecord->h_urec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled !!... Fail");
		return NULL;
	}
	cf_detail(AS_LDT, "ldt_aerospike_crec_create");
	return crec_create(lrecord);
}

static int
ldt_aerospike_crec_remove(const as_aerospike * as, const as_rec * crec)
{
	if (!as || !crec) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_remove: Invalid Parameters [as=%p, record=%p]... Fail", as, crec);
		return 2;
	}
	if (!udf_record_ldt_enabled(crec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled... Fail");
		return -3;
	}

	udf_record   * c_urecord = (udf_record *)as_rec_source(crec);
	if (!c_urecord) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_remove: Internal Error [Malformed Sub Record]... Fail");
		return -1;
	}
	ldt_record   * lrecord  = (ldt_record *)c_urecord->lrecord;
	if (!lrecord) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_remove: Internal Error [Invalid Head Record Reference in Sub Record]... Fail");
		return -1;
	}
	as_aerospike * las  = lrecord->as;
	cf_detail(AS_LDT, "Calling as_aerospike_rec_update() ldt_aerospike_crec_update" );
	return as_aerospike_rec_remove(las, crec);
}

static int
ldt_aerospike_crec_update(const as_aerospike * as, const as_rec *crec)
{
	cf_detail(AS_LDT, "[ENTER] as(%p) subrec(%p)", as, crec );
	if (!as || !crec) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_update: Invalid Parameters [as=%p, subrecord=%p]... Fail", as, crec);
		return 2;
	}
	if (!udf_record_ldt_enabled(crec)) {
		cf_warning(AS_LDT, "Large Object Not Enabled... Fail");
		return 3;
	}

	udf_record   * c_urecord = (udf_record *)as_rec_source(crec);
	if (!c_urecord) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_update: Internal Error [Malformed Sub Record]... Fail!!");
		return -1;
	}
	ldt_record   * lrecord  = (ldt_record *)c_urecord->lrecord;
	if (!lrecord) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_update: Internal Error [Invalid Head Record Reference in Sub Record]... Fail!!");
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
		cf_warning(AS_LDT, "ldt_aerospike_crec_close: Invalid Parameters [as=%p, subrecord=%p]... Fail", as, crec_p);
		return 2;
	}

	// Close of the record is only allowed if the user has not updated
	// it. Other wise it is a group commit
	udf_record *c_urecord = (udf_record *)as_rec_source(crec_p);
	if (!c_urecord) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_close: Internal Error [Malformed Sub Record]... Fail");
		return -1;
	}
	ldt_record  *lrecord  = (ldt_record *)c_urecord->lrecord;
	if (!lrecord) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_close: Internal Error [Invalid Head Record Reference in Sub Record]... Fail");
		return -1;
	}

	ldt_slot *lslotp   = slot_lookup_by_urec(lrecord, crec_p);
	if (!lslotp) {
		cf_warning(AS_LDT, "ldt_aerospike_crec_close: Invalid Operation [Sub Record close called for the record which is not open]... Fail");
		return -1;
	}
	cf_detail(AS_LDT, "ldt_aerospike_crec_close");
	if (c_urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) {
		cf_debug(AS_LDT, "Cannot close record with update ... it needs group commit");
		return -2;
	}
	udf_record_close(c_urecord);
	udf_record_cache_free(c_urecord);
	slot_destroy(lslotp, lrecord);
	c_urecord->flag &= ~UDF_RECORD_FLAG_ISVALID;
	return 0;
}

static as_rec *
ldt_aerospike_crec_open(const as_aerospike * as, const as_rec *rec, const char *bdig)
{
	static char * meth = "ldt_aerospike_crec_open()";
	if (!as || !rec || !bdig) {
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p digest=%p]... Fail", meth, as, rec, bdig);
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
		cf_warning(AS_LDT, "Large Object Not Enabled... Fail");
		return NULL;
	}
	as_ldt_subdigest_setversion(&keyd, lrecord->version);
	ldt_slot    *lslotp = NULL;
	int rv              = crec_open(lrecord, &keyd, &lslotp);
	if (rv) {
		// This basically means the record is not found.
		// Do we need to propagate error message rv
		// back somehow
		cf_info_digest(AS_LDT, &keyd, "%s Failed to open Sub Record rv=%d %ld", bdig, rv, lrecord->version);
		return NULL;
	} else {
		as_val_reserve(lslotp->c_urec_p);
		return lslotp->c_urec_p;
	}
}

static int
ldt_aerospike_rec_update(const as_aerospike * as, const as_rec * rec)
{
	static const char * meth = "ldt_aerospike_rec_update()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p]... Fail", meth, as, rec);
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
		cf_detail(AS_LDT, "<%s> ZERO return(%d) from as_aero_rec_update()", meth, ret );
	} else if (ret == -1) {
		// execution error return as it is
		cf_debug(AS_LDT, "<%s> Exec Error(%d) from as_aero_rec_update()", meth, ret );
	} else if (ret == -2) {
		// Record is not open. Unexpected with LDT usage, though a UDF test case
		// does come through here.
		cf_warning(AS_LDT, "%s: Record does not exist or is not open, cannot update", meth);
	}
	return ret;
}

static int
ldt_aerospike_rec_exists(const as_aerospike * as, const as_rec * rec)
{
	static const char * meth = "ldt_aerospike_rec_exists()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s Invalid Parameters: as=%p, record=%p", meth, as, rec);
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
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p]... Fail", meth, as, rec);
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

	FOR_EACH_SUBRECORD(i, j, lrecord) {
		as_aerospike_rec_remove(las, lrecord->chunk[i].slots[j].c_urec_p);
	}

	for (int i = 0; i < lrecord->max_chunks; i++) {
		ldt_slot_chunk *lchunk = &lrecord->chunk[i];
		chunk_destroy(lrecord, lchunk);
	}

	return as_aerospike_rec_remove(las, h_urec);
}

static int
ldt_aerospike_log(const as_aerospike * a, const char * file,
				  const int line, const int lvl, const char * msg)
{
	(void)a;
	// Logging for Lua Files (UDFs) should be labeled as "UDF", not "LDT".
	// If we want to distinguish between LDT and general UDF calls, then we
	// need to create a separate context for LDT.
	cf_fault_event(AS_UDF, lvl, file, line, "%s", (char *) msg);
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
	(void)as;
	// Does anyone really know what time it is?
	return cf_clock_getabsolute();

}

/**
 * Provide hook from Lua to set execution context
 */
static int
ldt_aerospike_set_context(const as_aerospike * as, const as_rec *rec, const uint32_t context)
{
	(void)as;
	static const char * meth = "ldt_aerospike_set_context()";
	if (!as || !rec) {
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p]... Fail", meth, as, rec);
		return 2;
	}
	// Delete needs propagation
	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}

	udf_record *h_urecord = (udf_record *)as_rec_source(lrecord->h_urec);
    if (h_urecord->tr->rsv.ns->ldt_enabled == false) {
		return -1;
	}

	cf_detail(AS_LDT, "ldt_aerospike_set_context from %d to %d", lrecord->udf_context, lrecord->udf_context | context);
	lrecord->udf_context |= context;

	return 0;
}

/**
 * Provide hook from Lua to fetch server config settings
 */
static int
ldt_aerospike_get_config(const as_aerospike * as, const as_rec *rec, const char *name)
{
	static const char * meth = "ldt_aerospike_get_config()";
	if (!as || !rec || !name) {
		cf_warning(AS_LDT, "%s: Invalid Parameters [as=%p, record=%p]... Fail", meth, as, rec);
		return 2;
	}

	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}

    int val = 0;

	if (strcmp(name, "write-block-size") == 0) {
		udf_record *h_urecord = (udf_record *)as_rec_source(lrecord->h_urec);
		val = h_urecord->tr->rsv.ns->storage_write_block_size;
	} else if (strcmp(name, "ldt-page-size") == 0) {
		udf_record *h_urecord = (udf_record *)as_rec_source(lrecord->h_urec);
		val = h_urecord->tr->rsv.ns->ldt_page_size;
	} else {
		cf_warning(AS_LDT, "Unknown config requested");
	}
    cf_detail(AS_LDT, "Returning %s = %d" ,name, val);
	return val;
}


const as_aerospike_hooks ldt_aerospike_hooks = {
	.rec_create       = ldt_aerospike_rec_create,
	.rec_update       = ldt_aerospike_rec_update,
	.rec_remove       = ldt_aerospike_rec_remove,
	.rec_exists       = ldt_aerospike_rec_exists,
	.log              = ldt_aerospike_log,
	.destroy          = ldt_aerospike_destroy,
	.get_current_time = ldt_aerospike_get_current_time,
	.set_context      = ldt_aerospike_set_context,
	.remove_subrec    = ldt_aerospike_crec_remove,
	.create_subrec    = ldt_aerospike_crec_create,
	.close_subrec     = ldt_aerospike_crec_close,
	.open_subrec      = ldt_aerospike_crec_open,
	.update_subrec    = ldt_aerospike_crec_update,
	.get_config       = ldt_aerospike_get_config
};
