/*
 * udf_record.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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

#include "base/udf_record.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_rec.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/transaction.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"


bool
udf_record_ldt_enabled(const as_rec * rec)
{
	udf_record *urecord = (udf_record *)as_rec_source(rec);
	if (!urecord)         return false;
	as_namespace *ns    = urecord->tr->rsv.ns;
	if (!ns)              return false;
	if (!ns->ldt_enabled) return false;
	else        		  return true;
}
/*
 * Function: Open storage record for passed in udf record
 *           also set up flag like exists / read et al.
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value :  0 on success
 * 				  -1 if the record's bin count exceeds the UDF limit
 *
 * Callers:
 * 		udf_record_open
 *
 * Note: There are no checks, so the caller has to make sure that all
 *       protections are taken and all checks are done.
 *
 *  Side effect:
 *  	Counters will be reset
 *  	flag will be set
 *  	bins will be opened
 */
int
udf_storage_record_open(udf_record *urecord)
{
	cf_debug_digest(AS_UDF, &urecord->tr->keyd, "[ENTER] Opening record key:");
	as_storage_rd  *rd    = urecord->rd;
	as_index       *r	  = urecord->r_ref->r;
	as_transaction *tr    = urecord->tr;

	as_storage_record_open(tr->rsv.ns, r, rd);

	// Deal with delete durability (enterprise only).
	if ((urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES) != 0 &&
			(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) == 0 &&
			set_delete_durablility(tr, rd) != 0) {
		as_storage_record_close(rd);
		return -1;
	}

	as_storage_rd_load_n_bins(rd); // TODO - handle error returned

	if (rd->n_bins > UDF_RECORD_BIN_ULIMIT) {
		cf_warning(AS_UDF, "record has too many bins (%d) for UDF processing", rd->n_bins);
		as_storage_record_close(rd);
		return -1;
	}

	// if multibin storage, we will use urecord->stack_bins, so set the size appropriately
	if ( ! tr->rsv.ns->storage_data_in_memory && ! tr->rsv.ns->single_bin ) {
		rd->n_bins = sizeof(urecord->stack_bins) / sizeof(as_bin);
	}

	as_storage_rd_load_bins(rd, urecord->stack_bins); // TODO - handle error returned
	urecord->starting_memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	as_storage_record_get_key(rd);

	urecord->flag   |= UDF_RECORD_FLAG_STORAGE_OPEN;

	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		urecord->lrecord->subrec_io++;
	}

	cf_detail_digest(AS_UDF, &tr->keyd, "Storage Open: Rec(%p) flag(%x) Digest:", urecord, urecord->flag );
	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		as_ldt_subrec_storage_validate(rd, "Reading");
	}
	return 0;
}

/*
 * Function: Close storage record if it open and also set flags
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value : 0 in case storage was open
 *                1 in case storage was not open
 *
 * Callers:
 * 		udf_record_close
 *
 *  Side effect:
 *  	flag will be reset
 *  	bins will be closed
 */
int
udf_storage_record_close(udf_record *urecord)
{
	if (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		as_index_ref   *r_ref = urecord->r_ref;
		as_storage_rd  *rd    = urecord->rd;

		// In case allow update is not set .. the record has been opened for
		// the aggregation. Do not do any rec property update.
		// Pick info from index and put it in storage record.
		size_t  rec_props_data_size = as_storage_record_rec_props_size(rd);
		uint8_t rec_props_data[rec_props_data_size];
		if (urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES) {
			if (rec_props_data_size > 0) {
				cf_detail(AS_LDT, "LDT_INDEXBITS Setting Property at close time parent=%d, esr=%d, sub=%d",
						  as_ldt_record_is_parent(rd->r),
						  as_ldt_record_is_esr(rd->r),
						  as_ldt_record_is_subrec(rd->r));
				as_storage_record_set_rec_props(rd, rec_props_data);
			}
		}

		bool is_subrec = (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) != 0;
		bool has_bins = as_bin_inuse_has(rd);

		if (! is_subrec) {
			if (as_ldt_record_is_parent(rd->r)) {
				cf_detail_digest(AS_LDT, &rd->r->keyd, "LDT_INDEXBIT Parent @ write: Digest:");
			}
		} else if (has_bins) {
			as_ldt_subrec_storage_validate(rd, "Writing");
		}

		if (r_ref) {
			if (urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) {
				as_storage_record_write(rd);
				urecord->flag &= ~UDF_RECORD_FLAG_HAS_UPDATES; // TODO - necessary?
			}

			if (! has_bins) {
				write_delete_record(r_ref->r, is_subrec ?
						urecord->tr->rsv.sub_tree : urecord->tr->rsv.tree);
			}

			as_storage_record_close(rd);
		} else {
			// Should never happen.
			cf_warning(AS_UDF, "Unexpected Internal Error (null r_ref)");
		}

		urecord->flag &= ~UDF_RECORD_FLAG_STORAGE_OPEN;
		cf_detail_digest(AS_UDF, &urecord->tr->keyd, "Storage Close:: Rec(%p) Flag(%x) Digest:",
				urecord, urecord->flag );
		return 0;
	} else {
		return 1;
	}
}

/*
 * Function: Open storage record for passed in udf record
 *           also set up flag like exists / read et al.
 *           Does as_record_get as well if it is not done yet.
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value :
 *  	 0 in case record is successfully read
 * 		-1 in case record is not found
 * 		-2 in case record is found but has expired
 *
 * Callers:
 * 		query_agg_istream_read
 * 		ldt_crec_open
 */
int
udf_record_open(udf_record * urecord)
{
	cf_debug_digest(AS_UDF, &urecord->tr->keyd, "[ENTER] Opening record key:");
	if (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		cf_info(AS_UDF, "Record already open");
		return 0;
	}
	as_transaction *tr    = urecord->tr;
	as_index_ref   *r_ref = urecord->r_ref;
	as_index_tree  *tree  = tr->rsv.tree;
	bool is_subrec = (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) != 0;

	if (is_subrec) {
		tree = tr->rsv.sub_tree;
	}

	int rec_rv = 0;
	if (!(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		cf_detail(AS_UDF, "Opening %sRecord ", is_subrec ? "Sub" : "");
		rec_rv = as_record_get_live(tree, &tr->keyd, r_ref, tr->rsv.ns);
	}

	if (!rec_rv) {
		as_index *r = r_ref->r;
		// check to see this isn't an expired record waiting to die
		if (! is_subrec && as_record_is_doomed(r, tr->rsv.ns)) {
			as_record_done(r_ref, tr->rsv.ns);
			cf_detail(AS_UDF, "udf_record_open: Record has expired cannot read");
			rec_rv = -2;
		} else {
			urecord->flag   |= UDF_RECORD_FLAG_OPEN;
			urecord->flag   |= UDF_RECORD_FLAG_PREEXISTS;
			cf_detail_digest(AS_UDF, &tr->keyd, "Open %p %x Digest:", urecord, urecord->flag);
			rec_rv = udf_storage_record_open(urecord);
		}
	} else {
		cf_detail_digest(AS_UDF, &urecord->tr->keyd, "udf_record_open: %s rec_get returned with %d", 
				is_subrec ? "sub" : "", rec_rv);
	}
	return rec_rv;
}

/*
 * Function: Close storage record for udf record. Release
 *           all locks and partition reservation / namespace
 *           reservation etc. if requested.
 *           Also cleans up entire cache (updated from udf)
 *
 * Parameters:
 * 		urec       : UDF record being operated on
 *
 * Return value : Nothing
 *
 * Callers:
 * 		query_agg_istream_read
 * 		ldt_aerospike_crec_close
 * 		as_query__agg
 * 		udf_record_destroy
 */
void
udf_record_close(udf_record *urecord)
{
	as_transaction *tr    = urecord->tr;
	cf_debug_digest(AS_UDF, &tr->keyd, "[ENTER] Closing record key:");

	if (urecord->flag & UDF_RECORD_FLAG_OPEN) {
		as_index_ref   *r_ref = urecord->r_ref;
		cf_detail(AS_UDF, "Closing %sRecord",
				  (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) ? "Sub" : "");
		udf_storage_record_close(urecord);
		as_record_done(r_ref, tr->rsv.ns);
		urecord->flag &= ~UDF_RECORD_FLAG_OPEN;
		cf_detail_digest(AS_UDF, &urecord->tr->keyd,
			"Storage Close:: Rec(%p) Flag(%x) Digest:", urecord, urecord->flag );
	}

	// Replication happens when the main record replicates
	if (urecord->particle_data) {
		cf_free(urecord->particle_data);
		urecord->particle_data = 0;
	}
	udf_record_cache_free(urecord);
}

/*
 * Function: This function called to reinitialize the udf_record. It sets up
 *           the basic value back to default. Can be called after the UDF
 *           record has been used. Reset the fact that record pre_exits or
 *           was actually read etc.
 *
 * Parameters:
 * 		urec	: UDF record being initialized
 *
 * Return value : Nothing
 *
 * Callers:
 * 		ldt_chunk_init (for chunk)
 * 		udf_rw_local   (parent record before calling UDF)
 */
void
udf_record_init(udf_record *urecord, bool allow_updates)
{
	urecord->tr                 = NULL;
	urecord->r_ref              = NULL;
	urecord->rd                 = NULL;
	urecord->dirty              = NULL;
	urecord->nupdates           = 0;
	urecord->ldt_rectype_bit_update   = 0;
	urecord->particle_data      = NULL;
	urecord->cur_particle_data  = NULL;
	urecord->end_particle_data  = NULL;
	urecord->starting_memory_bytes = 0;
	urecord->lrecord            = NULL;

	// Init flag
	urecord->flag               = UDF_RECORD_FLAG_ISVALID;

	if (allow_updates) {
		urecord->flag           |= UDF_RECORD_FLAG_ALLOW_UPDATES;
	}
	urecord->pickled_buf        = NULL;
	urecord->pickled_sz         = 0;

	as_rec_props_clear(&urecord->pickled_rec_props);

	urecord->op                 = UDF_OPTYPE_READ;
	urecord->keyd               = cf_digest_zero;
	for (uint32_t i = 0; i < UDF_RECORD_BIN_ULIMIT; i++) {
		urecord->updates[i].particle_buf = NULL;
	}
}

/*
 * Function: Cleans up the pickled if it is hanging from the udf_record.
 *           frees it as well if pickled_buf needs to be freed up.
 *
 * Parameters:
 * 		urec	: UDF record
 *
 * Return value : Nothing
 *
 * Callers:
 * 		udf_rw_finish
 */
void
udf_record_cleanup(udf_record *urecord, bool dofree)
{
	if (urecord->pickled_buf) {
		if (dofree) {
			cf_free(urecord->pickled_buf);
		}

		urecord->pickled_buf       = NULL;
		urecord->pickled_sz        = 0;
	}

	if (urecord->pickled_rec_props.p_data) {
		if (dofree) {
			cf_free(urecord->pickled_rec_props.p_data);
		}

		as_rec_props_clear(&urecord->pickled_rec_props);
	}
}

/*
static int print_buffer(as_buffer * buff) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    sbuf.data = buff->data;
    sbuf.size = buff->size;
    sbuf.alloc = buff->capacity;

    msgpack_zone mempool;
    msgpack_zone_init(&mempool, 2048);

    msgpack_object deserialized;
    msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, &deserialized);

    printf("msg_buf:\n");
    msgpack_object_print(stdout, deserialized);
    puts("");

    msgpack_zone_destroy(&mempool);
    return 0;
}
*/

/*
 * Function: Get bin value from cached copy. All the update in a
 *           commit window is not applied to the record directly
 *           but maintained in-memory cache. This function used
 *           to retrieve cached value
 *
 *           Similar function for get and free of cache
 *
 * Parameters:
 * 		urec    : Parent ldt record
 *
 * Return value :
 * 		value  (as_val) in case of success [for get]
 * 		NULL  in case of failure
 * 		set and free return Nothing
 *
 * Callers:
 * 		GET and SET
 * 		udf_record_get
 * 		udf_record_set
 * 		udf_record_remove
 *
 * 		FREE
 * 		udf_aerospike__execute_updates (when crossing commit window)
 * 		ldt_aerospike_crec_close       (when closing chunk sub record)
 * 		udf_record_close               (finally closing record/generally subrecord)
 * 		udf_rw_commit                  (commit the udf record or parent ldt record)
 *
 *		ldt_aerospike_crec_create
 */
static as_val *
udf_record_cache_get(udf_record * urecord, const char * name)
{
	cf_debug(AS_UDF, "[ENTER] BinName(%s) ", name );
	if ( urecord->nupdates > 0 ) {
		cf_detail(AS_UDF, "udf_record_get: %s find", name);
		for ( uint32_t i = 0; i < urecord->nupdates; i++ ) {
			udf_record_bin * bin = &(urecord->updates[i]);
			if ( strncmp(name, bin->name, AS_ID_BIN_SZ) == 0 ) {
				cf_detail(AS_UDF, "Bin %s found, type(%d)", name, bin->value->type );
				return bin->value; // note it's OK if the bin contains a nil
			}
		}
	}
	return NULL;
}

void
udf_record_cache_free(udf_record * urecord)
{
	cf_debug(AS_UDF, "[ENTER] NumUpdates(%d) ", urecord->nupdates );

	for (uint32_t i = 0; i < urecord->nupdates; i ++ ) {
		udf_record_bin * bin = &urecord->updates[i];
		if ( bin->name[0] != '\0' && bin->value != NULL ) {
			bin->name[0] = '\0';
			as_val_destroy(bin->value);
			bin->value = NULL;
		}
		if ( bin->name[0] != '\0' && bin->oldvalue != NULL ) {
			bin->name[0] = '\0';
			as_val_destroy(bin->oldvalue);
			bin->oldvalue = NULL;
		}
	}

	for (uint32_t i = 0; i < UDF_RECORD_BIN_ULIMIT; i++) {
		if (urecord->updates[i].particle_buf) {
			cf_free(urecord->updates[i].particle_buf);
			urecord->updates[i].particle_buf = NULL;
		}
	}
	urecord->nupdates = 0;
	urecord->flag &= ~UDF_RECORD_FLAG_TOO_MANY_BINS;
}

/**
 * Set the cache value for a bin, including flags.
 */
static void
udf_record_cache_set(udf_record * urecord, const char * name, as_val * value,
					 bool dirty)
{
	cf_debug(AS_UDF, "[ENTER] urecord(%p) name(%p)[%s] dirty(%d)",
			  urecord, name, name, dirty);

	bool modified = false;

	for ( uint32_t i = 0; i < urecord->nupdates; i++ ) {
		udf_record_bin * bin = &(urecord->updates[i]);

		// bin exists, then we will release old value and set new value.
		if ( strncmp(name, bin->name, AS_ID_BIN_SZ) == 0 ) {
			cf_detail(AS_UDF, "udf_record_set: %s found", name);

			// release previously set value
			as_val_destroy(bin->value);

			// set new value, with dirty flag
			if( value != NULL ) {
				bin->value = (as_val *) value;
			}
			bin->dirty = dirty;
			cf_detail(AS_UDF, "udf_record_set: %s set for %p:%p", name,
					urecord, bin->value);

			modified = true;
			break;
		}
	}

	// If not modified, then we will add the bin to the cache
	if ( ! modified ) {
		if ( urecord->nupdates < UDF_RECORD_BIN_ULIMIT ) {
			udf_record_bin * bin = &(urecord->updates[urecord->nupdates]);
			strncpy(bin->name, name, AS_ID_BIN_SZ);
			bin->value = (as_val *) value;
			bin->dirty = dirty;
			bin->ishidden = false;
			urecord->nupdates++;
			cf_detail(AS_UDF, "udf_record_set: %s not modified, add for %p:%p",
					name, urecord, bin->value);
		}
		else {
			cf_warning(AS_UDF, "UDF bin limit (%d) exceeded (bin %s)",
					UDF_RECORD_BIN_ULIMIT, name);
			urecord->flag |= UDF_RECORD_FLAG_TOO_MANY_BINS;
		}
	}
}

/**
 * Set the cache value for a bin, including flags.
 */
static void
udf_record_cache_sethidden(udf_record * urecord, const char * name)
{
	int modified = false;
	for ( uint32_t i = 0; i < urecord->nupdates; i++ ) {
		udf_record_bin * bin = &(urecord->updates[i]);

		// bin exists, then we will release old value and set new value.
		if ( strncmp(name, bin->name, AS_ID_BIN_SZ) == 0 ) {
			cf_detail(AS_UDF, "udf_record_cache_sethidden: %s found", name);
			// TODO make sure it is initialized to false
			bin->ishidden = true;
			modified      = true;
			break;
		}
	}

	// If not modified, then we will add the bin to the cache
	if ( ! modified ) {
		if ( urecord->nupdates < UDF_RECORD_BIN_ULIMIT ) {
			udf_record_bin * bin = &(urecord->updates[urecord->nupdates]);
			strncpy(bin->name, name, AS_ID_BIN_SZ);
			bin->ishidden = true;
			bin->dirty    = true;
			urecord->nupdates++;
			cf_detail(AS_UDF, "udf_record_cache_sethidden: %s not modified, add for %p:%p",
					name, urecord, bin->value);
		}
		else {
			cf_warning(AS_UDF, "UDF bin limit (%d) exceeded (bin %s)",
					UDF_RECORD_BIN_ULIMIT, name);
			urecord->flag |= UDF_RECORD_FLAG_TOO_MANY_BINS;
		}
	}
}

/*
 * Internal Function: Read and figure out if the bin is hidden
 *
 * Parameters:
 * 		r    : udf record
 * 		bname: Bin name of the bin which need to be read.
 *
 * Return value :
 * 	 	true:  if hidden
 * 	 	false: o/w or in case bin is not found
 *
 * Description:
 * 		Expectation is the record is already open. No checks are
 * 		performed in this function. Caller needs to make sure the
 * 		record is good to read e.g binname etc.
 *
 * Callers:
 * 		udf_aerospike__apply_update_atomic
 */
bool
udf_record_bin_ishidden(const udf_record *urecord, const char *name)
{
	if (!name) {
		return false;
	}
	as_bin * bb = as_bin_get(urecord->rd, name);

	if ( !bb ) {
		cf_detail(AS_UDF, "udf_record_get: bin not found (%s)", name);
		return false;
	}
	return as_bin_is_hidden(bb);
}

/*
 * Internal Function: Read the bin from storage and convert it
 *                    into as_val and return
 *
 * Parameters:
 * 		r    : udf record
 * 		bname: Bin name of the bin which need to be read.
 *
 * Return value :
 * 	 	value (as_val *) in case of success
 * 		NULL  in case of failure
 *
 * Description:
 * 		Expectation is the record is already open. No checks are
 * 		performed in this function. Caller needs to make sure the
 * 		record is good to read e.g binname etc.
 *
 * 		NB: as_val which is returned is allocated one. It is callers
 * 		    responsibility to free else in case it is passed on to
 * 		    lua ... lua has responsibility of garbage collecting it.
 * 		    Hence this function call incurs and malloc cost.
 *
 * Callers:
 * 		udf_record_get
 */
as_val *
udf_record_storage_get(const udf_record *urecord, const char *name)
{
	if (!name) {
		cf_detail(AS_UDF, "Passed Null bin name to storage get");
		return NULL;
	}
	as_bin * bb = as_bin_get(urecord->rd, name);

	if ( !bb ) {
		cf_detail(AS_UDF, "udf_record_get: bin not found (%s)", name);
		return NULL;
	}

	return as_bin_particle_to_asval(bb);
}

/*
 * Check and validate parameter before performing operation
 *
 * return:
 *      2 : UDF_ERR_INTERNAL_PARAM
 *      3 : UDF_ERR_RECORD_IS_NOT_VALID
 *      4 : UDF_ERR_PARAMETER
 *      0 : Success
 *
 */
int
udf_record_param_check(const as_rec *rec, const char *bname, char *fname, int lineno)
{
	if (!rec || !bname) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p bname=%p", rec, bname);
		return UDF_ERR_INTERNAL_PARAMETER;
	} 

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord) {
		return UDF_ERR_INTERNAL_PARAMETER;;
	}

	if (!(urecord->flag & UDF_RECORD_FLAG_ISVALID)) {
		if (!(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)) {
			cf_debug(AS_UDF, "(%s:%d): Trying to Open Invalid Record ", fname, lineno);
		} else {
			cf_debug(AS_UDF, "(%s:%d): Trying to Open Invalid SubRecord ", fname, lineno);
		}
		return UDF_ERR_RECORD_NOT_VALID;
	}

	as_namespace  * ns = urecord->tr->rsv.ns;
	if (strlen(bname) >= AS_ID_BIN_SZ || !as_bin_name_within_quota(ns, bname)) {
		cf_debug(AS_UDF, "Invalid Parameter: bin name %s too big", bname);
		return UDF_ERR_PARAMETER;
	}
	return 0;
}

/*********************************************************************
 * INTERFACE FUNCTIONS                                               *
 *																	 *
 * See the as_aerospike for the API definition						 *
 ********************************************************************/
static as_val *
udf_record_get(const as_rec * rec, const char * name)
{
	if (udf_record_param_check(rec, name, __FILE__, __LINE__)) {
		return NULL;
	}
	udf_record  *   urecord = (udf_record *) as_rec_source(rec);
	as_val *        value   = NULL;

	cf_debug(AS_UDF, "[ENTER] rec(%p) name(%s)", rec, name );

	// Get from cache
	value = udf_record_cache_get(urecord, name);

	// If value not NULL, then return it.
	if ( value != NULL ) {
		return value;
	}

	// Check in the cache before trying to look up in record
	// Note: Record may not have been created yet ... Do not
	// change the order unless you fully understand what you
	// are doing
	if ( !(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) ) {
		if (udf_record_open(urecord)) { // lazy read the record from storage
			return NULL;
		}
	}

	// Check if storage is available
	if ( !urecord->rd->ns ) {
		cf_detail(AS_UDF, "udf_record_get: storage unavailable");
		return NULL;
	}

	value = udf_record_storage_get(urecord, name);

	// We have a value, so we will cache it.
	// DO NOT remove this. We need to cache copy to makes sure ref count 
	// gets decremented post handing this as_val over to the lua world
	if (value) {
		udf_record_cache_set(urecord, name, value, false);
	}

	cf_detail(AS_UDF, "udf_record_get: end (%s) [%p,%p]", name, urecord, value);
	return value;
}

static int
udf_record_set(const as_rec * rec, const char * name, const as_val * value)
{
	int ret = udf_record_param_check(rec, name, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	cf_detail(AS_UDF, "udf_record_set: begin (%s)", name);
	if ( urecord && name ) {
		udf_record_cache_set(urecord, name, (as_val *) value, true);
	}
	cf_detail(AS_UDF, "udf_record_set: end (%s)", name);

	return 0;
}

/**
 * Set the flags for a specific bin.  This is how LDTs mark Hidden Bins. Other
 * uses may also apply.
 */
static int
udf_record_set_flags(const as_rec * rec, const char * name, uint8_t flags)
{
	int ret = udf_record_param_check(rec, name, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		return -1;
	}

	if (name) {
		if ((flags & LDT_FLAG_HIDDEN_BIN) != 0 ||
				(flags & LDT_FLAG_LDT_BIN) != 0 ||
				(flags & LDT_FLAG_CONTROL_BIN) != 0) {
			cf_debug(AS_UDF, "LDT flag(%d) Designates Hidden Bin", flags);
			udf_record_cache_sethidden(urecord, name);
		} else {
			cf_warning(AS_UDF, "Unidentified flag setting up %d", flags);
			return -2;
		}
	}

	urecord->flag |= UDF_RECORD_FLAG_METADATA_UPDATED;

	return 0;
}

/**
 * Set the Record Type bits for a record. Typically, this is how we show that
 * a record is of type LDT (which requires special handling). This function
 * allows us to either SET the record type (the "bits" parm is positive), or
 * UNSET the record type (the "bits" parm is negative).  When we want to
 * turn an "LDT Record" back into a "Normal Record", then we UNSET the LDT
 * flag (with a negative bits value).
 */
static int
udf_record_set_type(const as_rec * rec,  int8_t ldt_rectype_bit_update)
{
	if (!rec || !ldt_rectype_bit_update) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p rec_type_bits=%d", rec, ldt_rectype_bit_update);
		return 2;
	}
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	if (!udf_record_ldt_enabled(rec)
			&& (as_ldt_flag_has_parent(ldt_rectype_bit_update)
				|| as_ldt_flag_has_sub(ldt_rectype_bit_update))) {
		cf_warning(AS_LDT, "Cannot Set Large Object Bits .. Not Enabled !!");
		return -2;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		return -1;
	}

	urecord->ldt_rectype_bit_update = ldt_rectype_bit_update;
	cf_detail(AS_RW, "TO URECORD FROM LUA   Digest=%"PRIx64" bits %d",
			  *(uint64_t *)&urecord->rd->r->keyd.digest[8], urecord->ldt_rectype_bit_update);

	urecord->flag |= UDF_RECORD_FLAG_METADATA_UPDATED;

	return 0;
}

static int
udf_record_set_ttl(const as_rec * rec,  uint32_t  ttl)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		return -1;
	}

	urecord->tr->msgp->msg.record_ttl = ttl;
	urecord->flag |= UDF_RECORD_FLAG_METADATA_UPDATED;

	return 0;
}

static int
udf_record_drop_key(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		return -1;
	}

	// Flag the key to be dropped.
	if (urecord->rd->key) {
		urecord->rd->key = NULL;
		urecord->rd->key_size = 0;
	}

	urecord->flag |= UDF_RECORD_FLAG_METADATA_UPDATED;

	return 0;
}

/* Keep this for reference.
 * typedef enum {
	// The first two values -- do NOT used in single bin mode
	AS_INDEX_FLAG_SPECIAL_BINS		= 0x01, // First user of this is @LDT (to denote subrecs)
	AS_INDEX_FLAG_CHILD_REC 		= 0x02, // Child Record of a regular record (LDT)
	AS_INDEX_FLAG_CHILD_ESR         = 0x04, // Special Child Existence Sub Record (ESR)
	AS_INDEX_FLAG_UNUSED_0x08		= 0x08,

	// Combinations:
	AS_INDEX_ALL_SINGLE_BIN_FLAGS	= 0x0C,
	AS_INDEX_ALL_MULTI_BIN_FLAGS	= 0x0F
} as_index_flag;

static inline
bool as_index_is_flag_set(as_index* index, as_index_flag flag) {
	return (((as_index_flag_bits*)&index->flex_bits_2)->flag_bits & flag) != 0;
}

static inline
void as_index_set_flags(as_index* index, as_index_flag flags) {
	((as_index_flag_bits*)&index->flex_bits_2)->flag_bits |= flags;
}
 */

static int
udf_record_remove(const as_rec * rec, const char * name)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}
	udf_record * urecord = (udf_record *) as_rec_source(rec);


	cf_detail(AS_UDF, "udf_record_remove: begin (%s)", name);
	if ( urecord && name ) {
		udf_record_cache_set(urecord, name, (as_val *) &as_nil, true);
	}
	cf_detail(AS_UDF, "udf_record_remove: end (%s)", name);

	return 0;
}

static uint32_t
udf_record_ttl(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return 0;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);


	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		cf_debug(AS_UDF, "Return 0 TTL for subrecord ");
		return 0;
	}

	if ((urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {
		uint32_t now = as_record_void_time_get();

		return urecord->r_ref->r->void_time > now ?
				urecord->r_ref->r->void_time - now : 0;
	}
	else {
		cf_info(AS_UDF, "Error in getting ttl: no record found");
		return 0; // since we can't indicate the record doesn't exist
	}
	return 0;
}

static uint64_t
udf_record_last_update_time(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return 0;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (urecord && (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {
		return urecord->r_ref->r->last_update_time;
	}
	else {
		cf_warning(AS_UDF, "Error getting last update time: no record found");
		return 0;
	}
}

static uint16_t
udf_record_gen(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return 0;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (urecord && urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		return urecord->r_ref->r->generation;
	}
	else {
		cf_warning(AS_UDF, "Error in getting generation: no record found");
		return 0;
	}
}

// Local utility.
static as_val *
as_val_from_flat_key(uint8_t * flat_key, uint32_t size)
{
	uint8_t type = *flat_key;
	uint8_t * key = flat_key + 1;

	switch ( type ) {
		case AS_PARTICLE_TYPE_INTEGER:
			// TODO - verify size is (1 + 8) ???
			// Flat integer keys are in big-endian order.
			return (as_val *) as_integer_new(cf_swap_from_be64(*(int64_t *)key));
		case AS_PARTICLE_TYPE_STRING:
		{
			// Key length is size - 1, then +1 for null-termination.
			char * buf = cf_malloc(size);
			if (! buf) {
				return NULL;
			}

			uint32_t len = size - 1;
			memcpy(buf, key, len);
			buf[len] = '\0';

			return (as_val *) as_string_new(buf, true);
		}
		case AS_PARTICLE_TYPE_BLOB:
		{
			uint32_t blob_size = size - 1;
			uint8_t *buf = cf_malloc(blob_size);
			if (! buf) {
				return NULL;
			}

			memcpy(buf, key, blob_size);

			return (as_val *) as_bytes_new_wrap(buf, blob_size, true);
		}
		default:
			return NULL;
	}
}

static as_val *
udf_record_key(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return NULL;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (urecord && (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {
		if (urecord->rd->key) {
			return as_val_from_flat_key(urecord->rd->key, urecord->rd->key_size);
		}
		// TODO - perhaps look for the key in the message.
		return NULL;
	}
	else {
		cf_warning(AS_UDF, "Error in getting key: no record found");
		return NULL;
	}
}

static const char *
udf_record_setname(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return NULL;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (urecord && (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {
		return as_index_get_set_name(urecord->r_ref->r, urecord->rd->ns);
	}
	else {
		cf_warning(AS_UDF, "Error in getting set name: no record found");
		return NULL;
	}
}

bool
udf_record_destroy(as_rec *rec)
{
	if (!rec) {
		return false;
	}

	udf_record *urecord = (udf_record *) as_rec_source(rec);
	udf_record_close(urecord);
	udf_record_cleanup(urecord, true);
	return true;
} 

static as_bytes *
udf_record_digest(const as_rec *rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return NULL;
	}

	udf_record *urecord = (udf_record *)as_rec_source(rec);
	if (urecord && urecord->flag & UDF_RECORD_FLAG_OPEN) {
		cf_digest *keyd = cf_malloc(sizeof(cf_digest));
		if (!keyd) {
			return NULL;
		}
		memcpy(keyd, &urecord->keyd, CF_DIGEST_KEY_SZ);
		as_bytes *b = as_bytes_new_wrap(keyd->digest, CF_DIGEST_KEY_SZ, true);
		return b;
	}
	return NULL;
}

static int
udf_record_bin_names(const as_rec *rec, as_rec_bin_names_callback callback, void * udata)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return 1;
	}

	udf_record *urecord = (udf_record *)as_rec_source(rec);
	char * bin_names = NULL;
	if (urecord && (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {
		uint16_t nbins;

		if (urecord->rd->ns->single_bin) {
			nbins = 1;
			bin_names = alloca(1);
			*bin_names = 0;
		}
		else {
			nbins = urecord->rd->n_bins;
			bin_names = alloca(nbins * AS_ID_BIN_SZ);
			for (uint16_t i = 0; i < nbins; i++) {
				as_bin *b = &urecord->rd->bins[i];
				if (! as_bin_inuse(b)) {
					nbins = i;
					break;
				}
				const char * name = as_bin_get_name_from_id(urecord->rd->ns, b->id);
				strcpy(bin_names + (i * AS_ID_BIN_SZ), name);
			}
		}
		callback(bin_names, nbins, AS_ID_BIN_SZ, udata);
		return 0;
	}
	else {
		cf_warning(AS_UDF, "Error in getting bin names: no record found");
		bin_names = alloca(1);
		*bin_names = 0;
		callback(bin_names, 1, AS_ID_BIN_SZ, udata);
		return -1;
	}
}

static uint16_t
udf_record_numbins(const as_rec * rec)
{
	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, __FILE__, __LINE__);
	if (ret) {
		return 0;
	}

	udf_record *urecord = (udf_record *) as_rec_source(rec);
	if (urecord && (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {

		if (urecord->rd->ns->single_bin) {
			return 1;
		}

		uint16_t i;
		as_storage_rd *rd = urecord->rd;
		for (i = 0; i < rd->n_bins; i++) {
			as_bin *b = &rd->bins[i];
			if (! as_bin_inuse(b)) {
				break;
			}
		}
		return i;
	}
	else {
		cf_warning(AS_UDF, "Error in getting numbins: no record found");
		return 0;
	}
}

const as_rec_hooks udf_record_hooks = {
	.get		= udf_record_get,
	.set		= udf_record_set,
	.remove		= udf_record_remove,
	.ttl		= udf_record_ttl,
	.last_update_time	= udf_record_last_update_time,
	.gen		= udf_record_gen,
	.key		= udf_record_key,
	.setname	= udf_record_setname,
	.destroy	= NULL,
	.digest		= udf_record_digest,
	.set_flags	= udf_record_set_flags,	// @LDT:: added for control over LDT Bins from Lua
	.set_type	= udf_record_set_type,	// @LDT:: added for control over Rec Types from Lua
	.set_ttl	= udf_record_set_ttl,
	.drop_key	= udf_record_drop_key,
	.bin_names	= udf_record_bin_names,
	.numbins	= udf_record_numbins
};
