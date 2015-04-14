/*
 * udf_aerospike.c
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

#include "base/feature.h" // Turn new AS Features on/off

#include "base/udf_aerospike.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <asm/byteorder.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_boolean.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_bytes.h"
#include "aerospike/as_integer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"
#include "citrusleaf/cf_clock.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/secondary_index.h"
#include "base/thr_rw_internal.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "base/udf_rw.h"
#include "storage/storage.h"


static int udf_aerospike_rec_remove(const as_aerospike *, const as_rec *);
/*
 * Internal Function: udf_aerospike_delbin
 *
 * Parameters:
 * 		r 		- udf_record to be manipulated
 * 		bname 	- name of the bin to be deleted
 *
 * Return value:
 * 		0  on success
 * 	   -1  on failure
 *
 * Description:
 * 		The function deletes the bin with the name
 * 		passed in as parameter. The as_bin_destroy function
 * 		which is called here, only frees the data and
 * 		the bin is marked as not in use. The bin can then be reused later.
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__apply_update_atomic
 * 		In this function, if it fails at the time of update, the record is set
 * 		to rollback all the updates till this point. The case where it fails in
 * 		rollback is not handled.
 *
 * 		Side Notes:
 * 		i.	write_to_device will be set to true on a successful bin destroy.
 * 		If all the updates from udf_aerospike__apply_update_atomic (including this) are
 * 		successful, the record will be written to disk and reopened so that the rest of
 * 		sets of updates can be applied.
 *
 * 		ii.	If delete from sindex fails, we do not handle it.
 */
static int
udf_aerospike_delbin(udf_record * urecord, const char * bname)
{
	// Check that bname is not completely invalid
	if ( !bname || !bname[0] ) {
		cf_warning(AS_UDF, "udf_aerospike_delbin: Invalid Parameters [No bin name supplied]... Fail");
		return -1;
	}

	size_t          blen    = strlen(bname);
	as_storage_rd  *rd      = urecord->rd;
	as_transaction *tr      = urecord->tr;

	// Check quality of bname -- first check that it is proper length, then
	// check that we're not over quota for bins, then finally make sure that
	// the bin exists.
	if (blen > (AS_ID_BIN_SZ - 1 ) || !as_bin_name_within_quota(rd->ns, (byte *)bname, blen)) {
		// Can't read bin if name too large or over quota
		cf_warning(AS_UDF, "udf_aerospike_delbin: Invalid Parameters [bin name(%s) too big]... Fail", bname);
		return -1;
	}

	as_bin * b = as_bin_get(rd, (byte *)bname, blen);
	if ( !b ) {
		cf_warning(AS_UDF, "udf_aerospike_delbin: Invalid Operation [Bin name(%s) not found of delete]... Fail", bname);
		return -1;
	}

	SINDEX_BINS_SETUP(sbins, rd->ns->sindex_cnt);
	int sindex_found  = 0;
	bool has_sindex = as_sindex_ns_has_sindex(rd->ns);
	if (has_sindex) {
		SINDEX_GRLOCK();
		sindex_found += as_sindex_sbins_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns), b, sbins, AS_SINDEX_OP_DELETE);
		SINDEX_GUNLOCK();
	}

	int32_t i = as_bin_get_index(rd, (byte *)bname, blen);
	if (i != -1) {
		if (has_sindex) {
			if (sindex_found > 0) {	
				tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
				as_sindex_update_by_sbin(rd->ns, as_index_get_set_name(rd->r, rd->ns), sbins, sindex_found, &rd->keyd);
				//TODO: Check the error code returned through sindex_ret. (like out of sync )
			}
		}
		as_bin_destroy(rd, i);
	} else {
		cf_warning(AS_UDF, "udf_aerospike_delbin: Internal Error [Deleting non-existing bin %s]... Fail", bname);
	}

	if (has_sindex && sindex_found > 0) {
		as_sindex_sbin_freeall(sbins, sindex_found);
	}

	return 0;
}
/*
 * Internal function: udf__aerospike_get_particle_buf
 *
 * Parameters:
 * 		r 		-- udf_record_bin for which particle buf is requested
 * 		type    -- bin type
 * 		pbytes  -- current space required
 *
 * Return value:
 * 		NULL on failure
 * 		valid buf pointer success
 *
 * Description:
 * 		The function find space on preallocated particle_data for requested size.
 * 		In case it is found it tries to allocate space for bin independently. 
 * 		Return back the pointer to the offset on preallocated particle_data or newly
 * 		allocated space.
 *
 * 		Return NULL if both fails
 *
 *      Note: ubin->particle_buf will be set if new per bin memory is allocated.
 *
 * 		Callers:
 * 		udf_aerospike_setbin
 */
uint8_t *
udf__aerospike_get_particle_buf(udf_record *urecord, udf_record_bin *ubin, uint8_t type, int pbytes)
{
	if (pbytes > urecord->rd->ns->storage_write_block_size) {
		cf_warning(AS_UDF, "udf__aerospike_get_particle_buf: Invalid Operation [Bin %s data too big size=%d]... Fail", ubin->name, pbytes);
		return NULL;
	}

	int alloc_size = 0;
	switch(type) {
		case AS_LIST:
		case AS_BYTES:
		case AS_MAP:
		case AS_STRING: {
			alloc_size = urecord->rd->ns->storage_write_block_size;
			break;
		}
		case AS_BOOLEAN:
		case AS_INTEGER: {
			alloc_size = pbytes;
			break;
		}
		default: {
			cf_warning (AS_UDF, "udf__aerospike_get_particle_buf: Unknown Particle Type");
			break;
		}
	}

	uint8_t *buf = NULL;
	if (ubin->particle_buf) {
		buf = ubin->particle_buf;
	} else {
		// Disable dynamic shifting from the flat allocater to dynamic
		// allocation.
		if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
			buf = urecord->cur_particle_data;
			urecord->cur_particle_data += pbytes;
		} else if (alloc_size) {
			// If there is no space in preallocated buffer then go
			// ahead and allocate space per bin. This may happen
			// if user keeps doing lot of execute update exhausting
			// the buffer. After this point the record size check will
			// trip instead of at the code when bin value is set.
			ubin->particle_buf = cf_malloc(alloc_size);
			if (ubin->particle_buf) {
				buf = ubin->particle_buf;
			}
		}
	}
	return buf;
}
/*
 * Internal function: udf_aerospike_setbin
 *
 * Parameters:
 *      offset  -- offset of udf bin in updates array 
 * 		r 		-- udf_record to be manipulated
 * 		bname 	-- name of the bin to be deleted
 *		val		-- value to be updated with
 *
 * Return value:
 * 		0  on success
 * 	   -1  on failure
 *
 * Description:
 * 		The function sets the bin with the name
 * 		passed in as parameter to the value, passed as the third parameter.
 * 		Before updating the bin, it is checked if the value can fit in the storage
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__apply_update_atomic
 * 		In this function, if it fails at the time of update, the record is set
 * 		to rollback all the updates till this point. The case where it fails in
 * 		rollback is not handled.
 *
 * 		Side Notes:
 * 		i.	write_to_device will be set to true on a successful bin update.
 * 		If all the updates from udf_aerospike__apply_update_atomic (including this) are
 * 		successful, the record will be written to disk and reopened so that the rest of
 * 		sets of updates can be applied.
 *
 * 		ii.	If put in sindex fails, we do not handle it.
 *
 * 		TODO make sure anything goes into setbin only if the bin value is
 * 		          changed
 */
static int
udf_aerospike_setbin(udf_record * urecord, int offset, const char * bname, const as_val * val, bool is_hidden)
{
	if (bname == NULL || bname[0] == 0 ) {
		cf_warning(AS_UDF, "udf_aerospike_setbin: Invalid Parameters: [No bin name supplied]... Fail");
		return -1;
	}

	uint8_t type = as_val_type(val);
	if (is_hidden &&
			((type != AS_MAP) && (type != AS_LIST))) {
		cf_warning(AS_UDF, "udf_aerospike_setbin: Invalid Operation [Hidden %d type Not allowed]... Fail", type);
		return -3;
	}

	size_t          blen    = strlen(bname);
	as_storage_rd * rd      = urecord->rd;
	as_transaction *tr      = urecord->tr;
	as_index_ref  * index   = urecord->r_ref;

	SINDEX_BINS_SETUP(sbins, 2 * rd->ns->sindex_cnt);
	int sindex_found = 0;
	bool has_sindex          = as_sindex_ns_has_sindex(rd->ns);
	if (has_sindex) {
		SINDEX_GRLOCK();
	}
	as_bin * b = as_bin_get(rd, (byte *)bname, blen);

	if ( !b && (blen > (AS_ID_BIN_SZ - 1 )
				|| !as_bin_name_within_quota(rd->ns, (byte *)bname, blen)) ) {
		// Can't write bin
		cf_warning(AS_UDF, "udf_aerospike_setbin: Invalid Parameters: [Bin name %s too big]... Fail", bname);
		return -1;
	}
	if ( !b ) {
		// See if there's a free one, the hope is you will always find the bin because
		// you have already allocated bin space before calling this function.
		b = as_bin_create(index->r, rd, (byte *)bname, blen, 0);
		if (!b) {
			cf_warning(AS_UDF, "udf_aerospike_setbin: Internal Error [Bin %s not found.. Possibly ran out of bins]... Fail", bname);
			return -1;
		}
	}
	else {
		if (has_sindex ) {
			sindex_found += as_sindex_sbins_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns), 
								b, &sbins[sindex_found], AS_SINDEX_OP_DELETE);
		}
	}

	

	// we know we are doing an update now, make sure there is particle data,
	// set to be 1 wblock size now @TODO!
	uint32_t pbytes = 0;
	int ret = 0;

	cf_detail(AS_UDF, "udf_setbin: bin %s type %d ", bname, type );

	switch(type) {
		case AS_STRING: {
			as_string * v   = as_string_fromval(val);
			byte *      s   = (byte *) as_string_tostring(v);
			size_t      l   = as_string_len(v);

			// Save for later.
			// cf_detail(AS_UDF, "udf_setbin: string: binname %s value is %s",bname,s);

			if (rd->ns->storage_data_in_memory) {
				as_bin_particle_replace_from_mem(b, AS_PARTICLE_TYPE_STRING, s, l);
				//as_particle_frommem(b, AS_PARTICLE_TYPE_STRING, s, l, NULL, true);
			} else {
				pbytes = as_particle_size_from_mem(AS_PARTICLE_TYPE_STRING, s, l);
				//pbytes = l + as_particle_get_base_size(AS_PARTICLE_TYPE_STRING);
				uint8_t *particle_buf = udf__aerospike_get_particle_buf(urecord, &urecord->updates[offset], type, pbytes);
				if (particle_buf) {
					as_bin_particle_stack_from_mem(b, particle_buf, AS_PARTICLE_TYPE_STRING, s, l);
					//as_particle_frommem(b, AS_PARTICLE_TYPE_STRING, s, l,
					//					particle_buf,
					//					false);
				} else {
					cf_warning(AS_UDF, "udf_aerospike_setbin: Allocation Error [String: bin %s "
										"data size too big: pbytes %d]... Fail",
										bname, pbytes);
					ret = -4;
					break;
				}
			}
			break;
		}
		case AS_BYTES: {
			as_bytes *  v   = as_bytes_fromval(val);
			uint8_t *   s   = as_bytes_get(v);
			size_t      l   = as_bytes_size(v);

			if (rd->ns->storage_data_in_memory) {
				as_bin_particle_replace_from_mem(b, AS_PARTICLE_TYPE_BLOB, s, l);
				//as_particle_frommem(b, AS_PARTICLE_TYPE_BLOB, s, l, NULL, true);
			} else {
				pbytes = as_particle_size_from_mem(AS_PARTICLE_TYPE_BLOB, s, l);
				//pbytes = l + as_particle_get_base_size(AS_PARTICLE_TYPE_BLOB);
				uint8_t *particle_buf = udf__aerospike_get_particle_buf(urecord, &urecord->updates[offset], type, pbytes);
				if (particle_buf) {
					as_bin_particle_stack_from_mem(b, particle_buf, AS_PARTICLE_TYPE_BLOB, s, l);
					//as_particle_frommem(b, AS_PARTICLE_TYPE_BLOB, s, l, particle_buf, false);
				} else {
					cf_warning(AS_UDF, "udf_aerospike_setbin: Allocation Error [Bytes: bin %s "
										"data size too big: pbytes %d]... Fail",
										bname, pbytes);
					ret = -4;
					break;
				}
			}
			break;
		}
		case AS_BOOLEAN: {
			as_boolean *    v   = as_boolean_fromval(val);
			int64_t         i   = (int64_t) as_boolean_get(v);

			if (rd->ns->storage_data_in_memory) {
				as_bin_particle_replace_from_mem(b, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8);
				//as_particle_frommem(b, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8, NULL, true);
			} else {
				pbytes = as_particle_size_from_mem(AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8);
				//pbytes = 8 + as_particle_get_base_size(AS_PARTICLE_TYPE_INTEGER);
				uint8_t *particle_buf = udf__aerospike_get_particle_buf(urecord, &urecord->updates[offset], type, pbytes);
				if (particle_buf) {
					as_bin_particle_stack_from_mem(b, particle_buf, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8);
					//as_particle_frommem(b, AS_PARTICLE_TYPE_INTEGER,
					//					(uint8_t *) &i, 8,
					//					particle_buf, false);
				} else {
					cf_warning(AS_UDF, "udf_aerospike_setbin: Allocation Error [Bool: bin %s "
										"data size too big: pbytes %d]... Fail",
										bname, pbytes);
					ret = -4;
					break;
				}
			}
			break;
		}
		case AS_INTEGER: {
			as_integer *    v   = as_integer_fromval(val);
			int64_t         i   = as_integer_get(v);

			if (rd->ns->storage_data_in_memory) {
				as_bin_particle_replace_from_mem(b, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8);
				//as_particle_frommem(b, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8, NULL, true);
			} else {
				pbytes = as_particle_size_from_mem(AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8);
				//pbytes = 8 + as_particle_get_base_size(AS_PARTICLE_TYPE_INTEGER);
				uint8_t *particle_buf = udf__aerospike_get_particle_buf(urecord, &urecord->updates[offset], type, pbytes);
				if (particle_buf) {
					as_bin_particle_stack_from_mem(b, particle_buf, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8);
					//as_particle_frommem(b, AS_PARTICLE_TYPE_INTEGER,
					//					(uint8_t *) &i, 8, particle_buf,
					//					false);
				} else {
					cf_warning(AS_UDF, "udf_aerospike_setbin: Allocation Error [Integer: bin %s "
										"data size too big: pbytes %d]... Fail",
										bname, pbytes);
					ret = -4;
					break;
				}
			}
			break;
		}
		// @LDT : Possibly include AS_LDT in this list.  We need the LDT
		// bins to be updated by LDT lua calls, and that path takes us thru here.
		// However, we ALSO need to be able to set the particle type for the
		// bins -- so that requires extra processing here to take the LDT flags
		// and set the appropriate bin flags in the particle data.
		case AS_MAP:
		case AS_LIST: {
			as_buffer buf;
			as_buffer_init(&buf);
			as_serializer s;
			as_msgpack_init(&s);
			int res = as_serializer_serialize(&s, (as_val *) val, &buf);

			if (res != 0) {
				cf_warning(AS_UDF, "udf_aerospike_setbin: Internal Error [map-list: serialization failure (%d)]... Fail", res);
				ret = -1;
				as_serializer_destroy(&s);
				as_buffer_destroy(&buf);
				break;
			}
			uint8_t ptype;
			if (is_hidden) {
				ptype = as_particle_type_convert_to_hidden(to_particle_type(type));
			} else {
				ptype = to_particle_type(type);
			}
			if (rd->ns->storage_data_in_memory) {
				as_bin_particle_replace_from_mem(b, ptype, buf.data, buf.size);
				//as_particle_frommem(b, ptype, (uint8_t *) buf.data, buf.size, NULL, true);
			}
			else {
				pbytes = as_particle_size_from_mem(ptype, buf.data, buf.size);
				//pbytes = buf.size + as_particle_get_base_size(ptype);
				uint8_t *particle_buf = udf__aerospike_get_particle_buf(urecord, &urecord->updates[offset], type, pbytes);
				if (particle_buf) {
					as_bin_particle_stack_from_mem(b, particle_buf, ptype, buf.data, buf.size);
					//as_particle_frommem(b, ptype, (uint8_t *) buf.data, buf.size,
					//					particle_buf, false);
				} else {
					cf_warning(AS_UDF, "udf_aerospike_setbin: Allocation Error [Map-List: bin %s "
										"data size too big: pbytes %d]... Fail",
										bname, pbytes);
					ret = -4;
				}
			}
			as_serializer_destroy(&s);
			as_buffer_destroy(&buf);
			break;
		}
		default: {
			cf_warning(AS_UDF, "unrecognized object type %d, skipping", as_val_type(val) );
			break;
		}

	}

	// If something fail bailout
	if (ret) {
		if (sindex_found > 0) {
			as_sindex_sbin_freeall(sbins, sindex_found);
		}
		return ret;
	}
	
	// Update sindex if required
	if (has_sindex) {
		sindex_found += as_sindex_sbins_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns),
								b, &sbins[sindex_found], AS_SINDEX_OP_INSERT);
		SINDEX_GUNLOCK();
		if (sindex_found > 0) {
			tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
			as_sindex_update_by_sbin(rd->ns, as_index_get_set_name(rd->r, rd->ns), sbins, sindex_found, &rd->keyd);	
			as_sindex_sbin_freeall(sbins, sindex_found);
		}
	}

	return ret;
} // end udf_aerospike_setbin()

/*
 * Check and validate parameter before performing operation
 *
 * return:
 *      UDF_ERR * in case of failure
 *      0 in case of success
 */
static int
udf_aerospike_param_check(const as_aerospike *as, const as_rec *rec, char *fname, int lineno)
{
	if (!as) {
		cf_debug(AS_UDF, "Invalid Parameters: aerospike=%p", as);
		return UDF_ERR_INTERNAL_PARAMETER;
	}

	int ret = udf_record_param_check(rec, UDF_BIN_NONAME, fname, lineno);
	if (ret) {
		return ret;
	}
	return 0;
}

/*
 * Internal function: udf_aerospike__apply_update_atomic
 *
 * Parameters:
 * 		rec --	udf_record to be updated
 *
 * Return Values:
 * 		 0 success
 * 		-1 failure
 *
 * Description:
 * 		This function applies all the updates atomically. That is,
 * 		if one of the bin update/delete/create fails, the entire function
 * 		will fail. If the nth update fails, all the n-1 updates are rolled
 * 		back to their initial values
 *
 * 		Special Notes:
 * 		i. The basic checks of bin name being too long or if there is enough space
 * 		on the disk for the bin values is done before allocating space for any
 * 		of the bins.
 *
 * 		ii. If one of the updates to be rolled back is a bin creation,
 * 		udf_aerospike_delbin is called. This will not free up the bin metadata.
 * 		So there will be a small memory mismatch b/w replica (which did not get the
 * 		record at all and hence no memory is accounted) and the master will be seen.
 * 		To avoid such cases, we are doing checks upfront.
 *
 * 		Callers:
 * 		udf_aerospike__execute_updates
 * 		In this function, if udf_aerospike__apply_update_atomic fails, the record
 * 		is not committed to the storage. On success, record is closed which commits to
 * 		the storage and reopened for the next set of udf updates.
 * 		The return value from udf_aerospike__apply_update_atomic is passed on to the
 * 		callers of this function.
 */
int
udf_aerospike__apply_update_atomic(udf_record *urecord)
{
	int rc 					= 0;
	int failmax 			= 0;
	int new_bins 			= 0;	// How many new bins have to be created in this update
	as_storage_rd * rd		= urecord->rd;

	// This will iterate over all the updates and apply them to storage.
	// The items will remain, and be used as cache values. If an error
	// occurred during setbin(), we rollback all the operation which
	// is and return failure
	cf_detail(AS_UDF, "execute updates: %d updates", urecord->nupdates);

	// loop twice to make sure the updates are performed first so in case
	// something wrong it can be rolled back. The deletes will go through
	// successfully generally.

	// In first iteration, just calculate how many new bins need to be created
	for(uint32_t i = 0; i < urecord->nupdates; i++ ) {
		if ( urecord->updates[i].dirty ) {
			char *      k = urecord->updates[i].name;
			if ( k != NULL ) {
				if ( !as_bin_get(rd, (uint8_t *)k, strlen(k)) ) {
					new_bins++;
				}
			}
		}
	}
	// Free bins - total bins not in use in the record
	// Delta bins - new bins that need to be created
	int free_bins  = urecord->rd->n_bins - as_bin_inuse_count(urecord->rd);
	int delta_bins = new_bins - free_bins;
	cf_detail(AS_UDF, "Total bins %d, In use bins %d, Free bins %d , New bins %d, Delta bins %d",
			  urecord->rd->n_bins, as_bin_inuse_count(urecord->rd), free_bins, new_bins, delta_bins);

	// Allocate space for all the new bins that need to be created beforehand
	if (delta_bins > 0 && rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		as_bin_allocate_bin_space(urecord->r_ref->r, rd, delta_bins);
	}

	if (!rd->ns->storage_data_in_memory && !urecord->particle_data) {
		// 256 as upper bound on the LDT control bin, we may write version below
		// leave it at the end for its use
		urecord->particle_data = cf_malloc(rd->ns->storage_write_block_size + 256);
		urecord->cur_particle_data = urecord->particle_data;
		urecord->end_particle_data = urecord->particle_data + rd->ns->storage_write_block_size;
	}

	bool has_sindex = as_sindex_ns_has_sindex(rd->ns); 
	if (has_sindex) {
		SINDEX_GRLOCK();
	}
	bool is_record_dirty = false;
	bool is_record_flag_dirty = false;
	uint8_t old_index_flags = as_index_get_flags(rd->r);
	uint8_t new_index_flags = 0;

	// In second iteration apply updates.
	for(uint32_t i = 0; i < urecord->nupdates; i++ ) {
		urecord->updates[i].oldvalue  = NULL;
		urecord->updates[i].washidden = false;
		if ( urecord->updates[i].dirty && rc == 0) {

			char *      k = urecord->updates[i].name;
			as_val *    v = urecord->updates[i].value;
			bool        h = urecord->updates[i].ishidden;

			if ( k != NULL ) {
				if ( v == NULL || v->type == AS_NIL ) {
					// if the value is NIL, then do a delete
					cf_detail(AS_UDF, "execute update: position %d deletes bin %s", i, k);
					urecord->updates[i].oldvalue = udf_record_storage_get(urecord, k);
					urecord->updates[i].washidden = udf_record_bin_ishidden(urecord, k);
					// Only case delete fails if bin is not found that is 
					// as good as delete. Ignore return code !!
					udf_aerospike_delbin(urecord, k);
				}
				else {
					// otherwise, it is a set
					cf_detail(AS_UDF, "execute update: position %d sets bin %s", i, k);
					urecord->updates[i].oldvalue = udf_record_storage_get(urecord, k);
					urecord->updates[i].washidden = udf_record_bin_ishidden(urecord, k);
					rc = udf_aerospike_setbin(urecord, i, k, v, h);
					if (rc) {
						if (urecord->updates[i].oldvalue) {
							as_val_destroy(urecord->updates[i].oldvalue);
							urecord->updates[i].oldvalue = NULL;
						} 
						failmax = i;
						goto Rollback;
					}
				}
			}
			is_record_dirty = true;
		}
	}

	if (urecord->ldt_rectype_bits) {
		if (urecord->ldt_rectype_bits < 0) {
			// ldt_rectype_bits is negative in case we want to reset the bits 
			uint8_t rectype_bits = urecord->ldt_rectype_bits * -1; 
			new_index_flags = old_index_flags & ~rectype_bits;
		} else { 
			new_index_flags = old_index_flags | urecord->ldt_rectype_bits;  
		} 

		if (new_index_flags != old_index_flags) {
			as_index_clear_flags(rd->r, old_index_flags);
			as_index_set_flags(rd->r, new_index_flags);
			is_record_flag_dirty = true;
			cf_detail_digest(AS_RW, &urecord->tr->keyd, "Setting index flags from %d to %d new flag %d", old_index_flags, new_index_flags, as_index_get_flags(rd->r));
		}
	}

	{
		// This is _NOT_ for writing to the storage but for simply performing sizing
		// calculation. If we know the upper bounds of size of rec_props.. we could 
		// avoid this work and check with that much correction ... 
		//
		// See
		//  - udf_rw_post_processing for building rec_props for replication
		//  - udf_record_close for building rec_props for writing it to storage
		size_t  rec_props_data_size = as_storage_record_rec_props_size(rd);
		uint8_t rec_props_data[rec_props_data_size];
		if (rec_props_data_size > 0) {
			as_storage_record_set_rec_props(rd, rec_props_data);
		}

		// Version is set in the end after record size check. Setting version won't change the size of
		// the record. And if it were before size check then this setting of version as well needs to
		// be backed out.
		// TODO: Add backout logic would work till very first create call of LDT end up crossing over
		// record boundary
		if (as_ldt_record_is_parent(rd->r)) {
			int rv = as_ldt_parent_storage_set_version(rd, urecord->lrecord->version, urecord->end_particle_data, __FILE__, __LINE__);
			if (rv < 0) {
				cf_warning(AS_LDT, "udf_aerospike__apply_update_atomic: Internal Error "
							" [Failed to set the version on storage rv=%d]... Fail",rv);
				goto Rollback;
			}
		}

		if (! as_storage_record_size_and_check(rd)) {
			failmax = (int)urecord->nupdates;
			goto Rollback;
		}
	}

	if (has_sindex) {
		SINDEX_GUNLOCK();
	}

	// If there were updates do miscellaneous successful commit
	// tasks
	if (is_record_dirty 
			|| is_record_flag_dirty
			|| (urecord->flag & UDF_RECORD_FLAG_METADATA_UPDATED)) {
		// Set updated flag to true
		urecord->flag |= UDF_RECORD_FLAG_HAS_UPDATES;

		// Set up record to be flushed to storage
		urecord->rd->write_to_device = true;
	}

	// Clean up oldvalue cache and reset dirty. All the changes made 
	// here has made to the particle buffer. Nothing will now be backed out.
	for (uint32_t i = 0; i < urecord->nupdates; i++) {
		udf_record_bin * bin = &urecord->updates[i];
		if (bin->oldvalue != NULL ) {
			as_val_destroy(bin->oldvalue);
			bin->oldvalue = NULL;
		}
		bin->dirty    = false;
	}
	return rc;

Rollback:
	cf_debug(AS_UDF, "Rollback Called: failmax %d", failmax);
	for (int i = 0; i < failmax; i++) {
		if (urecord->updates[i].dirty) {
			char *      k = urecord->updates[i].name;
			// Pick the oldvalue for rollback
			as_val *    v = urecord->updates[i].oldvalue;
			bool        h = urecord->updates[i].washidden;
			if ( k != NULL ) {
				if ( v == NULL || v->type == AS_NIL ) {
					// if the value is NIL, then do a delete
					cf_detail(AS_UDF, "execute rollback: position %d deletes bin %s", i, k);
					rc = udf_aerospike_delbin(urecord, k);
				}
				else {
					// otherwise, it is a set
					cf_detail(AS_UDF, "execute rollback: position %d sets bin %s", i, k);
					rc = udf_aerospike_setbin(urecord, i, k, v, h);
					if (rc) {
						cf_warning(AS_UDF, "Rollback failed .. not good ... !!");
					}
				}
			}
			if (v) {
				as_val_destroy(v);
				cf_debug(AS_UDF, "ROLLBACK as_val_destroy()");
			}
		}
	}

	if (is_record_flag_dirty) {
		as_index_clear_flags(rd->r, new_index_flags);
		as_index_set_flags(rd->r, old_index_flags);
		is_record_flag_dirty = false;
	}

	if (has_sindex) {
		SINDEX_GUNLOCK();
	}

	// Reset the flat size in case the stuff is backedout !!! it should not
	// fail in the backout code ...
	if (! as_storage_record_size_and_check(rd)) {
		cf_warning(AS_LDT, "Does not fit even after rollback... it is trouble");
	}

	// Do not clean up the cache in case of failure
	return -1;
}

/*
 * Internal function: udf_aerospike_execute_updates
 *
 * Parameters:
 * 		rec - udf record to be updated
 *
 * Return values
 * 		 0 on success
 *		-1 on failure
 *
 * Description:
 * 		Execute set of udf_record updates. If these updates are successfully
 * 		applied atomically, the storage record is closed (committed to the disk)
 * 		and reopened. The cache is freed up at the end.
 *
 * 		Callers:
 * 		udf_aerospike_rec_create, interface func - aerospike:create(r)
 * 		udf_aerospike_rec_update, interface func - aerospike:update(r)
 * 		udf_aerospike__execute_updates is the key function which is executed in these
 * 		functions. The return value is directly passed on to the lua.
 */
int
udf_aerospike__execute_updates(udf_record * urecord)
{
	int rc = 0;
	as_storage_rd *rd    = urecord->rd;
	as_index_ref * r_ref = urecord->r_ref;

	if ( urecord->nupdates == 0  &&
			(urecord->flag & UDF_RECORD_FLAG_METADATA_UPDATED) == 0 ) {
		cf_detail(AS_UDF, "No Update when execute update is called");
		return 0;
	}

	// fail updates in case update is not allowed. Queries and scans do not
	// not allow updates. Updates will never be true .. just being paranoid
	if (!(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		cf_warning(AS_UDF, "Udf: execute updates: allow updates false; FAIL");
		return -1;
	}

	// Commit semantics is either all the update make it or none of it
	rc = udf_aerospike__apply_update_atomic(urecord);

	// allocate down if bins are deleted / not in use
	if (rd->ns && rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		int32_t delta_bins = (int32_t)as_bin_inuse_count(rd) - (int32_t)rd->n_bins;
		if (delta_bins) {
			as_bin_allocate_bin_space(r_ref->r, rd, delta_bins);
		}
	}
	return rc;
}

as_aerospike *
udf_aerospike_new()
{
	return as_aerospike_new(NULL, &udf_aerospike_hooks);
}

as_aerospike *
udf_aerospike_init(as_aerospike * as)
{
	return as_aerospike_init(as, NULL, &udf_aerospike_hooks);
}

static void
udf_aerospike_destroy(as_aerospike * as)
{
	as_aerospike_destroy(as);
}

static cf_clock
udf_aerospike_get_current_time(const as_aerospike * as)
{
	(void)as;
	return cf_clock_getabsolute();
}

/**
 * aerospike::create(record)
 * Function: udf_aerospike_rec_create
 *
 * Parameters:
 * 		as - as_aerospike
 *		rec - as_rec
 *
 * Return Values:
 * 		1 if record is being read or on a create, it already exists
 * 		o/w return value of udf_aerospike__execute_updates
 *
 * Description:
 * 		Create a new record in local storage.
 * 		The record will only be created if it does not exist.
 * 		This assumes the record has a digest that is valid for local storage.
 *
 *		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		lua interfacing function, mod_lua_aerospike_rec_create
 * 		The return value of udf_aerospike_rec_create is pushed on to the lua stack
 *
 * 		Notes:
 * 		The 'read' and 'exists' flag of udf_record are set to true.
*/
static int
udf_aerospike_rec_create(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord  = (udf_record *) as_rec_source(rec);

	// make sure record isn't already successfully read
	if (urecord->flag & UDF_RECORD_FLAG_OPEN) {
		cf_detail(AS_UDF, "udf_aerospike_rec_create: Record Already Exists");
		return 1;
	}
	as_transaction *tr    = urecord->tr;
	as_index_ref   *r_ref = urecord->r_ref;
	as_storage_rd  *rd    = urecord->rd;
	as_index_tree  *tree  = tr->rsv.tree;
	bool is_subrec        = false;

	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		tree      = tr->rsv.sub_tree;
		is_subrec = true;
	}

	// make sure we got the record as a create
	bool is_create = false;
	int rv = as_record_get_create(tree, &tr->keyd, r_ref, tr->rsv.ns, is_subrec);
	cf_detail_digest(AS_UDF, &tr->keyd, "Creating %sRecord",
			(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) ? "Sub" : "");

	// rv 0 means record exists, 1 means create, < 0 means fail
	// TODO: Verify correct result codes.
	if (rv == 1) {
		is_create = true;
	} else if (rv == 0) {
		cf_warning(AS_UDF, "udf_aerospike_rec_create: Record Already Exists 2");
		as_record_done(r_ref, tr->rsv.ns);
		// DO NOT change it has special meaning for caller
		return 1;
	} else if (rv < 0) {
		cf_warning(AS_UDF, "udf_aerospike_rec_create: Record Open Failed with rv=%d", rv);
		return rv;
	}

	// Associates the set name with the storage rec and index
	if (tr->msgp) {
		// Set the set name to index and close record if the setting the set name
		// is not successful
		int rv_set = as_record_set_set_from_msg(r_ref->r, tr->rsv.ns, &tr->msgp->msg);
		if (rv_set != 0) {
			cf_warning(AS_UDF, "udf_aerospike_rec_create: Failed to set setname");
			if (is_create) {
				as_index_delete(tree, &tr->keyd);
			}
			as_record_done(r_ref, tr->rsv.ns);
			return 4;
		}
	}

	urecord->flag |= UDF_RECORD_FLAG_OPEN;
	cf_detail(AS_UDF, "Open %p %x %"PRIx64"", urecord, urecord->flag, *(uint64_t *)&tr->keyd);

	as_index *r    = r_ref->r;
	// open up storage
	as_storage_record_create(urecord->tr->rsv.ns, urecord->r_ref->r,
		urecord->rd, &urecord->tr->keyd);

	cf_detail(AS_UDF, "as_storage_record_create: udf_aerospike_rec_create: r %p rd %p",
		urecord->r_ref->r, urecord->rd);

	// if multibin storage, we will use urecord->stack_bins, so set the size appropriately
	if ( ! rd->ns->storage_data_in_memory && ! rd->ns->single_bin ) {
		rd->n_bins = sizeof(urecord->stack_bins) / sizeof(as_bin);
	}

	// side effect: will set the unused bins to properly unused
	rd->bins       = as_bin_get_all(r, rd, urecord->stack_bins);
	urecord->flag |= UDF_RECORD_FLAG_STORAGE_OPEN;

	// If the message has a key, apply it to the record.
	get_msg_key(&tr->msgp->msg, rd);

	cf_detail(AS_UDF, "Storage Open %p %x %"PRIx64"", urecord, urecord->flag, *(uint64_t *)&tr->keyd);
	cf_detail(AS_UDF, "udf_aerospike_rec_create: Record created %d", urecord->flag);

	int rc         = udf_aerospike__execute_updates(urecord);
	if (rc) {
		//  Creating the udf record failed, destroy the as_record
		if (!as_bin_inuse_has(urecord->rd)) {
			udf_aerospike_rec_remove(as, rec);
		}
	}
	return rc;
}

/**
 * aerospike::update(record)
 * Function: udf_aerospike_rec_update
 *
 * Parameters:
 *
 * Return Values:
 * 		-2 if record does not exist
 * 		o/w return value of udf_aerospike__execute_updates
 *
 * Description:
 * 		Updates an existing record in local storage.
 * 		The record will only be updated if it exists.
 *
 *		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		lua interfacing function, mod_lua_aerospike_rec_update
 * 		The return value of udf_aerospike_rec_update is pushed on to the lua stack
 *
 * 		Notes:
 * 		If the record does not exist or is not read by anyone yet, we cannot
 * 		carry on with the update. 'exists' and 'set' are set to false on record
 * 		init or record remove.
*/
static int
udf_aerospike_rec_update(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);

	// make sure record exists and is already opened up
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)
			|| !(urecord->flag & UDF_RECORD_FLAG_OPEN) ) {
		cf_warning(AS_UDF, "Record not found to be open while updating urecord flag=%d", urecord->flag);
		return -2;
	}
	cf_detail_digest(AS_UDF, &urecord->rd->r->key, "Executing Updates");
	return udf_aerospike__execute_updates(urecord);
}

/**
 * Function udf_aerospike_rec_exists
 *
 * Parameters:
 *
 * Return Values:
 * 		1 if record exists
 * 		0 o/w
 *
 * Description:
 * Check to see if the record exists
 */
static int
udf_aerospike_rec_exists(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);

	return (urecord && (urecord->flag & UDF_RECORD_FLAG_OPEN)) ? true : false;
}

/*
 * Function: udf_aerospike_rec_remove
 *
 * Parameters:
 *
 * Return Values:
 *		1 if record does not exist
 *		0 on success
 *
 * Description:
 * Removes an existing record from local storage.
 * The record will only be removed if it exists.
 */
static int
udf_aerospike_rec_remove(const as_aerospike * as, const as_rec * rec)
{
	int ret = udf_aerospike_param_check(as, rec, __FILE__, __LINE__);
	if (ret) {
		return ret;
	}
	udf_record * urecord = (udf_record *) as_rec_source(rec);

	// make sure record is already exists before removing it
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		return 1;
	}

	as_index_tree *tree  = urecord->tr->rsv.tree;
	// remove index from tree. Will decrement ref count, but object still retains
	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)
		tree = urecord->tr->rsv.sub_tree;

	// Reset starting memory bytes in case the same record is created again
	// in the same UDF
	as_index_delete(tree, &urecord->tr->keyd);
	urecord->starting_memory_bytes = 0;

	// Close the storage record associates with this UDF record
	// do not release the reservation yet !!
	udf_record_close(urecord);
	return 0;
}

/**
 * Writes a log message
 */
static int
udf_aerospike_log(const as_aerospike * a, const char * file, const int line, const int lvl, const char * msg)
{
	(void)a;
	cf_fault_event(AS_UDF, lvl, file, NULL, line, (char *) msg);
	return 0;
}

// Would someone please explain the structure of these hooks?  Why are some null?
const as_aerospike_hooks udf_aerospike_hooks = {
	.rec_create       = udf_aerospike_rec_create,
	.open_subrec      = NULL,
	.close_subrec     = NULL,
	.update_subrec    = NULL,
	.create_subrec    = NULL,
	.rec_update       = udf_aerospike_rec_update,
	.rec_remove       = udf_aerospike_rec_remove,
	.rec_exists       = udf_aerospike_rec_exists,
	.log              = udf_aerospike_log,
	.get_current_time = udf_aerospike_get_current_time,
	.destroy          = udf_aerospike_destroy
};
