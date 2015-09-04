/*
 * secondary_index.c
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
 * SYNOPSIS
 * Abstraction to support secondary indexes with multiple implementations.
 * Currently there are two variants of secondary indexes supported.
 *
 * -  Aerospike Index B-tree, this is full fledged index implementation and
 *    maintains its own metadata and data structure for list of those indexes.
 *
 * -  Citrusleaf foundation indexes which are bare bone tree implementation
 *    with ability to insert delete update indexes. For these the current code
 *    manage all the data structure to manage different trees. [Will be
 *    implemented when required]
 *
 * This file implements all the translation function which can be called from
 * citrusleaf to prepare to do the operations on secondary index. Also
 * implements locking to make Aerospike Index (single threaded) code multi threaded.
 *
 */

/* Code flow --
 *
 * DDLs
 *
 * as_sindex_create --> ai_btree_create
 *
 * as_sindex_destroy --> Releases the si and change the state to AS_SINDEX_DESTROY
 *
 * BOOT INDEX 
 * 
 * as_sindex_boot_populateall --> If fast restart or data in memory and load at start up --> as_sbld_build_all
 *
 * SBIN creation
 *
 * as_sindex_sbins_from_rd  --> (For every bin in the record) as_sindex_sbins_from_bin
 *
 * as_sindex_sbins_from_bin -->  as_sindex_sbins_from_bin_buf
 *
 * as_sindex_sbins_from_buf --> as_sindex_sbins_from_bin_buf
 *
 * as_sindex_sbins_from_bin_buf --> (For every macthing sindex) --> as_sindex_sbin_from_sindex
 * 
 * as_sindex_sbin_from_sindex --> (If bin value macthes with sindex defn) --> as_sindex_add_asval_to_itype_sindex
 *
 * as_sindex_diff_sbins_from_buf --> ( For every macthing sindex) --> as_sindex_diff_sbins_from_sindex
 *
 * as_sindex_diff_sbins_from_sindex --> (If bin and buf matches with sindex defn) --> as_sindex_add_diff_asval_to_itype_sindex
 *                                  |
 *                                  --> Else --> as_sindex_add_asval_to_itype_sindex
 *
 * SBIN updates
 * 
 * as_sindex_update_by_sbin --> For every sbin --> as_sindex__op_by_sbin
 *
 * as_sindex__op_by_sbin --> If op == AS_SINDEX_OP_INSERT --> ai_btree_put
 *                       |
 *                       --> If op == AS_SINDEX_OP_DELETE --> ai_btree_delete
 *
 * DMLs using RECORD
 *
 * as_sindex_put_rd --> For each bin in the record --> as_sindex_sbin_from_sindex
 *
 * as_sindex_putall_rd --> For each sindex --> as_sindex_put_rd
 *
 */

#include "base/secondary_index.h"

#include <errno.h>
#include <limits.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "aerospike/as_arraylist.h"
#include "aerospike/as_arraylist_iterator.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_hashmap.h"
#include "aerospike/as_hashmap_iterator.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_pair.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"

#include "ai_btree.h"
#include "ai_globals.h"
#include "bt_iterator.h"
#include "cf_str.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/system_metadata.h"
#include "base/thr_sindex.h"
#include "base/udf_rw.h"


#define SINDEX_CRASH(str, ...) \
	cf_crash(AS_SINDEX, "SINDEX_ASSERT: "str, ##__VA_ARGS__);

#define AS_SINDEX_PROP_KEY_SIZE (AS_SET_NAME_MAX_SIZE + 20) // setname_binid_typeid

static cf_queue *g_q_index_keys_arr = NULL;

// Internal Functions
bool as_sindex__setname_match(as_sindex_metadata *imd, const char *setname);
int  as_sindex__pre_op_assert(as_sindex *si, int op);
int  as_sindex__post_op_assert(as_sindex *si, int op);
void as_sindex__process_ret(as_sindex *si, int ret, as_sindex_op op, uint64_t starttime, int pos);
void as_sindex__dup_meta(as_sindex_metadata *imd, as_sindex_metadata **qimd, bool refcounted);
int  as_sindex_sbin_from_sindex(as_sindex * si, as_bin *b, as_sindex_bin * sbin, as_val ** cdt_asval, 
					byte * buf, uint32_t buf_sz, as_particle_type type, bool from_buf);
void as_sindex_init_sbin(as_sindex_bin * sbin, as_sindex_op op, as_particle_type type, int simatch);
void                 as_sindex_set_binid_has_sindex(as_namespace *ns, int binid);
void                 as_sindex_reset_binid_has_sindex(as_namespace *ns, int binid);
bool                 as_sindex_binid_has_sindex(as_namespace *ns, int binid);
as_sindex_status     as_sindex_extract_bin_from_path(char * path_str, char *bin);
// Translation from sindex error code to string
const char *as_sindex_err_str(int op_code) {
	switch(op_code) {
		case AS_SINDEX_ERR_FOUND:               return "INDEX FOUND";           break;
		case AS_SINDEX_ERR_NO_MEMORY:           return "NO MEMORY";             break;
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:     return "UNKNOWN KEYTYPE";       break;
		case AS_SINDEX_ERR_BIN_NOTFOUND:        return "BIN NOT FOUND";         break;
		case AS_SINDEX_ERR_NOTFOUND:            return "NO INDEX";              break;
		case AS_SINDEX_ERR_PARAM:               return "ERR PARAM";             break;
		case AS_SINDEX_ERR_TYPE_MISMATCH:       return "KEY TYPE MISMATCH";     break;
		case AS_SINDEX_ERR:                     return "ERR GENERIC";           break;
		case AS_SINDEX_OK:                      return "OK";                    break;
		default:                                return "Unknown Code";          break;
	}
}

/*
 * Function as_sindex_isactive
 *
 * Returns sindex state
 */
inline bool as_sindex_isactive(as_sindex *si)
{
	if ((!si) || (!si->imd)) return FALSE;
	bool ret;
	if (si->state == AS_SINDEX_ACTIVE) {
		ret = TRUE;
	} else {
		ret = FALSE;
	}
	return ret;
}

/*
 * Notes-
 * 		Translation from sindex internal error code to generic client visible
 * 		Aerospike error code
 */
int as_sindex_err_to_clienterr(int err, char *fname, int lineno) {
	switch(err) {
		case AS_SINDEX_ERR_FOUND:        return AS_PROTO_RESULT_FAIL_INDEX_FOUND;
		case AS_SINDEX_ERR_NOTFOUND:     return AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND;
		case AS_SINDEX_ERR_NO_MEMORY:    return AS_PROTO_RESULT_FAIL_INDEX_OOM;
		case AS_SINDEX_ERR_PARAM:        return AS_PROTO_RESULT_FAIL_PARAMETER;
		case AS_SINDEX_ERR_NOT_READABLE: return AS_PROTO_RESULT_FAIL_INDEX_NOTREADABLE;
		case AS_SINDEX_OK:               return AS_PROTO_RESULT_OK;
		
		// Defensive internal error
		case AS_SINDEX_ERR_SET_MISMATCH:
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:
		case AS_SINDEX_ERR_BIN_NOTFOUND:
		case AS_SINDEX_ERR_TYPE_MISMATCH:
		case AS_SINDEX_ERR:
		default: cf_warning(AS_SINDEX, "%s Error at %s,%d",
							 as_sindex_err_str(err), fname, lineno);
											return AS_PROTO_RESULT_FAIL_INDEX_GENERIC;
	}
}

typedef struct sindex_set_binid_hash_ele_s {
	cf_ll_element ele;
	int           simatch;
} sindex_set_binid_hash_ele;

void
as_sindex__set_binid_hash_destroy(cf_ll_element * ele) {
	cf_free((sindex_set_binid_hash_ele * ) ele);
}

/*
 * Should happen under SINDEX_GWLOCK
 */
as_sindex_status
as_sindex__put_in_set_binid_hash(as_namespace * ns, char * set, int binid, int chosen_id)
{
	// Create fixed size key for hash
	// Get the linked list from the hash
	// If linked list does not exist then make one and put it in the hash
	// Append the chosen id in the linked list

	if (chosen_id < 0 || chosen_id > AS_SINDEX_MAX) {
		cf_debug(AS_SINDEX, "Put in set_binid hash got invalid simatch %d", chosen_id);
		return AS_SINDEX_ERR;
	}
	cf_ll * simatch_ll = NULL;
	// Create fixed size key for hash
	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);

	if (set == NULL ) {
		sprintf(si_prop, "_%d", binid);
	}
	else {
		sprintf(si_prop, "%s_%d", set, binid);
	}

	// Get the linked list from the hash
	int rv      = shash_get(ns->sindex_set_binid_hash, (void *)si_prop, (void *)&simatch_ll);

	// If linked list does not exist then make one and put it in the hash
	if (rv && rv != SHASH_ERR_NOTFOUND) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return AS_SINDEX_ERR;
	};
	if (rv == SHASH_ERR_NOTFOUND) {
		simatch_ll = cf_malloc(sizeof(cf_ll));
		cf_ll_init(simatch_ll, as_sindex__set_binid_hash_destroy, false);
		if (SHASH_OK != shash_put(ns->sindex_set_binid_hash, (void *)si_prop, (void *)&simatch_ll)) {
			cf_warning(AS_SINDEX, "shash put failed for key %s", si_prop);
			return AS_SINDEX_ERR;
		}
	}
	if (!simatch_ll) {
		return AS_SINDEX_ERR;
	}

	// Append the chosen id in the linked list
	sindex_set_binid_hash_ele * ele = cf_malloc(sizeof(sindex_set_binid_hash_ele));
	ele->simatch                    = chosen_id;
	cf_ll_append(simatch_ll, (cf_ll_element*)ele);
	return AS_SINDEX_OK;
}

/*
 * Should happen under SINDEX_GWLOCK
 */
as_sindex_status
as_sindex__delete_from_set_binid_hash(as_namespace * ns, as_sindex_metadata * imd)
{
	// Make a key
	// Get the sindex list corresponding to key
	// If the list does not exist, return does not exist
	// If the list exist 
	// 		match the path and type of incoming si to the existing sindexes in the list
	// 		If any element matches
	// 			Delete from the list
	// 			If the list size becomes 0
	// 				Delete the entry from the hash
	// 		If none of the element matches, return does not exist.
	//

	// Make a key
	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);
	if (imd->set == NULL ) {
		sprintf(si_prop, "_%d", imd->binid[0]);
	}
	else {
		sprintf(si_prop, "%s_%d", imd->set, imd->binid[0]);
	}

	// Get the sindex list corresponding to key
	cf_ll * simatch_ll = NULL;
	int rv             = shash_get(ns->sindex_set_binid_hash, (void *)si_prop, (void *)&simatch_ll);

	// If the list does not exist, return does not exist
	if (rv && rv != SHASH_ERR_NOTFOUND) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return AS_SINDEX_ERR_NOTFOUND;
	};
	if (rv == SHASH_ERR_NOTFOUND) {
		return AS_SINDEX_ERR_NOTFOUND;
	}

	// If the list exist 
	// 		match the path and type of incoming si to the existing sindexes in the list	
	bool    to_delete                    = false;
	cf_ll_element * ele                  = NULL;
	sindex_set_binid_hash_ele * prop_ele = NULL;
	if (simatch_ll) {
		ele = cf_ll_get_head(simatch_ll);
		while (ele) {
			prop_ele       = ( sindex_set_binid_hash_ele * ) ele;
			as_sindex * si = &(ns->sindex[prop_ele->simatch]);
			if (strcmp(si->imd->path_str, imd->path_str) == 0 && 
				si->imd->btype[0] == imd->btype[0] && si->imd->itype == imd->itype) {
				to_delete  = true;
				break;
			}
			ele = ele->next;
		}
	}
	else {
		return AS_SINDEX_ERR_NOTFOUND;
	}

	// 		If any element matches
	// 			Delete from the list
	if (to_delete && ele) {
		cf_ll_delete(simatch_ll, ele);
	}

	// 			If the list size becomes 0
	// 				Delete the entry from the hash
	if (cf_ll_size(simatch_ll) == 0) {
		rv = shash_delete(ns->sindex_set_binid_hash, si_prop);
		if (rv) {
			cf_debug(AS_SINDEX, "shash_delete fails with error %d", rv);
		}
	}

	// 		If none of the element matches, return does not exist.
	if (!to_delete) {
		return AS_SINDEX_ERR_NOTFOUND;
	}
	return AS_SINDEX_OK;
}

int
as_index_keys_ll_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
as_index_keys_ll_destroy_fn(cf_ll_element *ele)
{
	as_index_keys_ll_element * node = (as_index_keys_ll_element *) ele;
	if (node) {
		if (node->keys_arr) {
			as_index_keys_release_arr_to_queue(node->keys_arr);
			node->keys_arr = NULL;
		}
		cf_free(node);
	}
}

as_index_keys_arr *
as_index_get_keys_arr(void)
{
	as_index_keys_arr *keys_arr;
	if (cf_queue_pop(g_q_index_keys_arr, &keys_arr, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		keys_arr = cf_malloc(sizeof(as_index_keys_arr));
	}
	keys_arr->num = 0;
	return keys_arr;
}

void
as_index_keys_release_arr_to_queue(as_index_keys_arr *v)
{
	as_index_keys_arr * keys_arr = (as_index_keys_arr *)v;
	if (cf_queue_sz(g_q_index_keys_arr) < AS_INDEX_KEYS_ARRAY_QUEUE_HIGHWATER) {
		cf_queue_push(g_q_index_keys_arr, &keys_arr);
	} 
	else {
		cf_free(keys_arr);
	}
}
/*
 * Should happen under SINDEX_GRLOCK if called directly.
 */
as_sindex_status
as_sindex__simatch_list_by_set_binid(as_namespace * ns, const char *set, int binid, cf_ll ** simatch_ll)
{
	// Make the fixed size key (set_binid)
	// Look for the key in set_binid_hash
	// If found return the value (list of simatches)
	// Else return NULL

	// Make the fixed size key (set_binid)
	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);
	if (!set) {
		sprintf(si_prop, "_%d", binid);
	}
	else {
		sprintf(si_prop, "%s_%d", set, binid);
	}

	// Look for the key in set_binid_hash
	int rv             = shash_get(ns->sindex_set_binid_hash, (void *)si_prop, (void *)simatch_ll);
	
	// If not found return NULL
	if (rv || !(*simatch_ll)) {
		cf_debug(AS_SINDEX, "shash get failed with error %d", rv);
		return AS_SINDEX_ERR_NOTFOUND;
	};

	// Else return simatch_ll
	return AS_SINDEX_OK;
}

/*
 * Should happen under SINDEX_GRLOCK
 */
int
as_sindex__simatch_by_set_binid(as_namespace *ns, char * set, int binid, as_sindex_ktype type, as_sindex_type itype, char * path)
{
	// get the list corresponding to the list from the hash
	// if list does not exist return -1
	// If list exist
	// 		Iterate through all the elements in the list and match the path and type
	// 		If matches 
	// 			return the simatch
	// 	If none of the si matches
	// 		return -1

	cf_ll * simatch_ll = NULL;
	as_sindex__simatch_list_by_set_binid(ns, set, binid, &simatch_ll);

	// If list exist
	// 		Iterate through all the elements in the list and match the path and type
	int     simatch                      = -1;
	sindex_set_binid_hash_ele * prop_ele = NULL;
	cf_ll_element * ele                  = NULL;
	if (simatch_ll) {
		ele = cf_ll_get_head(simatch_ll);
		while (ele) {
			prop_ele = ( sindex_set_binid_hash_ele * ) ele;
			as_sindex * si = &(ns->sindex[prop_ele->simatch]);
			if (strcmp(si->imd->path_str, path) == 0 && 
				si->imd->btype[0] == type && si->imd->itype == itype) {
				simatch  = prop_ele->simatch;
				break;
			}
			ele = ele->next;
		}
	}
	else {
		return -1;
	}

	// 		If matches 
	// 			return the simatch
	// 	If none of the si matches
	// 		return -1
	return simatch;
}


int
as_sindex__simatch_by_iname(as_namespace *ns, char *idx_name)
{
	int simatch = -1;
	char iname[AS_ID_INAME_SZ]; memset(iname, 0, AS_ID_INAME_SZ);
	snprintf(iname, strlen(idx_name) + 1, "%s", idx_name);
	int rv      = shash_get(ns->sindex_iname_hash, (void *)iname, (void *)&simatch);
	cf_detail(AS_SINDEX, "Found iname simatch %s->%d rv=%d", iname, simatch, rv);
		
	if (rv) {
		return -1;
	}
	return simatch;
}

/*
 * Single cluttered interface for lookup. iname precedes binid
 * i.e if both are specified search is done with iname
 */
#define AS_SINDEX_LOOKUP_FLAG_SETCHECK     0x01
#define AS_SINDEX_LOOKUP_FLAG_ISACTIVE     0x02
#define AS_SINDEX_LOOKUP_FLAG_NORESERVE    0x04
as_sindex *
as_sindex__lookup_lockfree(as_namespace *ns, char *iname, char *set, int binid,
								as_sindex_ktype type, as_sindex_type itype, char * path, char flag)
{

	// If iname is not null then search in iname hash and store the simatch
	// Else then 
	// 		Check the possible existence of sindex over bin in the bit array
	//		If no possibility return NULL
	//		Search in the set_binid hash using setname, binid, itype and binid
	//		If found store simatch
	//		If not found return NULL
	//			Get the sindex corresponding to the simatch.
	// 			Apply the flags applied by caller.
	//          Validate the simatch
	
	int simatch   = -1;
	as_sindex *si = NULL;
	// If iname is not null then search in iname hash and store the simatch
	if (iname) {
		simatch   = as_sindex__simatch_by_iname(ns, iname);
	}
	// Else then 
	// 		Check the possible existence of sindex over bin in the bit array
	else {
		if (!as_sindex_binid_has_sindex(ns,  binid) ) {	
	//		If no possibility return NULL
			goto END;
		}
	//		Search in the set_binid hash using setname, binid, itype and binid
	//		If found store simatch
		simatch   = as_sindex__simatch_by_set_binid(ns, set, binid, type, itype, path);
	}
	//		If not found return NULL
	// 			Get the sindex corresponding to the simatch.
	if (simatch != -1) {
		si      = &ns->sindex[simatch];
	// 			Apply the flags applied by caller.
		if ((flag & AS_SINDEX_LOOKUP_FLAG_ISACTIVE)
			&& !as_sindex_isactive(si)) {
			si = NULL;
			goto END;
		}
	//          Validate the simatch
		if (simatch != si->simatch) {
			cf_warning(AS_SINDEX, "Inconsistent simatch reference between simatch stored in" 
									"si and simatch stored in hash");
		}
		if (!(flag & AS_SINDEX_LOOKUP_FLAG_NORESERVE))
			AS_SINDEX_RESERVE(si);
	}
END:
	return si;
}

as_sindex *
as_sindex__lookup(as_namespace *ns, char *iname, char *set, int binid, as_sindex_ktype type, 
						as_sindex_type itype, char * path, char flag)
{
	SINDEX_GRLOCK();
	as_sindex *si = as_sindex__lookup_lockfree(ns, iname, set, binid, type, itype, path, flag);
	SINDEX_GUNLOCK();
	return si;
}

as_sindex *
as_sindex_lookup_by_iname(as_namespace *ns, char * iname, char flag)
{
	return as_sindex__lookup(ns, iname, NULL, -1, 0, 0, NULL, flag);
}

as_sindex *
as_sindex_lookup_by_defns(as_namespace *ns, char *set, int binid, as_sindex_ktype type, as_sindex_type itype, char * path, char flag)
{
	return as_sindex__lookup(ns, NULL, set, binid, type, itype, path, flag);
}

as_sindex *
as_sindex_lookup_by_iname_lockfree(as_namespace *ns, char * iname, char flag)
{
	return as_sindex__lookup_lockfree(ns, iname, NULL, -1, 0, 0, NULL, flag);

}

as_sindex *
as_sindex_lookup_by_defns_lockfree(as_namespace *ns, char *set, int binid, as_sindex_ktype type, as_sindex_type itype, char * path, char flag)
{
	return as_sindex__lookup_lockfree(ns, NULL, set, binid, type, itype, path, flag);
}

/*
 *	Arguments
 *		imd     - To match the setname of sindex metadata.
 *		setname - set name to be matched
 *
 *	Returns
 * 		TRUE    - If setname given matches the one in imd
 * 		FALSE   - Otherwise
 */
bool
as_sindex__setname_match(as_sindex_metadata *imd, const char *setname)
{
	// If passed in setname does not match the one on imd
	if (setname && ((!imd->set) || strcmp(imd->set, setname))) goto Fail;
	if (!setname && imd->set)                                  goto Fail;
	return true;
Fail:
	cf_debug(AS_SINDEX, "Index Mismatch %s %s", imd->set, setname);
	return false;
}

/*
 * Returns -
 * 		AS_SINDEX_OK  - On success.
 *		Else on failure one of these -
 * 			AS_SINDEX_ERR
 * 			AS_SINDEX_ERR_OTHER
 * 			AS_SINDEX_ERR_NOT_READABLE
 * Notes -
 * 		Assert anything which is inconsistent with the way DML/DML/DDL/defrag_th/destroy_th
 * 		running in multi-threaded environment.
 * 		This is called before acquiring secondary index lock.
 *
 * Synchronization -
 * 		reserves the imd.
 * 		Caller of DML should always reserves si.
 */
int
as_sindex__pre_op_assert(as_sindex *si, int op)
{
	int ret = AS_SINDEX_ERR;
	if (!si) {
		SINDEX_CRASH("DML with NULL si"); return ret;
	}
	if (!si->imd) {
		SINDEX_CRASH("DML with NULL imd"); return ret;
	}

	// Caller of DML should always reserves si, If the state of si is not DESTROY and
	// if the count is 1 then caller did not reserve fail the assertion
	int count = cf_rc_count(si->imd);
	cf_debug(AS_SINDEX, "DML on index %s in %d state with reference count %d < 2", si->imd->iname, si->state, count);
	if ((count < 2) && (si->state != AS_SINDEX_DESTROY)) {
		cf_warning(AS_SINDEX, "Secondary index is improperly ref counted ... cannot be used");
		return ret;
	}
	ret = AS_SINDEX_OK;
	
	switch (op)
	{
		case AS_SINDEX_OP_READ:
			// First one signifies that index is still getting built
			// Second on signifies because of some failure secondary
			// is not in sync with primary
			if (!(si->flag & AS_SINDEX_FLAG_RACTIVE)
				|| ( (si->desync_cnt > 0)
					&& !(si->config.flag & AS_SINDEX_CONFIG_IGNORE_ON_DESYNC))) {
				ret = AS_SINDEX_ERR_NOT_READABLE;
			}
			break;
		case AS_SINDEX_OP_INSERT:
		case AS_SINDEX_OP_DELETE:
			break;
		default:
			cf_warning(AS_SINDEX, "Unidentified Secondary Index Op .. Ignoring!!");
	}
	return ret;
}

/*
 * Assert anything which is inconsistent with the way DML/DML/DDL/
 * defrag_th/destroy_th running in multi-threaded environment. This is called
 * after releasing secondary index lock
 */
int
as_sindex__post_op_assert(as_sindex *si, int op)
{
	int ret = -1;
	if (!si) {
		SINDEX_CRASH("DML with NULL si"); return ret;
	}
	if (!si->imd) {
		SINDEX_CRASH("DML with NULL imd"); return ret;
	}

	// Caller of DML should always reserves si, If the state of si is not DESTROY and
	// if the count is 1 then caller did not reserve fail the assertion
	if ((cf_rc_count(si->imd) < 2) && (si->state != AS_SINDEX_DESTROY)) {
		SINDEX_CRASH("DML on imd in %d state with < 2 reference count", si->state);
		return ret;
	}
	return AS_SINDEX_OK;
}

/*
 * Function as_sindex_pktype
 * 		Returns the type of particle indexed
 *
 * 	Returns -
 * 		On failure - AS_SINDEX_ERR_UNKNOWN_KEYTYPE
 */
as_particle_type
as_sindex_pktype(as_sindex_metadata * imd)
{
	switch(imd->btype[0]) {
		case AS_SINDEX_KTYPE_LONG: {
			return AS_PARTICLE_TYPE_INTEGER;
		}
		case AS_SINDEX_KTYPE_FLOAT: {
			return AS_PARTICLE_TYPE_FLOAT;
		}
		case AS_SINDEX_KTYPE_DIGEST: {
			return AS_PARTICLE_TYPE_STRING;
		}
		default: {
			cf_warning(AS_SINDEX, "UNKNOWN KEY TYPE FOUND. VERY BAD STATE");
		}
	}
	return AS_SINDEX_ERR_UNKNOWN_KEYTYPE;
}

/*
 * Function as_sindex_key_str
 *     Returns a static string representing the key type
 *
 */
char const *
as_sindex_ktype_str(as_sindex_ktype type)
{
	switch(type) {
	case AS_SINDEX_KTYPE_LONG:		return "INT SIGNED";
	case AS_SINDEX_KTYPE_FLOAT:		return "FLOAT";
	case AS_SINDEX_KTYPE_DIGEST:	return "TEXT";
	default:
		{
			cf_warning(AS_SINDEX, "UNKNOWN KEY TYPE %d. VERY BAD STATE", type);
			return "??????";
		}
	}
}

as_sindex_key_type
as_sindex_key_type_from_pktype(as_particle_type t)
{
	switch(t) {
		case AS_PARTICLE_TYPE_INTEGER :     return AS_SINDEX_KEY_TYPE_LONG;
		case AS_PARTICLE_TYPE_STRING  :     return AS_SINDEX_KEY_TYPE_DIGEST;
		default                       :     {
			cf_warning(AS_SINDEX, "bad particle type %d", t);
			return AS_SINDEX_KEY_TYPE_MAX;
		}
	}
	return AS_SINDEX_KEY_TYPE_MAX;
}

as_sindex_ktype
as_sindex_sktype_from_pktype(as_particle_type t)
{
	switch(t) {
		case AS_PARTICLE_TYPE_INTEGER :     return AS_SINDEX_KTYPE_LONG;
		case AS_PARTICLE_TYPE_FLOAT   :     return AS_SINDEX_KTYPE_FLOAT;
		case AS_PARTICLE_TYPE_STRING  :     return AS_SINDEX_KTYPE_DIGEST;
		default                       :     return AS_SINDEX_KTYPE_NONE;
	}
	return AS_SINDEX_KTYPE_NONE;
}
/*
 * Create duplicate copy of sindex metadata. New lock is created
 * used by index create by user at runtime or index creation at the boot time
 */
void
as_sindex__dup_meta(as_sindex_metadata *imd, as_sindex_metadata **qimd,
		bool refcounted)
{
	if (!imd) return;
	as_sindex_metadata *qimdp;

	if (refcounted) qimdp = cf_rc_alloc(sizeof(as_sindex_metadata));
	else            qimdp = cf_malloc(  sizeof(as_sindex_metadata));
	memset(qimdp, 0, sizeof(as_sindex_metadata));

	qimdp->ns_name = cf_strdup(imd->ns_name);

	// Set name is optional for create
	if (imd->set) {
		qimdp->set = cf_strdup(imd->set);
	} else {
		qimdp->set = NULL;
	}
	
	qimdp->iname       = cf_strdup(imd->iname);
	qimdp->itype       = imd->itype;
	qimdp->nprts       = imd->nprts;
	qimdp->mfd_slot    = imd->mfd_slot;
	qimdp->path_str    = cf_strdup(imd->path_str);
	qimdp->path_length = imd->path_length;
	memcpy(qimdp->path, imd->path, AS_SINDEX_MAX_DEPTH*sizeof(as_sindex_path));
	qimdp->num_bins = imd->num_bins;
	for (int i = 0; i < imd->num_bins; i++) {
		qimdp->bnames[i] = cf_strdup(imd->bnames[i]);
		qimdp->btype[i]  = imd->btype[i];
		qimdp->binid[i]  = imd->binid[i];
	}

    qimdp->oindx = imd->oindx;

	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,  "pthread_rwlockattr_init: %s",
					cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
		cf_crash( AS_TSVC, "pthread_rwlockattr_setkind_np: %s",
				cf_strerror(errno));
	}
	if (pthread_rwlock_init(&qimdp->slock, NULL)) {
		cf_crash(AS_SINDEX,
				"Could not create secondary index dml mutex ");
	}
	qimdp->flag |= IMD_FLAG_LOCKSET;

	*qimd = qimdp;
}

/*
 * Function to perform validation check on the return type and increment
 * decrement all the statistics.
 */
void
as_sindex__process_ret(as_sindex *si, int ret, as_sindex_op op,
		uint64_t starttime, int pos)
{
	switch(op) {
		case AS_SINDEX_OP_INSERT:
			if (AS_SINDEX_ERR_NO_MEMORY == ret) {
				cf_atomic_int_incr(&si->desync_cnt);
			}
			if (ret && ret != AS_SINDEX_KEY_FOUND) {
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Insert into %s failed at %d with %d",
						si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.write_errs);
			} else if (!ret) {
				cf_atomic64_incr(&si->stats.n_objects);
			}
			cf_atomic64_incr(&si->stats.n_writes);
			SINDEX_HIST_INSERT_DATA_POINT(si, write_hist, starttime);
			break;
		case AS_SINDEX_OP_DELETE:
			if (ret && ret != AS_SINDEX_KEY_NOTFOUND) {
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Delete from %s failed at %d with %d",
	                    si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.delete_errs);
			} else if (!ret) {
				cf_atomic64_decr(&si->stats.n_objects);
			}
			cf_atomic64_incr(&si->stats.n_deletes);
			SINDEX_HIST_INSERT_DATA_POINT(si, delete_hist, starttime);
			break;
		case AS_SINDEX_OP_READ:
			if (ret < 0) { // AS_SINDEX_CONTINUE(1) also OK
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Read from %s failed at %d with %d",
						si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.read_errs);
			}
			cf_atomic64_incr(&si->stats.n_reads);
			break;
		default:
			cf_crash(AS_SINDEX, "Invalid op");
	}
}

/*
 * assumes bin names are set
 */
int
as_sindex__populate_binid(as_namespace *ns, as_sindex_metadata *imd)
{
	int i = 0;
	for (i = 0; i < imd->num_bins; i++) {
		// Bin id should be around if not create it
		char bname[AS_ID_BIN_SZ];
		strncpy(bname, imd->bnames[i], AS_ID_BIN_SZ);
		if (bname[AS_ID_BIN_SZ - 1] != 0 || !as_bin_name_within_quota(ns, bname)) {
			cf_warning(AS_SINDEX, "bin name %s too big. Bin not added", bname);
			return -1;
		}
		imd->binid[i] = as_bin_get_or_assign_id(ns, bname);
		cf_debug(AS_SINDEX, " Assigned %d for %s %s", imd->binid[i], imd->bnames[i], bname);
	}
	for (i = imd->num_bins; i < AS_SINDEX_BINMAX; i++) {
		imd->binid[i] = -1;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex__op_by_sbin(as_namespace *ns, const char *set, int numbins, as_sindex_bin *start_sbin, cf_digest * pkey)
{
	// If numbins == 0 return AS_SINDEX_OK
	// Iterate through sbins
	// 		Reserve the SI.
	// 		Take the read lock on imd
	//		Get a value from sbin
	//			Get the related pimd
	//			Get the pimd write lock		
	//			If op is DELETE delete the values from sbin from sindex
	//			If op is INSERT put all the values from bin in sindex.
	//			Release the pimd lock
	//		Release the imd lock.
	//		Release the SI.

	as_sindex_status retval = AS_SINDEX_OK;
	if (!ns || !start_sbin) {
		return AS_SINDEX_ERR;	
	}

	// If numbins != 1 return AS_SINDEX_OK
	if (numbins != 1 ) {
		return AS_SINDEX_OK;
	}

	int simatch                = -1;
	as_sindex * si             = NULL;
	as_sindex_bin * sbin   = NULL;
	as_sindex_metadata * imd   = NULL;
	as_sindex_pmetadata * pimd = NULL;
	as_sindex_op op;
	// Iterate through sbins
	for (int i=0; i<numbins; i++) {
	// 		Reserve the SI.
		sbin = &start_sbin[i];
		simatch = sbin->simatch;
		if (simatch == -1) {
			cf_warning(AS_SINDEX, "simatch is coming as -1 while updating");
			return AS_SINDEX_ERR;
		}
		si = &ns->sindex[simatch];
		imd =  si->imd;
		op = sbin->op;
	// 		Take the read lock on imd
		SINDEX_RLOCK(&imd->slock);	
		for (int j=0; j<sbin->num_values; j++) {

			int ret = as_sindex__pre_op_assert(si, op);
			if (AS_SINDEX_OK != ret) {
				goto Cleanup;
			}
	//		Get a value from sbin
			void * skey;
			if (sbin->type == AS_PARTICLE_TYPE_INTEGER) {
				if (j==0) {
					skey = (void *)&(sbin->value.int_val);
				}
				else {
					skey = (void *)((uint64_t *)(sbin->values) + j);
				}
			}
			else if (sbin->type == AS_PARTICLE_TYPE_STRING) {
				if (j==0) {
					skey = (void *)&(sbin->value.str_val);
				}
				else {
					skey = (void *)((cf_digest *)(sbin->values) + j);
				}
			}
			else {
				retval = AS_SINDEX_ERR;
				goto Cleanup;
			}
	//			Get the related pimd	
			pimd = &imd->pimd[ai_btree_key_hash(imd, skey)];
			uint64_t starttime = 0;
			if (si->enable_histogram) {
				starttime = cf_getns();
			}

	//			Get the pimd write lock			
			SINDEX_WLOCK(&pimd->slock);

	//			If op is DELETE delete the value from sindex
			if (op == AS_SINDEX_OP_DELETE) {
				ret = ai_btree_delete(imd, pimd, skey, pkey);
			}
			else if (op == AS_SINDEX_OP_INSERT) {
	//			If op is INSERT put the value in sindex.
				ret = ai_btree_put(imd, pimd, skey, pkey);
			}

	//			Release the pimd lock
			SINDEX_UNLOCK(&pimd->slock);	
			as_sindex__process_ret(si, ret, op, starttime, __LINE__);
		}
		cf_debug(AS_SINDEX, " Secondary Index Op Finish------------- ");

	//		Release the imd lock.
	//		Release the SI.
	
	}
	Cleanup:
	SINDEX_UNLOCK(&imd->slock);
	AS_SINDEX_RELEASE(si);
	return retval;
}

void
as_sindex__stats_clear(as_sindex *si) {
	as_sindex_stat *s = &si->stats;
	
	s->n_objects            = 0;
	
	s->n_reads              = 0;
	s->read_errs            = 0;

	s->n_writes             = 0;
	s->write_errs           = 0;

	s->n_deletes            = 0;
	s->delete_errs          = 0;
	
	s->loadtime             = 0;
	s->recs_pending         = 0;

	s->n_defrag_records     = 0;
	s->defrag_time          = 0;

	// Aggregation stat
	s->n_aggregation        = 0;
	s->agg_response_size    = 0;
	s->agg_num_records      = 0;
	s->agg_errs             = 0;
	// Lookup stats
	s->n_lookup             = 0;
	s->lookup_response_size = 0;
	s->lookup_num_records   = 0;
	s->lookup_errs          = 0;

	si->enable_histogram = false;
	histogram_clear(s->_write_hist);
	histogram_clear(s->_si_prep_hist);
	histogram_clear(s->_delete_hist);
	histogram_clear(s->_query_hist);
	histogram_clear(s->_query_batch_io);
	histogram_clear(s->_query_batch_lookup);
	histogram_clear(s->_query_rcnt_hist);
	histogram_clear(s->_query_diff_hist);
}

void
as_sindex_gconfig_default(as_config *c)
{
	c->sindex_builder_threads         = 4;
	c->sindex_data_max_memory         = ULONG_MAX;
	c->sindex_data_memory_used        = 0;
}
void
as_sindex__config_default(as_sindex *si)
{
	si->config.defrag_period    = 1000;
	si->config.defrag_max_units = 1000;
	si->config.data_max_memory  = ULONG_MAX; // No Limit
	// related non config value defaults
	si->data_memory_used        = 0;
	si->config.flag             = 1; // Default is - index is active
}

void
as_sindex_config_var_copy(as_sindex *to_si, as_sindex_config_var *from_si_cfg)
{
	to_si->config.defrag_period    = from_si_cfg->defrag_period;
	to_si->config.defrag_max_units = from_si_cfg->defrag_max_units;
	to_si->config.data_max_memory  = from_si_cfg->data_max_memory;
	to_si->trace_flag              = from_si_cfg->trace_flag;
	to_si->enable_histogram        = from_si_cfg->enable_histogram;
	to_si->config.flag             = from_si_cfg->ignore_not_sync_flag;
}

void
as_sindex__create_pmeta(as_sindex *si, int simatch, int nptr)
{
	if (!si) return;
	if (nptr == 0) return;
	si->imd->pimd = cf_malloc(nptr * sizeof(as_sindex_pmetadata));
	memset(si->imd->pimd, 0, nptr*sizeof(as_sindex_pmetadata));

	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,
				"pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash(AS_TSVC,
				"pthread_rwlockattr_setkind_np: %s",cf_strerror(errno));

	for (int i = 0; i < nptr; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		if (pthread_rwlock_init(&pimd->slock, NULL)) {
			cf_crash(AS_SINDEX,
					"Could not create secondary index dml mutex ");
		}
		if (ai_post_index_creation_setup_pmetadata(si->imd, pimd,
													simatch, i)) {
			cf_crash(AS_SINDEX,
					"Something went reallly bad !!!");
		}
	}
}

// Hash a binname string.
static inline uint32_t
as_sindex__set_binid_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, sizeof(uint32_t));
}

// Hash a binname string.
static inline uint32_t
as_sindex__iname_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, strlen((const char*)p_key));
}

void
as_sindex__setup_histogram(as_sindex *si)
{
	char hist_name[AS_ID_INAME_SZ+64];
	sprintf(hist_name, "%s_write_us", si->imd->iname);
	if (NULL == (si->stats._write_hist = histogram_create(hist_name, HIST_MICROSECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex write histogram");
	
	sprintf(hist_name, "%s_si_prep_us", si->imd->iname);
	if (NULL == (si->stats._si_prep_hist = histogram_create(hist_name, HIST_MICROSECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex prepare histogram");

	sprintf(hist_name, "%s_delete_us", si->imd->iname);
	if (NULL == (si->stats._delete_hist = histogram_create(hist_name, HIST_MICROSECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex delete histogram");

	sprintf(hist_name, "%s_query", si->imd->iname);
	if (NULL == (si->stats._query_hist = histogram_create(hist_name, HIST_MILLISECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query histogram");

	sprintf(hist_name, "%s_query_batch_lookup_us", si->imd->iname);
	if (NULL == (si->stats._query_batch_lookup = histogram_create(hist_name, HIST_MICROSECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query batch-lookup histogram");

	sprintf(hist_name, "%s_query_batch_io_us", si->imd->iname);
	if (NULL == (si->stats._query_batch_io = histogram_create(hist_name, HIST_MICROSECONDS)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query io histogram");

	sprintf(hist_name, "%s_query_row_count", si->imd->iname);
	if (NULL == (si->stats._query_rcnt_hist = histogram_create(hist_name, HIST_RAW)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query row count histogram");

	sprintf(hist_name, "%s_query_diff_count", si->imd->iname);
	if (NULL == (si->stats._query_diff_hist = histogram_create(hist_name, HIST_RAW)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query diff histogram");

}

int
as_sindex__destroy_histogram(as_sindex *si)
{
	if (si->stats._write_hist)            cf_free(si->stats._write_hist);
	if (si->stats._si_prep_hist)          cf_free(si->stats._si_prep_hist);
	if (si->stats._delete_hist)           cf_free(si->stats._delete_hist);
	if (si->stats._query_hist)            cf_free(si->stats._query_hist);
	if (si->stats._query_batch_lookup)    cf_free(si->stats._query_batch_lookup);
	if (si->stats._query_batch_io)        cf_free(si->stats._query_batch_io);
	if (si->stats._query_rcnt_hist)       cf_free(si->stats._query_rcnt_hist);
	if (si->stats._query_diff_hist)       cf_free(si->stats._query_diff_hist);
	return 0;
}

/*
 * Main initialization function. Talks to Aerospike Index to pull up all the indexes
 * and populates sindex hanging from namespace
 */
int
as_sindex_init(as_namespace *ns)
{
	ns->sindex = cf_malloc(sizeof(as_sindex) * AS_SINDEX_MAX);
	if (!ns->sindex)
		cf_crash(AS_SINDEX,
				"Could not allocation memory for secondary index");

	ns->sindex_cnt = 0;
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si                    = &ns->sindex[i];
		memset(si, 0, sizeof(as_sindex));
		si->state                        = AS_SINDEX_INACTIVE;
		si->stats._delete_hist           = NULL;
		si->stats._query_hist            = NULL;
		si->stats._query_batch_lookup    = NULL;
		si->stats._query_batch_io        = NULL;
		si->stats._query_rcnt_hist       = NULL;
		si->stats._query_diff_hist       = NULL;
	}
	
	// binid to simatch lookup
	if (SHASH_OK != shash_create(&ns->sindex_set_binid_hash,
						as_sindex__set_binid_hash_fn, AS_SINDEX_PROP_KEY_SIZE, sizeof(cf_ll *),
						AS_SINDEX_MAX, 0)) {
		cf_crash(AS_AS, "Couldn't create sindex binid hash");
	}

	// iname to simatch lookup
	if (SHASH_OK != shash_create(&ns->sindex_iname_hash,
						as_sindex__iname_hash_fn, AS_ID_INAME_SZ, sizeof(uint32_t),
						AS_SINDEX_MAX, 0)) {
		cf_crash(AS_AS, "Couldn't create sindex iname hash");
	}

	// Init binid_has_sindex to zero
	memset(ns->binid_has_sindex, 0, sizeof(uint32_t)*AS_BINID_HAS_SINDEX_SIZE);	
	if (!g_q_index_keys_arr) {
		g_q_index_keys_arr = cf_queue_create(sizeof(void *), true);
	}
	return AS_SINDEX_OK;
}

/*
 * Cleanup function NOOP
 */
void
as_sindex_shutdown(as_namespace *ns)
{
	// NO OP
	return;
}

// Reserve the sindex so it does not get deleted under the hood
int
as_sindex_reserve(as_sindex *si, char *fname, int lineno)
{
	if (!as_sindex_isactive(si)) {
		cf_warning(AS_SINDEX, "Trying to reserve sindex %s in a state other than active. State is %d", 
							si->imd->iname, si->state);
	}
	if (si->imd) cf_rc_reserve(si->imd);
	int count = cf_rc_count(si->imd);
	cf_debug(AS_SINDEX, "Index %s in %d state Reserved to reference count %d < 2 at %s:%d", si->imd->iname, si->state, count, fname, lineno);
	return AS_SINDEX_OK;
}

int
as_sindex_reserve_all(as_namespace *ns, int *imatch)
{
	int count = 0;
	SINDEX_GRLOCK();
	// May want to let the delete go through ...
	if (ns->sindex) {
		for (int i = 0; i < AS_SINDEX_MAX; i++) {
			as_sindex *si = &ns->sindex[i];
			if (!as_sindex_isactive(si))    continue;
			imatch[count++] = i;
			AS_SINDEX_RESERVE(si);
		}
	}
	SINDEX_GUNLOCK();
	return count;
}


/*
 * Release, queue up the request for the destroy to clean up Aerospike Index thread,
 * Not done inline because main write thread could release the last reference.
 */
int
as_sindex_release(as_sindex *si, char *fname, int lineno)
{
	if (!si) return AS_SINDEX_OK;
	// Can be checked without locking
	uint64_t val = cf_rc_release(si->imd);
	if (val == 0) {
		cf_assert((si->state == AS_SINDEX_DESTROY),
					AS_SINDEX, CF_CRITICAL,
					" Invalid state at cleanup");
		cf_assert(!(si->state & AS_SINDEX_FLAG_DESTROY_CLEANUP),
					AS_SINDEX, CF_CRITICAL,
					" Invalid state at cleanup");
		si->flag |= AS_SINDEX_FLAG_DESTROY_CLEANUP;
		if (CF_QUEUE_OK != cf_queue_push(g_sindex_destroy_q, &si)) {
			return AS_SINDEX_ERR;
		}
	}
	else {
		SINDEX_RLOCK(&si->imd->slock);
		cf_debug(AS_SINDEX, "Index %s in %d state Released "
					"to reference count %d < 2 at %s:%d",
					si->imd->iname, si->state, val, fname, lineno);
		// Display a warning when rc math is messed-up during sindex-delete
		if(si->state == AS_SINDEX_DESTROY){
			cf_info(AS_SINDEX,"Returning from a sindex destroy op for: %s with reference count %"PRIu64"", 
								si->imd->iname, val);
		}
		SINDEX_UNLOCK(&si->imd->slock);
	}
	return AS_SINDEX_OK;
}

// Free if IMD has allocated the info in it
int
as_sindex_imd_free(as_sindex_metadata *imd)
{
	if (!imd) return 1;
	if (imd->ns_name)  cf_free(imd->ns_name);
	if (imd->iname)    cf_free(imd->iname);
	if (imd->set)      cf_free(imd->set);
	if (imd->path_str) cf_free(imd->path_str);
	if (imd->flag & IMD_FLAG_LOCKSET)           pthread_rwlock_destroy(&imd->slock);
	if (imd->num_bins) {
		for (int i=0; i<imd->num_bins; i++) {
			if (imd->bnames[i]) {
				cf_free(imd->bnames[i]);
				imd->bnames[i] = NULL;
			}
		}
	}
	return AS_SINDEX_OK;
}


void
as_sindex_destroy_pmetadata(as_sindex *si)
{
	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,
				"pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash(AS_TSVC,
				"pthread_rwlockattr_setkind_np: %s",cf_strerror(errno));

	for (int i = 0; i < si->imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		pthread_rwlock_destroy(&pimd->slock);
	}
	as_sindex__destroy_histogram(si);
	cf_free(si->imd->pimd);
	si->imd->pimd = NULL;
}

int as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata *imd);

/*
 * Description     : Checks whether an index with the same defn already exists.
 *                   Index defn ={index_name, bin_name, bintype, set_name, ns_name}
 *
 * Parameters      : ns  -> namespace in which index is created
 *                   imd -> imd for create request (does not have binid populated)
 *
 * Returns         : true  if index with the given defn already exists.
 *                   false otherwise
 *
 * Synchronization : Required lock acquired by lookup functions.
 */
bool
as_sindex_exists_by_defn(as_namespace* ns, as_sindex_metadata* imd)
{
	char *iname    = imd->iname;
	as_sindex * si = as_sindex_lookup_by_iname(ns, iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if(!si) {
		return false;
	}
	int binid     = as_bin_get_id(ns, imd->bnames[0]);
	for (int i = 0; i < AS_SINDEX_BINMAX; i++) {
		if(si->imd->bnames[i] && imd->bnames[i]) {
			if (binid == si->imd->binid[i]
					&& !strcmp(imd->bnames[i], si->imd->bnames[i])
					&& imd->btype[i] == si->imd->btype[i]) {
				AS_SINDEX_RELEASE(si);
				return true;
			}
		}
	}
	AS_SINDEX_RELEASE(si);
	return false;
}

// Always make this function get cfg default from as_sindex structure's values,
// instead of hard-coding it : useful when the defaults change.
// This also gets called during config-file init, at that time, we are using a
// dummy variable init.
void
as_sindex_config_var_default(as_sindex_config_var *si_cfg)
{
	// Mandatory memset, Totally worth the cost
	// Do not remove
	memset(si_cfg, 0, sizeof(as_sindex_config_var));

	as_sindex from_si;
	as_sindex__config_default(&from_si);

	// 2 of the 6 variables : enable-histogram and trace-flag are not a part of default-settings for si
	si_cfg->defrag_period        = from_si.config.defrag_period;
	si_cfg->defrag_max_units     = from_si.config.defrag_max_units;
	// related non config value defaults
	si_cfg->data_max_memory      = from_si.config.data_max_memory;
	si_cfg->ignore_not_sync_flag = from_si.config.flag;
}

/*
 * Client API to create new secondary index
 */
int
as_sindex_create(as_namespace *ns, as_sindex_metadata *imd, bool user_create)
{
	int ret = -1;
	// Ideally there should be one lock per namespace, but because the
	// Aerospike Index metadata is single global structure we need a overriding
	// lock for that. NB if it becomes per namespace have a file lock
	SINDEX_GWLOCK();
	if (as_sindex_lookup_by_iname_lockfree(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_NORESERVE)) {
		cf_detail(AS_SINDEX,"Index %s already exists", imd->iname);
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR_FOUND;
	}
	int i, chosen_id;
	as_sindex *si = NULL;
	for (i = 0; i < AS_SINDEX_MAX; i++) {
		if (ns->sindex[i].state == AS_SINDEX_INACTIVE) {
			si = &ns->sindex[i];
			chosen_id = i; break;
		}
	}

	if (!si || (i == AS_SINDEX_MAX))  {
		cf_warning(AS_SINDEX,
				"Maxed out secondary index limit no more indexes allowed");
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR;
	}

	imd->nprts  = ns->sindex_num_partitions;
	int id      = chosen_id;
	si          = &ns->sindex[id];
	as_sindex_metadata *qimd;
	if (!imd->oindx) {
		if (as_sindex__populate_binid(ns, imd)) {
			SINDEX_GUNLOCK();
			return AS_SINDEX_ERR_PARAM;
		}
	}

	// Reason for doing it upfront is to fail fast. Without doing
	// whole bunch of Aerospike Index work

	char si_prop[AS_SINDEX_PROP_KEY_SIZE];
	memset(si_prop, 0, AS_SINDEX_PROP_KEY_SIZE);

	if (imd->set == NULL ) {
		// sindex can be over a NULL set
		sprintf(si_prop, "_%d_%d", imd->binid[0], imd->btype[0]);
	}
	else {
		sprintf(si_prop, "%s_%d_%d", imd->set, imd->binid[0], imd->btype[0]);
	}

	as_sindex_status rv = as_sindex__put_in_set_binid_hash(ns, imd->set, imd->binid[0], chosen_id);
	if (rv != AS_SINDEX_OK) {
		cf_warning(AS_SINDEX, "Put in set_binid hash fails with error %d", rv);
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR;
	}

	cf_detail(AS_SINDEX, "Put binid simatch %d->%d", imd->binid[0], chosen_id);

	char iname[AS_ID_INAME_SZ]; memset(iname, 0, AS_ID_INAME_SZ);
	snprintf(iname, strlen(imd->iname)+1, "%s", imd->iname);
	if (SHASH_OK != shash_put(ns->sindex_iname_hash, (void *)iname, (void *)&chosen_id)) {
		cf_warning(AS_SINDEX, "Internal error ... Duplicate element found sindex iname hash [%s %s]",
						imd->iname, as_bin_get_name_from_id(ns, imd->binid[0]));
		
		rv = as_sindex__delete_from_set_binid_hash(ns, imd);
		if (rv) {
			cf_warning(AS_SINDEX, "Delete from set_binid hash fails with error %d", rv);
		}
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR;
	}
	cf_detail(AS_SINDEX, "Put iname simatch %s:%d->%d", iname, strlen(imd->iname), chosen_id);

	as_sindex__dup_meta(imd, &qimd, true);
	qimd->si    = si;
	qimd->nprts = imd->nprts;
	int bimatch = -1;

	ret = ai_btree_create(qimd, id, &bimatch, imd->nprts);
	if (!ret) { // create ref counted index metadata & hang it from sindex
		si->imd         = qimd;
		si->imd->bimatch = bimatch;
		si->state       = AS_SINDEX_ACTIVE;
		as_sindex_set_binid_has_sindex(ns, si->imd->binid[0]);
		si->trace_flag  = 0;
		si->desync_cnt  = 0;
		si->flag        = AS_SINDEX_FLAG_WACTIVE;
		si->new_imd     = NULL;
		as_sindex__create_pmeta(si, id, imd->nprts);
		// Always tune si to default settings to start with
		as_sindex__config_default(si);

		// If this si has a valid config-item and this si was created by smd boot-up,
		// then set the config-variables from si_cfg_array.

		// si_cfg_var_hash is deliberately kept as a transient structure.
		// Its applicable only for boot-time si-creation via smd
		// In the case of sindex creations via aql, i.e dynamic si creation,
		// this array wont exist and all si configs will be default configs.
		if (ns->sindex_cfg_var_hash) {
			// Check for duplicate si stanzas with the same si-name ?
			as_sindex_config_var check_si_conf;

			if (SHASH_OK == shash_get(ns->sindex_cfg_var_hash, (void *)iname, (void *)&check_si_conf)){
				// A valid config stanza exists for this si entry
				// Copy the config over to the new si
				// delete the old hash entry
				cf_info(AS_SINDEX,"Found custom configuration for SI:%s, applying", imd->iname);
				as_sindex_config_var_copy(si, &check_si_conf);
				shash_delete(ns->sindex_cfg_var_hash,  (void *)iname);
				check_si_conf.conf_valid_flag = true;
				shash_put_unique(ns->sindex_cfg_var_hash, (void *)iname, (void *)&check_si_conf);
			}
		}

		as_sindex__setup_histogram(si);
		as_sindex__stats_clear(si);

		ns->sindex_cnt++;
		si->ns          = ns;
		si->simatch     = chosen_id;
		as_sindex_reserve_data_memory(si->imd, ai_btree_get_isize(si->imd));
		
		// Only trigger scan if this create is done after boot
		if (user_create && g_sindex_boot_done) {
			// Reserve it before pushing it into queue
			AS_SINDEX_RESERVE(si);
			SINDEX_GUNLOCK();
			int rv = cf_queue_push(g_sindex_populate_q, &si);
			if (CF_QUEUE_OK != rv) {
				cf_warning(AS_SINDEX, "Failed to queue up for population... index=%s "
							"Internal Queue Error rv=%d, try dropping and recreating",
							si->imd->iname, rv);
			}
		} else {
			// Internal create is called before storage is initialized. Loading
			// of storage will fill up the indexes no need to queue it up for scan
			SINDEX_GUNLOCK();
		}
	} else {
		// TODO: When alc_btree_create fails, accept_cb should have a better
		//       way to handle failure. Currently it maintains a dummy si
		//       structures with not created flag. accept_cb should repair
		//       such dummy si structures and retry alc_btree_create.
		shash_delete(ns->sindex_iname_hash, (void *)iname);
		rv = as_sindex__delete_from_set_binid_hash(ns, imd);
		if (rv) {
			cf_warning(AS_SINDEX, "Delete from set_binid hash fails with error %d", rv);
		}
		as_sindex_imd_free(qimd);
		cf_debug(AS_SINDEX, "Create index %s failed ret = %d",
				imd->iname, ret);
		SINDEX_GUNLOCK();
	}
	return ret;
}


/*
 * Client API to mark index population finished, tick it ready for read
 */
int
as_sindex_populate_done(as_sindex *si)
{
	int ret = AS_SINDEX_OK;
	SINDEX_WLOCK(&si->imd->slock);
	// Setting flag is atomic: meta lockless
	si->flag |= AS_SINDEX_FLAG_RACTIVE;
	si->flag &= ~AS_SINDEX_FLAG_POPULATING;
	SINDEX_UNLOCK(&si->imd->slock);
	return ret;
}

bool
as_sindex_delete_checker(as_namespace *ns, as_sindex_metadata *imd)
{
	if (as_sindex_lookup_by_iname_lockfree(ns, imd->iname, 
			AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE)) {
		return true;
	} else {
		return false;
	}
}

/*
 * Client API to destroy secondary index, mark destroy
 * Deletes via smd or info-command user-delete requests.
 */
int
as_sindex_destroy(as_namespace *ns, as_sindex_metadata *imd)
{
	SINDEX_GWLOCK();
	as_sindex *si   = as_sindex_lookup_by_iname_lockfree(ns, imd->iname,
						AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE);

	if (si) {
		if (imd->post_op == 1) {
			as_sindex__dup_meta(imd, &si->new_imd, false);
		}
		else {
			si->new_imd = NULL;
		}
		si->state = AS_SINDEX_DESTROY;
		as_sindex_reset_binid_has_sindex(ns, imd->binid[0]);
		AS_SINDEX_RELEASE(si);
		SINDEX_GUNLOCK();
		return AS_SINDEX_OK;
	} else {
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR_NOTFOUND;
	}
}

/*
 * Client API to insert value into the index, pick value from passed in
 * record. Acquires the imd lock, caller should have reserved si.
 */
int
as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd)
{
	as_sindex *sindex_arr[AS_SINDEX_MAX];
	SINDEX_GRLOCK();
	int ret = AS_SINDEX_OK;
	int cnt = 0;

	// Reserve it before pushing it into queue
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		if (!as_sindex_isactive(si))  continue;
		AS_SINDEX_RESERVE(si);
		sindex_arr[cnt++] = si;
		if (cnt == ns->sindex_cnt) {
			break;
		}
	}
	SINDEX_GUNLOCK();

	for (int i=0; i<cnt; i++) {
		as_sindex *si = sindex_arr[i];
		as_sindex_put_rd(si, rd);
	}
	// Sindex release is done after sindex tree is updated
	return ret;
}

/*
 * Client API to insert value into the index, pick value from passed in
 * record. Acquires the imd lock, caller should have reserved si.
 */
int
as_sindex_put_rd(as_sindex *si, as_storage_rd *rd)
{
	as_sindex_metadata *imd = si->imd;
	// Validate Set name. Other function do this check while
	// performing searching for simatch.
	const char *setname = NULL;
	if (as_index_has_set(rd->r)) {
		setname = as_index_get_set_name(rd->r, si->ns);
	}

	SINDEX_RLOCK(&imd->slock);
	if (!as_sindex__setname_match(imd, setname)) {
		SINDEX_UNLOCK(&imd->slock);
		goto Cleanup;
	}
	SINDEX_UNLOCK(&imd->slock);

	// collect sbins
	SINDEX_GRLOCK();
	SINDEX_BINS_SETUP(sbins, 1);

	int sindex_found = 0;
	for (int i = 0; i < imd->num_bins; i++) {
		as_bin *b = as_bin_get(rd, imd->bnames[i]);
		as_val * cdt_val = NULL;
		if (b) {
			as_sindex_init_sbin(&sbins[sindex_found], AS_SINDEX_OP_INSERT, as_sindex_pktype(si->imd), si->simatch);
			sindex_found += as_sindex_sbin_from_sindex(si, b, &sbins[sindex_found], &cdt_val, NULL, 0, 0, false);
			// Only one sbin should be created here.
		}
		// FREE as_val
		if (cdt_val) {
			as_val_destroy(cdt_val);
		}
	}
	SINDEX_GUNLOCK();

	if (sindex_found) {
		as_sindex_update_by_sbin(rd->ns, setname, sbins, sindex_found, &rd->keyd);	
		as_sindex_sbin_freeall(sbins, sindex_found);
		return AS_SINDEX_OK;
	}

Cleanup:
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_OK;
}

/*
 * Returns -
 * 		AS_SINDEX_ERR_PARAM
 *		o/w return value from ai_btree_query
 *
 * Notes -
 * 		Client API to do range get from index based on passed in range key, returns
 * 		digest list
 *
 * Synchronization -
 * 		
 */
int
as_sindex_query(as_sindex *si, as_sindex_range *srange, as_sindex_qctx *qctx)
{
	if ((!si || !srange)) return AS_SINDEX_ERR_PARAM;
	as_sindex_metadata *imd = si->imd;
	SINDEX_RLOCK(&imd->slock);
	SINDEX_RLOCK(&imd->pimd[qctx->pimd_idx].slock);
	int ret = as_sindex__pre_op_assert(si, AS_SINDEX_OP_READ);
	if (AS_SINDEX_OK != ret) {
		SINDEX_UNLOCK(&imd->pimd[qctx->pimd_idx].slock);
		SINDEX_UNLOCK(&imd->slock);
		return ret;
	}
	uint64_t starttime = 0;
	ret = ai_btree_query(imd, srange, qctx);
	as_sindex__process_ret(si, ret, AS_SINDEX_OP_READ, starttime, __LINE__);
	SINDEX_UNLOCK(&imd->pimd[qctx->pimd_idx].slock);
	SINDEX_UNLOCK(&imd->slock);
	return ret;
}

int
as_sindex_repair(as_namespace *ns, as_sindex_metadata *imd)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	as_sindex *si = as_sindex_lookup_by_iname(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (si) {
		if (si->desync_cnt == 0) {
			return AS_SINDEX_OK;
		}
		int rv = cf_queue_push(g_sindex_populate_q, &si);
		if (CF_QUEUE_OK != rv) {
			cf_warning(AS_SINDEX, "Failed to queue up for population... index=%s "
					"Internal Queue Error rv=%d, retry repair", si->imd->iname, rv);
			AS_SINDEX_RELEASE(si);
			return AS_SINDEX_ERR;
		}
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR_NOTFOUND;
}

int
as_sindex_stats_str(as_namespace *ns, as_sindex_metadata *imd, cf_dyn_buf *db)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;

	as_sindex *si = as_sindex_lookup_by_iname(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);

	if (!si)
		return AS_SINDEX_ERR_NOTFOUND;

	// A good thing to cache the stats first.
	int      ns_objects  = ns->n_objects;
	uint64_t si_objects  = cf_atomic64_get(si->stats.n_objects);
	uint64_t pending     = cf_atomic64_get(si->stats.recs_pending);
	uint64_t si_memory   = cf_atomic64_get(si->data_memory_used);
	// To protect the pimd while accessing it.
	SINDEX_RLOCK(&si->imd->slock);
	uint64_t n_keys      = ai_btree_get_numkeys(si->imd);
	SINDEX_UNLOCK(&si->imd->slock);
	cf_dyn_buf_append_string(db, "keys=");
	cf_dyn_buf_append_uint64(db,  n_keys);
	cf_dyn_buf_append_string(db, ";objects=");
	cf_dyn_buf_append_uint64(db,  si_objects);
	SINDEX_RLOCK(&si->imd->slock);
	uint64_t i_size      = ai_btree_get_isize(si->imd);
	uint64_t n_size      = ai_btree_get_nsize(si->imd);
	SINDEX_UNLOCK(&si->imd->slock);
	cf_dyn_buf_append_string(db, ";ibtr_memory_used=");
	cf_dyn_buf_append_uint64(db,  i_size);
	cf_dyn_buf_append_string(db, ";nbtr_memory_used=");
	cf_dyn_buf_append_uint64(db,  n_size);
	cf_dyn_buf_append_string(db, ";si_accounted_memory=");
	cf_dyn_buf_append_uint64(db,  si_memory);
	cf_dyn_buf_append_string(db, ";load_pct=");
	if (si->flag & AS_SINDEX_FLAG_RACTIVE) {
		cf_dyn_buf_append_string(db, "100");
	} else {
		if (pending > ns_objects || pending < 0) {
			cf_dyn_buf_append_uint64(db, 100);
		} else {
			cf_dyn_buf_append_uint64(db, (ns_objects == 0) ? 100 : 100 - ((100 * pending) / ns_objects));
		}
	}

	cf_dyn_buf_append_string(db, ";loadtime=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.loadtime));
	// writes
	cf_dyn_buf_append_string(db, ";stat_write_reqs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_writes));
	cf_dyn_buf_append_string(db, ";stat_write_success=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_writes) - cf_atomic64_get(si->stats.write_errs));
	cf_dyn_buf_append_string(db, ";stat_write_errs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.write_errs));
	// delete
	cf_dyn_buf_append_string(db, ";stat_delete_reqs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_deletes));
	cf_dyn_buf_append_string(db, ";stat_delete_success=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_deletes) - cf_atomic64_get(si->stats.delete_errs));
	cf_dyn_buf_append_string(db, ";stat_delete_errs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.delete_errs));
	// defrag
	cf_dyn_buf_append_string(db, ";stat_gc_recs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_defrag_records));
	cf_dyn_buf_append_string(db, ";stat_gc_time=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.defrag_time));

	// Cache values
	uint64_t agg        = cf_atomic64_get(si->stats.n_aggregation);
	uint64_t agg_rec    = cf_atomic64_get(si->stats.agg_num_records);
	uint64_t agg_size   = cf_atomic64_get(si->stats.agg_response_size);
	uint64_t lkup       = cf_atomic64_get(si->stats.n_lookup);
	uint64_t lkup_rec   = cf_atomic64_get(si->stats.lookup_num_records);
	uint64_t lkup_size  = cf_atomic64_get(si->stats.lookup_response_size);
	uint64_t query      = agg      + lkup;
	uint64_t query_rec  = agg_rec  + lkup_rec;
	uint64_t query_size = agg_size + lkup_size;

	// Query
	cf_dyn_buf_append_string(db, ";query_reqs=");
	cf_dyn_buf_append_uint64(db,   query);
	cf_dyn_buf_append_string(db, ";query_avg_rec_count=");
	cf_dyn_buf_append_uint64(db,   query     ? query_rec  / query     : 0);
	cf_dyn_buf_append_string(db, ";query_avg_record_size=");
	cf_dyn_buf_append_uint64(db,   query_rec ? query_size / query_rec : 0);
	// Aggregation
	cf_dyn_buf_append_string(db, ";query_agg=");
	cf_dyn_buf_append_uint64(db,   agg);
	cf_dyn_buf_append_string(db, ";query_agg_avg_rec_count=");
	cf_dyn_buf_append_uint64(db,   agg       ? agg_rec    / agg       : 0);
	cf_dyn_buf_append_string(db, ";query_agg_avg_record_size=");
	cf_dyn_buf_append_uint64(db,   agg_rec   ? agg_size   / agg_rec   : 0);
	//Lookup
	cf_dyn_buf_append_string(db, ";query_lookups=");
	cf_dyn_buf_append_uint64(db,   lkup);
	cf_dyn_buf_append_string(db, ";query_lookup_avg_rec_count=");
	cf_dyn_buf_append_uint64(db,   lkup      ? lkup_rec   / lkup      : 0);
	cf_dyn_buf_append_string(db, ";query_lookup_avg_record_size=");
	cf_dyn_buf_append_uint64(db,   lkup_rec  ? lkup_size  / lkup_rec  : 0);

	//CONFIG
	cf_dyn_buf_append_string(db, ";gc-period=");
	cf_dyn_buf_append_uint64(db, si->config.defrag_period);
	cf_dyn_buf_append_string(db, ";gc-max-units=");
	cf_dyn_buf_append_uint32(db, si->config.defrag_max_units);
	cf_dyn_buf_append_string(db, ";data-max-memory=");
	if (si->config.data_max_memory == ULONG_MAX) {
		cf_dyn_buf_append_uint64(db, si->config.data_max_memory);
	} else {
		cf_dyn_buf_append_string(db, "ULONG_MAX");
	}

	cf_dyn_buf_append_string(db, ";tracing=");
	cf_dyn_buf_append_uint64(db, si->trace_flag);
	cf_dyn_buf_append_string(db, ";histogram=");
	cf_dyn_buf_append_string(db, si->enable_histogram ? "true" : "false");
	cf_dyn_buf_append_string(db, ";ignore-not-sync=");
	cf_dyn_buf_append_string(db, (si->config.flag & AS_SINDEX_CONFIG_IGNORE_ON_DESYNC) ? "true" : "false");

	AS_SINDEX_RELEASE(si);
	// Release reference
	return AS_SINDEX_OK;
}

// NB:  These are distinctly different from the column names in AA!
static char *as_col_type_defs[] =
  { "NONE",      "NUMERIC",   "NUMERIC",   "STRING", "FLOAT", "UNDEFINED",
    "UNDEFINED", "UNDEFINED", "UNDEFINED", "UNDEFINED" };

/*
 * Client API to describe index based passed in imd, populates passed info fully
 */
int
as_sindex_describe_str(as_namespace *ns, as_sindex_metadata *imd, cf_dyn_buf *db)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;

	as_sindex *si = as_sindex_lookup_by_iname(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_NORESERVE);
	if ((si) && (si->imd)) {
		SINDEX_RLOCK(&si->imd->slock)
		as_sindex_metadata *imd = si->imd;
		cf_dyn_buf_append_string(db, "indexname=");
		cf_dyn_buf_append_string(db, imd->iname);
		cf_dyn_buf_append_string(db, ";ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_string(db, ";set=");
		cf_dyn_buf_append_string(db, imd->set == NULL ? "NULL" : imd->set);
		cf_dyn_buf_append_string(db, ";numbins=");
		cf_dyn_buf_append_uint64(db, imd->num_bins);
		cf_dyn_buf_append_string(db, ";bins=");
		for (int i = 0; i < imd->num_bins; i++) {
			if (i) cf_dyn_buf_append_string(db, ",");
			cf_dyn_buf_append_buf(db, (uint8_t *)imd->bnames[i], strlen(imd->bnames[i]));
			cf_dyn_buf_append_string(db, ":");
			// HACKY
			cf_dyn_buf_append_string(db, as_col_type_defs[as_sindex_pktype(imd)]);
		}

		// Index State
		if (si->state == AS_SINDEX_ACTIVE) {
			if (si->flag & AS_SINDEX_FLAG_RACTIVE) {
				cf_dyn_buf_append_string(db, ";state=RW");
			}
			else if (si->flag & AS_SINDEX_FLAG_WACTIVE) {
				cf_dyn_buf_append_string(db, ";state=WO");
			}
			else {
				cf_dyn_buf_append_string(db, ";state=A");
			}
		}
		else if (si->state == AS_SINDEX_INACTIVE) {
			cf_dyn_buf_append_string(db, ";state=I");
		}
		else {
			cf_dyn_buf_append_string(db, ";state=D");
		}
		cf_dyn_buf_append_string(db, ";path_str=");
		cf_dyn_buf_append_string(db, si->imd->path_str);
		SINDEX_UNLOCK(&si->imd->slock)
		AS_SINDEX_RELEASE(si);
		return AS_SINDEX_OK;
	} else {
		return AS_SINDEX_ERR_NOTFOUND;
	}
	return AS_SINDEX_OK;
}

/*
 * Client API to start namespace scan to populate secondary index. The scan
 * is only performed in the namespace is warm start or if its data is not in
 * memory and data is loaded from. For cold start with data in memory the indexes
 * are populate upfront.
 *
 * This call is only made at the boot time.
 */
int
as_sindex_boot_populateall()
{
	// Initialize the secondary index builder. The thread pool is initialized
	// with maximum threads to go full throttle, then down-sized to the
	// configured number after the startup population job is done.
	as_sbld_init();

	int ns_cnt = 0;

	// Trigger namespace scan to populate all secondary indexes
	// mark all secondary index for a namespace as populated
	for (int i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];
		if (!ns || (ns->sindex_cnt == 0)) {
			continue;
		}

		// If FAST START
		// OR (Data not in memory AND load data at startup)
		if (!ns->cold_start
			|| (!ns->storage_data_in_memory)) {
			as_sbld_build_all(ns);
			cf_info(AS_SINDEX, "Queuing namespace %s for sindex population ", ns->name);
		} else {
			as_sindex_boot_populateall_done(ns);
		}
		ns_cnt++;
	}
	for (int i = 0; i < ns_cnt; i++) {
		int ret;
		// blocking call, wait till an item is popped out of Q :
		cf_queue_pop(g_sindex_populateall_done_q, &ret, CF_QUEUE_FOREVER);
		// TODO: Check for failure .. is generally fatal if it fails
	}

	// Down-size builder thread pool to configured value.
	as_sbld_resize_thread_pool(g_config.sindex_builder_threads);

	g_sindex_boot_done = true;

	// This above flag indicates that the basic sindex boot-up loader is done
	// Go and destroy the sindex_cfg_var_hash here to prevent run-time
	// si's from getting the config-file settings.
	for (int i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (ns->sindex_cfg_var_hash) {
			shash_reduce(ns->sindex_cfg_var_hash, as_sindex_cfg_var_hash_reduce_fn, NULL);
	    	shash_destroy(ns->sindex_cfg_var_hash);

	    	// Assign hash to NULL at the start and end of its lifetime
			ns->sindex_cfg_var_hash = NULL;
		}

	}

	return AS_SINDEX_OK;
}

/*
 * Client API to mark all the indexes in namespace populated and ready for read
 */
int
as_sindex_boot_populateall_done(as_namespace *ns)
{
	SINDEX_GWLOCK();
	int ret = AS_SINDEX_OK;

	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		if (!as_sindex_isactive(si))  continue;
		// This sindex is getting populating by it self scan
		if (si->flag & AS_SINDEX_FLAG_POPULATING) continue;
		si->flag |= AS_SINDEX_FLAG_RACTIVE;
	}
	SINDEX_GUNLOCK();
	cf_queue_push(g_sindex_populateall_done_q, &ret);
	cf_info(AS_SINDEX, "Namespace %s sindex population done", ns->name);
	return ret;
}

/*
 * Client API to check if there is secondary index on given namespace
 */
int
as_sindex_ns_has_sindex(as_namespace *ns)
{
	return (ns->sindex_cnt > 0);
}

char *as_sindex_type_defs[] =
{	"NONE", "LIST", "MAPKEYS", "MAPVALUES"
};

/*
 * Client API to list all the indexes in a namespace, returns list of imd with
 * index information, Caller should free it up
 */
int
as_sindex_list_str(as_namespace *ns, cf_dyn_buf *db)
{
	SINDEX_GRLOCK();
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		if (&(ns->sindex[i]) && (ns->sindex[i].imd)) {
			as_sindex si = ns->sindex[i];
			if (as_sindex_isactive(&si)) {
				AS_SINDEX_RESERVE(&si);
				SINDEX_RLOCK(&si.imd->slock);
			}
			cf_dyn_buf_append_string(db, "ns=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, ":set=");
			cf_dyn_buf_append_string(db, (si.imd->set) ? si.imd->set : "NULL");
			cf_dyn_buf_append_string(db, ":indexname=");
			cf_dyn_buf_append_string(db, si.imd->iname);
			cf_dyn_buf_append_string(db, ":num_bins=");
			cf_dyn_buf_append_uint64(db, si.imd->num_bins);
			cf_dyn_buf_append_string(db, ":bins=");
			for (int i = 0; i < si.imd->num_bins; i++) {
				if (i) cf_dyn_buf_append_string(db, ",");
				cf_dyn_buf_append_buf(db, (uint8_t *)si.imd->bnames[i], strlen(si.imd->bnames[i]));
				cf_dyn_buf_append_string(db, ":type=");
				cf_dyn_buf_append_string(db, as_sindex_ktype_str(si.imd->btype[0]));
				cf_dyn_buf_append_string(db, ":indextype=");
				cf_dyn_buf_append_string(db, as_sindex_type_defs[si.imd->itype]);

			}
			cf_dyn_buf_append_string(db, ":path=");
			cf_dyn_buf_append_string(db, si.imd->path_str);
			cf_dyn_buf_append_string(db, ":sync_state=");
			if (si.desync_cnt > 0) {
				cf_dyn_buf_append_string(db, "needsync");
			}
			else {
				cf_dyn_buf_append_string(db, "synced");
			}
			// Index State
			if (si.state == AS_SINDEX_ACTIVE) {
				if (si.flag & AS_SINDEX_FLAG_RACTIVE) {
					cf_dyn_buf_append_string(db, ":state=RW;");
				}
				else if (si.flag & AS_SINDEX_FLAG_WACTIVE) {
					cf_dyn_buf_append_string(db, ":state=WO;");
				}
				else {
					// should never come here.
					cf_dyn_buf_append_string(db, ":state=A;");
				}
			}
			else if (si.state == AS_SINDEX_INACTIVE) {
				cf_dyn_buf_append_string(db, ":state=I;");
			}
			else {
				cf_dyn_buf_append_string(db, ":state=D;");
			}
			if (as_sindex_isactive(&si)) {
				SINDEX_UNLOCK(&si.imd->slock);
				AS_SINDEX_RELEASE(&si);
			}
		}
	}
	SINDEX_GUNLOCK();
	return AS_SINDEX_OK;
}

// Needs comments
int
as_sindex_update_by_sbin(as_namespace *ns, const char *set, as_sindex_bin *start_sbin, int num_sbins, cf_digest * pkey)
{
	cf_debug(AS_SINDEX, "as_sindex_update_by_sbin");

	int sindex_ret = AS_SINDEX_OK;
	for (int i=0; i<num_sbins; i++) {
		if (start_sbin[i].op == AS_SINDEX_OP_DELETE) {
			sindex_ret = as_sindex__op_by_sbin(ns, set, 1, &start_sbin[i], pkey);
		}
	}
	for (int i=0; i<num_sbins; i++) {
		if (start_sbin[i].op == AS_SINDEX_OP_INSERT) {
			sindex_ret = as_sindex__op_by_sbin(ns, set, 1, &start_sbin[i], pkey);
		}
	}
	return sindex_ret;
}

/*
 * Returns -
 * 		NULL - On failure
 * 		si   - On success.
 * Notes -
 * 		Reserves the si if found in the srange
 * 		Releases the si if imd is null or bin type is mis matched.
 *
 */
as_sindex *
as_sindex_from_range(as_namespace *ns, char *set, as_sindex_range *srange)
{
	cf_debug(AS_SINDEX, "as_sindex_from_range");
	if (ns->single_bin) return NULL;
	as_sindex *si = as_sindex_lookup_by_defns(ns, set, srange->start.id,  
						as_sindex_sktype_from_pktype(srange->start.type), srange->itype, srange->bin_path, 
						AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (si && si->imd) {
		// Do the type check
		as_sindex_metadata *imd = si->imd;
		for (int i = 0; i < imd->num_bins; i++) {
			if ((imd->binid[i] == srange->start.id)
					&& (srange->start.type !=
						as_sindex_pktype(imd))) {
				cf_warning(AS_SINDEX, "Query and Index Bin Type Mismatch: "
						"[binid %d : Index Bin type %d : Query Bin Type %d]",
						imd->binid[i], as_sindex_pktype(imd), srange->start.type );
				AS_SINDEX_RELEASE(si);
				return NULL;
			}
		}
	}
	return si;
}

/*
 * The way to filter out imd information from the as_msg which is primarily
 * query with all the details. For the normal operations the imd is formed out
 * of the as_op.
 */
/*
 * Returns -
 * 		NULL      - On failure.
 * 		as_sindex - On success.
 *
 * Description -
 * 		Firstly obtains the simatch using ns name and set name.
 * 		Then returns the corresponding slot from sindex array.
 *
 * TODO
 * 		log messages
 */
as_sindex *
as_sindex_from_msg(as_namespace *ns, as_msg *msgp)
{
	cf_debug(AS_SINDEX, "as_sindex_from_msg");
	as_msg_field *ifp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_NAME);
	as_msg_field *sfp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_SET);

	if (!ifp) {
		cf_debug(AS_SINDEX, "Index name not found in the query request");
		return NULL;
	}

	char *setname = NULL;
	char *iname   = NULL;

	if (sfp) {
		setname   = cf_strndup((const char *)sfp->data, as_msg_field_get_value_sz(sfp));
	}
	iname         = cf_strndup((const char *)ifp->data, as_msg_field_get_value_sz(ifp));
	
	as_sindex *si = as_sindex_lookup_by_iname(ns, iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		cf_detail(AS_SINDEX, "Search did not find index ");
	}

	if (sfp)   cf_free(setname);
	if (iname) cf_free(iname);
	return si;
}


/*
 * Internal Function - as_sindex_range_free
 * 		frees the sindex range
 *
 * Returns
 * 		AS_SINDEX_OK - In every case
 */
int
as_sindex_range_free(as_sindex_range **range)
{
	cf_debug(AS_SINDEX, "as_sindex_range_free");
	as_sindex_range *sk = (*range);
//	as_sindex_sbin_freeall(&sk->start, sk->num_binval);
//	as_sindex_sbin_freeall(&sk->end, sk->num_binval);
	cf_free(sk);
	return AS_SINDEX_OK;
}

/*
 * Extract out range information from the as_msg and create the irange structure
 * if required allocates the memory.
 * NB: It is responsibility of caller to call the cleanup routine to clean the
 * range structure up and free up its memory
 *
 * query range field layout: contains - numranges, binname, start, end
 *
 * generic field header
 * 0   4 size = size of data only
 * 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
 *
 * numranges
 * 5   1 numranges (max 255 ranges)
 *
 * binname
 * 6   1 binnamelen b
 * 7   b binname
 *
 * particle (start & end)
 * +b    1 particle_type
 * +b+1  4 start_particle_size x
 * +b+5  x start_particle_data
 * +b+5+x      4 end_particle_size y
 * +b+5+x+y+4   y end_particle_data
 *
 * repeat "numranges" times from "binname"
 */


/*
 * Function as_sindex_assert_query
 * Returns -
 * 		Return value of as_sindex__pre_op_assert
 */
int
as_sindex_assert_query(as_sindex *si, as_sindex_range *range)
{
	return as_sindex__pre_op_assert(si, AS_SINDEX_OP_READ);
}

/*
 * Function as_sindex_binlist_from_msg
 *
 * Returns -
 * 		binlist - On success
 * 		NULL    - On failure
 *
 */
cf_vector *
as_sindex_binlist_from_msg(as_namespace *ns, as_msg *msgp, int * num_bins)
{
	cf_debug(AS_SINDEX, "as_sindex_binlist_from_msg");
	as_msg_field *bfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_QUERY_BINLIST);
	if (!bfp) {
		return NULL;
	}
	const uint8_t *data = bfp->data;
	int numbins         = *data++;
	*num_bins           = numbins;

	cf_vector *binlist  = cf_vector_create(AS_ID_BIN_SZ, numbins, 0);

	for (int i = 0; i < numbins; i++) {
		int binnamesz = *data++;
		if (binnamesz <= 0 || binnamesz > AS_ID_BIN_SZ - 1) {
			cf_warning(AS_SINDEX, "Size of the bin name in bin list of sindex query is out of bounds. Size %d", binnamesz);
			cf_vector_destroy(binlist);
			return NULL;
		}
		char binname[AS_ID_BIN_SZ];
		memcpy(&binname, data, binnamesz);
		binname[binnamesz] = 0;
		cf_vector_set(binlist, i, (void *)binname);
		data     += binnamesz;
	}

	cf_debug(AS_SINDEX, "Queried Bin List %d ", numbins);
	for (int i = 0; i < cf_vector_size(binlist); i++) {
		char binname[AS_ID_BIN_SZ];
		cf_vector_get(binlist, i, (void*)&binname);
		cf_debug(AS_SINDEX,  " String Queried is |%s| \n", binname);
	}

	return binlist;
}

/*
 * Returns -
 *		AS_SINDEX_OK        - On success.
 *		AS_SINDEX_ERR_PARAM - On failure.
 *		AS_SINDEX_ERR_OTHER - On failure.
 *
 * Description -
 *		Frames a sane as_sindex_range from msg.
 *
 *		We are not supporting multiranges right now. So numrange is always expected to be 1.
 */
int
as_sindex_range_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range *srange)
{
	cf_debug(AS_SINDEX, "as_sindex_range_from_msg");
	srange->num_binval = 0;
	// getting ranges
	as_msg_field *itype_fp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_TYPE);
	as_msg_field *rfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_RANGE);
	if (!rfp) {
		cf_warning(AS_SINDEX, "Required Index Range Not Found");
		return AS_SINDEX_ERR_PARAM;
	}
	const uint8_t *data = rfp->data;
	int numrange        = *data++;

	if (numrange != 1) {
		cf_warning(AS_SINDEX,
					"can't handle multiple ranges right now %d", rfp->data[0]);
		return AS_SINDEX_ERR_PARAM;
	}
	memset(srange, 0, sizeof(as_sindex_range));
	if (itype_fp) {
		srange->itype = *itype_fp->data;
	}
	else {
		srange->itype = AS_SINDEX_ITYPE_DEFAULT;
	}
	for (int i = 0; i < numrange; i++) {
		as_sindex_bin_data *start = &(srange->start);
		as_sindex_bin_data *end   = &(srange->end);
		// Populate Bin id
		uint8_t bin_path_len         = *data++;
		if (bin_path_len >= AS_SINDEX_MAX_PATH_LENGTH) {
			cf_warning(AS_SINDEX, "Index position size %d exceeds the max length %d", bin_path_len, AS_SINDEX_MAX_PATH_LENGTH);
			return AS_SINDEX_ERR_PARAM;
		}

		strncpy(srange->bin_path, (char *)data, bin_path_len);
		srange->bin_path[bin_path_len] = '\0';

		char binname[AS_ID_BIN_SZ];
		if (as_sindex_extract_bin_from_path(srange->bin_path, binname) == AS_SINDEX_OK) {
			int16_t id = as_bin_get_id(ns, binname);
			if (id != -1) {
				start->id   = id;
				end->id     = id;
			} else {
				return AS_SINDEX_ERR_BIN_NOTFOUND;
			}
		}
		else {
			return AS_SINDEX_ERR_PARAM;
		}

		data       += bin_path_len;

		// Populate type
		int type    = *data++;
		start->type = type;
		end->type   = start->type;
	
		if ((type == AS_PARTICLE_TYPE_INTEGER)) {
			// get start point
			uint32_t startl  = ntohl(*((uint32_t *)data));
			data            += sizeof(uint32_t);
			if (startl != 8) {
				cf_warning(AS_SINDEX,
					"Can only handle 8 byte numerics right now %ld", startl);
				goto Cleanup;
			}
			start->u.i64  = __cpu_to_be64(*((uint64_t *)data));
			data         += sizeof(uint64_t);

			// get end point
			uint32_t endl = ntohl(*((uint32_t *)data));
			data         += sizeof(uint32_t);
			if (endl != 8) {
				cf_warning(AS_SINDEX,
						"can only handle 8 byte numerics right now %ld", endl);
				goto Cleanup;
			}
			end->u.i64  = __cpu_to_be64(*((uint64_t *)data));
			data       += sizeof(uint64_t);
			if (start->u.i64 > end->u.i64) {
				cf_warning(AS_SINDEX,
                     "Invalid range from %ld to %ld", start->u.i64, end->u.i64);
				goto Cleanup;
			} else if (start->u.i64 == end->u.i64) {
				srange->isrange = FALSE;
			} else {
				srange->isrange = TRUE;
			}
			cf_debug(AS_SINDEX, "Range is equal %d,%d",
								start->u.i64, end->u.i64);
		} else if (type == AS_PARTICLE_TYPE_STRING) {
			// get start point
			uint32_t startl    = ntohl(*((uint32_t *)data));
			data              += sizeof(uint32_t);
			char* start_binval       = (char *)data;
			data              += startl;
			srange->isrange    = FALSE;

			if ((startl <= 0) || (startl >= AS_SINDEX_MAX_STRING_KSIZE)) {
				cf_warning(AS_SINDEX, "Out of bound query key size %ld", startl);
				goto Cleanup;
			}
			uint32_t endl	   = ntohl(*((uint32_t *)data));
			data              += sizeof(uint32_t);
			char * end_binval        = (char *)data;
			if (startl != endl && strncmp(start_binval, end_binval, startl)) {
				cf_warning(AS_SINDEX,
                           "Only Equality Query Supported in Strings %s-%s",
                           start_binval, end_binval);
				goto Cleanup;
			}
			cf_digest_compute(start_binval, startl, &(start->digest));
			cf_debug(AS_SINDEX, "Range is equal %s ,%s",
                               start_binval, end_binval);
		} else {
			cf_warning(AS_SINDEX, "Only handle String and Numeric type");
			goto Cleanup;
		}
		srange->num_binval = numrange;
	}
	return AS_SINDEX_OK;

Cleanup:
	return AS_SINDEX_ERR;
}

/*
 * Function as_sindex_rangep_from_msg
 *
 * Arguments
 * 		ns     - the namespace on which srange has to be build
 * 		msgp   - the msgp from which sent
 * 		srange - it builds this srange
 *
 * Returns
 * 		AS_SINDEX_OK - On success
 * 		else the return value of as_sindex_range_from_msg
 *
 * Description
 * 		Allocating space for srange and then calling as_sindex_range_from_msg.
 */
int
as_sindex_rangep_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range **srange)
{
	cf_debug(AS_SINDEX, "as_sindex_rangep_from_msg");
	*srange         = cf_malloc(sizeof(as_sindex_range));
	if (!(*srange)) {
		cf_warning(AS_SINDEX,
                 "Could not Allocate memory for range key. Aborting Query ...");
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	int ret = as_sindex_range_from_msg(ns, msgp, *srange);
	if (AS_SINDEX_OK != ret) {
		as_sindex_range_free(srange);
		*srange = NULL;
		return ret;
	}
	return AS_SINDEX_OK;
}


void
as_sindex_init_sbin(as_sindex_bin * sbin, as_sindex_op op, as_particle_type type, int simatch)
{
	sbin->simatch         = simatch;
	sbin->to_free         = false;
	sbin->num_values      = 0;
	sbin->op              = op;
	sbin->heap_capacity   = 0;
	sbin->type            = type;
	sbin->values          = NULL;
}

as_sindex_status
as_sindex_add_sbin_value_in_heap(as_sindex_bin * sbin, void * val)
{
	// Get the size of the data we are going to store
	// If to_free = false, this means this is the first 
	// time we are storing value for this sbin to heap
	// Check if there is need to copy the existing data from stack_buf
	// 		init_storage(num_values)
	// 		If num_values != 0
	//			Copy the existing data from stack to heap
	//			reduce the used stack_buf size
	// 		to_free = true;
	// 	Else
	// 		If (num_values == heap_capacity) 
	// 			extend the allocation and capacity
	// 	Copy the value to the appropriate position.

	uint32_t   size = 0;
	bool    to_copy = false;
	uint8_t    data_sz = 0;
	void * tmp_value = NULL;
	sbin_value_pool * stack_buf = sbin->stack_buf;

	// Get the size of the data we are going to store
	if (sbin->type == AS_PARTICLE_TYPE_INTEGER) {
		data_sz = sizeof(uint64_t);
	}
	else if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		data_sz = sizeof(cf_digest);
	}
	else {
		cf_warning(AS_SINDEX, "Bad type of data to index %d", sbin->type);
		return AS_SINDEX_ERR;
	}

	// If to_free = false, this means this is the first 
	// time we are storing value for this sbin to heap
	// Check if there is need to copy the existing data from stack_buf
	if (!sbin->to_free) {
		if (sbin->num_values == 0) {
			size = 2;
		}
		else if (sbin->num_values > 0) {
			to_copy = true;
			size = 2 * sbin->num_values;
			tmp_value = sbin->values;
		}
		else {
			return AS_SINDEX_ERR;
		}

		sbin->values  = cf_malloc(data_sz * size);
		if (!sbin->values) {
			cf_warning(AS_SINDEX, "malloc failed");
			return AS_SINDEX_ERR;
		}
		sbin->to_free = true;
		sbin->heap_capacity = size;

	//			Copy the existing data from stack to heap
	//			reduce the used stack_buf size
		if (to_copy) {
			if (!memcpy(sbin->values, tmp_value, data_sz * sbin->num_values)) {
				cf_warning(AS_SINDEX, "memcpy failed");
				return AS_SINDEX_ERR;
			}
			stack_buf->used_sz -= (sbin->num_values * data_sz);
		}
	}
	else
	{
	// 	Else
	// 		If (num_values == heap_capacity) 
	// 			extend the allocation and capacity
		if (sbin->heap_capacity ==  sbin->num_values) {
			sbin->heap_capacity = 2 * sbin->heap_capacity;
			sbin->values = cf_realloc(sbin->values, sbin->heap_capacity * data_sz);
			if (!sbin->values) {
				cf_warning(AS_SINDEX, "Realloc failed for size %d", sbin->heap_capacity * data_sz);
				sbin->heap_capacity = sbin->heap_capacity / 2;
				return AS_SINDEX_ERR;
			}
		}
	}
	
	// 	Copy the value to the appropriate position.
	if (sbin->type == AS_PARTICLE_TYPE_INTEGER) {
		if (!memcpy((void *)((uint64_t *)sbin->values + sbin->num_values), (void *)val, data_sz)) {
			cf_warning(AS_SINDEX, "memcpy failed");
			return AS_SINDEX_ERR;
		}
	}
	else if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		if (!memcpy((void *)((cf_digest *)sbin->values + sbin->num_values), (void *)val, data_sz)) {
			cf_warning(AS_SINDEX, "memcpy failed");
			return AS_SINDEX_ERR;
		}
	}
	else {
		cf_warning(AS_SINDEX, "Bad type of data to index %d", sbin->type);
		return AS_SINDEX_ERR;
	}

	sbin->num_values++;
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_integer_to_sbin(as_sindex_bin * sbin, uint64_t val)
{
	// If this is the first value coming to the sbin
	// 		assign the value to the local variable of struct.
	// Else
	// 		If to_free is true or stack_buf is full
	// 			add value to the heap
	// 		else 
	// 			If needed copy the values stored in sbin to stack_buf
	// 			add the value to end of stack buf
	
	sbin_value_pool * stack_buf = sbin->stack_buf;
	// If this is the first value coming to the sbin
	// 		assign the value to the local variable of struct.
	if (sbin->num_values == 0 ) {
		sbin->value.int_val = val;
		sbin->num_values++;
	}
	else if (sbin->num_values > 0) {
	
	// Else
	// 		If to_free is true or stack_buf is full
	// 			add value to the heap
		if (sbin->to_free || (stack_buf->used_sz + sizeof(uint64_t)) > AS_SINDEX_VALUESZ_ON_STACK ) {
			if (as_sindex_add_sbin_value_in_heap(sbin, (void *)&val)) {
				cf_warning(AS_SINDEX, "Adding value in sbin failed.");
				return AS_SINDEX_ERR;
			}
		}
		else {
	// 		else 
	//			If needed copy the values stored in sbin to stack_buf
			if (sbin->num_values == 1) {
				sbin->values = stack_buf->value + stack_buf->used_sz;
				(* (uint64_t *)sbin->values ) = sbin->value.int_val;
				stack_buf->used_sz += sizeof(uint64_t);
			}

	// 			add the value to end of stack buf
			*((uint64_t *)sbin->values + sbin->num_values) = val;
			sbin->num_values++;
			stack_buf->used_sz += sizeof(uint64_t);
		}
	}
	else {
		cf_warning(AS_SINDEX, "numvalues is coming as negative. Possible memory corruption in sbin.");
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_digest_to_sbin(as_sindex_bin * sbin, cf_digest val_dig)
{
	// If this is the first value coming to the sbin
	// 		assign the value to the local variable of struct.
	// Else
	// 		If to_free is true or stack_buf is full
	// 			add value to the heap
	// 		else 
	// 			If needed copy the values stored in sbin to stack_buf
	// 			add the value to end of stack buf

	sbin_value_pool * stack_buf = sbin->stack_buf;
	// If this is the first value coming to the sbin
	// 		assign the value to the local variable of struct.	
	if (sbin->num_values == 0 ) {
		sbin->value.str_val = val_dig;
		sbin->num_values++;
	}
	else if (sbin->num_values > 0) {
	
	// Else
	// 		If to_free is true or stack_buf is full
	// 			add value to the heap
		if (sbin->to_free || (stack_buf->used_sz + sizeof(cf_digest)) > AS_SINDEX_VALUESZ_ON_STACK ) {
			if (as_sindex_add_sbin_value_in_heap(sbin, (void *)&val_dig)) {
				cf_warning(AS_SINDEX, "Adding value in sbin failed.");
				return AS_SINDEX_ERR;
			}
		}
		else {
	// 		else 
	//			If needed copy the values stored in sbin to stack_buf
			if (sbin->num_values == 1) {
				sbin->values = stack_buf->value + stack_buf->used_sz;
				if (!memcpy(sbin->values, (void *)&sbin->value.str_val, sizeof(cf_digest))) {
					cf_warning(AS_SINDEX, "Memcpy failed");
					return AS_SINDEX_ERR;
				}
				stack_buf->used_sz += sizeof(cf_digest);
			}

	// 			add the value to end of stack buf
			if (!memcpy((void *)((cf_digest *)sbin->values + sbin->num_values), (void *)&val_dig, sizeof(cf_digest))) {
				cf_warning(AS_SINDEX, "Memcpy failed");
				return AS_SINDEX_ERR;
			}
			sbin->num_values++;
			stack_buf->used_sz += sizeof(cf_digest);
		}
	}
	else {
		cf_warning(AS_SINDEX, "numvalues is coming as negative. Possible memory corruption in sbin.");
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_string_to_sbin(as_sindex_bin * sbin, char * val)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	// Calculate digest and cal add_digest_to_sbin	
	cf_digest val_dig;
	cf_digest_compute(val, strlen(val), &val_dig);
	return as_sindex_add_digest_to_sbin(sbin, val_dig);
}

as_sindex_status
as_sindex_add_long_from_asval(as_val *val, as_sindex_bin *sbin)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (sbin->type != AS_PARTICLE_TYPE_INTEGER) {
		return AS_SINDEX_ERR;
	}
	
	as_integer *i = as_integer_fromval(val);
	if (!i) {
		return AS_SINDEX_ERR;
	}
	uint64_t int_val = (uint64_t)as_integer_get(i);
	return as_sindex_add_integer_to_sbin(sbin, int_val);
}

as_sindex_status
as_sindex_add_digest_from_asval(as_val *val, as_sindex_bin *sbin)
{
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (sbin->type != AS_PARTICLE_TYPE_STRING) {
		return AS_SINDEX_ERR;
	}

	as_string *s = as_string_fromval(val);
	if (!s) {
		return AS_SINDEX_ERR;
	}
	char * str_val = as_string_get(s);
	return as_sindex_add_string_to_sbin(sbin, str_val);
}

typedef as_sindex_status (*as_sindex_add_keytype_from_asval_fn)
(as_val *val, as_sindex_bin * sbin);
static const as_sindex_add_keytype_from_asval_fn 
			 as_sindex_add_keytype_from_asval[AS_SINDEX_KEY_TYPE_MAX] = {
	as_sindex_add_long_from_asval,
	as_sindex_add_digest_from_asval
};


as_sindex_status
as_sindex_add_asval_to_default_sindex(as_val *val, as_sindex_bin * sbin)
{
	return as_sindex_add_keytype_from_asval[as_sindex_key_type_from_pktype(sbin->type)](val, sbin);
}

typedef struct as_sindex_cdt_sbin_s {
	as_particle_type    type;
	as_sindex_bin * sbin;
} as_sindex_cdt_sbin;

static bool as_sindex_add_listvalues_foreach(as_val * element, void * udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_add_keytype_from_asval[as_sindex_key_type_from_pktype(sbin->type)](element, sbin);
	return true;
}

as_sindex_status
as_sindex_add_asval_to_list_sindex(as_val *val, as_sindex_bin * sbin)
{
	// If val type is not AS_LIST 
	// 		return AS_SINDEX_ERR
	// Else iterate through all values of list 
	// 		If type == AS_PARTICLE_TYPE_STRING
	// 			add all string type values to the sbin
	// 		If type == AS_PARTICLE_TYPE_INTEGER
	// 			add all integer type values to the sbin
	
	// If val type is not AS_LIST
	// 		return AS_SINDEX_ERR
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (val->type != AS_LIST) {
		return AS_SINDEX_ERR;
	}
	// Else iterate through all elements of map
	as_list * list               = as_list_fromval(val);
	if (as_list_foreach(list, as_sindex_add_listvalues_foreach, sbin)) {
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR;
}

static bool as_sindex_add_mapkeys_foreach(const as_val * key, const as_val * val, void * udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_add_keytype_from_asval[as_sindex_key_type_from_pktype(sbin->type)]((as_val *)key, sbin);
	return true;
}

static bool as_sindex_add_mapvalues_foreach(const as_val * key, const as_val * val, void * udata)
{
	as_sindex_bin * sbin = (as_sindex_bin *)udata;
	as_sindex_add_keytype_from_asval[as_sindex_key_type_from_pktype(sbin->type)]((as_val *)val, sbin);
	return true;
}

as_sindex_status	
as_sindex_add_asval_to_mapkeys_sindex(as_val *val, as_sindex_bin * sbin)
{
	// If val type is not AS_MAP
	// 		return AS_SINDEX_ERR
	// 		Defensive check. Should not happen.
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (val->type != AS_MAP) {
		cf_warning(AS_SINDEX, "Unexpected wrong type %d", val->type);
		return AS_SINDEX_ERR;
	}

	// Else iterate through all keys of map
	as_map * map                   = as_map_fromval(val);
	if (as_map_foreach(map, as_sindex_add_mapkeys_foreach, sbin)) {
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR;
}

as_sindex_status
as_sindex_add_asval_to_mapvalues_sindex(as_val *val, as_sindex_bin * sbin)
{
	// If val type is not AS_MAP
	// 		return AS_SINDEX_ERR
	// Else iterate through all values of all keys of the map
	// 		If type == AS_PARTICLE_TYPE_STRING
	// 			add all string type values to the sbin
	// 		If type == AS_PARTICLE_TYPE_INTEGER
	// 			add all integer type values to the sbin
	
	// If val type is not AS_MAP
	// 		return AS_SINDEX_ERR
	if (!val) {
		return AS_SINDEX_ERR;
	}
	if (val->type != AS_MAP) {
		return AS_SINDEX_ERR;
	}
	// Else iterate through all keys, values of map
	as_map * map                  = as_map_fromval(val);
	if (as_map_foreach(map, as_sindex_add_mapvalues_foreach, sbin)) {
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR;
}

typedef as_sindex_status (*as_sindex_add_asval_to_itype_sindex_fn)
(as_val *val, as_sindex_bin * sbin);
static const as_sindex_add_asval_to_itype_sindex_fn 
			 as_sindex_add_asval_to_itype_sindex[AS_SINDEX_ITYPE_MAX] = {
	as_sindex_add_asval_to_default_sindex,
	as_sindex_add_asval_to_list_sindex,
	as_sindex_add_asval_to_mapkeys_sindex,
	as_sindex_add_asval_to_mapvalues_sindex
};

as_sindex_status
as_sindex_add_diff_asval_to_default_sindex(as_val * old_val, as_val *new_val, as_sindex_bin * sbin, int * found)
{
	// If both old val and new val have the same expected type
	// Then compare the values and add it to the sbin accodingly.
	// Else add them separately to different bins if possible.

	int simatch = sbin->simatch;
	as_val_t expected_type = AS_UNDEF;
	as_particle_type type = sbin->type;
	if (type == AS_PARTICLE_TYPE_STRING) {
		expected_type = AS_STRING;
	}
	else if (type == AS_PARTICLE_TYPE_INTEGER) {
		expected_type = AS_INTEGER;
	}
	else {
		return AS_SINDEX_OK;
	}

	// If both old val and new val have the same expected type
	// Then compare the values and add it to the sbin accodingly.
	if (old_val && new_val) {
		if (old_val->type == expected_type && new_val->type == expected_type) {
			if (type == AS_PARTICLE_TYPE_STRING) {
				as_string *old_s = as_string_fromval(old_val);
				as_string *new_s = as_string_fromval(new_val);
				if (!old_s || !new_s) {
					return AS_SINDEX_OK;
				}
				char * old_str = as_string_get(old_s);
				char * new_str = as_string_get(new_s);

				if (strlen(old_str) != strlen(new_str)) {
					if (memcmp(old_str, new_str, strlen(old_str) + 1)) {
						sbin->op = AS_SINDEX_OP_DELETE;
						if (as_sindex_add_string_to_sbin(sbin, old_str) == AS_SINDEX_OK) {
							sbin++;
							as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
							*found += 1;
						}
						if (as_sindex_add_string_to_sbin(sbin, new_str) == AS_SINDEX_OK) {
							*found += 1;
						}
					}
				}
			}
			else if (type == AS_PARTICLE_TYPE_INTEGER) {
				as_integer *old_i = as_integer_fromval(old_val);
				as_integer *new_i = as_integer_fromval(new_val);
				if (!old_i || !new_i) {
					return AS_SINDEX_OK;
				}
				uint64_t old_int = as_integer_get(old_i);
				uint64_t new_int = as_integer_get(new_i);

				if (old_int != new_int) {
					sbin->op = AS_SINDEX_OP_DELETE;
					if (as_sindex_add_integer_to_sbin(sbin, old_int) == AS_SINDEX_OK) {
						sbin++;
						as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
						*found += 1;
					}
					if (as_sindex_add_integer_to_sbin(sbin, new_int) == AS_SINDEX_OK) {
						*found += 1;
					}
				}

			}
			return AS_SINDEX_OK;
		}
	}

	// Else add them separately to different bins if possible.
	as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, type, simatch);
	if (as_sindex_add_keytype_from_asval[as_sindex_key_type_from_pktype(sbin->type)](old_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found     += 1;
			sbin        = sbin + *found;
		}
	}

	as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
	if (as_sindex_add_keytype_from_asval[as_sindex_key_type_from_pktype(sbin->type)](new_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found += 1;	
		}
	}
	return AS_SINDEX_OK;
}

#define AS_SINDEX_SBIN_HASH_SZ 256

typedef struct as_sindex_sbin_value_hash_s {
	shash              * value_hash;
	as_particle_type     type;
	as_sindex_bin  * sbin;
} as_sindex_sbin_value_hash;


static inline uint32_t
as_sindex_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, sizeof(uint32_t));
}

static bool as_sindex_list_to_hash(as_val * element, void * udata)
{
	// If the type and val type matches,
	// 		Add them to the hash
	as_sindex_sbin_value_hash * list_hash = (as_sindex_sbin_value_hash *)udata;
	shash * h                             =  list_hash->value_hash;

	if (list_hash->type == AS_PARTICLE_TYPE_STRING) {
		if (element->type == AS_STRING) {
			char * str_val = as_string_get(as_string_fromval(element));
			bool value     = true;

			// Compute the digest as a fix sized key of the hash.
			cf_digest str_val_dig;
			cf_digest_compute(str_val, strlen(str_val), &str_val_dig);
			
			if (shash_put(h, &str_val_dig, &value) != SHASH_OK) {
				cf_debug(AS_SINDEX, "shash put failed");
				return false;
			}
		}
	}
	else if (list_hash->type == AS_PARTICLE_TYPE_INTEGER) {
		if (element->type == AS_INTEGER) {
			uint64_t int_val = as_integer_get(as_integer_fromval(element));
			bool value = true;
			if (shash_put(h, &int_val, &value) != SHASH_OK) {
				cf_debug(AS_SINDEX, "shash put failed");
				return false;
			}
		}
	}
	return true;
}

static bool as_sindex_compare_list_hash(as_val * element, void * udata)
{
	// If the type and val type matches
	// 		check if the value exist in the hash.
	// 		If does not exist, add it to the sbin
	as_sindex_sbin_value_hash * list_comp_add = (as_sindex_sbin_value_hash *)udata;
	shash * h                               =  list_comp_add->value_hash;


	if (list_comp_add->type == AS_PARTICLE_TYPE_STRING) {
		if (element->type == AS_STRING) {
			char * str_val = as_string_get(as_string_fromval(element));
			bool value;
			// Compute the digest as a fix sized key of the hash.
			cf_digest str_val_dig;
			cf_digest_compute(str_val, strlen(str_val), &str_val_dig);

			if (shash_get(h, &str_val_dig, &value) != SHASH_OK) {
				as_sindex_add_digest_to_sbin(list_comp_add->sbin, str_val_dig);
			}
		}
	}
	else if (list_comp_add->type == AS_PARTICLE_TYPE_INTEGER) {
		if (element->type == AS_INTEGER) {
			uint64_t int_val = as_integer_get(as_integer_fromval(element));
			bool value;
			if (shash_get(h, &int_val, &value) != SHASH_OK) {
				as_sindex_add_integer_to_sbin(list_comp_add->sbin, int_val);
			}
		}
	}
	return true;
}

as_sindex_status
as_sindex_add_diff_asval_to_list_sindex(as_val * old_val, as_val *new_val, as_sindex_bin * sbin, int * found)
{
	// If both old val and new val have the same expected type
	//		Add all old values of type "type" to a old hash
	//		Add all new values of type "type" to a new hash
	//		Iterate through all the values in the old and check it exist or not in the new hash
	//		If it does not exist add it to the sbin with OP DELETE
	//		Iterate through all the values in the old and check it exist or not in the old hash 
	//		If it does not exist add it to the sbin with OP INSERT
	// Else add them separately to the sbins

	int data_size = 0;
	as_particle_type type = sbin->type;
	if (type == AS_PARTICLE_TYPE_STRING) {
		data_size = 20;
	}
	else if (type == AS_PARTICLE_TYPE_INTEGER) {
		data_size = 8;
	}
	else {
		cf_debug(AS_SINDEX, "Invalid data type %d", type);
		return AS_SINDEX_ERR;
	}
	int simatch =  sbin->simatch;
	// If both old val and new val have the same expected type
	if (old_val && new_val) {
		if (old_val->type == AS_LIST && new_val->type == AS_LIST) {
			as_list * old_list = as_list_fromval(old_val);
			as_list * new_list = as_list_fromval(new_val);	

			//		Add all old values of type "type" to a old hash
			as_sindex_sbin_value_hash old_sbin_hash;
			old_sbin_hash.type      = type;
			shash_create(&(old_sbin_hash.value_hash), as_sindex_hash_fn, data_size, 1, AS_SINDEX_SBIN_HASH_SZ, 0);
			as_list_foreach(old_list, as_sindex_list_to_hash, (void *)&old_sbin_hash);

			//		Add all new values of type "type" to a new hash	
			as_sindex_sbin_value_hash new_sbin_hash;
			new_sbin_hash.type      = type;
			shash_create(&(new_sbin_hash.value_hash), as_sindex_hash_fn, data_size, 1, AS_SINDEX_SBIN_HASH_SZ, 0);
			as_list_foreach(new_list, as_sindex_list_to_hash, (void *)&new_sbin_hash);

			//		Iterate through all the values in the old and check if it exist or not in the new hash
			//		If it does not exist add it to the sbin with OP DELETE
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
			old_sbin_hash.sbin      = sbin;
			as_list_foreach(new_list, as_sindex_compare_list_hash, &old_sbin_hash);
			if (sbin->num_values) {
				*found     += 1;
				sbin        = sbin + 1;
			}

			//		Iterate through all the values in the old and check if it exist or not in the old hash 
			//		If it does not exist add it to the sbin with OP INSERT
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, type, simatch);
			new_sbin_hash.sbin      = sbin;
			as_list_foreach(old_list, as_sindex_compare_list_hash, &new_sbin_hash);
			if (sbin->num_values) {
				*found     += 1;
			}

			shash_destroy(old_sbin_hash.value_hash);
			shash_destroy(new_sbin_hash.value_hash);
		}
		return AS_SINDEX_OK;
	}

	// Else add them separately to the sbins
	as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, type, simatch);
	if (as_sindex_add_asval_to_list_sindex(old_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found     += 1;
			sbin        = sbin + *found;
		}
	}

	as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
	if (as_sindex_add_asval_to_list_sindex(new_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found += 1;	
		}
	}
	return AS_SINDEX_OK;
}

static bool as_sindex_mapkeys_to_hash(const as_val * key, const as_val * val, void * udata)
{
	// If the type and val type matches,
	// 		Add them to the hash
	as_sindex_sbin_value_hash * map_hash = (as_sindex_sbin_value_hash *)udata;
	shash * h                            =  map_hash->value_hash;

	if (map_hash->type == AS_PARTICLE_TYPE_STRING) {
		if (key->type == AS_STRING) {
			char * str_val = as_string_get(as_string_fromval(key));
			bool value = true;
			// Compute the digest as a fix sized key of the hash.
			cf_digest str_val_dig;
			cf_digest_compute(str_val, strlen(str_val), &str_val_dig);
			
			if (shash_put(h, &str_val_dig, &value) != SHASH_OK) {
				cf_debug(AS_SINDEX, "shash put failed");
				return false;
			}
		}
	}
	else if (map_hash->type == AS_PARTICLE_TYPE_INTEGER) {
		if (key->type == AS_INTEGER) {
			bool value = true;
			uint64_t int_val = as_integer_get(as_integer_fromval(key));
			if (shash_put(h, &int_val, &value) != SHASH_OK) {
				cf_debug(AS_SINDEX, "shash put failed");
				return false;
			}
		}
	}

	return true;
}

static bool as_sindex_compare_mapkeys_hash(const as_val * key, const as_val * val, void * udata)
{
	// If the type and val type matches
	// 		check if the value exist in the hash.
	// 		If does not exist, add it to the sbin
	as_sindex_sbin_value_hash * map_comp_add = (as_sindex_sbin_value_hash *)udata;
	shash * h                              =  map_comp_add->value_hash;

	if (map_comp_add->type == AS_PARTICLE_TYPE_STRING) {
		if (key->type == AS_STRING) {
			char * str_val = as_string_get(as_string_fromval(key));
			bool value;
			// Compute the digest as a fix sized key of the hash.
			cf_digest str_val_dig;
			cf_digest_compute(str_val, strlen(str_val), &str_val_dig);

			if (shash_get(h, &str_val_dig, &value) != SHASH_OK) {
				as_sindex_add_digest_to_sbin(map_comp_add->sbin, str_val_dig);
			}
		}
	}
	else if (map_comp_add->type == AS_PARTICLE_TYPE_INTEGER) {
		if (key->type == AS_INTEGER) {
			uint64_t int_val = as_integer_get(as_integer_fromval(key));
			bool value;
			if (shash_get(h, &int_val, &value) != SHASH_OK) {
				as_sindex_add_integer_to_sbin(map_comp_add->sbin, int_val);
			}
		}
	}

	return true;
}

as_sindex_status
as_sindex_add_diff_asval_to_mapkeys_sindex(as_val * old_val, as_val * new_val, as_sindex_bin * sbin, int * found)
{
	// If both old val and new val have the same expected type
	//		Add all old values of type "type" to a old hash
	//		Add all new values of type "type" to a new hash
	//		Iterate through all the values in the old and check it exist or not in the new hash
	//		If it does not exist add it to the sbin with OP DELETE
	//		Iterate through all the values in the old and check it exist or not in the old hash 
	//		If it does not exist add it to the sbin with OP INSERT
	// Else add them separately to the sbins

	int data_size = 0;
	as_particle_type type = sbin->type;
	if (type == AS_PARTICLE_TYPE_STRING) {
		data_size = 20;
	}
	else if (type == AS_PARTICLE_TYPE_INTEGER) {
		data_size = 8;
	}
	else {
		cf_debug(AS_SINDEX, "Invalid data type %d", type);
		return AS_SINDEX_ERR;
	}
	int simatch = sbin->simatch;

	// If both old val and new val have the same expected type
	if (old_val && new_val) {
		if (old_val->type == AS_MAP && new_val->type == AS_MAP) {
			as_map * old_map = as_map_fromval(old_val);
			as_map * new_map = as_map_fromval(new_val);	

	//		Add all old values of type "type" to a old hash
			as_sindex_sbin_value_hash old_sbin_hash;
			old_sbin_hash.type      = type;
			shash_create(&(old_sbin_hash.value_hash), as_sindex_hash_fn, data_size, 1, AS_SINDEX_SBIN_HASH_SZ, 0);
			as_map_foreach(old_map, as_sindex_mapkeys_to_hash, &old_sbin_hash);

	//		Add all new values of type "type" to a new hash	
			as_sindex_sbin_value_hash new_sbin_hash;
			new_sbin_hash.type      = type;
			shash_create(&(new_sbin_hash.value_hash), as_sindex_hash_fn, data_size, 1, AS_SINDEX_SBIN_HASH_SZ, 0);
			as_map_foreach(new_map, as_sindex_mapkeys_to_hash, &new_sbin_hash);
	
	//		Iterate through all the values in the old and check if it exist or not in the new hash
	//		If it does not exist add it to the sbin with OP DELETE
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
			old_sbin_hash.sbin = sbin;
			as_map_foreach(new_map, as_sindex_compare_mapkeys_hash, &old_sbin_hash);
			if (sbin->num_values) {
				*found     += 1;
				sbin        = sbin + 1;
			}

	//		Iterate through all the values in the old and check if it exist or not in the old hash 
	//		If it does not exist add it to the sbin with OP INSERT
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, type, simatch);
			new_sbin_hash.sbin = sbin;
			as_map_foreach(old_map, as_sindex_compare_mapkeys_hash, &new_sbin_hash);
			if (sbin->num_values) {
				*found     += 1;
			}

			shash_destroy(old_sbin_hash.value_hash);
			shash_destroy(new_sbin_hash.value_hash);
		}
		return AS_SINDEX_OK;
	}

	// Else add them separately to the sbins
	sbin->op = AS_SINDEX_OP_DELETE;
	if (as_sindex_add_asval_to_mapkeys_sindex(old_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found     += 1;
			sbin        = sbin + *found;
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
		}
	}

	if (as_sindex_add_asval_to_mapkeys_sindex(new_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found += 1;	
		}
	}

	return AS_SINDEX_OK;
}

static bool as_sindex_mapvalues_to_hash(const as_val * key, const as_val * value, void * udata)
{
	// If the type and val type matches,
	// 		Add them to the hash
	as_sindex_sbin_value_hash * map_hash = (as_sindex_sbin_value_hash *)udata;
	shash * h                            =  map_hash->value_hash;

	if (map_hash->type == AS_PARTICLE_TYPE_STRING) {
		if (value->type == AS_STRING) {
			char * str_val = as_string_get(as_string_fromval(value));
			bool value = true;
			// Compute the digest as a fix sized key of the hash.
			cf_digest str_val_dig;
			cf_digest_compute(str_val, strlen(str_val), &str_val_dig);
			
			if (shash_put(h, &str_val_dig, &value) != SHASH_OK) {
				cf_debug(AS_SINDEX, "shash put failed");
				return false;
			}
		}
	}
	else if (map_hash->type == AS_PARTICLE_TYPE_INTEGER) {
		if (value->type == AS_INTEGER) {
			bool hash_value = true;
			uint64_t int_val = as_integer_get(as_integer_fromval(value));
			if (shash_put(h, &int_val, &hash_value) != SHASH_OK) {
				cf_debug(AS_SINDEX, "shash put failed");
				return false;
			}
		}
	}

	return true;
}

static bool as_sindex_compare_mapvalues_hash(const as_val * key, const as_val * value, void * udata)
{
	// If the type and val type matches
	// 		check if the value exist in the hash.
	// 		If does not exist, add it to the sbin
	as_sindex_sbin_value_hash * map_comp_add = (as_sindex_sbin_value_hash *)udata;
	shash * h                              =  map_comp_add->value_hash;

	if (map_comp_add->type == AS_PARTICLE_TYPE_STRING) {
		if (value->type == AS_STRING) {
			char * str_val = as_string_get(as_string_fromval(value));
			bool value;
			// Compute the digest as a fix sized key of the hash.
			cf_digest str_val_dig;
			cf_digest_compute(str_val, strlen(str_val), &str_val_dig);

			if (shash_get(h, &str_val_dig, &value) != SHASH_OK) {
				as_sindex_add_digest_to_sbin(map_comp_add->sbin, str_val_dig);
			}
		}
	}
	else if (map_comp_add->type == AS_PARTICLE_TYPE_INTEGER) {
		if (value->type == AS_INTEGER) {
			uint64_t int_val = as_integer_get(as_integer_fromval(value));
			bool value;
			if (shash_get(h, &int_val, &value) != SHASH_OK) {
				as_sindex_add_integer_to_sbin(map_comp_add->sbin, int_val);
			}
		}
	}

	return true;
}

as_sindex_status
as_sindex_add_diff_asval_to_mapvalues_sindex(as_val * old_val, as_val * new_val, as_sindex_bin * sbin, int * found)
{
	// If both old val and new val have the same expected type
	//		Add all old values of type "type" to a old hash
	//		Add all new values of type "type" to a new hash
	//		Iterate through all the values in the old and check it exist or not in the new hash
	//		If it does not exist add it to the sbin with OP DELETE
	//		Iterate through all the values in the old and check it exist or not in the old hash 
	//		If it does not exist add it to the sbin with OP INSERT
	// Else add them separately to the sbins

	int data_size = 0;
	as_particle_type type = sbin->type;
	if (type == AS_PARTICLE_TYPE_STRING) {
		data_size = 20;
	}
	else if (type == AS_PARTICLE_TYPE_INTEGER) {
		data_size = 8;
	}
	else {
		cf_debug(AS_SINDEX, "Invalid data type %d", type);
		return AS_SINDEX_ERR;
	}
	int simatch = sbin->simatch;

	// If both old val and new val have the same expected type
	if (old_val && new_val) {
		if (old_val->type == AS_MAP && new_val->type == AS_MAP) {
			as_map * old_map = as_map_fromval(old_val);
			as_map * new_map = as_map_fromval(new_val);	

	//		Add all old values of type "type" to a old hash
			as_sindex_sbin_value_hash old_sbin_hash;
			old_sbin_hash.type      = type;
			shash_create(&(old_sbin_hash.value_hash), as_sindex_hash_fn, data_size, 1, AS_SINDEX_SBIN_HASH_SZ, 0);
			as_map_foreach(old_map, as_sindex_mapvalues_to_hash, &old_sbin_hash);

	//		Add all new values of type "type" to a new hash	
			as_sindex_sbin_value_hash new_sbin_hash;
			new_sbin_hash.type      = type;
			shash_create(&(new_sbin_hash.value_hash), as_sindex_hash_fn, data_size, 1, AS_SINDEX_SBIN_HASH_SZ, 0);
			as_map_foreach(new_map, as_sindex_mapvalues_to_hash, &new_sbin_hash);
	
	//		Iterate through all the values in the old and check if it exist or not in the new hash
	//		If it does not exist add it to the sbin with OP DELETE
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
			old_sbin_hash.sbin = sbin;
			as_map_foreach(new_map, as_sindex_compare_mapvalues_hash, &old_sbin_hash);
			if (sbin->num_values) {
				*found     += 1;
				sbin        = sbin + 1;
			}

	//		Iterate through all the values in the old and check if it exist or not in the old hash 
	//		If it does not exist add it to the sbin with OP INSERT
			as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, type, simatch);
			new_sbin_hash.sbin = sbin;
			as_map_foreach(old_map, as_sindex_compare_mapvalues_hash, &new_sbin_hash);
			if (sbin->num_values) {
				*found     += 1;
			}

			shash_destroy(old_sbin_hash.value_hash);
			shash_destroy(new_sbin_hash.value_hash);
		}
		return AS_SINDEX_OK;
	}

	// Else add them separately to the sbins
	as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, type, simatch);
	if (as_sindex_add_asval_to_mapvalues_sindex(old_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found     += 1;
			sbin        = sbin + *found;
		}
	}

	as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, type, simatch);
	if (as_sindex_add_asval_to_mapvalues_sindex(new_val, sbin) == AS_SINDEX_OK) {
		if (sbin->num_values) {
			*found += 1;	
		}
	}
	return AS_SINDEX_OK;
}

typedef as_sindex_status (*as_sindex_add_diff_asval_to_itype_sindex_fn)
			(as_val * old_val, as_val * new_val, as_sindex_bin * sbin, int * found);
			static const as_sindex_add_diff_asval_to_itype_sindex_fn 
			as_sindex_add_diff_asval_to_itype_sindex[AS_SINDEX_ITYPE_MAX] = {
	as_sindex_add_diff_asval_to_default_sindex,
	as_sindex_add_diff_asval_to_list_sindex,
	as_sindex_add_diff_asval_to_mapkeys_sindex,
	as_sindex_add_diff_asval_to_mapvalues_sindex
};

as_val * as_val_frombuf(byte *buf, uint32_t sz)
{
	as_val * v = NULL;
	as_buffer     asbuf;
	as_buffer_init(&asbuf);
	as_serializer s;
	as_msgpack_init(&s);

	asbuf.data = buf;
	asbuf.capacity = sz;
	asbuf.size = sz;

	as_serializer_deserialize(&s, &asbuf, &v);
	as_serializer_destroy(&s);
	return v;
}

int
as_sindex_sbin_from_sindex(as_sindex * si, as_bin *b, as_sindex_bin * sbin, as_val ** cdt_asval, 
					byte * buf, uint32_t buf_sz, as_particle_type type, bool from_buf)
{
	as_sindex_metadata * imd    = si->imd;
	as_particle_type imd_btype  = as_sindex_pktype(imd);
	bool deserialized           = !(*cdt_asval) ? false : true;
	as_val * cdt_val            = * cdt_asval;
	uint32_t  valsz             = 0;
	int sindex_found            = 0;
	as_particle_type bin_type   = 0;
	bool found = false;
	if (from_buf) {
		bin_type                = type;
	}
	else {
		bin_type                = as_bin_get_particle_type(b);
	}
	
	//		Prepare si
	// 		If path_length == 0
	if (imd->path_length == 0) {
		// 			If itype == AS_SINDEX_ITYPE_DEFAULT and bin_type == STRING OR INTEGER
		// 				Add the value to the sbin.
		if (imd->itype == AS_SINDEX_ITYPE_DEFAULT && bin_type == imd_btype) {
			if (bin_type == AS_PARTICLE_TYPE_INTEGER) {
				found = true;
				uint64_t int_val = 0;
				if (from_buf) {
					int_val =  __be64_to_cpup((void*)buf);
				}
				else {
					valsz = as_bin_particle_to_mem(b, (byte *)&sbin->value.int_val);
					int_val    = sbin->value.int_val;
				}

				if (as_sindex_add_integer_to_sbin(sbin, int_val) == AS_SINDEX_OK) {
					if (sbin->num_values) {
						sindex_found++;
					}
				}
			}	
			else if (bin_type == AS_PARTICLE_TYPE_STRING) {
				found = true;
				byte* bin_val;
				bool valid_str = true;
				cf_digest buf_dig;
				if (from_buf) {
					if ( buf_sz < 0 || buf_sz > AS_SINDEX_MAX_STRING_KSIZE) {
						cf_warning( AS_SINDEX, "sindex key size out of bounds %d ", buf_sz);
						valid_str = false;
					}
					else {
						cf_digest_compute(buf, buf_sz, &buf_dig);
					}
				}
				else {
					valsz = as_bin_particle_mem_size(b);
					if ( valsz < 0 || valsz > AS_SINDEX_MAX_STRING_KSIZE) {
						cf_warning( AS_SINDEX, "sindex key size out of bounds %d ", valsz);
						valid_str = false;
					}
					else {
						valsz = as_bin_particle_ptr(b, &bin_val);
						cf_digest_compute(bin_val, valsz, &buf_dig);
					}
				}
				if (valid_str) {					
					if (as_sindex_add_digest_to_sbin(sbin, buf_dig) == AS_SINDEX_OK) {
						if (sbin->num_values) {
							sindex_found++;
						}
					}
				}
			}
		}
	}
	// 		Else if path_length > 0 OR type == MAP or LIST 
	// 			Deserialize the bin if have not deserialized it yet.
	//			Extract as_val from path within the bin.
	//			Add the values to the sbin.
	if (!found) {
		if (bin_type == AS_PARTICLE_TYPE_MAP || bin_type == AS_PARTICLE_TYPE_LIST) {
			if (! deserialized) {
				if (from_buf) {
					cdt_val =  as_val_frombuf(buf, buf_sz);
				}
				else {
					cdt_val   = as_val_frombin(b);
				}
				deserialized = true;
			}
			as_val * res_val   = as_sindex_extract_val_from_path(imd, cdt_val);
			if (!res_val) {
				goto END;
			}
			if (as_sindex_add_asval_to_itype_sindex[imd->itype](res_val, sbin) == AS_SINDEX_OK) {
				if (sbin->num_values) {
					sindex_found++;
				}
			}
		}
	}
END:
	*cdt_asval = cdt_val;
	return sindex_found;	
}

// Returns the number of sindex found
int
as_sindex_sbins_from_bin_buf(as_namespace *ns, const char *set, as_bin *b, as_sindex_bin * start_sbin, 
					byte * buf, uint32_t buf_sz, as_particle_type type, as_sindex_op op, bool from_buf)
{
	// Check the sindex bit array.
	// If there is not sindex present on this bin return 0
	// Get the simatch_ll from set_binid_hash
	// If simatch_ll is NULL return 0
	// Iterate through simatch_ll
	// 		If path_length == 0
	// 			If itype == AS_SINDEX_ITYPE_DEFAULT and bin_type == STRING OR INTEGER
	// 				Add the value to the sbin.
	//			If itype == AS_SINDEX_ITYPE_MAP or AS_SINDEX_ITYPE_INVMAP and type = MAP
	//	 			Deserialize the bin if have not deserialized it yet.
	//				Extract as_val from path within the bin
	//				Add them to the sbin.
	// 			If itype == AS_SINDEX_ITYPE_LIST and type = LIST
	//	 			Deserialize the bin if have not deserialized it yet.
	//				Extract as_val from path within the bin.
	//				Add the values to the sbin.
	// 		Else if path_length > 0 and type == MAP or LIST 
	// 			Deserialize the bin if have not deserialized it yet.
	//			Extract as_val from path within the bin.
	//			Add the values to the sbin.
	// Return the number of sbins found.

	int sindex_found = 0;
	if (!b) {
		cf_warning(AS_SINDEX, "Null Bin Passed, No sbin created");
		return sindex_found;
	}
	if (!ns) {
		cf_warning(AS_SINDEX, "NULL Namespace Passed");
		return sindex_found;
	}
	if (!from_buf && !as_bin_inuse(b)) {
		return sindex_found;
	}

	// Check the sindex bit array.
	// If there is not sindex present on this bin return 0
	if (!as_sindex_binid_has_sindex(ns, b->id) ) {
		return sindex_found;
	}

	// Get the simatch_ll from set_binid_hash
	cf_ll * simatch_ll  = NULL;
	as_sindex__simatch_list_by_set_binid(ns, set, b->id, &simatch_ll);

	// If simatch_ll is NULL return 0
	if (!simatch_ll) {
		return sindex_found;
	}

	// Iterate through simatch_ll
	cf_ll_element             * ele    = cf_ll_get_head(simatch_ll);
	sindex_set_binid_hash_ele * si_ele = NULL;
	int                        simatch = -1;
	as_sindex                 * si     = NULL;
	as_val                   * cdt_val = NULL;
	int                   sbins_in_si  = 0;
	while (ele) {
		si_ele                = (sindex_set_binid_hash_ele *) ele;
		simatch               = si_ele->simatch;
		si                    = &ns->sindex[simatch];
		if (!as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}
		AS_SINDEX_RESERVE(si);   
		as_sindex_init_sbin(&start_sbin[sindex_found], op,  as_sindex_pktype(si->imd), simatch);
		uint64_t s_time = cf_getns();
		sbins_in_si          = as_sindex_sbin_from_sindex(si, b, &start_sbin[sindex_found], &cdt_val, buf, buf_sz, type, from_buf);
		if (sbins_in_si > 0) {
			sindex_found += sbins_in_si;
			// AS_SINDEX_RELEASE will happen after sindex tree has been updated	
			SINDEX_HIST_INSERT_DATA_POINT(si, si_prep_hist, s_time);
		}
		else {
			AS_SINDEX_RELEASE(si);
		}
		ele                   = ele->next;
	}

	// FREE as_val
	if (cdt_val) {
		as_val_destroy(cdt_val);
	}
	// Return the number of sbin found.
	return sindex_found;
}

int
as_sindex_sbins_from_bin(as_namespace *ns, const char *set, as_bin *b, as_sindex_bin * start_sbin, as_sindex_op op)
{
	return as_sindex_sbins_from_bin_buf(ns, set, b, start_sbin, NULL, 0, 0, op, false);
}

int
as_sindex_sbins_from_buf(as_namespace *ns, const char *set, as_bin *b, as_sindex_bin * start_sbin, 
					byte * buf, uint32_t buf_sz, as_particle_type type, as_sindex_op op)
{
	return as_sindex_sbins_from_bin_buf(ns, set, b, start_sbin, buf, buf_sz, type, op, true);
}


int
as_sindex_diff_sbins_from_sindex(as_sindex * si, as_bin * b, byte * buf, uint32_t buf_sz, as_particle_type type, 
		as_sindex_bin * sbin, as_val ** bin_val, as_val ** buf_val, bool * deserialized)
{
	as_sindex_metadata * imd       = si->imd;
	as_particle_type   imd_btype = as_sindex_pktype(imd);
	int sindex_found = 0;
	bool found = false;
	int simatch = si->simatch;
	uint32_t valsz = 0;
	as_particle_type bin_type = type;
	if (imd->path_length == 0 ) {
		// 			If itype == AS_SINDEX_ITYPE_DEFAULT and bin_type == STRING OR INTEGER
		// 				Compare the basic value of both bin and msgop
		//				If it changes add to a sbin.
		if (imd->itype == AS_SINDEX_ITYPE_DEFAULT ) {
			if (type == AS_PARTICLE_TYPE_INTEGER) {
				found = true;
				uint64_t buf_int = __be64_to_cpup((void*)buf);
				uint64_t bin_int = 0;
				valsz = as_bin_particle_to_mem(b, (byte *)&bin_int);
				if (buf_int != bin_int) {
					as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, imd_btype, simatch);
					if (as_sindex_add_integer_to_sbin(sbin, bin_int) == AS_SINDEX_OK) {
						if (sbin->num_values) {
							sindex_found++;
							sbin = sbin + sindex_found;
						}
					}

					as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, imd_btype, simatch);
					if (as_sindex_add_integer_to_sbin(sbin, buf_int) == AS_SINDEX_OK) {
						if (sbin->num_values) {
							sindex_found++;
						}
					}
				}
			}
			else if (type == AS_PARTICLE_TYPE_STRING) {
				found = true;
				bool valid_binstr = true;
				bool valid_bufstr = true;
				byte* bin_str;
				cf_digest bin_dig, buf_dig;
				bool has_changed = true;
				if ( buf_sz < 0 || buf_sz > AS_SINDEX_MAX_STRING_KSIZE) {
					cf_warning( AS_SINDEX, "sindex key size out of bounds %d ", buf_sz);
					valid_bufstr = false;
				}
				else {
					cf_digest_compute(buf, buf_sz, &buf_dig);
				}

				valsz = as_bin_particle_mem_size(b);
				if ( valsz < 0 || valsz > AS_SINDEX_MAX_STRING_KSIZE) {
					cf_warning( AS_SINDEX, "sindex key size out of bounds %d ", valsz);
					valid_binstr = false;
				}
				else {
					valsz = as_bin_particle_ptr(b, &bin_str);
					cf_digest_compute(bin_str, valsz, &bin_dig);
				}

				if (valid_bufstr && valid_binstr) {
					has_changed = memcmp(&buf_dig, &bin_dig, sizeof(cf_digest)) ? true : false;
				}

				if (has_changed) {
					if (valid_binstr) {
						as_sindex_init_sbin(sbin, AS_SINDEX_OP_DELETE, imd_btype, simatch);
						if (as_sindex_add_digest_to_sbin(sbin, bin_dig) == AS_SINDEX_OK) {
							if (sbin->num_values) {
								sindex_found++;
								sbin = sbin + sindex_found;
							}
						}
					}
					if (valid_bufstr) {
						as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, imd_btype, simatch);
						if (as_sindex_add_digest_to_sbin(sbin, buf_dig) == AS_SINDEX_OK) {
							if (sbin->num_values) {
								sindex_found++;
							}
						}
					}
				}
			}
		}
	}

	// 		Else if path_length > 0 OR type == MAP or LIST 
	// 			Deserialize the existing_bin and incoming_bin if have not deserialized it yet.
	//			Extract as_val from path within the bin and buf
	//			Compare the values in both vals and add them to the sbin.	
	if (!found && (bin_type == AS_PARTICLE_TYPE_MAP || bin_type == AS_PARTICLE_TYPE_LIST)) {
		if (!*deserialized) {
			*buf_val      = as_val_frombuf(buf, buf_sz);
			*bin_val      = as_val_frombin(b);
			*deserialized = true;
		}
		as_val * path_binval   = as_sindex_extract_val_from_path(imd, *bin_val);
		as_val * path_bufval   = as_sindex_extract_val_from_path(imd, *buf_val);	
		int found = 0;
		as_sindex_init_sbin(sbin, AS_SINDEX_OP_INSERT, imd_btype, simatch);			
		if (path_binval || path_bufval) {
			if (as_sindex_add_diff_asval_to_itype_sindex[imd->itype]
					(path_binval, path_bufval, sbin, &found) == AS_SINDEX_OK) {
				sindex_found += found;
			}
		}
	}
	return sindex_found;
}


int
as_sindex_diff_sbins_from_buf(as_namespace * ns, const char * set, as_bin * b, byte * buf, uint32_t buf_sz,
		as_particle_type type, as_sindex_bin * start_sbin)
{
	// Check if this bin has alteast one sindex over it.
	// If buf is null return the sbins from the bin.
	// If type of bins are not same, then get the sbins from both bins and msgop and send them back.
	// Get the simatch_ll from set_binid_hash
	// If simatch_ll is NULL return 0
	// Iterate through simatch_ll
	// 		If path_length == 0
	// 			If itype == AS_SINDEX_ITYPE_DEFAULT and bin_type == STRING OR INTEGER
	// 				Compare the basic value of both bin and msgop
	//				If it changes add to a sbin.
	//			If itype == AS_SINDEX_ITYPE_MAP or AS_SINDEX_ITYPE_INVMAP and type = MAP
	//	 			Deserialize the existing_bin and incoming_bin if have not deserialized it yet.
	//				Extract as_val from path within the bin and buf
	//				Compare the values in both vals and add them to the sbin.
	// 			If itype == AS_SINDEX_ITYPE_LIST and type = LIST
	//	 			Deserialize the existing_bin and incoming_bin if have not deserialized it yet.
	//				Extract as_val from path within the bin and buf
	//				Compare the values in both vals and add them to the sbin.
	// 		Else if path_length > 0 and type == MAP or LIST 
	// 			Deserialize the existing_bin and incoming_bin if have not deserialized it yet.
	//			Extract as_val from path within the bin and buf
	//			Compare the values in both vals and add them to the sbin.
	// Return the number of sbins found.

	int sindex_found = 0;

	// Do preliminary checks
	if (!b) {
		cf_warning(AS_SINDEX, "Null Bin Passed, No sbin created");
		return sindex_found;
	}
	if (!ns) {
		cf_warning(AS_SINDEX, "NULL Namespace Passed");
		return sindex_found;
	}
	if (!as_bin_inuse(b)) {
		return sindex_found;
	}

	// Check if this bin has alteast one sindex over it.
	if (!as_sindex_binid_has_sindex(ns, b->id) ) {
		 return sindex_found;
	}

	// If buf is null return the sbins from the bin.
	if (!buf) {
		sindex_found += as_sindex_sbins_from_bin(ns, set, b, start_sbin + sindex_found, AS_SINDEX_OP_DELETE);
		return sindex_found;	
	}

	// If type of bins are not same, then both cannot have values in the same index.
	// Get the sbins from both bins and buf and send them back.
	if (type != as_bin_get_particle_type(b)) {
		sindex_found += as_sindex_sbins_from_bin(ns, set, b, start_sbin + sindex_found, AS_SINDEX_OP_DELETE);
		sindex_found += as_sindex_sbins_from_buf(ns, set, b, start_sbin + sindex_found, buf, buf_sz, type, AS_SINDEX_OP_INSERT);
		return sindex_found;
	}

	// Get the simatch_ll from set_binid_hash
	cf_ll * simatch_ll = NULL;
	as_sindex__simatch_list_by_set_binid(ns, set, b->id, &simatch_ll);
	// If simatch_ll is NULL return 0
	if (!simatch_ll) {
		return sindex_found;
	}

	// Iterate through simatch_ll
	cf_ll_element             * ele    = cf_ll_get_head(simatch_ll);
	sindex_set_binid_hash_ele * si_ele = NULL;
	int                        simatch = -1;
	as_sindex                 * si     = NULL;
	as_val * buf_val                   = NULL;
	as_val * bin_val                   = NULL;
	bool                  deserialized = false;
	as_sindex_bin             * sbin   = NULL;
	int                    sbins_in_si = 0;
	while (ele) {
		si_ele        = (sindex_set_binid_hash_ele *) ele;
		simatch       = si_ele->simatch;
		si            = &ns->sindex[simatch];
		sbin          = start_sbin + sindex_found;
		if (!as_sindex_isactive(si)) {
			ele = ele->next;
			continue;
		}
		AS_SINDEX_RESERVE(si);
		sbins_in_si  = as_sindex_diff_sbins_from_sindex(si, b, buf, buf_sz, type, sbin, &bin_val, &buf_val, &deserialized);
		if (sbins_in_si > 0) {
			sindex_found += sbins_in_si;
			if (sbins_in_si > 1) {
			// Two ops (INSERT AND DELETE)are going to happen on the same index. we need to reserve it twice.
				AS_SINDEX_RESERVE(si);
			}
		}
		else {
			AS_SINDEX_RELEASE(si);
		}
		ele           = ele->next;
	}
	// FREE as_vals
	if (buf_val) {
		as_val_destroy(buf_val);
	}
	if (bin_val) {
		as_val_destroy(bin_val);
	}

	return sindex_found;
}

/*
 * returns number of sbins found.
 */
int
as_sindex_sbins_from_rd(as_storage_rd *rd, uint16_t from_bin, uint16_t to_bin, as_sindex_bin sbins[], as_sindex_op op)
{
	uint16_t count  = 0;
	for (uint16_t i = from_bin; i < to_bin; i++) {
		as_bin *b   = &rd->bins[i];
		count      += as_sindex_sbins_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns), b, &sbins[count], op);
	}
	return count;
}

int
as_sindex_sbin_free(as_sindex_bin *sbin)
{
	if (sbin->to_free) {
		if (! (sbin->type == AS_PARTICLE_TYPE_INTEGER || sbin->type == AS_PARTICLE_TYPE_STRING)) {
			cf_debug(AS_SINDEX, "sbin free got invalid data type in sbin %d", sbin->type);
			return AS_SINDEX_ERR;
		}
		cf_free(sbin->values);
	}
    return AS_SINDEX_OK;
}

int
as_sindex_sbin_freeall(as_sindex_bin *sbin, int numbins)
{
	for (int i = 0; i < numbins; i++)  {
		as_sindex_sbin_free(&sbin[i]);
	}
	return AS_SINDEX_OK;
} 

/*
 * Function to decide if current node has partition of the passed int digest
 * valid for returning result. Used by secondary index scan to filter out digest
 */
bool
as_sindex_partition_isqnode(as_namespace *ns, cf_digest *digest)
{
	as_partition *p = NULL;
	if (!ns) {
		cf_warning(AS_SINDEX, "Got null namespace. ABORT!");
		return false;
	}

	int pid       = as_partition_getid(*(digest));
	p             = &ns->partitions[pid];
	// NB: This is lockless check could result is false positives. Query
	// does the reservation before reading which would make sure these 
	// are filtered out. Only possible case this could happen is when 
	// qnode information is changing
	return (p->qnode == g_config.self_node);
}


/* This find out of the record can be defragged 
 * AS_SINDEX_GC_ERROR if found and cannot defrag
 * AS_SINDEX_GC_OK if can defrag
 * AS_SINDEX_GC_SKIP_ITERATION if skip current gc iteration. (partition lock timed out)
 */  
as_sindex_gc_status
as_sindex_can_defrag_record(as_namespace *ns, cf_digest *keyd)
{
	as_partition_reservation rsv;
	as_partition_id pid = as_partition_getid(*keyd);
	
	int timeout = 2; // 2 ms
	if (as_partition_reserve_migrate_timeout(ns, pid, &rsv, 0, timeout) != 0 ) {
		cf_atomic_int_add(&g_config.sindex_gc_timedout, 1);
		return AS_SINDEX_GC_SKIP_ITERATION;
	}

	int rv = AS_SINDEX_GC_ERROR;
	if (as_record_exists(rsv.tree, keyd, rsv.ns) != 0) {
		rv = AS_SINDEX_GC_OK;
	}
	as_partition_release(&rsv);
	return rv;

}

int
as_sindex_histogram_dumpall(as_namespace *ns)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	SINDEX_GRLOCK();

	for (int i = 0; i < ns->sindex_cnt; i++) {
		if (ns->sindex[i].state != AS_SINDEX_ACTIVE) continue;
		if (!ns->sindex[i].enable_histogram)         continue;
		as_sindex *si = &ns->sindex[i];
		if (si->stats._write_hist)
			histogram_dump(si->stats._write_hist);
		if (si->stats._si_prep_hist)
			histogram_dump(si->stats._si_prep_hist);
		if (si->stats._delete_hist)
			histogram_dump(si->stats._delete_hist);
		if (si->stats._query_hist)
			histogram_dump(si->stats._query_hist);
		if (si->stats._query_batch_lookup)
			histogram_dump(si->stats._query_batch_lookup);
		if (si->stats._query_batch_io)
			histogram_dump(si->stats._query_batch_io);
		if (si->stats._query_rcnt_hist)
			histogram_dump(si->stats._query_rcnt_hist);
		if (si->stats._query_diff_hist)
			histogram_dump(si->stats._query_diff_hist);
	}
	SINDEX_GUNLOCK();
	return AS_SINDEX_OK;
}

int
as_sindex_histogram_enable(as_namespace *ns, as_sindex_metadata *imd, bool enable)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;

	as_sindex *si = as_sindex_lookup_by_iname(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		return AS_SINDEX_ERR_NOTFOUND;
	}

	si->enable_histogram = enable;
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_OK;
}

/*
 * Function to check to make sure if two bins match.
 * Returns true if it matches
 */
/*
bool
as_sindex_sbin_match(as_sindex_bin *b1, as_sindex_bin *b2)
{
	if (!b1 || !b2)                                         return false;
	if (b1->id    != b2->id)                                return false;
	if (b1->type  != b2->type)                              return false;

	if ((b1->type == AS_PARTICLE_TYPE_INTEGER)
			&& (b1->u.i64 != b2->u.i64))                    return false;
	if ((b1->type == AS_PARTICLE_TYPE_STRING)
			&& (memcmp(&b1->digest, &b2->digest, AS_DIGEST_KEY_SZ)))  return false;

	return true;
}
*/

/*
 * ACCOUNTING ACCOUNTING ACCOUNTING
 *
 * Internal function API for tracking sindex memory usage. This get called
 * from inside Aerospike Index.
 *
 * TODO: Make accounting subsystem cache friendly. At high speed it
 *       cache misses it causes starts to matter
 *
 * Reserve locally first then globally
 */
bool
as_sindex_reserve_data_memory(as_sindex_metadata *imd, uint64_t bytes)
{
	if (!bytes)                           return true;
	if (!imd || !imd->si || !imd->si->ns) return false;
	as_namespace *ns = imd->si->ns;
	bool g_reserved  = false;
	bool ns_reserved = false;
	bool si_reserved = false;
	uint64_t val     = 0;

	// Global reservation
	val = cf_atomic_int_add(&g_config.sindex_data_memory_used, bytes);
	g_reserved = true;
	if (val > g_config.sindex_data_max_memory) goto FAIL;
	
	// Namespace reservation
	val = cf_atomic_int_add(&ns->sindex_data_memory_used, bytes);
	ns_reserved = true;
	if (val > ns->sindex_data_max_memory)      goto FAIL;
	
	// Secondary Index Specific
	val = cf_atomic_int_add(&imd->si->data_memory_used, bytes);
	si_reserved = true;
	if (val > imd->si->config.data_max_memory) goto FAIL;
	
	return true;

FAIL:
	if (ns_reserved) cf_atomic_int_sub(&ns->sindex_data_memory_used, bytes);
	if (g_reserved)  cf_atomic_int_sub(&g_config.sindex_data_memory_used, bytes);
	if (si_reserved)  cf_atomic_int_sub(&imd->si->data_memory_used, bytes);
	cf_warning(AS_SINDEX, "Data Memory Cap Hit for Secondary Index %s "
							"while reserving %ld bytes", imd->iname, bytes);
	return false;
}

// release locally first then globally
bool
as_sindex_release_data_memory(as_sindex_metadata *imd, uint64_t bytes)
{
	as_namespace *ns = imd->si->ns;
	if ((ns->sindex_data_memory_used < bytes)
		|| (imd->si->data_memory_used < bytes)
		|| (g_config.sindex_data_memory_used < bytes)) {
		cf_warning(AS_SINDEX, "Sindex memory usage accounting corrupted");
	}
	cf_atomic_int_sub(&ns->sindex_data_memory_used, bytes);
	cf_atomic_int_sub(&g_config.sindex_data_memory_used, bytes);
	cf_atomic_int_sub(&imd->si->data_memory_used, bytes);
	return true;
}

uint64_t
as_sindex_get_ns_memory_used(as_namespace *ns)
{
	if (as_sindex_ns_has_sindex(ns)) {
		return ns->sindex_data_memory_used;
	}
	return 0;
}

extern int as_info_parameter_get(char *param_str, char *param, char *value, int  *value_len);

/*
 * Client API function to set configuration parameters for secondary indexes
 */
int
as_sindex_set_config(as_namespace *ns, as_sindex_metadata *imd, char *params)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	as_sindex *si = as_sindex_lookup_by_iname(ns, imd->iname, AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		return AS_SINDEX_ERR_NOTFOUND;
	}
	SINDEX_WLOCK(&si->imd->slock);
	if (si->state == AS_SINDEX_ACTIVE) {
		char context[100];
		int  context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "ignore-not-sync", context, &context_len)) {
			if (strncmp(context, "true", 4)==0 || strncmp(context, "yes", 3)==0) {
				cf_info(AS_INFO,"Changing value of ignore-not-sync of ns %s sindex %s to %s", ns->name, imd->iname, context);
				si->config.flag |= AS_SINDEX_CONFIG_IGNORE_ON_DESYNC;
			} else if (strncmp(context, "false", 5)==0 || strncmp(context, "no", 2)==0) {
				cf_info(AS_INFO,"Changing value of ignore-not-sync of ns %s sindex %s to %s", ns->name, imd->iname, context);
				si->config.flag &= ~AS_SINDEX_CONFIG_IGNORE_ON_DESYNC;
			} else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "data-max-memory", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_detail(AS_INFO, "data-max-memory = %"PRIu64"",val);
			// Protect so someone does not reduce memory to below 1/2 current value, allow it
			// in case value is ULONG_MAX
			if (((si->config.data_max_memory != ULONG_MAX)
				&& (val < (si->config.data_max_memory / 2L)))
				|| (val < cf_atomic64_get(si->data_memory_used))) {
				goto Error;
			}
			cf_info(AS_INFO,"Changing value of data-max-memory of ns %s sindex %s from %"PRIu64"to %"PRIu64"",
							ns->name, imd->iname, si->config.data_max_memory, val);
			si->config.data_max_memory = val;
		}
		else if (0 == as_info_parameter_get(params, "gc-period", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_detail(AS_INFO, "gc-period = %"PRIu64"",val);
			if (val < 0) {
				goto Error;
			}
			cf_info(AS_INFO,"Changing value of gc-period of ns %s sindex %s from %"PRIu64"to %"PRIu64"",
							ns->name, imd->iname, si->config.defrag_period, val);
			si->config.defrag_period = val;
		}
		else if (0 == as_info_parameter_get(params, "gc-max-units", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_detail(AS_INFO, "gc-limit = %"PRIu64"",val);
			if (val < 0) {
				goto Error;
			}
			cf_info(AS_INFO,"Changing value of gc-max-units of ns %s sindex %s from %"PRIu64"to %"PRIu64"",
							ns->name, imd->iname, si->config.defrag_max_units, val);
			si->config.defrag_max_units = val;
		}
		else {
			goto Error;
		}
	}
	SINDEX_UNLOCK(&si->imd->slock);
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_OK;

Error:
	SINDEX_UNLOCK(&si->imd->slock);
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_ERR_PARAM;
}




// System Metadata Integration : System Metadata Integration
// System Metadata Integration : System Metadata Integration

/*
 *                +------------------+
 *  client -->    |  Secondary Index |
 *                +------------------+
 *                     /|\
 *                      | 4 accept
 *                  +----------+   2
 *                  |          |<-------   +------------------+ 1 request
 *                  | SMD      | 3 merge   |  Secondary Index | <------------|
 *                  |          |<------->  |                  | 5 response   | CLIENT
 *                  |          | 4 accept  |                  | ------------>|
 *                  |          |-------->  +------------------+
 *                  +----------+
 *                     |   4 accept
 *                    \|/
 *                +------------------+
 *  client -->    |  Secondary Index |
 *                +------------------+
 *
 *
 *  System Metadta module sits in the middle of multiple secondary index
 *  module on multiple nodes. The changes which eventually are made to the
 *  secondary index are always triggerred from SMD. Here is the flow.
 *
 *  Step1: Client send (could possibly be secondary index thread) triggers
 *         create / delete / update related to secondary index metadata.
 *
 *  Step2: The request passed through secondary index module (may be few
 *         node specific info is added on the way) to the SMD.
 *
 *  Step3: SMD send out the request to the paxos master.
 *
 *  Step4: Paxos master request the relevant metadata info from all the
 *         nodes in the cluster once it has all the data... [SMD always
 *         stores copy of the data, it is stored when the first time
 *         create happens]..it call secondary index merge callback
 *         function. The function is responsible for resolving the winning
 *         version ...
 *
 *  Step5: Once winning version is decided for all the registered module
 *         the changes are sent to all the node.
 *
 *  Step6: At each node accept_fn is called for each module. Which triggers
 *         the call to the secondary index create/delete/update functions
 *         which would be used to in-memory operation and make it available
 *         for the system.
 *
 *  There are two types of operations which look at the secondary index
 *  operations.
 *
 *  a) Normal operation .. they all look a the in-memory structure and
 *     data which is in sindex and ai_btree layer.
 *
 *  b) Other part which do DDL operation like which work through the SMD
 *     layer. Multiple operation happening from the multiple nodes which
 *     come through this layer. The synchronization is responsible of
 *     SMD layer. The part sindex / ai_btree code is responsible is to
 *     make sure when the call from the SMD comes there is proper sync
 *     between this and operation in section a
 *
 *  Set of function implemented below are merge function and accept function.
 */

int
as_sindex_smd_create()
{
	// Init's the smd interface.
	//
	// as_smd_module_create("SINDEX", as_sindex_smd_merge_cb, NULL, as_sindex_smd_accept_cb, NULL);
	//
	// Expectation: This would read stuff up from the JSON file which SMD
	//              maintance and make sure the requested data is loaded
	//              and the accept_fn is called. All the real logic is
	//              inside accept_fn function.
	return 0;
}

int
as_sindex_smd_merge_cb(char *module, as_smd_item_list_t **item_list_out,
						as_smd_item_list_t **item_lists_in, size_t num_lists,
						void *udata)
{
	// Applies merge and decides who wins. This is algorithm which should
	// be independent of who is running is .. any node which becomes paxos
	// master can execute it and get the same result.
	return 0;
}

extern int as_info_parse_params_to_sindex_imd(char *, as_sindex_metadata *, cf_dyn_buf *, bool, bool*);

/*
 * Description     : When a index has to be dropped and recreated during cluster state change
 * 				     this function is called.
 * Parameters      : imd, which is constructed from the final index defn given by paxos principal.
 * 
 * Returns         : 0 on all cases. Check log for errors.
 *
 * Synchronization : Does not explicitly take any locks
 */
int
as_sindex_update(as_sindex_metadata* imd)
{
	as_namespace *ns = as_namespace_get_byname(imd->ns_name);
	int ret          = as_sindex_create(ns, imd, true);
	if (ret != 0) {
		cf_warning(AS_SINDEX,"Index %s creation failed at the accept callback", imd->iname);
	}
	return 0;
}

/*
 * Description :
 *  	Checks the parameters passed to as_sindex_create function
 *
 * Parameters:
 * 		namespace, index metadata
 *
 * Returns:
 * 		AS_SINDEX_OK            - for valid parameters.
 * 		Appropriate error codes - otherwise
 *
 * Synchronization:
 * 		This function does not explicitly acquire any lock.
 */
int
as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata* imd)
{
	SINDEX_GRLOCK();

	int ret     = AS_SINDEX_OK;
	if (ns->sindex_cnt >= AS_SINDEX_MAX) {
		ret = AS_SINDEX_ERR_MAXCOUNT;
		goto END;
	}

	int simatch = as_sindex__simatch_by_iname(ns, imd->iname);

	if (simatch != -1) {
		cf_info(AS_SINDEX,"Index %s already exists", imd->iname);
		ret = AS_SINDEX_ERR_FOUND;
	} else {
		int16_t binid = as_bin_get_id(ns, imd->bnames[0]);
		if (binid != -1)
		{
			int simatch = as_sindex__simatch_by_set_binid(ns, imd->set, binid, imd->btype[0], imd->itype, imd->path_str);
			if (simatch != -1) {
				cf_info(AS_SINDEX," The bin %s is already indexed @ %d",imd->bnames[0], simatch);
				ret = AS_SINDEX_ERR_FOUND;
				goto END;
			}
		}

		for (int i = 0; i < AS_SINDEX_BINMAX; i++) {
			if (imd->bnames[i] && ( strlen(imd->bnames[i]) > ( AS_ID_INAME_SZ - 1 ) )) {
				cf_info(AS_SINDEX, "Index Name %s too long ", imd->bnames[i]);
				ret = AS_SINDEX_ERR_PARAM;
				goto END;
			}
		}
	}

END:
	SINDEX_GUNLOCK();
    return ret;
}

/*
 * Description: This cb function is called by paxos master, before doing the
 *              In the case of AS_SMD_SET_ACTION
 *              1) existence of index with the given index name.
 *              2) Whether the bin already has a index
 *              In case of AS_SMD_DELETE_ACTION
 *              1) the existence of index with the given name
 * Parameters:
 * 			   module -- module name (SINDEX_MODULE)
 * 			   item   -- action item that paxos master has received
 * 			   udata  -- user data for the callback
 *
 * Returns:
 * 	In case of AS_SMD_SET_ACTION:
 * 		AS_SIDNEX_ERR_INDEX_FOUND  - if index with the given name exists or bin
 * 									  already has an index
 * 		AS_SINDEX_OK			    - Otherwise
 * 	In case of AS_SMD_DELETE_ACTION
 * 		AS_SINDEX_ERR_NOTFOUND      - Index does not exist with the given index name
 * 		AS_SINDEX_OK				- Otherwise
 *
 * 	Synchronization
 * 		This function takes SINDEX GLOBAL WRITE LOCK and releasese it for checking deletion
 * 		operation.
 */
int
as_sindex_smd_can_accept_cb(char *module, as_smd_item_t *item, void *udata)
{
	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));

	char         * params = NULL;
	as_namespace * ns     = NULL;
	int retval            = AS_SINDEX_ERR;

	switch (item->action) {
		case AS_SMD_ACTION_SET:
			{
				params = item->value;
				bool smd_op = false;
				if (as_info_parse_params_to_sindex_imd(params, &imd, NULL, true, &smd_op)){
					goto ERROR;
				}
				ns     = as_namespace_get_byname(imd.ns_name);
				retval = as_sindex_create_check_params(ns, &imd);

				if(retval != AS_SINDEX_OK){
					cf_info(AS_SINDEX, "Callback from paxos master for validation failed with error code %d", retval);
					goto ERROR;
				}
				break;
			}
		case AS_SMD_ACTION_DELETE:
			{
				char * key_dup = cf_strdup(item->key);
				// Get ns name 
				// Using strtok_r instead of strtok due to MT environment.
				char * saveptr = NULL;
				char * ns_tmpname    = strtok_r(key_dup, ":", &saveptr);
				if (!ns_tmpname) {
					cf_warning(AS_SINDEX, "Failed to extract namspace name from SMD delete item value");
					retval = AS_SINDEX_ERR;
					cf_free(key_dup);
					goto ERROR;
				}
				else {
					cf_debug(AS_SINDEX, "Extracted namespace name from SMD delete item value -> %s", ns_tmpname);
				}

				// Get index name
				char * i_tmpname      = strtok_r(NULL, ":", &saveptr);
				if (!i_tmpname) {
					cf_warning(AS_SINDEX, "Failed to extract index name from SMD delete item value");
					retval = AS_SINDEX_ERR;
					cf_free(key_dup);
					goto ERROR;
				}
				else {
					cf_debug(AS_SINDEX, "Extracted index name from SMD delete item value -> %s", i_tmpname);
				}

				imd.ns_name = cf_strdup(ns_tmpname);
				imd.iname   = cf_strdup(i_tmpname);
				ns          = as_namespace_get_byname(imd.ns_name);
				if (as_sindex_lookup_by_iname(ns, imd.iname, AS_SINDEX_LOOKUP_FLAG_NORESERVE | AS_SINDEX_LOOKUP_FLAG_ISACTIVE)) {
					retval = AS_SINDEX_OK;
				}
				else {
					retval = AS_SINDEX_ERR_NOTFOUND;
				}
				cf_free(key_dup);
				break;
			}
	}

ERROR:	
	as_sindex_imd_free(&imd);
	return retval;
}

int
as_sindex_cfg_var_hash_reduce_fn(void *key, void *data, void *udata)
{
	// Parse through the entire si_cfg_array, do an shash_delete on all the valid entries
	// How do we know if its a valid-entry ? valid-entries get marked by the valid_flag in
	// the process of doing as_sindex_create() called by smd.
	// display a warning for those that are not valid and finally, free the entire structure

	as_sindex_config_var *si_cfg_var = (as_sindex_config_var *)data;

	if (! si_cfg_var->conf_valid_flag) {
		cf_warning(AS_SINDEX, "No secondary index %s found. Configuration stanza for %s ignored.", si_cfg_var->name, si_cfg_var->name);
	}

	return 0;
}

// Global flag to signal that all secondary index SMD is restored.
bool g_sindex_smd_restored = false;

/*
 * This function is called when the SMD has resolved the correct state of
 * metadata. This function needs to, based on the value, looks at the current
 * state of the index and trigger requests to secondary index to do the
 * needful. At the start of time there is nothing in sindex and this code
 * comes and setup indexes
 *
 * Expectation. SMD is responsible for persisting data and communicating back
 *              to sindex layer to create in-memory structures
 *
 *
 * Description: To perform sindex operations(ADD,MODIFY,DELETE), through SMD
 * 				This function called on every node, after paxos master decides
 * 				the final version of the sindex to be created. This is the final
 *				version and the only allowed version in the sindex.Operations coming
 *				to this function are least expected to fail, ideally they should
 *				never fail.
 *
 * Parameters:
 * 		module:             SINDEX_MODULE
 * 		as_smd_item_list_t: list of action items, to be performed on sindex.
 * 		udata:              ??
 *
 * Returns:
 * 		always 0
 *
 * Synchronization:
 * 		underlying secondary index all needs to take corresponding lock and
 * 		SMD is today single threaded no sync needed there
 */
int
as_sindex_smd_accept_cb(char *module, as_smd_item_list_t *items, void *udata, uint32_t accept_opt)
{
	if (accept_opt & AS_SMD_ACCEPT_OPT_CREATE) {
		cf_debug(AS_SINDEX, "all secondary index SMD restored");
		g_sindex_smd_restored = true;
		return 0;
	}

	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));
	char         * params = NULL;
	as_namespace * ns     = NULL;
	imd.post_op = 0;
	
	for (int i = 0; i < items->num_items; i++) {
		params = items->item[i]->value;
		switch (items->item[i]->action) {
			// TODO: Better handling of failure of the action items list
			case AS_SMD_ACTION_SET:
			{
				bool smd_op = false;
				if (as_info_parse_params_to_sindex_imd(params, &imd, NULL, true, &smd_op)) {
					cf_info(AS_SINDEX,"Parsing the index metadata for index creation failed");
					break;
				}

				ns         = as_namespace_get_byname(imd.ns_name);
				if (as_sindex_exists_by_defn(ns, &imd)) {
					cf_detail(AS_SINDEX, "Index with the same index defn already exists.");
					// Fail quietly for duplicate sindex requests
					continue;
				}
				// Pessimistic --Checking again. This check was already done by the paxos master.
				int retval = as_sindex_create_check_params(ns, &imd);
				if (retval != AS_SINDEX_OK) {
						// Two possible cases for reaching here
						// 1. It is possible that secondary index is not active hence defn check
						//    fails but params check pick up sindex in destroy state as well.
						// 2. SMD thread is single threaded ... not sure how can above definition
						//    check fail but params check pass. But just in case it does bail out
						//    destroy and recreate (Accept the final version). think !!!!
						cf_detail(AS_SINDEX, "Index creation failed. Error %d Dropping the index due to cluster state change", retval);
						imd.post_op = 1;
						as_sindex_destroy(ns, &imd);
				}
				else {
					retval = as_sindex_create(ns, &imd, true);
				}
				break;
			}
			case AS_SMD_ACTION_DELETE:
			{
				char * key_dup = cf_strdup(items->item[i]->key);
				
				char * saveptr = NULL;
				// Using strtok_r instead of strtok due to MT environment.
				// Get ns name 
				char * ns_tmpname    = strtok_r(key_dup, ":", &saveptr);
				if (!ns_tmpname) {
					cf_warning(AS_SINDEX, "Failed to extract namspace name from SMD delete item value");
					cf_free(key_dup);
					break;
				}
				else {
					cf_debug(AS_SINDEX, "Extracted namespace name from SMD delete item value -> %s", ns_tmpname);
				}

				// Get index name
				char * i_tmpname      = strtok_r(NULL, ":", &saveptr);
				if (!i_tmpname) {
					cf_warning(AS_SINDEX, "Failed to extract index name from SMD delete item value");
					cf_free(key_dup);
					break;
				}
				else {
					cf_debug(AS_SINDEX, "Extracted index name from SMD delete item value -> %s", i_tmpname);
				}

				imd.ns_name = cf_strdup(ns_tmpname);
				imd.iname   = cf_strdup(i_tmpname);
				ns          = as_namespace_get_byname(imd.ns_name);
				as_sindex_destroy(ns, &imd);
				cf_free(key_dup);
				break;
			}
		}
	}

	// Check if the incoming operation is merge. If it's merge
	// After merge resolution of cluster, drop the local sindex definitions which are not part
	// of the paxos principal's sindex definition.
	if (accept_opt & AS_SMD_ACCEPT_OPT_MERGE) {
		for (int k = 0; k < g_config.namespaces; k++) { // for each namespace
			as_namespace *local_ns = g_config.namespace[k];

			if (local_ns->sindex_cnt > 0) {
				as_sindex *del_list[AS_SINDEX_MAX];
				int        del_cnt = 0;
				SINDEX_GRLOCK();
				
				// Create List of Index to be Deleted
				for (int i = 0; i < AS_SINDEX_MAX; i++) {
					as_sindex *si = &local_ns->sindex[i];
					if (si && si->imd && as_sindex_isactive(si)) {
						int found     = 0;
						SINDEX_RLOCK(&si->imd->slock);
						for (int j = 0; j < items->num_items; j++) {
							char key[256];
							sprintf(key, "%s:%s", si->imd->ns_name, si->imd->iname);
							cf_detail(AS_SINDEX,"Item key %s \n", items->item[j]->key);

							if (strcmp(key, items->item[j]->key) == 0) {
								found = 1;
								cf_detail(AS_SINDEX, "Item found in merge list %s \n", si->imd->iname);
								break;
							}
						}
						SINDEX_UNLOCK(&si->imd->slock);

						if (found == 0) { // Was not found in the merged list from paxos principal
							AS_SINDEX_RESERVE(si);
							del_list[del_cnt] = si;
							del_cnt++;
						}
					}
				}

				SINDEX_GUNLOCK();

				// Delete Index
				for (int i = 0 ; i < del_cnt; i++) {
					if (del_list[i]) {
						as_sindex_destroy(local_ns, del_list[i]->imd);
						AS_SINDEX_RELEASE(del_list[i]);
						del_list[i] = NULL;
					}
				}
			}
		}
	}

	as_sindex_imd_free(&imd);

	return(0);
}

// Set the binid'th bit of the bin_has_sindex array.
// It is always called under SINDEX GWLOCK.
void
as_sindex_set_binid_has_sindex(as_namespace *ns, int binid)
{
	int index     = binid / 32;
	uint32_t temp = ns->binid_has_sindex[index];
	temp         |= (1 << (binid % 32));
	ns->binid_has_sindex[index] = temp;
}

// Tries to reset the binid'th bit of bin_has_sindex array.
// It is always called under SINDEX GWLOCK
void
as_sindex_reset_binid_has_sindex(as_namespace *ns, int binid)
{
	// Iterate over all sindex to check if any other bin with same id has sindex.
	int i          = 0;
	int j          = 0;
	as_sindex * si = NULL;

	while (i < AS_SINDEX_MAX && j < ns->sindex_cnt) {
		si = &ns->sindex[i];
		if (si != NULL) {
			if (si->state == AS_SINDEX_ACTIVE) {
				j++;
				if (si->imd->binid[0] == binid) {
					return;
				}
			}
		}
		i++;
	}

	int index     = binid / 32;
	uint32_t temp = ns->binid_has_sindex[index];
	temp         &= ~(1 << (binid % 32));
	ns->binid_has_sindex[index] = temp;
}

// Check the binid'th bit of bin_has_sindex array. 
// 		If set, bin with bin-id as binid has atleast one sindex over it.
// 		Else not.
// It is always called under SINDEX GRLOCK
bool
as_sindex_binid_has_sindex(as_namespace *ns, int binid)
{
	int index      = binid / 32;
	uint32_t temp  = ns->binid_has_sindex[index];
	return (temp & (1 << (binid % 32))) ? true : false;
}

as_sindex_status
as_sindex_add_mapkey_in_path(as_sindex_metadata * imd, char * path_str, int start, int end)
{
	if (end < start) {
		return AS_SINDEX_ERR;
	}

	int path_length = imd->path_length;
	char int_str[20];
	strncpy(int_str, path_str+start, end-start+1);
	int_str[end-start+1] = '\0';
	char * str_part;
	imd->path[path_length-1].value.key_int = strtol(int_str, &str_part, 10);
	if (str_part == int_str || (*str_part != '\0')) {
		imd->path[path_length-1].value.key_str  = cf_strndup(int_str, strlen(int_str)+1);
		imd->path[path_length-1].mapkey_type = AS_PARTICLE_TYPE_STRING;
	}
	else {
		imd->path[path_length-1].mapkey_type = AS_PARTICLE_TYPE_INTEGER;	
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_add_listelement_in_path(as_sindex_metadata * imd, char * path_str, int start, int end)
{
	if (end < start) {
		return AS_SINDEX_ERR;
	}
	int path_length = imd->path_length;
	char int_str[10];
	strncpy(int_str, path_str+start, end-start+1);
	int_str[end-start+1] = '\0';
	char * str_part;
	imd->path[path_length-1].value.index = strtol(int_str, &str_part, 10);
	if (str_part == int_str || (*str_part != '\0')) {
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_parse_subpath(as_sindex_metadata * imd, char * path_str, int start, int end)
{
	int path_len = strlen(path_str);
	bool overflow = end >= path_len ? true : false;

	if (start == 0 ) {
		if (overflow) {
			imd->bnames[0] = cf_strndup(path_str+start, end-start);	
		}
		else if (path_str[end] == '.') {
			imd->bnames[0] = cf_strndup(path_str+start, end-start);
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_MAP;	
		}
		else if (path_str[end] == '[') {
			imd->bnames[0] = cf_strndup(path_str+start, end-start);
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else if (path_str[start] == '.') {
		if (overflow) {
			if (as_sindex_add_mapkey_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
		}
		else if (path_str[end] == '.') {
			// take map value
			if (as_sindex_add_mapkey_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
			// add type for next node in path
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_MAP;	
		}
		else if (path_str[end] == '[') {
			// value
			if (as_sindex_add_mapkey_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
			// add type for next node in path
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else if (path_str[start] == '[') {
		if (!overflow && path_str[end] == ']') {
			//take list value
			if (as_sindex_add_listelement_in_path(imd, path_str, start+1, end-1) != AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else if (path_str[start] == ']') {
		if (end - start != 1) {
			return AS_SINDEX_ERR;
		}
		else if (overflow) {
			return AS_SINDEX_OK;
		}
		if (path_str[end] == '.') {
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_MAP;
		}
		else if (path_str[end] == '[') {
			imd->path_length++;
			imd->path[imd->path_length-1].type = AS_PARTICLE_TYPE_LIST;
		}
		else {
			return AS_SINDEX_ERR;
		}
	}
	else {
		return AS_SINDEX_ERR;
	}
	return AS_SINDEX_OK;
}
/*
 * This function parses the path_str and populate array of path structure in 
 * imd.
 * Each element of the path is the way to reach the the next path.
 * For e.g 
 * bin.k1[1][0]
 * array of the path structure would be like - 
 * path[0].type = AS_PARTICLE_TYPE_MAP . path[0].value.key_str = k1  path[0].value.ke
 * path[1].type = AS_PARTICLE_TYPE_LIST . path[1].value.index  = 1 
 * path[2].type = AS_PARTICLE_TYPE_LIST . path[2].value.index  = 0
*/
as_sindex_status
as_sindex_extract_bin_path(as_sindex_metadata * imd, char * path_str)
{
	int    path_len    = strlen(path_str);
	int    start       = 0;
	int    end         = 0;
	if (path_len > AS_SINDEX_MAX_PATH_LENGTH) {
		cf_warning(AS_SINDEX, "Bin path length exceeds the maximum allowed.");
		return AS_SINDEX_ERR;
	}
	// Iterate through the path_str and search for character (., [, ])
	// which leads to sublevels in maps and lists
	while (end < path_len) {		
		if (path_str[end] == '.' || path_str[end] == '[' || path_str[end] == ']') {	
			if (as_sindex_parse_subpath(imd, path_str, start, end)!=AS_SINDEX_OK) {
				return AS_SINDEX_ERR;
			}
			start = end;
			if (imd->path_length >= AS_SINDEX_MAX_DEPTH) {
				cf_warning(AS_SINDEX, "Bin position depth level exceeds the max depth allowed %d", AS_SINDEX_MAX_DEPTH);
				return AS_SINDEX_ERR;
			}
		}
		end++;
	}
	if (as_sindex_parse_subpath(imd, path_str, start, end)!=AS_SINDEX_OK) {
		return AS_SINDEX_ERR;
	}
/*
// For debugging
	cf_info(AS_SINDEX, "After parsing : bin name: %s", imd->bnames[0]);
	for (int i=0; i<imd->path_length; i++) {
		if(imd->path[i].type == AS_PARTICLE_TYPE_MAP ) {
			if (imd->path[i].key_type == AS_PARTICLE_TYPE_INTEGER) {
				cf_info(AS_SINDEX, "map key_int %d", imd->path[i].value.key_int);
			}
			else if (imd->path[i].key_type == AS_PARTICLE_TYPE_STRING){
				cf_info(AS_SINDEX, "map key_str %s", imd->path[i].value.key_str);
			}
			else {
				cf_info(AS_SINDEX, "ERROR EEROR EERROR ERRROR REERROR");
			}
		}
		else{
			cf_info(AS_SINDEX, "list index %d", imd->path[i].value.index);
		}
	}
*/
	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_extract_bin_from_path(char * path_str, char *bin)
{
	int    path_len    = strlen(path_str);
	int    end         = 0;
	if (path_len > AS_SINDEX_MAX_PATH_LENGTH) {
		cf_warning(AS_SINDEX, "Bin path length exceeds the maximum allowed.");
		return AS_SINDEX_ERR;
	}

	while (end < path_len && path_str[end] != '.' && path_str[end] != '[' && path_str[end] != ']') {
		end++;
	}

	if (end > 0 && end < AS_ID_BIN_SZ) {
		strncpy(bin, path_str, end);
		bin[end] = '\0';
	}
	else {
		return AS_SINDEX_ERR;
	}

	return AS_SINDEX_OK;
}

as_sindex_status
as_sindex_destroy_value_path(as_sindex_metadata * imd)
{
	for (int i=0; i<imd->path_length; i++) {
		if (imd->path[i].type == AS_PARTICLE_TYPE_MAP && 
				imd->path[i].mapkey_type == AS_PARTICLE_TYPE_STRING) {
			cf_free(imd->path[i].value.key_str);
		}
	}
	return AS_SINDEX_OK;
}

/*
 * This function checks the existence of path stored in the sindex metadata
 * in a bin
 */
as_val *
as_sindex_extract_val_from_path(as_sindex_metadata * imd, as_val * v)
{
	if (!v) {
		return NULL;
	}

	as_val * val = v;
	
	as_particle_type imd_btype = as_sindex_pktype(imd);
	if (imd->path_length == 0) {
		goto END;
	}
	as_sindex_path *path = imd->path;
	for (int i=0; i<imd->path_length; i++) {
		switch(val->type) {
			case AS_STRING:
			case AS_INTEGER:
				return NULL;
			case AS_LIST: {
				if (path[i].type != AS_PARTICLE_TYPE_LIST) {
					return NULL;
				}
				int index = path[i].value.index;
				as_arraylist* list  = (as_arraylist*) as_list_fromval(val);
				as_arraylist_iterator it;
				as_arraylist_iterator_init( &it, list);
				int j = 0;
				while( as_arraylist_iterator_has_next( &it) && j<=index) {
					val = (as_val*) as_arraylist_iterator_next( &it);
					j++;
				}
				if (j-1 != index ) {
					return NULL;
				}
				break;
			}
			case AS_MAP: {
				if (path[i].type != AS_PARTICLE_TYPE_MAP) {
					return NULL;
				}
				as_map * map = as_map_fromval(val);
				as_val * key;
				if (path[i].mapkey_type == AS_PARTICLE_TYPE_STRING) {
					key = (as_val *)as_string_new(path[i].value.key_str, false);
				}
				else if (path[i].mapkey_type == AS_PARTICLE_TYPE_INTEGER) {
					key = (as_val *)as_integer_new(path[i].value.key_int);
				}
				else {
					cf_warning(AS_SINDEX, "Possible false data in sindex metadata");
					return NULL;
				}
				val = as_map_get(map, key);
				if (key) {
					as_val_destroy(key);
				}
				if ( !val ) {
					return NULL;
				}
				break;
			}
			default:
				return NULL;
		}
	}

END:
	if (imd->itype == AS_SINDEX_ITYPE_DEFAULT) {
		if (val->type == AS_INTEGER && imd_btype == AS_PARTICLE_TYPE_INTEGER) {
			return val;
		}
		else if (val->type == AS_STRING && imd_btype == AS_PARTICLE_TYPE_STRING) {
			return val;
		}
	}
	else if (imd->itype == AS_SINDEX_ITYPE_MAPKEYS ||  imd->itype == AS_SINDEX_ITYPE_MAPVALUES) {
		if (val->type == AS_MAP) {
			return val;
		}
	}
	else if (imd->itype == AS_SINDEX_ITYPE_LIST) {
		if (val->type == AS_LIST) {
			return val;
		}
	}
	return NULL;
}

// Sindex ticker start
void
as_sindex_ticker_start(as_namespace * ns, as_sindex * si)
{
	cf_info(AS_SINDEX, "Sindex-ticker start: ns=%s si=%s job=%s", ns->name ? ns->name : "<all>", 
			si ? si->imd->iname : "<all>", si ? "SINDEX_POPULATE" : "SINDEX_POPULATEALL");

}
// Sindex ticker
void
as_sindex_ticker(as_namespace * ns, as_sindex * si, uint64_t n_obj_scanned, uint64_t start_time)
{
	const uint64_t sindex_ticker_obj_count = 500000;

	if (n_obj_scanned % sindex_ticker_obj_count == 0 && n_obj_scanned != 0) {
		// Ticker can be dumped from here, we'll be in this place for both
		// sindex populate and populate-all.
		// si memory gets set from as_sindex_reserve_data_memory() which in turn gets set from :
		// ai_btree_put() <- for every single sindex insertion (boot-time/dynamic)
		// as_sindex_create() : for dynamic si creation, cluster change, smd on boot-up.

		uint64_t si_memory   = 0;
		char   * si_name     = NULL;
		
		if (si) {
			si_memory        = cf_atomic64_get(si->data_memory_used);
			si_name          = si->imd->iname;
		}
		else {
			si_memory        = (uint64_t)cf_atomic_int_get(ns->sindex_data_memory_used);
			si_name          = "<all>";
		}

		uint64_t n_objects       = (uint64_t)cf_atomic_int_get(ns->n_objects);
		uint64_t pct_obj_scanned = n_objects == 0 ? 100 : ((n_obj_scanned * 100) / n_objects);
		uint64_t elapsed         = (cf_getms() - start_time);
		uint64_t est_time        = (elapsed * n_objects)/n_obj_scanned - elapsed;

		cf_info(AS_SINDEX, " Sindex-ticker: ns=%s si=%s obj-scanned=%"PRIu64" si-mem-used=%"PRIu64""
				" progress=%d%% est-time=%"PRIu64" ms",
				ns->name, si_name, n_obj_scanned, si_memory, pct_obj_scanned, est_time);
	}
}

// Sindex ticker end
void
as_sindex_ticker_done(as_namespace * ns, as_sindex * si, uint64_t start_time)
{
	uint64_t si_memory   = 0;
	char   * si_name     = NULL;

	if (si) {
		si_memory        = cf_atomic64_get(si->data_memory_used);
		si_name          = si->imd->iname;
	}
	else {
		si_memory        = (uint64_t)cf_atomic_int_get(ns->sindex_data_memory_used);
		si_name          = "<all>";
	}

	cf_info(AS_SINDEX, "Sindex-ticker done: ns=%s si=%s si-mem-used=%"PRIu64" elapsed=%"PRIu64" ms", 
				ns->name, si_name, si_memory, cf_getms() - start_time);

}
