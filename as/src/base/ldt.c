/*
 * ldt.c
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

/* LDT Module
 *  BASICS
 *
 *    Structure: LDT bins are the bin types which can store large or linked
 *               data. The structures as
 *
 *              +------------------------------------------------------------
 * LDT RECORD   |       LDT BIN                  |                |     .....
 *              +------------------------------------------------------------
 *                  |  |        |       |
 *           +------+  +-----+  +-----+ +-----+
 *           | ESR  |  | SUB |  | SUB | | SUB |
 *           +------+  +-----+  +-----+ +-----+
 *                      |   |            |   |
 *                +-----+   +-----+      |   |
 *                | SUB |   | SUB |      |   |
 *                +-----+   +-----+      |   |
 *                                       |   |
 *                                 +-----+   +-----+
 *                                 | SUB |   | SUB |
 *                                 +-----+   +-----+
 *
 *  There are three parts to it
 *
 *  LDT RECORD : Which is the parent record which contain LDT bin type.
 *  LDT ESR    : It is LDT existence record one corresponding to each LDT bin.
 *  LDT SUBREC : Many different LDT subrecords which linked together to
 *               implement Large(linked) data type. The structure/organization
 *               and links between these are user programmable.
 *  LDT SUB    : SUBREC + ESR are collectively called SUB
 *
 *  LDT bin type is user exposed type AS_LDT. This type of bin can only be
 *  manipulated through System User Defined Function (SDF), through specially
 *  implemented functions.
 *
 *  Three types of maps
 *
 *  Record Property Map:  RPM_*
 *  Property Map:         PM__*
 *
 *  HIDDEN BIN TO STORE METADATA INFO
 *
 *              +------------------------------------------------------------
 * LDT RECORD   | REC_LDT_CTRL_BIN type:RPM |  LDT BIN  | LDT BIN | ....
 *              +------------------------------------------------------------
 *
 *              +--------------------------------
 * LDT BIN      | list {type:PM, type:M} | .....
 *              +--------------------------------
 *
 *              +------------------------------------------------------
 * LDT SUBREC   | SUBREC_PROP_BIN type:PM | other lua specific stuff | .....
 *              +-----------------------------------------------------
 *
 *              +------------------------------------------------------
 * LDT ESR      | SUBREC_PROP_BIN type:PM | ???
 *              +-----------------------------------------------------
 *
 *
 *  Example of implementation of (Large) Linked Data Type is LSTACK/LLIST/LSET.
 *
 *  ldt_aerospike.c ldt_record.c implement as_aerospike / as_record to expose
 *  primitives which are used by LUA to implement LDT.
 *
 *  This file implements functionality which is required for handling server
 *  side functionality like maintenance / expiry / expiration / deletion /
 *  read / write / migration / duplication resolution for the aerospike
 *  records containing Large (linked) data type bins.
 *
 *
 *  Major Changes:
 *
 *   LDT is different from other data type in following ways which makes life little
 *   more fun !!! :)
 *
 *    1. They spread over multiple record, so making sure entire LDT change moves when 
 *       doing and migrations and replication. This could span multiple to-fro trip between 
 *       nodes.
 *    2. LDT are generally huge sized we need to minimize moving them as much as possible.
 *    3. Generally operation in the LDT are _NOT_ idempotent (unless user can enforce by 
 *       himself e.g unique key in Llist) ...
 *
 * Things to know
 *
 * - LDT data is organized as, parent records and subrecords (connected to form entire large data). 
 *   Partition structure will have its own tree from which the LDT subrecord will be hanging. This 
 *   is to make sure LDT record and LDT subrecord has different search space. This is avoid digest 
 *   collision between two spaces.
 *
 * - LDT Version:
 *   Each LDT has version in it which is different from the what is in the partition. This version 
 *   is per LDT, which gets regenerated every time LDT data is migrated from a partition version to 
 *   another partition version. (signifying something changed in the record). 
 *
 *   This version is embedded in the child sub record. So child subrecord digest look list <SUBD:version> 
 *   (where last 8 bytes is version). What it means is at any point in time same subrecord can exist 
 *   as different digest in the Aerospike but the one which version matching parent's version is valid 
 *   one, others are cleaned up by the background GC task. 
 *
 * - The LDT subrecord has its own digest to identify it. Following logic is used to make sure that 
 *   in entire life of server no two subrecord digest which is generated repeat.  
 *   [See ldt_aerospike.c for randomizer function ]
 *
 *   - 12 bits of partition distinguishing bits and hence LDT_SUB for record in two different partition 
 *     are in different space. This also means the when LDT_SUB digest is generated on two different 
 *     nodes they can never collide as at any point of time the write to record in a given partition
 *     happens only on one node.
 *
 *   - 2 bytes for lock has [2-3] for same lock
 *
 *   - 3 random bytes to make sure of uniqueness. At a given clock time on multiple node. [4-6]. The 
 *     seed for this random number if picked from the digest itself to make it thread safe for a digest.
 *
 *   - 1 byte to for the storage distribution ... [7]. We want to make sure LDT subrecord falls on the 
 *     same device as the LDT record
 *
 *   - 6 bytes are based on system clock [8-14]
 *
 *   - 6 bytes of version which is generated at partition version creation
 *
 * - Read/Write: Because LDT are huge size we cannot ship get op (duplicate resolution) for entire 
 *   record. So for the records with LDT bins the request is send to all the duplicates [writes/UDF] 
 *   as well, with the protection at the initiating node (master if it is sync / origin [acting 
 *   master node] in case of proxy). In case op is read the result is returned back to the originating 
 *   node along with the generation and winner result is sent back to the client. In case of write 
 *   after applying UDF then normal replication is triggered to send result to replica set along with 
 *   generation. And the winner generation wins. Need better solution. Also it is necessary to make
 *   sure that the ops have order what it means is when the op is send it gets executed unless cluster 
 *   view has changed. 
 *   [ ldt.c / proxy.c / thr_rw.c to see the shipped op logic ]
 *
 *    NB: See code for the details of locking and protection mechanism. And
 *        ACID semantics
 *    NB: See the code comments for the details of reducing the network bandwidth.
 *
 *   Algorithm
 *   =========
 *   -- If there are no duplicates; perform LDT UDF execution and replicate
 *   -- If there are duplicates; find the winning node i.e node with winning LDT record based on parent 
 *      generation + ttl out of all the duplicate versions (it could be master / primary version / 
 *      zombie). Ship the operation to that node using proxy subsystem. 
 *   -- Apply LDT UDF on the winning node and replicate to (master / replica )
 * 
 *
 * - Replication: Replication at both at the time of the duplicates/migration and at the normal runtime 
 *   would pack entire changes done to LDT into one packet and send it over to the replica set. This 
 *   is to make sure the Atomic semantics becomes less dependent on the network "finickiness". And
 *   make sure entire thing makes to replica [We still not solved the case where entire thing either 
 *   makes to the storage or none makes it. But that is independent additional change]
 *
 *   Algorithm
 *   =========
 *   Replicating data from Partition X -> Partition Y write subrecord with version as in destination 
 *   node. And when doing so for Partition X-> Partition Y then write subrecord with version as in source node.
 *
 *   -- After write is applied pack up parent and all the modified sub record in single message along with 
 *      -- The source partition's current version 
 *      -- The source current outgoing migration LDT version.
 *      and send it to master (in case write happens on non-master node) replicas.
 *   -- At the destination on receiving the replication request check source partition version and destination 
 *      partition version. 
 *      - If the replication request is coming from partition version which is different then
 *        - For LDT parent record 
 *        - Skip replication unless there is incoming migration from replication source node and replication 
 *          is in the RECORD mode.
 *        - If not migrating it will either be happening in future in that case replicating parent before subrec 
 *          is order violation or would have been already migrated in this case replication partition would match.
 *      - For LDT subrec replicate with source ldt version. This could be optimized for certain cases where 
 *        migration is expected to start in future ... But for now just writing it.
 *
 *
 * - Migration: At the source of migrations the migration runs reduce in two phase
 *     Phase 1: Reduce the subrecord tree 
 *     Phase 2: Reduce record tree and send all the parent records. 
 *
 *   When migrating data from one partition version to another unless entire record is moved existing 
 *   subrecord cannot be overwritten at the destination... this could cause data corruption if the 
 *   parent never makes it. So whenever the subrecord is moved from one partition version to another 
 *   new version for the subrecord is introduced ...
 
 *   Every time partition migrates in it will generate a new version for the given LDT. This is to make
 *   sure that all the incoming subrecord will have new digest so the subrecord never collide and create 
 *   new copies. When the parent record of the winner LDT moves in it will have the partition version 
 *   stamped in it. All the subrecord will be lazily / aggressively cleaned up from the system.
 *    [ See migrate.c for the detail code for this ]
 *
 *   Algorithm
 *   =========
 *   - While migrating,  all subrecord should make it to the destination node before the parent node makes it
 *   - While there is incoming or outgoing migration writes / read from the record should not fail.
 *   - Migration from Partition X -> Partition Y creates new version of subrecord. System needs to be 
 *     protected it from being GC. 
 *
 *   Migration Source
 *   - Generate migration LDT version number (only done once per outgoing migration at the start of migration and
 *     is used by all LDT in that migration cycle) and send over to the node along with MIG_START message and node name.
 *   - Set partition state to MIG_SUBRECORD_TX and Reduce sub record tree and send sub record along with
 *     - Sub record generation
 *     - Current migration ldt version
 *     - Parent record generation and ttl
 *   - Set partition state to MIG_RECORD_TX and Reduce parent record tree and send parent record with
 *     - Current migration version
 *     - Parent record generation and ttl.
 *   - When migration finishes or aborts, Set partition MIG_NONE_TX.
 *   - Restart from step one after new partition migration.
 *
 *   Migration Destination
 *   - On receiving MIG_START request create mig object and track node wise current incoming version number.
 *   - Move partition rx_state to MIG_SUBRECORD_RX.
 *   - On receiving incoming SUB_RECORD migrate, check if the parent has winning generation if yes write the sub 
 *     record with current incoming ldt version. If not drop the incoming.
 *   - If this is first RECORD then move partition to state MIG_RECORD_RX.
 *   - On receiving incoming RECORD, check for the winner generation if yes then write the Parent record with 
 *     new version. If not drop the incoming.
 *   - When migration finishes or aborts remove entry node->current incoming ldt version entry from tracking DS
 *     and move partition to MIG_RECORD_NONE_TX state.
 *     
 * - Expiry / Eviction : Nsup threads only runs on the LDT record tree. If the parent record is deleted then 
 *   all the existence subrecord are deleted from the sub record tree. See the logic the version cleaned and 
 *   delete thread logic for sub record tree for details of how subrecord gets cleaned up. (NOTE: NSup refers 
 *   to the Namespace Supervisor thread, which takes care of record expiration and eviction.)
 *
 *  - Warm restart : Both the record and subrecord are tree are allocated from arena shared memory .. attaching 
 *    at the boot time simply bring back the entire state of system as it was when the node went down.
 *
 *  - Subrecord Version clean / delete: A background thread walk through the sub record main tree and keep 
 *    checking if the existence record is present for the LDT subrecord ... if it exists then it checks to see 
 *    if the LDT parent record version matches. If it does not match the version of LDT subrecord is cleaned up. 
 *    If it matches the version is retained. In case existence sub record is missing the subrecord is cleaned 
 *    up from the sub record tree.
 *
 *    NB: When the bin is deletes the ESR record is simply deleted from the sub record tree. And LDT subrecords 
 *    are lazily cleaned up.
 *
 *    See the details in code in case two different ESR show up for two different versions of LDT.
 *
 *    Algorithm
 *    =========
 *    - Reduce sub record tree and check for
 *      - If parent is around
 *      - if ESR is around
 *      - if parent version matches sub record version.
 *    - Skip all the above check if the current partition state is SUBRECORD_RX or RECORD_RX and tracking 
 *      DS has entry for the incoming ldt version entry.
 *
 *  - Defrag : Defrag when walking through storage looks up key in both record
 *    tree and sub record tree. If not found the record is candidate for defrag.
 */

#include "base/ldt.h"

#include <unistd.h>

#include "aerospike/as_msgpack.h"
#include "aerospike/as_types.h"

#include "base/datamodel.h"
#include "base/ldt_record.h"
#include "fabric/fabric.h"
#include "fabric/migrate.h"
#include "fabric/partition.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/udf.h"

// Use this to turn on/off debug sections
#define DEBUG false

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// LDT specific Property Map (PM) Fields:
// One Prop Map per LDT (in the main record), and one Prop Map per subrecord
// bin.  For Sub Records, the property map is in bin: "SR_PROP_BIN", and is
// referenced by variable SUBREC_PROP_BIN (in both Aerospike C and Lua code).
// In Top Records, the contents of the LDT Bin  comprises a list of two maps.
// The first map (at location ldtList[1]) is the the Property Map (PM)
// and is same for all LDTs.  The second map (at location ldtList[2]) is a
// control map that is specific to the LDT (LSet, LList, LMap, LStack).
//
// We can access the Property Map from either the Lua code:   e.g.
// local ldtList = topRec[bin];
// local propMap = ldtList[1];
// local esrDigest = propMap[PM_EsrDigest];
//
// Or, we can access it from the C code:
// (NOTE: This needs to change if we change the code from which this example
//  was taken.)
//	char * prop_bin_name = SUBREC_PROP_BIN;
//	int namesz = strlen( prop_bin_name );
//	as_bin * binp = as_bin_get( rd, prop_bin_name, namesz );
//	as_val * valp = as_val_frombin( binp );
//	as_map * mapp = as_map_fromval(valp); // keep this step for debugging
//	as_bytes * digest_bytes = (as_bytes *) as_map_get( mapp, PM_ParentDigest );

//-- ------------------------------------------------------------------------
//-- Control Map Names: for Property Maps and Control Maps
//-- ------------------------------------------------------------------------
//-- Note:  All variables that are field names will be upper case.
//-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
//-- values -- within any given map.  They do NOT have to be unique across
//-- the maps (and there's no need -- they serve different purposes).
//-- Note that we've tried to make the mapping somewhat cannonical where
//-- possible.
//


//------------------------------------------------------------------------------
// Does a UDF package and op name correspond to one of our internal LDT UDFs?
//

const char * LDT_UDF_PACKAGE_NAMES[] = {
		"llist",
		"lmap",
		"lset",
		"lstack"
};

const uint32_t NUM_LDT_UDF_PACKAGE_NAMES =
		sizeof(LDT_UDF_PACKAGE_NAMES) / sizeof(const char*);

typedef struct ldt_op_props_s {
	const char *	op_name;
	int				op_type;
} ldt_op_props;

const ldt_op_props LLIST_OP_PROPS[] = {
		{ "add",			LDT_WRITE_OP },
		{ "add_all",		LDT_WRITE_OP },
		{ "config",			LDT_READ_OP },
		{ "destroy",		LDT_WRITE_OP },		
		{ "exists",			LDT_READ_OP },
		{ "filter",			LDT_READ_OP },
		{ "find",			LDT_READ_OP },
		{ "take",			LDT_WRITE_OP },
		{ "find_from",		LDT_READ_OP },
		{ "take_from",		LDT_WRITE_OP },
		{ "find_first",		LDT_READ_OP },
		{ "take_first",		LDT_WRITE_OP },
		{ "find_last",		LDT_READ_OP },
		{ "take_last",		LDT_WRITE_OP },
		{ "find_range",		LDT_READ_OP },
		{ "take_range",		LDT_WRITE_OP },
		{ "find_range_lim",	LDT_READ_OP },
		{ "take_range_lim",	LDT_WRITE_OP },
		{ "get_capacity",	LDT_READ_OP },
		{ "ldt_exists",		LDT_READ_OP },
		{ "range",			LDT_READ_OP },
		{ "remove",			LDT_WRITE_OP },
		{ "remove_all",		LDT_WRITE_OP },
		{ "remove_range",	LDT_WRITE_OP },
		{ "scan",			LDT_READ_OP },
		{ "setPageSize",	LDT_WRITE_OP },
		{ "size",			LDT_READ_OP },
		{ "update",			LDT_WRITE_OP },
		{ "update_all",		LDT_WRITE_OP }
};

const ldt_op_props LMAP_OP_PROPS[] = {
		{ "config",			LDT_READ_OP },
		{ "destroy",		LDT_WRITE_OP },
		{ "filter",			LDT_READ_OP },
		{ "get",			LDT_READ_OP },
		{ "get_capacity",	LDT_READ_OP },
		{ "ldt_exists",		LDT_READ_OP },
		{ "put",			LDT_WRITE_OP },
		{ "put_all",		LDT_WRITE_OP },
		{ "remove",			LDT_WRITE_OP },
		{ "scan",			LDT_READ_OP },
		{ "set_capacity",	LDT_WRITE_OP },
		{ "size",			LDT_READ_OP }
};

const ldt_op_props LSET_OP_PROPS[] = {
		{ "add",			LDT_WRITE_OP },
		{ "add_all",		LDT_WRITE_OP },
		{ "config",			LDT_READ_OP },
		{ "destroy",		LDT_WRITE_OP },
		{ "exists",			LDT_READ_OP },
		{ "filter",			LDT_READ_OP },
		{ "get",			LDT_READ_OP },
		{ "get_capacity",	LDT_READ_OP },
		{ "ldt_exists",		LDT_READ_OP },
		{ "remove",			LDT_WRITE_OP },
		{ "scan",			LDT_READ_OP },
		{ "set_capacity",	LDT_WRITE_OP },
		{ "size",			LDT_READ_OP }
};

const ldt_op_props LSTACK_OP_PROPS[] = {
		{ "config",			LDT_READ_OP },
		{ "destroy",		LDT_WRITE_OP },
		{ "filter",			LDT_READ_OP },
		{ "get_capacity",	LDT_READ_OP },
		{ "ldt_exists",		LDT_READ_OP },
		{ "one",			LDT_READ_OP },
		{ "peek",			LDT_READ_OP },
		{ "push",			LDT_WRITE_OP },
		{ "push_all",		LDT_WRITE_OP },
		{ "same",			LDT_READ_OP },
		{ "set_capacity",	LDT_WRITE_OP },
		{ "size",			LDT_READ_OP }
};

// Order MUST match LDT_UDF_PACKAGE_NAMES:
const ldt_op_props * OP_LOOKUPS[] = {
		LLIST_OP_PROPS,
		LMAP_OP_PROPS,
		LSET_OP_PROPS,
		LSTACK_OP_PROPS
};

// Order MUST match LDT_UDF_PACKAGE_NAMES:
const uint32_t OP_PROPS_COUNTS[] = {
		sizeof(LLIST_OP_PROPS)	/ sizeof(ldt_op_props),
		sizeof(LMAP_OP_PROPS)	/ sizeof(ldt_op_props),
		sizeof(LSET_OP_PROPS)	/ sizeof(ldt_op_props),
		sizeof(LSTACK_OP_PROPS)	/ sizeof(ldt_op_props)
};

static int
lookup_op_type(const ldt_op_props * op_props, uint32_t max, const char *op_name)
{
	for (uint32_t i = 0; i < max; i++) {
		if (strcmp(op_props[i].op_name, op_name) == 0) {
			return op_props[i].op_type;
		}
	}

	return -1;
}

/*
 * Public function to ask whether specified UDF package name corresponds to any
 * of our internal LDT packages.
 *
 * Return values:
 *   -1 - not an internal package
 * >= 0 - index of an internal package
 */
int
as_ldt_package_index(const char *package_name)
{
	if (*package_name != 'l') {
		return -1;
	}

	for (uint32_t i = 0; i < NUM_LDT_UDF_PACKAGE_NAMES; i++) {
		if (strcmp(package_name, LDT_UDF_PACKAGE_NAMES[i]) == 0) {
			return i;
		}
	}

	return -1;
}

/*
 * Public function to ask op type of specified internal LDT UDF op name.
 *
 * Return values:
 * -1 - op name does not correspond to a known op
 *  0 - op is a read
 *  1 - op is a write
 */
int
as_ldt_op_type(int package_index, const char *op_name)
{
	if (package_index < 0) {
		return -1;
	}

	return lookup_op_type(OP_LOOKUPS[package_index],
			OP_PROPS_COUNTS[package_index], op_name);
}

//
//------------------------------------------------------------------------------


/*
 * Used by migration to generate version at the beginning of partition
 * migration... based on the MAC address and current clock time....
 * LDT version is 5byte value .... that would mean there can be around
 * 2^40 different versions ... reasonably big number
 *
 * Much of this logic depends on the fact that the system clocks are in sync.
 */
uint64_t
as_ldt_generate_version()
{
	// MAC address for starting bits
	uint64_t version = g_config.self_node;

	// clock for randomizer
	srand(cf_clock_getabsoluteus());

	// Randomize last 6 bytes
	version += (rand() + 1) & 0x0000FF;
	version += (rand() + 1) & 0x0000FF00;
	version += (rand() + 1) & 0x0000FF0000;
	version += (rand() + 1) & 0x0000FF000000;
	version += (rand() + 1) & 0x0000FF00000000;
	version += (rand() + 1) & 0x0000FF0000000000;
	return (version & 0x0000FFFFFFFFFFFF);
}

/*
 * Set version in the subrecord digest
 */
void
as_ldt_subdigest_setversion(cf_digest *dig, uint64_t version)
{
	int s = DIGEST_VERSION_START_POS;
	// overwrite the last 6 bytes
	cf_detail(AS_LDT, "Set Version %"PRIx64"", version);
	dig->digest[s]     = (version & 0xff0000000000) >> 40;
	dig->digest[s + 1] = (version & 0xff00000000) >> 32;
	dig->digest[s + 2] = (version & 0xff000000) >> 24;
	dig->digest[s + 3] = (version & 0xff0000) >> 16;
	dig->digest[s + 4] = (version & 0xff00) >> 8;
	dig->digest[s + 5] = (version & 0xff);
}

/*
 * Reset version in the subrecord digest
 */
void
as_ldt_subdigest_resetversion(cf_digest *dig)
{
	// overwrite the last 6 bytes
	for (int i = 0; i < LDT_VERSION_SZ; i++) {
		dig->digest[DIGEST_VERSION_START_POS + i] = 0;
	}
	return;
}


uint64_t
as_ldt_subdigest_getversion(cf_digest *dig)
{
	uint64_t version = 0;
	uint64_t temp = 0;
	temp = dig->digest[DIGEST_VERSION_START_POS];
	version += temp << 40;
	temp = dig->digest[DIGEST_VERSION_START_POS + 1];
	version += temp << 32;
	temp = dig->digest[DIGEST_VERSION_START_POS + 2];
	version += temp << 24;
	temp = dig->digest[DIGEST_VERSION_START_POS + 3];
	version += temp << 16;
	temp = dig->digest[DIGEST_VERSION_START_POS + 4];
	version += temp << 8;
	temp = dig->digest[DIGEST_VERSION_START_POS + 5];
	version += temp;

	return version;
}

void
as_ldt_subrec_storage_validate(as_storage_rd *rd, char *op)
{
	if (!as_ldt_record_is_sub(rd->r)) {
		cf_warning(AS_LDT, "as_ldt_subrec_storage_validate %s: LDT_INDEXBITS SUBREC bit not set in SUBREC index", op);
	}

	cf_digest esr_digest;
	cf_digest parent_digest;
	if (as_ldt_subrec_storage_get_digests(rd, &esr_digest, &parent_digest)) {
		cf_warning(AS_LDT, "as_ldt_subrec_storage_validate %s Parent or ESR digest not set in subrecord", op);
	}

	uint32_t  esr_pid = as_partition_getid(&esr_digest);
	uint32_t  parent_pid = as_partition_getid(&parent_digest);
	uint32_t  subrec_pid = as_partition_getid(&rd->r->keyd);
	cf_detail(AS_LDT, "parent_pid = %d, esr_pid=%d subrec_pid=%d",
			parent_pid, esr_pid, subrec_pid);

	if ((parent_pid != esr_pid) || (parent_pid != subrec_pid) || (esr_pid != subrec_pid)) {
		cf_info_digest(AS_LDT, &parent_digest, "Parent Digest: ");
		cf_info_digest(AS_LDT, &esr_digest, "ESR Digest: ");
		cf_info_digest(AS_LDT, &rd->r->keyd, "Sub-Rec Digest: ");

		cf_warning(AS_LDT, "%s Corrupted Property Map ... digest mismatch [%d %d %d]",
				op, parent_pid, esr_pid, subrec_pid);
	}
}

/*
 * Internal function: To generate digest for the chunk record
 *
 * Parameter:
 * 		ns:		        namespace of record
 * 		keyd(in/out):    digest of the parent digest
 *
 * Return: Nothing
 *
 * Description:
 * 		This function manipulates digest bits to produce the digest for the
 * 		chunk record. The bits in passed digest is changed.
 *
 * Side Note:
 * There is need to make sure sub record digest never repeat themselves for
 * because if they do collide then LDT_SUB for two different record may show
 * up as duplicates and no one can be winner. Deciding on one winner needs
 * complex re-randomization logic and update of parent SUBRECORD/RECORD of the
 * current LDT_SUB. This is could possibly lead to LDT corruption. Also if
 * LDT_SUB of same record come together committing atomic winner for a entire
 * LDT record is impossible.
 *
 * Randomizer_new Logic:
 *
 *   - 12 bits of partition distinguishing bits and hence LDT_SUB for record
 *     in two different partition are in different space. This also means the
 *     when LDT_SUB digest is generated on two different nodes they can never
 *     collide as at any point of time the write to record in a given partition
 *     happens only on one node.
 *
 *   - 2 bytes for lock has [2-3] for same lock
 *
 *   - 3 bytes generated by atomic increment to make sure they are unique across
 *     thread and across node based on when they are started and how many create
 *     calls have been made. 
 *
 *   - 1 byte to for the storage distribution ... [7]. We want to make sure
 *     LDT subrecord falls on the same device as the LDT record
 *
 *   - 6 bytes are based on real clock [8-14]
 *
 *   - 6 bytes of version which is generated at partition version creation
 *
 */
cf_atomic32 randomizer_int = 0;
void
as_ldt_digest_randomizer(cf_digest *dig)
{
	// For collision to happen in digest randomizer. Only condition where in can
	// collide is on two nodes if the subRecord is generated on same partition in
	// same subRecord at the same time (clock skew possible) at the same increment
	// counter (this could happen over time) 
	// Reasonably ok unique digest bits
	uint32_t digest_rand = cf_atomic32_incr(&randomizer_int);

	// 3 bytes make there could be 10^8 digest generated in a microsecond
	// window to have collision
	memcpy(&dig->digest[DIGEST_SCRAMBLE_BYTE1], &digest_rand, 3);

	// 3 bytes system clock in microseconds.
	uint32_t clock = (uint32_t)cf_clock_getabsoluteus();
	dig->digest[DIGEST_CLOCK_ZERO_BYTE]                  = ((clock & 0x00ff0000) >> 16);
	*(uint32_t *)(&dig->digest[DIGEST_CLOCK_START_BYTE]) = ((clock & 0x0000ffff));

	as_ldt_subdigest_resetversion(dig);
}

/*
 * LDT Read/Write/apply UDF Algorithm
 *
 * When the user request reaches the master or acting master (in case master is
 * not in sync mode origin is acting master, master proxy request to the
 * acting master)
 * Following code path is followed
 *
 * -- thr_tsvc
 *    -- as_rw_start
 *       -- internal_rw_start
 *
 * In case duplicate resolution is triggered and if in the duplicates that are
 * received, if any has LDT bin, there can two possible cases
 *  case 1: The record which wins the duplicate resolution has LDT bin
 *  case 2: The record which wins the duplicate resolution does not have
 *          LDT bin.
 *
 * In case 2 the winner record is written locally and then normal code path of
 * read/write/UDF apply is followed.
 *
 * NB: When duplicate resolution request is received by the node having duplicate
 *     version of partition it only sends back the place holder for the LDT bins
 *     and not the entire record. [Reason being the entire record with LDT bin may
 *     be prohibitively large. There could some sort of bounds of up to which size
 *     it can sent and after which it cannot be for duplicate resolution, but this
 *     could mean sending back many records in one single packet].
 *
 * In case 1 the Read/Write/UDF apply is sent to the winner node. At the winner
 * node the op (winner node actually creates transaction has write hash etc and
 * lock; this actually means the synchronization is done both at the master and
 * the node where op is being performed .. but write hash entry is needed so that
 * winner node can replicate) ... in case the task is running on winner node which
 * is migrating its partition to the remote the replication has some special
 * condition where in it has to make sure that unless the partition has reached
 * the record replication stage only subrecords are sent And in case the partition
 * is still being replicated in the subrecord go into migrate tree [please see the
 * details of migration in the migration logic section].
 *
 * Weird (unexpected) Cases
 *
 * 1. If replication is slow for some reason and the client request times out.
 *    Not sure how to handle it.
 *
 * 2. If the winner node dies without replicating ... then the data is kind of
 *    un available and probably lost if the LDT bin get updates in the node's
 *    absence. We still do not have mechanism of merging LDT ...
 *
 * NB: In future if we decide to do record merging instead of the entire one
 *     record overwriting other... this mechanism of shipping op will not work
 *     because record needs to be merged always before any operation on LDT
 *     can be performed. To merge two versions of records both have to be on
 *     the same node all the time which is kind of difficult. Other options
 *     is all the ops go to all the nodes. And no replication happens and let
 *     the record come in the normal course of replication.
 *
 * Note: Invariant of the problems it is ok to perform retries of shipop etc.
 *       duplicate data is deemed lesser of evil that no data. Also the effect
 *       is the DS specific. For timeseries any retry will result in the unique
 *       key constraint in LLIST or in case of LSET or LMAP the op may be idempotent
 */
int
as_ldt_shipop(rw_request *rw, cf_node dest_node)
{
	// Create transaction to trigger proxy request. The only
	// parts of transaction needed is msgp / keyd / proto_fd
	as_proxy_shipop(dest_node, rw);
	return 0;
}

/*
 * This is an internal function, that will get the requested prop_type from
 * inside prop_map and populate it inside value pointer(type of which is
 * determined by property type). Caller has responsibility of making sure
 * value has enough space.
 *
 * Create the key field to access the map. We're starting with a single char,
 * so we have to turn that into a string, and then that string into an as_val,
 * which is an as_string.
 *
 * Parameters: prop_map    : Property Map
 *             prop_type   : Type of property requested
 *             value(out)  : Value to be populated. Type is determined by prop_type
 *
 * Returns: 0 in case of success
 *          o/w failure
*/
int
as_ldt_get_from_map(const as_map *prop_map, char prop_type, void *value)
{
	cf_detail(AS_LDT, "Property %c from map", prop_type);
	// Create the key field to access the map.  We're starting with a single char,
	// so we have to turn that into a string, and then that string into an as_val,
	// which is an as_string.
	char key_buffer[2];
	sprintf(key_buffer, "%c", prop_type);
	as_string key_val;
	as_string_init(&key_val, key_buffer, false);
	switch (prop_type) {
		case PM_EsrDigest:
		case PM_ParentDigest:
		case PM_SelfDigest:
		{
			as_bytes * digest_bytes = (as_bytes *) as_map_get( (const as_map *)prop_map, (as_val *)&key_val);
			if (!digest_bytes) {
				cf_warning(AS_LDT, "Could not find %c type info in property map",
						prop_type);
				// Not necessary to destroy key_val.
				return -2;
			}
			as_ldt_bytes_todigest(digest_bytes, (cf_digest *) value);
			if ( DEBUG) {
				char * valstr = as_val_tostring( (as_val *) digest_bytes);
				cf_detail(AS_LDT, "Got digest %s", valstr);
				cf_free(valstr);
			}
			// Not necessary to destroy digest_bytes or key_val.
			break;
		}
		case RPM_Version:
		{
			as_integer *int_valp    = (as_integer *)as_map_get((const as_map *)prop_map, (as_val *)&key_val);
			if (!int_valp) {
				cf_warning(AS_LDT, "Failed to get version %c", prop_type);
				// Not necessary to destroy key_val.
				return -2;
			}
			*((uint64_t *)value)    = as_integer_toint(int_valp);

			// when using as_val_tostring(), always place in a temp var and
			// then free it after use.
			if ( DEBUG ) {
				char * valstr1 = as_val_tostring((as_val *) &key_val);
				char * valstr2 = as_val_tostring((as_val *) int_valp);
				cf_detail(AS_LDT, "Get Version Key(%s):Int(%s)",
						valstr1, valstr2);
				cf_free(valstr1);
				cf_free(valstr2);
			}
			// Not necessary to destroy key_val or int_val.
			break;
		}
		default:
		{}
	}
	return 0;
}

/*
 * This is an internal function, that will set the passed in value for the
 * prop_type in the property map.
 *
 * Create the key field to access the map. We're starting with a single char,
 * so we have to turn that into a string, and then that string into an as_val,
 * which is an as_string.
 *
 * Parameters: prop_map    : Property Map
 *             prop_type   : Type of property requested to be set
 *             value       : Value to be set. Type is determined by prop_type
 *
 * Returns: 0 in case of success
 *          o/w failure
 */
int
as_ldt_set_in_map(as_map *prop_map, char prop_type, void *value)
{
	cf_detail(AS_LDT, "[ENTER] PropType(%c)", prop_type );

	int rv = 0;
	switch (prop_type) {
		case RPM_Version:
		{
			uint64_t ldt_version = *(uint64_t *)value;
			char key_buffer[2];
			sprintf(key_buffer, "%c", prop_type);
			as_string *key_val =  as_string_new_strdup(key_buffer);
			as_integer *int_val = as_integer_new(ldt_version);
			if (int_val) {
				// when using as_val_tostring(), always place in a temp var and
				// then free it after use.
				as_map_set(prop_map, (as_val *)key_val, (as_val *)int_val);
				if ( DEBUG ) {
					char * valstr1 = as_val_tostring((as_val*)key_val);
					char * valstr2 = as_val_tostring((as_val*)int_val);
					cf_detail(AS_LDT, "Set Version %s:%s", valstr1, valstr2);
					cf_free(valstr1);
					cf_free(valstr2);
				}
			} else {
				cf_debug(AS_LDT, "Failed to set version %c=%lu",
						prop_type, ldt_version);
				// note: not necessary to destroy key_val here.
				rv = -2;
			}
			break;
		}
		default:
		{}
	}
	return rv;
}


/*
 * Sets the version value on the record. The passed in record should be
 * opened / locked and partition reserved. New bin will be created in case
 * the bin does not exits (Could happen but not sure yet how !!!)
 *
 * Parameters:
 * 			rd:          Record to set version into.
 * 			ldt_version: 5 LSB as version for 8 passed in bytes
 *
 * Returns: <0 in case of failure
 * 			>0  number of bytes copied in case of success
 */
int
as_ldt_parent_storage_set_version(as_storage_rd *rd, uint64_t ldt_version, uint8_t *pp_stack_particles, char *fname, int lineno)
{
	// No op when version is disabled
	if (!rd->ns->ldt_enabled)
		return 0;

	as_bin * binp           = as_bin_get(rd, REC_LDT_CTRL_BIN);
	int rv                  = 0;
	if (!binp) {
		cf_warning_digest(AS_LDT, &rd->r->keyd, "as_ldt_parent_storage_set_version: [LDT Control bin not found %s %d]", fname, lineno);
		return -1;
	}
	as_val * valp           = as_bin_particle_to_asval( binp );
	if (!valp) {
		cf_warning(AS_LDT, "as_ldt_parent_storage_set_version : [LDT Control bin Deserialization error]... Fail");
		return -2;
	}

	// We must always retrieve typed values from as_val using the type specific
	// accessor function -- which will return NULL if we guessed wrong on the
	// type that we're extracting.
	as_map * prop_map        = as_map_fromval(valp);
	if( !prop_map ) {
		cf_warning(AS_LDT, "as_ldt_parent_storage_set_version: [LDT Control bin is not of type MAP]... Fail");
		as_val_destroy(valp);
		return -2;
	}
	rv = as_ldt_set_in_map(prop_map, RPM_Version, (void *)&ldt_version);

	if (rv) {
		cf_warning(AS_LDT, "as_ldt_parent_storage_set_version: [LDT Control bin version cannot be set rv=%d] ... Fail", rv);
		as_val_destroy(valp);
		return -3;
	}
	if ( DEBUG ) {
		// as_val_tostring() values must always be captured and freed.
		char * valstr =  as_val_tostring(valp);
		cf_detail(AS_LDT, "After property map set result %s", valstr );
		cf_free(valstr);
	}

	if (rd->ns->storage_data_in_memory) {
		as_bin_particle_replace_from_asval(binp, valp);
		// TODO - check for failure?
	}
	else {
		as_bin_particle_stack_from_asval(binp, pp_stack_particles, valp);
	}

	as_bin_particle_map_set_hidden(binp);

	as_val_destroy(valp);
	cf_debug(AS_LDT, "(%s:%d) Setting parent version to %ld %d", fname, lineno, ldt_version,rd->r->generation);
	//PRINT_STACK();

	return 0;
}

/*
 * Gets the version value on the record. The passed in record should be
 * opened / locked and partition reserved. New bin will be created in case
 * the bin does not exits (Could happen but not sure yet how !!!)
 *
 * Returns: 0 in case of success
 *          o/w failure
 *
 * TODO: We need a better version not partition version and preferably something
 *      in memory rather than storage.
 *
 * Parameter: vinfo (out) gets populated with the version information
 */
int
as_ldt_parent_storage_get_version(as_storage_rd *rd, uint64_t *ldt_version, bool no_fail, char *fname, int lineno)
{
	// No op when version is disabled
	if (!rd->ns->ldt_enabled)
		return 0;

	// Pull out bin
	as_bin * binp           = as_bin_get(rd, REC_LDT_CTRL_BIN);
	int       rv            = 0;
	if (!binp) {
		if (as_ldt_record_is_parent(rd->r)) {
			if (no_fail) {
				cf_warning_digest(AS_LDT, &rd->r->keyd, "Control bin not found LDT parent record %s %d", fname, lineno);
			}
		} else {
			cf_debug(AS_LDT, "Control bin not found");
		}
		rv                      = -1;
	} else {
		const as_val * valp           = as_bin_particle_to_asval( binp );
		if (!valp) {
			if (no_fail) {
				cf_warning(AS_LDT, "Property Bin %s Corrupted", REC_LDT_CTRL_BIN);
			}
			return -2;
		}

		// We must always retrieve typed values from as_val using the type specific
		// accessor function -- which will return NULL if we guessed wrong on the
		// type that we're extracting.
		const as_map * prop_map        = as_map_fromval(valp);
		if( !prop_map ) {
			cf_warning(AS_LDT, "Control bin is not of type MAP");
			as_val_destroy(valp);
			return -2;
		}

		rv                      = as_ldt_get_from_map(prop_map, RPM_Version, (void *)ldt_version);
		as_val_destroy(valp);
	}
	cf_debug(AS_LDT, "(%s,%d)Version Search find %ld with rv = %d", fname, lineno, *ldt_version, rv);
	return rv;
}


/*
 * Given the record pointer, and bin_name, get the LDT Property bin,
 * and from that get the ESR digest.
 * The design of all LDTs is that the LDT bin contents comprises a list of
 * two maps.  The first map (at location list[1]) is the same for all LDTs.
 *
 * Unpack the Property map and look inside it's soul (for that pass it into
 * as_ldt__get_digest_from_pmap.
 *
 * Parameters: rd:         : Opened storage record.
 *			   digest_type : Type of digest value to pull out ESR/PARENT
 * 			   keyd (out)  : key digest for the parent record is filled up.
 *             NULL:         in case of failure
 *
 * Returns: 0 in case of success
 *          o/w failure
 */
int
as_ldt_subrec_storage_get_digests(as_storage_rd *rd, cf_digest *edigest, cf_digest *pdigest)
{
	if (!rd) {
		cf_warning(AS_LDT, "Invalid Parameter [%p]", rd);
		return -1;
	}

	as_bin * binp        = as_bin_get(rd, SUBREC_PROP_BIN);
	if (!binp) {
		cf_debug(AS_LDT, "Property Bin Not found");
		return -1;
	}

	as_val * valp        = as_bin_particle_to_asval(binp);
	if (!valp) {
		cf_warning(AS_LDT, "Property Bin %s Corrupted", SUBREC_PROP_BIN);
		return -2;
	}
	cf_detail(AS_LDT, "Got a value from the bin: type(%d)", valp->type);

	// We must always retrieve typed values from as_val using the type specific
	// accessor function -- which will return NULL if we guessed wrong on the
	// type that we're extracting.
	const as_map * prop_map        = as_map_fromval(valp);
	if( !prop_map ) {
		cf_debug(AS_LDT, "Control bin is not of type MAP");
		return -2;
	}

	if ( DEBUG ) {
		// when using as_val_tostring(), always place in a temp var and
		// then free it after use.
		char * valstr = as_val_tostring( valp );
		cf_debug(AS_LDT, "Map %s", valstr );
		cf_free(valstr);
	}
	int rv  = 0;
	if ((edigest && as_ldt_get_from_map(prop_map, PM_EsrDigest, edigest)) 
			|| (pdigest && as_ldt_get_from_map(prop_map, PM_ParentDigest, pdigest))) {
		cf_warning(AS_LDT, "as_ldt_subrec_storage_get_digests: Property Bin %s Corrupted "
				" [Cannot get esr or parent digest]... Fail", SUBREC_PROP_BIN);
		rv = -3;
	}
	as_val_destroy(valp);
	return rv;
}


inline void
as_ldt_record_set_rectype_bits(as_index *r, const as_rec_props *props)
{
	uint16_t *ldt_rectype_bits;
	if (props->p_data) {
		if (as_rec_props_get_value(props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
									(uint8_t**)&ldt_rectype_bits) == 0) {
			cf_detail(AS_LDT, "Setting flag %d in index", *ldt_rectype_bits);
			as_index_set_flags(r, *ldt_rectype_bits);
		}
	}
}

/*
 * This is debug validation code. Following validations are done.
 * - In case it is LDT ESR it has to have correct digest to the PARENT. Its version
 *   matches that of the parent. And also matches what is in its digest.
 * - In case it is LDT SUBREC it has to have correct digest to the ESR / PARENT. Its
 *   version matches that of the ESR it is pointing to as also with the PARENT
 */
inline bool
as_ldt_flag_has_subrec(uint16_t flag)
{
	if (flag & AS_INDEX_FLAG_CHILD_REC) {
		return true;
	} else {
		return false;
	}
}

inline bool
as_ldt_flag_has_esr(uint16_t flag)
{
	if (flag & AS_INDEX_FLAG_CHILD_ESR) {
		return true;
	} else {
		return false;
	}
}

inline bool
as_ldt_flag_has_sub(uint16_t flag)
{
	if ((flag & AS_INDEX_FLAG_CHILD_REC)
			|| (flag & AS_INDEX_FLAG_CHILD_ESR)) {
		return true;
	} else {
		return false;
	}
}

inline bool
as_ldt_flag_has_parent(uint16_t flag)
{
	if (flag & AS_INDEX_FLAG_SPECIAL_BINS) {
		return true;
	} else {
		return false;
	}
}

/*
 * Function to peek in ESR to determine if the version of the subrec
 * and the one in ESR matches. If it does then return true otherwise
 * false
 *
 * Assumption: the partition is reserved and tree is locked already
 * ESR record when opened in here is with skip_lock.
 */
bool
as_ldt_is_parent_and_version_match(uint64_t subrec_version, as_index_tree *tree, cf_digest *keyd, as_namespace *ns)
{
	int rv = 0;
	as_storage_rd rd;
	as_index_ref  r_ref;
	r_ref.skip_lock = true;

	if (as_record_get_live(tree, keyd, &r_ref, ns)) {
		cf_warning(AS_LDT, "LDT_SUB_GC Could not find parent record");
		return false;
	}
	as_index *r     = r_ref.r;

	if (!as_ldt_record_is_parent(r)) {
		// if parent is not a LDT parent version does not match
		cf_debug(AS_LDT, "LDT_INDEXBIT Expected Parent Bits Not Found");
		as_record_done(&r_ref, ns);
		return false;
	}

	rv              = as_storage_record_open(ns, r, &rd);
	if (0 != rv) {
		cf_warning_digest(AS_UDF, keyd,
				"LDT_SUB_GC Could not open record @ version rv=%d: Digest:", rv);
		as_record_done(&r_ref, ns);
		return false;
	}
	cf_atomic_int_incr(&ns->lstats.ldt_gc_io);
	as_storage_rd_load_n_bins(&rd); // TODO - handle error returned
	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];
	as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned

	uint64_t parent_version = 0;
	rv = as_ldt_parent_storage_get_version(&rd, &parent_version, false, __FILE__, __LINE__);
	if (0 != rv) {
		cf_debug(AS_LDT, "LDT_SUB_GC Something wrong could not get LDT parent version rv = %d", rv);
		goto Cleanup;
	}

	cf_debug_digest(AS_LDT, keyd, "LDT_SUB_GC Subrec and parent version check %ld != %ld %p",
			  parent_version, subrec_version, rd.r);
	if (parent_version == subrec_version) {
		rv = 0;
	} else {
		rv = -4;
	}

Cleanup:
	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);
	if (rv) return false;
	else    return true;
	return rv;
}

/*
 * Main thread that walks through the LDT subrecord tree and cleans
 * up stale SUBRECORD entries. This is kind of parallel to the nsup
 * thread ... it has the following responsibilities:
 *
 * 1. LDT subrecord cleanup if the parent LDT record is deleted
 *
 * 2. LDT subrecord cleanup if the LDT bin is deleted
 *
 * 3. LDT subrecord cleanup in case after migration some stale versions
 *    of are lying around.
 *
 * Synchronization:
 * 		Will hold sub_tree lock while creating list
 * 		Will hold subrecord lock while cleaning it up and deleting it from tree
 * 		Will hold tree and record lock while determining if the record
 * 		need to be deleted.
 *
 * Side effect:
 *
 *    Sub records will be deleted
 *    Memory usage counters will be updated
 *
 * Returns : Never
 *
 * Parameter: None
 *
 * TODO: Not needed once we have 128byte subrecord index record
 * need to be opened only in case of storage in memory case, for storage
 * accounting
 *
 * TODO: IO efficiency track LDT_GC_IO
 */
#define LDT_SUB_GC_NO_ESR                      1
#define LDT_SUB_GC_NO_PARENT                   2
#define LDT_SUB_GC_PARENT_VERSION_MISMATCH     3
void
as_ldt_sub_gc_fn(as_index_ref *r_ref, void *udata)
{
	ldt_sub_gc_info *linfo  = (ldt_sub_gc_info *)udata;
	as_index *r             = r_ref->r;
	as_namespace *ns        = linfo->ns;
	as_partition *p         = &ns->partitions[as_partition_getid(&r->keyd)];

	// Miscellaneous Checks
	if (!as_ldt_record_is_sub(r)) {
		cf_warning(AS_LDT, "LDT_SUB_GC: Missing Index bits !!");
		as_record_done(r_ref, ns);
		return;
	}
	cf_atomic_int_incr(&ns->lstats.ldt_gc_processed);

	if (r->void_time != 0) {
		cf_debug(AS_LDT, "No void time should be set in subrecord !!! found %d", r->void_time);
	}

	// Subrecord Version
	cf_digest subrec_digest = r->keyd;
	uint64_t subrec_version = as_ldt_subdigest_getversion(&subrec_digest);
	cf_detail(AS_LDT, "LDT_SUB_GC Sub Record Version %ld", subrec_version);

	// If there is incoming migration and subrecord is of incoming migration, then
	// skip it. The parent may not have made it yet so garbage collecting this would
	// be problem.
	if (true == as_migrate_is_incoming(&subrec_digest, subrec_version, p->id, 0)) {
		cf_detail(AS_LDT, " LDT_SUB_GC Skipping Defrag for version %ld ", subrec_version);
		as_record_done(r_ref, ns);
		return;
	}

	// STEP 1: OPEN SUBRECORD AND PULL OUT
	// a) PARENT DIGEST
	// b) ESR DIGEST
	// c) VERSION
	// d) For in-memory case memory usage

	// LDT_GC_IO: SUBRECORD
	as_storage_rd rd;
	int rv                  = as_storage_record_open(ns, r, &rd);
	if (0 != rv) {
		cf_warning(AS_UDF, "LDT_SUB_GC Could not open record %"PRIx64"!! rv=%d", *(uint64_t *)&r->keyd, rv);
		as_record_done(r_ref, ns);
		return;
	}
	cf_atomic_int_incr(&ns->lstats.ldt_gc_io);
	as_storage_rd_load_n_bins(&rd); // TODO - handle error returned
	as_bin stack_bins[(!ns->storage_data_in_memory) ? rd.n_bins : 0];
	as_storage_rd_load_bins(&rd, stack_bins); // TODO - handle error returned

	// Read Parent and ESR digest 
	cf_digest esr_digest;
	cf_digest parent_digest;
	if (as_ldt_subrec_storage_get_digests(&rd, &esr_digest, &parent_digest)) {
		goto Cleanup;
	}

	// Do not check esr of esr.
	bool check_esr          = false;
	if (!as_ldt_record_is_esr(r)) {
		// ESR digest with matching Version
		as_ldt_subdigest_setversion(&esr_digest, subrec_version);
		check_esr = true;
	}

	as_ldt_subrec_storage_validate(&rd, "Defragging");
	as_storage_record_close(&rd);

	// STEP 2: Check if we should delete subrec
	// a) Check if parent record exist (if not the record is deleted)
	// b) Check if the esr record exists (if not the bin is deleted)
	// c) Check if the version matches that of parent/esr ??? (if not the version is invalid)
	bool delete = false;
	char type   = 0;
	rv = 0;

	if (check_esr && (rv = as_record_exists(p->sub_vp, &esr_digest))) {
		delete = true;
		type   = LDT_SUB_GC_NO_ESR;
	} else if ((rv = as_record_exists_live(p->vp, &parent_digest, ns))) {
		delete = true;
		type   = LDT_SUB_GC_NO_PARENT;
	} else if (!as_ldt_is_parent_and_version_match(subrec_version, p->vp, &parent_digest, ns)) {
		// LDT_GC_IO: Parent IO
		delete = true;
		type   = LDT_SUB_GC_PARENT_VERSION_MISMATCH;
	} else {
		cf_detail(AS_LDT, "LDT_SUB_GC Found both parent and ESR record !!");
	}

	if (delete) {
		cf_detail_digest(AS_LDT, &subrec_digest, "LDT_SUB_GC Expiry of the SubRecord type=%d version=%ld for partition %d rv=%d",
				type, subrec_version, p->id, rv);
		cf_detail_digest(AS_LDT, &subrec_digest, "Sub-Rec Digest: ");
		cf_detail_digest(AS_LDT, &esr_digest, "ESR Digest: ");
		cf_detail_digest(AS_LDT, &parent_digest, "Parent Digest: ");
		as_index_delete(p->sub_vp, &subrec_digest);
		switch (type) {
			case LDT_SUB_GC_NO_ESR: 
				cf_atomic_int_incr(&ns->lstats.ldt_gc_no_esr_cnt);
				break;
			case LDT_SUB_GC_NO_PARENT: 
				cf_atomic_int_incr(&ns->lstats.ldt_gc_no_parent_cnt);
				break;
			case LDT_SUB_GC_PARENT_VERSION_MISMATCH: 
				cf_atomic_int_incr(&ns->lstats.ldt_gc_parent_version_mismatch_cnt);
				break;
			default:
				break;
		}
		cf_atomic_int_incr(&ns->lstats.ldt_gc_cnt);
		linfo->num_gc++;
	}
	as_record_done(r_ref, ns);

	// TODO: Have a better slow down strategy !!!
	usleep(ns->ldt_gc_sleep_us);
	return;

Cleanup:
	as_storage_record_close(&rd);
	as_record_done(r_ref, ns);
	usleep(ns->ldt_gc_sleep_us);
}


/*
 * Function to look at the parent and check if the migration component
 * is a merge candidate. Following checks are made
 * 1. Check generation and void time of the incoming subrec component
 *    with its parent generation and void time on sending node and the
 *    receiving node.
 *
 * Assumption: Caller should have proper partition reservation...
 *
 * Parameter:
 *          rsv:   Partition Reservation
 *          c:     Incoming component
 */
bool
as_ldt_merge_component_is_candidate(as_partition_reservation *rsv, as_record_merge_component *c)
{
	as_index_ref r_ref;
	r_ref.skip_lock     = false;
	if (as_record_get_live(rsv->tree, &c->pdigest, &r_ref, rsv->ns)) {
		return true;
	}
	as_index *r  = r_ref.r;
	bool rv = false;

	// If component has higher generation ttl then it is merge candidate
	if (c->pgeneration > r->generation ||
			(c->pgeneration == r->generation &&
					(r->void_time != 0 && (c->pvoid_time == 0 ||
							c->pvoid_time > r->void_time)))) {
		as_record_done(&r_ref, rsv->ns);
		rv = true;
	} else {
		rv = false;
		as_record_done(&r_ref, rsv->ns);
	}
	cf_detail_digest(AS_LDT, &r->keyd, "Local Parent vs incoming [%d %d] void_time [%u %u]", r->generation, c->pgeneration, r->void_time, c->pvoid_time);
	return rv;

#if 0
	// We have version as well want to do something smart ???
	as_storage_rd rd;
	as_storage_record_open(rsv->ns, r, &rd, keyd);
	uint64_t parent_version;
	int is_candidate = false;
	if (!as_ldt_parent_storage_get_version(&rd, &parent_version)) {
		cf_warning(AS_LDT, "Something wrong: could not get LDT parent version");
		as_storage_record_close(r, &rd);
		as_record_done(&r_ref, rsv->ns);
		return true;
	}
	if (parent_version == c->version) { /* do something */ }
#endif
}

/*
 * Main routine to replicate the chunks of LDT objects. The LDT directory rec
 * is not replicated using this function. This function is called for each chunk
 * that got updated as part of the single LDT operation. Note that in a single
 * LDT operation, there can be only few chunks that change. i.e chunks in one
 * path of the tree structure.
 *
 * Assumption:
 * 1. All records should have been closed.
 * 2. Pickled buf for all the record and subrecord which needs shipping should have
 * 	  been filled.
 *
 * Function:
 *
 * 1. Walk through each sub record and use its pickled buf to create
 *    RW_OP_WRITE. Pack it in the buffer and push it into the RW_MULTI_OP
 *    packet.
 * 2. This function packs entire pickled buf into the message that is one extra
 *    allocation into the multi-op over the fabric. The message hangs from the
 *    wr for the parent record for the retransmit
 */
int
as_ldt_record_pickle(ldt_record *lrecord,
				  uint8_t               ** pickled_buf,
				  size_t                 * pickled_sz)
{
	cf_detail(AS_LDT, "Enter: MULTI_OP: Packing LDT record");

	udf_record *h_urecord  = as_rec_source(lrecord->h_urec);
	as_transaction   *h_tr = h_urecord->tr;

	// Do an early check if we need to replicate to other nodes. In cases like
	// single-replica or single-node we don't need to do any replication.
	cf_node dest_nodes_tmp[AS_CLUSTER_SZ];
	memset(dest_nodes_tmp, 0, sizeof(dest_nodes_tmp));
	int listsz = as_partition_get_other_replicas(h_tr->rsv.p, dest_nodes_tmp);
	if (listsz == 0) {
		return 0;
	}

	bool is_delete       = (h_urecord->pickled_buf) ? false : true;
	int  ret             = 0;
	int  ops             = 0;
	// TODO: Change this hard coded value to a number based on number of 
	//       record which has changed
	msg *m[lrecord->num_slots_used + 1];
	memset(m, 0, (lrecord->num_slots_used + 1) * sizeof(msg *));


	if (is_delete) {
		*pickled_buf = 0;
		*pickled_sz  = 0;
	} else {
		size_t sz     = 0;

		m[ops] = as_fabric_msg_get(M_TYPE_RW);
		if (!m[ops]) {
			ret = -3;
			goto Out;
		}
		if (!is_delete && h_urecord->pickled_buf) {
			cf_detail(AS_LDT, "MULTI_OP: Packing LDT Head Record");
			repl_write_ldt_make_message(m[ops], h_tr,
							&h_urecord->pickled_buf,
							h_urecord->pickled_sz,
							&h_urecord->pickled_rec_props,
							false);

			sz += msg_get_wire_size(m[ops]);
			ops++;
		}

		// This macro is a for-loop thru the SR list and a test for valid SR entry
		FOR_EACH_SUBRECORD(i, j, lrecord) {
			udf_record *c_urecord = &lrecord->chunk[i].slots[j].c_urecord;
			as_transaction *c_tr  = c_urecord->tr;


			m[ops] = as_fabric_msg_get(M_TYPE_RW);
			if (!m[ops]) {
				ret = -3;
				goto Out;
			}
			if (c_urecord->op == UDF_OPTYPE_READ) {
				// Skip Reads
				continue;
			}

			bool reset_flag = true;
			if (c_urecord->op == UDF_OPTYPE_DELETE) {
				// Fake it as delete
				if (c_tr->msgp->msg.info2 & AS_MSG_INFO2_DELETE) {
					reset_flag = false;	
				} else {
					c_tr->msgp->msg.info2 |= AS_MSG_INFO2_DELETE;
					reset_flag = true;
				}
			}

			repl_write_ldt_make_message(m[ops], c_tr,
							&c_urecord->pickled_buf,
							c_urecord->pickled_sz,
							&c_urecord->pickled_rec_props,
							true);

			if (reset_flag) {
				c_tr->msgp->msg.info2 &= ~AS_MSG_INFO2_DELETE;
			}
			
			sz += msg_get_wire_size(m[ops]);
			ops++;
		}

		if (sz) {
			uint8_t *buf = cf_malloc(sz);
			if (!buf) {
				pickled_sz   = 0;
				*pickled_buf = NULL;
				ret          = -1;
				goto Out;
			}
			*pickled_buf = buf;
			*pickled_sz  = sz;

			for (int i = 0; i < ops; i++) {
				buf += msg_to_wire(m[i], buf);
			}
		}
	}
Out:

	if (ret) {
		cf_debug(AS_LDT, "MULTI_OP Packing failed with ret = %d", ret);
		if (*pickled_buf) {
			cf_free(*pickled_buf);
			*pickled_buf = NULL;
			*pickled_sz  = 0;
		}
	}

	for (int i = 0; i < ops; i++) {
		if(m[i]) {
			as_fabric_msg_put(m[i]);
		}
	}
	// TODO: Check value of ret and do the needed cleanup
	return ret;
}
int
as_ldt_get_key(char c, as_string *key, char *key_buffer)
{
	sprintf(key_buffer, "%c", c);
	as_string_init(key, key_buffer, false);
	return 0;
}

char *
as_ldt_leaf_getNext(as_storage_rd *rd)
{
	as_bin * bb    = as_bin_get(rd, "LsrControlBin");
	as_val * srMap = as_bin_particle_to_asval(bb);

	char key_buffer[2]; as_string key;
	as_ldt_get_key((char)LF_NextPage, &key, key_buffer);

	as_bytes *dig = (as_bytes *)as_hashmap_get((as_hashmap*)srMap, (as_val *)&key);
	char *curDigest = as_val_tostring(dig);	
	as_val_destroy(srMap);
	return curDigest;
}

bool
as_bin_is_ldt_bin(as_map * prop_map)
{
	bool ret = true;

	char key_buffer[2]; as_string key;
	as_ldt_get_key((char) PM_Magic, &key, key_buffer);

	as_val * value = as_hashmap_get((as_hashmap *)prop_map, (as_val *)&key); 
	if (!value) {
		return false;
	}

	if (strcmp (as_string_get((as_string*)value), "MAGIC")) {
		ret = false;
	}
	
	if (ret) {
		as_ldt_get_key((char )PM_LdtType, &key, key_buffer);
		value = as_hashmap_get((as_hashmap *)prop_map, (as_val *) &key); 
		if (strcmp(as_string_get((as_string*)value), "LLIST"))
			ret = false;
	}
	return ret;
}
as_list *
as_ldt_leaf_scan(as_storage_rd *rd)
{
	as_bin * bb  = as_bin_get(rd, "LsrListBin");
	as_list *sl  = (as_list *)as_bin_particle_to_asval(bb);
	return sl;
}

as_list * 
as_bin_get_llist(as_namespace *ns, as_storage_rd *rd, as_index_tree *sub_tree, as_val *ctrl_list, uint64_t ldt_version) 
{
	as_map *propMap = as_list_get_map((as_list *)ctrl_list, 0);
	if (!propMap) {
		cf_debug(AS_LDT, "Control Bin Property Map not of type map");
		return NULL;
	}

	as_map *ldtMap = as_list_get_map((as_list *)ctrl_list, 1);
	if (!ldtMap) {
		cf_debug(AS_LDT, "Control Bin ldt Map not of type map");
		return NULL;
	}
	
	char key_buffer[2]; as_string key;

	as_ldt_get_key((char)LS_StoreState , &key, key_buffer);
	as_string *storestate = (as_string *)as_hashmap_get((as_hashmap *)ldtMap, (as_val *)&key);
	char *storestatestr = as_val_tostring(storestate);
	cf_debug(AS_LDT, "storestate |%s|", storestatestr);
	if (strcmp("\"C\"", storestatestr) == 0) {
		as_ldt_get_key((char)LS_CompactList, &key, key_buffer);
		as_list *rl = (as_list *)as_hashmap_get((as_hashmap *)ldtMap, (as_val *)&key);
		as_val_reserve(rl);
		cf_free(storestatestr);
		return rl;
	}	
	cf_free(storestatestr);

	as_ldt_get_key((char)LS_LeftLeafDigest, &key, key_buffer);
	as_bytes *leftMostDigest = (as_bytes *)as_hashmap_get((as_hashmap *)ldtMap, (as_val *)&key);
	char *curDigest = as_val_tostring(leftMostDigest);

	as_arraylist *rl = as_arraylist_new(100, 100);
	while (true) {
		if (!curDigest || (!strcmp(curDigest,"0"))) { 
			return (as_list *)rl;
		}
			
		// 1. Setup digest
		cf_digest keyd;
		if (as_ldt_string_todigest(curDigest, &keyd)) {
			return (as_list *)rl;
		}
		as_ldt_subdigest_setversion(&keyd, ldt_version);
		cf_free(curDigest);

		// 2. Open Record
		as_index_ref   sub_r_ref; 
		as_storage_rd  sub_rd;
		sub_r_ref.skip_lock = true;
		int rv = as_record_get_live(sub_tree, &keyd, &sub_r_ref, ns);
		if (rv) {
			cf_warning_digest(AS_LDT, &rd->r->keyd, " LDT stucture invalid %ld", ldt_version);
			as_val_destroy(rl);
			return NULL;
		}

		as_record *sub_r = sub_r_ref.r;

		as_storage_record_open(ns, sub_r, &sub_rd);
		as_storage_rd_load_n_bins(&sub_rd); // TODO - handle error returned
		// Have bound checks ...
		as_bin stack_bins[(sub_r && !ns->storage_data_in_memory) ? sub_rd.n_bins : 0];
		as_storage_rd_load_bins(&sub_rd, stack_bins); // TODO - handle error returned

		// 3. Scan the current leaf
		as_list *sl = as_ldt_leaf_scan(&sub_rd); 
		as_arraylist_iterator it;
		as_arraylist_iterator_init(&it, (as_arraylist *)sl);
		while (as_arraylist_iterator_has_next(&it)) {
			as_val * val = (as_val *)as_arraylist_iterator_next(&it);
			as_val_reserve(val);
			as_arraylist_append((as_arraylist *)rl, val);
		}
		as_val_destroy(sl);
		as_arraylist_iterator_destroy(&it);

		// 4. Find next 
		curDigest = as_ldt_leaf_getNext(&sub_rd);
		
		// 5. Close Current
		as_storage_record_close(&sub_rd);
		as_record_done(&sub_r_ref, ns);
	}
	return (as_list *)rl;
}

as_val *
as_llist_scan(as_namespace *ns, as_index_tree *sub_tree, as_storage_rd  *rd, as_bin *binp) 
{
	uint8_t type = as_bin_get_particle_type(binp);
	if (! (type == AS_PARTICLE_TYPE_LIST || type == AS_PARTICLE_TYPE_HIDDEN_LIST)) {
		return NULL;
	}

	as_val * valp  = as_bin_particle_to_asval(binp);
	if (!valp) {
		cf_warning(AS_LDT, "Property Bin %s Corrupted", SUBREC_PROP_BIN);
		return NULL;
	}

	// We must always retrieve typed values from as_val using the type specific
	// accessor function -- which will return NULL if we guessed wrong on the
	// type that we're extracting.
	const as_list * ctrl_list = as_list_fromval(valp);
	if (!ctrl_list) {
		cf_debug(AS_LDT, "Control bin is not of type list");
		as_val_destroy(valp);
		return NULL;
	}

	// Not ref counted need not be destroyed...
	as_map *prop_map = as_list_get_map(ctrl_list, 0);
	if (!prop_map) {
		cf_debug(AS_LDT, "Control Bin Property Map not of type map");
		as_val_destroy(valp);
		return NULL;
	}

	as_list *result_list = NULL;
	if (as_bin_is_ldt_bin(prop_map)) {
		cf_debug(AS_LDT, "Bin %s is LDT Bin", as_bin_get_name_from_id(ns, binp->id));	
		uint64_t ldt_version;
		if (!as_ldt_parent_storage_get_version(rd, &ldt_version, true, __FILE__, __LINE__)) {
			result_list = as_bin_get_llist(ns, rd, sub_tree, (as_val *)ctrl_list, ldt_version); 
		} 
	} else {
		cf_debug(AS_LDT, "Bin %s is _NOT_ LDT Bin", as_bin_get_name_from_id(rd->ns, binp->id));	
	}

	as_val_destroy(valp);
	return (as_val *)result_list;
}

/*
 * Parse (Actually, probe) the error string, and if we see this pattern:
 * FileName:Line# 4digits:LDT-<Error String>
 * For example:
 * .../aerospike-lua-core/src/ldt/lib_llist.lua:982: 0002:LDT-Top Record Not Found
 * All UDF errors (LDT included), have the "filename:line# " prefix, and then
 * LDT errors follow that with a known pattern:
 * (4 digits, colon, LDT-<Error String>).
 * We will check the error string by looking for specific markers after the
 * the space that follows the filename:line#.  If we see the markers, we will
 * parse the LDT error code and use that as the wire protocol error if it is
 * one of the special ones:
 * (1)  "0002:LDT-Top Record Not Found"
 * (2)  "0125:LDT-Item Not Found"
 */
long
ldt_get_error_code(void *val, size_t vlen) 
{
	if (!vlen)
		return 0;

	char * charptr;
	char * valptr = (char *) val;
	long   error_code;

	// Error Format   "XXXX:LDT-<message>"
	if ((charptr = strchr((const char *) val, ' ')) != NULL) {
		if (&charptr[9] < &valptr[vlen]) {
			if (memcmp(&charptr[5], ":LDT-", 5) == 0) {
				error_code = strtol(&charptr[1], NULL, 10);
				cf_debug(AS_UDF, "LDT Error: Code(%ld) String(%s)",
						error_code, (char *) val);
				return error_code;
			}
		}
	}
	return 0;
}

void 
ldt_update_err_stats(as_namespace *ns, as_val *result_val)
{
	if (!ns->ldt_enabled || !result_val) {
		return;
	}

	as_string * s   = as_string_fromval(result_val);
	char *      rs  = (char *) as_string_tostring(s);

    if ( s ) {
		long code = ldt_get_error_code(rs, as_string_len(s));
		switch (code) {
			case ERR_TOP_REC_NOT_FOUND: 
				cf_atomic_int_incr(&ns->lstats.ldt_err_toprec_not_found);
				break;
			case ERR_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_item_not_found);
				break;
			case ERR_INTERNAL:
				cf_atomic_int_incr(&ns->lstats.ldt_err_internal);
				break;
			case ERR_UNIQUE_KEY:
				cf_atomic_int_incr(&ns->lstats.ldt_err_unique_key_violation);
				break;
			case ERR_INSERT:
				cf_atomic_int_incr(&ns->lstats.ldt_err_insert_fail);
				break;
			case ERR_SEARCH:
				cf_atomic_int_incr(&ns->lstats.ldt_err_search_fail);
				break;
			case ERR_DELETE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_delete_fail);
				break;
			case ERR_VERSION:
				cf_atomic_int_incr(&ns->lstats.ldt_err_version_mismatch);
				break;

			case ERR_CAPACITY_EXCEEDED:
				cf_atomic_int_incr(&ns->lstats.ldt_err_capacity_exceeded);
				break;
			case ERR_INPUT_PARM:
				cf_atomic_int_incr(&ns->lstats.ldt_err_param);
				break;

			case ERR_TYPE_MISMATCH:
				cf_atomic_int_incr(&ns->lstats.ldt_err_op_bintype_mismatch);
				break;

			case ERR_NULL_BIN_NAME:
			case ERR_BIN_NAME_NOT_STRING:
			case ERR_BIN_NAME_TOO_LONG:
				cf_atomic_int_incr(&ns->lstats.ldt_err_param);
				break;

			case ERR_TOO_MANY_OPEN_SUBRECS:
				cf_atomic_int_incr(&ns->lstats.ldt_err_too_many_open_subrec);
				break;
			case ERR_SUB_REC_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_subrec_not_found);
				break;
			case ERR_BIN_DOES_NOT_EXIST:
				cf_atomic_int_incr(&ns->lstats.ldt_err_bin_does_not_exist);
				break;
			case ERR_BIN_ALREADY_EXISTS:
				cf_atomic_int_incr(&ns->lstats.ldt_err_bin_exits);
				break;
			case ERR_BIN_DAMAGED:
				cf_atomic_int_incr(&ns->lstats.ldt_err_bin_damaged);
				break;

			case ERR_SUBREC_POOL_DAMAGED:
			case ERR_SUBREC_DAMAGED:
			case ERR_SUBREC_OPEN:
			case ERR_SUBREC_UPDATE:
			case ERR_SUBREC_CREATE:
			case ERR_SUBREC_DELETE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_subrec_internal);
				break;

			case ERR_SUBREC_CLOSE:
			case ERR_TOPREC_UPDATE:
			case ERR_TOPREC_CREATE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_toprec_internal);
				break;


			case ERR_FILTER_BAD:
			case ERR_FILTER_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_filter);
				break;
			case ERR_KEY_BAD:
			case ERR_KEY_FIELD_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_key);
				break;
			case ERR_INPUT_CREATESPEC:
				cf_atomic_int_incr(&ns->lstats.ldt_err_createspec);
				break;
			case ERR_INPUT_USER_MODULE_NOT_FOUND:
				cf_atomic_int_incr(&ns->lstats.ldt_err_usermodule);
				break;

			case ERR_INPUT_TOO_LARGE:
				cf_atomic_int_incr(&ns->lstats.ldt_err_input_too_large);
				break;
			case ERR_NS_LDT_NOT_ENABLED:
				cf_atomic_int_incr(&ns->lstats.ldt_err_ldt_not_enabled);
				break;
			
			default:
				cf_atomic_int_incr(&ns->lstats.ldt_err_unknown);
		}
    } else {
		cf_atomic_int_incr(&ns->lstats.ldt_err_unknown);
	}
	return;
}

void
as_ldt_get_property(as_rec_props *props, bool *is_ldt_parent,
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

	cf_detail(AS_LDT, "Parent=%d Subrec=%d", *is_ldt_parent, *is_ldt_sub);
}
