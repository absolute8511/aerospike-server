/*
 * transaction_policy.h
 *
 * Copyright (C) 2014-2016 Aerospike, Inc.
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

#pragma once

/***********************************************************************/
/*                                                                     */
/* Note:  The following transaction polices are also declared          */
/*        identically in (and must be kept in sync. with)              */
/*        the Aerospike C Client v3 file:                              */
/*                                                                     */
/*          aerospike-client-c/src/include/aerospike/as_policy.h       */
/*                                                                     */
/***********************************************************************/

/**
 *  Consistency Level
 *
 *  Specifies the number of replicas to be consulted
 *  in a read operation to provide the desired
 *  consistency guarantee.
 *
 *  @ingroup client_policies
 */
typedef enum as_policy_consistency_level_e {

	/**
	 *  Involve a single replica in the operation.
	 */
	AS_POLICY_CONSISTENCY_LEVEL_ONE,

	/**
	 *  Involve all replicas in the operation.
	 */
	AS_POLICY_CONSISTENCY_LEVEL_ALL,

} as_policy_consistency_level;

/**
 *  Commit Level
 *
 *  Specifies the number of replicas required to be successfully
 *  committed before returning success in a write operation
 *  to provide the desired consistency guarantee.
 *
 *  @ingroup client_policies
 */
typedef enum as_policy_commit_level_e {

	/**
	 *  Return success only after successfully committing all replicas.
	 */
	AS_POLICY_COMMIT_LEVEL_ALL,

	/**
	 *  Return success after successfully committing the master replica.
	 */
	AS_POLICY_COMMIT_LEVEL_MASTER,

} as_policy_commit_level;



//==========================================================
// Per-transaction consistency guarantees.
//
// Client-request guarantee level is respected unless
// corresponding server's namespace override is enabled.

// Extract the read consistency level from an as_msg.
// Note: not a strict check: both bits == 0 means the default, anything else
// means the alternative.
#define PROTO_CONSISTENCY_LEVEL(asmsg)								\
	(((asmsg.info1 & AS_MSG_INFO1_CONSISTENCY_LEVEL_B0) == 0 &&		\
	  (asmsg.info1 & AS_MSG_INFO1_CONSISTENCY_LEVEL_B1) == 0) ?		\
			AS_POLICY_CONSISTENCY_LEVEL_ONE :						\
			AS_POLICY_CONSISTENCY_LEVEL_ALL)

// Extract the write commit level from an as_msg.
// Note: not a strict check: both bits == 0 means the default, anything else
// means the alternative.
#define PROTO_COMMIT_LEVEL(asmsg)									\
	(((asmsg.info3 & AS_MSG_INFO3_COMMIT_LEVEL_B0) == 0 &&			\
	  (asmsg.info3 & AS_MSG_INFO3_COMMIT_LEVEL_B1) == 0) ?			\
			AS_POLICY_COMMIT_LEVEL_ALL :							\
			AS_POLICY_COMMIT_LEVEL_MASTER)

// Determine the read consistency level for this transaction based upon the
// server's namespace and client policy settings.
#define TRANSACTION_CONSISTENCY_LEVEL(tr)							\
	(tr->rsv.ns->read_consistency_level_override ?					\
			tr->rsv.ns->read_consistency_level :					\
			PROTO_CONSISTENCY_LEVEL(tr->msgp->msg))

// Determine the write commit level for this transaction based upon the server's
// namespace and client policy settings.
#define TRANSACTION_COMMIT_LEVEL(tr)								\
	(tr->rsv.ns->write_commit_level_override ?						\
			tr->rsv.ns->write_commit_level :						\
			PROTO_COMMIT_LEVEL(tr->msgp->msg))
