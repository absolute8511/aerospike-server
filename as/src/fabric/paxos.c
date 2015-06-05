/*
 * paxos.c
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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
 *  Paxos consensus algorithm
 *
 */

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

#include "fabric/paxos.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_random.h"

#include "fault.h"
#include "msg.h"
#include "queue.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/thr_info.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/migrate.h"
#include "storage/storage.h"


/* SYNOPSIS
 * Paxos
 *
 * Generations
 * A Paxos generation consists of two unsigned 32-bit fields: a sequence
 * number, which tracks each separate Paxos action, and a proposal number,
 * which identifies convergence attempts within each sequence.  A sequence
 * number of zero is invalid and used for internal record keeping.
 *
 * Pending transaction list
 * We maintain a list of pending transactions.  This list holds the last
 * AS_PAXOS_ALPHA transactions we've seen.
 *
 * code flow
 * _event, _spark, _thr
 * weird state structure in _thr!!!
 */


/* Function forward references: */

void as_paxos_current_init(as_paxos *p);


/* AS_PAXOS_PROTOCOL_IDENTIFIER
 * Select the appropriate message identifier for the active Paxos protocol. */
#define AS_PAXOS_PROTOCOL_IDENTIFIER() (AS_PAXOS_PROTOCOL_V1 == g_config.paxos_protocol ? AS_PAXOS_MSG_V1_IDENTIFIER : \
										(AS_PAXOS_PROTOCOL_V2 == g_config.paxos_protocol ? AS_PAXOS_MSG_V2_IDENTIFIER : \
										 (AS_PAXOS_PROTOCOL_V3 == g_config.paxos_protocol ? AS_PAXOS_MSG_V3_IDENTIFIER : \
										  AS_PAXOS_MSG_V4_IDENTIFIER)))

/* AS_PAXOS_PROTOCOL_IS_V
 * Is the current Paxos protocol version the given version number? */
#define AS_PAXOS_PROTOCOL_IS_V(n) (AS_PAXOS_PROTOCOL_V ## n == g_config.paxos_protocol)

/* AS_PAXOS_PROTOCOL_IS_AT_LEAST_V
 * Is the current Paxos protocol version greater than or equal to the given version number? */
#define AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(n) ((((int)(g_config.paxos_protocol) - AS_PAXOS_PROTOCOL_V ## n)) >= 0)

/* AS_PAXOS_PROTOCOL_VERSION_NUMBER
 * Return the version number for the given Paxos protocol identifier. */
#define AS_PAXOS_PROTOCOL_VERSION_NUMBER(n) ((n) - AS_PAXOS_PROTOCOL_NONE)

/* AS_PAXOS_ENABLED
 * Is this node sending out and receiving Paxos messages? */
#define AS_PAXOS_ENABLED() (AS_PAXOS_PROTOCOL_NONE != g_config.paxos_protocol)

/* AS_PAXOS_AUTO_DUN_MASTER_DELAY
 * Number of Paxos retransmit check iterations to wait before attempting
 * to recover from a broken cluster state via the auto-dun-master method.
 * XXX -- Should be configurable. */
#define AS_PAXOS_AUTO_DUN_MASTER_DELAY 5

/* AS_PAXOS_AUTO_DUN_ALL_DELAY
 * Number of Paxos retransmit check iterations to wait before attempting
 * to recover from a broken cluster state via the auto-dun-all method.
 * XXX -- Should be configurable. */
#define AS_PAXOS_AUTO_DUN_ALL_DELAY 5

/*
 * The migrate key changes once when a Paxos vote completes.
 * Every migration operation stores its key and sends it as part of its start
 * message. If a migrate message's key does not match its global key, the
 * migrate is terminated.
 */
static uint64_t g_cluster_key;

// Set the cluster key
void
as_paxos_set_cluster_key(uint64_t cluster_key)
{
	g_cluster_key = cluster_key;
}

// Get the cluster key
uint64_t
as_paxos_get_cluster_key()
{
	return (g_cluster_key);
}

// Get the cluster key and cluster size
uint64_t
as_paxos_get_cluster_key_and_size(size_t *cluster_size)
{
	if (cluster_size) {
		*cluster_size = g_config.paxos->cluster_size;
	}

	if (!g_config.paxos->cluster_size) {
		cf_warning(AS_PAXOS, ">>>>cluster size is zero!!!<<<<");
	}

	return (g_cluster_key);
}

/* as_paxos_state_next
 * This is just a little bit of syntactic sugar around the transitions in
 * the state machine; it saves us from some nasty if/else constructions.
 * These #defines are here to avoid cluttering up the global namespace */
#define ACK 0
#define NACK 1
int
as_paxos_state_next(int s, int next)
{
	const int states[15][2] = {
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* UNDEF */
		{ AS_PAXOS_MSG_COMMAND_PREPARE_ACK, AS_PAXOS_MSG_COMMAND_PREPARE_NACK },  /* PREPARE */
		{ AS_PAXOS_MSG_COMMAND_COMMIT, AS_PAXOS_MSG_COMMAND_UNDEF },              /* PREPARE_ACK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* PREPARE_NACK */
		{ AS_PAXOS_MSG_COMMAND_COMMIT_ACK, AS_PAXOS_MSG_COMMAND_COMMIT_NACK },    /* COMMIT */
		{ AS_PAXOS_MSG_COMMAND_CONFIRM, AS_PAXOS_MSG_COMMAND_UNDEF },             /* COMMIT_ACK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* COMMIT_NACK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* CONFIRM */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* SYNC_REQUEST */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* SYNC */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* PARTITION_SYNC_REQUEST */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* PARTITION_SYNC */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* HEARTBEAT_EVENT */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* RETRANSMIT_CHECK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF }                /* SET_SUCC_LIST */
	};

	return(states[s][next]);
}

/*
 * Names of the Paxos command messages.
 * (NB:  Must match order and number of "AS_PAXOS_MSG_COMMAND_*" definitions in "paxos.h".)
 */
static char *as_paxos_cmd_name[] = {
	"UNDEF",
	"PREPARE",
	"PREPARE_ACK",
	"PREPARE_NACK",
	"COMMIT",
	"COMMIT_ACK",
	"COMMIT_NACK",
	"CONFIRM",
	"SYNC_REQUEST",
	"SYNC",
	"PARTITION_SYNC_REQUEST",
	"PARTITION_SYNC",
	"HEARTBEAT_EVENT",
	"RETRANSMIT_CHECK",
	"SET_SUCC_LIST"
};

/*
 * Show the results of the succession list.
 * Stop after the first ZERO entry, but go no longer than AS_CLUSTER_SZ.
 */
void
as_paxos_dump_succession_list(char *msg, cf_node slist[], int index)
{
	// Use the smaller one
	int list_size = ((g_config.paxos_max_cluster_size < AS_CLUSTER_SZ) ?
					 g_config.paxos_max_cluster_size : AS_CLUSTER_SZ);
	int i;
	cf_debug(AS_PAXOS, "DUMP SUCCESSION LIST:(%s):Index:%d\n", msg, index);
	for (i = 0; i < list_size; i++) {
		if (slist[i] == (cf_node) 0) {
			break; // print no more after first zero
		} else {
			cf_debug(AS_PAXOS, "(%d)[%lu] ", i, slist[i]);
		}
	} // end for each node position
	cf_debug(AS_PAXOS, " END OF LIST");
} // end as_paxos_dump_succession_list()

void
dump_partition_state()
{
	/*
	 * Print out the data loss statistics
	 */
	char printbuf[100];
	int pos = 0; // location to print from
	printbuf[0] = '\0';

	cf_debug(AS_PAXOS, " Partition State Dump");

	for (int index = 0; index < g_config.paxos_max_cluster_size; index++) {
		if (g_config.paxos->succession[index] == (cf_node) 0)
			continue;
		cf_debug(AS_PAXOS, " Node %"PRIx64"", g_config.paxos->succession[index]);
		for (int i = 0; i < g_config.namespaces; i++) {
			cf_debug(AS_PAXOS, " Name Space: %s", g_config.namespace[i]->name);
			int k = 0;
			as_partition_vinfo *parts = g_config.paxos->c_partition_vinfo[i][index];
			if (NULL == parts) {
				cf_debug(AS_PAXOS, " STATE is EMPTY");
				continue;
			}
			for (int j = 0; j < AS_PARTITIONS; j++) {
				int bytes = sprintf((char *) (printbuf + pos), " %"PRIx64"", parts[j].iid);
				if (bytes <= 0) {
					cf_debug(AS_PAXOS, "printing error. Bailing ...");
					return;
				}
				pos += bytes;
				if (k % 2 == 1) {
					cf_detail(AS_PAXOS, "%s", (char *) printbuf);
					pos = 0;
					printbuf[0] = '\0';
				}
				k++;
			}
		}
		if (pos > 0) {
			cf_debug(AS_PAXOS, "%s", (char *) printbuf);
			pos = 0;
			printbuf[0] = '\0';
		}
	}
} // end dump_partition_state()

void
as_paxos_print_cluster_key(const char *message)
{
	cf_debug(AS_PAXOS, "%s: cluster key %"PRIx64"", message, as_paxos_get_cluster_key());
}

/* as_paxos_sync_generate
 * Generate a Paxos synchronization message; returns a pointer to the message,
 * or NULL on error */
msg *
as_paxos_sync_msg_generate(uint64_t cluster_key)
{
	as_paxos *p = g_config.paxos;
	msg *m = NULL;
	int e = 0;

	cf_debug(AS_PAXOS, "SYNC sending cluster key %"PRIx64"", cluster_key);

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get fabric message");
		return(NULL);
	}

	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_SYNC);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, p->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, p->gen.proposal);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. */
	if (!AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	e += msg_set_buf(m, AS_PAXOS_MSG_SUCCESSION, (byte *)p->succession, g_config.paxos_max_cluster_size * sizeof(cf_node), MSG_SET_COPY);
	e += msg_set_uint64(m, AS_PAXOS_MSG_CLUSTER_KEY, cluster_key);
	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to generate sync message");
		return(NULL);
	}

	return(m);
}

/* as_paxos_sync_msg_apply
 * Apply a synchronization message; returns 0 on success */
int
as_paxos_sync_msg_apply(msg *m)
{
	as_paxos *p = g_config.paxos;

	byte *bufp = NULL;
	size_t bufsz = g_config.paxos_max_cluster_size * sizeof(cf_node);

	uint64_t cluster_key = 0;

	int e = 0;
	cf_node self = g_config.self_node;

	cf_assert(m, AS_PAXOS, CF_CRITICAL, "invalid argument");

	as_paxos_generation gen;
	memset(&gen, 0, sizeof(as_paxos_generation));

	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &gen.sequence);
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, &gen.proposal);

	e += msg_get_buf(m, AS_PAXOS_MSG_SUCCESSION, &bufp, &bufsz, MSG_GET_DIRECT);
	e += msg_get_uint64(m, AS_PAXOS_MSG_CLUSTER_KEY, &cluster_key);

	if ((0 > e) || (NULL == bufp)) {
		cf_warning(AS_PAXOS, "unpacking sync message failed");
		return(-1);
	}

	cf_node succession[AS_CLUSTER_SZ];
	memcpy(succession, bufp, g_config.paxos_max_cluster_size * sizeof(cf_node));

	/* Check if we need to ignore this message */
	if (succession[0] == p->succession[0]) { // current succession list has same principal as local succession list
		if (!p->alive[0]) { // Let this through
			cf_info(AS_PAXOS, "Sync message received from a principal %"PRIx64" that is back from the dead?", p->succession[0]);
		}
		/* compare generations */
		if ((gen.sequence < p->gen.sequence) || ((gen.sequence == p->gen.sequence) && (gen.proposal < p->gen.proposal))) {
			cf_warning(AS_PAXOS, "Sync message ignored from %"PRIx64" - [%d.%d]@%"PRIx64" is arriving after [%d.%d]@%"PRIx64,
					   succession[0], gen.sequence, gen.proposal, succession[0], p->gen.sequence, p->gen.proposal, p->succession[0]);
			return(-1);
		}
	}

	/* Apply the sync msg to the current state */
	p->gen.sequence = gen.sequence;
	p->gen.proposal = gen.proposal;

	memcpy(p->succession, bufp, g_config.paxos_max_cluster_size * sizeof(cf_node));

	cf_debug(AS_PAXOS, "SYNC getting cluster key %"PRIx64"", cluster_key);

	as_paxos_set_cluster_key(cluster_key);

	/*
	 * Disallow migration requests into this node until we complete partition rebalancing
	 */
	as_partition_disallow_migrations();

	/* Fix up the auxiliary state around the succession table and destroy
	 * any pending transactions */
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		p->alive[i] = (0 != p->succession[i]) ? true : false;
		if (p->alive[i]) {
			cf_debug(AS_PAXOS, "setting succession[%d] = %"PRIx64" to alive", i, p->succession[i]);
			as_hb_set_is_node_dunned(p->succession[i], false, "Paxos SYNC Apply");
		}
	}

	memset(p->pending, 0, sizeof(p->pending));

	as_paxos_current_init(p);

	/* If this succession list doesn't include ourselves, then fail */
	if (false == as_paxos_succession_ismember(self)) {
		cf_warning(AS_PAXOS, " sync message falied - succession list does not contain self");
		return(-1);
	}
	return(0);
}

/* as_paxos_partition_sync_request_msg_generate
 * Generate a Paxos partition synchronization request message; returns a pointer to the message,
 * or NULL on error */
msg *
as_paxos_partition_sync_request_msg_generate()
{
	as_paxos *p = g_config.paxos;
	msg *m = NULL;
	int e = 0;

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get fabric message");
		return(NULL);
	}

	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, p->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, p->gen.proposal);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. */
	if (!AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	/*
	 * Normally partition locks need to be held for accessing partition vinfo
	 * In this case, however, migrates are disallowed when we are doing the copy
	 * and the partition_vinfo should be consistent
	 */
	size_t array_size = g_config.namespaces;
	cf_debug(AS_PAXOS, "Partition Sync request Array Size = %d ", array_size);
	size_t elem_size = sizeof(as_partition_vinfo) * AS_PARTITIONS;

	if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITION, array_size, elem_size)) {
		cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
		return(NULL);
	}

	size_t n_elem = 0;
	for (int i = 0; i < g_config.namespaces; i++) {
		as_partition_vinfo vi[AS_PARTITIONS];
		for (int j = 0; j < AS_PARTITIONS; j++)
			memcpy(&vi[j], &g_config.namespace[i]->partitions[j].version_info, sizeof(as_partition_vinfo));
		e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITION, n_elem, (uint8_t *)vi, elem_size);
		cf_debug(AS_PAXOS, "writing element %d", n_elem);
		n_elem++;
	}

	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to generate sync message");
		return(NULL);
	}

	/* Include the partition sizes array in all Paxos protocol v3 or greater PARTITION_SYNC_REQUEST messages. */
	if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
		array_size = g_config.namespaces;
		cf_debug(AS_PAXOS, "Partitionsz Sync request Array Size = %d ", array_size);
		elem_size = sizeof(uint64_t) * AS_PARTITIONS;

		if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITIONSZ, array_size, elem_size)) {
			cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
			return(NULL);
		}

		n_elem = 0;
		for (int i = 0; i < g_config.namespaces; i++) {
			uint64_t partitionsz[AS_PARTITIONS];
			for (int j = 0; j < AS_PARTITIONS; j++) {
				partitionsz[j] = (g_config.namespace[i]->partitions[j].vp)
								 ? g_config.namespace[i]->partitions[j].vp->elements
								 : 0;
				partitionsz[j] += (g_config.namespace[i]->partitions[j].sub_vp)
								  ? g_config.namespace[i]->partitions[j].sub_vp->elements
								  : 0;
				cf_detail(AS_PAXOS, "Assigning partition size for pid %d, %ld", j, partitionsz[j]);
			}
			e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITIONSZ, n_elem, (uint8_t *)partitionsz, elem_size);
			cf_debug(AS_PAXOS, "writing element %d", n_elem);
			n_elem++;
		}

		if (0 > e) {
			cf_warning(AS_PAXOS, "unable to generate sync message");
			return(NULL);
		}
	}

	return(m);
}

/* as_paxos_partition_sync_request_msg_apply
 * Apply a partition sync request  message; returns 0 on success */
int
as_paxos_partition_sync_request_msg_apply(msg *m, int n_pos)
{
	as_paxos *p = g_config.paxos;
	int e = 0;

	cf_assert(m, AS_PAXOS, CF_CRITICAL, "invalid argument");

	as_paxos_generation gen;
	memset(&gen, 0, sizeof(as_paxos_generation));

	/* We trust this state absolutely */
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &gen.sequence);
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, &gen.proposal);

	if ((gen.sequence != p->gen.sequence) || (gen.proposal != p->gen.proposal)) {
		cf_warning(AS_PAXOS, "sequence/proposal do not match (%"PRIu32", %"PRIu32" | %"PRIu32", %"PRIu32"). partition sync request not applied",
				   gen.sequence, p->gen.sequence, gen.proposal, p->gen.proposal);
		return(-1);
	}

	size_t array_size = g_config.namespaces;

	int size;
	if (0 != msg_get_buf_array_size(m, AS_PAXOS_MSG_PARTITION, &size)) {
		cf_warning(AS_PAXOS, "Unable to read partition sync message");
		return(-1);
	}
	if (size != array_size) {
		cf_warning(AS_PAXOS, "Different number of namespaces (expected: %d, received in partition sync message: %d) between nodes in same cluster ~~ Please check node configurations", array_size, size);
		return(-1);
	}

	/*
	 * reset the values of this node's partition version in the global list
	 */
	size_t elem = 0;
	for (int i = 0; i < g_config.namespaces; i++) {
		byte *bufp = NULL;
		size_t bufsz = sizeof(as_partition_vinfo) * AS_PARTITIONS;
		as_partition_vinfo *vi = p->c_partition_vinfo[i][n_pos];
		if (NULL == vi) {
			vi = cf_rc_alloc(bufsz);
			cf_assert(vi, AS_PAXOS, CF_CRITICAL, "rc_alloc: %s", cf_strerror(errno));
			p->c_partition_vinfo[i][n_pos] = vi;
		}
		memset(vi, 0, bufsz);
		e += msg_get_buf_array(m, AS_PAXOS_MSG_PARTITION, elem, &bufp, &bufsz, MSG_GET_DIRECT);
		elem++;
		if ((0 > e) || (NULL == bufp)) {
			cf_warning(AS_PAXOS, "unpacking partition sync request message failed");
			return(-1);
		}
		memcpy(vi, bufp, sizeof(as_partition_vinfo) * AS_PARTITIONS);
	}

	/* Require the partition sizes array in all Paxos protocol v3 or greater PARTITION_SYNC_REQUEST messages. */
	if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
		array_size = g_config.namespaces;
		if (0 != msg_get_buf_array_size(m, AS_PAXOS_MSG_PARTITIONSZ, &size)) {
			cf_warning(AS_PAXOS, "Unable to read partition sync message");
			return(-1);
		}
		if (size != array_size) {
			cf_warning(AS_PAXOS, "Different number of namespaces (expected: %d, received in partition sync message: %d) between nodes in same cluster ~~ Please check node configurations", array_size, size);
			return(-1);
		}
		elem = 0;
		for (int i = 0; i < g_config.namespaces; i++) {
			byte *bufp = NULL;
			size_t bufsz = sizeof(uint64_t) * AS_PARTITIONS;
			uint64_t *partitionsz = p->c_partition_size[i][n_pos];
			memset(partitionsz, 0, bufsz);
			e += msg_get_buf_array(m, AS_PAXOS_MSG_PARTITIONSZ, elem, &bufp, &bufsz, MSG_GET_DIRECT);
			elem++;
			if ((0 > e) || (NULL == bufp)) {
				cf_warning(AS_PAXOS, "unpacking partition sync request message failed");
				return(-1);
			}
			memcpy(partitionsz, bufp, sizeof(uint64_t) * AS_PARTITIONS);
		}
	}

	return(0);
}

/* as_paxos_partition_sync_msg_generate
 * Generate a Paxos partition synchronization message; returns a pointer to the message,
 * or NULL on error */
msg *
as_paxos_partition_sync_msg_generate()
{
	as_paxos *p = g_config.paxos;
	msg *m = NULL;
	int e = 0;

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get fabric message");
		return(NULL);
	}

	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_PARTITION_SYNC);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, p->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, p->gen.proposal);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. */
	if (!AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	e += msg_set_buf(m, AS_PAXOS_MSG_SUCCESSION, (byte *)p->succession, g_config.paxos_max_cluster_size * sizeof(cf_node), MSG_SET_COPY);
	/*
	 * Create a message with the global partition version info
	 */
	/*
	 * find cluster size
	 */
	size_t cluster_size = 0;
	for (int j = 0; j < g_config.paxos_max_cluster_size; j++)
		if (p->succession[j] != (cf_node)0)
			cluster_size++;

	if (2 > cluster_size) {
		cf_warning(AS_PAXOS, "Cluster size is wrong %d. unable to set fabric message", cluster_size);
		return(NULL);
	}

	size_t array_size = cluster_size * g_config.namespaces;
	cf_debug(AS_PAXOS, "Array Size = %d ", array_size);
	size_t elem_size = sizeof(as_partition_vinfo) * AS_PARTITIONS;

	if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITION, array_size, elem_size)) {
		cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
		return(NULL);
	}

	size_t n_elem = 0;
	for (int i = 0; i < g_config.namespaces; i++)
		for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
			if (p->succession[j] == (cf_node)0)
				continue;
			as_partition_vinfo *vi = p->c_partition_vinfo[i][j];
			if (NULL == vi) {
				cf_warning(AS_PAXOS, "unable to generate partition sync message. no data for [ns=%d][node=%d]", i, j);
				return (NULL);
			}
			cf_debug(AS_PAXOS, "writing element %d", n_elem);
			e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITION, n_elem, (uint8_t *)vi, elem_size);
			if (0 > e) {
				cf_warning(AS_PAXOS, "unable to generate sync message");
				return(NULL);
			}
			n_elem++;
		}
	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to generate sync message");
		return(NULL);
	}

	/* Include the partition sizes array in all Paxos protocol v3 or greater PARTITION_SYNC messages. */
	if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
		array_size = cluster_size * g_config.namespaces;
		elem_size = sizeof(uint64_t) * AS_PARTITIONS;

		if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITIONSZ, array_size, elem_size)) {
			cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
			return(NULL);
		}
		n_elem = 0;
		for (int i = 0; i < g_config.namespaces; i++)
			for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
				if (p->succession[j] == (cf_node)0)
					continue;

				uint64_t *partitionsz = p->c_partition_size[i][j];
				// populate latest for the self node
				if (p->succession[j] == g_config.self_node) {
					for (int j = 0; j < AS_PARTITIONS; j++) {
						partitionsz[j] = (g_config.namespace[i]->partitions[j].vp)
										 ? g_config.namespace[i]->partitions[j].vp->elements
										 : 0;
						partitionsz[j] += (g_config.namespace[i]->partitions[j].sub_vp)
										  ? g_config.namespace[i]->partitions[j].sub_vp->elements
										  : 0;
						cf_detail(AS_PAXOS, "Assigning partition size for pid %d, %ld", j, partitionsz[j]);
					}
				}
				cf_debug(AS_PAXOS, "writing element %d", n_elem);
				e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITIONSZ, n_elem, (uint8_t *)partitionsz, elem_size);
				if (0 > e) {
					cf_warning(AS_PAXOS, "unable to generate sync message");
					return(NULL);
				}
				n_elem++;
			}
		if (0 > e) {
			cf_warning(AS_PAXOS, "unable to generate sync message");
			return(NULL);
		}
	}

	if (cf_context_at_severity(AS_PAXOS, CF_DEBUG)) {
		dump_partition_state();
	}

	return(m);
}

/* as_paxos_partition_sync_msg_apply
 * Apply a partition synchronization message; returns 0 on success */
int
as_paxos_partition_sync_msg_apply(msg *m)
{
	as_paxos *p = g_config.paxos;
	byte *bufp = NULL;
	size_t bufsz = g_config.paxos_max_cluster_size * sizeof(cf_node);
	int e = 0;

	cf_assert(m, AS_PAXOS, CF_CRITICAL, "invalid argument");

	as_paxos_generation gen;
	memset(&gen, 0, sizeof(as_paxos_generation));

	/* We trust this state absolutely */
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &gen.sequence);
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, &gen.proposal);
	if ((gen.sequence != p->gen.sequence) || (gen.proposal != p->gen.proposal)) {
		cf_warning(AS_PAXOS, "sequence/proposal do not match. partition sync message not applied");
		return(-1);
	}

	e += msg_get_buf(m, AS_PAXOS_MSG_SUCCESSION, &bufp, &bufsz, MSG_GET_DIRECT);

	if ((0 > e) || (NULL == bufp)) {
		cf_warning(AS_PAXOS, "unpacking succession list from partition sync message failed");
		return(-1);
	}

	/*
	 * Make sure that the bits are identical
	 */
	if (0 != memcmp(p->succession, bufp, g_config.paxos_max_cluster_size * sizeof(cf_node))) {
		cf_warning(AS_PAXOS, "succession lists mismatch from partition sync message");
		return(-1);
	}

	/*
	 * find cluster size
	 */
	size_t cluster_size = 0;
	for (int j = 0; j < g_config.paxos_max_cluster_size; j++)
		if (p->succession[j] != (cf_node)0)
			cluster_size++;

	if (2 > cluster_size) {
		cf_warning(AS_PAXOS, "Cluster size is wrong %d. unable to apply partition sync message", cluster_size);
		return(-1);
	}

	/*
	 * Check if the state of this node is correct for applying a partition sync message
	 */
	if (as_partition_get_migration_flag() == true) {
		cf_info(AS_PAXOS, "Node allows migrations. Ignoring duplicate partition sync message.");
		return(-1);
	}

	size_t array_size = cluster_size * g_config.namespaces;

	int size;
	if (0 != msg_get_buf_array_size(m, AS_PAXOS_MSG_PARTITION, &size)) {
		cf_warning(AS_PAXOS, "Unable to read partition sync message");
		return(-1);
	}
	if (size != array_size) {
		cf_warning(AS_PAXOS, "Expected array size %d, received %d, Unable to read partition sync message", array_size, size);
		return(-1);
	}

	/*
	 * reset the values of this node's partition version in the global list
	 */
	size_t elem = 0;
	for (int i = 0; i < g_config.namespaces; i++)
		for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
			if (p->succession[j] == (cf_node)0)
				continue;
			byte *bufp = NULL;
			bufsz = sizeof(as_partition_vinfo) * AS_PARTITIONS;
			as_partition_vinfo *vi = p->c_partition_vinfo[i][j];
			if (NULL == vi) {
				vi = cf_rc_alloc(bufsz);
				cf_assert(vi, AS_PAXOS, CF_CRITICAL, "rc_alloc: %s", cf_strerror(errno));
				p->c_partition_vinfo[i][j] = vi;
			}
			memset(vi, 0, bufsz);
			e += msg_get_buf_array(m, AS_PAXOS_MSG_PARTITION, elem, &bufp, &bufsz, MSG_GET_DIRECT);
			elem++;
			if ((0 > e) || (NULL == bufp)) {
				cf_warning(AS_PAXOS, "unpacking partition sync message failed");
				return(-1);
			}
			memcpy(vi, bufp, sizeof(as_partition_vinfo) * AS_PARTITIONS);
		}

	/* Require the partition sizes array in all Paxos protocol v3 or greater PARTITION_SYNC messages. */
	if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
		array_size = cluster_size * g_config.namespaces;

		if (0 != msg_get_buf_array_size(m, AS_PAXOS_MSG_PARTITIONSZ, &size)) {
			cf_warning(AS_PAXOS, "Unable to read partition sync message");
			return(-1);
		}
		if (size != array_size) {
			cf_warning(AS_PAXOS, "Expected array size %d, received %d, Unable to read partition sync message", array_size, size);
			return(-1);
		}
		elem = 0;
		for (int i = 0; i < g_config.namespaces; i++)
			for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
				if (p->succession[j] == (cf_node)0)
					continue;
				byte *bufp = NULL;
				bufsz = sizeof(uint64_t) * AS_PARTITIONS;
				uint64_t *partitionsz = p->c_partition_size[i][j];
				memset(partitionsz, 0, bufsz);
				e += msg_get_buf_array(m, AS_PAXOS_MSG_PARTITIONSZ, elem, &bufp, &bufsz, MSG_GET_DIRECT);
				elem++;
				if ((0 > e) || (NULL == bufp)) {
					cf_warning(AS_PAXOS, "unpacking partition sync message failed");
					return(-1);
				}
				memcpy(partitionsz, bufp, sizeof(uint64_t) * AS_PARTITIONS);
				for (int k = 0; k < AS_PARTITIONS; k++) {
					cf_detail(AS_PAXOS, "Got size of pid %d = %ld", k, partitionsz[k]);
				}
			}
	}

	if (cf_context_at_severity(AS_PAXOS, CF_DEBUG)) {
		dump_partition_state();
	}

	return(0);
}

/* as_paxos_succession_insert
 * Insert a node into the succession list, maintaining descending sorted order; return 0 on success */
int
as_paxos_succession_insert(cf_node n)
{
	as_paxos *p = g_config.paxos;
	int i;

	for (i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// Node already exists - May happen in some rare error cases.
		if (n == p->succession[i]) {
			cf_warning(AS_PAXOS, "New node %"PRIx64" already found in Paxos succession list", n);
			p->alive[i] = true;
			break;
		}
		// Found the end of the list - Insert node here.
		if (0 == p->succession[i]) {
			p->succession[i] = n;
			p->alive[i] = true;
			break;
		}
		// Found where this node belongs - Shift the other nodes down and insert node here.
		if (n > p->succession[i]) {
			memmove(&p->succession[i + 1], &p->succession[i], (g_config.paxos_max_cluster_size - i) * sizeof(cf_node));
			memmove(&p->alive[i + 1], &p->alive[i], (g_config.paxos_max_cluster_size - i) * sizeof(bool));
			p->succession[i] = n;
			p->alive[i] = true;
			break;
		}
	}

	if (g_config.paxos_max_cluster_size == i)
		return(-1);
	else
		return(0);
}

/* as_paxos_succession_remove
 * Remove a node from the succession list; return 0 on success */
int
as_paxos_succession_remove(cf_node n)
{
	as_paxos *p = g_config.paxos;
	int i;

	bool found = false;
	/* Find the offset into the succession list of the failed node */
	for (i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (n == p->succession[i]) {
			found = true;
			break;
		}
	}
	if (found == false) {
		cf_info(AS_PAXOS, "Departed node %"PRIx64" is not found in paxos succession list", n);
		return(0);
	}

	/* Remove the node from the succession, applying a little bit of
	 * optimization to avoid unnecessary memmove()s */
	if ((g_config.paxos_max_cluster_size - 1 == i) || (0 == p->succession[i + 1])) {
		p->succession[i] = 0;
		p->alive[i] = false;
	} else {
		memmove(&p->succession[i], &p->succession[i + 1], ((g_config.paxos_max_cluster_size - i) - 1) * sizeof(cf_node));
		memmove(&p->alive[i], &p->alive[i + 1], ((g_config.paxos_max_cluster_size - i) - 1) * sizeof(bool));
	}

	/* Fix up any votes in progress, since vote-keeping is indexed on
	 * position within the succession list */
	for (int j = 0; j < AS_PAXOS_ALPHA; j++) {
		if ((0 == p->pending[j].gen.sequence) || (p->pending[j].confirmed))
			continue;

		if ((g_config.paxos_max_cluster_size - 1 == i) || (0 == p->succession[i + 1]))
			p->pending[j].votes[i] = false;
		else
			memmove(&p->pending[j].votes[i], &p->pending[j].votes[i + 1], ((g_config.paxos_max_cluster_size - i) - 1) * sizeof(bool));
	}

	return(0);
}

/* as_paxos_succession_getprincipal
 * Get the head of the Paxos succession list, or zero if there is none */
cf_node
as_paxos_succession_getprincipal()
{
	as_paxos *p = g_config.paxos;

	if (!p) {
		cf_warning(AS_PAXOS, "Paxos is not yet initialized ~~ returning NULL principal");
		return 0;
	}

	/* Find the first living node in the succession */
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((0 != p->succession[i]) && p->alive[i]) {
			return(p->succession[i]);
		}
	}

	return 0;
}

/* as_paxos_succession_ismember
 * Returns true if the specified node is in the succession and alive */
bool
as_paxos_succession_ismember(cf_node n)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((n == p->succession[i]) && p->alive[i])
			return(true);
	}

	return(false);
}

/* as_paxos_set_protocol
 * Set the Paxos protocol version.
 * Returns 0 if successful, else returns -1.  */
int
as_paxos_set_protocol(paxos_protocol_enum protocol)
{
	if (g_config.paxos_protocol == protocol) {
		cf_info(AS_PAXOS, "no Paxos protocol change needed");
		return(0);
	}

	switch (protocol) {
		case AS_PAXOS_PROTOCOL_V1:
		case AS_PAXOS_PROTOCOL_V2:
		case AS_PAXOS_PROTOCOL_V3:
			cf_debug(AS_PAXOS, "setting Paxos protocol to version %d", AS_PAXOS_PROTOCOL_VERSION_NUMBER(protocol));

			if (AS_PAXOS_PROTOCOL_V1 == protocol && AS_CLUSTER_LEGACY_SZ != g_config.paxos_max_cluster_size) {
				cf_warning(AS_PAXOS, "setting paxos protocol version v1 only allowed when paxos_max_cluster_size = %d not the current value of %d",
						   AS_CLUSTER_LEGACY_SZ, g_config.paxos_max_cluster_size);
				return(-1);
			}
			as_partition_allow_migrations();
			g_config.paxos_protocol = protocol;
			break;
		case AS_PAXOS_PROTOCOL_V4:
			cf_warning(AS_PAXOS, "cannot dynamically set Paxos protocol to version %d", AS_PAXOS_PROTOCOL_VERSION_NUMBER(protocol));
			return(-1);
		case AS_PAXOS_PROTOCOL_NONE:
			cf_info(AS_PAXOS, "disabling Paxos messaging");
			as_partition_disallow_migrations();
			g_config.paxos_protocol = protocol;
			break;
		default:
			cf_warning(AS_PAXOS, "unknown Paxos protocol version number: %d", protocol);
			return(-1);
	}

	return(0);
}

/* as_paxos_set_recovery_policy
 * Set the Paxos recovery policy.
 * Returns 0 if successful, else returns -1.  */
int
as_paxos_set_recovery_policy(paxos_recovery_policy_enum policy)
{
	g_config.paxos_recovery_policy = policy;

	return(0);
}

/* as_paxos_partition_sync_states_all
 * Returns true if the specified node is in the succession and alive
 * Sets the partition sync state to be true */
bool
as_paxos_partition_sync_states_all()
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// It is important to check the p->alive[] flag here. If a node has just departed, we return
		// true so that this paxos reconfiguration can complete
		if (((cf_node)0 != p->succession[i]) && p->alive[i] && (false == p->partition_sync_state[i])) {
			return(false);
		}
		if ((p->alive[i] == false) && ((cf_node)0 != p->succession[i])) {
			cf_debug(AS_PAXOS, "Node %"PRIx64" appears to have departed during a paxos vote", p->succession[i]);
		}
	}

	return(true);
}

/* as_paxos_set_partition_sync_state
 * Returns true if the specified node is in the succession and alive
 * Sets the partition sync state to be true */
bool
as_paxos_set_partition_sync_state(cf_node n)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((n == p->succession[i]) && p->alive[i]) {
			p->partition_sync_state[i] = true;
			return(true);
		}
	}

	return(false);
}

/* as_paxos_get_succession_index
 * Returns the position of this node in the succession list */
int
as_paxos_get_succession_index(cf_node n)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((n == p->succession[i]) && p->alive[i]) {
			return(i);
		}
	}

	return(-1);
}

/* as_paxos_succession_setdeceased
 * Mark a node in the succession list as deceased */
void
as_paxos_succession_setdeceased(cf_node n)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((n == p->succession[i]) && p->alive[i]) {
			p->alive[i] = false;
			break;
		}
	}
}

/* as_paxos_succession_setrevived
 * Mark a node in the succession list as alive */
void
as_paxos_succession_setrevived(cf_node n)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (n == p->succession[i]) {
			if (! p->alive[i]) {
				cf_info(AS_PAXOS, "Node %"PRIx64" revived", n);
				p->alive[i] = true;
			}
			break;
		}
	}
}

/* as_paxos_succession_quorum
 * Return true if a quorum of the nodes in the succession list are alive */
bool
as_paxos_succession_quorum()
{
	as_paxos *p = g_config.paxos;
	int a = 0, c = 0;
	bool r;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (0 != p->succession[i]) {
			c++;
			if (p->alive[i])
				a++;
		}
	}

	r = (a >= ((c >> 1) + 1)) ? true : false;
	return(r);
}

void
as_paxos_current_init(as_paxos *p)
{
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		p->current[i] = NULL;
	}

	p->current[0] = &p->pending[0];
	p->current[0]->gen.sequence = p->gen.sequence;
	p->current[0]->gen.proposal = p->gen.proposal;
}

// Gets the transaction candidate with the largest principal.
// Currently only used in an ignored NACK message.
as_paxos_transaction *
as_paxos_current_get()
{
	as_paxos *p = g_config.paxos;

	as_paxos_transaction *max = p->current[0];

	for (int i = 1; i < g_config.paxos_max_cluster_size; i++) {
		if (NULL == p->current[i])
			return(max);

		if (p->current[i]->c.p_node > max->c.p_node)
			max = p->current[i];
	}

	return(max);
}

// Get the current sequence number (for this node, or for the old principal).
// (No guarantee this is bulletproof...)
uint32_t
as_paxos_current_get_sequence()
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (NULL == p->current[i])
			return(p->current[0]->gen.sequence);

		if (p->current[i]->c.p_node == g_config.self_node)
			return(p->current[i]->gen.sequence);
	}

	return(p->current[0]->gen.sequence);
}

// Checks to see if this transaction has the highest sequence for a
// transaction with this principal.
bool
as_paxos_current_is_candidate(as_paxos_transaction t)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (NULL == p->current[i])
			return(true);

		// if lesser sequence for same master, reject
		if (t.c.p_node == p->current[i]->c.p_node
				&& t.gen.sequence <= p->current[i]->gen.sequence)
			return(false);
	}

	return(true);
}

// Add a transaction with a new principal to the current array,
// or update the transaction for a principal already in the array.
void
as_paxos_current_update(as_paxos_transaction *t)
{
	as_paxos *p = g_config.paxos;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (NULL == p->current[i] || t->c.p_node == p->current[i]->c.p_node) {
			p->current[i] = t;
			return;
		}
	}
}

/* as_paxos_transaction_search
 * Search the pending transaction table for a transaction with a specific
 * generation; return NULL if no corresponding transaction is found */
as_paxos_transaction *
as_paxos_transaction_search(as_paxos_transaction s)
{
	as_paxos *p = g_config.paxos;
	as_paxos_transaction *t = NULL;

	/* Scan through the list until we find a transaction with a matching
	 * generation */
	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (p->pending[i].c.p_node == s.c.p_node && p->pending[i].gen.sequence == s.gen.sequence) {
			t = &p->pending[i];
			break;
		}
	}

	return(t);
}

/* as_paxos_transaction_compare
 * Compare two transactions; return 0 if they are equal, nonzero otherwise */
int
as_paxos_transaction_compare(as_paxos_transaction *t1, as_paxos_transaction *t2)
{
	cf_assert(t1, AS_PAXOS, CF_CRITICAL, "invalid transaction (t1)");
	cf_assert(t2, AS_PAXOS, CF_CRITICAL, "invalid transaction (t2)");

	if ((t1->gen.sequence == t2->gen.sequence) &&
			(t1->gen.proposal == t2->gen.proposal) &&
			(0 == memcmp(&t1->c, &t2->c, sizeof(as_paxos_change))))
		return(0);
	else
		return(-1);
}

/* as_paxos_transaction_update
 * Update a pending transaction with the information from a successor */
void
as_paxos_transaction_update(as_paxos_transaction *oldt, as_paxos_transaction *newt)
{
	cf_assert(oldt, AS_PAXOS, CF_CRITICAL, "invalid transaction (oldt)");
	cf_assert(newt, AS_PAXOS, CF_CRITICAL, "invalid transaction (newt)");

	oldt->gen.sequence = newt->gen.sequence;
	oldt->gen.proposal = newt->gen.proposal;
	memcpy(&oldt->c, &newt->c, sizeof(oldt->c));
}

/* as_paxos_transaction_establish
 * Establish a new transaction in the first available slot in the
 * pending transaction table; return NULL on internal error */
as_paxos_transaction *
as_paxos_transaction_establish(as_paxos_transaction *s)
{
	as_paxos *p = g_config.paxos;
	as_paxos_transaction *t = NULL, *oldest = NULL;

	cf_assert(s, AS_PAXOS, CF_CRITICAL, "invalid transaction");

	/* Scan through the list to find an empty slot; if none was found, reuse
	 * the oldest retired transaction */
	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (0 == p->pending[i].gen.sequence) {
			t = &p->pending[i];
			break;
		}

		/* Keep a pointer to the oldest transaction we've seen (retired take priority over non-retired) */
		if (!oldest
			|| (p->pending[i].retired && !oldest->retired)
			|| (p->pending[i].retired == oldest->retired
				&& (p->pending[i].gen.sequence < oldest->gen.sequence
					|| (p->pending[i].gen.sequence == oldest->gen.sequence && p->pending[i].gen.proposal < oldest->gen.proposal)))) {
			oldest = &p->pending[i];
		}
	}

	/* Sanity checking to make sure we found a slot, reusing the one
	 * occupied by the oldest transaction if necessary */
	if (!t) {
		cf_debug(AS_PAXOS, "reusing oldest transaction slot");
		t = oldest;
	}

	/* Copy the transaction information into the slot; update the current
	 * pointer */
	memcpy(t, s, sizeof(as_paxos_transaction));
	t->retired = false;
	t->confirmed = false;
	memset(t->votes, 0, sizeof(t->votes));
	as_paxos_current_update(t);

	return(t);
}

/* as_paxos_transaction_confirm
 * Mark a transaction as confirmed */
void
as_paxos_transaction_confirm(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid argument");

	t->confirmed = true;

	as_paxos *p = g_config.paxos;
	// We also have to confirm transactions that have sequence numbers less than this transaction.
	// It is possible that messages have been lost
	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if ((p->pending[i].gen.sequence != 0)
			&& (p->pending[i].gen.sequence < t->gen.sequence)
			&& !(p->pending[i].confirmed)) {
			p->pending[i].confirmed = true;
			break;
		}
	}
}

/* as_paxos_transaction_retire
 * Mark a transaction as retired */
void
as_paxos_transaction_retire(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid argument");

	t->retired = true;
}

/* as_paxos_transaction_destroy
 * Destroy the contents of a pointer to a pending transaction */
void
as_paxos_transaction_destroy(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid argument");
	memset(t, 0, sizeof(as_paxos_transaction));
}

/* as_paxos_transaction_vote
 * Count a vote for a pending transaction.  s is a pointer to the
 * corresponding entry in the pending transaction table, n is the cf_node
 * whose vote we are counting, and t is a pointer to the transaction they
 * think they are voting for.  Return as follows:
 *    ..REJECT if the vote has been rejected
 *    ..ACCEPT if the vote has been accepted, but a quorum hasn't been reached
 *    ..QUORUM if the vote has been accepted and a quorum has been reached
 */
as_paxos_transaction_vote_result
as_paxos_transaction_vote(as_paxos_transaction *s, cf_node n, as_paxos_transaction *t)
{
	as_paxos *p = g_config.paxos;
	as_paxos_transaction_vote_result r;
	int c = 0, v = 0;

	cf_assert(s, AS_PAXOS, CF_CRITICAL, "invalid transaction (s)");
	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid transaction (t)");

	/* Make sure that the transaction we're voting on is the same one we
	 * have in the pending transaction table */
	if (0 != as_paxos_transaction_compare(s, t))
		return(AS_PAXOS_TRANSACTION_VOTE_REJECT);

	/* Record the vote, counting the number of living nodes, c, and the
	 * number of votes, v, as we go */
	int j = 0;
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (0 == p->succession[i])
			continue;

		if (p->alive[i])
			c++;

		if (n == p->succession[i]) {
			s->votes[i] = true;
			j = i;
		}

		if (s->votes[i])
			v++;
	}

	//r = ((v >= ((c >> 1) + 1)) || ((v == c) && (v == 1))) ? AS_PAXOS_TRANSACTION_VOTE_QUORUM : AS_PAXOS_TRANSACTION_VOTE_ACCEPT;
	r = (v == ((c >> 1) + 1)) ? AS_PAXOS_TRANSACTION_VOTE_QUORUM : AS_PAXOS_TRANSACTION_VOTE_ACCEPT;
	return(r);
}

/* as_paxos_transaction_vote_reset
 * Reset the count of votes for a transaction */
void
as_paxos_transaction_vote_reset(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid argument");

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++)
		t->votes[i] = false;
}

/* as_paxos_transaction_getnext
 * Get the next applicable transaction */
as_paxos_transaction *
as_paxos_transaction_getnext(cf_node master_node)
{
	as_paxos *p = g_config.paxos;
	as_paxos_transaction *t = NULL;

	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (p->pending[i].c.p_node == master_node && p->pending[i].confirmed &&
				(p->pending[i].gen.sequence == p->gen.sequence + 1)) {
			t = &p->pending[i];
			break;
		}
	}

	return(t);
}

void
as_paxos_send_sync_messages() {
	as_paxos *p = g_config.paxos;
	uint64_t cluster_key = as_paxos_get_cluster_key();
	msg *reply = NULL;
	char sbuf[(AS_CLUSTER_SZ * 17) + 49];
	snprintf(sbuf, 49, "SUCCESSION [%d.%d]@%"PRIx64": ", p->gen.sequence, p->gen.proposal, g_config.self_node);
	snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", p->succession[0]);

	for (int i = 1; i < g_config.paxos_max_cluster_size; i++) {
		if ((cf_node)0 != p->succession[i])
			snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", p->succession[i]);
		if (((cf_node)0 != p->succession[i]) && (p->partition_sync_state[i] == false)) {
			if (NULL == (reply = as_paxos_sync_msg_generate(cluster_key)))
				cf_warning(AS_PAXOS, "unable to construct sync message");
			else {
				cf_info(AS_PAXOS, "sending sync message to %"PRIx64"", p->succession[i]);
				if (0 != as_fabric_send(p->succession[i], reply, AS_FABRIC_PRIORITY_HIGH)) {
					cf_warning(AS_PAXOS, "sync message lost in fabric");
					as_fabric_msg_put(reply);
				}
			}
		}
	}

	cf_info(AS_PAXOS, sbuf);
	as_paxos_print_cluster_key("COMMAND CONFIRM");
}

void as_paxos_start_second_phase()
{
	as_paxos *p = g_config.paxos;
	cf_node self = g_config.self_node;

	/*
	 * Generate one uuid and use this for the cluster key
	 */
	uint64_t cluster_key = 0;

	// Generate a non-zero cluster key.
	while (!(cluster_key = cf_get_rand64()))
	  ;
	as_paxos_set_cluster_key(cluster_key);

	/*
	 * Disallow migration requests into this node until we complete partition rebalancing
	 */
	as_partition_disallow_migrations();

	/* Earlier code used to synchronize only new members. However, this code is changed to
	 * send sync messages to all members of the cluster. On receiving a sync, all nodes are expected to send
	 * the partition state back to the cluster master (principal) */
	if (p->succession[0] != self)
		cf_warning(AS_PAXOS, "Principal is not first in the succession list!");

	/* Principal's state is always local. Copy its current partition
	 * version information into global table and set state flag.
	 * Note that the index for the principal is 0 */

	for (int i = 0; i < g_config.namespaces; i++) {
		as_partition_vinfo *vi = p->c_partition_vinfo[i][0];
		size_t vi_sz = sizeof(as_partition_vinfo) * AS_PARTITIONS;
		if (NULL == vi) {
			vi = cf_rc_alloc(sizeof(as_partition_vinfo) * AS_PARTITIONS);
			cf_assert(vi, AS_PAXOS, CF_CRITICAL, "rc_alloc: %s", cf_strerror(errno));
			p->c_partition_vinfo[i][0] = vi;
		}
		memset(vi, 0, vi_sz);
		for (int j = 0; j < AS_PARTITIONS; j++)
			memcpy(&vi[j], &g_config.namespace[i]->partitions[j].version_info, sizeof(as_partition_vinfo));
	}
	p->partition_sync_state[0] = true; /* Principal's state is always local */
	for (int i = 1; i < g_config.paxos_max_cluster_size; i++)
		p->partition_sync_state[i] = false;

	as_paxos_send_sync_messages();
}

/* as_paxos_transaction_apply
 * Apply all confirmed pending transactions */
void
as_paxos_transaction_apply(cf_node from_id)
{
	as_paxos *p = g_config.paxos;
	as_paxos_transaction *t = NULL;
	cf_node self = g_config.self_node;
	int n_xact = 0;

	/*
	 * Apply all the changes that are part of this transaction but only if the node is principal
	 * Note that all the changes need to be applied before any synchronization messages are sent out
	 * The non principals will have their changes applied when they receive sync messages
	 * Non-principal transactions are also retired here to reclaim the space in the transaction table
	 */
	if (self != as_paxos_succession_getprincipal()) {
		while (NULL != (t = as_paxos_transaction_getnext(from_id))) {
			/* Update the generation count in anticipation of the sync message*/
			p->gen.sequence = t->gen.sequence;
			p->gen.proposal = t->gen.proposal;
			n_xact++;
			cf_debug(AS_PAXOS, "Non-principal retiring transaction 0x%x (nc %d) from %"PRIx64, t, t->c.n_change, from_id);
			as_paxos_transaction_retire(t);
		}

		cf_debug(AS_PAXOS, "Non-principal retired %d transactions", n_xact);

		return;
	}

	while (NULL != (t = as_paxos_transaction_getnext(self))) {
		cf_debug(AS_PAXOS, "Applying transaction 0x%x", t);

		if ((t->c.p_node != 0) && (t->c.p_node != as_paxos_succession_getprincipal()))
			cf_warning(AS_PAXOS, "Applying transaction not from %"PRIx64" principal is %"PRIx64"",
					   t->c.p_node, as_paxos_succession_getprincipal());

		if ((t->c.p_node == 0))
			cf_warning(AS_PAXOS, "Applying transaction from null principal node");

		/* Update the generation count */
		p->gen.sequence = t->gen.sequence;
		p->gen.proposal = t->gen.proposal;
		for (int i = 0; i < t->c.n_change; i++) {
			switch(t->c.type[i]) {
				case AS_PAXOS_CHANGE_NOOP:
					break;
				case AS_PAXOS_CHANGE_SUCCESSION_ADD:
					if (g_config.self_node == t->c.id[i]) {
						cf_warning(AS_PAXOS, "Found self %"PRIx64" on the succession list!", g_config.self_node);
					}
					cf_debug(AS_PAXOS, "inserting node %"PRIx64"", t->c.id[i]);
					n_xact++;
					if (0 != as_paxos_succession_insert(t->c.id[i]))
						cf_crash(AS_PAXOS, "succession list full");

					break;
				case AS_PAXOS_CHANGE_SUCCESSION_REMOVE:
					cf_debug(AS_PAXOS, "removing failed node %"PRIx64"", t->c.id[i]);
					if (p->principal_pro_tempore == t->c.id[i]) {
						cf_info(AS_PAXOS, "removed node %"PRIx64" is no longer principal pro tempore", t->c.id[i]);
						p->principal_pro_tempore = 0;
					}
					n_xact++;
					if (0 != as_paxos_succession_remove(t->c.id[i]))
						cf_warning(AS_PAXOS, "unable to remove node from succession: %"PRIx64"", t->c.id[i]);

					if (g_config.self_node == t->c.id[i])
						cf_warning(AS_PAXOS, "voted off the island! - ignoring...");

					break;
				case AS_PAXOS_CHANGE_UNKNOWN:
				default:
					cf_warning(AS_PAXOS, "unknown command, ignoring");
					break;
			}
		}

		cf_debug(AS_PAXOS, "Principal retiring transaction 0x%x", t);
		as_paxos_transaction_retire(t);
	}

	cf_debug(AS_PAXOS, "Principal applied %d transactions", n_xact);

	if (0 == n_xact) {
		/*
		 * No transactions have been applied. So do not send sync messages.
		 * This is most likely because confirmation messages have crossed over
		 */
		cf_warning(AS_PAXOS, "No changes applied on paxos confirmation message, principal is %"PRIx64". No sync messages will be sent", as_paxos_succession_getprincipal());
		return;
	}

	as_paxos_start_second_phase();
}

/* as_paxos_wire_change_create
 * Create an as_paxos_change object. */
static as_paxos_wire_change *
as_paxos_wire_change_create()
{
	as_paxos_wire_change *wc;

	int wc_sz = sizeof(as_paxos_wire_change) + (sizeof(uint8_t) + sizeof(cf_node)) * g_config.paxos_max_cluster_size;

	if (!(wc = cf_malloc(wc_sz))) {
		cf_crash(AS_PAXOS, "Failed to allocate an as_paxos_wire_change of size %d", wc_sz);
		return 0;
	}

	return wc;
}

/* as_paxos_wire_change_initialize
 * Create and initialize an as_paxos_wire_change object from an as_paxos_change object. */
static int
as_paxos_wire_change_initialize(as_paxos_wire_change **pwc, as_paxos_change *c)
{
	as_paxos_wire_change *wc;

	if (!(wc = as_paxos_wire_change_create())) {
		cf_crash(AS_PAXOS, "Failed to allocate an as_paxos_wire_change");
		return -1;
	}

	wc->p_node = c->p_node;
	wc->n_change = c->n_change;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++)
		wc->payload[i] = c->type[i];

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++)
		*(cf_node *)&(wc->payload[g_config.paxos_max_cluster_size + i * sizeof(cf_node)]) = c->id[i];

	*pwc = wc;
	return 0;
}

/* as_paxos_wire_change_destroy
 * Free a previously-allocated as_paxos_wire_change object. */
static void
as_paxos_wire_change_destroy(as_paxos_wire_change *wc)
{
	cf_free(wc);
}

/* as_paxos_change_copy_from_as_paxos_wire_change
 * Copy the contents an as_paxos_change object from an as_paxos_wire_change object. */
static int
as_paxos_change_copy_from_as_paxos_wire_change(as_paxos_change *c, as_paxos_wire_change *wc)
{
	c->p_node = wc->p_node;
	c->n_change = wc->n_change;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++)
		c->type[i] = wc->payload[i];

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++)
		c->id[i] = *(cf_node *) & (wc->payload[g_config.paxos_max_cluster_size + i * sizeof(cf_node)]);

	return 0;
}

/* as_paxos_msg_wrap
 * Encapsulate a Paxos transaction into a new message structure; returns a
 * pointer to the message, or NULL on error */
msg *
as_paxos_msg_wrap(as_paxos_transaction *t, uint32_t c)
{
	msg *m = NULL;
	int e = 0;

	if (!AS_PAXOS_ENABLED()) {
		cf_warning(AS_PAXOS, "Paxos messaging disabled ~~ Not wrapping message");
		return(NULL);
	}

	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid transaction");

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get a fabric message");
		return(NULL);
	}

	/* Wrap up the message contents; track all the return values as we go */
	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, c);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, t->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, t->gen.proposal);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. */
	if (!AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	as_paxos_wire_change *wc;

	if (0 != as_paxos_wire_change_initialize(&wc, &t->c)) {
		cf_crash(AS_PAXOS, "Failed to create as_paxos_wire_change object.");
		return(NULL);
	}

	size_t bufsz = sizeof(as_paxos_wire_change) + (sizeof(uint8_t) + sizeof(cf_node)) * g_config.paxos_max_cluster_size;
	e += msg_set_buf(m, AS_PAXOS_MSG_CHANGE, (void *)wc, bufsz, MSG_SET_COPY);
	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to wrap message");
		return(NULL);
	}

	as_paxos_wire_change_destroy(wc);

	return(m);
}

/* as_paxos_msg_unwrap
 * De-encapsulate a Paxos message into the provided transaction structure;
 * returns the message command type, or -1 on error */
int
as_paxos_msg_unwrap(msg *m, as_paxos_transaction *t)
{
	uint32_t c;
	int e = 0;
	as_paxos_wire_change *bufp = NULL;
	size_t bufsz;

	if (!AS_PAXOS_ENABLED()) {
		cf_warning(AS_PAXOS, "Paxos messaging disabled ~~ Not unwrapping message");
		return(-1);
	}

	cf_assert(m, AS_PAXOS, CF_CRITICAL, "invalid message");
	cf_assert(t, AS_PAXOS, CF_CRITICAL, "invalid transaction");

	/* Initialize the transaction structure */
	memset(t, 0, sizeof(as_paxos_transaction));

	/* Make sure this is actually a Paxos message */
	if (0 > msg_get_uint32(m, AS_PAXOS_MSG_ID, &c)) {
		cf_warning(AS_PAXOS, "received Paxos message without a valid ID");
		return(-1);
	}

	/* Check the protocol. */
	if (AS_PAXOS_PROTOCOL_IDENTIFIER() != c) {
		cf_warning(AS_PAXOS, "received Paxos message not for the currently active protocol version (received 0x%04x ; expected 0x%04x) ~~ Ignoring message!", c, AS_PAXOS_PROTOCOL_IDENTIFIER());
		return(-1);
	}

	/* The Paxos protocol v2 or greater provides a means of peaceful coexistence between nodes with different maximum cluster sizes:
	   If the succession list length of the incoming message does not agree with our maximum cluster size, simply ignore it. */
	if (AS_PAXOS_MSG_V1_IDENTIFIER != c) {
		if (0 > (e += msg_get_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, &c))) {
			cf_warning(AS_PAXOS, "Received Paxos protocol v%d message without succession list length ~~ Ignoring message!", AS_PAXOS_PROTOCOL_VERSION_NUMBER(c));
			return(-1);
		}
		if (c != g_config.paxos_max_cluster_size) {
			cf_warning(AS_PAXOS, "Received Paxos message with a different maximum cluster size (received %d ; expected %d) ~~ Ignoring message!", c, g_config.paxos_max_cluster_size);
			return(-1);
		}
	}

	/* Unwrap the message contents; track all the return values as we go.
	 * Because synchronization messages are not expected to have generation
	 * numbers or change structures, they are handled slightly differently
	 * from the Paxos protocol messages.
	 * NB: the strange gyrations around bufp are because of the semantics of
	 * msg_get_buf() and the fact that the as_paxos_change structure is
	 * allocated directly within the as_paxos_transaction */
	e += msg_get_uint32(m, AS_PAXOS_MSG_COMMAND, &c);
	if (c != AS_PAXOS_MSG_COMMAND_SYNC_REQUEST && c != AS_PAXOS_MSG_COMMAND_SYNC &&
		c != AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST && c != AS_PAXOS_MSG_COMMAND_PARTITION_SYNC  &&
		c != AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT && c != AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK) {
		e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &t->gen.sequence);
		e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, &t->gen.proposal);
		size_t orig_bufsz = sizeof(as_paxos_wire_change) + (sizeof(uint8_t) + sizeof(cf_node)) * g_config.paxos_max_cluster_size;
		bufsz = orig_bufsz;
		e += msg_get_buf(m, AS_PAXOS_MSG_CHANGE, (byte **)&bufp, &bufsz, MSG_GET_DIRECT);
		if (bufsz != orig_bufsz)
			cf_warning(AS_PAXOS, "received Paxos message with size %d, expected size %d", bufsz, orig_bufsz);
	}

	if (0 > e) {
		cf_warning(AS_PAXOS, "message unwrapping failed");
		return(-1);
	}

	if (NULL != bufp)
		as_paxos_change_copy_from_as_paxos_wire_change(&t->c, bufp);

	return(c);
}

/* as_paxos_send_to_sl
 * Send a msg to the proposed succession list. */
static int
as_paxos_send_to_sl(int cmd, as_paxos_transaction *tr, msg *px_msg, int priority)
{
	int rv = 0;
	as_node_list nl;
	as_paxos *p = g_config.paxos;
	int succ_list_len = 0, succ_list_num_alive = 0, sent_count = 0;

	nl.sz = 1;
	nl.nodes[0] = g_config.self_node;
	nl.alloc_sz = MAX_NODES_LIST;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if ((p->alive[i] && (g_config.self_node != p->succession[i]))) {
			nl.nodes[(nl.sz)++] = p->succession[i];
		}

		if (p->succession[i]) {
			succ_list_len++;
		}

		if (p->alive[i]) {
			succ_list_num_alive++;
		}
	}

	cf_debug(AS_PAXOS, "Paxos succession list length %d ; num. alive %d", succ_list_len, succ_list_num_alive);

	if (nl.sz == 1) {
		if (!(rv = as_fabric_send(nl.nodes[0], px_msg, priority))) {
			sent_count++;
		}
		goto Done;
	}

	// careful with the ref count here: need to increment before every
	// send except the last
	for (int i = 0; i < nl.sz ; i++) {
		if (i != nl.sz - 1) {
			msg_incr_ref(px_msg);
		}
		if (!(rv = as_fabric_send(nl.nodes[i], px_msg, priority))) {
			sent_count++;
		} else {
			goto Cleanup;
		}
	}
	goto Done;

Cleanup:
	as_fabric_msg_put(px_msg);

Done:
	cf_debug(AS_PAXOS, "sent Paxos command %s for transaction [%d.%d]@%"PRIx64" to %d of %d nodes",
			 as_paxos_cmd_name[cmd], tr->gen.sequence, tr->gen.proposal, g_config.self_node, sent_count, nl.sz);

	return rv;
}

/* as_paxos_spark
 * Begin a Paxos state change */
void
as_paxos_spark(as_paxos_change *c)
{
	as_paxos *p = g_config.paxos;
	cf_node self = g_config.self_node;
	as_paxos_transaction *s, t;
	msg *m = NULL;

	cf_debug(AS_PAXOS, "Entering as_paxos_spark");

	/* Populate a new transaction with the change list*/
	if (NULL == c) {
		cf_warning(AS_PAXOS, "Illegal params in as_paxos_spark");
		return;
	}
	memcpy(&t.c, c, sizeof(as_paxos_change));

	/* No matter what, we mark the node as dead immediately and check
	 * for quorum visibility; this lets us stop-the-world quickly if
	 * the cluster has collapsed
	 */
	for (int i = 0; i < t.c.n_change; i++) {
		if (t.c.type[i] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) {
			cf_debug(AS_PAXOS, "Node departure %"PRIx64"", t.c.id[i]);
			if (p->principal_pro_tempore == t.c.id[i]) {
				cf_info(AS_PAXOS, "in spark, removed node %"PRIx64" is no longer principal pro tempore", t.c.id[i]);
				p->principal_pro_tempore = 0;
			}
			as_paxos_succession_setdeceased(t.c.id[i]);
			if (false == as_paxos_succession_quorum()) {
				// Note:  Current policy is to continue providing service availability,
				//         even upon node departure exceeding the previous cluster quorum.
				cf_debug(AS_PAXOS, "quorum visibility lost! Continuing anyway ...");
			}
		}
	}

	/*
	 * If this is not the principal, we are done
	 */
	if (self != as_paxos_succession_getprincipal()) {
		cf_debug(AS_PAXOS, "I'm not Paxos principal ~~ Not sparking.");
		return;
	}

	t.gen.sequence = as_paxos_current_get_sequence() + 1;
	t.gen.proposal = 0;

	char change_buf[AS_CLUSTER_SZ * (2 * sizeof(cf_node) + 6) + 1] = { '\0' };
	char *op = "", *cb = change_buf;
	int count = 0;
	for (int i = 0; i < t.c.n_change; i++) {
		switch (t.c.type[i]) {
		  case AS_PAXOS_CHANGE_NOOP:
			  op = "NOP";
			  break;
		  case AS_PAXOS_CHANGE_SYNC:
			  op = "SYN";
			  break;
		  case AS_PAXOS_CHANGE_SUCCESSION_ADD:
			  op = "ADD";
			  break;
		  case AS_PAXOS_CHANGE_SUCCESSION_REMOVE:
			  op = "DEL";
			  break;
		  case AS_PAXOS_CHANGE_UNKNOWN:
		  default:
			  op = "UNC";
			  break;
		}
		count = sprintf(cb, "%s %"PRIx64"; ", op, t.c.id[i]);
		cb += count;
	}
	*cb = '\0';
	cf_info(AS_PAXOS, "as_paxos_spark establishing transaction [%"PRIu32".%"PRIu32"]@%"PRIx64" ClSz = %d ; # change = %d : %s",
			t.gen.sequence, t.gen.proposal, g_config.self_node, p->cluster_size, t.c.n_change, change_buf);

	/* Populate a new transaction with the change, establish it
	 * in the pending transaction table, and send the message */
	if (NULL == (s = as_paxos_transaction_establish(&t))) {
		cf_warning(AS_PAXOS, "unable to establish transaction");
		return;
	}

	int cmd = AS_PAXOS_MSG_COMMAND_PREPARE;
	if (NULL == (m = as_paxos_msg_wrap(s, cmd))) {
		cf_warning(AS_PAXOS, "failed to construct Paxos command %s msg", as_paxos_cmd_name[cmd]);
		return;
	}

	int rv;
	if ((rv = as_paxos_send_to_sl(cmd, s, m, AS_FABRIC_PRIORITY_HIGH))) {
		cf_warning(AS_PAXOS, "in spark, sending Paxos command %s to successon list failed: rv %d", as_paxos_cmd_name[cmd], rv);
		as_fabric_msg_put(m);
	}
}

/* as_paxos_msgq_push
 * A shim to push an incoming message onto the Paxos queue.  NB: all message
 * processing is deferred! */
int
as_paxos_msgq_push(cf_node id, msg *m, void *udata)
{
	as_paxos *p = g_config.paxos;
	as_paxos_msg *qm;

	cf_assert(m, AS_PAXOS, CF_CRITICAL, "null message");

	qm = cf_calloc(1, sizeof(as_paxos_msg));
	cf_assert(qm, AS_PAXOS, CF_CRITICAL, "allocation: %s", cf_strerror(errno));
	qm->id = id;
	qm->m = m;

	uint32_t c;
	msg_get_uint32(m, AS_PAXOS_MSG_COMMAND, &c);
	cf_debug(AS_PAXOS, "PAXOS message with ID %d received from node %"PRIx64"", c, id);

	if (0 != cf_queue_push(p->msgq, &qm))
		cf_warning(AS_PAXOS, "PUSH FAILED: PAXOS message with ID %d received from node %"PRIx64"", c, id);

	return(0);
}

/* as_paxos_event
 * An event processing stub for messages coming from the fabric */
int
as_paxos_event(int nevents, as_fabric_event_node *events, void *udata)
{
	if ((1 > nevents) || (g_config.paxos_max_cluster_size < nevents) || !events) {
		cf_warning(AS_PAXOS, "Illegal state in as_paxos_event, node events is: %d", nevents);
		return (0);
	}

	msg *m = NULL;
	int e = 0;

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "as_paxos_event: unable to get a fabric message");
		return(0);
	}

	/* Wrap up the message contents; track all the return values as we go */
	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT);
	e += msg_set_uint32(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS_COUNT, nevents);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. */
	if (!AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	e += msg_set_buf(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS, (void *)events, sizeof(as_fabric_event_node) * g_config.paxos_max_cluster_size, MSG_SET_COPY);
	if (0 > e) {
		cf_warning(AS_PAXOS, "as_paxos_event: unable to wrap heartbeat message");
		return(0);
	}

	return (as_paxos_msgq_push(g_config.self_node, m, NULL));
}

/*
 *  Forcibly set the Paxos succession list to the given list of node IDs
 *  and then trigger new cluster formation if this node is the principal node
 *  (i.e., the first entry of the nodes list.)
 */
void
as_paxos_process_set_succession_list(cf_node *nodes)
{
	cf_debug(AS_PAXOS, "Paxos thread processing Set Succession List event:");

	if (nodes) {
		cf_node node, *nodes_p = nodes;
		int i = 0;
		while ((node = *nodes_p++)) {
			cf_debug(AS_PAXOS, "SLNode[%d] = %"PRIx64"", i, node);
			i++;
		}
	}

	// Halt migrations before forcibly modifying the succession list.
	// [Note:  This is also done on the principal when the second phase is started below.]
	as_partition_disallow_migrations();

	as_paxos *p = g_config.paxos;
	bool list_end = false;
	cf_node *nodes_p = nodes;
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (!list_end) {
			if (!(p->succession[i] = *nodes_p++)) {
				list_end = true;
			}
			p->alive[i] = true;
		} else {
			p->succession[i] = 0;
			p->alive[i] = false;
		}
	}

	if (g_config.self_node == nodes[0]) {
		cf_warning(AS_PAXOS, "[I am the principal node ~~ Forcing a cluster SYNC event.]");
		as_paxos_start_second_phase();
	} else {
		cf_warning(AS_PAXOS, "[I am not the principal node ~~ Waiting for a SYNC event.]");
	}
}

void
as_paxos_process_heartbeat_event(msg *m)
{
	int e = 0;
	uint32_t nevents = 0;
	size_t bufsz = sizeof(as_fabric_event_node) * g_config.paxos_max_cluster_size;
	as_fabric_event_node *events = NULL;
	/*
	 * Extract the heartbeat information from the message
	 */
	e += msg_get_uint32(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS_COUNT, &nevents);
	e += msg_get_buf(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS, (byte **)&events, &bufsz, MSG_GET_DIRECT);

	if (0 > e) {
		cf_warning(AS_PAXOS, "as_paxos_process_heartbeat_event: unable to unwrap heartbeat message");
		return;
	}

	/*
	 * See if we are principal now
	 */
	cf_node old_principal = as_paxos_succession_getprincipal();
	cf_node self = g_config.self_node;
	as_paxos *p = g_config.paxos;

	as_paxos_change c;
	memset(&c, 0, sizeof(as_paxos_change));
	int j = 0;

	/*
	 * First remove departed nodes. We need to do that to compute the new principal
	 */
	for (int i = 0; i < nevents; i++)
		switch (events[i].evt) {
			case FABRIC_NODE_ARRIVE:
			case FABRIC_NODE_UNDUN:
				/*
				 * Do nothing. Will process in next loop
				 */
				break;
			case FABRIC_NODE_DEPART:
			case FABRIC_NODE_DUN:
				if (p->principal_pro_tempore == events[i].nodeid) {
					cf_info(AS_PAXOS, "departed node %"PRIx64" is no longer principal pro tempore", events[i].nodeid);
					p->principal_pro_tempore = 0;
				}
				if (as_paxos_succession_ismember(events[i].nodeid)) {
					c.type[j] = AS_PAXOS_CHANGE_SUCCESSION_REMOVE;
					c.id[j] = events[i].nodeid;
					cf_debug(AS_PAXOS, "Node departure %"PRIx64"", c.id[j]);
					as_paxos_succession_setdeceased(c.id[j]);
					j++;
					if (false == as_paxos_succession_quorum()) {
						// Note:  Current policy is to continue providing service availability,
						//         even upon node departure exceeding the previous cluster quorum.
						cf_debug(AS_PAXOS, "quorum visibility lost! Continuing anyway ...");
					}
				}
				break;
			default:
				cf_warning(AS_PAXOS, "unknown event type received in as_paxos_event() - aborting");
				return;
		}

	cf_node principal = as_paxos_succession_getprincipal();

	for (int i = 0; i < nevents; i++)
		switch (events[i].evt) {
			case FABRIC_NODE_ARRIVE:
			case FABRIC_NODE_UNDUN:

				// Mark the node as alive if it is already in the succession list.
				as_paxos_succession_setrevived(events[i].nodeid);

				/*
				 * Check if this pulse came from a node whose principal is different than ours
				 * This means two clusters are merging - figure out who wins
				 */

				if (! as_paxos_succession_ismember(events[i].nodeid)) {
					cf_debug(AS_PAXOS, "Node arrival %"PRIx64" cluster principal %"PRIx64" pulse principal %"PRIx64"",
							 events[i].nodeid, principal, events[i].p_node);

					if ((0 != events[i].p_node) && (events[i].p_node != principal)) {
						if (principal < events[i].p_node) {
							/*
							 * We lose. We wait to be assimilated by the other
							 * TODO: Should we send a sync message to the other principal
							 */
							cf_info(AS_PAXOS, "Skip node arrival %"PRIx64" cluster principal %"PRIx64" pulse principal %"PRIx64"",
									 events[i].nodeid, principal, events[i].p_node);
							//if (true == as_paxos_succession_ismember(events[i].nodeid))
							//	cf_warning(AS_PAXOS, "Skipped arrival node %"PRIx64" is in succession list!", events[i].nodeid);
							//if (true == as_paxos_succession_ismember(events[i].p_node))
							//	cf_warning(AS_PAXOS, "Skipped arrival node's principal %"PRIx64" is in succession list!", events[i].p_node);
							break; // skip this event
						}
					}

					c.type[j] = AS_PAXOS_CHANGE_SUCCESSION_ADD;
					c.id[j] = events[i].nodeid;
					cf_debug(AS_PAXOS, "Node arrival %"PRIx64"", c.id[j]);
					j++;
				}

				break;
			case FABRIC_NODE_DEPART:
			case FABRIC_NODE_DUN:
				/* Already processed in earlier loop */
				break;
			default:
				cf_warning(AS_PAXOS, "unknown event type received in as_paxos_event() - aborting");
				return;
		}

	/*
	 * If we find that we are the new principal and we were not an old principal before, then any non-live nodes in the paxos list need to be added to the change list
	 */
	if ((self == principal) && (self != old_principal)) {
		/* Go through the succession list and add nodes into the change list that are marked as dead */

		for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
			if (p->succession[i] == (cf_node) 0)
				continue;
			if (p->alive[i])
				continue;
			bool found = false;
			for (int k = 0; k < j; k++)
				if ((c.type[k] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) && (c.id[k] == p->succession[i])) {
					found = true;
					break;
				}
			if (!found) {
				c.type[j] = AS_PAXOS_CHANGE_SUCCESSION_REMOVE;
				c.id[j] = p->succession[i];
				cf_debug(AS_PAXOS, "Adding dead node %"PRIx64" to departure list", c.id[j]);
				j++;
			}
		}
	}

	c.n_change = j;
	/*
	 *  The principal is stored with the change to enable members of other clusters to ignore this transaction
	 */
	c.p_node = principal;

	/*
	 * Spark paxos if there are any events
	 */
	if (c.n_change > 0) {
		as_paxos_spark(&c);
	} else {
		cf_debug(AS_PAXOS, "Skipping call as_paxos_spark");
	}
}

void
as_paxos_send_partition_sync_request(cf_node p_node) {
	msg *reply = NULL;
	if (NULL == (reply = as_paxos_partition_sync_request_msg_generate())) {
		cf_warning(AS_PAXOS, "unable to construct partition sync request message to node %"PRIx64"", p_node);
		return;
	} else if (0 != as_fabric_send(p_node, reply, AS_FABRIC_PRIORITY_HIGH)) {
		cf_warning(AS_PAXOS, "unable to send partition sync message to node %"PRIx64"", p_node);
		as_fabric_msg_put(reply);
		return;
	}
	cf_info(AS_PAXOS, "Sent partition sync request to node %"PRIx64"", p_node);
}

/* as_paxos_retransmit_check
 * An event processing stub for messages coming from the fabric */
int
as_paxos_retransmit_check()
{
	msg *m = NULL;
	int e = 0;

	if (!AS_PAXOS_ENABLED()) {
		cf_info(AS_PAXOS, "Paxos messaging disabled ~~ Not retransmitting check message.");
		return(0);
	}

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "as_paxos_event: unable to get a fabric message");
		return(0);
	}

	/* Wrap up the message contents; track all the return values as we go */
	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. */
	if (!AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	if (0 > e) {
		cf_warning(AS_PAXOS, "as_paxos_retransmit_check: unable to wrap retransmit check message");
		return(0);
	}

	return (as_paxos_msgq_push(g_config.self_node, m, NULL));
}

/* as_paxos_dun_hold
 * Set whether automatic dunning of all nodes in other clusters should persist. */
void
as_paxos_dun_hold(bool is_dunned)
{
	g_config.paxos->dun_other_clusters = is_dunned;
}

#define NODE_IS_MISSING 0
#define NODE_IS_NOT_MISSING -1

int
as_paxos_add_missing_node(cf_node *missing_nodes, cf_node node)
{
	int i = 0;
	for (i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// find node in succ_list that is not in succession
		cf_node curr = missing_nodes[i];
		if (curr == (cf_node) 0)
			break;
		if (curr == node) // already added
			return (NODE_IS_NOT_MISSING);
	}
	if (i == g_config.paxos_max_cluster_size)
		cf_warning(AS_PAXOS, "as_paxos_add_missing_node: Node %"PRIx64" cannot be added. No space in missing node list", node);
	else
		missing_nodes[i] = node;

	return (NODE_IS_MISSING);
}

void
as_paxos_add_missing_nodes(cf_node *missing_nodes, cf_node *succ_list, bool *are_nodes_not_dunned)
{
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// find node in succ_list that is not in succession
		cf_node curr = succ_list[i];
		if (curr == (cf_node) 0)
			break;

		if (NODE_IS_MISSING == as_paxos_add_missing_node(missing_nodes, curr)) {
			if (! as_hb_get_is_node_dunned(curr)) {
				*are_nodes_not_dunned = true;
			}
		}
	}
}

void
as_paxos_process_retransmit_check()
{
	as_paxos *p = g_config.paxos;

	// Perform a general consistency check between our succession list and the list
	// that heart beat thinks is correct. First get a copy of the heartbeat's compiled list
	cf_node succ_list_index[AS_CLUSTER_SZ];
	cf_node succ_list[AS_CLUSTER_SZ][AS_CLUSTER_SZ];
	cf_node missing_nodes[AS_CLUSTER_SZ];
	memset(missing_nodes, 0, sizeof(missing_nodes));

	// lock
	pthread_mutex_lock(&g_config.hb_paxos_lock);

	memcpy(succ_list_index, g_config.hb_paxos_succ_list_index, sizeof(succ_list_index));
	memcpy(succ_list, g_config.hb_paxos_succ_list, sizeof(succ_list));

	// unlock
	pthread_mutex_unlock(&g_config.hb_paxos_lock);

	// for each node in the succession list
	// compare the node's succession list with this server's succession list

	bool cluster_integrity_fault = false;
	bool are_nodes_not_dunned = false;
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		cf_debug(AS_PAXOS, "Cluster Integrity Check: %d, %"PRIx64"", i, succ_list_index[i]);
		if (succ_list_index[i] == (cf_node) 0) {
			break; // we are done
		}

		if (p->succession[0] != succ_list[i][0]) {
			if (as_hb_get_is_node_dunned(succ_list_index[i])) {
				cf_debug(AS_PAXOS, "[Skipping integrity check vs. dunned node %"PRIx64" in other cluster w/ principal %"PRIx64"]", succ_list_index[i], succ_list[i][0]);
				continue;
			} else if (p->dun_other_clusters) {
				cf_warning(AS_PAXOS, "Dunning node %"PRIx64" in other cluster (w/ principal %"PRIx64")", succ_list_index[i], succ_list[i][0]);
				as_hb_set_is_node_dunned(succ_list_index[i], true, "Paxos Integrity Check");
				continue;
			}
		}

		char sbuf[(AS_CLUSTER_SZ * 17) + 27];
		snprintf(sbuf, 27, "HEARTBEAT %"PRIx64" :", succ_list_index[i]);
		for (int j = 0; j < g_config.paxos_max_cluster_size; j++) {
			if ((cf_node)0 != succ_list[i][j]) {
				snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", succ_list[i][j]);
			}
		}
		cf_debug(AS_PAXOS, sbuf);
		if (memcmp(p->succession, succ_list[i], sizeof(succ_list[i])) != 0) {
			cf_info(AS_PAXOS, "Cluster Integrity Check: Detected succession list discrepancy between node %"PRIx64" and self %"PRIx64"",
					succ_list_index[i], g_config.self_node);

#if 0
			as_paxos_dump_succession_list("Paxos List", p->succession, i);
			as_paxos_dump_succession_list("Node List", succ_list[i], i);
#endif

			if (g_config.auto_dun) {
				as_hb_set_is_node_dunned(succ_list_index[i], true, "paxos");
			}

			cluster_integrity_fault = true;
			as_paxos_add_missing_nodes(missing_nodes, succ_list[i], &are_nodes_not_dunned);
		}
	} // end for each node

	cf_node p_node = as_paxos_succession_getprincipal();

	if (cluster_integrity_fault) {
		char sbuf[(AS_CLUSTER_SZ * 17) + 99];

		switch (g_config.paxos_recovery_policy) {

			case AS_PAXOS_RECOVERY_POLICY_MANUAL:
			{
				if (are_nodes_not_dunned) {
					snprintf(sbuf, 97, "CLUSTER INTEGRITY FAULT. [Phase 1 of 2] To fix, issue this command across all nodes:  dun:nodes=");
				} else {
					snprintf(sbuf, 99, "CLUSTER INTEGRITY FAULT. [Phase 2 of 2] To fix, issue this command across all nodes:  undun:nodes=");
				}

				bool nodes_missing = false;
				for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
					if ((cf_node)0 == missing_nodes[i]) {
						break;
					}

					snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64",", missing_nodes[i]);
					nodes_missing = true;
				}

				// Remove last comma
				sbuf[strlen(sbuf) - 1] = 0;

				if (nodes_missing)
					cf_info(AS_PAXOS, sbuf);

				break;
			}

			case AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_MASTER:
			{
				static int delay = 0;
				sbuf[0] = '\0';
				bool nodes_missing = false;
				for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
					if ((cf_node)0 == missing_nodes[i]) {
						break;
					}

					snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64",", missing_nodes[i]);
					nodes_missing = true;
				}

				if (p->cluster_size > 1) {
					if (are_nodes_not_dunned) {
						cf_warning(AS_PAXOS, "CLUSTER INTEGRITY FAULT DETECTED!  I am %sprincipal.", (g_config.self_node == as_paxos_succession_getprincipal() ? "" : "not "));
						if  (g_config.self_node == as_paxos_succession_getprincipal()) {
							snprintf(sbuf, 17, "%"PRIx64, g_config.self_node);
							cf_warning(AS_PAXOS, "AUTOMATIC CLUSTER INTEGRITY FAULT RECOVERY [Phase 1 of 2]:  dunning self:  %s.", sbuf);
							as_hb_set_are_nodes_dunned(sbuf, strlen(sbuf), true);
						}
					} else {
						if  (g_config.self_node == as_paxos_succession_getprincipal()) {
							if (++delay < AS_PAXOS_AUTO_DUN_MASTER_DELAY) {
								cf_warning(AS_PAXOS, "I am principal and waiting to undun self:  Count %d of %d.", delay, AS_PAXOS_AUTO_DUN_MASTER_DELAY);
							} else {
								cf_warning(AS_PAXOS, "I am principal and the delay (%d) has expired.", delay);
								snprintf(sbuf, 17, "%"PRIx64, g_config.self_node);
								cf_warning(AS_PAXOS, "AUTOMATIC CLUSTER INTEGRITY FAULT RECOVERY [Phase 2 of 2]:  undunning self:  %s.", sbuf);
								as_hb_set_are_nodes_dunned(sbuf, strlen(sbuf), false);
								delay = 0;
							}
						}
					}
				}

				break;
			}

			case AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_ALL:
			{
				static int delay = 0;
				static cf_node principal = 0;
				sbuf[0] = '\0';
				bool nodes_missing = false;
				for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
					if ((cf_node)0 == missing_nodes[i]) {
						break;
					}

					snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64",", missing_nodes[i]);
					nodes_missing = true;
				}

				if ((p->cluster_size > 1) && are_nodes_not_dunned) {
					principal = as_paxos_succession_getprincipal();
					cf_warning(AS_PAXOS, "CLUSTER INTEGRITY FAULT DETECTED!  I am %s principal.", (g_config.self_node == principal ? "" : "not "));
					snprintf(sbuf, 17, "%"PRIx64, principal);
					cf_warning(AS_PAXOS, "AUTOMATIC CLUSTER INTEGRITY FAULT RECOVERY [Phase 1 of 2]:  dunning principal:  %s.", sbuf);
					as_hb_set_are_nodes_dunned(sbuf, strlen(sbuf), true);
					delay = 0;
				} else {
					if (principal) {
						if (++delay < AS_PAXOS_AUTO_DUN_ALL_DELAY) {
							cf_warning(AS_PAXOS, "I am %sprincipal and waiting to undun principal:  Count %d of %d.",
									   (g_config.self_node == principal ? "" : "not "), delay, AS_PAXOS_AUTO_DUN_ALL_DELAY);
						} else {
							cf_warning(AS_PAXOS, "I am %sprincipal and the delay (%d) has expired.", (g_config.self_node == principal ? "" : "not "), delay);
							snprintf(sbuf, 17, "%"PRIx64, principal);
							cf_warning(AS_PAXOS, "AUTOMATIC CLUSTER INTEGRITY FAULT RECOVERY [Phase 2 of 2]:  undunning principal:  %s.", sbuf);
							as_hb_set_are_nodes_dunned(sbuf, strlen(sbuf), false);
							principal = 0;
							delay = 0;
						}
					} else {
						cf_warning(AS_PAXOS, "principal not set ~~ not automatically undunning");
					}
				}

				break;
			}

			default:
				cf_crash(AS_PAXOS, "unknown Paxos recovery policy %d", g_config.paxos_recovery_policy);
		}
	}

	as_paxos_set_cluster_integrity(p, !cluster_integrity_fault);

	// If migration is enabled, we are already in a cluster, hence we are done.
	if (as_partition_get_migration_flag() == true) {
		return;
	}

	// Otherwise, we are in the middle of a Paxos reconfiguration of the cluster:
	//   - Principal sends a SYNC message to all cluster nodes (including itself.)
	//   - Non-principals send a PARTITION_SYNC_REQUEST message to the principal.
	if (g_config.self_node == as_paxos_succession_getprincipal()) {
		cf_info(AS_PAXOS, "as_paxos_retransmit_check: principal %"PRIx64" retransmitting sync messages to nodes that have not responded yet ... ", p_node);
		as_paxos_send_sync_messages();
	} else {
		cf_info(AS_PAXOS, "as_paxos_retransmit_check: node %"PRIx64" retransmitting partition sync request to principal %"PRIx64" ... ", g_config.self_node, p_node);
		as_paxos_send_partition_sync_request(p_node);
	}
}

/* as_paxos_thr
 * A thread to handle all Paxos events */
void *
as_paxos_thr(void *arg)
{
	as_paxos *p = g_config.paxos;
	cf_node self = g_config.self_node;
	int c;

	/* Event processing loop */
	for ( ; ; ) {
		as_paxos_msg *qm = NULL;
		msg *reply = NULL;
		/* NB: t is the transaction being processed; s is a pointer to the
		 * corresponding entry in the pending transaction list; r is a pointer
		 * to a rejected transaction */
		as_paxos_transaction *r, *s, t;

		cf_debug(AS_PAXOS, "Popping paxos queue 0x%x", p->msgq);

		/* Get the next message from the queue */
		if (0 != cf_queue_pop(p->msgq, &qm, CF_QUEUE_FOREVER))
			cf_crash(AS_PAXOS, "cf_queue_pop failed");

		/* Unwrap and sanity check the message, then undertake the
		 * appropriate action */
		if (0 > (c = as_paxos_msg_unwrap(qm->m, &t))) {
			cf_warning(AS_PAXOS, "failed to unwrap Paxos message from node %"PRIx64" ~~ check Paxos protocol version", qm->id);
			goto cleanup;
		}

		cf_debug(AS_PAXOS, "unwrapped | received paxos message from node %"PRIx64" command %s (%d)", qm->id, as_paxos_cmd_name[c], c);

		if (c == AS_PAXOS_MSG_COMMAND_SET_SUCC_LIST) {
			as_paxos_process_set_succession_list(t.c.id);
			c = AS_PAXOS_MSG_COMMAND_SYNC;
			goto cleanup;
		}

		if (c == AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT) {
			as_paxos_process_heartbeat_event(qm->m);
			goto cleanup;
		}

		if (c == AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK) {
			as_paxos_process_retransmit_check();
			goto cleanup;
		}

		/* Hold the state lock */
//        if (0 != pthread_mutex_lock(&p->lock))
//		    cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_CRITICAL, "couldn't get Paxos lock: %s", cf_strerror(errno));

		/* Only the principal will accept messages from nodes that aren't in
		 * the succession, unless they're synchronization messages */
		if (false == as_paxos_succession_ismember(qm->id)) {
			cf_debug(AS_PAXOS, "got a message from a node not in the succession: %"PRIx64, qm->id);
			if (!((self == as_paxos_succession_getprincipal()) || (AS_PAXOS_MSG_COMMAND_SYNC == c))) {
				cf_debug(AS_PAXOS, "ignoring message from a node not in the succession: %"PRIx64" command %d", qm->id, c);
				goto cleanup;
			}
		}

		cf_node principal = as_paxos_succession_getprincipal();
		/*
		 * Refuse transactions with changes initiated by a principal that is not the current principal
		 * If the principal node is set to 0, let this through. This will be the case for sync messages
		 */
		if ((t.c.p_node != 0) && (t.c.p_node != principal)) {
			/*
			 * Check if this new principal out ranks our own principal - could have just arrived
			 * Since it is possible we have not yet removed failed nodes for our state
			 * reject the transaction only if it is also from a node NOT in our current
			 * succession list
			 */
			if ((t.c.p_node < principal) && (false == as_paxos_succession_ismember(qm->id))) {
				cf_debug(AS_PAXOS, "Ignoring transaction from principal %"PRIx64" < current principal %"PRIx64" from node %"PRIx64" not in succession list", t.c.p_node, principal, qm->id);
				goto cleanup;
			}
			/*
			 * reject transaction if a node from the succession list is sending this to us and we are the principal
			 */
			if ((true == as_paxos_succession_ismember(qm->id)) && (principal == self)) {
				cf_debug(AS_PAXOS, "Ignoring transaction from node %"PRIx64" in succession list", qm->id);
				goto cleanup;
			}
		}

		// Check if our principal is sending a node removal list that contains us
		// This can actually happen in some one-way network situations.
		// Ignore this transaction. The principal will not bother since our vote will not be needed for this vote
		// We are only getting this message because the principal is sending to all nodes known by fabric
		if ((t.c.p_node == principal) && (self != principal)) {
			for (int i = 0; i < t.c.n_change; i++) {
				switch(t.c.type[i]) {
					case AS_PAXOS_CHANGE_NOOP:
						break;
					case AS_PAXOS_CHANGE_SUCCESSION_ADD:
						if (self == t.c.id[i]) {
							cf_info(AS_PAXOS, "Ignoring self(%"PRIx64") add from Principal %"PRIx64"", self, principal);
							goto cleanup;
						}
						break;
					case AS_PAXOS_CHANGE_SUCCESSION_REMOVE:
						if (self == t.c.id[i]) {
							cf_info(AS_PAXOS, "Ignoring self(%"PRIx64") remove from Principal %"PRIx64"", self, principal);
							goto cleanup;
						}
						break;
					case AS_PAXOS_CHANGE_UNKNOWN:
					default:
						cf_warning(AS_PAXOS, "unknown command, ignoring");
						break;
				}
			}
		}

		switch(c) {
			case AS_PAXOS_MSG_COMMAND_PREPARE:
			case AS_PAXOS_MSG_COMMAND_COMMIT:
				cf_debug(AS_PAXOS, "received prepare/commit message from %"PRIx64"", qm->id);
				/* Search for a transaction with this sequence number in the
				 * pending transaction list */
				if (NULL != (s = as_paxos_transaction_search(t))) {
					/* We've seen this transaction before: check the proposal
					 * number */
					if (t.gen.proposal < s->gen.proposal) {
						/* Reject: the proposal number is less than latest
						 * one we've received */
						cf_debug(AS_PAXOS, "rejecting older Paxos command gen:  %d vs. %d", t.gen.proposal, s->gen.proposal);
						reply = as_paxos_msg_wrap(s, as_paxos_state_next(c, NACK));
					} else {
						/* Accept: if the proposal number is newer, update
						 * our copy of the transaction */
						if (t.gen.proposal > s->gen.proposal) {
							as_paxos_transaction_update(s, &t);
						}
						reply = as_paxos_msg_wrap(s, as_paxos_state_next(c, ACK));
					}
				} else {
					/* We've never seen a proposal for this sequence number */
					if (as_paxos_current_is_candidate(t)) {
						/* Accept */
						if (NULL == (s = as_paxos_transaction_establish(&t))) {
							cf_warning(AS_PAXOS, "unable to establish transaction");
							break;
						}
						reply = as_paxos_msg_wrap(s, as_paxos_state_next(c, ACK));
					} else
						/* Reject: the proposed sequence number is out of
						 * order */
						/* FIXME we need to come up with a different way to do this */
						reply = as_paxos_msg_wrap(as_paxos_current_get(), as_paxos_state_next(c, NACK));
				}

				/* Error check the reply and transmit it to the principal */
				if (NULL == reply)
					cf_warning(AS_PAXOS, "unable to construct reply message");
				else if (0 != as_fabric_send(qm->id, reply, AS_FABRIC_PRIORITY_HIGH))
					as_fabric_msg_put(reply);

				break;
			case AS_PAXOS_MSG_COMMAND_PREPARE_ACK:
			case AS_PAXOS_MSG_COMMAND_COMMIT_ACK:
				cf_debug(AS_PAXOS, "received prepare_ack/commit_ack message from %"PRIx64"", qm->id);
				if (self != as_paxos_succession_getprincipal()) {
					cf_debug(AS_PAXOS, "I'm not principal ~~ Ignoring ACK %d message from %"PRIx64, c, qm->id);
					break;
				}

				if (NULL == (s = as_paxos_transaction_search(t))) {
					cf_warning(AS_PAXOS, "received acknowledgment for unknown type");
					break;
				}

				/* Attempt to record the vote; if this results in a quorum,
				 * send a commit message and reset the vote count */
				switch(as_paxos_transaction_vote(s, qm->id, &t)) {
					case AS_PAXOS_TRANSACTION_VOTE_ACCEPT:
						cf_debug(AS_PAXOS, "received ACCEPT vote from %"PRIx64"", qm->id);
						break;
					case AS_PAXOS_TRANSACTION_VOTE_REJECT:
						cf_debug(AS_PAXOS, "received REJECT vote from %"PRIx64"", qm->id);
						break;
					case AS_PAXOS_TRANSACTION_VOTE_QUORUM:
						cf_debug(AS_PAXOS, "received ACCEPT vote from %"PRIx64" and reached quorum", qm->id);
						int cmd, rv;
						if (!(reply = as_paxos_msg_wrap(s, cmd = as_paxos_state_next(c, ACK)))) {
							cf_warning(AS_PAXOS, "failed to wrap Paxos command %s msg", as_paxos_cmd_name[cmd]);
							break;
						}
						if ((rv = as_paxos_send_to_sl(cmd, s, reply, AS_FABRIC_PRIORITY_HIGH))) {
							cf_warning(AS_PAXOS, "sending Paxos command %s to successon list failed: rv %d", as_paxos_cmd_name[cmd], rv);
							as_fabric_msg_put(reply);
						}
						as_paxos_transaction_vote_reset(s);
						break;
				}

				break;
			case AS_PAXOS_MSG_COMMAND_PREPARE_NACK:
			case AS_PAXOS_MSG_COMMAND_COMMIT_NACK:
				cf_debug(AS_PAXOS, "received prepare_nack/commit_nack message from %"PRIx64"", qm->id);
				if (self != as_paxos_succession_getprincipal()) {
					cf_debug(AS_PAXOS, "I'm not principal ~~ Ignoring NACK %d message from %"PRIx64, c, qm->id);
					break;
				}

				if (NULL == (r = as_paxos_transaction_search(t))) {
					cf_warning(AS_PAXOS, "received negative acknowledgment for unknown transaction");
					break;
				}

				break;
			case AS_PAXOS_MSG_COMMAND_CONFIRM:
				/* At this point, we cannot complain -- so we just accept
				 * what we're told */
				cf_debug(AS_PAXOS, "received state confirmation message from %"PRIx64"", qm->id);
				if (NULL == (s = as_paxos_transaction_search(t))) {
					s = as_paxos_transaction_establish(&t);
				} else {
					as_paxos_transaction_update(s, &t);
				}
				as_paxos_transaction_confirm(s);

				/*
				 * If we are the principal and this confirmation message is not from us, ignore it.
				 * This case happens when two clusters are merging and the winning cluster's principal assimilates this node.
				 * The subsequent sync message from the new principal will clean the state up
				 */
				cf_node principal = as_paxos_succession_getprincipal();
				if ((self == principal) && (qm->id != self)) {
					cf_debug(AS_PAXOS, "Principal %"PRIx64" is ignoring confirmation message from foreign principal %"PRIx64"", self, qm->id);
					break;
				}
				/*
				 * Apply the transaction locally and send
				 * sync requests to all nodes other than the principal.
				 */
				as_paxos_transaction_apply(qm->id);

				/*
				 * Check for the single node cluster case but only if the node is principal
				 */
				if ((true == as_paxos_partition_sync_states_all()) && (self == principal)) {
					/*
					 * The principal can now balance its partitions
					 * Should we have another phase to the synchronizations to make sure that every
					 * cluster node has had its state updated before starting partition rebalance?
					 * Currently, the answer to this question is "no."
					 */
					cf_info(AS_PAXOS, "SINGLE NODE CLUSTER!!!");
					/* Clean out the sync states array */
					memset(p->partition_sync_state, 0, sizeof(p->partition_sync_state));

					as_partition_balance();

					if (p->cb) {
						as_paxos_change c;
						c.n_change = 1;
						c.type[0] = AS_PAXOS_CHANGE_SYNC;
						c.id[0] = 0;
						for (int i = 0; i < p->n_callbacks; i++)
							(p->cb[i])(p->gen, &c, p->succession, p->cb_udata[i]);
					}
				}

				/*
				 *  This should not happen, but log a warning if it ever does.
				 */
				if (p->principal_pro_tempore && (principal != p->principal_pro_tempore) && (principal != g_config.self_node)) {
					cf_warning(AS_PAXOS, "joining cluster with non-self principal %"PRIx64" which is not principal pro tempore %"PRIx64,
							   principal, p->principal_pro_tempore);
				}

				/*
				 * The principal pro tempore no longer, but the principal in fact.
				 */
				p->principal_pro_tempore = 0;

				/*
				 * TODO - We need to detect case where paxos messages get lost and retransmit
				 */

				break;
			case AS_PAXOS_MSG_COMMAND_SYNC_REQUEST:
				cf_debug(AS_PAXOS, "received sync request message from %"PRIx64"", qm->id);
				if (self != as_paxos_succession_getprincipal())
					break;

				uint64_t cluster_key = as_paxos_get_cluster_key();
				if (NULL == (reply = as_paxos_sync_msg_generate(cluster_key)))
					cf_warning(AS_PAXOS, "unable to construct reply message");
				else if (0 != as_fabric_send(qm->id, reply, AS_FABRIC_PRIORITY_HIGH))
					as_fabric_msg_put(reply);

				break;
			case AS_PAXOS_MSG_COMMAND_SYNC:
				cf_debug(AS_PAXOS, "received sync message from %"PRIx64"", qm->id);

				/*
				 * A principal should never get the sync message unless
				 * it is from another principal as part of cluster merge
				 */
				principal = as_paxos_succession_getprincipal();

				/*
				 * Check if this new principal out ranks our own principal - could have just arrived
				 * Since it is possible we have not yet removed failed nodes for our state
				 * reject the transaction only if it is also from a node NOT in our current
				 * succession list
				 */
				if ((qm->id < principal) && (false == as_paxos_succession_ismember(qm->id))) {
					cf_debug(AS_PAXOS, "Ignoring sync message from principal %"PRIx64" < current principal %"PRIx64" and not in succession list", qm->id, principal);
					break;
				}

				/*
				 * Check if the principal sending the node is greater than our current principal or is part of the succession list
				 */
				if (self == principal) {
					cf_debug(AS_PAXOS, "Principal applying sync message from %"PRIx64"", qm->id);
				}

				/*
				 * Check if we have already SYNC'd with a greater principal pro tempore.
				 */
				if (qm->id < p->principal_pro_tempore) {
					cf_info(AS_PAXOS, "Ignoring sync message from principal %"PRIx64" < principal pro tempore %"PRIx64,
							qm->id, p->principal_pro_tempore);
					break;
				}

				if (0 != as_paxos_sync_msg_apply(qm->m)) {
					cf_warning(AS_PAXOS, "unable to apply received state from sync msg");
					break;
				}

				char sbuf[(AS_CLUSTER_SZ * 17) + 49];
				snprintf(sbuf, 49, "SUCCESSION [%d.%d]@%"PRIx64"*: ", p->gen.sequence, p->gen.proposal, qm->id);
				for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
					if ((cf_node)0 != p->succession[i]) {
						snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", p->succession[i]);
					}
				}
				cf_info(AS_PAXOS, sbuf);
				as_paxos_print_cluster_key("SYNC MESSAGE");

				if (qm->id != p->succession[0]) {
					cf_warning(AS_PAXOS, "Received paxos sync message from someone who is not principal %"PRIx64"", qm->id);
				}

				/*
				 * The incoming SYNC message principal is now our principal pro tempore.
				 */
				cf_info(AS_PAXOS, "node %"PRIx64" is %s principal pro tempore",
						qm->id, (p->principal_pro_tempore != qm->id ? "now" : "still"));
				p->principal_pro_tempore = qm->id;

				/*
				 * Send the partition state to the principal as part of
				 * the partition sync request
				 */
				as_paxos_send_partition_sync_request(qm->id);
				break;
			case AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST:
				cf_debug(AS_PAXOS, "received partition sync request message from %"PRIx64"", qm->id);
				if (self != as_paxos_succession_getprincipal()) {
					cf_warning(AS_PAXOS, "Received paxos partition sync request - not a principal, ignoring ...");
					break;
				}
				/*
				 * First note this node's partition data, assuming the paxos sequence matches
				 * If all nodes have not responded, then do nothing.
				 * If all nodes have responded, send PARTITION_SYNC to all nodes.
				 */
				cf_info(AS_PAXOS, "Received paxos partition sync request from %"PRIx64"", qm->id);
				int npos; // find position of the sender in the succession list
				if (0 > (npos = as_paxos_get_succession_index(qm->id))) {
					/*
					 * This is an inconsistent state and is detected and fixed in the heartbeat code.
					 */
					cf_warning(AS_PAXOS, "Received paxos partition sync request from node not in succession list, ignoring ...");
					break;
				}
				bool already_sent_partition_sync_messages = as_paxos_partition_sync_states_all();
				/*
				 * apply the partition sync request
				 */
				if ((0 != as_paxos_partition_sync_request_msg_apply(qm->m, npos)) || (false == as_paxos_set_partition_sync_state(qm->id))) {
					cf_warning(AS_PAXOS, "unable to apply received state in partition sync request from node %"PRIx64"", qm->id);
					break;
				}

				if (true == as_paxos_partition_sync_states_all()) {
					if (already_sent_partition_sync_messages) { // this is a retransmission of partition sync messages
						cf_info(AS_PAXOS, "Re-sending paxos partition sync message to %"PRIx64"", p->succession[npos]);
						if (NULL == (reply = as_paxos_partition_sync_msg_generate()))
							cf_warning(AS_PAXOS, "unable to construct partition sync message to node %"PRIx64"", p->succession[npos]);
						else if (0 != as_fabric_send(p->succession[npos], reply, AS_FABRIC_PRIORITY_HIGH)) {
							as_fabric_msg_put(reply);
							cf_warning(AS_PAXOS, "unable to send partition sync message to node %"PRIx64"", p->succession[npos]);
						}
					}
					else { //sending partition sync message to all nodes
						cf_info(AS_PAXOS, "All partition data has been received by principal");
						for (int i = 1; i < g_config.paxos_max_cluster_size; i++) { /* skip the principal */
							if (((cf_node)0 != p->succession[i]) && p->alive[i]) {
								cf_info(AS_PAXOS, "Sending paxos partition sync message to %"PRIx64"", p->succession[i]);
								if (NULL == (reply = as_paxos_partition_sync_msg_generate()))
									cf_warning(AS_PAXOS, "unable to construct partition sync message to node %"PRIx64"", p->succession[i]);
								else if (0 != as_fabric_send(p->succession[i], reply, AS_FABRIC_PRIORITY_HIGH)) {
									as_fabric_msg_put(reply);
									cf_warning(AS_PAXOS, "unable to send partition sync message to node %"PRIx64"", p->succession[i]);
								}
							}
						}
						/* Do not cleanout the sync states array - allow for retransmits*/
						// memset(p->partition_sync_state, 0, sizeof(p->partition_sync_state));
						/*
						* The principal can now balance its partitions
						* Should we have another phase to the synchronizations to make sure that every
						* cluster node has had its state updated before starting partition rebalance?
						* Currently, the answer to this question is "no."
						*/
						as_partition_balance();

						if (p->cb) {
							as_paxos_change c;
							c.n_change = 1;
							c.type[0] = AS_PAXOS_CHANGE_SYNC;
							c.id[0] = 0;
							for (int i = 0; i < p->n_callbacks; i++)
								(p->cb[i])(p->gen, &c, p->succession, p->cb_udata[i]);
						}
					}
				}

				break;
			case AS_PAXOS_MSG_COMMAND_PARTITION_SYNC:
				cf_info(AS_PAXOS, "received partition sync message from %"PRIx64"", qm->id);
				/*
				 * Received the cluster's current partition data. Make sure that the paxos sequence matches
				 * Accept the message and continue normal processing if the paxos sequence matches
				 * Ignore message if the paxos sequence does not match.
				 */

				if (0 != as_paxos_partition_sync_msg_apply(qm->m)) {
					cf_warning(AS_PAXOS, "unable to apply partition sync message state");
					break;
				}

				/*
				 * We now need to perform migrations as a result of external
				 * synchronizations, since nodes outside the cluster could contain data due to a cluster merge
				 */
				as_partition_balance();

				if (p->cb) {
					as_paxos_change c;
					c.n_change = 1;
					c.type[0] = AS_PAXOS_CHANGE_SYNC;
					c.id[0] = 0;
					for (int i = 0; i < p->n_callbacks; i++)
						(p->cb[i])(p->gen, &c, p->succession, p->cb_udata[i]);
				}
				break;
			default:
				cf_warning(AS_PAXOS, "unknown command %d received from %"PRIx64"", c, qm->id);
				break;
		}

cleanup:
		/* Release the state lock */
//        if (0 != pthread_mutex_unlock(&p->lock))
//		    cf_fault(CF_FAULT_SCOPE_THREAD, CF_FAULT_SEVERITY_CRITICAL, "couldn't release Paxos lock: %s", cf_strerror(errno));

		/* Free the message */
		as_fabric_msg_put(qm->m);
		cf_free(qm);
	}

	return(NULL);
}

/* as_paxos_init
 * Initialize the Paxos state structures */
void
as_paxos_init()
{
	g_config.paxos = cf_calloc(1, sizeof(as_paxos));
	cf_assert(g_config.paxos, AS_PAXOS, CF_CRITICAL, "allocation: %s", cf_strerror(errno));

	as_paxos *p = g_config.paxos; // shortcut pointer

	if (0 != pthread_mutex_init(&p->lock, NULL)) {
		cf_crash(AS_PAXOS, "unable to init mutex: %s", cf_strerror(errno));
	}

	as_paxos_set_cluster_integrity(p, false);

	as_paxos_current_init(p);
	p->msgq = cf_queue_create(sizeof(void *), true);
	p->ready = false;
	p->cluster_size = 0;

	// For now there's one paxos, with the info callback as the first callback.
	p->n_callbacks = 1;
	p->cb[0] = as_info_paxos_event;
	p->cb_udata[0] = NULL;

	// initialize the global structures for storing succession lists from heartbeats
	memset(g_config.hb_paxos_succ_list_index, 0, sizeof(g_config.hb_paxos_succ_list_index));
	memset(g_config.hb_paxos_succ_list, 0, sizeof(g_config.hb_paxos_succ_list));

	/* Register with the fabric */
	as_fabric_register_event_fn(&as_paxos_event, NULL);
	as_fabric_register_msg_fn(M_TYPE_PAXOS, as_paxos_msg_template, sizeof(as_paxos_msg_template), &as_paxos_msgq_push, NULL);

	/* this may not be needed but just do it anyway */
	memset(p->c_partition_vinfo, 0, sizeof(p->c_partition_vinfo));
	/* Clean out the sync states array */
	memset(p->partition_sync_state, 0, sizeof(p->partition_sync_state));

	memset(p->succession, 0, sizeof(p->succession));
	p->succession[0] = g_config.self_node;

	memset(p->alive, 0, sizeof(p->alive));
	p->alive[0] = true;

	p->ready = true;

	// Generate a non-zero cluster key.
	uint64_t cluster_key;

	while ((cluster_key = cf_get_rand64()) == 0) {
		;
	}

	/*
	 * Set the cluster key
	 */
	as_paxos_set_cluster_key(cluster_key);
	as_paxos_print_cluster_key("IGNITION");

	size_t successful_storage_reads = 0;
	size_t failed_storage_reads = 0;
	size_t n_null_storage = 0;
	size_t n_found_storage = 0;
	size_t n_uninit_storage = 0;
	/* Mark all partitions in all namespaces as lost unless there is partition data in storage*/
	for (int i = 0; i < g_config.namespaces; i++) {
		/* Initialize every partition's iid and vtp values */
		as_namespace *ns = g_config.namespace[i];

		if (NAMESPACE_HAS_PERSISTENCE(ns)) { // skip loop if no persistent storage
			for (int j = 0; j < AS_PARTITIONS; j++) {
				as_partition_vinfo vinfo;
				size_t vinfo_len = sizeof(vinfo);

				// Find if the value has been set in storage
				if (0 == as_storage_info_get(ns, j, (uint8_t *)&vinfo, &vinfo_len)) {
					successful_storage_reads++;
					if (vinfo_len == sizeof(as_partition_vinfo)) {
						cf_debug(AS_PAXOS, "{%s:%d} Partition version read from storage: iid %"PRIx64"", ns->name, j, vinfo.iid);
						memcpy(&ns->partitions[j].version_info, &vinfo, sizeof(as_partition_vinfo));
						if (is_partition_null(&vinfo))
							n_null_storage++;
						else {
							cf_debug(AS_PAXOS, "{%s:%d} Partition successful revive from storage", ns->name, j);
							ns->partitions[j].state = AS_PARTITION_STATE_SYNC;
							n_found_storage++;
						}
					}
					else { // treat partition as lost - common on startup
						cf_debug(AS_PAXOS, "{%s:%d} Error getting info from storage, got len %d; partition will be treated as lost", ns->name, j, vinfo_len);
						n_uninit_storage++;
					}
				}
				else {
					failed_storage_reads++;
				}
			} // end for
		} // end if

		/* Allocate and initialize the global partition state structure */
		size_t vi_sz = sizeof(as_partition_vinfo) * AS_PARTITIONS;
		as_partition_vinfo *vi = cf_rc_alloc(vi_sz);
		cf_assert(vi, AS_PAXOS, CF_CRITICAL, "rc_alloc: %s", cf_strerror(errno));
		for (int j = 0; j < AS_PARTITIONS; j++)
			memcpy(&vi[j], &ns->partitions[j].version_info, sizeof(as_partition_vinfo));
		p->c_partition_vinfo[i][0] = vi;

		/* Initialize the partition sizes array for sending in all Paxos protocol v3 or greater PARTITION_SYNC_REQUEST and PARTITION_SYNC messages. */
		if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
			for (int j = 0; j < AS_PARTITIONS; j++) {
				p->c_partition_size[i][0][j] = ns->partitions[j].vp ? ns->partitions[j].vp->elements : 0;
				p->c_partition_size[i][0][j] += ns->partitions[j].sub_vp ? ns->partitions[j].sub_vp->elements : 0;
			}
		}
	}

	cf_debug(AS_PAXOS, "storage info reads: total %d successful %d failed %d", successful_storage_reads + failed_storage_reads, successful_storage_reads, failed_storage_reads);
	cf_info(AS_PAXOS, "partitions from storage: total %d found %d lost(set) %d lost(unset) %d", n_found_storage + n_null_storage + n_uninit_storage, n_found_storage, n_null_storage, n_uninit_storage);

	as_partition_balance_init();

	cf_info(AS_PAXOS, "Paxos service ignited: %"PRIx64, p->succession[0]);
}

/*
 *  Register/deregister a Paxos cluster state change callback function.
 *
 *  XXX -- These two functions not are thread safe with respect to callbacks
 *          being simultaneously registered and deregistered (very unlikely),
 *          as well as if callbacks are being (de-)registered simultaneous with
 *          cluster state changes (also very unlikely), since the callback
 *          invocations happen on a separate thread (the Paxos thread.)
 */

int
as_paxos_register_change_callback(as_paxos_change_callback cb, void *udata)
{
	as_paxos *p = g_config.paxos;

	if (p->n_callbacks < MAX_CHANGE_CALLBACKS - 1) {
		p->cb[p->n_callbacks] = cb;
		p->cb_udata[p->n_callbacks] = udata;
		p->n_callbacks++;
		return(0);
	}
	return(-1);
}

int
as_paxos_deregister_change_callback(as_paxos_change_callback cb, void *udata)
{
	as_paxos *p = g_config.paxos;
	int i = 0;
	bool found = false;

	while (i < p->n_callbacks) {
		if (!found && (p->cb[i] == cb) && (p->cb_udata[i] == udata)) {
			found = true;
			p->cb[i] = NULL;
			p->cb_udata[i] = NULL;
		} else if (found) {
			p->cb[i - 1] = p->cb[i];
			p->cb_udata[i - 1] = p->cb_udata[i];
		}
		i++;
	}

	if (found) {
		p->n_callbacks--;
	}

	return (found ? 0 : -1);
}

/* as_paxos_sup_thr
 * paxos supervisor logic for retransmission */
void *
as_paxos_sup_thr(void *arg)
{
	cf_info(AS_PAXOS, "paxos supervisor thread started");

	for ( ; ; ) {

		struct timespec delay = { g_config.paxos_retransmit_period, 0 };
		nanosleep(&delay, NULL);
		// drop a retransmit check paxos message into the paxos message queue.
		as_paxos_retransmit_check();

	}

	return(NULL);
}

/* as_paxos_start
 * Start the Paxos service */
void
as_paxos_start()
{
	uint32_t wait_ms = g_config.hb_timeout * g_config.hb_interval * 2;
	uint32_t wait_intervals = wait_ms < 100 ? 1 : wait_ms / 100;

	cf_info(AS_PAXOS, "listening for other nodes (max %u milliseconds) ...",
			wait_ms);

	while (wait_intervals > 0) {
		usleep(100000);

		if (as_partition_balance_is_multi_node_cluster()) {
			// Heartbeats have been received from other node(s) - we'll be in a
			// multi-node cluster.
			cf_info(AS_PAXOS, "... other node(s) detected - node will operate in a multi-node cluster");
			break;
		}

		wait_intervals--;
	}

	if (wait_intervals == 0) {
		// Didn't hear from other nodes, assume we'll be a single node cluster.
		cf_info(AS_PAXOS, "... no other nodes detected - node will operate as a single-node cluster");

		as_partition_balance_init_single_node_cluster();
	}

	as_paxos *p = g_config.paxos;
	pthread_attr_t thr_attr;
	pthread_t thr_id;
	pthread_t sup_thr_id;

	/* Start the Paxos service thread */
	if (0 != pthread_attr_init(&thr_attr))
		cf_crash(AS_PAXOS, "unable to initialize thread attributes: %s", cf_strerror(errno));
	if (0 != pthread_attr_setscope(&thr_attr, PTHREAD_SCOPE_SYSTEM))
		cf_crash(AS_PAXOS, "unable to set thread scope: %s", cf_strerror(errno));
	if (0 != pthread_create(&thr_id, &thr_attr, as_paxos_thr, p))
		cf_crash(AS_PAXOS, "unable to create paxos thread: %s", cf_strerror(errno));
	if (0 != pthread_create(&sup_thr_id, 0, as_paxos_sup_thr, 0))
		cf_crash(AS_PAXOS, "unable to create paxos supervisor thread: %s", cf_strerror(errno));
}

/* as_paxos_get_cluster_integrity
 * Get the Paxos cluster integrity state.
 */
bool
as_paxos_get_cluster_integrity(as_paxos *p)
{
	return p->cluster_has_integrity;
}

/* as_paxos_set_cluster_integrity
 * Set the Paxos cluster integrity state.
 */
void
as_paxos_set_cluster_integrity(as_paxos *p, bool state)
{
	p->cluster_has_integrity = state;
}

/* as_paxos_dump
 * Print info. about the Paxos state to the log.
 * (Verbose true prints partition map as well.)
 */
void
as_paxos_dump(bool verbose)
{
	as_paxos *p = g_config.paxos;
	bool self = false, principal = false;

	cf_info(AS_PAXOS, "Paxos Cluster Size: %d [soft max: %d ; hard max: %d]", p->cluster_size, g_config.paxos_max_cluster_size, AS_CLUSTER_SZ);

	cf_info(AS_PAXOS, "Cluster Key: %"PRIx64"", as_paxos_get_cluster_key());

	cf_info(AS_PAXOS, "Cluster Generation: [%d.%d]@%"PRIx64, p->gen.sequence, p->gen.proposal, p->succession[0]);

	cf_info(AS_PAXOS, "Cluster State: Has Integrity %s", (p->cluster_has_integrity ? "" : "FAULT"));

	cf_info(AS_PAXOS, "Migrations are%s allowed.", (as_partition_get_migration_flag() ? "" : " NOT"));

	// Print the succession list.
	cf_node principal_node = as_paxos_succession_getprincipal();
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		cf_node node = p->succession[i];
		if ((cf_node) 0 == node)
			continue;
		self = (node == g_config.self_node);
		principal = (node == principal_node);
		cf_info(AS_PAXOS, "SuccessionList[%d]: Node %"PRIx64" %s%s %s", i, node,
				(self ? "[Self]" : ""), (principal ? "[Principal]" : ""), (p->alive[i] ? "" : "DEAD"));
	}
}

/* as_paxos_get_succession_list
 * Get the Paxos succession list and log it to the given "cf_dyn_buf *".
 * The first element of the list will become the Paxos principal.
 * Returns 0 if successful, -1 otherwise.
 */
int as_paxos_get_succession_list(cf_dyn_buf *db)
{
	as_paxos *p = g_config.paxos;
	char hex_node_id[18];
	bool need_comma = false;
	char line[AS_CLUSTER_SZ * 17];
	int pos = 0;

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		cf_node node = p->succession[i];

		if ((cf_node) 0 == node)
			continue;

		snprintf(hex_node_id, 17 + (need_comma ? 1 : 0), "%s%"PRIx64"", (need_comma ? "," : ""), node);
		pos += snprintf(&line[pos], 17 + (need_comma ? 1 : 0), "%s%"PRIx64"", (need_comma ? "," : ""), node);
		cf_dyn_buf_append_string(db, hex_node_id);

		need_comma = true;
	}
	cf_dyn_buf_append_string(db, "\n");
	cf_info(AS_PAXOS, "Paxos Succession List: %s", line);

	return 0;
}

/* as_paxos_set_succession_list
 * Set the Paxos succession list from list of node IDs.
 * The first element of the list will become the Paxos principal.
 * Returns 0 if successful, -1 otherwise.
 */
int
as_paxos_set_succession_list(char *nodes_str, int nodes_str_len)
{
	cf_info(AS_PAXOS, "Paxos set succ list: nodes = \"%s\"", nodes_str);

	as_paxos_transaction px_trans;
	int num_nodes = 0;
	if (as_hb_nodes_str_to_cf_nodes(nodes_str, nodes_str_len, px_trans.c.id, &num_nodes)) {
		cf_warning(AS_PAXOS, "set-sl command: failed to parse node list string (\"%s\") to nodes", nodes_str);
		return -1;
	}

	// Send a Set Succession List event to Paxos.

	msg *px_msg;
	if (!(px_msg = as_paxos_msg_wrap(&px_trans, AS_PAXOS_MSG_COMMAND_SET_SUCC_LIST))) {
		cf_warning(AS_PAXOS, "failed to wrap Set Succ List msg");
		return -1;
	}

	return as_paxos_msgq_push(g_config.self_node, px_msg, NULL);
}
