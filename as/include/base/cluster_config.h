/*
 * cluster_config.h
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
 * cluster topology, Rack Awareness
 *
 */
#pragma once
#include <stdint.h> // needed for the uintXX_t types
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "fault.h"
#include "util.h"

/*
** global #defines that control features/functionality go here
*/


/* SYNOPSIS
 * Describe the cluster topology and hold the rules for managing the cluster.
 * For example, nodes in the same group should not hold both the master and
 * a prole for the same partition.
 * <> Node Names
 * <> Group (i.e. Rack) Names
 * <> Cluster Names
 * <> Data Center Names
 */

/**
 * The Cluster Config structure shows the topology of the cluster.
 * Each Node has an ID and is optionally a member of a group. For
 * earlier versions of Aerospike the name was configured automatically,
 * but with the introduction of V4 (topology), we now allow manual
 * configuration of nodes and groups.
 *
 * There is an implicit hierarchy of a cluster:
 * Company -> Data Centers -> Clusters -> Groups -> Nodes
 *
 * V1: May 2013.  All we're doing now is the Group Membership, since
 * that is needed to compute "Rack Awareness".
 * (*) Statically defined
 */
#define CL_MODE_NO_TOPOLOGY 0
#define CL_MODE_STATIC 1
#define CL_MODE_DYNAMIC 2
#define CL_STR_NONE   "none"
#define CL_STR_STATIC "static"
#define CL_STR_DYNAMIC "dynamic"
extern const char *cc_mode_str[];
// TODO:
// NOTE: These values will be set to the GLOBAL "Max Node Count" value.
#define CL_MAX_NODES 127
#define CL_MAX_GROUPS 127

// Define the types we'll use to hold Group ID and Node ID
// Once upon a time, it was all uint16_t, but once things changed, it became
// clear that we should define types for these values.

/* Hold the Group ID portion of the config.self_node value */
typedef uint16_t cc_group_t;
/* Hold the Node ID portion of the config.self_node value */
typedef uint32_t cc_node_t;

typedef enum { unknown, balanced, unbalanced, invalid } cluster_state_t;
extern const char *cc_state_str[];

typedef struct cluster_config_s {
	// We moved this up to the TOP LEVEL config section
	// uint8_t  cluster_mode; // Off (0) Static (1) or Dynamic (2)

	cc_node_t cl_self_node; // THIS node ID (read from config file)
	cc_group_t cl_self_group; // The Group for THIS node

	uint16_t node_count; // Total number of nodes defined
	cc_node_t node_ids[CL_MAX_NODES]; // Admin configured IDs of the nodes

	uint16_t group_count; // Total number of groups defined
	uint16_t group_node_count[CL_MAX_NODES]; // Count of nodex per group
	cc_group_t group_ids[CL_MAX_NODES]; // Admin configured IDs of the groups
	cf_node full_node_val[CL_MAX_NODES]; // The full 3-part name
	uint32_t membership[CL_MAX_NODES]; // For each node position, show group
									   // membership (Index into group array).
	cluster_state_t cluster_state;

} cluster_config_t;

// Functions in cluster_config.c
void cc_cluster_config_defaults(cluster_config_t *cc);
int cc_locate_node(const cluster_config_t *cc, const cc_node_t node_id);
int cc_add_node(cluster_config_t *cc, const cc_node_t node_id);
int cc_locate_group(const cluster_config_t *cc, const cc_group_t group_id);
int cc_add_group(cluster_config_t *cc, const cc_group_t group_id);
int cc_add_node_group_entry(cluster_config_t *cc, const cc_node_t node,
		const cc_group_t group);
int cc_add_fullnode_group_entry(cluster_config_t *cc, const cf_node fullnode);
int cc_locate_node_group(const cluster_config_t *cc, const cc_node_t node_id);
uint16_t cc_compute_port(const cf_node self_node);
cc_group_t cc_compute_group_id(const cf_node self_node);
cc_node_t cc_compute_node_id(const cf_node self_node);
cf_node cc_compute_self_node(const uint16_t port_num, const cc_group_t group_id,
		const cc_node_t node_id);
void cc_show_cluster_state(const cluster_config_t *cc);
cluster_state_t cc_get_cluster_state(const cluster_config_t *cc);
void cc_cluster_config_dump(const bool verbose);
