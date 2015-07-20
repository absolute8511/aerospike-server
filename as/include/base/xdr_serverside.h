/*
 * xdr_serverside.h
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
 * runtime structures and definitions for serverside of XDR
 */

#pragma once

#include "datamodel.h"
#include "xdr_config.h"


int as_xdr_supported();
int as_xdr_init();
int as_xdr_shutdown();
void xdr_broadcast_lastshipinfo(uint64_t val[]);
int xdr_create_named_pipe(xdr_config *xc);
int xdr_send_nsinfo();
int xdr_send_nodemap();
int xdr_send_clust_state_change(cf_node node, int8_t change);
uint64_t xdr_min_lastshipinfo();
void xdr_clmap_update(int changetype, cf_node succession[], int listsize);
void xdr_write(as_namespace *ns, cf_digest keyd, as_generation generation, cf_node masternode, bool is_delete, uint16_t set_id);
void as_xdr_start();
int as_open_namedpipe();
int as_xdr_stop();
int as_info_command_xdr(char *name, char *params, cf_dyn_buf *db);
int xdr_internal_read_response(as_namespace *ptr_namespace, int tr_result_code, uint32_t generation, uint32_t void_time, as_bin** as_bins, uint16_t n_as_bins, char* setname, void* from_xdr);
int as_xdr_set_shipping(bool shipping_status);
void xdr_handle_failednodeprocessingdone(cf_node);
