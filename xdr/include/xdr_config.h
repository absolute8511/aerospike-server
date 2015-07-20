/*
 * xdr_config.h
 *
 * Copyright (C) 2011-2014 Aerospike, Inc.
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
 *  Server configuration declarations for the XDR module
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#include "fault.h"
#include "util.h"

//Length definitions. This should be in sync with the server definitions.
//It is bad that we are not using a common header file for all this.
#define CLUSTER_MAX_SZ		128
#define NAMESPACE_MAX_SZ	32
#define NAMESPACE_MAX_NUM	32
#define XDR_MAX_DGLOG_FILES 1

/* Configuration parser switch() case identifiers. The server (cfg.c) needs to
 * see these. The server configuration parser and the XDR configuration parser
 * will care about different subsets of these options. Order is not important,
 * other than for organizational sanity.
 */
typedef enum {
	// Generic:
	// Token not found:
	XDR_CASE_NOT_FOUND,
	// Alternative beginning of parsing context:
	XDR_CASE_CONTEXT_BEGIN,
	// End of parsing context:
	XDR_CASE_CONTEXT_END,

	// Top-level options:
	XDR_CASE_SERVICE_BEGIN,
	XDR_CASE_NAMESPACE_BEGIN,
	XDR_CASE_XDR_BEGIN,

	// Server options:
	XDR_CASE_SERVICE_USER,
	XDR_CASE_SERVICE_GROUP,

	// Namespace options:
	XDR_CASE_NS_ENABLE_XDR,
	XDR_CASE_NS_XDR_REMOTE_DATACENTER,
	XDR_CASE_NS_DEFAULT_TTL,
	XDR_CASE_NS_MAX_TTL,
	XDR_CASE_NS_STORAGE_ENGINE_BEGIN,
	XDR_CASE_NS_SET_BEGIN,

	// Namespace storage options:
	XDR_CASE_NS_STORAGE_MEMORY,
	XDR_CASE_NS_STORAGE_SSD,
	XDR_CASE_NS_STORAGE_DEVICE,
	XDR_CASE_NS_STORAGE_KV,

	// Main XDR options:
	XDR_CASE_ENABLE_XDR,
	XDR_CASE_NAMEDPIPE_PATH_OLD,
	XDR_CASE_NAMEDPIPE_PATH,
	XDR_CASE_DIGESTLOG_PATH_OLD,
	XDR_CASE_DIGESTLOG_PATH,
	XDR_CASE_ERRORLOG_PATH_OLD,
	XDR_CASE_ERRORLOG_PATH,
	XDR_CASE_INFO_PORT_OLD,
	XDR_CASE_INFO_PORT,
	XDR_CASE_DATACENTER_BEGIN,
	XDR_CASE_MAX_RECS_INFLIGHT_OLD,
	XDR_CASE_MAX_RECS_INFLIGHT,
	XDR_CASE_FORWARD_XDR_WRITES,
	XDR_CASE_THREADS_OLD,
	XDR_CASE_THREADS,
	XDR_CASE_TIMEOUT,
	XDR_CASE_STOP_WRITES_NOXDR,
	XDR_CASE_XDR_DELETE_SHIPPING_ENABLED,
	XDR_CASE_XDR_CHECK_DATA_BEFORE_DELETE,
	XDR_CASE_XDR_NSUP_DELETES_ENABLED,
	XDR_CASE_XDR_FORWARD_WITH_GENCHECK,
	XDR_CASE_XDR_CONFLICT_DIGESTDUMP,
	XDR_CASE_XDR_HOTKEY_MAXSKIP,
	XDR_CASE_XDR_SHIPPING_ENABLED,
	XDR_CASE_XDR_PIDFILE,
	XDR_CASE_XDR_WRITE_BATCH_SIZE,
	XDR_CASE_XDR_READ_BATCH_SIZE,
	XDR_CASE_XDR_DO_VERSION_CHECK,

	// Remote datacenter options:
	XDR_CASE_DC_NODE_ADDRESS_PORT,
	XDR_CASE_DC_INT_EXT_IPMAP,
	XDR_CASE_XDR_INFO_TIMEOUT,
	XDR_CASE_XDR_COMPRESSION_THRESHOLD,
	XDR_CASE_XDR_SHIP_DELAY,
	XDR_CASE_XDR_SHIP_THREADS,
	XDR_CASE_XDR_SHIP_SLAB_SIZE
} xdr_cfg_case_id;

/* Configuration parser token plus case-identifier pair. The server (cfg.c)
 * needs to see this.
 */
typedef struct xdr_cfg_opt_s {
	const char*		tok;
	xdr_cfg_case_id	case_id;
} xdr_cfg_opt;

/* The various xdr_cfg_opt arrays. The server (cfg.c) needs to see these.
 */
extern const xdr_cfg_opt XDR_GLOBAL_OPTS[];
extern const xdr_cfg_opt XDR_SERVICE_OPTS[];
extern const xdr_cfg_opt XDR_OPTS[];
extern const xdr_cfg_opt XDR_DC_OPTS[];
extern const xdr_cfg_opt XDR_NS_OPTS[];
extern const xdr_cfg_opt XDR_NS_STORAGE_OPTS[];
extern const xdr_cfg_opt XDR_NS_SET_OPTS[];

/* The various xdr_cfg_opt array counts. The server (cfg.c) needs to see these.
 */
extern const int NUM_XDR_GLOBAL_OPTS;
extern const int NUM_XDR_SERVICE_OPTS;
extern const int NUM_XDR_OPTS;
extern const int NUM_XDR_DC_OPTS;
extern const int NUM_XDR_NS_OPTS;
extern const int NUM_XDR_NS_STORAGE_OPTS;
extern const int NUM_XDR_NS_SET_OPTS;

// Some static knobs shared between XDR and asd
#define XDR_TIME_ADJUST	300000 // 5 min (ms) time value. Base macro for XDR(for LST adjustment in failure cases) and asd (as parameter for printing warrnings).

#define DC_MAX_NUM 32
typedef struct xdr_lastship_s {
	cf_node		node;
	uint64_t	time[DC_MAX_NUM];
} xdr_lastship_s;


// Config option in case the configuration value is changed
typedef struct xdr_new_config_s {
	int		xdr_write_batch_size;
	int		xdr_max_recs_inflight;
	int		xdr_read_batch_size;
	int		xdr_threads;
	int		xdr_ship_threads;   // Coinfigured number of shipper threads·
	int		xdr_ship_slab_size; // Coinfigured size of shipper thread slab size·
} xdr_new_config;

//Config option which is maintained both by the server and the XDR module
typedef struct xdr_config {
	// Is the XDR feature supported in the build?
	bool	xdr_supported;

	//This section is used by both the server and the XDR module
	bool	xdr_global_enabled;
	char	*xdr_digestpipe_path;

	//This section is not used by the server and is meant only for XDR module
	uid_t	uid;
	gid_t	gid;

	// Ring buffer configuration
	int	xdr_info_port;
	char 	*xdr_digestlog_path[XDR_MAX_DGLOG_FILES];
	uint64_t xdr_digestlog_file_size[XDR_MAX_DGLOG_FILES];
	bool 	xdr_digestlog_overwrite;
	bool	xdr_digestlog_persist;
	uint8_t xdr_num_digestlog_paths;
	char	*xdr_errorlog_path;
	cf_fault_severity xdr_errorlog_level;
	int	xdr_digestpipe_fd;
	int	xdr_local_port;
	int	xdr_write_batch_size;
	int	xdr_max_recs_inflight;
	int	xdr_read_batch_size;
	int	xdr_ship_slab_size;     // Maximum number of records, on receival of which, xdr start shipping.
	int	xdr_ship_threads;       // Number threads which will read from server and ship records.
	int	xdr_timeout;
	int	xdr_nw_timeout;
	int	xdr_threads;
	int	xdr_forward_xdrwrites;
	int	xdr_stop_writes_noxdr;
	int 	xdr_internal_shipping_delay;
	int	xdr_flag;
	xdr_new_config xdr_new_cfg;
	bool	xdr_shipping_enabled;
	bool	xdr_delete_shipping_enabled;
	bool	xdr_nsup_deletes_enabled;
	int 	xdr_hotkey_maxskip;
	bool	xdr_fwd_with_gencheck;
	char	*xdr_conflict_digestdump_filepath;
	FILE	*xdr_conflict_digestdump_file;
	bool	xdr_check_data_before_delete;
	int		xdr_info_request_timeout_ms;
	int     xdr_compression_threshold;
	char	*xdr_pidfile;
	char	*xdr_read_mode_string;
	bool	xdr_do_version_check;
} xdr_config;

// Prototypes
void xdr_config_defaults(xdr_config *c);

// XDR states
typedef enum xdr_state_e {
	XDR_COMING_UP,
	XDR_DLOG_WRITER_UP,
	XDR_UP,
	XDR_GOING_DOWN,
	XDR_DOWN
} xdr_state;
