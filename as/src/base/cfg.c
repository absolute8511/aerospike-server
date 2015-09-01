/*
 * cfg.c
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

#include "base/cfg.h"

#include <errno.h>
#include <grp.h>
#include <limits.h>
#include <pthread.h>
#include <pwd.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>

#include "xdr_config.h"

#include "aerospike/mod_lua_config.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_shash.h"

#include "cf_str.h"
#include "dynbuf.h"
#include "fault.h"
#include "hist.h"
#include "hist_track.h"
#include "msg.h"
#include "olock.h"
#include "util.h"

#include "base/cluster_config.h"
#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/security_config.h"
#include "base/thr_info.h"
#include "base/transaction_policy.h"
#include "fabric/migrate.h"


//==========================================================
// Globals.
//

// The runtime configuration instance.
as_config g_config;


//==========================================================
// Forward declarations.
//

void cfg_add_mesh_seed_addr_port(char* addr, int port);
as_set* cfg_add_set(as_namespace* ns);
void cfg_add_storage_file(as_namespace* ns, char* file_name);
void cfg_add_storage_device(as_namespace* ns, char* device_name, char* shadow_name);
void cfg_init_si_var(as_namespace* ns);
uint32_t cfg_obj_size_hist_max(uint32_t hist_max);
void cfg_create_all_histograms();
int cfg_reset_self_node(as_config* config_p);
void cfg_use_hardware_values(as_config* c);


//==========================================================
// Helper - set as_config defaults.
//

void
cfg_set_defaults()
{
	as_config *c = &g_config;

	memset(c, 0, sizeof(as_config));

	// Service defaults.
	c->paxos_single_replica_limit = 1; // by default all clusters obey replication counts
	c->n_service_threads = 4;
	c->n_transaction_queues = 4; // calculated when use_queue_per_device is set, see thr_tsvc_queue_init()
	c->n_transaction_threads_per_queue = 4;
	c->n_proto_fd_max = 15000;
	c->allow_inline_transactions = true; // allow data-in-memory namespaces to process transactions in service threads
	c->n_batch_threads = 4;
	c->batch_max_buffers_per_queue = 255; // maximum number of buffers allowed in a single queue
	c->batch_max_requests = 5000; // maximum requests/digests in a single batch
	c->batch_max_unused_buffers = 256; // maximum number of buffers allowed in batch buffer pool
	c->batch_priority = 200; // # of rows between a quick context switch?
	c->n_batch_index_threads = 4;
	c->n_fabric_workers = 16;
	c->fb_health_bad_pct = 0; // percent of successful messages in a burst at/below which node is deemed bad
	c->fb_health_good_pct = 50; // percent of successful messages in a burst at/above which node is deemed ok
	c->fb_health_msg_per_burst = 0; // health probe paxos messages per "burst", 0 disables feature
	c->fb_health_msg_timeout = 200; // milliseconds after which to give up on health probe message
	c->hist_track_back = 1800;
	c->hist_track_slice = 10;
	c->n_info_threads = 16;
	c->ldt_benchmarks = false;
	c->microbenchmarks = false;
	c->migrate_max_num_incoming = AS_MIGRATE_DEFAULT_MAX_NUM_INCOMING; // for receiver-side migration flow-control
	c->migrate_read_priority = 10; // # of rows between a quick context switch? not a great way to tune
	c->migrate_read_sleep = 500; // # of rows between a quick context switch? not a great way to tune
	c->migrate_rx_lifetime_ms = AS_MIGRATE_DEFAULT_RX_LIFETIME_MS; // for debouncing re-transmitted migrate start messages
	c->n_migrate_threads = 1;
	c->migrate_xmit_hwm = 10; // these are actually far more interesting for tuning parameters
	c->migrate_xmit_lwm = 5; // because the monitor the queue depth
	c->migrate_xmit_priority = 40; // # of rows between a quick context switch? not a great way to tune
	c->migrate_xmit_sleep = 500; // # of rows between a quick context switch? not a great way to tune
	c->nsup_delete_sleep = 100; // 100 microseconds means a delete rate of 10k TPS
	c->nsup_period = 120; // run nsup once every 2 minutes
	c->nsup_startup_evict = true;
	c->paxos_max_cluster_size = AS_CLUSTER_DEFAULT_SZ; // default the maximum cluster size to a "reasonable" value
	c->paxos_protocol = AS_PAXOS_PROTOCOL_V3; // default to 3.0 "sindex" paxos protocol version
	c->paxos_recovery_policy = AS_PAXOS_RECOVERY_POLICY_MANUAL; // default to the manual paxos recovery policy
	c->paxos_retransmit_period = 5; // run paxos retransmit once every 5 seconds
	c->proto_fd_idle_ms = 60000; // 1 minute reaping of proto file descriptors
	c->proto_slow_netio_sleep_ms = 1; // 1 ms sleep between retry for slow queries
	c->run_as_daemon = true; // set false only to run in debugger & see console output
	c->scan_max_active = 100;
	c->scan_max_done = 100;
	c->scan_max_udf_transactions = 32;
	c->scan_threads = 4;
	c->storage_benchmarks = false;
	c->ticker_interval = 10;
	c->transaction_max_ns = 1000 * 1000 * 1000; // 1 second
	c->transaction_pending_limit = 20;
	c->transaction_repeatable_read = false;
	c->transaction_retry_ms = 1000;
	as_sindex_gconfig_default(c);
	as_query_gconfig_default(c);
	c->work_directory = "/opt/aerospike";
	c->dump_message_above_size = PROTO_SIZE_MAX;
	c->fabric_dump_msgs = false;
	c->max_msgs_per_type = -1; // by default, the maximum number of "msg" objects per type is unlimited
	c->memory_accounting = false;
	c->asmalloc_enabled = true;

	// Network service defaults.
	c->socket.proto = SOCK_STREAM; // not configurable, but addr and port are
	c->socket_reuse_addr = true;

	// Fabric TCP socket keepalive defaults.
	c->fabric_keepalive_enabled = true;
	c->fabric_keepalive_time = 1; // seconds
	c->fabric_keepalive_intvl = 1; // seconds
	c->fabric_keepalive_probes = 10; // tries

	// Network heartbeat defaults.
	c->hb_mode = AS_HB_MODE_UNDEF; // must supply heartbeat mode in the configuration file
	c->hb_interval = 150;
	c->hb_timeout = 10;
	c->hb_mesh_rw_retry_timeout = 500;
	c->hb_protocol = AS_HB_PROTOCOL_V2; // default to the latest heartbeat protocol version

	// Network info defaults.
	c->info_fastpath_enabled = true; // by default, don't force Info requests to go through the transaction queue

	// XDR defaults.
	xdr_config_defaults(&(c->xdr_cfg));

	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		c->xdr_lastship[i].node = 0;

		for (int j = 0; j < DC_MAX_NUM; j++) {
			c->xdr_lastship[i].time[j] = 0;
		}

		c->xdr_clmap[i] = 0;
	}

	for (int j = 0; j < DC_MAX_NUM; j++) {
		c->xdr_self_lastshiptime[j] = 0;
	}

	// Mod-lua defaults.
	c->mod_lua.server_mode      = true;
	c->mod_lua.cache_enabled    = true;
	strcpy(c->mod_lua.system_path, "/opt/aerospike/sys/udf/lua");
	strcpy(c->mod_lua.user_path, "/opt/aerospike/usr/udf/lua");

	// Cluster Topology: With the new Rack Aware feature, we allow the customers
	// to define their nodes and groups with THEIR names, and thus overrule the
	// autogenerate node ID based on MAC address and port (i.e. Hardware
	// config). The DEFAULT value for "cluster mode" will be the old style -->
	// No Topology. We have to see a CLUSTER definition of "static" or "dynamic"
	// in the config file in order to change to the new mode.
	c->cluster_mode = CL_MODE_NO_TOPOLOGY;

	// TODO - security set default config API?
	c->sec_cfg.privilege_refresh_period = 60 * 5; // refresh socket privileges every 5 minutes
	c->sec_cfg.syslog_local = AS_SYSLOG_NONE;

	// TODO - not sure why these are in configuration - just to be global?
	cf_atomic_int_set(&c->migrate_num_incoming, 0);
	cf_atomic_int_set(&c->migrate_num_incoming_accepted, 0);
	cf_atomic_int_set(&c->migrate_num_incoming_refused, 0);
	c->start_ms = cf_getms();
	c->record_locks = olock_create(16 * 1024, true); // TODO - configurable number of locks?

	c->namespaces = 0;
}


//==========================================================
// All configuration items must have a switch() case
// identifier somewhere in this enum. The order is not
// important, other than for organizational sanity.
//

typedef enum {
	// Generic:
	// Token not found:
	CASE_NOT_FOUND,
	// End of parsing context:
	CASE_CONTEXT_END,

	// Top-level options:
	// In canonical configuration file order:
	CASE_SERVICE_BEGIN,
	CASE_LOG_BEGIN,
	CASE_NETWORK_BEGIN,
	CASE_NAMESPACE_BEGIN,
	// For XDR only:
	CASE_XDR_BEGIN,
	// Recent (non-2.x) functionality:
	CASE_MOD_LUA_BEGIN,
	CASE_CLUSTER_BEGIN,
	CASE_SECURITY_BEGIN,

	// Service options:
	// Normally visible, in canonical configuration file order:
	CASE_SERVICE_USER,
	CASE_SERVICE_GROUP,
	CASE_SERVICE_PAXOS_SINGLE_REPLICA_LIMIT,
	CASE_SERVICE_PIDFILE,
	CASE_SERVICE_SERVICE_THREADS,
	CASE_SERVICE_TRANSACTION_QUEUES,
	CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE,
	CASE_SERVICE_CLIENT_FD_MAX, // renamed
	CASE_SERVICE_PROTO_FD_MAX,
	// Normally hidden:
	CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS,
	CASE_SERVICE_AUTO_DUN,
	CASE_SERVICE_AUTO_UNDUN,
	CASE_SERVICE_BATCH_THREADS,
	CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE,
	CASE_SERVICE_BATCH_MAX_REQUESTS,
	CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS,
	CASE_SERVICE_BATCH_PRIORITY,
	CASE_SERVICE_BATCH_INDEX_THREADS,
	CASE_SERVICE_FABRIC_WORKERS,
	CASE_SERVICE_FB_HEALTH_BAD_PCT,
	CASE_SERVICE_FB_HEALTH_GOOD_PCT,
	CASE_SERVICE_FB_HEALTH_MSG_PER_BURST,
	CASE_SERVICE_FB_HEALTH_MSG_TIMEOUT,
	CASE_SERVICE_GENERATION_DISABLE,
	CASE_SERVICE_HIST_TRACK_BACK,
	CASE_SERVICE_HIST_TRACK_SLICE,
	CASE_SERVICE_HIST_TRACK_THRESHOLDS,
	CASE_SERVICE_INFO_THREADS,
	CASE_SERVICE_LDT_BENCHMARKS,
	CASE_SERVICE_MICROBENCHMARKS,
	CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING,
	CASE_SERVICE_MIGRATE_READ_PRIORITY,
	CASE_SERVICE_MIGRATE_READ_SLEEP,
	CASE_SERVICE_MIGRATE_RX_LIFETIME_MS,
	CASE_SERVICE_MIGRATE_THREADS,
	CASE_SERVICE_MIGRATE_XMIT_HWM,
	CASE_SERVICE_MIGRATE_XMIT_LWM,
	CASE_SERVICE_MIGRATE_PRIORITY, // renamed
	CASE_SERVICE_MIGRATE_XMIT_PRIORITY,
	CASE_SERVICE_MIGRATE_XMIT_SLEEP,
	CASE_SERVICE_NSUP_DELETE_SLEEP,
	CASE_SERVICE_NSUP_PERIOD,
	CASE_SERVICE_NSUP_STARTUP_EVICT,
	CASE_SERVICE_PAXOS_MAX_CLUSTER_SIZE,
	CASE_SERVICE_PAXOS_PROTOCOL,
	CASE_SERVICE_PAXOS_RECOVERY_POLICY,
	CASE_SERVICE_PAXOS_RETRANSMIT_PERIOD,
	CASE_SERVICE_PROTO_FD_IDLE_MS,
	CASE_SERVICE_QUERY_BATCH_SIZE,
	CASE_SERVICE_QUERY_IN_TRANSACTION_THREAD,
	CASE_SERVICE_QUERY_PRE_RESERVE_QNODES,
	CASE_SERVICE_QUERY_PRIORITY,
	CASE_SERVICE_QUERY_PRIORITY_SLEEP_US,
	CASE_SERVICE_QUERY_THRESHOLD,
	CASE_SERVICE_QUERY_UNTRACKED_TIME_MS,
	CASE_SERVICE_REPLICATION_FIRE_AND_FORGET,
	CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION,
	CASE_SERVICE_RUN_AS_DAEMON,
	CASE_SERVICE_SCAN_MAX_ACTIVE,
	CASE_SERVICE_SCAN_MAX_DONE,
	CASE_SERVICE_SCAN_MAX_UDF_TRANSACTIONS,
	CASE_SERVICE_SCAN_THREADS,
	CASE_SERVICE_SINDEX_DATA_MAX_MEMORY,
	CASE_SERVICE_SNUB_NODES,
	CASE_SERVICE_STORAGE_BENCHMARKS,
	CASE_SERVICE_TICKER_INTERVAL,
	CASE_SERVICE_TRANSACTION_DUPLICATE_THREADS,
	CASE_SERVICE_TRANSACTION_MAX_MS,
	CASE_SERVICE_TRANSACTION_PENDING_LIMIT,
	CASE_SERVICE_TRANSACTION_REPEATABLE_READ,
	CASE_SERVICE_TRANSACTION_RETRY_MS,
	CASE_SERVICE_UDF_RUNTIME_MAX_GMEMORY,
	CASE_SERVICE_UDF_RUNTIME_MAX_MEMORY,
	CASE_SERVICE_USE_QUEUE_PER_DEVICE,
	CASE_SERVICE_WORK_DIRECTORY,
	CASE_SERVICE_WRITE_DUPLICATE_RESOLUTION_DISABLE,
	// For special debugging or bug-related repair:
	CASE_SERVICE_ASMALLOC_ENABLED,
	CASE_SERVICE_DUMP_MESSAGE_ABOVE_SIZE,
	CASE_SERVICE_FABRIC_DUMP_MSGS,
	CASE_SERVICE_MAX_MSGS_PER_TYPE,
	CASE_SERVICE_MEMORY_ACCOUNTING,
	CASE_SERVICE_PROLE_EXTRA_TTL,
	// Deprecated:
	CASE_SERVICE_BATCH_RETRANSMIT,
	CASE_SERVICE_CLIB_LIBRARY,
	CASE_SERVICE_DEFRAG_QUEUE_ESCAPE,
	CASE_SERVICE_DEFRAG_QUEUE_HWM,
	CASE_SERVICE_DEFRAG_QUEUE_LWM,
	CASE_SERVICE_DEFRAG_QUEUE_PRIORITY,
	CASE_SERVICE_NSUP_AUTO_HWM,
	CASE_SERVICE_NSUP_AUTO_HWM_PCT,
	CASE_SERVICE_NSUP_MAX_DELETES,
	CASE_SERVICE_NSUP_QUEUE_HWM,
	CASE_SERVICE_NSUP_QUEUE_LWM,
	CASE_SERVICE_NSUP_QUEUE_ESCAPE,
	CASE_SERVICE_NSUP_REDUCE_PRIORITY,
	CASE_SERVICE_NSUP_REDUCE_SLEEP,
	CASE_SERVICE_NSUP_THREADS,
	CASE_SERVICE_SCAN_MEMORY,
	CASE_SERVICE_SCAN_PRIORITY,
	CASE_SERVICE_SCAN_RETRANSMIT,
	CASE_SERVICE_SCHEDULER_PRIORITY,
	CASE_SERVICE_SCHEDULER_TYPE,
	CASE_SERVICE_TRIAL_ACCOUNT_KEY,

	// Service paxos protocol options (value tokens):
	CASE_SERVICE_PAXOS_PROTOCOL_V1,
	CASE_SERVICE_PAXOS_PROTOCOL_V2,
	CASE_SERVICE_PAXOS_PROTOCOL_V3,
	CASE_SERVICE_PAXOS_PROTOCOL_V4,

	// Service paxos recovery policy options (value tokens):
	CASE_SERVICE_PAXOS_RECOVERY_AUTO_DUN_ALL,
	CASE_SERVICE_PAXOS_RECOVERY_AUTO_DUN_MASTER,
	CASE_SERVICE_PAXOS_RECOVERY_MANUAL,

	// Logging options:
	// Normally visible:
	CASE_LOG_FILE_BEGIN,
	// Normally hidden:
	CASE_LOG_CONSOLE_BEGIN,

	// Logging file options:
	// Normally visible:
	CASE_LOG_FILE_CONTEXT,
	// Not supported:
	CASE_LOG_FILE_SPECIFIC,

	// Logging console options:
	// Normally visible:
	CASE_LOG_CONSOLE_CONTEXT,
	// Not supported:
	CASE_LOG_CONSOLE_SPECIFIC,

	// Network options:
	// In canonical configuration file order:
	CASE_NETWORK_SERVICE_BEGIN,
	CASE_NETWORK_HEARTBEAT_BEGIN,
	CASE_NETWORK_FABRIC_BEGIN,
	CASE_NETWORK_INFO_BEGIN,

	// Network service options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_SERVICE_ADDRESS,
	CASE_NETWORK_SERVICE_PORT,
	// Normally hidden:
	CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS, // renamed
	CASE_NETWORK_SERVICE_ACCESS_ADDRESS,
	CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME,
	CASE_NETWORK_SERVICE_REUSE_ADDRESS,

	// Network heartbeat options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_HEARTBEAT_MODE,
	CASE_NETWORK_HEARTBEAT_ADDRESS,
	CASE_NETWORK_HEARTBEAT_PORT,
	CASE_NETWORK_HEARTBEAT_MESH_ADDRESS,
	CASE_NETWORK_HEARTBEAT_MESH_PORT,
	CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT,
	CASE_NETWORK_HEARTBEAT_INTERVAL,
	CASE_NETWORK_HEARTBEAT_TIMEOUT,
	// Normally hidden:
	CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS,
	CASE_NETWORK_HEARTBEAT_MCAST_TTL,
	CASE_NETWORK_HEARTBEAT_MESH_RW_RETRY_TIMEOUT,
	CASE_NETWORK_HEARTBEAT_PROTOCOL,

	// Network heartbeat mode options (value tokens):
	CASE_NETWORK_HEARTBEAT_MODE_MESH,
	CASE_NETWORK_HEARTBEAT_MODE_MULTICAST,

	// Network heartbeat protocol options (value tokens):
	CASE_NETWORK_HEARTBEAT_PROTOCOL_RESET,
	CASE_NETWORK_HEARTBEAT_PROTOCOL_V1,
	CASE_NETWORK_HEARTBEAT_PROTOCOL_V2,

	// Network fabric options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_FABRIC_ADDRESS,
	CASE_NETWORK_FABRIC_PORT,
	// Normally hidden, in canonical configuration file order:
	CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED,
	CASE_NETWORK_FABRIC_KEEPALIVE_TIME,
	CASE_NETWORK_FABRIC_KEEPALIVE_INTVL,
	CASE_NETWORK_FABRIC_KEEPALIVE_PROBES,

	// Network info options:
	// Normally visible, in canonical configuration file order:
	CASE_NETWORK_INFO_ADDRESS,
	CASE_NETWORK_INFO_PORT,
	// Normally hidden:
	CASE_NETWORK_INFO_ENABLE_FASTPATH,

	// Namespace options:
	// Normally visible, in canonical configuration file order:
	CASE_NAMESPACE_REPLICATION_FACTOR,
	CASE_NAMESPACE_LIMIT_SIZE, // renamed
	CASE_NAMESPACE_MEMORY_SIZE,
	CASE_NAMESPACE_DEFAULT_TTL,
	CASE_NAMESPACE_STORAGE_ENGINE_BEGIN,
	// For XDR only:
	CASE_NAMESPACE_ENABLE_XDR,
	CASE_NAMESPACE_SETS_ENABLE_XDR,
	CASE_NAMESPACE_XDR_REMOTE_DATACENTER,
	CASE_NAMESPACE_FORWARD_XDR_WRITES,
	CASE_NAMESPACE_ALLOW_NONXDR_WRITES,
	CASE_NAMESPACE_ALLOW_XDR_WRITES,
	// Normally hidden:
	CASE_NAMESPACE_COLD_START_EVICT_TTL,
	CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY,
	CASE_NAMESPACE_DATA_IN_INDEX,
	CASE_NAMESPACE_DISALLOW_NULL_SETNAME,
	CASE_NAMESPACE_EVICT_TENTHS_PCT,
	CASE_NAMESPACE_HIGH_WATER_DISK_PCT,
	CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT,
	CASE_NAMESPACE_LDT_ENABLED,
	CASE_NAMESPACE_LDT_GC_RATE,
	CASE_NAMESPACE_LDT_PAGE_SIZE,
	CASE_NAMESPACE_MAX_TTL,
	CASE_NAMESPACE_OBJ_SIZE_HIST_MAX,
	CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE,
	CASE_NAMESPACE_SET_BEGIN,
	CASE_NAMESPACE_SI_BEGIN,
	CASE_NAMESPACE_SINDEX_BEGIN,
	CASE_NAMESPACE_SINGLE_BIN,
	CASE_NAMESPACE_STOP_WRITES_PCT,
	CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE,
	// Deprecated:
	CASE_NAMESPACE_ALLOW_VERSIONS,
	CASE_NAMESPACE_DEMO_READ_MULTIPLIER,
	CASE_NAMESPACE_DEMO_WRITE_MULTIPLIER,
	CASE_NAMESPACE_HIGH_WATER_PCT,
	CASE_NAMESPACE_LOW_WATER_PCT,

	// Namespace conflict-resolution-policy options (value tokens):
	CASE_NAMESPACE_CONFLICT_RESOLUTION_GENERATION,
	CASE_NAMESPACE_CONFLICT_RESOLUTION_TTL,

	// Namespace read consistency level options:
	CASE_NAMESPACE_READ_CONSISTENCY_ALL,
	CASE_NAMESPACE_READ_CONSISTENCY_OFF,
	CASE_NAMESPACE_READ_CONSISTENCY_ONE,

	// Namespace write commit level options:
	CASE_NAMESPACE_WRITE_COMMIT_ALL,
	CASE_NAMESPACE_WRITE_COMMIT_MASTER,
	CASE_NAMESPACE_WRITE_COMMIT_OFF,

	// Namespace storage-engine options (value tokens):
	CASE_NAMESPACE_STORAGE_MEMORY,
	CASE_NAMESPACE_STORAGE_SSD,
	CASE_NAMESPACE_STORAGE_DEVICE,
	CASE_NAMESPACE_STORAGE_KV,

	// Namespace storage-engine device options:
	// Normally visible, in canonical configuration file order:
	CASE_NAMESPACE_STORAGE_DEVICE_DEVICE,
	CASE_NAMESPACE_STORAGE_DEVICE_FILE,
	CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_SCHEDULER_MODE,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE,
	CASE_NAMESPACE_STORAGE_DEVICE_MEMORY_ALL, // renamed
	CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY,
	// Normally hidden:
	CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM,
	CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT,
	CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_OSYNC,
	CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS,
	CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC,
	CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE,
	CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT,
	CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE,
	CASE_NAMESPACE_STORAGE_DEVICE_SIGNATURE,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_THREADS,
	// Deprecated:
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_MAX_BLOCKS,
	CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_PERIOD,
	CASE_NAMESPACE_STORAGE_DEVICE_LOAD_AT_STARTUP,
	CASE_NAMESPACE_STORAGE_DEVICE_PERSIST,
	CASE_NAMESPACE_STORAGE_DEVICE_READONLY,
	CASE_NAMESPACE_STORAGE_DEVICE_WRITE_SMOOTHING_PERIOD,

	// Namespace storage-engine kv options:
	CASE_NAMESPACE_STORAGE_KV_DEVICE,
	CASE_NAMESPACE_STORAGE_KV_FILESIZE,
	CASE_NAMESPACE_STORAGE_KV_READ_BLOCK_SIZE,
	CASE_NAMESPACE_STORAGE_KV_WRITE_BLOCK_SIZE,
	CASE_NAMESPACE_STORAGE_KV_NUM_WRITE_BLOCKS,
	CASE_NAMESPACE_STORAGE_KV_COND_WRITE,

	// Namespace set options:
	CASE_NAMESPACE_SET_DISABLE_EVICTION,
	CASE_NAMESPACE_SET_ENABLE_XDR,
	// Deprecated:
	CASE_NAMESPACE_SET_EVICT_HWM_COUNT,
	CASE_NAMESPACE_SET_EVICT_HWM_PCT,
	CASE_NAMESPACE_SET_STOP_WRITE_COUNT,
	CASE_NAMESPACE_SET_STOP_WRITE_PCT,

	// Namespace set set-enable-xdr options (value tokens):
	CASE_NAMESPACE_SET_ENABLE_XDR_USE_DEFAULT,
	CASE_NAMESPACE_SET_ENABLE_XDR_FALSE,
	CASE_NAMESPACE_SET_ENABLE_XDR_TRUE,

	// Namespace secondary-index options:
	CASE_NAMESPACE_SI_GC_PERIOD,
	CASE_NAMESPACE_SI_GC_MAX_UNITS,
	CASE_NAMESPACE_SI_DATA_MAX_MEMORY,
	CASE_NAMESPACE_SI_TRACING,
	CASE_NAMESPACE_SI_HISTOGRAM,
	CASE_NAMESPACE_SI_IGNORE_NOT_SYNC,

	// Namespace sindex options:
	CASE_NAMESPACE_SINDEX_DATA_MAX_MEMORY,
	CASE_NAMESPACE_SINDEX_NUM_PARTITIONS,

	// Mod-lua options:
	CASE_MOD_LUA_CACHE_ENABLED,
	CASE_MOD_LUA_SYSTEM_PATH,
	CASE_MOD_LUA_USER_PATH,

	// Cluster options:
	CASE_CLUSTER_SELF_NODE_ID,
	CASE_CLUSTER_SELF_GROUP_ID,
	CASE_CLUSTER_GROUP_BEGIN,
	CASE_CLUSTER_MODE,

	// Cluster group options:
	CASE_CLUSTER_GROUP_NODE_ID,
	CASE_CLUSTER_GROUP_GROUP_ATTR,

	// Security options:
	CASE_SECURITY_ENABLE_SECURITY,
	CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD,
	CASE_SECURITY_LOG_BEGIN,
	CASE_SECURITY_SYSLOG_BEGIN,

	// Security (Aerospike) log options:
	CASE_SECURITY_LOG_REPORT_AUTHENTICATION,
	CASE_SECURITY_LOG_REPORT_DATA_OP,
	CASE_SECURITY_LOG_REPORT_SYS_ADMIN,
	CASE_SECURITY_LOG_REPORT_USER_ADMIN,
	CASE_SECURITY_LOG_REPORT_VIOLATION,

	// Security syslog options:
	CASE_SECURITY_SYSLOG_LOCAL,
	CASE_SECURITY_SYSLOG_REPORT_AUTHENTICATION,
	CASE_SECURITY_SYSLOG_REPORT_DATA_OP,
	CASE_SECURITY_SYSLOG_REPORT_SYS_ADMIN,
	CASE_SECURITY_SYSLOG_REPORT_USER_ADMIN,
	CASE_SECURITY_SYSLOG_REPORT_VIOLATION

} cfg_case_id;


//==========================================================
// All configuration items must appear below as a cfg_opt
// struct in the appropriate array. Order within an array is
// not important, other than for organizational sanity.
//

typedef struct cfg_opt_s {
	const char*	tok;
	cfg_case_id	case_id;
} cfg_opt;

const cfg_opt GLOBAL_OPTS[] = {
		{ "service",						CASE_SERVICE_BEGIN },
		{ "logging",						CASE_LOG_BEGIN },
		{ "network",						CASE_NETWORK_BEGIN },
		{ "namespace",						CASE_NAMESPACE_BEGIN },
		{ "xdr",							CASE_XDR_BEGIN },
		{ "mod-lua",						CASE_MOD_LUA_BEGIN },
		{ "cluster",						CASE_CLUSTER_BEGIN },
		{ "security",						CASE_SECURITY_BEGIN }
};

const cfg_opt SERVICE_OPTS[] = {
		{ "user",							CASE_SERVICE_USER },
		{ "group",							CASE_SERVICE_GROUP },
		{ "paxos-single-replica-limit",		CASE_SERVICE_PAXOS_SINGLE_REPLICA_LIMIT },
		{ "pidfile",						CASE_SERVICE_PIDFILE },
		{ "service-threads",				CASE_SERVICE_SERVICE_THREADS },
		{ "transaction-queues",				CASE_SERVICE_TRANSACTION_QUEUES },
		{ "transaction-threads-per-queue",	CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE },
		{ "client-fd-max",					CASE_SERVICE_CLIENT_FD_MAX },
		{ "proto-fd-max",					CASE_SERVICE_PROTO_FD_MAX },
		{ "allow-inline-transactions",		CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS },
		{ "auto-dun",						CASE_SERVICE_AUTO_DUN },
		{ "auto-undun",						CASE_SERVICE_AUTO_UNDUN },
		{ "batch-threads",					CASE_SERVICE_BATCH_THREADS },
		{ "batch-max-buffers-per-queue",	CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE },
		{ "batch-max-requests",				CASE_SERVICE_BATCH_MAX_REQUESTS },
		{ "batch-max-unused-buffers",		CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS },
		{ "batch-priority",					CASE_SERVICE_BATCH_PRIORITY },
		{ "batch-index-threads",			CASE_SERVICE_BATCH_INDEX_THREADS },
		{ "fabric-workers",					CASE_SERVICE_FABRIC_WORKERS },
		{ "fb-health-bad-pct",				CASE_SERVICE_FB_HEALTH_BAD_PCT },
		{ "fb-health-good-pct",				CASE_SERVICE_FB_HEALTH_GOOD_PCT },
		{ "fb-health-msg-per-burst",		CASE_SERVICE_FB_HEALTH_MSG_PER_BURST },
		{ "fb-health-msg-timeout",			CASE_SERVICE_FB_HEALTH_MSG_TIMEOUT },
		{ "generation-disable",				CASE_SERVICE_GENERATION_DISABLE },
		{ "hist-track-back",				CASE_SERVICE_HIST_TRACK_BACK },
		{ "hist-track-slice",				CASE_SERVICE_HIST_TRACK_SLICE },
		{ "hist-track-thresholds",			CASE_SERVICE_HIST_TRACK_THRESHOLDS },
		{ "info-threads",					CASE_SERVICE_INFO_THREADS },
		{ "ldt-benchmarks",					CASE_SERVICE_LDT_BENCHMARKS },
		{ "microbenchmarks",				CASE_SERVICE_MICROBENCHMARKS },
		{ "migrate-max-num-incoming",		CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING },
		{ "migrate-read-priority",			CASE_SERVICE_MIGRATE_READ_PRIORITY },
		{ "migrate-read-sleep",				CASE_SERVICE_MIGRATE_READ_SLEEP },
		{ "migrate-rx-lifetime-ms",			CASE_SERVICE_MIGRATE_RX_LIFETIME_MS },
		{ "migrate-threads",				CASE_SERVICE_MIGRATE_THREADS },
		{ "migrate-xmit-hwm",				CASE_SERVICE_MIGRATE_XMIT_HWM },
		{ "migrate-xmit-lwm",				CASE_SERVICE_MIGRATE_XMIT_LWM },
		{ "migrate-priority",				CASE_SERVICE_MIGRATE_PRIORITY },
		{ "migrate-xmit-priority",			CASE_SERVICE_MIGRATE_XMIT_PRIORITY },
		{ "migrate-xmit-sleep",				CASE_SERVICE_MIGRATE_XMIT_SLEEP },
		{ "nsup-delete-sleep",				CASE_SERVICE_NSUP_DELETE_SLEEP },
		{ "nsup-period",					CASE_SERVICE_NSUP_PERIOD },
		{ "nsup-startup-evict",				CASE_SERVICE_NSUP_STARTUP_EVICT },
		{ "paxos-max-cluster-size",			CASE_SERVICE_PAXOS_MAX_CLUSTER_SIZE },
		{ "paxos-protocol",					CASE_SERVICE_PAXOS_PROTOCOL },
		{ "paxos-recovery-policy",			CASE_SERVICE_PAXOS_RECOVERY_POLICY },
		{ "paxos-retransmit-period",		CASE_SERVICE_PAXOS_RETRANSMIT_PERIOD },
		{ "proto-fd-idle-ms",				CASE_SERVICE_PROTO_FD_IDLE_MS },
		{ "query-batch-size",				CASE_SERVICE_QUERY_BATCH_SIZE },
		{ "query-in-transaction-thread",	CASE_SERVICE_QUERY_IN_TRANSACTION_THREAD },
		{ "query-pre-reserve-qnodes", 		CASE_SERVICE_QUERY_PRE_RESERVE_QNODES },
		{ "query-priority", 				CASE_SERVICE_QUERY_PRIORITY },
		{ "query-priority-sleep-us", 		CASE_SERVICE_QUERY_PRIORITY_SLEEP_US },
		{ "query-threshold", 				CASE_SERVICE_QUERY_THRESHOLD },
		{ "query-untracked-time-ms",		CASE_SERVICE_QUERY_UNTRACKED_TIME_MS },
		{ "replication-fire-and-forget",	CASE_SERVICE_REPLICATION_FIRE_AND_FORGET },
		{ "respond-client-on-master-completion", CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION },
		{ "run-as-daemon",					CASE_SERVICE_RUN_AS_DAEMON },
		{ "scan-max-active",				CASE_SERVICE_SCAN_MAX_ACTIVE },
		{ "scan-max-done",					CASE_SERVICE_SCAN_MAX_DONE },
		{ "scan-max-udf-transactions",		CASE_SERVICE_SCAN_MAX_UDF_TRANSACTIONS },
		{ "scan-threads",					CASE_SERVICE_SCAN_THREADS },
		{ "sindex-data-max-memory",			CASE_SERVICE_SINDEX_DATA_MAX_MEMORY },
		{ "snub-nodes",						CASE_SERVICE_SNUB_NODES },
		{ "storage-benchmarks",				CASE_SERVICE_STORAGE_BENCHMARKS },
		{ "ticker-interval",				CASE_SERVICE_TICKER_INTERVAL },
		{ "transaction-duplicate-threads",	CASE_SERVICE_TRANSACTION_DUPLICATE_THREADS },
		{ "transaction-max-ms",				CASE_SERVICE_TRANSACTION_MAX_MS },
		{ "transaction-pending-limit",		CASE_SERVICE_TRANSACTION_PENDING_LIMIT },
		{ "transaction-repeatable-read",	CASE_SERVICE_TRANSACTION_REPEATABLE_READ },
		{ "transaction-retry-ms",			CASE_SERVICE_TRANSACTION_RETRY_MS },
		{ "udf-runtime-max-gmemory",		CASE_SERVICE_UDF_RUNTIME_MAX_GMEMORY },
		{ "udf-runtime-max-memory",			CASE_SERVICE_UDF_RUNTIME_MAX_MEMORY },
		{ "use-queue-per-device",			CASE_SERVICE_USE_QUEUE_PER_DEVICE },
		{ "work-directory",					CASE_SERVICE_WORK_DIRECTORY },
		{ "write-duplicate-resolution-disable", CASE_SERVICE_WRITE_DUPLICATE_RESOLUTION_DISABLE },
		{ "asmalloc-enabled",				CASE_SERVICE_ASMALLOC_ENABLED },
		{ "dump-message-above-size",		CASE_SERVICE_DUMP_MESSAGE_ABOVE_SIZE },
		{ "fabric-dump-msgs",				CASE_SERVICE_FABRIC_DUMP_MSGS },
		{ "max-msgs-per-type",				CASE_SERVICE_MAX_MSGS_PER_TYPE },
		{ "memory-accounting",				CASE_SERVICE_MEMORY_ACCOUNTING },
		{ "prole-extra-ttl",				CASE_SERVICE_PROLE_EXTRA_TTL },
		{ "batch-retransmit",				CASE_SERVICE_BATCH_RETRANSMIT },
		{ "clib-library",					CASE_SERVICE_CLIB_LIBRARY },
		{ "defrag-queue-escape",			CASE_SERVICE_DEFRAG_QUEUE_ESCAPE },
		{ "defrag-queue-hwm",				CASE_SERVICE_DEFRAG_QUEUE_HWM },
		{ "defrag-queue-lwm",				CASE_SERVICE_DEFRAG_QUEUE_LWM },
		{ "defrag-queue-priority",			CASE_SERVICE_DEFRAG_QUEUE_PRIORITY },
		{ "nsup-auto-hwm",					CASE_SERVICE_NSUP_AUTO_HWM },
		{ "nsup-auto-hwm-pct",				CASE_SERVICE_NSUP_AUTO_HWM_PCT },
		{ "nsup-max-deletes",				CASE_SERVICE_NSUP_MAX_DELETES },
		{ "nsup-queue-escape",				CASE_SERVICE_NSUP_QUEUE_ESCAPE },
		{ "nsup-queue-hwm",					CASE_SERVICE_NSUP_QUEUE_HWM },
		{ "nsup-queue-lwm",					CASE_SERVICE_NSUP_QUEUE_LWM },
		{ "nsup-reduce-priority",			CASE_SERVICE_NSUP_REDUCE_PRIORITY },
		{ "nsup-reduce-sleep",				CASE_SERVICE_NSUP_REDUCE_SLEEP },
		{ "nsup-threads",					CASE_SERVICE_NSUP_THREADS },
		{ "scan-memory",					CASE_SERVICE_SCAN_MEMORY },
		{ "scan-priority",					CASE_SERVICE_SCAN_PRIORITY },
		{ "scan-retransmit",				CASE_SERVICE_SCAN_RETRANSMIT },
		{ "scheduler-priority",				CASE_SERVICE_SCHEDULER_PRIORITY },
		{ "scheduler-type",					CASE_SERVICE_SCHEDULER_TYPE },
		{ "trial-account-key",				CASE_SERVICE_TRIAL_ACCOUNT_KEY },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SERVICE_PAXOS_PROTOCOL_OPTS[] = {
		{ "v1",								CASE_SERVICE_PAXOS_PROTOCOL_V1 },
		{ "v2",								CASE_SERVICE_PAXOS_PROTOCOL_V2 },
		{ "v3",								CASE_SERVICE_PAXOS_PROTOCOL_V3 },
		{ "v4",								CASE_SERVICE_PAXOS_PROTOCOL_V4 }
};

const cfg_opt SERVICE_PAXOS_RECOVERY_OPTS[] = {
		{ "auto-dun-all",					CASE_SERVICE_PAXOS_RECOVERY_AUTO_DUN_ALL },
		{ "auto-dun-master",				CASE_SERVICE_PAXOS_RECOVERY_AUTO_DUN_MASTER },
		{ "manual",							CASE_SERVICE_PAXOS_RECOVERY_MANUAL }
};

const cfg_opt LOGGING_OPTS[] = {
		{ "file",							CASE_LOG_FILE_BEGIN },
		{ "console",						CASE_LOG_CONSOLE_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt LOGGING_FILE_OPTS[] = {
		{ "context",						CASE_LOG_FILE_CONTEXT },
		{ "specific",						CASE_LOG_FILE_SPECIFIC },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt LOGGING_CONSOLE_OPTS[] = {
		{ "context",						CASE_LOG_CONSOLE_CONTEXT },
		{ "specific",						CASE_LOG_CONSOLE_SPECIFIC },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_OPTS[] = {
		{ "service",						CASE_NETWORK_SERVICE_BEGIN },
		{ "heartbeat",						CASE_NETWORK_HEARTBEAT_BEGIN },
		{ "fabric",							CASE_NETWORK_FABRIC_BEGIN },
		{ "info",							CASE_NETWORK_INFO_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_SERVICE_OPTS[] = {
		{ "address",						CASE_NETWORK_SERVICE_ADDRESS },
		{ "port",							CASE_NETWORK_SERVICE_PORT },
		{ "external-address",				CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS },
		{ "access-address",					CASE_NETWORK_SERVICE_ACCESS_ADDRESS },
		{ "network-interface-name",			CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME },
		{ "reuse-address",					CASE_NETWORK_SERVICE_REUSE_ADDRESS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_HEARTBEAT_OPTS[] = {
		{ "mode",							CASE_NETWORK_HEARTBEAT_MODE },
		{ "address",						CASE_NETWORK_HEARTBEAT_ADDRESS },
		{ "port",							CASE_NETWORK_HEARTBEAT_PORT },
		{ "mesh-address",					CASE_NETWORK_HEARTBEAT_MESH_ADDRESS },
		{ "mesh-port",						CASE_NETWORK_HEARTBEAT_MESH_PORT },
		{ "mesh-seed-address-port",			CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT },
		{ "interval",						CASE_NETWORK_HEARTBEAT_INTERVAL },
		{ "timeout",						CASE_NETWORK_HEARTBEAT_TIMEOUT },
		{ "interface-address",				CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS },
		{ "mcast-ttl",						CASE_NETWORK_HEARTBEAT_MCAST_TTL },
		{ "mesh-rw-retry-timeout",			CASE_NETWORK_HEARTBEAT_MESH_RW_RETRY_TIMEOUT },
		{ "protocol",						CASE_NETWORK_HEARTBEAT_PROTOCOL },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_HEARTBEAT_MODE_OPTS[] = {
		{ "mesh",							CASE_NETWORK_HEARTBEAT_MODE_MESH },
		{ "multicast",						CASE_NETWORK_HEARTBEAT_MODE_MULTICAST }
};

const cfg_opt NETWORK_HEARTBEAT_PROTOCOL_OPTS[] = {
		{ "reset",							CASE_NETWORK_HEARTBEAT_PROTOCOL_RESET },
		{ "v1",								CASE_NETWORK_HEARTBEAT_PROTOCOL_V1 },
		{ "v2",								CASE_NETWORK_HEARTBEAT_PROTOCOL_V2 }
};

const cfg_opt NETWORK_FABRIC_OPTS[] = {
		{ "address",						CASE_NETWORK_FABRIC_ADDRESS },
		{ "port",							CASE_NETWORK_FABRIC_PORT },
		{ "keepalive-enabled",				CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED },
		{ "keepalive-time",					CASE_NETWORK_FABRIC_KEEPALIVE_TIME },
		{ "keepalive-intvl",				CASE_NETWORK_FABRIC_KEEPALIVE_INTVL },
		{ "keepalive-probes",				CASE_NETWORK_FABRIC_KEEPALIVE_PROBES },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NETWORK_INFO_OPTS[] = {
		{ "address",						CASE_NETWORK_INFO_ADDRESS },
		{ "port",							CASE_NETWORK_INFO_PORT },
		{ "enable-fastpath",				CASE_NETWORK_INFO_ENABLE_FASTPATH },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_OPTS[] = {
		{ "replication-factor",				CASE_NAMESPACE_REPLICATION_FACTOR },
		{ "limit-size",						CASE_NAMESPACE_LIMIT_SIZE },
		{ "memory-size",					CASE_NAMESPACE_MEMORY_SIZE },
		{ "default-ttl",					CASE_NAMESPACE_DEFAULT_TTL },
		{ "storage-engine",					CASE_NAMESPACE_STORAGE_ENGINE_BEGIN },
		{ "enable-xdr",						CASE_NAMESPACE_ENABLE_XDR },
		{ "sets-enable-xdr",				CASE_NAMESPACE_SETS_ENABLE_XDR },
		{ "xdr-remote-datacenter",			CASE_NAMESPACE_XDR_REMOTE_DATACENTER },
		{ "ns-forward-xdr-writes",			CASE_NAMESPACE_FORWARD_XDR_WRITES },
		{ "allow-nonxdr-writes",			CASE_NAMESPACE_ALLOW_NONXDR_WRITES },
		{ "allow-xdr-writes",				CASE_NAMESPACE_ALLOW_XDR_WRITES },
		{ "cold-start-evict-ttl",			CASE_NAMESPACE_COLD_START_EVICT_TTL },
		{ "conflict-resolution-policy",		CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY },
		{ "data-in-index",					CASE_NAMESPACE_DATA_IN_INDEX },
		{ "disallow-null-setname",			CASE_NAMESPACE_DISALLOW_NULL_SETNAME },
		{ "evict-tenths-pct",				CASE_NAMESPACE_EVICT_TENTHS_PCT },
		{ "high-water-disk-pct",			CASE_NAMESPACE_HIGH_WATER_DISK_PCT },
		{ "high-water-memory-pct",			CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT },
		{ "ldt-enabled",					CASE_NAMESPACE_LDT_ENABLED },
		{ "ldt-gc-rate",					CASE_NAMESPACE_LDT_GC_RATE },
		{ "ldt-page-size",					CASE_NAMESPACE_LDT_PAGE_SIZE },
		{ "max-ttl",						CASE_NAMESPACE_MAX_TTL },
		{ "obj-size-hist-max",				CASE_NAMESPACE_OBJ_SIZE_HIST_MAX },
		{ "read-consistency-level-override", CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE },
		{ "set",							CASE_NAMESPACE_SET_BEGIN },
		{ "si",								CASE_NAMESPACE_SI_BEGIN },
		{ "sindex",							CASE_NAMESPACE_SINDEX_BEGIN },
		{ "single-bin",						CASE_NAMESPACE_SINGLE_BIN },
		{ "stop-writes-pct",				CASE_NAMESPACE_STOP_WRITES_PCT },
		{ "write-commit-level-override",	CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE },
		{ "allow-versions",					CASE_NAMESPACE_ALLOW_VERSIONS },
		{ "demo-read-multiplier",			CASE_NAMESPACE_DEMO_READ_MULTIPLIER },
		{ "demo-write-multiplier",			CASE_NAMESPACE_DEMO_WRITE_MULTIPLIER },
		{ "high-water-pct",					CASE_NAMESPACE_HIGH_WATER_PCT },
		{ "low-water-pct",					CASE_NAMESPACE_LOW_WATER_PCT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_CONFLICT_RESOLUTION_OPTS[] = {
		{ "generation",						CASE_NAMESPACE_CONFLICT_RESOLUTION_GENERATION },
		{ "ttl",							CASE_NAMESPACE_CONFLICT_RESOLUTION_TTL }
};

const cfg_opt NAMESPACE_READ_CONSISTENCY_OPTS[] = {
		{ "all",							CASE_NAMESPACE_READ_CONSISTENCY_ALL },
		{ "off",							CASE_NAMESPACE_READ_CONSISTENCY_OFF },
		{ "one",							CASE_NAMESPACE_READ_CONSISTENCY_ONE }
};

const cfg_opt NAMESPACE_WRITE_COMMIT_OPTS[] = {
		{ "all",							CASE_NAMESPACE_WRITE_COMMIT_ALL },
		{ "master",							CASE_NAMESPACE_WRITE_COMMIT_MASTER },
		{ "off",							CASE_NAMESPACE_WRITE_COMMIT_OFF }
};

const cfg_opt NAMESPACE_STORAGE_OPTS[] = {
		{ "memory",							CASE_NAMESPACE_STORAGE_MEMORY },
		{ "ssd",							CASE_NAMESPACE_STORAGE_SSD },
		{ "device",							CASE_NAMESPACE_STORAGE_DEVICE },
		{ "kv",								CASE_NAMESPACE_STORAGE_KV }
};

const cfg_opt NAMESPACE_STORAGE_DEVICE_OPTS[] = {
		{ "device",							CASE_NAMESPACE_STORAGE_DEVICE_DEVICE },
		{ "file",							CASE_NAMESPACE_STORAGE_DEVICE_FILE },
		{ "filesize",						CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE },
		{ "scheduler-mode",					CASE_NAMESPACE_STORAGE_DEVICE_SCHEDULER_MODE },
		{ "write-block-size",				CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE },
		{ "memory-all",						CASE_NAMESPACE_STORAGE_DEVICE_MEMORY_ALL },
		{ "data-in-memory",					CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY },
		{ "cold-start-empty",				CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY },
		{ "defrag-lwm-pct",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT },
		{ "defrag-queue-min",				CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN },
		{ "defrag-sleep",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP },
		{ "defrag-startup-minimum",			CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM },
		{ "disable-odirect",				CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT },
		{ "enable-osync",					CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_OSYNC },
		{ "flush-max-ms",					CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS },
		{ "fsync-max-sec",					CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC },
		{ "max-write-cache",				CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE },
		{ "min-avail-pct",					CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT },
		{ "post-write-queue",				CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE },
		{ "signature",						CASE_NAMESPACE_STORAGE_DEVICE_SIGNATURE },
		{ "write-threads",					CASE_NAMESPACE_STORAGE_DEVICE_WRITE_THREADS },
		{ "defrag-max-blocks",				CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_MAX_BLOCKS },
		{ "defrag-period",					CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_PERIOD },
		{ "load-at-startup",				CASE_NAMESPACE_STORAGE_DEVICE_LOAD_AT_STARTUP },
		{ "persist",						CASE_NAMESPACE_STORAGE_DEVICE_PERSIST },
		{ "readonly",						CASE_NAMESPACE_STORAGE_DEVICE_READONLY },
		{ "write-smoothing-period",			CASE_NAMESPACE_STORAGE_DEVICE_WRITE_SMOOTHING_PERIOD },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_STORAGE_KV_OPTS[] = {
		{ "device",							CASE_NAMESPACE_STORAGE_KV_DEVICE },
		{ "filesize",						CASE_NAMESPACE_STORAGE_KV_FILESIZE },
		{ "read-block-size",				CASE_NAMESPACE_STORAGE_KV_READ_BLOCK_SIZE },
		{ "write-block-size",				CASE_NAMESPACE_STORAGE_KV_WRITE_BLOCK_SIZE },
		{ "num-write-blocks",				CASE_NAMESPACE_STORAGE_KV_NUM_WRITE_BLOCKS },
		{ "cond-write",						CASE_NAMESPACE_STORAGE_KV_COND_WRITE },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SET_OPTS[] = {
		{ "set-disable-eviction",			CASE_NAMESPACE_SET_DISABLE_EVICTION },
		{ "set-enable-xdr",					CASE_NAMESPACE_SET_ENABLE_XDR },
		{ "set-evict-hwm-count",			CASE_NAMESPACE_SET_EVICT_HWM_COUNT },
		{ "set-evict-hwm-pct",				CASE_NAMESPACE_SET_EVICT_HWM_PCT },
		{ "set-stop-write-count",			CASE_NAMESPACE_SET_STOP_WRITE_COUNT },
		{ "set-stop-write-pct",				CASE_NAMESPACE_SET_STOP_WRITE_PCT },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SET_ENABLE_XDR_OPTS[] = {
		{ "use-default",					CASE_NAMESPACE_SET_ENABLE_XDR_USE_DEFAULT },
		{ "false",							CASE_NAMESPACE_SET_ENABLE_XDR_FALSE },
		{ "true",							CASE_NAMESPACE_SET_ENABLE_XDR_TRUE }
};

const cfg_opt NAMESPACE_SI_OPTS[] = {
		{ "si-gc-period",					CASE_NAMESPACE_SI_GC_PERIOD },
		{ "si-gc-max-units",				CASE_NAMESPACE_SI_GC_MAX_UNITS },
		{ "si-data-max-memory",				CASE_NAMESPACE_SI_DATA_MAX_MEMORY },
		{ "si-tracing",						CASE_NAMESPACE_SI_TRACING},
		{ "si-histogram",					CASE_NAMESPACE_SI_HISTOGRAM },
		{ "si-ignore-not-sync",				CASE_NAMESPACE_SI_IGNORE_NOT_SYNC },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt NAMESPACE_SINDEX_OPTS[] = {
		{ "data-max-memory",				CASE_NAMESPACE_SINDEX_DATA_MAX_MEMORY },
		{ "num-partitions",					CASE_NAMESPACE_SINDEX_NUM_PARTITIONS },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt MOD_LUA_OPTS[] = {
		{ "cache-enabled",					CASE_MOD_LUA_CACHE_ENABLED },
		{ "system-path",					CASE_MOD_LUA_SYSTEM_PATH },
		{ "user-path",						CASE_MOD_LUA_USER_PATH },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt CLUSTER_OPTS[] = {
		{ "self-node-id",					CASE_CLUSTER_SELF_NODE_ID },
		{ "self-group-id",					CASE_CLUSTER_SELF_GROUP_ID },
		{ "group",							CASE_CLUSTER_GROUP_BEGIN },
		{ "mode",							CASE_CLUSTER_MODE },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt CLUSTER_GROUP_OPTS[] = {
		{ "node-id",						CASE_CLUSTER_GROUP_NODE_ID },
		{ "group-attr",						CASE_CLUSTER_GROUP_GROUP_ATTR },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_OPTS[] = {
		{ "enable-security",				CASE_SECURITY_ENABLE_SECURITY },
		{ "privilege-refresh-period",		CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD },
		{ "log",							CASE_SECURITY_LOG_BEGIN },
		{ "syslog",							CASE_SECURITY_SYSLOG_BEGIN },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_LOG_OPTS[] = {
		{ "report-authentication",			CASE_SECURITY_LOG_REPORT_AUTHENTICATION },
		{ "report-data-op",					CASE_SECURITY_LOG_REPORT_DATA_OP },
		{ "report-sys-admin",				CASE_SECURITY_LOG_REPORT_SYS_ADMIN },
		{ "report-user-admin",				CASE_SECURITY_LOG_REPORT_USER_ADMIN },
		{ "report-violation",				CASE_SECURITY_LOG_REPORT_VIOLATION },
		{ "}",								CASE_CONTEXT_END }
};

const cfg_opt SECURITY_SYSLOG_OPTS[] = {
		{ "local",							CASE_SECURITY_SYSLOG_LOCAL },
		{ "report-authentication",			CASE_SECURITY_SYSLOG_REPORT_AUTHENTICATION },
		{ "report-data-op",					CASE_SECURITY_SYSLOG_REPORT_DATA_OP },
		{ "report-sys-admin",				CASE_SECURITY_SYSLOG_REPORT_SYS_ADMIN },
		{ "report-user-admin",				CASE_SECURITY_SYSLOG_REPORT_USER_ADMIN },
		{ "report-violation",				CASE_SECURITY_SYSLOG_REPORT_VIOLATION },
		{ "}",								CASE_CONTEXT_END }
};

const int NUM_GLOBAL_OPTS							= sizeof(GLOBAL_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_OPTS							= sizeof(SERVICE_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_PAXOS_PROTOCOL_OPTS			= sizeof(SERVICE_PAXOS_PROTOCOL_OPTS) / sizeof(cfg_opt);
const int NUM_SERVICE_PAXOS_RECOVERY_OPTS			= sizeof(SERVICE_PAXOS_RECOVERY_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_OPTS							= sizeof(LOGGING_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_FILE_OPTS						= sizeof(LOGGING_FILE_OPTS) / sizeof(cfg_opt);
const int NUM_LOGGING_CONSOLE_OPTS					= sizeof(LOGGING_CONSOLE_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_OPTS							= sizeof(NETWORK_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_SERVICE_OPTS					= sizeof(NETWORK_SERVICE_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_HEARTBEAT_OPTS				= sizeof(NETWORK_HEARTBEAT_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_HEARTBEAT_MODE_OPTS			= sizeof(NETWORK_HEARTBEAT_MODE_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_HEARTBEAT_PROTOCOL_OPTS		= sizeof(NETWORK_HEARTBEAT_PROTOCOL_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_FABRIC_OPTS					= sizeof(NETWORK_FABRIC_OPTS) / sizeof(cfg_opt);
const int NUM_NETWORK_INFO_OPTS						= sizeof(NETWORK_INFO_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_OPTS						= sizeof(NAMESPACE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_CONFLICT_RESOLUTION_OPTS	= sizeof(NAMESPACE_CONFLICT_RESOLUTION_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_READ_CONSISTENCY_OPTS		= sizeof(NAMESPACE_READ_CONSISTENCY_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_WRITE_COMMIT_OPTS			= sizeof(NAMESPACE_WRITE_COMMIT_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_OPTS				= sizeof(NAMESPACE_STORAGE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_DEVICE_OPTS			= sizeof(NAMESPACE_STORAGE_DEVICE_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_STORAGE_KV_OPTS				= sizeof(NAMESPACE_STORAGE_KV_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SET_OPTS					= sizeof(NAMESPACE_SET_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SET_ENABLE_XDR_OPTS			= sizeof(NAMESPACE_SET_ENABLE_XDR_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SI_OPTS						= sizeof(NAMESPACE_SI_OPTS) / sizeof(cfg_opt);
const int NUM_NAMESPACE_SINDEX_OPTS					= sizeof(NAMESPACE_SINDEX_OPTS) / sizeof(cfg_opt);
const int NUM_MOD_LUA_OPTS							= sizeof(MOD_LUA_OPTS) / sizeof(cfg_opt);
const int NUM_CLUSTER_OPTS							= sizeof(CLUSTER_OPTS) / sizeof(cfg_opt);
const int NUM_CLUSTER_GROUP_OPTS					= sizeof(CLUSTER_GROUP_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_OPTS							= sizeof(SECURITY_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_LOG_OPTS						= sizeof(SECURITY_LOG_OPTS) / sizeof(cfg_opt);
const int NUM_SECURITY_SYSLOG_OPTS					= sizeof(SECURITY_SYSLOG_OPTS) / sizeof(cfg_opt);


//==========================================================
// Configuration value constants not for switch() cases.
//

const char* DEVICE_SCHEDULER_MODES[] = {
		"anticipatory",
		"cfq",				// best for rotational drives
		"deadline",
		"noop"				// best for SSDs
};

const int NUM_DEVICE_SCHEDULER_MODES = sizeof(DEVICE_SCHEDULER_MODES) / sizeof(const char*);


//==========================================================
// Generic parsing utilities.
//

// Don't use these functions. Use the cf_str functions, which have better error
// handling, and support K, M, B/G, etc.
#undef atoi
#define atoi() DO_NOT_USE
#undef atol
#define atol() DO_NOT_USE
#undef atoll
#define atol() DO_NOT_USE

//------------------------------------------------
// Parsing state (context) tracking & switching.
//

typedef enum {
	GLOBAL,
	SERVICE,
	LOGGING, LOGGING_FILE, LOGGING_CONSOLE,
	NETWORK, NETWORK_SERVICE, NETWORK_HEARTBEAT, NETWORK_FABRIC, NETWORK_INFO,
	NAMESPACE, NAMESPACE_STORAGE_DEVICE, NAMESPACE_STORAGE_KV, NAMESPACE_SET, NAMESPACE_SI, NAMESPACE_SINDEX,
	XDR, XDR_DATACENTER,
	MOD_LUA,
	CLUSTER, CLUSTER_GROUP,
	SECURITY, SECURITY_LOG, SECURITY_SYSLOG,
	// Must be last, use for sanity-checking:
	PARSER_STATE_MAX_PLUS_1
} as_config_parser_state;

// For detail logging only - keep in sync with as_config_parser_state.
const char* CFG_PARSER_STATES[] = {
		"GLOBAL",
		"SERVICE",
		"LOGGING", "LOGGING_FILE", "LOGGING_CONSOLE",
		"NETWORK", "NETWORK_SERVICE", "NETWORK_HEARTBEAT", "NETWORK_FABRIC", "NETWORK_INFO",
		"NAMESPACE", "NAMESPACE_STORAGE_DEVICE", "NAMESPACE_STORAGE_KV", "NAMESPACE_SET", "NAMESPACE_SI", "NAMESPACE_SINDEX",
		"XDR", "XDR_DATACENTER",
		"MOD_LUA",
		"CLUSTER", "CLUSTER_GROUP",
		"SECURITY", "SECURITY_LOG", "SECURITY_SYSLOG"
};

typedef struct cfg_parser_state_s {
	as_config_parser_state	current;
	as_config_parser_state	stack[8];
	int						depth;
} cfg_parser_state;

void
cfg_parser_state_init(cfg_parser_state* p_state)
{
	p_state->current = p_state->stack[0] = GLOBAL;
	p_state->depth = 0;
}

void
cfg_begin_context(cfg_parser_state* p_state, as_config_parser_state context)
{
	if (context < 0 || context >= PARSER_STATE_MAX_PLUS_1) {
		cf_crash(AS_CFG, "parsing - unknown context");
	}

	as_config_parser_state prev_context = p_state->stack[p_state->depth];

	if (++p_state->depth >= (int)sizeof(p_state->stack)) {
		cf_crash(AS_CFG, "parsing - context too deep");
	}

	p_state->current = p_state->stack[p_state->depth] = context;

	// To see this log, change NO_SINKS_LIMIT in fault.c:
	cf_detail(AS_CFG, "begin context: %s -> %s", CFG_PARSER_STATES[prev_context], CFG_PARSER_STATES[context]);
}

void
cfg_end_context(cfg_parser_state* p_state)
{
	as_config_parser_state prev_context = p_state->stack[p_state->depth];

	if (--p_state->depth < 0) {
		cf_crash(AS_CFG, "parsing - can't end context depth 0");
	}

	p_state->current = p_state->stack[p_state->depth];

	// To see this log, change NO_SINKS_LIMIT in fault.c:
	cf_detail(AS_CFG, "end context: %s -> %s", CFG_PARSER_STATES[prev_context], CFG_PARSER_STATES[p_state->current]);
}

//------------------------------------------------
// Given a token, return switch() case identifier.
//

cfg_case_id
cfg_find_tok(const char* tok, const cfg_opt opts[], int num_opts)
{
	for (int i = 0; i < num_opts; i++) {
		if (strcmp(tok, opts[i].tok) == 0) {
			return opts[i].case_id;
		}
	}

	return CASE_NOT_FOUND;
}

xdr_cfg_case_id
xdr_cfg_find_tok(const char* tok, const xdr_cfg_opt opts[], int num_opts)
{
	for (int i = 0; i < num_opts; i++) {
		if (strcmp(tok, opts[i].tok) == 0) {
			return opts[i].case_id;
		}
	}

	return XDR_CASE_NOT_FOUND;
}

//------------------------------------------------
// Value parsing and sanity-checking utilities.
//

typedef struct cfg_line_s {
	int		num;
	char*	name_tok;
	char*	val_tok_1;
	char*	val_tok_2;
} cfg_line;

void
cfg_future_name_tok(const cfg_line* p_line)
{
	// To see this log, change NO_SINKS_LIMIT in fault.c:
	cf_info(AS_CFG, "line %d :: %s is not yet supported",
			p_line->num, p_line->name_tok);
}

void
cfg_future_val_tok_1(const cfg_line* p_line)
{
	cf_warning(AS_CFG, "line %d :: %s value '%s' is not yet supported",
			p_line->num, p_line->name_tok, p_line->val_tok_1);
}

void
cfg_renamed_name_tok(const cfg_line* p_line, const char* new_tok)
{
	cf_warning(AS_CFG, "line %d :: %s was renamed - please use '%s'",
			p_line->num, p_line->name_tok, new_tok);
}

void
cfg_renamed_val_tok_1(const cfg_line* p_line, const char* new_tok)
{
	cf_warning(AS_CFG, "line %d :: %s value '%s' was renamed - please use '%s'",
			p_line->num, p_line->name_tok, p_line->val_tok_1, new_tok);
}

void
cfg_deprecated_name_tok(const cfg_line* p_line)
{
	cf_warning(AS_CFG, "line %d :: %s is deprecated - please remove",
			p_line->num, p_line->name_tok);
}

void
cfg_deprecated_val_tok_1(const cfg_line* p_line)
{
	cf_warning(AS_CFG, "line %d :: %s value '%s' is deprecated - please remove",
			p_line->num, p_line->name_tok, p_line->val_tok_1);
}

void
cfg_unknown_name_tok(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: unknown config parameter name '%s'",
			p_line->num, p_line->name_tok);
}

void
cfg_unknown_val_tok_1(const cfg_line* p_line)
{
	cf_crash_nostack(AS_CFG, "line %d :: %s has unknown value '%s'",
			p_line->num, p_line->name_tok, p_line->val_tok_1);
}

void
cfg_not_supported(const cfg_line* p_line, const char *feature)
{
	cf_crash_nostack(AS_CFG, "line %d :: illegal value '%s' for config parameter '%s' - feature %s is not supported",
			p_line->num, p_line->val_tok_1, p_line->name_tok, feature);
}

char*
cfg_strdup_anyval_no_checks(const cfg_line* p_line, const char* val_tok)
{
	char* str = cf_strdup(val_tok);

	if (! str) {
		cf_crash_nostack(AS_CFG, "line %d :: failed alloc for %s: %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return str;
}

char*
cfg_strdup_no_checks(const cfg_line* p_line)
{
	return cfg_strdup_anyval_no_checks(p_line, p_line->val_tok_1);
}

char*
cfg_strdup_val2_no_checks(const cfg_line* p_line)
{
	return cfg_strdup_anyval_no_checks(p_line, p_line->val_tok_2);
}

char*
cfg_strdup_anyval(const cfg_line* p_line, const char* val_tok, bool is_required)
{
	if (val_tok[0] == 0) {
		if (is_required) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must have a value specified",
					p_line->num, p_line->name_tok);
		}

		// Do not duplicate empty strings.
		return NULL;
	}

	return cfg_strdup_anyval_no_checks(p_line, val_tok);
}

char*
cfg_strdup(const cfg_line* p_line, bool is_required)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_1, is_required);
}

char*
cfg_strdup_val2(const cfg_line* p_line, bool is_required)
{
	return cfg_strdup_anyval(p_line, p_line->val_tok_2, is_required);
}

char*
cfg_strdup_one_of(const cfg_line* p_line, const char* toks[], int num_toks)
{
	for (int i = 0; i < num_toks; i++) {
		if (strcmp(p_line->val_tok_1, toks[i]) == 0) {
			return cfg_strdup_no_checks(p_line);
		}
	}

	uint32_t valid_toks_size = (num_toks * 2) + 1;

	for (int i = 0; i < num_toks; i++) {
		valid_toks_size += strlen(toks[i]);
	}

	char valid_toks[valid_toks_size];

	for (int i = 0; i < num_toks; i++) {
		strcat(valid_toks, toks[i]);
		strcat(valid_toks, ", ");
	}

	cf_crash_nostack(AS_CFG, "line %d :: %s must be one of: %snot %s",
			p_line->num, p_line->name_tok, valid_toks, p_line->val_tok_1);

	// Won't get here, but quiet warnings...
	return NULL;
}

void
cfg_strcpy(const cfg_line* p_line, char* p_str, size_t max_size)
{
	// TODO - should we check for empty string?

	if (strlen(p_line->val_tok_1) >= max_size) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be < %lu characters long, not %s",
				p_line->num, p_line->name_tok, max_size, p_line->val_tok_1);
	}

	strcpy(p_str, p_line->val_tok_1);
}

bool
cfg_bool(const cfg_line* p_line)
{
	if (strcasecmp(p_line->val_tok_1, "true") == 0 || strcasecmp(p_line->val_tok_1, "yes") == 0) {
		return true;
	}

	if (strcasecmp(p_line->val_tok_1, "false") == 0 || strcasecmp(p_line->val_tok_1, "no") == 0) {
		return false;
	}

	if (*p_line->val_tok_1 == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be true or false or yes or no",
				p_line->num, p_line->name_tok);
	}

	cf_crash_nostack(AS_CFG, "line %d :: %s must be true or false or yes or no, not %s",
			p_line->num, p_line->name_tok, p_line->val_tok_1);

	// Won't get here, but quiet warnings...
	return false;
}

bool
cfg_bool_no_value_is_true(const cfg_line* p_line)
{
	return (*p_line->val_tok_1 == '\0') ? true : cfg_bool(p_line);
}

int64_t
cfg_i64_anyval_no_checks(const cfg_line* p_line, char* val_tok)
{
	if (*val_tok == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify an integer value",
				p_line->num, p_line->name_tok);
	}

	int64_t value;

	if (0 != cf_str_atoi_64(val_tok, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be a number, not %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return value;
}

int64_t
cfg_i64_no_checks(const cfg_line* p_line)
{
	return cfg_i64_anyval_no_checks(p_line, p_line->val_tok_1);
}

int64_t
cfg_i64_val2_no_checks(const cfg_line* p_line)
{
	return cfg_i64_anyval_no_checks(p_line, p_line->val_tok_2);
}

int64_t
cfg_i64(const cfg_line* p_line, int64_t min, int64_t max)
{
	int64_t value = cfg_i64_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %ld and <= %ld, not %ld",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

int
cfg_int_no_checks(const cfg_line* p_line)
{
	int64_t value = cfg_i64_no_checks(p_line);

	if (value < INT_MIN || value > INT_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %ld overflows int",
				p_line->num, p_line->name_tok, value);
	}

	return (int)value;
}

int
cfg_int(const cfg_line* p_line, int min, int max)
{
	int value = cfg_int_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %d and <= %d, not %d",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

int
cfg_int_val2_no_checks(const cfg_line* p_line)
{
	int64_t value = cfg_i64_val2_no_checks(p_line);

	if (value < INT_MIN || value > INT_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %ld overflows int",
				p_line->num, p_line->name_tok, value);
	}

	return (int)value;
}

int
cfg_int_val2(const cfg_line* p_line, int min, int max)
{
	int value = cfg_int_val2_no_checks(p_line);

	if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %d and <= %d, not %d",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint64_t
cfg_u64_anyval_no_checks(const cfg_line* p_line, char* val_tok)
{
	if (*val_tok == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify an unsigned integer value",
				p_line->num, p_line->name_tok);
	}

	uint64_t value;

	if (0 != cf_str_atoi_u64(val_tok, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an unsigned number, not %s",
				p_line->num, p_line->name_tok, val_tok);
	}

	return value;
}

uint64_t
cfg_u64_no_checks(const cfg_line* p_line)
{
	return cfg_u64_anyval_no_checks(p_line, p_line->val_tok_1);
}

uint64_t
cfg_u64_val2_no_checks(const cfg_line* p_line)
{
	return cfg_u64_anyval_no_checks(p_line, p_line->val_tok_2);
}

uint64_t
cfg_u64(const cfg_line* p_line, uint64_t min, uint64_t max)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %lu, not %lu",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %lu and <= %lu, not %lu",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint32_t
cfg_u32_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UINT_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned int",
				p_line->num, p_line->name_tok, value);
	}

	return (uint32_t)value;
}

uint32_t
cfg_u32(const cfg_line* p_line, uint32_t min, uint32_t max)
{
	uint32_t value = cfg_u32_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u, not %u",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint16_t
cfg_u16_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > USHRT_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned short",
				p_line->num, p_line->name_tok, value);
	}

	return (uint16_t)value;
}

uint16_t
cfg_u16(const cfg_line* p_line, uint16_t min, uint16_t max)
{
	uint16_t value = cfg_u16_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u, not %u",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

uint8_t
cfg_u8_no_checks(const cfg_line* p_line)
{
	uint64_t value = cfg_u64_no_checks(p_line);

	if (value > UCHAR_MAX) {
		cf_crash_nostack(AS_CFG, "line %d :: %s %lu overflows unsigned char",
				p_line->num, p_line->name_tok, value);
	}

	return (uint8_t)value;
}

uint8_t
cfg_u8(const cfg_line* p_line, uint8_t min, uint8_t max)
{
	uint8_t value = cfg_u8_no_checks(p_line);

	if (min == 0) {
		if (value > max) {
			cf_crash_nostack(AS_CFG, "line %d :: %s must be <= %u, not %u",
					p_line->num, p_line->name_tok, max, value);
		}
	}
	else if (value < min || value > max) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= %u and <= %u, not %u",
				p_line->num, p_line->name_tok, min, max, value);
	}

	return value;
}

double
cfg_pct_fraction(const cfg_line* p_line)
{
	if (*p_line->val_tok_1 == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify a numeric value",
				p_line->num, p_line->name_tok);
	}

	double value = atof(p_line->val_tok_1);

	if (value < 0.0 || value > 100.0) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be >= 0 and <= 100, not %s",
				p_line->num, p_line->name_tok, p_line->val_tok_1);
	}

	return value / 100.0;
}

uint64_t
cfg_seconds(const cfg_line* p_line)
{
	if (*p_line->val_tok_1 == '\0') {
		cf_crash_nostack(AS_CFG, "line %d :: %s must specify an unsigned integer value with time unit (s, m, h, or d)",
				p_line->num, p_line->name_tok);
	}

	uint64_t value;

	if (0 != cf_str_atoi_seconds(p_line->val_tok_1, &value)) {
		cf_crash_nostack(AS_CFG, "line %d :: %s must be an unsigned number with time unit (s, m, h, or d), not %s",
				p_line->num, p_line->name_tok, p_line->val_tok_1);
	}

	return value;
}

// Minimum & maximum port numbers:
const int CFG_MIN_PORT = 1024;
const int CFG_MAX_PORT = USHRT_MAX;

int
cfg_port(const cfg_line* p_line)
{
	return cfg_int(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
}

int
cfg_port_val2(const cfg_line* p_line)
{
	return cfg_int_val2(p_line, CFG_MIN_PORT, CFG_MAX_PORT);
}

//------------------------------------------------
// Constants used in parsing.
//

// Token delimiter characters:
const char CFG_WHITESPACE[] = " \t\n\r\f\v";


//==========================================================
// Public API - parse the configuration file.
//

as_config*
as_config_init(const char *config_file)
{
	as_config* c = &g_config; // shortcut pointer

	// Set the service context defaults. Values parsed from the config file will
	// override the defaults.
	cfg_set_defaults();

	FILE* FD;
	char iobuf[256];
	int line_num = 0;
	cfg_parser_state state;

	cfg_parser_state_init(&state);

	as_namespace* ns = NULL;
	cf_fault_sink* sink = NULL;
	as_set* p_set = NULL; // local variable used for set initialization
	as_sindex_config_var si_cfg;

	uint64_t config_val = 0;
	cc_group_t cluster_group_id = 0; // hold the group name while we process nodes (0 not a valid ID #)
	cc_node_t cluster_node_id; // capture the node id in a group

	// Flag mutually exclusive configuration options.
	bool transaction_queues_set = false;

	// Open the configuration file for reading.
	if (NULL == (FD = fopen(config_file, "r"))) {
		cf_crash_nostack(AS_CFG, "couldn't open configuration file %s: %s", config_file, cf_strerror(errno));
	}

	// Parse the configuration file, line by line.
	while (fgets(iobuf, sizeof(iobuf), FD)) {
		line_num++;

		// First chop the comment off, if there is one.

		char* p_comment = strchr(iobuf, '#');

		if (p_comment) {
			*p_comment = '\0';
		}

		// Find (and null-terminate) up to three whitespace-delimited tokens in
		// the line, a 'name' token and up to two 'value' tokens.

		cfg_line line = { line_num, NULL, NULL, NULL };

		line.name_tok = strtok(iobuf, CFG_WHITESPACE);

		// If there are no tokens, ignore this line, get the next line.
		if (! line.name_tok) {
			continue;
		}

		line.val_tok_1 = strtok(NULL, CFG_WHITESPACE);

		if (! line.val_tok_1) {
			line.val_tok_1 = ""; // in case it's used where NULL can't be used
		}
		else {
			line.val_tok_2 = strtok(NULL, CFG_WHITESPACE);
		}

		if (! line.val_tok_2) {
			line.val_tok_2 = ""; // in case it's used where NULL can't be used
		}

		// Note that we can't see this output until a logging sink is specified.
		cf_detail(AS_CFG, "line %d :: %s %s %s", line_num, line.name_tok, line.val_tok_1, line.val_tok_2);

		// Parse the directive.
		switch (state.current) {

		//==================================================
		// Parse top-level items.
		//
		case GLOBAL:
			switch (cfg_find_tok(line.name_tok, GLOBAL_OPTS, NUM_GLOBAL_OPTS)) {
			case CASE_SERVICE_BEGIN:
				cfg_begin_context(&state, SERVICE);
				break;
			case CASE_LOG_BEGIN:
				cfg_begin_context(&state, LOGGING);
				break;
			case CASE_NETWORK_BEGIN:
				cfg_begin_context(&state, NETWORK);
				break;
			case CASE_NAMESPACE_BEGIN:
				// Create the namespace objects.
				ns = as_namespace_create(line.val_tok_1, 2);
				cfg_begin_context(&state, NAMESPACE);
				break;
			case CASE_XDR_BEGIN:
				cfg_begin_context(&state, XDR);
				break;
			case CASE_MOD_LUA_BEGIN:
				cfg_begin_context(&state, MOD_LUA);
				break;
			case CASE_CLUSTER_BEGIN:
				cfg_begin_context(&state, CLUSTER);
				break;
			case CASE_SECURITY_BEGIN:
				cfg_begin_context(&state, SECURITY);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse service context items.
		//
		case SERVICE:
			switch(cfg_find_tok(line.name_tok, SERVICE_OPTS, NUM_SERVICE_OPTS)) {
			case CASE_SERVICE_USER:
				{
					struct passwd* pwd;
					if (NULL == (pwd = getpwnam(line.val_tok_1))) {
						cf_crash_nostack(AS_CFG, "line %d :: user not found: %s", line_num, line.val_tok_1);
					}
					c->uid = pwd->pw_uid;
					endpwent();
				}
				break;
			case CASE_SERVICE_GROUP:
				{
					struct group* grp;
					if (NULL == (grp = getgrnam(line.val_tok_1))) {
						cf_crash_nostack(AS_CFG, "line %d :: group not found: %s", line_num, line.val_tok_1);
					}
					c->gid = grp->gr_gid;
					endgrent();
				}
				break;
			case CASE_SERVICE_PAXOS_SINGLE_REPLICA_LIMIT:
				c->paxos_single_replica_limit = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_PIDFILE:
				c->pidfile = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_SERVICE_THREADS:
				c->n_service_threads = cfg_int(&line, 1, MAX_DEMARSHAL_THREADS);
				break;
			case CASE_SERVICE_TRANSACTION_QUEUES:
				c->n_transaction_queues = cfg_int(&line, 1, MAX_TRANSACTION_QUEUES);
				transaction_queues_set = true;
				break;
			case CASE_SERVICE_TRANSACTION_THREADS_PER_QUEUE:
				c->n_transaction_threads_per_queue = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_CLIENT_FD_MAX:
				cfg_renamed_name_tok(&line, "proto-fd-max");
				// Intentional fall-through.
			case CASE_SERVICE_PROTO_FD_MAX:
				c->n_proto_fd_max = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_ALLOW_INLINE_TRANSACTIONS:
				c->allow_inline_transactions = cfg_bool(&line);
				break;
			case CASE_SERVICE_AUTO_DUN:
				c->auto_dun = cfg_bool(&line);
				break;
			case CASE_SERVICE_AUTO_UNDUN:
				c->auto_undun = cfg_bool(&line);
				break;
			case CASE_SERVICE_BATCH_THREADS:
				c->n_batch_threads = cfg_int(&line, 0, MAX_BATCH_THREADS);
				break;
			case CASE_SERVICE_BATCH_MAX_BUFFERS_PER_QUEUE:
				c->batch_max_buffers_per_queue = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_MAX_REQUESTS:
				c->batch_max_requests = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_MAX_UNUSED_BUFFERS:
				c->batch_max_unused_buffers = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_PRIORITY:
				c->batch_priority = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_INDEX_THREADS:
				c->n_batch_index_threads = cfg_int(&line, 1, MAX_BATCH_THREADS);
				break;
			case CASE_SERVICE_FABRIC_WORKERS:
				c->n_fabric_workers = cfg_int(&line, 1, MAX_FABRIC_WORKERS);
				break;
			case CASE_SERVICE_FB_HEALTH_BAD_PCT:
				c->fb_health_bad_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_SERVICE_FB_HEALTH_GOOD_PCT:
				c->fb_health_good_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_SERVICE_FB_HEALTH_MSG_PER_BURST:
				c->fb_health_msg_per_burst = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_FB_HEALTH_MSG_TIMEOUT:
				c->fb_health_msg_timeout = cfg_u32(&line, 1, 200);
				break;
			case CASE_SERVICE_GENERATION_DISABLE:
				c->generation_disable = cfg_bool(&line);
				break;
			case CASE_SERVICE_HIST_TRACK_BACK:
				c->hist_track_back = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_HIST_TRACK_SLICE:
				c->hist_track_slice = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_HIST_TRACK_THRESHOLDS:
				c->hist_track_thresholds = cfg_strdup_no_checks(&line);
				// TODO - if config key present but no value (not even space) failure mode is bad...
				break;
			case CASE_SERVICE_INFO_THREADS:
				c->n_info_threads = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_LDT_BENCHMARKS:
				c->ldt_benchmarks = cfg_bool(&line);
				break;
			case CASE_SERVICE_MICROBENCHMARKS:
				c->microbenchmarks = cfg_bool(&line);
				break;
			case CASE_SERVICE_MIGRATE_MAX_NUM_INCOMING:
				c->migrate_max_num_incoming = cfg_int(&line, 0, INT_MAX);
				break;
			case CASE_SERVICE_MIGRATE_READ_PRIORITY:
				c->migrate_read_priority = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_READ_SLEEP:
				c->migrate_read_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_RX_LIFETIME_MS:
				c->migrate_rx_lifetime_ms = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_THREADS:
				c->n_migrate_threads = cfg_int(&line, 0, MAX_NUM_MIGRATE_XMIT_THREADS);
				break;
			case CASE_SERVICE_MIGRATE_XMIT_HWM:
				c->migrate_xmit_hwm = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_XMIT_LWM:
				c->migrate_xmit_lwm = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_PRIORITY:
				cfg_renamed_name_tok(&line, "migrate-xmit-priority");
				// Intentional fall-through.
			case CASE_SERVICE_MIGRATE_XMIT_PRIORITY:
				c->migrate_xmit_priority = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_MIGRATE_XMIT_SLEEP:
				c->migrate_xmit_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_NSUP_DELETE_SLEEP:
				c->nsup_delete_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_NSUP_PERIOD:
				c->nsup_period = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_NSUP_STARTUP_EVICT:
				c->nsup_startup_evict = cfg_bool(&line);
				break;
			case CASE_SERVICE_PAXOS_MAX_CLUSTER_SIZE:
				c->paxos_max_cluster_size = cfg_u64(&line, 2, AS_CLUSTER_SZ);
				break;
			case CASE_SERVICE_PAXOS_PROTOCOL:
				switch(cfg_find_tok(line.val_tok_1, SERVICE_PAXOS_PROTOCOL_OPTS, NUM_SERVICE_PAXOS_PROTOCOL_OPTS)) {
				case CASE_SERVICE_PAXOS_PROTOCOL_V1:
					c->paxos_protocol = AS_PAXOS_PROTOCOL_V1;
					break;
				case CASE_SERVICE_PAXOS_PROTOCOL_V2:
					c->paxos_protocol = AS_PAXOS_PROTOCOL_V2;
					break;
				case CASE_SERVICE_PAXOS_PROTOCOL_V3:
					c->paxos_protocol = AS_PAXOS_PROTOCOL_V3;
					break;
				case CASE_SERVICE_PAXOS_PROTOCOL_V4:
					c->paxos_protocol = AS_PAXOS_PROTOCOL_V4;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_SERVICE_PAXOS_RECOVERY_POLICY:
				switch(cfg_find_tok(line.val_tok_1, SERVICE_PAXOS_RECOVERY_OPTS, NUM_SERVICE_PAXOS_RECOVERY_OPTS)) {
				case CASE_SERVICE_PAXOS_RECOVERY_AUTO_DUN_ALL:
					c->paxos_recovery_policy = AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_ALL;
					break;
				case CASE_SERVICE_PAXOS_RECOVERY_AUTO_DUN_MASTER:
					c->paxos_recovery_policy = AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_MASTER;
					break;
				case CASE_SERVICE_PAXOS_RECOVERY_MANUAL:
					c->paxos_recovery_policy = AS_PAXOS_RECOVERY_POLICY_MANUAL;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_SERVICE_PAXOS_RETRANSMIT_PERIOD:
				c->paxos_retransmit_period = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_PROTO_FD_IDLE_MS:
				c->proto_fd_idle_ms = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_BATCH_SIZE:
				c->query_bsize = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_IN_TRANSACTION_THREAD:
				c->query_in_transaction_thr = cfg_bool(&line);
				break;
			case CASE_SERVICE_QUERY_PRE_RESERVE_QNODES:
				c->qnodes_pre_reserved = cfg_bool(&line);
				break;
			case CASE_SERVICE_QUERY_PRIORITY:
				c->query_priority = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_PRIORITY_SLEEP_US:
				c->query_sleep_us = cfg_u64_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_THRESHOLD:
				c->query_threshold = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_QUERY_UNTRACKED_TIME_MS:
				c->query_untracked_time_ms = cfg_u64_no_checks(&line);
				break;
			case CASE_SERVICE_REPLICATION_FIRE_AND_FORGET:
				c->replication_fire_and_forget = cfg_bool(&line);
				break;
			case CASE_SERVICE_RESPOND_CLIENT_ON_MASTER_COMPLETION:
				c->respond_client_on_master_completion = cfg_bool(&line);
				break;
			case CASE_SERVICE_RUN_AS_DAEMON:
				c->run_as_daemon = cfg_bool_no_value_is_true(&line);
				break;
			case CASE_SERVICE_SCAN_MAX_ACTIVE:
				c->scan_max_active = cfg_u32(&line, 0, 200);
				break;
			case CASE_SERVICE_SCAN_MAX_DONE:
				c->scan_max_done = cfg_u32(&line, 0, 1000);
				break;
			case CASE_SERVICE_SCAN_MAX_UDF_TRANSACTIONS:
				c->scan_max_udf_transactions = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_SCAN_THREADS:
				c->scan_threads = cfg_u32(&line, 0, 32);
				break;
			case CASE_SERVICE_SINDEX_DATA_MAX_MEMORY:
				config_val = cfg_u64_no_checks(&line);
				if (config_val < c->sindex_data_memory_used) {
					cf_warning(AS_CFG, "sindex-data-max-memory must"
							" be greater than existing used memory %ld (line %d)",
							cf_atomic_int_get(&c->sindex_data_memory_used), line_num);
				}
				else {
					c->sindex_data_max_memory = config_val; // this is in addition to namespace memory
				}
				break;
			case CASE_SERVICE_SNUB_NODES:
				c->snub_nodes = cfg_bool(&line);
				break;
			case CASE_SERVICE_STORAGE_BENCHMARKS:
				c->storage_benchmarks = cfg_bool(&line);
				break;
			case CASE_SERVICE_TICKER_INTERVAL:
				c->ticker_interval = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_TRANSACTION_DUPLICATE_THREADS:
				c->n_transaction_duplicate_threads = cfg_int_no_checks(&line);
				break;
			case CASE_SERVICE_TRANSACTION_MAX_MS:
				c->transaction_max_ns = cfg_u64_no_checks(&line) * 1000000;
				break;
			case CASE_SERVICE_TRANSACTION_PENDING_LIMIT:
				c->transaction_pending_limit = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_TRANSACTION_REPEATABLE_READ:
				c->transaction_repeatable_read = cfg_bool(&line);
				break;
			case CASE_SERVICE_TRANSACTION_RETRY_MS:
				c->transaction_retry_ms = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_UDF_RUNTIME_MAX_GMEMORY:
				config_val = cfg_u64_no_checks(&line);
				if (config_val < c->udf_runtime_gmemory_used) {
					cf_crash_nostack(AS_CFG, "udf-runtime-max-gmemory must"
							" be greater than existing used memory %ld (line %d)",
							cf_atomic_int_get(&c->udf_runtime_gmemory_used), line_num);
				}
				c->udf_runtime_max_gmemory = config_val;
				break;
			case CASE_SERVICE_UDF_RUNTIME_MAX_MEMORY:
				config_val = cfg_u64_no_checks(&line);
				break;
			case CASE_SERVICE_USE_QUEUE_PER_DEVICE:
				c->use_queue_per_device = cfg_bool(&line);
				break;
			case CASE_SERVICE_WORK_DIRECTORY:
				c->work_directory = cfg_strdup_no_checks(&line);
				break;
			case CASE_SERVICE_WRITE_DUPLICATE_RESOLUTION_DISABLE:
				c->write_duplicate_resolution_disable = cfg_bool(&line);
				break;
			case CASE_SERVICE_ASMALLOC_ENABLED:
				c->asmalloc_enabled = cfg_bool(&line);
				break;
			case CASE_SERVICE_DUMP_MESSAGE_ABOVE_SIZE:
				c->dump_message_above_size = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_FABRIC_DUMP_MSGS:
				c->fabric_dump_msgs = cfg_bool(&line);
				break;
			case CASE_SERVICE_MAX_MSGS_PER_TYPE:
				c->max_msgs_per_type = cfg_i64_no_checks(&line);
				msg_set_max_msgs_per_type(c->max_msgs_per_type = c->max_msgs_per_type >= 0 ? c->max_msgs_per_type : -1);
				break;
			case CASE_SERVICE_MEMORY_ACCOUNTING:
				c->memory_accounting = cfg_bool(&line);
				break;
			case CASE_SERVICE_PROLE_EXTRA_TTL:
				c->prole_extra_ttl = cfg_u32_no_checks(&line);
				break;
			case CASE_SERVICE_BATCH_RETRANSMIT:
			case CASE_SERVICE_CLIB_LIBRARY:
			case CASE_SERVICE_DEFRAG_QUEUE_ESCAPE:
			case CASE_SERVICE_DEFRAG_QUEUE_HWM:
			case CASE_SERVICE_DEFRAG_QUEUE_LWM:
			case CASE_SERVICE_DEFRAG_QUEUE_PRIORITY:
			case CASE_SERVICE_NSUP_AUTO_HWM:
			case CASE_SERVICE_NSUP_AUTO_HWM_PCT:
			case CASE_SERVICE_NSUP_MAX_DELETES:
			case CASE_SERVICE_NSUP_QUEUE_ESCAPE:
			case CASE_SERVICE_NSUP_QUEUE_HWM:
			case CASE_SERVICE_NSUP_QUEUE_LWM:
			case CASE_SERVICE_NSUP_REDUCE_PRIORITY:
			case CASE_SERVICE_NSUP_REDUCE_SLEEP:
			case CASE_SERVICE_NSUP_THREADS:
			case CASE_SERVICE_SCAN_MEMORY:
			case CASE_SERVICE_SCAN_PRIORITY:
			case CASE_SERVICE_SCAN_RETRANSMIT:
			case CASE_SERVICE_SCHEDULER_PRIORITY:
			case CASE_SERVICE_SCHEDULER_TYPE:
			case CASE_SERVICE_TRIAL_ACCOUNT_KEY:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				if (c->use_queue_per_device && transaction_queues_set) {
					cf_crash_nostack(AS_CFG, "can't set use-queue-per-device and explicit transaction-queues");
				}
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse logging context items.
		//
		case LOGGING:
			switch(cfg_find_tok(line.name_tok, LOGGING_OPTS, NUM_LOGGING_OPTS)) {
			case CASE_LOG_FILE_BEGIN:
				if ((sink = cf_fault_sink_hold(line.val_tok_1)) == NULL) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add file %s as log sink", line_num, line.val_tok_1);
				}
				cfg_begin_context(&state, LOGGING_FILE);
				break;
			case CASE_LOG_CONSOLE_BEGIN:
				if ((sink = cf_fault_sink_hold("stderr")) == NULL) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add stderr as log sink", line_num);
				}
				cfg_begin_context(&state, LOGGING_CONSOLE);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse logging::file context items.
		//
		case LOGGING_FILE:
			switch(cfg_find_tok(line.name_tok, LOGGING_FILE_OPTS, NUM_LOGGING_FILE_OPTS)) {
			case CASE_LOG_FILE_CONTEXT:
				if (0 != cf_fault_sink_addcontext(sink, line.val_tok_1, line.val_tok_2)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging file context %s %s", line_num, line.val_tok_1, line.val_tok_2);
				}
				break;
			case CASE_LOG_FILE_SPECIFIC:
				cfg_future_name_tok(&line); // TODO - deprecate?
				break;
			case CASE_CONTEXT_END:
				sink = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse logging::console context items.
		//
		case LOGGING_CONSOLE:
			switch(cfg_find_tok(line.name_tok, LOGGING_CONSOLE_OPTS, NUM_LOGGING_CONSOLE_OPTS)) {
			case CASE_LOG_CONSOLE_CONTEXT:
				if (0 != cf_fault_sink_addcontext(sink, line.val_tok_1, line.val_tok_2)) {
					cf_crash_nostack(AS_CFG, "line %d :: can't add logging console context %s %s", line_num, line.val_tok_1, line.val_tok_2);
				}
				break;
			case CASE_LOG_CONSOLE_SPECIFIC:
				cfg_future_name_tok(&line); // TODO - deprecate?
				break;
			case CASE_CONTEXT_END:
				sink = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse network context items.
		//
		case NETWORK:
			switch(cfg_find_tok(line.name_tok, NETWORK_OPTS, NUM_NETWORK_OPTS)) {
			case CASE_NETWORK_SERVICE_BEGIN:
				cfg_begin_context(&state, NETWORK_SERVICE);
				break;
			case CASE_NETWORK_HEARTBEAT_BEGIN:
				cfg_begin_context(&state, NETWORK_HEARTBEAT);
				break;
			case CASE_NETWORK_FABRIC_BEGIN:
				cfg_begin_context(&state, NETWORK_FABRIC);
				break;
			case CASE_NETWORK_INFO_BEGIN:
				cfg_begin_context(&state, NETWORK_INFO);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::service context items.
		//
		case NETWORK_SERVICE:
			switch(cfg_find_tok(line.name_tok, NETWORK_SERVICE_OPTS, NUM_NETWORK_SERVICE_OPTS)) {
			case CASE_NETWORK_SERVICE_ADDRESS:
				// TODO - is the strdup necessary (addr ever freed)?
				c->socket.addr = strcmp(line.val_tok_1, "any") == 0 ? cf_strdup("0.0.0.0") : cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_SERVICE_PORT:
				c->socket.port = cfg_port(&line);
				break;
			case CASE_NETWORK_SERVICE_EXTERNAL_ADDRESS:
				cfg_renamed_name_tok(&line, "access-address");
				// Intentional fall-through.
			case CASE_NETWORK_SERVICE_ACCESS_ADDRESS:
				c->external_address = cfg_strdup_no_checks(&line);
				c->is_external_address_virtual = strcmp(line.val_tok_2, "virtual") == 0;
				break;
			case CASE_NETWORK_SERVICE_NETWORK_INTERFACE_NAME:
				c->network_interface_name = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_SERVICE_REUSE_ADDRESS:
				c->socket_reuse_addr = cfg_bool_no_value_is_true(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::heartbeat context items.
		//
		case NETWORK_HEARTBEAT:
			switch(cfg_find_tok(line.name_tok, NETWORK_HEARTBEAT_OPTS, NUM_NETWORK_HEARTBEAT_OPTS)) {
			case CASE_NETWORK_HEARTBEAT_MODE:
				switch(cfg_find_tok(line.val_tok_1, NETWORK_HEARTBEAT_MODE_OPTS, NUM_NETWORK_HEARTBEAT_MODE_OPTS)) {
				case CASE_NETWORK_HEARTBEAT_MODE_MULTICAST:
					c->hb_mode = AS_HB_MODE_MCAST;
					break;
				case CASE_NETWORK_HEARTBEAT_MODE_MESH:
					c->hb_mode = AS_HB_MODE_MESH;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NETWORK_HEARTBEAT_ADDRESS:
				c->hb_addr = strcmp(line.val_tok_1, "any") == 0 ? cf_strdup("0.0.0.0") : cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_PORT:
				c->hb_port = cfg_int_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MESH_ADDRESS:
				c->hb_init_addr = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MESH_PORT:
				c->hb_init_port = cfg_port(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MESH_SEED_ADDRESS_PORT:
				cfg_add_mesh_seed_addr_port(cfg_strdup_no_checks(&line), cfg_port_val2(&line));
				break;
			case CASE_NETWORK_HEARTBEAT_INTERVAL:
				c->hb_interval = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_TIMEOUT:
				c->hb_timeout = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_INTERFACE_ADDRESS:
				c->hb_tx_addr = cfg_strdup_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MCAST_TTL:
				c->hb_mcast_ttl = cfg_u8_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_MESH_RW_RETRY_TIMEOUT:
				c->hb_mesh_rw_retry_timeout = cfg_u32_no_checks(&line);
				break;
			case CASE_NETWORK_HEARTBEAT_PROTOCOL:
				switch(cfg_find_tok(line.val_tok_1, NETWORK_HEARTBEAT_PROTOCOL_OPTS, NUM_NETWORK_HEARTBEAT_PROTOCOL_OPTS)) {
				case CASE_NETWORK_HEARTBEAT_PROTOCOL_RESET:
					c->hb_protocol = AS_HB_PROTOCOL_RESET;
					break;
				case CASE_NETWORK_HEARTBEAT_PROTOCOL_V1:
					c->hb_protocol = AS_HB_PROTOCOL_V1;
					break;
				case CASE_NETWORK_HEARTBEAT_PROTOCOL_V2:
					c->hb_protocol = AS_HB_PROTOCOL_V2;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_CONTEXT_END:
				if (c->hb_init_addr && c->hb_mesh_seed_addrs[0]) {
					cf_crash_nostack(AS_CFG, "can't use both mesh-address and mesh-seed-address-port");
				}
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::fabric context items.
		//
		case NETWORK_FABRIC:
			switch(cfg_find_tok(line.name_tok, NETWORK_FABRIC_OPTS, NUM_NETWORK_FABRIC_OPTS)) {
			case CASE_NETWORK_FABRIC_ADDRESS:
				cfg_future_name_tok(&line); // TODO - deprecate?
				break;
			case CASE_NETWORK_FABRIC_PORT:
				c->fabric_port = cfg_port(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_ENABLED:
				c->fabric_keepalive_enabled = cfg_bool(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_TIME:
				c->fabric_keepalive_time = cfg_int_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_INTVL:
				c->fabric_keepalive_intvl = cfg_int_no_checks(&line);
				break;
			case CASE_NETWORK_FABRIC_KEEPALIVE_PROBES:
				c->fabric_keepalive_probes = cfg_int_no_checks(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse network::info context items.
		//
		case NETWORK_INFO:
			switch(cfg_find_tok(line.name_tok, NETWORK_INFO_OPTS, NUM_NETWORK_INFO_OPTS)) {
			case CASE_NETWORK_INFO_ADDRESS:
				cfg_future_name_tok(&line); // TODO - deprecate?
				break;
			case CASE_NETWORK_INFO_PORT:
				c->info_port = cfg_port(&line);
				break;
			case CASE_NETWORK_INFO_ENABLE_FASTPATH:
				c->info_fastpath_enabled = cfg_bool(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse namespace items.
		//
		case NAMESPACE:
			switch(cfg_find_tok(line.name_tok, NAMESPACE_OPTS, NUM_NAMESPACE_OPTS)) {
			case CASE_NAMESPACE_REPLICATION_FACTOR:
				ns->cfg_replication_factor = ns->replication_factor = cfg_u16(&line, 1, AS_CLUSTER_SZ);
				break;
			case CASE_NAMESPACE_LIMIT_SIZE:
				cfg_renamed_name_tok(&line, "memory-size");
				// Intentional fall-through.
			case CASE_NAMESPACE_MEMORY_SIZE:
				ns->memory_size = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_DEFAULT_TTL:
				ns->default_ttl = cfg_seconds(&line);
				break;
			case CASE_NAMESPACE_STORAGE_ENGINE_BEGIN:
				switch(cfg_find_tok(line.val_tok_1, NAMESPACE_STORAGE_OPTS, NUM_NAMESPACE_STORAGE_OPTS)) {
				case CASE_NAMESPACE_STORAGE_MEMORY:
					ns->storage_type = AS_STORAGE_ENGINE_MEMORY;
					ns->storage_data_in_memory = true;
					break;
				case CASE_NAMESPACE_STORAGE_SSD:
					cfg_renamed_val_tok_1(&line, "device");
					// Intentional fall-through.
				case CASE_NAMESPACE_STORAGE_DEVICE:
					ns->storage_type = AS_STORAGE_ENGINE_SSD;
					ns->storage_data_in_memory = false;
					cfg_begin_context(&state, NAMESPACE_STORAGE_DEVICE);
					break;
				case CASE_NAMESPACE_STORAGE_KV:
					ns->storage_type = AS_STORAGE_ENGINE_KV;
					ns->storage_data_in_memory = false;
					cfg_begin_context(&state, NAMESPACE_STORAGE_KV);
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_ENABLE_XDR:
				ns->enable_xdr = cfg_bool(&line);
				if (ns->enable_xdr && ! c->xdr_cfg.xdr_supported) {
					cfg_not_supported(&line, "XDR");
				}
				break;
			case CASE_NAMESPACE_SETS_ENABLE_XDR:
				ns->sets_enable_xdr = cfg_bool(&line);
				if (ns->sets_enable_xdr && ! c->xdr_cfg.xdr_supported) {
					cfg_not_supported(&line, "XDR");
				}
				break;
			case CASE_NAMESPACE_FORWARD_XDR_WRITES:
				ns->ns_forward_xdr_writes = cfg_bool(&line);
				if (ns->ns_forward_xdr_writes && ! c->xdr_cfg.xdr_supported) {
					cfg_not_supported(&line, "XDR");
				}
				break;
			case CASE_NAMESPACE_ALLOW_NONXDR_WRITES:
				ns->ns_allow_nonxdr_writes = cfg_bool(&line);
				if (ns->ns_allow_nonxdr_writes && ! c->xdr_cfg.xdr_supported) {
					cfg_not_supported(&line, "XDR");
				}
				break;
			case CASE_NAMESPACE_ALLOW_XDR_WRITES:
				ns->ns_allow_xdr_writes = cfg_bool(&line);
				if (ns->ns_allow_xdr_writes && ! c->xdr_cfg.xdr_supported) {
					cfg_not_supported(&line, "XDR");
				}
				break;
			case CASE_NAMESPACE_XDR_REMOTE_DATACENTER:
				// The server isn't interested in this, but the XDR module is!
				break;
			case CASE_NAMESPACE_COLD_START_EVICT_TTL:
				ns->cold_start_evict_ttl = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_CONFLICT_RESOLUTION_POLICY:
				switch(cfg_find_tok(line.val_tok_1, NAMESPACE_CONFLICT_RESOLUTION_OPTS, NUM_NAMESPACE_CONFLICT_RESOLUTION_OPTS)) {
				case CASE_NAMESPACE_CONFLICT_RESOLUTION_GENERATION:
					ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
					break;
				case CASE_NAMESPACE_CONFLICT_RESOLUTION_TTL:
					ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_TTL;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_DATA_IN_INDEX:
				ns->data_in_index = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_DISALLOW_NULL_SETNAME:
				ns->disallow_null_setname = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_EVICT_TENTHS_PCT:
				ns->evict_tenths_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_HIGH_WATER_DISK_PCT:
				ns->hwm_disk = (float)cfg_pct_fraction(&line);
				break;
			case CASE_NAMESPACE_HIGH_WATER_MEMORY_PCT:
				ns->hwm_memory = (float)cfg_pct_fraction(&line);
				break;
			case CASE_NAMESPACE_LDT_ENABLED:
				ns->ldt_enabled = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_LDT_GC_RATE:
				ns->ldt_gc_sleep_us = cfg_u64(&line, 1, LDT_SUB_GC_MAX_RATE) * 1000000;
				break;
			case CASE_NAMESPACE_LDT_PAGE_SIZE:
				ns->ldt_page_size = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_MAX_TTL:
				ns->max_ttl = cfg_seconds(&line);
				break;
			case CASE_NAMESPACE_OBJ_SIZE_HIST_MAX:
				ns->obj_size_hist_max = cfg_obj_size_hist_max(cfg_u32_no_checks(&line));
				break;
			case CASE_NAMESPACE_READ_CONSISTENCY_LEVEL_OVERRIDE:
				switch(cfg_find_tok(line.val_tok_1, NAMESPACE_READ_CONSISTENCY_OPTS, NUM_NAMESPACE_READ_CONSISTENCY_OPTS)) {
				case CASE_NAMESPACE_READ_CONSISTENCY_ALL:
					ns->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ALL;
					ns->read_consistency_level_override = true;
					break;
				case CASE_NAMESPACE_READ_CONSISTENCY_OFF:
					ns->read_consistency_level_override = false;
					break;
				case CASE_NAMESPACE_READ_CONSISTENCY_ONE:
					ns->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
					ns->read_consistency_level_override = true;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_SET_BEGIN:
				p_set = cfg_add_set(ns);
				cfg_strcpy(&line, p_set->name, AS_SET_NAME_MAX_SIZE);
				cfg_begin_context(&state, NAMESPACE_SET);
				break;
			case CASE_NAMESPACE_SI_BEGIN:
				cfg_init_si_var(ns);
				as_sindex_config_var_default(&si_cfg);
				cfg_strcpy(&line, si_cfg.name, AS_ID_INAME_SZ);
				cfg_begin_context(&state, NAMESPACE_SI);
				break;
			case CASE_NAMESPACE_SINDEX_BEGIN:
				cfg_begin_context(&state, NAMESPACE_SINDEX);
				break;
			case CASE_NAMESPACE_SINGLE_BIN:
				ns->single_bin = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STOP_WRITES_PCT:
				ns->stop_writes_pct = (float)cfg_pct_fraction(&line);
				break;
			case CASE_NAMESPACE_WRITE_COMMIT_LEVEL_OVERRIDE:
				switch(cfg_find_tok(line.val_tok_1, NAMESPACE_WRITE_COMMIT_OPTS, NUM_NAMESPACE_WRITE_COMMIT_OPTS)) {
				case CASE_NAMESPACE_WRITE_COMMIT_ALL:
					ns->write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
					ns->write_commit_level_override = true;
					break;
				case CASE_NAMESPACE_WRITE_COMMIT_MASTER:
					ns->write_commit_level = AS_POLICY_COMMIT_LEVEL_MASTER;
					ns->write_commit_level_override = true;
					break;
				case CASE_NAMESPACE_WRITE_COMMIT_OFF:
					ns->write_commit_level_override = false;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_ALLOW_VERSIONS:
			case CASE_NAMESPACE_DEMO_READ_MULTIPLIER:
			case CASE_NAMESPACE_DEMO_WRITE_MULTIPLIER:
			case CASE_NAMESPACE_HIGH_WATER_PCT:
			case CASE_NAMESPACE_LOW_WATER_PCT:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				if (ns->data_in_index && ! (ns->single_bin && ns->storage_data_in_memory && ns->storage_type == AS_STORAGE_ENGINE_SSD)) {
					cf_crash_nostack(AS_CFG, "ns %s data-in-index can't be true unless storage-engine is device and both single-bin and data-in-memory are true", ns->name);
				}
				if (ns->ldt_enabled && ns->single_bin) {
					cf_crash_nostack(AS_CFG, "ns %s ldt-enabled and single-bin can't both be true", ns->name);
				}
				if (ns->storage_data_in_memory) {
					ns->storage_post_write_queue = 0; // override default (or configuration mistake)
					c->n_namespaces_in_memory++;
				}
				else {
					c->n_namespaces_not_in_memory++;
				}
				if (ns->ldt_page_size > ns->storage_write_block_size) {
					ns->ldt_page_size = ns->storage_write_block_size - 1024; // 1K headroom
				}
				ns = NULL;
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::storage-engine device context items.
		//
		case NAMESPACE_STORAGE_DEVICE:
			switch(cfg_find_tok(line.name_tok, NAMESPACE_STORAGE_DEVICE_OPTS, NUM_NAMESPACE_STORAGE_DEVICE_OPTS)) {
			case CASE_NAMESPACE_STORAGE_DEVICE_DEVICE:
				cfg_add_storage_device(ns, cfg_strdup(&line, true), cfg_strdup_val2(&line, false));
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FILE:
				cfg_add_storage_file(ns, cfg_strdup(&line, true));
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FILESIZE:
				ns->storage_filesize = cfg_i64(&line, 1024 * 1024, AS_STORAGE_MAX_DEVICE_SIZE);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_SCHEDULER_MODE:
				ns->storage_scheduler_mode = cfg_strdup_one_of(&line, DEVICE_SCHEDULER_MODES, NUM_DEVICE_SCHEDULER_MODES);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_BLOCK_SIZE:
				ns->storage_write_block_size = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MEMORY_ALL:
				cfg_renamed_name_tok(&line, "data-in-memory");
				// Intentional fall-through.
			case CASE_NAMESPACE_STORAGE_DEVICE_DATA_IN_MEMORY:
				ns->storage_data_in_memory = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_COLD_START_EMPTY:
				ns->storage_cold_start_empty = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_LWM_PCT:
				ns->storage_defrag_lwm_pct = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_QUEUE_MIN:
				ns->storage_defrag_queue_min = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_SLEEP:
				ns->storage_defrag_sleep = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_STARTUP_MINIMUM:
				ns->storage_defrag_startup_minimum = cfg_int(&line, 1, 99);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DISABLE_ODIRECT:
				ns->storage_disable_odirect = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_ENABLE_OSYNC:
				ns->storage_enable_osync = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FLUSH_MAX_MS:
				ns->storage_flush_max_us = cfg_u64_no_checks(&line) * 1000;
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_FSYNC_MAX_SEC:
				ns->storage_fsync_max_us = cfg_u64_no_checks(&line) * 1000000;
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MAX_WRITE_CACHE:
				ns->storage_max_write_cache = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_MIN_AVAIL_PCT:
				ns->storage_min_avail_pct = cfg_u32(&line, 0, 100);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_POST_WRITE_QUEUE:
				ns->storage_post_write_queue = cfg_u32(&line, 0, 2 * 1024);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_SIGNATURE:
				ns->storage_signature = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_THREADS:
				ns->storage_write_threads = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_MAX_BLOCKS:
			case CASE_NAMESPACE_STORAGE_DEVICE_DEFRAG_PERIOD:
			case CASE_NAMESPACE_STORAGE_DEVICE_LOAD_AT_STARTUP:
			case CASE_NAMESPACE_STORAGE_DEVICE_PERSIST:
			case CASE_NAMESPACE_STORAGE_DEVICE_READONLY:
			case CASE_NAMESPACE_STORAGE_DEVICE_WRITE_SMOOTHING_PERIOD:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::storage-engine kv context items.
		//
		case NAMESPACE_STORAGE_KV:
			switch(cfg_find_tok(line.name_tok, NAMESPACE_STORAGE_KV_OPTS, NUM_NAMESPACE_STORAGE_KV_OPTS)) {
			case CASE_NAMESPACE_STORAGE_KV_DEVICE:
				cfg_add_storage_file(ns, cfg_strdup(&line, true));
				break;
			case CASE_NAMESPACE_STORAGE_KV_FILESIZE:
				ns->storage_filesize = cfg_i64_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_KV_READ_BLOCK_SIZE:
				ns->storage_read_block_size = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_KV_WRITE_BLOCK_SIZE:
				ns->storage_write_block_size = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_KV_NUM_WRITE_BLOCKS:
				ns->storage_num_write_blocks = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_STORAGE_KV_COND_WRITE:
				ns->cond_write = cfg_bool(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::set context items.
		//
		case NAMESPACE_SET:
			switch(cfg_find_tok(line.name_tok, NAMESPACE_SET_OPTS, NUM_NAMESPACE_SET_OPTS)) {
			case CASE_NAMESPACE_SET_DISABLE_EVICTION:
				DISABLE_SET_EVICTION(p_set, cfg_bool(&line));
				break;
			case CASE_NAMESPACE_SET_ENABLE_XDR:
				switch(cfg_find_tok(line.val_tok_1, NAMESPACE_SET_ENABLE_XDR_OPTS, NUM_NAMESPACE_SET_ENABLE_XDR_OPTS)) {
				case CASE_NAMESPACE_SET_ENABLE_XDR_USE_DEFAULT:
					p_set->enable_xdr = AS_SET_ENABLE_XDR_DEFAULT;
					break;
				case CASE_NAMESPACE_SET_ENABLE_XDR_FALSE:
					p_set->enable_xdr = AS_SET_ENABLE_XDR_FALSE;
					break;
				case CASE_NAMESPACE_SET_ENABLE_XDR_TRUE:
					p_set->enable_xdr = AS_SET_ENABLE_XDR_TRUE;
					break;
				case CASE_NOT_FOUND:
				default:
					cfg_unknown_val_tok_1(&line);
					break;
				}
				break;
			case CASE_NAMESPACE_SET_EVICT_HWM_COUNT:
			case CASE_NAMESPACE_SET_EVICT_HWM_PCT:
			case CASE_NAMESPACE_SET_STOP_WRITE_COUNT:
			case CASE_NAMESPACE_SET_STOP_WRITE_PCT:
				cfg_deprecated_name_tok(&line);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::si context items.
		//
		case NAMESPACE_SI:
			switch(cfg_find_tok(line.name_tok, NAMESPACE_SI_OPTS, NUM_NAMESPACE_SI_OPTS)) {
			case CASE_NAMESPACE_SI_GC_PERIOD:
				si_cfg.defrag_period= cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_SI_GC_MAX_UNITS:
				si_cfg.defrag_max_units = cfg_u32_no_checks(&line);
				break;
			case CASE_NAMESPACE_SI_DATA_MAX_MEMORY:
				si_cfg.data_max_memory = cfg_u64_no_checks(&line);
				break;
			case CASE_NAMESPACE_SI_TRACING:
				si_cfg.trace_flag = cfg_u16_no_checks(&line);
				break;
			case CASE_NAMESPACE_SI_HISTOGRAM:
				si_cfg.enable_histogram = cfg_bool(&line);
				break;
			case CASE_NAMESPACE_SI_IGNORE_NOT_SYNC:
				si_cfg.ignore_not_sync_flag = cfg_bool(&line) ? 1 : 0;
				break;
			case CASE_CONTEXT_END:
				if (SHASH_OK != shash_put_unique(ns->sindex_cfg_var_hash, (void*)si_cfg.name, (void*)&si_cfg)) {
					cf_crash_nostack(AS_CFG, "ns %s failed inserting hash for si config item %s", ns->name, si_cfg.name);
				}
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_val_tok_1(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse namespace::sindex context items.
		//
		case NAMESPACE_SINDEX:
			switch(cfg_find_tok(line.name_tok, NAMESPACE_SINDEX_OPTS, NUM_NAMESPACE_SINDEX_OPTS)) {
			case CASE_NAMESPACE_SINDEX_DATA_MAX_MEMORY:
				config_val = cfg_u64_no_checks(&line);
				if (config_val < ns->sindex_data_memory_used) {
					cf_warning(AS_CFG, "sindex-data-max-memory must"
							" be greater than existing used memory %ld (line %d)",
							cf_atomic_int_get(&ns->sindex_data_max_memory), line_num);
				}
				else {
					ns->sindex_data_max_memory = config_val; // this is in addition to namespace memory
				}
				break;
			case CASE_NAMESPACE_SINDEX_NUM_PARTITIONS:
				// FIXME - minimum should be 1, but currently crashes.
				ns->sindex_num_partitions = cfg_u32(&line, MIN_PARTITIONS_PER_INDEX, MAX_PARTITIONS_PER_INDEX);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse xdr context items.
		//
		case XDR:
			switch(xdr_cfg_find_tok(line.name_tok, XDR_OPTS, NUM_XDR_OPTS)) {
			case XDR_CASE_ENABLE_XDR:
				c->xdr_cfg.xdr_global_enabled = cfg_bool(&line);
				if (c->xdr_cfg.xdr_global_enabled && ! c->xdr_cfg.xdr_supported) {
					cfg_not_supported(&line, "XDR");
				}
				break;
			case XDR_CASE_NAMEDPIPE_PATH:
				c->xdr_cfg.xdr_digestpipe_path = cfg_strdup_no_checks(&line);
				break;
			case XDR_CASE_FORWARD_XDR_WRITES:
				c->xdr_cfg.xdr_forward_xdrwrites = cfg_bool(&line);
				break;
			case XDR_CASE_XDR_DELETE_SHIPPING_ENABLED:
				c->xdr_cfg.xdr_delete_shipping_enabled = cfg_bool(&line);
				break;
			case XDR_CASE_XDR_NSUP_DELETES_ENABLED:
				c->xdr_cfg.xdr_nsup_deletes_enabled = cfg_bool(&line);
				break;
			case XDR_CASE_STOP_WRITES_NOXDR:
				c->xdr_cfg.xdr_stop_writes_noxdr = cfg_bool(&line);
				break;
			case XDR_CASE_DATACENTER_BEGIN:
				cfg_begin_context(&state, XDR_DATACENTER);
				break;
			case XDR_CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case XDR_CASE_NOT_FOUND:
			default:
				// We do not use a default case here. Any other config option is
				// specific to the XDR module and the server is not interested
				// in it.
				break;
			}
			break;

		//----------------------------------------
		// Parse xdr::datacenter context items.
		//
		case XDR_DATACENTER:
			// This is a hack to avoid defining a new array for the datacenter
			// subsection. The server is not interested in the details. It just
			// wants the subsection to end. So just check for the closing brace.
			switch(xdr_cfg_find_tok(line.name_tok, XDR_DC_OPTS, NUM_XDR_DC_OPTS)) {
			case XDR_CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			default:
				// Ignore all lines in datacenter subsection except for end.
				break;
			}
			break;

		//==================================================
		// Parse mod-lua context items.
		//
		case MOD_LUA:
			switch(cfg_find_tok(line.name_tok, MOD_LUA_OPTS, NUM_MOD_LUA_OPTS)) {
			case CASE_MOD_LUA_CACHE_ENABLED:
				c->mod_lua.cache_enabled = cfg_bool(&line);
				break;
			case CASE_MOD_LUA_SYSTEM_PATH:
				cfg_strcpy(&line, c->mod_lua.system_path, sizeof(c->mod_lua.system_path));
				break;
			case CASE_MOD_LUA_USER_PATH:
				cfg_strcpy(&line, c->mod_lua.user_path, sizeof(c->mod_lua.user_path));
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse cluster context items.
		//
		case CLUSTER:
			switch(cfg_find_tok(line.name_tok, CLUSTER_OPTS, NUM_CLUSTER_OPTS)) {
			case CASE_CLUSTER_SELF_NODE_ID:
				c->cluster.cl_self_node = cfg_u32_no_checks(&line);
				break;
			case CASE_CLUSTER_SELF_GROUP_ID:
				c->cluster.cl_self_group = cfg_u16_no_checks(&line);
				break;
			case CASE_CLUSTER_MODE:
				// Define the MODE for this cluster: static or dynamic.
				if (strcmp(line.val_tok_1, CL_STR_STATIC) == 0) {
					c->cluster_mode = CL_MODE_STATIC;
				}
				else if (strcmp(line.val_tok_1, CL_STR_DYNAMIC) == 0) {
					c->cluster_mode = CL_MODE_DYNAMIC;
				}
				else if (strcmp(line.val_tok_1, CL_STR_NONE) == 0) {
					// Same as default case -- for now.  Leave as separate
					// test, though, to make future changes easier.
					c->cluster_mode = CL_MODE_NO_TOPOLOGY;
				}
				else {
					c->cluster_mode = CL_MODE_NO_TOPOLOGY;
				}
				break;
			case CASE_CLUSTER_GROUP_BEGIN:
				cluster_group_id = cfg_u16_no_checks(&line);
				cc_add_group(&(c->cluster), cluster_group_id);
				cfg_begin_context(&state, CLUSTER_GROUP);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse cluster::group context items.
		//
		case CLUSTER_GROUP:
			switch(cfg_find_tok(line.name_tok, CLUSTER_GROUP_OPTS, NUM_CLUSTER_GROUP_OPTS)) {
			case CASE_CLUSTER_GROUP_NODE_ID:
				// For each node ID, register the node and group.
				cluster_node_id = cfg_u32_no_checks(&line);
				cc_add_node_group_entry(&(c->cluster), cluster_node_id, cluster_group_id);
				cf_detail(AS_CFG, "node ID(%08x) Group ID(%04x)", cluster_node_id, cluster_group_id);
				break;
			case CASE_CONTEXT_END:
				cluster_group_id = 0; // clear the group ID
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parse security context items.
		//
		case SECURITY:
			switch(cfg_find_tok(line.name_tok, SECURITY_OPTS, NUM_SECURITY_OPTS)) {
			case CASE_SECURITY_ENABLE_SECURITY:
				c->sec_cfg.security_enabled = cfg_bool(&line);
				break;
			case CASE_SECURITY_PRIVILEGE_REFRESH_PERIOD:
				c->sec_cfg.privilege_refresh_period = cfg_u32(&line, 10, 60 * 60 * 24);
				break;
			case CASE_SECURITY_LOG_BEGIN:
				cfg_begin_context(&state, SECURITY_LOG);
				break;
			case CASE_SECURITY_SYSLOG_BEGIN:
				cfg_begin_context(&state, SECURITY_SYSLOG);
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse security::log context items.
		//
		case SECURITY_LOG:
			switch(cfg_find_tok(line.name_tok, SECURITY_LOG_OPTS, NUM_SECURITY_LOG_OPTS)) {
			case CASE_SECURITY_LOG_REPORT_AUTHENTICATION:
				c->sec_cfg.report.authentication |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_SECURITY_LOG_REPORT_DATA_OP:
				as_security_config_log_scope(AS_SEC_SINK_LOG, line.val_tok_1, line.val_tok_2);
				break;
			case CASE_SECURITY_LOG_REPORT_SYS_ADMIN:
				c->sec_cfg.report.sys_admin |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_SECURITY_LOG_REPORT_USER_ADMIN:
				c->sec_cfg.report.user_admin |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_SECURITY_LOG_REPORT_VIOLATION:
				c->sec_cfg.report.violation |= cfg_bool(&line) ? AS_SEC_SINK_LOG : 0;
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//----------------------------------------
		// Parse security::syslog context items.
		//
		case SECURITY_SYSLOG:
			switch(cfg_find_tok(line.name_tok, SECURITY_SYSLOG_OPTS, NUM_SECURITY_SYSLOG_OPTS)) {
			case CASE_SECURITY_SYSLOG_LOCAL:
				c->sec_cfg.syslog_local = (as_sec_syslog_local)cfg_int(&line, AS_SYSLOG_MIN, AS_SYSLOG_MAX);
				break;
			case CASE_SECURITY_SYSLOG_REPORT_AUTHENTICATION:
				c->sec_cfg.report.authentication |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_SECURITY_SYSLOG_REPORT_DATA_OP:
				as_security_config_log_scope(AS_SEC_SINK_SYSLOG, line.val_tok_1, line.val_tok_2);
				break;
			case CASE_SECURITY_SYSLOG_REPORT_SYS_ADMIN:
				c->sec_cfg.report.sys_admin |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_SECURITY_SYSLOG_REPORT_USER_ADMIN:
				c->sec_cfg.report.user_admin |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_SECURITY_SYSLOG_REPORT_VIOLATION:
				c->sec_cfg.report.violation |= cfg_bool(&line) ? AS_SEC_SINK_SYSLOG : 0;
				break;
			case CASE_CONTEXT_END:
				cfg_end_context(&state);
				break;
			case CASE_NOT_FOUND:
			default:
				cfg_unknown_name_tok(&line);
				break;
			}
			break;

		//==================================================
		// Parser state is corrupt.
		//
		default:
			cf_crash_nostack(AS_CFG, "line %d :: invalid parser top-level state %d", line_num, state.current);
			break;
		}
	}

	fclose(FD);

	//--------------------------------------------
	// Checks that must wait until everything is parsed. Alternatively, such
	// checks can be done in as_config_post_process() - doing them here means
	// failure logs show in the console, doing them in as_config_post_process()
	// means failure logs show in the log file.
	//

	as_security_config_check();

	return &g_config;
}


//==========================================================
// Public API - configuration-related tasks after parsing.
//

void
as_config_post_process(as_config *c, const char *config_file)
{
	//--------------------------------------------
	// Re-read the configuration file and print it to the logs, line by line.
	// This will be the first thing to appear in the log file(s).
	//

	FILE* FD;

	if (NULL == (FD = fopen(config_file, "r"))) {
		cf_crash_nostack(AS_CFG, "couldn't re-open configuration file %s: %s", config_file, cf_strerror(errno));
	}

	char iobuf[256];

	while (fgets(iobuf, sizeof(iobuf), FD)) {
		char* p = iobuf;
		char* p_last = p + (strlen(p) - 1);

		if ('\n' == *p_last) {
			*p_last-- = '\0';
		}

		if (p_last >= p && '\r' == *p_last) {
			*p_last = '\0';
		}

		cf_info(AS_CFG, "%s", p);
	}

	fclose(FD);

	//
	// Done echoing configuration file to log.
	//--------------------------------------------

	// Check the configured file descriptor limit against the system limit.
	struct rlimit fd_limit;

	getrlimit(RLIMIT_NOFILE, &fd_limit);

	if (c->n_proto_fd_max < 0 || (rlim_t)c->n_proto_fd_max > fd_limit.rlim_cur) {
		cf_crash(AS_CFG, "%lu system file descriptors not enough, config specified %d", fd_limit.rlim_cur, c->n_proto_fd_max);
	}

	cf_info(AS_CFG, "system file descriptor limit: %lu, proto-fd-max: %d", fd_limit.rlim_cur, c->n_proto_fd_max);

	// Setup performance metrics histograms.
	cfg_create_all_histograms();

	// Since cfg_use_hardware_values() has side effects, we MUST call it, and
	// THEN if we are doing the new topology, set the new type of Self Node
	// value.
	c->self_node = (cf_node) 0;
	cfg_use_hardware_values(c);
	// Cache the HW value - which will be overridden if cache-aware is on.
	c->hw_self_node = c->self_node;

	// If we're in "manual cluster topology" mode, then that means the user has
	// defined a node ID and Group ID in the config file, and thus we will
	// override the HW Generated file and create a PORT + GROUP + NODE value
	// for the "Self Node" value that is passed around by Paxos.
	//
	// FOR FUTURE CONSIDERATION:: We may want to use the new naming style even
	// when Rack-Aware mode is turned off -- provided that the group and node
	// values are valid.  However, in order to use "cfg_reset_self_node()", we
	// ALSO must make sure that other things are in effect (e.g. Paxos V4).
	// So -- this is just a bookmark for some future changes:
	// TODO: Update this to set new self_node even in "No Topology" mode.
	if (c->cluster_mode != CL_MODE_NO_TOPOLOGY) {
		cf_info(AS_CFG, "Rack Aware mode enabled");
		// Do some checking here to verify that the user gave us valid group
		// and node id values.  If not, then stop here and do not proceed.
		if (c->cluster.cl_self_group == 0) {
			cf_crash_nostack(AS_CFG,
				"Cluster 'self-group-id' must be set to a non-zero value");
		}
		if (c->cluster.cl_self_node == 0 && c->cluster_mode == CL_MODE_STATIC) {
			cf_crash_nostack(AS_CFG,
				"Cluster 'self-node-id' must be set to a non-zero value when in 'static' mode");
		}
		// Check that we are NOT claiming we can do Rack-Aware for RF > 2.
		// Although that will be fixed soon, for now we must crash if the
		// user has specified RA is ON for RF > 2.
		// TODO: Remove this when RA works for RF > 2.
		for (uint i = 0; i < g_config.namespaces; i++) {
			as_namespace *ns = g_config.namespace[i];

			if (ns->replication_factor > 2) {
				cf_crash_nostack(AS_CFG,
					"Rack-Aware Feature is not currently available for Replication Factor greater than 2.");
			}
		}

		// If we got this far, then group/node should be ok.
		cfg_reset_self_node(c);
	}
	else if (AS_PAXOS_PROTOCOL_V4 == c->paxos_protocol) {
		cf_crash_nostack(AS_CFG, "must only use Paxos protocol V4 with Rack Aware enabled");
	}
	else {
		cf_info(AS_CFG, "Rack Aware mode not enabled");
	}

	cf_info(AS_CFG, "Node id %"PRIx64, c->self_node);

	// Handle specific service address (as opposed to 'any') if configured.
	if (strcmp(g_config.socket.addr, "0.0.0.0") != 0) {
		if (g_config.external_address) {
			if (strcmp(g_config.external_address, g_config.socket.addr) != 0) {
				cf_crash_nostack(AS_CFG, "external address '%s' does not match service address '%s'",
						g_config.external_address, g_config.socket.addr);
			}
		}
		else {
			// Set external address to avoid updating service list continuously.
			g_config.external_address = g_config.socket.addr;
		}
	}

	if (g_config.external_address && ! g_config.is_external_address_virtual) {
		// Check if external address matches any address in service list.
		uint8_t buf[512];
		cf_ifaddr *ifaddr;
		int	ifaddr_sz;
		cf_ifaddr_get(&ifaddr, &ifaddr_sz, buf, sizeof(buf));

		cf_dyn_buf_define(temp_service_db);
		build_service_list(ifaddr, ifaddr_sz, &temp_service_db);

		char *service_str = cf_dyn_buf_strdup(&temp_service_db);

		if (! (service_str && strstr(service_str, g_config.external_address))) {
			cf_crash_nostack(AS_CFG, "external address '%s' does not match service addresses '%s'",
					g_config.external_address,
					service_str ? service_str : "null");
		}

		cf_dyn_buf_free(&temp_service_db);
		cf_free(service_str);
	}
}


//==========================================================
// Item-specific parsing utilities.
//

void
cfg_add_mesh_seed_addr_port(char* addr, int port)
{
	int i;

	for (i = 0; i < AS_CLUSTER_SZ; i++) {
		if (g_config.hb_mesh_seed_addrs[i] == NULL) {
			g_config.hb_mesh_seed_addrs[i] = addr;
			g_config.hb_mesh_seed_ports[i] = port;
			break;
		}
	}

	if (i == AS_CLUSTER_SZ) {
		cf_crash_nostack(AS_CFG, "can't configure more than %d mesh-seed-address-port entries", AS_CLUSTER_SZ);
	}
}

as_set*
cfg_add_set(as_namespace* ns)
{
	if (ns->sets_cfg_count >= AS_SET_MAX_COUNT) {
		cf_crash_nostack(AS_CFG, "namespace %s - too many sets", ns->name);
	}

	// Lazily allocate temporary sets config array.
	if (! ns->sets_cfg_array) {
		size_t array_size = AS_SET_MAX_COUNT * sizeof(as_set);

		ns->sets_cfg_array = (as_set*)cf_malloc(array_size);
		memset(ns->sets_cfg_array, 0, array_size);
	}

	return &ns->sets_cfg_array[ns->sets_cfg_count++];
}

void
cfg_add_storage_file(as_namespace* ns, char* file_name)
{
	int i;

	for (i = 0; i < AS_STORAGE_MAX_FILES; i++) {
		if (! ns->storage_files[i]) {
			ns->storage_files[i] = file_name;
			break;
		}
	}

	if (i == AS_STORAGE_MAX_FILES) {
		cf_crash_nostack(AS_CFG, "namespace %s - too many storage files", ns->name);
	}
}

void
cfg_add_storage_device(as_namespace* ns, char* device_name, char* shadow_name)
{
	int i;

	for (i = 0; i < AS_STORAGE_MAX_DEVICES; i++) {
		if (! ns->storage_devices[i]) {
			ns->storage_devices[i] = device_name;
			ns->storage_shadows[i] = shadow_name;
			break;
		}
	}

	if (i == AS_STORAGE_MAX_DEVICES) {
		cf_crash_nostack(AS_CFG, "namespace %s - too many storage devices", ns->name);
	}
}

void
cfg_init_si_var(as_namespace* ns)
{
	if (! ns->sindex_cfg_var_hash) {
		if (SHASH_OK != shash_create(&ns->sindex_cfg_var_hash,
							as_sindex_config_var_hash_fn, AS_ID_INAME_SZ, sizeof(as_sindex_config_var),
							AS_SINDEX_MAX, 0)) {
			cf_crash_nostack(AS_CFG, "namespace %s couldn't create sindex cfg item hash", ns->name);
		}
	}
	else if (shash_get_size(ns->sindex_cfg_var_hash) >= AS_SINDEX_MAX) {
		cf_crash_nostack(AS_CFG, "namespace %s - too many secondary indexes", ns->name);
	}
}

uint32_t
cfg_obj_size_hist_max(uint32_t hist_max)
{
	uint32_t round_to = OBJ_SIZE_HIST_NUM_BUCKETS;
	uint32_t round_max = hist_max != 0 ?
			((hist_max + round_to - 1) / round_to) * round_to : round_to;

	if (round_max != hist_max) {
		cf_info(AS_CFG, "rounding obj-size-hist-max %u up to %u", hist_max, round_max);
	}

	return round_max; // in 128-byte blocks
}


//==========================================================
// Other (non-item-specific) utilities.
//

static void
create_and_check_hist_track(cf_hist_track** h, const char* name,
		histogram_scale scale)
{
	if (NULL == (*h = cf_hist_track_create(name, scale))) {
		cf_crash(AS_AS, "couldn't create histogram: %s", name);
	}

	as_config* c = &g_config;

	if (c->hist_track_back != 0 &&
			! cf_hist_track_start(*h, c->hist_track_back, c->hist_track_slice, c->hist_track_thresholds)) {
		cf_crash_nostack(AS_AS, "couldn't enable histogram tracking: %s", name);
	}
}

static void
create_and_check_hist(histogram** h, const char* name, histogram_scale scale)
{
	if (NULL == (*h = histogram_create(name, scale))) {
		cf_crash(AS_AS, "couldn't create histogram: %s", name);
	}
}

void
cfg_create_all_histograms()
{
	as_config* c = &g_config;

	create_and_check_hist_track(&c->rt_hist, "reads", HIST_MILLISECONDS);
	create_and_check_hist_track(&c->q_hist, "query", HIST_MILLISECONDS);
	create_and_check_hist_track(&c->q_rcnt_hist, "query_rec_count", HIST_RAW);
	create_and_check_hist_track(&c->ut_hist, "udf", HIST_MILLISECONDS);
	create_and_check_hist_track(&c->wt_hist, "writes_master", HIST_MILLISECONDS);
	create_and_check_hist_track(&c->px_hist, "proxy", HIST_MILLISECONDS);
	create_and_check_hist_track(&c->wt_reply_hist, "writes_reply", HIST_MILLISECONDS);

	create_and_check_hist(&c->rt_cleanup_hist, "reads_cleanup", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_net_hist, "reads_net", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_net_hist, "writes_net", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_storage_read_hist, "reads_storage_read", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_storage_open_hist, "reads_storage_open", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_tree_hist, "reads_tree", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_internal_hist, "reads_internal", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_internal_hist, "writes_internal", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_start_hist, "reads_start", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_start_hist, "writes_start", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_q_process_hist, "reads_q_process", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_q_process_hist, "writes_q_process", HIST_MILLISECONDS);
	create_and_check_hist(&c->q_wait_hist, "q_wait", HIST_MILLISECONDS);
	create_and_check_hist(&c->demarshal_hist, "demarshal_hist", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_master_wait_prole_hist, "wt_master_wait_prole", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_prole_hist, "writes_prole", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_resolve_hist, "reads_resolve", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_resolve_hist, "writes_resolve", HIST_MILLISECONDS);
	create_and_check_hist(&c->rt_resolve_wait_hist, "reads_resolve_wait", HIST_MILLISECONDS);
	create_and_check_hist(&c->wt_resolve_wait_hist, "writes_resolve_wait", HIST_MILLISECONDS);
	create_and_check_hist(&c->error_hist, "error", HIST_MILLISECONDS);
	create_and_check_hist(&c->batch_index_reads_hist, "batch_index_reads", HIST_MILLISECONDS);
	create_and_check_hist(&c->batch_q_process_hist, "batch_q_process", HIST_MILLISECONDS);
	create_and_check_hist(&c->info_tr_q_process_hist, "info_tr_q_process", HIST_MILLISECONDS);
	create_and_check_hist(&c->info_q_wait_hist, "info_q_wait", HIST_MILLISECONDS);
	create_and_check_hist(&c->info_post_lock_hist, "info_post_lock", HIST_MILLISECONDS);
	create_and_check_hist(&c->info_fulfill_hist, "info_fulfill", HIST_MILLISECONDS);
	create_and_check_hist(&c->write_storage_close_hist, "write_storage_close", HIST_MILLISECONDS);
	create_and_check_hist(&c->write_sindex_hist, "write_sindex", HIST_MILLISECONDS);
	create_and_check_hist(&c->defrag_storage_close_hist, "defrag_storage_close", HIST_MILLISECONDS);
	create_and_check_hist(&c->prole_fabric_send_hist, "prole_fabric_send", HIST_MILLISECONDS);

	create_and_check_hist(&c->ldt_multiop_prole_hist, "ldt_multiop_prole", HIST_MILLISECONDS);
	create_and_check_hist(&c->ldt_io_record_cnt_hist, "ldt_rec_io_count", HIST_RAW);
	create_and_check_hist(&c->ldt_update_record_cnt_hist, "ldt_rec_update_count", HIST_RAW);
	create_and_check_hist(&c->ldt_update_io_bytes_hist, "ldt_rec_update_bytes", HIST_RAW);
	create_and_check_hist(&c->ldt_hist, "ldt", HIST_MILLISECONDS);

#ifdef HISTOGRAM_OBJECT_LATENCY
	create_and_check_hist(&c->read0_hist, "read_0bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read1_hist, "read_1bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read2_hist, "read_2bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read3_hist, "read_3bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read4_hist, "read_4bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read5_hist, "read_5bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read6_hist, "read_6bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read7_hist, "read_7bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read8_hist, "read_8bucket", HIST_MILLISECONDS);
	create_and_check_hist(&c->read9_hist, "read_9bucket", HIST_MILLISECONDS);
#endif
}

/**
 * cfg_reset_self_node:
 * If we're in "Topology Mode", then we repurpose the self-node value from
 * "PORT + MAC ADDRESS" to our altered state:
 *
 * Rebuild the self node value as follows:
 * Top 16 bits: Port Number
 * Next 16 bits: Group ID
 * Bottom 32 bits: Node ID
 */
int
cfg_reset_self_node(as_config * config_p) {
	cf_node self_node = config_p->self_node;

	// Take the existing Self Node, pull out the Port Number, then rebuild as
	// PORT + GROUP ID + NODE ID (16 bits::16 bits::32 bits)
	cf_debug(AS_CFG,"[ENTER] set self Node:: group(%u) Node (%u)\n",
		config_p->cluster.cl_self_group, config_p->cluster.cl_self_node);

	if (AS_PAXOS_PROTOCOL_V4 != config_p->paxos_protocol) {
		cf_crash_nostack(AS_CFG, "must use Paxos protocol V4 with Rack Aware enabled");
	}

	cc_node_t node_id = config_p->cluster.cl_self_node;
	cc_group_t group_id = config_p->cluster.cl_self_group;
	uint16_t port_num = cc_compute_port(self_node);

	// If cluster mode is DYNAMIC, then construct self-node-id from the
	// service IP address.
	if (config_p->cluster_mode == CL_MODE_DYNAMIC) {
		cf_info(AS_CFG, "Cluster Mode Dynamic: Config IP address for Self Node");
		int a, b, c, d;
		if (4 != sscanf(config_p->node_ip, "%d.%d.%d.%d", &a, &b, &c, &d)) {
			cf_crash_nostack(AS_CFG, "could not extract 4 octets from node IP address \"%s\" for node ID", config_p->node_ip);
		}
		else {
			node_id = ((((((a << 8) | b) << 8) | c) << 8) | d);
			cf_info(AS_CFG, "Setting node ID to %u (0x%08X) from IP address \"%s\"", node_id, node_id, config_p->node_ip);
		}
	}
	else if (config_p->cluster_mode == CL_MODE_STATIC) {
		cf_info(AS_CFG, "Cluster Mode Static: Config self-node-id (%"PRIx64") for Self Node", node_id);
	}

	cf_node new_self = cc_compute_self_node(port_num, group_id, node_id);

	config_p->self_node = new_self;

	return 0;
} // end cfg_reset_self_node()

/**
 * cfg_use_hardware_values
 * Some configuration information -- such as the number of processors, amount of
 * memory, hardware addresses, etc. -- should be read from hardware sources
 * rather than specified via configuration parameters
 */
void
cfg_use_hardware_values(as_config* c)
{
	// Use this array if interface name is configured in config file.
	const char *config_interface_names[] = { 0, 0 };

	if (c->self_node == 0) {
		const char **interface_names = NULL;
		if (c->network_interface_name) {
			// Use network interface name provided in the configuration.
			config_interface_names[0] = c->network_interface_name;
			interface_names = config_interface_names;
		}
		if (0 > (cf_nodeid_get(c->fabric_port, &(c->self_node), &(c->node_ip), c->hb_mode, &(c->hb_addr), interface_names))) {
			cf_crash_nostack(AS_CFG, "could not get unique id and/or ip address");
		}
	}
}
