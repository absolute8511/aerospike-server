/*
 * thr_info.c
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

#include "base/thr_info.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <malloc.h>
#include <mcheck.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <arpa/inet.h>
#include <time.h>
#include <unistd.h>

#include "citrusleaf/cf_shash.h"
#include "citrusleaf/cf_vector.h"

#include "xdr_config.h"

#include "cf_str.h"
#include "dynbuf.h"
#include "jem.h"
#include "meminfo.h"
#include "queue.h"

#include "ai_obj.h"
#include "ai_btree.h"

#include "base/asm.h"
#include "base/datamodel.h"
#include "base/thr_batch.h"
#include "base/thr_proxy.h"
#include "base/thr_tsvc.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "base/xdr_serverside.h"
#include "base/secondary_index.h"
#include "base/security.h"
#include "base/system_metadata.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/paxos.h"
#include "fabric/migrate.h"
#include "base/udf_cask.h"
#include "base/thr_scan.h"
#include "base/monitor.h"
#include "base/thr_sindex.h"
#include "base/ldt.h"

#define STR_NS              "ns"
#define STR_SET             "set"
#define STR_INDEXNAME       "indexname"
#define STR_NUMBIN          "numbins"
#define STR_INDEXDATA       "indexdata"
#define STR_TYPE_NUMERIC    "numeric"
#define STR_TYPE_STRING     "string"
#define STR_ITYPE           "indextype"
#define STR_ITYPE_DEFAULT   "DEFAULT"
#define STR_ITYPE_LIST      "LIST"
#define STR_ITYPE_MAPKEYS   "MAPKEYS"
#define STR_ITYPE_MAPVALUES "MAPVALUES"
#define STR_BINTYPE         "bintype"

extern int as_nsup_queue_get_size();

// Use the following macro to enforce locking around Info requests at run-time.
// (Warning:  This will unnecessarily increase contention and Info request timeouts!)
// #define USE_INFO_LOCK

typedef int (*as_info_get_tree_fn) (char *name, char *subtree, cf_dyn_buf *db);
typedef int (*as_info_get_value_fn) (char *name, cf_dyn_buf *db);
typedef int (*as_info_command_fn) (char *name, char *parameters, cf_dyn_buf *db);

void as_query_set_job_tracking(bool);

// Sets a static value - set to 0 to remove a previous value.
int as_info_set_buf(const char *name, const uint8_t *value, size_t value_sz, bool def);
int as_info_set(const char *name, const char *value, bool def);

// For dynamic items - you will get called when the name is requested. The
// dynbuf will be fully set up for you - just add the information you want to
// return.
int as_info_set_dynamic(char *name, as_info_get_value_fn gv_fn, bool def);

// For tree items - you will get called when the name is requested, and it will
// have the name you registered (name) and the subtree portion (value). The
// dynbuf will be fully set up for you - just add the information you want to
// return
int as_info_set_tree(char *name, as_info_get_tree_fn gv_fn);

// For commands - you will be called with the parameters.
int as_info_set_command(char *name, as_info_command_fn command_fn, as_sec_perm required_perm);

// Acceptable timediffs in XDR lastship times.
// (Print warning only if time went back by at least 5 minutes.)
#define XDR_ACCEPTABLE_TIMEDIFF XDR_TIME_ADJUST


static int as_info_queue_get_size(void);
int as_info_parameter_get(char *param_str, char *param, char *value, int *value_len);
int info_get_objects(char *name, cf_dyn_buf *db);
void clear_microbenchmark_histograms();
void clear_ldt_histograms();
int info_get_tree_sets(char *name, char *subtree, cf_dyn_buf *db);
int info_get_tree_bins(char *name, char *subtree, cf_dyn_buf *db);
int info_get_tree_sindexes(char *name, char *subtree, cf_dyn_buf *db);
int as_tscan_get_pending_job_count();
void as_storage_show_wblock_stats(as_namespace *ns);
void as_storage_summarize_wblock_stats(as_namespace *ns);
int as_storage_analyze_wblock(as_namespace* ns, int device_index, uint32_t wblock_id);

static cf_queue *g_info_work_q = 0;

typedef struct {
	as_transaction tr;
} info_work;

//
// Info has its own fabric service
// which allows it to communicate things like the IP addresses of
// all the other nodes
//

#define INFO_FIELD_OP	0
#define INFO_FIELD_GENERATION 1
#define INFO_FIELD_SERVICE_ADDRESS 2

#define INFO_OP_UPDATE 0
#define INFO_OP_ACK 1

msg_template info_mt[] = {
	{ INFO_FIELD_OP,	M_FT_UINT32 },
	{ INFO_FIELD_GENERATION, M_FT_UINT32 },
	{ INFO_FIELD_SERVICE_ADDRESS, M_FT_STR }
};

// Is dumping GLibC-level memory stats enabled?
static bool g_mstats_enabled = false;

// Is GLibC-level memory tracing enabled?
static bool g_mtrace_enabled = false;

// Default location for the memory tracing output:
#define DEFAULT_MTRACE_FILENAME  "/tmp/mtrace.out"

//
// The dynamic list has a name, and a function to call
//

typedef struct info_static_s {
	struct info_static_s	*next;
	bool   def; // default, but default is a reserved word
	char *name;
	char *value;
	size_t	value_sz;
} info_static;


typedef struct info_dynamic_s {
	struct info_dynamic_s *next;
	bool 	def;  // default, but that's a reserved word
	char *name;
	as_info_get_value_fn	value_fn;
} info_dynamic;

typedef struct info_command_s {
	struct info_command_s *next;
	char *name;
	as_info_command_fn 		command_fn;
	as_sec_perm				required_perm; // required security permission
} info_command;

typedef struct info_tree_s {
	struct info_tree_s *next;
	char *name;
	as_info_get_tree_fn	tree_fn;
} info_tree;


#define EOL		'\n' // incoming commands are separated by EOL
#define SEP		'\t'
#define TREE_SEP		'/'

#define INFO_COMMAND_SINDEX_FAILCODE(num, message)	\
	if (db) { \
		cf_dyn_buf_append_string(db, "FAIL:");			\
		cf_dyn_buf_append_int(db, num); 				\
		cf_dyn_buf_append_string(db, ":");				\
		cf_dyn_buf_append_string(db, message);          \
	}


//
// This call is expensive, so put a cache in front of it
//

static as_partition_states g_ps_cache;
pthread_mutex_t			g_ps_cache_LOCK = PTHREAD_MUTEX_INITIALIZER;
uint64_t				g_ps_cache_lastms = 0;

void
info_partition_getstates(as_partition_states *ps)
{
	pthread_mutex_lock(&g_ps_cache_LOCK);

	uint64_t	now = cf_getms();

	if (g_ps_cache_lastms + 1000 < now) {
		as_partition_getstates(&g_ps_cache);
		g_ps_cache_lastms = now;
	}

	*ps = g_ps_cache;

	pthread_mutex_unlock(&g_ps_cache_LOCK);
	return;
}

//
// DYNAMIC FUNCTIONS
// These functions are internal bits that allow us to gather some basic
// statistics
//

#if SIZEOF_ATOMIC_INT == 4
#define APPEND_STAT_COUNTER(__db, __stat)  cf_dyn_buf_append_uint32(__db, __stat)
#elif SIZEOF_ATOMIC_INT == 8
#define APPEND_STAT_COUNTER(__db, __stat)  cf_dyn_buf_append_uint64(__db, __stat)
#else
#define APPEND_STAT_COUNTER(__db, __stat)  STATS_MUST_BE_4_OR_8_BYTES
#endif


void info_append_uint64(char *name, char *stats_name, uint64_t value, cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, ";");
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_string(db, stats_name);
	cf_dyn_buf_append_string(db, "=");
	cf_dyn_buf_append_uint64(db, value);
}

int
info_get_utilization(cf_dyn_buf *db)
{
	uint64_t	total_number_objects    = 0;
	uint64_t	total_number_objects_sub= 0;
	uint64_t	used_disk_size          = 0;
	uint64_t	total_disk_size         = 0;
	uint64_t	total_memory_size       = 0;
	uint64_t    disk_free_pct           = 0;
	uint64_t    mem_free_pct            = 0;
	uint64_t    used_memory_size        = 0;
	uint64_t    used_data_memory        = 0;
	uint64_t    used_pindex_memory      = 0;
	uint64_t    used_sindex_memory      = 0;

	for (uint i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		total_number_objects    += ns->n_objects;
		total_number_objects_sub += ns->n_sub_objects;
		total_disk_size         += ns->ssd_size;
		total_memory_size       += ns->memory_size;
		used_data_memory        += ns->n_bytes_memory;
		used_pindex_memory      += as_index_size_get(ns) * (ns->n_objects + ns->n_sub_objects);
		used_sindex_memory      += cf_atomic_int_get(ns->sindex_data_memory_used);

		uint64_t inuse_disk_bytes = 0;
		as_storage_stats(ns, 0, &inuse_disk_bytes);
		used_disk_size          += inuse_disk_bytes;
	}
	// total used memory = memory used by (data + primary index + secondary index)
	used_memory_size = used_data_memory + used_pindex_memory + used_sindex_memory;
	disk_free_pct    = (total_disk_size && (total_disk_size > used_disk_size))
					   ? (((total_disk_size - used_disk_size) * 100L) / total_disk_size)
					   : 0;
	mem_free_pct     =  (total_memory_size && (total_memory_size > used_memory_size))
						? (((total_memory_size - used_memory_size) * 100L) / total_memory_size)
						: 0;


	info_append_uint64("", "objects",                  total_number_objects, db);
	info_append_uint64("", "sub-records",              total_number_objects_sub, db);
	info_append_uint64("", "total-bytes-disk",         total_disk_size,      db);
	info_append_uint64("", "used-bytes-disk",          used_disk_size,       db);
	info_append_uint64("", "free-pct-disk",            disk_free_pct,        db);
	info_append_uint64("", "total-bytes-memory",       total_memory_size,    db);
	info_append_uint64("", "used-bytes-memory",        used_memory_size,     db);
	info_append_uint64("", "data-used-bytes-memory",   used_data_memory,     db);
	info_append_uint64("", "index-used-bytes-memory",  used_pindex_memory,   db);
	info_append_uint64("", "sindex-used-bytes-memory", used_sindex_memory,   db);
	info_append_uint64("", "free-pct-memory",          mem_free_pct,         db);
	return(0);
}

// #define INFO_SEGV_TEST 1
#ifdef INFO_SEGV_TEST
char *segv_test = "segv test";
int
info_segv_test(char *name, cf_dyn_buf *db)
{
	*segv_test = 'E';
	cf_dyn_buf_append_string(db, "segv");
	return(0);
}
#endif

int
info_get_stats(char *name, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "info_get_stats");

	cf_dyn_buf_append_string(db, "cluster_size=");
	cf_dyn_buf_append_int(db, g_config.paxos->cluster_size);
	cf_dyn_buf_append_string(db, ";cluster_key=");
	cf_dyn_buf_append_uint64_x(db, as_paxos_get_cluster_key());
	cf_dyn_buf_append_string(db, ";cluster_integrity=");
	cf_dyn_buf_append_string(db, (as_paxos_get_cluster_integrity(g_config.paxos) ? "true" : "false"));

	info_get_utilization(db);

	cf_dyn_buf_append_string(db, ";stat_read_reqs=");
	APPEND_STAT_COUNTER(db, g_config.stat_read_reqs);
	cf_dyn_buf_append_string(db, ";stat_read_reqs_xdr=");
	APPEND_STAT_COUNTER(db, g_config.stat_read_reqs_xdr);
	cf_dyn_buf_append_string(db, ";stat_read_success=");
	APPEND_STAT_COUNTER(db, g_config.stat_read_success);
	cf_dyn_buf_append_string(db, ";stat_read_errs_notfound=");
	APPEND_STAT_COUNTER(db, g_config.stat_read_errs_notfound);
	cf_dyn_buf_append_string(db, ";stat_read_errs_other=");
	APPEND_STAT_COUNTER(db, g_config.stat_read_errs_other);


	cf_dyn_buf_append_string(db, ";stat_write_reqs=");
	APPEND_STAT_COUNTER(db, g_config.stat_write_reqs);
	cf_dyn_buf_append_string(db, ";stat_write_reqs_xdr=");
	APPEND_STAT_COUNTER(db, g_config.stat_write_reqs_xdr);
	cf_dyn_buf_append_string(db, ";stat_write_success=");
	APPEND_STAT_COUNTER(db, g_config.stat_write_success);
	cf_dyn_buf_append_string(db, ";stat_write_errs=");
	APPEND_STAT_COUNTER(db, g_config.stat_write_errs);
	cf_dyn_buf_append_string(db, ";stat_xdr_pipe_writes=");
	APPEND_STAT_COUNTER(db, g_config.stat_xdr_pipe_writes);
	cf_dyn_buf_append_string(db, ";stat_xdr_pipe_miss=");
	APPEND_STAT_COUNTER(db, g_config.stat_xdr_pipe_miss);

	cf_dyn_buf_append_string(db, ";stat_delete_success=");
	APPEND_STAT_COUNTER(db, g_config.stat_delete_success);
	cf_dyn_buf_append_string(db, ";stat_rw_timeout=");
	APPEND_STAT_COUNTER(db, g_config.stat_rw_timeout);

	cf_dyn_buf_append_string(db, ";udf_read_reqs=");
	APPEND_STAT_COUNTER(db, g_config.udf_read_reqs);
	cf_dyn_buf_append_string(db, ";udf_read_success=");
	APPEND_STAT_COUNTER(db, g_config.udf_read_success);
	cf_dyn_buf_append_string(db, ";udf_read_errs_other=");
	APPEND_STAT_COUNTER(db, g_config.udf_read_errs_other);

	cf_dyn_buf_append_string(db, ";udf_write_reqs=");
	APPEND_STAT_COUNTER(db, g_config.udf_write_reqs);
	cf_dyn_buf_append_string(db, ";udf_write_success=");
	APPEND_STAT_COUNTER(db, g_config.udf_write_success);
	cf_dyn_buf_append_string(db, ";udf_write_err_others=");
	APPEND_STAT_COUNTER(db, g_config.udf_write_errs_other);

	cf_dyn_buf_append_string(db, ";udf_delete_reqs=");
	APPEND_STAT_COUNTER(db, g_config.udf_delete_reqs);
	cf_dyn_buf_append_string(db, ";udf_delete_success=");
	APPEND_STAT_COUNTER(db, g_config.udf_delete_success);
	cf_dyn_buf_append_string(db, ";udf_delete_err_others=");
	APPEND_STAT_COUNTER(db, g_config.udf_delete_errs_other);

	cf_dyn_buf_append_string(db, ";udf_lua_errs=");
	APPEND_STAT_COUNTER(db, g_config.udf_lua_errs);

	cf_dyn_buf_append_string(db, ";udf_scan_rec_reqs=");
	APPEND_STAT_COUNTER(db, g_config.udf_scan_rec_reqs);

	cf_dyn_buf_append_string(db, ";udf_query_rec_reqs=");
	APPEND_STAT_COUNTER(db, g_config.udf_scan_rec_reqs);

	cf_dyn_buf_append_string(db, ";udf_replica_writes=");
	APPEND_STAT_COUNTER(db, g_config.udf_replica_writes);

	cf_dyn_buf_append_string(db, ";stat_proxy_reqs=");
	APPEND_STAT_COUNTER(db, g_config.stat_proxy_reqs);
	cf_dyn_buf_append_string(db, ";stat_proxy_reqs_xdr=");
	APPEND_STAT_COUNTER(db, g_config.stat_proxy_reqs_xdr);
	cf_dyn_buf_append_string(db, ";stat_proxy_success=");
	APPEND_STAT_COUNTER(db, g_config.stat_proxy_success);
	cf_dyn_buf_append_string(db, ";stat_proxy_errs=");
	APPEND_STAT_COUNTER(db, g_config.stat_proxy_errs);
	cf_dyn_buf_append_string(db, ";stat_ldt_proxy=");
	APPEND_STAT_COUNTER(db, g_config.ldt_proxy_initiate);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_trans_to_proxy_retry=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_trans_to_proxy_retry);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_transaction_reenqueue=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_transaction_reenqueue);

	cf_dyn_buf_append_string(db,   ";stat_slow_trans_queue_push=");
	APPEND_STAT_COUNTER(db, g_config.stat_slow_trans_queue_push);

	cf_dyn_buf_append_string(db,   ";stat_slow_trans_queue_pop=");
	APPEND_STAT_COUNTER(db, g_config.stat_slow_trans_queue_pop);

	cf_dyn_buf_append_string(db,   ";stat_slow_trans_queue_batch_pop=");
	APPEND_STAT_COUNTER(db, g_config.stat_slow_trans_queue_batch_pop);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_regular_processed=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_regular_processed);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_prole_retry=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_prole_retry);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_err_ack_dup_trans_reenqueue=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_err_ack_dup_trans_reenqueue);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_partition_transaction_queue_count=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_partition_transaction_queue_count);

	cf_dyn_buf_append_string(db,   ";stat_cluster_key_err_ack_rw_trans_reenqueue=");
	APPEND_STAT_COUNTER(db, g_config.stat_cluster_key_err_ack_rw_trans_reenqueue);

	cf_dyn_buf_append_string(db, ";stat_expired_objects=");
	APPEND_STAT_COUNTER(db, g_config.stat_expired_objects);
	cf_dyn_buf_append_string(db, ";stat_evicted_objects=");
	APPEND_STAT_COUNTER(db, g_config.stat_evicted_objects);
	cf_dyn_buf_append_string(db, ";stat_deleted_set_objects=");
	APPEND_STAT_COUNTER(db, g_config.stat_deleted_set_objects);
	cf_dyn_buf_append_string(db, ";stat_evicted_set_objects=");
	APPEND_STAT_COUNTER(db, g_config.stat_evicted_set_objects);
	cf_dyn_buf_append_string(db, ";stat_evicted_objects_time=");
	APPEND_STAT_COUNTER(db, g_config.stat_evicted_objects_time);
	cf_dyn_buf_append_string(db, ";stat_zero_bin_records=");
	APPEND_STAT_COUNTER(db, g_config.stat_zero_bin_records);
	cf_dyn_buf_append_string(db, ";stat_nsup_deletes_not_shipped=");
	APPEND_STAT_COUNTER(db, g_config.stat_nsup_deletes_not_shipped);

	cf_dyn_buf_append_string(db, ";err_tsvc_requests=");
	APPEND_STAT_COUNTER(db, g_config.err_tsvc_requests);
	cf_dyn_buf_append_string(db, ";err_out_of_space=");
	APPEND_STAT_COUNTER(db, g_config.err_out_of_space);
	cf_dyn_buf_append_string(db, ";err_duplicate_proxy_request=");
	APPEND_STAT_COUNTER(db, g_config.err_duplicate_proxy_request);
	cf_dyn_buf_append_string(db, ";err_rw_request_not_found=");
	APPEND_STAT_COUNTER(db, g_config.err_rw_request_not_found);
	cf_dyn_buf_append_string(db, ";err_rw_pending_limit=");
	APPEND_STAT_COUNTER(db, g_config.err_rw_pending_limit);
	cf_dyn_buf_append_string(db, ";err_rw_cant_put_unique=");
	APPEND_STAT_COUNTER(db, g_config.err_rw_cant_put_unique);

	cf_dyn_buf_append_string(db, ";fabric_msgs_sent=");
	APPEND_STAT_COUNTER(db, g_config.fabric_msgs_sent);

	cf_dyn_buf_append_string(db, ";fabric_msgs_rcvd=");
	APPEND_STAT_COUNTER(db, g_config.fabric_msgs_rcvd);

	cf_dyn_buf_append_string(db, ";paxos_principal=");
	char paxos_principal[19];
	snprintf(paxos_principal, 19, "%"PRIX64"", as_paxos_succession_getprincipal());
	cf_dyn_buf_append_string(db, paxos_principal);

	cf_dyn_buf_append_string(db, ";migrate_msgs_sent=");
	APPEND_STAT_COUNTER(db, g_config.migrate_msgs_sent);

	cf_dyn_buf_append_string(db, ";migrate_msgs_recv=");
	APPEND_STAT_COUNTER(db, g_config.migrate_msgs_rcvd);

	cf_dyn_buf_append_string(db, ";migrate_progress_send=");
	APPEND_STAT_COUNTER(db, g_config.migrate_progress_send);

	cf_dyn_buf_append_string(db, ";migrate_progress_recv=");
	APPEND_STAT_COUNTER(db, g_config.migrate_progress_recv);

	cf_dyn_buf_append_string(db, ";migrate_num_incoming_accepted=");
	APPEND_STAT_COUNTER(db, g_config.migrate_num_incoming_accepted);

	cf_dyn_buf_append_string(db, ";migrate_num_incoming_refused=");
	APPEND_STAT_COUNTER(db, g_config.migrate_num_incoming_refused);

	cf_dyn_buf_append_string(db, ";queue=");
	cf_dyn_buf_append_int(db, thr_tsvc_queue_get_size() );

	cf_dyn_buf_append_string(db, ";transactions=");
	APPEND_STAT_COUNTER(db, g_config.proto_transactions);

	cf_dyn_buf_append_string(db, ";reaped_fds=");
	APPEND_STAT_COUNTER(db, g_config.reaper_count);

	cf_dyn_buf_append_string(db, ";tscan_initiate=");
	APPEND_STAT_COUNTER(db, g_config.tscan_initiate);
	cf_dyn_buf_append_string(db, ";tscan_pending=");
	APPEND_STAT_COUNTER(db, as_tscan_get_pending_job_count());
	cf_dyn_buf_append_string(db, ";tscan_succeeded=");
	APPEND_STAT_COUNTER(db, g_config.tscan_succeeded);
	cf_dyn_buf_append_string(db, ";tscan_aborted=");
	APPEND_STAT_COUNTER(db, g_config.tscan_aborted);

	cf_dyn_buf_append_string(db, ";batch_initiate=");
	APPEND_STAT_COUNTER(db, g_config.batch_initiate);
	cf_dyn_buf_append_string(db, ";batch_queue=");
	cf_dyn_buf_append_int(db, as_batch_queue_size());
	cf_dyn_buf_append_string(db, ";batch_tree_count=");
	APPEND_STAT_COUNTER(db, g_config.batch_tree_count);
	cf_dyn_buf_append_string(db, ";batch_timeout=");
	APPEND_STAT_COUNTER(db, g_config.batch_timeout);
	cf_dyn_buf_append_string(db, ";batch_errors=");
	APPEND_STAT_COUNTER(db, g_config.batch_errors);

	cf_dyn_buf_append_string(db, ";info_queue=");
	cf_dyn_buf_append_int(db, as_info_queue_get_size());

	cf_dyn_buf_append_string(db, ";delete_queue=");
	APPEND_STAT_COUNTER(db, as_nsup_queue_get_size());

	cf_dyn_buf_append_string(db, ";proxy_in_progress=");
	APPEND_STAT_COUNTER(db, as_proxy_inprogress());
	cf_dyn_buf_append_string(db, ";proxy_initiate=");
	APPEND_STAT_COUNTER(db, g_config.proxy_initiate);
	cf_dyn_buf_append_string(db, ";proxy_action=");
	APPEND_STAT_COUNTER(db, g_config.proxy_action);
	cf_dyn_buf_append_string(db, ";proxy_retry=");
	APPEND_STAT_COUNTER(db, g_config.proxy_retry);
	cf_dyn_buf_append_string(db, ";proxy_retry_q_full=");
	APPEND_STAT_COUNTER(db, g_config.proxy_retry_q_full);
	cf_dyn_buf_append_string(db, ";proxy_unproxy=");
	APPEND_STAT_COUNTER(db, g_config.proxy_unproxy);
	cf_dyn_buf_append_string(db, ";proxy_retry_same_dest=");
	APPEND_STAT_COUNTER(db, g_config.proxy_retry_same_dest);
	cf_dyn_buf_append_string(db, ";proxy_retry_new_dest=");
	APPEND_STAT_COUNTER(db, g_config.proxy_retry_new_dest);

	cf_dyn_buf_append_string(db, ";write_master=");
	APPEND_STAT_COUNTER(db, g_config.write_master);
	cf_dyn_buf_append_string(db, ";write_prole=");
	APPEND_STAT_COUNTER(db, g_config.write_prole);

	cf_dyn_buf_append_string(db, ";read_dup_prole=");
	APPEND_STAT_COUNTER(db, g_config.read_dup_prole);
	cf_dyn_buf_append_string(db, ";rw_err_dup_internal=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_dup_internal);
	cf_dyn_buf_append_string(db, ";rw_err_dup_cluster_key=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_dup_cluster_key);
	cf_dyn_buf_append_string(db, ";rw_err_dup_send=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_dup_send);

	cf_dyn_buf_append_string(db, ";rw_err_write_internal=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_write_internal);
	cf_dyn_buf_append_string(db, ";rw_err_write_cluster_key=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_write_cluster_key);
	cf_dyn_buf_append_string(db, ";rw_err_write_send=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_write_send);

	cf_dyn_buf_append_string(db, ";rw_err_ack_internal=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_ack_internal);
	cf_dyn_buf_append_string(db, ";rw_err_ack_nomatch=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_ack_nomatch);
	cf_dyn_buf_append_string(db, ";rw_err_ack_badnode=");
	APPEND_STAT_COUNTER(db, g_config.rw_err_ack_badnode);

	cf_dyn_buf_append_string(db, ";client_connections=");
	cf_dyn_buf_append_int(db, (g_config.proto_connections_opened - g_config.proto_connections_closed));

	cf_dyn_buf_append_string(db, ";waiting_transactions=");
	APPEND_STAT_COUNTER(db, g_config.n_waiting_transactions);

	cf_dyn_buf_append_string(db, ";tree_count=");
	APPEND_STAT_COUNTER(db, g_config.global_tree_count);

	cf_dyn_buf_append_string(db, ";record_refs=");
	APPEND_STAT_COUNTER(db, g_config.global_record_ref_count);

	cf_dyn_buf_append_string(db, ";record_locks=");
	APPEND_STAT_COUNTER(db, g_config.global_record_lock_count);

	cf_dyn_buf_append_string(db, ";migrate_tx_objs=");
	APPEND_STAT_COUNTER(db, g_config.migrate_tx_object_count);

	cf_dyn_buf_append_string(db, ";migrate_rx_objs=");
	APPEND_STAT_COUNTER(db, g_config.migrate_rx_object_count);

	cf_dyn_buf_append_string(db, ";ongoing_write_reqs=");
	APPEND_STAT_COUNTER(db, g_config.write_req_object_count);

	cf_dyn_buf_append_string(db, ";err_storage_queue_full=");
	APPEND_STAT_COUNTER(db, g_config.err_storage_queue_full);

	as_partition_states ps;
	info_partition_getstates(&ps);
	cf_dyn_buf_append_string(db, ";partition_actual=");
	cf_dyn_buf_append_int(db, ps.sync_actual);
	cf_dyn_buf_append_string(db, ";partition_replica=");
	cf_dyn_buf_append_int(db, ps.sync_replica);
	cf_dyn_buf_append_string(db, ";partition_desync=");
	cf_dyn_buf_append_int(db, (ps.desync + ps.zombie + ps.wait));
	cf_dyn_buf_append_string(db, ";partition_absent=");
	cf_dyn_buf_append_int(db, ps.absent);
	cf_dyn_buf_append_string(db, ";partition_object_count=");
	cf_dyn_buf_append_int(db, ps.n_objects);
	cf_dyn_buf_append_string(db, ";partition_ref_count=");
	cf_dyn_buf_append_int(db, ps.n_ref_count);

	// get the system wide info
	int freepct;
	bool swapping;
	cf_meminfo(0, 0, &freepct, &swapping);
	cf_dyn_buf_append_string(db, ";system_free_mem_pct=");
	cf_dyn_buf_append_int(db, freepct);

	cf_dyn_buf_append_string(db, ";sindex_ucgarbage_found=");
	APPEND_STAT_COUNTER(db, g_config.query_false_positives);
	
	cf_dyn_buf_append_string(db, ";sindex_gc_locktimedout=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_timedout);

	cf_dyn_buf_append_string(db, ";sindex_gc_inactivity_dur=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_inactivity_dur);

	cf_dyn_buf_append_string(db, ";sindex_gc_activity_dur=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_activity_dur);

	cf_dyn_buf_append_string(db, ";sindex_gc_list_creation_time=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_list_creation_time);

	cf_dyn_buf_append_string(db, ";sindex_gc_list_deletion_time=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_list_deletion_time);
	
	cf_dyn_buf_append_string(db, ";sindex_gc_objects_validated=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_objects_validated);

	cf_dyn_buf_append_string(db, ";sindex_gc_garbage_found=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_garbage_found);

	cf_dyn_buf_append_string(db, ";sindex_gc_garbage_cleaned=");
	APPEND_STAT_COUNTER(db, g_config.sindex_gc_garbage_cleaned);

	cf_dyn_buf_append_string(db, ";system_swapping=");
	cf_dyn_buf_append_string(db, swapping ? "true" : "false");

	cf_dyn_buf_append_string(db, ";err_replica_null_node=");
	APPEND_STAT_COUNTER(db, g_config.err_replica_null_node);
	cf_dyn_buf_append_string(db, ";err_replica_non_null_node=");
	APPEND_STAT_COUNTER(db, g_config.err_replica_non_null_node);
	cf_dyn_buf_append_string(db, ";err_sync_copy_null_node=");
	APPEND_STAT_COUNTER(db, g_config.err_sync_copy_null_node);
	cf_dyn_buf_append_string(db, ";err_sync_copy_null_master=");
	APPEND_STAT_COUNTER(db, g_config.err_sync_copy_null_master);

	cf_dyn_buf_append_string(db, ";storage_defrag_corrupt_record=");
	APPEND_STAT_COUNTER(db, g_config.err_storage_defrag_corrupt_record);
	cf_dyn_buf_append_string(db, ";err_write_fail_prole_unknown=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_prole_unknown);
	cf_dyn_buf_append_string(db, ";err_write_fail_prole_generation=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_prole_generation);
	cf_dyn_buf_append_string(db, ";err_write_fail_unknown=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_unknown);
	cf_dyn_buf_append_string(db, ";err_write_fail_key_exists=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_key_exists);
	cf_dyn_buf_append_string(db, ";err_write_fail_generation=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_generation);
	cf_dyn_buf_append_string(db, ";err_write_fail_generation_xdr=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_generation_xdr);
	cf_dyn_buf_append_string(db, ";err_write_fail_bin_exists=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_bin_exists);
	cf_dyn_buf_append_string(db, ";err_write_fail_parameter=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_parameter);
	cf_dyn_buf_append_string(db, ";err_write_fail_incompatible_type=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_incompatible_type);
	cf_dyn_buf_append_string(db, ";err_write_fail_noxdr=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_noxdr);
	cf_dyn_buf_append_string(db, ";err_write_fail_prole_delete=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_prole_delete);
	cf_dyn_buf_append_string(db, ";err_write_fail_not_found=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_not_found);
	cf_dyn_buf_append_string(db, ";err_write_fail_key_mismatch=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_key_mismatch);
	cf_dyn_buf_append_string(db, ";err_write_fail_record_too_big=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_record_too_big);
	cf_dyn_buf_append_string(db, ";err_write_fail_bin_name=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_bin_name);
	cf_dyn_buf_append_string(db, ";err_write_fail_bin_not_found=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_bin_not_found);
	cf_dyn_buf_append_string(db, ";err_write_fail_forbidden=");
	APPEND_STAT_COUNTER(db, g_config.err_write_fail_forbidden);
	cf_dyn_buf_append_string(db, ";stat_duplicate_operation=");
	APPEND_STAT_COUNTER(db, g_config.stat_duplicate_operation);
	cf_dyn_buf_append_string(db, ";uptime=");
	APPEND_STAT_COUNTER(db, ((cf_getms() - g_config.start_ms) / 1000) );

	// Write errors are now split into write_errs_notfound and write_errs_other
	cf_dyn_buf_append_string(db, ";stat_write_errs_notfound=");
	APPEND_STAT_COUNTER(db, g_config.stat_write_errs_notfound);
	cf_dyn_buf_append_string(db, ";stat_write_errs_other=");
	APPEND_STAT_COUNTER(db, g_config.stat_write_errs_other);

	cf_dyn_buf_append_string(db, ";heartbeat_received_self=");
	cf_dyn_buf_append_uint64(db, g_config.heartbeat_received_self);
	cf_dyn_buf_append_string(db, ";heartbeat_received_foreign=");
	cf_dyn_buf_append_uint64(db, g_config.heartbeat_received_foreign);

	// Query stats. Aggregation + Lookups
	cf_dyn_buf_append_string(db, ";");
	as_query_stat(name, db);

	return(0);
}


cf_atomic32	 g_node_info_generation = 0;


int
info_get_cluster_generation(char *name, cf_dyn_buf *db)
{
	cf_dyn_buf_append_int(db, g_node_info_generation);

	return(0);
}

int
info_get_partition_generation(char *name, cf_dyn_buf *db)
{
	cf_dyn_buf_append_int(db, g_config.partition_generation);

	return(0);
}

int
info_get_partition_info(char *name, cf_dyn_buf *db)
{
	as_partition_getinfo_str(db);

	return(0);
}

int
info_get_replicas_read(char *name, cf_dyn_buf *db)
{
	as_partition_getreplica_read_str(db);

	return(0);
}

int
info_get_replicas_prole(char *name, cf_dyn_buf *db)
{
	as_partition_getreplica_prole_str(db);

	return(0);
}

int
info_get_replicas_write(char *name, cf_dyn_buf *db)
{
	as_partition_getreplica_write_str(db);

	return(0);
}

int
info_get_replicas_master(char *name, cf_dyn_buf *db)
{
	as_partition_getreplica_master_str(db);

	return(0);
}

int
info_get_replicas_all(char *name, cf_dyn_buf *db)
{
	as_partition_get_replicas_all_str(db);

	return(0);
}

//
// COMMANDS
//

int
info_command_dun(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "dun command received: params %s", params);

	char nodes_str[AS_CLUSTER_SZ * 17];
	int  nodes_str_len = sizeof(nodes_str);

	if (0 != as_info_parameter_get(params, "nodes", nodes_str, &nodes_str_len)) {
		cf_info(AS_INFO, "dun command: no nodes to be dunned");
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	if (0 != as_hb_set_are_nodes_dunned(nodes_str, nodes_str_len, true)) {
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	cf_info(AS_INFO, "dun command executed: params %s", params);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}

int
info_command_undun(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "undun command received: params %s", params);

	char nodes_str[AS_CLUSTER_SZ * 17];
	int  nodes_str_len = sizeof(nodes_str);

	if (0 != as_info_parameter_get(params, "nodes", nodes_str, &nodes_str_len)) {
		cf_info(AS_INFO, "undun command: no nodes to be undunned");
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	if (0 != as_hb_set_are_nodes_dunned(nodes_str, nodes_str_len, false)) {
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	cf_dyn_buf_append_string(db, "ok");
	cf_info(AS_INFO, "undun command executed: params %s", params);

	return(0);
}

int
info_command_get_sl(char *name, char *params, cf_dyn_buf *db)
{
	char *result = "error";

	/*
	 *  Get the Paxos Succession List:
	 *
	 *  Command Format:  "get-sl:"
	 */

	if (!as_paxos_get_succession_list(db)) {
		result = "ok";
	}

	cf_dyn_buf_append_string(db, result);

	return 0;
}

int
info_command_set_sl(char *name, char *params, cf_dyn_buf *db)
{
	char nodes_str[AS_CLUSTER_SZ * 17];
	int  nodes_str_len = sizeof(nodes_str);
	char *result = "error";

	/*
	 *  Set the Paxos Succession List:
	 *
	 *  Command Format:  "set-sl:nodes=<PrincipalNodeID>{,<NodeID>}*"
	 *
	 *  where <PrincipalNodeID> is to become the Paxos principal, and the <NodeID>s
	 *  are the other members of the cluster.
	 */
	nodes_str[0] = '\0';
	if (as_info_parameter_get(params, "nodes", nodes_str, &nodes_str_len)) {
		cf_info(AS_INFO, "The \"%s:\" command requires a \"nodes\" list containing at least one node ID to be the new Paxos principal", name);
		cf_dyn_buf_append_string(db, result);
		return 0;
	}

	if (!as_paxos_set_succession_list(nodes_str, nodes_str_len)) {
		result = "ok";
	}

	cf_dyn_buf_append_string(db, result);

	return 0;
}

int
info_command_snub(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "snub command received: params %s", params);

	char node_str[50];
	int  node_str_len = sizeof(node_str);

	char time_str[50];
	int  time_str_len = sizeof(time_str);
	cf_clock snub_time;

	if (0 != as_info_parameter_get(params, "node", node_str, &node_str_len)) {
		cf_info(AS_INFO, "snub command: no node to be snubbed");
		return(0);
	}

	cf_node node;
	if (0 != cf_str_atoi_u64_x(node_str, &node, 16)) {
		cf_info(AS_INFO, "snub command: not a valid format, should look like a 64-bit hex number, is %s", node_str);
		return(0);
	}

	if (0 != as_info_parameter_get(params, "time", time_str, &time_str_len)) {
		cf_info(AS_INFO, "snub command: no time, that's OK (infinite)");
		snub_time = 1000LL * 3600LL * 24LL * 365LL * 30LL; // 30 years is close to eternity
	}
	else {
		if (0 != cf_str_atoi_u64(time_str, &snub_time) ) {
			cf_info(AS_INFO, "snub command: time must be an integer, is: %s", time_str);
			return(0);
		}
	}

	as_hb_snub(node , snub_time);
	cf_info(AS_INFO, "snub command executed: params %s", params);

	return(0);
}

int
info_command_tip(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "tip command received: params %s", params);

	char host_str[50];
	int  host_str_len = sizeof(host_str);

	char port_str[50];
	int  port_str_len = sizeof(port_str);


	if (0 != as_info_parameter_get(params, "host", host_str, &host_str_len)) {
		cf_info(AS_INFO, "tip command: no host, must add a host parameter");
		return(0);
	}

	if (0 != as_info_parameter_get(params, "port", port_str, &port_str_len)) {
		cf_info(AS_INFO, "tip command: no port, must have port");
		return(0);
	}

	int port = 0;
	if (0 != cf_str_atoi(port_str, &port)) {
		cf_info(AS_INFO, "tip command: port must be an integer, is: %s", port_str);
		return(0);
	}

	as_hb_tip(host_str, port);
	cf_info(AS_INFO, "tip command executed: params %s", params);

	return(0);
}

int
info_command_tip_clear(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "tip clear command received: params %s", params);

	as_hb_tip_clear();

	cf_info(AS_INFO, "tip clear command executed: params %s", params);

	return(0);
}

int
info_command_scan_kill(char *name, char *params, cf_dyn_buf *db)
{
	char tid_str[50];
	int  tid_len = sizeof(tid_str);

	cf_debug(AS_INFO, "scan kill command received: params %s", params);

	if (0 != as_info_parameter_get(params, "tid", tid_str, &tid_len)) {
		cf_info(AS_INFO, "scan-kill, just have tid");
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	int tid = 0;
	if (0 != cf_str_atoi(tid_str, &tid) ) {
		cf_info(AS_INFO, "scan-kill, just have integer tid: have instead %s", tid_str);
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	// TODO - add missing functionality for tscan

	cf_dyn_buf_append_string(db, "ok");

	cf_info(AS_INFO, "scan kill command executed: params %s", params);

	return(0);
}

int
info_command_show_devices(char *name, char *params, cf_dyn_buf *db)
{
	char ns_str[512];
	int  ns_len = sizeof(ns_str);

	if (0 != as_info_parameter_get(params, "namespace", ns_str, &ns_len)) {
		cf_info(AS_INFO, "show-devices requires namespace parameter");
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}

	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "show-devices: namespace %s not found", ns_str);
		cf_dyn_buf_append_string(db, "error");
		return(0);
	}
	as_storage_show_wblock_stats(ns);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}

int
info_command_get_min_config(char *name, char *params, cf_dyn_buf *db)
{
	uint64_t minxdrls = xdr_min_lastshipinfo();
	cf_dyn_buf_append_uint64(db, minxdrls);
	return(0);
}

int
info_command_dump_fabric(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-fabric:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_fabric_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_hb(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-hb:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_hb_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_migrates(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-migrates:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_migrate_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_msgs(char *name, char *params, cf_dyn_buf *db)
{
	bool once = true;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-msgs:{mode=<mode>}" [the "mode" argument is optional]
	 *
	 *   where <mode> is one of:  {"on" | "off" | "once"} and defaults to "once".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "mode", param_str, &param_str_len)) {
		if (!strncmp(param_str, "on", 3)) {
			g_config.fabric_dump_msgs = true;
		} else if (!strncmp(param_str, "off", 4)) {
			g_config.fabric_dump_msgs = false;
			once = false;
		} else if (!strncmp(param_str, "once", 5)) {
			once = true;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"mode\" value must be one of {\"on\", \"off\", \"once\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	if (once) {
		as_fabric_msg_queue_dump();
	}

	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

static int
is_numeric_string(char *str)
{
	if (!*str)
		return 0;

	while (isdigit(*str))
		str++;

	return (!*str);
}

int
info_command_dump_wb(char *name, char *params, cf_dyn_buf *db)
{
	as_namespace *ns;
	int device_index, wblock_id;
	char param_str[100];
	int param_str_len;

	/*
	 *  Command Format:  "dump-wb:ns=<Namespace>;dev=<DeviceID>;id=<WBlockId>"
	 *
	 *   where <Namespace> is the name of the namespace,
	 *         <DeviceID> is the drive number (a non-negative integer), and
	 *         <WBlockID> is a non-negative integer corresponding to an active wblock.
	 */
	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "ns", param_str, &param_str_len)) {
		if (!(ns = as_namespace_get_byname(param_str))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"ns\" value must be the name of an existing namespace, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"ns=<Namespace>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "dev", param_str, &param_str_len)) {
		if (!is_numeric_string(param_str) || (0 > (device_index = atoi(param_str)))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"dev\" value must be a non-negative integer, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"dev=<DeviceID>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	param_str[0] = '\0';
	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "id", param_str, &param_str_len)) {
		if (!is_numeric_string(param_str) || (0 > (wblock_id = atoi(param_str)))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"id\" value must be a non-negative integer, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"id=<WBlockID>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	if (!as_storage_analyze_wblock(ns, device_index, (uint32_t) wblock_id))
		cf_dyn_buf_append_string(db, "ok");
	else
		cf_dyn_buf_append_string(db, "error");

	return(0);
}

int
info_command_dump_wb_summary(char *name, char *params, cf_dyn_buf *db)
{
	as_namespace *ns;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-wb-summary:ns=<Namespace>"
	 *
	 *  where <Namespace> is the name of an existing namespace.
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "ns", param_str, &param_str_len)) {
		if (!(ns = as_namespace_get_byname(param_str))) {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"ns\" value must be the name of an existing namespace, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return(0);
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an argument of the form \"ns=<Namespace>\"", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	as_storage_summarize_wblock_stats(ns);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}

int
info_command_dump_wr(char *name, char *params, cf_dyn_buf *db)
{
	as_dump_wr();
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_paxos(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-paxos:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	as_paxos_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_dump_ra(char *name, char *params, cf_dyn_buf *db)
{
	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-ra:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}
	cc_cluster_config_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");
	return(0);
}

int
info_command_alloc_info(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "alloc-info command received: params %s", params);

#ifdef MEM_COUNT
	/*
	 *  Command Format:  "alloc-info:loc=<loc>"
	 *
	 *   where <loc> is a string of the form:  <Filename>:<LineNumber>
	 */

	char param_str[100], file[100];
	int line;
	int param_str_len = sizeof(param_str);

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "loc", param_str, &param_str_len)) {
		char *colon_ptr = strchr(param_str, ':');
		if (colon_ptr) {
			*colon_ptr++ = '\0';
			strncpy(file, param_str, sizeof(file));
			line = atoi(colon_ptr);
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command \"loc\" parameter (received: \"%s\") needs to be of the form: <Filename>:<LineNumber>", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires a \"loc\" parameter of the form: <Filename>:<LineNumber>", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	char *status = "ok";
	if (mem_count_alloc_info(file, line, db)) {
		status = "error";
	}
	cf_dyn_buf_append_string(db, status);
#else
	cf_warning(AS_INFO, "memory allocation counting not compiled into build ~~ rebuild with \"MEM_COUNT=1\" to use");
	cf_dyn_buf_append_string(db, "error");
#endif

	return 0;
}

int
info_command_mem(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "mem command received: params %s", params);

#ifdef MEM_COUNT
	/*
	 *	Command Format:	 "mem:{top_n=<N>;sort_by=<opt>}"
	 *
	 *	 where <opt> is one of:
	 *      "space"         --  Net allocation size.
	 *      "time"          --  Most recently allocated.
	 *      "net_count"     --  Net number of allocation calls.
	 *      "total_count"   --  Total number of allocation calls.
	 *      "change"        --  Delta in allocation size.
	 */

	// These next values are the initial defaults for the report to be run.
	static int top_n = 10;
	static sort_field_t sort_by = CF_ALLOC_SORT_NET_SZ;

	char param_str[100];
	int param_str_len = sizeof(param_str);

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "top_n", param_str, &param_str_len)) {
		int new_top_n = atoi(param_str);
		if ((new_top_n >= 1) && (new_top_n <= 100000)) {
			top_n = new_top_n;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command \"top_n\" value (received: %d) must be >= 1 and <= 100000.", name, new_top_n);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	param_str_len = sizeof(param_str);
	if (!as_info_parameter_get(params, "sort_by", param_str, &param_str_len)) {
		if (!strcmp(param_str, "space")) {
			sort_by = CF_ALLOC_SORT_NET_SZ;
		} else if (!strcmp(param_str, "time")) {
			sort_by = CF_ALLOC_SORT_TIME_LAST_MODIFIED;
		} else if (!strcmp(param_str, "net_count")) {
			sort_by = CF_ALLOC_SORT_NET_ALLOC_COUNT;
		} else if (!strcmp(param_str, "total_count")) {
			sort_by = CF_ALLOC_SORT_TOTAL_ALLOC_COUNT;
		} else if (!strcmp(param_str, "change")) {
			sort_by = CF_ALLOC_SORT_DELTA_SZ;
		} else {
			cf_warning(AS_INFO, "Unknown \"%s:\" command \"sort_by\" option (received: \"%s\".)  Must be one of: {\"space\", \"time\", \"net_count\", \"total_count\", \"change\"}.", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	char *status = "ok";
	if (mem_count_report(sort_by, top_n, db)) {
		status = "error";
	}
	cf_dyn_buf_append_string(db, status);
#else
	cf_warning(AS_INFO, "memory allocation counting not compiled into build ~~ rebuild with \"MEM_COUNT=1\" to use");
	cf_dyn_buf_append_string(db, "error");
#endif

	return 0;
}

static void
info_log_with_datestamp(void (*log_fn)(void))
{
	char datestamp[1024];
	struct tm nowtm;
	time_t now = time(NULL);
	gmtime_r(&now, &nowtm);
	strftime(datestamp, sizeof(datestamp), "%b %d %Y %T %Z:\n", &nowtm);

	/* Output the date-stamp followed by the output of the log function. */
	fprintf(stderr, datestamp);
	log_fn();
	fprintf(stderr, "\n");
}

int
info_command_mstats(char *name, char *params, cf_dyn_buf *db)
{
	bool enable = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	cf_debug(AS_INFO, "mstats command received: params %s", params);

	/*
	 *  Command Format:  "mstats:{enable=<opt>}" [the "enable" argument is optional]
	 *
	 *   where <opt> is one of:  {"true" | "false"} and by default dumps the memory stats once.
	 */

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "enable", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 3)) {
			enable = true;
		} else if (!strncmp(param_str, "false", 4)) {
			enable = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"enable\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}

		if (g_mstats_enabled && !enable) {
			cf_info(AS_INFO, "mstats:  memory stats disabled");
			g_mstats_enabled = enable;
		} else if (!g_mstats_enabled && enable) {
			cf_info(AS_INFO, "mstats:  memory stats enabled");
			g_mstats_enabled = enable;
		}
	} else {
		// No parameter supplied -- Just do it once and don't change the enabled state.
		info_log_with_datestamp(malloc_stats);
	}

	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

int
info_command_mtrace(char *name, char *params, cf_dyn_buf *db)
{
	bool enable = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	cf_debug(AS_INFO, "mtrace command received: params %s", params);

	/*
	 *  Command Format:  "mtrace:{enable=<opt>}" [the "enable" argument is optional]
	 *
	 *   where <opt> is one of:  {"true" | "false"} and by default toggles the current state.
	 */

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "enable", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 3)) {
			enable = true;
		} else if (!strncmp(param_str, "false", 4)) {
			enable = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"enable\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		enable = !g_mtrace_enabled;
	}

	// Use the default mtrace output file if not already set in the environment.
	setenv("MALLOC_TRACE", DEFAULT_MTRACE_FILENAME, false);
	cf_debug(AS_INFO, "mtrace:  MALLOC_TRACE = \"%s\"", getenv("MALLOC_TRACE"));

	if (g_mtrace_enabled && !enable) {
		cf_info(AS_INFO, "mtrace:  memory tracing disabled");
		muntrace();
		g_mtrace_enabled = enable;
	} else if (!g_mtrace_enabled && enable) {
		cf_info(AS_INFO, "mtrace:  memory tracing enabled");
		mtrace();
		g_mtrace_enabled = enable;
	}

	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

int
info_command_jem_stats(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "jem_stats command received: params %s", params);

#ifdef USE_JEM
	/*
	 *	Command Format:	 "jem-stats:"
	 *
	 *  Logs the JEMalloc statistics to the console.
	 */
	info_log_with_datestamp(jem_log_stats);
	cf_dyn_buf_append_string(db, "ok");
#else
	cf_warning(AS_INFO, "JEMalloc interface not compiled into build ~~ rebuild with \"USE_JEM=1\" to use");
	cf_dyn_buf_append_string(db, "error");
#endif

	return 0;
}

int
info_command_double_free(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "df command received: params %s", params);

#ifdef USE_DF_DETECT
	/*
	 *  Purpose:         Do an intentional double "free()" to test Double "free()" Detection.
	 *
	 *  Command Format:  "df:"
	 *
	 *  This command operates in a 3-cycle to trigger a double "free()" condition:
	 *
	 *  - Executing this command the first time will dynamically allocate a small block of memory.
	 *
	 *  - Executing this command the second time will free the block.
	 *
	 *  - Executing it a third time will actually perform the double "free()" and should trigger
	 *       the double "free()" detector, which will log an informative warning message.
	 *
	 *  - Executing it thereafter will repeat the 3-cycle, albeit using a different pseudo-random block size.
	 *
	 *  ***Warning***:  This command is provided *only* for testing abnormal situations.
	 *                  Do not use it unless you are prepared for the potential consequences!
	 */

	static size_t block_sz = 1024, incr = 255, max = 2048;
	static char *ptr = 0;
	static int ctr = 0;

	if (!ptr) {
		cf_dyn_buf_append_string(db, "calling cf_""malloc(");
		cf_dyn_buf_append_int(db, block_sz);
		cf_dyn_buf_append_string(db, ")");
		ptr = cf_malloc(block_sz);
	} else {
		cf_dyn_buf_append_string(db, "calling cf_""free(0x");
		cf_dyn_buf_append_uint64_x(db, (uint64_t) ptr);
		cf_dyn_buf_append_string(db, ")");
		cf_free(ptr);
		if (ctr++) {
			ctr = 0;
			ptr = 0;
			block_sz = (block_sz + incr) % max;
		}
	}
#else
	cf_warning(AS_INFO, "Double \"free()\" Detection support is not compiled into build ~~ rebuild with \"USE_DF_DETECT=1\" to use");
	cf_dyn_buf_append_string(db, "error");
#endif

	return 0;
}

int
info_command_asm(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "asm command received: params %s", params);

#ifdef USE_ASM
	/*
	 *  Purpose:         Control the operation of the ASMalloc library.
	 *
	 *	Command Format:	 "asm:{enable=<opt>;<thresh>=<int>;features=<feat>;stats}"
	 *
	 *   where <opt> is one of:  {"true" | "false"},
	 *
	 *   and <thresh> is one of:
	 *
	 *      "block_size"     --  Minimum block size in bytes to trigger a mallocation alerts.
	 *      "delta_size"     --  Minimum size change in bytes between mallocation alerts per thread.
	 *      "delta_time"     --  Minimum time in seconds between mallocation alerts per thread.
	 *
	 *   and <int> is the new integer value for the given threshold (-1 means infinite.)
	 *
	 *   and <feat> is a hexadecimal value representing a bit vector of ASMalloc features to enable.
	 *
	 *   One or more of: {"enable" | <thresh> | "features"} may be supplied.
	 */

	bool enable_cmd = false, thresh_cmd = false, features_cmd = false, stats_cmd = false;

	bool enable = false;
	uint64_t value = 0;
	uint64_t features = 0;

	char param_str[100];
	int param_str_len = sizeof(param_str);

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "enable", param_str, &param_str_len)) {
		enable_cmd = true;

		if (!strncmp(param_str, "true", 3)) {
			enable = true;
		} else if (!strncmp(param_str, "false", 4)) {
			enable = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"enable\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "block_size", param_str, &param_str_len)) {
		thresh_cmd = true;

		if (!strcmp(param_str, "-1")) {
			value = UINT64_MAX;
		} else {
			cf_str_atoi_u64_x(param_str, &value, 10);
		}

		g_thresh_block_size = value;
	}

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "delta_size", param_str, &param_str_len)) {
		thresh_cmd = true;

		if (!strcmp(param_str, "-1")) {
			value = UINT64_MAX;
		} else {
			cf_str_atoi_u64_x(param_str, &value, 10);
		}

		g_thresh_delta_size = value;
	}

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "delta_time", param_str, &param_str_len)) {
		thresh_cmd = true;

		if (!strcmp(param_str, "-1")) {
			value = UINT64_MAX;
		} else {
			cf_str_atoi_u64_x(param_str, &value, 10);
		}

		g_thresh_delta_time = value;
	}

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "features", param_str, &param_str_len)) {
		features_cmd = true;
		cf_str_atoi_u64_x(param_str, &features, 16);
	}

	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "stats", param_str, &param_str_len)) {
		stats_cmd = true;
		as_asm_cmd(ASM_CMD_PRINT_STATS);
	}

	// If we made it this far, actually perform the requested action(s).

	if (enable_cmd) {
		cf_info(AS_INFO, "asm: setting enable = %s ", (enable ? "true" : "false"));

		g_config.asmalloc_enabled = g_asm_hook_enabled = enable;

		if (enable != g_asm_cb_enabled) {
			g_asm_cb_enabled = enable;
			as_asm_cmd(ASM_CMD_SET_CALLBACK, (enable ? my_cb : NULL), (enable ? g_my_cb_udata : NULL));
		}
	}

	if (thresh_cmd) {
		cf_info(AS_INFO, "asm: setting thresholds: block_size = %lu ; delta_size = %lu ; delta_time = %lu",
				g_thresh_block_size, g_thresh_delta_size, g_thresh_delta_time);
		as_asm_cmd(ASM_CMD_SET_THRESHOLDS, g_thresh_block_size, g_thresh_delta_size, g_thresh_delta_time);
	}

	if (features_cmd) {
		cf_info(AS_INFO, "asm: setting features = 0x%lx ", features);
		as_asm_cmd(ASM_CMD_SET_FEATURES, features);
	}

	if (!(enable_cmd || thresh_cmd || features_cmd || stats_cmd)) {
		cf_warning(AS_INFO, "The \"%s:\" command must contain at least one of {\"enable\", \"features\", \"block_size\", \"delta_size\", \"delta_time\", \"stats\"}, not \"%s\"", name, params);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	cf_dyn_buf_append_string(db, "ok");
#else
	cf_warning(AS_INFO, "ASMalloc support is not compiled into build ~~ rebuild with \"USE_ASM=1\" to use");
	cf_dyn_buf_append_string(db, "error");
#endif // defined(USE_ASM)

	return 0;
}

/*
 *  Print out System Metadata info.
 */
int
info_command_dump_smd(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "dump-smd command received: params %s", params);

	bool verbose = false;
	char param_str[100];
	int param_str_len = sizeof(param_str);

	/*
	 *  Command Format:  "dump-smd:{verbose=<opt>}" [the "verbose" argument is optional]
	 *
	 *  where <opt> is one of:  {"true" | "false"} and defaults to "false".
	 */
	param_str[0] = '\0';
	if (!as_info_parameter_get(params, "verbose", param_str, &param_str_len)) {
		if (!strncmp(param_str, "true", 5)) {
			verbose = true;
		} else if (!strncmp(param_str, "false", 6)) {
			verbose = false;
		} else {
			cf_warning(AS_INFO, "The \"%s:\" command argument \"verbose\" value must be one of {\"true\", \"false\"}, not \"%s\"", name, param_str);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	as_smd_dump(verbose);
	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

/*
 *  Manipulate System Metatdata.
 */
int
info_command_smd_cmd(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "smd command received: params %s", params);

	/*
	 *	Command Format:	 "smd:cmd=<cmd>;module=<string>{node=<hexadecimal string>;key=<string>;value=<hexadecimal string>}"
	 *
	 *	 where <cmd> is one of:
	 *      "create"       --  Create a new container for the given module's metadata.
	 *      "destroy"      --  Destroy the container for the given module's metadata after deleting all the metadata within it.
	 *      "set"          --  Add a new, or modify an existing, item of metadata in a given module.
	 *      "delete"       --  Delete an existing item of metadata from a given module.
	 *      "get"          --  Look up the given key in the given module's metadata.
	 *      "init"         --  (Re-)Initialize the System Metadata module.
	 *      "start"        --  Start up the System Metadata module for receiving Paxos state change events.
	 *      "shutdown"     --  Terminate the System Metadata module.
	 */

	char cmd[10], module[256], node[17], key[256], value[1024];
	int cmd_len = sizeof(cmd);
	int module_len = sizeof(module);
	int node_len = sizeof(node);
	int key_len = sizeof(key);
	int value_len = sizeof(value);
	cf_node node_id = 0;

	cmd[0] = '\0';
	if (!as_info_parameter_get(params, "cmd", cmd, &cmd_len)) {
		if (strcmp(cmd, "create") && strcmp(cmd, "destroy") && strcmp(cmd, "set") && strcmp(cmd, "delete") && strcmp(cmd, "get") &&
				strcmp(cmd, "init") && strcmp(cmd, "start") && strcmp(cmd, "shutdown")) {
			cf_warning(AS_INFO, "Unknown \"%s:\" command \"cmd\" cmdtion (received: \"%s\".)  Must be one of: {\"create\", \"destroy\", \"set\", \"delete\", \"get\", \"init\", \"start\", \"shutdown\"}.", name, cmd);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	} else {
		cf_warning(AS_INFO, "The \"%s:\" command requires an \"cmd\" parameter", name);
		cf_dyn_buf_append_string(db, "error");
		return 0;
	}

	module[0] = '\0';
	if (strcmp(cmd, "init") && strcmp(cmd, "start") && strcmp(cmd, "shutdown")) {
		if (as_info_parameter_get(params, "module", module, &module_len)) {
			cf_warning(AS_INFO, "The \"%s:\" command requires a \"module\" parameter", name);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	if (!strcmp(cmd, "get")) {
		node[0] = '\0';
		if (!as_info_parameter_get(params, "node", node, &node_len)) {
			if (cf_str_atoi_u64_x(node, &node_id, 16)) {
				cf_warning(AS_INFO, "The \"%s:\" command \"node\" parameter must be a 64-bit hex number, not \"%s\"", node);
				cf_dyn_buf_append_string(db, "error");
				return 0;
			}
		} 
	}

	if (!strcmp(cmd, "set") || !strcmp(cmd, "delete") || !strcmp(cmd, "get")) {
		key[0] = '\0';
		if (as_info_parameter_get(params, "key", key, &key_len)) {
			cf_warning(AS_INFO, "The \"%s:\" command \"%s\" requires a \"key\" parameter", name, cmd);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	if (!strcmp(cmd, "set")) {
		value[0] = '\0';
		if (as_info_parameter_get(params, "value", value, &value_len)) {
			cf_warning(AS_INFO, "The \"%s:\" command \"%s\" requires a \"value\" parameter", name, cmd);
			cf_dyn_buf_append_string(db, "error");
			return 0;
		}
	}

	as_smd_info_cmd(cmd, node_id, module, key, value);
	cf_dyn_buf_append_string(db, "ok");

	return 0;
}

int
info_command_mon_cmd(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "add-module command received: params %s", params);

	/* Command format : "jobs:[module=<string>;cmd=<command>;<parameters]"
	*                   asinfo -v 'jobs'              -> list all jobs
	*                   asinfo -v 'jobs:module=query' -> list all jobs for query module
	*                   asinfo -v 'jobs:module=query;cmd=kill-job;trid=<trid>'
	*                   asinfo -v 'jobs:module=query;cmd=set-priority;trid=<trid>;value=<val>'
	* where <module> is one of following :
	* 		- query
	* 		- scan
	*/

	char cmd[13];
	char module[21];
	char job_id[24];
	char val_str[11];
	int cmd_len       = sizeof(cmd);
	int module_len    = sizeof(module);
	int job_id_len    = sizeof(job_id);
	int val_len       = sizeof(val_str);
	uint64_t trid     = 0;
	uint32_t value    = 0;

	cmd[0]     = '\0';
	module[0]  = '\0';
	job_id[0]  = '\0';
	val_str[0] = '\0';

	// Read the parameters: module cmd trid value
	int rv = as_info_parameter_get(params, "module", module, &module_len);
	if (rv == -1) {
		as_mon_info_cmd(NULL, NULL, 0, 0, db);
		return 0;
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"module\" parameter too long (> ");
		cf_dyn_buf_append_int(db, module_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	rv = as_info_parameter_get(params, "cmd", cmd, &cmd_len);
	if (rv == -1) {
		as_mon_info_cmd(module, NULL, 0, 0, db);
		return 0;
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"cmd\" parameter too long (> ");
		cf_dyn_buf_append_int(db, cmd_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	rv = as_info_parameter_get(params, "trid", job_id, &job_id_len);
	if (rv == 0) {
		trid  = strtoull(job_id, NULL, 10);
	}
	else if (rv == -1) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":no \"trid\" parameter specified");
		return 0;
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"trid\" parameter too long (> ");
		cf_dyn_buf_append_int(db, job_id_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	rv = as_info_parameter_get(params, "value", val_str, &val_len);
	if (rv == 0) {
		value = strtoul(val_str, NULL, 10);
	}
	else if (rv == -2) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_PARAMETER);
		cf_dyn_buf_append_string(db, ":\"value\" parameter too long (> ");
		cf_dyn_buf_append_int(db, val_len-1);
		cf_dyn_buf_append_string(db, " chars)");
		return 0;
	}

	cf_info(AS_SCAN, "%s %s %lu %u", module, cmd, trid, value);
	as_mon_info_cmd(module, cmd, trid, value, db);
	return 0;
}



int
info_service_config_get(cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "transaction-queues=");
	cf_dyn_buf_append_int(db, g_config.n_transaction_queues);
	cf_dyn_buf_append_string(db, ";transaction-threads-per-queue=");
	cf_dyn_buf_append_int(db, g_config.n_transaction_threads_per_queue);
	cf_dyn_buf_append_string(db, ";transaction-duplicate-threads=");
	cf_dyn_buf_append_int(db, g_config.n_transaction_duplicate_threads);
	cf_dyn_buf_append_string(db, ";transaction-pending-limit=");
	cf_dyn_buf_append_int(db, g_config.transaction_pending_limit);
	cf_dyn_buf_append_string(db, ";migrate-threads=");
	cf_dyn_buf_append_int(db, g_config.n_migrate_threads);
	cf_dyn_buf_append_string(db, ";migrate-xmit-priority=");
	cf_dyn_buf_append_int(db, g_config.migrate_xmit_priority);
	cf_dyn_buf_append_string(db, ";migrate-xmit-sleep=");
	cf_dyn_buf_append_int(db, g_config.migrate_xmit_sleep);
	cf_dyn_buf_append_string(db, ";migrate-read-priority=");
	cf_dyn_buf_append_int(db, g_config.migrate_read_priority);
	cf_dyn_buf_append_string(db, ";migrate-read-sleep=");
	cf_dyn_buf_append_int(db, g_config.migrate_read_sleep);
	cf_dyn_buf_append_string(db, ";migrate-xmit-hwm=");
	cf_dyn_buf_append_int(db, g_config.migrate_xmit_hwm);
	cf_dyn_buf_append_string(db, ";migrate-xmit-lwm=");
	cf_dyn_buf_append_int(db, g_config.migrate_xmit_lwm);
	cf_dyn_buf_append_string(db, ";migrate-max-num-incoming=");
	cf_dyn_buf_append_int(db, g_config.migrate_max_num_incoming);
	cf_dyn_buf_append_string(db, ";migrate-rx-lifetime-ms=");
	cf_dyn_buf_append_int(db, g_config.migrate_rx_lifetime_ms);
	cf_dyn_buf_append_string(db, ";proto-fd-max=");
	cf_dyn_buf_append_int(db, g_config.n_proto_fd_max);
	cf_dyn_buf_append_string(db, ";proto-fd-idle-ms=");
	cf_dyn_buf_append_int(db, g_config.proto_fd_idle_ms);
	cf_dyn_buf_append_string(db, ";proto-slow-netio-sleep-ms=");
	cf_dyn_buf_append_int(db, g_config.proto_slow_netio_sleep_ms);
	cf_dyn_buf_append_string(db, ";transaction-retry-ms=");
	cf_dyn_buf_append_int(db, g_config.transaction_retry_ms);
	cf_dyn_buf_append_string(db, ";transaction-max-ms=");
	cf_dyn_buf_append_int(db, (int)(g_config.transaction_max_ns / 1000000));
	cf_dyn_buf_append_string(db, ";transaction-repeatable-read=");
	cf_dyn_buf_append_string(db, g_config.transaction_repeatable_read ? "true" : "false");
	cf_dyn_buf_append_string(db, ";dump-message-above-size=");
	cf_dyn_buf_append_int(db, g_config.dump_message_above_size);
	cf_dyn_buf_append_string(db, ";ticker-interval=");
	cf_dyn_buf_append_int(db, g_config.ticker_interval);
	cf_dyn_buf_append_string(db, ";microbenchmarks=");
	cf_dyn_buf_append_string(db, g_config.microbenchmarks ? "true" : "false");
	cf_dyn_buf_append_string(db, ";storage-benchmarks=");
	cf_dyn_buf_append_string(db, g_config.storage_benchmarks ? "true" : "false");
	cf_dyn_buf_append_string(db, ";ldt-benchmarks=");
	cf_dyn_buf_append_string(db, g_config.ldt_benchmarks ? "true" : "false");
	cf_dyn_buf_append_string(db, ";scan-priority=");
	cf_dyn_buf_append_int(db, g_config.scan_priority);
	cf_dyn_buf_append_string(db, ";scan-sleep=");
	cf_dyn_buf_append_int(db, g_config.scan_sleep);

	cf_dyn_buf_append_string(db, ";batch-threads=");
	cf_dyn_buf_append_int(db, g_config.n_batch_threads);
	cf_dyn_buf_append_string(db, ";batch-max-requests=");
	cf_dyn_buf_append_int(db, g_config.batch_max_requests);
	cf_dyn_buf_append_string(db, ";batch-priority=");
	cf_dyn_buf_append_int(db, g_config.batch_priority);

	cf_dyn_buf_append_string(db, ";nsup-delete-sleep=");
	cf_dyn_buf_append_int(db, g_config.nsup_delete_sleep);
	cf_dyn_buf_append_string(db, ";nsup-period=");
	cf_dyn_buf_append_int(db, g_config.nsup_period);
	cf_dyn_buf_append_string(db, ";nsup-startup-evict=");
	cf_dyn_buf_append_string(db, g_config.nsup_startup_evict ? "true" : "false");
	cf_dyn_buf_append_string(db, ";paxos-retransmit-period=");
	cf_dyn_buf_append_int(db, g_config.paxos_retransmit_period);
	cf_dyn_buf_append_string(db, ";paxos-single-replica-limit=");
	cf_dyn_buf_append_int(db, g_config.paxos_single_replica_limit);
	cf_dyn_buf_append_string(db, ";paxos-max-cluster-size=");
	cf_dyn_buf_append_int(db, g_config.paxos_max_cluster_size);
	cf_dyn_buf_append_string(db, ";paxos-protocol=");
	cf_dyn_buf_append_string(db, (AS_PAXOS_PROTOCOL_V1 == g_config.paxos_protocol ? "v1" :
								  (AS_PAXOS_PROTOCOL_V2 == g_config.paxos_protocol ? "v2" :
								   (AS_PAXOS_PROTOCOL_V3 == g_config.paxos_protocol ? "v3" :
									(AS_PAXOS_PROTOCOL_V4 == g_config.paxos_protocol ? "v4" :
									 (AS_PAXOS_PROTOCOL_NONE == g_config.paxos_protocol ? "none" : "undefined"))))));
	cf_dyn_buf_append_string(db, ";paxos-recovery-policy=");
	cf_dyn_buf_append_string(db, (AS_PAXOS_RECOVERY_POLICY_MANUAL == g_config.paxos_recovery_policy ? "manual" :
								  (AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_MASTER == g_config.paxos_recovery_policy ? "auto-dun-master" :
								   (AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_ALL == g_config.paxos_recovery_policy ? "auto-dun-all" : "undefined"))));
	cf_dyn_buf_append_string(db, ";write-duplicate-resolution-disable=");
	cf_dyn_buf_append_string(db, g_config.write_duplicate_resolution_disable ? "true" : "false");
	cf_dyn_buf_append_string(db, ";respond-client-on-master-completion=");
	cf_dyn_buf_append_string(db, g_config.respond_client_on_master_completion ? "true" : "false");
	cf_dyn_buf_append_string(db, ";replication-fire-and-forget=");
	cf_dyn_buf_append_string(db, g_config.replication_fire_and_forget ? "true" : "false");
	cf_dyn_buf_append_string(db, ";info-threads=");
	cf_dyn_buf_append_int(db, g_config.n_info_threads);
	cf_dyn_buf_append_string(db, ";allow-inline-transactions=");
	cf_dyn_buf_append_string(db, g_config.allow_inline_transactions ? "true" : "false");
	cf_dyn_buf_append_string(db, ";use-queue-per-device=");
	cf_dyn_buf_append_string(db, g_config.use_queue_per_device ? "true" : "false");
	cf_dyn_buf_append_string(db, ";snub-nodes=");
	cf_dyn_buf_append_string(db, g_config.snub_nodes ? "true" : "false");
	cf_dyn_buf_append_string(db, ";fb-health-msg-per-burst=");
	cf_dyn_buf_append_int(db, g_config.fb_health_msg_per_burst);
	cf_dyn_buf_append_string(db, ";fb-health-msg-timeout=");
	cf_dyn_buf_append_int(db, g_config.fb_health_msg_timeout);
	cf_dyn_buf_append_string(db, ";fb-health-good-pct=");
	cf_dyn_buf_append_int(db, g_config.fb_health_good_pct);
	cf_dyn_buf_append_string(db, ";fb-health-bad-pct=");
	cf_dyn_buf_append_int(db, g_config.fb_health_bad_pct);
	cf_dyn_buf_append_string(db, ";auto-dun=");
	cf_dyn_buf_append_string(db, g_config.auto_dun ? "true" : "false");
	cf_dyn_buf_append_string(db, ";auto-undun=");
	cf_dyn_buf_append_string(db, g_config.auto_undun ? "true" : "false");
	cf_dyn_buf_append_string(db, ";prole-extra-ttl=");
	cf_dyn_buf_append_int(db, g_config.prole_extra_ttl);
	cf_dyn_buf_append_string(db, ";max-msgs-per-type=");
	cf_dyn_buf_append_int(db, g_config.max_msgs_per_type);
	cf_dyn_buf_append_string(db, ";service-threads=");
	cf_dyn_buf_append_int(db, g_config.n_service_threads);
	cf_dyn_buf_append_string(db, ";fabric-workers=");
	cf_dyn_buf_append_int(db, g_config.n_fabric_workers);

	if (g_config.pidfile) {
		cf_dyn_buf_append_string(db, ";pidfile=");
		cf_dyn_buf_append_string(db, g_config.pidfile);
	}
	if (g_config.generation_disable) {
		cf_dyn_buf_append_string(db, ";generation-disable=");
		cf_dyn_buf_append_string(db, g_config.generation_disable ? "true" : "false");
	}
#ifdef MEM_COUNT
	cf_dyn_buf_append_string(db, ";memory-accounting=");
	cf_dyn_buf_append_string(db, g_config.memory_accounting ? "true" : "false");
#endif

#ifdef USE_ASM
	cf_dyn_buf_append_string(db, ";asmalloc_enabled=");
	cf_dyn_buf_append_string(db, g_config.asmalloc_enabled ? "true" : "false");
#endif

	cf_dyn_buf_append_string(db, ";udf-runtime-gmax-memory=");
	cf_dyn_buf_append_uint64(db, g_config.udf_runtime_max_gmemory);
	cf_dyn_buf_append_string(db, ";udf-runtime-max-memory=");
	cf_dyn_buf_append_uint64(db, g_config.udf_runtime_max_memory);

	cf_dyn_buf_append_string(db, ";sindex-populator-scan-priority=");
	cf_dyn_buf_append_uint64(db, g_config.sindex_populator_scan_priority);
	cf_dyn_buf_append_string(db, ";sindex-data-max-memory=");
	if (g_config.sindex_data_max_memory == ULONG_MAX) {
		cf_dyn_buf_append_uint64(db, g_config.sindex_data_max_memory);
	} else {
		cf_dyn_buf_append_string(db, "ULONG_MAX");
	}

	cf_dyn_buf_append_string(db, ";query-threads=");
	cf_dyn_buf_append_int(db, g_config.query_threads);
	cf_dyn_buf_append_string(db, ";query-worker-threads=");
	cf_dyn_buf_append_int(db, g_config.query_worker_threads);
	cf_dyn_buf_append_string(db, ";query-priority=");
	cf_dyn_buf_append_uint64(db, g_config.query_priority);
	cf_dyn_buf_append_string(db, ";query-in-transaction-thread=");
	cf_dyn_buf_append_uint64(db, g_config.query_in_transaction_thr);
	cf_dyn_buf_append_string(db, ";query-req-in-query-thread=");
	cf_dyn_buf_append_uint64(db, g_config.query_req_in_query_thread);
	cf_dyn_buf_append_string(db, ";query-req-max-inflight=");
	cf_dyn_buf_append_uint64(db, g_config.query_req_max_inflight);
	cf_dyn_buf_append_string(db, ";query-bufpool-size=");
	cf_dyn_buf_append_uint64(db, g_config.query_bufpool_size);
	cf_dyn_buf_append_string(db, ";query-batch-size=");
	cf_dyn_buf_append_uint64(db, g_config.query_bsize);
	cf_dyn_buf_append_string(db, ";query-sleep=");
	cf_dyn_buf_append_uint64(db, g_config.query_sleep);
	cf_dyn_buf_append_string(db, ";query-job-tracking=");
	cf_dyn_buf_append_string(db, (g_config.query_job_tracking) ? "true" : "false");
	cf_dyn_buf_append_string(db, ";query-short-q-max-size=");
	cf_dyn_buf_append_uint64(db, g_config.query_short_q_max_size);
	cf_dyn_buf_append_string(db, ";query-long-q-max-size=");
	cf_dyn_buf_append_uint64(db, g_config.query_long_q_max_size);
	cf_dyn_buf_append_string(db, ";query-rec-count-bound=");
	cf_dyn_buf_append_uint64(db, g_config.query_rec_count_bound);
	cf_dyn_buf_append_string(db, ";query-threshold=");
	cf_dyn_buf_append_uint64(db, g_config.query_threshold);
	cf_dyn_buf_append_string(db, ";query-untracked-time=");
	cf_dyn_buf_append_uint64(db, g_config.query_untracked_time_ns/1000); // Show it in micro seconds

	return(0);
}


int
info_namespace_config_get(char* context, cf_dyn_buf *db)
{
	as_namespace *ns = as_namespace_get_byname(context);
	if (!ns) {
		cf_dyn_buf_append_string(db, "Namespace not found");
		return -1;
	}

	cf_dyn_buf_append_string(db, "memory-size=");
	cf_dyn_buf_append_uint64(db, ns->memory_size);

	cf_dyn_buf_append_string(db, ";high-water-disk-pct=");
	cf_dyn_buf_append_int(db, ns->hwm_disk * 100);

	cf_dyn_buf_append_string(db, ";high-water-memory-pct=");
	cf_dyn_buf_append_int(db, ns->hwm_memory * 100);

	cf_dyn_buf_append_string(db, ";evict-tenths-pct=");
	cf_dyn_buf_append_uint32(db, ns->evict_tenths_pct);

	cf_dyn_buf_append_string(db, ";stop-writes-pct=");
	cf_dyn_buf_append_int(db, ns->stop_writes_pct * 100);

	cf_dyn_buf_append_string(db, ";cold-start-evict-ttl=");
	cf_dyn_buf_append_uint32(db, ns->cold_start_evict_ttl);

	cf_dyn_buf_append_string(db, ";repl-factor=");
	cf_dyn_buf_append_uint32(db, ns->replication_factor);

	cf_dyn_buf_append_string(db, ";default-ttl=");
	cf_dyn_buf_append_uint64(db, ns->default_ttl);

	cf_dyn_buf_append_string(db, ";max-ttl=");
	cf_dyn_buf_append_uint64(db, ns->max_ttl);

	cf_dyn_buf_append_string(db, ";conflict-resolution-policy=");
	if(ns->conflict_resolution_policy == AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION) {
		cf_dyn_buf_append_string(db, "generation");
	}
	else if(ns->conflict_resolution_policy == AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_TTL) {
		cf_dyn_buf_append_string(db, "ttl");
	}
	else {
		cf_dyn_buf_append_string(db, "undefined");
	}

	cf_dyn_buf_append_string(db, ";allow_versions=");
	cf_dyn_buf_append_string(db, ns->allow_versions ? "true" : "false");

	cf_dyn_buf_append_string(db, ";single-bin=");
	cf_dyn_buf_append_string(db, ns->single_bin ? "true" : "false");

	cf_dyn_buf_append_string(db, ";ldt-enabled=");
	cf_dyn_buf_append_string(db, ns->ldt_enabled ? "true" : "false");
	
	cf_dyn_buf_append_string(db, ";ldt-page-size=");
	cf_dyn_buf_append_uint64(db, ns->ldt_page_size);

	cf_dyn_buf_append_string(db, ";ldt_scan_buff_max=");
	cf_dyn_buf_append_uint64(db, ns->ldt_scan_buff_max);

	cf_dyn_buf_append_string(db, ";enable-xdr=");
	cf_dyn_buf_append_string(db, ns->enable_xdr ? "true" : "false");

	cf_dyn_buf_append_string(db, ";sets-enable-xdr=");
	cf_dyn_buf_append_string(db, ns->sets_enable_xdr ? "true" : "false");

	cf_dyn_buf_append_string(db, ";ns-forward-xdr-writes=");
	cf_dyn_buf_append_string(db, ns->ns_forward_xdr_writes ? "true" : "false");

	cf_dyn_buf_append_string(db, ";disallow-null-setname=");
	cf_dyn_buf_append_string(db, ns->disallow_null_setname ? "true" : "false");

	cf_dyn_buf_append_string(db, ";total-bytes-memory=");
	cf_dyn_buf_append_uint64(db, ns->memory_size);

	cf_dyn_buf_append_string(db, ";read-consistency-level-override=");
	cf_dyn_buf_append_string(db, NS_READ_CONSISTENCY_LEVEL_NAME());

	cf_dyn_buf_append_string(db, ";write-commit-level-override=");
	cf_dyn_buf_append_string(db, NS_WRITE_COMMIT_LEVEL_NAME());

	// if storage, lots of information about the storage
	if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {

		info_append_uint64("", "total-bytes-disk", ns->ssd_size, db);
		info_append_uint64("", "defrag-lwm-pct", ns->storage_defrag_lwm_pct, db);
		info_append_uint64("", "defrag-queue-min", ns->storage_defrag_queue_min, db);
		info_append_uint64("", "defrag-sleep", ns->storage_defrag_sleep, db);
		info_append_uint64("", "defrag-startup-minimum", ns->storage_defrag_startup_minimum, db);
		info_append_uint64("", "flush-max-ms", ns->storage_flush_max_us / 1000, db);
		info_append_uint64("", "fsync-max-sec", ns->storage_fsync_max_us / 1000000, db);
		info_append_uint64("", "write-smoothing-period", ns->storage_write_smoothing_period, db);
		info_append_uint64("", "max-write-cache", ns->storage_max_write_cache, db);
		info_append_uint64("", "min-avail-pct", ns->storage_min_avail_pct, db);
		info_append_uint64("", "post-write-queue", (uint64_t)ns->storage_post_write_queue, db);

		if (ns->storage_data_in_memory)
			cf_dyn_buf_append_string(db, ";data-in-memory=true");
		else
			cf_dyn_buf_append_string(db, ";data-in-memory=false");

		for (uint i = 0; i < AS_STORAGE_MAX_DEVICES; i++) {
			if (ns->storage_devices[i] == 0)        break;
			cf_dyn_buf_append_string(db, ";dev=");
			cf_dyn_buf_append_string(db, ns->storage_devices[i]);
		}
		for (uint i = 0; i < AS_STORAGE_MAX_FILES; i++) {
			if (ns->storage_files[i] == 0)        break;
			cf_dyn_buf_append_string(db, ";file=");
			cf_dyn_buf_append_string(db, ns->storage_files[i]);
		}
		if         (ns->storage_filesize != 0) {
			cf_dyn_buf_append_string(db, ";filesize=");
			cf_dyn_buf_append_uint64(db, ns->storage_filesize);
		}
		if (ns->storage_blocksize != 0) {
			cf_dyn_buf_append_string(db, ";blocksize=");
			cf_dyn_buf_append_uint32(db, ns->storage_blocksize);
		}
		if (ns->storage_write_threads != 0) {
			cf_dyn_buf_append_string(db, ";writethreads=");
			cf_dyn_buf_append_uint32(db, ns->storage_write_threads);
		}
		if (ns->storage_max_write_cache != 0) {
			cf_dyn_buf_append_string(db, ";writecache=");
			cf_dyn_buf_append_uint64(db, ns->storage_max_write_cache);
		}

		cf_dyn_buf_append_string(db, ";obj-size-hist-max=");
		cf_dyn_buf_append_uint32(db, ns->obj_size_hist_max);
	} // SSD

	// Info. about KV stores.
	if (ns->storage_type == AS_STORAGE_ENGINE_KV) {

		info_append_uint64("", "total-bytes-disk", ns->kv_size, db);

		for (uint i = 0; i < AS_STORAGE_MAX_DEVICES; i++) {
			if (!(ns->storage_devices[i]))
				break;
			cf_dyn_buf_append_string(db, ";dev=");
			cf_dyn_buf_append_string(db, ns->storage_devices[i]);
		}

		if (ns->storage_filesize) {
			cf_dyn_buf_append_string(db, ";filesize=");
			cf_dyn_buf_append_uint64(db, ns->storage_filesize);
		}
	} // KV

	return (0);
}
void
info_network_info_config_get(cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "service-address=");
	cf_dyn_buf_append_string(db, g_config.socket.addr);
	cf_dyn_buf_append_string(db, ";service-port=");
	cf_dyn_buf_append_int(db, g_config.socket.port);

	if (g_config.hb_mode == AS_HB_MODE_MESH) {
		if (g_config.hb_init_addr) {
			cf_dyn_buf_append_string(db, ";mesh-address=");
			cf_dyn_buf_append_string(db, g_config.hb_init_addr);
			if (g_config.hb_init_port) {
				cf_dyn_buf_append_string(db, ";mesh-port=");
				cf_dyn_buf_append_int(db, g_config.hb_init_port);
			}
		} else {
			for (int i = 0; i < AS_CLUSTER_SZ; i++) {
				if (g_config.hb_mesh_seed_addrs[i]) {
					cf_dyn_buf_append_string(db, ";mesh-seed-address-port=");
					cf_dyn_buf_append_string(db, g_config.hb_mesh_seed_addrs[i]);
					cf_dyn_buf_append_string(db, ":");
					cf_dyn_buf_append_int(db, g_config.hb_mesh_seed_ports[i]);
				} else {
					break;
				}
			}
		}
	}

	if (g_config.network_interface_name) {
		cf_dyn_buf_append_string(db, ";network-interface-name=");
		cf_dyn_buf_append_string(db, g_config.network_interface_name);
	}
	if (g_config.external_address) {
		cf_dyn_buf_append_string(db, ";access-address=");
		cf_dyn_buf_append_string(db, g_config.external_address);
	}
	cf_dyn_buf_append_string(db, ";reuse-address=");
	cf_dyn_buf_append_string(db, g_config.socket_reuse_addr ? "true" : "false");
	cf_dyn_buf_append_string(db, ";fabric-port=");
	cf_dyn_buf_append_int(db, g_config.fabric_port);

	cf_dyn_buf_append_string(db, ";fabric-keepalive-enabled=");
	cf_dyn_buf_append_string(db, g_config.fabric_keepalive_enabled ? "true" : "false");
	cf_dyn_buf_append_string(db, ";fabric-keepalive-time=");
	cf_dyn_buf_append_int(db, g_config.fabric_keepalive_time);
	cf_dyn_buf_append_string(db, ";fabric-keepalive-intvl=");
	cf_dyn_buf_append_int(db, g_config.fabric_keepalive_intvl);
	cf_dyn_buf_append_string(db, ";fabric-keepalive-probes=");
	cf_dyn_buf_append_int(db, g_config.fabric_keepalive_probes);

// network-info-port is the asd info port variable/output, This was chosen because info-port conflicts with XDR config parameter.
// Ideally XDR should use xdr-info-port and asd should use info-port.
	cf_dyn_buf_append_string(db, ";network-info-port=");
	cf_dyn_buf_append_int(db, g_config.info_port);
	cf_dyn_buf_append_string(db, ";enable-fastpath=");
	cf_dyn_buf_append_string(db, (g_config.info_fastpath_enabled ? "true" : "false"));
}

void
info_network_heartbeat_config_get(cf_dyn_buf *db)
{
	if (g_config.hb_mode == AS_HB_MODE_MCAST) {
		cf_dyn_buf_append_string(db, "heartbeat-mode=multicast");
	}
	else if (g_config.hb_mode == AS_HB_MODE_MESH) {
		cf_dyn_buf_append_string(db, "heartbeat-mode=mesh");
	}
	else {
		cf_dyn_buf_append_string(db, "heartbeat-mode=UNKNOWN");
	}

	if (g_config.hb_tx_addr) {
		cf_dyn_buf_append_string(db, ";heartbeat-interface-address=");
		cf_dyn_buf_append_string(db, g_config.hb_tx_addr);
	}

	cf_dyn_buf_append_string(db, ";heartbeat-protocol=");
	cf_dyn_buf_append_string(db, (AS_HB_PROTOCOL_V1 == g_config.hb_protocol ? "v1" :
								  (AS_HB_PROTOCOL_V2 == g_config.hb_protocol ? "v2" :
								   (AS_HB_PROTOCOL_RESET == g_config.hb_protocol ? "reset" :
									(AS_HB_PROTOCOL_NONE == g_config.hb_protocol ? "none" : "undefined")))));
	cf_dyn_buf_append_string(db, ";heartbeat-address=");
	cf_dyn_buf_append_string(db, g_config.hb_addr);
	cf_dyn_buf_append_string(db, ";heartbeat-port=");
	cf_dyn_buf_append_int(db, g_config.hb_port);
	cf_dyn_buf_append_string(db, ";heartbeat-interval=");
	cf_dyn_buf_append_int(db, g_config.hb_interval);
	cf_dyn_buf_append_string(db, ";heartbeat-timeout=");
	cf_dyn_buf_append_int(db, g_config.hb_timeout);
}

// TODO - security API?
void
info_security_config_get(cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "enable-security=");
	cf_dyn_buf_append_string(db, g_config.sec_cfg.security_enabled ? "true" : "false");
	cf_dyn_buf_append_string(db, ";privilege-refresh-period=");
	cf_dyn_buf_append_uint32(db, g_config.sec_cfg.privilege_refresh_period);
	cf_dyn_buf_append_string(db, ";report-authentication-sinks=");
	cf_dyn_buf_append_uint32(db, g_config.sec_cfg.report.authentication);
	cf_dyn_buf_append_string(db, ";report-data-op-sinks=");
	cf_dyn_buf_append_uint32(db, g_config.sec_cfg.report.data_op);
	cf_dyn_buf_append_string(db, ";report-sys-admin-sinks=");
	cf_dyn_buf_append_uint32(db, g_config.sec_cfg.report.sys_admin);
	cf_dyn_buf_append_string(db, ";report-user-admin-sinks=");
	cf_dyn_buf_append_uint32(db, g_config.sec_cfg.report.user_admin);
	cf_dyn_buf_append_string(db, ";report-violation-sinks=");
	cf_dyn_buf_append_uint32(db, g_config.sec_cfg.report.violation);
	cf_dyn_buf_append_string(db, ";syslog-local=");
	cf_dyn_buf_append_int(db, g_config.sec_cfg.syslog_local);
}

void
info_xdr_config_get(cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "enable-xdr=");
	cf_dyn_buf_append_string(db, g_config.xdr_cfg.xdr_global_enabled ? "true" : "false");
	cf_dyn_buf_append_string(db, ";forward-xdr-writes=");
	cf_dyn_buf_append_string(db, g_config.xdr_cfg.xdr_forward_xdrwrites ? "true" : "false");
	cf_dyn_buf_append_string(db, ";xdr-delete-shipping-enabled=");
	cf_dyn_buf_append_string(db, g_config.xdr_cfg.xdr_delete_shipping_enabled ? "true" : "false");
	cf_dyn_buf_append_string(db, ";xdr-nsup-deletes-enabled=");
	cf_dyn_buf_append_string(db, g_config.xdr_cfg.xdr_nsup_deletes_enabled ? "true" : "false");
	cf_dyn_buf_append_string(db, ";stop-writes-noxdr=");
	cf_dyn_buf_append_string(db, g_config.xdr_cfg.xdr_stop_writes_noxdr ? "true" : "false");
}


int
info_command_config_get(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "config-get command received: params %s", params);

	if(params) {

		char context[1024];
		int context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "context", context, &context_len)) {
			if (strcmp(context, "namespace") == 0) {
				context_len = sizeof(context);
				if (0 != as_info_parameter_get(params, "id", context, &context_len)) {
					cf_dyn_buf_append_string(db, "Error:invalid id");
					return(0);
				}
				info_namespace_config_get(context, db);
				return(0);
			}
			else if (strcmp(context, "service") == 0) {
				info_service_config_get(db);
				return(0);
			}
			else if (strcmp(context, "network.info") == 0) {
				info_network_info_config_get(db);
				return(0);
			}
			else if (strcmp(context, "network.heartbeat") == 0) {
				info_network_heartbeat_config_get(db);
				return(0);
			}
			else if (strcmp(context, "security") == 0) {
				info_security_config_get(db);
				return(0);
			}
			else if (strcmp(context, "xdr") == 0) {
				info_xdr_config_get(db);
				return(0);
			}
			else {
				cf_dyn_buf_append_string(db, "Error:Invalid context");
				return(0);
			}
		} else if (strcmp(params, "") != 0) {
			cf_dyn_buf_append_string(db, "Error: Invalid get-config parameter");
			return(0);
		}
	}

	// We come here when context is not mentioned.
	// In that case we want to print everything.
	info_service_config_get(db);
	cf_dyn_buf_append_char(db, ';');
	info_network_info_config_get(db);
	cf_dyn_buf_append_char(db, ';');
	info_network_heartbeat_config_get(db);
	cf_dyn_buf_append_char(db, ';');
	info_security_config_get(db);
	cf_dyn_buf_append_char(db, ';');
	info_xdr_config_get(db);

	// Add the current histogram tracking settings.
	cf_hist_track_get_settings(g_config.rt_hist, db);
	cf_hist_track_get_settings(g_config.wt_hist, db);
	cf_hist_track_get_settings(g_config.px_hist, db);
	cf_hist_track_get_settings(g_config.wt_reply_hist, db);
	cf_hist_track_get_settings(g_config.ut_hist, db);
	cf_hist_track_get_settings(g_config.q_hist, db);
	cf_hist_track_get_settings(g_config.q_rcnt_hist, db);

	return(0);
}


//
// config-set:context=service;variable=value;
// config-set:context=network.heartbeat;variable=value;
// config-set:context=namespace;id=test;variable=value;
//
int
info_command_config_set(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "config-set command received: params %s", params);

	char context[1024];
	int  context_len = sizeof(context);
	int val;
	char bool_val[2][6] = {"false", "true"};

	if (0 != as_info_parameter_get(params, "context", context, &context_len))
		goto Error;
	if (strcmp(context, "service") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "migrate-xmit-priority", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-xmit-priority from %d to %d ", g_config.migrate_xmit_priority, val);
			g_config.migrate_xmit_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-xmit-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-xmit-sleep from %d to %d ", g_config.migrate_xmit_sleep, val);
			g_config.migrate_xmit_sleep = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-read-priority", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-read-priority from %d to %d ", g_config.migrate_read_priority, val);
			g_config.migrate_read_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-read-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-read-sleep from %d to %d ", g_config.migrate_read_sleep, val);
			g_config.migrate_read_sleep = val;
		}
		else if (0 == as_info_parameter_get(params, "transaction-retry-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			if (val == 0)
				goto Error;
			cf_info(AS_INFO, "Changing value of transaction-retry-ms from %d to %d ", g_config.transaction_retry_ms, val);
			g_config.transaction_retry_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "transaction-max-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of transaction-retry-ms from %d to %d ", (g_config.transaction_max_ns / 1000000), val);
			g_config.transaction_max_ns = (uint64_t)val * 1000000;
		}
		else if (0 == as_info_parameter_get(params, "transaction-pending-limit", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of transaction-pending-limit from %d to %d ", g_config.transaction_pending_limit, val);
			g_config.transaction_pending_limit = val;
		}
		else if (0 == as_info_parameter_get(params, "transaction-repeatable-read", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of transaction-repeatable-read from %s to %s", bool_val[g_config.transaction_repeatable_read], context);
				g_config.transaction_repeatable_read = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of transaction-repeatable-read from %s to %s", bool_val[g_config.transaction_repeatable_read], context);
				g_config.transaction_repeatable_read = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "ticker-interval", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of ticker-interval from %d to %d ", g_config.ticker_interval, val);
			g_config.ticker_interval = val;
		}
		else if (0 == as_info_parameter_get(params, "dump-message-above-size", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of dump-message-above-size from %d to %d ", g_config.dump_message_above_size, val);
			g_config.dump_message_above_size = val;
		}
		else if (0 == as_info_parameter_get(params, "microbenchmarks", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				clear_microbenchmark_histograms();
				cf_info(AS_INFO, "Changing value of microbenchmarks from %s to %s", bool_val[g_config.microbenchmarks], context);
				g_config.microbenchmarks = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of microbenchmarks from %s to %s", bool_val[g_config.microbenchmarks], context);
				g_config.microbenchmarks = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "storage-benchmarks", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				as_storage_histogram_clear_all();
				cf_info(AS_INFO, "Changing value of storage-benchmarks from %s to %s", bool_val[g_config.storage_benchmarks], context);
				g_config.storage_benchmarks = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of storage-benchmarks from %s to %s", bool_val[g_config.storage_benchmarks], context);
				g_config.storage_benchmarks = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "ldt-benchmarks", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				clear_ldt_histograms();
				cf_info(AS_INFO, "Changing value of ldt-benchmarks from %s to %s", bool_val[g_config.ldt_benchmarks], context);
				g_config.ldt_benchmarks = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of ldt-benchmarks from %s to %s", bool_val[g_config.ldt_benchmarks], context);
				g_config.ldt_benchmarks = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "scan-priority", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of scan-priority from %d to %d ", g_config.scan_priority, val);
			g_config.scan_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "scan-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 0)
				goto Error;
			cf_info(AS_INFO, "Changing value of scan-sleep from %d to %d ", g_config.scan_sleep, val);
			g_config.scan_sleep = val;
		}
		else if (0 == as_info_parameter_get(params, "batch-max-requests", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of batch-max-requests from %d to %d ", g_config.batch_max_requests, val);
			g_config.batch_max_requests = val;
		}
		else if (0 == as_info_parameter_get(params, "batch-priority", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of batch-priority from %d to %d ", g_config.batch_priority, val);
			g_config.batch_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "proto-fd-max", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of proto-fd-max from %d to %d ", g_config.n_proto_fd_max, val);
			g_config.n_proto_fd_max = val;
		}
		else if (0 == as_info_parameter_get(params, "proto-fd-idle-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of proto-fd-idle-ms from %d to %d ", g_config.proto_fd_idle_ms, val);
			g_config.proto_fd_idle_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "proto-slow-netio-sleep-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of proto-slow-netio-sleep-ms from %d to %d ", g_config.proto_slow_netio_sleep_ms, val);
			g_config.proto_slow_netio_sleep_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "nsup-delete-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of nsup-delete-sleep from %d to %d ", g_config.nsup_delete_sleep, val);
			g_config.nsup_delete_sleep = val;
		}
		else if (0 == as_info_parameter_get(params, "nsup-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of nsup-period from %d to %d ", g_config.nsup_period, val);
			g_config.nsup_period = val;
		}
		else if (0 == as_info_parameter_get(params, "paxos-retransmit-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of paxos-retransmit-period from %d to %d ", g_config.paxos_retransmit_period, val);
			g_config.paxos_retransmit_period = val;
		}
		else if (0 == as_info_parameter_get(params, "paxos-max-cluster-size", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || (1 >= val) || (val > AS_CLUSTER_SZ))
				goto Error;
			cf_info(AS_INFO, "Changing value of paxos-max-cluster-size from %d to %d ", g_config.paxos_max_cluster_size, val);
			g_config.paxos_max_cluster_size = val;
		}
		else if (0 == as_info_parameter_get(params, "paxos-protocol", context, &context_len)) {
			paxos_protocol_enum protocol = (!strcmp(context, "v1") ? AS_PAXOS_PROTOCOL_V1 :
											(!strcmp(context, "v2") ? AS_PAXOS_PROTOCOL_V2 :
											 (!strcmp(context, "v3") ? AS_PAXOS_PROTOCOL_V3 :
											  (!strcmp(context, "v4") ? AS_PAXOS_PROTOCOL_V4 :
											   (!strcmp(context, "none") ? AS_PAXOS_PROTOCOL_NONE :
												AS_PAXOS_PROTOCOL_UNDEF)))));
			if (AS_PAXOS_PROTOCOL_UNDEF == protocol)
				goto Error;
			if (0 > as_paxos_set_protocol(protocol))
				goto Error;
			cf_info(AS_INFO, "Changing value of paxos-protocol version to %s", context);
		}
		else if (0 == as_info_parameter_get(params, "paxos-recovery-policy", context, &context_len)) {
			paxos_recovery_policy_enum policy = (!strcmp(context, "manual") ? AS_PAXOS_RECOVERY_POLICY_MANUAL :
												 (!strcmp(context, "auto-dun-master") ? AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_MASTER :
												  (!strcmp(context, "auto-dun-all") ? AS_PAXOS_RECOVERY_POLICY_AUTO_DUN_ALL :
												   AS_PAXOS_RECOVERY_POLICY_UNDEF)));
			if (AS_PAXOS_RECOVERY_POLICY_UNDEF == policy)
				goto Error;
			if (0 > as_paxos_set_recovery_policy(policy))
				goto Error;
			cf_info(AS_INFO, "Changing value of paxos-recovery-policy to %s", context);
		}
		else if (0 == as_info_parameter_get(params, "migrate-xmit-hwm", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-xmit-hwm from %d to %d ", g_config.migrate_xmit_hwm, val);
			g_config.migrate_xmit_hwm = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-xmit-lwm", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-xmit-lwm from %d to %d ", g_config.migrate_xmit_lwm, val);
			g_config.migrate_xmit_lwm = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-max-num-incoming", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || (0 > val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-max-num-incoming from %d to %d ", g_config.migrate_max_num_incoming, val);
			g_config.migrate_max_num_incoming = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-rx-lifetime-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || (0 > val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-rx-lifetime-ms from %d to %d ", g_config.migrate_rx_lifetime_ms, val);
			g_config.migrate_rx_lifetime_ms = val;
		}
		else if (0 == as_info_parameter_get(params, "migrate-threads", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || (0 > val) || (MAX_NUM_MIGRATE_XMIT_THREADS < val))
				goto Error;
			cf_info(AS_INFO, "Changing value of migrate-theads from %d to %d ", g_config.n_migrate_threads, val);
			as_migrate_set_num_xmit_threads(val);
		}
		else if (0 == as_info_parameter_get(params, "generation-disable", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of generation-disable from %s to %s", bool_val[g_config.generation_disable], context);
				g_config.generation_disable = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of generation-disable from %s to %s", bool_val[g_config.generation_disable], context);
				g_config.generation_disable = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "write-duplicate-resolution-disable", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of write-duplicate-resolution-disable from %s to %s", bool_val[g_config.write_duplicate_resolution_disable], context);
				g_config.write_duplicate_resolution_disable = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of write-duplicate-resolution-disable from %s to %s", bool_val[g_config.write_duplicate_resolution_disable], context);
				g_config.write_duplicate_resolution_disable = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "respond-client-on-master-completion", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of respond-client-on-master-completion from %s to %s", bool_val[g_config.respond_client_on_master_completion], context);
				g_config.respond_client_on_master_completion = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of respond-client-on-master-completion from %s to %s", bool_val[g_config.respond_client_on_master_completion], context);
				g_config.respond_client_on_master_completion = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "replication-fire-and-forget", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of replication-fire-and-forget from %s to %s", bool_val[g_config.replication_fire_and_forget], context);
				g_config.replication_fire_and_forget = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of replication-fire-and-forget from %s to %s", bool_val[g_config.replication_fire_and_forget], context);
				g_config.replication_fire_and_forget = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "use-queue-per-device", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of use-queue-per-device from %s to %s", bool_val[g_config.use_queue_per_device], context);
				g_config.use_queue_per_device = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of use-queue-per-device from %s to %s", bool_val[g_config.use_queue_per_device], context);
				g_config.use_queue_per_device = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "allow-inline-transactions", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of allow-inline-transactions from %s to %s", bool_val[g_config.allow_inline_transactions], context);
				g_config.allow_inline_transactions = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of allow-inline-transactions from %s to %s", bool_val[g_config.allow_inline_transactions], context);
				g_config.allow_inline_transactions = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "snub-nodes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of snub-nodes from %s to %s", bool_val[g_config.snub_nodes], context);
				g_config.snub_nodes = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of snub-nodes from %s to %s", bool_val[g_config.snub_nodes], context);
				g_config.snub_nodes = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "fb-health-msg-per-burst", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of fb-health-msg-per-burst from %d to %d ", g_config.fb_health_msg_per_burst, val);
			g_config.fb_health_msg_per_burst = val;
		}
		else if (0 == as_info_parameter_get(params, "fb-health-msg-timeout", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of fb-health-msg-timeout from %d to %d ", g_config.fb_health_msg_timeout, val);
			g_config.fb_health_msg_timeout = val;
		}
		else if (0 == as_info_parameter_get(params, "fb-health-good-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of fb-health-good-pct from %d to %d ", g_config.fb_health_good_pct, val);
			g_config.fb_health_good_pct = val;
		}
		else if (0 == as_info_parameter_get(params, "fb-health-bad-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of fb-health-bad-pct from %d to %d ", g_config.fb_health_bad_pct, val);
			g_config.fb_health_bad_pct = val;
		}
		else if (0 == as_info_parameter_get(params, "auto-dun", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of auto-dun %s to %s", bool_val[g_config.auto_dun], context);
				g_config.auto_dun = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of auto-dun from %s to %s", bool_val[g_config.auto_dun], context);
				g_config.auto_dun = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "auto-undun", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of auto-undun from %s to %s", bool_val[g_config.auto_undun], context);
				g_config.auto_undun = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of auto-undun from %s to %s", bool_val[g_config.auto_undun], context);
				g_config.auto_undun = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "non-master-sets-delete", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of non-master-sets-delete from %s to %s", bool_val[g_config.non_master_sets_delete], context);
				g_config.non_master_sets_delete = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of non-master-sets-delete from %s to %s", bool_val[g_config.non_master_sets_delete], context);
				g_config.non_master_sets_delete = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "prole-extra-ttl", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of prole-extra-ttl from %d to %d ", g_config.prole_extra_ttl, val);
			g_config.prole_extra_ttl = val;
		}
		else if (0 == as_info_parameter_get(params, "max-msgs-per-type", context, &context_len)) {
			if ((0 != cf_str_atoi(context, &val)) || (val == 0))
				goto Error;
			cf_info(AS_INFO, "Changing value of max-msgs-per-type from %d to %d ", g_config.max_msgs_per_type, val);
			msg_set_max_msgs_per_type(g_config.max_msgs_per_type = (val >= 0 ? val : -1));
		}
#ifdef MEM_COUNT
		else if (0 == as_info_parameter_get(params, "memory-accounting", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				g_config.memory_accounting = true;
				cf_info(AS_INFO, "Changing value of memory-accounting from %s to %s", bool_val[g_config.memory_accounting], context);
				mem_count_init(MEM_COUNT_ENABLE_DYNAMIC);
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				g_config.memory_accounting = false;
				cf_info(AS_INFO, "Changing value of memory-accounting from %s to %s", bool_val[g_config.memory_accounting], context);
				mem_count_init(MEM_COUNT_DISABLE);
			}
			else
				goto Error;
		}
#endif
		else if (0 == as_info_parameter_get(params, "udf-runtime-gmax-memory", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "global udf-runtime-max-memory = %"PRIu64"", val);
			if (val > g_config.udf_runtime_max_gmemory)
				g_config.udf_runtime_max_gmemory = val;
			if (val < (g_config.udf_runtime_max_gmemory / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of udf-runtime-gmax-memory from %d to %d ", g_config.udf_runtime_max_gmemory, val);
			g_config.udf_runtime_max_gmemory = val;
		}
		else if (0 == as_info_parameter_get(params, "udf-runtime-max-memory", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "udf-runtime-max-memory = %"PRIu64"", val);
			if (val > g_config.udf_runtime_max_memory)
				g_config.udf_runtime_max_memory = val;
			if (val < (g_config.udf_runtime_max_memory / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of udf-runtime-max-memory from %d to %d ", g_config.udf_runtime_max_memory, val);
			g_config.udf_runtime_max_memory = val;
		}
		else if (0 == as_info_parameter_get(params, "query-buf-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-buf-size = %"PRIu64"", val);
			if (val < 1024) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-buf-size from %"PRIu64" to %"PRIu64"", g_config.query_buf_size, val);
			g_config.query_buf_size = val;
		}
		else if (0 == as_info_parameter_get(params, "query-threshold", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-threshold = %"PRIu64"", val);
			if (val <= 0) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-threshold from %"PRIu64" to %"PRIu64"", g_config.query_threshold, val);
			g_config.query_threshold = val;
		}
		else if (0 == as_info_parameter_get(params, "query-untracked-time", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-untracked-time = %"PRIu64" micro seconds", val);
			if (val < 0) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-untracked-time from %"PRIu64" micro seconds to %"PRIu64" micro seconds", 
						g_config.query_untracked_time_ns/1000, val);
			g_config.query_untracked_time_ns = val * 1000;
		}
		else if (0 == as_info_parameter_get(params, "query-rec-count-bound", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "query-rec-count-bound = %"PRIu64"", val);
			if (val <= 0) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-rec-count-bound from %"PRIu64" to %"PRIu64" ", g_config.query_rec_count_bound, val);
			g_config.query_rec_count_bound = val;
		}
		else if ( 0 == as_info_parameter_get(params, "sindex-populator-scan-priority", context, &context_len)) {
			int val = 0;
			if (0 != cf_str_atoi(context, &val) || (val != 1 && val != 3 && val != 5)) {
				cf_warning(AS_INFO, "sindex-populator-scan-priority: value must be among (1, 3, 5), Current is: %s", context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of sindex-populator-scan-priority from %"PRIu64" to %"PRIu64" ", g_config.sindex_populator_scan_priority, val);
			g_config.sindex_populator_scan_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "sindex-data-max-memory", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "sindex-data-max-memory = %"PRIu64"", val);
			if (val > g_config.sindex_data_max_memory) {
				g_config.sindex_data_max_memory = val;
			}
			if (val < (g_config.sindex_data_max_memory / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of sindex-data-max-memory from %d to %d ", g_config.sindex_data_max_memory, val);
			g_config.sindex_data_max_memory = val;
		}
		else if (0 == as_info_parameter_get(params, "query-threads", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-threads = %d", val);
			if (val == 0) {
				cf_warning(AS_INFO, "query-threads should be a number %s", context);
				goto Error;
			}
			int old_val = g_config.query_threads;
			int new_val = 0;
			if (as_query_reinit(val, &new_val) != AS_QUERY_OK) {
				cf_warning(AS_INFO, "Config not changed.");
				goto Error;
			}

			cf_info(AS_INFO, "Changing value of query-threads from %d to %d",
					old_val, new_val);
		}
		else if (0 == as_info_parameter_get(params, "query-worker-threads", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-worker-threads = %d", val);
			if (val == 0) {
				cf_warning(AS_INFO, "query-worker-threads should be a number %s", context);
				goto Error;
			}
			int old_val = g_config.query_threads;
			int new_val = 0;
			if (as_query_worker_reinit(val, &new_val) != AS_QUERY_OK) {
				cf_warning(AS_INFO, "Config not changed.");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-worker-threads from %d to %d",
					old_val, new_val);
		}
		else if (0 == as_info_parameter_get(params, "query-priority", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query_priority = %d", val);
			if (val == 0) {
				cf_warning(AS_INFO, "query_priority should be a number %s", context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-priority from %d to %d ", g_config.query_priority, val);
			g_config.query_priority = val;
		}
		else if (0 == as_info_parameter_get(params, "query-sleep", context, &context_len)) {
			uint64_t val = atoll(context);
			if(val == 0) {
				cf_warning(AS_INFO, "query_sleep should be a number %s", context);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-sleep from %d to %d ", g_config.query_sleep, val);
			g_config.query_sleep = val;
		}
		else if (0 == as_info_parameter_get(params, "query-batch-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-batch-size = %d", val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-batch-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-batch-size from %d to %d ", g_config.query_bsize, val);
			g_config.query_bsize = val;
		}
		else if (0 == as_info_parameter_get(params, "query-req-max-inflight", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-req-max-inflight = %d", val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-req-max-inflight should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-req-max-inflight from %d to %d ", g_config.query_req_max_inflight, val);
			g_config.query_req_max_inflight = val;
		}
		else if (0 == as_info_parameter_get(params, "query-bufpool-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-bufpool-size = %d", val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-bufpool-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-bufpool-size from %d to %d ", g_config.query_bufpool_size, val);
			g_config.query_bufpool_size = val;
		}
		else if (0 == as_info_parameter_get(params, "query-in-transaction-thread", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-in-transaction-thread  from %s to %s", bool_val[g_config.query_in_transaction_thr], context);
				g_config.query_in_transaction_thr = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-in-transaction-thread  from %s to %s", bool_val[g_config.query_in_transaction_thr], context);
				g_config.query_in_transaction_thr = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "query-req-in-query-thread", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-req-in-query-thread from %s to %s", bool_val[g_config.query_req_in_query_thread], context);
				g_config.query_req_in_query_thread = true;

			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-req-in-query-thread from %s to %s", bool_val[g_config.query_req_in_query_thread], context);
				g_config.query_req_in_query_thread = false;
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "query-job-tracking", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of query-job-tracking from %s to %s", bool_val[g_config.query_job_tracking], context);
				as_query_set_job_tracking(true);
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of query-job-tracking from %s to %s", bool_val[g_config.query_job_tracking], context);
				as_query_set_job_tracking(false);
			}
			else
				goto Error;
		}
		else if (0 == as_info_parameter_get(params, "query-short-q-max-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-short-q-max-size = %d", val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-short-q-max-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-short-q-max-size from %d to %d ", g_config.query_short_q_max_size, val);
			g_config.query_short_q_max_size = val;
		}
		else if (0 == as_info_parameter_get(params, "query-long-q-max-size", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_info(AS_INFO, "query-long-q-max-size = %d", val);
			if((int)val <= 0) {
				cf_warning(AS_INFO, "query-long-q-max-size should be a positive number");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of query-longq-max-size from %d to %d ", g_config.query_long_q_max_size, val);
			g_config.query_long_q_max_size = val;
		}
		else if (0 == as_info_parameter_get(params, "sindex-gc-enable-histogram", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of sindex-gc-enable-histogram to %s", context);
				g_config.sindex_gc_enable_histogram = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of sindex-gc-enable-histogram from to %s", context);
				g_config.sindex_gc_enable_histogram = false;
			}
		}
		else if (0 == as_info_parameter_get(params, "query-microbenchmark", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) { 
				cf_info(AS_INFO, "Changing value of query-enable-histogram to %s", context);
				g_config.query_enable_histogram = true;
			}    
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) { 
				cf_info(AS_INFO, "Changing value of query-enable-histogram to %s", context);
				g_config.query_enable_histogram = false;
			}    
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "pre-reserve-qnodes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) { 
				cf_info(AS_INFO, "Changing value of reserve-qnodes-upfront to %s", context);
				g_config.qnodes_pre_reserved = true;
			}    
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) { 
				cf_info(AS_INFO, "Changing value of reserve-qnodes-upfront to %s", context);
				g_config.qnodes_pre_reserved = false;
			}    
			else {
				goto Error;
			}
		}
		else {
			goto Error;
		}
	}
	else if (strcmp(context, "network.heartbeat") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "interval", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of interval from %d to %d ", g_config.hb_interval, val);
			g_config.hb_interval = val;
		}
		else if (0 == as_info_parameter_get(params, "timeout", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val))
				goto Error;
			cf_info(AS_INFO, "Changing value of timeout from %d to %d ", g_config.hb_timeout, val);
			g_config.hb_timeout = val;
		}
		else if (0 == as_info_parameter_get(params, "protocol", context, &context_len)) {
			hb_protocol_enum protocol = (!strcmp(context, "v1") ? AS_HB_PROTOCOL_V1 :
										 (!strcmp(context, "v2") ? AS_HB_PROTOCOL_V2 :
										  (!strcmp(context, "reset") ? AS_HB_PROTOCOL_RESET :
										   (!strcmp(context, "none") ? AS_HB_PROTOCOL_NONE :
											AS_HB_PROTOCOL_UNDEF))));
			if (AS_HB_PROTOCOL_UNDEF == protocol)
				goto Error;
			cf_info(AS_INFO, "Changing value of heartbeat protocol version to %s", context);
			if (0 > as_hb_set_protocol(protocol))
				goto Error;
		}
		else
			goto Error;
	}
	else if (strcmp(context, "network.info") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "enable-fastpath", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-fastpath from %s to %s", bool_val[g_config.info_fastpath_enabled], context);
				g_config.info_fastpath_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-fastpath from %s to %s", bool_val[g_config.info_fastpath_enabled], context);
				g_config.info_fastpath_enabled = false;
			}
			else
				goto Error;
		}
		else
			goto Error;
	}
	else if (strcmp(context, "namespace") == 0) {
		context_len = sizeof(context);
		if (0 != as_info_parameter_get(params, "id", context, &context_len))
			goto Error;
		as_namespace *ns = as_namespace_get_byname(context);
		if (!ns)
			goto Error;

		context_len = sizeof(context);
		// configure namespace/set related parameters:
		if (0 == as_info_parameter_get(params, "set", context, &context_len)) {
			// checks if there is a vmap set with the same name and if so returns a ptr to it
			// if not, it creates an set structure, initializes it and returns a ptr to it.
			as_set * p_set = as_namespace_init_set(ns, context);
			cf_debug(AS_INFO, "set name is %s\n", p_set->name);
			context_len = sizeof(context);
			if (0 == as_info_parameter_get(params, "set-stop-write-count", context, &context_len)) {
				uint64_t val = atoll(context);
				cf_info(AS_INFO, "Changing value of set-stop-write-count of ns %s set %s to %"PRIu64, ns->name, p_set->name, val);
				cf_atomic64_set(&p_set->stop_write_count, val);
			}
			else if (0 == as_info_parameter_get(params, "set-evict-hwm-count", context, &context_len)) {
				uint64_t val = atoll(context);
				cf_info(AS_INFO, "Changing value of set-evict-hwm-count of ns %s set %s to %"PRIu64, ns->name, p_set->name, val);
				cf_atomic64_set(&p_set->evict_hwm_count, val);
			}
			else if (0 == as_info_parameter_get(params, "set-enable-xdr", context, &context_len)) {
				// TODO - make sure context is null-terminated.
				if ((strncmp(context, "true", 4) == 0) || (strncmp(context, "yes", 3) == 0)) {
					cf_info(AS_INFO, "Changing value of set-enable-xdr of ns %s set %s to %s", ns->name, p_set->name, context);
					cf_atomic32_set(&p_set->enable_xdr, AS_SET_ENABLE_XDR_TRUE);
				}
				else if ((strncmp(context, "false", 5) == 0) || (strncmp(context, "no", 2) == 0)) {
					cf_info(AS_INFO, "Changing value of set-enable-xdr of ns %s set %s to %s", ns->name, p_set->name, context);
					cf_atomic32_set(&p_set->enable_xdr, AS_SET_ENABLE_XDR_FALSE);
				}
				else if (strncmp(context, "use-default", 11) == 0) {
					cf_info(AS_INFO, "Changing value of set-enable-xdr of ns %s set %s to %s", ns->name, p_set->name, context);
					cf_atomic32_set(&p_set->enable_xdr, AS_SET_ENABLE_XDR_DEFAULT);
				}
				else {
					goto Error;
				}
			}
			else if (0 == as_info_parameter_get(params, "set-delete", context, &context_len)) {
				if ((strncmp(context, "true", 4) == 0) || (strncmp(context, "yes", 3) == 0)) {
					cf_info(AS_INFO, "Changing value of set-delete of ns %s set %s to %s", ns->name, p_set->name, context);
					SET_DELETED_ON(p_set);
				}
				else if ((strncmp(context, "false", 5) == 0) || (strncmp(context, "no", 2) == 0)) {
					cf_info(AS_INFO, "Changing value of set-delete of ns %s set %s to %s", ns->name, p_set->name, context);
					SET_DELETED_OFF(p_set);
				}
				else {
					goto Error;
				}
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "memory-size", context, &context_len)) {
			uint64_t val;

			if (0 != cf_str_atoi_u64(context, &val)) {
				goto Error;
			}
			cf_debug(AS_INFO, "memory-size = %"PRIu64"", val);
			if (val > ns->memory_size)
				ns->memory_size = val;
			if (val < (ns->memory_size / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of memory-size of ns %s from %d to %d ", ns->name, ns->memory_size, val);
			ns->memory_size = val;
		}
		else if (0 == as_info_parameter_get(params, "high-water-disk-pct", context, &context_len)) {
			cf_info(AS_INFO, "Changing value of high-water-disk-pct of ns %s from %1.3f to %1.3f ", ns->name, ns->hwm_disk, atof(context) / (float)100);
			ns->hwm_disk = atof(context) / (float)100;
		}
		else if (0 == as_info_parameter_get(params, "high-water-memory-pct", context, &context_len)) {
			cf_info(AS_INFO, "Changing value of high-water-memory-pct memory of ns %s from %1.3f to %1.3f ", ns->name, ns->hwm_memory, atof(context) / (float)100);
			ns->hwm_memory = atof(context) / (float)100;
		}
		else if (0 == as_info_parameter_get(params, "evict-tenths-pct", context, &context_len)) {
			cf_info(AS_INFO, "Changing value of evict-tenths-pct memory of ns %s from %d to %d ", ns->name, ns->evict_tenths_pct, atoi(context));
			ns->evict_tenths_pct = atoi(context);
		}
		else if (0 == as_info_parameter_get(params, "stop-writes-pct", context, &context_len)) {
			cf_info(AS_INFO, "Changing value of stop-writes-pct memory of ns %s from %1.3f to %1.3f ", ns->name, ns->stop_writes_pct, atof(context) / (float)100);
			ns->stop_writes_pct = atof(context) / (float)100;
		}
		else if (0 == as_info_parameter_get(params, "default-ttl", context, &context_len)) {
			uint64_t val;
			if (cf_str_atoi_seconds(context, &val) != 0) {
				cf_warning(AS_INFO, "default-ttl must be an unsigned number with time unit (s, m, h, or d)");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of default-ttl memory of ns %s from %"PRIu64" to %"PRIu64" ", ns->name, ns->default_ttl, val);
			ns->default_ttl = val;
		}
		else if (0 == as_info_parameter_get(params, "max-ttl", context, &context_len)) {
			uint64_t val;
			if (cf_str_atoi_seconds(context, &val) != 0) {
				cf_warning(AS_INFO, "max-ttl must be an unsigned number with time unit (s, m, h, or d)");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of max-ttl memory of ns %s from %"PRIu64" to %"PRIu64" ", ns->name, ns->max_ttl, val);
			ns->max_ttl = val;
		}
		else if (0 == as_info_parameter_get(params, "obj-size-hist-max", context, &context_len)) {
			uint32_t hist_max = (uint32_t)atoi(context);
			uint32_t round_to = OBJ_SIZE_HIST_NUM_BUCKETS;
			uint32_t round_max = hist_max ? ((hist_max + round_to - 1) / round_to) * round_to : round_to;
			if (round_max != hist_max) {
				cf_info(AS_INFO, "rounding obj-size-hist-max %u up to %u", hist_max, round_max);
			}
			cf_info(AS_INFO, "Changing value of obj-size-hist-max of ns %s to %u", ns->name, round_max);
			cf_atomic32_set(&ns->obj_size_hist_max, round_max); // in 128-byte blocks
		}
		else if (0 == as_info_parameter_get(params, "conflict-resolution-policy", context, &context_len)) {
			if (strncmp(context, "generation", 10) == 0) {
				cf_info(AS_INFO, "Changing value of conflict-resolution-policy of ns %s from %d to %s", ns->name, ns->conflict_resolution_policy, context);
				ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
			}
			else if (strncmp(context, "ttl", 3) == 0) {
				cf_info(AS_INFO, "Changing value of conflict-resolution-policy of ns %s from %d to %s", ns->name, ns->conflict_resolution_policy, context);
				ns->conflict_resolution_policy = AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_TTL;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "allow-versions", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of allow-versions of ns %s from %s to %s", ns->name, bool_val[ns->allow_versions], context);
				ns->allow_versions = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of allow-versions of ns %s from %s to %s", ns->name, bool_val[ns->allow_versions], context);
				ns->allow_versions = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "ldt-enabled", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of ldt-enabled of ns %s from %s to %s", ns->name, bool_val[ns->ldt_enabled], context);
				ns->ldt_enabled = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of ldt-enabled of ns %s from %s to %s", ns->name, bool_val[ns->ldt_enabled], context);
				ns->ldt_enabled = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "ldt-page-size", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
	  		if (val > ns->storage_write_block_size) {
				// 1Kb head room
				val = ns->storage_write_block_size - 1024;
			}
			cf_info(AS_INFO, "Changing value of ldt-page-size of ns %s from %d to %d ", ns->name, ns->ldt_page_size, val);
			ns->ldt_page_size = val;
		}
		else if (0 == as_info_parameter_get(params, "ldt-scan-buff-max", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of ldt-scan-buff-max of ns %s from %d to %d ", ns->name, ns->ldt_scan_buff_max, val);
			ns->ldt_scan_buff_max = val;
		}
		else if (0 == as_info_parameter_get(params, "ldt-gc-rate", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			uint64_t rate = (uint64_t)val;

			if ((rate == 0) || (rate > LDT_SUB_GC_MAX_RATE)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of ldt-gc-rate of ns %s from %lu to %d", ns->name, (1000 * 1000)/ns->ldt_gc_sleep_us , val);
			ns->ldt_gc_sleep_us = 1000 * 1000 / rate;
		}
		else if (0 == as_info_parameter_get(params, "defrag-lwm-pct", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of defrag-lwm-pct of ns %s from %d to %d ", ns->name, ns->storage_defrag_lwm_pct, val);

			uint32_t old_val = ns->storage_defrag_lwm_pct;

			ns->storage_defrag_lwm_pct = val;
			ns->defrag_lwm_size = (ns->storage_write_block_size * ns->storage_defrag_lwm_pct) / 100;

			if (ns->storage_defrag_lwm_pct > old_val) {
				as_storage_defrag_sweep(ns);
			}
		}
		else if (0 == as_info_parameter_get(params, "defrag-queue-min", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of defrag-queue-min of ns %s from %u to %d", ns->name, ns->storage_defrag_queue_min, val);
			ns->storage_defrag_queue_min = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "defrag-sleep", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of defrag-sleep of ns %s from %u to %d", ns->name, ns->storage_defrag_sleep, val);
			ns->storage_defrag_sleep = (uint32_t)val;
		}
		else if (0 == as_info_parameter_get(params, "flush-max-ms", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of flush-max-ms of ns %s from %lu to %d", ns->name, ns->storage_flush_max_us / 1000, val);
			ns->storage_flush_max_us = (uint64_t)val * 1000;
		}
		else if (0 == as_info_parameter_get(params, "fsync-max-sec", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of fsync-max-sec of ns %s from %lu to %d", ns->name, ns->storage_fsync_max_us / 1000000, val);
			ns->storage_fsync_max_us = (uint64_t)val * 1000000;
		}
		else if (0 == as_info_parameter_get(params, "enable-xdr", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->enable_xdr], context);
				ns->enable_xdr = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->enable_xdr], context);
				ns->enable_xdr = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "sets-enable-xdr", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of sets-enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->sets_enable_xdr], context);
				ns->sets_enable_xdr = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of sets-enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->sets_enable_xdr], context);
				ns->sets_enable_xdr = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "ns-forward-xdr-writes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of sets-enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->ns_forward_xdr_writes], context);
				ns->ns_forward_xdr_writes = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of sets-enable-xdr of ns %s from %s to %s", ns->name, bool_val[ns->ns_forward_xdr_writes], context);
				ns->ns_forward_xdr_writes = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "disallow-null-setname", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of disallow-null-setname of ns %s from %s to %s", ns->name, bool_val[ns->disallow_null_setname], context);
				ns->disallow_null_setname = true;
			}
			else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of disallow-null-setname of ns %s from %s to %s", ns->name, bool_val[ns->disallow_null_setname], context);
				ns->disallow_null_setname = false;
			}
			else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "write-smoothing-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if ( ns->storage_write_smoothing_period < 0 || (ns->storage_defrag_startup_minimum > 0 && ns->storage_defrag_startup_minimum < 5) || ns->storage_defrag_startup_minimum > 1000)
				goto Error;
			cf_info(AS_INFO, "Changing value of write-smoothing-period of ns %s from %d to %d ", ns->name, ns->storage_write_smoothing_period, val);
			ns->storage_write_smoothing_period = val;

		}
		else if (0 == as_info_parameter_get(params, "max-write-cache", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val)) {
				goto Error;
			}
			if (val < (1024 * 1024 * 4)) {
				cf_warning(AS_INFO, "can't set max-write-cache less than 4M");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of max-write-cache of ns %s from %lu to %d ", ns->name, ns->storage_max_write_cache, val);
			ns->storage_max_write_cache = (uint64_t)val;
			ns->storage_max_write_q = (int)(ns->storage_max_write_cache / ns->storage_write_block_size);
		}
		else if (0 == as_info_parameter_get(params, "min-avail-pct", context, &context_len)) {
			ns->storage_min_avail_pct = atoi(context);
			cf_info(AS_INFO, "Changing value of min-avail-pct of ns %s from %u to %u ", ns->name, ns->storage_min_avail_pct, atoi(context));
		}
		else if (0 == as_info_parameter_get(params, "post-write-queue", context, &context_len)) {
			if (ns->storage_data_in_memory) {
				cf_warning(AS_INFO, "ns %s, can't set post-write-queue if data-in-memory", ns->name);
				goto Error;
			}
			if (0 != cf_str_atoi(context, &val)) {
				cf_warning(AS_INFO, "ns %s, post-write-queue %s is not a number", ns->name, context);
				goto Error;
			}
			if ((uint32_t)val > (2 * 1024)) {
				cf_warning(AS_INFO, "ns %s, post-write-queue %u must be < 2K", ns->name, val);
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of post-write-queue of ns %s from %d to %d ", ns->name, ns->storage_post_write_queue, val);
			cf_atomic32_set(&ns->storage_post_write_queue, (uint32_t)val);
		}
		else if (0 == as_info_parameter_get(params, "sindex-data-max-memory", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_debug(AS_INFO, "sindex-data-max-memory = %"PRIu64"", val);
			if (val > ns->sindex_data_max_memory)
				ns->sindex_data_max_memory = val;
			if (val < (ns->sindex_data_max_memory / 2L)) { // protect so someone does not reduce memory to below 1/2 current value
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of sindex-data-max-memory of ns %s from %d to %d ", ns->name, ns->sindex_data_max_memory, val);
			ns->sindex_data_max_memory = val;
		}
		else if (0 == as_info_parameter_get(params, "indexname", context, &context_len)) {
			as_sindex_metadata imd;
			memset((void *)&imd, 0, sizeof(imd));
			imd.ns_name = cf_strdup(ns->name);
			imd.iname   = cf_strdup(context);
			int ret_val = as_sindex_set_config(ns, &imd, params);

			if (imd.ns_name) cf_free(imd.ns_name);
			if (imd.iname) cf_free(imd.iname);

			if (ret_val) {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "read-consistency-level-override", context, &context_len)) {
			char *original_value = NS_READ_CONSISTENCY_LEVEL_NAME();
			if (strcmp(context, "all") == 0) {
				ns->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ALL;
				ns->read_consistency_level_override = true;
			}
			else if (strcmp(context, "off") == 0) {
				ns->read_consistency_level_override = false;
			}
			else if (strcmp(context, "one") == 0) {
				ns->read_consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ONE;
				ns->read_consistency_level_override = true;
			}
			else {
				goto Error;
			}
			if (strcmp(original_value, context)) {
				cf_info(AS_INFO, "Changing value of read-consistency-level-override of ns %s from %s to %s", ns->name, original_value, context);
			}
		}
		else if (0 == as_info_parameter_get(params, "write-commit-level-override", context, &context_len)) {
			char *original_value = NS_WRITE_COMMIT_LEVEL_NAME();
			if (strcmp(context, "all") == 0) {
				ns->write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
				ns->write_commit_level_override = true;
			}
			else if (strcmp(context, "master") == 0) {
				ns->write_commit_level = AS_POLICY_COMMIT_LEVEL_MASTER;
				ns->write_commit_level_override = true;
			}
			else if (strcmp(context, "off") == 0) {
				ns->write_commit_level_override = false;
			}
			else {
				goto Error;
			}
			if (strcmp(original_value, context)) {
				cf_info(AS_INFO, "Changing value of write-commit-level-override of ns %s from %s to %s", ns->name, original_value, context);
			}
		}
		else {
			goto Error;
		}
	} // end of namespace stanza
	else if (strcmp(context, "security") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "privilege-refresh-period", context, &context_len)) {
			if (0 != cf_str_atoi(context, &val) || val < 10 || val > 60 * 60 * 24) {
				cf_warning(AS_INFO, "privilege-refresh-period must be an unsigned integer between 10 and 86400");
				goto Error;
			}
			cf_info(AS_INFO, "Changing value of privilege-refresh-period from %u to %d", g_config.sec_cfg.privilege_refresh_period, val);
			g_config.sec_cfg.privilege_refresh_period = (uint32_t)val;
		}
		else {
			goto Error;
		}
	}
	else if (strcmp(context, "xdr") == 0) {
		context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "enable-xdr", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {

				// If this is for a fresh start, we better close the existing pipe
				// and reopen as it can be in a broken state.
				context_len = sizeof(context);
				if (0 == as_info_parameter_get(params, "freshstart", context, &context_len)) {
					cf_info(AS_INFO, "Closing the digest pipe for a fresh start");
					close(g_config.xdr_cfg.xdr_digestpipe_fd);
					g_config.xdr_cfg.xdr_digestpipe_fd = -1;
				}

				// Create the named pipe if it is not already open
				// Note below that we do not close named pipe when xdr is disabled.
				if (g_config.xdr_cfg.xdr_digestpipe_fd == -1) {

					if (xdr_create_named_pipe(&(g_config.xdr_cfg)) != 0) {
						goto Error;
					}

					// We need to send the namespace info
					if (xdr_send_nsinfo() != 0) {
						goto Error;
					}

					if (xdr_send_nodemap() != 0) {
						goto Error;
					}
				}

				// Everything set.
				cf_info(AS_INFO, "Changing value of enable-xdr from %s to %s", bool_val[g_config.xdr_cfg.xdr_global_enabled], context);
				g_config.xdr_cfg.xdr_global_enabled = true;

			} else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of enable-xdr from %s to %s", bool_val[g_config.xdr_cfg.xdr_global_enabled], context);
				g_config.xdr_cfg.xdr_global_enabled = false;
				/* Closing the named pipe is not really necessary. Moreover, this
				 * will allow us to have additional functionality where XDR on server
				 * can be temporarily disabled.
				 */
			} else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "lastshiptime", context, &context_len)) {
			uint64_t val[DC_MAX_NUM];
			char * tmp_val;
			char *  delim = {","};
			int i = 0;

			// We do not want junk values in val[]. This is LST array.
			// Not doing that will lead to wrong LST time going back warnings from the code below.
			memset(val, 0, sizeof(uint64_t) * DC_MAX_NUM);

			tmp_val = strtok(context, (const char *) delim);
			while(tmp_val) {
				if(i >= DC_MAX_NUM) {
					cf_warning(AS_INFO, "Suspicious \"xdr\" Info command \"lastshiptime\" value: \"%s\"", params);
					break;
				}
				if (0 > cf_str_atoi_u64(tmp_val, &(val[i++]))) {
					cf_warning(AS_INFO, "bad number in \"xdr\" Info command \"lastshiptime\": \"%s\" for DC %d ~~ Using 0", tmp_val, i - 1);
					val[i - 1] = 0;
				}
				tmp_val = strtok(NULL, (const char *) delim);
			}

			for(i = 0; i < DC_MAX_NUM; i++) {
				// Warning only if time went back by 5 mins or more
				// We are doing subtraction of two uint64_t here. We should be more careful and first check
				// if the first value is greater.
				if ((g_config.xdr_self_lastshiptime[i] > val[i]) &&
						((g_config.xdr_self_lastshiptime[i] - val[i]) > XDR_ACCEPTABLE_TIMEDIFF)) {
					cf_warning(AS_INFO, "XDR last ship time of this node for DC %d went back to %"PRIu64" from %"PRIu64"",
							i, val[i], g_config.xdr_self_lastshiptime[i]);
					cf_debug(AS_INFO, "(Suspicious \"xdr\" Info command \"lastshiptime\" value: \"%s\".)", params);
				}

				g_config.xdr_self_lastshiptime[i] = val[i];
			}

			xdr_broadcast_lastshipinfo(val);
		}
		else if (0 == as_info_parameter_get(params, "failednodeprocessingdone", context, &context_len)) {
			cf_node nodeid = atoll(context);
			xdr_handle_failednodeprocessingdone(nodeid);
		}
		else if (0 == as_info_parameter_get(params, "stop-writes-noxdr", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of stop-writes-noxdr from %s to %s", bool_val[g_config.xdr_cfg.xdr_stop_writes_noxdr], context);
				g_config.xdr_cfg.xdr_stop_writes_noxdr = true;
			} else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of stop-writes-noxdr from %s to %s", bool_val[g_config.xdr_cfg.xdr_stop_writes_noxdr], context);
				g_config.xdr_cfg.xdr_stop_writes_noxdr = false;
			} else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "forward-xdr-writes", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of forward-xdr-writes from %s to %s", bool_val[g_config.xdr_cfg.xdr_forward_xdrwrites], context);
				g_config.xdr_cfg.xdr_forward_xdrwrites = true;
			} else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of forward-xdr-writes from %s to %s", bool_val[g_config.xdr_cfg.xdr_forward_xdrwrites], context);
				g_config.xdr_cfg.xdr_forward_xdrwrites = false;
			} else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "xdr-delete-shipping-enabled", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of xdr-delete-shipping-enabled from %s to %s", bool_val[g_config.xdr_cfg.xdr_delete_shipping_enabled], context);
				g_config.xdr_cfg.xdr_delete_shipping_enabled = true;
			} else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of xdr-delete-shipping-enabled from %s to %s", bool_val[g_config.xdr_cfg.xdr_delete_shipping_enabled], context);
				g_config.xdr_cfg.xdr_delete_shipping_enabled = false;
			} else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "xdr-nsup-deletes-enabled", context, &context_len)) {
			if (strncmp(context, "true", 4) == 0 || strncmp(context, "yes", 3) == 0) {
				cf_info(AS_INFO, "Changing value of xdr-nsup-deletes-enabled from %s to %s", bool_val[g_config.xdr_cfg.xdr_nsup_deletes_enabled], context);
				g_config.xdr_cfg.xdr_nsup_deletes_enabled = true;
			} else if (strncmp(context, "false", 5) == 0 || strncmp(context, "no", 2) == 0) {
				cf_info(AS_INFO, "Changing value of xdr-nsup-deletes-enabled from %s to %s", bool_val[g_config.xdr_cfg.xdr_nsup_deletes_enabled], context);
				g_config.xdr_cfg.xdr_nsup_deletes_enabled = false;
			} else {
				goto Error;
			}
		}
		else
			goto Error;
	}
	else
		goto Error;

	cf_dyn_buf_append_string(db, "ok");
	return(0);

Error:
	cf_dyn_buf_append_string(db, "error");
	return(0);
}

//
// log-set:log=id;context=foo;level=bar
// ie:
//   log-set:log=0;context=rw;level=debug


int
info_command_log_set(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "log-set command received: params %s", params);

	char id_str[50];
	int  id_str_len = sizeof(id_str);
	int  id = -1;
	bool found_id = true;
	cf_fault_sink *s = 0;

	if (0 != as_info_parameter_get(params, "id", id_str, &id_str_len)) {
		if (0 != as_info_parameter_get(params, "log", id_str, &id_str_len)) {
			cf_debug(AS_INFO, "log set command: no log id to be set - doing all");
			found_id = false;
		}
	}
	if (found_id == true) {
		if (0 != cf_str_atoi(id_str, &id) ) {
			cf_info(AS_INFO, "log set command: id must be an integer, is: %s", id_str);
			cf_dyn_buf_append_string(db, "error-id-not-integer");
			return(0);
		}
		s = cf_fault_sink_get_id(id);
		if (!s) {
			cf_info(AS_INFO, "log set command: sink id %d invalid", id);
			cf_dyn_buf_append_string(db, "error-bad-id");
			return(0);
		}
	}

	// now, loop through all context strings. If we find a known context string,
	// do the set
	for (int c_id = 0; c_id < CF_FAULT_CONTEXT_UNDEF; c_id++) {

		char level_str[50];
		int  level_str_len = sizeof(level_str);
		char *context = cf_fault_context_strings[c_id];
		if (0 != as_info_parameter_get(params, context, level_str, &level_str_len)) {
			continue;
		}
		for (uint i = 0; level_str[i]; i++) level_str[i] = toupper(level_str[i]);

		if (0 != cf_fault_sink_addcontext(s, context, level_str)) {
			cf_info(AS_INFO, "log set command: addcontext failed: context %s level %s", context, level_str);
			cf_dyn_buf_append_string(db, "error-invalid-context-or-level");
			return(0);
		}
	}

	cf_info(AS_INFO, "log-set command executed: params %s", params);

	cf_dyn_buf_append_string(db, "ok");

	return(0);
}


// latency:hist=reads;back=180;duration=60;slice=10;
// throughput:hist=reads;back=180;duration=60;slice=10;
// hist-track-start:hist=reads;back=43200;slice=30;thresholds=1,4,16,64;
// hist-track-stop:hist=reads;
//
// hist     - optional histogram name - if none, command applies to all cf_hist_track objects
//
// for start command:
// back     - total time span in seconds over which to cache data
// slice    - period in seconds at which to cache histogram data
// thresholds - comma-separated bucket (ms) values to track, must be powers of 2. e.g:
//				1,4,16,64
// defaults are:
// - config value for back - mandatory, serves as flag for tracking
// - config value if it exists for slice, otherwise 10 seconds
// - config value if it exists for thresholds, otherwise internal defaults (1,8,64)
//
// for query commands:
// back     - start search this many seconds before now, default: minimum to get last slice
//			  using back=0 will get cached data from oldest cached data
// duration - seconds (forward) from start to search, default 0: everything to present
// slice    - intervals (in seconds) to analyze, default 0: everything as one slice
//
// e.g. query:
// latency:hist=reads;back=180;duration=60;slice=10;
// output (CF_HIST_TRACK_FMT_PACKED format) is:
// requested value  latency:hist=reads;back=180;duration=60;slice=10
// value is  reads:23:26:24-GMT,ops/sec,>1ms,>8ms,>64ms;23:26:34,30618.2,0.05,0.00,0.00;
// 23:26:44,31942.1,0.02,0.00,0.00;23:26:54,30966.9,0.01,0.00,0.00;23:27:04,30380.4,0.01,0.00,0.00;
// 23:27:14,37833.6,0.01,0.00,0.00;23:27:24,38502.7,0.01,0.00,0.00;23:27:34,39191.4,0.02,0.00,0.00;
//
// explanation:
// 23:26:24-GMT - timestamp of histogram starting first slice
// ops/sec,>1ms,>8ms,>64ms - labels for the columns: throughput, and which thresholds
// 23:26:34,30618.2,0.05,0.00,0.00; - timestamp of histogram ending slice, throughput, latencies

int
info_command_hist_track(char *name, char *params, cf_dyn_buf *db)
{
	cf_debug(AS_INFO, "hist track %s command received: params %s", name, params);

	char value_str[50];
	int  value_str_len = sizeof(value_str);
	cf_hist_track* hist_p = NULL;

	if (0 != as_info_parameter_get(params, "hist", value_str, &value_str_len)) {
		cf_debug(AS_INFO, "hist track %s command: no histogram specified - doing all", name);
	}
	else {
		if (0 == strcmp(value_str, "reads")) {
			hist_p = g_config.rt_hist;
		}
		else if (0 == strcmp(value_str, "writes") || 0 == strcmp(value_str, "writes_master")) {
			hist_p = g_config.wt_hist;
		}
		else if (0 == strcmp(value_str, "proxy")) {
			hist_p = g_config.px_hist;
		}
		else if (0 == strcmp(value_str, "writes_reply")) {
			hist_p = g_config.wt_reply_hist;
		}
		else if (0 == strcmp(value_str, "udf")) {
			hist_p = g_config.ut_hist;
		}
		else if (0 == strcmp(value_str, "query")) {
			hist_p = g_config.q_hist;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: unrecognized histogram: %s", name, value_str);
			cf_dyn_buf_append_string(db, "error-bad-hist-name");
			return 0;
		}
	}

	if (0 == strcmp(name, "hist-track-stop")) {
		if (hist_p) {
			cf_hist_track_stop(hist_p);
		}
		else {
			cf_hist_track_stop(g_config.rt_hist);
			cf_hist_track_stop(g_config.wt_hist);
			cf_hist_track_stop(g_config.px_hist);
			cf_hist_track_stop(g_config.wt_reply_hist);
			cf_hist_track_stop(g_config.ut_hist);
			cf_hist_track_stop(g_config.q_rcnt_hist);
			cf_hist_track_stop(g_config.q_hist);
		}

		cf_dyn_buf_append_string(db, "ok");

		return 0;
	}

	bool start_cmd = 0 == strcmp(name, "hist-track-start");

	// Note - default query params will get the most recent saved slice.
	uint32_t back_sec = start_cmd ? g_config.hist_track_back : (g_config.hist_track_slice * 2) - 1;
	uint32_t slice_sec = start_cmd ? g_config.hist_track_slice : 0;
	int i;

	value_str_len = sizeof(value_str);

	if (0 == as_info_parameter_get(params, "back", value_str, &value_str_len)) {
		if (0 == cf_str_atoi(value_str, &i)) {
			back_sec = i >= 0 ? (uint32_t)i : (uint32_t)-i;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: back is not a number, using default", name);
		}
	}

	value_str_len = sizeof(value_str);

	if (0 == as_info_parameter_get(params, "slice", value_str, &value_str_len)) {
		if (0 == cf_str_atoi(value_str, &i)) {
			slice_sec = i >= 0 ? (uint32_t)i : (uint32_t)-i;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: slice is not a number, using default", name);
		}
	}

	if (start_cmd) {
		char* thresholds = g_config.hist_track_thresholds;

		value_str_len = sizeof(value_str);

		if (0 == as_info_parameter_get(params, "thresholds", value_str, &value_str_len)) {
			thresholds = value_str;
		}

		cf_debug(AS_INFO, "hist track start command: back %u, slice %u, thresholds %s",
				back_sec, slice_sec, thresholds ? thresholds : "null");

		if (hist_p) {
			if (cf_hist_track_start(hist_p, back_sec, slice_sec, thresholds)) {
				cf_dyn_buf_append_string(db, "ok");
			}
			else {
				cf_dyn_buf_append_string(db, "error-bad-start-params");
			}
		}
		else {
			if (cf_hist_track_start(g_config.rt_hist, back_sec, slice_sec, thresholds) &&
				cf_hist_track_start(g_config.wt_hist, back_sec, slice_sec, thresholds) &&
				cf_hist_track_start(g_config.px_hist, back_sec, slice_sec, thresholds) &&
				cf_hist_track_start(g_config.q_hist, back_sec, slice_sec, thresholds) &&
				cf_hist_track_start(g_config.q_rcnt_hist, back_sec, slice_sec, thresholds) &&
				cf_hist_track_start(g_config.wt_reply_hist, back_sec, slice_sec, thresholds) &&
				cf_hist_track_start(g_config.ut_hist, back_sec, slice_sec, thresholds)) {

				cf_dyn_buf_append_string(db, "ok");
			}
			else {
				cf_dyn_buf_append_string(db, "error-bad-start-params");
			}
		}

		return 0;
	}

	// From here on it's latency or throughput...

	uint32_t duration_sec = 0;

	value_str_len = sizeof(value_str);

	if (0 == as_info_parameter_get(params, "duration", value_str, &value_str_len)) {
		if (0 == cf_str_atoi(value_str, &i)) {
			duration_sec = i >= 0 ? (uint32_t)i : (uint32_t)-i;
		}
		else {
			cf_info(AS_INFO, "hist track %s command: duration is not a number, using default", name);
		}
	}

	bool throughput_only = 0 == strcmp(name, "throughput");

	cf_debug(AS_INFO, "hist track %s command: back %u, duration %u, slice %u",
			name, back_sec, duration_sec, slice_sec);

	if (hist_p) {
		cf_hist_track_get_info(hist_p, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
	}
	else {
		cf_hist_track_get_info(g_config.rt_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
		cf_hist_track_get_info(g_config.wt_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
		cf_hist_track_get_info(g_config.px_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
		cf_hist_track_get_info(g_config.wt_reply_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
		cf_hist_track_get_info(g_config.ut_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
		cf_hist_track_get_info(g_config.q_hist, back_sec, duration_sec, slice_sec, throughput_only, CF_HIST_TRACK_FMT_PACKED, db);
	}

	return 0;
}


// Generic info system functions
// These functions act when an INFO message comes in over the PROTO pipe
// collects the static and dynamic portions, puts it in a 'dyn buf',
// and sends a reply
//

// Error strings for security check results.
static void
append_sec_err_str(cf_dyn_buf *db, uint32_t result, as_sec_perm cmd_perm) {
	switch (result) {
	case AS_SEC_ERR_NOT_AUTHENTICATED:
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_uint32(db, result);
		cf_dyn_buf_append_string(db, ":not authenticated");
		return;
	case AS_SEC_ERR_ROLE_VIOLATION:
		switch (cmd_perm) {
		case PERM_INDEX_MANAGE:
			INFO_COMMAND_SINDEX_FAILCODE(result, "role violation");
			return;
		case PERM_UDF_MANAGE:
			cf_dyn_buf_append_string(db, "error=role_violation");
			return;
		default:
			break;
		}
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_uint32(db, result);
		cf_dyn_buf_append_string(db, ":role violation");
		return;
	default:
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_uint32(db, result);
		cf_dyn_buf_append_string(db, ":unexpected security error");
		return;
	}
}

static pthread_mutex_t		g_info_lock = PTHREAD_MUTEX_INITIALIZER;
info_static		*static_head = 0;
info_dynamic	*dynamic_head = 0;
info_tree		*tree_head = 0;
info_command	*command_head = 0;
//
// Pull up all elements in both list into the buffers
// (efficient enough if you're looking for lots of things)
// But only gets 'default' values
//

int
info_all(const as_file_handle* fd_h, cf_dyn_buf *db)
{
	uint8_t auth_result = as_security_check(fd_h, PERM_NONE);

	if (auth_result != AS_PROTO_RESULT_OK) {
		as_security_log(fd_h, auth_result, PERM_NONE, "info-all request", NULL);
		append_sec_err_str(db, auth_result, PERM_NONE);
		cf_dyn_buf_append_char(db, EOL);
		return 0;
	}

	info_static *s = static_head;
	while (s) {
		if (s->def == true) {
			cf_dyn_buf_append_string( db, s->name);
			cf_dyn_buf_append_char( db, SEP );
			cf_dyn_buf_append_buf( db, (uint8_t *) s->value, s->value_sz);
			cf_dyn_buf_append_char( db, EOL );
		}
		s = s->next;
	}

	info_dynamic *d = dynamic_head;
	while (d) {
		if (d->def == true) {
			cf_dyn_buf_append_string( db, d->name);
			cf_dyn_buf_append_char(db, SEP );
			d->value_fn(d->name, db);
			cf_dyn_buf_append_char(db, EOL);
		}
		d = d->next;
	}

	return(0);
}

//
// Parse the input buffer. It contains a list of keys that should be spit back.
// Do the parse, call the necessary function collecting the information in question
// Filling the dynbuf

int
info_some(char *buf, char *buf_lim, const as_file_handle* fd_h, cf_dyn_buf *db)
{
	uint8_t auth_result = as_security_check(fd_h, PERM_NONE);

	if (auth_result != AS_PROTO_RESULT_OK) {
		// TODO - log null-terminated buf as detail?
		as_security_log(fd_h, auth_result, PERM_NONE, "info request", NULL);
		append_sec_err_str(db, auth_result, PERM_NONE);
		cf_dyn_buf_append_char(db, EOL);
		return 0;
	}

	// For each incoming name
	char	*c = buf;
	char	*tok = c;

	while (c < buf_lim) {

		if ( *c == EOL ) {
			*c = 0;
			char *name = tok;
			bool handled = false;

			// search the static queue first always
			info_static *s = static_head;
			while (s) {
				if (strcmp(s->name, name) == 0) {
					// return exact command string received from client
					cf_dyn_buf_append_string( db, name);
					cf_dyn_buf_append_char( db, SEP );
					cf_dyn_buf_append_buf( db, (uint8_t *) s->value, s->value_sz);
					cf_dyn_buf_append_char( db, EOL );
					handled = true;
					break;
				}
				s = s->next;
			}

			// didn't find in static, try dynamic
			if (!handled) {
				info_dynamic *d = dynamic_head;
				while (d) {
					if (strcmp(d->name, name) == 0) {
						// return exact command string received from client
						cf_dyn_buf_append_string( db, d->name);
						cf_dyn_buf_append_char(db, SEP );
						d->value_fn(d->name, db);
						cf_dyn_buf_append_char(db, EOL);
						handled = true;
						break;
					}
					d = d->next;
				}
			}

			// search the tree
			if (!handled) {

				// see if there's a '/',
				char *branch = strchr( name, TREE_SEP);
				if (branch) {
					*branch = 0;
					branch++;

					info_tree *t = tree_head;
					while (t) {
						if (strcmp(t->name, name) == 0) {
							// return exact command string received from client
							cf_dyn_buf_append_string( db, t->name);
							cf_dyn_buf_append_char( db, TREE_SEP);
							cf_dyn_buf_append_string( db, branch);
							cf_dyn_buf_append_char(db, SEP );
							t->tree_fn(t->name, branch, db);
							cf_dyn_buf_append_char(db, EOL);
							handled = true;
							break;
						}
						t = t->next;
					}
				}
			}

			tok = c + 1;
		}
		// commands have parameters
		else if ( *c == ':' ) {
			*c = 0;
			char *name = tok;

			// parse parameters
			tok = c + 1;
			while (*c != EOL) c++;
			*c = 0;
			char *param = tok;

			// search the command list
			info_command *cmd = command_head;
			while (cmd) {
				if (strcmp(cmd->name, name) == 0) {
					// return exact command string received from client
					cf_dyn_buf_append_string( db, name);
					cf_dyn_buf_append_char( db, ':');
					cf_dyn_buf_append_string( db, param);
					cf_dyn_buf_append_char( db, SEP );

					uint8_t result = as_security_check(fd_h, cmd->required_perm);

					as_security_log(fd_h, result, cmd->required_perm, name, param);

					if (result == AS_PROTO_RESULT_OK) {
						cmd->command_fn(cmd->name, param, db);
					}
					else {
						append_sec_err_str(db, result, cmd->required_perm);
					}

					cf_dyn_buf_append_char( db, EOL );
					break;
				}
				cmd = cmd->next;
			}

			if (!cmd) {
				cf_info(AS_INFO, "received command %s, not registered", name);
			}

			tok = c + 1;
		}

		c++;

	}
	return(0);
}

int
as_info_buffer(uint8_t *req_buf, size_t req_buf_len, cf_dyn_buf *rsp)
{

#ifdef USE_INFO_LOCK
	pthread_mutex_lock(&g_info_lock);
#endif

	// Either we'e doing all, or doing some
	if (req_buf_len == 0) {
		info_all(NULL, rsp);
	}
	else {
		info_some((char *)req_buf, (char *)(req_buf + req_buf_len), NULL, rsp);
	}

#ifdef USE_INFO_LOCK
	pthread_mutex_unlock(&g_info_lock);
#endif

	return(0);
}

//
// Worker threads!
// these actually do the work. There is a lot of network activity,
// writes and such, don't want to clog up the main queue
//

void *
thr_info_fn(void *gcc_is_ass)
{
	for ( ; ; ) {

		info_work work;

		if (0 != cf_queue_pop(g_info_work_q, &work, CF_QUEUE_FOREVER)) {
			cf_crash(AS_TSVC, "unable to pop from info work queue");
		}

		as_transaction *tr = &work.tr;
		as_proto *pr = (as_proto *) tr->msgp;

		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(info_q_wait_hist);

		// Allocate an output buffer sufficiently large to avoid ever resizing
		cf_dyn_buf_define_size(db, 128 * 1024);
		// write space for the header
		uint64_t	h = 0;
		cf_dyn_buf_append_buf(&db, (uint8_t *) &h, sizeof(h));

#ifdef USE_INFO_LOCK
		pthread_mutex_lock(&g_info_lock);
#endif

		// Either we'e doing all, or doing some
		if (pr->sz == 0) {
			info_all(tr->proto_fd_h, &db);
		}
		else {
			info_some((char *)pr->data, (char *)pr->data + pr->sz, tr->proto_fd_h, &db);
		}

#ifdef USE_INFO_LOCK
		pthread_mutex_unlock(&g_info_lock);
#endif

		MICROBENCHMARK_HIST_INSERT_AND_RESET_P(info_post_lock_hist);

		// write the proto header in the space we pre-wrote
		db.buf[0] = 2;
		db.buf[1] = 1;
		uint64_t	sz = db.used_sz - 8;
		db.buf[4] = (sz >> 24) & 0xff;
		db.buf[5] = (sz >> 16) & 0xff;
		db.buf[6] = (sz >> 8) & 0xff;
		db.buf[7] = sz & 0xff;

		// write the data buffer
		uint8_t	*b = db.buf;
		uint8_t	*lim = db.buf + db.used_sz;
		while (b < lim) {
			int rv = send(tr->proto_fd_h->fd, b, lim - b, MSG_NOSIGNAL);
			if ((rv < 0) && (errno != EAGAIN) ) {
				if (errno == EPIPE) {
					cf_debug(AS_INFO, "thr_info: client request gave up while I was processing: fd %d", tr->proto_fd_h->fd);
				} else {
					cf_info(AS_INFO, "thr_info: can't write all bytes, fd %d error %d", tr->proto_fd_h->fd, errno);
				}
				AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
				tr->proto_fd_h = 0;
				break;
			}
			else if (rv > 0)
				b += rv;
			else
				usleep(1);
		}

		cf_dyn_buf_free(&db);

		cf_free(tr->msgp);

		if (tr->proto_fd_h)	{
			tr->proto_fd_h->t_inprogress = false;
			AS_RELEASE_FILE_HANDLE(tr->proto_fd_h);
		}

		MICROBENCHMARK_HIST_INSERT_P(info_fulfill_hist);
	}
	return(0);

}

//
// received an info request from a file descriptor
// Called by the thr_tsvc when an info message is seen
// calls functions info_all or info_some to collect the response
// calls write to send the response back
//
// Proto will be freed by the caller
//

int
as_info(as_transaction *tr)
{
	info_work  work;

	work.tr = *tr;

	tr->proto_fd_h = 0;
	tr->msgp = 0;

	MICROBENCHMARK_HIST_INSERT_AND_RESET_P(info_tr_q_process_hist);

	if (0 != cf_queue_push(g_info_work_q, &work)) {
		cf_warning(AS_INFO, "PUSH FAILED: todo: kill info request file descriptor or something");
		return(-1);
	}

	return(0);
}

// Return the number of pending Info requests in the queue.
static int
as_info_queue_get_size()
{
	return cf_queue_sz(g_info_work_q);
}

// Registers a dynamic name-value calculator.
// the get_value_fn will be called if a request comes in for this name.
// only does the registration!
// def means it's part of the default set - will get returned if nothing is passed


int
as_info_set_dynamic(char *name, as_info_get_value_fn gv_fn, bool def)
{
	int rv = -1;
	pthread_mutex_lock(&g_info_lock);

	info_dynamic *e = dynamic_head;
	while (e) {
		if (strcmp(name, e->name) == 0) {
			e->value_fn = gv_fn;
			break;
		}

		e = e->next;
	}

	if (!e) {
		e = cf_malloc(sizeof(info_dynamic));
		if (!e) goto Cleanup;
		e->def = def;
		e->name = cf_strdup(name);
		if (!e->name) {
			cf_free(e);
			goto Cleanup;
		}
		e->value_fn = gv_fn;
		e->next = dynamic_head;
		dynamic_head = e;
	}
	rv = 0;
Cleanup:
	pthread_mutex_unlock(&g_info_lock);
	return(rv);
}


// Registers a tree-based name-value calculator.
// the get_value_fn will be called if a request comes in for this name.
// only does the registration!


int
as_info_set_tree(char *name, as_info_get_tree_fn gv_fn)
{
	int rv = -1;
	pthread_mutex_lock(&g_info_lock);

	info_tree *e = tree_head;
	while (e) {
		if (strcmp(name, e->name) == 0) {
			e->tree_fn = gv_fn;
			break;
		}

		e = e->next;
	}

	if (!e) {
		e = cf_malloc(sizeof(info_tree));
		if (!e) goto Cleanup;
		e->name = cf_strdup(name);
		if (!e->name) {
			cf_free(e);
			goto Cleanup;
		}
		e->tree_fn = gv_fn;
		e->next = tree_head;
		tree_head = e;
	}
	rv = 0;
Cleanup:
	pthread_mutex_unlock(&g_info_lock);
	return(rv);
}


// Registers a command handler
// the get_value_fn will be called if a request comes in for this name, and
// parameters will be passed in
// This function only does the registration!

int
as_info_set_command(char *name, as_info_command_fn command_fn, as_sec_perm required_perm)
{
	int rv = -1;
	pthread_mutex_lock(&g_info_lock);

	info_command *e = command_head;
	while (e) {
		if (strcmp(name, e->name) == 0) {
			e->command_fn = command_fn;
			break;
		}

		e = e->next;
	}

	if (!e) {
		e = cf_malloc(sizeof(info_command));
		if (!e) goto Cleanup;
		e->name = cf_strdup(name);
		if (!e->name) {
			cf_free(e);
			goto Cleanup;
		}
		e->command_fn = command_fn;
		e->required_perm = required_perm;
		e->next = command_head;
		command_head = e;
	}
	rv = 0;
Cleanup:
	pthread_mutex_unlock(&g_info_lock);
	return(rv);
}



//
// Sets a static name-value pair
// def means it's part of the default set - will get returned if nothing is passed

int
as_info_set_buf(const char *name, const uint8_t *value, size_t value_sz, bool def)
{
	pthread_mutex_lock(&g_info_lock);

	// Delete case
	if (value_sz == 0 || value == 0) {

		info_static *p = 0;
		info_static *e = static_head;

		while (e) {
			if (strcmp(name, e->name) == 0) {
				if (p) {
					p->next = e->next;
					cf_free(e->name);
					cf_free(e->value);
					cf_free(e);
				}
				else {
					info_static *_t = static_head->next;
					cf_free(e->name);
					cf_free(e->value);
					cf_free(static_head);
					static_head = _t;
				}
				break;
			}
			p = e;
			e = e->next;
		}
	}
	// insert case
	else {

		info_static *e = static_head;

		// search for old value and overwrite
		while(e) {
			if (strcmp(name, e->name) == 0) {
				cf_free(e->value);
				e->value = cf_malloc(value_sz);
				memcpy(e->value, value, value_sz);
				e->value_sz = value_sz;
				break;
			}
			e = e->next;
		}

		// not found, insert fresh
		if (e == 0) {
			info_static *_t = cf_malloc(sizeof(info_static));
			_t->next = static_head;
			_t->def = def;
			_t->name = cf_strdup(name);
			_t->value = cf_malloc(value_sz);
			memcpy(_t->value, value, value_sz);
			_t->value_sz = value_sz;
			static_head = _t;
		}
	}

	pthread_mutex_unlock(&g_info_lock);
	return(0);

}

//
// A helper function. Commands have the form:
// cmd:param=value;param=value
//
// The main parser gives us the entire parameter string
// so use this function to scan through and get the particular parameter value
// you're looking for
//
// The 'param_string' is the param passed by the command parser into a command
//
// @return  0 : success
//         -1 : parameter not found
//         -2 : parameter found but value is too long
//

int
as_info_parameter_get(char *param_str, char *param, char *value, int *value_len)
{
	cf_detail(AS_INFO, "parameter get: paramstr %s seeking param %s", param_str, param);

	char *c = param_str;
	char *tok = param_str;
	int param_len = strlen(param);

	while (*c) {
		if (*c == '=') {
			if ( ( param_len == c - tok) && (0 == memcmp(tok, param, param_len) ) ) {
				c++;
				tok = c;
				while ( *c != 0 && *c != ';') c++;
				if (*value_len <= c - tok)	{
					// The found value is too long.
					return(-2);
				}
				*value_len = c - tok;
				memcpy(value, tok, *value_len);
				value[*value_len] = 0;
				return(0);
			}
			c++;
		}
		else if (*c == ';') {
			c++;
			tok = c;
		}
		else c++;

	}

	return(-1);
}

int
as_info_set(const char *name, const char *value, bool def)
{
	return(as_info_set_buf(name, (const uint8_t *) value, strlen(value), def ) );
}

static pthread_t info_debug_ticker_th;

void *
info_debug_ticker_fn(void *gcc_is_ass)
{
	size_t total_ns_memory_inuse = 0;

	// Helps to know how many messages are going in an out, some general status
	do {
		struct timespec delay = { g_config.ticker_interval, 0 }; // this is the time between log lines - should be a config parameter
		nanosleep(&delay, NULL);

		if ((g_config.paxos == 0) || (g_config.paxos->ready == false)) {
			cf_info(AS_INFO, " fabric: cluster not ready yet");
		} else {

			uint64_t freemem;
			int		 freepct;
			bool     swapping = false;
			cf_meminfo(0, &freemem, &freepct, &swapping);
			cf_info(AS_INFO, " system memory: free %"PRIu64"kb ( %d percent free ) %s",
					freemem / 1024,
					freepct,
					(swapping == true) ? "SWAPPING!" : ""
					);

			cf_info(AS_INFO, " migrates in progress ( %d , %d ) ::: ClusterSize %zd ::: objects %"PRIu64" ::: sub_objects %"PRIu64,
					cf_atomic32_get(g_config.migrate_progress_send),
					cf_atomic32_get(g_config.migrate_progress_recv),
					g_config.paxos->cluster_size,  // add real cluster size when srini has it
					thr_info_get_object_count(),
					thr_info_get_subobject_count()
					);
			cf_info(AS_INFO, " rec refs %"PRIu64" ::: rec locks %"PRIu64" ::: trees %"PRIu64" ::: wr reqs %"PRIu64" ::: mig tx %"PRIu64" ::: mig rx %"PRIu64"",
					cf_atomic_int_get(g_config.global_record_ref_count),
					cf_atomic_int_get(g_config.global_record_lock_count),
					cf_atomic_int_get(g_config.global_tree_count),
					cf_atomic_int_get(g_config.write_req_object_count),
					cf_atomic_int_get(g_config.migrate_tx_object_count),
					cf_atomic_int_get(g_config.migrate_rx_object_count)
				 	);
			cf_info(AS_INFO, " replica errs :: null %"PRIu64" non-null %"PRIu64" ::: sync copy errs :: node %"PRIu64" :: master %"PRIu64" ",
					cf_atomic_int_get(g_config.err_replica_null_node),
					cf_atomic_int_get(g_config.err_replica_non_null_node),
					cf_atomic_int_get(g_config.err_sync_copy_null_node),
					cf_atomic_int_get(g_config.err_sync_copy_null_master)
					);

			cf_info(AS_INFO, "   trans_in_progress: wr %d prox %d wait %d ::: q %d ::: bq %d ::: iq %d ::: dq %d : fds - proto (%d, %"PRIu64", %"PRIu64") : hb (%d, %"PRIu64", %"PRIu64") : fab (%d, %"PRIu64", %"PRIu64")",
					as_write_inprogress(), as_proxy_inprogress(), g_config.n_waiting_transactions, thr_tsvc_queue_get_size(), as_batch_queue_size(), as_info_queue_get_size(), as_nsup_queue_get_size(),
					g_config.proto_connections_opened - g_config.proto_connections_closed,
					g_config.proto_connections_opened, g_config.proto_connections_closed,
					g_config.heartbeat_connections_opened - g_config.heartbeat_connections_closed,
					g_config.heartbeat_connections_opened, g_config.heartbeat_connections_closed,
					g_config.fabric_connections_opened - g_config.fabric_connections_closed,
					g_config.fabric_connections_opened, g_config.fabric_connections_closed
					);

			cf_info(AS_INFO, "   heartbeat_received: self %lu : foreign %lu", g_config.heartbeat_received_self, g_config.heartbeat_received_foreign);
			cf_info(AS_INFO, "   heartbeat_stats: %s", as_hb_stats(false));

			cf_info(AS_INFO, "   tree_counts: nsup %"PRIu64" scan %"PRIu64" batch %"PRIu64" dup %"PRIu64" wprocess %"PRIu64" migrx %"PRIu64" migtx %"PRIu64" ssdr %"PRIu64" ssdw %"PRIu64" rw %"PRIu64"",
					cf_atomic_int_get(g_config.nsup_tree_count),
					cf_atomic_int_get(g_config.scan_tree_count),
					cf_atomic_int_get(g_config.batch_tree_count),
					cf_atomic_int_get(g_config.dup_tree_count),
					cf_atomic_int_get(g_config.wprocess_tree_count),
					cf_atomic_int_get(g_config.migrx_tree_count),
					cf_atomic_int_get(g_config.migtx_tree_count),
					cf_atomic_int_get(g_config.ssdr_tree_count),
					cf_atomic_int_get(g_config.ssdw_tree_count),
					cf_atomic_int_get(g_config.rw_tree_count)
					);

			// namespace disk and memory size and ldt gc stats
			total_ns_memory_inuse = 0;
			for (int i = 0; i < g_config.namespaces; i++) {
				as_namespace *ns = g_config.namespace[i];
				int available_pct;
				uint64_t inuse_disk_bytes;
				as_storage_stats(ns, &available_pct, &inuse_disk_bytes);
				size_t ns_memory_inuse = ns->n_bytes_memory + (as_index_size_get(ns) * ns->n_objects);
				if (ns->storage_data_in_memory) {
					cf_info(AS_INFO, "namespace %s: disk inuse: %"PRIu64" memory inuse: %"PRIu64" (bytes) "
							"sindex memory inuse: %"PRIu64" (bytes) "
							"avail pct %d",
							ns->name, inuse_disk_bytes, ns_memory_inuse,
							ns->sindex_data_memory_used,
							available_pct);
					if (ns->ldt_enabled) {
						uint64_t cnt              = cf_atomic_int_get(ns->lstats.ldt_gc_processed);
						uint64_t io               = cf_atomic_int_get(ns->lstats.ldt_gc_io);
						uint64_t gc               = cf_atomic_int_get(ns->lstats.ldt_gc_cnt);
						uint64_t no_esr           = cf_atomic_int_get(ns->lstats.ldt_gc_no_esr_cnt);
						uint64_t no_parent        = cf_atomic_int_get(ns->lstats.ldt_gc_no_parent_cnt);
						uint64_t version_mismatch = cf_atomic_int_get(ns->lstats.ldt_gc_parent_version_mismatch_cnt);
						cf_info(AS_INFO, "namespace %s: ldt_gc: cnt %"PRIu64" io %"PRIu64" gc %"PRIu64" (%"PRIu64", %"PRIu64", %"PRIu64")",
								ns->name, cnt, io, gc, no_esr, no_parent, version_mismatch);
					}
				}
				else {
					uint32_t n_reads_from_cache = cf_atomic32_get(ns->n_reads_from_cache);
					uint32_t n_total_reads = cf_atomic32_get(ns->n_reads_from_device) + n_reads_from_cache;
					cf_atomic32_set(&ns->n_reads_from_device, 0);
					cf_atomic32_set(&ns->n_reads_from_cache, 0);
					ns->cache_read_pct = (float)(100 * n_reads_from_cache) / (float)(n_total_reads == 0 ? 1 : n_total_reads);

					cf_info(AS_INFO, "namespace %s: disk inuse: %"PRIu64" memory inuse: %"PRIu64" (bytes) "
							"sindex memory inuse: %"PRIu64" (bytes) "
							"avail pct %d cache-read pct %.2f",
							ns->name, inuse_disk_bytes, ns_memory_inuse,
							ns->sindex_data_memory_used,
							available_pct,
							ns->cache_read_pct);
					if (ns->ldt_enabled) {
						uint64_t cnt              = cf_atomic_int_get(ns->lstats.ldt_gc_processed);
						uint64_t io               = cf_atomic_int_get(ns->lstats.ldt_gc_io);
						uint64_t gc               = cf_atomic_int_get(ns->lstats.ldt_gc_cnt);
						uint64_t no_esr           = cf_atomic_int_get(ns->lstats.ldt_gc_no_esr_cnt);
						uint64_t no_parent        = cf_atomic_int_get(ns->lstats.ldt_gc_no_parent_cnt);
						uint64_t version_mismatch = cf_atomic_int_get(ns->lstats.ldt_gc_parent_version_mismatch_cnt);
						cf_info(AS_INFO, "namespace %s: ldt_gc: cnt %"PRIu64" io %"PRIu64" gc %"PRIu64" (%"PRIu64", %"PRIu64", %"PRIu64")",
								ns->name, cnt, io, gc, no_esr, no_parent, version_mismatch);
					}
				}

				total_ns_memory_inuse += ns_memory_inuse;
				as_sindex_histogram_dumpall(ns);
			}

			as_partition_states ps;
			info_partition_getstates(&ps);
			cf_info(AS_INFO, "   partitions: actual %d sync %d desync %d zombie %d wait %d absent %d",
					ps.sync_actual, ps.sync_replica, ps.desync, ps.zombie, ps.wait, ps.absent);

			if (g_config.rt_hist)
				cf_hist_track_dump(g_config.rt_hist);
			if (g_config.wt_hist)
				cf_hist_track_dump(g_config.wt_hist);
			if (g_config.px_hist)
				cf_hist_track_dump(g_config.px_hist);
			if (g_config.wt_reply_hist)
				cf_hist_track_dump(g_config.wt_reply_hist);
			if (g_config.ut_hist)
				cf_hist_track_dump(g_config.ut_hist);
			if (g_config.q_hist)
				cf_hist_track_dump(g_config.q_hist);
			if (g_config.q_rcnt_hist)
				cf_hist_track_dump(g_config.q_rcnt_hist);

			as_query_histogram_dumpall();
			as_sindex_gc_histogram_dumpall();

			if (g_config.microbenchmarks) {
				if (g_config.rt_cleanup_hist)
					histogram_dump(g_config.rt_cleanup_hist);
				if (g_config.rt_net_hist)
					histogram_dump(g_config.rt_net_hist);
				if (g_config.wt_net_hist)
					histogram_dump(g_config.wt_net_hist);
				if (g_config.rt_storage_read_hist)
					histogram_dump(g_config.rt_storage_read_hist);
				if (g_config.rt_storage_open_hist)
					histogram_dump(g_config.rt_storage_open_hist);
				if (g_config.rt_tree_hist)
					histogram_dump(g_config.rt_tree_hist);
				if (g_config.rt_internal_hist)
					histogram_dump(g_config.rt_internal_hist);
				if (g_config.wt_internal_hist)
					histogram_dump(g_config.wt_internal_hist);
				if (g_config.rt_start_hist)
					histogram_dump(g_config.rt_start_hist);
				if (g_config.wt_start_hist)
					histogram_dump(g_config.wt_start_hist);
				if (g_config.rt_q_process_hist)
					histogram_dump(g_config.rt_q_process_hist);
				if (g_config.wt_q_process_hist)
					histogram_dump(g_config.wt_q_process_hist);
				if (g_config.q_wait_hist)
					histogram_dump(g_config.q_wait_hist);
				if (g_config.demarshal_hist)
					histogram_dump(g_config.demarshal_hist);
				if (g_config.wt_master_wait_prole_hist)
					histogram_dump(g_config.wt_master_wait_prole_hist);
				if (g_config.wt_prole_hist)
					histogram_dump(g_config.wt_prole_hist);
				if (g_config.rt_resolve_hist)
					histogram_dump(g_config.rt_resolve_hist);
				if (g_config.wt_resolve_hist)
					histogram_dump(g_config.wt_resolve_hist);
				if (g_config.rt_resolve_wait_hist)
					histogram_dump(g_config.rt_resolve_wait_hist);
				if (g_config.wt_resolve_wait_hist)
					histogram_dump(g_config.wt_resolve_wait_hist);
				if (g_config.error_hist)
					histogram_dump(g_config.error_hist);
				if (g_config.batch_q_process_hist)
					histogram_dump(g_config.batch_q_process_hist);
				if (g_config.info_tr_q_process_hist)
					histogram_dump(g_config.info_tr_q_process_hist);
				if (g_config.info_q_wait_hist)
					histogram_dump(g_config.info_q_wait_hist);
				if (g_config.info_post_lock_hist)
					histogram_dump(g_config.info_post_lock_hist);
				if (g_config.info_fulfill_hist)
					histogram_dump(g_config.info_fulfill_hist);
				if (g_config.write_storage_close_hist)
					histogram_dump(g_config.write_storage_close_hist);
				if (g_config.write_sindex_hist)
					histogram_dump(g_config.write_sindex_hist);
				if (g_config.defrag_storage_close_hist)
					histogram_dump(g_config.defrag_storage_close_hist);
				if (g_config.prole_fabric_send_hist)
					histogram_dump(g_config.prole_fabric_send_hist);
			}

			if (g_config.storage_benchmarks) {
				as_storage_ticker_stats();
			}

			if (g_config.ldt_benchmarks) {
				histogram_dump(g_config.ldt_multiop_prole_hist);
				histogram_dump(g_config.ldt_update_record_cnt_hist);
				histogram_dump(g_config.ldt_io_record_cnt_hist);
				histogram_dump(g_config.ldt_update_io_bytes_hist);
				histogram_dump(g_config.ldt_hist);
			}
#ifdef HISTOGRAM_OBJECT_LATENCY
			if (g_config.read0_hist)
				histogram_dump(g_config.read0_hist);
			if (g_config.read1_hist)
				histogram_dump(g_config.read1_hist);
			if (g_config.read2_hist)
				histogram_dump(g_config.read2_hist);
			if (g_config.read3_hist)
				histogram_dump(g_config.read3_hist);
			if (g_config.read4_hist)
				histogram_dump(g_config.read4_hist);
			if (g_config.read5_hist)
				histogram_dump(g_config.read5_hist);
			if (g_config.read6_hist)
				histogram_dump(g_config.read6_hist);
			if (g_config.read7_hist)
				histogram_dump(g_config.read7_hist);
			if (g_config.read8_hist)
				histogram_dump(g_config.read8_hist);
			if (g_config.read9_hist)
				histogram_dump(g_config.read9_hist);
#endif

#ifdef MEM_COUNT
			if (g_config.memory_accounting) {
				mem_count_stats();
			}
#endif

#ifdef USE_ASM
			if (g_asm_hook_enabled) {
				static uint64_t iter = 0;
				static asm_stats_t *asm_stats = NULL;
				static vm_stats_t *vm_stats = NULL;
				size_t vm_size = 0;
				size_t total_accounted_memory = 0;

				as_asm_hook((void *) iter++, &asm_stats, &vm_stats);

				if (asm_stats) {
#ifdef DEBUG_ASM
					fprintf(stderr, "***THR_INFO:  asm:  mem_count: %lu ; net_mmaps: %lu ; net_shm: %lu***\n",
							asm_stats->mem_count, asm_stats->net_mmaps, asm_stats->net_shm);
#endif
					total_accounted_memory = asm_stats->mem_count + asm_stats->net_mmaps + asm_stats->net_shm;
				}

				if (vm_stats) {
					// N.B.:  The VM stats description is used implicitly by the accessor "vm_stats_*()" macros!
					vm_stats_desc_t *vm_stats_desc = vm_stats->desc;
					vm_size = vm_stats_get_key_value(vm_stats, VM_SIZE);
#ifdef DEBUG_ASM
					fprintf(stderr, "***THR_INFO:  vm:  %s: %lu KB; %s: %lu KB ; %s: %lu KB ; %s: %lu KB***\n",
							vm_stats_key_name(VM_PEAK),
							vm_stats_get_key_value(vm_stats, VM_PEAK),
							vm_stats_key_name(VM_SIZE),
							vm_size,
							vm_stats_key_name(VM_RSS),
							vm_stats_get_key_value(vm_stats, VM_RSS),
							vm_stats_key_name(VM_DATA),
							vm_stats_get_key_value(vm_stats, VM_DATA));
#endif

					// Convert from KB to B.
					vm_size *= 1024;

					// Calculate the storage efficiency percentages.
					double dynamic_eff = ((double) total_accounted_memory / (double) MAX(vm_size, 1)) * 100.0;
					double obj_eff = ((double) total_ns_memory_inuse / (double) MAX(vm_size, 1)) * 100.0;

#ifdef DEBUG_ASM
					fprintf(stderr, "VM size: %lu ; Total Accounted Memory: %lu (%.3f%%) ; Total NS Memory in use: %lu (%.3f%%)\n",
							vm_size, total_accounted_memory, dynamic_eff, total_ns_memory_inuse, obj_eff);
#endif
					cf_info(AS_INFO, "VM size: %lu ; Total Accounted Memory: %lu (%.3f%%) ; Total NS Memory in use: %lu (%.3f%%)",
							vm_size, total_accounted_memory, dynamic_eff, total_ns_memory_inuse, obj_eff);
				}
			}
#endif // defined(USE_ASM)

			if (g_mstats_enabled) {
				info_log_with_datestamp(malloc_stats);
			}

			if (g_config.fabric_dump_msgs) {
				as_fabric_msg_queue_dump();
			}
		}

	} while(1);

	return(0);
}

// This function is meant to kick-start the info debug ticker that prints
// system statistics. It should start only when the system is booted-up
// fully and is ready to take reads and writes. It should wait for
// the boot-time secondary index loading to be done.
void
info_debug_ticker_start()
{
	// Create a debug thread to pump out some statistics
	if (g_config.ticker_interval)
		pthread_create(&info_debug_ticker_th, 0, info_debug_ticker_fn, 0);
}


//
//
// service interfaces management
//
// There's a worker thread - info_interfaces_fn ---
// which continually polls the interfaces to see if anything changed.
// When it changes, it updates a generation count.
// There's a hash table of all the other nodes in the cluster, and a counter
// to see that they're all up-to-date on the generation
//
//
// The fabric message in question can be expanded to do more than service interfaces.
// By expanding the 'info_node_info' structure, and the fabric_msg, you can carry
// more dynamic information than just the remote node's interfaces
// But that's all that we can think of at the moment - the paxos communication method
// makes sure that the distributed key system is properly distributed
//

static pthread_t info_interfaces_th;


int
interfaces_compar(const void *a, const void *b)
{
	cf_ifaddr		*if_a = (cf_ifaddr *) a;
	cf_ifaddr		*if_b = (cf_ifaddr *) b;

	if (if_a->family != if_b->family) {
		if (if_a->family < if_b->family)	return(-1);
		else								return(1);
	}

	if (if_a->family == AF_INET) {
		struct sockaddr_in	*in_a = (struct sockaddr_in *) &if_a->sa;
		struct sockaddr_in  *in_b = (struct sockaddr_in *) &if_b->sa;

		return( memcmp( &in_a->sin_addr, &in_b->sin_addr, sizeof(in_a->sin_addr) ) );
	}
	else if (if_a->family == AF_INET6) {
		struct sockaddr_in6	*in_a = (struct sockaddr_in6 *) &if_a->sa;
		struct sockaddr_in6 *in_b = (struct sockaddr_in6 *) &if_b->sa;

		return( memcmp( &in_a->sin6_addr, &in_b->sin6_addr, sizeof(in_a->sin6_addr) ) );
	}

	cf_warning(AS_INFO, " interfaces compare: unknown families");
	return(0);
}

static pthread_mutex_t		g_service_lock = PTHREAD_MUTEX_INITIALIZER;
char 		*g_service_str = 0;
uint32_t	g_service_generation = 0;

//
// What other nodes are out there, and what are their ip addresses?
//

typedef struct {
	uint64_t	last;				// last notice we got from a given node
	char 		*service_addr;		// string representing the service address
	uint32_t	generation;			// acked generation counter
} info_node_info;


// To avoid the services bug, g_info_node_info_hash should *always* be a subset
// of g_info_node_info_history_hash. In order to ensure this, every modification
// of g_info_node_info_hash should first involve grabbing the lock for the same
// key in g_info_node_info_history_hash.
shash *g_info_node_info_history_hash = 0;
shash *g_info_node_info_hash = 0;

int info_node_info_reduce_fn(void *key, void *data, void *udata);


void
build_service_list(cf_ifaddr * ifaddr, int ifaddr_sz, cf_dyn_buf *db) {
	for (int i = 0; i < ifaddr_sz; i++) {

		if (ifaddr[i].family == AF_INET) {
			struct sockaddr_in *sin = (struct sockaddr_in *) & (ifaddr[i].sa);
			char    addr_str[50];

			inet_ntop(AF_INET, &sin->sin_addr, addr_str, sizeof(addr_str));
			if ( strcmp(addr_str, "127.0.0.1") == 0) continue;

			cf_dyn_buf_append_string(db, addr_str);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_int(db, g_config.socket.port);
			cf_dyn_buf_append_char(db, ';');
		}
	}
	
	// take off the last ';' if there was any string there
	if (db->used_sz > 0)
		cf_dyn_buf_chomp(db);
}


//
// Note: if all my interfaces go down, service_str will be 0
//
void *
info_interfaces_fn(void *gcc_is_ass)
{

	uint8_t	buf[512];

	// currently known set
	cf_ifaddr		known_ifs[100];
	int				known_ifs_sz = 0;

	while (1) {

		cf_ifaddr *ifaddr;
		int			ifaddr_sz;
		cf_ifaddr_get(&ifaddr, &ifaddr_sz, buf, sizeof(buf));

		bool changed = false;
		if (ifaddr_sz == known_ifs_sz) {
			// sort it
			qsort(ifaddr, ifaddr_sz, sizeof(cf_ifaddr), interfaces_compar);

			// Compare to the old list
			for (int i = 0; i < ifaddr_sz; i++) {
				if (0 != interfaces_compar( &known_ifs[i], &ifaddr[i])) {
					changed = true;
					break;
				}
			}
		} else { 
			changed = true;
		}

		if (changed == true) {

			cf_dyn_buf_define(service_db);

			build_service_list(ifaddr, ifaddr_sz, &service_db);

			memcpy(known_ifs, ifaddr, sizeof(cf_ifaddr) * ifaddr_sz);
			known_ifs_sz = ifaddr_sz;

			pthread_mutex_lock(&g_service_lock);

			if (g_service_str)	cf_free(g_service_str);
			g_service_str = cf_dyn_buf_strdup(&service_db);

			g_service_generation++;

			pthread_mutex_unlock(&g_service_lock);

		}

		// reduce the info_node hash to apply any transmits
		shash_reduce(g_info_node_info_hash, info_node_info_reduce_fn, 0);

		sleep(2);

	}
	return(0);
}

//
// pushes the external address to everyone in the fabric
//

void *
info_interfaces_static_fn(void *gcc_is_ass)
{

	cf_info(AS_INFO, " static external network definition ");

	// check external-address is matching with given addresses in service list 
	uint8_t buf[512];
	cf_ifaddr *ifaddr;
	int	ifaddr_sz;
	cf_ifaddr_get(&ifaddr, &ifaddr_sz, buf, sizeof(buf));

	cf_dyn_buf_define(temp_service_db);
	build_service_list(ifaddr, ifaddr_sz, &temp_service_db);

	char * service_str = cf_dyn_buf_strdup(&temp_service_db);
	if (! g_config.is_external_address_virtual && strstr(service_str, g_config.external_address) == NULL) {
		cf_crash(AS_INFO, "external address:%s is not matching with any of service addresses:%s",
				g_config.external_address, service_str);
	}

	cf_dyn_buf_free(&temp_service_db);
	cf_free(service_str);

	// For valid external-address specify the same in service-list 
	cf_dyn_buf_define(service_db);
	cf_dyn_buf_append_string(&service_db, g_config.external_address);
	cf_dyn_buf_append_char(&service_db, ':');
	cf_dyn_buf_append_int(&service_db, g_config.socket.port);

	pthread_mutex_lock(&g_service_lock);

	g_service_generation = 1;
	g_service_str = cf_dyn_buf_strdup(&service_db);

	pthread_mutex_unlock(&g_service_lock);

	cf_dyn_buf_free(&service_db);
	while (1) {

		// reduce the info_node hash to apply any transmits
		shash_reduce(g_info_node_info_hash, info_node_info_reduce_fn, 0);

		sleep(2);

	}
	return(0);
}

// This reduce function will eliminate elements from the info hash
// which are no longer in the succession list


int
info_paxos_event_reduce_fn(void *key, void *data, void *udata)
{
	cf_node		*node = (cf_node *) key;		// this is a single element
	cf_node		*succession = (cf_node *)	udata; // this is an array
	info_node_info *infop = (info_node_info *)data;

	uint i = 0;
	while (succession[i]) {
		if (*node == succession[i])
			break;
		i++;
	}

	if (succession[i] == 0) {
		cf_debug(AS_INFO, " paxos event reduce: removing node %"PRIx64, *node);
		if (infop->service_addr)    cf_free(infop->service_addr);
		return(SHASH_REDUCE_DELETE);
	}

	return(0);

}

//
// Maintain the info_node_info hash as a shadow of the succession list
//

void
as_info_paxos_event(as_paxos_generation gen, as_paxos_change *change, cf_node succession[], void *udata)
{

	uint64_t start_ms = cf_getms();

	cf_debug(AS_INFO, "info received new paxos state:");

	// Make sure all elements in the succession list are in the hash
	info_node_info info;
	info.last = 0;
	info.generation = 0;

	pthread_mutex_t *vlock_info_hash;
	info_node_info *infop_info_hash;

	pthread_mutex_t *vlock_info_history_hash;
	info_node_info *infop_info_history_hash;

	uint i = 0;
	while (succession[i]) {
		if (succession[i] != g_config.self_node) {

			info.service_addr = 0;

			// Get lock for info_history_hash
			if (SHASH_OK != shash_get_vlock(g_info_node_info_history_hash,
					&(succession[i]), (void **) &infop_info_history_hash,
					&vlock_info_history_hash)) {
				// Node not in info_history_hash, so add it.

				// This may fail, but this is ok. This should only fail when
				// info_msg_fn is also trying to add this key, so either
				// way the entry will be in the hash table.
				shash_put_unique(g_info_node_info_history_hash,
								 &(succession[i]), &info);

				if (SHASH_OK != shash_get_vlock(g_info_node_info_history_hash,
						&(succession[i]), (void **) &infop_info_history_hash,
						&vlock_info_history_hash)) {
					cf_assert(false, AS_INFO, CF_CRITICAL,
							"could not create info_history_hash entry for %"PRIx64, &(succession[i]));
					continue;
				}
			}

			if (SHASH_OK != shash_get_vlock(g_info_node_info_hash, &(succession[i]),
					(void **) &infop_info_hash, &vlock_info_hash)) {
				if (infop_info_history_hash->service_addr) {
					// We remember the service address for this node!
					// Use this service address from info_history_hash in
					// the new entry into info_hash.
					cf_debug(AS_INFO, "info: from paxos notification: copying service address from info history hash for node %"PRIx64, succession[i]);
					info.service_addr = cf_strdup( infop_info_history_hash->service_addr );
					cf_assert(info.service_addr, AS_INFO, CF_CRITICAL, "malloc");
				}

				if (SHASH_OK == shash_put_unique(g_info_node_info_hash, &(succession[i]), &info)) {
					cf_debug(AS_INFO, "info: from paxos notification: inserted node %"PRIx64, succession[i]);
				} else {
					if (info.service_addr)	cf_free(info.service_addr);
					cf_assert(false, AS_INFO, CF_CRITICAL,
							"could not insert node %"PRIx64" from paxos notification",
							succession[i]);
				}
			} else {
				pthread_mutex_unlock(vlock_info_hash);
			}

			pthread_mutex_unlock(vlock_info_history_hash);
		}
		i++;
	}

	cf_debug(AS_INFO, "info: paxos succession list has %d elements. after insert, info hash has %d", i, shash_get_size(g_info_node_info_hash));

	// detect node deletion by reducing the hash table and deleting what needs deleting
	cf_debug(AS_INFO, "paxos event: try removing nodes");

	shash_reduce_delete(g_info_node_info_hash, info_paxos_event_reduce_fn, succession);

	cf_debug(AS_INFO, "info: after delete, info hash has %d", i, shash_get_size(g_info_node_info_hash));

	// probably, something changed in the list. Just ask the clients to update
	cf_atomic32_incr(&g_node_info_generation);

	cf_debug(AS_INFO, "as_info_paxos_event took %"PRIu64" ms", cf_getms() - start_ms);
}

// This goes in a reduce function for retransmitting my information to another node

int
info_node_info_reduce_fn(void *key, void *data, void *udata)
{
	cf_node *node = (cf_node *)key;
	info_node_info *infop = (info_node_info *) data;
	int rv;

	if (infop->generation < g_service_generation) {

		cf_debug(AS_INFO, "sending service string %s to node %"PRIx64, g_service_str, *node);

		pthread_mutex_lock(&g_service_lock);

		msg *m = as_fabric_msg_get(M_TYPE_INFO);
		if (0 == m) {
			cf_debug(AS_INFO, " could not get fabric message");
			return(-1);
		}

		msg_set_uint32(m, INFO_FIELD_OP, INFO_OP_UPDATE);
		msg_set_uint32(m, INFO_FIELD_GENERATION, g_service_generation);
		if (g_service_str)
			msg_set_str(m, INFO_FIELD_SERVICE_ADDRESS, g_service_str, MSG_SET_COPY);

		pthread_mutex_unlock(&g_service_lock);

		if ((rv = as_fabric_send(*node, m, AS_FABRIC_PRIORITY_MEDIUM))) {
			cf_warning(AS_INFO, "failed to send msg %p type %d to node %p (rv %d)", m, m->type, *node, rv);
			as_fabric_msg_put(m);
		}
	}

	return(0);
}

//
// Receive a message from a remote node, jam it in my table
//


int
info_msg_fn(cf_node node, msg *m, void *udata)
{
	uint32_t op = 9999;
	msg_get_uint32(m, INFO_FIELD_OP, &op);
	int rv;

	switch (op) {
	case INFO_OP_UPDATE:
		{
			cf_debug(AS_INFO, " received service address from node %"PRIx64, node);

			pthread_mutex_t *vlock_info_hash;
			info_node_info *infop_info_hash;

			pthread_mutex_t *vlock_info_history_hash;
			info_node_info *infop_info_history_hash;

			// Get lock for info_history_hash
			if (SHASH_OK != shash_get_vlock(g_info_node_info_history_hash, &node,
					(void **) &infop_info_history_hash, &vlock_info_history_hash)) {
				// Node not in info_history_hash, so add it.

				info_node_info info;
				info.last = 0;
				info.service_addr = 0;
				info.generation = 0;

				// This may fail, but this is ok. This should only fail when
				// as_info_paxos_event is also trying to add this key, so either
				// way the entry will be in the hash table.
				shash_put_unique(g_info_node_info_history_hash, &node, &info);

				if (SHASH_OK != shash_get_vlock(g_info_node_info_history_hash, &node,
						(void **) &infop_info_history_hash,
						&vlock_info_history_hash)) {
					cf_assert(false, AS_INFO, CF_CRITICAL,
							"could not create info_history_hash entry for %"PRIx64, node);
					break;
				}
			}

			if (infop_info_history_hash->service_addr)
				cf_free(infop_info_history_hash->service_addr);

			infop_info_history_hash->service_addr = 0;

			if (0 != msg_get_str(m, INFO_FIELD_SERVICE_ADDRESS,
								 &(infop_info_history_hash->service_addr), 0,
								 MSG_GET_COPY_MALLOC)) {
				cf_warning(AS_INFO, "failed to get service address from an Info msg");
				pthread_mutex_unlock(vlock_info_history_hash);
				break;
			}

			cf_debug(AS_INFO, " new service address is: %s", infop_info_history_hash->service_addr);

			// See if element is in info_hash
			// - if yes, update the service address.
			if (SHASH_OK == shash_get_vlock(g_info_node_info_hash, &node,
					(void **) &infop_info_hash, &vlock_info_hash)) {

				if (infop_info_hash->service_addr)
					cf_free(infop_info_hash->service_addr);

				infop_info_hash->service_addr = 0;

				// Already unpacked msg in msg_get_str, so just copy the value
				// from infop_info_history_hash.
				if (!infop_info_history_hash->service_addr) {
					cf_warning(AS_INFO, "ignoring bad Info msg with NULL service_addr");
					pthread_mutex_unlock(vlock_info_hash);
					pthread_mutex_unlock(vlock_info_history_hash);
					break;
				}

				infop_info_hash->service_addr =
					cf_strdup( infop_info_history_hash->service_addr );
				cf_assert(infop_info_hash->service_addr, AS_INFO, CF_CRITICAL, "malloc");

				pthread_mutex_unlock(vlock_info_hash);

			} else {
				// Before history_hash was added to code base, we would throw
				// away message in this case.
				cf_debug(AS_INFO, "node %"PRIx64" not in info_hash, saving service address in info_history_hash", node);
			}

			pthread_mutex_unlock(vlock_info_history_hash);

			// Send the ack.
			msg_set_unset(m, INFO_FIELD_SERVICE_ADDRESS);
			msg_set_uint32(m, INFO_FIELD_OP, INFO_OP_ACK);

			if ((rv = as_fabric_send(node, m, AS_FABRIC_PRIORITY_HIGH))) {
				cf_warning(AS_INFO, "failed to send msg %p type %d to node %p (rv %d)", m, m->type, node, rv);
				as_fabric_msg_put(m);
			}
		}
		break;

	case INFO_OP_ACK:
		{

			cf_debug(AS_INFO, " received ACK from node %"PRIx64, node);

			uint32_t	gen;
			msg_get_uint32(m, INFO_FIELD_GENERATION, &gen);
			info_node_info	*info;
			pthread_mutex_t	*vlock;
			if (0 == shash_get_vlock(g_info_node_info_hash, &node, (void **) &info, &vlock)) {

				info->generation = gen;

				pthread_mutex_unlock(vlock);
			}

			as_fabric_msg_put(m);

		}
		break;

	default:
		as_fabric_msg_put(m);
		break;
	}

	return(0);
}

//
// This dynamic function reduces the info_node_info hash and builds up the string of services
//


int
info_get_services_reduce_fn(void *key, void *data, void *udata)
{

	cf_dyn_buf *db = (cf_dyn_buf *) udata;
	info_node_info *infop = (info_node_info *) data;

	if (infop->service_addr) {
		cf_dyn_buf_append_string(db, infop->service_addr);
		cf_dyn_buf_append_char(db, ';');
	}
	return(0);
}



int
info_get_services(char *name, cf_dyn_buf *db)
{
	shash_reduce(g_info_node_info_hash, info_get_services_reduce_fn, (void *) db);

	cf_dyn_buf_chomp(db);

	return(0);
}

int
info_get_services_alumni(char *name, cf_dyn_buf *db)
{

	shash_reduce(g_info_node_info_history_hash, info_get_services_reduce_fn, (void *) db);

	cf_dyn_buf_chomp(db);

	return(0);
}



//
// Iterate through the current namespace list and cons up a string
//

int
info_get_namespaces(char *name, cf_dyn_buf *db)
{
	for (uint i = 0; i < g_config.namespaces; i++) {
		cf_dyn_buf_append_string(db, g_config.namespace[i]->name);
		cf_dyn_buf_append_char(db, ';');
	}

	if (g_config.namespaces > 0) {
		cf_dyn_buf_chomp(db);
	}

	return(0);
}

int
info_get_logs(char *name, cf_dyn_buf *db)
{
	cf_fault_sink_strlist(db);
	return(0);
}

int
info_get_objects(char *name, cf_dyn_buf *db)
{
	uint64_t	objects = 0;

	for (uint i = 0; i < g_config.namespaces; i++) {
		objects += g_config.namespace[i]->n_objects;
	}

	cf_dyn_buf_append_uint64(db, objects);
	return(0);
}

int
info_get_sets(char *name, cf_dyn_buf *db)
{
	return info_get_tree_sets(name, "", db);
}

int
info_get_bins(char *name, cf_dyn_buf *db)
{
	return info_get_tree_bins(name, "", db);
}

int
info_get_config( char* name, cf_dyn_buf *db)
{
	return info_command_config_get(name, NULL, db);
}

int
info_get_sindexes(char *name, cf_dyn_buf *db)
{
	return info_get_tree_sindexes(name, "", db);
}

uint64_t
thr_info_get_object_count()
{
	uint64_t objects = 0;

	for (uint i = 0; i < g_config.namespaces; i++) {
		objects += g_config.namespace[i]->n_objects;
	}

	return objects;
}

uint64_t
thr_info_get_subobject_count()
{
	uint64_t sub_objects = 0;

	for (uint i = 0; i < g_config.namespaces; i++) {
		sub_objects += g_config.namespace[i]->n_sub_objects;
	}

	return sub_objects;
}


void
info_get_namespace_info(as_namespace *ns, cf_dyn_buf *db)
{
	uint64_t free_pct;
	uint64_t data_memory;
	uint64_t pindex_memory;
	uint64_t sindex_memory;
	uint64_t used_memory;

	as_master_prole_stats mp;
	as_partition_get_master_prole_stats(ns, &mp);

	// what everyone wants to know: the number of objects and size
	info_append_uint64("", "objects",  ns->n_objects, db);
	info_append_uint64("", "sub-objects",  ns->n_sub_objects, db);
	info_append_uint64("", "master-objects", mp.n_master_records, db);
	info_append_uint64("", "master-sub-objects", mp.n_master_sub_records, db);
	info_append_uint64("", "prole-objects", mp.n_prole_records, db);
	info_append_uint64("", "prole-sub-objects", mp.n_prole_sub_records, db);
	info_append_uint64("", "expired-objects",  ns->n_expired_objects, db);
	info_append_uint64("", "evicted-objects",  ns->n_evicted_objects, db);
	info_append_uint64("", "set-deleted-objects", ns->n_deleted_set_objects, db);
	info_append_uint64("", "set-evicted-objects", ns->n_evicted_set_objects, db);
	info_append_uint64("", "nsup-cycle-duration", (uint64_t)ns->nsup_cycle_duration, db);
	info_append_uint64("", "nsup-cycle-sleep-pct", (uint64_t)ns->nsup_cycle_sleep_pct, db);

	// total used memory =  data memory + primary index memory + secondary index memory
	data_memory   = ns->n_bytes_memory;
	pindex_memory = as_index_size_get(ns) * ns->n_objects;
	sindex_memory = cf_atomic_int_get(ns->sindex_data_memory_used);
	used_memory   = data_memory + pindex_memory + sindex_memory;

	info_append_uint64("", "used-bytes-memory",        used_memory,     db);
	info_append_uint64("", "data-used-bytes-memory",   data_memory,     db);
	info_append_uint64("", "index-used-bytes-memory", pindex_memory,   db);
	info_append_uint64("", "sindex-used-bytes-memory", sindex_memory,   db);

	free_pct = (ns->memory_size && (ns->memory_size > used_memory))
			   ? (((ns->memory_size - used_memory) * 100L) / ns->memory_size)
			   : 0;

	info_append_uint64("", "free-pct-memory",  free_pct, db);
	info_append_uint64("", "max-void-time",  ns->max_void_time, db);
	info_append_uint64("", "non-expirable-objects", ns->non_expirable_objects, db);
	info_append_uint64("", "current-time",  as_record_void_time_get(), db);

	cf_dyn_buf_append_string(db, ";stop-writes=");
	cf_dyn_buf_append_string(db, cf_atomic32_get(ns->stop_writes) != 0 ? "true" : "false");

	cf_dyn_buf_append_string(db, ";hwm-breached=");
	cf_dyn_buf_append_string(db, cf_atomic32_get(ns->hwm_breached) != 0 ? "true" : "false");

	// remaining bin-name slots (yes, this can be negative)
	if (! ns->single_bin) {
		cf_dyn_buf_append_string(db, ";available-bin-names=");
		cf_dyn_buf_append_int(db, BIN_NAMES_QUOTA - (int)cf_vmapx_count(ns->p_bin_name_vmap));
	}

	// LDT operational statistics
	//
	// print only if LDT is enabled
	if (ns->ldt_enabled) {	
		cf_dyn_buf_append_string(db, ";ldt-reads=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_read_reqs));
		cf_dyn_buf_append_string(db, ";ldt-read-success=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_read_success));
		cf_dyn_buf_append_string(db, ";ldt-deletes=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_delete_reqs));
		cf_dyn_buf_append_string(db, ";ldt-delete-success=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_delete_success));
		cf_dyn_buf_append_string(db, ";ldt-writes=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_write_reqs));
		cf_dyn_buf_append_string(db, ";ldt-write-success=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_write_success));
		cf_dyn_buf_append_string(db, ";ldt-updates=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_update_reqs));

		cf_dyn_buf_append_string(db, ";ldt-gc-io=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_gc_io));
		cf_dyn_buf_append_string(db, ";ldt-gc-cnt=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_gc_cnt));
		cf_dyn_buf_append_string(db, ";ldt-randomizer-retry=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_randomizer_retry));

		cf_dyn_buf_append_string(db, ";ldt-errors=");
		cf_dyn_buf_append_uint32(db, ns->lstats.ldt_errs);

		cf_dyn_buf_append_string(db, ";ldt-err-toprec-notfound=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_toprec_not_found));
		cf_dyn_buf_append_string(db, ";ldt-err-item-notfound=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_item_not_found));

		cf_dyn_buf_append_string(db, ";ldt-err-internal=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_internal));
		cf_dyn_buf_append_string(db, ";ldt-err-unique-key-violation=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_unique_key_violation));

		cf_dyn_buf_append_string(db, ";ldt-err-insert-fail=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_insert_fail));
		cf_dyn_buf_append_string(db, ";ldt-err-delete-fail=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_delete_fail));
		cf_dyn_buf_append_string(db, ";ldt-err-search-fail=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_search_fail));
		cf_dyn_buf_append_string(db, ";ldt-err-version-mismatch=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_version_mismatch));


		cf_dyn_buf_append_string(db, ";ldt-err-capacity-exceeded=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_capacity_exceeded));
		cf_dyn_buf_append_string(db, ";ldt-err-param=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_param));

		cf_dyn_buf_append_string(db, ";ldt-err-op-bintype-mismatch=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_op_bintype_mismatch));
		cf_dyn_buf_append_string(db, ";ldt-err-too-many-open-subrec=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_too_many_open_subrec));

		cf_dyn_buf_append_string(db, ";ldt-err-subrec-not-found=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_subrec_not_found));
		cf_dyn_buf_append_string(db, ";ldt-err-bin-does-not-exist=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_bin_does_not_exist));
		cf_dyn_buf_append_string(db, ";ldt-err-bin-exits=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_bin_exits));
		cf_dyn_buf_append_string(db, ";ldt-err-bin-damaged=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_bin_damaged));

		cf_dyn_buf_append_string(db, ";ldt-err-toprec-internal=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_toprec_internal));
		cf_dyn_buf_append_string(db, ";ldt-err-subrec-internal=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_subrec_internal));

		cf_dyn_buf_append_string(db, ";ldt-err-filer=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_filter));
		cf_dyn_buf_append_string(db, ";ldt-err-key=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_key));
		cf_dyn_buf_append_string(db, ";ldt-err-createspec=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_createspec));
		cf_dyn_buf_append_string(db, ";ldt-err-usermodule=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_usermodule));
		cf_dyn_buf_append_string(db, ";ldt-err-input-too-large=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_input_too_large));
		cf_dyn_buf_append_string(db, ";ldt-err-ldt-not-enabled=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_ldt_not_enabled));

		cf_dyn_buf_append_string(db, ";ldt-err-unknown=");
		cf_dyn_buf_append_uint32(db, cf_atomic_int_get(ns->lstats.ldt_err_unknown));
	}


	// if storage, lots of information about the storage
	//

	if (ns->storage_type == AS_STORAGE_ENGINE_SSD) {

		int available_pct = 0;
		uint64_t inuse_disk_bytes = 0;
		as_storage_stats(ns, &available_pct, &inuse_disk_bytes);

		info_append_uint64("", "used-bytes-disk",  inuse_disk_bytes, db);
		free_pct = (ns->ssd_size && (ns->ssd_size > inuse_disk_bytes)) ? (((ns->ssd_size - inuse_disk_bytes) * 100L) / ns->ssd_size) : 0;
		info_append_uint64("", "free-pct-disk",  free_pct, db);
		info_append_uint64("", "available_pct",  available_pct, db); // the underscore is an unfortunate legacy

		if (! ns->storage_data_in_memory) {
			cf_dyn_buf_append_string(db, ";cache-read-pct=");
			cf_dyn_buf_append_int(db, (int)(ns->cache_read_pct + 0.5));
		}
	} // SSD
}

//
// Iterate through the current namespace list and cons up a string
//

int
info_get_tree_namespace(char *name, char *subtree, cf_dyn_buf *db)
{

	as_namespace *ns = as_namespace_get_byname(subtree);
	if (!ns)   {
		cf_dyn_buf_append_string(db, "type=unknown");
		return(0);
	}

	switch (ns->storage_type) {
		case AS_STORAGE_ENGINE_UNDEF:
		default:
			cf_dyn_buf_append_string(db, "type=illegal");
			goto Done;

		case AS_STORAGE_ENGINE_SSD:
			cf_dyn_buf_append_string(db, "type=device");
			break;
		case AS_STORAGE_ENGINE_MEMORY:
			cf_dyn_buf_append_string(db, "type=memory");
			break;
		case AS_STORAGE_ENGINE_KV:
			cf_dyn_buf_append_string(db, "type=kv");
			break;
	}

	info_get_namespace_info(ns, db);
	cf_dyn_buf_append_string(db, ";");
	info_namespace_config_get(ns->name, db);

Done:
	return 0;
}

int
info_get_tree_sets(char *name, char *subtree, cf_dyn_buf *db)
{
	char *set_name    = NULL;
	as_namespace *ns  = NULL;

	// if there is a subtree, get the namespace
	if (subtree && strlen(subtree) > 0) {
		// see if subtree has a sep as well
		set_name = strchr(subtree, TREE_SEP);

		// pull out namespace, and namespace name...
		if (set_name) {
			int ns_name_len = (set_name - subtree);
			char ns_name[ns_name_len + 1];
			memcpy(ns_name, subtree, ns_name_len);
			ns_name[ns_name_len] = '\0';
			ns = as_namespace_get_byname(ns_name);
			set_name++; // currently points to the TREE_SEP, which is not what we want.
		}
		else {
			ns = as_namespace_get_byname(subtree);
		}

		if (!ns) {
			cf_dyn_buf_append_string(db, "ns_type=unknown");
			return(0);
		}
	}

	// format w/o namespace is ns1:set1:prop1=val1:prop2=val2:..propn=valn;ns1:set2...;ns2:set1...;
	if (!ns) {
		for (uint i = 0; i < g_config.namespaces; i++) {
			as_namespace_get_set_info(g_config.namespace[i], set_name, db);
		}
	}
	// format w namespace w/o set name is ns:set1:prop1=val1:prop2=val2...propn=valn;ns:set2...;
	// format w namespace & set name is prop1=val1:prop2=val2...propn=valn;
	else {
		as_namespace_get_set_info(ns, set_name, db);
	}
	return(0);
}

int
info_get_tree_bins(char *name, char *subtree, cf_dyn_buf *db)
{
	as_namespace *ns  = NULL;

	// if there is a subtree, get the namespace
	if (subtree && strlen(subtree) > 0) {
		ns = as_namespace_get_byname(subtree);

		if (!ns) {
			cf_dyn_buf_append_string(db, "ns_type=unknown");
			return 0;
		}
	}

	// format w/o namespace is
	// ns:num-bin-names=val1,bin-names-quota=val2,name1,name2,...;ns:...
	if (!ns) {
		for (uint i = 0; i < g_config.namespaces; i++) {
			as_namespace_get_bins_info(g_config.namespace[i], db, true);
		}
	}
	// format w/namespace is
	// num-bin-names=val1,bin-names-quota=val2,name1,name2,...
	else {
		as_namespace_get_bins_info(ns, db, false);
	}

	return 0;
}

int
info_command_hist_dump(char *name, char *params, cf_dyn_buf *db)
{
	char value_str[128];
	int  value_str_len = sizeof(value_str);

	if (0 != as_info_parameter_get(params, "ns", value_str, &value_str_len)) {
		cf_info(AS_INFO, "hist-dump %s command: no namespace specified", name);
		cf_dyn_buf_append_string(db, "error-no-namespace");
		return 0;
	}

	as_namespace *ns = as_namespace_get_byname(value_str);

	if (!ns) {
		cf_info(AS_INFO, "hist-dump %s command: unknown namespace: %s", name, value_str);
		cf_dyn_buf_append_string(db, "error-unknown-namespace");
		return 0;
	}

	value_str_len = sizeof(value_str);

	if (0 != as_info_parameter_get(params, "hist", value_str, &value_str_len)) {
		cf_info(AS_INFO, "hist-dump %s command:", name);
		cf_dyn_buf_append_string(db, "error-no-hist-name");

		return 0;
	}

	// get optional set field
	char set_name_str[AS_SET_NAME_MAX_SIZE];
	int set_name_str_len = sizeof(set_name_str);
	set_name_str[0] = 0;

	as_info_parameter_get(params, "set", set_name_str, &set_name_str_len);

	// format is ns1:ns_hist1=bucket_count,offset,b1,b2,b3...;
	as_namespace_get_hist_info(ns, set_name_str, value_str, db, true);

	return 0;
}


int
info_get_tree_log(char *name, char *subtree, cf_dyn_buf *db)
{
	// see if subtree has a sep as well
	int sink_id;
	char *context = strchr(subtree, TREE_SEP);
	if (context) { // this means: log/id/context ,
		*context = 0;
		context++;

		if (0 != cf_str_atoi(subtree, &sink_id)) return(-1);

		cf_fault_sink_context_strlist(sink_id, context, db);
	}
	else { // this means just: log/id , so get all contexts
		if (0 != cf_str_atoi(subtree, &sink_id)) return(-1);

		cf_fault_sink_context_all_strlist(sink_id, db);
	}

	return(0);
}


int
info_get_tree_sindexes(char *name, char *subtree, cf_dyn_buf *db)
{
	char *index_name    = NULL;
	as_namespace *ns  = NULL;

	// if there is a subtree, get the namespace
	if (subtree && strlen(subtree) > 0) {
		// see if subtree has a sep as well
		index_name = strchr(subtree, TREE_SEP);

		// pull out namespace, and namespace name...
		if (index_name) {
			int ns_name_len = (index_name - subtree);
			char ns_name[ns_name_len + 1];
			memcpy(ns_name, subtree, ns_name_len);
			ns_name[ns_name_len] = '\0';
			ns = as_namespace_get_byname(ns_name);
			index_name++; // currently points to the TREE_SEP, which is not what we want.
		}
		else {
			ns = as_namespace_get_byname(subtree);
		}

		if (!ns) {
			cf_dyn_buf_append_string(db, "ns_type=unknown");
			return(0);
		}
	}

	// format w/o namespace is ns1:set1:prop1=val1:prop2=val2:..propn=valn;ns1:set2...;ns2:set1...;
	if (!ns) {
		for (uint i = 0; i < g_config.namespaces; i++) {
			as_sindex_list_str(g_config.namespace[i], db);
		}
	}
	// format w namespace w/o set name is ns:set1:prop1=val1:prop2=val2...propn=valn;ns:set2...;
	// format w namespace & set name is prop1=val1:prop2=val2...propn=valn;
	else if (!index_name) {
		as_sindex_list_str(ns, db);
	}
	else {
		as_sindex_metadata imd;
		memset(&imd, 0, sizeof(imd));
		imd.ns_name = cf_strdup(ns->name);
		imd.iname   = cf_strdup(index_name);

		int resp = as_sindex_stats_str(ns, &imd, db);
		if (resp) {
			cf_dyn_buf_append_string(db, "Invalid Stats");
			INFO_COMMAND_SINDEX_FAILCODE(
					as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
					as_sindex_err_str(resp));
		}
		if (imd.ns_name) cf_free(imd.ns_name);
		if (imd.iname) cf_free(imd.iname);
	}
	return(0);
}




int
info_get_service(char *name, cf_dyn_buf *db)
{
	pthread_mutex_lock(&g_service_lock);
	cf_dyn_buf_append_string(db, g_service_str ? g_service_str : " ");
	pthread_mutex_unlock(&g_service_lock);

	return(0);
}

void
clear_ldt_histograms()
{
	histogram_clear(g_config.ldt_multiop_prole_hist);
	histogram_clear(g_config.ldt_update_record_cnt_hist);
	histogram_clear(g_config.ldt_io_record_cnt_hist);
	histogram_clear(g_config.ldt_update_io_bytes_hist);
	histogram_clear(g_config.ldt_hist);
}

void
clear_microbenchmark_histograms()
{
	histogram_clear(g_config.rt_cleanup_hist);
	histogram_clear(g_config.rt_net_hist);
	histogram_clear(g_config.wt_net_hist);
	histogram_clear(g_config.rt_storage_read_hist);
	histogram_clear(g_config.rt_storage_open_hist);
	histogram_clear(g_config.rt_tree_hist);
	histogram_clear(g_config.rt_internal_hist);
	histogram_clear(g_config.wt_internal_hist);
	histogram_clear(g_config.rt_start_hist);
	histogram_clear(g_config.wt_start_hist);
	histogram_clear(g_config.rt_q_process_hist);
	histogram_clear(g_config.wt_q_process_hist);
	histogram_clear(g_config.q_wait_hist);
	histogram_clear(g_config.demarshal_hist);
	histogram_clear(g_config.wt_master_wait_prole_hist);
	histogram_clear(g_config.wt_prole_hist);
	histogram_clear(g_config.rt_resolve_hist);
	histogram_clear(g_config.wt_resolve_hist);
	histogram_clear(g_config.rt_resolve_wait_hist);
	histogram_clear(g_config.wt_resolve_wait_hist);
	histogram_clear(g_config.error_hist);
	histogram_clear(g_config.batch_q_process_hist);
	histogram_clear(g_config.info_tr_q_process_hist);
	histogram_clear(g_config.info_q_wait_hist);
	histogram_clear(g_config.info_post_lock_hist);
	histogram_clear(g_config.info_fulfill_hist);
	histogram_clear(g_config.write_storage_close_hist);
	histogram_clear(g_config.write_sindex_hist);
	histogram_clear(g_config.defrag_storage_close_hist);
	histogram_clear(g_config.prole_fabric_send_hist);
}

// SINDEX
// wire protocol examples:
// 1.) NUMERIC:    sindex-create:ns=usermap;set=demo;indexname=um_age;indexdata=age,numeric
// 2.) STRING:     sindex-create:ns=usermap;set=demo;indexname=um_state;indexdata=state,string
// 3.) FUNCTIONAL: sindex-create:ns=usermap;set=demo;indexname=um_func;type=functional;indexdata=file,func,numeric;nfargs=1;fargs='arg1'
// 4.) USERLAND:   sindex-create:ns=usermap;set=demo;indexname=um_userland;type=userland;indexdata=file,func,numeric;nfargs=1;fargs='arg1'
//

/*
 *  Description:  Parses the parameter passed to asinfo and fills up the imd
 *  			  with the parsed value
 *
 *  Usage: Gets invoked for info_command_sindex_create and destroy.
 *       : Also gets invoked from smd's accept-callback functions.
 *
 *  Parameters:
 *  	params --- string passed to asinfo call
 *  	imd    --  parses the params and fills this sindex struct.
 *
 *  Returns
 *  	0 if it successfully fills up imd
 *      otherwise AS_SINDEX_ERR_PARAM.
 */
int
as_info_parse_params_to_sindex_imd(char* params, as_sindex_metadata *imd, cf_dyn_buf* db,
		bool is_create, bool *is_smd_op)
{
	if (!imd) {
		cf_info(AS_INFO, "Failed to create secondary index : internal error");
		return AS_SINDEX_ERR_PARAM;
	}

	if (strlen(params) > SINDEX_SMD_VALUE_SIZE) {
		cf_info(AS_INFO, "Index definition %s length longer than allowed(1024)", params);
		return AS_SINDEX_ERR_PARAM;
	}

	// NAMESPACE NAMESPACE NAMESPACE
	char ns_str[128];
	int ns_len       = sizeof(ns_str);
	imd->post_op     = 0;

	char indexname_str[AS_ID_INAME_SZ];
	int  indname_len  = sizeof(indexname_str);
	int ret = as_info_parameter_get(params, STR_INDEXNAME, indexname_str, &indname_len);

	if ( ret == -1 ) {
		cf_warning(AS_INFO, "Failed to create secondary index : Indexname not specified"
				" for secondary index creation");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Index Name Not Specified");
		return AS_SINDEX_ERR_PARAM;
	}
	if ( ret == -2 ) {
		cf_warning(AS_INFO, "Failed to create secondary index : The indexname is longer than %d characters",
				AS_ID_INAME_SZ);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Indexname too long");
		return AS_SINDEX_ERR_PARAM;
	}

	ret = as_info_parameter_get(params, STR_NS, ns_str, &ns_len);

	if ( ret == -1 ) {
		cf_warning(AS_INFO, "Failed to create secondary index : Namespace not specified"
				" for sindex creation %s ", indexname_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Namespace Not Specified");
		return AS_SINDEX_ERR_PARAM;
	}
	if (ret == -2 ) {
		cf_warning(AS_INFO, "Failed to create secondary index: Namespace specified is longer"
				" than %d characters for sindex creation %s ", 128, indexname_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Invalid namespace for SINDEX creation ");
		return AS_SINDEX_ERR_PARAM;
	}

	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_warning(AS_INFO, "Failed to create secondary index: namespace %s not found "
				"for secondary index creation %s", ns_str, indexname_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		return AS_SINDEX_ERR_PARAM;
	}
	if (ns->single_bin) {
		cf_warning(AS_INFO, "Failed to create secondary index: Secondary Index Not "
				"Allowed on Single Bin Namespace %s", ns_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Single Bin Namespace");
		return AS_SINDEX_ERR_PARAM;
	}
	imd->ns_name = cf_strdup(ns->name);


	// SETNAME SETNAME SETNAME ... (Optional)
	char set_str[AS_SET_NAME_MAX_SIZE];
	int set_len  = sizeof(set_str);
	int res = as_info_parameter_get(params, STR_SET, set_str, &set_len);
	if (!res) {
		imd->set = cf_strdup(set_str);
	} else if (res == -2) {
		cf_warning(AS_INFO, "Failed to create secondary index : invalid setname"
				"  for secondary index creation %s", indexname_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Invalid Set Name");
		return AS_SINDEX_ERR_PARAM;
	}

	//set the indexname to imd
	imd->iname      = cf_strdup(indexname_str);

	char cluster_op[6];
	int cluster_op_len = 6;
	if (as_info_parameter_get(params, "cluster_op", cluster_op, &cluster_op_len) != 0) {
		*is_smd_op = true;
	}
	else if (strcmp(cluster_op, "true") == 0) {
		*is_smd_op = true;
	}
	else if (strcmp(cluster_op, "false") == 0) {
		*is_smd_op = false;
	}

	if (is_create == false) {
		return 0;
	}

	// Get the index type. 
	// It could be list, mapkeys, mapvalues, or by default none.
	char indextype_str[128];
	memset(indextype_str, 0, 128);
	int  indtype_len = sizeof(indextype_str);
	if (as_info_parameter_get(params, STR_ITYPE, indextype_str, &indtype_len)) {
		// if not specified the index type is normal
		imd->itype = AS_SINDEX_ITYPE_DEFAULT;
	}
	else {
		if (strncasecmp(indextype_str, STR_ITYPE_DEFAULT, 7) == 0) {
			imd->itype = AS_SINDEX_ITYPE_DEFAULT;
		}
		else if (strncasecmp(indextype_str, STR_ITYPE_LIST, 4) == 0) {
			imd->itype = AS_SINDEX_ITYPE_LIST;
		}
		else if (strncasecmp(indextype_str, STR_ITYPE_MAPKEYS, 7) == 0) {
			imd->itype = AS_SINDEX_ITYPE_MAPKEYS;
		}
		else if (strncasecmp(indextype_str, STR_ITYPE_MAPVALUES, 9) == 0) {
			imd->itype = AS_SINDEX_ITYPE_MAPVALUES;
		}
		else {
			cf_warning(AS_INFO, "Failed to create secondary index : invalid type of index"
					" for sindex creation %s %s", indexname_str, indextype_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					"Invalid type must be [none, list, mapkeys, mapvalues]");
			return AS_SINDEX_ERR_PARAM;
		}
	}

	// Gather indexdata	
	char indexdata_str[1024];
	int  indexdata_len = sizeof(indexdata_str);
	if (as_info_parameter_get(params, STR_INDEXDATA, indexdata_str,
				&indexdata_len)) {
		cf_warning(AS_INFO, "Failed to create secondary index : invalid indexdata for"
				" sindex creation %s %s", indexname_str, indexdata_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Invalid indexdata");
		return AS_SINDEX_ERR_PARAM;
	}
	// Read indexdata
	cf_vector *str_v = cf_vector_create(sizeof(void *), 10, VECTOR_FLAG_INITZERO);
	cf_str_split(",", indexdata_str, str_v);
	if (0 != (cf_vector_size(str_v) % 2) || AS_SINDEX_BINMAX < (cf_vector_size(str_v) / 2)) {
		cf_warning(AS_INFO, "Failed to create secondary index : number of bins more than"
				"  %d for sindex creation %s", AS_SINDEX_BINMAX, indexname_str);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"invalid indexdata");
		cf_vector_destroy(str_v);
		return AS_SINDEX_ERR_PARAM;
	}

	// Bin data
	// TODO: Remove the half cooked multi-col index support from server.
	int bincount = 0;
	for (int i = 0; i < (cf_vector_size(str_v) / 2); i++) {
		// Not more than AS_SINDEX_BINMAX are allowed
		if (bincount >= AS_SINDEX_BINMAX) {
			cf_warning(AS_INFO, "Failed to create secondary index: More bins are specified "
					"than %d for sindex creation %s ", AS_SINDEX_BINMAX, indexname_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					"More bins specified than allowed");
			cf_vector_destroy(str_v);
			return AS_SINDEX_ERR_PARAM;
		}

		char *path_str;
		cf_vector_get(str_v, i * 2, &path_str);
		imd->path_str = cf_strdup(path_str);
		// Extract the path and bin
		if (as_sindex_extract_bin_path(imd, path_str)) {
			cf_warning(AS_INFO, "Failed to create secondary index: Path_str is not valid- %s", path_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Invalid path");
			return AS_SINDEX_ERR_PARAM;
		}

		if (!imd->bnames[i]) {
			cf_warning(AS_INFO, "Failed to create secondary index: bin name must be specified for sindex creation");
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Invalid bin name");
			cf_vector_destroy(str_v);
			return AS_SINDEX_ERR_PARAM;
		}

		// Extract data type to index
		char *type_str = NULL;
		cf_vector_get(str_v, i * 2 + 1, &type_str);

		if (!type_str) {
			cf_warning(AS_INFO, "Failed to create secondary index: bin type must be specified"
					" for sindex creation %s ", indexname_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Invalid type must be [numeric,string]");
			cf_vector_destroy(str_v);
			return AS_SINDEX_ERR_PARAM;
		}
		else if (strncasecmp(type_str, "string", 6) == 0) {
			imd->btype[i] = AS_SINDEX_KTYPE_DIGEST;
		} else if (strncasecmp(type_str, "numeric", 7) == 0) {
			imd->btype[i] = AS_SINDEX_KTYPE_LONG;
		} else {
			cf_warning(AS_INFO, "Failed to create secondary index : invalid bin type %s "
					"for sindex creation %s", type_str, indexname_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					"Invalid type must be [numeric,string]");
			cf_vector_destroy(str_v);
			return AS_SINDEX_ERR_PARAM;
		}
		bincount++;
	}

	imd->num_bins = bincount;
	for (int i = 0; i < imd->num_bins; i++) {
		if (imd->bnames[i] && (strlen(imd->bnames[i]) >= BIN_NAME_MAX_SZ)) {
			cf_warning(AS_INFO, "Failed to create secondary creation: Bin Name %s longer "
					"than allowed (%d) for sindex creation %s", imd->bnames[i],
					BIN_NAME_MAX_SZ, indexname_str);
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER, "Bin Name too long");
			return AS_SINDEX_ERR_PARAM;
		}
	}
	cf_vector_destroy(str_v);
	return AS_SINDEX_OK;
}

// called for asinfo command to create a new sindex
int info_command_sindex_create(char *name, char *params, cf_dyn_buf *db)
{
	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));
	bool is_smd_op = true;

	// Check info-command params for correctness.
	int res = as_info_parse_params_to_sindex_imd(params, &imd, db, true, &is_smd_op);

	if (res != 0) {
		cf_info(AS_INFO, "Create Index Failed");
		goto ERR;
	}

	as_namespace *ns = as_namespace_get_byname(imd.ns_name);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		goto ERR;
	}

	// Check SI subsystem for limits.
	// Checks for:
	// Index already exists
	// bin already indexed (includes set-level checks also)
	// Index name too-long
	res = as_sindex_create_check_params(ns, &imd);

	// Populate error message correctly:
	if (res == AS_SINDEX_ERR_FOUND) {
		cf_info(AS_INFO, "Index with the same index defn already exists or bin has already been indexed.");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_FOUND,
				"Index with the same name already exists or this bin has already been indexed.");
		goto ERR;
	} 
	else if (res == AS_SINDEX_ERR_PARAM) {
		cf_info(AS_INFO, "Index-name is too long, should be a max of: %d.", AS_ID_INAME_SZ - 1);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_NAME_MAXLEN,
				"Index-name is too long.");
		goto ERR;
	}
	else if (res == AS_SINDEX_ERR_MAXCOUNT) {
		cf_info(AS_INFO, "More than %d index are not allowed per namespace.", AS_SINDEX_MAX);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_MAXCOUNT,
				"Reached maximum number of sindex allowed");
		goto ERR;

	}
	else if (res != AS_SINDEX_OK) {
		cf_info(AS_INFO, "Index creation failed. Error %d", res);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_GENERIC,"");
		goto ERR;
	}

	if (is_smd_op == true)
	{
		cf_info(AS_INFO, "Index creation request received for %s:%s via SMD", imd.ns_name, imd.iname);
		char module[] = SINDEX_MODULE;
		char key[SINDEX_SMD_KEY_SIZE];
		sprintf(key, "%s:%s", imd.ns_name, imd.iname);
		int resp      = as_smd_set_metadata(module, key, params);

		if (resp != 0) {
			cf_info(AS_INFO, "Queuing the index %s metadata to SMD failed with error %s",
					imd.iname, as_sindex_err_str(resp));
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					as_sindex_err_str(resp));

			goto ERR;
		}
	}
	else if (is_smd_op == false) {
		int resp         = as_sindex_create(ns, &imd, true);
		if (0 != resp) {
			cf_info(AS_INFO, "Create Index %s failed with error %s",
					imd.iname, as_sindex_err_str(resp));
			INFO_COMMAND_SINDEX_FAILCODE(
					as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
					as_sindex_err_str(resp));
			goto ERR;
		}
	}
	cf_dyn_buf_append_string(db, "OK");
ERR:
	as_sindex_imd_free(&imd);
	return(0);

}

int info_command_sindex_delete(char *name, char *params, cf_dyn_buf *db) {

	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));
	bool is_smd_op = true;
	int res = as_info_parse_params_to_sindex_imd(params, &imd, db, false, &is_smd_op);

	if (res != 0) {
		cf_info(AS_INFO, "Destroy Index Failed");
		goto ERR;
	}

	cf_info(AS_INFO, " Secondary index deletion called for ns:%s si:%s", imd.ns_name, imd.iname);

	as_namespace *ns = as_namespace_get_byname(imd.ns_name);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		goto ERR;
	}

	// If SI does not exists in the system, return error
	// Do not use as_sindex_exists_by_defn() here, it'll fail because bname is null.
	if (!as_sindex_delete_checker(ns, &imd)) {
		cf_info(AS_INFO, "Index-deletion failed, index %s:%s does not exist on the system.", imd.ns_name, imd.iname);
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND,
				"Index-deletion failed, index does not exist on the system.");
		goto ERR;
	}

	if(is_smd_op == false)
	{
		int resp = as_sindex_destroy(ns, &imd);
		if (0 != resp) {
			cf_info(AS_INFO, "Delete Index %s Fail with error %s",
					imd.iname, as_sindex_err_str(resp));
			INFO_COMMAND_SINDEX_FAILCODE(
					as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
					as_sindex_err_str(resp));
			goto ERR;
		}
	}
	else if (is_smd_op == true)
	{
		cf_info(AS_INFO, "Index deletion request received for %s:%s via SMD", imd.ns_name, imd.iname);
		char module[] = SINDEX_MODULE;
		char key[SINDEX_SMD_KEY_SIZE];
		sprintf(key, "%s:%s", imd.ns_name, imd.iname);
		as_smd_delete_metadata(module, key);
	}
	cf_dyn_buf_append_string(db, "OK");
ERR:
	as_sindex_imd_free(&imd);
	return 0;
}

int info_command_sindex_dump(char *name, char *params, cf_dyn_buf *db) {
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		cf_info(AS_INFO, "invalid ns");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Specified");
		return 0;
	}
	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		return 0;
	}

	char set_str[AS_SET_NAME_MAX_SIZE];
	int set_len = sizeof(set_str); // get optional set
	if (as_info_parameter_get(params, STR_SET, set_str, &set_len)) {
		set_str[0] = '\0';
	}

	char fname[128];
	int fname_len = sizeof(ns_str);
	if (as_info_parameter_get(params, "file", fname, &fname_len)) {
		fname[0] = '\0';
	}

	if (!ai_btree_dump(ns_str, set_str, fname)) {
		cf_dyn_buf_append_string(db, "ok");
	} else {
		cf_dyn_buf_append_string(db, "error");
	}

	return 0;
}

int info_command_sindex_describe(char *name, char *params, cf_dyn_buf *db) {
	// get namespace and make sure it exists
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		cf_info(AS_INFO, "invalid ns");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Specified");
		return 0;
	}
	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		return 0;
	}

	// get optional set
	char set_str[AS_SET_NAME_MAX_SIZE];
	int set_len = sizeof(set_str);
	if (as_info_parameter_get(params, STR_SET, set_str, &set_len)) {
		set_str[0] = '\0';
	}
	// get indexname
	char index_name_str[128];
	int  index_len = sizeof(index_name_str);
	if (as_info_parameter_get(params, "indexname", index_name_str, &index_len)) {
		cf_info(AS_INFO, "invalid indexname");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Index Name Not Specified");
		return 0;
	}

	as_sindex_metadata	imd;
	memset((void *)&imd, 0, sizeof(imd));
	imd.ns_name = cf_strdup(ns->name);
	imd.iname   = cf_strdup(index_name_str);
	imd.set		= cf_strdup(set_str);

	int resp = as_sindex_describe_str(ns, &imd, db);
	if (resp) {
		cf_info(AS_INFO, "Describe For Index %s Fail with error %s",
				index_name_str, as_sindex_err_str(resp));
		INFO_COMMAND_SINDEX_FAILCODE(
			as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
			as_sindex_err_str(resp));
	}
	if (imd.ns_name)    cf_free(imd.ns_name);
	if (imd.iname)      cf_free(imd.iname);
	if (imd.set)        cf_free(imd.set);

	return(0);
}

int info_command_sindex_repair(char *name, char *params, cf_dyn_buf *db) {
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (0 != as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		cf_info(AS_INFO, "invalid ns");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Specified");
		return 0;
	}
	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		return 0;
	}

	// get indexname
	char index_name_str[128];
	int  index_len = sizeof(index_name_str);
	if (as_info_parameter_get(params, "indexname", index_name_str, &index_len)) {
		cf_info(AS_INFO, "invalid indexname");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Index Name Not Specified");
		return 0;
	}

	// get optional set
	char set_str[AS_SET_NAME_MAX_SIZE];
	bool hasset = false;
	int set_len = sizeof(set_str);
	if (as_info_parameter_get(params, STR_SET, set_str, &set_len)) {
		hasset = false;
		set_str[0] = '\0';
	}

	as_sindex_metadata imd;
	memset(&imd, 0, sizeof(imd));
	imd.ns_name = cf_strdup(ns->name);
	imd.iname   = cf_strdup(index_name_str);

	int resp = as_sindex_repair(ns, &imd);
	if (resp) {
		cf_dyn_buf_append_string(db, "Invalid Repair");
		INFO_COMMAND_SINDEX_FAILCODE(
				as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
				as_sindex_err_str(resp));
	}
	else {
		cf_dyn_buf_append_string(db, "Ok");
	}
	if (imd.ns_name) cf_free(imd.ns_name);
	if (imd.iname) cf_free(imd.iname);
	return(0);
}

int info_command_abort_scan(char *name, char *params, cf_dyn_buf *db) {
	char context[100];
	int  context_len = sizeof(context);
	int rv = -1;
	if (0 == as_info_parameter_get(params, "id", context, &context_len)) {
		uint64_t trid;
		trid = strtoull(context, NULL, 10);
		if (trid != 0) {
			rv = as_tscan_abort(trid);
		}
	}

	if (rv != 0) {
		cf_dyn_buf_append_string(db, "ERROR:");
		cf_dyn_buf_append_int(db, AS_PROTO_RESULT_FAIL_NOTFOUND);
		cf_dyn_buf_append_string(db, ":Transaction Not Found");
	}
	else {
		cf_dyn_buf_append_string(db, "OK");
	}

	return 0;
}

int info_command_query_kill(char *name, char *params, cf_dyn_buf *db) {
	char context[100];
	int  context_len = sizeof(context);
	int  rv          = AS_QUERY_ERR;
	if (0 == as_info_parameter_get(params, "trid", context, &context_len)) {
		uint64_t trid;
		trid = strtoull(context, NULL, 10);
		if (trid != 0) {
			rv = as_query_kill(trid);
		}
	}

	if (AS_QUERY_OK != rv) {
		cf_dyn_buf_append_string(db, "Transaction Not Found");
	}
	else {
		cf_dyn_buf_append_string(db, "Ok");
	}

	return 0;



}
int info_command_sindex_stat(char *name, char *params, cf_dyn_buf *db) {
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (0 != as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		cf_info(AS_INFO, "invalid ns");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Specified");
		return 0;
	}
	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Namespace Not Found");
		return 0;
	}

	// get indexname
	char index_name_str[128];
	int  index_len = sizeof(index_name_str);
	if (as_info_parameter_get(params, "indexname", index_name_str, &index_len)) {
		cf_info(AS_INFO, "invalid indexname");
		INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
				"Index Name Not Specified");
		return 0;
	}

	// get optional set
	char set_str[AS_SET_NAME_MAX_SIZE];
	bool hasset = false;
	int set_len = sizeof(set_str);
	if (as_info_parameter_get(params, STR_SET, set_str, &set_len)) {
		hasset = false;
		set_str[0] = '\0';
	}

	as_sindex_metadata imd;
	memset(&imd, 0, sizeof(imd));
	imd.ns_name = cf_strdup(ns->name);
	imd.iname   = cf_strdup(index_name_str);

	int resp = as_sindex_stats_str(ns, &imd, db);
	if (resp)  {
		cf_dyn_buf_append_string(db, "Invalid Stats");
		INFO_COMMAND_SINDEX_FAILCODE(
				as_sindex_err_to_clienterr(resp, __FILE__, __LINE__),
				as_sindex_err_str(resp));
	}
	if (imd.ns_name) cf_free(imd.ns_name);
	if (imd.iname) cf_free(imd.iname);
	return(0);
}


// sindex-histogram:ns=test_D;[set=demo];indexname=indname;enable=true/false
int info_command_sindex_histogram(char *name, char *params, cf_dyn_buf *db)
{
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (0 != as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		cf_info(AS_INFO, "invalid ns");
		cf_dyn_buf_append_string(db, "invalid ns");
		return 0;
	}
	as_namespace *ns = as_namespace_get_byname(ns_str);
	if (!ns) {
		cf_info(AS_INFO, "ns not found");
		cf_dyn_buf_append_string(db, "invalid ns");
		return 0;
	}

	// get indexname
	char index_name_str[128];
	int  index_len = sizeof(index_name_str);
	if (as_info_parameter_get(params, "indexname", index_name_str, &index_len)) {
		cf_info(AS_INFO, "invalid indexname");
		cf_dyn_buf_append_string(db, "invalid indexname");
		return 0;
	}

	// get optional set
	char set_str[AS_SET_NAME_MAX_SIZE];
	bool hasset = false;
	int set_len = sizeof(set_str);
	if (as_info_parameter_get(params, STR_SET, set_str, &set_len)) {
		hasset = false;
		set_str[0] = '\0';
	}

	char op[64];
	int op_len = sizeof(op);

	if (as_info_parameter_get(params, "enable", op, &op_len)) {
		cf_info(AS_INFO, "invalid Op");
		cf_dyn_buf_append_string(db, "Invalid Op");
		return 0;
	}

	bool enable = false;
	if (!strncmp(op, "true", 5)) {
		enable = true;
	} else if (!strncmp(op, "false", 6)) {
		enable = false;
	}

	as_sindex_metadata	imd;
	memset(&imd, 0, sizeof(imd));
	imd.ns_name = cf_strdup(ns->name);
	imd.iname   = cf_strdup(index_name_str);

	int resp = as_sindex_histogram_enable(ns, &imd, enable);
	if (resp) {
		cf_dyn_buf_append_string(db, "Fail: ");
		cf_dyn_buf_append_string(db, as_sindex_err_str(resp));
	} else {
		cf_dyn_buf_append_string(db, "Ok");
	}
	if (imd.ns_name) cf_free(imd.ns_name);
	if (imd.iname)   cf_free(imd.iname);
	return(0);
}

int info_command_sindex_qnodemap(char *name, char *params, cf_dyn_buf *db)
{
	int found = 0;

	for (int i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		for (int j = 0; j < AS_PARTITIONS; j++) {
			// ns name
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, ":");

			as_partition *p = &ns->partitions[j];
			// pid
			cf_dyn_buf_append_int(db, j);
			cf_dyn_buf_append_string(db, ":");
			// size
			cf_dyn_buf_append_uint32(db, p->vp ? p->vp->elements : 0);
			cf_dyn_buf_append_string(db, ":");
			// state
			cf_dyn_buf_append_char(db, as_partition_getstate_str(p->state));
			cf_dyn_buf_append_string(db, ":");
			// Qnode
			char qnode[128];
			sprintf(qnode, "%"PRIX64"", p->qnode);
			cf_dyn_buf_append_string(db, qnode);
			cf_dyn_buf_append_string(db, ":");
			// Current Node Type
			if (g_config.self_node == p->replica[0])
				cf_dyn_buf_append_string(db, "M");
			else
				cf_dyn_buf_append_string(db, "R");

			if (g_config.self_node == p->qnode)
				cf_dyn_buf_append_string(db, "Q");
			cf_dyn_buf_append_string(db, ":");
			cf_dyn_buf_append_uint64(db, (uint64_t) p->n_bytes_memory);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, (uint64_t) p->vp->elements);
			cf_dyn_buf_append_char(db, ';');
		}
		found++;
	}
	if (found == 0) {
		cf_dyn_buf_append_string(db, "Empty");
	}
	else {
		cf_dyn_buf_chomp(db);
	}
	return(0);
}

int info_command_sindex_list(char *name, char *params, cf_dyn_buf *db) {
	bool listall = true;
	char ns_str[128];
	int ns_len = sizeof(ns_str);
	if (!as_info_parameter_get(params, "ns", ns_str, &ns_len)) {
		listall = false;
	}

	if (listall) {
		bool found = 0;
		for (int i = 0; i < g_config.namespaces; i++) {
			as_namespace *ns = g_config.namespace[i];
			if (ns) {
				if (!as_sindex_list_str(ns, db)) {
					found++;
				}
				else {
					cf_detail(AS_INFO, "No indexes for namespace %s", ns->name);
				}
			}
		}
		if (found == 0) {
			cf_dyn_buf_append_string(db, "Empty");
		}
		else {
			cf_dyn_buf_chomp(db);
		}
	}
	else {
		as_namespace *ns = as_namespace_get_byname(ns_str);
		if (!ns) {
			cf_info(AS_INFO, "ns not found");
			INFO_COMMAND_SINDEX_FAILCODE(AS_PROTO_RESULT_FAIL_PARAMETER,
					"Namespace Not Found");
			return 0;
		} else {
			if (as_sindex_list_str(ns, db)) {
				cf_info(AS_INFO, "ns not found");
				cf_dyn_buf_append_string(db, "Empty");
			}
			return 0;
		}
	}
	return(0);
}

// Defined in "make_in/version.c" (auto-generated by the build system.)
extern const char aerospike_build_id[];
extern const char aerospike_build_type[];

int
as_info_init()
{
	// g_info_node_info_history_hash is a hash of all nodes that have ever been
	// recognized by this node - either via paxos or info messages.
	shash_create(&g_info_node_info_history_hash, cf_nodeid_shash_fn, sizeof(cf_node), sizeof(info_node_info), 64, SHASH_CR_MT_BIGLOCK);

	// g_info_node_info_hash is a hash of all nodes *currently* in the cluster.
	// This hash should *always* be a subset of g_info_node_info_history_hash -
	// to ensure this, you should take the lock on the corresponding key in
	// info_history_hash before modifying an element in this hash table. This
	// hash is used to create the services list.
	shash_create(&g_info_node_info_hash, cf_nodeid_shash_fn, sizeof(cf_node), sizeof(info_node_info), 64, SHASH_CR_MT_BIGLOCK);

	// create worker threads
	g_info_work_q = cf_queue_create(sizeof(info_work), true);

	char vstr[64];
	sprintf(vstr, "%s build %s", aerospike_build_type, aerospike_build_id);

	// Set some basic values
	as_info_set("version", vstr, true);                  // Returns the edition and build number.
	as_info_set("build", aerospike_build_id, true);      // Returns the build number for this server.
	as_info_set("edition", aerospike_build_type, true);  // Return the edition of this build.
	as_info_set("digests", "RIPEMD160", false);          // Returns the hashing algorithm used by the server for key hashing.
	as_info_set("status", "ok", false);                  // Always returns ok, used to verify service port is open.
	as_info_set("STATUS", "OK", false);                  // Always returns OK, used to verify service port is open.

	char istr[21];
	cf_str_itoa(AS_PARTITIONS, istr, 10);
	as_info_set("partitions", istr, false);              // Returns the number of partitions used to hash keys across.

	cf_str_itoa_u64(g_config.self_node, istr, 16);
	as_info_set("node", istr, true);                     // Node ID. Unique 15 character hex string for each node based on the mac address and port.
	as_info_set("name", istr, false);                    // Alias to 'node'.
	// Returns list of features supported by this server
	as_info_set("features", "replicas-all;replicas-master;replicas-prole;udf", true);
	if (g_config.hb_mode == AS_HB_MODE_MCAST) {
		sprintf(istr, "%s:%d", g_config.hb_addr, g_config.hb_port);
		as_info_set("mcast", istr, false);               // Returns the multicast heartbeat address and port used by this server. Only available in multicast heartbeat mode.
	}
	else if (g_config.hb_mode == AS_HB_MODE_MESH) {
		sprintf(istr, "%s:%d", g_config.hb_addr, g_config.hb_port);
		as_info_set("mesh", istr, false);                // Returns the heartbeat address and port used by this server. Only available in mesh heartbeat mode.
	}

	// All commands accepted by asinfo/telnet
	as_info_set("help", "alloc-info;asm;build;bins;config-get;config-set;df;digests;"
				"dump-fabric;dump-hb;dump-migrates;dump-msgs;dump-paxos;dump-smd;"
				"dump-wb;dump-wb-summary;dump-wr;dun;get-config;get-sl;hist-dump;"
				"hist-track-start;hist-track-stop;jem-stats;jobs;latency;log;log-set;"
				"logs;mcast;mem;mesh;mstats;mtrace;name;namespace;namespaces;"
				"node;service;services;services-alumni;set-config;set-log;sets;set-sl;"
				"show-devices;sindex;sindex-create;sindex-delete;sindex-dump;"
				"sindex-histogram;sindex-qnodemap;sindex-repair;"
				"smd;snub;statistics;status;tip;tip-clear;undun;version;"
				"xdr-min-lastshipinfo",
				false);
	/*
	 * help intentionally does not include the following:
	 * cluster-generation;features;objects;
	 * partition-generation;partition-info;partitions;replicas-master;
	 * replicas-prole;replicas-read;replicas-write;throughput
	 */

	// Set up some dynamic functions
	as_info_set_dynamic("bins", info_get_bins, false);                // Returns bin usage information and used bin names.
	as_info_set_dynamic("cluster-generation", info_get_cluster_generation, true); // Returns cluster generation.
	as_info_set_dynamic("get-config", info_get_config, false);        // Returns running config for specified context.
	as_info_set_dynamic("logs", info_get_logs, false);                // Returns a list of log file locations in use by this server.
	as_info_set_dynamic("namespaces", info_get_namespaces, false);    // Returns a list of namespace defined on this server.
	as_info_set_dynamic("objects", info_get_objects, false);          // Returns the number of objects stored on this server.
	as_info_set_dynamic("partition-generation", info_get_partition_generation, true); // Returns the current partition generation.
	as_info_set_dynamic("partition-info", info_get_partition_info, false);  // Returns partition ownership information.
	as_info_set_dynamic("replicas-read",info_get_replicas_read, false);     //
	as_info_set_dynamic("replicas-prole",info_get_replicas_prole, false);   // Base 64 encoded binary representation of partitions this node is prole (replica) for.
	as_info_set_dynamic("replicas-write",info_get_replicas_write, false);   //
	as_info_set_dynamic("replicas-master",info_get_replicas_master, false); // Base 64 encoded binary representation of partitions this node is master (replica) for.
	as_info_set_dynamic("replicas-all", info_get_replicas_all, false);      // Base 64 encoded binary representation of partitions this node is replica for.
	as_info_set_dynamic("service",info_get_service, false);           // IP address and server port for this node, expected to be a single.
	                                                                  // address/port per node, may be multiple address if this node is configured.
	                                                                  // to listen on multiple interfaces (typically not advised).
	as_info_set_dynamic("services",info_get_services, true);          // List of addresses of neighbor cluster nodes to advertise for Application to connect.
	as_info_set_dynamic("services-alumni",info_get_services_alumni, true); // All neighbor addresses (services) this server has ever know about.
	as_info_set_dynamic("sets", info_get_sets, false);                // Returns set statistics for all or a particular set.
	as_info_set_dynamic("statistics", info_get_stats, true);          // Returns system health and usage stats for this server.

#ifdef INFO_SEGV_TEST
	as_info_set_dynamic("segvtest", info_segv_test, true);
#endif

	// Tree-based names
	as_info_set_tree("bins", info_get_tree_bins);           // Returns bin usage information and used bin names for all or a particular namespace.
	as_info_set_tree("log", info_get_tree_log);             //
	as_info_set_tree("namespace", info_get_tree_namespace); // Returns health and usage stats for a particular namespace.
	as_info_set_tree("sets", info_get_tree_sets);           // Returns set statistics for all or a particular set.

	// Define commands
	as_info_set_command("alloc-info", info_command_alloc_info, PERM_NONE);                    // Lookup a memory allocation by program location.
	as_info_set_command("asm", info_command_asm, PERM_SERVICE_CTRL);                          // Control the operation of the ASMalloc library.
	as_info_set_command("config-get", info_command_config_get, PERM_NONE);                    // Returns running config for specified context.
	as_info_set_command("config-set", info_command_config_set, PERM_SET_CONFIG);              // Set a configuration parameter at run time, configuration parameter must be dynamic.
	as_info_set_command("df", info_command_double_free, PERM_SERVICE_CTRL);                   // Do an intentional double "free()" to test Double "free()" Detection.
	as_info_set_command("dump-fabric", info_command_dump_fabric, PERM_LOGGING_CTRL);          // Print debug information about fabric to the log file.
	as_info_set_command("dump-hb", info_command_dump_hb, PERM_LOGGING_CTRL);                  // Print debug information about heartbeat state to the log file.
	as_info_set_command("dump-migrates", info_command_dump_migrates, PERM_LOGGING_CTRL);      // Print debug information about migration.
	as_info_set_command("dump-msgs", info_command_dump_msgs, PERM_LOGGING_CTRL);              // Print debug information about existing 'msg' objects and queues to the log file.
	as_info_set_command("dump-paxos", info_command_dump_paxos, PERM_LOGGING_CTRL);            // Print debug information about Paxos state to the log file.
	as_info_set_command("dump-ra", info_command_dump_ra, PERM_LOGGING_CTRL);                  // Print debug information about Rack Aware state.
	as_info_set_command("dump-smd", info_command_dump_smd, PERM_LOGGING_CTRL);                // Print information about System Metadata (SMD) to the log file.
	as_info_set_command("dump-wb", info_command_dump_wb, PERM_LOGGING_CTRL);                  // Print debug information about Write Bocks (WB) to the log file.
	as_info_set_command("dump-wb-summary", info_command_dump_wb_summary, PERM_LOGGING_CTRL);  // Print summary information about all Write Blocks (WB) on a device to the log file.
	as_info_set_command("dump-wr", info_command_dump_wr, PERM_LOGGING_CTRL);                  // Print debug information about transaction hash table to the log file.
	as_info_set_command("dun", info_command_dun, PERM_SERVICE_CTRL);                          // Instruct this server to ignore another node.
	as_info_set_command("get-config", info_command_config_get, PERM_NONE);                    // Returns running config for all or a particular context.
	as_info_set_command("get-sl", info_command_get_sl, PERM_NONE);                            // Get the Paxos succession list.
	as_info_set_command("hist-dump", info_command_hist_dump, PERM_NONE);                      // Returns a histogram snapshot for a particular histogram.
	as_info_set_command("hist-track-start", info_command_hist_track, PERM_SERVICE_CTRL);      // Start or Restart histogram tracking.
	as_info_set_command("hist-track-stop", info_command_hist_track, PERM_SERVICE_CTRL);       // Stop histogram tracking.
	as_info_set_command("jem-stats", info_command_jem_stats, PERM_LOGGING_CTRL);              // Print JEMalloc statistics to the log file.
	as_info_set_command("latency", info_command_hist_track, PERM_NONE);                       // Returns latency and throughput information.
	as_info_set_command("log-set", info_command_log_set, PERM_LOGGING_CTRL);                  // Set values in the log system.
	as_info_set_command("mem", info_command_mem, PERM_NONE);                                  // Report on memory usage.
	as_info_set_command("mstats", info_command_mstats, PERM_LOGGING_CTRL);                    // Dump GLibC-level memory stats.
	as_info_set_command("mtrace", info_command_mtrace, PERM_SERVICE_CTRL);                    // Control GLibC-level memory tracing.
	as_info_set_command("set-config", info_command_config_set, PERM_SET_CONFIG);              // Set config values.
	as_info_set_command("set-log", info_command_log_set, PERM_LOGGING_CTRL);                  // Set values in the log system.
	as_info_set_command("set-sl", info_command_set_sl, PERM_SERVICE_CTRL);                    // Set the Paxos succession list.
	as_info_set_command("show-devices", info_command_show_devices, PERM_LOGGING_CTRL);        // Print snapshot of wblocks to the log file.
	as_info_set_command("smd", info_command_smd_cmd, PERM_SERVICE_CTRL);                      // Manipulate the System Metadata.
	as_info_set_command("snub", info_command_snub, PERM_SERVICE_CTRL);                        // Ignore heartbeats from a node for a specified amount of time.
	as_info_set_command("throughput", info_command_hist_track, PERM_NONE);                    // Returns throughput info.
	as_info_set_command("tip", info_command_tip, PERM_SERVICE_CTRL);                          // Add external IP to mesh-mode heartbeats.
	as_info_set_command("tip-clear", info_command_tip_clear, PERM_SERVICE_CTRL);              // Clear tip list from mesh-mode heartbeats.
	as_info_set_command("undun", info_command_undun, PERM_SERVICE_CTRL);                      // Instruct this server to not ignore another node.
	as_info_set_command("xdr-min-lastshipinfo", info_command_get_min_config, PERM_NONE);      // Get the min XDR lastshipinfo.

	// SINDEX
	as_info_set_dynamic("sindex", info_get_sindexes, false);
	as_info_set_tree("sindex", info_get_tree_sindexes);
	as_info_set_command("sindex-create", info_command_sindex_create, PERM_INDEX_MANAGE);  // Create a secondary index.
	as_info_set_command("sindex-delete", info_command_sindex_delete, PERM_INDEX_MANAGE);  // Delete a secondary index.

	// UDF
	as_info_set_dynamic("udf-list", udf_cask_info_list, false);
	as_info_set_command("udf-put", udf_cask_info_put, PERM_UDF_MANAGE);
	as_info_set_command("udf-get", udf_cask_info_get, PERM_NONE);
	as_info_set_command("udf-remove", udf_cask_info_remove, PERM_UDF_MANAGE);
	as_info_set_command("udf-clear-cache", udf_cask_info_clear_cache, PERM_UDF_MANAGE);

	// JOBS
	as_info_set_command("jobs", info_command_mon_cmd, PERM_JOB_MONITOR);  // Manipulate the multi-key lookup monitoring infrastructure.

	// Undocumented Secondary Index Command
	as_info_set_command("sindex-histogram", info_command_sindex_histogram, PERM_SERVICE_CTRL);
	as_info_set_command("sindex-repair", info_command_sindex_repair, PERM_SERVICE_CTRL);
	as_info_set_command("sindex-dump", info_command_sindex_dump, PERM_SERVICE_CTRL);  // Dumps to a file, not log.
	as_info_set_command("sindex-qnodemap", info_command_sindex_qnodemap, PERM_NONE);

	as_info_set_dynamic("query-list", as_query_list, false);
	as_info_set_command("query-kill", info_command_query_kill, PERM_QUERY_MANAGE);
	as_info_set_dynamic("query-stat", as_query_stat, false);
	as_info_set_command("scan-abort", info_command_abort_scan, PERM_SCAN_MANAGE);   // Abort a tscan with a given id.
	as_info_set_dynamic("scan-list", as_tscan_list, false);                         // List job ids of all scans.
	as_info_set_command("sindex-describe", info_command_sindex_describe, PERM_NONE);
	as_info_set_command("sindex-stat", info_command_sindex_stat, PERM_NONE);
	as_info_set_command("sindex-list", info_command_sindex_list, PERM_NONE);

	// Spin up the Info threads *after* all static and dynamic Info commands have been added
	// so we can guarantee that the static and dynamic lists will never again be changed.
	pthread_attr_t thr_attr;
	pthread_t tid;
	if (0 != pthread_attr_init(&thr_attr))
		cf_crash(AS_INFO, "pthread_attr_init: %s", cf_strerror(errno));

	for (int i = 0; i < g_config.n_info_threads; i++) {
		if (0 != pthread_create(&tid, &thr_attr, thr_info_fn, (void *) 0 )) {
			cf_crash(AS_INFO, "pthread_create: %s", cf_strerror(errno));
		}
	}

	as_fabric_register_msg_fn(M_TYPE_INFO, info_mt, sizeof(info_mt), info_msg_fn, 0 /* udata */ );

	// Take necessery steps if specific address is given in service address 
	if (strcmp(g_config.socket.addr, "0.0.0.0") != 0 ) {
		if (g_config.external_address != NULL){
			// check external-address is matches with service address
			if (strcmp(g_config.external_address, g_config.socket.addr) != 0) {
				cf_crash(AS_INFO, "external address:%s is not matching with service address:%s",
						g_config.external_address, g_config.socket.addr);
			}
		} else {
			// Check if service address is any. If not any then put this adress in external address 
			// to avoid updation of service list continuosly
			g_config.external_address = g_config.socket.addr;
		}
	}

	// if there's a statically configured external interface, use this simple function to monitor
	// and transmit
	if (g_config.external_address)
		pthread_create(&info_interfaces_th, 0, info_interfaces_static_fn, 0);
	// Or if we've got interfaces, monitor and transmit
	else
		pthread_create(&info_interfaces_th, 0, info_interfaces_fn, 0);

	return(0);
}
