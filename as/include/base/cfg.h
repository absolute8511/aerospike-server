/*
 * cfg.h
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
 * configuration structure
 */

#pragma once

#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <stdbool.h>
#include <stdint.h>

#include "xdr_config.h"

#include "aerospike/mod_lua_config.h"
#include "citrusleaf/cf_atomic.h"

#include "hist.h"
#include "hist_track.h"
#include "olock.h"
#include "queue.h"
#include "socket.h"
#include "util.h"

#include "base/cluster_config.h"
#include "base/datamodel.h"
#include "base/security_config.h"
#include "base/system_metadata.h"
#include "fabric/paxos.h"


#define MAX_TRANSACTION_QUEUES 128
#define MAX_DEMARSHAL_THREADS  256	// maximum number of demarshal worker threads
#define MAX_FABRIC_WORKERS 128		// maximum fabric worker threads
#define MAX_BATCH_THREADS 64		// maximum batch worker threads

struct as_namespace_s;

/* as_config
 * Runtime configuration */
typedef struct as_config_s {

	/* Global service configuration */
	uid_t				uid;
	gid_t				gid;
	char				*pidfile;
	bool				run_as_daemon;

	/* A unique instance ID: Either HW inspired, or Cluster Group/Node ID */
	cf_node				self_node;
	uint16_t			cluster_mode;
	cf_node				hw_self_node; // Cache the HW value self-node value, for various uses.

	/* IP address */
	char				*node_ip;

	/* Heartbeat system */
	hb_mode_enum		hb_mode;
	hb_protocol_enum	hb_protocol;
	char				*hb_addr;
	char 				*hb_init_addr;
	int					hb_port;
	int					hb_init_port;
	char				*hb_mesh_seed_addrs[AS_CLUSTER_SZ];
	int					hb_mesh_seed_ports[AS_CLUSTER_SZ];
	char				*hb_tx_addr;
	uint32_t			hb_interval;
	uint32_t			hb_timeout;
	unsigned char		hb_mcast_ttl;
	uint32_t			hb_mesh_rw_retry_timeout;

	uint64_t			start_ms; // filled with the start time of the server

	/* tuning parameters */
	int					n_migrate_threads;
	int					n_info_threads;
	int					n_batch_index_threads;
	int					n_batch_threads;

	/* Query tunables */
	uint32_t			query_threads;
	uint32_t			query_worker_threads;
	uint32_t			query_priority;
	uint32_t			query_sleep;
	uint32_t			query_bsize;
	bool				query_job_tracking;
	bool				query_in_transaction_thr;
	uint64_t			query_buf_size;
	uint32_t			query_threshold;
	uint32_t			query_rec_count_bound;
	bool				query_req_in_query_thread;
	uint32_t			query_req_max_inflight;
	uint32_t			query_bufpool_size;
	uint32_t			query_short_q_max_size;
	uint32_t			query_long_q_max_size;
	uint32_t			query_untracked_time_ns;

	int					n_transaction_queues;
	int					n_transaction_threads_per_queue;
	int					n_transaction_duplicate_threads;
	int					n_service_threads;
	int					n_fabric_workers;
	bool				use_queue_per_device;
	bool				allow_inline_transactions;

	/* max client file descriptors */
	int					n_proto_fd_max;

	/* after this many milliseconds, connections are aborted unless transaction is in progress */
	int					proto_fd_idle_ms;

	/* sleep this many millisecond before retrying for all the blocked query */
	int					proto_slow_netio_sleep_ms;

	/* The TCP port for the fabric */
	int					fabric_port;

	/* Fabric TCP socket keepalive parameters */
	bool				fabric_keepalive_enabled;
	int					fabric_keepalive_time;
	int					fabric_keepalive_intvl;
	int					fabric_keepalive_probes;

	/* The TCP port for the info socket */
	int					info_port;

	/* Whether to bypass thr_tsvc for Info protocol requests. */
	bool				info_fastpath_enabled;

	/* The TCP socket for the listener */
	cf_socket_cfg		socket;

	char				*external_address; // hostname that clients will connect on
	bool				is_external_address_virtual;
	char				*network_interface_name; // network_interface_name to use on this machine for generating the IP addresses

	/* Whether or not a socket can be reused (SO_REUSEADDR) */
	bool				socket_reuse_addr;

	/* Consensus algorithm runtime data */
	as_paxos			*paxos;

	/*
	 * heartbeat: takes the lock and fills in this structure
	 * paxos: read only uses it for detecting changes
	 */
	cf_node				hb_paxos_succ_list_index[AS_CLUSTER_SZ];
	cf_node				hb_paxos_succ_list[AS_CLUSTER_SZ][AS_CLUSTER_SZ];
	pthread_mutex_t		hb_paxos_lock;

	/* System Metadata module state */
	as_smd_t			*smd;

	/* a global generation count on all partition state changes */
	cf_atomic_int		partition_generation;

	/* The transaction queues */
	uint32_t			transactionq_current;
	cf_queue			*transactionq_a[MAX_TRANSACTION_QUEUES];
	cf_queue			*transaction_slow_q; // One slow queue to hold sleepy trans

	/* object lock structure */
	olock				*record_locks;

	/* global configuration for how often to print 'ticker' info to the log - 0 is no ticker */
	uint32_t			ticker_interval;

	// whether to collect microbenchmarks
	bool				microbenchmarks;

	// whether to collect storage benchmarks
	bool				storage_benchmarks;
	
	// whether to collect ldt benchmarks
	bool				ldt_benchmarks;

	// whether memory accounting is enabled
	bool				memory_accounting;

	// whether ASMalloc integration is enabled
	bool				asmalloc_enabled;

	// whether to log information about existing "msg" objects and queues
	bool				fabric_dump_msgs;

	// maximum number of "msg" objects permitted per type
	int64_t				max_msgs_per_type;

	// size at which to dump incoming data
	uint32_t			dump_message_above_size;

	// the common work directory cache
	char				*work_directory;

	/*
	**  TUNING PARAMETERS
	*/

	/* global timeout configuration */
	uint32_t			transaction_retry_ms;
	// max time (ns) in the system before we kick the request out forever
	uint64_t			transaction_max_ns;
	// transaction pending limit - number of pending transactions ON A SINGLE RECORD (0 means no limit)
	uint32_t			transaction_pending_limit;
	/* transaction_repeatable_read flag defines whether a read should attempt to get all duplicate values before returning */
	bool				transaction_repeatable_read;
	/* disable generation checking */
	bool				generation_disable;
	bool				write_duplicate_resolution_disable;
	/* respond client on master completion */
	bool				respond_client_on_master_completion;
	// replication is queued and sent
	bool				replication_fire_and_forget;
	/* enables node snubbing - this code caused a Paxos issue in the past */
	bool				snub_nodes;

	uint32_t			scan_max_active;	// maximum number of active scans allowed
	uint32_t			scan_max_done;		// maximum number of finished scans kept for monitoring
	uint32_t			scan_threads;		// size of scan thread pool

	// maximum count of database requests in a single batch
	uint32_t			batch_max_requests;
	// maximum number of buffers allowed in a buffer queue at any one time.  Fail batch if full.
	uint32_t			batch_max_buffers_per_queue;
	// maximum number of buffers allowed in buffer pool at any one time.
	uint32_t			batch_max_unused_buffers;
	// number of records between an enforced context switch - thus 1 is very low priority, 1000000 would be very high
	uint32_t			batch_priority;  // Used by old batch functionality only.

	// nsup (expiration and eviction) tuning parameters
	uint32_t			nsup_delete_sleep; // sleep this many microseconds between generating delete transactions, default 0
	uint32_t			nsup_period;
	bool				nsup_startup_evict;

	/* tuning parameter for how often to run retransmit checks for paxos */
	uint32_t			paxos_retransmit_period;
	/* parameter that let the cluster run under lower replication factor for less than 1 */
	uint32_t			paxos_single_replica_limit; // cluster size at which, and below, the cluster will run with repl factor 1
	/* Maximum size of cluster allowed to be formed. */
	uint64_t			paxos_max_cluster_size;
	/* Currently-active Paxos protocol version. */
	paxos_protocol_enum	paxos_protocol;
	/* Currently-active Paxos recovery policy. */
	paxos_recovery_policy_enum	paxos_recovery_policy;

	/* number of records between an enforced context switch --- 1 is lowest possible priority, 10000 would be full-speed */
	uint32_t			migrate_xmit_priority;
	uint32_t			migrate_xmit_sleep;
	uint32_t			migrate_read_priority;
	uint32_t			migrate_read_sleep;
	uint32_t			migrate_xmit_hwm;
	uint32_t			migrate_xmit_lwm;
	// For receiver-side migration flow control:
	int					migrate_max_num_incoming;
	cf_atomic_int		migrate_num_incoming;
	// For debouncing re-tansmitted migrate start messages:
	int					migrate_rx_lifetime_ms;

	uint32_t			fb_health_msg_per_burst; // health probe paxos messages per "burst"
	uint32_t			fb_health_msg_timeout; // milliseconds after which to give up on health probe message
	uint32_t			fb_health_good_pct; // percent of successful messages in a burst at/above which node is deemed ok
	uint32_t			fb_health_bad_pct; // percent of successful messages in a burst at/below which node is deemed bad
	bool				auto_dun; // enables fb health and paxos to dun nodes
	bool				auto_undun; // enables fb health to undun nodes that have been dunned

	// Temporary dangling prole garbage collection.
	uint32_t			prole_extra_ttl;	// seconds beyond expiry time after which we garbage collect, 0 for no garbage collection
	bool				non_master_sets_delete;	// locally delete non-master records in sets that are being emptied

	xdr_config			xdr_cfg;							// XDR related config parameters
	xdr_lastship_s		xdr_lastship[AS_CLUSTER_SZ];		// last XDR shipping info of other nodes
	cf_node				xdr_clmap[AS_CLUSTER_SZ];			// cluster map as known to XDR
	uint64_t			xdr_self_lastshiptime[DC_MAX_NUM];	// last XDR shipping by this node

	// configuration to put cap on amount of memory
	// all secondary index put together can take
	// this is to protect cluster. This override the
	// per namespace configured value
	uint32_t		sindex_builder_threads;   // Secondary index builder thread pool size
	uint64_t		sindex_data_max_memory;   // Maximum memory for secondary index trees
	cf_atomic_int	sindex_data_memory_used;  // Maximum memory for secondary index trees
	cf_atomic_int   sindex_gc_timedout;           // Number of time sindex gc iteration timed out waiting for partition lock
	uint64_t        sindex_gc_inactivity_dur;     // Commulative sum of sindex GC thread inactivity.
	uint64_t        sindex_gc_activity_dur;       // Commulative sum of sindex gc thread activity.
	uint64_t        sindex_gc_list_creation_time; // Commulative sum of list creation phase in sindex GC
	uint64_t        sindex_gc_list_deletion_time; // Commulative sum of list deletion phase in sindex GC
	uint64_t        sindex_gc_garbage_found;      // Amount of garbage found during list creation phase
	uint64_t        sindex_gc_garbage_cleaned;    // Amount of garbage deleted during list deletion phase
	uint64_t        sindex_gc_objects_validated;  // Commulative sum of sindex objects validated
	bool            sindex_gc_enable_histogram;
	histogram      *_sindex_gc_validate_obj_hist; // Histogram to track time taken to validate sindex object
	histogram      *_sindex_gc_delete_obj_hist;   // Histogram to track time taken to delelte sindex object by GC
	histogram      *_sindex_gc_pimd_rlock_hist;   // HIstogram to track time spent under pimd rlock by sindex GC
	histogram      *_sindex_gc_pimd_wlock_hist;   // Histogram to track time spent under pimd wlock by sindex GC

	bool                qnodes_pre_reserved;      // If true we will reserve all the qnodes upfront 
												  // before processing query. Default - TRUE
	cf_atomic64			query_reqs;
	cf_atomic64			query_fail;
	cf_atomic64			query_short_queue_full;
	cf_atomic64			query_long_queue_full;
	cf_atomic64			query_short_running;
	cf_atomic64			query_long_running;
	cf_atomic64			query_tracked;
	cf_atomic64			query_false_positives;
	bool				query_enable_histogram;

	// Aggregation stat
	cf_atomic64			n_aggregation;
	cf_atomic64			n_agg_success;
	cf_atomic64			n_agg_abort;
	cf_atomic64			n_agg_errs;
	cf_atomic64			agg_response_size;
	cf_atomic64			agg_num_records;

	// Lookup stat
	cf_atomic64			n_lookup;
	cf_atomic64			n_lookup_success;
	cf_atomic64			n_lookup_abort;
	cf_atomic64			n_lookup_errs;
	cf_atomic64			lookup_response_size;
	cf_atomic64			lookup_num_records;

	uint64_t			udf_runtime_max_memory; // Maximum runtime memory allowed for per UDF
	uint64_t			udf_runtime_max_gmemory; // maximum runtime memory alloed for all UDF
	cf_atomic_int		udf_runtime_gmemory_used; // Current runtime memory reserve by per UDF - BUG if global should be 64?

	/*
	** STATISTICS
	*/
	cf_atomic_int		fabric_msgs_sent;
	cf_atomic_int		fabric_msgs_rcvd;
	cf_atomic_int		fabric_msgs_selfsend;  // not included in prev send + receive
	cf_atomic_int		fabric_write_short;
	cf_atomic_int		fabric_write_medium;
	cf_atomic_int		fabric_write_long;
	cf_atomic_int		fabric_read_short;
	cf_atomic_int		fabric_read_medium;
	cf_atomic_int		fabric_read_long;
	cf_atomic_int		migrate_msgs_sent;
	cf_atomic_int		migrate_msgs_rcvd;
	cf_atomic_int		migrate_inserts_sent;
	cf_atomic_int		migrate_inserts_rcvd;
	cf_atomic_int		migrate_acks_sent;
	cf_atomic_int		migrate_acks_rcvd;
	cf_atomic_int		migrate_progress_send;
	cf_atomic_int		migrate_progress_recv;
	cf_atomic_int		migrate_reads;
	cf_atomic_int		migrate_num_incoming_accepted;
	cf_atomic_int		migrate_num_incoming_refused; // For receiver-side migration flow control.
	cf_atomic_int		proto_transactions;
	cf_atomic_int		proxy_initiate; // initiated
	cf_atomic_int		ldt_proxy_initiate; // initiated
	cf_atomic_int		proxy_action;   // did it
	cf_atomic_int		proxy_retry;    // retried it
	cf_atomic_int		ldt_proxy_timeout;    // retried it
	cf_atomic_int		proxy_retry_q_full;
	cf_atomic_int		proxy_unproxy;
	cf_atomic_int		proxy_retry_same_dest;
	cf_atomic_int		proxy_retry_new_dest;
	cf_atomic_int		basic_scans_succeeded;
	cf_atomic_int		basic_scans_failed;
	cf_atomic_int		aggr_scans_succeeded;
	cf_atomic_int		aggr_scans_failed;
	cf_atomic_int		udf_bg_scans_succeeded;
	cf_atomic_int		udf_bg_scans_failed;
	cf_atomic_int		write_master;
	cf_atomic_int		write_prole;
	cf_atomic_int		read_dup_prole;
	cf_atomic_int		rw_err_dup_internal;
	// When rw_dup_prole() sees a cluster key mismatch, we increment this counter.
	cf_atomic_int		rw_err_dup_cluster_key;
	cf_atomic_int		rw_err_dup_send;
	cf_atomic_int		rw_err_write_internal;
	cf_atomic_int		rw_err_write_cluster_key;
	cf_atomic_int		rw_err_write_send;
	cf_atomic_int		rw_err_ack_internal;
	cf_atomic_int		rw_err_ack_nomatch;
	cf_atomic_int		rw_err_ack_badnode;
	cf_atomic_int		proto_connections_opened;
	cf_atomic_int		proto_connections_closed;
	cf_atomic_int		fabric_connections_opened;
	cf_atomic_int		fabric_connections_closed;
	cf_atomic_int		heartbeat_connections_opened;
	cf_atomic_int		heartbeat_connections_closed;
	cf_atomic_int		heartbeat_received_self;
	cf_atomic_int		heartbeat_received_foreign;
	cf_atomic_int		info_connections_opened;
	cf_atomic_int		info_connections_closed;
	cf_atomic_int		n_waiting_transactions;
	cf_atomic_int		global_record_ref_count;
	cf_atomic_int		global_record_lock_count;
	cf_atomic_int		global_tree_count;
	cf_atomic_int		write_req_object_count;
	cf_atomic_int		migrate_tx_object_count;
	cf_atomic_int		migrate_rx_object_count;
	cf_atomic_int		nsup_tree_count;
	cf_atomic_int		nsup_subtree_count;
	cf_atomic_int		scan_tree_count;
	cf_atomic_int		dup_tree_count;
	cf_atomic_int		wprocess_tree_count;
	cf_atomic_int		migrx_tree_count;
	cf_atomic_int		migtx_tree_count;
	cf_atomic_int		ssdr_tree_count;
	cf_atomic_int		ssdw_tree_count;
	cf_atomic_int		rw_tree_count;
	cf_atomic_int		reaper_count;

	cf_atomic_int		batch_index_initiate;
	cf_atomic_int		batch_index_complete;
	cf_atomic_int		batch_index_timeout;
	cf_atomic_int		batch_index_errors;

	cf_atomic_int		batch_initiate;
	cf_atomic_int		batch_tree_count;
	cf_atomic_int		batch_timeout;
	cf_atomic_int		batch_errors;

	cf_hist_track *		rt_hist; // histogram that tracks read performance
	cf_hist_track *		ut_hist; // histogram that tracks udf performance
	cf_hist_track *		wt_hist; // histogram that tracks write performance
	cf_hist_track *		px_hist; // histogram that tracks proxy performance
	cf_hist_track *		wt_reply_hist; // write histogram from start to reply to client
	cf_hist_track *		q_hist;  // histogram that tracks query performance
	cf_hist_track *		q_rcnt_hist;  // histogram that tracks query row count

	uint32_t			hist_track_back; // total time span in seconds over which to cache data
	uint32_t			hist_track_slice; // period in seconds at which to cache histogram data
	char *				hist_track_thresholds; // comma-separated bucket (ms) values to track

	histogram *			rt_cleanup_hist; // histogram around as_storage_record_close and as_record_done
	histogram *			rt_net_hist; // histogram around the network send on reads
	histogram *			wt_net_hist; // histogram around the network send on writes
	histogram *			rt_storage_read_hist; // histogram taken from after opening the device to after reading from device
	histogram *			rt_storage_open_hist; // histogram around as_storage_record_open
	histogram *			rt_tree_hist; // histogram from rw_complete to fetching record from rb tree
	histogram *			rt_internal_hist; // read histogram from internal to rw_complete
	histogram *			wt_internal_hist; // write histogram from internal to either send to prole or return to client (if no replication)
	histogram *			rt_start_hist; // read histogram from read_start to internal
	histogram *			wt_start_hist; // write histogram from write_start to internal
	histogram *			rt_q_process_hist; // histogram from transaction off q to read_start
	histogram *			wt_q_process_hist; // histogram from transaction off q to write_start
	histogram *			q_wait_hist; // histogram taken right after transaction is plucked off the q
	histogram *			demarshal_hist; // histogram around demarshal loop only
	histogram *			wt_master_wait_prole_hist; // histogram of time spent on master between sending rw and ack
	histogram *			wt_prole_hist; // histogram that tracks write replication performance (in fabric)
	histogram *			rt_resolve_hist; // histogram that tracks duplicate resolution after receiving all messages from other nodes
	histogram *			wt_resolve_hist; // histogram that tracks duplicate resolution after receiving all messages from other nodes
	histogram *			rt_resolve_wait_hist; // histogram that tracks the time the master waits for other nodes to complete duplicate resolution on reads
	histogram *			wt_resolve_wait_hist; // histogram that tracks the time the master waits for other nodes to complete duplicate resolution on writes
	histogram *			error_hist;  // histogram of error requests only
	histogram *			batch_index_reads_hist;        // New batch index protocol latency histogram.
	histogram *			batch_q_process_hist; // Old batch direct protocol latency histogram.
	histogram *			info_tr_q_process_hist;  // histogram of time spent processing info messages in transaction q
	histogram *			info_q_wait_hist;  // histogram of time info transaction spends on info q
	histogram *			info_post_lock_hist; // histogram of time spent processing the Info command under the mutex before sending the response on the network
	histogram *			info_fulfill_hist; // histogram of time spent to fulfill info request after taking it off the info q

	histogram *			write_storage_close_hist; // histogram of time spent around record_storage close on a write path
	histogram *			write_sindex_hist; // secondary index latency histogram
	histogram *			defrag_storage_close_hist; // histogram of time spent around record_storage close on a defrag path
	histogram *			prole_fabric_send_hist; // histogram of time spent for prole fabric getting queued

#ifdef HISTOGRAM_OBJECT_LATENCY
	// these track read latencies of different size databuckets
	histogram *			read0_hist;
	histogram *			read1_hist;
	histogram *			read2_hist;
	histogram *			read3_hist;
	histogram *			read4_hist;
	histogram *			read5_hist;
	histogram *			read6_hist;
	histogram *			read7_hist;
	histogram *			read8_hist;
	histogram *			read9_hist;
#endif

	// LDT related histogram
	histogram *			ldt_multiop_prole_hist;   // histogram that tracks LDT multi op replication performance (in fabric)
	histogram *			ldt_update_record_cnt_hist; // histogram that tracks number of records written (write/update)
                                             // by LDT UDF execluding parent record
	histogram *			ldt_io_record_cnt_hist; // histogram that tracks number of records opened (write/update)
                                             // by LDT UDF execluding parent record
	histogram *			ldt_update_io_bytes_hist;   // histogram that tracks number bytes written by LDT every transaction
	histogram * 		ldt_hist;            // histogram that tracks ldt performance

	cf_atomic_int		stat_read_reqs;
	cf_atomic_int		stat_read_reqs_xdr;
	cf_atomic_int		stat_read_success;
	cf_atomic_int		stat_read_errs_notfound;
	cf_atomic_int		stat_read_errs_other;

	cf_atomic_int		stat_write_reqs;
	cf_atomic_int		stat_write_reqs_xdr;
	cf_atomic_int		stat_write_success;
	cf_atomic_int		stat_write_errs; // deprecated
	cf_atomic_int		stat_write_errs_notfound;
	cf_atomic_int		stat_write_errs_other;
	cf_atomic_int		stat_write_latency_gt50;
	cf_atomic_int		stat_write_latency_gt100;
	cf_atomic_int		stat_write_latency_gt250;
	cf_atomic_int		stat_xdr_pipe_writes;
	cf_atomic_int		stat_xdr_pipe_miss;

	cf_atomic_int		stat_delete_success;
	cf_atomic_int		stat_rw_timeout;

	cf_atomic_int		stat_compressed_pkts_received;

	cf_atomic_int		stat_proxy_reqs;
	cf_atomic_int		stat_proxy_reqs_xdr;
	cf_atomic_int		stat_proxy_success;
	cf_atomic_int		stat_proxy_errs;
	cf_atomic_int		stat_proxy_retransmits;
	cf_atomic_int		stat_proxy_redirect;

	// When another proxy has passed in a transaction, and it is from a different
	// cluster instance (cluster key mismatch), then we retry the transaction
	// by diverting it to another proxy (either proxy_divert() or proxy_send_redirect()).
	cf_atomic_int		stat_cluster_key_trans_to_proxy_retry;

	// When the cluster keys match (either it's a client-generated transaction,
	// or the proxy tr CK matches the Partition CK), BUT the node itself is not yet
	// consistent (the Node CK doesn't match the Partition CK), we must re-queue
	// the transaction.
	cf_atomic_int		stat_cluster_key_transaction_reenqueue;

	// When the cluster keys match (either it's a client-generated transaction,
	// or the proxy tr CK matches the Partition CK), BUT the node itself is not yet
	// consistent (the Node CK doesn't match the Partition CK), we queue the
	// transaction in a special "slow" transaction queue -- until the
	// CK mismatch has been resolved.
	cf_atomic_int		stat_slow_trans_queue_push;

	// After we have queued up a transaction on the slow queue, we then count
	// the number of times pop off a transaction from the slow queue and
	// we re-queue it on to the regular queue.  We expect slow queue push
	// and pop to match.
	cf_atomic_int		stat_slow_trans_queue_pop;

	// When we pop from the slow queue -- we pop the entire batch of held
	// transactions in one quick "whooosh".  Here we count the number of
	// slow queue batch POP operations.
	cf_atomic_int		stat_slow_trans_queue_batch_pop;

	// For all REGULAR jobs (that pass thru the CK test), count the number of
	// regular RW jobs processed.
	cf_atomic_int		stat_cluster_key_regular_processed;

	// Problem writing the prole -- Retry (return CK error), which will cause
	// the master to requeue the transaction and try it again.  This is also known
	// as the "ABSENT PARTITION" or "TURD" problem, which we fix by having the
	// prole spot the CK mismatch and returning CLUSTER_KEY_MISMATCH error.
	cf_atomic_int		stat_cluster_key_prole_retry;

	// The Number of Partitions that were impacted by Partition Transaction Queue.
	cf_atomic_int		stat_cluster_key_partition_transaction_queue_count;

	// When a Prole Write fails, it returns a CLUSTER_KEY_MISMATCH error to the
	// master, who then re-queues the transaction to be performed again.
	// There are two cases: One for Duplicate transactions and one for RW trans.
	cf_atomic_int		stat_cluster_key_err_ack_dup_trans_reenqueue;
	cf_atomic_int		stat_cluster_key_err_ack_rw_trans_reenqueue;

	cf_atomic_int		stat_expired_objects;
	cf_atomic_int		stat_evicted_objects;
	cf_atomic_int		stat_deleted_set_objects;
	cf_atomic_int		stat_evicted_set_objects;
	cf_atomic_int		stat_evicted_objects_time;
	cf_atomic_int		stat_zero_bin_records;
	cf_atomic_int		stat_nsup_deletes_not_shipped;

	cf_atomic_int		err_tsvc_requests;
	cf_atomic_int		err_tsvc_requests_timeout;
	cf_atomic_int		err_out_of_space;
	cf_atomic_int		err_duplicate_proxy_request;
	cf_atomic_int		err_rw_request_not_found;
	cf_atomic_int		err_rw_cant_put_unique;
	cf_atomic_int		err_rw_pending_limit;

	cf_atomic_int		err_replica_null_node;
	cf_atomic_int		err_replica_non_null_node;
	cf_atomic_int		err_sync_copy_null_node;
	cf_atomic_int		err_sync_copy_null_master;

	cf_atomic_int		err_storage_queue_full;
	cf_atomic_int		err_storage_defrag_corrupt_record;

	cf_atomic_int		err_write_fail_unknown;
	cf_atomic_int		err_write_fail_key_exists;
	cf_atomic_int		err_write_fail_generation;
	cf_atomic_int		err_write_fail_generation_xdr;
	cf_atomic_int		err_write_fail_bin_exists;
	cf_atomic_int		err_write_fail_parameter;
	cf_atomic_int		err_write_fail_noxdr;
	cf_atomic_int		err_write_fail_prole_unknown;
	cf_atomic_int		err_write_fail_prole_generation;
	cf_atomic_int		err_write_fail_not_found;
	cf_atomic_int		err_write_fail_incompatible_type;
	cf_atomic_int		err_write_fail_prole_delete;
	cf_atomic_int		err_write_fail_key_mismatch;
	cf_atomic_int		err_write_fail_record_too_big;
	cf_atomic_int		err_write_fail_bin_name;
	cf_atomic_int		err_write_fail_bin_not_found;
	cf_atomic_int		err_write_fail_forbidden;

	cf_atomic_int		stat_duplicate_operation;

	//stats for UDF read - write operation.
	cf_atomic_int		udf_read_reqs;
	cf_atomic_int		udf_read_success;
	cf_atomic_int		udf_read_errs_other;

	cf_atomic_int		udf_write_reqs;
	cf_atomic_int		udf_write_success;
	cf_atomic_int		udf_write_errs_other;

	cf_atomic_int		udf_delete_reqs;
	cf_atomic_int		udf_delete_success;
	cf_atomic_int		udf_delete_errs_other;

	cf_atomic_int		udf_lua_errs;

	cf_atomic_int		udf_scan_rec_reqs;
	cf_atomic_int		udf_query_rec_reqs;
	cf_atomic_int		udf_replica_writes;

	// For Lua Garbage Collection, we want to track three things:
	// (1) The number of times we were below the GC threshold
	// (2) The number of times we performed "Light GC" (step-wise gc)
	// (3) The number of times we performed "Heavy GC" (full gc)
	// Currently, however, there is no direct connection between the g_config
	// object and the mod-lua world, so we will need to use some other
	// mechanism to fill in these stats.  They are inactive for now.
	// (May 19, 2014 tjl)
	// cf_atomic_int	stat_lua_gc_delay;
	// cf_atomic_int	stat_lua_gc_step;
	// cf_atomic_int	stat_lua_gc_full;

	/* Namespaces */
	uint32_t			namespaces;
	struct as_namespace_s * namespace[AS_NAMESPACE_SZ];

	// To speed up transaction enqueue's determination of data-in-memory:
	uint32_t			n_namespaces_in_memory;
	uint32_t			n_namespaces_not_in_memory;

	// MOD_LUA Config
	mod_lua_config		mod_lua;

	// Cluster Config Info
	cluster_config_t	cluster;

	// Security configuration info.
	as_sec_config		sec_cfg;

} as_config;

/* Configuration function declarations */
extern as_config *as_config_init(const char *config_file);
extern void as_config_post_process(as_config *c, const char *config_file);

/* Declare an instance of the configuration structure in global scope */
extern as_config g_config;
