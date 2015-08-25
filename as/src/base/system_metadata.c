/*
 * system_metadata.c
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
 *  SYNOPSIS
 *    The System Metadata module provides a mechanism for synchronizing
 *    module metadata cluster-wide.  While each module is responsible
 *    for the interpretation of its own metadata, the System Metadata
 *    module provides persistence and automatic distribution of changes
 *    to that opaque metadata.
 */

// #define DEBUG  // [Note:  Requires DEBUG_HASH to be defined in "cf_rchash.c" as well.]

#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>

#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_rchash.h"
#include "citrusleaf/cf_shash.h"

#include "msg.h"

#include "base/cfg.h"
#include "base/secondary_index.h"
#include "base/system_metadata.h"
#include "fabric/fabric.h"
#include "jansson.h"


/*
**                                 System Metadata Theory of Operation
**                                 ===================================
**
**   Overview:
**   ---------
**
**   The System Metadata (SMD) module provides the means for an Aerospike cluster to manage and
**   automatically and consistently distribute data describing the state of any number of modules
**   within each of the cluster nodes.  This data is called "system metadata."  System metadata
**   is managed on a module-by-module basis, where each registered module has a set of zero or
**   more SMD items.  An SMD item has properties describing the item (module name, key, value
**   generation, and modification timestamp.)  The contents (value) of an SMD item is opaque to
**   the SMD module itself.  At creation time, modules may register policy callback functions
**   to perform the actions of merging and accepting metadata updates, or else select the system
**   default policy for these operations.
**
**   Initialization:
**   ---------------
**
**   Prior to use, the System Metadata module must first be initialized by calling "as_smd_init()"
**   to create the SMD internal data structures and launch a captive thread to process all
**   incoming system metadata events.  During this phase, all system metadata operations will be
**   handled locally on each node.
**
**   Once all server components have been initialized, SMD may be started via "as_smd_start()".
**   At this point, SMD will begin handling Paxos cluster state change events and begin
**   communicating with SMD in other cluster nodes via SMD fabric messages to synchronize
**   system metadata cluster-wide.  Fabric transactions are used guarantee message delivery
**   succeeds or fails atomically, with re-try handled automatically at fabric level.
**
**   The System Metadata module may be terminated using "as_smd_shutdown()", which de-registers
**   the SMD fabric message type and causes the captive thread to exit.  At this point, it is
**   permissible to re-initialize (and then re-start) the System Metadata module again.
**
**   Life Cycle of System Metadata:
**   ------------------------------
**
**   For a server component to use System Metadata, the component must first create its SMD
**   module.  The SMD API names modules via a name string which must be unique within the
**   server.  Calling "as_smd_create_module()" will create a container object in SMD to
**   hold the module's metadata and register any supplied policy callback functions provided by
**   the component.  To release the component's SMD module, call "as_smd_destroy_module()".
**
**   After a module has been created, new metadata items may be added, or existing items may
**   be modified, using "as_smd_set_metadata()".  Existing metadata items may be removed using
**   "as_smd_delete_metadata()".  Metadata may be searched using "as_smd_get_metadata()", which
**   can return one or more items for one or more modules, depending upon the item list passed
**   in, and sends the search results to a user-supplied callback function.
**
**   Each module's metadata is automatically persisted via serialization (in JSON format) to a file
**   upon each accepted metadata item change and also when the module is destroyed.  When a module
**   is created (usually at server start-up time), if an existing SMD file is found for the module,
**   its contents will be loaded in as the initial values of the module's metadata.
**
**   System Metadata Policy Callback Functions:
**   ------------------------------------------
**
**   There are three SMD policy callback functions a module may register.  If NULL is passed
**   for a callback function pointer in "as_smd_module_create()", the system default policy
**   will be selected for that operation.  All policy callbacks are executed in the context
**   of the SMD thread.
**
**   The SMD policy callbacks operate as follows:
**
**     1). The Merge Callback ("as_smd_merge_cb()"):  When a Paxos cluster state change occurs,
**          each module's Merge callback will be executed on the Paxos principle to create a new,
**          unified view of each module's metadata.  The system default merge policy is to simply
**          form a union of all nodes' metadata items for the given module, taking the latest
**          version of metadata items with duplicate keys, chosen first by highest generation
**          and second by highest timestamp.
**
**     2). The Accept Callback ("as_smd_accept_cb()"):  When a modules SMD item(s) are changed,
**          or when a module is created and fully restored from persistence, the Accept callback
**          will be invoked on every node to commit the change, with the originator of the accept
**          event passed as the accept option parameter value.
**
**          This callback will be invoked in three distinct cases:
**
**          First, when a module is created and its persisted metadata (if any) has been fully
**          restored, this callback will be invoked with the OPT_CREATE accept option and a
**          NULL item list.  This event is the proper point for synchronizing with any other
**          thread(s) who depend upon the given module being fully initialized.
**
**          Second, after the the Paxos principal has determined the merged metadata for a
**          module, it will distribute the new metadata to all cluster nodes (including itself)
**          for processing via the Accept callback with the OPT_MERGE accept option and an item
**          list of length 0 or greater.  The system default accept policy is simply to replace
**          any preexisting metadata items for the module with the received metadata items.
**          Modules will generally, however, define their own Accept callback to take actions
**          based upon the changed metadata, such as creating secondary indexes or defining new
**          User Defined Functions (UDFs.)
**
**          Third, when a metadata item is set or deleted via the SMD API (or at module creation
**          time, via restoration from persisted state), the Accept callback will be invoked with
**          the OPT_API accept option and an item list of length 1.  Note that at system start-up
**          time, prior to cluster formation, the metadata change will be handled locally.  Once
**          a cluster has been joined, however, each metadata change event will be proxied to
**          the Paxos principal, who will forward it to every cluster node (including itself)
**          for acceptance.
**
**     3). The Can Accept Callback ("as_smd_can_accept_cb()"):  When the Paxos principal
**          receives a metadata change request (set or delete), it will first attempt to
**          validate the request via any registered Can Accept callback.  If the callback
**          exists, it must return non-zero for the item to be processed.  Otherwise the item
**          will be rejected.
**
**   Threading Structure:
**   --------------------
**
**   The System Metadata module relies on a single, captive thread to handle all incoming SMD
**   fabric messages, public SMD API operations, and to invoke module's registered policy
**   callbacks.  Single-thread access means no locking of SMD data structures is necessary.
**
**   The SMD thread waits on a queue for messages from either the local node (created and sent
**   via the System Metadata API functions) or from other cluster nodes (via System Metadata
**   fabric messages.)
**
**   Initially the System Metadata module is inactive until the "as_smd_init()" function launches
**   the System Metadata thread.  At this point, only node-local SMD commands and events will be
**   processed.  When "as_smd_start()" is called, a START message will be sent telling the SMD
**   thread to also begin receiving SMD events for Paxos cluster state change notifications
**   and from other cluster nodes via SMD fabric messages.  SMD will now perform the full
**   policy callback processing as describe above.  The System Metadata module will be running
**   until the "as_smd_shutdown()" function sends a SHUTDOWN message, upon receipt of which the
**   System Metadata thread will exit cleanly.
**
**   Internal Messaging Structure:
**   -----------------------------
**
**   Each public SMD API function invocation corresponds to an event message being sent to the
**   System Metadata thread via its message queue for processing.  Internal command messages
**   (those not generated by API calls) are also sent via the message queue to handle Paxos
**   events, incoming SMD fabric messages, and other internal utility functions.
**
**   Each event is defined by an event type, options bits, and a metadata item (which may be
**   NULL or partially populated, depending upon the command type.)
**
**   The SMD command message types are:
**
**    1). INIT / START / SHUTDOWN:  These messages correspond to the APIs controlling the
**          running of the SMD subsystem itself and its captive thread.
**
**    2). CREATE_MODULE / DESTROY_MODULE:  These messages create and destroy module objects
**          containing metadata items.
**
**    3). SET_METADATA / DELETE_METADATA / GET_METADATA:  The SMD API sends these messages to
**          set, delete, and get metadata items.
**
**    4). INTERNAL:  This message type is used for non-API "internal" events such as the event
**          triggered by a Paxos cluster state change notification, incoming SMD fabric
**          messages from other nodes, or to dump info. about the state of system metadata to
**          the system log.
**
**   Debugging Utilities:
**   --------------------
**
**   The state of the System Metadata module can be logged using the "dump-smd:" Info command:
**
**     dump-smd:[verbose={"true"|"false"}]   (Default: "false".)
**
**   The optional option "verbose" parameter may be set to "true" to log additional detailed
**   information about the system metadata, such as information about all modules' metadata items.
**
**   System Metadata may be directly manipulated using the "smd:" Info command:
**
**     smd:cmd=<SMDCommand>[;module=<String>;node=<HexNodeID>;key=<String>;value=<String>]
**
**   where <SMDCommand> is one of:  {create|destroy|set|delete|get|init|start|shutdown}, and:
**    - The "init", "start", and "shutdown" commands take no parameters;
**    - The "create" and "destroy" commands require a "module" parameter;
**    - The "set" command requires "key" and "value", the "delete" command only requires "key";
**    - The "get" command can take "module", "key" and "node" parameters, which if specified as
**       empty, e.g., "module=;key=", will perform a wildcard metadata item retrieval.
**
**   Open Issues:
**   ------------
**
**   The SMD API currently provides no mechanism for notifying the caller whether (or when)
**   the request has succeeded (or failed.)  The challenge is that in general the asynchronous
**   event may be triggered on a remote node, e.g., the Paxos principal.  Support for an optional
**   callback for this purpose (per-module or per-API call) may be added in the future.
**
*/


/* Define constants. */


/* Maximum length for System Metadata persistence files. */
#define MAX_PATH_LEN  (1024)

/* Time in milliseconds to wait for an incoming message. */
#define AS_SMD_WAIT_INTERVAL_MS  (1000)

/* Time in milliseconds for System Metadata proxy transactions to the Paxos principal. */
#define AS_SMD_TRANSACT_TIMEOUT_MS  (1000)


/* Declare Private Types */


/*
 *  Type for System Metadata command option flags.
 */
typedef enum as_smd_cmd_opt_e {
	AS_SMD_CMD_OPT_NONE           = 0x00,
	AS_SMD_CMD_OPT_DUMP_SMD       = 0x01,
	AS_SMD_CMD_OPT_VERBOSE        = 0x02,
	AS_SMD_CMD_OPT_PAXOS_CHANGED  = 0x04
} as_smd_cmd_opt_t;

/*
 *  Types of API commands sent to the System Metadata module.
 */
typedef enum as_smd_cmd_type_e {
	AS_SMD_CMD_INIT,              // System Metadata API initialization
	AS_SMD_CMD_START,             // System Metadata start receiving Paxos changes
	AS_SMD_CMD_CREATE_MODULE,     // Metadata container creation
	AS_SMD_CMD_DESTROY_MODULE,    // Metadata container destruction
	AS_SMD_CMD_SET_METADATA,      // Add new, or modify existing, metadata item
	AS_SMD_CMD_DELETE_METADATA,   // Existing metadata item deletion
	AS_SMD_CMD_GET_METADATA,      // Get single metadata item
	AS_SMD_CMD_INTERNAL,          // System Metadata system internal command
	AS_SMD_CMD_SHUTDOWN           // System Metadata shut down
} as_smd_cmd_type_t;

/*
 *  Name of the given System Metadata API command type.
 */
#define AS_SMD_CMD_TYPE_NAME(cmd)  (AS_SMD_CMD_INIT == cmd ? "INIT" : \
									(AS_SMD_CMD_START == cmd ? "START" : \
									 (AS_SMD_CMD_CREATE_MODULE == cmd ? "CREATE" : \
									  (AS_SMD_CMD_DESTROY_MODULE == cmd ? "DESTROY" : \
									   (AS_SMD_CMD_SET_METADATA == cmd ? "SET" : \
										(AS_SMD_CMD_DELETE_METADATA == cmd ? "DELETE" : \
										 (AS_SMD_CMD_GET_METADATA == cmd ? "GET" : \
										  (AS_SMD_CMD_INTERNAL == cmd ? "INTERNAL" : \
										   (AS_SMD_CMD_SHUTDOWN == cmd ? "SHUTDOWN" : "<UNKNOWN>")))))))))

/*
 *  Type for System Metadata event messages sent via the API.
 */
typedef struct as_smd_cmd_s {
	as_smd_cmd_type_t type;            // System Metadata command type
	uint32_t options;                  // Bit vector of event options of type "as_smd_cmd_opt_t"
	as_smd_item_t *item;               // Metadata item associated with this event (only relevant fields are set)
	void *a, *b, *c, *d, *e, *f;       // Generic storage for command parameters.
} as_smd_cmd_t;

/*
 *  Types of operation messages handled by the System Metadata module, received as msg events.
 */
typedef enum as_smd_msg_op_e {
	AS_SMD_MSG_OP_SET_ITEM,                 // Add a new, or modify an existing, metadata item
	AS_SMD_MSG_OP_DELETE_ITEM,              // Delete an existing metadata item (must already exist)
	AS_SMD_MSG_OP_MY_CURRENT_METADATA,      // Current metadata sent from a node to the principal
	AS_SMD_MSG_OP_ACCEPT_THIS_METADATA,     // New blessed metadata sent from the principal to a node
	AS_SMD_MSG_OP_NODES_CURRENT_METADATA    // Another node's current metadata sent from the principal to a node
} as_smd_msg_op_t;

/*
 *  Name of the given System Metadata message operation.
 */
#define AS_SMD_MSG_OP_NAME(op)  (AS_SMD_MSG_OP_SET_ITEM == op ? "SET_ITEM" : \
								 (AS_SMD_MSG_OP_DELETE_ITEM == op ? "DELETE_ITEM" : \
								  (AS_SMD_MSG_OP_MY_CURRENT_METADATA == op ? "MY_CURRENT_METADATA" : \
								   (AS_SMD_MSG_OP_ACCEPT_THIS_METADATA == op ? "ACCEPT_THIS_METADATA" : \
									(AS_SMD_MSG_OP_NODES_CURRENT_METADATA == op ? "NODES_CURRENT_METADATA" : "<UNKNOWN>")))))

/*
 *  Name of the given System Metadata action.
 */
#define AS_SMD_ACTION_NAME(action)  (AS_SMD_ACTION_SET == action ? "SET" : \
									 (AS_SMD_ACTION_DELETE == action ? "DELETE" : "<UNKNOWN>"))


/* Define API Command / Message Type / Callback Action Correspondence Macros. */


/*
 *  Message operation corresponding to the given API command type.
 *   (Default to SET_ITEM for the unknown case.)
 */
#define CMD_TYPE2MSG_OP(cmd)  (AS_SMD_CMD_SET_METADATA == cmd ? AS_SMD_MSG_OP_SET_ITEM : \
							   (AS_SMD_CMD_DELETE_METADATA == cmd ? AS_SMD_MSG_OP_DELETE_ITEM : AS_SMD_MSG_OP_SET_ITEM))

/*
 *  API action corresponding to the given message operation.
 *   (Default to SET for the unknown case.)
 */
#define MSG_OP2ACTION(op)  (AS_SMD_MSG_OP_SET_ITEM == op ? AS_SMD_ACTION_SET : \
							(AS_SMD_MSG_OP_DELETE_ITEM == op ? AS_SMD_ACTION_DELETE : AS_SMD_ACTION_SET))

/*
 *  Type for System Metadata messages transmitted via the fabric.
 */
typedef struct as_smd_msg_s {
	as_smd_msg_op_t op;         // System Metadata operation
	uint64_t cluster_key;       // Sending node's cluster key
	cf_node node_id;            // Sending node's ID
	char *module_name;          // Name of the module.
	uint32_t num_items;         // Number of metadata items
	as_smd_item_list_t *items;  // List of metadata items associated with this message (only relevant fields are set)
	uint32_t options;           // Message options (originator)
} as_smd_msg_t;

/*
 *  Types of events sent to and processed by the System Metadata thread.
 */
typedef enum as_smd_event_type_e {
	AS_SMD_CMD,                 // SMD API command
	AS_SMD_MSG,                 // SMD fabric message
} as_smd_event_type_t;

/*
 *  Type for an event object handled by the System Metadata system.
 *     An event can either be an API command or a message transmitted via the fabric.
 */
typedef struct as_smd_event_s {
	as_smd_event_type_t type;   // Selector determining event type (command or message)
	union {
		as_smd_cmd_t cmd;       // SMD command event sent via the SMD API
		as_smd_msg_t msg;       // SMD message event sent via fabric
	} u;
} as_smd_event_t;

/*
 *  Type for the key for items in the external metadata hash table: node_id, key_len, key (flexible array member, sized by key_len.)
 */
typedef struct as_smd_external_item_key_s {
	cf_node node_id;            // ID of the source cluster node.
	size_t key_len;             // Length of the key string.
	char key[];                 // Flexible array member for the null-terminated key string.
} as_smd_external_item_key_t;

/*
 *  Define the template for System Metadata messages.
 *
 *  System Metadata message structure:
 *     0). Transaction ID - UINT64 (Required for Fabric Transact.)
 *     1). System Metadata Protocol Version Identifier - (uint32_t <==> UINT32)  [Only V2 for now.]
 *     2). Cluster Key - (uint64_t <==> UINT64)
 *     3). Operation - (uint32_t <==> UINT32)
 *     4). Number of items - (uint32_t <==> UINT32)
 *     5). Action[] - Array of (uint32_t <==> UINT32)
 *     6). Module[] - Array of (char * <==> STR)
 *     7). Key[] - Array of (char * <==> STR)
 *     8). Value[] - Array of (char * <==> STR)
 *     9). Generation[] - Array of (uint32_t <==> UINT32)
 *     10). Timestamp[] - Array of (uint64_t <==> UINT64)
 *     11). Module Name - (char * <==> STR)
 *     12). Options - (uint32_t <==> UINT32)
 */
static const msg_template as_smd_msg_template[] = {
#define AS_SMD_MSG_TRID 0
	{ AS_SMD_MSG_TRID, M_FT_UINT64 },              // Transaction ID for Fabric Transact
#define AS_SMD_MSG_V2_IDENTIFIER  0x123B
#define AS_SMD_MSG_ID 1
	{ AS_SMD_MSG_ID, M_FT_UINT32 },                // Version of the System Metadata protocol
#define AS_SMD_MSG_CLUSTER_KEY 2
	{ AS_SMD_MSG_CLUSTER_KEY, M_FT_UINT64 },       // Cluster key corresponding to msg contents
#define AS_SMD_MSG_OP 3
	{ AS_SMD_MSG_OP, M_FT_UINT32 },                // Metadata operation
#define AS_SMD_MSG_NUM_ITEMS 4
	{ AS_SMD_MSG_NUM_ITEMS, M_FT_UINT32 },         // Number of metadata items
#define AS_SMD_MSG_ACTION 5
	{ AS_SMD_MSG_ACTION, M_FT_ARRAY_UINT32 },      // Metadata action array
#define AS_SMD_MSG_MODULE 6
	{ AS_SMD_MSG_MODULE, M_FT_ARRAY_STR },         // Metadata module array
#define AS_SMD_MSG_KEY 7
	{ AS_SMD_MSG_KEY, M_FT_ARRAY_STR },            // Metadata key array
#define AS_SMD_MSG_VALUE 8
	{ AS_SMD_MSG_VALUE, M_FT_ARRAY_STR },          // Metadata value array
#define AS_SMD_MSG_GENERATION 9
	{ AS_SMD_MSG_GENERATION, M_FT_ARRAY_UINT32 },  // Metadata generation array
#define AS_SMD_MSG_TIMESTAMP 10
	{ AS_SMD_MSG_TIMESTAMP, M_FT_ARRAY_UINT64 },   // Metadata timestamp array
#define AS_SMD_MSG_MODULE_NAME 11
	{ AS_SMD_MSG_MODULE_NAME, M_FT_STR },          // Name of module the message is from or else NULL if from all.
#define AS_SMD_MSG_OPTIONS 12
	{ AS_SMD_MSG_OPTIONS, M_FT_UINT32 }            // Option flags specifying the originator of the message (i.e., MERGE/API)
};

/*
 *  State of operation of the System Metadata module.
 */
typedef enum as_smd_state_e {
	AS_SMD_STATE_IDLE,                     // Not initialized yet
	AS_SMD_STATE_INITIALIZED,              // Ready to receive API calls
	AS_SMD_STATE_RUNNING,                  // Normal operation:  Receiving Paxos state changes
	AS_SMD_STATE_EXITING                   // Shutting down
} as_smd_state_t;

/*
 *  Name of the given System Metadata state.
 */
#define AS_SMD_STATE_NAME(state)  (AS_SMD_STATE_IDLE == state ? "IDLE" : \
								   (AS_SMD_STATE_INITIALIZED == state ? "INITIALIZED" : \
									(AS_SMD_STATE_RUNNING == state ? "RUNNING" : \
									 (AS_SMD_STATE_EXITING == state ? "EXITING" : "UNKNOWN"))))

/*
 *  Internal representation of the state of the System Metadata module.
 */
struct as_smd_s {

	// System Metadata thread ID.
	pthread_t thr_id;

	// System Metadata thread attributes.
	pthread_attr_t thr_attr;

	// Is the System Metadata module up and running?
	as_smd_state_t state;

	// Hash table mapping module name (char *) ==> module object (as_smd_module_t *).
	rchash *modules;

	// Message queue for receiving System Metadata messages.
	cf_queue *msgq;

	// Scoreboard of what cluster nodes the Paxos principal has received metadata from:  cf_node ==> shash *.
	shash *scoreboard;

};

/*
 *  Type representing a module and holding all metadata for the module.
 */
typedef struct as_smd_module_s {

	// Name of this module.
	char *module;

	// This module's merge metadata callback function (or NULL if none.)
	as_smd_merge_cb merge_cb;

	// User data for the merge metadata callback (or NULL if none.)
	void *merge_udata;

	// This module's accept metadata callback function (or NULL if none.)
	as_smd_accept_cb accept_cb;

	// User data for the accept metadata callback (or NULL if none.)
	void *accept_udata;

	// This module's user_op validation callback (or NULL if none.)
	as_smd_can_accept_cb can_accept_cb;

	// User data for the user_op validation callback (or NULL if none.)
	void *can_accept_udata;

	// Parsed JSON representation of the module's metadata.
	json_t *json;

	// Hash table of metadata registered by this node mapping key (char *) ==> metadata item (as_smd_item_t *).
	rchash *my_metadata;

	// Hash table of metadata received from all external nodes mapping key (as_smd_external_item_key_t *) ==> metadata item (as_smd_item_t *).
	rchash *external_metadata;

} as_smd_module_t;


/* Define macros. */


/*
 *  Free and set to NULL a pointer if non-NULL.
 */
#define CF_FREE_AND_NULLIFY(ptr) \
	if (ptr) {                   \
		cf_free(ptr);            \
		ptr = NULL;              \
	}

/*
 *  Free members of a metadata item if non-NULL.
 */
#define RELEASE_ITEM_MEMBERS(ptr)          \
	CF_FREE_AND_NULLIFY(ptr->module_name); \
	CF_FREE_AND_NULLIFY(ptr->key);         \
	CF_FREE_AND_NULLIFY(ptr->value);


/* Function forward references. */


static int as_smd_module_persist(as_smd_module_t *module_obj);
void *as_smd_thr(void *arg);


/* Internal message passing functions. */


/*
 *  Allocate a System Metadata cmd event object to handle API commands.
 *  (Note:  Using 0 for "node_id" is shorthand for the current node.)
 *
 *  Release using "as_smd_destroy_event()".
 */
static as_smd_event_t *as_smd_create_cmd_event(as_smd_cmd_type_t type, ...)
{
	as_smd_event_t *evt = NULL;
	as_smd_item_t *item = NULL;

	// In Commands:  Internal
	uint32_t options = 0;

	// (Always zero.)
	cf_node node_id = 0;

	// In Commands:  Create / Destroy / Set / Delete / Get
	char *module = NULL;

	// In Commands:  Set / Delete / Get
	char *key = NULL;

	// In Commands:  Set
	char *value = NULL;
	uint32_t generation = 0;
	uint64_t timestamp = 0UL;

	// In Commands:  Create
	as_smd_merge_cb merge_cb = NULL;
	void *merge_udata = NULL;
	as_smd_accept_cb accept_cb = NULL;
	void *accept_udata = NULL;
	as_smd_can_accept_cb can_accept_cb = NULL;
	void *can_accept_udata = NULL;

	// In Commands:  Get
	as_smd_get_cb get_cb = NULL;
	void *get_udata = NULL;

	// Handle variable arguments.
	va_list args;
	va_start(args, type);
	switch (type) {
		case AS_SMD_CMD_INIT:
		case AS_SMD_CMD_START:
		case AS_SMD_CMD_SHUTDOWN:
			// (No additional arguments.)
			break;

		case AS_SMD_CMD_CREATE_MODULE:
			module = va_arg(args, char *);
			merge_cb = va_arg(args, as_smd_merge_cb);
			merge_udata = va_arg(args, void *);
			accept_cb = va_arg(args, as_smd_accept_cb);
			accept_udata = va_arg(args, void *);
			can_accept_cb = va_arg(args, as_smd_can_accept_cb);
			can_accept_udata = va_arg(args, void *);
			break;

		case AS_SMD_CMD_DESTROY_MODULE:
			module = va_arg(args, char *);
			break;

		case AS_SMD_CMD_SET_METADATA:
			module = va_arg(args, char *);
			key = va_arg(args, char *);
			value = va_arg(args, char *);
			generation = va_arg(args, uint32_t);
			timestamp = va_arg(args, uint64_t);
			break;

		case AS_SMD_CMD_DELETE_METADATA:
			module = va_arg(args, char *);
			key = va_arg(args, char *);
			break;

		case AS_SMD_CMD_GET_METADATA:
			module = va_arg(args, char *);
			key = va_arg(args, char *);
			get_cb = va_arg(args, as_smd_get_cb);
			get_udata = va_arg(args, void *);
			break;

		case AS_SMD_CMD_INTERNAL:
			cf_debug(AS_SMD, "At event creation for Paxos state change");
			options = va_arg(args, uint32_t);
			break;
	}
	va_end(args);

	// Allocate an event object and initialize it as a command.
	if (!(evt = (as_smd_event_t *) cf_calloc(1, sizeof(as_smd_event_t)))) {
		cf_crash(AS_SMD, "failed to allocate a System Metadata cmd event");
	}
	evt->type = AS_SMD_CMD;
	as_smd_cmd_t *cmd = &(evt->u.cmd);
	cmd->type = type;
	cmd->options = options;

	// Only events with the module specified will create a cmd containing a metadata item.
	if (module) {
		// Create the metadata item.
		// [NB: Reference-counted for insertion in metadata "rchash" table.]
		if (!(item = (as_smd_item_t *) cf_rc_alloc(sizeof(as_smd_item_t)))) {
			cf_crash(AS_SMD, "failed to allocate a System Metadata metadata item");
		}
		memset(item, 0, sizeof(as_smd_item_t));

		cmd->item = item;

		// Set the originating node ID.
		// (Note:  Using 0 for "node_id" is shorthand for the current node.)
		item->node_id = (!node_id ? g_config.self_node : node_id);

		item->action = MSG_OP2ACTION(CMD_TYPE2MSG_OP(type));

		// Populate the item with duplicated metadata
		// (Note:  The caller is responsible for releasing any dynamically-allocated values passed in.)

		if (module) {
			item->module_name = cf_strdup(module);
		}

		if (key) {
			item->key = cf_strdup(key);
		}

		if (value) {
			size_t value_len = strlen(value) + 1;
			if (!(item->value = (char *) cf_malloc(value_len))) {
				cf_crash(AS_SMD, "failed to allocate a System Metadata cmd event value field of size %zu", value_len);
			}
			strncpy(item->value, value, value_len);
		}

		item->generation = generation;

		item->timestamp = timestamp;
	}

	// Store the policy callback information generically.
	if (AS_SMD_CMD_CREATE_MODULE == type) {
		cmd->a = merge_cb;
		cmd->b = merge_udata;
		cmd->c = accept_cb;
		cmd->d = accept_udata;
		cmd->e = can_accept_cb;
		cmd->f = can_accept_udata;
	} else if (AS_SMD_CMD_GET_METADATA == type) {
		cmd->a = get_cb;
		cmd->b = get_udata;
	}

	return evt;
}

/*
 *  Allocate a System Metadata msg event object to handle an incoming SMD fabric msg.
 *
 *  Release using "as_smd_destroy_event()".
 */
static as_smd_event_t *as_smd_create_msg_event(as_smd_msg_op_t op, cf_node node_id, msg *msg)
{
	as_smd_event_t *evt = NULL;
	int e = 0;
	size_t ignored_len = 0;

	// Allocate an event object and initialize it as a msg.
	if (!(evt = (as_smd_event_t *) cf_calloc(1, sizeof(as_smd_event_t)))) {
		cf_crash(AS_SMD, "failed to allocate a System Metadata msg event");
	}
	evt->type = AS_SMD_MSG;
	as_smd_msg_t *smd_msg = &(evt->u.msg);

	smd_msg->op = op;
	smd_msg->node_id = node_id;

	if ((e = msg_get_uint64(msg, AS_SMD_MSG_CLUSTER_KEY, &(smd_msg->cluster_key)))) {
		cf_warning(AS_SMD, "failed to get cluster key from System Metadata fabric msg (err %d)", e);
		return 0;
	}

	if ((e = msg_get_str(msg, AS_SMD_MSG_MODULE_NAME, &(smd_msg->module_name), 0, MSG_GET_COPY_MALLOC))) {
		cf_debug(AS_SMD, "failed to get module name from System Metadata fabric msg (err %d)", e);
	}

	if ((e = msg_get_uint32(msg, AS_SMD_MSG_NUM_ITEMS, &(smd_msg->num_items)))) {
		cf_warning(AS_SMD, "failed to get number of metadata items from System Metadata fabric msg (err %d)", e);
		return 0;
	}

	cf_debug(AS_SMD, "ascme():  Received msg w/ node_id %016lX ; ck %016lx ; num_items %d", smd_msg->node_id, smd_msg->cluster_key, smd_msg->num_items);

	// Create a msg event with space for the incoming metadata items.
	if (!(smd_msg->items = as_smd_item_list_create(smd_msg->num_items))) {
		cf_warning(AS_SMD, "failed to allocate array of %d metadata items for a msg event", smd_msg->num_items);
		cf_free(evt);
		return 0;
	}
	smd_msg->items->node_id = node_id;

	// Populate the msg event items from the fabric msg.
	for (int i = 0; i < smd_msg->num_items; i++) {
		as_smd_item_t *item = smd_msg->items->item[i];

		item->node_id = node_id;

		e += msg_get_uint32_array(msg, AS_SMD_MSG_ACTION, i, &(item->action));
		e += msg_get_str_array(msg, AS_SMD_MSG_MODULE, i, &(item->module_name), &ignored_len, MSG_GET_COPY_MALLOC);
		e += msg_get_str_array(msg, AS_SMD_MSG_KEY, i, &(item->key), &ignored_len, MSG_GET_COPY_MALLOC);

		// Extract additional msg fields if necessary.
		if (AS_SMD_ACTION_DELETE != item->action) {
			e += msg_get_str_array(msg, AS_SMD_MSG_VALUE, i, &(item->value), &ignored_len, MSG_GET_COPY_MALLOC);
			e += msg_get_uint32_array(msg, AS_SMD_MSG_GENERATION, i, &(item->generation));
			e += msg_get_uint64_array(msg, AS_SMD_MSG_TIMESTAMP, i, &(item->timestamp));
		}

		// Validate incoming item.
		if ((0 > e) || (AS_SMD_ACTION_SET == item->action && !(item->value))) {
			cf_crash(AS_SMD, "failed to unpack incoming fabric System Metadata msg item %d (err %d ; op %d ; val %p)",
					 i, e, smd_msg->op, item->value);
		} else {
			cf_debug(AS_SMD, "Unpacked inbound System Metadata fabric msg for operation %s with item %d: module \"%s\" ; key \"%s\" (rv %d)",
					 AS_SMD_MSG_OP_NAME(smd_msg->op), i, item->module_name, item->key, e);
		}
	}

	return evt;
}


/* Memory release functions for object types passed to the callback functions. */


/*
 *  Release a reference-counted metadata item.
 *  (Note:  This is *not* a public API.)
 */
static void as_smd_item_destroy(as_smd_item_t *item)
{
	if (item) {
		if (!cf_rc_release(item)) {
			RELEASE_ITEM_MEMBERS(item);
			cf_rc_free(item);
		}
	}
}

/*
 *  Allocate an empty list of to contain metadata items.
 *  (Note:  This is *not* a public API.)
 */
static as_smd_item_list_t *as_smd_item_list_alloc(size_t num_items)
{
	as_smd_item_list_t *item_list = NULL;

	if ((item_list = (as_smd_item_list_t *) cf_malloc(sizeof(as_smd_item_list_t) + num_items * sizeof(as_smd_item_t *)))) {
		item_list->node_id = 0;
		item_list->num_items = num_items;
		item_list->module_name = NULL;
		memset(item_list->item, 0, num_items * sizeof(as_smd_item_t *));
	}

	return item_list;
}

/*
 *  Create an empty list of reference-counted metadata items.
 *  (Note:  This is a public API for creating merge callback function arguments.)
 */
as_smd_item_list_t *as_smd_item_list_create(size_t num_items)
{
	as_smd_item_list_t *item_list = NULL;

	if ((item_list = as_smd_item_list_alloc(num_items))) {
		// Use num_items to count the number of successfully allocated items.
		item_list->num_items = 0;
		for (int i = 0; i < num_items; i++) {
			if (!(item_list->item[i] = (as_smd_item_t *) cf_rc_alloc(sizeof(as_smd_item_t)))) {
				as_smd_item_list_destroy(item_list);
				item_list = NULL;
				break;
			}
			memset(item_list->item[i], 0, sizeof(as_smd_item_t));
			item_list->num_items++;
		}
	}

	return item_list;
}

/*
 *  Release a list of reference-counted metadata items.
 *  (Note:  This is a public API for releasing merge callback function arguments.)
 */
void as_smd_item_list_destroy(as_smd_item_list_t *items)
{
	if (items) {
		for (int i = 0; i < items->num_items; i++) {
			as_smd_item_destroy(items->item[i]);
			items->item[i] = NULL;
		}
		cf_free(items);
	}
}

/*
 *  Release a System Metadata event object (either a cmd or a msg.)
 */
static void as_smd_destroy_event(as_smd_event_t *evt)
{
	if (evt) {
		if (AS_SMD_CMD == evt->type) {
			as_smd_cmd_t *cmd = &(evt->u.cmd);

			// Give back the item reference if necessary.
			as_smd_item_destroy(cmd->item);
			cmd->item = NULL;
		} else if (AS_SMD_MSG == evt->type) {
			as_smd_msg_t *msg = &(evt->u.msg);

			// Release the module name.
			if (msg->module_name) {
				cf_free(msg->module_name);
				msg->module_name = NULL;
			}

			// Release the msg item list.
			as_smd_item_list_destroy(msg->items);
			msg->num_items = 0;
			msg->items = NULL;
		} else {
			cf_warning(AS_SMD, "not destroying unknown type of System Metadata event (%d)", evt->type);
			return;
		}

		// Release the event itself.
		cf_free(evt);
	} else {
		cf_warning(AS_SMD, "not freeing NULL System Metadata event");
	}
}

/*
 *  Send an event to the System Metadata thread via the message queue.
 */
static int as_smd_send_event(as_smd_t *smd, as_smd_event_t *evt)
{
	int retval = 0;

	if (!smd) {
		cf_warning(AS_SMD, "System Metadata is not initialized ~~ Not sending event!");
		as_smd_destroy_event(evt);
		return -1;
	}

	if ((retval = cf_queue_push(smd->msgq, &evt))) {
		cf_crash(AS_SMD, "failed to send %s event to the System Metadata thread (retval %d)",
				 (AS_SMD_CMD == evt->type ? AS_SMD_CMD_TYPE_NAME(evt->u.cmd.type) : AS_SMD_MSG_OP_NAME(evt->u.msg.op)), retval);
	}

	return retval;
}


/* System Metadata Module Init / Start / Shutdown API */


/*
 *  Hash the given string using the 32-bit FNV-1a hash algorithm for use with rchash tables.
 */
static uint32_t str_hash_fn(void *value, uint32_t value_len)
{
	uint32_t hash = 2166136261;

	while (value_len--) {
		hash ^= * (uint8_t *) value++;
		hash *= 16777619;
	}

	return hash;
}
/*
 *  Hash the given string using the 32-bit FNV-1a hash algorithm for use with rchash tables.
 */
static uint32_t shash_str_hash_fn(void *value)
{
	int value_len = strlen((char*)value) + 1;
	uint32_t hash = 2166136261;

	while (value_len--) {
		hash ^= * (uint8_t *) value++;
		hash *= 16777619;
	}

	return hash;
}

/*
 *  Free a module object from the modules rchash table.
 */
static void modules_rchash_destructor_fn(void *object)
{
	as_smd_module_t *module_obj = (as_smd_module_t *) object;

	cf_debug(AS_SMD, "mrdf(%p) [module \"%s\"] called!", object, module_obj->module);

	// Ensure that the module's callbacks cannot be called again.
	module_obj->merge_cb = module_obj->merge_udata = NULL;
	module_obj->accept_cb = module_obj->accept_udata = NULL;
	module_obj->can_accept_cb = module_obj->can_accept_udata = NULL;

	// Persist the module's metadata.
	int retval = 0;
	if ((retval = as_smd_module_persist(module_obj))) {
		cf_warning(AS_SMD, "failed to persist System Metadata for module \"%s\" upon destruction (rv %d)", module_obj->module, retval);
	}

	// Release the module's JSON if necessary.
	json_decref(module_obj->json);
	module_obj->json = NULL;

	// Free the module's name.
	CF_FREE_AND_NULLIFY(module_obj->module);

	// Free both of the module's metadata hash tables.

#ifdef DEBUG
	cf_debug(AS_SMD, "Dumping my_metadata:");
	rchash_dump(module_obj->my_metadata);
#endif

	rchash_destroy(module_obj->my_metadata);

#ifdef DEBUG
	cf_debug(AS_SMD, "Dumping external_metadata:");
	rchash_dump(module_obj->external_metadata);
#endif

	rchash_destroy(module_obj->external_metadata);
}

/*
 *  Free a metadata item from the metadata rchash table.
 */
static void metadata_rchash_destructor_fn(void *object)
{
	as_smd_item_t *item = (as_smd_item_t *) object;

	cf_debug(AS_SMD, "mdrdf(%p) [key \"%s\"] called!", object, item->key);

	// Free up the members of the item.
	RELEASE_ITEM_MEMBERS(item);
}

typedef struct as_smd_item_freq_s {
	as_smd_item_t *item;
	uint32_t freq;
} as_smd_item_freq_t;

/*
 *  Handle a cluster state change event notification from Paxos.
 */
static void as_smd_paxos_state_changed_fn(as_paxos_generation gen, as_paxos_change *change, cf_node succession[], void *udata)
{
	as_smd_t *smd = (as_smd_t *) udata;

	cf_debug(AS_SMD, "aspscf():  Received Paxos state changed event!");

	// Send an INTERNAL + Paxos Changed command to the System Metadata thread.
	as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_INTERNAL, AS_SMD_CMD_OPT_PAXOS_CHANGED));
}

/*
 *  Create and initialize a System Metadata module. (Local method for now.)
 */
static as_smd_t *as_smd_create(void)
{
	as_smd_t *smd = (as_smd_t *) cf_calloc(1, sizeof(as_smd_t));

	if (! smd) {
		cf_crash(AS_SMD, "failed to allocate the System Metadata object");
	}

	// Go to the not yet initialized state.
	smd->state = AS_SMD_STATE_IDLE;

	// Create the System Metadata modules hash table.
	if (RCHASH_OK != rchash_create(&(smd->modules), str_hash_fn, modules_rchash_destructor_fn, 0, 127, RCHASH_CR_MT_BIGLOCK)) {
		cf_crash(AS_SMD, "failed to create the System Metadata modules hash table");
	}

	// Create the scoreboard hash table.
	if (SHASH_OK != shash_create(&(smd->scoreboard), ptr_hash_fn, sizeof(cf_node), sizeof(shash *), 127, SHASH_CR_MT_BIGLOCK)) {
		cf_crash(AS_SMD, "failed to create the System Metadata scoreboard hash table");
	}

	// Create the System Metadata message queue.
	if (!(smd->msgq = cf_queue_create(sizeof(as_smd_event_t *), true))) {
		cf_crash(AS_SMD, "failed to create the System Metadata message queue");
	}

	// Create the System Metadata thread.

	if (pthread_attr_init(&(smd->thr_attr))) {
		cf_crash(AS_SMD, "failed to initialize the System Metadata thread attributes");
	}

	if (pthread_create(&(smd->thr_id), &(smd->thr_attr), as_smd_thr, smd)) {
		cf_crash(AS_SMD, "failed to create the System Metadata thread");
	}

	// Send an INIT message to the System Metadata thread.
	int retval = 0;
	if ((retval = as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_INIT)))) {
		cf_crash(AS_SMD, "failed to send INIT message to System Metadata thread");
	}

	return smd;
}

/*
 *  Initialize the single global System Metadata module.
 */
as_smd_t *as_smd_init(void)
{
	if (! g_config.smd) {
		g_config.smd = as_smd_create();
	} else {
		cf_warning(AS_SMD, "System Metadata is already initialized");
	}

	return g_config.smd;
}

/*
 *  Convert an incoming fabric message into the corresponding msg event and post it to the System Metadata message queue.
 */
static int as_smd_msgq_push(cf_node node_id, msg *msg, void *udata)
{
	as_smd_t *smd = (as_smd_t *) udata;
	as_smd_event_t *evt = NULL;

	cf_debug(AS_SMD, "asmp():  Receiving a System Metadata message from node %016lX", node_id);

	// Make sure System Metadata is running before processing msg.
	if (smd && smd->state != AS_SMD_STATE_RUNNING) {
		cf_warning(AS_SMD, "System Metadata not initialized ~~ Ignoring incoming fabric msg!");
		return -1;
	}

	// Verify the System Metadata fabric protocol version.
	uint32_t version;
	int e = msg_get_uint32(msg, AS_SMD_MSG_ID, &version);
	if (0 > e) {
		cf_warning(AS_SMD, "failed to get protocol version from System Metadata fabric msg");
		return -1;
	} else if (AS_SMD_MSG_V2_IDENTIFIER != version) {
		cf_warning(AS_SMD, "received System Metadata fabric msg for unknown protocol version (read: %d ; expected: %d) ~~ Ignoring message!",
				   version, AS_SMD_MSG_V2_IDENTIFIER);
		return -1;
	}

	// Extract the operation from the incoming fabric msg.
	uint32_t op = 0;
	e = msg_get_uint32(msg, AS_SMD_MSG_OP, &op);

	cf_debug(AS_SMD, "Operation received %s", AS_SMD_MSG_OP_NAME(op));
	// Create a System Metadata msg event object and populate it from the fabric msg.
	if (!(evt = as_smd_create_msg_event(op, node_id, msg))) {
		cf_crash(AS_SMD, "failed to create a System Metadata msg event");
	}

	as_smd_msg_t *smd_msg = &(evt->u.msg);
	if (smd_msg->num_items) {
		as_smd_item_t *item = smd_msg->items->item[0]; // (Only log the fist item.)
		cf_debug(AS_SMD, "asmp(): op: %s num_items %d ; node: %016lX ; item 0: module: \"%s\" ; key: \"%s\" ; value: \"%s\" ; generation: %u ; timestamp: %lu",
				 AS_SMD_MSG_OP_NAME(smd_msg->op), smd_msg->num_items, item->node_id, item->module_name, item->key, item->value, item->generation, item->timestamp);
	} else {
		cf_debug(AS_SMD, "asmp():  op: %s [Zero metadata items]", AS_SMD_MSG_OP_NAME(smd_msg->op));
	}

	if (AS_SMD_MSG_OP_ACCEPT_THIS_METADATA == op) {
		if ((e = msg_get_str(msg, AS_SMD_MSG_MODULE_NAME, &(smd_msg->items->module_name), 0, MSG_GET_COPY_MALLOC))) {
			cf_warning(AS_SMD, "could not get module name from the fabric msg");
			smd_msg->items->module_name = NULL;
		}

		if ((e = msg_get_uint32(msg, AS_SMD_MSG_OPTIONS, &(smd_msg->options)))) {
			cf_warning(AS_SMD, "could not get info flags from the fabric msg");
			smd_msg->options = 0;
		} else {
			cf_debug(AS_SMD, "SMD msg options flag set to 0x%08x from the fabric msg", smd_msg->options);
		}
	}

	// Send the msg event to the System Metadata thread.
	return as_smd_send_event(smd, evt);
}

/*
 *  Receiver function for System Metadata fabric transactions.
 */
static int as_smd_transact_recv_fn(cf_node node_id, msg *msg, void *transact_data, void *udata)
{
	as_smd_t *smd = (as_smd_t *) udata;
	int retval = 0;

	cf_debug(AS_SMD, "astrf():  node %016lX (%s) received SMD transaction from node %016lX (%s)",
			 g_config.self_node, (as_paxos_succession_getprincipal() == g_config.self_node ? "Paxos principal" : "regular node"),
			 node_id, (as_paxos_succession_getprincipal() == node_id ? "Paxos principal" : "regular node"));

	// Send the received msg to the System Metadata thread.
	if ((retval = as_smd_msgq_push(node_id, msg, smd))) {
		cf_warning(AS_SMD, "failed to push received transact msg (retval %d)", retval);
	}

	// Complete the transaction by replying to the received msg.
	as_fabric_transact_reply(msg, transact_data);

	return retval;
}

/*
 *  Start the System Metadata module to begin receiving Paxos state change events.
 */
int as_smd_start(as_smd_t *smd)
{
	// Register System Metadata fabric transact message type.
	if (as_fabric_transact_register(M_TYPE_SMD, as_smd_msg_template, sizeof(as_smd_msg_template), as_smd_transact_recv_fn, smd)) {
		cf_crash(AS_SMD, "Failed to register System Metadata fabric transact msg type!");
	}

	// Register to receive Paxos state changed events.
	if (as_paxos_register_change_callback(as_smd_paxos_state_changed_fn, smd)) {
		cf_crash(AS_SMD, "Failed to register System Metadata Paxos state changed callback!");
	}

	// Send a START message to the System Metadata thread.
	int retval = 0;
	if ((retval = as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_START)))) {
		cf_crash(AS_SMD, "failed to send START message to System Metadata thread");
	}

	return retval;
}

/*
 *  Terminate the System Metadata module.
 */
int as_smd_shutdown(as_smd_t *smd)
{
	// Send a SHUTDOWN message to the System Metadata thread.
	return as_smd_send_event(smd, as_smd_create_cmd_event(AS_SMD_CMD_SHUTDOWN));
}


/*
 *  Public System Metadata Manipulation API Functions:
 *   These functions are executed in the context of a module using System Metadata.
 */


/*
 *  Create a container for the named module's metadata and register the policy callback functions.
 *  (Pass a NULL callback function pointer to select the default policy.)
 */
int as_smd_create_module(char *module, as_smd_merge_cb merge_cb, void *merge_udata, as_smd_accept_cb accept_cb, void *accept_udata,
						 as_smd_can_accept_cb can_accept_cb, void *can_accept_udata)
{
	// Send a CREATE command to the System Metadata thread.
	return as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_CREATE_MODULE, module, merge_cb, merge_udata, accept_cb, accept_udata,
							 can_accept_cb, can_accept_udata));
}

/*
 *  Destroy the container for the named module's metadata, releasing all of its metadata.
 */
int as_smd_destroy_module(char *module)
{
	// Send a DESTROY command to the System Metadata thread.
	return as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_DESTROY_MODULE, module));
}

/*
 *  Add a new, or modify an existing, metadata item in an existing module.
 */
int as_smd_set_metadata(char *module, char *key, char *value)
{
	// Send an SET command to the System Metadata thread.
	return as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_SET_METADATA, module, key, value, 0, 0UL));
}

/*
 *  Add a new, or modify an existing, metadata item (with generation and timestamp) in an existing module.
 *  (Note:  This is an internal-only function, not available via the public SMD API.)
 */
int as_smd_set_metadata_gen_ts(char *module, char *key, char *value, uint32_t generation, uint64_t timestamp)
{
	// Send an SET command to the System Metadata thread.
	return as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_SET_METADATA, module, key, value, generation, timestamp));
}

/*
 *  Delete an existing metadata item from an existing module.
 */
int as_smd_delete_metadata(char *module, char *key)
{
	// Send a DELETE command to the System Metadata thread.
	return as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_DELETE_METADATA, module, key));
}

/*
 *  Retrieve metadata item(s.) (Pass NULL for module and/or key for "all".)
 */
int as_smd_get_metadata(char *module, char *key, as_smd_get_cb cb, void *udata)
{
	// Send a GET command to the System Metadata thread.
	return as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_GET_METADATA, module, key, cb, udata));
}


/*
 *  Info Command Functions:
 *   These functions are executed in the context of the Info system.
 */


/*
 *  Reduce function to print a single metadata item.
 */
static int as_smd_metadata_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;

	cf_info(AS_SMD, "%016lX\t\"%s\"\t\"%s\"\t\"%s\"\t%u\t\t%lu", item->node_id, item->module_name, item->key, item->value, item->generation, item->timestamp);

	return 0;
}

/*
 *  Reduce function to print info. about a single System Metadata module.
 */
static int as_smd_dump_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	char *module = (char *) key;
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	int *module_num = (int *) udata;
	int num_items = 0;

	cf_info(AS_SMD, "Module %d: \"%s\" [\"%s\"]: ", *module_num++, module, module_obj->module);
	cf_info(AS_SMD, "merge cb: %p", module_obj->merge_cb);
	cf_info(AS_SMD, "merge udata: %p", module_obj->merge_udata);
	cf_info(AS_SMD, "accept cb: %p", module_obj->accept_cb);
	cf_info(AS_SMD, "accept udata: %p", module_obj->accept_udata);
	cf_info(AS_SMD, "can accept cb: %p", module_obj->can_accept_cb);
	cf_info(AS_SMD, "can accept udata: %p", module_obj->can_accept_udata);

	cf_info(AS_SMD, "My Metadata:");
	cf_info(AS_SMD, "number of metadata items: %d", num_items = rchash_get_size(module_obj->my_metadata));
	if (num_items) {
		cf_info(AS_SMD, "Node ID\t\tModule\tKey\tValue\t\tGeneration\tTimestamp");
		rchash_reduce(module_obj->my_metadata, as_smd_metadata_reduce_fn, NULL);
	}

	cf_info(AS_SMD, "External Metadata:");
	cf_info(AS_SMD, "number of metadata items: %d", num_items = rchash_get_size(module_obj->external_metadata));
	if (num_items) {
		cf_info(AS_SMD, "Node ID\t\tModule\tKey\tValue\t\tGeneration\tTimestamp");
		rchash_reduce(module_obj->external_metadata, as_smd_metadata_reduce_fn, NULL);
	}

	return 0;
}

/*
 *  Print info. about the System Metadata state to the log.
 *  (Verbose event option prints detailed info. about the metadata values.)
 */
void as_smd_dump_metadata(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	// Print info. about the System Metadata system.
	cf_info(AS_SMD, "System Metadata Status:");
	cf_info(AS_SMD, "-----------------------");
	cf_info(AS_SMD, "thr_id: %p", smd->thr_id);
	cf_info(AS_SMD, "thr_attr: %p", smd->thr_attr);
	cf_info(AS_SMD, "state: %s", AS_SMD_STATE_NAME(smd->state));
	cf_info(AS_SMD, "number of modules: %d", rchash_get_size(smd->modules));
	cf_info(AS_SMD, "number of pending messages in queue: %d", cf_queue_sz(smd->msgq));

	// If verbose, dump info. about the metadata itself.
	if (cmd->options & AS_SMD_CMD_OPT_VERBOSE) {
		int module_num = 0;
		rchash_reduce(smd->modules, as_smd_dump_reduce_fn, &module_num);
	}
}

/*
 *  Print info. about the System Metadata state to the log.
 *  (Verbose true prints detailed info. about the metadata values.)
 */
void as_smd_dump(bool verbose)
{
	// Send an INTERNAL + DUMP_SMD + verbosity command to the System Metadata thread.
	as_smd_send_event(g_config.smd, as_smd_create_cmd_event(AS_SMD_CMD_INTERNAL,
					  (AS_SMD_CMD_OPT_DUMP_SMD | (verbose ? AS_SMD_CMD_OPT_VERBOSE : 0))));
}

/*
 *  Callback used to receive System Metadata items requested via the Info SMD "get" command.
 */
static int as_smd_info_get_fn(char *module, as_smd_item_list_t *items, void *udata)
{
	for (int i = 0; i < items->num_items; i++) {
		as_smd_item_t *item = items->item[i];
		cf_info(AS_SMD, "SMD Info get metadata item[%d]:  module \"%s\" ; key \"%s\" ; value \"%s\" ; generation %u ; timestamp %lu",
				i, item->module_name, item->key, item->value, item->generation, item->timestamp);
	}

	return 0;
}

/*
 *  Manipulate the System Metadata and log the result.
 */
void as_smd_info_cmd(char *cmd, cf_node node_id, char *module, char *key, char *value)
{
	int retval = 0;

	// Invoke the appropriate System Metadata API function.

	if (!strcmp(cmd, "create")) {
		if ((retval = as_smd_create_module(module, NULL, NULL, NULL, NULL, NULL, NULL))) {
			cf_warning(AS_SMD, "System Metadata create module \"%s\" failed (retval %d)", module, retval);
		}
	} else if (!strcmp(cmd, "destroy")) {
		if ((retval = as_smd_destroy_module(module))) {
			cf_warning(AS_SMD, "System Metadata destroy module \"%s\" failed (retval %d)", module, retval);
		}
	} else if (!strcmp(cmd, "set")) {
		if (((retval = as_smd_set_metadata(module, key, value)))) {
			cf_warning(AS_SMD, "System Metadata set item: module: \"%s\" key: \"%s\" value: \"%s\" failed (retval %d)", module, key, value, retval);
		}
	} else if (!strcmp(cmd, "delete")) {
		if (((retval = as_smd_delete_metadata(module, key)))) {
			cf_warning(AS_SMD, "System Metadata delete item: module: \"%s\" key: \"%s\" failed (retval %d)", module, key, retval);
		}
	} else if (!strcmp(cmd, "get")) {
		if ((retval = as_smd_get_metadata(module, key, as_smd_info_get_fn, NULL))) {
			cf_warning(AS_SMD, "System Metadata get node: %016lX module: \"%s\" key: \"%s\" failed (retval %d)", node_id, module, key, retval);
		}
	} else if (!strcmp(cmd, "init")) {
		as_smd_init();
	} else if (!strcmp(cmd, "start")) {
		if (g_config.smd) {
			if ((retval = as_smd_start(g_config.smd))) {
				cf_warning(AS_SMD, "System Metadata start up failed (retval %d)", retval);
			}
		} else {
			cf_warning(AS_SMD, "System Metadata is not initialized");
		}
	} else if (!strcmp(cmd, "shutdown")) {
		if (g_config.smd) {
			as_smd_shutdown(g_config.smd);
		} else {
			cf_warning(AS_SMD, "System Metadata is not initialized");
		}
	} else {
		cf_warning(AS_SMD, "unknown System Metadata command: \"%s\"", cmd);
	}
}


/*
 *  System Metadata Internals:
 *   These functions are executed in the context of the System Metadata thread,
 *   except for the fabric callbacks.
 */


/* Metadata persistence functions. */


/*
 *  Read in metadata for the given module from the standard location.
 *  Return:  0 if successful, -1 otherwise.
 */
static int as_smd_read(char *module, json_t **module_smd)
{
	int retval = 0;
	json_t *root = NULL;

	char smd_path[MAX_PATH_LEN];
	size_t load_flags = JSON_REJECT_DUPLICATES;
	json_error_t json_error;

	snprintf(smd_path, MAX_PATH_LEN, "%s/smd/%s.smd", g_config.work_directory, module);

	// Check if the persisted metadata file exists before attempting to read it.
	struct stat buf;
	if (!stat(smd_path, &buf)) {
		if (!(root = json_load_file(smd_path, load_flags, &json_error))) {
			cf_warning(AS_SMD, "failed to load System Metadata for module \"%s\" from file \"%s\" with JSON error: %s ; source: %s ; line: %d ; column: %d ; position: %d",
					   module, smd_path, json_error.text, json_error.source, json_error.line, json_error.column, json_error.position);
			retval = -1;
		}
	} else {
		cf_debug(AS_SMD, "failed to read persisted System Metadata file \"%s\" for module \"%s\": %s (%d)", smd_path, module, cf_strerror(errno), errno);
	}

	if (module_smd) {
		*module_smd = root;
	}

	return retval;
}

/*
 *  Write out metadata for the given module to the the standard location.
 *  Return:  0 if successful, -1 otherwise.
 *
 *  Note:  Any pre-existing file will be saved prior to write for
 *          manual recovery in case of system failure.
 */
static int as_smd_write(char *module, json_t *module_smd)
{
	int retval = 0;

	char smd_path[MAX_PATH_LEN];
	size_t dump_flags = JSON_INDENT(3) | JSON_ENSURE_ASCII | JSON_PRESERVE_ORDER;

	snprintf(smd_path, MAX_PATH_LEN, "%s/smd/%s.smd", g_config.work_directory, module);

	// If a file with this name already exists, rename the old version first.
	struct stat buf;
	if (!stat(smd_path, &buf)) {
		char smd_save_path[MAX_PATH_LEN];
		snprintf(smd_save_path, MAX_PATH_LEN, "%s.save", smd_path);
		if (0 > rename(smd_path, smd_save_path)) {
			cf_warning(AS_SMD, "error on renaming existing metadata file \"%s\": %s (%d)", smd_save_path, cf_strerror(errno), errno);
		}
	} else {
		cf_debug(AS_SMD, "failed to rename old persisted System Metadata file \"%s\" for module \"%s\": %s (%d)", smd_path, module, cf_strerror(errno), errno);
	}

	if (0 > json_dump_file(module_smd, smd_path, dump_flags)) {
		cf_warning(AS_SMD, "failed to dump System Metadata for module \"%s\" to file \"%s\": %s (%d)", module, smd_path, cf_strerror(errno), errno);
		retval = -1;
	}

	return retval;
}

/*
 *  Load persisted System Metadata for the given module:
 *    Read the module's JSON file (if it exists) and add each metadata found therein.
 *    Return:   The number of metadata items restored (which may be 0) if reading
 *               the metadata file was successful, -1 otherwise.
 */
static int as_smd_module_restore(as_smd_module_t *module_obj)
{
	int retval = 0;

	// Load the module's metadata (if persisted.)
	if ((retval = as_smd_read(module_obj->module, &(module_obj->json)))) {
		cf_warning(AS_SMD, "failed to read persisted System Metadata for module \"%s\"", module_obj->module);
		return -1;
	}

	size_t num_items = json_array_size(module_obj->json);
	for (int i = 0; i < num_items; i++) {
		json_t *json_item = json_array_get(module_obj->json, i);

		if (!json_is_object(json_item)) {
			// Warn and skip the bad item.
			cf_warning(AS_SMD, "non-JSON object %d of type %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, json_typeof(json_item), module_obj->module);
			continue;
		}

		size_t num_fields = json_object_size(json_item);
		if (5 != num_fields) {
			// Warn if the item doesn't have the right number of fields.
			cf_warning(AS_SMD, "wrong number of fields %d (expected 5) for object %d in persisted System Metadata for module \"%s\"", num_fields, i, module_obj->module);
		}

		char *module = (char *) json_string_value(json_object_get(json_item, "module"));
		if (!module) {
			cf_warning(AS_SMD, "missing \"module\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, module_obj->module);
			continue;
		} else if (strcmp(module_obj->module, module)) {
			cf_warning(AS_SMD, "incorrect module \"%s\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", module, i, module_obj->module);
			continue;
		}

		char *key = (char *) json_string_value(json_object_get(json_item, "key"));
		if (!key) {
			cf_warning(AS_SMD, "missing \"key\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, module_obj->module);
			continue;
		}

		char *value = (char *) json_string_value(json_object_get(json_item, "value"));
		if (!value) {
			cf_warning(AS_SMD, "missing \"value\" for object %d in persisted System Metadata for module \"%s\" ~~ Skipping!", i, module_obj->module);
			continue;
		}

		// [Note:  Should really use uint32_t, but Jansson integers are longs.]
		uint64_t generation = 1;
		json_t *generation_obj = json_object_get(json_item, "generation");
		if (!generation_obj) {
			cf_warning(AS_SMD, "missing \"generation\" for object %d in persisted System Metadata for module \"%s\" ~~ Using 1!", i, module_obj->module);
		} else {
			if (0 >= (generation = json_integer_value(generation_obj))) {
				cf_warning(AS_SMD, "bad \"generation\" for object %d in persisted System Metadata for module \"%s\" ~~ Using 1!", i, module_obj->module);
				generation = 1;
			}
		}

		uint64_t timestamp = cf_getms();
		json_t *timestamp_obj = json_object_get(json_item, "timestamp");
		if (!timestamp_obj) {
			cf_warning(AS_SMD, "missing \"timestamp\" for object %d in persisted System Metadata for module \"%s\" ~~ Using now!", i, module_obj->module);
		} else {
			if (0 >= (timestamp = json_integer_value(timestamp_obj))) {
				cf_warning(AS_SMD, "bad \"timestamp\" for object %d in persisted System Metadata for module \"%s\" ~~ Using now!", i, module_obj->module);
				timestamp = cf_getms();
			}
		}

		// Send the item metadata add command.
		as_smd_set_metadata_gen_ts(module, key, value, generation, timestamp);

		// Another metadata item was successfully restored.
		retval++;
	}

	// Release the module's JSON if necessary.
	json_decref(module_obj->json);
	module_obj->json = NULL;

	return retval;
}

/*
 *  Serialize a single metadata item into a JSON object and add it to the array passed in via "udata".
 */
static int as_smd_serialize_into_json_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	json_t *array = (json_t *) udata;
	json_t *metadata_obj = NULL;

	// Create an empty JSON object to hold the
	if (!(metadata_obj = json_object())) {
		cf_warning(AS_SMD, "failed to create JSON object to serialize metadata item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
		return 0;
	}

	// Add each of the item's properties to the JSON object.
	int e = 0;
	e += json_object_set_new(metadata_obj, "module", json_string(item->module_name));
	e += json_object_set_new(metadata_obj, "key", json_string(item->key));
	e += json_object_set_new(metadata_obj, "value", json_string(item->value));
	e += json_object_set_new(metadata_obj, "generation", json_integer(item->generation));
	e += json_object_set_new(metadata_obj, "timestamp", json_integer(item->timestamp));

	if (e) {
		cf_warning(AS_SMD, "failed to serialize fields of metadata item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
	} else {
		if (json_array_append_new(array, metadata_obj)) {
			cf_warning(AS_SMD, "failed to add to array metadata item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
		}
	}

	return 0;
}

/*
 *  Store persistently System Metadata for the given module:
 *    Convert each of the module's metadata items into a JSON object and write an array of the results to the module's JSON file.
 */
static int as_smd_module_persist(as_smd_module_t *module_obj)
{
	int retval = 0;

	if (module_obj->json) {
		cf_warning(AS_SMD, "module \"%s\" JSON is unexpectedly non-NULL (rc %d) ~~ Nulling!", module_obj->module, module_obj->json->refcount);
		json_decref(module_obj->json);
		module_obj->json = NULL;
	}

	// Create an empty JSON array.
	if (!(module_obj->json = json_array())) {
		cf_warning(AS_SMD, "failed to create JSON array for persisting module \"%s\"", module_obj->module);
		return -1;
	}

	// Walk the module's metadata hash table and create a JSON array of objects, one for each item.
	rchash_reduce(module_obj->my_metadata, as_smd_serialize_into_json_reduce_fn, module_obj->json);

	// Store the module's metadata persistently if necessary.
	if (module_obj->json && (retval = as_smd_write(module_obj->module, module_obj->json))) {
		cf_warning(AS_SMD, "failed to write persisted System Metadata file for module \"%s\"", module_obj->module);
		retval = -1;
	}

	// Release the module's JSON if necessary.
	json_decref(module_obj->json);
	module_obj->json = NULL;

	return retval;
}

/*
 *  Create a metadata container for the given module.
 */
static int as_smd_module_create(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	as_smd_item_t *item = cmd->item;
	as_smd_module_t *module_obj;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread - creating module \"%s\"", item->module_name);

	// Verify the module does not yet exist.
	if (RCHASH_OK == (retval = rchash_get(smd->modules, item->module_name, strlen(item->module_name) + 1, (void **) &module_obj))) {
		// (Note:  This is not a problem ~~ May have come over the wire.)
		cf_detail(AS_SMD, "System Metadata module \"%s\" already exists", item->module_name);

		// Give back the reference.
		cf_rc_release(module_obj);

		return retval;
	}

	// Create the module object.
	// [NB:  Reference-counted for insertion in modules "rchash" table.]
	if (!(module_obj = (as_smd_module_t *) cf_rc_alloc(sizeof(as_smd_module_t)))) {
		cf_crash(AS_SMD, "failed to allocate module object for System Metadata module \"%s\"", item->module_name);
	}
	memset(module_obj, 0, sizeof(as_smd_module_t));

	// Set the module's name.
	module_obj->module = cf_strdup(item->module_name);

	// Create the module's local metadata hash table.
	if (RCHASH_OK != rchash_create(&(module_obj->my_metadata), str_hash_fn, metadata_rchash_destructor_fn, 0, 127, RCHASH_CR_MT_BIGLOCK)) {
		cf_crash(AS_SMD, "failed to create the local metadata hash table for System Metadata module \"%s\"", item->module_name);
	}

	// Create the module's external metadata hash table.
	if (RCHASH_OK != rchash_create(&(module_obj->external_metadata), str_hash_fn, metadata_rchash_destructor_fn, 0, 127, RCHASH_CR_MT_BIGLOCK)) {
		cf_crash(AS_SMD, "failed to create the external metadata hash table for System Metadata module \"%s\"", item->module_name);
	}

	// Add the module to the modules hash table.
	if (RCHASH_OK != (retval = rchash_put_unique(smd->modules, item->module_name, strlen(item->module_name) + 1, module_obj))) {
		cf_crash(AS_SMD, "failed to add System Metadata module \"%s\" to modules table (retval %d)", item->module_name, retval);
	}

	// Set the callback functions and their respective user data.
	module_obj->merge_cb = cmd->a;
	module_obj->merge_udata = cmd->b;
	module_obj->accept_cb = cmd->c;
	module_obj->accept_udata = cmd->d;
	module_obj->can_accept_cb = cmd->e;
	module_obj->can_accept_udata = cmd->f;

	int num_items = as_smd_module_restore(module_obj);
	if (0 > num_items) {
		cf_warning(AS_SMD, "failed to restore persisted System Metadata for module \"%s\"", item->module_name);
	}

	// Set an empty metadata item, signifying the completion of module creation,
	//  including the restoration of zero or more persisted metadata items.
	//  (Will trigger an Accept callback with the OPT_CREATE accept option.)
	if ((retval = as_smd_set_metadata(module_obj->module, NULL, NULL))) {
		cf_warning(AS_SMD, "failed to send SMD module \"%s\" creation complete event", module_obj->module);
	}

	return retval;
}

/*
 *  Find or create a System Metadata module object.
 *  The name if the module can be at two places
 *  1. With each item
 *  2. At the item_list level
 *
 *  First preference is to get the information from the specific item
 *  If the item is NULL, get the information from the item_list.
 */
static as_smd_module_t *as_smd_module_get(as_smd_t *smd, as_smd_item_t *item, as_smd_item_list_t *item_list, as_smd_msg_t *msg)
{
	as_smd_module_t *module_obj = NULL;
	int retval = 0;

	char *module_name = NULL;

	// First check for a given message with the module name set.
	if (msg && msg->module_name) {
		cf_debug(AS_SMD, "asmg():  Name of module from message: \"%s\"", module_name);
		module_name = msg->module_name;
	} else if (item && item->module_name) {
		// Next, see if an item is passed and it has module name set.  This takes precedence.
		module_name = item->module_name;
		cf_debug(AS_SMD, "asmg():  Name of module from the item: \"%s\"", module_name);
	} else if (item_list && (item_list->module_name != NULL)) {
		// If there is no item, see if there is module name at item_list level.
		module_name = item_list->module_name;
		cf_debug(AS_SMD, "asmg():  Name of module from item_list: \"%s\"", module_name);
	} else {
		// If the message, item, and item_list are NULL, we cannot do anything.
		cf_debug(AS_SMD, "asmg():  No module name found!");
		return NULL;
	}

	if (RCHASH_OK != (retval = rchash_get(smd->modules, module_name, strlen(module_name) + 1, (void **) &module_obj))) {
		as_smd_cmd_t cmd;
		as_smd_item_t fakeitem;
		// Could not find the module object corresponding to the module name. Create one.
		// Note:  No policy callback will be set if the module is created on-the-fly.
		//
		// Ideally, we should not land into this situation at all.
		// All the legal module objects should get created upfront
		// TODO : Should we not throw a warning/crash here and not create a new module ???
		memset(&cmd, 0, sizeof(as_smd_cmd_t));
		fakeitem.module_name = module_name;	// Only the module name is used. All the callback pointers will be NULL.
		cmd.type = AS_SMD_CMD_CREATE_MODULE;
		cmd.item = &fakeitem;
		if ((retval = as_smd_module_create(smd, &cmd))) {
			cf_warning(AS_SMD, "failed to create System Metadata module \"%s\" (rv %d)", module_name, retval);
		} else {
			cf_debug(AS_SMD, "created System Metadata module \"%s\" on-the-fly", module_name);

			if (RCHASH_OK != (retval = rchash_get(smd->modules, module_name, strlen(module_name) + 1, (void **) &module_obj))) {
				cf_crash(AS_SMD, "failed to get System Metadata module \"%s\" after creation (rv %d)", module_name, retval);
			}
		}
	}

	return module_obj;
}

/*
 *  Destroy a metadata container for the given module after releasing all contained metadata.
 */
static int as_smd_module_destroy(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	as_smd_item_t *item = cmd->item;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread - destroying module \"%s\"", item->module_name);

	// Remove the module's object from the hash table.
	if (RCHASH_OK != (retval = rchash_delete(smd->modules, item->module_name, strlen(item->module_name) + 1))) {
		cf_warning(AS_SMD, "failed to delete System Metadata module \"%s\" (retval %d)", item->module_name, retval);
		return retval;
	}

	return retval;
}

/*
 *  Get or create a new System Metadata fabric msg to perform the given operation on the given metadata items.
 */
static msg *as_smd_msg_get(as_smd_msg_op_t op, as_smd_item_t **item, size_t num_items, char *module_name, uint32_t accept_opt)
{
	msg *msg = NULL;

	cf_debug(AS_SMD, "Getting a msg for module %s with num_items %d for accept option 0x%08x", module_name, num_items, accept_opt);

	// Get an existing (or create a new) System Metadata fabric msg.
	if (!(msg = as_fabric_msg_get(M_TYPE_SMD))) {
		cf_warning(AS_SMD, "failed to get a System Metadata msg");
		return 0;
	}

	// Populate a System Metadata fabric msg from the metadata item.
	int e = 0;
	e += msg_set_uint32(msg, AS_SMD_MSG_ID, AS_SMD_MSG_V2_IDENTIFIER);
	e += msg_set_uint64(msg, AS_SMD_MSG_CLUSTER_KEY, as_paxos_get_cluster_key());
	e += msg_set_uint32(msg, AS_SMD_MSG_OP, op);
	e += msg_set_uint32(msg, AS_SMD_MSG_NUM_ITEMS, num_items);

	if (module_name != NULL) {
		e += msg_set_str(msg, AS_SMD_MSG_MODULE_NAME, module_name, MSG_SET_COPY);
		e += msg_set_uint32(msg, AS_SMD_MSG_OPTIONS, accept_opt);
	}

	if (num_items) {
		int module_sz = 0;
		int key_sz    = 0;
		int value_sz    = 0;
		for (int i = 0; i < num_items; i++) {
			module_sz += strlen(item[i]->module_name) + 1;
			key_sz += strlen(item[i]->key) + 1;
			if (AS_SMD_ACTION_DELETE != item[i]->action) {
				if (item[i]->value) {
					value_sz += strlen(item[i]->value) + 1;
				}
			}
		}
		// Set the array sizes to the number of items.
		e += msg_set_uint32_array_size(msg, AS_SMD_MSG_ACTION, num_items);
		e += msg_set_str_array_size(msg, AS_SMD_MSG_MODULE, num_items, module_sz);
		e += msg_set_str_array_size(msg, AS_SMD_MSG_KEY, num_items, key_sz);
		// (Note:  The corresponding fields won't be used for items with the DELETE action.)
		e += msg_set_str_array_size(msg, AS_SMD_MSG_VALUE, num_items, value_sz);
		e += msg_set_uint32_array_size(msg, AS_SMD_MSG_GENERATION, num_items);
		e += msg_set_uint64_array_size(msg, AS_SMD_MSG_TIMESTAMP, num_items);

		if (0 > e) {
			cf_warning(AS_SMD, "failed to set array size when constructing outbound System Metadata fabric msg for operation %s with %d items: module \"%s\" ; key \"%s\" (rv %d)",
					   AS_SMD_MSG_OP_NAME(op), num_items, item[0]->module_name, item[0]->key, e);
			as_fabric_msg_put(msg);
			return 0;
		}

		// Put all of the items' fields into the msg.
		for (int i = 0; i < num_items; i++) {
			e += msg_set_uint32_array(msg, AS_SMD_MSG_ACTION, i, item[i]->action);
			e += msg_set_str_array(msg, AS_SMD_MSG_MODULE, i, item[i]->module_name);
			e += msg_set_str_array(msg, AS_SMD_MSG_KEY, i, item[i]->key);
			if (AS_SMD_ACTION_DELETE != item[i]->action) {
				if (item[i]->value) {
					e += msg_set_str_array(msg, AS_SMD_MSG_VALUE, i, item[i]->value);
				} else {
					cf_warning(AS_SMD, "SMD item value is not set for op %s", AS_SMD_MSG_OP_NAME(op));
				}
				e += msg_set_uint32_array(msg, AS_SMD_MSG_GENERATION, i, item[i]->generation);
				e += msg_set_uint64_array(msg, AS_SMD_MSG_TIMESTAMP, i, item[i]->timestamp);
			}

			if (0 > e) {
				cf_warning(AS_SMD, "failed to construct outbound System Metadata fabric msg for operation %s with item %d: module \"%s\" ; key \"%s\" (rv %d)",
						   AS_SMD_MSG_OP_NAME(op), i, item[i]->module_name, item[i]->key, e);
				as_fabric_msg_put(msg);
				return 0;
			} else {
				cf_debug(AS_SMD, "Constructed outbound System Metadata fabric msg for operation %s with item %d: module \"%s\" ; key \"%s\" (rv %d)",
						 AS_SMD_MSG_OP_NAME(op), i, item[i]->module_name, item[i]->key, e);
			}
		}
	}

	return msg;
}

/*
 *  Callback for fabric transact responses, both when forwarding metadata change commands to the Paxos principal
 *   and when receiving message events from the Paxos principal.
 *
 *   Note:  This function is currently shared between all System Metadata transactions, which works for now
 *           since the different transaction types don't require separate completion processing.
 */
static int transact_complete_fn(msg *response, void *udata, int fabric_err)
{
//	as_smd_t *smd = (as_smd_t *) udata; // (Not used.)
	int e = 0;
	uint32_t op = 0;

	if (!response) {
		cf_warning(AS_SMD, "Null response message passed in transaction complete!");
		return -1;
	}

	if (AS_FABRIC_SUCCESS != fabric_err) {
		cf_warning(AS_SMD, "System Metadata transaction failed with fabric error %d", fabric_err);
		as_fabric_msg_put(response);
		return -1;
	}

	if (0 > (e = msg_get_uint32(response, AS_SMD_MSG_OP, &op))) {
		cf_warning(AS_SMD, "failed to get metadata operation from failed transaction response msg (err %d ; fabric err %d)", e, fabric_err);
	}

	cf_debug(AS_SMD, "System Metadata received transaction complete for operation %s (fabric error %d)", AS_SMD_MSG_OP_NAME(op), fabric_err);

	as_fabric_msg_put(response);

	return 0;
}

/*
 *  Send the metadata item change message to the Paxos principal.
 */
static int as_smd_proxy_to_principal(as_smd_t *smd, as_smd_msg_op_t op, as_smd_item_t *item)
{
	int retval = 0;

	// Send the new metadata to the Paxos principal.
	cf_node principal = as_paxos_succession_getprincipal();

	if (!principal) {
		cf_warning(AS_SMD, "failed to get the Paxos principal node ~~ Not proxying SMD msg");
		return -1;
	}

	msg *msg = NULL;

	cf_debug(AS_SMD, "forwarding %s metadata request to Paxos principal node %016lX", AS_SMD_MSG_OP_NAME(op), principal);

	// Get an existing (or create a new) System Metadata fabric msg for the appropriate operation and metadata item.
	size_t num_items = 1;
	if (!(msg = as_smd_msg_get(op, &item, num_items, item->module_name, AS_SMD_ACCEPT_OPT_API))) {
		cf_warning(AS_SMD, "failed to get a System Metadata fabric msg for operation %s transact start for module \"%s\"", AS_SMD_MSG_OP_NAME(op), item->module_name);
		return -1;
	}

	as_fabric_transact_start(principal, msg, AS_SMD_TRANSACT_TIMEOUT_MS, transact_complete_fn, smd);

	return retval;
}

/*
 *  Locally change a metadata item.
 */
static int as_smd_metadata_change_local(as_smd_t *smd, as_smd_msg_op_t op, as_smd_item_t *item)
{
	int retval = 0;

	as_smd_module_t *module_obj = NULL;

	cf_debug(AS_SMD, "System Metadata thread - locally %s'ing metadata: node %016lX ; action %s ; module \"%s\" ; key \"%s\"",
			 AS_SMD_MSG_OP_NAME(op), item->node_id, AS_SMD_ACTION_NAME(item->action), item->module_name, item->key);

	// Find the module's object.
	if (RCHASH_OK != (retval = rchash_get(smd->modules, item->module_name, strlen(item->module_name) + 1, (void **) &module_obj))) {
		cf_warning(AS_SMD, "failed to find System Metadata module \"%s\" (retval %d)", item->module_name, retval);
		return retval;
	}

	if (AS_SMD_ACTION_DELETE == item->action) {
		// Delete the metadata from the module's local metadata hash table.
		if (RCHASH_OK != (retval = rchash_delete(module_obj->my_metadata, item->key, strlen(item->key) + 1))) {
			cf_warning(AS_SMD, "failed to delete key \"%s\" from System Metadata module \"%s\" (retval %d)", item->key, item->module_name, retval);
		}
	} else if (item->key) {
		// Handle the Set case:

		// Add reference to item for storage in the hash table.
		// (Note:  One reference to the item will be released by the thread when it releases the containing command.)
		cf_rc_reserve(item);

		// Select metadata local hash table for incoming metadata.
		rchash *metadata_hash = module_obj->my_metadata;

		// The length of the key string includes the NULL terminator.
		uint32_t key_len = strlen(item->key) + 1;

		// If the item is local, simply use the key string within the item.
		void *key = item->key;

		// Default to generation 1.
		if (!item->generation) {
			item->generation = 1;
		}

		// Default timestamp to now.
		if (!item->timestamp) {
			item->timestamp = cf_getms();
		}

		// Add new, or replace existing, metadata in the module's metadata hash table.
		if (RCHASH_OK != (retval = rchash_put(metadata_hash, key, key_len, item))) {
			cf_warning(AS_SMD, "failed to set metadata for key \"%s\" for System Metadata module \"%s\" (retval %d)", item->key, item->module_name, retval);
		}
	} else {
		cf_debug(AS_SMD, "(not setting empty metadata item for module \"%s\")", module_obj->module);
	}

#ifdef DEBUG
	rchash_dump(module_obj->my_metadata);
#endif

	// Give back the module reference.
	cf_rc_release(module_obj);

	return retval;
}

/*
 *  Handle a metadata change request by proxying to Paxos principal or short-circuiting locally during node start-up.
 */
static int as_smd_metadata_change(as_smd_t *smd, as_smd_msg_op_t op, as_smd_item_t *item)
{
	int retval = 0;

	if ((AS_SMD_STATE_RUNNING == smd->state) && item->key) {
		// Forward to Paxos principal.
		// [Ideally, would re-try or at least notify (via an as-yet nonexistent mechanism) upon failure.]
		return as_smd_proxy_to_principal(smd, op, item);
	} else {
		// Short-circuit to handle change locally when this node is starting up
		// or when an initially-empty module is being created, as indicated by NULL item key (and value.)

		cf_debug(AS_SMD, "handling metadata change type %s locally: module \"%s\" ; key \"%s\"", AS_SMD_MSG_OP_NAME(op), item->module_name, item->key);

		if (item && (retval = as_smd_metadata_change_local(smd, op, item))) {
			cf_warning(AS_SMD, "failed to %s a metadata item locally: module \"%s\" ; key \"%s\" ; value \"%s\"", AS_SMD_MSG_OP_NAME(op), item->module_name, item->key, item->value);
		}

		uint32_t accept_opt = AS_SMD_ACCEPT_OPT_API;
		as_smd_item_list_t *item_list = NULL;

		if (!item->key) {
			// Empty key (and value) indicates creation of an initially-empty module.
			accept_opt = AS_SMD_ACCEPT_OPT_CREATE;
		} else {
			// While restoring pass this info to the module as well.  This is needed
			// at the boot to make sure metadata init is done before the data init is done.
			item_list = as_smd_item_list_alloc(1);
			item_list->item[0] = item;
		}

		as_smd_module_t *module_obj   = as_smd_module_get(smd, item, NULL, NULL);

		if (module_obj->accept_cb) {
			// Invoke the module's registered accept policy callback function.
			(module_obj->accept_cb)(module_obj->module, item_list, module_obj->accept_udata, accept_opt);
		}

		// Persist the accepted metadata for this module.
		if (as_smd_module_persist(module_obj)) {
			cf_warning(AS_SMD, "failed to persist accepted metadata for module \"%s\"", module_obj->module);
		}

		cf_rc_release(module_obj);

		if (item_list) {
			cf_free(item_list);
		}
	}

	return retval;
}

/*
 *  Type representing the state of a metadata get request.
 */
typedef struct as_smd_metadata_get_state_s {
	size_t num_items;                   // Number of matching items.
	as_smd_item_t *item;                // Item to compare with each item.
	as_smd_item_list_t *item_list;      // List of matching items.
	rchash_reduce_fn reduce_fn;         // Reduce function to apply to matching items.
} as_smd_metadata_get_state_t;

/*
 *  Reduce function to count one metadata item.
 */
static int as_smd_count_matching_item_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_metadata_get_state_t *get_state = (as_smd_metadata_get_state_t *) udata;

	// Count each matching item.
	if (!strcmp(get_state->item->key, "") || !strcmp(get_state->item->key, item->key)) {
		get_state->num_items += 1;
	}

	return 0;
}

/*
 *  Reduce function to return a single metadata option, if it matches the pattern.
 */
static int as_smd_metadata_get_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_metadata_get_state_t *get_state = (as_smd_metadata_get_state_t *) udata;
	as_smd_item_list_t *item_list = get_state->item_list;

	// Add each matching item to the list.
	if (!strcmp(get_state->item->key, "") || !strcmp(get_state->item->key, item->key)) {
		cf_rc_reserve(item);
		item_list->item[item_list->num_items] = item;
		item_list->num_items += 1;
	}

	return 0;
}

/*
 *  Reduce function to perform a given reduce function on each matching module.
 */
static int as_smd_matching_module_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	char *module = (char *) key;
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_metadata_get_state_t *get_state = (as_smd_metadata_get_state_t *) udata;

	// Perform the given reduce function on matching module's metadata.
	if (!strcmp(get_state->item->module_name, "") || !strcmp(get_state->item->module_name, module)) {
		rchash_reduce(module_obj->my_metadata, get_state->reduce_fn, get_state);
	}

	return 0;
}

/*
 *  Search for metadata according to the given search criteria.
 *  The incoming item's module and/or key can be NULL to perform a wildcard match.
 */
static int as_smd_metadata_get(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	as_smd_item_t *item = cmd->item;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread - get metadata: module \"%s\" ; node %016lX ; key \"%s\"", item->module_name, item->node_id, item->key);

	// Extract the user's callback function and user data.
	as_smd_get_cb get_cb = cmd->a;
	void *get_udata = cmd->b;

	if (!get_cb) {
		cf_warning(AS_SMD, "no System Metadata get callback supplied ~~ Ignoring get metadata request!");
		return -1;
	}

	as_smd_metadata_get_state_t get_state;
	get_state.num_items = 0;
	get_state.item = item;
	get_state.item_list = NULL;
	get_state.reduce_fn = as_smd_count_matching_item_reduce_fn;

	// Count the number of matching items.
	rchash_reduce(smd->modules, as_smd_matching_module_reduce_fn, &get_state);

	// Allocate a list of sufficient size for the get result.
	as_smd_item_list_t *item_list = NULL;
	if (!(item_list = as_smd_item_list_alloc(get_state.num_items))) {
		cf_warning(AS_SMD, "failed to allocate metadata item list of length %d ~~ Not getting metadata!", get_state.num_items);
		return -1;
	}
	get_state.item_list = item_list;

	// (Note:  Use num_items to count the position for each metadata item.)
	item_list->num_items = 0;

	// Add matching items to the list.
	get_state.reduce_fn = as_smd_metadata_get_reduce_fn;
	rchash_reduce(smd->modules, as_smd_matching_module_reduce_fn, &get_state);

	// Invoke the user's callback function.
	(get_cb)(item->module_name, item_list, get_udata);

	// Release the item list.
	as_smd_item_list_destroy(item_list);

	return retval;
}

/*
 *  Cleanly release all System Metadata resources.
 */
static void as_smd_terminate(as_smd_t *smd)
{
	cf_debug(AS_SMD, "SMD Terminate called");

	// After this is NULLed out, no more messages will be sent to the System Metadata queue.
	g_config.smd = NULL;

	// De-register reception of Paxos state changed events.
	if (as_paxos_deregister_change_callback(as_smd_paxos_state_changed_fn, smd)) {
		cf_crash(AS_SMD, "Failed to deregister System Metadata Paxos state changed callback!");
	}

	// De-register the System Metadata fabric transact message type.
	// [Note:  Don't need to remove the handler, simply drop the msg in the handler function.]
//	as_fabric_transact_register(M_TYPE_SMD, NULL, 0, NULL, NULL);

	// Go to the not started up yet state.
	smd->state = AS_SMD_STATE_IDLE;

	// Destroy the message queue.
	cf_queue_destroy(smd->msgq);

	// Release the scoreboard hash table.
	shash_destroy(smd->scoreboard);

	// Release the modules hash table.
	rchash_destroy(smd->modules);

	// Release the System Metadata object.
	cf_free(smd);
}

/*
 *  Reduce function to count one metadata item.
 */
static int as_smd_count_item_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
//	as_smd_item_t *item = (as_smd_item_t *) object; // (Not used.)
	size_t *num_items = (size_t *) udata;

	*num_items += 1;

	return 0;
}

/*
 *  Reduce function to count metadata items in one module.
 */
static int as_smd_module_count_items_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *module = (char *) key; // (Not used.)
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	size_t *num_items = (size_t *) udata;

	// Increase the running total by the count the number of metadata items in this module.
	rchash_reduce(module_obj->my_metadata, as_smd_count_item_reduce_fn, num_items);

	return 0;
}

/*
 *  Reduce function to serialize one metadata item.
 */
static int as_smd_item_serialize_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *smd_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_item_list_t *item_list = (as_smd_item_list_t *) udata;

	// Add a this metadata item to the list.
	cf_rc_reserve(item);
	item_list->item[item_list->num_items] = item;
	item_list->num_items += 1;

	return 0;
}

/*
 *  Reduce function to serialize all of a module's metadata items.
 */
static int as_smd_module_serialize_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *module = (char *) key; // (Not used.)
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_item_list_t *item_list = (as_smd_item_list_t *) udata;

	// Serialize all of this module's metadata items.
	rchash_reduce(module_obj->my_metadata, as_smd_item_serialize_reduce_fn, item_list);

	return 0;
}

/*
 *  Handle a Paxos state changed message.
 *  This function collects all metadata items in this node, from all the module,
 *  currently (UDF, SINDEX) and sends it to the Paxos principal for merging the metadata.
 */
static void as_smd_paxos_changed(as_smd_t *smd, as_smd_cmd_t *cmd)
{
	cf_debug(AS_SMD, "System Metadata thread received Paxos state changed cmd event!");

	// Q: Does the Paxos state need to be passed and considered with the event???
	// (The cluster key is being sent in the msg for verification by the principal.)

	// Determine the number of metadata items to be sent.
	size_t num_items = 0;
	rchash_reduce(smd->modules, as_smd_module_count_items_reduce_fn, &num_items);

	cf_debug(AS_SMD, "sending %d serialized metadata items to the Paxos principal", num_items);

	// Copy all reference-counted metadata item pointers from the hash table into an item list.
	// (Note:  Even if this node has no metadata items, we must still send a message to the principal.)
	as_smd_item_list_t *item_list;
	if (!(item_list = as_smd_item_list_alloc(num_items))) {
		cf_crash(AS_SMD, "failed to create a System Metadata item list of size %d", num_items);
	}
	// (Note:  Use num_items to count the position for each serialized metadata item.)
	item_list->num_items = 0;
	// set the module name to NULL, because this item_list consists of items from all modules registered to SMD
	item_list->module_name = NULL;
	rchash_reduce(smd->modules, as_smd_module_serialize_reduce_fn, item_list);

	cf_debug(AS_SMD, "aspc():  num_items = %d (%d)", item_list->num_items, num_items);

	// Build a System Metadata fabric msg containing serialized metadata from the item list.
	msg *msg = NULL;
	as_smd_msg_op_t my_smd_op = AS_SMD_MSG_OP_MY_CURRENT_METADATA;
	if (!(msg = as_smd_msg_get(my_smd_op, item_list->item, item_list->num_items, NULL, 0))) {
		cf_crash(AS_SMD, "failed to get a System Metadata fabric msg for operation %s transact start", AS_SMD_MSG_OP_NAME(my_smd_op));
	}

	// The metadata has been copied into the fabric msg and can now be released.
	as_smd_item_list_destroy(item_list);

	// Send the serialized metadata to the Paxos principal.
	cf_node principal = as_paxos_succession_getprincipal();
	as_fabric_transact_start(principal, msg, AS_SMD_TRANSACT_TIMEOUT_MS, transact_complete_fn, smd);
}

/*
 *  Destroy a node's scoreboard hash table mapping module to metadata item count.
 */
static int as_smd_scoreboard_reduce_delete_fn(void *key, void *data, void *udata)
{
	cf_node node_id = (cf_node) key;
	shash *module_item_count_hash = *((shash **) data);

	cf_debug(AS_SMD, "destroying module item count hash for node %016lX", node_id);

	shash_destroy(module_item_count_hash);

	return SHASH_REDUCE_DELETE;
}

/*
 *  Remove the metadata item from the hash table.
 */
static int as_smd_reduce_delete_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	return RCHASH_REDUCE_DELETE;
}

/*
 *  Delete all of this module's external metadata items.
 */
static int as_smd_delete_external_metadata_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *module = (char *) key; // (Not used.)
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_t *smd = (as_smd_t *) udata;

	rchash_reduce(module_obj->external_metadata, as_smd_reduce_delete_fn, smd);
	cf_debug(AS_SMD, "All the entries in the scoreboard has been deleted");

	return 0;
}

/*
 *  Clear out the temporary state used to merge metadata upon cluster state change.
 */
static void as_smd_clear_scoreboard(as_smd_t *smd)
{
	shash_reduce_delete(smd->scoreboard, as_smd_scoreboard_reduce_delete_fn, smd);
	rchash_reduce(smd->modules, as_smd_delete_external_metadata_reduce_fn, smd);
}

/*
 *  Apply a metadata change locally using the registered merge policy, defaulting to union.
 */
static int as_smd_apply_metadata_change(as_smd_t *smd, as_smd_module_t *module_obj, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	as_smd_item_t *item = smd_msg->items->item[0]; // (Only log the fist item.)
	cf_debug(AS_SMD, "System Metadata thread - applying metadata %s change: item 0:  module \"%s\" ; key \"%s\" ; value \"%s\" ; action %d",
			 AS_SMD_MSG_OP_NAME(smd_msg->op), module_obj->module, item->key, item->value, item->action);

	// [Note:  Only 1 item should ever be changed via this path.]
	if (1 != smd_msg->num_items) {
		cf_crash(AS_SMD, "unexpected number of metadata items being changed: %d != 1", smd_msg->num_items);
	}

#if 0 
	if (module_obj->merge_cb) {
		// Invoke the module's registered merge policy callback function.
		(module_obj->merge_cb)(module_obj->module, smd_msg->item, NULL, module_obj->merge_udata);
	} else {
#endif
		cf_debug(AS_SMD, "asamc():  num_items %d", smd_msg->num_items);

		// By default, simply perform a union operation on an item-by-item basis.
		for (int i = 0; i < smd_msg->num_items; i++) {
			item = smd_msg->items->item[i];
			if (module_obj->can_accept_cb) {
				int ret = (module_obj->can_accept_cb)(module_obj->module, item, module_obj->can_accept_udata);
				if (ret != 0) {
					cf_info(AS_SMD, "Paxos principal rejected the user operation with error code %s", as_sindex_err_str(ret));
					continue;
				} else {
					cf_debug(AS_SMD, "Paxos principal validity check succeeded.");
				}
			}

			// Default timestamp to now.
			if (!item->timestamp) {
				item->timestamp = cf_getms();
			}

			cf_debug(AS_SMD, "asamc():  processing item %d: module \"%s\" key \"%s\" action %s gen %u ts %lu", i, item->module_name, item->key, AS_SMD_ACTION_NAME(item->action), item->generation, item->timestamp);

			// Perform the appropriate union operation.

			as_smd_item_t *existing_item = NULL;
			if (RCHASH_OK == rchash_get(module_obj->my_metadata, item->key, strlen(item->key) + 1, (void **) &existing_item)) {
				cf_debug(AS_SMD, "asamc():  Old item exists.");
			} else {
				cf_debug(AS_SMD, "asamc():  Old item does not exist.");

				if (AS_SMD_ACTION_DELETE == item->action) {
					cf_warning(AS_SMD, "deleting a non-extant item: module \"%s\" ; key \"%s\"", item->module_name, item->key);
				}
			}

			if (!existing_item) {
				// For delete, if item already doesn't exist, there's nothing to do.
				if (AS_SMD_ACTION_DELETE == item->action) {
					continue;
				} else {
					// Otherwise, default to generation 1.
					if (!item->generation) {
						item->generation = 1;
					}
				}
			}

			// Choose the most up-to-date item data.
			if (existing_item && (AS_SMD_ACTION_DELETE != item->action)) {
				// Default to the next generation.
				if (!item->generation) {
					item->generation = existing_item->generation + 1;
				}

				// Choose the newest first by the highest generation and second by the highest timestamp.
				if ((existing_item->generation > item->generation) ||
						((existing_item->generation == item->generation) && (existing_item->timestamp > item->timestamp))) {

					cf_debug(AS_SMD, "old item is newer");

					// If the existing item is newer, skip the incoming item.
					cf_rc_release(existing_item);
					continue;
				} else {
					// Otherwise, advance the generation.
					item->generation = existing_item->generation + 1;

					cf_debug(AS_SMD, "New items is newer:  Going to gen %u ts %lu", item->generation, item->timestamp);
				}
				cf_rc_release(existing_item);
				existing_item = NULL;
			}

			// For each member of the Paxos succession list,
			//   Generate a new SMD fabric msg sharing the properties of the incoming msg event.
			//   Start a transaction to send the msg out to the node.
			//   The transaction recv function performs the accept metadata function locally.

			// (Note:  Ideally this map-transaction-over-succession-list operation should be provided by Paxos.)
			for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
				msg *msg = NULL;
				cf_node node_id = g_config.paxos->succession[i];
				if (!node_id) {
					continue;
				}
				as_smd_msg_op_t accept_op = AS_SMD_MSG_OP_ACCEPT_THIS_METADATA;
				if (!(msg = as_smd_msg_get(accept_op, smd_msg->items->item, smd_msg->num_items, module_obj->module, AS_SMD_ACCEPT_OPT_API))) {
					cf_warning(AS_SMD, "failed to get a System Metadata fabric msg for operation %s transact start ~~ Skipping node %016lX!",
							   AS_SMD_MSG_OP_NAME(accept_op), node_id);
					continue;
				}
				as_fabric_transact_start(node_id, msg, AS_SMD_TRANSACT_TIMEOUT_MS, transact_complete_fn, smd);
			}
		}
#if 0 
	}
#endif

	return retval;
}

/*
 *  Increment hash table value by the given delta, starting from zero if not found, and return the new total.
 */
static int as_smd_shash_incr(shash *ht, as_smd_module_t *module_obj, size_t delta)
{
	size_t count = 0;

	if (SHASH_OK != shash_get(ht, &module_obj, &count)) {
		// If not found, start at zero.
		count = 0;
	}

	count += delta;

	if (SHASH_OK != shash_put(ht, &module_obj, &count)) {
		cf_crash(AS_SMD, "failed to increment shash value for module \"%s\"", module_obj->module);
	}

	cf_debug(AS_SMD, "incrementing metadata item count for module \"%s\" to %d", module_obj->module, count);

	return count;
}

/*
 *  Add the metadata items from this msg to the appropriate modules' external hash tables.
 */
static shash *as_smd_store_metadata_by_module(as_smd_t *smd, as_smd_msg_t *smd_msg)
{
	as_smd_item_list_t *items = smd_msg->items;
	shash *module_item_count_hash = NULL;

	// Allocate a hash table mapping module ==> number of metadata items from this node.
	if (SHASH_OK != shash_create(&module_item_count_hash, ptr_hash_fn, sizeof(as_smd_module_t *), sizeof(size_t), 19, SHASH_CR_MT_BIGLOCK)) {
		cf_warning(AS_SMD, "failed to allocate module item count hash table");
		return NULL;
	}

	for (int i = 0; i < items->num_items; i++) {
		as_smd_item_t *item = items->item[i];

		// Find the appropriate module's external hash table for this item.
		as_smd_module_t *module_obj = NULL;
		if (!(module_obj = as_smd_module_get(smd, item, NULL, NULL))) {
			cf_warning(AS_SMD, "failed to get System Metadata module \"%s\" ~~ Skipping item!", item->module_name);
			continue;
		}

		// The length of the key string includes the NULL terminator.
		uint32_t key_len = strlen(item->key) + 1;
		uint32_t stack_key_len = sizeof(as_smd_external_item_key_t) + key_len;

		as_smd_external_item_key_t *stack_key = alloca(stack_key_len);
		if (!stack_key) {
			cf_crash(AS_SMD, "Failed to allocate stack key of size %d bytes!", stack_key_len);
		}
		stack_key->node_id = item->node_id;
		stack_key->key_len = key_len;
		memcpy(&(stack_key->key), item->key, key_len);

		// Warn if the item is already present.
		as_smd_item_t *old_item = NULL;
		rchash *metadata_hash = module_obj->external_metadata;
		if (RCHASH_OK == rchash_get(metadata_hash, stack_key, stack_key_len, (void **) &old_item)) {
			cf_warning(AS_SMD, "found existing metadata item: node: %016lX module: \"%s\" key: \"%s\" value: \"%s\" ~~ Replacing with value: \"%s\"!",
					   item->node_id, item->module_name, item->key, old_item->value, item->value);
			// Give back the item reference.
			cf_rc_release(old_item);
		}

		// Add reference to item for storage in the hash table.
		// (Note:  One reference to the item will be released by the thread when it releases the containing msg.)
		cf_rc_reserve(item);

		// Insert the new metadata into the module's external metadata hash table, replacing any previous contents.
		if (RCHASH_OK != rchash_put(metadata_hash, stack_key, stack_key_len, item)) {
			cf_warning(AS_SMD, "failed to insert metadata for key \"%s\" for System Metadata module \"%s\"", item->key, item->module_name);
		}

		cf_debug(AS_SMD, "Stored metadata by module for item %d: module \"%s\" ; key \"%s\"", i, module_obj->module, stack_key->key);
		// Increment the number of items for this module in this node's hash table.
		as_smd_shash_incr(module_item_count_hash, module_obj, 1);

		// Give back the module reference.
		cf_rc_release(module_obj);
	}

	return module_item_count_hash;
}

/*
 *  Type for searching for and returning metadata items from a given node.
 */
typedef struct as_smd_node_item_search_s {
	cf_node node_id;                 // Node to look for.
	as_smd_item_list_t *item_list;   // Where to store the result.
} as_smd_node_item_search_t;

/*
 *  Reduce function to find and add metadata items from a given node to an item list.
 */
static int as_smd_item_list_for_node_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	as_smd_external_item_key_t *item_key = (as_smd_external_item_key_t *) key;
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_node_item_search_t *search = (as_smd_node_item_search_t *) udata;

	// Add each matching metadata item to the list.
	if (item_key->node_id == search->node_id) {
		cf_rc_reserve(item);
		search->item_list->item[search->item_list->num_items] = item;
		search->item_list->num_items += 1;
		cf_debug(AS_SMD, "For the node \"%016lX\", num_items is %d", item_key->node_id, search->item_list->num_items);
	}

	return 0;
}

/*
 *  Reduce function to create a list of metadata items from an rchash table.
 */
static int as_smd_list_items_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
//	char *item_key = (char *) key; // (Not used.)
	as_smd_item_t *item = (as_smd_item_t *) object;
	as_smd_item_list_t *item_list = (as_smd_item_list_t *) udata;

	cf_debug(AS_SMD, "adding to item list item: node: %016lX ; module: \"%s\" ; key: \"%s\"", item->node_id, item->module_name, item->key);
	cf_debug(AS_SMD, "item list: %p", item_list);
	cf_debug(AS_SMD, "item list length: %d", item_list->num_items);

	cf_rc_reserve(item);

	item_list->item[item_list->num_items] = item;
	item_list->num_items += 1;

	return 0;
}

/*
 *  Invoke the merge policy callback function for this module.
 */
static int as_smd_invoke_merge_reduce_fn(void *key, uint32_t keylen, void *object, void *udata)
{
	char *module = (char *) key;
	as_smd_module_t *module_obj = (as_smd_module_t *) object;
	as_smd_t *smd = (as_smd_t *) udata;

	cf_debug(AS_SMD, "invoking merge policy for module \"%s\"", module);

	as_smd_item_list_t *item_list_out = NULL;
	size_t num_lists = g_config.paxos->cluster_size;
	as_smd_item_list_t **item_lists_in = NULL;
	if (!(item_lists_in = (as_smd_item_list_t **) cf_calloc(num_lists, sizeof(as_smd_item_list_t *)))) {
		cf_crash(AS_SMD, "failed to allocate %d System Metadata item lists", num_lists);
	}

	int list_num = 0;
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		cf_node node_id = g_config.paxos->succession[i];

		// Skip any non-existent nodes.
		if (!node_id) {
			cf_detail(AS_SMD, "[Skipping non-existent succession list item #%d.]", i);
			continue;
		}

		shash *module_item_count_hash = NULL;
		if (SHASH_OK != shash_get(smd->scoreboard, &node_id, &module_item_count_hash)) {
			cf_debug(AS_SMD, "***Cluster Size Is: %d ; Scoreboard size is %d***", g_config.paxos->cluster_size, shash_get_size(smd->scoreboard));

			// Node may be in succession but not officially in cluster yet....
			cf_debug(AS_SMD, "failed to get module item count hash for node %016lX ~~ Skipping!", node_id);
			continue;
		}

		size_t num_items = 0;
		if (SHASH_OK != shash_get(module_item_count_hash, &module_obj, &num_items)) {
			cf_debug(AS_SMD, "failed to get module items count for module \"%s\" from node %016lX ~~ Using 0!", module_obj->module, node_id);
		}

		// Start with an an empty item list.
		if (!(item_lists_in[list_num] = as_smd_item_list_alloc(num_items))) {
			cf_crash(AS_SMD, "failed to create merge item list for node %016lX", node_id);
		}

		item_lists_in[list_num]->module_name = cf_strdup(module);

		// Only search for items to add to the list any exist.
		if (num_items) {
			// Add all of this node's metadata items to this list.
			as_smd_node_item_search_t search;
			search.node_id = node_id;
			search.item_list = item_lists_in[list_num];

			// (Note:  Use num_items to count the position for each metadata item.)
			item_lists_in[list_num]->num_items = 0;
			rchash_reduce(module_obj->external_metadata, as_smd_item_list_for_node_reduce_fn, &search);
		}

		list_num++;
	}

	// Merge the metadata item lists for this module.
	if (module_obj->merge_cb) {
		// Invoke the module's registered merge policy callback function.
		(module_obj->merge_cb)(module, &item_list_out, item_lists_in, list_num, module_obj->merge_udata);
	} else {
		cf_debug(AS_SMD, "no merge cb registered ~~ performing default merge policy: union");

		// No merge policy registered ~~ Default to union.
		rchash *merge_hash = NULL;
		if (RCHASH_OK != rchash_create(&merge_hash, str_hash_fn, metadata_rchash_destructor_fn, 0, 127, RCHASH_CR_MT_BIGLOCK)) {
			cf_crash(AS_SMD, "failed to create merge hash table for module \"%s\"", module_obj->module);
		}

		// Run through all metadata items in all node's lists.
		for (int i = 0; i < list_num; i++) {
			if (item_lists_in[i]) {
				for (int j = 0; j < item_lists_in[i]->num_items; j++) {
					as_smd_item_t *new_item = item_lists_in[i]->item[j];
					uint32_t key_len = strlen(new_item->key) + 1;

					// Look for an existing items with this key.
					as_smd_item_t *existing_item = NULL;
					if (RCHASH_OK != rchash_get(merge_hash, new_item->key, key_len, (void **) &existing_item)) {
						// If not found, insert this item.
						if (RCHASH_OK != rchash_put(merge_hash, new_item->key, key_len, new_item)) {
							cf_crash(AS_SMD, "failed to insert item into merge hash");
						}
					} else {
						// Otherwise, choose a winner first by the highest generation and second by the highest timestamp.
						as_smd_item_t *winning_item = ((existing_item->generation > new_item->generation) ||
								((existing_item->generation == new_item->generation) &&
								 (existing_item->timestamp > new_item->timestamp)) ? existing_item : new_item);

						// And insert it back into the hash table.
						if (RCHASH_OK != rchash_put(merge_hash, winning_item->key, key_len, winning_item)) {
							cf_crash(AS_SMD, "failed to insert item into merge hash");
						}
					}
				}
			}
		}

		// Create a merged items list.
		size_t num_items = rchash_get_size(merge_hash);
		if (!(item_list_out = as_smd_item_list_alloc(num_items))) {
			cf_crash(AS_SMD, "failed to create System Metadata items list of size %d", num_items);
		}

		// Populate the merged items list from the hash table.
		// (Note:  Use num_items to count the position for each metadata item.)
		item_list_out->num_items = 0;
		rchash_reduce(merge_hash, as_smd_list_items_reduce_fn, item_list_out);
		rchash_destroy(merge_hash);
	}

	// Sent out a merged metadata msg via fabric transaction to every cluster node.
	msg *msg = NULL;
	as_smd_msg_op_t merge_op = AS_SMD_MSG_OP_ACCEPT_THIS_METADATA;
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		cf_node node_id = g_config.paxos->succession[i];
		// Skip any non-existent nodes.
		if (!node_id) {
			continue;
		}
		if (!(msg = as_smd_msg_get(merge_op, item_list_out->item, item_list_out->num_items, module, AS_SMD_ACCEPT_OPT_MERGE))) {
			cf_crash(AS_SMD, "failed to get a System Metadata fabric msg for operation %s", AS_SMD_MSG_OP_NAME(merge_op));
		}
		as_fabric_transact_start(node_id, msg, AS_SMD_TRANSACT_TIMEOUT_MS, transact_complete_fn, smd);
	}

#if 0 
	// Apparently not necessary to do this here, but still need to make sure 
	// the list containers do not leak.
	// Release the item lists.
	for (int i = 0; i < num_lists; i++) {
		as_smd_item_list_destroy(item_lists_in[i]);
	}
	cf_free(item_lists_in);

	// Release the merged items list.
	as_smd_item_list_destroy(item_list_out);
#endif

	return 0;
}

/*
 *  Receive a node's metadata on the Paxos principal to be combined via the registered merge policy.
 */
static int as_smd_receive_metadata(as_smd_t *smd, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	// Only the Paxos principal receives other node's metadata.)
	if (as_paxos_succession_getprincipal() != g_config.self_node) {
		cf_debug(AS_SMD, "non-principal node %016lX received metadata from node %016lX ~~ Ignoring!", g_config.self_node, smd_msg->node_id);
		return -1;
	}

	cf_debug(AS_SMD, "System Metadata thread - received %d metadata items from node %016lX", smd_msg->num_items, smd_msg->node_id);

	if (as_paxos_get_cluster_key() != smd_msg->cluster_key) {
		cf_debug(AS_SMD, "received SMD with non-current cluster key (%016lx != %016lx) from node %016lX ~~ Ignoring!",
				 smd_msg->cluster_key, as_paxos_get_cluster_key(), smd_msg->node_id);
		return -1;
	}

	// Store the all of the metadata items received from this node in the appropriate module's external metadata hash table.
	// And return the item counts by module in a hash table.
	shash *module_item_count_hash = NULL;
	if (!(module_item_count_hash = as_smd_store_metadata_by_module(smd, smd_msg))) {
		cf_crash(AS_SMD, "failed to store metadata by module from node %016lX", smd_msg->node_id);
	}

	// If something is already there, its obsolete, so release it.
	shash *prev_module_item_count_hash = NULL;
	if (SHASH_OK == shash_get(smd->scoreboard, &(smd_msg->node_id), &prev_module_item_count_hash)) {
		cf_debug(AS_SMD, "found an obsolete module item count hash for node %016lX ~~ Deleting!", smd_msg->node_id);
		if (SHASH_OK != shash_delete(smd->scoreboard, &(smd_msg->node_id))) {
			cf_warning(AS_SMD, "failed to delete obsolete module item count hash for node %016lX", smd_msg->node_id);
		}
		shash_destroy(prev_module_item_count_hash);
	}

	// Note that this node has provided its metadata for this cluster state change.
	if (SHASH_OK != shash_put_unique(smd->scoreboard, &(smd_msg->node_id), &module_item_count_hash)) {
		cf_warning(AS_SMD, "failed to put unique node %016lX into System Metadata scoreboard hash table", smd_msg->node_id);
	}

	// Merge the metadata when all nodes have reported in.
	if (shash_get_size(smd->scoreboard) == g_config.paxos->cluster_size) {
		cf_debug(AS_SMD, "received metadata from all %d cluster nodes ~~ invoking merge policies", g_config.paxos->cluster_size);

		// TODO: Make sure we're still principal. 
		if (as_paxos_succession_getprincipal() != g_config.self_node) {
			cf_warning(AS_SMD, "no longer Paxos principal ~~ Not invoking merge!");
		} else {
			cf_debug(AS_SMD, "Invoking merge reduce in Paxos principal");
			// Invoke the merge policy for each module and send the results to all nodes.
			rchash_reduce(smd->modules, as_smd_invoke_merge_reduce_fn, smd);
		}

		// Clear out the state used to notify cluster nodes of the new metadata.
		as_smd_clear_scoreboard(smd);
	} else if (shash_get_size(smd->scoreboard) > g_config.paxos->cluster_size) {
		// Cluster is unstable.
		// While one node is coming up, one of other nodes has gone down.
		// e.g Consider 3 node cluster. Add new node. Cluster size is 4.
		// Paxos principal has received information from 3 nodes and waiting for fourth node.
		// So score board size is 3.
		// But now two node has gone down. Cluster size is reduced to 2.
		as_smd_clear_scoreboard(smd);
	} else {
		cf_debug(AS_SMD, "Cluster size = %d and smd->scoreboard size = %d ", g_config.paxos->cluster_size, shash_get_size(smd->scoreboard));
	}

	return retval;
}

static int metadata_local_deleteall_fn(void * key, uint32_t key_len, void *object, void *udata)
{
	return RCHASH_REDUCE_DELETE;
}

/*
 *  Accept a metadata change from the Paxos principal using the registered accept policy.
 */
static int as_smd_accept_metadata(as_smd_t *smd, as_smd_module_t *module_obj, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	// There will be:
	//    0 items when, after the merge, no valid metadata items were found according to the merge algorithm.
	//    1 item when the user issues a set/delete metadata API call to a specific module (e.g., SINDEX, UDF.)
	// >= 1 items when, after the merge, a non-empty list of items is valid according to the merge algorithm.
	if (smd_msg->items->num_items) {
		as_smd_item_t *item = smd_msg->items->item[0]; // (Only log the fist item.)
		cf_debug(AS_SMD, "System Metadata thread - accepting metadata %s change: %d items: item 0: module \"%s\" ; key \"%s\" ; value \"%s\"",
				 AS_SMD_MSG_OP_NAME(smd_msg->op), smd_msg->items->num_items, module_obj->module, item->key, item->value);
	} else {
		// Allow empty item list for merge and module create.
		if (smd_msg->options & (AS_SMD_ACCEPT_OPT_MERGE | AS_SMD_ACCEPT_OPT_CREATE)) {
			cf_debug(AS_SMD, "System Metadata thread - accepting metadata %s change: Zero items coming from merge", AS_SMD_MSG_OP_NAME(smd_msg->op));
		} else {
			cf_debug(AS_SMD, "System Metadata thread - accepting metadata %s change: Zero items ~~ Returning!", AS_SMD_MSG_OP_NAME(smd_msg->op));
			return retval;
		}
	}

	cf_debug(AS_SMD, "accepting replacement metadata from incoming System Metadata msg");

#if 1 // DEBUG
	// It should never be null. Being defensive to bail out just in case.
	if (!module_obj) {
		cf_crash(AS_SMD, "SMD module NULL in accept metadata!");
	}
#endif

	// In case of merge (after cluster state change) drop the existing local metadata definitions
	// This is done to clean up some metadata, which could have been dropped during the merge
	if (smd_msg->options & AS_SMD_ACCEPT_OPT_MERGE) {
		rchash_reduce(module_obj->my_metadata, metadata_local_deleteall_fn, NULL);
	}

	for (int i = 0; i < smd_msg->items->num_items; i++) {
		as_smd_item_t *item = smd_msg->items->item[i];
		if ((retval = as_smd_metadata_change_local(smd, smd_msg->op, item))) {
			cf_warning(AS_SMD, "failed to perform the default accept replace local metadata operation %s (rv %d) for item %d: module \"%s\" ; key \"%s\" ; value \"%s\"",
					   AS_SMD_MSG_OP_NAME(smd_msg->op), retval, i, item->module_name, item->key, item->value);
		}
	}

	// Accept the metadata item list for this module.
	if (module_obj->accept_cb) {
		// Invoke the module's registered accept policy callback function.
		cf_debug(AS_SMD, "Calling accept callback with OPT_MERGE for module %s with nitems %d", smd_msg->items->module_name, smd_msg->items->num_items);
		(module_obj->accept_cb)(module_obj->module, smd_msg->items, module_obj->accept_udata, smd_msg->options);
	}

	// Persist the accepted metadata for this module.
	if (as_smd_module_persist(module_obj)) {
		cf_warning(AS_SMD, "failed to persist accepted metadata for module \"%s\"", module_obj->module);
	}

	return retval;
}

/*
 *  Receive a remote node's metadata and store it locally.
 */
static int as_smd_receive_nodes_metadata(as_smd_t *smd, as_smd_module_t *module_obj, as_smd_msg_t *smd_msg)
{
	int retval = 0;

	as_smd_item_t *item = smd_msg->items->item[0]; // (Only log the fist item.)
	cf_debug(AS_SMD, "System Metadata thread - receiving node %016lX's metadata: item 0: module \"%s\" ; key \"%s\" ; value \"%s\"",
			 smd_msg->node_id, module_obj->module, item->key, item->value);

	// NB: The node ID in the msg may not be the expected node!! 
	return retval;
}

/* Struct to be passed as udata for shash reduce in
 * majority consensus merge policy
 */
typedef struct as_smd_merge_info_s {
	int num_list;
	as_smd_item_list_t *merge_list;
} as_smd_merge_info_t;

static int as_smd_merge_resolution_reduce_fn(void *key, void *data, void *udata)
{
	as_smd_item_freq_t **item_freq = (as_smd_item_freq_t **) data;
	as_smd_item_freq_t *itfq = (*item_freq);

	as_smd_merge_info_t *list = (as_smd_merge_info_t *) udata;

	// If majority of nodes(half or more number of the nodes in the cluster contains
	// this metadata accept it.
	cf_debug(AS_SMD, "Key %s and module %s", (itfq)->item->key, (itfq)->item->module_name);
	if (itfq->freq >= (list->num_list + 1) / 2) {
		cf_debug(AS_SMD, "Item freq %d", (itfq)->freq);

		list->merge_list->item[list->merge_list->num_items] = itfq->item;
		list->merge_list->num_items++;
		cf_debug(AS_SMD, "Num items in the merged list %d", list->merge_list->num_items);
	} else {
		cf_debug(AS_SMD, " key %s in the module %s is dropped after merge", (itfq)->item->key, (itfq)->item->module_name);
	}
	cf_free(*item_freq);

	return 0;
}

static void incr_item_frequency_shash_update(void *key, void *value_old, void *value_new, void *udata)
{
	//to count the frequency of the item.
	as_smd_item_freq_t **item_freq = (as_smd_item_freq_t **) value_old;
	(*item_freq)->freq++;
	value_new = value_old;
}

/*
 * Majority Consensus Merge Policy in SMD.
 * Algorithm      : "Agree upon majority consensus in the cluster."
 *                : If a metadata item is present in exactly half or more the number
 *                  nodes in the cluster , accept it as final truth.
 * Implementation : Hash all the unique metadata items based on the value(which is metadata defn itself).
 *                  why value?.. item's Key alone is not sufficient to decide the uniqueness of a metadata defn
 *                  (e.g., SINDEX module)
 *                  Count the frequency of each item.
 *                  If frequency of a given item is equal to or greater than half of the
 *                  cluster size, accept the given metadata item.
 * Parameter:
 *      module         : Module(SINDEX/UDF) for which this merge is being executed
 *      merged_list    : Final list of merged items, after merge resolution
 *      lists_to_merge : list that contains a list of metadata items in each node
 *      cluster_size   : No. of active nodes in the cluster
 *      udata          : User specific data for callback
 */
int as_smd_majority_consensus_merge(char *module, as_smd_item_list_t **merged_list,
									as_smd_item_list_t **lists_to_merge, size_t num_list, void *udata)
{
	cf_debug(AS_SMD, "Executing majority consensus merge policy for module %s ", module);

	if (lists_to_merge == NULL) {
		return -1;
	}

	shash *merge_hash = NULL;
	if (SHASH_OK != shash_create(&merge_hash, shash_str_hash_fn, AS_SMD_MAJORITY_CONSENSUS_KEYSIZE, sizeof(as_smd_item_freq_t *), 17, SHASH_CR_MT_BIGLOCK)) {
		cf_crash(AS_SMD, "Memory allocation for hash during merge resolution, failed ");
	}

	// Traverse through the set of list containing metadata items
	for(int i = 0; i < num_list; i++) {
		// Traverse through the list of metadata items from every node
		int nitems = lists_to_merge[i]->num_items;
		cf_debug(AS_SMD, "Number of items %d", nitems);

		for (int j = 0; j < nitems; j++) {
			as_smd_item_freq_t * item_freq;
			as_smd_item_t *curitem = lists_to_merge[i]->item[j];

			// Note: The value (not key) of the metadata item is used as the key in merge_hash.
			char  key[AS_SMD_MAJORITY_CONSENSUS_KEYSIZE] = {"\0"};
			if (!(curitem->value) || !(strlen(curitem->value))) {
				cf_warning(AS_SMD, "In SMD module \"%s\", in metadata item list %d from node %016lX, item %d, key \"%s\" has an invalid NULL value ~~ skipping it for consideration during merge resolution", curitem->module_name, i, curitem->node_id, j, curitem->key);
				continue;
			}
			int keylen = strlen(curitem->value);
			if (keylen > AS_SMD_MAJORITY_CONSENSUS_KEYSIZE) {
				cf_warning(AS_SMD, "Metadata item from module %s with key %s is not considered for merge resolution as the key is too large (%d)", curitem->module_name, curitem->value, keylen);
				continue;
			}

			strncpy(key, curitem->value, keylen);
			// If the item is already present in the hash, increment the frequency of the item.
			if (SHASH_OK == shash_get(merge_hash, key, (void *) (&item_freq))) {
				cf_debug(AS_SMD, "Item found %s", item_freq->item->value);
				as_smd_item_freq_t* new_item_freq = item_freq;
				shash_update(merge_hash, key, (void *) (&item_freq), (void *) (&new_item_freq), incr_item_frequency_shash_update, NULL);
			} else {
				// Otherwise put the item in the hash.
				item_freq = cf_malloc(sizeof(as_smd_item_freq_t));
				item_freq->item = curitem;
				item_freq->freq = 1;

				if (SHASH_OK != shash_put_unique(merge_hash, key, (void *) (&item_freq))) {
					cf_warning(AS_SMD, "Metadata item from module %s with key %s is not considered for merge resolution", item_freq->item->module_name, item_freq->item->value);
				}
				cf_debug(AS_SMD, "Put into the hash %s", item_freq->item->value);
			}
		}
	}
	int num_items = shash_get_size(merge_hash);
	// Pass necessary information to shash_reduce function, so that it can either
	// accept that item or reject it.
	as_smd_merge_info_t merge_info;
	merge_info.num_list = num_list;
	if (*merged_list == NULL) {
		(*merged_list) = as_smd_item_list_alloc(num_items);
		if (num_list > 0 && (lists_to_merge[0] != NULL) && (lists_to_merge[0]->module_name != NULL)) {
			(*merged_list)->module_name = cf_strdup(lists_to_merge[0]->module_name);
		}
	}
	merge_info.merge_list = (*merged_list);
	merge_info.merge_list->num_items = 0;

	// Shash reduce to traverse each element in the hash,
	// decide if it is the final accepted metadata item,
	// and delete the content of structure the pointer is pointing to.
	shash_reduce(merge_hash, as_smd_merge_resolution_reduce_fn, (void *) &merge_info);
	shash_destroy(merge_hash);
	cf_debug(AS_SMD, "Majority consensus merge policy execution complete!");

	return 0;
}

/*
 *  Process an SMD event, which may be either an SMD API command or an incoming SMD fabric msg.
 */
static void as_smd_process_event (as_smd_t *smd, as_smd_event_t *evt)
{
	if (AS_SMD_CMD == evt->type) {

		/***** Handle SMD API Command Event *****/

		as_smd_cmd_t *cmd = &(evt->u.cmd);

		cf_debug(AS_SMD, "SMD thread received command: \"%s\" ; options: 0x%08x", AS_SMD_CMD_TYPE_NAME(cmd->type), cmd->options);

		if (cmd->item) {
			cf_debug(AS_SMD, "SMD event item: node %016lX ; module \"%s\" ; key \"%s\" ; value %p ; generation %u ; timestamp %zu",
					 cmd->item->node_id, cmd->item->module_name, cmd->item->key, cmd->item->value, cmd->item->generation, cmd->item->timestamp);
		}

		switch (cmd->type) {
			case AS_SMD_CMD_INIT:
				smd->state = AS_SMD_STATE_INITIALIZED;
				break;

			case AS_SMD_CMD_START:
				smd->state = AS_SMD_STATE_RUNNING;
				break;

			case AS_SMD_CMD_CREATE_MODULE:
				as_smd_module_create(smd, cmd);
				break;

			case AS_SMD_CMD_DESTROY_MODULE:
				as_smd_module_destroy(smd, cmd);
				break;

			case AS_SMD_CMD_SET_METADATA:
			case AS_SMD_CMD_DELETE_METADATA:
				as_smd_metadata_change(smd, CMD_TYPE2MSG_OP(cmd->type), cmd->item);
				break;

			case AS_SMD_CMD_GET_METADATA:
				as_smd_metadata_get(smd, cmd);
				break;

			case AS_SMD_CMD_INTERNAL:
				if (cmd->options & AS_SMD_CMD_OPT_DUMP_SMD) {
					as_smd_dump_metadata(smd, cmd);
				} else if (cmd->options & AS_SMD_CMD_OPT_PAXOS_CHANGED) {
					as_smd_paxos_changed(smd, cmd);
				} else {
					cf_warning(AS_SMD, "Unknown System Metadata internal event options received: 0x%08x ~~ Ignoring event!", cmd->options);
				}
				break;

			case AS_SMD_CMD_SHUTDOWN:
				smd->state = AS_SMD_STATE_EXITING;
				break;

			default:
				cf_crash(AS_SMD, "received unknown System Metadata event type %d", cmd->type);
				break;
		}
	} else if (AS_SMD_MSG == evt->type) {

		/***** Handle SMD Fabric Transaction Message Event *****/

		as_smd_msg_t *msg = &(evt->u.msg);
		as_smd_item_t *item = NULL;

		if (msg->num_items) {
			item = msg->items->item[0]; // (Only log the fist item.)
			cf_debug(AS_SMD, "SMD thread received fabric msg event with op %s item: item 0: node %016lX module \"%s\" ; key \"%s\" ; value \"%s\"",
					 AS_SMD_MSG_OP_NAME(msg->op), item->node_id, item->module_name, item->key, item->value);
		} else {
			cf_debug(AS_SMD, "SMD thread received fabric msg event with op %s [Zero metadata items]", AS_SMD_MSG_OP_NAME(msg->op));
			if ((AS_SMD_MSG_OP_SET_ITEM == msg->op) || (AS_SMD_MSG_OP_DELETE_ITEM == msg->op)) {
				cf_crash(AS_SMD, "SMD thread received invalid empty metadata items list from node %016lX for message %s",
						 msg->node_id, AS_SMD_MSG_OP_NAME(msg->op));
			}
		}

		// Find (or create) the module's object.
		as_smd_module_t *module_obj = as_smd_module_get(smd, (msg->num_items > 0 ? msg->items->item[0] : NULL), msg->items, msg);

		switch (msg->op) {
			case AS_SMD_MSG_OP_SET_ITEM:
			case AS_SMD_MSG_OP_DELETE_ITEM:
				as_smd_apply_metadata_change(smd, module_obj, msg);
				break;

			case AS_SMD_MSG_OP_MY_CURRENT_METADATA:
				as_smd_receive_metadata(smd, msg);
				break;

			case AS_SMD_MSG_OP_ACCEPT_THIS_METADATA:
				as_smd_accept_metadata(smd, module_obj, msg);
				break;

			case AS_SMD_MSG_OP_NODES_CURRENT_METADATA:
				as_smd_receive_nodes_metadata(smd, module_obj, msg);
				break;
		}

		if (module_obj) {
			// Give back the reference.
			cf_rc_release(module_obj);
		}
	} else {
		// This should never happen.
		cf_warning(AS_SMD, "received unknown type of System Metadata event (%d)", evt->type);
	}
}

/*
 *  Thread to handle all System Metadata events, incoming via the API or the fabric.
 */
void *as_smd_thr(void *arg)
{
	as_smd_t *smd = (as_smd_t *) arg;
	int retval = 0;

	cf_debug(AS_SMD, "System Metadata thread created");

	// Receive incoming messages via the message queue.
	// Process each message.
	// Destroy the message after processing.

	for ( ; smd->state != AS_SMD_STATE_EXITING ; ) {

		as_smd_event_t *evt = NULL;

		if ((retval = cf_queue_pop(smd->msgq, &evt, AS_SMD_WAIT_INTERVAL_MS))) {
			if (CF_QUEUE_ERR == retval) {
				cf_warning(AS_SMD, "failed to pop an event (retval %d)", retval);
			}
		}

		if (CF_QUEUE_EMPTY == retval) {
			// [Could handle any periodic / background events here when there's nothing else to do.]
			cf_detail(AS_SMD, "System Metadata thread - received timeout event");
		} else {
			as_smd_process_event(smd, evt);

#ifdef DEBUG
			rchash_dump(smd->modules);
#endif

			// Release the event message.
			as_smd_destroy_event(evt);
		}
	}

	// Release System Metadata resources.
	as_smd_terminate(smd);

	// Exit the System Metadata thread.
	return NULL;
}
