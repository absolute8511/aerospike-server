/*
 * thr_sindex.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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
 * secondary index function declarations
 */

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include "queue.h"
#include "ai_obj.h"
#include "hist.h"
#include "citrusleaf/cf_digest.h"

#include "base/datamodel.h"

#define SET_TIME_FOR_SINDEX_GC_HIST(start_time)                                           \
do {                                                                                      \
	if (g_config.sindex_gc_enable_histogram) {                                            \
		start_time = cf_getns();                                                          \
	}                                                                                     \
} while(0);

#define SINDEX_GC_HIST_INSERT_DATA_POINT(type, start_time_ns)                             \
do {                                                                                      \
	if (start_time_ns != 0 && g_config.sindex_gc_enable_histogram && g_config._ ##type) { \
		histogram_insert_data_point(g_config._ ##type, start_time_ns);                    \
	}                                                                                     \
} while(0);


#define SINDEX_GC_QUEUE_HIGHWATER  10
#define SINDEX_GC_NUM_OBJS_PER_ARR 20

typedef struct acol_digest_t {
	cf_digest dig;
	ai_obj    acol;
} acol_digest;

typedef struct objs_to_defrag_arr_t {
	acol_digest acol_digs[SINDEX_GC_NUM_OBJS_PER_ARR];
	uint32_t    num;
} objs_to_defrag_arr;

typedef struct ll_sindex_gc_element_s {
	cf_ll_element        ele;
	objs_to_defrag_arr * objs_to_defrag;
} ll_sindex_gc_element;

extern pthread_rwlock_t sindex_rwlock;
extern cf_queue *g_sindex_populate_q;
extern cf_queue *g_sindex_destroy_q;
extern cf_queue *g_sindex_populateall_done_q;
extern bool      g_sindex_boot_done;

void as_sindex_thr_init();
void as_sindex_gc_histogram_dumpall();
objs_to_defrag_arr * as_sindex_gc_get_defrag_arr(void);

#define MAX_POPULATOR_THREADS 32

void as_spop_init();
int as_spop_populate_all(as_namespace* ns);
void as_spop_resize_thread_pool(uint32_t n_threads);
