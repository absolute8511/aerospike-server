/*
 * alloc.c
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

#include <citrusleaf/alloc.h>

#include <dlfcn.h>
#include <pthread.h>
#include <malloc.h>		// for mallinfo()
#include <math.h>		// for exp2()
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/param.h>	// for MIN()
#ifdef USE_DF_DETECT
#include <sys/syscall.h>
#endif

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_shash.h>
#include <citrusleaf/cf_types.h> // for byte

#include "fault.h"


// #define USE_CIRCUS 1

// #define EXTRA_CHECKS 1

/*
 *  Alignment size for aligned allocations.
 *  (Note:  While 512 works for reading from SSDs in anecdotal laboratory test conditions,
 *           we're retaining the original page size alignment.)
 */
#define VALLOC_SZ    (4096)

// Define this to halt when a memory accounting inconsistency is detected.
// (Otherwise, simply log the occurrence and keep going.)
// #define STRICT_MEMORY_ACCOUNTING 1

void *   (*g_malloc_fn) (size_t s) = 0;
int      (*g_posix_memalign_fn) (void **memptr, size_t alignment, size_t sz);
void     (*g_free_fn) (void *p) = 0;
void *   (*g_calloc_fn) (size_t nmemb, size_t sz) = 0;
void *   (*g_realloc_fn) (void *p, size_t sz) = 0;
char *   (*g_strdup_fn) (const char *s) = 0;
char *   (*g_strndup_fn) (const char *s, size_t n) = 0;


#ifdef MEM_COUNT

#include "dynbuf.h"

/*
 *  Account for memory by using the actual size allocated, rather than the requested allocation size.
 */
#define USE_MALLOC_USABLE_SIZE

struct shash_s *mem_count_shash = NULL;
cf_atomic64 mem_count = 0;
cf_atomic64 mem_count_mallocs = 0;
cf_atomic64 mem_count_frees = 0;
cf_atomic64 suppressed_free_warnings = 0;
cf_atomic64 mem_count_callocs = 0;
cf_atomic64 mem_count_reallocs = 0;
cf_atomic64 mem_count_strdups = 0;
cf_atomic64 mem_count_strndups = 0;
cf_atomic64 mem_count_vallocs = 0;

cf_atomic64 mem_count_malloc_total = 0;
cf_atomic64 mem_count_free_total = 0;
cf_atomic64 mem_count_calloc_total = 0;
cf_atomic64 mem_count_realloc_plus_total = 0;
cf_atomic64 mem_count_realloc_minus_total = 0;
cf_atomic64 mem_count_strdup_total = 0;
cf_atomic64 mem_count_strndup_total = 0;
cf_atomic64 mem_count_valloc_total = 0;

/*
 * Maximum length of a string representing a location in the program.
 */
#define MAX_LOCATION_LEN  100

/*
 * Type representing a location in the program, a string of the form:  "<Filename>:<LineNumber>".
 */
typedef char location_t[MAX_LOCATION_LEN];

/*
 * Type of allocation operation being performed.
 */
typedef enum alloc_type_enum
{
	CF_ALLOC_TYPE_CALLOC,
	CF_ALLOC_TYPE_MALLOC,
	CF_ALLOC_TYPE_REALLOC,
	CF_ALLOC_TYPE_VALLOC,
	CF_ALLOC_TYPE_FREE,
	CF_ALLOC_TYPE_STRDUP,
	CF_ALLOC_TYPE_STRNDUP
} alloc_type;

/*
 * Type representing the location in the program and
 *  size in bytes of a memory allocation by "{c,m,re,v}alloc()".
 */
typedef struct alloc_loc_s {
	location_t loc;               // Location of memory allocation in the program.
	alloc_type type;              // Type of the allocation.
	size_t sz;                    // Size in bytes of the allocation.
} alloc_loc_t;

/*
 * Type representing cumulative information about allocations happening at a given location in the program.
 */
typedef struct alloc_info_s {
	size_t net_sz;                // Net allocation in bytes at this program location.
	ssize_t delta_sz;             // Last change in net allocation in bytes at this program location.
	size_t net_alloc_count;       // Net number of allocations at this program location.
	size_t total_alloc_count;     // Total number of allocations at this program location.
	time_t time_last_modified;    // Time of last net allocation change at this program location.
	// TODO: Including this field here is a total hack, since it's only used for report generation!
	// (Wasting the space in the accounting records could be avoided if a different record type was used for reporting.)
	location_t loc;               // Location of memory allocation in the program.
} alloc_info_t;

/*
 * Is memory accounting enabled?
 */
static bool g_memory_accounting_enabled = false;

/*
 * Are warning log messages about unmatched "free()"s to be suppressed?
 * (Useful to prevent log spamming when using run-time enabled memory accounting.)
 */
static bool g_suppress_free_warnings = false;

/*
 * Hash table mapping pointers to the location and size of allocation.
 */
static shash *ptr2loc_shash = NULL;

/*
 * Hash table mapping allocation locations to the allocation info.
 */
static shash *loc2alloc_shash = NULL;

/*
 * Lock to serialize memory counting.
 */
pthread_mutex_t mem_count_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Forward references.
 */
static void make_location(location_t *loc, char *file, int line);
static void copy_location(location_t *loc_out, location_t *loc_in);

/********************************************************************************/

/*
 *  Enable wrappers for memory allocation functions for light-weight memory accounting.
 */
//#define WRAP_MALLOC

#ifdef WRAP_MALLOC

#include <execinfo.h>

/*
 *  Enable logging of unusually-large memory allocations.
 */
//#define WARN_ON_HUGE_ALLOCS

/*
 *  Minimum size of (re-)allocation to alert about.
 */
#define CF_SIZE_TO_WARN_ON  (10 * 1024 * 1024) // 10 MB

/*
 *  Enable debug printouts.
 */
//#define DEBUG

/*
 *  Define macro to control printouts
 */
#ifdef DEBUG
#define dfprintf fprintf
#else
#define dfprintf if (false) fprintf
#endif

/*
 *  Are the memory management functions be wrapped?
 */
static const bool g_wrap_malloc = true;

// (Forward references are necessary for the renamed original functions.)
void * __real_calloc(size_t nmemb, size_t size);
void *__real_malloc(size_t size);
void __real_free(void *ptr);
void *__real_realloc(void *ptr, size_t size);
char *__real_strdup(const char *s);
char *__real_strndup(const char *s, size_t n);
int __real_posix_memalign(void **memptr, size_t alignment, size_t size);

#ifdef WARN_ON_HUGE_ALLOCS
/*
 *  Print the current backtrace.
 */
void bt(void)
{
	void *bts[CF_FAULT_BACKTRACE_DEPTH];
	int btn = backtrace(bts, CF_FAULT_BACKTRACE_DEPTH);
	char **btstr = backtrace_symbols(bts, btn);
	if (!btstr) {
		fprintf(stderr, "***No Backtrace!!!***\n");
		fflush(stdout);
	} else {
		for (int i = 0; i < btn; i++) {
			fprintf(stderr, "Backtrace Frame [%d]: %s\n", i, btstr[i]);
		}
		free(btstr);
	}
}
#endif

/*
 *  Wrapper function for "calloc(3)".
 */
void *__wrap_calloc(size_t nmemb, size_t size)
{
	cf_atomic64_incr(&mem_count_callocs);
	dfprintf(stderr, "called calloc(%zu, %zu)\n", nmemb, size);

	void *retval = __real_calloc(nmemb, size);
	cf_atomic64_add(&mem_count, malloc_usable_size(retval));
	cf_atomic64_add(&mem_count_calloc_total, malloc_usable_size(retval));

#ifdef WARN_ON_HUGE_ALLOCS
	if ((nmemb * size > CF_SIZE_TO_WARN_ON) || (nmemb > CF_SIZE_TO_WARN_ON) || (size > CF_SIZE_TO_WARN_ON) ) {
		cf_warning(CF_ALLOC, "HUGE SIZE ALLOCATION:  calloc(%zu, %zu)", nmemb, size);
		fprintf(stderr, "HUGE SIZE ALLOCATION:  calloc(%zu, %zu)\n", nmemb, size);
		bt();
	}
#endif

	return retval;
}

/*
 *  Wrapper function for "malloc(3)".
 */
void *__wrap_malloc(size_t size)
{
	cf_atomic64_incr(&mem_count_mallocs);
	dfprintf(stderr, "called malloc(%zu)\n", size);

	void *retval = __real_malloc(size);
	cf_atomic64_add(&mem_count, malloc_usable_size(retval));
	cf_atomic64_add(&mem_count_malloc_total, malloc_usable_size(retval));

#ifdef WARN_ON_HUGE_ALLOCS
	if (size > CF_SIZE_TO_WARN_ON) {
		cf_warning(CF_ALLOC, "HUGE SIZE ALLOCATION:  malloc(%zu)", size);
		fprintf(stderr, "HUGE SIZE ALLOCATION:  malloc(%zu)\n", size);
		bt();
	}
#endif

	return retval;
}

/*
 *  Wrapper function for "free(3)".
 */
void __wrap_free(void *ptr)
{
	// Only count non-"free(0)"'s.
	if (ptr) {
		cf_atomic64_incr(&mem_count_frees);
		dfprintf(stderr, "called free(%p)\n", ptr);

		cf_atomic64_add(&mem_count, - malloc_usable_size(ptr));
		cf_atomic64_add(&mem_count_free_total, - malloc_usable_size(ptr));
	}
	__real_free(ptr);
}

/*
 *  Wrapper function for "realloc(3)".
 */
void *__wrap_realloc(void *ptr, size_t size)
{
	cf_atomic64_incr(&mem_count_reallocs);
	dfprintf(stderr, "called realloc(%p, %zu)\n", ptr, size);

	int64_t orig_size = (ptr ? malloc_usable_size(ptr) : 0);
	int64_t	delta = 0;

	void *retval = __real_realloc(ptr, size);

	if (!size) {
		delta = - orig_size;
	} else {
		// [Note:  If realloc() fails, NULL is returned and the original block is left unchanged.]
		if (retval) {
			delta = malloc_usable_size(retval) - orig_size;
		}
	}

	cf_atomic64_add(&mem_count, delta);
	if (delta > 0) {
		cf_atomic64_add(&mem_count_realloc_plus_total, delta);
	} else {
		cf_atomic64_add(&mem_count_realloc_minus_total, delta);
	}

	if (!ptr) {
		cf_atomic64_incr(&mem_count_mallocs);
	} else if (!size) {
		cf_atomic64_incr(&mem_count_frees);
	} else {
		cf_atomic64_incr(&mem_count_frees);
		cf_atomic64_incr(&mem_count_mallocs);
	}

#ifdef WARN_ON_HUGE_ALLOCS
	if (size > CF_SIZE_TO_WARN_ON) {
		cf_warning(CF_ALLOC, "HUGE SIZE ALLOCATION:  realloc(%p, %zu)", ptr, size);
		fprintf(stderr, "HUGE SIZE ALLOCATION:  realloc(%p, %zu)\n", ptr, size);
		bt();
	}
#endif

	return retval;
}

/*
 *  Wrapper function for "strdup(3)".
 */
char *__wrap_strdup(const char *s)
{
	cf_atomic64_incr(&mem_count_strdups);
	dfprintf(stderr, "called strdup(\"%s\")\n", s);

	char *retval = __real_strdup(s);
	// NOTE:  Calls "malloc()" internally ~~ don't double-count.
//	cf_atomic64_add(&mem_count, malloc_usable_size(retval));
	cf_atomic64_add(&mem_count_strdup_total, malloc_usable_size(retval));

	return retval;
}

/*
 *  Wrapper function for "strndup(3)".
 */
char *__wrap_strndup(const char *s, size_t n)
{
	cf_atomic64_incr(&mem_count_strndups);
	dfprintf(stderr, "called strndup(\"%s\", %zu)\n", s, n);

	char *retval = __real_strndup(s, n);
	// Note:  Calls "malloc()" internally ~~ don't double-count.
//	cf_atomic64_add(&mem_count, malloc_usable_size(retval));
	cf_atomic64_add(&mem_count_strndup_total, malloc_usable_size(retval));

	return retval;
}

/*
 *  Wrapper function for "posix_memalign(3)".
 */
int __wrap_posix_memalign(void **memptr, size_t alignment, size_t size)
{

	cf_atomic64_incr(&mem_count_vallocs);
	dfprintf(stderr, "called posix_memalign(%p, %zu, %zu)\n", memptr, alignment, size);

	int retval = __real_posix_memalign(memptr, alignment, size);
	if (memptr) {
		cf_atomic64_add(&mem_count, malloc_usable_size(*memptr));
		cf_atomic64_add(&mem_count_valloc_total, malloc_usable_size(*memptr));
	}

	return retval;
}

#else // !defined(WRAP_MALLOC)

/*
 *  Are the memory management functions be wrapped?
 */
static const bool g_wrap_malloc = false;

#ifdef USE_ASM

#ifndef PREPRO
#include "mallocations.h"
#endif

void (*g_mallocation_set)(uint16_t type, uint16_t loc, ssize_t delta_size) = NULL;
void (*g_mallocation_get)(uint16_t *type, uint16_t loc, ssize_t *total_size, ssize_t *delta_size, struct timespec *last_time) = NULL;

// (Forward reference.)
static void get_human_readable_memory_size(ssize_t sz, double *quantity, char **scale);

/*
 *  Callback function to log messages from the library.
 */
void my_cb(uint64_t thread_id, uint16_t type, uint16_t loc, ssize_t delta_size, ssize_t total_size, struct timespec *last_time, void *udata)
{
	as_mallocation_t *asm_array = (as_mallocation_t *) udata, *asm_loc = &(asm_array[loc]);

	asm_loc->total_size += delta_size;

#if 1
	double quantity = 0.0;
	char *scale = "B";
	get_human_readable_memory_size(asm_loc->total_size, &quantity, &scale);
	fprintf(stderr, "my_cb(): thread %lu ; type %d ; loc %d (%s:%d); delta_size %ld ; total_size %ld (%.3f %s)\n",
			thread_id, type, loc, mallocations[loc].file, mallocations[loc].line, delta_size, asm_loc->total_size, quantity, scale);
	cf_warning(CF_ALLOC, "my_cb(): thread %lu ; type %d ; loc %d (%s:%d); delta_size %ld ; total_size %ld (%.3f %s)",
			   thread_id, type, loc, mallocations[loc].file, mallocations[loc].line, delta_size, asm_loc->total_size, quantity, scale);
#else
	// Alternative output format showing last time instead of human-readable size.
	fprintf(stderr, "my_cb(): thread %lu ; type %d ; loc %d (%s:%d); delta_size %ld ; total_size %ld ; last_time %lu.%09lu\n",
			thread_id, type, loc, mallocations[loc].file, mallocations[loc].line, delta_size, total_size, last_time->tv_sec, last_time->tv_nsec);
	cf_warning(CF_ALLOC, "my_cb(): thread %lu ; type %d ; loc %d (%s:%d); delta_size %ld ; total_size %ld ; last_time %lu.%09lu",
			   thread_id, type, loc, mallocations[loc].file, mallocations[loc].line, delta_size, total_size, last_time->tv_sec, last_time->tv_nsec);
#endif
}

/*
 *  Register an immediately-upcoming memory allocation-related function on this thread.
 *
 *  Return 0 if successful, -1 otherwise.
 *
 *  XXX -- Do we have to do anything to guarantee this happens before the library function call?
 */
int mallocation_register(mallocation_type_t type, malloc_loc_t loc, ssize_t delta_size)
{
	int rv = -1;

	if (g_mallocation_set) {
		(g_mallocation_set)((uint16_t) type, (uint16_t) loc, delta_size);
		rv = 0;
	}

	return rv;
}

#ifdef USE_DF_DETECT
/*
 * Double "free()" Detection:
 *
 *   Purpose:
 *   --------
 *
 *   Detect potential heap corruption events caused by calling "free()" twice sequentially on
 *   the same dynamically-allocated memory block pointer.  (The additional per-block bookkeeping
 *   info. is also very useful when examining memory under a debugger.)
 *
 *   Method:
 *   -------
 *
 *   Add a 32-byte "hidden" header before each allocated block of memory(*) with following format:
 *
 *        ptr      ==> [ uint64_t:  ptr ^ HEADER_MASK                          ] : Header Signature
 *        ptr +  8 ==> [ size_t:    original allocation size                   ] : Size of Actual User Data
 *        ptr + 16 ==> [ uint32_t:  allocating TID | uint32_t:  allocating loc ] : Allocator Information
 *        ptr + 24 ==> [ uint32_t:  freeing TID    | uint32_t:  freeing loc    ] : Freer Information
 *    returned ptr ==> [ void *:    ...Actual User Data...                     ] : Actual User Data
 *
 *   where TID is the Linux Task ID of the allocating/freeing thread, and loc is the corresponding mallocation.
 *
 *   The caller (i.e., code outside of this module) only sees the Actual User Data portion of the block.
 *
 *   Whenever a block is (re-)allocated, the header is initialized, setting the Header Signature,
 *    Size of Actual User Data, and Allocator Information, and the Freer Information is initialized to 0.
 *
 *   Whenever a block is freed, first the header is checked:
 *
 *     1). For a matching Header Signature.  (If no match, then we may have detected another form for corruption,
 *          but for now, treat this case "safely" throughout, as if no header exists on this block.)
 *
 *     2). For whether the Freer Information is already set:
 *          A). If so, we have detected a probable double free ~~ Log the damage!!!
 *          B). If not, set the Freer Information corresponding to the present call.
 *
 *     And finally "free()" actually is called:
 *          If this is a double "free()" (or other heap corruption) and JEMalloc debugging is enabled,
 *          JEMalloc may now detect the problem via its own internal consistency checks and abort the program.
 *          Otherwise, a SEGV may be imminent.
 *
 *   (*) Note that in the case of aligned allocations, an extra leading page of "VALLOC_SZ" bytes is allocated
 *        to maintain proper alignment, and the double "free()" detection header is located at the bottom of that page.
 *        Thus, when freeing aligned allocations (detectable via the original mallocation type), the pointer actually
 *        freed must point to the start of the aligned allocation block, rather than to the header.
 *
 *   Dependencies:
 *   -------------
 *
 *   Double "free()" detection is layered upon the ASMalloc infrastructure, which provides for each program
 *   source code location where a dynamic memory allocation-related function is called, a unique, small, positive
 *   integer (a "mallocation") and related utility types and functions.  Thus building with ASMalloc support is
 *   necessary when using double "free()" detection.  (Pre-loading the ASMalloc library at run time, however,
 *   is not required.)
 */

/*
 *  Size of double "free()" detection header (in bytes.)
 */
#define HEADER_SZ    (4 * sizeof(uint64_t))

/*
 *  Double "free()" detection header signature:
 *    This mask is to be XOR'd with the pointer value and stored at the pointer location.
 */
#define HEADER_MASK  (0x0123456789abcdef)

/*
 *  Initialize double "free()" detection block header.
 */
static void *cf_init_header(void *ptr, size_t size, malloc_loc_t loc)
{
	uint64_t *header = (uint64_t *) ptr;

	// Aligned allocations have an extra leading page containing the header at the end.
	if (MALLOCATION_TYPE_VALLOC == mallocations[loc].type) {
		header = (uint64_t *) ((char *) ptr + VALLOC_SZ - HEADER_SZ);
	}

	header[0] = (uint64_t) header ^ HEADER_MASK;
	header[1] = size;
	header[2] = (uint64_t) (syscall(SYS_gettid) << 32) | loc;
	header[3] = 0;

	return (void *) &header[4];
}
#endif

/*
 *  Wrapper for "calloc()" that notes the location.
 */
void *cf_calloc_loc(size_t nmemb, size_t size, malloc_loc_t loc)
{
	mallocation_register(MALLOCATION_TYPE_CALLOC, loc, nmemb * size);

	void *retval = 0;

#ifdef USE_DF_DETECT
	// XXX -- Uses "malloc()" to emulate "calloc()"!
	size_t orig_size = nmemb * size, new_size = orig_size + HEADER_SZ;
	void *ptr = malloc(new_size);
	if (ptr) {
		retval = cf_init_header(ptr, orig_size, loc);
		memset(retval, 0, orig_size);
	}
#else
	retval = calloc(nmemb, size);
#endif

	return retval;
}

/*
 *  Wrapper for "malloc()" that notes the location.
 */
void *cf_malloc_loc(size_t size, malloc_loc_t loc)
{
	mallocation_register(MALLOCATION_TYPE_MALLOC, loc, size);

	void *retval = 0;

#ifdef USE_DF_DETECT
	size_t new_size = size + HEADER_SZ;
	void *ptr = malloc(new_size);
	if (ptr) {
		retval = cf_init_header(ptr, size, loc);
	}
#else
	retval = malloc(size);
#endif

	return retval;
}

/*
 *  Wrapper for "free()" that notes the location.
 */
void cf_free_loc(void *ptr, malloc_loc_t loc)
{
#ifdef USE_DF_DETECT
	if (ptr) {
		char *ptr2 = (char *) ptr - HEADER_SZ;
		uint64_t *header = (uint64_t *) ptr2;

		if (((uint64_t) ptr2 ^ HEADER_MASK) != header[0]) {
			cf_warning(CF_ALLOC, "***Notice:  cfl(%p) can't find DF_DETECT header @ File: \"%s\" Line: %d TID %ld***",
					   ptr, mallocations[loc].file, mallocations[loc].line, syscall(SYS_gettid));
			// XXX -- Treat this case by assuming no header exists....
			PRINT_STACK();
		} else {
			int mloc = header[2] & ~-(1 << 16);

			if (header[3]) {
				// [Note:  The double-doublequote prevents unintended macroexpansion in the format string.]
				cf_warning(CF_ALLOC, "***Detected double cf_""free(%p)!***", ptr);

				size_t msize = (size_t) header[1];
				int mtid = header[2] >> 32;
				cf_warning(CF_ALLOC, "***Original cf_""malloc(%zu) @ File: \"%s\" Line: %d TID: %d***",
						   msize, mallocations[mloc].file, mallocations[mloc].line, mtid);

				int floc = header[3] & ~-(1 << 16);
				int ftid = header[3] >> 32;
				cf_warning(CF_ALLOC, "***First cf_""free(%p) @ File: \"%s\" Line: %d TID %d***",
						   ptr, mallocations[floc].file, mallocations[floc].line, ftid);

				cf_warning(CF_ALLOC, "***Second cf_""free(%p) @ File: \"%s\" Line: %d TID %ld***",
						   ptr, mallocations[loc].file, mallocations[loc].line, syscall(SYS_gettid));

				PRINT_STACK();

			} else {
				header[3] = (uint64_t) (syscall(SYS_gettid) << 32) | loc;
			}

			// Account for the extra leading page containing the header on aligned allocations.
			if (MALLOCATION_TYPE_VALLOC == mallocations[mloc].type) {
				ptr2 = ptr - VALLOC_SZ;
			}

			ptr = ptr2;
		}
	}
#endif

	mallocation_register(MALLOCATION_TYPE_FREE, loc, - malloc_usable_size(ptr));

	free(ptr);
}

/*
 *  Wrapper for "realloc()" that notes the location.
 */
void *cf_realloc_loc(void *ptr, size_t size, malloc_loc_t loc)
{
	void *retval = 0;

#ifdef USE_DF_DETECT
	if (size) {

		mallocation_register(MALLOCATION_TYPE_REALLOC, loc, size);

		size_t new_size = size + HEADER_SZ;
		if (ptr) {
			// A true "realloc()".
			bool found_header = false;
			char *ptr2 = (char *) ptr - HEADER_SZ;
			uint64_t *header = (uint64_t *) ptr2;

			if (((uint64_t) ptr2 ^ HEADER_MASK) != header[0]) {
				cf_warning(CF_ALLOC, "***Notice:  crl(%p) [#1] can't find DF_DETECT header @ File: \"%s\" Line: %d TID %ld***",
						   ptr, mallocations[loc].file, mallocations[loc].line, syscall(SYS_gettid));
				// XXX -- Treat this case by assuming no header exists....
				PRINT_STACK();
			} else {
				// (NB:  Could do more header validation here.)
				found_header = true;
				ptr = ptr2;
			}

			if ((ptr2 = realloc(ptr, new_size))) {
				if (found_header) {
					ptr2 = cf_init_header(ptr2, size, loc);
				}
				retval = ptr2;
			}
		} else {
			// Acts like "malloc()".
			void *ptr2 = realloc(ptr, new_size);
			if (ptr2) {
				retval = cf_init_header(ptr2, size, loc);
			}
		}
	} else {
		// Acts like "free()".
		if (ptr) {
			char *ptr2 = (char *) ptr - HEADER_SZ;
			uint64_t *header = (uint64_t *) ptr2;

			if (((uint64_t) ptr2 ^ HEADER_MASK) != header[0]) {
				cf_warning(CF_ALLOC, "***Notice:  crl(%p) [#2] can't find DF_DETECT header @ File: \"%s\" Line: %d TID %ld***",
						   ptr, mallocations[loc].file, mallocations[loc].line, syscall(SYS_gettid));
				// XXX -- Treat this case by assuming no header exists....
				PRINT_STACK();
			} else {
				int mloc = header[2] & ~-(1 << 16);

				if (header[3]) {
					cf_warning(CF_ALLOC, "***Detected double cf_""free(%p) [actually cf_realloc\(%p, 0)]!***", ptr,ptr);

					size_t msize = (size_t) header[1];
					int mtid = header[2] >> 32;
					cf_warning(CF_ALLOC, "***Original cf_""malloc(%zu) @ File: \"%s\" Line: %d TID: %d***",
							   msize, mallocations[mloc].file, mallocations[mloc].line, mtid);

					int floc = header[3] & ~-(1 << 16);
					int ftid = header[3] >> 32;
					cf_warning(CF_ALLOC, "***First cf_""free(%p) @ File: \"%s\" Line: %d TID %d***",
							   ptr, mallocations[floc].file, mallocations[floc].line, ftid);

					cf_warning(CF_ALLOC, "***Second cf_""free(%p) @ File: \"%s\" Line: %d TID %ld***",
							   ptr, mallocations[loc].file, mallocations[loc].line, syscall(SYS_gettid));

					PRINT_STACK();
				} else {
					header[3] = (uint64_t) (syscall(SYS_gettid) << 32) | loc;
				}

				// Account for the extra leading page containing the header on aligned allocations.
				if (MALLOCATION_TYPE_VALLOC == mallocations[mloc].type) {
					ptr2 = ptr - VALLOC_SZ;
				}

				ptr = ptr2;
			}
		}

		mallocation_register(MALLOCATION_TYPE_REALLOC, loc, - malloc_usable_size(ptr));

		// XXX -- It's possible for "realloc(p, 0)" to return a non-NULL pointer suitable for passing to "free()".
		//        (This case is not currently handled using a header, but ?should? still work, just with a "Notice:"
		//        message and stack being logged by "cf_{free,realloc}_loc()".
		retval = realloc(ptr, size);
	}
#else
	mallocation_register(MALLOCATION_TYPE_REALLOC, loc, (size ? size : - malloc_usable_size(ptr)));

	retval = realloc(ptr, size);
#endif

	return retval;
}

/*
 *  Wrapper for "strdup()" that notes the location.
 */
char *cf_strdup_loc(const char *s, malloc_loc_t loc)
{
	mallocation_register(MALLOCATION_TYPE_STRDUP, loc, strlen(s) + 1);

	char *retval = 0;

#ifdef USE_DF_DETECT
	// XXX -- Uses "malloc()" to emulate "strdup()"!
	size_t size = strlen(s) + 1;
	size_t new_size = size + HEADER_SZ;
	void *ptr = malloc(new_size);
	if (ptr) {
		if ((retval = cf_init_header(ptr, size, loc))) {
			memcpy(retval, s, size - 1);
			* ((char *) retval + size - 1) = '\0';
		}
	}
#else
	// Disable inlining of "strdup()".
	retval = (*(&(strdup)))(s);
#endif

	return retval;
}

/*
 *  Wrapper for "strndup()" that notes the location.
 */
char *cf_strndup_loc(const char *s, size_t n, malloc_loc_t loc)
{
	mallocation_register(MALLOCATION_TYPE_STRNDUP, loc, n);

	char *retval = 0;

#ifdef USE_DF_DETECT
	// XXX -- Uses "malloc()" to emulate "strndup()"!
	size_t size = MIN(strlen(s), n) + 1;
	size_t new_size = size + HEADER_SZ;
	void *ptr = malloc(new_size);
	if (ptr) {
		if ((retval = cf_init_header(ptr, size, loc))) {
			memset(retval, 0, size);
			memcpy(retval, s, size - 1);
			* ((char *) retval + size - 1) = '\0';
		}
	}
#else
	// Disable inlining of "strndup()".
	retval = (*(&(strndup)))(s, n);
#endif

	return retval;
}

/*
 *  Wrapper for "valloc()" that notes the location.
 */
void *cf_valloc_loc(size_t size, malloc_loc_t loc)
{
	mallocation_register(MALLOCATION_TYPE_VALLOC, loc, size);

	void *retval = 0;
	void *ptr = 0;

#ifdef USE_DF_DETECT
	// NB:  Allocate sufficient extra space to ensure proper alignment when including the header.
	size_t new_size = size + VALLOC_SZ;
	retval = (!posix_memalign(&ptr, VALLOC_SZ, new_size) ? ptr : 0);

	if (retval && ptr) {
		retval = cf_init_header(ptr, size, loc);
	}
#else
	retval = (!posix_memalign(&ptr, VALLOC_SZ, size) ? ptr : 0);
#endif

	return retval;
}

/*
 *  Wrapper functions for re-directing wrapped calls to "cf_*()" functions.
 */

/*
 *  Wrapper function for "calloc(3)".
 */
void *__wrap_calloc(size_t nmemb, size_t size)
{
	void *retval = cf_calloc(nmemb, size);

	return retval;
}

/*
 *  Wrapper function for "malloc(3)".
 */
void *__wrap_malloc(size_t size)
{
	void *retval = cf_malloc(size);

	return retval;
}

/*
 *  Wrapper function for "free(3)".
 */
void __wrap_free(void *ptr)
{
	cf_free(ptr);
}

/*
 *  Wrapper function for "realloc(3)".
 */
void *__wrap_realloc(void *ptr, size_t size)
{
	void *retval = cf_realloc(ptr, size);

	return retval;
}

#endif // defined(USE_ASM)

#endif // defined(WRAP_MALLOC)

/********************************************************************************/

#ifdef USE_MALLINFO
/*
 *  Print heap usage statistics.
 *
 *  [Note:  This only describes the main GLibC arena.]
 */
static void
log_mallinfo(void)
{
	struct mallinfo mi = mallinfo();

	cf_info(CF_ALLOC, "struct mallinfo *%p = {", &mi);
	cf_info(CF_ALLOC, "\tarena = %d;\t\t/* non-mmapped space allocated from system */", mi.arena);
	cf_info(CF_ALLOC, "\tordblks = %d;\t\t/* number of free chunks */", mi.ordblks);
	cf_info(CF_ALLOC, "\tsmblks = %d;\t\t/* number of fastbin blocks */ *GLIBC UNUSED*", mi.smblks);
	cf_info(CF_ALLOC, "\thblks = %d;\t\t/* number of mmapped regions */", mi.hblks);
	cf_info(CF_ALLOC, "\thblkhd = %d;\t\t/* space in mmapped regions */", mi.hblkhd);
	cf_info(CF_ALLOC, "\tusmblks = %d;\t\t/* maximum total allocated space */ *GLIBC UNUSED*", mi.usmblks);
	cf_info(CF_ALLOC, "\tfsmblks = %d;\t\t/* space available in freed fastbin blocks */ *GLIBC UNUSED*", mi.fsmblks);
	cf_info(CF_ALLOC, "\tuordblks = %d;\t\t/* total allocated space */", mi.uordblks);
	cf_info(CF_ALLOC, "\tfordblks = %d;\t\t/* total free space */", mi.fordblks);
	cf_info(CF_ALLOC, "\tkeepcost = %d;\t\t/* top-most, releasable (via malloc_trim) space */", mi.keepcost);
	cf_info(CF_ALLOC, "}");

	size_t total_used = mi.arena + mi.hblkhd;

	cf_info(CF_ALLOC, "total_used: %zu ; diff: %ld", total_used, total_used - mem_count);
}
#endif

/*
 *  Hash function for the loc2alloc shash table.
 */
static uint32_t
location_hash_fn(void *loc)
{
	char *b = (char *) loc;
	uint32_t acc = 0;

	for (int i = 0; i < sizeof(location_t); i++) {
		acc += *b++;
	}

	return acc;
}

/* mem_count_init
 * This function must be called prior to using the memory counting allocation functions. */
int
mem_count_init(mem_count_mode_t mode)
{
	// If enabled, the malloc wrappers take precedence.
	if (g_wrap_malloc) {
		return(0);
	}

	pthread_mutex_lock(&mem_count_lock);

	g_memory_accounting_enabled = (mode != MEM_COUNT_DISABLE);
	g_suppress_free_warnings = (mode != MEM_COUNT_ENABLE);

	if (!g_memory_accounting_enabled || mem_count_shash) {
		cf_debug(CF_ALLOC, "memory accounting %sabled", (!g_memory_accounting_enabled ? "dis" : "en"));
		pthread_mutex_unlock(&mem_count_lock);
		return(-1);
	}

	if (SHASH_OK != shash_create(&mem_count_shash, ptr_hash_fn, sizeof(void *), sizeof(size_t *), 100000, SHASH_CR_MT_MANYLOCK | SHASH_CR_UNTRACKED)) {
		cf_crash(CF_ALLOC, "Failed to allocate mem_count_shash");
	}

	if (SHASH_OK != shash_create(&ptr2loc_shash, ptr_hash_fn, sizeof(void *), sizeof(alloc_loc_t), 100000, SHASH_CR_MT_MANYLOCK | SHASH_CR_UNTRACKED)) {
		cf_crash(CF_ALLOC, "Failed to allocate ptr2loc_shash");
	}

	if (SHASH_OK != shash_create(&loc2alloc_shash, location_hash_fn, sizeof(location_t), sizeof(alloc_info_t), 10000, SHASH_CR_MT_MANYLOCK | SHASH_CR_UNTRACKED)) {
		cf_crash(CF_ALLOC, "Failed to allocate loc2alloc_shash");
	}

	cf_atomic64_set(&mem_count, 0);
	cf_atomic64_set(&mem_count_mallocs, 0);
	cf_atomic64_set(&mem_count_frees, 0);
	cf_atomic64_set(&suppressed_free_warnings, 0);
	cf_atomic64_set(&mem_count_callocs, 0);
	cf_atomic64_set(&mem_count_reallocs, 0);
	cf_atomic64_set(&mem_count_strdups, 0);
	cf_atomic64_set(&mem_count_strndups, 0);
	cf_atomic64_set(&mem_count_vallocs, 0);

	cf_atomic64_set(&mem_count_malloc_total, 0);
	cf_atomic64_set(&mem_count_free_total, 0);
	cf_atomic64_set(&mem_count_calloc_total, 0);
	cf_atomic64_set(&mem_count_realloc_plus_total, 0);
	cf_atomic64_set(&mem_count_realloc_minus_total, 0);
	cf_atomic64_set(&mem_count_strdup_total, 0);
	cf_atomic64_set(&mem_count_strndup_total, 0);
	cf_atomic64_set(&mem_count_valloc_total, 0);

	pthread_mutex_unlock(&mem_count_lock);

	return(0);
}

/* get_human_readable_memory_size
 * Return human-readable value (in terms of floating point powers of 2 ^ 10) for a given memory size. */
static void
get_human_readable_memory_size(ssize_t sz, double *quantity, char **scale)
{
	size_t asz = labs(sz);

	if (asz >= (1 << 30)) {
		*scale = "GB";
		*quantity = ((double) sz) / exp2(30.0);
	} else if (asz >= (1 << 20)) {
		*scale = "MB";
		*quantity = ((double) sz) / exp2(20.0);
	} else if (asz >= (1 << 10)) {
		*scale = "KB";
		*quantity = ((double) sz) / exp2(10.0);
	} else {
		*scale = "B";
		*quantity = (double) sz;
	}
}

/* mem_count_stats
 * Print the current memory allocation statistics. */
void
mem_count_stats()
{
	if (!g_memory_accounting_enabled && !g_wrap_malloc) {
		cf_debug(CF_ALLOC, "memory accounting not enabled");
		return;
	}

	cf_info(CF_ALLOC, "Mem Count Stats:");
	cf_info(CF_ALLOC, "=============================================");

	size_t mc = cf_atomic64_get(mem_count);
	double quantity = 0.0;
	char *scale = "B";
	get_human_readable_memory_size(mc, &quantity, &scale);

	size_t mcm = cf_atomic64_get(mem_count_mallocs);
	size_t mcf = cf_atomic64_get(mem_count_frees);
	size_t mcc = cf_atomic64_get(mem_count_callocs);
	size_t mcr = cf_atomic64_get(mem_count_reallocs);
	size_t mcs = cf_atomic64_get(mem_count_strdups);
	size_t mcsn = cf_atomic64_get(mem_count_strndups);
	size_t mcv = cf_atomic64_get(mem_count_vallocs);

	cf_info(CF_ALLOC, "mem_count: %ld (%.3f %s)", mc, quantity, scale);
	cf_info(CF_ALLOC, "=============================================");
	cf_info(CF_ALLOC, "net mallocs: %ld", (mcm + mcc + mcv + mcs + mcsn - mcf));
	cf_info(CF_ALLOC, "=============================================");
	cf_info(CF_ALLOC, "mem_count_mallocs: %ld (%ld)", mcm, cf_atomic64_get(mem_count_malloc_total));
	if (!g_suppress_free_warnings) {
		cf_info(CF_ALLOC, "mem_count_frees: %ld (%ld)", mcf, cf_atomic64_get(mem_count_free_total));
	} else {
		size_t sfw = cf_atomic64_get(suppressed_free_warnings);
		cf_info(CF_ALLOC, "mem_count_frees: %ld (+%ld)", mcf, sfw);
	}
	cf_info(CF_ALLOC, "mem_count_callocs: %ld (%ld)", mcc, cf_atomic64_get(mem_count_calloc_total));
	cf_info(CF_ALLOC, "mem_count_reallocs: %ld (%ld / %ld)", mcr, cf_atomic64_get(mem_count_realloc_plus_total), cf_atomic64_get(mem_count_realloc_minus_total));
	cf_info(CF_ALLOC, "mem_count_strdups: %ld (%ld)", mcs, cf_atomic64_get(mem_count_strdup_total));
	cf_info(CF_ALLOC, "mem_count_strndups: %ld (%ld)", mcsn, cf_atomic64_get(mem_count_strndup_total));
	cf_info(CF_ALLOC, "mem_count_vallocs: %ld (%ld)", mcv, cf_atomic64_get(mem_count_valloc_total));
	cf_info(CF_ALLOC, "=============================================");

#ifdef USE_MALLINFO
	// Note: -- This only describes the main GLibC arena.
	log_mallinfo();
#endif
}

/* mem_count_alloc_info
 * Lookup the allocation info for a location in the program.
 * (This is the workhorse for the "alloc_info" info command.)
 * Return 0 if successful, -1 otherwise. */
int
mem_count_alloc_info(char *file, int line, cf_dyn_buf *db)
{
	if (!mem_count_shash) {
		cf_info(CF_ALLOC, "memory accounting never enabled ~~ no data collected yet");
		return -1;
	}

	location_t loc;
	alloc_info_t alloc_info;
	make_location(&loc, file, line);

	if (SHASH_OK == shash_get(loc2alloc_shash, &loc, &alloc_info)) {
		double quantity = 0.0;
		char *scale = "B";

		cf_info(CF_ALLOC, "Allocation @ location \"%s\":", loc);
		get_human_readable_memory_size(alloc_info.net_sz, &quantity, &scale);
		cf_info(CF_ALLOC, "\tnet_sz: %zu (%.3f %s)", alloc_info.net_sz, quantity, scale);
		get_human_readable_memory_size(alloc_info.delta_sz, &quantity, &scale);
		cf_info(CF_ALLOC, "\tdelta_sz: %ld (%.3f %s)", alloc_info.delta_sz, quantity, scale);
		cf_info(CF_ALLOC, "\tnet_alloc_count: %zu", alloc_info.net_alloc_count);
		cf_info(CF_ALLOC, "\ttotal_alloc_count: %zu", alloc_info.total_alloc_count);
		struct tm *mod_time_tm = gmtime(&(alloc_info.time_last_modified));
		char time_str[50];
		strftime(time_str, sizeof(time_str), "%b %d %Y %T %Z", mod_time_tm);
		cf_info(CF_ALLOC, "\ttime_last_modified: %ld (%s)", alloc_info.time_last_modified, time_str);
	} else {
		cf_warning(CF_ALLOC, "Cannot find allocation at location: \"%s\".", loc);
		return -1;
	}

	return 0;
}

/*
 * Type representing the different kinds of report records.
 * (Note:  This is a union so we can share the stack allocation.)
 */
typedef union output_u {
	alloc_loc_t *p2l_output;      // The ptr2loc report.
	alloc_info_t *l2a_output;     // The loc2alloc report.
} output_u_t;

/*
 * Type representing a memory allocation count report.
 */
typedef struct mem_count_report_s {
	sort_field_t sort_field;     // Field to sort on (input.)
	int top_n;                   // Number of top entries to return (input.)
	int num_records;             // Number of records of in the report (output.)
	// NB:  Exactly one of the following two report fields will be stack-allocated by the caller:
	output_u_t u;                // The report itself (output.)
} mem_count_report_t;

/* mem_count_report_p2l_reduce_fn
 * This shash reduce function is used to extract the records for the pointer to program location report.
 */
static int
mem_count_report_p2l_reduce_fn(void *key, void *data, void *udata)
{
	alloc_loc_t *alloc_loc = (alloc_loc_t *) data;
	mem_count_report_t *report = (mem_count_report_t *) udata;
	alloc_loc_t *rec = report->u.p2l_output;

	if (alloc_loc->sz > rec[report->top_n - 1].sz) {
		int i = 0;
		while (i < report->num_records) {
			if (alloc_loc->sz > rec[i].sz) {
				memmove(&(rec[i + 1]), &(rec[i]), (report->num_records - i - 1) * sizeof(alloc_loc_t));
				break;
			}
			i++;
		}
		copy_location(&(rec[i].loc), &(alloc_loc->loc));
		rec[i].sz = alloc_loc->sz;
		if (report->num_records < report->top_n) {
			report->num_records++;
		}
	}

	return 0;
}

/* copy_alloc_info
 * Duplicate the values of one allocation info object to another. */
static void
copy_alloc_info(alloc_info_t *to, alloc_info_t *from)
{
	to->net_sz = from->net_sz;
	to->delta_sz = from->delta_sz;
	to->net_alloc_count = from->net_alloc_count;
	to->total_alloc_count = from->total_alloc_count;
	to->time_last_modified = from->time_last_modified;
}

/* mem_count_report_l2a_reduce_fn
 * This shash reduce function is used to extract the records for the program location to allocation info report.
 */
static int
mem_count_report_l2a_reduce_fn(void *key, void *data, void *udata)
{
	location_t *loc = (location_t *) key;
	alloc_info_t *alloc_info = (alloc_info_t *) data;
	mem_count_report_t *report = (mem_count_report_t *) udata;
	alloc_info_t *rec = report->u.l2a_output;

	switch (report->sort_field) {
	  case CF_ALLOC_SORT_NET_SZ:
		  if (alloc_info->net_sz > rec[report->top_n - 1].net_sz) {
			  int i = 0;
			  while (i < report->num_records) {
				  if (alloc_info->net_sz > rec[i].net_sz) {
					  memmove(&(rec[i + 1]), &(rec[i]), (report->num_records - i - 1) * sizeof(alloc_info_t));
					  break;
				  }
				  i++;
			  }
			  copy_location(&(rec[i].loc), loc);
			  copy_alloc_info(&(rec[i]), alloc_info);
			  if (report->num_records < report->top_n) {
				  report->num_records++;
			  }
		  }
		  break;

	  case CF_ALLOC_SORT_TIME_LAST_MODIFIED:
		  if (alloc_info->time_last_modified > rec[report->top_n - 1].time_last_modified) {
			  int i = 0;
			  while (i < report->num_records) {
				  if (alloc_info->time_last_modified > rec[i].time_last_modified) {
					  memmove(&(rec[i + 1]), &(rec[i]), (report->num_records - i - 1) * sizeof(alloc_info_t));
					  break;
				  }
				  i++;
			  }
			  copy_location(&(rec[i].loc), loc);
			  copy_alloc_info(&(rec[i]), alloc_info);
			  if (report->num_records < report->top_n) {
				  report->num_records++;
			  }
		  }
		  break;

	  case CF_ALLOC_SORT_NET_ALLOC_COUNT:
		  if (alloc_info->net_alloc_count > rec[report->top_n - 1].net_alloc_count) {
			  int i = 0;
			  while (i < report->num_records) {
				  if (alloc_info->net_alloc_count > rec[i].net_alloc_count) {
					  memmove(&(rec[i + 1]), &(rec[i]), (report->num_records - i - 1) * sizeof(alloc_info_t));
					  break;
				  }
				  i++;
			  }
			  copy_location(&(rec[i].loc), loc);
			  copy_alloc_info(&(rec[i]), alloc_info);
			  if (report->num_records < report->top_n) {
				  report->num_records++;
			  }
		  }
		  break;

	  case CF_ALLOC_SORT_TOTAL_ALLOC_COUNT:
		  if (alloc_info->total_alloc_count > rec[report->top_n - 1].total_alloc_count) {
			  int i = 0;
			  while (i < report->num_records) {
				  if (alloc_info->total_alloc_count > rec[i].total_alloc_count) {
					  memmove(&(rec[i + 1]), &(rec[i]), (report->num_records - i - 1) * sizeof(alloc_info_t));
					  break;
				  }
				  i++;
			  }
			  copy_location(&(rec[i].loc), loc);
			  copy_alloc_info(&(rec[i]), alloc_info);
			  if (report->num_records < report->top_n) {
				  report->num_records++;
			  }
		  }
		  break;

	  case CF_ALLOC_SORT_DELTA_SZ:
		  if (labs(alloc_info->delta_sz) > labs(rec[report->top_n - 1].delta_sz)) {
			  int i = 0;
			  while (i < report->num_records) {
				  if (labs(alloc_info->delta_sz) > labs(rec[i].delta_sz)) {
					  memmove(&(rec[i + 1]), &(rec[i]), (report->num_records - i - 1) * sizeof(alloc_info_t));
					  break;
				  }
				  i++;
			  }
			  copy_location(&(rec[i].loc), loc);
			  copy_alloc_info(&(rec[i]), alloc_info);
			  if (report->num_records < report->top_n) {
				  report->num_records++;
			  }
		  }
		  break;

	  default:
		  cf_warning(CF_ALLOC, "Unknown mem count report sort field: %d", report->sort_field);
		  return -1;
	}

	return 0;
}

/* mem_count_report
 * Generate and print the memory accounting report selected by sort_field and top_n.
 * (This is the workhorse for the "alloc_info" info command.)
 * Return 0 if successful, -1 otherwise. */
int
mem_count_report(sort_field_t sort_field, int top_n, cf_dyn_buf *db)
{
	if (!mem_count_shash) {
		cf_info(CF_ALLOC, "memory accounting never enabled ~~ no data collected yet");
		return -1;
	}

	mem_count_report_t report;
	size_t output_u_sz = top_n * MAX(sizeof(alloc_loc_t), sizeof(alloc_info_t));

	cf_debug(CF_ALLOC, "Size of mem count shashes: p2l: %u ; l2a: %u", shash_get_size(ptr2loc_shash), shash_get_size(loc2alloc_shash));

	/* First report: By pointer value. */

	report.sort_field = sort_field;
	report.top_n = top_n;
	if (!(report.u.p2l_output = alloca(output_u_sz))) {
		cf_crash(CF_ALLOC, "failed to alloca(%zu) report.u.p2l_output", output_u_sz);
	}
	memset(report.u.p2l_output, 0, output_u_sz);
	report.num_records = 0;

	shash_reduce(ptr2loc_shash, mem_count_report_p2l_reduce_fn, &report);

	cf_info(CF_ALLOC, "Mem Ptr Size Report:");
	cf_info(CF_ALLOC, "--------------------");
	for (int i = 0; i < MIN(report.num_records, report.top_n); i++) {
		cf_info(CF_ALLOC, "Top %2d:  Location: %-25s sz: %zu", i, report.u.p2l_output[i].loc, report.u.p2l_output[i].sz);
	}
	cf_info(CF_ALLOC, "--------------------");

	/* Second report: By program location. */

	memset(report.u.l2a_output, 0, output_u_sz);
	report.num_records = 0;

	shash_reduce(loc2alloc_shash, mem_count_report_l2a_reduce_fn, &report);

	cf_info(CF_ALLOC, "Mem Loc Count Report: (sorted by %s):", (CF_ALLOC_SORT_NET_SZ == sort_field ? "space" :
																(CF_ALLOC_SORT_DELTA_SZ == sort_field ? "change" :
																 (CF_ALLOC_SORT_NET_ALLOC_COUNT == sort_field ? "net_count" :
																  (CF_ALLOC_SORT_TOTAL_ALLOC_COUNT == sort_field ? "total_count" :
																   (CF_ALLOC_SORT_TIME_LAST_MODIFIED == sort_field ? "time" : "???"))))));
	cf_info(CF_ALLOC, "---------------------");
	for (int i = 0; i < MIN(report.num_records, report.top_n); i++) {
		alloc_info_t *rec = &(report.u.l2a_output[i]);
		cf_info(CF_ALLOC, " Top %2d: Location: %-25s sz: %10zu  dsz: %10ld  na: %6zu  ta: %6zu  tlm: %ld", i, rec->loc, rec->net_sz, rec->delta_sz, rec->net_alloc_count, rec->total_alloc_count, rec->time_last_modified);
	}
	cf_info(CF_ALLOC, "---------------------");

	return 0;
}

/* mem_count_shutdown
 * NB:  This can only be called when no other threads are running. */
void
mem_count_shutdown()
{
	if (!g_memory_accounting_enabled) {
		cf_debug(CF_ALLOC, "memory accounting not enabled");
		return;
	}

	shash_destroy(loc2alloc_shash);
	shash_destroy(ptr2loc_shash);
	shash_destroy(mem_count_shash);
}

/* make_location
 * Combine the file and line number into a program location. */
static void
make_location(location_t *loc, char *file, int line)
{
	memset((char *) loc, 0, sizeof(location_t));
	snprintf((char *) loc, sizeof(location_t), "%s:%d", file, line);
}

/* copy_location
 * Copy one location_t object representing a program location to another. */
static void
copy_location(location_t *loc_out, location_t *loc_in)
{
	memset((char *) loc_out, 0, sizeof(location_t));
	memcpy((char *) loc_out, (char *) loc_in, sizeof(location_t));
}

/* update_alloc_info
 * Update function to combine old and new allocation info for a particular location in the program.
 * The old value will be NULL if the key is not present.
 * The updated new value is the output. */
static void
update_alloc_info(void *key, void *value_old, void *value_new, void *udata)
{
	location_t *loc = (location_t *) key;
	alloc_info_t *alloc_info_old = (alloc_info_t *) value_old;
	alloc_info_t *alloc_info_new = (alloc_info_t *) value_new;

	alloc_info_new->net_sz += (alloc_info_old ? alloc_info_old->net_sz : 0);
	alloc_info_new->net_alloc_count += (alloc_info_old ? alloc_info_old->net_alloc_count : 0);
	alloc_info_new->total_alloc_count += (alloc_info_old ? alloc_info_old->total_alloc_count : 0);
	alloc_info_new->time_last_modified = time(NULL);

	make_location(&(alloc_info_new->loc), "(unused)", 0);

	if (0 > alloc_info_new->net_alloc_count) {
		cf_crash(CF_ALLOC, "allocation count for location \"%s\" just went negative!", loc);
	}
}

/* update_alloc_at_location
 * Internal function to change the memory counting for a location in a file. */
static void
update_alloc_at_location(void *p, size_t sz, alloc_type type, char *file, int line)
{
	alloc_loc_t alloc_loc;

#ifdef USE_MALLOC_USABLE_SIZE
	sz = malloc_usable_size(p);
#endif

	cf_atomic64_add(&mem_count, (CF_ALLOC_TYPE_FREE != type ? sz : - sz));

	location_t *loc = &alloc_loc.loc;
	make_location(loc, file, line);
	alloc_loc.sz = sz;
	alloc_loc.type = type;

	if (CF_ALLOC_TYPE_FREE != type) {
		int rv;
		if (SHASH_OK != (rv = shash_put_unique(ptr2loc_shash, &p, &alloc_loc))) {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not put %p into ptr2loc_shash with sz: %zu loc: \"%s\"", p, sz, loc);
#else
			cf_warning(CF_ALLOC, "Could not put %p into ptr2loc_shash with sz: %zu loc: \"%s\" (rv: %d) [IGNORED]", p, sz, loc, rv);
			if (SHASH_ERR_FOUND == rv) {
				if (SHASH_OK != (rv = shash_get(ptr2loc_shash, &p, &alloc_loc))) {
					cf_warning(CF_ALLOC, "Found existing {loc: \"%s\"; type: %d; sz: %d} @ ptr %p in ptr2loc_shash", alloc_loc.loc, alloc_loc.type, alloc_loc.sz, p);
				} else {
					cf_warning(CF_ALLOC, "Could not find existing allocation @ %p in ptr2loc_shash (rv: %d)", p, rv);
				}
			}
			return;
#endif
		}
	} else {
		if (SHASH_OK != shash_get_and_delete(ptr2loc_shash, &p, &alloc_loc)) {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not get %p from ptr2loc_shash with sz: %zu loc: \"%s\"", p, sz, loc);
#else
			cf_warning(CF_ALLOC, "Could not get %p from ptr2loc_shash with sz: %zu loc: \"%s\" [IGNORED]", p, sz, loc);
			return;
#endif
		}
	}

	// Need to stack-allocate old and new values to pass into the update function.
	alloc_info_t alloc_info_old;
	alloc_info_t alloc_info_new;

	alloc_info_new.net_sz = alloc_info_new.delta_sz = (CF_ALLOC_TYPE_FREE != type ? sz : -sz);
	alloc_info_new.net_alloc_count = (CF_ALLOC_TYPE_FREE != type ? 1 : -1);
	alloc_info_new.total_alloc_count = 1;

	if (SHASH_OK != shash_update(loc2alloc_shash, loc, &alloc_info_old, &alloc_info_new, update_alloc_info, 0)) {
#ifdef STRICT_MEMORY_ACCOUNTING
		cf_crash(CF_ALLOC, "Could not update loc2alloc_shash with sz: %zu loc: \"%s\"", sz, loc);
#else
		cf_warning(CF_ALLOC, "Could not update loc2alloc_shash with sz: %zu loc: \"%s\" [IGNORED]", sz, loc);
#endif
	}
}

void *
cf_malloc_at(size_t sz, char *file, int line)
{
	void *p = malloc(sz);

	if (!g_memory_accounting_enabled) {
		return(p);
	}

	pthread_mutex_lock(&mem_count_lock);
	cf_atomic64_incr(&mem_count_mallocs);

	if (p) {
		int rv;
		if (SHASH_OK == (rv = shash_put_unique(mem_count_shash, &p, &sz))) {
			update_alloc_at_location(p, sz, CF_ALLOC_TYPE_MALLOC, file, line);
		} else {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
			cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d (rv: %d) [IGNORED]", p, sz, file, line, rv);
			if (SHASH_ERR_FOUND == rv) {
				if (SHASH_OK != (rv = shash_get(mem_count_shash, &p, &sz))) {
					cf_warning(CF_ALLOC, "Found existing allocation with sz: %d @ ptr %p in mem_count_shash", sz, p);
				} else {
					cf_warning(CF_ALLOC, "Could not find existing allocation @ %p in mem_count_shash (rv: %d)", p, rv);
				}
			}
#endif
		}
	}

	pthread_mutex_unlock(&mem_count_lock);
	return(p);
}

void
cf_free_at(void *p, char *file, int line)
{
	if (!g_memory_accounting_enabled) {
		free(p);
		return;
	}

	/* Apparently freeing 0 is both being done by our code and permitted in the GLIBC implementation. */
	if (!p) {
		cf_info(CF_ALLOC, "[Ignoring cf_free(0) @ %s:%d!]", file, line);
		return;
	}

	pthread_mutex_lock(&mem_count_lock);

	size_t sz = 0;

	if (SHASH_OK == shash_get_and_delete(mem_count_shash, &p, &sz)) {
		cf_atomic64_incr(&mem_count_frees);
		update_alloc_at_location(p, sz, CF_ALLOC_TYPE_FREE, file, line);
		free(p);
	} else {
#ifdef STRICT_MEMORY_ACCOUNTING
		cf_crash(CF_ALLOC, "Could not find pointer %p in mem_count_shash @ %s:%d", p, file, line);
#else
		if (!g_suppress_free_warnings) {
			cf_warning(CF_ALLOC, "Could not find pointer %p in mem_count_shash @ %s:%d [IGNORED]", p, file, line);
		} else {
			cf_atomic64_incr(&suppressed_free_warnings);
		}
#endif
	}

	pthread_mutex_unlock(&mem_count_lock);
}

void *
cf_calloc_at(size_t nmemb, size_t sz, char *file, int line)
{
	void *p = calloc(nmemb, sz);

	if (!g_memory_accounting_enabled) {
		return(p);
	}

	pthread_mutex_lock(&mem_count_lock);
	cf_atomic64_incr(&mem_count_callocs);

	if (p) {
		if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
			update_alloc_at_location(p, sz, CF_ALLOC_TYPE_CALLOC, file, line);
		} else {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
			cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d [IGNORED]", p, sz, file, line);
#endif
		}
	}

	pthread_mutex_unlock(&mem_count_lock);
	return(p);
}

void *
cf_realloc_at(void *ptr, size_t sz, char *file, int line)
{
	void *p = realloc(ptr, sz);

	if (!g_memory_accounting_enabled) {
		return(p);
	}

	pthread_mutex_lock(&mem_count_lock);
	cf_atomic64_incr(&mem_count_reallocs);

	if (!ptr) {
		// If ptr is NULL, realloc() is equivalent to malloc().
		if (p) {
			if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
				cf_atomic64_incr(&mem_count_mallocs);
				update_alloc_at_location(p, sz, CF_ALLOC_TYPE_MALLOC, file, line);
			} else {
#ifdef STRICT_MEMORY_ACCOUNTING
				cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
				cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d [IGNORED]", p, sz, file, line);
#endif
			}
		}
	} else if (!sz) {
		// if sz is NULL, realloc() is equivalent to free().
		if (SHASH_OK == shash_get_and_delete(mem_count_shash, &ptr, &sz)) {
			cf_atomic64_incr(&mem_count_frees);
			update_alloc_at_location(ptr, sz, CF_ALLOC_TYPE_FREE, file, line);
		} else {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not find pointer %p in mem_count_shash @ %s:%d", p, file, line);
#else
			if (!g_suppress_free_warnings) {
				cf_warning(CF_ALLOC, "Could not find pointer %p in mem_count_shash @ %s:%d [IGNORED]", p, file, line);
			} else {
				cf_atomic64_incr(&suppressed_free_warnings);
			}
#endif
		}
	} else {
		// Otherwise, the old block is freed and a new block of the requested size is allocated.
		if (p) {
			size_t old_sz = 0;

			cf_atomic64_incr(&mem_count_frees);
			cf_atomic64_incr(&mem_count_mallocs);

			if (SHASH_OK == shash_get_and_delete(mem_count_shash, &ptr, &old_sz)) {
				update_alloc_at_location(ptr, old_sz, CF_ALLOC_TYPE_FREE, file, line);
				if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
					update_alloc_at_location(p, sz, CF_ALLOC_TYPE_REALLOC, file, line);
				} else {
#ifdef STRICT_MEMORY_ACCOUNTING
					cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
					cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d [IGNORED]", p, sz, file, line);
#endif
				}
			} else {
#ifdef STRICT_MEMORY_ACCOUNTING
				cf_crash(CF_ALLOC, "Could not find pointer %p in mem_count_shash", p);
#else
				if (!g_suppress_free_warnings) {
					cf_warning(CF_ALLOC, "Could not find pointer %p in mem_count_shash @ %s:%d [IGNORED]", p, file, line);
				} else {
					cf_atomic64_incr(&suppressed_free_warnings);
				}
				if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
					update_alloc_at_location(p, sz, CF_ALLOC_TYPE_REALLOC, file, line);
				} else {
					cf_debug(CF_ALLOC, "Could not put realloc'd pointer %p in mem_count_shash @ %s:%d", p, file, line);
				}
#endif
			}
		}
	}

	pthread_mutex_unlock(&mem_count_lock);
	return(p);
}

void *
cf_strdup_at(const char *s, char *file, int line)
{
#ifdef FORCE_WRAP // WRAP_MALLOC
	// Force compiler not to optimize "strdup()" and call the wrapper function.
	char *(*my_strdup)(const char *s) = strdup;
	void *p = my_strdup(s);
#else
	// Disable inlining of "strdup()".
	void *p = (*(&(strdup)))(s);
//	void *p = strdup(s);
#endif

	if (!g_memory_accounting_enabled) {
		return(p);
	}

	pthread_mutex_lock(&mem_count_lock);

	size_t sz = strlen(s) + 1;

	cf_atomic64_incr(&mem_count_strdups);

	if (p) {
		if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
			update_alloc_at_location(p, sz, CF_ALLOC_TYPE_STRDUP, file, line);
		} else {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
			cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d [IGNORED]", p, sz, file, line);
#endif
		}
	}

	pthread_mutex_unlock(&mem_count_lock);
	return(p);
}

void *
cf_strndup_at(const char *s, size_t n, char *file, int line)
{
#ifdef FORCE_WRAP // WRAP_MALLOC
	// Force compiler not to optimize "strndup()" and call the wrapper function.
	char *(*my_strndup)(const char *s, size_t n) = strndup;
	void *p = my_strndup(s, n);
#else
	// Disable inlining of "strndup()".
	void *p = (*(&(strndup)))(s, n);
//	void *p = strndup(s, n);
#endif

	if (!g_memory_accounting_enabled) {
		return(p);
	}

	pthread_mutex_lock(&mem_count_lock);

	size_t sz = MIN(n, strlen(s)) + 1;

	cf_atomic64_incr(&mem_count_strndups);

	if (p) {
		if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
			update_alloc_at_location(p, sz, CF_ALLOC_TYPE_STRNDUP, file, line);
		} else {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
			cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d [IGNORED]", p, sz, file, line);
#endif
		}
	}

	pthread_mutex_unlock(&mem_count_lock);
	return(p);
}

void *
cf_valloc_at(size_t sz, char *file, int line)
{
	void *p = 0;

	if (!g_memory_accounting_enabled) {
		if (0 == posix_memalign(&p, VALLOC_SZ, sz)) {
			return(p);
		} else {
			return(0);
		}
	}

	pthread_mutex_lock(&mem_count_lock);
	cf_atomic64_incr(&mem_count_vallocs);

	if (0 == posix_memalign(&p, VALLOC_SZ, sz)) {
		if (SHASH_OK == shash_put_unique(mem_count_shash, &p, &sz)) {
			update_alloc_at_location(p, sz, CF_ALLOC_TYPE_VALLOC, file, line);
		} else {
#ifdef STRICT_MEMORY_ACCOUNTING
			cf_crash(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d", p, sz, file, line);
#else
			cf_warning(CF_ALLOC, "Could not add ptr: %p sz: %zu to mem_count_shash @ %s:%d [IGNORED]", p, sz, file, line);
#endif
		}
		pthread_mutex_unlock(&mem_count_lock);
		return(p);
	} else {
#ifdef STRICT_MEMORY_ACCOUNTING
		cf_crash(CF_ALLOC, "posix_memalign() failed to allocate sz: %zu @ %s:%d", sz, file, line);
#else
		cf_warning(CF_ALLOC, "posix_memalign() failed to allocate sz: %zu @ %s:%d [IGNORED]", sz, file, line);
#endif
	}

	pthread_mutex_unlock(&mem_count_lock);
	return(0);
}

#endif  // defined(MEM_COUNT)


int
alloc_function_init(char *so_name)
{
	if (so_name) {
//		void *clib_h = dlopen(so_name, RTLD_LAZY | RTLD_LOCAL );
		void *clib_h = dlopen(so_name, RTLD_NOW | RTLD_GLOBAL );
		if (!clib_h) {
			cf_warning(AS_AS, " WARNING: could not initialize memory subsystem, allocator %s not found",so_name);
			fprintf(stderr, " WARNING: could not initialize memory subsystem, allocator %s not found\n",so_name);
			return(-1);
		}

		g_malloc_fn = dlsym(clib_h, "malloc");
		g_posix_memalign_fn = dlsym(clib_h, "posix_memalign");
		g_free_fn = dlsym(clib_h, "free");
		g_calloc_fn = dlsym(clib_h, "calloc");
		g_realloc_fn = dlsym(clib_h, "realloc");
		g_strdup_fn = dlsym(clib_h, "strdup");
		g_strndup_fn = dlsym(clib_h, "strndup");

		// dlclose(alloc_fn);
	}
	else {
		g_malloc_fn = malloc;
		g_posix_memalign_fn = posix_memalign;
		g_free_fn = free;
		g_calloc_fn = calloc;
		g_realloc_fn = realloc;
		g_strdup_fn = strdup;
		g_strndup_fn = strndup;
	}
	return(0);
}


#ifdef USE_CIRCUS

#define CIRCUS_SIZE (1024 * 1024)

// default is track all
// int cf_alloc_track_sz = 0;

// track something specific in size
int cf_alloc_track_sz = 40;

#define STATE_FREE 1
#define STATE_ALLOC 2
#define STATE_RESERVE 3

char *state_str[] = {0, "free", "alloc", "reserve" };

typedef struct {
	void *ptr;
	char file[16];
	int  line;
	int  state;
} suspect;

typedef struct {
	pthread_mutex_t LOCK;
	int		alloc_sz;
	int		idx;
	suspect s[];

} free_ring;


free_ring *g_free_ring;

void
cf_alloc_register_free(void *p, char *file, int line)
{
	pthread_mutex_lock(&g_free_ring->LOCK);

	int idx = g_free_ring->idx;
	suspect *s = &g_free_ring->s[idx];
	s->ptr = p;
	memcpy(s->file,file,15);
	s->file[15] = 0;
	s->line = line;
	s->state = STATE_FREE;
	idx++;
	g_free_ring->idx = (idx == CIRCUS_SIZE) ? 0 : idx;

//	if (idx == 1024)
//		raise(SIGINT);

	pthread_mutex_unlock(&g_free_ring->LOCK);

}

void
cf_alloc_register_alloc(void *p, char *file, int line)
{
	pthread_mutex_lock(&g_free_ring->LOCK);

	int idx = g_free_ring->idx;
	suspect *s = &g_free_ring->s[idx];
	s->ptr = p;
	memcpy(s->file,file,15);
	s->file[15] = 0;
	s->line = line;
	s->state = STATE_ALLOC;
	idx++;
	g_free_ring->idx = (idx == CIRCUS_SIZE) ? 0 : idx;

//	if (idx == 1024)
//		raise(SIGINT);

	pthread_mutex_unlock(&g_free_ring->LOCK);

}

void
cf_alloc_register_reserve(void *p, char *file, int line)
{
	pthread_mutex_lock(&g_free_ring->LOCK);

	int idx = g_free_ring->idx;
	suspect *s = &g_free_ring->s[idx];
	s->ptr = p;
	memcpy(s->file,file,15);
	s->file[15] = 0;
	s->line = line;
	s->state = STATE_RESERVE;

	idx++;
	g_free_ring->idx = (idx == CIRCUS_SIZE) ? 0 : idx;

//	if (idx == 1024)
//		raise(SIGINT);

	pthread_mutex_unlock(&g_free_ring->LOCK);

}


void
cf_alloc_print_history(void *p, char *file, int line)
{
// log the history out to the log file, good if you're about to crash
	pthread_mutex_lock(&g_free_ring->LOCK);

	cf_info(CF_ALLOC, "--------- p %p history (idx %d) ------------",p, g_free_ring->idx);
	cf_info(CF_ALLOC, "--------- 	  %s %d ------------",file,line);

	for (int i = g_free_ring->idx - 1;i >= 0; i--) {
		if (g_free_ring->s[i].ptr == p) {
			suspect *s = &g_free_ring->s[i];
			cf_info(CF_ALLOC, "%05d : %s %s %d",
				i, state_str[s->state], s->file, s->line);
		}
	}

	for (int i = g_free_ring->alloc_sz - 1; i >= g_free_ring->idx; i--) {
		if (g_free_ring->s[i].ptr == p) {
			suspect *s = &g_free_ring->s[i];
			cf_info(CF_ALLOC, "%05d : %s %s %d",
				i, state_str[s->state], s->file, s->line);
		}
	}

	pthread_mutex_unlock(&g_free_ring->LOCK);


}


void
cf_rc_init(char *clib_path) {

	alloc_function_init(clib_path);

	// if we're using the circus, initialize it
	g_free_ring = cf_malloc( sizeof(free_ring) + (CIRCUS_SIZE * sizeof(suspect)) );

	pthread_mutex_init(&g_free_ring->LOCK, 0);
	g_free_ring->alloc_sz = CIRCUS_SIZE;
	g_free_ring->idx = 0;
	memset(g_free_ring->s, 0, CIRCUS_SIZE * sizeof(suspect));
	return;

}

#else // NO CIRCUS



void
cf_rc_init(char *clib_path) {
	alloc_function_init(clib_path);
}

#endif

/*
**
**
**
**
**
*/




/* cf_rc_count
 * Get the reservation count for a memory region */
cf_rc_counter
cf_rc_count(void *addr)
{
#ifdef EXTRA_CHECKS
	if (addr == 0) {
		cf_warning(CF_ALLOC, "rccount: null address");
		raise(SIGINT);
		return(0);
	}
#endif

	cf_rc_hdr *hdr = (cf_rc_hdr *) ( ((uint8_t *)addr) - sizeof(cf_rc_hdr));

	return((int) hdr->count );
}

/* notes regarding 'add' and 'decr' vs 'addunless' ---
**
** A suggestion is to use 'addunless' in both the reserve and release code
** which you will see below. That use pattern causes somewhat better
** behavior in buggy use patterns, and allows asserts in cases where the
** calling code is behaving incorrectly. For example, if attempting to reserve a
** reference count where the count has already dropped to 0 (thus is free),
** can be signaled with a message - and halted at 0, which is far safer.
** However, please not that using this functionality
** changes the API, as the return from 'addunless' is true or false (1 or 0)
** not the old value. Thus, the return from cf_rc_reserve and release will always
** be 0 or 1, instead of the current reference count number.
**
** As some of the citrusleaf client code currently uses the reference count
** as reserved, this code is using the 'add' and 'subtract'
*/


/* cf_rc_alloc
 * Allocate a reference-counted memory region.  This region will be filled
 * with bytes of value zero */
void *
cf_rc_alloc_at(size_t sz, char *file, int line)
{
	uint8_t *addr;
	size_t asz = sizeof(cf_rc_hdr) + sz; // debug for stability - rounds us back to regular alignment on all systems

#if defined(MEM_COUNT) && !defined(USE_ASM)
	// Track the calling program location.
	addr = cf_malloc_at(asz, file, line);
#else
	// XXX -- Will not track in MEM_COUNT hash table when using ASMalloc!!
	addr = cf_malloc(asz);
#endif

	if (NULL == addr)
		return(NULL);

	cf_rc_hdr *hdr = (cf_rc_hdr *) addr;
	hdr->count = 1;  // doesn't have to be atomic
	hdr->sz = sz;
	byte *base = addr + sizeof(cf_rc_hdr);

#ifdef USE_CIRCUS
	if (cf_alloc_track_sz && (cf_alloc_track_sz == hdr->sz))
		cf_alloc_register_alloc(addr, file, line);
#endif

	return(base);
}


/* cf_rc_free
 * Deallocate a reference-counted memory region */
void
cf_rc_free_at(void *addr, char *file, int line)
{
	cf_assert(addr, CF_ALLOC, CF_CRITICAL, "null address");

	cf_rc_hdr *hdr = (cf_rc_hdr *) ( ((uint8_t *)addr) - sizeof(cf_rc_hdr));

#if 0
	if (hdr->count != 0) {
		cf_warning(CF_ALLOC, "rcfree: freeing an object that still has a refcount %p",addr);
#ifdef USE_CIRCUS
		cf_alloc_print_history(addr, file, line);
#endif
		raise(SIGINT);
		return;
	}
#endif

#ifdef USE_CIRCUS
	if (cf_alloc_track_sz && (cf_alloc_track_sz == hdr->sz))
		cf_alloc_register_free(addr, file, line);
#endif

#if defined(MEM_COUNT) && !defined(USE_ASM)
	cf_free_at((void *)hdr, file, line);
#else
	// XXX -- Will not track in MEM_COUNT hash table when using ASMalloc!!
	cf_free((void *)hdr);
#endif

	return;
}


int
cf_rc_reserve(void *addr)
{
	cf_rc_hdr *hdr = (cf_rc_hdr *) ( ((uint8_t *)addr) - sizeof(cf_rc_hdr));
	int i = (int) cf_atomic32_add(&hdr->count, 1);
	return(i);
}

int
cf_rc_release(void *addr) {
	int c;
	cf_rc_hdr *hdr = (cf_rc_hdr *) ( ((uint8_t *)addr) - sizeof(cf_rc_hdr));
	c = cf_atomic32_decr(&hdr->count);
	return(c);
}

int
cf_rc_releaseandfree(void *addr) {
	int c;
	cf_rc_hdr *hdr = (cf_rc_hdr *) ( ((uint8_t *)addr) - sizeof(cf_rc_hdr));
	c = cf_atomic32_decr(&hdr->count);
	if (0 == c) {
		cf_free((void *)hdr);
	}
	return(c);
}
