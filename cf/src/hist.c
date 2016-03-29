/*
 * hist.c
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

#include "hist.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "dynbuf.h"
#include "fault.h"


//==========================================================
// Histogram with logarithmic buckets.
//

//------------------------------------------------
// Create a histogram. There's no destroy(), but
// you can just cf_free() the histogram.
//
histogram*
histogram_create(const char *name, histogram_scale scale)
{
	if (! (name && strlen(name) < HISTOGRAM_NAME_SIZE)) {
		return NULL;
	}

	if (! (scale >= 0 && scale < HIST_SCALE_MAX_PLUS_1)) {
		return NULL;
	}

	histogram *h = cf_malloc(sizeof(histogram));

	if (! h) {
		return NULL;
	}

	strcpy(h->name, name);
	memset((void *)&h->counts, 0, sizeof(h->counts));

	switch (scale) {
	case HIST_MILLISECONDS:
		h->scale_tag = HIST_TAG_MILLISECONDS;
		h->time_div = 1000 * 1000;
		break;
	case HIST_MICROSECONDS:
		h->scale_tag = HIST_TAG_MICROSECONDS;
		h->time_div = 1000;
		break;
	default:
		h->scale_tag = HIST_TAG_RAW;
		h->time_div = 0;
		// If histogram_insert_data_point() is called for a raw histogram, the
		// divide by 0 will crash - consider that a high-performance assert.
		break;
	}

	return h;
}

//------------------------------------------------
// Clear a histogram.
//
void
histogram_clear(histogram *h)
{
	for (int i = 0; i < N_BUCKETS; i++) {
		cf_atomic64_set(&h->counts[i], 0);
	}
}

//------------------------------------------------
// Dump a histogram to log.
//
// Note - DO NOT change the log output format in
// this method - tools such as as_log_latency
// assume this format.
//
void
histogram_dump(histogram *h)
{
	int b;
	uint64_t counts[N_BUCKETS];

	for (b = 0; b < N_BUCKETS; b++) {
		counts[b] = cf_atomic64_get(h->counts[b]);
	}

	int i = N_BUCKETS;
	int j = 0;
	uint64_t total_count = 0;

	for (b = 0; b < N_BUCKETS; b++) {
		if (counts[b] != 0) {
			if (i > b) {
				i = b;
			}

			j = b;
			total_count += counts[b];
		}
	}

	char buf[100];
	int pos = 0;
	int k = 0;

	buf[0] = '\0';

	cf_info(AS_INFO, "histogram dump: %s (%lu total) %s", h->name, total_count,
			h->scale_tag);

	for ( ; i <= j; i++) {
		if (counts[i] == 0) { // print only non-zero columns
			continue;
		}

		int bytes = sprintf(buf + pos, " (%02d: %010lu)", i, counts[i]);

		if (bytes <= 0) {
			cf_info(AS_INFO, "histogram dump error");
			return;
		}

		pos += bytes;

		if ((k & 3) == 3) { // maximum of 4 printed columns per log line
			 cf_info(AS_INFO, "%s", buf);
			 pos = 0;
			 buf[0] = '\0';
		}

		k++;
	}

	if (pos > 0) {
		cf_info(AS_INFO, "%s", buf);
	}
}

//------------------------------------------------
// BYTE_MSB[n] returns the position of the most
// significant bit. If no bits are set (n = 0) it
// returns 0. Otherwise the positions are 1 ... 8
// from low to high, so e.g. n = 13 returns 4:
//
//		bits:		0  0  0  0  1  1  0  1
//		position:	8  7  6  5 [4] 3  2  1
//
static const char BYTE_MSB[] = {
		0, 1, 2, 2, 3, 3, 3, 3,  4, 4, 4, 4, 4, 4, 4, 4,
		5, 5, 5, 5, 5, 5, 5, 5,  5, 5, 5, 5, 5, 5, 5, 5,
		6, 6, 6, 6, 6, 6, 6, 6,  6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6,  6, 6, 6, 6, 6, 6, 6, 6,

		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7, 7, 7, 7, 7, 7,

		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,

		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8,  8, 8, 8, 8, 8, 8, 8, 8
};

//------------------------------------------------
// Returns the position of the most significant
// bit of n. Positions are 1 ... 64 from low to
// high, so:
//
//		n			msb(n)
//		--------	------
//		0			0
//		1			1
//		2 ... 3		2
//		4 ... 7		3
//		8 ... 15	4
//		etc.
//
static int
msb(uint64_t n)
{
	int shift = 0;

	while (true) {
		uint64_t n_div_256 = n >> 8;

		if (n_div_256 == 0) {
			return shift + (int)BYTE_MSB[n];
		}

		n = n_div_256;
		shift += 8;
	}

	// Should never get here.
	cf_crash(AS_INFO, "end of msb()");
	return -1;
}

//------------------------------------------------
// Insert a time interval data point. The interval
// is time elapsed since start_ns, converted to
// milliseconds or microseconds as appropriate.
// Assumes start_ns was obtained via cf_getns()
// some time ago. Generates a histogram with
// either:
//
//		bucket	millisecond range
//		------	-----------------
//		0		0 to 1  (more exactly, 0.999999)
//		1		1 to 2  (more exactly, 1.999999)
//		2		2 to 4  (more exactly, 3.999999)
//		3		4 to 8  (more exactly, 7.999999)
//		4		8 to 16 (more exactly, 15.999999)
//		etc.
//
// or:
//
//		bucket	microsecond range
//		------	-----------------
//		0		0 to 1  (more exactly, 0.999)
//		1		1 to 2  (more exactly, 1.999)
//		2		2 to 4  (more exactly, 3.999)
//		3		4 to 8  (more exactly, 7.999)
//		4		8 to 16 (more exactly, 15.999)
//		etc.
//
uint64_t
histogram_insert_data_point(histogram *h, uint64_t start_ns)
{
	uint64_t end_ns = cf_getns();
	uint64_t delta_t = (end_ns - start_ns) / h->time_div;

	int bucket = 0;

	if (delta_t != 0) {
		bucket = msb(delta_t);

		if (start_ns > end_ns) {
			// Either the clock went backwards, or wrapped. (Assume the former,
			// since it takes ~580 years from 0 to wrap.)
			cf_warning(AS_INFO, "clock went backwards: start %lu end %lu",
					start_ns, end_ns);
			bucket = 0;
		}
	}

	cf_atomic64_incr(&h->counts[bucket]);

	return end_ns;
}

//------------------------------------------------
// Insert a raw data point. Generates a histogram
// with:
//
//		bucket	value range
//		------	-----------
//		0		0
//		1		1
//		2		2, 3
//		3		4 to 7
//		4		8 to 15
//		etc.
//
void
histogram_insert_raw(histogram *h, uint64_t value)
{
	cf_atomic64_incr(&h->counts[msb(value)]);
}


//==========================================================
// Histogram with linear buckets.
//

//------------------------------------------------
// Create a linear histogram.
//
linear_histogram*
linear_histogram_create(char *name, uint64_t start, uint64_t max_offset,
		int num_buckets)
{
	if (! (name && strlen(name) < HISTOGRAM_NAME_SIZE)) {
		return NULL;
	}

	if (num_buckets > MAX_LINEAR_BUCKETS) {
		cf_crash(AS_INFO, "linear histogram num_buckets %u > max %u",
				num_buckets, MAX_LINEAR_BUCKETS);
	}

	linear_histogram *h = cf_malloc(sizeof(linear_histogram));

	if (! h) {
		return NULL;
	}

	if (0 != pthread_mutex_init(&h->info_lock, NULL)) {
		cf_free(h);
		return NULL;
	}

	strcpy(h->name, name);
	h->num_buckets = num_buckets;
	h->start = start;
	h->bucket_width = max_offset / h->num_buckets;

	// Avoid divide by zero while inserting data point.
	if (h->bucket_width == 0) {
		h->bucket_width = 1;
	}

	memset((void *)&h->counts, 0, sizeof(h->counts));
	h->info_snapshot[0] = 0;

	return h;
}

//------------------------------------------------
// Destroy a linear histogram.
//
void
linear_histogram_destroy(linear_histogram *h)
{
	pthread_mutex_destroy(&h->info_lock);
	cf_free(h);
}

//------------------------------------------------
// Clear and re-scale a linear histogram.
//
// Note - not thread safe! No other methods for
// this object can be concurrent with this call.
//
void
linear_histogram_clear(linear_histogram *h, uint64_t start, uint64_t max_offset)
{
	h->start = start;
	h->bucket_width = max_offset / h->num_buckets;

	// Avoid divide by zero while inserting data point.
	if (h->bucket_width == 0) {
		h->bucket_width = 1;
	}

	memset((void *)&h->counts, 0, sizeof(h->counts));
}

//------------------------------------------------
// Dump a linear histogram to log.
//
// Note - DO NOT change the log output format in
// this method - public documentation assumes this
// format.
//
void
linear_histogram_dump(linear_histogram *h)
{
	int b;
	uint64_t counts[h->num_buckets];

	for (b = 0; b < h->num_buckets; b++) {
		counts[b] = cf_atomic64_get(h->counts[b]);
	}

	int i = h->num_buckets;
	int j = 0;
	uint64_t total_count = 0;

	for (b = 0; b < h->num_buckets; b++) {
		if (counts[b] != 0) {
			if (i > b) {
				i = b;
			}

			j = b;
			total_count += counts[b];
		}
	}

	char buf[100];
	int pos = 0;
	int k = 0;

	buf[0] = '\0';

	cf_debug(AS_NSUP, "linear histogram dump: %s [%lu %lu]/[%lu] (%lu total)",
			h->name, h->start, h->start + (h->num_buckets * h->bucket_width),
			h->bucket_width, total_count);

	for ( ; i <= j; i++) {
		if (counts[i] == 0) { // print only non-zero columns
			continue;
		}

		int bytes = sprintf(buf + pos, " (%02d: %010lu)", i, counts[i]);

		if (bytes <= 0) {
			cf_debug(AS_NSUP, "linear histogram dump error");
			return;
		}

		pos += bytes;

		if ((k & 3) == 3) { // maximum of 4 printed columns per log line
			 cf_debug(AS_NSUP, "%s", buf);
			 pos = 0;
			 buf[0] = '\0';
		}

		k++;
	}

	if (pos > 0) {
		cf_debug(AS_NSUP, "%s", buf);
	}
}

//------------------------------------------------
// Access method for total count.
//
uint64_t
linear_histogram_get_total(linear_histogram *h)
{
	uint64_t total_count = 0;

	for (int i = 0; i < h->num_buckets; i++) {
		total_count += cf_atomic64_get(h->counts[i]);
	}

	return total_count;
}

//------------------------------------------------
// Insert a data point. Points out of range will
// end up in the bucket at the appropriate end.
//
void
linear_histogram_insert_data_point(linear_histogram *h, uint64_t point)
{
	int64_t offset = point - h->start;
	int64_t bucket = 0;

	if (offset > 0) {
		bucket = offset / h->bucket_width;

		if (bucket >= (int64_t)h->num_buckets) {
			bucket = h->num_buckets - 1;
		}
	}

	cf_atomic64_incr(&h->counts[bucket]);
}

//------------------------------------------------
// Get details of the "threshold" bucket - the
// bucket at which the specified percentage of
// total count has been accumulated, starting from
// low bucket.
//
// Note - not thread safe! Relies on counts not
// changing during this call.
//
bool
linear_histogram_get_thresholds_for_fraction(linear_histogram *h,
		uint32_t tenths_pct, uint64_t *p_low, uint64_t *p_high,
		uint32_t *p_mid_tenths_pct)
{
	return linear_histogram_get_thresholds_for_subtotal(h,
			(linear_histogram_get_total(h) * tenths_pct) / 1000, p_low, p_high,
			p_mid_tenths_pct);
}

//------------------------------------------------
// Get details of the "threshold" bucket - the
// bucket at which the specified sub-total count
// has been accumulated, starting from low bucket.
//
// Note - not thread safe! Relies on counts not
// changing during this call.
//
bool
linear_histogram_get_thresholds_for_subtotal(linear_histogram *h,
		uint64_t subtotal, uint64_t *p_low, uint64_t *p_high,
		uint32_t *p_mid_tenths_pct)
{
	uint64_t count = 0;
	int i;

	for (i = 0; i < h->num_buckets; i++) {
		count += h->counts[i];

		if (count > subtotal) {
			break;
		}
	}

	if (i == h->num_buckets) {
		// This means subtotal >= h->total_count.
		*p_low = 0;
		*p_high = 0;
		*p_mid_tenths_pct = 0;
		return count != 0;
	}

	*p_low = h->start + (i * h->bucket_width);
	*p_high = *p_low + h->bucket_width;

	uint64_t bucket_subtotal = h->counts[i] - (count - subtotal);

	// Round up to nearest tenth of a percent.
	*p_mid_tenths_pct =
			((bucket_subtotal * 1000) + h->counts[i] - 1) / h->counts[i];

	return i == h->num_buckets - 1;
}

//------------------------------------------------
// Save a linear histogram "snapshot".
//
void
linear_histogram_save_info(linear_histogram *h)
{
	pthread_mutex_lock(&h->info_lock);

	// Write num_buckets, the bucket width, and the first bucket's count.
	int i = 0;
	int pos = snprintf(h->info_snapshot, INFO_SNAPSHOT_SIZE, "%d,%ld,%ld",
			h->num_buckets, h->bucket_width, cf_atomic64_get(h->counts[i++]));

	while (pos < INFO_SNAPSHOT_SIZE && i < h->num_buckets) {
		pos += snprintf(h->info_snapshot + pos, INFO_SNAPSHOT_SIZE - pos,
				",%ld", cf_atomic64_get(h->counts[i++]));
	}

	pthread_mutex_unlock(&h->info_lock);
}

//------------------------------------------------
// Append a linear histogram "snapshot" to db.
//
void
linear_histogram_get_info(linear_histogram *h, cf_dyn_buf *db)
{
	pthread_mutex_lock(&h->info_lock);
	cf_dyn_buf_append_string(db, h->info_snapshot);
	pthread_mutex_unlock(&h->info_lock);
}
