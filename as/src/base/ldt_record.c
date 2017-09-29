/*
 * ldt_record.c
 *
 * Copyright (C) 2013-2016 Aerospike, Inc.
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
 * as_record interface for large stack objects
 *
 */

#include "base/ldt_record.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_rec.h"
#include "aerospike/as_val.h"

#include "fault.h"
#include "base/ldt.h"


/*********************************************************************
 * FUNCTIONS                                                         *
 *                                                                   *
 * NB: Entire ldt_record is just a wrapper over the udf_record       *
 *     implementation                                                *
 ********************************************************************/
static as_val *
ldt_record_get(const as_rec * rec, const char * name)
{
	static const char * meth = "ldt_record_get()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p, name=%p]... Fail", meth, rec, name);
		return NULL;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return NULL;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_get(h_urec, name);
}

static int
ldt_record_set(const as_rec * rec, const char * name, const as_val * value)
{
	static const char * meth = "ldt_record_set()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p, name=%p]:", meth, rec, name);
		return 2;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set(h_urec, name, value);
}

static int
ldt_record_set_flags(const as_rec * rec, const char * name,  uint8_t  flags)
{
	static const char * meth = "ldt_record_set_flags()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p, name=%p]... Fail", meth, rec, name);
		return 2;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set_flags(h_urec, name, flags);
}

/**
 * Set the record type.  If "rec_type" is negative, then we "unset" the rec_type,
 * which is needed before we delete a record that no longer contains any LDTs.
 */
static int
ldt_record_set_type(const as_rec * rec,  int8_t rec_type )
{
	static const char * meth = "ldt_record_set_type()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return 2;
	}

	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set_type(h_urec, rec_type);
}

static int
ldt_record_set_ttl(const as_rec * rec,  uint32_t ttl)
{
	static const char * meth = "ldt_record_set_ttl()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return 2;
	}

	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set_ttl(h_urec, ttl);
}

static int
ldt_record_drop_key(const as_rec * rec)
{
	static const char * meth = "ldt_record_drop_key()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return 2;
	}

	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_drop_key(h_urec);
}

static int
ldt_record_remove(const as_rec * rec, const char * name)
{
	static const char * meth = "ldt_record_remove()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p, name=%p]... Fail", meth, rec, name);
		return 2;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_remove(h_urec, name);
}

static uint32_t
ldt_record_ttl(const as_rec * rec)
{
	static char * const meth = "ldt_record_ttl()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return 0;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_ttl(h_urec);
}

static uint64_t
ldt_record_last_update_time(const as_rec * rec)
{
	static const char * meth = "ldt_record_last_update_time()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]", meth, rec);
		return 0;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec = lrecord->h_urec;

	return as_rec_last_update_time(h_urec);
}

static uint16_t
ldt_record_gen(const as_rec * rec)
{
	static const char * meth = "ldt_record_gen()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return 0;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}

	const as_rec *h_urec = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_gen(h_urec);
}

static as_val *
ldt_record_key(const as_rec * rec)
{
	static const char * meth = "ldt_record_key()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Parameters: record=%p", meth, rec);
		return 0;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec = lrecord->h_urec;

	return as_rec_key(h_urec);
}

static const char *
ldt_record_setname(const as_rec * rec)
{
	static const char * meth = "ldt_record_setname()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Parameters: record=%p", meth, rec);
		return 0;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec = lrecord->h_urec;

	return as_rec_setname(h_urec);
}

static as_bytes *
ldt_record_digest(const as_rec * rec)
{
	static const char * meth = "ldt_record_digest()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return NULL;
	}

	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_digest(h_urec);
}

static int
ldt_record_bin_names(const as_rec * rec, as_rec_bin_names_callback callback, void * context)
{
	static const char * meth = "ldt_record_bin_names()";
	if (!rec) {
		cf_warning(AS_UDF, "%s: Invalid Parameters [record=%p]... Fail", meth, rec);
		return 2;
	}

	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_bin_names(h_urec, callback, context);
}

static uint16_t
ldt_record_numbins(const as_rec * rec)
{
	static const char * meth = "ldt_record_numbins()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Parameters: record=%p", meth, rec);
		return 0;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec = lrecord->h_urec;

	return as_rec_numbins(h_urec);
}

const as_rec_hooks ldt_record_hooks = {
	.get		= ldt_record_get,
	.set		= ldt_record_set,
	.remove		= ldt_record_remove,
	.ttl		= ldt_record_ttl,
	.last_update_time	= ldt_record_last_update_time,
	.gen		= ldt_record_gen,
	.key		= ldt_record_key,
	.setname	= ldt_record_setname,
	.destroy	= NULL,
	.digest		= ldt_record_digest,
	.set_flags	= ldt_record_set_flags,
	.set_type	= ldt_record_set_type,
	.set_ttl	= ldt_record_set_ttl,
	.drop_key	= ldt_record_drop_key,
	.bin_names	= ldt_record_bin_names,
	.numbins	= ldt_record_numbins
};
