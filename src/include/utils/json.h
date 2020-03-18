/*-------------------------------------------------------------------------
 *
 * json.h
 *	  Declarations for JSON data type support.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/json.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSON_H
#define JSON_H

#include "lib/stringinfo.h"

/* functions in json.c */
extern void escape_json(StringInfo buf, const char *str);
extern char *JsonEncodeDateTime(char *buf, Datum value, Oid typid,
								const int *tzp);
extern Datum json_build_object_worker(int nargs, Datum *args, bool *nulls,
									  Oid *types, bool absent_on_null,
									  bool unique_keys);
extern Datum json_build_array_worker(int nargs, Datum *args, bool *nulls,
									 Oid *types, bool absent_on_null);

#endif							/* JSON_H */
