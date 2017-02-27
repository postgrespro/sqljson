/*-------------------------------------------------------------------------
 *
 * jsonpath.h
 *	Definitions of jsonpath datatype
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/include/utils/jsonpath.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_H
#define JSONPATH_H

#include "fmgr.h"
#include "utils/jsonb.h"
#include "nodes/pg_list.h"

typedef struct
{
	int32	vl_len_;	/* varlena header (do not touch directly!) */
} JsonPath;

#define DatumGetJsonPathP(d)			((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetJsonPathPCopy(d)		((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_JSONPATH_P(x)			DatumGetJsonPathP(PG_GETARG_DATUM(x))
#define PG_GETARG_JSONPATH_P_COPY(x)	DatumGetJsonPathPCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONPATH_P(p)			PG_RETURN_POINTER(p)

typedef enum JsonPathItemType {
		jpiNull = jbvNull,
		jpiString = jbvString,
		jpiNumeric = jbvNumeric,
		jpiBool = jbvBool,
		jpiAnd,
		jpiOr,
		jpiNot,
		jpiIsUnknown,
		jpiEqual,
		jpiNotEqual,
		jpiLess,
		jpiGreater,
		jpiLessOrEqual,
		jpiGreaterOrEqual,
		jpiAdd,
		jpiSub,
		jpiMul,
		jpiDiv,
		jpiMod,
		jpiPlus,
		jpiMinus,
		jpiAnyArray,
		jpiAnyKey,
		jpiIndexArray,
		jpiAny,
		jpiKey,
		jpiCurrent,
		jpiRoot,
		jpiVariable,
		jpiFilter,
		jpiExists
} JsonPathItemType;


/*
 * Support functions to parse/construct binary value.
 * Unlike many other representation of expression the first/main
 * node is not an operation but left operand of expression. That
 * allows to implement cheep follow-path descending in jsonb
 * structure and then execute operator with right operand which
 * is always a constant.
 */

typedef struct JsonPathItem {
	JsonPathItemType	type;
	int32			nextPos;
	char			*base;

	union {
		struct {
			char		*data;  /* for bool, numeric and string/key */
			int32		datalen; /* filled only for string/key */
		} value;

		struct {
			int32	left;
			int32	right;
		} args;

		int32		arg;

		struct {
			int32		nelems;
			int32	*elems;
		} array;

		struct {
			uint32	first;
			uint32	last;
		} anybounds;
	};
} JsonPathItem;

extern void jspInit(JsonPathItem *v, JsonPath *js);
extern void jspInitByBuffer(JsonPathItem *v, char *base, int32 pos);
extern bool jspGetNext(JsonPathItem *v, JsonPathItem *a);
extern void jspGetArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetLeftArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetRightArg(JsonPathItem *v, JsonPathItem *a);
extern Numeric	jspGetNumeric(JsonPathItem *v);
extern bool		jspGetBool(JsonPathItem *v);
extern char * jspGetString(JsonPathItem *v, int32 *len);

/*
 * Parsing
 */

typedef struct JsonPathParseItem JsonPathParseItem;

struct JsonPathParseItem {
	JsonPathItemType	type;
	JsonPathParseItem	*next; /* next in path */

	union {
		struct {
			JsonPathParseItem	*left;
			JsonPathParseItem	*right;
		} args;

		JsonPathParseItem	*arg;

		Numeric		numeric;
		bool		boolean;
		struct {
			uint32	len;
			char	*val; /* could not be not null-terminated */
		} string;

		struct {
			int		nelems;
			int32	*elems;
		} array;

		struct {
			uint32	first;
			uint32	last;
		} anybounds;
	};
};

extern JsonPathParseItem* parsejsonpath(const char *str, int len);

/*
 * Execution
 */

typedef enum JsonPathExecResult {
	jperOk = 0,
	jperError,
	jperFatalError,
	jperNotFound
} JsonPathExecResult;

typedef Datum (*JsonPathVariable_cb)(void *, bool *);

typedef struct JsonPathVariable	{
	text					*varName;
	Oid						typid;
	int32					typmod; /* do we need it here? */
	JsonPathVariable_cb		cb;
	void					*cb_arg;
} JsonPathVariable;



JsonPathExecResult	executeJsonPath(JsonPath *path,
									List	*vars, /* list of JsonPathVariable */
									Jsonb *json,
									List **foundJson);

#endif
