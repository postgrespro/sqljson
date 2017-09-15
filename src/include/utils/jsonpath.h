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
	int32	vl_len_;/* varlena header (do not touch directly!) */
	uint32	header;	/* just version, other bits are reservedfor future use */
	char	data[FLEXIBLE_ARRAY_MEMBER];
} JsonPath;

#define JSONPATH_VERSION	(0x01)

#define DatumGetJsonPath(d)			((JsonPath*)DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetJsonPathCopy(d)		((JsonPath*)DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_JSONPATH(x)		DatumGetJsonPath(PG_GETARG_DATUM(x))
#define PG_GETARG_JSONPATH_COPY(x)	DatumGetJsonPathCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONPATH(p)		PG_RETURN_POINTER(p)

/*
 * All node's type of jsonpath expression
 */
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
 * structure and then execute operator with right operand
 */

typedef struct JsonPathItem {
	JsonPathItemType	type;
	int32			nextPos;
	char			*base;

	union {
		/* classic operator with two operands: and, or etc */
		struct {
			int32	left;
			int32	right;
		} args;

		/* any unary operation */
		int32		arg;

		/* storage for jpiIndexArray: indexes of array */
		struct {
			int32	nelems;
			int32	*elems;
		} array;

		/* jpiAny: levels */
		struct {
			uint32	first;
			uint32	last;
		} anybounds;

		struct {
			char		*data;  /* for bool, numeric and string/key */
			int32		datalen; /* filled only for string/key */
		} value;
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

		/* classic operator with two operands: and, or etc */
		struct {
			JsonPathParseItem	*left;
			JsonPathParseItem	*right;
		} args;

		/* any unary operation */
		JsonPathParseItem	*arg;

		/* storage for jpiIndexArray: indexes of array */
		struct {
			int		nelems;
			int32	*elems;
		} array;

		/* jpiAny: levels */
		struct {
			uint32	first;
			uint32	last;
		} anybounds;

		/* scalars */
		Numeric		numeric;
		bool		boolean;
		struct {
			uint32	len;
			char	*val; /* could not be not null-terminated */
		} string;
	} value;
};

extern JsonPathParseItem* parsejsonpath(const char *str, int len);

/*
 * Evaluation of jsonpath
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
	int32					typmod;
	JsonPathVariable_cb		cb;
	void					*cb_arg;
} JsonPathVariable;



JsonPathExecResult	executeJsonPath(JsonPath *path,
									List	*vars, /* list of JsonPathVariable */
									Jsonb *json,
									List **foundJson);

#endif
