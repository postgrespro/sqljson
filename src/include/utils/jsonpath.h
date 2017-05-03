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
#define JSONPATH_LAX		(0x80000000)
#define JSONPATH_HDRSZ		(offsetof(JsonPath, data))

#define DatumGetJsonPath(d)			((JsonPath*)DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetJsonPathCopy(d)		((JsonPath*)DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_JSONPATH(x)		DatumGetJsonPath(PG_GETARG_DATUM(x))
#define PG_GETARG_JSONPATH_COPY(x)	DatumGetJsonPathCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONPATH(p)		PG_RETURN_POINTER(p)

#define jspIsScalar(type) ((type) >= jpiNull && (type) <= jpiBool)

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
		jpiExists,
		jpiType,
		jpiSize,
		jpiAbs,
		jpiFloor,
		jpiCeiling,
		jpiDouble,
		jpiDatetime,
		jpiKeyValue,
		jpiSubscript,
		jpiLast,
		jpiStartsWith,
		jpiMap,
		jpiSequence,
		jpiArray,
		jpiObject,
		jpiObjectField,
		jpiReduce,
		jpiFold,
		jpiFoldl,
		jpiFoldr,
		jpiMin,
		jpiMax,
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

	/* position form base to next node */
	int32			nextPos;

	/*
	 * pointer into JsonPath value to current node, all
	 * positions of current are relative to this base
	 */
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
			struct {
				int32	from;
				int32	to;
			}	   *elems;
		} array;

		/* jpiAny: levels */
		struct {
			uint32	first;
			uint32	last;
		} anybounds;

		struct {
			int32	nelems;
			int32  *elems;
		} sequence;

		struct {
			int32	nfields;
			struct {
				int32	key;
				int32	val;
			}	   *fields;
		} object;

		struct {
			char		*data;  /* for bool, numeric and string/key */
			int32		datalen; /* filled only for string/key */
		} value;
	} content;
} JsonPathItem;

#define jspHasNext(jsp) ((jsp)->nextPos > 0)

extern void jspInit(JsonPathItem *v, JsonPath *js);
extern void jspInitByBuffer(JsonPathItem *v, char *base, int32 pos);
extern bool jspGetNext(JsonPathItem *v, JsonPathItem *a);
extern void jspGetArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetLeftArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetRightArg(JsonPathItem *v, JsonPathItem *a);
extern Numeric	jspGetNumeric(JsonPathItem *v);
extern bool		jspGetBool(JsonPathItem *v);
extern char * jspGetString(JsonPathItem *v, int32 *len);
extern bool jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from,
								 JsonPathItem *to, int i);
extern void jspGetSequenceElement(JsonPathItem *v, int i, JsonPathItem *elem);
extern void jspGetObjectField(JsonPathItem *v, int i,
							  JsonPathItem *key, JsonPathItem *val);

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
			struct
			{
				JsonPathParseItem *from;
				JsonPathParseItem *to;
			}	   *elems;
		} array;

		/* jpiAny: levels */
		struct {
			uint32	first;
			uint32	last;
		} anybounds;

		struct {
			List   *elems;
		} sequence;

		struct {
			List   *fields;
		} object;

		/* scalars */
		Numeric		numeric;
		bool		boolean;
		struct {
			uint32	len;
			char	*val; /* could not be not null-terminated */
		} string;
	} value;
};

typedef struct JsonPathParseResult
{
	JsonPathParseItem *expr;
	bool		lax;
} JsonPathParseResult;

extern JsonPathParseResult* parsejsonpath(const char *str, int len);

/*
 * Evaluation of jsonpath
 */

typedef enum JsonPathExecStatus
{
	jperOk = 0,
	jperError,
	jperFatalError,
	jperNotFound
} JsonPathExecStatus;

typedef uint64 JsonPathExecResult;

#define jperStatus(jper)	((JsonPathExecStatus)(uint32)(jper))
#define jperIsError(jper)	(jperStatus(jper) == jperError)
#define jperGetError(jper)	((uint32)((jper) >> 32))
#define jperMakeError(err)	(((uint64)(err) << 32) | jperError)

typedef Datum (*JsonPathVariable_cb)(void *, bool *);

typedef struct JsonPathVariable	{
	text					*varName;
	Oid						typid;
	int32					typmod;
	JsonPathVariable_cb		cb;
	void					*cb_arg;
} JsonPathVariable;



typedef struct JsonValueList
{
	JsonbValue *singleton;
	List	   *list;
} JsonValueList;

JsonPathExecResult	executeJsonPath(JsonPath *path,
									List	*vars, /* list of JsonPathVariable */
									Jsonb *json,
									JsonValueList *foundJson);

#endif
