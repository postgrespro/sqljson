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
#include "nodes/primnodes.h"

typedef struct
{
	int32	vl_len_;	/* varlena header (do not touch directly!) */
	uint32	header;		/* version and flags (see below) */
	char	data[FLEXIBLE_ARRAY_MEMBER];
} JsonPath;

#define JSONPATH_VERSION	(0x01)
#define JSONPATH_LAX		(0x80000000)
#define JSONPATH_HDRSZ		(offsetof(JsonPath, data))

#define DatumGetJsonPathP(d)			((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetJsonPathPCopy(d)		((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_JSONPATH_P(x)			DatumGetJsonPathP(PG_GETARG_DATUM(x))
#define PG_GETARG_JSONPATH_P_COPY(x)	DatumGetJsonPathPCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONPATH_P(p)			PG_RETURN_POINTER(p)

#define jspIsScalar(type) ((type) >= jpiNull && (type) <= jpiBool)

/*
 * All node's type of jsonpath expression
 */
typedef enum JsonPathItemType {
		jpiNull = jbvNull,			/* NULL literal */
		jpiString = jbvString,		/* string literal */
		jpiNumeric = jbvNumeric,	/* numeric literal */
		jpiBool = jbvBool,			/* boolean literal: TRUE or FALSE */
		jpiAnd,				/* predicate && predicate */
		jpiOr,				/* predicate || predicate */
		jpiNot,				/* ! predicate */
		jpiIsUnknown,		/* (predicate) IS UNKNOWN */
		jpiEqual,			/* expr == expr */
		jpiNotEqual,		/* expr != expr */
		jpiLess,			/* expr < expr */
		jpiGreater,			/* expr > expr */
		jpiLessOrEqual,		/* expr <= expr */
		jpiGreaterOrEqual,	/* expr >= expr */
		jpiAdd,				/* expr + expr */
		jpiSub,				/* expr - expr */
		jpiMul,				/* expr * expr */
		jpiDiv,				/* expr / expr */
		jpiMod,				/* expr % expr */
		jpiPlus,			/* + expr */
		jpiMinus,			/* - expr */
		jpiAnyArray,		/* [*] */
		jpiAnyKey,			/* .* */
		jpiIndexArray,		/* [subscript, ...] */
		jpiAny,				/* .** */
		jpiKey,				/* .key */
		jpiCurrent,			/* @ */
		jpiRoot,			/* $ */
		jpiVariable,		/* $variable */
		jpiFilter,			/* ? (predicate) */
		jpiExists,			/* EXISTS (expr) predicate */
		jpiType,			/* .type() item method */
		jpiSize,			/* .size() item method */
		jpiAbs,				/* .abs() item method */
		jpiFloor,			/* .floor() item method */
		jpiCeiling,			/* .ceiling() item method */
		jpiDouble,			/* .double() item method */
		jpiDatetime,		/* .datetime() item method */
		jpiKeyValue,		/* .keyvalue() item method */
		jpiSubscript,		/* array subscript: 'expr' or 'expr TO expr' */
		jpiLast,			/* LAST array subscript */
		jpiStartsWith,		/* STARTS WITH predicate */
		jpiLikeRegex,		/* LIKE_REGEX predicate */
		jpiSequence,		/* sequence constructor: 'expr, ...' */
		jpiArray,			/* array constructor: '[expr, ...]' */
		jpiObject,			/* object constructor: '{ key : value, ... }' */
		jpiObjectField,		/* element of object constructor: 'key : value' */
} JsonPathItemType;

/* XQuery regex mode flags for LIKE_REGEX predicate */
#define JSP_REGEX_ICASE		0x01	/* i flag, case insensitive */
#define JSP_REGEX_SLINE		0x02	/* s flag, single-line mode */
#define JSP_REGEX_MLINE		0x04	/* m flag, multi-line mode */
#define JSP_REGEX_WSPACE	0x08	/* x flag, expanded syntax */

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

		struct {
			int32		expr;
			char		*pattern;
			int32		patternlen;
			uint32		flags;
		} like_regex;
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
			JsonPathParseItem *expr;
			char	*pattern; /* could not be not null-terminated */
			uint32	patternlen;
			uint32	flags;
		} like_regex;

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

/* Result of jsonpath predicate evaluation */
typedef enum JsonPathBool
{
	jpbFalse = 0,
	jpbTrue = 1,
	jpbUnknown = 2
} JsonPathBool;

/* Result of jsonpath evaluation */
typedef ErrorData *JsonPathExecResult;

/* Special pseudo-ErrorData with zero sqlerrcode for existence queries. */
extern ErrorData jperNotFound[1];

#define jperOk						NULL
#define jperIsError(jper)			((jper) && (jper)->sqlerrcode)
#define jperIsErrorData(jper)		((jper) && (jper)->elevel > 0)
#define jperGetError(jper)			((jper)->sqlerrcode)
#define jperMakeErrorData(edata)	(edata)
#define jperGetErrorData(jper)		(jper)
#define jperFree(jper)				((jper) && (jper)->sqlerrcode ? \
	(jper)->elevel > 0 ? FreeErrorData(jper) : pfree(jper) : (void) 0)
#define jperReplace(jper1, jper2) (jperFree(jper1), (jper2))

/* Returns special SQL/JSON ErrorData with zero elevel */
static inline JsonPathExecResult
jperMakeError(int sqlerrcode)
{
	ErrorData  *edata = palloc0(sizeof(*edata));

	edata->sqlerrcode = sqlerrcode;

	return edata;
}

typedef Datum (*JsonPathVariable_cb)(void *, bool *);

typedef struct JsonPathVariable	{
	text					*varName;
	Oid						typid;
	int32					typmod;
	JsonPathVariable_cb		cb;
	void					*cb_arg;
} JsonPathVariable;

typedef struct JsonPathVariableEvalContext
{
	JsonPathVariable var;
	struct ExprContext *econtext;
	struct ExprState  *estate;
	Datum		value;
	bool		isnull;
	bool		evaluated;
} JsonPathVariableEvalContext;

typedef struct JsonValueList
{
	JsonbValue *singleton;
	List	   *list;
} JsonValueList;

JsonPathExecResult	executeJsonPath(JsonPath *path,
									List	*vars, /* list of JsonPathVariable */
									Jsonb *json,
									JsonValueList *foundJson);

extern bool  JsonbPathExists(Datum jb, JsonPath *path, List *vars);
extern Datum JsonbPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper,
			   bool *empty, List *vars);
extern JsonbValue *JsonbPathValue(Datum jb, JsonPath *jp, bool *empty,
			   List *vars);

extern bool JsonPathExists(Datum json, JsonPath *path, List *vars);
extern JsonbValue *JsonPathValue(Datum json, JsonPath *jp, bool *empty,
			  List *vars);
extern Datum JsonPathQuery(Datum json, JsonPath *jp, JsonWrapper wrapper,
			  bool *empty, List *vars);

extern Datum EvalJsonPathVar(void *cxt, bool *isnull);

#endif
