/*-------------------------------------------------------------------------
 *
 * jsonpath.h
 *	Definitions for jsonpath datatype
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/include/utils/jsonpath.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_H
#define JSONPATH_H

#include "fmgr.h"
#include "executor/tablefunc.h"
#include "utils/jsonb.h"
#include "utils/jsonapi.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/jsonb.h"

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		header;			/* version and flags (see below) */
	uint32		ext_items_count; /* number of items that need cache for external execution */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} JsonPath;

#define JSONPATH_VERSION	(0x01)
#define JSONPATH_LAX		(0x80000000)
#define JSONPATH_HDRSZ		(offsetof(JsonPath, data))

/* flags for JsonPathItem */
#define JSPI_OUT_PATH		0x01

#define DatumGetJsonPathP(d)			((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetJsonPathPCopy(d)		((JsonPath *) DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_JSONPATH_P(x)			DatumGetJsonPathP(PG_GETARG_DATUM(x))
#define PG_GETARG_JSONPATH_P_COPY(x)	DatumGetJsonPathPCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONPATH_P(p)			PG_RETURN_POINTER(p)

#define jspIsScalar(type) ((type) >= jpiNull && (type) <= jpiBool)

/*
 * All node's type of jsonpath expression
 */
typedef enum JsonPathItemType
{
	jpiNull = jbvNull,			/* NULL literal */
	jpiString = jbvString,		/* string literal */
	jpiNumeric = jbvNumeric,	/* numeric literal */
	jpiBool = jbvBool,			/* boolean literal: TRUE or FALSE */
	jpiAnd,						/* predicate && predicate */
	jpiOr,						/* predicate || predicate */
	jpiNot,						/* ! predicate */
	jpiIsUnknown,				/* (predicate) IS UNKNOWN */
	jpiEqual,					/* expr == expr */
	jpiNotEqual,				/* expr != expr */
	jpiLess,					/* expr < expr */
	jpiGreater,					/* expr > expr */
	jpiLessOrEqual,				/* expr <= expr */
	jpiGreaterOrEqual,			/* expr >= expr */
	jpiAdd,						/* expr + expr */
	jpiSub,						/* expr - expr */
	jpiMul,						/* expr * expr */
	jpiDiv,						/* expr / expr */
	jpiMod,						/* expr % expr */
	jpiPlus,					/* + expr */
	jpiMinus,					/* - expr */
	jpiAnyArray,				/* [*] */
	jpiAnyKey,					/* .* */
	jpiIndexArray,				/* [subscript, ...] */
	jpiAny,						/* .** */
	jpiKey,						/* .key */
	jpiCurrent,					/* @ */
	jpiCurrentN,				/* @N */
	jpiRoot,					/* $ */
	jpiVariable,				/* $variable */
	jpiFilter,					/* ? (predicate) */
	jpiExists,					/* EXISTS (expr) predicate */
	jpiType,					/* .type() item method */
	jpiSize,					/* .size() item method */
	jpiAbs,						/* .abs() item method */
	jpiFloor,					/* .floor() item method */
	jpiCeiling,					/* .ceiling() item method */
	jpiDouble,					/* .double() item method */
	jpiDatetime,				/* .datetime() item method */
	jpiKeyValue,				/* .keyvalue() item method */
	jpiSubscript,				/* array subscript: 'expr' or 'expr TO expr' */
	jpiLast,					/* LAST array subscript */
	jpiStartsWith,				/* STARTS WITH predicate */
	jpiLikeRegex,				/* LIKE_REGEX predicate */
	jpiSequence,				/* sequence constructor: 'expr, ...' */
	jpiArray,					/* array constructor: '[expr, ...]' */
	jpiObject,					/* object constructor: '{ key : value, ... }' */
	jpiObjectField,				/* element of object constructor: 'key : value' */
	jpiLambda,					/* lambda expression: 'arg => expr' or '(arg,...) => expr' */
	jpiArgument,				/* lambda argument */
	jpiMethod,					/* user item method */
	jpiFunction,				/* user function */

	jpiBinary = 0xFF			/* for jsonpath operators implementation only */
} JsonPathItemType;

/* XQuery regex mode flags for LIKE_REGEX predicate */
#define JSP_REGEX_ICASE		0x01	/* i flag, case insensitive */
#define JSP_REGEX_SLINE		0x02	/* s flag, single-line mode */
#define JSP_REGEX_MLINE		0x04	/* m flag, multi-line mode */
#define JSP_REGEX_WSPACE	0x08	/* x flag, expanded syntax */
#define JSP_REGEX_QUOTE		0x10	/* q flag, no special characters */

#define jspIsBooleanOp(type) ( \
	(type) == jpiAnd || \
	(type) == jpiOr || \
	(type) == jpiNot || \
	(type) == jpiIsUnknown || \
	(type) == jpiEqual || \
	(type) == jpiNotEqual || \
	(type) == jpiLess || \
	(type) == jpiGreater || \
	(type) == jpiLessOrEqual || \
	(type) == jpiGreaterOrEqual || \
	(type) == jpiExists || \
	(type) == jpiStartsWith \
)

/*
 * Support functions to parse/construct binary value.
 * Unlike many other representation of expression the first/main
 * node is not an operation but left operand of expression. That
 * allows to implement cheap follow-path descending in jsonb
 * structure and then execute operator with right operand
 */

typedef struct JsonPathItem
{
	JsonPathItemType type;
	uint8		flags;

	/* position form base to next node */
	int32		nextPos;

	/*
	 * pointer into JsonPath value to current node, all positions of current
	 * are relative to this base
	 */
	char	   *base;

	union
	{
		/* classic operator with two operands: and, or etc */
		struct
		{
			int32		left;
			int32		right;
		}			args;

		/* any unary operation */
		int32		arg;

		/* storage for jpiIndexArray: indexes of array */
		struct
		{
			int32		nelems;
			struct
			{
				int32		from;
				int32		to;
			}		   *elems;
		}			array;

		/* jpiAny: levels */
		struct
		{
			uint32		first;
			uint32		last;
		}			anybounds;

		struct
		{
			int32		nelems;
			int32	   *elems;
		}			sequence;

		struct
		{
			int32		nfields;
			struct
			{
				int32		key;
				int32		val;
			}		   *fields;
		}			object;

		struct
		{
			int32		level;
		}			current;

		struct
		{
			char	   *data;	/* for bool, numeric and string/key */
			int32		datalen;	/* filled only for string/key */
		}			value;

		struct
		{
			int32		expr;
			char	   *pattern;
			int32		patternlen;
			uint32		flags;
		}			like_regex;

		struct
		{
			int32		id;
			int32	   *params;
			int32		nparams;
			int32		expr;
		}			lambda;

		struct
		{
			int32		id;
			int32		item;
			char	   *name;
			int32		namelen;
			int32	   *args;
			int32		nargs;
		}			func;
	}			content;
} JsonPathItem;

#define jspHasNext(jsp) ((jsp)->nextPos > 0)
#define jspOutPath(jsp) (((jsp)->flags & JSPI_OUT_PATH) != 0)

extern void jspInit(JsonPathItem *v, JsonPath *js);
extern void jspInitByBuffer(JsonPathItem *v, char *base, int32 pos);
extern bool jspGetNext(JsonPathItem *v, JsonPathItem *a);
extern void jspGetArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetLeftArg(JsonPathItem *v, JsonPathItem *a);
extern void jspGetRightArg(JsonPathItem *v, JsonPathItem *a);
extern Numeric jspGetNumeric(JsonPathItem *v);
extern bool jspGetBool(JsonPathItem *v);
extern char *jspGetString(JsonPathItem *v, int32 *len);
extern bool jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from,
								 JsonPathItem *to, int i);
extern void jspGetSequenceElement(JsonPathItem *v, int i, JsonPathItem *elem);
extern void jspGetObjectField(JsonPathItem *v, int i,
							  JsonPathItem *key, JsonPathItem *val);
extern JsonPathItem *jspGetLambdaParam(JsonPathItem *func, int index,
				  JsonPathItem *arg);
extern JsonPathItem *jspGetLambdaExpr(JsonPathItem *lambda, JsonPathItem *expr);
extern JsonPathItem *jspGetFunctionArg(JsonPathItem *func, int index,
				  JsonPathItem *arg);
extern JsonPathItem *jspGetMethodItem(JsonPathItem *method, JsonPathItem *arg);

extern const char *jspOperationName(JsonPathItemType type);

/*
 * Parsing support data structures.
 */

typedef struct JsonPathParseItem JsonPathParseItem;

struct JsonPathParseItem
{
	JsonPathItemType type;
	uint8		flags;
	JsonPathParseItem *next;	/* next in path */

	union
	{

		/* classic operator with two operands: and, or etc */
		struct
		{
			JsonPathParseItem *left;
			JsonPathParseItem *right;
		}			args;

		/* any unary operation */
		JsonPathParseItem *arg;

		/* storage for jpiIndexArray: indexes of array */
		struct
		{
			int			nelems;
			struct JsonPathParseArraySubscript
			{
				JsonPathParseItem *from;
				JsonPathParseItem *to;
			}		   *elems;
		}			array;

		/* jpiAny: levels */
		struct
		{
			uint32		first;
			uint32		last;
		}			anybounds;

		struct
		{
			JsonPathParseItem *expr;
			char	   *pattern;	/* could not be not null-terminated */
			uint32		patternlen;
			uint32		flags;
		}			like_regex;

		struct {
			List   *elems;
		} sequence;

		struct {
			List   *fields;
		} object;

		struct {
			int		level;
		} current;

		JsonPath   *binary;

		struct {
			List   *params;
			JsonPathParseItem *expr;
		} lambda;

		struct {
			List   *args;
			char   *name;
			int32	namelen;
		} func;

		/* scalars */
		Numeric numeric;
		bool		boolean;
		struct
		{
			uint32		len;
			char	   *val;	/* could not be not null-terminated */
		}			string;
	}			value;
};

typedef struct JsonPathParseResult
{
	JsonPathParseItem *expr;
	bool		lax;
} JsonPathParseResult;

extern JsonPathParseResult *parsejsonpath(const char *str, int len);

/*
 * Evaluation of jsonpath
 */

/* External variable passed into jsonpath. */
typedef struct JsonPathVariableEvalContext
{
	char	   *name;
	Oid			typid;
	int32		typmod;
	struct ExprContext *econtext;
	struct ExprState  *estate;
	MemoryContext mcxt;		/* memory context for cached value */
	Datum		value;
	bool		isnull;
	bool		evaluated;
} JsonPathVariableEvalContext;

/* Type of SQL/JSON item */
typedef enum JsonItemType
{
	/* Scalar types */
	jsiNull = jbvNull,
	jsiString = jbvString,
	jsiNumeric = jbvNumeric,
	jsiBool = jbvBool,
	/* Composite types */
	jsiArray = jbvArray,
	jsiObject = jbvObject,
	/* Binary (i.e. struct Jsonb) jbvArray/jbvObject */
	jsiBinary = jbvBinary,

	/*
	 * Virtual types.
	 *
	 * These types are used only for in-memory JSON processing and serialized
	 * into JSON strings/numbers when outputted to json/jsonb.
	 */
	jsiDatetime = 0x20,
	jsiDouble = 0x21
} JsonItemType;

/* SQL/JSON item */
typedef struct JsonItem
{
	struct JsonItem *next;

	union
	{
		int			type;	/* XXX JsonItemType */

		JsonbValue	jbv;

		struct
		{
			int			type;
			Datum		value;
			Oid			typid;
			int32		typmod;
			int			tz;
		}			datetime;

		struct
		{
			int			type;
			double		val;
		}			dbl;
	} val;
} JsonItem;

#define JsonItemJbv(jsi)			(&(jsi)->val.jbv)
#define JsonItemBool(jsi)			(JsonItemJbv(jsi)->val.boolean)
#define JsonItemNumeric(jsi)		(JsonItemJbv(jsi)->val.numeric)
#define JsonItemNumericDatum(jsi)	NumericGetDatum(JsonItemNumeric(jsi))
#define JsonItemString(jsi)			(JsonItemJbv(jsi)->val.string)
#define JsonItemBinary(jsi)			(JsonItemJbv(jsi)->val.binary)
#define JsonItemArray(jsi)			(JsonItemJbv(jsi)->val.array)
#define JsonItemObject(jsi)			(JsonItemJbv(jsi)->val.object)
#define JsonItemDatetime(jsi)		((jsi)->val.datetime)
#define JsonItemDouble(jsi)			((jsi)->val.dbl.val)
#define JsonItemDoubleDatum(jsi)	Float8GetDatum(JsonItemDouble(jsi))

#define JsonItemGetType(jsi)		((jsi)->val.type)
#define JsonItemIsNull(jsi)			(JsonItemGetType(jsi) == jsiNull)
#define JsonItemIsBool(jsi)			(JsonItemGetType(jsi) == jsiBool)
#define JsonItemIsNumeric(jsi)		(JsonItemGetType(jsi) == jsiNumeric)
#define JsonItemIsString(jsi)		(JsonItemGetType(jsi) == jsiString)
#define JsonItemIsBinary(jsi)		(JsonItemGetType(jsi) == jsiBinary)
#define JsonItemIsArray(jsi)		(JsonItemGetType(jsi) == jsiArray)
#define JsonItemIsObject(jsi)		(JsonItemGetType(jsi) == jsiObject)
#define JsonItemIsDatetime(jsi)		(JsonItemGetType(jsi) == jsiDatetime)
#define JsonItemIsDouble(jsi)		(JsonItemGetType(jsi) == jsiDouble)
#define JsonItemIsScalar(jsi)		(IsAJsonbScalar(JsonItemJbv(jsi)) || \
									 JsonItemIsDatetime(jsi) || \
									 JsonItemIsDouble(jsi))
#define JsonItemIsNumber(jsi)		(JsonItemIsNumeric(jsi) || \
									 JsonItemIsDouble(jsi))

typedef union Jsonx
{
	Jsonb		jb;
	Json		js;
} Jsonx;

#define DatumGetJsonxP(datum, isJsonb) \
	((isJsonb) ? (Jsonx *) DatumGetJsonbP(datum) : (Jsonx *) DatumGetJsonP(datum))

typedef JsonbContainer JsonxContainer;

typedef struct JsonxIterator
{
	bool		isJsonb;
	union
	{
		JsonbIterator *jb;
		JsonIterator *js;
	}			it;
} JsonxIterator;

/*
 * Represents "base object" and it's "id" for .keyvalue() evaluation.
 */
typedef struct JsonBaseObjectInfo
{
	JsonxContainer *jbc;
	int			id;
} JsonBaseObjectInfo;

/*
 * Special data structure representing stack of current items.  We use it
 * instead of regular list in order to evade extra memory allocation.  These
 * items are always allocated in local variables.
 */
typedef struct JsonItemStackEntry
{
	JsonBaseObjectInfo base;
	JsonItem   *item;
	struct JsonItemStackEntry *parent;
} JsonItemStackEntry;

typedef JsonItemStackEntry *JsonItemStack;

typedef int (*JsonPathVarCallback) (void *vars, bool isJsonb,
									char *varName, int varNameLen,
									JsonItem *val, JsonbValue *baseObject);

typedef struct JsonLambdaArg
{
	struct JsonLambdaArg *next;
	JsonItem   *val;
	const char *name;
	int			namelen;
} JsonLambdaArg;

/*
 * Context of jsonpath execution.
 */
typedef struct JsonPathExecContext
{
	void	   *vars;			/* variables to substitute into jsonpath */
	JsonPathVarCallback getVar;
	JsonLambdaArg *args;		/* for lambda evaluation */
	JsonItem   *root;			/* for $ evaluation */
	JsonItemStack stack;		/* for @ evaluation */
	JsonBaseObjectInfo baseObject;	/* "base object" for .keyvalue()
									 * evaluation */
	int			lastGeneratedObjectId;	/* "id" counter for .keyvalue()
										 * evaluation */
	void	  **cache;
	MemoryContext cache_mcxt;
	int			innermostArraySize; /* for LAST array index evaluation */
	bool		laxMode;		/* true for "lax" mode, false for "strict"
								 * mode */
	bool		ignoreStructuralErrors; /* with "true" structural errors such
										 * as absence of required json item or
										 * unexpected json item type are
										 * ignored */
	bool		throwErrors;	/* with "false" all suppressible errors are
								 * suppressed */
	bool		isJsonb;
} JsonPathExecContext;

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#define jspStrictAbsenseOfErrors(cxt)	(!(cxt)->laxMode)
#define jspAutoUnwrap(cxt)				((cxt)->laxMode)
#define jspAutoWrap(cxt)				((cxt)->laxMode)
#define jspIgnoreStructuralErrors(cxt)	((cxt)->ignoreStructuralErrors)
#define jspThrowErrors(cxt)				((cxt)->throwErrors)

/* Result of jsonpath predicate evaluation */
typedef enum JsonPathBool
{
	jpbFalse = 0,
	jpbTrue = 1,
	jpbUnknown = 2
} JsonPathBool;

/* Result of jsonpath expression evaluation */
typedef enum JsonPathExecResult
{
	jperOk = 0,
	jperNotFound = 1,
	jperError = 2
} JsonPathExecResult;

#define jperIsError(jper)			((jper) == jperError)

/*
 * List of SQL/JSON items with shortcut for single-value list.
 */
typedef struct JsonValueList
{
	JsonItem   *head;
	JsonItem   *tail;
	int			length;
} JsonValueList;

typedef struct JsonValueListIterator
{
	JsonItem   *next;
} JsonValueListIterator;

typedef struct JsonPathFuncContext
{
	JsonPathExecContext *cxt;
	JsonValueList  *result;
	const char	   *funcname;
	JsonItem	   *jb;
	JsonItem	   *item;
	JsonPathItem   *args;
	void		  **argscache;
	int				nargs;
} JsonPathFuncContext;


extern JsonItem *JsonbValueToJsonItem(JsonbValue *jbv, JsonItem *jsi);
extern Jsonb *JsonItemToJsonb(JsonItem *jsi);
extern Json *JsonItemToJson(JsonItem *jsi);
extern void JsonItemFromDatum(Datum val, Oid typid, int32 typmod,
				  JsonItem *res, bool isJsonb);
extern Datum JsonItemToJsonxDatum(JsonItem *jsi, bool isJsonb);
extern Datum JsonbValueToJsonxDatum(JsonbValue *jbv, bool isJsonb);

extern bool JsonPathExists(Datum jb, JsonPath *path,
			   List *vars, bool isJsonb, bool *error);
extern Datum JsonPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper,
			   bool *empty, bool *error, List *vars, bool isJsonb);
extern JsonItem *JsonPathValue(Datum jb, JsonPath *jp, bool *empty,
			   bool *error, List *vars, bool isJsonb);

extern int EvalJsonPathVar(void *vars, bool isJsonb, char *varName,
				int varNameLen, JsonItem *val, JsonbValue *baseObject);

extern const TableFuncRoutine JsonTableRoutine;
extern const TableFuncRoutine JsonbTableRoutine;

extern int JsonbType(JsonItem *jb);
extern int JsonxArraySize(JsonItem *jb, bool isJsonb);

extern JsonItem *copyJsonItem(JsonItem *src);
extern JsonItem *JsonWrapItemInArray(JsonItem *jbv, bool isJsonb);
extern JsonbValue *JsonWrapItemsInArray(const JsonValueList *items,
										bool isJsonb);
extern void JsonAppendWrappedItems(JsonValueList *found, JsonValueList *items,
								   bool isJsonb);

extern void pushJsonItem(JsonItemStack *stack, JsonItemStackEntry *entry,
						 JsonItem *item, JsonBaseObjectInfo *base);
extern void popJsonItem(JsonItemStack *stack);

#define JsonValueListLength(jvl) ((jvl)->length)
#define JsonValueListIsEmpty(jvl) (!(jvl)->length)
#define JsonValueListHead(jvl) ((jvl)->head)
extern void JsonValueListClear(JsonValueList *jvl);
extern void JsonValueListAppend(JsonValueList *jvl, JsonItem *jbv);
extern List *JsonValueListGetList(JsonValueList *jvl);
extern void JsonValueListInitIterator(const JsonValueList *jvl,
									  JsonValueListIterator *it);
extern JsonItem *JsonValueListNext(const JsonValueList *jvl,
								   JsonValueListIterator *it);
extern void JsonValueListConcat(JsonValueList *jvl1, JsonValueList jvl2);
extern void JsonxIteratorInit(JsonxIterator *it, JsonxContainer *jxc,
							  bool isJsonb);
extern JsonbIteratorToken JsonxIteratorNext(JsonxIterator *it, JsonbValue *jbv,
											bool skipNested);

extern JsonPathExecResult jspExecuteItem(JsonPathExecContext *cxt,
										 JsonPathItem *jsp, JsonItem *jb,
										 JsonValueList *found);
extern JsonPathExecResult jspExecuteItemNested(JsonPathExecContext *cxt,
											   JsonPathItem *jsp, JsonItem *jb,
											   JsonValueList *found);
extern JsonPathExecResult jspExecuteLambda(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonItem *jb,
										   JsonValueList *found,
										   JsonItem **params, int nparams,
										   void **pcache);
extern JsonPathBool jspCompareItems(int32 op, JsonItem *jb1, JsonItem *jb2);

/* Standard error message for SQL/JSON errors */
#define ERRMSG_JSON_ARRAY_NOT_FOUND			"SQL/JSON array not found"
#define ERRMSG_JSON_OBJECT_NOT_FOUND		"SQL/JSON object not found"
#define ERRMSG_JSON_MEMBER_NOT_FOUND		"SQL/JSON member not found"
#define ERRMSG_JSON_NUMBER_NOT_FOUND		"SQL/JSON number not found"
#define ERRMSG_JSON_SCALAR_REQUIRED			"SQL/JSON scalar required"
#define ERRMSG_MORE_THAN_ONE_JSON_ITEM		"more than one SQL/JSON item"
#define ERRMSG_SINGLETON_JSON_ITEM_REQUIRED	"singleton SQL/JSON item required"
#define ERRMSG_NON_NUMERIC_JSON_ITEM		"non-numeric SQL/JSON item"
#define ERRMSG_INVALID_JSON_SUBSCRIPT		"invalid SQL/JSON subscript"
#define ERRMSG_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION	\
	"invalid argument for SQL/JSON datetime function"
#define ERRMSG_NO_JSON_ITEM					"no SQL/JSON item"

#endif
