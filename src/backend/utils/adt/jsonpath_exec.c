/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.c
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/json.h"
#include "utils/jsonpath.h"
#include "utils/varlena.h"

#ifdef JSONPATH_JSON_C
#define JSONXOID JSONOID
#else
#define JSONXOID JSONBOID

/* Special pseudo-ErrorData with zero sqlerrcode for existence queries. */
ErrorData jperNotFound[1];
#endif


typedef struct JsonPathExecContext
{
	List	   *vars;
	JsonbValue *root;				/* for $ evaluation */
	int			innermostArraySize;	/* for LAST array index evaluation */
	bool		laxMode;
	bool		ignoreStructuralErrors;
} JsonPathExecContext;

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#define jspStrictAbsenseOfErrors(cxt)	(!(cxt)->laxMode)
#define jspAutoUnwrap(cxt)				((cxt)->laxMode)
#define jspAutoWrap(cxt)				((cxt)->laxMode)
#define jspIgnoreStructuralErrors(cxt)	((cxt)->ignoreStructuralErrors)

typedef struct JsonValueListIterator
{
	ListCell   *lcell;
} JsonValueListIterator;

#define JsonValueListIteratorEnd ((ListCell *) -1)

static inline JsonPathExecResult recursiveExecute(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonbValue *jb,
										   JsonValueList *found);

static inline JsonPathExecResult recursiveExecuteUnwrap(JsonPathExecContext *cxt,
							JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);

static inline JsonbValue *wrapItemsInArray(const JsonValueList *items);


static inline void
JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv)
{
	if (jvl->singleton)
	{
		jvl->list = list_make2(jvl->singleton, jbv);
		jvl->singleton = NULL;
	}
	else if (!jvl->list)
		jvl->singleton = jbv;
	else
		jvl->list = lappend(jvl->list, jbv);
}

static inline void
JsonValueListConcat(JsonValueList *jvl1, JsonValueList jvl2)
{
	if (jvl1->singleton)
	{
		if (jvl2.singleton)
			jvl1->list = list_make2(jvl1->singleton, jvl2.singleton);
		else
			jvl1->list = lcons(jvl1->singleton, jvl2.list);

		jvl1->singleton = NULL;
	}
	else if (jvl2.singleton)
	{
		if (jvl1->list)
			jvl1->list = lappend(jvl1->list, jvl2.singleton);
		else
			jvl1->singleton = jvl2.singleton;
	}
	else if (jvl1->list)
		jvl1->list = list_concat(jvl1->list, jvl2.list);
	else
		jvl1->list = jvl2.list;
}

static inline int
JsonValueListLength(const JsonValueList *jvl)
{
	return jvl->singleton ? 1 : list_length(jvl->list);
}

static inline bool
JsonValueListIsEmpty(JsonValueList *jvl)
{
	return !jvl->singleton && list_length(jvl->list) <= 0;
}

static inline JsonbValue *
JsonValueListHead(JsonValueList *jvl)
{
	return jvl->singleton ? jvl->singleton : linitial(jvl->list);
}

static inline void
JsonValueListClear(JsonValueList *jvl)
{
	jvl->singleton = NULL;
	jvl->list = NIL;
}

static inline List *
JsonValueListGetList(JsonValueList *jvl)
{
	if (jvl->singleton)
		return list_make1(jvl->singleton);

	return jvl->list;
}

/*
 * Get the next item from the sequence advancing iterator.
 */
static inline JsonbValue *
JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)
{
	if (it->lcell == JsonValueListIteratorEnd)
		return NULL;

	if (it->lcell)
		it->lcell = lnext(it->lcell);
	else
	{
		if (jvl->singleton)
		{
			it->lcell = JsonValueListIteratorEnd;
			return jvl->singleton;
		}

		it->lcell = list_head(jvl->list);
	}

	if (!it->lcell)
	{
		it->lcell = JsonValueListIteratorEnd;
		return NULL;
	}

	return lfirst(it->lcell);
}

#ifndef JSONPATH_JSON_C
/*
 * Initialize a binary JsonbValue with the given jsonb container.
 */
static inline JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = &jb->root;
	jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

	return jbv;
}
#endif

/*
 * Transform a JsonbValue into a binary JsonbValue by encoding it to a
 * binary jsonb container.
 */
static inline JsonbValue *
JsonbWrapInBinary(JsonbValue *jbv, JsonbValue *out)
{
	Jsonb	   *jb = JsonbValueToJsonb(jbv);

	if (!out)
		out = palloc(sizeof(*out));

	return JsonbInitBinary(out, jb);
}

/********************Execute functions for JsonPath***************************/

/*
 * Find value of jsonpath variable in a list of passing params
 */
static void
computeJsonPathVariable(JsonPathItem *variable, List *vars, JsonbValue *value)
{
	ListCell			*cell;
	JsonPathVariable	*var = NULL;
	bool				isNull;
	Datum				computedValue;
	char				*varName;
	int					varNameLength;

	Assert(variable->type == jpiVariable);
	varName = jspGetString(variable, &varNameLength);

	foreach(cell, vars)
	{
		var = (JsonPathVariable*)lfirst(cell);

		if (varNameLength == VARSIZE_ANY_EXHDR(var->varName) &&
			!strncmp(varName, VARDATA_ANY(var->varName), varNameLength))
			break;

		var = NULL;
	}

	if (var == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("could not find '%s' passed variable",
						pnstrdup(varName, varNameLength))));

	computedValue = var->cb(var->cb_arg, &isNull);

	if (isNull)
	{
		value->type = jbvNull;
		return;
	}

	switch(var->typid)
	{
		case BOOLOID:
			value->type = jbvBool;
			value->val.boolean = DatumGetBool(computedValue);
			break;
		case NUMERICOID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(computedValue);
			break;
			break;
		case INT2OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												int2_numeric, computedValue));
			break;
		case INT4OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												int4_numeric, computedValue));
			break;
		case INT8OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												int8_numeric, computedValue));
			break;
		case FLOAT4OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												float4_numeric, computedValue));
			break;
		case FLOAT8OID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(DirectFunctionCall1(
												float4_numeric, computedValue));
			break;
		case TEXTOID:
		case VARCHAROID:
			value->type = jbvString;
			value->val.string.val = VARDATA_ANY(computedValue);
			value->val.string.len = VARSIZE_ANY_EXHDR(computedValue);
			break;
		case DATEOID:
		case TIMEOID:
		case TIMETZOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			value->type = jbvDatetime;
			value->val.datetime.typid = var->typid;
			value->val.datetime.typmod = var->typmod;
			value->val.datetime.tz = 0;
			value->val.datetime.value = computedValue;
			break;
		case JSONXOID:
			{
				Jsonb	   *jb = DatumGetJsonbP(computedValue);

				if (JB_ROOT_IS_SCALAR(jb))
					JsonbExtractScalar(&jb->root, value);
				else
					JsonbInitBinary(value, jb);
			}
			break;
		case (Oid) -1: /* raw JsonbValue */
			*value = *(JsonbValue *) DatumGetPointer(computedValue);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("only bool, numeric and text types could be casted to supported jsonpath types")));
	}
}

/*
 * Convert jsonpath's scalar or variable node to actual jsonb value
 */
static void
computeJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item, JsonbValue *value)
{
	switch(item->type)
	{
		case jpiNull:
			value->type = jbvNull;
			break;
		case jpiBool:
			value->type = jbvBool;
			value->val.boolean = jspGetBool(item);
			break;
		case jpiNumeric:
			value->type = jbvNumeric;
			value->val.numeric = jspGetNumeric(item);
			break;
		case jpiString:
			value->type = jbvString;
			value->val.string.val = jspGetString(item, &value->val.string.len);
			break;
		case jpiVariable:
			computeJsonPathVariable(item, cxt->vars, value);
			break;
		default:
			elog(ERROR, "Wrong type");
	}
}


/*
 * Returns jbv* type of of JsonbValue. Note, it never returns
 * jbvBinary as is - jbvBinary is used as mark of store naked
 * scalar value. To improve readability it defines jbvScalar
 * as alias to jbvBinary
 */
#define jbvScalar jbvBinary
static inline int
JsonbType(JsonbValue *jb)
{
	int type = jb->type;

	if (jb->type == jbvBinary)
	{
		JsonbContainer	*jbc = (void *) jb->val.binary.data;

		if (JsonContainerIsScalar(jbc))
			type = jbvScalar;
		else if (JsonContainerIsObject(jbc))
			type = jbvObject;
		else if (JsonContainerIsArray(jbc))
			type = jbvArray;
		else
			elog(ERROR, "Unknown container type: 0x%08x", jbc->header);
	}

	return type;
}

/*
 * Get the type name of a SQL/JSON item.
 */
static const char *
JsonbTypeName(JsonbValue *jb)
{
	JsonbValue jbvbuf;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = (void *) jb->val.binary.data;

		if (JsonContainerIsScalar(jbc))
			jb = JsonbExtractScalar(jbc, &jbvbuf);
		else if (JsonContainerIsArray(jbc))
			return "array";
		else if (JsonContainerIsObject(jbc))
			return "object";
		else
			elog(ERROR, "Unknown container type: 0x%08x", jbc->header);
	}

	switch (jb->type)
	{
		case jbvObject:
			return "object";
		case jbvArray:
			return "array";
		case jbvNumeric:
			return "number";
		case jbvString:
			return "string";
		case jbvBool:
			return "boolean";
		case jbvNull:
			return "null";
		case jbvDatetime:
			switch (jb->val.datetime.typid)
			{
				case DATEOID:
					return "date";
				case TIMEOID:
					return "time without time zone";
				case TIMETZOID:
					return "time with time zone";
				case TIMESTAMPOID:
					return "timestamp without time zone";
				case TIMESTAMPTZOID:
					return "timestamp with time zone";
				default:
					elog(ERROR, "unknown jsonb value datetime type oid %d",
						 jb->val.datetime.typid);
			}
			return "unknown";
		default:
			elog(ERROR, "Unknown jsonb value type: %d", jb->type);
			return "unknown";
	}
}

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
static int
JsonbArraySize(JsonbValue *jb)
{
	if (jb->type == jbvArray)
		return jb->val.array.nElems;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc =  (void *) jb->val.binary.data;

		if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
			return JsonContainerSize(jbc);
	}

	return -1;
}

/*
 * Compare two numerics.
 */
static int
compareNumeric(Numeric a, Numeric b)
{
	return	DatumGetInt32(
				DirectFunctionCall2(
					numeric_cmp,
					PointerGetDatum(a),
					PointerGetDatum(b)
				)
			);
}

/*
 * Cross-type comparison of two datetime SQL/JSON items.  If items are
 * uncomparable, 'error' flag is set.
 */
static int
compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2, bool *error)
{
	PGFunction	cmpfunc = NULL;

	switch (typid1)
	{
		case DATEOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = date_cmp;
					break;
				case TIMESTAMPOID:
					cmpfunc = date_cmp_timestamp;
					break;
				case TIMESTAMPTZOID:
					cmpfunc = date_cmp_timestamptz;
					break;
				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMEOID:
			switch (typid2)
			{
				case TIMEOID:
					cmpfunc = time_cmp;
					break;
				case TIMETZOID:
					val1 = DirectFunctionCall1(time_timetz, val1);
					cmpfunc = timetz_cmp;
					break;
				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMETZOID:
			switch (typid2)
			{
				case TIMEOID:
					val2 = DirectFunctionCall1(time_timetz, val2);
					cmpfunc = timetz_cmp;
					break;
				case TIMETZOID:
					cmpfunc = timetz_cmp;
					break;
				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMESTAMPOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = timestamp_cmp_date;
					break;
				case TIMESTAMPOID:
					cmpfunc = timestamp_cmp;
					break;
				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp_timestamptz;
					break;
				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMESTAMPTZOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = timestamptz_cmp_date;
					break;
				case TIMESTAMPOID:
					cmpfunc = timestamptz_cmp_timestamp;
					break;
				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp;
					break;
				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		default:
			elog(ERROR, "unknown SQL/JSON datetime type oid: %d", typid1);
	}

	if (!cmpfunc)
		elog(ERROR, "unknown SQL/JSON datetime type oid: %d", typid2);

	*error = false;

	return DatumGetInt32(DirectFunctionCall2(cmpfunc, val1, val2));
}

/*
 * Check equality of two SLQ/JSON items of the same type.
 */
static inline JsonPathBool
checkEquality(JsonbValue *jb1, JsonbValue *jb2, bool not)
{
	bool	eq = false;

	if (jb1->type != jb2->type)
	{
		if (jb1->type == jbvNull || jb2->type == jbvNull)
			return not ? jpbTrue : jpbFalse;

		return jpbUnknown;
	}

	switch (jb1->type)
	{
		case jbvNull:
			eq = true;
			break;
		case jbvString:
			eq = (jb1->val.string.len == jb2->val.string.len &&
					memcmp(jb2->val.string.val, jb1->val.string.val,
						   jb1->val.string.len) == 0);
			break;
		case jbvBool:
			eq = (jb2->val.boolean == jb1->val.boolean);
			break;
		case jbvNumeric:
			eq = (compareNumeric(jb1->val.numeric, jb2->val.numeric) == 0);
			break;
		case jbvDatetime:
			{
				bool		error;

				eq = compareDatetime(jb1->val.datetime.value,
									 jb1->val.datetime.typid,
									 jb2->val.datetime.value,
									 jb2->val.datetime.typid,
									 &error) == 0;

				if (error)
					return jpbUnknown;

				break;
			}

		case jbvBinary:
		case jbvObject:
		case jbvArray:
			return jpbUnknown;

		default:
			elog(ERROR, "Unknown jsonb value type %d", jb1->type);
	}

	return (not ^ eq) ? jpbTrue : jpbFalse;
}

/*
 * Compare two SLQ/JSON items using comparison operation 'op'.
 */
static JsonPathBool
makeCompare(int32 op, JsonbValue *jb1, JsonbValue *jb2)
{
	int			cmp;
	bool		res;

	if (jb1->type != jb2->type)
	{
		if (jb1->type != jbvNull && jb2->type != jbvNull)
			/* non-null items of different types are not order-comparable */
			return jpbUnknown;

		if (jb1->type != jbvNull || jb2->type != jbvNull)
			/* comparison of nulls to non-nulls returns always false */
			return jpbFalse;

		/* both values are JSON nulls */
	}

	switch (jb1->type)
	{
		case jbvNull:
			cmp = 0;
			break;
		case jbvNumeric:
			cmp = compareNumeric(jb1->val.numeric, jb2->val.numeric);
			break;
		case jbvString:
			cmp = varstr_cmp(jb1->val.string.val, jb1->val.string.len,
							 jb2->val.string.val, jb2->val.string.len,
							 DEFAULT_COLLATION_OID);
			break;
		case jbvDatetime:
			{
				bool		error;

				cmp = compareDatetime(jb1->val.datetime.value,
									  jb1->val.datetime.typid,
									  jb2->val.datetime.value,
									  jb2->val.datetime.typid,
									  &error);

				if (error)
					return jpbUnknown;
			}
			break;
		default:
			return jpbUnknown;
	}

	switch (op)
	{
		case jpiEqual:
			res = (cmp == 0);
			break;
		case jpiNotEqual:
			res = (cmp != 0);
			break;
		case jpiLess:
			res = (cmp < 0);
			break;
		case jpiGreater:
			res = (cmp > 0);
			break;
		case jpiLessOrEqual:
			res = (cmp <= 0);
			break;
		case jpiGreaterOrEqual:
			res = (cmp >= 0);
			break;
		default:
			elog(ERROR, "Unknown operation");
			return jpbUnknown;
	}

	return res ? jpbTrue : jpbFalse;
}

static JsonbValue *
copyJsonbValue(JsonbValue *src)
{
	JsonbValue	*dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
}

/*
 * Execute next jsonpath item if it does exist.
 */
static inline JsonPathExecResult
recursiveExecuteNext(JsonPathExecContext *cxt,
					 JsonPathItem *cur, JsonPathItem *next,
					 JsonbValue *v, JsonValueList *found, bool copy)
{
	JsonPathItem elem;
	bool		hasNext;

	if (!cur)
		hasNext = next != NULL;
	else if (next)
		hasNext = jspHasNext(cur);
	else
	{
		next = &elem;
		hasNext = jspGetNext(cur, next);
	}

	if (hasNext)
		return recursiveExecute(cxt, next, v, found);

	if (found)
		JsonValueListAppend(found, copy ? copyJsonbValue(v) : v);

	return jperOk;
}

/*
 * Execute jsonpath expression and automatically unwrap each array item from
 * the resulting sequence in lax mode.
 */
static inline JsonPathExecResult
recursiveExecuteAndUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb, JsonValueList *found)
{
	if (jspAutoUnwrap(cxt))
	{
		JsonValueList seq = { 0 };
		JsonValueListIterator it = { 0 };
		JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &seq);
		JsonbValue *item;

		if (jperIsError(res))
			return res;

		while ((item = JsonValueListNext(&seq, &it)))
		{
			if (item->type == jbvArray)
			{
				JsonbValue *elem = item->val.array.elems;
				JsonbValue *last = elem + item->val.array.nElems;

				for (; elem < last; elem++)
					JsonValueListAppend(found, copyJsonbValue(elem));
			}
			else if (item->type == jbvBinary &&
					 JsonContainerIsArray(item->val.binary.data))
			{
				JsonbValue	elem;
				JsonbIterator *it = JsonbIteratorInit(item->val.binary.data);
				JsonbIteratorToken tok;

				while ((tok = JsonbIteratorNext(&it, &elem, true)) != WJB_DONE)
				{
					if (tok == WJB_ELEM)
						JsonValueListAppend(found, copyJsonbValue(&elem));
				}
			}
			else
				JsonValueListAppend(found, item);
		}

		return jperOk;
	}

	return recursiveExecute(cxt, jsp, jb, found);
}

/*
 * Execute comparison expression.  True is returned only if found any pair of
 * items from the left and right operand's sequences which is satisfying
 * condition.  In strict mode all pairs should be comparable, otherwise an error
 * is returned.
 */
static JsonPathBool
executeComparison(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	JsonValueList lseq = { 0 };
	JsonValueList rseq = { 0 };
	JsonValueListIterator lseqit = { 0 };
	JsonbValue *lval;
	bool		error = false;
	bool		found = false;

	jspGetLeftArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperReplace(res, jpbUnknown);

	jspGetRightArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &rseq);
	if (jperIsError(res))
		return jperReplace(res, jpbUnknown);

	while ((lval = JsonValueListNext(&lseq, &lseqit)))
	{
		JsonValueListIterator rseqit = { 0 };
		JsonbValue *rval;

		while ((rval = JsonValueListNext(&rseq, &rseqit)))
		{
			JsonPathBool cmp;

			switch (jsp->type)
			{
				case jpiEqual:
					cmp = checkEquality(lval, rval, false);
					break;
				case jpiNotEqual:
					cmp = checkEquality(lval, rval, true);
					break;
				case jpiLess:
				case jpiGreater:
				case jpiLessOrEqual:
				case jpiGreaterOrEqual:
					cmp = makeCompare(jsp->type, lval, rval);
					break;
				default:
					elog(ERROR, "Unknown operation");
					cmp = jpbUnknown;
					break;
			}

			if (cmp == jpbTrue)
			{
				if (!jspStrictAbsenseOfErrors(cxt))
					return jpbTrue;

				found = true;
			}
			else if (cmp == jpbUnknown)
			{
				if (jspStrictAbsenseOfErrors(cxt))
					return jpbUnknown;

				error = true;
			}
		}
	}

	if (found) /* possible only in strict mode */
		return jpbTrue;

	if (error) /* possible only in lax mode */
		return jpbUnknown;

	return jpbFalse;
}

/*
 * Execute binary arithemitc expression on singleton numeric operands.
 * Array operands are automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeBinaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
						JsonbValue *jb, JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathItem elem;
	JsonValueList lseq = { 0 };
	JsonValueList rseq = { 0 };
	JsonbValue *lval;
	JsonbValue *rval;
	JsonbValue	lvalbuf;
	JsonbValue	rvalbuf;
	Numeric	  (*func)(Numeric, Numeric, ErrorData **);
	Numeric		res;
	bool		hasNext;
	ErrorData  *edata;

	jspGetLeftArg(jsp, &elem);

	/* XXX by standard unwrapped only operands of multiplicative expressions */
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);

	if (jper == jperOk)
	{
		jspGetRightArg(jsp, &elem);
		jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &rseq); /* XXX */
	}

	if (jper != jperOk ||
		JsonValueListLength(&lseq) != 1 ||
		JsonValueListLength(&rseq) != 1)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	lval = JsonValueListHead(&lseq);

	if (JsonbType(lval) == jbvScalar)
		lval = JsonbExtractScalar(lval->val.binary.data, &lvalbuf);

	if (lval->type != jbvNumeric)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	rval = JsonValueListHead(&rseq);

	if (JsonbType(rval) == jbvScalar)
		rval = JsonbExtractScalar(rval->val.binary.data, &rvalbuf);

	if (rval->type != jbvNumeric)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	hasNext = jspGetNext(jsp, &elem);

	if (!found && !hasNext)
		return jperOk;

	switch (jsp->type)
	{
		case jpiAdd:
			func = numeric_add_internal;
			break;
		case jpiSub:
			func = numeric_sub_internal;
			break;
		case jpiMul:
			func = numeric_mul_internal;
			break;
		case jpiDiv:
			func = numeric_div_internal;
			break;
		case jpiMod:
			func = numeric_mod_internal;
			break;
		default:
			elog(ERROR, "unknown jsonpath arithmetic operation %d", jsp->type);
			func = NULL;
			break;
	}

	edata = NULL;
	res = func(lval->val.numeric, rval->val.numeric, &edata);

	if (edata)
		return jperMakeErrorData(edata);

	lval = palloc(sizeof(*lval));
	lval->type = jbvNumeric;
	lval->val.numeric = res;

	return recursiveExecuteNext(cxt, jsp, &elem, lval, found, false);
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb,  JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathExecResult jper2;
	JsonPathItem elem;
	JsonValueList seq = { 0 };
	JsonValueListIterator it = { 0 };
	JsonbValue *val;
	bool		hasNext;

	jspGetArg(jsp, &elem);
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &seq);

	if (jperIsError(jper))
		return jperReplace(jper, jperMakeError(ERRCODE_JSON_NUMBER_NOT_FOUND));

	jper = jperNotFound;

	hasNext = jspGetNext(jsp, &elem);

	while ((val = JsonValueListNext(&seq, &it)))
	{
		if (JsonbType(val) == jbvScalar)
			JsonbExtractScalar(val->val.binary.data, val);

		if (val->type == jbvNumeric)
		{
			if (!found && !hasNext)
				return jperOk;
		}
		else if (!found && !hasNext)
			continue; /* skip non-numerics processing */

		if (val->type != jbvNumeric)
			return jperMakeError(ERRCODE_JSON_NUMBER_NOT_FOUND);

		switch (jsp->type)
		{
			case jpiPlus:
				break;
			case jpiMinus:
				val->val.numeric =
					DatumGetNumeric(DirectFunctionCall1(
						numeric_uminus, NumericGetDatum(val->val.numeric)));
				break;
			default:
				elog(ERROR, "unknown jsonpath arithmetic operation %d", jsp->type);
		}

		jper2 = recursiveExecuteNext(cxt, jsp, &elem, val, found, false);

		if (jperIsError(jper2))
			return jper2;

		if (jper2 == jperOk)
		{
			if (!found)
				return jperOk;
			jper = jperOk;
		}
	}

	return jper;
}

/*
 * implements jpiAny node (** operator)
 */
static JsonPathExecResult
recursiveAny(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			 JsonValueList *found, uint32 level, uint32 first, uint32 last)
{
	JsonPathExecResult	res = jperNotFound;
	JsonbIterator		*it;
	int32				r;
	JsonbValue			v;

	check_stack_depth();

	if (level > last)
		return res;

	it = JsonbIteratorInit(jb->val.binary.data);

	/*
	 * Recursively iterate over jsonb objects/arrays
	 */
	while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			r = JsonbIteratorNext(&it, &v, true);
			Assert(r == WJB_VALUE);
		}

		if (r == WJB_VALUE || r == WJB_ELEM)
		{

			if (level >= first ||
				(first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
				 v.type != jbvBinary))	/* leaves only requested */
			{
				/* check expression */
				bool		ignoreStructuralErrors = cxt->ignoreStructuralErrors;

				cxt->ignoreStructuralErrors = true;
				res = recursiveExecuteNext(cxt, NULL, jsp, &v, found, true);
				cxt->ignoreStructuralErrors = ignoreStructuralErrors;

				if (jperIsError(res))
					break;

				if (res == jperOk && !found)
					break;
			}

			if (level < last && v.type == jbvBinary)
			{
				res = recursiveAny(cxt, jsp, &v, found, level + 1, first, last);

				if (jperIsError(res))
					break;

				if (res == jperOk && found == NULL)
					break;
			}
		}
	}

	return res;
}

/*
 * Execute array subscript expression and convert resulting numeric item to the
 * integer type with truncation.
 */
static JsonPathExecResult
getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			  int32 *index)
{
	JsonbValue *jbv;
	JsonValueList found = { 0 };
	JsonbValue	tmp;
	JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &found);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&found) != 1)
		return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

	jbv = JsonValueListHead(&found);

	if (JsonbType(jbv) == jbvScalar)
		jbv = JsonbExtractScalar(jbv->val.binary.data, &tmp);

	if (jbv->type != jbvNumeric)
		return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

	*index = DatumGetInt32(DirectFunctionCall1(numeric_int4,
							DirectFunctionCall2(numeric_trunc,
											NumericGetDatum(jbv->val.numeric),
											Int32GetDatum(0))));

	return jperOk;
}

static JsonPathBool
executeStartsWithPredicate(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	JsonValueList lseq = { 0 };
	JsonValueList rseq = { 0 };
	JsonValueListIterator lit = { 0 };
	JsonbValue *whole;
	JsonbValue *initial;
	JsonbValue	initialbuf;
	bool		error = false;
	bool		found = false;

	jspGetRightArg(jsp, &elem);
	res = recursiveExecute(cxt, &elem, jb, &rseq);
	if (jperIsError(res))
		return jperReplace(res, jpbUnknown);

	if (JsonValueListLength(&rseq) != 1)
		return jpbUnknown;

	initial = JsonValueListHead(&rseq);

	if (JsonbType(initial) == jbvScalar)
		initial = JsonbExtractScalar(initial->val.binary.data, &initialbuf);

	if (initial->type != jbvString)
		return jpbUnknown;

	jspGetLeftArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperReplace(res, jpbUnknown);

	while ((whole = JsonValueListNext(&lseq, &lit)))
	{
		JsonbValue	wholebuf;

		if (JsonbType(whole) == jbvScalar)
			whole = JsonbExtractScalar(whole->val.binary.data, &wholebuf);

		if (whole->type != jbvString)
		{
			if (jspStrictAbsenseOfErrors(cxt))
				return jpbUnknown;

			error = true;
		}
		else if (whole->val.string.len >= initial->val.string.len &&
				 !memcmp(whole->val.string.val,
						 initial->val.string.val,
						 initial->val.string.len))
		{
			if (!jspStrictAbsenseOfErrors(cxt))
				return jpbTrue;

			found = true;
		}
	}

	if (found) /* possible only in strict mode */
		return jpbTrue;

	if (error) /* possible only in lax mode */
		return jpbUnknown;

	return jpbFalse;
}

static JsonPathBool
executeLikeRegexPredicate(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	JsonValueList seq = { 0 };
	JsonValueListIterator it = { 0 };
	JsonbValue *str;
	text	   *regex;
	uint32		flags = jsp->content.like_regex.flags;
	int			cflags = REG_ADVANCED;
	bool		error = false;
	bool		found = false;

	if (flags & JSP_REGEX_ICASE)
		cflags |= REG_ICASE;
	if (flags & JSP_REGEX_MLINE)
		cflags |= REG_NEWLINE;
	if (flags & JSP_REGEX_SLINE)
		cflags &= ~REG_NEWLINE;
	if (flags & JSP_REGEX_WSPACE)
		cflags |= REG_EXPANDED;

	regex = cstring_to_text_with_len(jsp->content.like_regex.pattern,
									 jsp->content.like_regex.patternlen);

	jspInitByBuffer(&elem, jsp->base, jsp->content.like_regex.expr);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &seq);
	if (jperIsError(res))
		return jperReplace(res, jpbUnknown);

	while ((str = JsonValueListNext(&seq, &it)))
	{
		JsonbValue	strbuf;

		if (JsonbType(str) == jbvScalar)
			str = JsonbExtractScalar(str->val.binary.data, &strbuf);

		if (str->type != jbvString)
		{
			if (jspStrictAbsenseOfErrors(cxt))
				return jpbUnknown;

			error = true;
		}
		else if (RE_compile_and_execute(regex, str->val.string.val,
										str->val.string.len, cflags,
										DEFAULT_COLLATION_OID, 0, NULL))
		{
			if (!jspStrictAbsenseOfErrors(cxt))
				return jpbTrue;

			found = true;
		}
	}

	if (found) /* possible only in strict mode */
		return jpbTrue;

	if (error) /* possible only in lax mode */
		return jpbUnknown;

	return jpbFalse;
}

/*
 * Try to parse datetime text with the specified datetime template and
 * default time-zone 'tzname'.
 * Returns 'value' datum, its type 'typid' and 'typmod'.
 */
static bool
tryToParseDatetime(const char *fmt, int fmtlen, text *datetime, char *tzname,
				   bool strict, Datum *value, Oid *typid, int32 *typmod, int *tz)
{
	MemoryContext mcxt = CurrentMemoryContext;
	bool		ok = false;

	PG_TRY();
	{
		*value = to_datetime(datetime, fmt, fmtlen, tzname, strict,
							 typid, typmod, tz);
		ok = true;
	}
	PG_CATCH();
	{
		if (ERRCODE_TO_CATEGORY(geterrcode()) != ERRCODE_DATA_EXCEPTION)
			PG_RE_THROW();

		FlushErrorState();
		MemoryContextSwitchTo(mcxt);
	}
	PG_END_TRY();

	return ok;
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next jsonpath.
 */
static inline JsonPathExecResult
appendBoolResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
				 JsonValueList *found, JsonPathBool res)
{
	JsonPathItem next;
	JsonbValue	jbv;
	bool		hasNext = jspGetNext(jsp, &next);

	if (!found && !hasNext)
		return jperOk;	/* found singleton boolean value */

	if (res == jpbUnknown)
	{
		jbv.type = jbvNull;
	}
	else
	{
		jbv.type = jbvBool;
		jbv.val.boolean = res == jpbTrue;
	}

	return recursiveExecuteNext(cxt, jsp, &next, &jbv, found, true);
}

/* Execute boolean-valued jsonpath expression. */
static inline JsonPathBool
recursiveExecuteBool(JsonPathExecContext *cxt, JsonPathItem *jsp,
					 JsonbValue *jb, bool canHaveNext)
{
	JsonPathItem arg;
	JsonPathBool res;
	JsonPathBool res2;

	if (!canHaveNext && jspHasNext(jsp))
		elog(ERROR, "boolean jsonpath item can not have next item");

	switch (jsp->type)
	{
		case jpiAnd:
			jspGetLeftArg(jsp, &arg);
			res = recursiveExecuteBool(cxt, &arg, jb, false);

			if (res == jpbFalse)
				return jpbFalse;

			/*
			 * SQL/JSON says that we should check second arg
			 * in case of jperError
			 */

			jspGetRightArg(jsp, &arg);
			res2 = recursiveExecuteBool(cxt, &arg, jb, false);

			return res2 == jpbTrue ? res : res2;

		case jpiOr:
			jspGetLeftArg(jsp, &arg);
			res = recursiveExecuteBool(cxt, &arg, jb, false);

			if (res == jpbTrue)
				return jpbTrue;

			jspGetRightArg(jsp, &arg);
			res2 = recursiveExecuteBool(cxt, &arg, jb, false);

			return res2 == jpbFalse ? res : res2;

		case jpiNot:
			jspGetArg(jsp, &arg);

			res = recursiveExecuteBool(cxt, &arg, jb, false);

			if (res == jpbUnknown)
				return jpbUnknown;

			return res == jpbTrue ? jpbFalse : jpbTrue;

		case jpiIsUnknown:
			jspGetArg(jsp, &arg);
			res = recursiveExecuteBool(cxt, &arg, jb, false);
			return res == jpbUnknown ? jpbTrue : jpbFalse;

		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			return executeComparison(cxt, jsp, jb);

		case jpiStartsWith:
			return executeStartsWithPredicate(cxt, jsp, jb);

		case jpiLikeRegex:
			return executeLikeRegexPredicate(cxt, jsp, jb);

		case jpiExists:
			jspGetArg(jsp, &arg);

			if (jspStrictAbsenseOfErrors(cxt))
			{
				/*
				 * In strict mode we must get a complete list of values
				 * to check that there are no errors at all.
				 */
				JsonValueList vals = { 0 };
				JsonPathExecResult res =
					recursiveExecute(cxt, &arg, jb, &vals);

				if (jperIsError(res))
					return jperReplace(res, jpbUnknown);

				return JsonValueListIsEmpty(&vals) ? jpbFalse : jpbTrue;
			}
			else
			{
				JsonPathExecResult res = recursiveExecute(cxt, &arg, jb, NULL);

				if (jperIsError(res))
					return jperReplace(res, jpbUnknown);

				return res == jperOk ? jpbTrue : jpbFalse;
			}

		default:
			elog(ERROR, "invalid boolean jsonpath item type: %d", jsp->type);
			return jpbUnknown;
	}
}

/*
 * Main executor function: walks on jsonpath structure and tries to find
 * correspoding parts of jsonb. Note, jsonb and jsonpath values should be
 * avaliable and untoasted during work because JsonPathItem, JsonbValue
 * and found could have pointers into input values. If caller wants just to
 * check matching of json by jsonpath then it doesn't provide a found arg.
 * In this case executor works till first positive result and does not check
 * the rest if it is possible. In other case it tries to find all satisfied
 * results
 */
static JsonPathExecResult
recursiveExecuteNoUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
						 JsonbValue *jb, JsonValueList *found)
{
	JsonPathItem		elem;
	JsonPathExecResult	res = jperNotFound;
	bool				hasNext;

	check_stack_depth();

	switch (jsp->type)
	{
		/* all boolean item types: */
		case jpiAnd:
		case jpiOr:
		case jpiNot:
		case jpiIsUnknown:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiExists:
		case jpiStartsWith:
		case jpiLikeRegex:
			{
				JsonPathBool st = recursiveExecuteBool(cxt, jsp, jb, true);

				res = appendBoolResult(cxt, jsp, found, st);
				break;
			}

		case jpiKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbValue	*v, key;
				JsonbValue	obj;

				if (jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &obj);

				key.type = jbvString;
				key.val.string.val = jspGetString(jsp, &key.val.string.len);

				v = findJsonbValueFromContainer(jb->val.binary.data, JB_FOBJECT, &key);

				if (v != NULL)
				{
					res = recursiveExecuteNext(cxt, jsp, NULL, v, found, false);

					if (jspHasNext(jsp) || !found)
						pfree(v); /* free value if it was not added to found list */
				}
				else if (!jspIgnoreStructuralErrors(cxt))
				{
					Assert(found);
					res = jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
				}
			}
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				res = jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
			}
			break;
		case jpiRoot:
			jb = cxt->root;
			/* fall through */
		case jpiCurrent:
			{
				JsonbValue *v;
				JsonbValue	vbuf;
				bool		copy = true;

				if (JsonbType(jb) == jbvScalar)
				{
					if (jspHasNext(jsp))
						v = &vbuf;
					else
					{
						v = palloc(sizeof(*v));
						copy = false;
					}

					JsonbExtractScalar(jb->val.binary.data, v);
				}
				else
					v = jb;

				res = recursiveExecuteNext(cxt, jsp, NULL, v, found, copy);
				break;
			}
		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				hasNext = jspGetNext(jsp, &elem);

				if (jb->type == jbvArray)
				{
					JsonbValue *el = jb->val.array.elems;
					JsonbValue *last_el = el + jb->val.array.nElems;

					for (; el < last_el; el++)
					{
						res = recursiveExecuteNext(cxt, jsp, &elem, el, found, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
				else
				{
					JsonbValue	v;
					JsonbIterator *it;
					JsonbIteratorToken r;

					it = JsonbIteratorInit(jb->val.binary.data);

					while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
					{
						if (r == WJB_ELEM)
						{
							res = recursiveExecuteNext(cxt, jsp, &elem, &v, found, true);

							if (jperIsError(res))
								break;

							if (res == jperOk && !found)
								break;
						}
					}
				}
			}
			else if (!jspIgnoreStructuralErrors(cxt))
				res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray)
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = JsonbArraySize(jb);
				bool		binary = jb->type == jbvBinary;

				cxt->innermostArraySize = size; /* for LAST evaluation */

				hasNext = jspGetNext(jsp, &elem);

				for (i = 0; i < jsp->content.array.nelems; i++)
				{
					JsonPathItem from;
					JsonPathItem to;
					int32		index;
					int32		index_from;
					int32		index_to;
					bool		range = jspGetArraySubscript(jsp, &from, &to, i);

					res = getArrayIndex(cxt, &from, jb, &index_from);

					if (jperIsError(res))
						break;

					if (range)
					{
						res = getArrayIndex(cxt, &to, jb, &index_to);

						if (jperIsError(res))
							break;
					}
					else
						index_to = index_from;

					if (!jspIgnoreStructuralErrors(cxt) &&
						(index_from < 0 ||
						 index_from > index_to ||
						 index_to >= size))
					{
						res = jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);
						break;
					}

					if (index_from < 0)
						index_from = 0;

					if (index_to >= size)
						index_to = size - 1;

					res = jperNotFound;

					for (index = index_from; index <= index_to; index++)
					{
						JsonbValue *v = binary ?
							getIthJsonbValueFromContainer(jb->val.binary.data,
														  (uint32) index) :
							&jb->val.array.elems[index];

						if (v == NULL)
							continue;

						res = recursiveExecuteNext(cxt, jsp, &elem, v, found,
												   !binary);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;
			}
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
			}
			break;

		case jpiLast:
			{
				JsonbValue	tmpjbv;
				JsonbValue *lastjbv;
				int			last;
				bool		hasNext;

				if (cxt->innermostArraySize < 0)
					elog(ERROR,
						 "evaluating jsonpath LAST outside of array subscript");

				hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
				{
					res = jperOk;
					break;
				}

				last = cxt->innermostArraySize - 1;

				lastjbv = hasNext ? &tmpjbv : palloc(sizeof(*lastjbv));

				lastjbv->type = jbvNumeric;
				lastjbv->val.numeric = DatumGetNumeric(DirectFunctionCall1(
											int4_numeric, Int32GetDatum(last)));

				res = recursiveExecuteNext(cxt, jsp, &elem, lastjbv, found, hasNext);
			}
			break;
		case jpiAnyKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbIterator	*it;
				int32			r;
				JsonbValue		v;
				JsonbValue		bin;

				if (jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &bin);

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_VALUE)
					{
						res = recursiveExecuteNext(cxt, jsp, &elem, &v, found, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
			}
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				res = jperMakeError(ERRCODE_JSON_OBJECT_NOT_FOUND);
			}
			break;
		case jpiAdd:
		case jpiSub:
		case jpiMul:
		case jpiDiv:
		case jpiMod:
			res = executeBinaryArithmExpr(cxt, jsp, jb, found);
			break;
		case jpiPlus:
		case jpiMinus:
			res = executeUnaryArithmExpr(cxt, jsp, jb, found);
			break;
		case jpiFilter:
			{
				JsonPathBool st;

				jspGetArg(jsp, &elem);
				st = recursiveExecuteBool(cxt, &elem, jb, false);
				if (st != jpbTrue)
					res = jperNotFound;
				else
					res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
				break;
			}
		case jpiAny:
			{
				JsonbValue jbvbuf;

				hasNext = jspGetNext(jsp, &elem);

				/* first try without any intermediate steps */
				if (jsp->content.anybounds.first == 0)
				{
					bool		ignoreStructuralErrors = cxt->ignoreStructuralErrors;

					cxt->ignoreStructuralErrors = true;
					res = recursiveExecuteNext(cxt, jsp, &elem, jb, found, true);
					cxt->ignoreStructuralErrors = ignoreStructuralErrors;

					if (res == jperOk && !found)
							break;
				}

				if (jb->type == jbvArray || jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &jbvbuf);

				if (jb->type == jbvBinary)
					res = recursiveAny(cxt, hasNext ? &elem : NULL, jb, found,
									   1,
									   jsp->content.anybounds.first,
									   jsp->content.anybounds.last);
				break;
			}
		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			{
				JsonbValue	vbuf;
				JsonbValue *v;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
				{
					res = jperOk; /* skip evaluation */
					break;
				}

				v = hasNext ? &vbuf : palloc(sizeof(*v));

				computeJsonPathItem(cxt, jsp, v);

				res = recursiveExecuteNext(cxt, jsp, &elem, v, found, hasNext);
			}
			break;
		case jpiType:
			{
				JsonbValue *jbv = palloc(sizeof(*jbv));

				jbv->type = jbvString;
				jbv->val.string.val = pstrdup(JsonbTypeName(jb));
				jbv->val.string.len = strlen(jbv->val.string.val);

				res = recursiveExecuteNext(cxt, jsp, NULL, jbv, found, false);
			}
			break;
		case jpiSize:
			{
				int			size = JsonbArraySize(jb);

				if (size < 0)
				{
					if (!jspAutoWrap(cxt))
					{
						if (!jspIgnoreStructuralErrors(cxt))
							res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
						break;
					}

					size = 1;
				}

				jb = palloc(sizeof(*jb));

				jb->type = jbvNumeric;
				jb->val.numeric =
					DatumGetNumeric(DirectFunctionCall1(int4_numeric,
														Int32GetDatum(size)));

				res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, false);
			}
			break;
		case jpiAbs:
		case jpiFloor:
		case jpiCeiling:
			{
				JsonbValue jbvbuf;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbvbuf);

				if (jb->type == jbvNumeric)
				{
					Datum		datum = NumericGetDatum(jb->val.numeric);

					switch (jsp->type)
					{
						case jpiAbs:
							datum = DirectFunctionCall1(numeric_abs, datum);
							break;
						case jpiFloor:
							datum = DirectFunctionCall1(numeric_floor, datum);
							break;
						case jpiCeiling:
							datum = DirectFunctionCall1(numeric_ceil, datum);
							break;
						default:
							break;
					}

					jb = palloc(sizeof(*jb));

					jb->type = jbvNumeric;
					jb->val.numeric = DatumGetNumeric(datum);

					res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, false);
				}
				else
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
			}
			break;
		case jpiDouble:
			{
				JsonbValue jbv;
				ErrorData  *edata = NULL;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbv);

				if (jb->type == jbvNumeric)
				{
					/* only check success of numeric to double cast */
					(void) numeric_float8_internal(jb->val.numeric, &edata);
				}
				else if (jb->type == jbvString)
				{
					/* cast string as double */
					char	   *str = pnstrdup(jb->val.string.val,
											   jb->val.string.len);
					double		val;

					val = float8in_internal_safe(str, NULL, "double precision",
												 str, &edata);
					pfree(str);

					if (!edata)
					{
						jb = &jbv;
						jb->type = jbvNumeric;
						jb->val.numeric = float8_numeric_internal(val, &edata);
					}
				}
				else
				{
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
					break;
				}

				if (edata)
				{
					if (ERRCODE_TO_CATEGORY(edata->sqlerrcode) !=
						ERRCODE_DATA_EXCEPTION)
						ThrowErrorData(edata);

					FreeErrorData(edata);
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
				}
				else
				{
					res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
				}
			}
			break;
		case jpiDatetime:
			{
				JsonbValue	jbvbuf;
				Datum		value;
				text	   *datetime;
				Oid			typid;
				int32		typmod = -1;
				int			tz;
				bool		hasNext;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbvbuf);

				res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);

				if (jb->type != jbvString)
					break;

				datetime = cstring_to_text_with_len(jb->val.string.val,
													jb->val.string.len);

				if (jsp->content.args.left)
				{
					char	   *template_str;
					int			template_len;
					char	   *tzname = NULL;

					jspGetLeftArg(jsp, &elem);

					if (elem.type != jpiString)
						elog(ERROR, "invalid jsonpath item type for .datetime() argument");

					template_str = jspGetString(&elem, &template_len);

					if (jsp->content.args.right)
					{
						JsonValueList tzlist = { 0 };
						JsonPathExecResult tzres;
						JsonbValue *tzjbv;

						jspGetRightArg(jsp, &elem);
						tzres = recursiveExecuteNoUnwrap(cxt, &elem, jb,
														 &tzlist);

						if (jperIsError(tzres))
							return tzres;

						if (JsonValueListLength(&tzlist) != 1)
							break;

						tzjbv = JsonValueListHead(&tzlist);

						if (tzjbv->type != jbvString)
							break;

						tzname = pnstrdup(tzjbv->val.string.val,
										  tzjbv->val.string.len);
					}

					if (tryToParseDatetime(template_str, template_len, datetime,
										   tzname, false,
										   &value, &typid, &typmod, &tz))
						res = jperOk;

					if (tzname)
						pfree(tzname);
				}
				else
				{
					const char *templates[] = {
						"yyyy-mm-dd HH24:MI:SS TZH:TZM",
						"yyyy-mm-dd HH24:MI:SS TZH",
						"yyyy-mm-dd HH24:MI:SS",
						"yyyy-mm-dd",
						"HH24:MI:SS TZH:TZM",
						"HH24:MI:SS TZH",
						"HH24:MI:SS"
					};
					int			i;

					for (i = 0; i < sizeof(templates) / sizeof(*templates); i++)
					{
						if (tryToParseDatetime(templates[i], -1, datetime,
											   NULL, true,  &value, &typid,
											   &typmod, &tz))
						{
							res = jperOk;
							break;
						}
					}
				}

				pfree(datetime);

				if (jperIsError(res))
					break;

				hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
					break;

				jb = hasNext ? &jbvbuf : palloc(sizeof(*jb));

				jb->type = jbvDatetime;
				jb->val.datetime.value = value;
				jb->val.datetime.typid = typid;
				jb->val.datetime.typmod = typmod;
				jb->val.datetime.tz = tz;

				res = recursiveExecuteNext(cxt, jsp, &elem, jb, found, hasNext);
			}
			break;
		case jpiKeyValue:
			if (JsonbType(jb) != jbvObject)
				res = jperMakeError(ERRCODE_JSON_OBJECT_NOT_FOUND);
			else
			{
				int32		r;
				JsonbValue	bin;
				JsonbValue	key;
				JsonbValue	val;
				JsonbValue	obj;
				JsonbValue	keystr;
				JsonbValue	valstr;
				JsonbIterator *it;
				JsonbParseState *ps = NULL;

				hasNext = jspGetNext(jsp, &elem);

				if (jb->type == jbvBinary
					? !JsonContainerSize(jb->val.binary.data)
					: !jb->val.object.nPairs)
				{
					res = jperNotFound;
					break;
				}

				/* make template object */
				obj.type = jbvBinary;

				keystr.type = jbvString;
				keystr.val.string.val = "key";
				keystr.val.string.len = 3;

				valstr.type = jbvString;
				valstr.val.string.val = "value";
				valstr.val.string.len = 5;

				if (jb->type == jbvObject)
					jb = JsonbWrapInBinary(jb, &bin);

				it = JsonbIteratorInit(jb->val.binary.data);

				while ((r = JsonbIteratorNext(&it, &key, true)) != WJB_DONE)
				{
					if (r == WJB_KEY)
					{
						Jsonb	   *jsonb;
						JsonbValue  *keyval;

						res = jperOk;

						if (!hasNext && !found)
							break;

						r = JsonbIteratorNext(&it, &val, true);
						Assert(r == WJB_VALUE);

						pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

						pushJsonbValue(&ps, WJB_KEY, &keystr);
						pushJsonbValue(&ps, WJB_VALUE, &key);


						pushJsonbValue(&ps, WJB_KEY, &valstr);
						pushJsonbValue(&ps, WJB_VALUE, &val);

						keyval = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

						jsonb = JsonbValueToJsonb(keyval);

						JsonbInitBinary(&obj, jsonb);

						res = recursiveExecuteNext(cxt, jsp, &elem, &obj, found, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
			}
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
static JsonPathExecResult
recursiveExecuteUnwrapArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
							JsonbValue *jb, JsonValueList *found)
{
	JsonPathExecResult res = jperNotFound;

	if (jb->type == jbvArray)
	{
		JsonbValue *elem = jb->val.array.elems;
		JsonbValue *last = elem + jb->val.array.nElems;

		for (; elem < last; elem++)
		{
			res = recursiveExecuteNoUnwrap(cxt, jsp, elem, found);

			if (jperIsError(res))
				break;
			if (res == jperOk && !found)
				break;
		}
	}
	else
	{
		JsonbValue	v;
		JsonbIterator *it;
		JsonbIteratorToken tok;

		it = JsonbIteratorInit(jb->val.binary.data);

		while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
		{
			if (tok == WJB_ELEM)
			{
				res = recursiveExecuteNoUnwrap(cxt, jsp, &v, found);
				if (jperIsError(res))
					break;
				if (res == jperOk && !found)
					break;
			}
		}
	}

	return res;
}

/*
 * Execute jsonpath with unwrapping of current item if it is an array.
 */
static inline JsonPathExecResult
recursiveExecuteUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, JsonValueList *found)
{
	if (jspAutoUnwrap(cxt) && JsonbType(jb) == jbvArray)
		return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

	return recursiveExecuteNoUnwrap(cxt, jsp, jb, found);
}

/*
 * Wrap a non-array SQL/JSON item into an array for applying array subscription
 * path steps in lax mode.
 */
static inline JsonbValue *
wrapItem(JsonbValue *jbv)
{
	JsonbParseState *ps = NULL;
	JsonbValue	jbvbuf;

	switch (JsonbType(jbv))
	{
		case jbvArray:
			/* Simply return an array item. */
			return jbv;

		case jbvScalar:
			/* Extract scalar value from singleton pseudo-array. */
			jbv = JsonbExtractScalar(jbv->val.binary.data, &jbvbuf);
			break;

		case jbvObject:
			/*
			 * Need to wrap object into a binary JsonbValue for its unpacking
			 * in pushJsonbValue().
			 */
			if (jbv->type != jbvBinary)
				jbv = JsonbWrapInBinary(jbv, &jbvbuf);
			break;

		default:
			/* Ordinary scalars can be pushed directly. */
			break;
	}

	pushJsonbValue(&ps, WJB_BEGIN_ARRAY, NULL);
	pushJsonbValue(&ps, WJB_ELEM, jbv);
	jbv = pushJsonbValue(&ps, WJB_END_ARRAY, NULL);

	return JsonbWrapInBinary(jbv, NULL);
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
static inline JsonPathExecResult
recursiveExecute(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
				 JsonValueList *found)
{
	if (jspAutoUnwrap(cxt))
	{
		switch (jsp->type)
		{
			case jpiKey:
			case jpiAnyKey:
		/*	case jpiAny: */
			case jpiFilter:
			/* all methods excluding type() and size() */
			case jpiAbs:
			case jpiFloor:
			case jpiCeiling:
			case jpiDouble:
			case jpiDatetime:
			case jpiKeyValue:
				return recursiveExecuteUnwrap(cxt, jsp, jb, found);

			case jpiAnyArray:
			case jpiIndexArray:
				jb = wrapItem(jb);
				break;

			default:
				break;
		}
	}

	return recursiveExecuteNoUnwrap(cxt, jsp, jb, found);
}


/*
 * Public interface to jsonpath executor
 */
JsonPathExecResult
executeJsonPath(JsonPath *path, List *vars, Jsonb *json, JsonValueList *foundJson)
{
	JsonPathExecContext cxt;
	JsonPathItem	jsp;
	JsonbValue		jbv;

	jspInit(&jsp, path);

	cxt.vars = vars;
	cxt.laxMode = (path->header & JSONPATH_LAX) != 0;
	cxt.ignoreStructuralErrors = cxt.laxMode;
	cxt.root = JsonbInitBinary(&jbv, json);
	cxt.innermostArraySize = -1;

	if (jspStrictAbsenseOfErrors(&cxt) && !foundJson)
	{
		/*
		 * In strict mode we must get a complete list of values to check
		 * that there are no errors at all.
		 */
		JsonValueList vals = { 0 };
		JsonPathExecResult res = recursiveExecute(&cxt, &jsp, &jbv, &vals);

		if (jperIsError(res))
			return res;

		return JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
	}

	return recursiveExecute(&cxt, &jsp, &jbv, foundJson);
}

static Datum
returnDATUM(void *arg, bool *isNull)
{
	*isNull = false;
	return	PointerGetDatum(arg);
}

static Datum
returnNULL(void *arg, bool *isNull)
{
	*isNull = true;
	return Int32GetDatum(0);
}

/*
 * Convert jsonb object into list of vars for executor
 */
static List*
makePassingVars(Jsonb *jb)
{
	JsonbValue		v;
	JsonbIterator	*it;
	int32			r;
	List			*vars = NIL;

	it = JsonbIteratorInit(&jb->root);

	r =  JsonbIteratorNext(&it, &v, true);

	if (r != WJB_BEGIN_OBJECT)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("passing variable json is not a object")));

	while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			JsonPathVariable	*jpv = palloc0(sizeof(*jpv));

			jpv->varName = cstring_to_text_with_len(v.val.string.val,
													v.val.string.len);

			JsonbIteratorNext(&it, &v, true);

			jpv->cb = returnDATUM;

			switch(v.type)
			{
				case jbvBool:
					jpv->typid = BOOLOID;
					jpv->cb_arg = DatumGetPointer(BoolGetDatum(v.val.boolean));
					break;
				case jbvNull:
					jpv->cb = returnNULL;
					break;
				case jbvString:
					jpv->typid = TEXTOID;
					jpv->cb_arg = cstring_to_text_with_len(v.val.string.val,
														   v.val.string.len);
					break;
				case jbvNumeric:
					jpv->typid = NUMERICOID;
					jpv->cb_arg = v.val.numeric;
					break;
				case jbvBinary:
					jpv->typid = JSONXOID;
					jpv->cb_arg = DatumGetPointer(JsonbPGetDatum(JsonbValueToJsonb(&v)));
					break;
				default:
					elog(ERROR, "unsupported type in passing variable json");
			}

			vars = lappend(vars, jpv);
		}
	}

	return vars;
}

static void
throwJsonPathError(JsonPathExecResult res)
{
	int			err;
	if (!jperIsError(res))
		return;

	if (jperIsErrorData(res))
		ThrowErrorData(jperGetErrorData(res));

	err = jperGetError(res);

	switch (err)
	{
		case ERRCODE_JSON_ARRAY_NOT_FOUND:
			ereport(ERROR,
					(errcode(err),
					 errmsg("SQL/JSON array not found")));
			break;
		case ERRCODE_JSON_OBJECT_NOT_FOUND:
			ereport(ERROR,
					(errcode(err),
					 errmsg("SQL/JSON object not found")));
			break;
		case ERRCODE_JSON_MEMBER_NOT_FOUND:
			ereport(ERROR,
					(errcode(err),
					 errmsg("SQL/JSON member not found")));
			break;
		case ERRCODE_JSON_NUMBER_NOT_FOUND:
			ereport(ERROR,
					(errcode(err),
					 errmsg("SQL/JSON number not found")));
			break;
		case ERRCODE_JSON_SCALAR_REQUIRED:
			ereport(ERROR,
					(errcode(err),
					 errmsg("SQL/JSON scalar required")));
			break;
		case ERRCODE_SINGLETON_JSON_ITEM_REQUIRED:
			ereport(ERROR,
					(errcode(err),
					 errmsg("Singleton SQL/JSON item required")));
			break;
		case ERRCODE_NON_NUMERIC_JSON_ITEM:
			ereport(ERROR,
					(errcode(err),
					 errmsg("Non-numeric SQL/JSON item")));
			break;
		case ERRCODE_INVALID_JSON_SUBSCRIPT:
			ereport(ERROR,
					(errcode(err),
					 errmsg("Invalid SQL/JSON subscript")));
			break;
		case ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION:
			ereport(ERROR,
					(errcode(err),
					 errmsg("Invalid argument for SQL/JSON datetime function")));
			break;
		default:
			ereport(ERROR,
					(errcode(err),
					 errmsg("Unknown SQL/JSON error")));
			break;
	}
}

static Datum
jsonb_jsonpath_exists(PG_FUNCTION_ARGS)
{
	Jsonb				*jb = PG_GETARG_JSONB_P(0);
	JsonPath			*jp = PG_GETARG_JSONPATH_P(1);
	JsonPathExecResult	res;
	List				*vars = NIL;

	if (PG_NARGS() == 3)
		vars = makePassingVars(PG_GETARG_JSONB_P(2));

	res = executeJsonPath(jp, vars, jb, NULL);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (jperIsError(res))
	{
		jperFree(res);
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(res == jperOk);
}

Datum
jsonb_jsonpath_exists2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_exists(fcinfo);
}

Datum
jsonb_jsonpath_exists3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_exists(fcinfo);
}

static inline Datum
jsonb_jsonpath_predicate(FunctionCallInfo fcinfo, List *vars)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonbValue *jbv;
	JsonValueList found = { 0 };
	JsonPathExecResult res;

	res = executeJsonPath(jp, vars, jb, &found);

	throwJsonPathError(res);

	if (JsonValueListLength(&found) != 1)
		throwJsonPathError(jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED));

	jbv = JsonValueListHead(&found);

	if (JsonbType(jbv) == jbvScalar)
		JsonbExtractScalar(jbv->val.binary.data, jbv);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (jbv->type == jbvNull)
		PG_RETURN_NULL();

	if (jbv->type != jbvBool)
		PG_RETURN_NULL(); /* XXX */

	PG_RETURN_BOOL(jbv->val.boolean);
}

Datum
jsonb_jsonpath_predicate2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_predicate(fcinfo, NIL);
}

Datum
jsonb_jsonpath_predicate3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_predicate(fcinfo,
									makePassingVars(PG_GETARG_JSONB_P(2)));
}

static Datum
jsonb_jsonpath_query(FunctionCallInfo fcinfo)
{
	FuncCallContext	*funcctx;
	List			*found;
	JsonbValue		*v;
	ListCell		*c;

	if (SRF_IS_FIRSTCALL())
	{
		JsonPath			*jp = PG_GETARG_JSONPATH_P(1);
		Jsonb				*jb;
		JsonPathExecResult	res;
		MemoryContext		oldcontext;
		List				*vars = NIL;
		JsonValueList		found = { 0 };

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		jb = PG_GETARG_JSONB_P_COPY(0);
		if (PG_NARGS() == 3)
			vars = makePassingVars(PG_GETARG_JSONB_P(2));

		res = executeJsonPath(jp, vars, jb, &found);

		if (jperIsError(res))
			throwJsonPathError(res);

		PG_FREE_IF_COPY(jp, 1);

		funcctx->user_fctx = JsonValueListGetList(&found);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	found = funcctx->user_fctx;

	c = list_head(found);

	if (c == NULL)
		SRF_RETURN_DONE(funcctx);

	v = lfirst(c);
	funcctx->user_fctx = list_delete_first(found);

	SRF_RETURN_NEXT(funcctx, JsonbPGetDatum(JsonbValueToJsonb(v)));
}

Datum
jsonb_jsonpath_query2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_query(fcinfo);
}

Datum
jsonb_jsonpath_query3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_query(fcinfo);
}

static Datum
jsonb_jsonpath_query_wrapped(FunctionCallInfo fcinfo, List *vars)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonValueList found = { 0 };
	JsonPathExecResult	res;
	int				size;

	res = executeJsonPath(jp, vars, jb, &found);

	if (jperIsError(res))
		throwJsonPathError(res);

	size = JsonValueListLength(&found);

	if (size == 0)
		PG_RETURN_NULL();

	if (size == 1)
		PG_RETURN_JSONB_P(JsonbValueToJsonb(JsonValueListHead(&found)));

	PG_RETURN_JSONB_P(JsonbValueToJsonb(wrapItemsInArray(&found)));
}

Datum
jsonb_jsonpath_query_wrapped2(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_query_wrapped(fcinfo, NIL);
}

Datum
jsonb_jsonpath_query_wrapped3(PG_FUNCTION_ARGS)
{
	return jsonb_jsonpath_query_wrapped(fcinfo,
										makePassingVars(PG_GETARG_JSONB_P(2)));
}

/* Construct a JSON array from the item list */
static inline JsonbValue *
wrapItemsInArray(const JsonValueList *items)
{
	JsonbParseState *ps = NULL;
	JsonValueListIterator it = { 0 };
	JsonbValue *jbv;

	pushJsonbValue(&ps, WJB_BEGIN_ARRAY, NULL);

	while ((jbv = JsonValueListNext(items, &it)))
	{
		JsonbValue	bin;

		if (jbv->type == jbvBinary &&
			JsonContainerIsScalar(jbv->val.binary.data))
			JsonbExtractScalar(jbv->val.binary.data, jbv);

		if (jbv->type == jbvObject || jbv->type == jbvArray)
			jbv = JsonbWrapInBinary(jbv, &bin);

		pushJsonbValue(&ps, WJB_ELEM, jbv);
	}

	return pushJsonbValue(&ps, WJB_END_ARRAY, NULL);
}
