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
#include "utils/datum.h"
#include "utils/formatting.h"
#include "utils/json.h"
#include "utils/jsonpath.h"
#include "utils/varlena.h"

typedef struct JsonPathExecContext
{
	List	   *vars;
	bool		lax;
	int			innermostArraySize;	/* for LAST array index evaluation */
} JsonPathExecContext;

static JsonPathExecResult recursiveExecute(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonbValue *jb,
										   List **found);

static JsonPathExecResult recursiveExecuteUnwrap(JsonPathExecContext *cxt,
							JsonPathItem *jsp, JsonbValue *jb, List **found);

static inline JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = &jb->root;
	jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

	return jbv;
}

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
	ListCell   *cell;
	JsonPathVariable *var = NULL;
	bool		isNull;
	Datum		computedValue;
	char	   *varName;
	int			varNameLength;

	Assert(variable->type == jpiVariable);
	varName = jspGetString(variable, &varNameLength);

	foreach(cell, vars)
	{
		var = (JsonPathVariable *) lfirst(cell);

		if (varNameLength == VARSIZE_ANY_EXHDR(var->varName) &&
			!strncmp(varName, VARDATA_ANY(var->varName), varNameLength))
			break;

		var = NULL;
	}

	if (var == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("could not find jsonpath variable '%s'",
						pnstrdup(varName, varNameLength))));

	computedValue = var->cb(var->cb_arg, &isNull);

	if (isNull)
	{
		value->type = jbvNull;
		return;
	}

	switch (var->typid)
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
			value->val.datetime.value = computedValue;
			break;
		case JSONBOID:
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
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
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
static int
JsonbType(JsonbValue *jb)
{
	int type = jb->type;

	if (jb->type == jbvBinary)
	{
		JsonbContainer	*jbc = jb->val.binary.data;

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

static const char *
JsonbTypeName(JsonbValue *jb)
{
	JsonbValue jbvbuf;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = jb->val.binary.data;

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

static int
JsonbArraySize(JsonbValue *jb)
{
	if (jb->type == jbvArray)
		return jb->val.array.nElems;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = jb->val.binary.data;

		if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
			return JsonContainerSize(jbc);
	}

	return -1;
}

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

static JsonPathExecResult
checkEquality(JsonbValue *jb1, JsonbValue *jb2, bool not)
{
	bool	eq = false;

	if (jb1->type != jb2->type)
	{
		if (jb1->type == jbvNull || jb2->type == jbvNull)
			return not ? jperOk : jperNotFound;

		return jperError;
	}

	if (jb1->type == jbvBinary)
		return jperError;

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
					return jperError;

				break;
			}
		default:
			elog(ERROR,"1Wrong state");
	}

	return (not ^ eq) ? jperOk : jperNotFound;
}

static JsonPathExecResult
makeCompare(int32 op, JsonbValue *jb1, JsonbValue *jb2)
{
	int			cmp;
	bool		res;

	if (jb1->type != jb2->type)
	{
		if (jb1->type != jbvNull && jb2->type != jbvNull)
			/* non-null items of different types are not order-comparable */
			return jperError;

		if (jb1->type != jbvNull || jb2->type != jbvNull)
			/* comparison of nulls to non-nulls returns always false */
			return jperNotFound;

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
					return jperError;
			}
			break;
		default:
			return jperError;
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
			return jperError;
	}

	return res ? jperOk : jperNotFound;
}

static JsonbValue *
copyJsonbValue(JsonbValue *src)
{
	JsonbValue	*dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
}

static inline JsonPathExecResult
recursiveExecuteAndUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb, List **found)
{
	if (cxt->lax)
	{
		List *seq = NIL;
		JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &seq);
		ListCell *lc;

		if (jperIsError(res))
			return res;

		foreach(lc, seq)
		{
			JsonbValue *item = lfirst(lc);

			if (item->type == jbvArray)
			{
				JsonbValue *elem = item->val.array.elems;
				JsonbValue *last = elem + item->val.array.nElems;

				for (; elem < last; elem++)
					*found = lappend(*found, copyJsonbValue(elem));
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
						*found = lappend(*found, copyJsonbValue(&elem));
				}
			}
			else
				*found = lappend(*found, item);
		}

		return jperOk;
	}

	return recursiveExecute(cxt, jsp, jb, found);
}

static JsonPathExecResult
executeExpr(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	List	   *lseq = NIL;
	List	   *rseq = NIL;
	ListCell   *llc;
	ListCell   *rlc;
	bool		error = false;
	bool		found = false;

	jspGetLeftArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperError;

	jspGetRightArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &rseq);
	if (jperIsError(res))
		return jperError;

	foreach(llc, lseq)
	{
		JsonbValue *lval = lfirst(llc);

		foreach(rlc, rseq)
		{
			JsonbValue *rval = lfirst(rlc);

			switch (jsp->type)
			{
				case jpiEqual:
					res = checkEquality(lval, rval, false);
					break;
				case jpiNotEqual:
					res = checkEquality(lval, rval, true);
					break;
				case jpiLess:
				case jpiGreater:
				case jpiLessOrEqual:
				case jpiGreaterOrEqual:
					res = makeCompare(jsp->type, lval, rval);
					break;
				default:
					elog(ERROR, "Unknown operation");
			}

			if (res == jperOk)
			{
				if (cxt->lax)
					return jperOk;

				found = true;
			}
			else if (res == jperError)
			{
				if (!cxt->lax)
					return jperError;

				error = true;
			}
		}
	}

	if (found) /* possible only in strict mode */
		return jperOk;

	if (error) /* possible only in lax mode */
		return jperError;

	return jperNotFound;
}

static JsonPathExecResult
executeBinaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
						JsonbValue *jb, List **found)
{
	JsonPathExecResult jper;
	JsonPathItem elem;
	List	   *lseq = NIL;
	List	   *rseq = NIL;
	JsonbValue *lval;
	JsonbValue *rval;
	JsonbValue	lvalbuf;
	JsonbValue	rvalbuf;
	Datum		ldatum;
	Datum		rdatum;
	Datum		res;

	jspGetLeftArg(jsp, &elem);

	/* XXX by standard unwrapped only operands of multiplicative expressions */
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);

	if (jper == jperOk)
	{
		jspGetRightArg(jsp, &elem);
		jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &rseq); /* XXX */
	}

	if (jper != jperOk || list_length(lseq) != 1 || list_length(rseq) != 1)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	lval = linitial(lseq);

	if (JsonbType(lval) == jbvScalar)
		lval = JsonbExtractScalar(lval->val.binary.data, &lvalbuf);

	if (lval->type != jbvNumeric)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	rval = linitial(rseq);

	if (JsonbType(rval) == jbvScalar)
		rval = JsonbExtractScalar(rval->val.binary.data, &rvalbuf);

	if (rval->type != jbvNumeric)
		return jperMakeError(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED);

	if (!found)
		return jperOk;

	ldatum = NumericGetDatum(lval->val.numeric);
	rdatum = NumericGetDatum(rval->val.numeric);

	switch (jsp->type)
	{
		case jpiAdd:
			res = DirectFunctionCall2(numeric_add, ldatum, rdatum);
			break;
		case jpiSub:
			res = DirectFunctionCall2(numeric_sub, ldatum, rdatum);
			break;
		case jpiMul:
			res = DirectFunctionCall2(numeric_mul, ldatum, rdatum);
			break;
		case jpiDiv:
			res = DirectFunctionCall2(numeric_div, ldatum, rdatum);
			break;
		case jpiMod:
			res = DirectFunctionCall2(numeric_mod, ldatum, rdatum);
			break;
		default:
			elog(ERROR, "unknown jsonpath arithmetic operation %d", jsp->type);
	}

	lval = palloc(sizeof(*lval));
	lval->type = jbvNumeric;
	lval->val.numeric = DatumGetNumeric(res);

	*found = lappend(*found, lval);

	return jperOk;
}

static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, List **found)
{
	JsonPathExecResult jper;
	JsonPathItem elem;
	List	   *seq = NIL;
	ListCell   *lc;

	jspGetArg(jsp, &elem);
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, &seq);

	if (jperIsError(jper))
		return jperMakeError(ERRCODE_JSON_NUMBER_NOT_FOUND);

	jper = jperNotFound;

	foreach(lc, seq)
	{
		JsonbValue *val = lfirst(lc);
		JsonbValue	valbuf;

		if (JsonbType(val) == jbvScalar)
			val = JsonbExtractScalar(val->val.binary.data, &valbuf);

		if (val->type == jbvNumeric)
		{
			jper = jperOk;

			if (!found)
				return jper;
		}
		else if (!found)
			continue; /* skip non-numerics processing */

		if (val->type != jbvNumeric)
			return jperMakeError(ERRCODE_JSON_NUMBER_NOT_FOUND);

		val = copyJsonbValue(val);

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

		*found = lappend(*found, val);
	}

	return jper;
}

/*
 * implements jpiAny node (** operator)
 */
static JsonPathExecResult
recursiveAny(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			 List **found, uint32 level, uint32 first, uint32 last)
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

			if (level >= first)
			{
				/* check expression */
				if (jsp)
				{
					res = recursiveExecute(cxt, jsp, &v, found);
					if (res == jperOk && !found)
						break;
				}
				else
				{
					res = jperOk;
					if (!found)
						break;
					*found = lappend(*found, copyJsonbValue(&v));
				}
			}

			if (level < last && v.type == jbvBinary)
			{
				res = recursiveAny(cxt, jsp, &v, found, level + 1, first, last);

				if (res == jperOk && found == NULL)
					break;
			}
		}
	}

	return res;
}

static JsonPathExecResult
getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			  int32 *index)
{
	JsonbValue *jbv;
	List	   *found = NIL;
	JsonbValue	tmp;
	JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &found);

	if (jperIsError(res))
		return res;

	if (list_length(found) != 1)
		return jperMakeError(ERRCODE_INVALID_JSON_SUBSCRIPT);

	jbv = linitial(found);

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

static JsonPathExecResult
executeStartsWithPredicate(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	List	   *lseq = NIL;
	List	   *rseq = NIL;
	ListCell   *lc;
	JsonbValue *initial;
	JsonbValue	initialbuf;
	bool		error = false;
	bool		found = false;

	jspGetRightArg(jsp, &elem);
	res = recursiveExecute(cxt, &elem, jb, &rseq);
	if (jperIsError(res))
		return jperError;

	if (list_length(rseq) != 1)
		return jperError;

	initial = linitial(rseq);

	if (JsonbType(initial) == jbvScalar)
		initial = JsonbExtractScalar(initial->val.binary.data, &initialbuf);

	if (initial->type != jbvString)
		return jperError;

	jspGetLeftArg(jsp, &elem);
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperError;

	foreach(lc, lseq)
	{
		JsonbValue *whole = lfirst(lc);
		JsonbValue	wholebuf;

		if (JsonbType(whole) == jbvScalar)
			whole = JsonbExtractScalar(whole->val.binary.data, &wholebuf);

		if (whole->type != jbvString)
		{
			if (!cxt->lax)
				return jperError;

			error = true;
		}
		else if (whole->val.string.len >= initial->val.string.len &&
				 !memcmp(whole->val.string.val,
						 initial->val.string.val,
						 initial->val.string.len))
		{
			if (cxt->lax)
				return jperOk;

			found = true;
		}
	}

	if (found) /* possible only in strict mode */
		return jperOk;

	if (error) /* possible only in lax mode */
		return jperError;

	return jperNotFound;
}

static JsonPathExecResult
executeLikeRegexPredicate(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb)
{
	JsonPathExecResult res;
	JsonPathItem elem;
	List	   *lseq = NIL;
	ListCell   *lc;
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
	res = recursiveExecuteAndUnwrap(cxt, &elem, jb, &lseq);
	if (jperIsError(res))
		return jperError;

	foreach(lc, lseq)
	{
		JsonbValue *str = lfirst(lc);
		JsonbValue	strbuf;

		if (JsonbType(str) == jbvScalar)
			str = JsonbExtractScalar(str->val.binary.data, &strbuf);

		if (str->type != jbvString)
		{
			if (!cxt->lax)
				return jperError;

			error = true;
		}
		else if (RE_compile_and_execute(regex, str->val.string.val,
										str->val.string.len, cflags,
										DEFAULT_COLLATION_OID, 0, NULL))
		{
			if (cxt->lax)
				return jperOk;

			found = true;
		}
	}

	if (found) /* possible only in strict mode */
		return jperOk;

	if (error) /* possible only in lax mode */
		return jperError;

	return jperNotFound;
}

static bool
tryToParseDatetime(const char *template, text *datetime,
				   Datum *value, Oid *typid, int32 *typmod)
{
	MemoryContext mcxt = CurrentMemoryContext;
	bool		ok = false;

	PG_TRY();
	{
		*value = to_datetime(datetime, template, -1, true, typid, typmod);
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
						 JsonbValue *jb, List **found)
{
	JsonPathItem		elem;
	JsonPathExecResult	res = jperNotFound;
	bool				hasNext;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch(jsp->type) {
		case jpiAnd:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecute(cxt, &elem, jb, NULL);
			if (res != jperNotFound)
			{
				JsonPathExecResult res2;

				/*
				 * SQL/JSON says that we should check second arg
				 * in case of jperError
				 */

				jspGetRightArg(jsp, &elem);
				res2 = recursiveExecute(cxt, &elem, jb, NULL);

				res = (res2 == jperOk) ? res : res2;
			}
			break;
		case jpiOr:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecute(cxt, &elem, jb, NULL);
			if (res != jperOk)
			{
				JsonPathExecResult res2;

				jspGetRightArg(jsp, &elem);
				res2 = recursiveExecute(cxt, &elem, jb, NULL);

				res = (res2 == jperNotFound) ? res : res2;
			}
			break;
		case jpiNot:
			jspGetArg(jsp, &elem);
			switch ((res = recursiveExecute(cxt, &elem, jb, NULL)))
			{
				case jperOk:
					res = jperNotFound;
					break;
				case jperNotFound:
					res = jperOk;
					break;
				default:
					break;
			}
			break;
		case jpiIsUnknown:
			jspGetArg(jsp, &elem);
			res = recursiveExecute(cxt, &elem, jb, NULL);
			res = jperIsError(res) ? jperOk : jperNotFound;
			break;
		case jpiKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbValue	*v, key;

				key.type = jbvString;
				key.val.string.val = jspGetString(jsp, &key.val.string.len);

				v = findJsonbValueFromContainer(jb->val.binary.data, JB_FOBJECT, &key);

				if (v != NULL)
				{
					if (jspGetNext(jsp, &elem))
					{
						res = recursiveExecute(cxt, &elem, v, found);
						pfree(v);
					}
					else
					{
						res = jperOk;
						if (found)
							*found = lappend(*found, v);
						else
							pfree(v);
					}
				}
				else if (!cxt->lax)
				{
					Assert(found);
					res = jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
				}
			}
			else if (!cxt->lax)
			{
				Assert(found);
				res = jperMakeError(ERRCODE_JSON_MEMBER_NOT_FOUND);
			}
			break;
		case jpiCurrent:
			if (!jspGetNext(jsp, &elem))
			{
				/* we are last in chain of node */
				res = jperOk;
				if (found)
				{
					JsonbValue *v;

					if (JsonbType(jb) == jbvScalar)
						v = JsonbExtractScalar(jb->val.binary.data,
											   palloc(sizeof(*v)));
					else
						v = copyJsonbValue(jb);

					*found = lappend(*found, v);
				}
			}
			else if (JsonbType(jb) == jbvScalar)
			{
				JsonbValue	v;

				JsonbExtractScalar(jb->val.binary.data, &v);

				res = recursiveExecute(cxt, &elem, &v, found);
			}
			else
			{
				res = recursiveExecute(cxt, &elem, jb, found);
			}
			break;
		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				JsonbIterator	*it;
				int32			r;
				JsonbValue		v;

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_ELEM)
					{
						if (hasNext == true)
						{
							res = recursiveExecute(cxt, &elem, &v, found);

							if (jperIsError(res))
								break;

							if (res == jperOk && found == NULL)
								break;
						}
						else
						{
							res = jperOk;

							if (found == NULL)
								break;

							*found = lappend(*found, copyJsonbValue(&v));
						}
					}
				}
			}
			else
				res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray)
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = JsonbArraySize(jb);

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

					if (!cxt->lax &&
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
						JsonbValue *v =
							getIthJsonbValueFromContainer(jb->val.binary.data,
														  (uint32) index);

						if (v == NULL)
							continue;

						if (hasNext)
						{
							res = recursiveExecute(cxt, &elem, v, found);

							if (jperIsError(res))
								break;

							if (res == jperOk && !found)
								break;
						}
						else
						{
							res = jperOk;

							if (!found)
								break;

							*found = lappend(*found, v);
						}
					}

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;
			}
			else
				res = jperMakeError(ERRCODE_JSON_ARRAY_NOT_FOUND);
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

				if (hasNext)
					res = recursiveExecute(cxt, &elem, lastjbv, found);
				else
				{
					res = jperOk;
					*found = lappend(*found, lastjbv);
				}
			}
			break;
		case jpiAnyKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbIterator	*it;
				int32			r;
				JsonbValue		v;

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_VALUE)
					{
						if (hasNext == true)
						{
							res = recursiveExecute(cxt, &elem, &v, found);

							if (jperIsError(res))
								break;

							if (res == jperOk && found == NULL)
								break;
						}
						else
						{
							res = jperOk;

							if (found == NULL)
								break;

							*found = lappend(*found, copyJsonbValue(&v));
						}
					}
				}
			}
			else if (!cxt->lax)
			{
				Assert(found);
				res = jperMakeError(ERRCODE_JSON_OBJECT_NOT_FOUND);
			}
			break;
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			res = executeExpr(cxt, jsp, jb);
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
		case jpiRoot:
			if (jspGetNext(jsp, &elem))
			{
				res = recursiveExecute(cxt, &elem, jb, found);
			}
			else
			{
				res = jperOk;
				if (found)
					*found = lappend(*found, copyJsonbValue(jb));
			}

			break;
		case jpiFilter:
			jspGetArg(jsp, &elem);
			res = recursiveExecute(cxt, &elem, jb, NULL);
			if (res != jperOk)
				res = jperNotFound;
			else if (jspGetNext(jsp, &elem))
				res = recursiveExecute(cxt, &elem, jb, found);
			else if (found)
				*found = lappend(*found, copyJsonbValue(jb));
			break;
		case jpiAny:
		{
			bool hasNext = jspGetNext(jsp, &elem);

			/* first try without any intermediate steps */
			if (jsp->content.anybounds.first == 0)
			{
				if (hasNext)
				{
					res = recursiveExecute(cxt, &elem, jb, found);
					if (res == jperOk && !found)
						break;
				}
				else
				{
					res = jperOk;
					if (!found)
						break;
					*found = lappend(*found, copyJsonbValue(jb));
				}
			}

			if (jb->type == jbvBinary)
				res = recursiveAny(cxt, hasNext ? &elem : NULL, jb, found,
								   1,
								   jsp->content.anybounds.first,
								   jsp->content.anybounds.last);
			break;
		}
		case jpiExists:
			jspGetArg(jsp, &elem);

			if (cxt->lax)
				res = recursiveExecute(cxt, &elem, jb, NULL);
			else
			{
				List *vals = NIL;

				/*
				 * In strict mode we must get a complete list of values
				 * to check that there are no errors at all.
				 */
				res = recursiveExecute(cxt, &elem, jb, &vals);

				if (!jperIsError(res))
					res = list_length(vals) > 0 ? jperOk : jperNotFound;
			}
			break;
		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			if (jspGetNext(jsp, &elem))
			{
				JsonbValue jbv;
				computeJsonPathItem(cxt, jsp, &jbv);
				res = recursiveExecute(cxt, &elem, &jbv, found);
			}
			else
			{
				res = jperOk;
				if (found)
				{
					JsonbValue *jbv = palloc(sizeof(*jbv));
					computeJsonPathItem(cxt, jsp, jbv);
					*found = lappend(*found, jbv);
				}
			}
			break;
		case jpiType:
			{
				JsonbValue *jbv = palloc(sizeof(*jbv));

				jbv->type = jbvString;
				jbv->val.string.val = pstrdup(JsonbTypeName(jb));
				jbv->val.string.len = strlen(jbv->val.string.val);

				res = jperOk;

				if (jspGetNext(jsp, &elem))
					res = recursiveExecute(cxt, &elem, jbv, found);
				else if (found)
					*found = lappend(*found, jbv);
			}
			break;
		case jpiSize:
			{
				int			size = JsonbArraySize(jb);

				if (size < 0)
				{
					if (!cxt->lax)
					{
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

				res = jperOk;

				if (jspGetNext(jsp, &elem))
					res = recursiveExecute(cxt, &elem, jb, found);
				else if (found)
					*found = lappend(*found, jb);
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

					res = jperOk;

					if (jspGetNext(jsp, &elem))
						res = recursiveExecute(cxt, &elem, jb, found);
					else if (found)
						*found = lappend(*found, jb);
				}
				else
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
			}
			break;
		case jpiDouble:
			{
				JsonbValue jbv;
				MemoryContext mcxt = CurrentMemoryContext;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbv);

				PG_TRY();
				{
					if (jb->type == jbvNumeric)
					{
						/* only check success of numeric to double cast */
						DirectFunctionCall1(numeric_float8,
											NumericGetDatum(jb->val.numeric));
						res = jperOk;
					}
					else if (jb->type == jbvString)
					{
						/* cast string as double */
						char	   *str = pnstrdup(jb->val.string.val,
												   jb->val.string.len);
						Datum		val = DirectFunctionCall1(
											float8in, CStringGetDatum(str));
						pfree(str);

						jb = &jbv;
						jb->type = jbvNumeric;
						jb->val.numeric = DatumGetNumeric(DirectFunctionCall1(
														float8_numeric, val));
						res = jperOk;

					}
					else
						res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
				}
				PG_CATCH();
				{
					if (ERRCODE_TO_CATEGORY(geterrcode()) !=
														ERRCODE_DATA_EXCEPTION)
						PG_RE_THROW();

					FlushErrorState();
					MemoryContextSwitchTo(mcxt);
					res = jperMakeError(ERRCODE_NON_NUMERIC_JSON_ITEM);
				}
				PG_END_TRY();

				if (res == jperOk)
				{
					if (jspGetNext(jsp, &elem))
						res = recursiveExecute(cxt, &elem, jb, found);
					else if (found)
						*found = lappend(*found, copyJsonbValue(jb));
				}
			}
			break;
		case jpiDatetime:
			{
				JsonbValue	jbvbuf;
				Datum		value;
				text	   *datetime_txt;
				Oid			typid;
				int32		typmod = -1;
				bool		hasNext;

				if (JsonbType(jb) == jbvScalar)
					jb = JsonbExtractScalar(jb->val.binary.data, &jbvbuf);

				if (jb->type != jbvString)
				{
					res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
					break;
				}

				datetime_txt = cstring_to_text_with_len(jb->val.string.val,
														jb->val.string.len);

				res = jperOk;

				if (jsp->content.arg)
				{
					text	   *template_txt;
					char	   *template_str;
					int			template_len;
					MemoryContext mcxt = CurrentMemoryContext;

					jspGetArg(jsp, &elem);

					if (elem.type != jpiString)
						elog(ERROR, "invalid jsonpath item type for .datetime() argument");

					template_str = jspGetString(&elem, &template_len);
					template_txt = cstring_to_text_with_len(template_str,
															template_len);

					PG_TRY();
					{
						value = to_datetime(datetime_txt,
											template_str, template_len,
											false,
											&typid, &typmod);
					}
					PG_CATCH();
					{
						if (ERRCODE_TO_CATEGORY(geterrcode()) !=
														ERRCODE_DATA_EXCEPTION)
							PG_RE_THROW();

						FlushErrorState();
						MemoryContextSwitchTo(mcxt);

						res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
					}
					PG_END_TRY();

					pfree(template_txt);
				}
				else
				{
					if (!tryToParseDatetime("yyyy-mm-dd HH24:MI:SS TZH:TZM",
									datetime_txt, &value, &typid, &typmod) &&
						!tryToParseDatetime("yyyy-mm-dd HH24:MI:SS TZH",
									datetime_txt, &value, &typid, &typmod) &&
						!tryToParseDatetime("yyyy-mm-dd HH24:MI:SS",
									datetime_txt, &value, &typid, &typmod) &&
						!tryToParseDatetime("yyyy-mm-dd",
									datetime_txt, &value, &typid, &typmod) &&
						!tryToParseDatetime("HH24:MI:SS TZH:TZM",
									datetime_txt, &value, &typid, &typmod) &&
						!tryToParseDatetime("HH24:MI:SS TZH",
									datetime_txt, &value, &typid, &typmod) &&
						!tryToParseDatetime("HH24:MI:SS",
									datetime_txt, &value, &typid, &typmod))
						res = jperMakeError(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
				}

				pfree(datetime_txt);

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

				if (hasNext)
					res = recursiveExecute(cxt, &elem, jb, found);
				else
					*found = lappend(*found, jb);
			}
			break;
		case jpiKeyValue:
			if (JsonbType(jb) != jbvObject)
				res = jperMakeError(ERRCODE_JSON_OBJECT_NOT_FOUND);
			else
			{
				int32		r;
				JsonbValue	key;
				JsonbValue	val;
				JsonbValue	obj;
				JsonbValue	keystr;
				JsonbValue	valstr;
				JsonbIterator *it;
				JsonbParseState *ps = NULL;

				hasNext = jspGetNext(jsp, &elem);

				if (!JsonContainerSize(jb->val.binary.data))
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

				it = JsonbIteratorInit(jb->val.binary.data);

				while ((r = JsonbIteratorNext(&it, &key, true)) != WJB_DONE)
				{
					if (r == WJB_KEY)
					{
						Jsonb	   *jsonb;
						JsonbValue *keyval;

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

						if (hasNext)
						{
							res = recursiveExecute(cxt, &elem, &obj, found);

							if (jperIsError(res))
								break;

							if (res == jperOk && !found)
								break;
						}
						else
							*found = lappend(*found, copyJsonbValue(&obj));
					}
				}
			}
			break;
		case jpiStartsWith:
			res = executeStartsWithPredicate(cxt, jsp, jb);
			break;
		case jpiLikeRegex:
			res = executeLikeRegexPredicate(cxt, jsp, jb);
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

static JsonPathExecResult
recursiveExecuteUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, List **found)
{
	if (cxt->lax && JsonbType(jb) == jbvArray)
	{
		JsonbValue	v;
		JsonbIterator *it;
		JsonbIteratorToken tok;
		JsonPathExecResult res = jperNotFound;

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

		return res;
	}

	return recursiveExecuteNoUnwrap(cxt, jsp, jb, found);
}

/*
 * Wrap a non-array SQL/JSON item into an array for applying array subscription
 * path steps in lax mode.
 */
static JsonbValue *
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

static JsonPathExecResult
recursiveExecute(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
				 List **found)
{
	check_stack_depth();

	if (cxt->lax)
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
executeJsonPath(JsonPath *path, List *vars, Jsonb *json, List **foundJson)
{
	JsonPathExecContext cxt;
	JsonPathItem	jsp;
	JsonbValue		jbv;

	JsonbInitBinary(&jbv, json);

	jspInit(&jsp, path);

	cxt.vars = vars;
	cxt.lax = (path->header & JSONPATH_LAX) != 0;
	cxt.innermostArraySize = -1;

	if (!cxt.lax && !foundJson)
	{
		/*
		 * In strict mode we must get a complete list of values to check
		 * that there are no errors at all.
		 */
		List	   *vals = NIL;
		JsonPathExecResult res = recursiveExecute(&cxt, &jsp, &jbv, &vals);

		if (jperIsError(res))
			return res;

		return list_length(vals) > 0 ? jperOk : jperNotFound;
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
	JsonbValue	v;
	JsonbIterator *it;
	int32		r;
	List	   *vars = NIL;

	it = JsonbIteratorInit(&jb->root);

	r =  JsonbIteratorNext(&it, &v, true);

	if (r != WJB_BEGIN_OBJECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("json containing jsonpath variables is not an object")));

	while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			JsonPathVariable *jpv = palloc0(sizeof(*jpv));

			jpv->varName = cstring_to_text_with_len(v.val.string.val,
													v.val.string.len);

			JsonbIteratorNext(&it, &v, true);

			/* Datums are copied from jsonb into the current memory context. */
			jpv->cb = returnDATUM;

			switch (v.type)
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
					jpv->cb_arg = DatumGetPointer(
						datumCopy(NumericGetDatum(v.val.numeric), false, -1));
					break;
				case jbvBinary:
					jpv->typid = JSONBOID;
					jpv->cb_arg = DatumGetPointer(JsonbPGetDatum(JsonbValueToJsonb(&v)));
					break;
				default:
					elog(ERROR, "invalid jsonb value type");
			}

			vars = lappend(vars, jpv);
		}
	}

	return vars;
}

static void
throwJsonPathError(JsonPathExecResult res)
{
	if (!jperIsError(res))
		return;

	switch (jperGetError(res))
	{
		case ERRCODE_JSON_ARRAY_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON array not found")));
			break;
		case ERRCODE_JSON_OBJECT_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON object not found")));
			break;
		case ERRCODE_JSON_MEMBER_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON member not found")));
			break;
		case ERRCODE_JSON_NUMBER_NOT_FOUND:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON number not found")));
			break;
		case ERRCODE_JSON_SCALAR_REQUIRED:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("SQL/JSON scalar required")));
			break;
		case ERRCODE_SINGLETON_JSON_ITEM_REQUIRED:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Singleton SQL/JSON item required")));
			break;
		case ERRCODE_NON_NUMERIC_JSON_ITEM:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Non-numeric SQL/JSON item")));
			break;
		case ERRCODE_INVALID_JSON_SUBSCRIPT:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Invalid SQL/JSON subscript")));
			break;
		case ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION:
			ereport(ERROR,
					(errcode(jperGetError(res)),
					 errmsg("Invalid argument for SQL/JSON datetime function")));
			break;
		default:
			ereport(ERROR,
					(errcode(jperGetError(res)),
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
		PG_RETURN_NULL();

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

static Datum
jsonb_jsonpath_query(FunctionCallInfo fcinfo)
{
	FuncCallContext	*funcctx;
	List			*found = NIL;
	JsonbValue		*v;
	ListCell		*c;

	if (SRF_IS_FIRSTCALL())
	{
		JsonPath			*jp = PG_GETARG_JSONPATH_P(1);
		Jsonb				*jb;
		JsonPathExecResult	res;
		MemoryContext		oldcontext;
		List				*vars = NIL;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		jb = PG_GETARG_JSONB_P_COPY(0);
		if (PG_NARGS() == 3)
			vars = makePassingVars(PG_GETARG_JSONB_P(2));

		res = executeJsonPath(jp, vars, jb, &found);

		if (jperIsError(res))
			throwJsonPathError(res);

		PG_FREE_IF_COPY(jp, 1);

		funcctx->user_fctx = found;

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
