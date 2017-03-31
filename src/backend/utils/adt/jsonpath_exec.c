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
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/json.h"
#include "utils/jsonpath.h"
#include "utils/varlena.h"

typedef struct JsonPathExecContext
{
	List	   *vars;
	bool		lax;
} JsonPathExecContext;

static JsonPathExecResult recursiveExecute(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonbValue *jb,
										   List **found);

static inline JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = &jb->root;
	jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

	return jbv;
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

		if (jbc->header & JB_FSCALAR)
			type = jbvScalar;
		else if (jbc->header & JB_FOBJECT)
			type = jbvObject;
		else if (jbc->header & JB_FARRAY)
			type = jbvArray;
		else
			elog(ERROR, "Unknown container type: 0x%08x", jbc->header);
	}

	return type;
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
	res = recursiveExecute(cxt, &elem, jb, &lseq);
	if (res != jperOk)
		return res;

	jspGetRightArg(jsp, &elem);
	res = recursiveExecute(cxt, &elem, jb, &rseq);
	if (res != jperOk)
		return res;

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

	if (error) /* possible only in non-strict mode */
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

	jper = recursiveExecute(cxt, &elem, jb, &lseq);

	if (jper == jperOk)
	{
		jspGetRightArg(jsp, &elem);
		jper = recursiveExecute(cxt, &elem, jb, &rseq);
	}

	if (jper != jperOk || list_length(lseq) != 1 || list_length(rseq) != 1)
		return jperError; /* ERRCODE_SINGLETON_JSON_ITEM_REQUIRED; */

	lval = linitial(lseq);

	if (JsonbType(lval) == jbvScalar)
		lval = JsonbExtractScalar(lval->val.binary.data, &lvalbuf);

	if (lval->type != jbvNumeric)
		return jperError; /* ERRCODE_SINGLETON_JSON_ITEM_REQUIRED; */

	rval = linitial(rseq);

	if (JsonbType(rval) == jbvScalar)
		rval = JsonbExtractScalar(rval->val.binary.data, &rvalbuf);

	if (rval->type != jbvNumeric)
		return jperError; /* ERRCODE_SINGLETON_JSON_ITEM_REQUIRED; */

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

static JsonbValue *
copyJsonbValue(JsonbValue *src)
{
	JsonbValue	*dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
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
	jper = recursiveExecute(cxt, &elem, jb, &seq);

	if (jper == jperError)
		return jperError; /* ERRCODE_JSON_NUMBER_NOT_FOUND; */

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
			return jperError; /* ERRCODE_JSON_NUMBER_NOT_FOUND; */

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
recursiveExecute(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
				 List **found)
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
			switch((res = recursiveExecute(cxt, &elem, jb, NULL)))
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
			res = (res == jperError) ? jperOk : jperNotFound;
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

							if (res == jperError)
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
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray)
			{
				JsonbValue		*v;
				int				i;

				hasNext = jspGetNext(jsp, &elem);

				for(i=0; i<jsp->content.array.nelems; i++)
				{
					/* TODO for future: array index can be expression */
					v = getIthJsonbValueFromContainer(jb->val.binary.data,
													  jsp->content.array.elems[i]);

					if (v == NULL)
						continue;

					if (hasNext == true)
					{
						res = recursiveExecute(cxt, &elem, v, found);

						if (res == jperError || found == NULL)
							break;

						if (res == jperOk && found == NULL)
								break;
					}
					else
					{
						res = jperOk;

						if (found == NULL)
							break;

						*found = lappend(*found, v);
					}
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

							if (res == jperError)
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
			res = recursiveExecute(cxt, &elem, jb, NULL);
			break;
		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			res = jperOk;
			if (found)
			{
				JsonbValue *jbv = palloc(sizeof(*jbv));
				computeJsonPathItem(cxt, jsp, jbv);
				*found = lappend(*found, jbv);
			}
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
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

	cxt.vars = vars;
	cxt.lax = false; /* FIXME */

	JsonbInitBinary(&jbv, json);
	
	jspInit(&jsp, path);

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

	if (res == jperError)
		elog(ERROR, "Something wrong");

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
jsonb_jsonpath_query(PG_FUNCTION_ARGS)
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

		if (res == jperError)
			elog(ERROR, "Something wrong");

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
