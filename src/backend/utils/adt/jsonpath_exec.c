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
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/json.h"
#include "utils/jsonpath.h"

/********************Execute functions for JsonPath***************************/

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
		case (Oid) -1: /* raw JsonbValue */
			*value = *(JsonbValue *) DatumGetPointer(computedValue);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only bool, numeric and text types could be casted to supported jsonpath types")));
	}
}

static void
computeJsonPathItem(JsonPathItem *item, List *vars, JsonbValue *value)
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
			computeJsonPathVariable(item, vars, value);
			break;
		default:
			elog(ERROR, "Wrong type");
	}
}


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
static bool
checkScalarEquality(JsonPathItem *jsp,  List *vars, JsonbValue *jb)
{
	JsonbValue		computedValue;

	if (jb->type == jbvBinary)
		return false;

	computeJsonPathItem(jsp, vars, &computedValue);

	if (jb->type != computedValue.type)
		return false;

	switch(computedValue.type)
	{
		case jbvNull:
			return true;
		case jbvString:
			return (computedValue.val.string.len == jb->val.string.len &&
					memcmp(jb->val.string.val, computedValue.val.string.val,
						   computedValue.val.string.len) == 0);
		case jbvBool:
			return (jb->val.boolean == computedValue.val.boolean);
		case jbvNumeric:
			return (compareNumeric(computedValue.val.numeric, jb->val.numeric) == 0);
		default:
			elog(ERROR,"Wrong state");
	}

	return false;
}

static bool
makeCompare(JsonPathItem *jsp, List *vars, int32 op, JsonbValue *jb)
{
	int				res;
	JsonbValue		computedValue;

	if (jb->type != jbvNumeric)
		return false;

	computeJsonPathItem(jsp, vars, &computedValue);

	if (computedValue.type != jbvNumeric)
		return false;

	res = compareNumeric(jb->val.numeric, computedValue.val.numeric);

	switch(op)
	{
		case jpiEqual:
			return (res == 0);
		case jpiLess:
			return (res < 0);
		case jpiGreater:
			return (res > 0);
		case jpiLessOrEqual:
			return (res <= 0);
		case jpiGreaterOrEqual:
			return (res >= 0);
		default:
			elog(ERROR, "Unknown operation");
	}

	return false;
}

static JsonPathExecResult
executeExpr(JsonPathItem *jsp, List *vars, int32 op, JsonbValue *jb)
{
	JsonPathExecResult res = jperNotFound;
	/*
	 * read arg type
	 */
	Assert(jspGetNext(jsp, NULL) == false);
	Assert(jsp->type == jpiString || jsp->type == jpiNumeric ||
		   jsp->type == jpiNull || jsp->type == jpiBool ||
		   jsp->type == jpiVariable);

	switch(op)
	{
		case jpiEqual:
			res = checkScalarEquality(jsp, vars, jb) ? jperOk : jperNotFound;
			break;
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			res = makeCompare(jsp, vars, op, jb) ? jperOk : jperNotFound;
			break;
		default:
			elog(ERROR, "Unknown operation");
	}

	return res;
}

static JsonbValue*
copyJsonbValue(JsonbValue *src)
{
	JsonbValue	*dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
}

static JsonPathExecResult
recursiveExecute(JsonPathItem *jsp, List *vars, JsonbValue *jb, List **found)
{
	JsonPathItem		elem;
	JsonPathExecResult	res = jperNotFound;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch(jsp->type) {
		case jpiAnd:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecute(&elem, vars, jb, NULL);
			if (res == jperOk)
			{
				jspGetRightArg(jsp, &elem);
				res = recursiveExecute(&elem, vars, jb, NULL);
			}
			break;
		case jpiOr:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecute(&elem, vars, jb, NULL);
			if (res == jperNotFound)
			{
				jspGetRightArg(jsp, &elem);
				res = recursiveExecute(&elem, vars, jb, NULL);
			}
			break;
		case jpiNot:
			jspGetArg(jsp, &elem);
			switch((res = recursiveExecute(&elem, vars, jb, NULL)))
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
						res = recursiveExecute(&elem, vars, v, found);
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
			jspGetNext(jsp, &elem);
			if (JsonbType(jb) == jbvScalar)
			{
				JsonbValue	v;

				JsonbExtractScalar(jb->val.binary.data, &v);

				res = recursiveExecute(&elem, vars, &v, NULL);
			}
			else
			{
				res = recursiveExecute(&elem, vars, jb, NULL);
			}
			break;
		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				JsonbIterator	*it;
				int32			r;
				JsonbValue		v;
				bool			hasNext;

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_ELEM)
					{
						if (hasNext == true)
						{
							res = recursiveExecute(&elem, vars, &v, found);

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
				bool			hasNext;
				int				i;

				hasNext = jspGetNext(jsp, &elem);

				for(i=0; i<jsp->array.nelems; i++)
				{
					/* TODO for future: array index can be expression */
					v = getIthJsonbValueFromContainer(jb->val.binary.data,
													  jsp->array.elems[i]);

					if (v == NULL)
						continue;

					if (hasNext == true)
					{
						res = recursiveExecute(&elem, vars, v, found);

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
				bool			hasNext;

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_VALUE)
					{
						if (hasNext == true)
						{
							res = recursiveExecute(&elem, vars, &v, found);

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
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			jspGetArg(jsp, &elem);
			res = executeExpr(&elem, vars, jsp->type, jb);
			break;
		case jpiRoot:
			if (jspGetNext(jsp, &elem))
			{
				res = recursiveExecute(&elem, vars, jb, found);
			}
			else
			{
				res = jperOk;
				if (found)
					*found = lappend(*found, copyJsonbValue(jb));
			}

			break;
		case jpiExpression:
			/* no-op actually */
			jspGetArg(jsp, &elem);
			res = recursiveExecute(&elem, vars, jb, NULL);
			if (res == jperOk && found)
				*found = lappend(*found, copyJsonbValue(jb));
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

JsonPathExecResult
executeJsonPath(JsonPath *path, List *vars, Jsonb *json, List **foundJson)
{
	JsonPathItem	jsp;
	JsonbValue		jbv;

	jbv.type = jbvBinary;
	jbv.val.binary.data = &json->root;
	jbv.val.binary.len = VARSIZE_ANY_EXHDR(json);

	jspInit(&jsp, path);

	return recursiveExecute(&jsp, vars, &jbv, foundJson);
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
