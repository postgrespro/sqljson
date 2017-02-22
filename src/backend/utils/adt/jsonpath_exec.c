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
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonpath.h"

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

static bool
checkScalarEquality(JsonPathItem *jsp,  JsonbValue *jb)
{
	int		len;
	char	*s;

	if (jb->type == jbvBinary)
		return false;

	if ((int)jb->type != (int)jsp->type /* see enums */)
		return false;

	switch(jsp->type)
	{
		case jpiNull:
			return true;
		case jpiString:
			s = jspGetString(jsp, &len);
			return (len == jb->val.string.len && memcmp(jb->val.string.val, s, len) == 0);
		case jpiBool:
			return (jb->val.boolean == jspGetBool(jsp));
		case jpiNumeric:
			return (compareNumeric(jspGetNumeric(jsp), jb->val.numeric) == 0);
		default:
			elog(ERROR,"Wrong state");
	}

	return false;
}

static bool
makeCompare(JsonPathItem *jsp, int32 op, JsonbValue *jb)
{
	int	res;

	if (jb->type != jbvNumeric)
		return false;
	if (jsp->type != jpiNumeric)
		return false;

	res = compareNumeric(jb->val.numeric, jspGetNumeric(jsp));

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

static bool
executeExpr(JsonPathItem *jsp, int32 op, JsonbValue *jb)
{
	bool res = false;
	/*
	 * read arg type
	 */
	Assert(jspGetNext(jsp, NULL) == false);
	Assert(jsp->type == jpiString || jsp->type == jpiNumeric ||
		   jsp->type == jpiNull || jsp->type == jpiBool);

	switch(op)
	{
		case jpiEqual:
			res = checkScalarEquality(jsp, jb);
			break;
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			res = makeCompare(jsp, op, jb);
			break;
		default:
			elog(ERROR, "Unknown operation");
	}

	return res;
}

static JsonPathExecResult
recursiveExecute(JsonPathItem *jsp, JsonbValue *jb, List **found)
{
	JsonPathItem		elem;
	JsonPathExecResult	res = jperNotFound;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch(jsp->type) {
		case jpiAnd:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecute(&elem, jb, NULL);
			if (res == jperOk)
			{
				jspGetRightArg(jsp, &elem);
				res = recursiveExecute(&elem, jb, NULL);
			}
			break;
		case jpiOr:
			jspGetLeftArg(jsp, &elem);
			res = recursiveExecute(&elem, jb, NULL);
			if (res == jperNotFound)
			{
				jspGetRightArg(jsp, &elem);
				res = recursiveExecute(&elem, jb, NULL);
			}
			break;
		case jpiNot:
			jspGetArg(jsp, &elem);
			switch((res = recursiveExecute(&elem, jb, NULL)))
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
						res = recursiveExecute(&elem, v, found);
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

				res = recursiveExecute(&elem, &v, NULL);
			}
			else
			{
				res = recursiveExecute(&elem, jb, NULL);
			}
			break;
		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				JsonbIterator	*it;
				int32			r;
				JsonbValue		v, *pv;
				bool			hasNext;

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_ELEM)
					{
						if (hasNext == true)
						{
							res = recursiveExecute(&elem, &v, found);

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

							pv = palloc(sizeof(*pv));
							*pv = v;
							*found = lappend(*found, pv);
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
						res = recursiveExecute(&elem, v, found);

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
				JsonbValue		v, *pv;
				bool			hasNext;

				hasNext = jspGetNext(jsp, &elem);
				it = JsonbIteratorInit(jb->val.binary.data);

				while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_VALUE)
					{
						if (hasNext == true)
						{
							res = recursiveExecute(&elem, &v, found);

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

							pv = palloc(sizeof(*pv));
							*pv = v;
							*found = lappend(*found, pv);
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
			res = executeExpr(&elem, jsp->type, jb);
			break;
		case jpiRoot:
			/* no-op actually */
			jspGetNext(jsp, &elem);
			res = recursiveExecute(&elem, jb, found);
			break;
		case jpiExpression:
			/* no-op actually */
			jspGetNext(jsp, &elem);
			res = recursiveExecute(&elem, jb, NULL);
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

JsonPathExecResult
executeJsonPath(JsonPath *path, Jsonb *json, List **foundJson)
{
	JsonPathItem	jsp;
	JsonbValue		jbv;

	jbv.type = jbvBinary;
	jbv.val.binary.data = &json->root;
	jbv.val.binary.len = VARSIZE_ANY_EXHDR(json);

	jspInit(&jsp, path);

	return recursiveExecute(&jsp, &jbv, foundJson);
}

Datum
jsonb_jsonpath_exists(PG_FUNCTION_ARGS)
{
	Jsonb				*jb = PG_GETARG_JSONB_P(0);
	JsonPath			*jp = PG_GETARG_JSONPATH_P(1);
	JsonPathExecResult	res;

	res = executeJsonPath(jp, jb, NULL);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (res == jperError)
		elog(ERROR, "Something wrong");

	PG_RETURN_BOOL(res == jperOk);
}

Datum
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

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		jb = PG_GETARG_JSONB_P_COPY(0);
		res = executeJsonPath(jp, jb, &found);

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
