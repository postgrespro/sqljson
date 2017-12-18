/*-------------------------------------------------------------------------
 *
 * jsonpathx.c
 *	   Extended jsonpath item methods and operators for jsonb type.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	   contrib/jsonpathx/jsonpathx.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/jsonpath.h"

PG_MODULE_MAGIC;

static JsonPathExecResult
jspThrowComparisonError(JsonPathExecContext *cxt, const char *methodName)
{
	if (jspThrowErrors(cxt))
		ereport(ERROR,
				(errcode(ERRCODE_JSON_SCALAR_REQUIRED),
				 errmsg(ERRMSG_JSON_SCALAR_REQUIRED),
				 errdetail("boolean lambda expression in jsonpath .%s() "
						   "returned Unknown", methodName)));
	return jperError;
}

static JsonPathExecResult
jspThrowSingletonRequiredError(JsonPathExecContext *cxt, const char *methodName)
{
	if (jspThrowErrors(cxt))
		ereport(ERROR,
				(errcode(ERRCODE_SINGLETON_JSON_ITEM_REQUIRED),
				 errmsg(ERRMSG_SINGLETON_JSON_ITEM_REQUIRED),
				 errdetail("lambda expression in .%s() should return singleton item",
						   methodName)));
	return jperError;
}

static JsonPathExecResult
jspThrowArrayNotFoundError(JsonPathExecContext *cxt, const char *methodName)
{
	if (jspThrowErrors(cxt))
		ereport(ERROR,
				(errcode(ERRCODE_JSON_ARRAY_NOT_FOUND),
				 errmsg(ERRMSG_JSON_ARRAY_NOT_FOUND),
				 errdetail("jsonpath .%s() is applied to not an array",
						   methodName)));

	return jperError;
}

static JsonPathExecResult
jspThrowWrongArgumentsError(JsonPathExecContext *cxt, int req, int given,
							const char *methodName)
{
	if (jspThrowErrors(cxt))
		ereport(ERROR,
				(errcode(ERRCODE_JSON_SCALAR_REQUIRED),
				 errmsg(ERRMSG_JSON_SCALAR_REQUIRED),
				 errdetail("jsonpath .%s() requires %d arguments "
						   "but given %d", methodName, req, given)));
	return jperError;
}


static JsonPathExecResult
jspExecuteSingleton(JsonPathExecContext *cxt, JsonPathItem *jsp,
					JsonItem *jb, JsonItem **result)
{
	JsonValueList reslist = {0};
	JsonPathExecResult res = jspExecuteItem(cxt, jsp, jb, &reslist);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&reslist) != 1)
		return jspThrowSingletonRequiredError(cxt, "");

	*result = JsonValueListHead(&reslist);

	return jperOk;
}

static JsonPathExecResult
jspMap(JsonPathFuncContext *fcxt, bool flat)
{
	JsonPathExecContext *cxt = fcxt->cxt;
	JsonItem   *jb = fcxt->item;
	JsonPathItem *func = &fcxt->args[jb ? 0 : 1];
	void	  **funccache = &fcxt->argscache[jb ? 0 : 1];
	JsonPathExecResult res;
	JsonItem   *args[3];
	JsonItem	jbvidx;
	int			index = 0;
	int			nargs = 1;

	if (fcxt->nargs != (jb ? 1 : 2))
		return jspThrowWrongArgumentsError(cxt, jb ? 1 : 2, fcxt->nargs,
										   fcxt->funcname);

	if (func->type == jpiLambda && func->content.lambda.nparams > 1)
	{
		args[nargs++] = &jbvidx;
		JsonItemGetType(&jbvidx) = jbvNumeric;
	}

	if (!jb)
	{
		JsonValueList items = {0};
		JsonValueListIterator iter;
		JsonItem   *item;

		res = jspExecuteItem(cxt, &fcxt->args[0], fcxt->jb, &items);

		if (jperIsError(res))
			return res;

		JsonValueListInitIterator(&items, &iter);

		while ((item = JsonValueListNext(&items, &iter)))
		{
			JsonValueList reslist = {0};

			args[0] = item;

			if (nargs > 1)
			{
				JsonItemNumeric(&jbvidx) = DatumGetNumeric(
					DirectFunctionCall1(int4_numeric, Int32GetDatum(index)));
				index++;
			}

			res = jspExecuteLambda(cxt, func, fcxt->jb, &reslist,
								   args, nargs, funccache);
			if (jperIsError(res))
				return res;

			if (flat)
			{
				JsonValueListConcat(fcxt->result, reslist);
			}
			else
			{
				if (JsonValueListLength(&reslist) != 1)
					return jspThrowSingletonRequiredError(cxt, fcxt->funcname);

				JsonValueListAppend(fcxt->result, JsonValueListHead(&reslist));
			}
		}
	}
	else if (JsonbType(jb) != jbvArray)
	{
		JsonValueList reslist = {0};
		JsonItemStackEntry entry;

		if (!jspAutoWrap(cxt))
			return jspThrowArrayNotFoundError(cxt, flat ? "flatmap" : "map");

		args[0] = jb;

		if (nargs > 1)
			JsonItemNumeric(&jbvidx) = DatumGetNumeric(
				DirectFunctionCall1(int4_numeric, Int32GetDatum(0)));

		/* push additional stack entry for the whole item */
		pushJsonItem(&cxt->stack, &entry, jb, &cxt->baseObject);
		res = jspExecuteLambda(cxt, func, jb, &reslist, args, nargs, funccache);
		popJsonItem(&cxt->stack);

		if (jperIsError(res))
			return res;

		if (flat)
		{
			JsonValueListConcat(fcxt->result, reslist);
		}
		else
		{
			if (JsonValueListLength(&reslist) != 1)
				return jspThrowSingletonRequiredError(cxt, fcxt->funcname);

			JsonValueListAppend(fcxt->result, JsonValueListHead(&reslist));
		}
	}
	else
	{
		JsonbValue	elembuf;
		JsonbValue *elem;
		JsonxIterator it;
		JsonbIteratorToken tok;
		JsonValueList result = {0};
		JsonItemStackEntry entry;
		int			size = JsonxArraySize(jb, cxt->isJsonb);
		int			i;
		bool		isBinary = JsonItemIsBinary(jb);

		if (isBinary && size > 0)
		{
			elem = &elembuf;
			JsonxIteratorInit(&it, JsonItemBinary(jb).data, cxt->isJsonb);
			tok = JsonxIteratorNext(&it, &elembuf, false);
			if (tok != WJB_BEGIN_ARRAY)
				elog(ERROR, "unexpected jsonb token at the array start");
		}

		/* push additional stack entry for the whole array */
		pushJsonItem(&cxt->stack, &entry, jb, &cxt->baseObject);

		if (nargs > 1)
		{
			nargs = 3;
			args[2] = jb;
		}

		for (i = 0; i < size; i++)
		{
			JsonValueList reslist = {0};
			JsonItem	elemjsi;

			if (isBinary)
			{
				tok = JsonxIteratorNext(&it, elem, true);
				if (tok != WJB_ELEM)
					break;
			}
			else
				elem = &JsonItemArray(jb).elems[i];

			args[0] = JsonbValueToJsonItem(elem, &elemjsi);

			if (nargs > 1)
			{
				JsonItemNumeric(&jbvidx) = DatumGetNumeric(
					DirectFunctionCall1(int4_numeric, Int32GetDatum(index)));
				index++;
			}

			res = jspExecuteLambda(cxt, func, jb, &reslist, args, nargs, funccache);

			if (jperIsError(res))
			{
				popJsonItem(&cxt->stack);
				return res;
			}

			if (JsonValueListLength(&reslist) != 1)
			{
				popJsonItem(&cxt->stack);
				return jspThrowSingletonRequiredError(cxt, fcxt->funcname);
			}

			if (flat)
			{
				JsonItem   *jsarr = JsonValueListHead(&reslist);

				if (JsonbType(jsarr) == jbvArray)
				{
					if (JsonItemIsBinary(jsarr))
					{
						JsonxIterator it;
						JsonbIteratorToken tok;
						JsonbValue	elem;

						JsonxIteratorInit(&it, JsonItemBinary(jsarr).data, cxt->isJsonb);

						while ((tok = JsonxIteratorNext(&it, &elem, true)) != WJB_DONE)
						{
							if (tok == WJB_ELEM)
							{
								JsonItem   *jsi = palloc(sizeof(*jsi));

								JsonValueListAppend(&result,
													JsonbValueToJsonItem(&elem, jsi));
							}
						}
					}
					else
					{
						JsonbValue  *elem;
						int			size = JsonItemArray(jsarr).nElems;
						int			i = 0;

						for (i = 0; i < size; i++)
						{
							JsonItem   *jsi = palloc(sizeof(*jsi));

							elem = &JsonItemArray(jsarr).elems[i];

							JsonValueListAppend(&result,
												JsonbValueToJsonItem(elem, jsi));
						}
					}
				}
				else if (jspAutoWrap(cxt))
				{
					JsonValueListConcat(&result, reslist);
				}
				else
				{
					popJsonItem(&cxt->stack);
					return jspThrowArrayNotFoundError(cxt, fcxt->funcname);
				}
			}
			else
			{
				JsonValueListConcat(&result, reslist);
			}
		}

		popJsonItem(&cxt->stack);

		JsonAppendWrappedItems(fcxt->result, &result, cxt->isJsonb);
	}

	return jperOk;
}

PG_FUNCTION_INFO_V1(jsonpath_map);
Datum
jsonpath_map(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspMap((void *) PG_GETARG_POINTER(0), false));
}

PG_FUNCTION_INFO_V1(jsonpath_flatmap);
Datum
jsonpath_flatmap(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspMap((void *) PG_GETARG_POINTER(0), true));
}

typedef enum FoldType { FOLD_REDUCE, FOLD_LEFT, FOLD_RIGHT } FoldType;

typedef struct FoldContext
{
	JsonPathExecContext *cxt;
	JsonPathItem *func;
	const char *funcname;
	void	  **pfunccache;
	JsonItem   *item;
	JsonItem   *result;
	JsonItem   *args[4];
	JsonItem  **argres;
	JsonItem  **argelem;
	JsonItem	argidx;
	int			nargs;
} FoldContext;

static void
foldInit(FoldContext *fcxt, JsonPathExecContext *cxt, JsonPathItem *func,
		 void **pfunccache, JsonItem *array, JsonItem *item,
		 JsonItem *result, FoldType foldtype, const char *funcname)
{
	fcxt->cxt = cxt;
	fcxt->func = func;
	fcxt->pfunccache = pfunccache;
	fcxt->item = item;
	fcxt->result = result;
	fcxt->funcname = funcname;

	if (foldtype == FOLD_RIGHT)
	{
		/* swap args for foldr() */
		fcxt->argres = &fcxt->args[1];
		fcxt->argelem = &fcxt->args[0];
	}
	else
	{
		fcxt->argres = &fcxt->args[0];
		fcxt->argelem = &fcxt->args[1];
	}

	fcxt->nargs = 2;

	if (func->type == jpiLambda && func->content.lambda.nparams > 2)
	{
		fcxt->args[fcxt->nargs++] = &fcxt->argidx;
		JsonItemGetType(&fcxt->argidx) = jbvNumeric;
		if (array)
			fcxt->args[fcxt->nargs++] = array;
	}
}

static JsonPathExecResult
foldAccumulate(FoldContext *fcxt, JsonItem *element, int index)
{
	JsonValueList reslist = {0};
	JsonPathExecResult res;

	if (!fcxt->result) /* first element of reduce */
	{
		fcxt->result = element;
		return jperOk;
	}

	*fcxt->argres = fcxt->result;
	*fcxt->argelem = element;

	if (fcxt->nargs > 2)
		JsonItemNumeric(&fcxt->argidx) = DatumGetNumeric(
			DirectFunctionCall1(int4_numeric, Int32GetDatum(index)));

	res = jspExecuteLambda(fcxt->cxt, fcxt->func, fcxt->item, &reslist,
						   fcxt->args, fcxt->nargs, fcxt->pfunccache);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&reslist) != 1)
		return jspThrowSingletonRequiredError(fcxt->cxt, fcxt->funcname);

	fcxt->result = JsonValueListHead(&reslist);

	return jperOk;
}

static JsonItem *
foldDone(FoldContext *fcxt)
{
	return fcxt->result;
}

static void
list_reverse(List *itemlist)
{
	ListCell   *curlc;
	ListCell   *prevlc = NULL;

	if (list_length(itemlist) <= 1)
		return;

	curlc = itemlist->head;
	itemlist->head = itemlist->tail;
	itemlist->tail = curlc;

	while (curlc)
	{
		ListCell *next = curlc->next;

		curlc->next = prevlc;
		prevlc = curlc;
		curlc = next;
	}
}

static JsonPathExecResult
jspFoldSeq(JsonPathFuncContext *fcxt, FoldType ftype)
{
	FoldContext foldcxt;
	JsonValueList items = {0};
	JsonItem   *result = NULL;
	JsonPathExecResult res;
	int			size;

	res = jspExecuteItem(fcxt->cxt, &fcxt->args[0], fcxt->jb, &items);

	if (jperIsError(res))
		return res;

	size = JsonValueListLength(&items);

	if (ftype == FOLD_REDUCE)
	{
		if (!size)
			return jperNotFound;

		if (size == 1)
		{
			JsonValueListAppend(fcxt->result, JsonValueListHead(&items));
			return jperOk;
		}
	}
	else
	{
		res = jspExecuteSingleton(fcxt->cxt, &fcxt->args[2], fcxt->jb, &result);
		if (jperIsError(res))
			return res;

		if (!size)
		{
			JsonValueListAppend(fcxt->result, result);
			return jperOk;
		}
	}

	foldInit(&foldcxt, fcxt->cxt, &fcxt->args[1], &fcxt->argscache[1], NULL,
			 fcxt->jb, result, ftype, fcxt->funcname);

	if (ftype == FOLD_RIGHT)
	{
		List	   *itemlist = JsonValueListGetList(&items);
		ListCell   *lc;
		int			index = list_length(itemlist) - 1;

		list_reverse(itemlist);

		foreach(lc, itemlist)
		{
			res = foldAccumulate(&foldcxt, lfirst(lc), index--);

			if (jperIsError(res))
			{
				(void) foldDone(&foldcxt);
				return res;
			}
		}
	}
	else
	{
		JsonValueListIterator iter;
		JsonItem   *item;
		int			index = 0;

		JsonValueListInitIterator(&items, &iter);

		while ((item = JsonValueListNext(&items, &iter)))
		{
			res = foldAccumulate(&foldcxt, item, index++);

			if (jperIsError(res))
			{
				(void) foldDone(&foldcxt);
				return res;
			}
		}
	}

	result = foldDone(&foldcxt);

	JsonValueListAppend(fcxt->result, result);

	return jperOk;
}

static JsonPathExecResult
jspFoldArray(JsonPathFuncContext *fcxt, FoldType ftype, JsonItem *item)
{
	JsonItem   *result = NULL;
	JsonPathExecResult res;
	int			size;

	if (JsonbType(item) != jbvArray)
	{
		if (!jspAutoWrap(fcxt->cxt))
			return jspThrowArrayNotFoundError(fcxt->cxt, fcxt->funcname);

		if (ftype == FOLD_REDUCE)
		{
			JsonValueListAppend(fcxt->result, item);
			return jperOk;
		}

		item = JsonWrapItemInArray(item, fcxt->cxt->isJsonb);
	}

	size = JsonxArraySize(item, fcxt->cxt->isJsonb);

	if (ftype == FOLD_REDUCE)
	{
		if (!size)
			return jperNotFound;
	}
	else
	{
		res = jspExecuteSingleton(fcxt->cxt, &fcxt->args[1], fcxt->jb, &result);
		if (jperIsError(res))
			return res;
	}

	if (ftype == FOLD_REDUCE && size == 1)
	{
		JsonbValue	jbvresbuf;
		JsonbValue *jbvres;

		result = palloc(sizeof(*result));

		if (JsonItemIsBinary(item))
		{
			jbvres = fcxt->cxt->isJsonb ?
				getIthJsonbValueFromContainer(JsonItemBinary(item).data, 0, &jbvresbuf) :
				getIthJsonValueFromContainer((JsonContainer *) JsonItemBinary(item).data, 0, &jbvresbuf);

			if (!jbvres)
				return jperNotFound;
		}
		else
		{
			Assert(JsonItemIsArray(item));
			jbvres = &JsonItemArray(item).elems[0];
		}

		JsonbValueToJsonItem(jbvres, result);
	}
	else if (size)
	{
		FoldContext foldcxt;
		JsonxIterator it;
		JsonbIteratorToken tok;
		JsonbValue	elembuf;
		JsonbValue *elem;
		JsonItem	itembuf;
		int			i;
		bool		foldr = ftype == FOLD_RIGHT;
		bool		useIter = false;

		if (JsonItemIsBinary(item))
		{
			if (foldr)
			{
				/* unpack array for reverse iteration */
				JsonbParseState *ps = NULL;
				JsonbValue *jbv = (fcxt->cxt->isJsonb ? pushJsonbValue : pushJsonValue)
					(&ps, WJB_ELEM, JsonItemJbv(item));

				item = JsonbValueToJsonItem(jbv, &itembuf);
			}
			else
			{
				elem = &elembuf;
				useIter = true;
				JsonxIteratorInit(&it, JsonItemBinary(item).data, fcxt->cxt->isJsonb);
				tok = JsonxIteratorNext(&it, elem, false);
				if (tok != WJB_BEGIN_ARRAY)
					elog(ERROR, "unexpected jsonb token at the array start");
			}
		}

		foldInit(&foldcxt, fcxt->cxt, &fcxt->args[0], &fcxt->argscache[0],
				 item, fcxt->jb, result, ftype, fcxt->funcname);

		for (i = 0; i < size; i++)
		{
			JsonItem	elbuf;
			JsonItem   *el;
			int			index;

			if (useIter)
			{
				tok = JsonxIteratorNext(&it, elem, true);
				if (tok != WJB_ELEM)
					break;
				index = i;
			}
			else
			{
				index = foldr ? size - i - 1 : i;
				elem = &JsonItemArray(item).elems[index];
			}

			el = JsonbValueToJsonItem(elem, &elbuf);

			if (!i && ftype == FOLD_REDUCE)
				el = copyJsonItem(el);

			res = foldAccumulate(&foldcxt, el, index);

			if (jperIsError(res))
			{
				(void) foldDone(&foldcxt);
				return res;
			}
		}

		result = foldDone(&foldcxt);
	}

	JsonValueListAppend(fcxt->result, result);

	return jperOk;
}

static JsonPathExecResult
jspFold(JsonPathFuncContext *fcxt, FoldType ftype, const char *funcName)
{
	JsonItem   *item = fcxt->item;
	int			nargs = (ftype == FOLD_REDUCE ? 1 : 2) + (item ? 0 : 1);

	if (fcxt->nargs != nargs)
		return jspThrowWrongArgumentsError(fcxt->cxt, nargs, fcxt->nargs, funcName);

	return item ? jspFoldArray(fcxt, ftype, item) : jspFoldSeq(fcxt, ftype);
}

PG_FUNCTION_INFO_V1(jsonpath_reduce);
Datum
jsonpath_reduce(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspFold((void *) PG_GETARG_POINTER(0), FOLD_REDUCE, "reduce"));
}

PG_FUNCTION_INFO_V1(jsonpath_fold);
Datum
jsonpath_fold(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspFold((void *) PG_GETARG_POINTER(0), FOLD_LEFT, "fold"));
}

PG_FUNCTION_INFO_V1(jsonpath_foldl);
Datum
jsonpath_foldl(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspFold((void *) PG_GETARG_POINTER(0), FOLD_LEFT, "foldl"));
}

PG_FUNCTION_INFO_V1(jsonpath_foldr);
Datum
jsonpath_foldr(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspFold((void *) PG_GETARG_POINTER(0), FOLD_RIGHT, "foldr"));
}

static JsonPathExecResult
jspMinMax(JsonPathFuncContext *fcxt, bool max, const char *funcName)
{
	JsonItem   *item = fcxt->item;
	JsonItem   *result = NULL;
	JsonPathItemType cmpop = max ? jpiGreater : jpiLess;

	if (fcxt->nargs != (item ? 0 : 1))
		return jspThrowWrongArgumentsError(fcxt->cxt, item ? 0 : 1, fcxt->nargs,
										   funcName);

	if (!item)
	{
		JsonValueList items = {0};
		JsonValueListIterator iter;
		JsonItem   *item;
		JsonPathExecResult res;

		res = jspExecuteItem(fcxt->cxt, &fcxt->args[0], fcxt->jb, &items);
		if (jperIsError(res))
			return res;

		if (!JsonValueListLength(&items))
			return jperNotFound;

		JsonValueListInitIterator(&items, &iter);

		while ((item = JsonValueListNext(&items, &iter)))
		{
			if (result)
			{
				JsonPathBool cmp = jspCompareItems(cmpop, item, result);

				if (cmp == jpbUnknown)
					return jspThrowComparisonError(fcxt->cxt, funcName);

				if (cmp == jpbTrue)
					result = item;
			}
			else
			{
				result = item;
			}
		}
	}
	else if (JsonbType(item) != jbvArray)
	{
		if (!jspAutoWrap(fcxt->cxt))
			return jspThrowArrayNotFoundError(fcxt->cxt, funcName);

		result = item;
	}
	else
	{
		JsonbValue	elmebuf;
		JsonbValue *elem;
		JsonxIterator it;
		JsonbIteratorToken tok;
		int			size = JsonxArraySize(item, fcxt->cxt->isJsonb);
		int			i;
		bool		isBinary = JsonItemIsBinary(item);

		if (isBinary)
		{
			elem = &elmebuf;
			JsonxIteratorInit(&it, JsonItemBinary(item).data, fcxt->cxt->isJsonb);
			tok = JsonxIteratorNext(&it, &elmebuf, false);
			if (tok != WJB_BEGIN_ARRAY)
				elog(ERROR, "unexpected jsonb token at the array start");
		}

		for (i = 0; i < size; i++)
		{
			JsonItem	elemjsi;

			if (isBinary)
			{
				tok = JsonxIteratorNext(&it, elem, true);
				if (tok != WJB_ELEM)
					break;
			}
			else
				elem = &JsonItemArray(item).elems[i];

			if (!i)
			{
				result = palloc(sizeof(*result));
				JsonbValueToJsonItem(elem, result);
			}
			else
			{
				JsonPathBool cmp = jspCompareItems(cmpop,
												  JsonbValueToJsonItem(elem, &elemjsi),
												  result);

				if (cmp == jpbUnknown)
					return jspThrowComparisonError(fcxt->cxt, funcName);

				if (cmp == jpbTrue)
					*result = elemjsi;
			}
		}

		if (!result)
			return jperNotFound;
	}

	JsonValueListAppend(fcxt->result, result);
	return jperOk;
}

PG_FUNCTION_INFO_V1(jsonpath_min);
Datum
jsonpath_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspMinMax((void *) PG_GETARG_POINTER(0), false, "min"));
}

PG_FUNCTION_INFO_V1(jsonpath_max);
Datum
jsonpath_max(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(jspMinMax((void *) PG_GETARG_POINTER(0), true, "max"));
}
