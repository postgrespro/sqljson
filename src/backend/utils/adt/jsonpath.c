/*-------------------------------------------------------------------------
 *
 * jsonpath.c
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath.c
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

/*****************************INPUT/OUTPUT************************************/
static int
flattenJsonPathParseItem(StringInfo buf, JsonPathParseItem *item)
{
	int32	pos = buf->len - VARHDRSZ; /* position from begining of jsonpath data */
	int32	chld, next;

	check_stack_depth();

	appendStringInfoChar(buf, (char)(item->type));
	alignStringInfoInt(buf);

	next = (item->next) ? buf->len : 0;
	appendBinaryStringInfo(buf, (char*)&next /* fake value */, sizeof(next));

	switch(item->type)
	{
		case jpiKey:
		case jpiString:
		case jpiVariable:
			appendBinaryStringInfo(buf, (char*)&item->string.len, sizeof(item->string.len));
			appendBinaryStringInfo(buf, item->string.val, item->string.len);
			appendStringInfoChar(buf, '\0');
			break;
		case jpiNumeric:
			appendBinaryStringInfo(buf, (char*)item->numeric, VARSIZE(item->numeric));
			break;
		case jpiBool:
			appendBinaryStringInfo(buf, (char*)&item->boolean, sizeof(item->boolean));
			break;
		case jpiAnd:
		case jpiOr:
			{
				int32	left, right;

				left = buf->len;
				appendBinaryStringInfo(buf, (char*)&left /* fake value */, sizeof(left));
				right = buf->len;
				appendBinaryStringInfo(buf, (char*)&right /* fake value */, sizeof(right));

				chld = flattenJsonPathParseItem(buf, item->args.left);
				*(int32*)(buf->data + left) = chld;
				chld = flattenJsonPathParseItem(buf, item->args.right);
				*(int32*)(buf->data + right) = chld;
			}
			break;
		case jpiEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiNot:
		case jpiExpression:
			{
				int32 arg;

				arg = buf->len;
				appendBinaryStringInfo(buf, (char*)&arg /* fake value */, sizeof(arg));

				chld = flattenJsonPathParseItem(buf, item->arg);
				*(int32*)(buf->data + arg) = chld;
			}
			break;
		case jpiAnyArray:
		case jpiAnyKey:
		case jpiCurrent:
		case jpiRoot:
		case jpiNull:
			break;
		case jpiIndexArray:
			appendBinaryStringInfo(buf,
								   (char*)&item->array.nelems,
								   sizeof(item->array.nelems));
			appendBinaryStringInfo(buf,
								   (char*)item->array.elems,
								   item->array.nelems * sizeof(item->array.elems[0]));
			break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", item->type);
	}

	if (item->next)
		*(int32*)(buf->data + next) = flattenJsonPathParseItem(buf, item->next);

	return  pos;
}

Datum
jsonpath_in(PG_FUNCTION_ARGS)
{
	char				*in = PG_GETARG_CSTRING(0);
	int32				len = strlen(in);
	JsonPathParseItem	*jsonpath = parsejsonpath(in, len);
	JsonPath				*res;
	StringInfoData		buf;

	initStringInfo(&buf);
	enlargeStringInfo(&buf, 4 * len /* estimation */);

	appendStringInfoSpaces(&buf, VARHDRSZ);

	if (!jsonpath)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for jsonpath: \"%s\"", in)));

	flattenJsonPathParseItem(&buf, jsonpath);

	res = (JsonPath*)buf.data;
	SET_VARSIZE(res, buf.len);

	PG_RETURN_JSONPATH_P(res);
}

static void
printOperation(StringInfo buf, JsonPathItemType type)
{
	switch(type)
	{
		case jpiAnd:
			appendBinaryStringInfo(buf, " && ", 4); break;
		case jpiOr:
			appendBinaryStringInfo(buf, " || ", 4); break;
		case jpiEqual:
			appendBinaryStringInfo(buf, " = ", 3); break;
		case jpiLess:
			appendBinaryStringInfo(buf, " < ", 3); break;
		case jpiGreater:
			appendBinaryStringInfo(buf, " > ", 3); break;
		case jpiLessOrEqual:
			appendBinaryStringInfo(buf, " <= ", 4); break;
		case jpiGreaterOrEqual:
			appendBinaryStringInfo(buf, " >= ", 4); break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", type);
	}
}

static void
printJsonPathItem(StringInfo buf, JsonPathItem *v, bool inKey, bool printBracketes)
{
	JsonPathItem	elem;
	int				i;

	check_stack_depth();

	switch(v->type)
	{
		case jpiNull:
			appendStringInfoString(buf, "null");
			break;
		case jpiKey:
			if (inKey)
				appendStringInfoChar(buf, '.');
			escape_json(buf, jspGetString(v, NULL));
			break;
		case jpiString:
			escape_json(buf, jspGetString(v, NULL));
			break;
		case jpiVariable:
			appendStringInfoChar(buf, '$');
			escape_json(buf, jspGetString(v, NULL));
			break;
		case jpiNumeric:
			appendStringInfoString(buf,
									DatumGetCString(DirectFunctionCall1(numeric_out,
										PointerGetDatum(jspGetNumeric(v)))));
			break;
		case jpiBool:
			if (jspGetBool(v))
				appendBinaryStringInfo(buf, "true", 4);
			else
				appendBinaryStringInfo(buf, "false", 5);
			break;
		case jpiAnd:
		case jpiOr:
			appendStringInfoChar(buf, '(');
			jspGetLeftArg(v, &elem);
			printJsonPathItem(buf, &elem, false, true);
			printOperation(buf, v->type);
			jspGetRightArg(v, &elem);
			printJsonPathItem(buf, &elem, false, true);
			appendStringInfoChar(buf, ')');
			break;
		case jpiExpression:
			appendBinaryStringInfo(buf, "?(", 2);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendStringInfoChar(buf, ')');
			break;
		case jpiEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			printOperation(buf, v->type);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, true);
			break;
		case jpiNot:
			appendBinaryStringInfo(buf, "(! ", 2);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, true);
			appendStringInfoChar(buf, ')');
			break;
		case jpiCurrent:
			Assert(!inKey);
			appendStringInfoChar(buf, '@');
			break;
		case jpiRoot:
			Assert(!inKey);
			appendStringInfoChar(buf, '$');
			break;
		case jpiAnyArray:
			appendBinaryStringInfo(buf, "[*]", 3);
			break;
		case jpiAnyKey:
			if (inKey)
				appendStringInfoChar(buf, '.');
			appendStringInfoChar(buf, '*');
			break;
		case jpiIndexArray:
			appendStringInfoChar(buf, '[');
			for(i = 0; i< v->array.nelems; i++)
			{
				if (i)
					appendStringInfoChar(buf, ',');
				appendStringInfo(buf, "%d", v->array.elems[i]);
			}
			appendStringInfoChar(buf, ']');
			break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", v->type);
	}

	if (jspGetNext(v, &elem))
		printJsonPathItem(buf, &elem, true, true);
}

Datum
jsonpath_out(PG_FUNCTION_ARGS)
{
	JsonPath			*in = PG_GETARG_JSONPATH_P(0);
	StringInfoData	buf;
	JsonPathItem		v;

	initStringInfo(&buf);
	enlargeStringInfo(&buf, VARSIZE(in) /* estimation */);

	jspInit(&v, in);
	printJsonPathItem(&buf, &v, false, true);

	PG_RETURN_CSTRING(buf.data);
}

/********************Support functions for JsonPath****************************/

#define read_byte(v, b, p) do {			\
	(v) = *(uint8*)((b) + (p));			\
	(p) += 1;							\
} while(0)								\

#define read_int32(v, b, p) do {		\
	(v) = *(uint32*)((b) + (p));		\
	(p) += sizeof(int32);				\
} while(0)								\

#define read_int32_n(v, b, p, n) do {	\
	(v) = (int32*)((b) + (p));			\
	(p) += sizeof(int32) * (n);			\
} while(0)								\

void
jspInit(JsonPathItem *v, JsonPath *js)
{
	jspInitByBuffer(v, VARDATA(js), 0);
}

void
jspInitByBuffer(JsonPathItem *v, char *base, int32 pos)
{
	v->base = base;

	read_byte(v->type, base, pos);

	switch(INTALIGN(pos) - pos)
	{
		case 3: pos++;
		case 2: pos++;
		case 1: pos++;
		default: break;
	}

	read_int32(v->nextPos, base, pos);

	switch(v->type)
	{
		case jpiNull:
		case jpiRoot:
		case jpiCurrent:
		case jpiAnyArray:
		case jpiAnyKey:
			break;
		case jpiKey:
		case jpiString:
		case jpiVariable:
			read_int32(v->value.datalen, base, pos);
			/* follow next */
		case jpiNumeric:
		case jpiBool:
			v->value.data = base + pos;
			break;
		case jpiAnd:
		case jpiOr:
			read_int32(v->args.left, base, pos);
			read_int32(v->args.right, base, pos);
			break;
		case jpiEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiNot:
		case jpiExpression:
			read_int32(v->arg, base, pos);
			break;
		case jpiIndexArray:
			read_int32(v->array.nelems, base, pos);
			read_int32_n(v->array.elems, base, pos, v->array.nelems);
			break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", v->type);
	}
}

void
jspGetArg(JsonPathItem *v, JsonPathItem *a)
{
	Assert(
		v->type == jpiEqual ||
		v->type == jpiLess ||
		v->type == jpiGreater ||
		v->type == jpiLessOrEqual ||
		v->type == jpiGreaterOrEqual ||
		v->type == jpiExpression ||
		v->type == jpiNot
	);

	jspInitByBuffer(a, v->base, v->arg);
}

bool
jspGetNext(JsonPathItem *v, JsonPathItem *a)
{
	if (v->nextPos > 0)
	{
		Assert(
			v->type == jpiKey ||
			v->type == jpiAnyArray ||
			v->type == jpiAnyKey ||
			v->type == jpiIndexArray ||
			v->type == jpiCurrent ||
			v->type == jpiRoot
		);

		if (a)
			jspInitByBuffer(a, v->base, v->nextPos);
		return true;
	}

	return false;
}

void
jspGetLeftArg(JsonPathItem *v, JsonPathItem *a)
{
	Assert(
		v->type == jpiAnd ||
		v->type == jpiOr
	);

	jspInitByBuffer(a, v->base, v->args.left);
}

void
jspGetRightArg(JsonPathItem *v, JsonPathItem *a)
{
	Assert(
		v->type == jpiAnd ||
		v->type == jpiOr
	);

	jspInitByBuffer(a, v->base, v->args.right);
}

bool
jspGetBool(JsonPathItem *v)
{
	Assert(v->type == jpiBool);

	return (bool)*v->value.data;
}

Numeric
jspGetNumeric(JsonPathItem *v)
{
	Assert(v->type == jpiNumeric);

	return (Numeric)v->value.data;
}

char*
jspGetString(JsonPathItem *v, int32 *len)
{
	Assert(
		v->type == jpiKey ||
		v->type == jpiString ||
		v->type == jpiVariable
	);

	if (len)
		*len = v->value.datalen;
	return v->value.data;
}
