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

/*
 * Convert AST to flat jsonpath type representation
 */
static int
flattenJsonPathParseItem(StringInfo buf, JsonPathParseItem *item,
						 bool allowCurrent, bool insideArraySubscript)
{
	/* position from begining of jsonpath data */
	int32	pos = buf->len - JSONPATH_HDRSZ;
	int32	chld, next;

	check_stack_depth();

	appendStringInfoChar(buf, (char)(item->type));
	alignStringInfoInt(buf);

	next = (item->next) ? buf->len : 0;

	/*
	 * actual value will be recorded later, after next and
	 * children processing
	 */
	appendBinaryStringInfo(buf, (char*)&next /* fake value */, sizeof(next));

	switch(item->type)
	{
		case jpiString:
		case jpiVariable:
		case jpiKey:
			appendBinaryStringInfo(buf, (char*)&item->value.string.len,
								   sizeof(item->value.string.len));
			appendBinaryStringInfo(buf, item->value.string.val, item->value.string.len);
			appendStringInfoChar(buf, '\0');
			break;
		case jpiNumeric:
			appendBinaryStringInfo(buf, (char*)item->value.numeric,
								   VARSIZE(item->value.numeric));
			break;
		case jpiBool:
			appendBinaryStringInfo(buf, (char*)&item->value.boolean,
								   sizeof(item->value.boolean));
			break;
		case jpiAnd:
		case jpiOr:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiAdd:
		case jpiSub:
		case jpiMul:
		case jpiDiv:
		case jpiMod:
		case jpiStartsWith:
		case jpiDatetime:
			{
				int32	left, right;

				left = buf->len;

				/*
				 * first, reserve place for left/right arg's positions, then
				 * record both args and sets actual position in reserved places
				 */
				appendBinaryStringInfo(buf, (char*)&left /* fake value */, sizeof(left));
				right = buf->len;
				appendBinaryStringInfo(buf, (char*)&right /* fake value */, sizeof(right));

				chld = !item->value.args.left ? 0 :
					flattenJsonPathParseItem(buf, item->value.args.left,
											 allowCurrent,
											 insideArraySubscript);
				*(int32*)(buf->data + left) = chld;

				chld = !item->value.args.right ? 0 :
					flattenJsonPathParseItem(buf, item->value.args.right,
											 allowCurrent,
											 insideArraySubscript);
				*(int32*)(buf->data + right) = chld;
			}
			break;
		case jpiLikeRegex:
			{
				int32	offs;

				appendBinaryStringInfo(buf,
									   (char *) &item->value.like_regex.flags,
									   sizeof(item->value.like_regex.flags));
				offs = buf->len;
				appendBinaryStringInfo(buf, (char *) &offs /* fake value */, sizeof(offs));

				appendBinaryStringInfo(buf,
									(char *) &item->value.like_regex.patternlen,
									sizeof(item->value.like_regex.patternlen));
				appendBinaryStringInfo(buf, item->value.like_regex.pattern,
									   item->value.like_regex.patternlen);
				appendStringInfoChar(buf, '\0');

				chld = flattenJsonPathParseItem(buf, item->value.like_regex.expr,
												allowCurrent,
												insideArraySubscript);
				*(int32 *)(buf->data + offs) = chld;
			}
			break;
		case jpiFilter:
		case jpiIsUnknown:
		case jpiNot:
		case jpiPlus:
		case jpiMinus:
		case jpiExists:
		case jpiArray:
			{
				int32 arg = item->value.arg ? buf->len : 0;

				appendBinaryStringInfo(buf, (char*)&arg /* fake value */, sizeof(arg));

				if (!item->value.arg)
					break;

				chld = flattenJsonPathParseItem(buf, item->value.arg,
												item->type == jpiFilter ||
												allowCurrent,
												insideArraySubscript);
				*(int32*)(buf->data + arg) = chld;
			}
			break;
		case jpiNull:
			break;
		case jpiRoot:
			break;
		case jpiAnyArray:
		case jpiAnyKey:
			break;
		case jpiCurrent:
			if (!allowCurrent)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("@ is not allowed in root expressions")));
			break;
		case jpiLast:
			if (!insideArraySubscript)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("LAST is allowed only in array subscripts")));
			break;
		case jpiIndexArray:
			{
				int32		nelems = item->value.array.nelems;
				int			offset;
				int			i;

				appendBinaryStringInfo(buf, (char *) &nelems, sizeof(nelems));

				offset = buf->len;

				appendStringInfoSpaces(buf, sizeof(int32) * 2 * nelems);

				for (i = 0; i < nelems; i++)
				{
					int32	   *ppos;
					int32		topos;
					int32		frompos =
						flattenJsonPathParseItem(buf,
												item->value.array.elems[i].from,
												true, true);

					if (item->value.array.elems[i].to)
						topos = flattenJsonPathParseItem(buf,
												item->value.array.elems[i].to,
												true, true);
					else
						topos = 0;

					ppos = (int32 *) &buf->data[offset + i * 2 * sizeof(int32)];

					ppos[0] = frompos;
					ppos[1] = topos;
				}
			}
			break;
		case jpiAny:
			appendBinaryStringInfo(buf,
								   (char*)&item->value.anybounds.first,
								   sizeof(item->value.anybounds.first));
			appendBinaryStringInfo(buf,
								   (char*)&item->value.anybounds.last,
								   sizeof(item->value.anybounds.last));
			break;
		case jpiType:
		case jpiSize:
		case jpiAbs:
		case jpiFloor:
		case jpiCeiling:
		case jpiDouble:
		case jpiKeyValue:
			break;
		case jpiSequence:
			{
				int32		nelems = list_length(item->value.sequence.elems);
				ListCell   *lc;
				int			offset;

				appendBinaryStringInfo(buf, (char *) &nelems, sizeof(nelems));

				offset = buf->len;

				appendStringInfoSpaces(buf, sizeof(int32) * nelems);

				foreach(lc, item->value.sequence.elems)
				{
					int32		pos =
						flattenJsonPathParseItem(buf, lfirst(lc),
												 allowCurrent, insideArraySubscript);

					*(int32 *) &buf->data[offset] = pos;
					offset += sizeof(int32);
				}
			}
			break;
		case jpiObject:
			{
				int32		nfields = list_length(item->value.object.fields);
				ListCell   *lc;
				int			offset;

				appendBinaryStringInfo(buf, (char *) &nfields, sizeof(nfields));

				offset = buf->len;

				appendStringInfoSpaces(buf, sizeof(int32) * 2 * nfields);

				foreach(lc, item->value.object.fields)
				{
					JsonPathParseItem *field = lfirst(lc);
					int32		keypos =
						flattenJsonPathParseItem(buf, field->value.args.left,
												 allowCurrent,
												 insideArraySubscript);
					int32		valpos =
						flattenJsonPathParseItem(buf, field->value.args.right,
												 allowCurrent,
												 insideArraySubscript);
					int32	   *ppos = (int32 *) &buf->data[offset];

					ppos[0] = keypos;
					ppos[1] = valpos;

					offset += 2 * sizeof(int32);
				}
			}
			break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", item->type);
	}

	if (item->next)
		*(int32*)(buf->data + next) =
			flattenJsonPathParseItem(buf, item->next, allowCurrent,
									 insideArraySubscript);

	return  pos;
}

Datum
jsonpath_in(PG_FUNCTION_ARGS)
{
	char				*in = PG_GETARG_CSTRING(0);
	int32				len = strlen(in);
	JsonPathParseResult	*jsonpath = parsejsonpath(in, len);
	JsonPath			*res;
	StringInfoData		buf;

	initStringInfo(&buf);
	enlargeStringInfo(&buf, 4 * len /* estimation */);

	appendStringInfoSpaces(&buf, JSONPATH_HDRSZ);

	if (!jsonpath)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for jsonpath: \"%s\"", in)));

	flattenJsonPathParseItem(&buf, jsonpath->expr, false, false);

	res = (JsonPath*)buf.data;
	SET_VARSIZE(res, buf.len);
	res->header = JSONPATH_VERSION;
	if (jsonpath->lax)
		res->header |= JSONPATH_LAX;

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
			appendBinaryStringInfo(buf, " == ", 4); break;
		case jpiNotEqual:
			appendBinaryStringInfo(buf, " != ", 4); break;
		case jpiLess:
			appendBinaryStringInfo(buf, " < ", 3); break;
		case jpiGreater:
			appendBinaryStringInfo(buf, " > ", 3); break;
		case jpiLessOrEqual:
			appendBinaryStringInfo(buf, " <= ", 4); break;
		case jpiGreaterOrEqual:
			appendBinaryStringInfo(buf, " >= ", 4); break;
		case jpiAdd:
			appendBinaryStringInfo(buf, " + ", 3); break;
		case jpiSub:
			appendBinaryStringInfo(buf, " - ", 3); break;
		case jpiMul:
			appendBinaryStringInfo(buf, " * ", 3); break;
		case jpiDiv:
			appendBinaryStringInfo(buf, " / ", 3); break;
		case jpiMod:
			appendBinaryStringInfo(buf, " % ", 3); break;
		case jpiStartsWith:
			appendBinaryStringInfo(buf, " starts with ", 13); break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", type);
	}
}

static int
operationPriority(JsonPathItemType op)
{
	switch (op)
	{
		case jpiSequence:
			return -1;
		case jpiOr:
			return 0;
		case jpiAnd:
			return 1;
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiStartsWith:
			return 2;
		case jpiAdd:
		case jpiSub:
			return 3;
		case jpiMul:
		case jpiDiv:
		case jpiMod:
			return 4;
		case jpiPlus:
		case jpiMinus:
			return 5;
		default:
			return 6;
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
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiAdd:
		case jpiSub:
		case jpiMul:
		case jpiDiv:
		case jpiMod:
		case jpiStartsWith:
			if (printBracketes)
				appendStringInfoChar(buf, '(');
			jspGetLeftArg(v, &elem);
			printJsonPathItem(buf, &elem, false,
							  operationPriority(elem.type) <=
							  operationPriority(v->type));
			printOperation(buf, v->type);
			jspGetRightArg(v, &elem);
			printJsonPathItem(buf, &elem, false,
							  operationPriority(elem.type) <=
							  operationPriority(v->type));
			if (printBracketes)
				appendStringInfoChar(buf, ')');
			break;
		case jpiLikeRegex:
			if (printBracketes)
				appendStringInfoChar(buf, '(');

			jspInitByBuffer(&elem, v->base, v->content.like_regex.expr);
			printJsonPathItem(buf, &elem, false,
							  operationPriority(elem.type) <=
							  operationPriority(v->type));

			appendBinaryStringInfo(buf, " like_regex ", 12);

			escape_json(buf, v->content.like_regex.pattern);

			if (v->content.like_regex.flags)
			{
				appendBinaryStringInfo(buf, " flag \"", 7);

				if (v->content.like_regex.flags & JSP_REGEX_ICASE)
					appendStringInfoChar(buf, 'i');
				if (v->content.like_regex.flags & JSP_REGEX_SLINE)
					appendStringInfoChar(buf, 's');
				if (v->content.like_regex.flags & JSP_REGEX_MLINE)
					appendStringInfoChar(buf, 'm');
				if (v->content.like_regex.flags & JSP_REGEX_WSPACE)
					appendStringInfoChar(buf, 'x');

				appendStringInfoChar(buf, '"');
			}

			if (printBracketes)
				appendStringInfoChar(buf, ')');
			break;
		case jpiPlus:
		case jpiMinus:
			if (printBracketes)
				appendStringInfoChar(buf, '(');
			appendStringInfoChar(buf, v->type == jpiPlus ? '+' : '-');
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false,
							  operationPriority(elem.type) <=
							  operationPriority(v->type));
			if (printBracketes)
				appendStringInfoChar(buf, ')');
			break;
		case jpiFilter:
			appendBinaryStringInfo(buf, "?(", 2);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendStringInfoChar(buf, ')');
			break;
		case jpiNot:
			appendBinaryStringInfo(buf, "!(", 2);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendStringInfoChar(buf, ')');
			break;
		case jpiIsUnknown:
			appendStringInfoChar(buf, '(');
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendBinaryStringInfo(buf, ") is unknown", 12);
			break;
		case jpiExists:
			appendBinaryStringInfo(buf,"exists (", 8);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
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
		case jpiLast:
			appendBinaryStringInfo(buf, "last", 4);
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
			for (i = 0; i < v->content.array.nelems; i++)
			{
				JsonPathItem from;
				JsonPathItem to;
				bool		range = jspGetArraySubscript(v, &from, &to, i);

				if (i)
					appendStringInfoChar(buf, ',');

				printJsonPathItem(buf, &from, false, from.type == jpiSequence);

				if (range)
				{
					appendBinaryStringInfo(buf, " to ", 4);
					printJsonPathItem(buf, &to, false, to.type == jpiSequence);
				}
			}
			appendStringInfoChar(buf, ']');
			break;
		case jpiAny:
			if (inKey)
				appendStringInfoChar(buf, '.');

			if (v->content.anybounds.first == 0 &&
				v->content.anybounds.last == PG_UINT32_MAX)
				appendBinaryStringInfo(buf, "**", 2);
			else if (v->content.anybounds.first == v->content.anybounds.last)
			{
				if (v->content.anybounds.first == PG_UINT32_MAX)
					appendStringInfo(buf, "**{last}");
				else
					appendStringInfo(buf, "**{%u}", v->content.anybounds.first);
			}
			else if (v->content.anybounds.first == PG_UINT32_MAX)
				appendStringInfo(buf, "**{last to %u}", v->content.anybounds.last);
			else if (v->content.anybounds.last == PG_UINT32_MAX)
				appendStringInfo(buf, "**{%u to last}", v->content.anybounds.first);
			else
				appendStringInfo(buf, "**{%u to %u}", v->content.anybounds.first,
												   v->content.anybounds.last);
			break;
		case jpiType:
			appendBinaryStringInfo(buf, ".type()", 7);
			break;
		case jpiSize:
			appendBinaryStringInfo(buf, ".size()", 7);
			break;
		case jpiAbs:
			appendBinaryStringInfo(buf, ".abs()", 6);
			break;
		case jpiFloor:
			appendBinaryStringInfo(buf, ".floor()", 8);
			break;
		case jpiCeiling:
			appendBinaryStringInfo(buf, ".ceiling()", 10);
			break;
		case jpiDouble:
			appendBinaryStringInfo(buf, ".double()", 9);
			break;
		case jpiDatetime:
			appendBinaryStringInfo(buf, ".datetime(", 10);
			if (v->content.args.left)
			{
				jspGetLeftArg(v, &elem);
				printJsonPathItem(buf, &elem, false, false);

				if (v->content.args.right)
				{
					appendBinaryStringInfo(buf, ", ", 2);
					jspGetRightArg(v, &elem);
					printJsonPathItem(buf, &elem, false, false);
				}
			}
			appendStringInfoChar(buf, ')');
			break;
		case jpiKeyValue:
			appendBinaryStringInfo(buf, ".keyvalue()", 11);
			break;
		case jpiSequence:
			if (printBracketes || jspHasNext(v))
				appendStringInfoChar(buf, '(');

			for (i = 0; i < v->content.sequence.nelems; i++)
			{
				JsonPathItem elem;

				if (i)
					appendBinaryStringInfo(buf, ", ", 2);

				jspGetSequenceElement(v, i, &elem);

				printJsonPathItem(buf, &elem, false, elem.type == jpiSequence);
			}

			if (printBracketes || jspHasNext(v))
				appendStringInfoChar(buf, ')');
			break;
		case jpiArray:
			appendStringInfoChar(buf, '[');
			if (v->content.arg)
			{
				jspGetArg(v, &elem);
				printJsonPathItem(buf, &elem, false, false);
			}
			appendStringInfoChar(buf, ']');
			break;
		case jpiObject:
			appendStringInfoChar(buf, '{');

			for (i = 0; i < v->content.object.nfields; i++)
			{
				JsonPathItem key;
				JsonPathItem val;

				jspGetObjectField(v, i, &key, &val);

				if (i)
					appendBinaryStringInfo(buf, ", ", 2);

				printJsonPathItem(buf, &key, false, false);
				appendBinaryStringInfo(buf, ": ", 2);
				printJsonPathItem(buf, &val, false, val.type == jpiSequence);
			}

			appendStringInfoChar(buf, '}');
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

	if (!(in->header & JSONPATH_LAX))
		appendBinaryStringInfo(&buf, "strict ", 7);

	jspInit(&v, in);
	printJsonPathItem(&buf, &v, false, v.type != jpiSequence);

	PG_RETURN_CSTRING(buf.data);
}

/********************Support functions for JsonPath****************************/

/*
 * Support macroses to read stored values
 */

#define read_byte(v, b, p) do {			\
	(v) = *(uint8*)((b) + (p));			\
	(p) += 1;							\
} while(0)								\

#define read_int32(v, b, p) do {		\
	(v) = *(uint32*)((b) + (p));		\
	(p) += sizeof(int32);				\
} while(0)								\

#define read_int32_n(v, b, p, n) do {	\
	(v) = (void *)((b) + (p));			\
	(p) += sizeof(int32) * (n);			\
} while(0)								\

/*
 * Read root node and fill root node representation
 */
void
jspInit(JsonPathItem *v, JsonPath *js)
{
	Assert((js->header & ~JSONPATH_LAX) == JSONPATH_VERSION);
	jspInitByBuffer(v, js->data, 0);
}

/*
 * Read node from buffer and fill its representation
 */
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
		case jpiType:
		case jpiSize:
		case jpiAbs:
		case jpiFloor:
		case jpiCeiling:
		case jpiDouble:
		case jpiKeyValue:
		case jpiLast:
			break;
		case jpiKey:
		case jpiString:
		case jpiVariable:
			read_int32(v->content.value.datalen, base, pos);
			/* follow next */
		case jpiNumeric:
		case jpiBool:
			v->content.value.data = base + pos;
			break;
		case jpiAnd:
		case jpiOr:
		case jpiAdd:
		case jpiSub:
		case jpiMul:
		case jpiDiv:
		case jpiMod:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiStartsWith:
		case jpiDatetime:
			read_int32(v->content.args.left, base, pos);
			read_int32(v->content.args.right, base, pos);
			break;
		case jpiLikeRegex:
			read_int32(v->content.like_regex.flags, base, pos);
			read_int32(v->content.like_regex.expr, base, pos);
			read_int32(v->content.like_regex.patternlen, base, pos);
			v->content.like_regex.pattern = base + pos;
			break;
		case jpiNot:
		case jpiExists:
		case jpiIsUnknown:
		case jpiPlus:
		case jpiMinus:
		case jpiFilter:
		case jpiArray:
			read_int32(v->content.arg, base, pos);
			break;
		case jpiIndexArray:
			read_int32(v->content.array.nelems, base, pos);
			read_int32_n(v->content.array.elems, base, pos,
						 v->content.array.nelems * 2);
			break;
		case jpiAny:
			read_int32(v->content.anybounds.first, base, pos);
			read_int32(v->content.anybounds.last, base, pos);
			break;
		case jpiSequence:
			read_int32(v->content.sequence.nelems, base, pos);
			read_int32_n(v->content.sequence.elems, base, pos,
						 v->content.sequence.nelems);
			break;
		case jpiObject:
			read_int32(v->content.object.nfields, base, pos);
			read_int32_n(v->content.object.fields, base, pos,
						 v->content.object.nfields * 2);
			break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", v->type);
	}
}

void
jspGetArg(JsonPathItem *v, JsonPathItem *a)
{
	Assert(
		v->type == jpiFilter ||
		v->type == jpiNot ||
		v->type == jpiIsUnknown ||
		v->type == jpiExists ||
		v->type == jpiPlus ||
		v->type == jpiMinus ||
		v->type == jpiArray
	);

	jspInitByBuffer(a, v->base, v->content.arg);
}

bool
jspGetNext(JsonPathItem *v, JsonPathItem *a)
{
	if (jspHasNext(v))
	{
		Assert(
			v->type == jpiString ||
			v->type == jpiNumeric ||
			v->type == jpiBool ||
			v->type == jpiNull ||
			v->type == jpiKey ||
			v->type == jpiAny ||
			v->type == jpiAnyArray ||
			v->type == jpiAnyKey ||
			v->type == jpiIndexArray ||
			v->type == jpiFilter ||
			v->type == jpiCurrent ||
			v->type == jpiExists ||
			v->type == jpiRoot ||
			v->type == jpiVariable ||
			v->type == jpiLast ||
			v->type == jpiAdd ||
			v->type == jpiSub ||
			v->type == jpiMul ||
			v->type == jpiDiv ||
			v->type == jpiMod ||
			v->type == jpiPlus ||
			v->type == jpiMinus ||
			v->type == jpiEqual ||
			v->type == jpiNotEqual ||
			v->type == jpiGreater ||
			v->type == jpiGreaterOrEqual ||
			v->type == jpiLess ||
			v->type == jpiLessOrEqual ||
			v->type == jpiAnd ||
			v->type == jpiOr ||
			v->type == jpiNot ||
			v->type == jpiIsUnknown ||
			v->type == jpiType ||
			v->type == jpiSize ||
			v->type == jpiAbs ||
			v->type == jpiFloor ||
			v->type == jpiCeiling ||
			v->type == jpiDouble ||
			v->type == jpiDatetime ||
			v->type == jpiKeyValue ||
			v->type == jpiStartsWith ||
			v->type == jpiSequence ||
			v->type == jpiArray ||
			v->type == jpiObject
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
		v->type == jpiOr ||
		v->type == jpiEqual ||
		v->type == jpiNotEqual ||
		v->type == jpiLess ||
		v->type == jpiGreater ||
		v->type == jpiLessOrEqual ||
		v->type == jpiGreaterOrEqual ||
		v->type == jpiAdd ||
		v->type == jpiSub ||
		v->type == jpiMul ||
		v->type == jpiDiv ||
		v->type == jpiMod ||
		v->type == jpiDatetime ||
		v->type == jpiStartsWith
	);

	jspInitByBuffer(a, v->base, v->content.args.left);
}

void
jspGetRightArg(JsonPathItem *v, JsonPathItem *a)
{
	Assert(
		v->type == jpiAnd ||
		v->type == jpiOr ||
		v->type == jpiEqual ||
		v->type == jpiNotEqual ||
		v->type == jpiLess ||
		v->type == jpiGreater ||
		v->type == jpiLessOrEqual ||
		v->type == jpiGreaterOrEqual ||
		v->type == jpiAdd ||
		v->type == jpiSub ||
		v->type == jpiMul ||
		v->type == jpiDiv ||
		v->type == jpiMod ||
		v->type == jpiDatetime ||
		v->type == jpiStartsWith
	);

	jspInitByBuffer(a, v->base, v->content.args.right);
}

bool
jspGetBool(JsonPathItem *v)
{
	Assert(v->type == jpiBool);

	return (bool)*v->content.value.data;
}

Numeric
jspGetNumeric(JsonPathItem *v)
{
	Assert(v->type == jpiNumeric);

	return (Numeric)v->content.value.data;
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
		*len = v->content.value.datalen;
	return v->content.value.data;
}

bool
jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from, JsonPathItem *to,
					 int i)
{
	Assert(v->type == jpiIndexArray);

	jspInitByBuffer(from, v->base, v->content.array.elems[i].from);

	if (!v->content.array.elems[i].to)
		return false;

	jspInitByBuffer(to, v->base, v->content.array.elems[i].to);

	return true;
}

void
jspGetSequenceElement(JsonPathItem *v, int i, JsonPathItem *elem)
{
	Assert(v->type == jpiSequence);

	jspInitByBuffer(elem, v->base, v->content.sequence.elems[i]);
}

void
jspGetObjectField(JsonPathItem *v, int i, JsonPathItem *key, JsonPathItem *val)
{
	Assert(v->type == jpiObject);
	jspInitByBuffer(key, v->base, v->content.object.fields[i].key);
	jspInitByBuffer(val, v->base, v->content.object.fields[i].val);
}
