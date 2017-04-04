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

static inline int32
appendJsonPathItemHeader(StringInfo buf, JsonPathItemType type, char flags)
{
	int32		nextPos = 0;	/* fake value */
	int32		nextOffs;

	appendStringInfoChar(buf, (char) type);
	appendStringInfoChar(buf, (char) flags);
	alignStringInfoInt(buf);

	nextOffs = buf->len;

	/*
	 * actual value will be recorded later, after next and
	 * children processing
	 */
	appendBinaryStringInfo(buf, (char *) &nextPos, sizeof(nextPos));

	return nextOffs;
}

static int32
copyJsonPathItem(StringInfo buf, JsonPathItem *item, int level,
				 int32 *pLastOffset, int32 *pNextOffset)
{
	int32		pos = buf->len - JSONPATH_HDRSZ;
	JsonPathItem next;
	int32		offs = 0;
	int32		argLevel = level;
	int32		nextOffs;

	check_stack_depth();

	nextOffs = appendJsonPathItemHeader(buf, item->type, item->flags);

	switch (item->type)
	{
		case jpiNull:
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
		case jpiMin:
		case jpiMax:
			break;

		case jpiRoot:
			if (level > 0)
			{
				/* replace $ with @N */
				int32		lev = level - 1;

				buf->data[pos + JSONPATH_HDRSZ] =
					lev > 0 ? jpiCurrentN : jpiCurrent;

				if (lev > 0)
					appendBinaryStringInfo(buf, (const char *) &lev, sizeof(lev));
			}
			break;

		case jpiCurrentN:
			appendBinaryStringInfo(buf, (char *) &item->content.current.level,
								   sizeof(item->content.current.level));
			break;

		case jpiKey:
		case jpiString:
		case jpiVariable:
			{
				int32		len;
				const char *data = jspGetString(item, &len);

				appendBinaryStringInfo(buf, (const char *) &len, sizeof(len));
				appendBinaryStringInfo(buf, data, len);
				appendStringInfoChar(buf, '\0');
				break;
			}

		case jpiNumeric:
			{
				Numeric		num = jspGetNumeric(item);

				appendBinaryStringInfo(buf, (char *) num, VARSIZE(num));
				break;
			}

		case jpiBool:
			appendStringInfoChar(buf, jspGetBool(item) ? 1 : 0);
			break;

		case jpiMap:
			if (level)
				argLevel++;
			/* fall through */
		case jpiFilter:
		case jpiReduce:
			if (level)
				argLevel++;
			/* fall through */
		case jpiNot:
		case jpiExists:
		case jpiIsUnknown:
		case jpiPlus:
		case jpiMinus:
		case jpiDatetime:
		case jpiArray:
			{
				JsonPathItem arg;
				int32		argoffs;
				int32		argpos;

				argoffs = buf->len;
				appendBinaryStringInfo(buf, (const char *) &offs, sizeof(offs));

				if (!item->content.arg)
					break;

				jspGetArg(item, &arg);
				argpos = copyJsonPathItem(buf, &arg, argLevel, NULL, NULL);
				*(int32 *) &buf->data[argoffs] = argpos - pos;
				break;
			}

		case jpiFold:
		case jpiFoldl:
		case jpiFoldr:
			if (level)
				argLevel++;
			/* fall through */
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
			{
				JsonPathItem larg;
				JsonPathItem rarg;
				int32		loffs;
				int32		roffs;
				int32		lpos;
				int32		rpos;

				loffs = buf->len;
				appendBinaryStringInfo(buf, (const char *) &offs, sizeof(offs));

				roffs = buf->len;
				appendBinaryStringInfo(buf, (const char *) &offs, sizeof(offs));

				jspGetLeftArg(item, &larg);
				lpos = copyJsonPathItem(buf, &larg, argLevel, NULL, NULL);
				*(int32 *) &buf->data[loffs] = lpos - pos;

				jspGetRightArg(item, &rarg);
				rpos = copyJsonPathItem(buf, &rarg, argLevel, NULL, NULL);
				*(int32 *) &buf->data[roffs] = rpos - pos;

				break;
			}

		case jpiIndexArray:
			{
				int32		nelems = item->content.array.nelems;
				int32		i;
				int			offset;

				if (level)
					argLevel++;

				appendBinaryStringInfo(buf, (char *) &nelems, sizeof(nelems));
				offset = buf->len;
				appendStringInfoSpaces(buf, sizeof(int32) * 2 * nelems);

				for (i = 0; i < nelems; i++, offset += 2 * sizeof(int32))
				{
					JsonPathItem from;
					JsonPathItem to;
					int32	   *ppos;
					int32		frompos;
					int32		topos;
					bool		range;

					range = jspGetArraySubscript(item, &from, &to, i);

					frompos = copyJsonPathItem(buf, &from, argLevel, NULL, NULL) - pos;

					if (range)
						topos = copyJsonPathItem(buf, &to, argLevel, NULL, NULL) - pos;
					else
						topos = 0;

					ppos = (int32 *) &buf->data[offset];
					ppos[0] = frompos;
					ppos[1] = topos;
				}
			}
			break;

		case jpiAny:
			appendBinaryStringInfo(buf, (char *) &item->content.anybounds.first,
								   sizeof(item->content.anybounds.first));
			appendBinaryStringInfo(buf, (char *) &item->content.anybounds.last,
								   sizeof(item->content.anybounds.last));
			break;

		case jpiSequence:
			{
				int32		nelems = item->content.sequence.nelems;
				int32		i;
				int			offset;

				appendBinaryStringInfo(buf, (char *) &nelems, sizeof(nelems));
				offset = buf->len;
				appendStringInfoSpaces(buf, sizeof(int32) * nelems);

				for (i = 0; i < nelems; i++, offset += sizeof(int32))
				{
					JsonPathItem el;
					int32		elpos;

					jspGetSequenceElement(item, i, &el);

					elpos = copyJsonPathItem(buf, &el, level, NULL, NULL);
					*(int32 *) &buf->data[offset] = elpos - pos;
				}
			}
			break;

		case jpiObject:
			{
				int32		nfields = item->content.object.nfields;
				int32		i;
				int			offset;

				appendBinaryStringInfo(buf, (char *) &nfields, sizeof(nfields));
				offset = buf->len;
				appendStringInfoSpaces(buf, sizeof(int32) * 2 * nfields);

				for (i = 0; i < nfields; i++, offset += 2 * sizeof(int32))
				{
					JsonPathItem key;
					JsonPathItem val;
					int32		keypos;
					int32		valpos;
					int32	   *ppos;

					jspGetObjectField(item, i, &key, &val);

					keypos = copyJsonPathItem(buf, &key, level, NULL, NULL);
					valpos = copyJsonPathItem(buf, &val, level, NULL, NULL);

					ppos = (int32 *) &buf->data[offset];
					ppos[0] = keypos - pos;
					ppos[1] = valpos - pos;
				}
			}
			break;

		default:
			elog(ERROR, "Unknown jsonpath item type: %d", item->type);
	}

	if (jspGetNext(item, &next))
	{
		int32		nextPos = copyJsonPathItem(buf, &next, level,
											   pLastOffset, pNextOffset);

		*(int32 *) &buf->data[nextOffs] = nextPos - pos;
	}
	else if (pLastOffset)
	{
		*pLastOffset = pos;
		*pNextOffset = nextOffs;
	}

	return pos;
}

static int32
copyJsonPath(StringInfo buf, JsonPath *jp, int level, int32 *last, int32 *next)
{
	JsonPathItem root;

	alignStringInfoInt(buf);

	jspInit(&root, jp);

	return copyJsonPathItem(buf, &root, level, last, next);
}

/*
 * Convert AST to flat jsonpath type representation
 */
static int
flattenJsonPathParseItem(StringInfo buf, JsonPathParseItem *item,
						 int nestingLevel, bool insideArraySubscript)
{
	/* position from begining of jsonpath data */
	int32	pos = buf->len - JSONPATH_HDRSZ;
	int32	chld, next, last;
	int			argNestingLevel = nestingLevel;

	check_stack_depth();

	if (item->type == jpiBinary)
		pos = copyJsonPath(buf, item->value.binary, nestingLevel, &last, &next);
	else
	{
		next = appendJsonPathItemHeader(buf, item->type, item->flags);
		last = pos;
	}

	switch(item->type)
	{
		case jpiBinary:
			break;
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
		case jpiFold:
		case jpiFoldl:
		case jpiFoldr:
			argNestingLevel++;
			/* fall through */
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

				chld = flattenJsonPathParseItem(buf, item->value.args.left,
												argNestingLevel,
												insideArraySubscript);
				*(int32*)(buf->data + left) = chld - pos;
				chld = flattenJsonPathParseItem(buf, item->value.args.right,
												argNestingLevel,
												insideArraySubscript);
				*(int32*)(buf->data + right) = chld - pos;
			}
			break;
		case jpiMap:
			argNestingLevel++;
			/* fall through */
		case jpiFilter:
		case jpiReduce:
			argNestingLevel++;
			/* fall through */
		case jpiIsUnknown:
		case jpiNot:
		case jpiPlus:
		case jpiMinus:
		case jpiExists:
		case jpiDatetime:
		case jpiArray:
			{
				int32 arg = item->value.arg ? buf->len : 0;

				appendBinaryStringInfo(buf, (char*)&arg /* fake value */, sizeof(arg));

				if (!item->value.arg)
					break;

				chld = flattenJsonPathParseItem(buf, item->value.arg,
												argNestingLevel,
												insideArraySubscript);
				*(int32*)(buf->data + arg) = chld - pos;
			}
			break;
		case jpiNull:
			break;
		case jpiRoot:
			break;
		case jpiAnyArray:
		case jpiAnyKey:
			break;
		case jpiCurrentN:
			if (item->value.current.level < 0 ||
				item->value.current.level >= nestingLevel)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid outer item reference in jsonpath @")));

			appendBinaryStringInfo(buf, (char *) &item->value.current.level,
								   sizeof(item->value.current.level));
			break;
		case jpiCurrent:
			if (nestingLevel <= 0)
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
												nestingLevel + 1, true) - pos;

					if (item->value.array.elems[i].to)
						topos = flattenJsonPathParseItem(buf,
												item->value.array.elems[i].to,
												nestingLevel + 1, true) - pos;
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
		case jpiMin:
		case jpiMax:
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
					int32		elempos =
						flattenJsonPathParseItem(buf, lfirst(lc),
												 nestingLevel,
												 insideArraySubscript);

					*(int32 *) &buf->data[offset] = elempos - pos;
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
												 nestingLevel,
												 insideArraySubscript);
					int32		valpos =
						flattenJsonPathParseItem(buf, field->value.args.right,
												 nestingLevel,
												 insideArraySubscript);
					int32	   *ppos = (int32 *) &buf->data[offset];

					ppos[0] = keypos - pos;
					ppos[1] = valpos - pos;

					offset += 2 * sizeof(int32);
				}
			}
			break;
		default:
			elog(ERROR, "Unknown jsonpath item type: %d", item->type);
	}

	if (item->next)
		*(int32*)(buf->data + next) =
			flattenJsonPathParseItem(buf, item->next, nestingLevel,
									 insideArraySubscript) - last;

	return  pos;
}

static JsonPath *
encodeJsonPath(JsonPathParseItem *item, bool lax, int32 sizeEstimation)
{
	JsonPath   *res;
	StringInfoData buf;

	if (!item)
		return NULL;

	initStringInfo(&buf);
	enlargeStringInfo(&buf, sizeEstimation);

	appendStringInfoSpaces(&buf, JSONPATH_HDRSZ);

	flattenJsonPathParseItem(&buf, item, 0, false);

	res = (JsonPath *) buf.data;
	SET_VARSIZE(res, buf.len);
	res->header = JSONPATH_VERSION;
	if (lax)
		res->header |= JSONPATH_LAX;

	return res;
}

Datum
jsonpath_in(PG_FUNCTION_ARGS)
{
	char				*in = PG_GETARG_CSTRING(0);
	int32				len = strlen(in);
	JsonPathParseResult	*jsonpath = parsejsonpath(in, len);
	JsonPath			*res;

	if (!jsonpath)
		PG_RETURN_NULL();

	res = encodeJsonPath(jsonpath->expr, jsonpath->lax, 4 * len);

	PG_RETURN_JSONPATH(res);
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
		case jpiCurrentN:
			Assert(!inKey);
			appendStringInfo(buf, "@%d", v->content.current.level);
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
			else if (v->content.anybounds.first == 0)
				appendStringInfo(buf, "**{,%u}", v->content.anybounds.last);
			else if (v->content.anybounds.last == PG_UINT32_MAX)
				appendStringInfo(buf, "**{%u,}", v->content.anybounds.first);
			else if (v->content.anybounds.first == v->content.anybounds.last)
				appendStringInfo(buf, "**{%u}", v->content.anybounds.first);
			else
				appendStringInfo(buf, "**{%u,%u}", v->content.anybounds.first,
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
			if (v->content.arg)
			{
				jspGetArg(v, &elem);
				printJsonPathItem(buf, &elem, false, false);
			}
			appendStringInfoChar(buf, ')');
			break;
		case jpiKeyValue:
			appendBinaryStringInfo(buf, ".keyvalue()", 11);
			break;
		case jpiMap:
			appendBinaryStringInfo(buf, ".map(", 5);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendStringInfoChar(buf, ')');
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
		case jpiReduce:
			appendBinaryStringInfo(buf, ".reduce(", 8);
			jspGetArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendStringInfoChar(buf, ')');
			break;
		case jpiFold:
		case jpiFoldl:
		case jpiFoldr:
			if (v->type == jpiFold)
				appendBinaryStringInfo(buf, ".fold(", 6);
			else if (v->type == jpiFoldl)
				appendBinaryStringInfo(buf, ".foldl(", 7);
			else
				appendBinaryStringInfo(buf, ".foldr(", 7);
			jspGetLeftArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendBinaryStringInfo(buf, ", ", 2);
			jspGetRightArg(v, &elem);
			printJsonPathItem(buf, &elem, false, false);
			appendStringInfoChar(buf, ')');
			break;
		case jpiMin:
			appendBinaryStringInfo(buf, ".min()", 6);
			break;
		case jpiMax:
			appendBinaryStringInfo(buf, ".max()", 6);
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
	JsonPath			*in = PG_GETARG_JSONPATH(0);
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
	v->base = base + pos;

	read_byte(v->type, base, pos);
	read_byte(v->flags, base, pos);
	pos = INTALIGN((uintptr_t)(base + pos)) - (uintptr_t) base;
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
		case jpiMin:
		case jpiMax:
			break;
		case jpiCurrentN:
			read_int32(v->content.current.level, base, pos);
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
		case jpiFold:
		case jpiFoldl:
		case jpiFoldr:
			read_int32(v->content.args.left, base, pos);
			read_int32(v->content.args.right, base, pos);
			break;
		case jpiNot:
		case jpiExists:
		case jpiIsUnknown:
		case jpiPlus:
		case jpiMinus:
		case jpiFilter:
		case jpiDatetime:
		case jpiMap:
		case jpiArray:
		case jpiReduce:
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
		v->type == jpiDatetime ||
		v->type == jpiMap ||
		v->type == jpiArray ||
		v->type == jpiReduce
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
			v->type == jpiCurrentN ||
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
			v->type == jpiMap ||
			v->type == jpiSequence ||
			v->type == jpiArray ||
			v->type == jpiObject ||
			v->type == jpiReduce ||
			v->type == jpiFold ||
			v->type == jpiFoldl ||
			v->type == jpiFoldr ||
			v->type == jpiMin ||
			v->type == jpiMax
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
		v->type == jpiStartsWith ||
		v->type == jpiFold ||
		v->type == jpiFoldl ||
		v->type == jpiFoldr
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
		v->type == jpiStartsWith ||
		v->type == jpiFold ||
		v->type == jpiFoldl ||
		v->type == jpiFoldr
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

static void
checkJsonPathArgsMismatch(JsonPath *jp1, JsonPath *jp2)
{
	if ((jp1->header & ~JSONPATH_LAX) != JSONPATH_VERSION ||
		jp1->header != jp2->header)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jsonpath headers does not match")));
}

static inline void
jspInitParseItem(JsonPathParseItem *item, JsonPathItemType type,
				 JsonPathParseItem *next)
{
	item->type = type;
	item->flags = 0;
	item->next = next;
}

static inline void
jspInitParseItemUnary(JsonPathParseItem *item, JsonPathItemType type,
					  JsonPathParseItem *next, JsonPathParseItem *arg)
{
	jspInitParseItem(item, type, next);
	item->value.arg = arg;
}

static inline void
jspInitParseItemBinary(JsonPathParseItem *item, JsonPathItemType type,
					   JsonPathParseItem *left, JsonPathParseItem *right,
					   JsonPathParseItem *next)
{
	jspInitParseItem(item, type, next);
	item->value.args.left = left;
	item->value.args.right = right;
}

static inline void
jspInitParseItemBin(JsonPathParseItem *item, JsonPath *path,
					JsonPathParseItem *next)
{
	jspInitParseItem(item, jpiBinary, next);
	item->value.binary = path;
}

static inline void
jspInitParseItemString(JsonPathParseItem *item, JsonPathItemType type,
					   char *str, uint32 len, JsonPathParseItem *next)
{
	jspInitParseItem(item, type, next);
	item->value.string.val = str;
	item->value.string.len = len;
}


static Datum
jsonpath_op_jsonpath(FunctionCallInfo fcinfo, JsonPathItemType op)
{
	JsonPath   *jp1 = PG_GETARG_JSONPATH(0);
	JsonPath   *jp2 = PG_GETARG_JSONPATH(1);
	JsonPathParseItem jpi1;
	JsonPathParseItem jpi2;
	JsonPathParseItem jpi;

	checkJsonPathArgsMismatch(jp1, jp2);

	jspInitParseItemBin(&jpi1, jp1, NULL);
	jspInitParseItemBin(&jpi2, jp2, NULL);
	jspInitParseItemBinary(&jpi, op, &jpi1, &jpi2, NULL);

	PG_RETURN_JSONPATH(encodeJsonPath(&jpi,
									  (jp1->header & JSONPATH_LAX) != 0,
									  VARSIZE(jp1) + VARSIZE(jp2) -
									  JSONPATH_HDRSZ + 16));
}

static Datum
jsonpath_op_jsonb(FunctionCallInfo fcinfo, JsonPathItemType op)
{
	JsonPath   *jp = PG_GETARG_JSONPATH(0);
	Jsonb	   *jb = PG_GETARG_JSONB(1);
	JsonbValue	jbv;
	JsonPathParseItem jpi1;
	JsonPathParseItem jpi2;
	JsonPathParseItem jpi;

	if (JB_ROOT_IS_SCALAR(jb))
	{
		JsonbExtractScalar(&jb->root, &jbv);

		/* jbv and jpi scalar types have the same values */
		jspInitParseItem(&jpi2, jbv.type, NULL);

		switch (jbv.type)
		{
			case jbvNull:
				break;

			case jbvBool:
				jpi2.value.boolean = jbv.val.boolean;
				break;

			case jbvString:
				jpi2.value.string.val = jbv.val.string.val;
				jpi2.value.string.len = jbv.val.string.len;
				break;

			case jbvNumeric:
				jpi2.value.numeric = jbv.val.numeric;
				break;

			default:
				elog(ERROR, "invalid scalar jsonb value type: %d", jbv.type);
				break;
		}

	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("jsonpath operations with non-scalar jsonb are not yet supported")));

	jspInitParseItemBin(&jpi1, jp, NULL);
	jspInitParseItemBinary(&jpi, op, &jpi1, &jpi2, NULL);

	PG_RETURN_JSONPATH(encodeJsonPath(&jpi,
									  (jp->header & JSONPATH_LAX) != 0,
									  VARSIZE(jp) + VARSIZE(jb)));
}

Datum
jsonpath_eq_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiEqual);
}

Datum
jsonpath_eq_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiEqual);
}

Datum
jsonpath_ne_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiNotEqual);
}

Datum
jsonpath_ne_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiNotEqual);
}

Datum
jsonpath_lt_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiLess);
}

Datum
jsonpath_lt_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiLess);
}

Datum
jsonpath_le_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiLessOrEqual);
}

Datum
jsonpath_le_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiLessOrEqual);
}

Datum
jsonpath_gt_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiGreater);
}

Datum
jsonpath_gt_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiGreater);
}

Datum
jsonpath_ge_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiGreaterOrEqual);
}

Datum
jsonpath_ge_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiGreaterOrEqual);
}

Datum
jsonpath_pl_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiAdd);
}

Datum
jsonpath_pl_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiAdd);
}

Datum
jsonpath_mi_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiSub);
}

Datum
jsonpath_mi_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiSub);
}

Datum
jsonpath_mul_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiMul);
}

Datum
jsonpath_mul_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiMul);
}

Datum
jsonpath_div_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiDiv);
}

Datum
jsonpath_div_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiDiv);
}

Datum
jsonpath_mod_jsonpath(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonpath(fcinfo, jpiMod);
}

Datum
jsonpath_mod_jsonb(PG_FUNCTION_ARGS)
{
	return jsonpath_op_jsonb(fcinfo, jpiMod);
}

Datum
jsonpath_object_field(PG_FUNCTION_ARGS)
{
	JsonPath   *jpObj = PG_GETARG_JSONPATH(0);
	text	   *jpFld = PG_GETARG_TEXT_PP(1);
	JsonPathParseItem jpiObj;
	JsonPathParseItem jpiFld;

	jspInitParseItemBin(&jpiObj, jpObj, &jpiFld);
	jspInitParseItemString(&jpiFld, jpiKey,
						   VARDATA_ANY(jpFld), VARSIZE_ANY_EXHDR(jpFld), NULL);

	PG_RETURN_JSONPATH(encodeJsonPath(&jpiObj,
									  (jpObj->header & JSONPATH_LAX) != 0,
									  INTALIGN(VARSIZE(jpObj)) + 8 +
									  jpiFld.value.string.len));
}

Datum
jsonpath_array_element(PG_FUNCTION_ARGS)
{
	JsonPath   *arr = PG_GETARG_JSONPATH(0);
	int32		idx = PG_GETARG_INT32(1);
	JsonPathParseItem jpiArr;
	JsonPathParseItem jpiArrIdx;
	JsonPathParseItem jpiIdx;
	struct JsonPathParseArraySubscript subscript;

	jspInitParseItemBin(&jpiArr, arr, &jpiArrIdx);

	jspInitParseItem(&jpiArrIdx, jpiIndexArray, NULL);
	jpiArrIdx.value.array.nelems = 1;
	jpiArrIdx.value.array.elems = &subscript;

	subscript.from = &jpiIdx;
	subscript.to = NULL;

	jspInitParseItem(&jpiIdx, jpiNumeric, NULL);
	jpiIdx.value.numeric = DatumGetNumeric(
			DirectFunctionCall1(int4_numeric, Int32GetDatum(idx)));

	PG_RETURN_JSONPATH(encodeJsonPath(&jpiArr,
									  (arr->header & JSONPATH_LAX) != 0,
									  INTALIGN(VARSIZE(arr)) + 28 +
									  VARSIZE(jpiIdx.value.numeric)));
}

Datum
jsonpath_filter(PG_FUNCTION_ARGS)
{
	JsonPath   *jpRoot = PG_GETARG_JSONPATH(0);
	JsonPath   *jpFilter = PG_GETARG_JSONPATH(1);
	JsonPathItem root;
	JsonPathParseItem jppiRoot;
	JsonPathParseItem jppiFilter;
	JsonPathParseItem jppiFilterArg;

	checkJsonPathArgsMismatch(jpRoot, jpFilter);

	jspInit(&root, jpFilter);

	if (!jspIsBooleanOp(root.type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jsonpath filter must be boolean expression")));

	jspInitParseItemBin(&jppiRoot, jpRoot, &jppiFilter);
	jspInitParseItemUnary(&jppiFilter, jpiFilter, NULL, &jppiFilterArg);
	jspInitParseItemBin(&jppiFilterArg, jpFilter, NULL);

	PG_RETURN_JSONPATH(encodeJsonPath(&jppiRoot,
									  (jpRoot->header & JSONPATH_LAX) != 0,
									  INTALIGN(VARSIZE(jpRoot)) + 12 +
									  VARSIZE(jpFilter)));
}

/* TODO jsonpath @ jsonb */
