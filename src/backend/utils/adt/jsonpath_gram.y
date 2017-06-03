/*-------------------------------------------------------------------------
 *
 * jsonpath_gram.y
 *	 Grammar definitions for jsonpath datatype
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_gram.y
 *
 *-------------------------------------------------------------------------
 */

%{
#include "postgres.h"

#include "fmgr.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/jsonpath.h"

#include "utils/jsonpath_scanner.h"

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

static JsonPathParseItem*
makeItemType(int type)
{
	JsonPathParseItem* v = palloc(sizeof(*v));

	v->type = type;
	v->flags = 0;
	v->datatype = JPI_JSON;
	v->next = NULL;

	return v;
}


static JsonPathParseItem*
makeItemCurrentN(int level)
{
	JsonPathParseItem *v;

	if (!level)
		return makeItemType(jpiCurrent);

	v = makeItemType(jpiCurrentN);
	v->value.current.level = level;

	return v;
}

static JsonPathParseItem*
makeItemString(string *s)
{
	JsonPathParseItem *v;

	if (s == NULL)
	{
		v = makeItemType(jpiNull);
	}
	else
	{
		v = makeItemType(jpiString);
		v->value.string.val = s->val;
		v->value.string.len = s->len;
	}

	return v;
}

static JsonPathParseItem*
makeItemVariable(string *s)
{
	JsonPathParseItem *v;

	v = makeItemType(jpiVariable);
	v->value.string.val = s->val;
	v->value.string.len = s->len;

	return v;
}

static JsonPathParseItem*
makeItemKey(string *s)
{
	JsonPathParseItem *v;

	v = makeItemString(s);
	v->type = jpiKey;

	return v;
}

static JsonPathParseItem*
makeItemNumeric(string *s)
{
	JsonPathParseItem		*v;

	v = makeItemType(jpiNumeric);
	v->value.numeric =
		DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(s->val), 0, -1));

	return v;
}

static JsonPathParseItem*
makeItemBool(bool val) {
	JsonPathParseItem *v = makeItemType(jpiBool);

	v->value.boolean = val;
	v->datatype = JPI_BOOL;

	return v;
}

static JsonPathParseItem*
makeItemBinary(int type, JsonPathParseItem* la, JsonPathParseItem *ra)
{
	JsonPathParseItem  *v = makeItemType(type);

	if (la)
		v->flags |= la->flags & JSPI_EXT_EXEC;
	if (ra)
		v->flags |= ra->flags & JSPI_EXT_EXEC;
	v->value.args.left = la;
	v->value.args.right = ra;

	return v;
}

static JsonPathParseItem *
makeItemOperator(string *opname, JsonPathParseItem *la, JsonPathParseItem *ra)
{
	JsonPathParseItem *v = makeItemType(jpiOperator);

	v->flags |= JSPI_EXT_EXEC;
	v->datatype = JPI_UNKNOWN;
	v->value.op.name = opname->val;
	v->value.op.namelen = opname->len;
	v->value.op.left = la;
	v->value.op.right = ra;

	return v;
}

static JsonPathParseItem *
makeItemCast(JsonPathParseItem *arg, struct JsonPathTypeName *type)
{
	JsonPathParseItem *v = makeItemType(jpiCast);

	v->flags |= JSPI_EXT_EXEC;
	v->datatype = JPI_UNKNOWN;
	v->value.cast.type_name = type->name;
	v->value.cast.type_is_array = type->isarray;
	v->value.cast.type_mods = type->typmods;
	v->value.cast.arg = arg;

	return v;
}

static JsonPathParseItem*
makeItemUnary(int type, JsonPathParseItem* a)
{
	JsonPathParseItem  *v;

	if (type == jpiPlus && a->type == jpiNumeric && !a->next)
		return a;

	if (type == jpiMinus && a->type == jpiNumeric && !a->next)
	{
		v = makeItemType(jpiNumeric);
		v->value.numeric =
			DatumGetNumeric(DirectFunctionCall1(numeric_uminus,
												NumericGetDatum(a->value.numeric)));
		return v;
	}

	v = makeItemType(type);

	if (a)
		v->flags |= a->flags & JSPI_EXT_EXEC;
	v->value.arg = a;

	return v;
}

static JsonPathParseItem *
makeItemBinaryExpr(int type, JsonPathParseItem* la, JsonPathParseItem *ra)
{
	JsonPathParseItem  *v = makeItemBinary(type, la, ra);

	if (type == jpiStartsWith)
		v->datatype = JPI_BOOL;
	else if ((la->datatype == JPI_UNKNOWN && !la->next) ||
			 (ra->datatype == JPI_UNKNOWN && !ra->next))
	{
		string opname;

		switch (type)
		{
			case jpiAdd:
				opname.val = "+";
				break;
			case jpiSub:
				opname.val = "-";
				break;
			case jpiMul:
				opname.val = "*";
				break;
			case jpiDiv:
				opname.val = "/";
				break;
			case jpiMod:
				opname.val = "%";
				break;
			case jpiEqual:
				opname.val = "="; /* FIXME == */
				break;
			case jpiNotEqual:
				opname.val = "<>"; /* FIXME != */
				break;
			case jpiLess:
				opname.val = "<";
				break;
			case jpiLessOrEqual:
				opname.val = "<=";
				break;
			case jpiGreater:
				opname.val = ">";
				break;
			case jpiGreaterOrEqual:
				opname.val = ">=";
				break;
			case jpiAnd:
				opname.val = "&&";
				break;
			case jpiOr:
				opname.val = "||";
				break;
			default:
				opname.val = NULL;
				break;
		}

		if (opname.val)
		{
			opname.len = strlen(opname.val);
			v = makeItemOperator(&opname, la, ra);
		}
		else
			v->datatype = JPI_UNKNOWN;
	}
	else
	{
		switch (type)
		{
			case jpiAnd:
			case jpiOr:
				if (la->datatype != JPI_BOOL || la->next ||
					ra->datatype != JPI_BOOL || ra->next)
					yyerror(NULL, "expected boolean expression");
				/* fall through */
			case jpiEqual:
			case jpiNotEqual:
			case jpiLess:
			case jpiLessOrEqual:
			case jpiGreater:
			case jpiGreaterOrEqual:
				v->datatype = JPI_BOOL;
				break;

			default:
				v->datatype = JPI_JSON;
				break;
		}
	}

	return v;
}

static JsonPathParseItem *
makeItemUnaryExpr(int type, JsonPathParseItem *a)
{
	JsonPathParseItem  *v = makeItemUnary(type, a);

	if (type == jpiIsUnknown)
	{
		if ((a->datatype != JPI_UNKNOWN && a->datatype != JPI_BOOL) || a->next)
			yyerror(NULL, "expected boolean expression in IS UNKNOWN argument");

		v->datatype = JPI_BOOL;
	}
	else if (type == jpiExists)
		v->datatype = JPI_BOOL;
	else if (a->datatype == JPI_UNKNOWN && !a->next)
		v->datatype = JPI_UNKNOWN;
	else if (type == jpiNot)
	{
		if (a->datatype != JPI_BOOL || a->next)
			yyerror(NULL, "expected boolean expression in NOT argument");

		v->datatype = JPI_BOOL;
	}
	else
		v->datatype = JPI_JSON;

	return v;
}

static JsonPathParseItem*
makeItemList(List *list)
{
	JsonPathParseItem *head, *end, *lastext = NULL;
	ListCell   *cell = list_head(list);

	head = end = (JsonPathParseItem *) lfirst(cell);

	if (!lnext(cell))
		return head;

	/* find last item with external execution */
	for_each_cell(cell, lnext(cell))
	{
		JsonPathParseItem *c = (JsonPathParseItem *) lfirst(cell);

		if (c->flags & JSPI_EXT_EXEC)
			lastext = c;
	}

	/* append items to the end of already existing list */
	while (end->next)
	{
		if (lastext)
			end->flags |= JSPI_EXT_EXEC;		/* propagate flag  */
		end = end->next;
	}

	cell = list_head(list);

	for_each_cell(cell, lnext(cell))
	{
		JsonPathParseItem *c = (JsonPathParseItem *) lfirst(cell);

		if (lastext)
		{
			end->flags |= JSPI_EXT_EXEC;		/* propagate flag  */
			if (lastext == c)
				lastext = NULL;		/* stop flag propagation */
		}

		end->next = c;
		end = c;
	}

	return head;
}

static JsonPathParseItem*
makeIndexArray(List *list)
{
	JsonPathParseItem	*v = makeItemType(jpiIndexArray);
	ListCell			*cell;
	int					i = 0;

	Assert(list_length(list) > 0);
	v->value.array.nelems = list_length(list);

	v->value.array.elems = palloc(sizeof(v->value.array.elems[0]) * v->value.array.nelems);

	foreach(cell, list)
	{
		JsonPathParseItem *jpi = lfirst(cell);

		Assert(jpi->type == jpiSubscript);

		v->value.array.elems[i].from = jpi->value.args.left;
		v->value.array.elems[i++].to = jpi->value.args.right;

		v->flags |= jpi->value.args.left->flags & JSPI_EXT_EXEC;

		if (jpi->value.args.right)
			v->flags |= jpi->value.args.right->flags & JSPI_EXT_EXEC;
	}

	return v;
}

static JsonPathParseItem*
makeAny(int first, int last)
{
	JsonPathParseItem *v = makeItemType(jpiAny);

	v->value.anybounds.first = (first > 0) ? first : 0;
	v->value.anybounds.last = (last >= 0) ? last : PG_UINT32_MAX;

	return v;
}

static JsonPathParseItem *
makeItemLikeRegex(JsonPathParseItem *expr, string *pattern, string *flags)
{
	JsonPathParseItem *v = makeItemType(jpiLikeRegex);
	int			i;

	v->value.like_regex.expr = expr;
	v->value.like_regex.pattern = pattern->val;
	v->value.like_regex.patternlen = pattern->len;
	v->value.like_regex.flags = 0;

	for (i = 0; flags && i < flags->len; i++)
	{
		switch (flags->val[i])
		{
			case 'i':
				v->value.like_regex.flags |= JSP_REGEX_ICASE;
				break;
			case 's':
				v->value.like_regex.flags &= ~JSP_REGEX_MLINE;
				v->value.like_regex.flags |= JSP_REGEX_SLINE;
				break;
			case 'm':
				v->value.like_regex.flags &= ~JSP_REGEX_SLINE;
				v->value.like_regex.flags |= JSP_REGEX_MLINE;
				break;
			case 'x':
				v->value.like_regex.flags |= JSP_REGEX_WSPACE;
				break;
			default:
				yyerror(NULL, "unrecognized flag of LIKE_REGEX predicate");
				break;
		}
	}

	v->flags |= expr->flags & JSPI_EXT_EXEC;
	v->datatype = JPI_BOOL;

	return v;
}

static JsonPathParseItem *
makeItemSequence(List *elems)
{
	JsonPathParseItem  *v = makeItemType(jpiSequence);
	ListCell   *lc;

	v->value.sequence.elems = elems;

	foreach(lc, elems)
		v->flags |= ((JsonPathParseItem *) lfirst(lc))->flags & JSPI_EXT_EXEC;

	return v;
}

static JsonPathParseItem *
makeItemObject(List *fields)
{
	JsonPathParseItem *v = makeItemType(jpiObject);
	ListCell   *lc;

	v->value.object.fields = fields;

	foreach(lc, fields)
	{
		JsonPathParseItem *field = lfirst(lc);

		v->flags |= (field->value.args.left->flags |
					 field->value.args.right->flags) & JSPI_EXT_EXEC;
	}

	return v;
}

static inline JsonPathParseItem *
setItemOutPathMode(JsonPathParseItem *jpi)
{
	jpi->flags |= JSPI_OUT_PATH;
	return jpi;
}

static List *
setItemsOutPathMode(List *items)
{
	ListCell   *cell;

	foreach(cell, items)
		setItemOutPathMode(lfirst(cell));

	return items;
}

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="jsonpath_yy"
%error-verbose
%parse-param {JsonPathParseResult **result}

%union {
	string				str;
	List				*elems;		/* list of JsonPathParseItem */
	List				*indexs;	/* list of integers */
	JsonPathParseItem	*value;
	JsonPathParseResult *result;
	JsonPathItemType	optype;
	bool				boolean;
	struct JsonPathTypeName {
		List			   *name;
		List			   *typmods;
		bool				isarray;
	} type_name;
}

%token	<str>		TO_P NULL_P TRUE_P FALSE_P IS_P UNKNOWN_P EXISTS_P
%token	<str>		IDENT_P STRING_P NUMERIC_P INT_P VARIABLE_P
%token	<str>		OR_P AND_P NOT_P
%token	<str>		LESS_P LESSEQUAL_P EQUAL_P NOTEQUAL_P GREATEREQUAL_P GREATER_P
%token	<str>		ANY_P STRICT_P LAX_P LAST_P STARTS_P WITH_P LIKE_REGEX_P FLAG_P
%token	<str>		ABS_P SIZE_P TYPE_P FLOOR_P DOUBLE_P CEILING_P DATETIME_P
%token	<str>		KEYVALUE_P MAP_P REDUCE_P FOLD_P FOLDL_P FOLDR_P
%token	<str>		MIN_P MAX_P CURRENT_P TYPECAST_P OPERATOR_P

%type	<result>	result

%type	<value>		scalar_value path_primary expr pexpr array_accessor
					any_path accessor_op key delimited_predicate
					index_elem starts_with_initial opt_datetime_template
					expr_or_seq expr_seq object_field

%type	<elems>		accessor_expr accessor_ops expr_list object_field_list qualified_name

%type	<indexs>	index_list

%type	<optype>	comp_op method fold

%type	<boolean>	mode opt_type_array_bounds

%type	<str>		key_name

%type	<type_name>	type_name

%left	OR_P
%left	AND_P
%right	NOT_P
%left	EQUAL_P NOTEQUAL_P LESS_P LESSEQUAL_P GREATER_P GREATEREQUAL_P STARTS_P LIKE_REGEX_P
%left	POSTFIXOP		/* dummy for postfix OPERATOR_P rules */
%left	OPERATOR_P		/* multi-character ops and user-defined operators */
%left	'+' '-'
%left	'*' '/' '%'
%left	UMINUS
%nonassoc '(' ')'
%left	TYPECAST_P

/* Grammar follows */
%%

result:
	mode expr_or_seq				{
										*result = palloc(sizeof(JsonPathParseResult));
										(*result)->expr = $2;
										(*result)->lax = $1;
									}
	| /* EMPTY */					{ *result = NULL; }
	;

expr_or_seq:
	expr							{ $$ = $1; }
	| expr_seq						{ $$ = $1; }
	;

expr_seq:
	expr_list						{ $$ = makeItemSequence($1); }
	;

expr_list:
	expr ',' expr					{ $$ = list_make2($1, $3); }
	| expr_list ',' expr			{ $$ = lappend($1, $3); }
	;

mode:
	STRICT_P						{ $$ = false; }
	| LAX_P							{ $$ = true; }
	| /* EMPTY */					{ $$ = true; }
	;

scalar_value:
	STRING_P						{ $$ = makeItemString(&$1); }
	| NULL_P						{ $$ = makeItemString(NULL); }
	| TRUE_P						{ $$ = makeItemBool(true); }
	| FALSE_P						{ $$ = makeItemBool(false); }
	| NUMERIC_P						{ $$ = makeItemNumeric(&$1); }
	| INT_P							{ $$ = makeItemNumeric(&$1); }
	| VARIABLE_P 					{ $$ = makeItemVariable(&$1); }
	;

comp_op:
	EQUAL_P							{ $$ = jpiEqual; }
	| NOTEQUAL_P					{ $$ = jpiNotEqual; }
	| LESS_P						{ $$ = jpiLess; }
	| GREATER_P						{ $$ = jpiGreater; }
	| LESSEQUAL_P					{ $$ = jpiLessOrEqual; }
	| GREATEREQUAL_P				{ $$ = jpiGreaterOrEqual; }
	;

delimited_predicate:
	'(' expr ')'					{ $$ = $2; }
	| EXISTS_P '(' expr ')'			{ $$ = makeItemUnaryExpr(jpiExists, $3); }
	;

starts_with_initial:
	STRING_P						{ $$ = makeItemString(&$1); }
	| VARIABLE_P					{ $$ = makeItemVariable(&$1); }
	;

path_primary:
	scalar_value					{ $$ = $1; }
	| '$'							{ $$ = makeItemType(jpiRoot); }
	| '@'							{ $$ = makeItemType(jpiCurrent); }
	| CURRENT_P						{ $$ = makeItemCurrentN(pg_atoi(&$1.val[1], 4, 0)); }
	| LAST_P						{ $$ = makeItemType(jpiLast); }
	| '(' expr_seq ')'				{ $$ = $2; }
	| '[' ']'						{ $$ = makeItemUnary(jpiArray, NULL); }
	| '[' expr_or_seq ']'			{ $$ = makeItemUnary(jpiArray, $2); }
	| '{' object_field_list '}'		{ $$ = makeItemObject($2); }
	;

object_field_list:
	/* EMPTY */								{ $$ = NIL; }
	| object_field							{ $$ = list_make1($1); }
	| object_field_list ',' object_field	{ $$ = lappend($1, $3); }
	;

object_field:
	key_name ':' expr
		{ $$ = makeItemBinary(jpiObjectField, makeItemString(&$1), $3); }
	;

accessor_expr:
	path_primary					{ $$ = list_make1($1); }
	| '.' key						{ $$ = list_make2(makeItemType(jpiCurrent), $2); }
	| '(' expr ')' accessor_op		{ $$ = list_make2($2, $4); }
	| accessor_expr accessor_op		{ $$ = lappend($1, $2); }
	| accessor_expr '.' '(' key ')'
		{ $$ = lappend($1, setItemOutPathMode($4)); }
	| accessor_expr '.' '(' key accessor_ops ')'
		{ $$ = list_concat($1, setItemsOutPathMode(lcons($4, $5))); }
	| accessor_expr '.' '(' '*' ')'
		{ $$ = lappend($1, setItemOutPathMode(makeItemType(jpiAnyKey))); }
	| accessor_expr '.' '(' '*' accessor_ops ')'
		{ $$ = list_concat($1, setItemsOutPathMode(lcons(makeItemType(jpiAnyKey), $5))); }
	| accessor_expr '.' '(' array_accessor ')'
		{ $$ = lappend($1, setItemOutPathMode($4)); }
	| accessor_expr '.' '(' array_accessor accessor_ops ')'
		{ $$ = list_concat($1, setItemsOutPathMode(lcons($4, $5))); }
	| accessor_expr '.' '(' any_path ')'
		{ $$ = lappend($1, setItemOutPathMode($4)); }
	| accessor_expr '.' '(' any_path accessor_ops ')'
		{ $$ = list_concat($1, setItemsOutPathMode(lcons($4, $5))); }
	;

accessor_ops:
	accessor_op						{ $$ = list_make1($1); }
	| accessor_ops accessor_op		{ $$ = lappend($1, $2); }
	;

pexpr:
	expr							{ $$ = $1; }
	| '(' expr ')'					{ $$ = $2; }
	;

opt_type_array_bounds:
	'[' ']'							{ $$ = TRUE; }
	| /* empty */					{ $$ = FALSE; }
	;

qualified_name:
	key_name	 					{ $$ = list_make1(makeItemKey(&$1)); }
	| qualified_name '.' key_name	{ $$ = lappend($1, makeItemKey(&$3)); }
	;

type_name:
	qualified_name opt_type_array_bounds
		{ $$.name = $1; $$.typmods = NIL; $$.isarray = $2; }
	| qualified_name '(' expr_list ')' opt_type_array_bounds
		{ $$.name = $1; $$.typmods = $3; $$.isarray = $5; }
	;

expr:
	accessor_expr						{ $$ = makeItemList($1); }
	| EXISTS_P '(' expr ')'				{ $$ = makeItemUnaryExpr(jpiExists, $3); }
	| pexpr comp_op pexpr %prec EQUAL_P	{ $$ = makeItemBinaryExpr($2, $1, $3); }
	| pexpr AND_P pexpr					{ $$ = makeItemBinaryExpr(jpiAnd, $1, $3); }
	| pexpr OR_P pexpr					{ $$ = makeItemBinaryExpr(jpiOr, $1, $3); }
	| NOT_P delimited_predicate			{ $$ = makeItemUnaryExpr(jpiNot, $2); }
	| '(' expr ')' IS_P UNKNOWN_P		{ $$ = makeItemUnaryExpr(jpiIsUnknown, $2); }
	| pexpr STARTS_P WITH_P starts_with_initial
										{ $$ = makeItemBinaryExpr(jpiStartsWith, $1, $4); }
	| pexpr LIKE_REGEX_P STRING_P 		{ $$ = makeItemLikeRegex($1, &$3, NULL); };
	| pexpr LIKE_REGEX_P STRING_P FLAG_P STRING_P
										{ $$ = makeItemLikeRegex($1, &$3, &$5); };
	| pexpr TYPECAST_P type_name		{ $$ = makeItemCast($1, &$3); }
	| '+' pexpr %prec UMINUS			{ $$ = makeItemUnaryExpr(jpiPlus, $2); }
	| '-' pexpr %prec UMINUS			{ $$ = makeItemUnaryExpr(jpiMinus, $2); }
	| OPERATOR_P pexpr %prec OPERATOR_P	{ $$ = makeItemOperator(&$1, NULL, $2); }
	| pexpr OPERATOR_P %prec POSTFIXOP	{ $$ = makeItemOperator(&$2, $1, NULL); }
	| pexpr '+' pexpr					{ $$ = makeItemBinaryExpr(jpiAdd, $1, $3); }
	| pexpr '-' pexpr					{ $$ = makeItemBinaryExpr(jpiSub, $1, $3); }
	| pexpr '*' pexpr					{ $$ = makeItemBinaryExpr(jpiMul, $1, $3); }
	| pexpr '/' pexpr					{ $$ = makeItemBinaryExpr(jpiDiv, $1, $3); }
	| pexpr '%' pexpr					{ $$ = makeItemBinaryExpr(jpiMod, $1, $3); }
	| pexpr OPERATOR_P pexpr			{ $$ = makeItemOperator(&$2, $1, $3); }
	;

index_elem:
	pexpr							{ $$ = makeItemBinary(jpiSubscript, $1, NULL); }
	| pexpr TO_P pexpr				{ $$ = makeItemBinary(jpiSubscript, $1, $3); }
	;

index_list:
	index_elem						{ $$ = list_make1($1); }
	| index_list ',' index_elem		{ $$ = lappend($1, $3); }
	;

array_accessor:
	'[' '*' ']'						{ $$ = makeItemType(jpiAnyArray); }
	| '[' index_list ']'			{ $$ = makeIndexArray($2); }
	;

any_path:
	ANY_P							{ $$ = makeAny(-1, -1); }
	| ANY_P '{' INT_P '}'			{ $$ = makeAny(pg_atoi($3.val, 4, 0),
												   pg_atoi($3.val, 4, 0)); }
	| ANY_P '{' ',' INT_P '}'		{ $$ = makeAny(-1, pg_atoi($4.val, 4, 0)); }
	| ANY_P '{' INT_P ',' '}'		{ $$ = makeAny(pg_atoi($3.val, 4, 0), -1); }
	| ANY_P '{' INT_P ',' INT_P '}'	{ $$ = makeAny(pg_atoi($3.val, 4, 0),
												   pg_atoi($5.val, 4, 0)); }
	;

accessor_op:
	'.' key							{ $$ = $2; }
	| '.' '*'						{ $$ = makeItemType(jpiAnyKey); }
	| array_accessor				{ $$ = $1; }
	| '.' array_accessor			{ $$ = $2; }
	| '.' any_path					{ $$ = $2; }
	| '.' method '(' ')'			{ $$ = makeItemType($2); }
	| '.' DATETIME_P '(' opt_datetime_template ')'
									{ $$ = makeItemUnary(jpiDatetime, $4); }
	| '.' MAP_P '(' expr ')'
									{ $$ = makeItemUnary(jpiMap, $4); }
//	| '.' COUNT_P '(' ')'			{ $$ = makeItemType(jpiCount); }
	| '.' REDUCE_P '(' expr ')'
									{ $$ = makeItemUnary(jpiReduce, $4); }
	| '.' fold '(' expr ',' expr ')'
									{ $$ = makeItemBinary($2, $4, $6); }
	| '?' '(' expr ')'
		{
			if ($3->datatype == JPI_JSON || $3->next)
				yyerror(NULL, "expected boolean expression");
			$$ = makeItemUnary(jpiFilter, $3);
		}
	;

fold:
	FOLD_P							{ $$ = jpiFold; }
	| FOLDL_P						{ $$ = jpiFoldl; }
	| FOLDR_P						{ $$ = jpiFoldr; }
	;

opt_datetime_template:
	STRING_P						{ $$ = makeItemString(&$1); }
	| /* EMPTY */					{ $$ = NULL; }
	;

key:
	key_name						{ $$ = makeItemKey(&$1); }
	;

key_name:
	IDENT_P
	| STRING_P
	| TO_P
	| NULL_P
	| TRUE_P
	| FALSE_P
	| INT_P
	| IS_P
	| UNKNOWN_P
	| EXISTS_P
	| STRICT_P
	| LAX_P
	| ABS_P
	| SIZE_P
	| TYPE_P
	| FLOOR_P
	| DOUBLE_P
	| CEILING_P
	| DATETIME_P
	| KEYVALUE_P
	| LAST_P
	| STARTS_P
	| WITH_P
	| LIKE_REGEX_P
	| FLAG_P
	| MAP_P
	| REDUCE_P
	| FOLD_P
	| FOLDL_P
	| FOLDR_P
	| MIN_P
	| MAX_P
	;

method:
	ABS_P							{ $$ = jpiAbs; }
	| SIZE_P						{ $$ = jpiSize; }
	| TYPE_P						{ $$ = jpiType; }
	| FLOOR_P						{ $$ = jpiFloor; }
	| DOUBLE_P						{ $$ = jpiDouble; }
	| CEILING_P						{ $$ = jpiCeiling; }
	| KEYVALUE_P					{ $$ = jpiKeyValue; }
	| MIN_P							{ $$ = jpiMin; }
	| MAX_P							{ $$ = jpiMax; }
	;
%%

