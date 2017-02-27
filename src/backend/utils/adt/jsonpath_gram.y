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
#include "miscadmin.h"
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

	CHECK_FOR_INTERRUPTS();

	v->type = type;
	v->next = NULL;

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
		v->string.val = s->val;
		v->string.len = s->len;
	}

	return v;
}

static JsonPathParseItem*
makeItemVariable(string *s)
{
	JsonPathParseItem *v;

	v = makeItemType(jpiVariable);
	v->string.val = s->val;
	v->string.len = s->len;

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
	v->numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(s->val), 0, -1));

	return v;
}

static JsonPathParseItem*
makeItemBool(bool val) {
	JsonPathParseItem *v = makeItemType(jpiBool);

	v->boolean = val;

	return v;
}

static JsonPathParseItem*
makeItemBinary(int type, JsonPathParseItem* la, JsonPathParseItem *ra)
{
	JsonPathParseItem  *v = makeItemType(type);

	v->args.left = la;
	v->args.right = ra;

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
		v->numeric =
			DatumGetNumeric(DirectFunctionCall1(numeric_uminus,
												NumericGetDatum(a->numeric)));
		return v;
	}

	v = makeItemType(type);

	v->arg = a;

	return v;
}

static JsonPathParseItem*
makeItemList(List *list) {
	JsonPathParseItem	*head, *end;
	ListCell	*cell;

	head = end = (JsonPathParseItem*)linitial(list);

	foreach(cell, list)
	{
		JsonPathParseItem	*c = (JsonPathParseItem*)lfirst(cell);

		if (c == head)
			continue;

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
	v->array.nelems = list_length(list);

	v->array.elems = palloc(sizeof(v->array.elems[0]) * v->array.nelems);

	foreach(cell, list)
		v->array.elems[i++] = lfirst_int(cell);

	return v;
}

static JsonPathParseItem*
makeAny(int first, int last)
{
	JsonPathParseItem *v = makeItemType(jpiAny);

	v->anybounds.first = (first > 0) ? first : 0;
	v->anybounds.last = (last >= 0) ? last : PG_UINT32_MAX;

	return v;
}

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="jsonpath_yy"
%error-verbose
%parse-param {JsonPathParseItem **result}

%union {
	string				str;
	List				*elems;		/* list of JsonPathParseItem */
	List				*indexs;
	JsonPathParseItem	*value;
	int					optype;
}

%token	<str>		TO_P NULL_P TRUE_P FALSE_P
%token	<str>		STRING_P NUMERIC_P INT_P VARIABLE_P
%token	<str>		OR_P AND_P NOT_P
%token	<str>		LESS_P LESSEQUAL_P EQUAL_P NOTEQUAL_P GREATEREQUAL_P GREATER_P
%token	<str>		ANY_P

%type	<value>		result jsonpath scalar_value path_primary expr
					array_accessor any_path accessor_op key unary_expr
					predicate delimited_predicate // numeric

%type	<elems>		accessor_expr /* path absolute_path relative_path */

%type	<indexs>	index_elem index_list

%type	<optype>	comp_op

%left	OR_P
%left	AND_P
%right	NOT_P
%left	'+' '-'
%left	'*' '/' '%'
%nonassoc '(' ')'

/* Grammar follows */
%%

result:
	jsonpath						{ *result = $1; }
	| /* EMPTY */					{ *result = NULL; }
	;

scalar_value:
	STRING_P						{ $$ = makeItemString(&$1); }
	| TO_P							{ $$ = makeItemString(&$1); }
	| NULL_P						{ $$ = makeItemString(NULL); }
	| TRUE_P						{ $$ = makeItemBool(true); }
	| FALSE_P						{ $$ = makeItemBool(false); }
	| NUMERIC_P						{ $$ = makeItemNumeric(&$1); }
	| INT_P							{ $$ = makeItemNumeric(&$1); }
	| VARIABLE_P 					{ $$ = makeItemVariable(&$1); }
	;

/*
numeric:
	NUMERIC_P						{ $$ = makeItemNumeric(&$1); }
	| INT_P							{ $$ = makeItemNumeric(&$1); }
	| VARIABLE_P 					{ $$ = makeItemVariable(&$1); }
	;
*/

jsonpath:
	expr
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
	'(' predicate ')'				{ $$ = $2; }
//	| EXISTS '(' relative_path ')'	{ $$ = makeItemUnary(jpiExists, $2); }
	;

predicate:
	delimited_predicate				{ $$ = $1; }
	| expr comp_op expr				{ $$ = makeItemBinary($2, $1, $3); }
//	| expr LIKE_REGEX pattern		{ $$ = ...; }
//	| expr STARTS WITH STRING_P		{ $$ = ...; }
//	| expr STARTS WITH '$' STRING_P	{ $$ = ...; }
//	| expr STARTS WITH '$' STRING_P	{ $$ = ...; }
//	| '.' any_key right_expr		{ $$ = makeItemList(list_make2($2, $3)); }
//	| '(' predicate ')' IS UNKNOWN	{ $$ = makeItemUnary(jpiIsUnknown, $2); }
	| predicate AND_P predicate		{ $$ = makeItemBinary(jpiAnd, $1, $3); }
	| predicate OR_P predicate		{ $$ = makeItemBinary(jpiOr, $1, $3); }
	| NOT_P delimited_predicate 	{ $$ = makeItemUnary(jpiNot, $2); }
	;

path_primary:
	scalar_value					{ $$ = $1; }
	| '$'							{ $$ = makeItemType(jpiRoot); }
	| '@'							{ $$ = makeItemType(jpiCurrent); }
	;

accessor_expr:
	path_primary					{ $$ = list_make1($1); }
	| '.' key						{ $$ = list_make2(makeItemType(jpiCurrent), $2); }
	| accessor_expr accessor_op		{ $$ = lappend($1, $2); }
	;

unary_expr:
	accessor_expr					{ $$ = makeItemList($1); }
	| '+' unary_expr				{ $$ = makeItemUnary(jpiPlus, $2); }
	| '-' unary_expr				{ $$ = makeItemUnary(jpiMinus, $2); }
	;

//	| '(' expr ')'					{ $$ = $2; }

expr:
	unary_expr						{ $$ = $1; }
	| expr '+' expr					{ $$ = makeItemBinary(jpiAdd, $1, $3); }
	| expr '-' expr					{ $$ = makeItemBinary(jpiSub, $1, $3); }
	| expr '*' expr					{ $$ = makeItemBinary(jpiMul, $1, $3); }
	| expr '/' expr					{ $$ = makeItemBinary(jpiDiv, $1, $3); }
	| expr '%' expr					{ $$ = makeItemBinary(jpiMod, $1, $3); }
	;

index_elem:
	INT_P							{ $$ = list_make1_int(pg_atoi($1.val, 4, 0)); }
	| INT_P TO_P INT_P				{
										int start = pg_atoi($1.val, 4, 0),
											stop = pg_atoi($3.val, 4, 0),
											i;

										$$ = NIL;

										for(i=start; i<= stop; i++)
											$$ = lappend_int($$, i);
									}
	;

index_list:
	index_elem						{ $$ = $1; }
	| index_list ',' index_elem		{ $$ = list_concat($1, $3); }
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
	| '?' '(' predicate ')'			{ $$ = makeItemUnary(jpiFilter, $3); }
	;

key:
	STRING_P						{ $$ = makeItemKey(&$1); }
	| TO_P							{ $$ = makeItemKey(&$1); }
	| NULL_P						{ $$ = makeItemKey(&$1); }
	| TRUE_P						{ $$ = makeItemKey(&$1); }
	| FALSE_P						{ $$ = makeItemKey(&$1); }
	;
/*
absolute_path:
	'$'	 							{ $$ = list_make1(makeItemType(jpiRoot)); }
	| '$' path						{ $$ = lcons(makeItemType(jpiRoot), $2); }
	;

relative_path:
	key								{ $$ = list_make1(makeItemType(jpiCurrent), $1); }
	| key path						{ $$ = lcons(makeItemType(jpiCurrent), lcons($1, $2)); }
	| '@'							{ $$ = list_make1(makeItemType(jpiCurrent)); }
	| '@' path						{ $$ = lcons(makeItemType(jpiCurrent), $2); }
	;

path:
	accessor_op						{ $$ = list_make($1); }
	| path accessor_op				{ $$ = lappend($1, $2); }
	;
*/
%%

