/*-------------------------------------------------------------------------
 *
 * jsonpath_gram.y
 *     Grammar definitions for jsonpath datatype
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/jsonpath_gram.y
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
	JsonPathParseItem  *v = makeItemType(type);

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
makeItemExpression(List *path, JsonPathParseItem *right_expr)
{
	JsonPathParseItem	*expr = makeItemUnary(jpiExpression, right_expr);

	return makeItemList(lappend(path, expr));
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
	JsonPathParseItem	*value;
}

%token	<str>		TO_P NULL_P TRUE_P FALSE_P 
%token	<str>		STRING_P NUMERIC_P INT_P

%token	<str>		OR_P AND_P NOT_P

%type	<value>		result scalar_value 

%type	<elems>		joined_key path absolute_path relative_path

%type 	<value>		key any_key right_expr expr jsonpath numeric

%left	OR_P 
%left	AND_P
%right	NOT_P
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
	| '$' STRING_P					{ $$ = makeItemVariable(&$2); }
	;

numeric:
	NUMERIC_P                       { $$ = makeItemNumeric(&$1); }
	| INT_P                         { $$ = makeItemNumeric(&$1); }
	| '$' STRING_P					{ $$ = makeItemVariable(&$2); } 
	;

right_expr:
	'='	scalar_value				{ $$ = makeItemUnary(jpiEqual, $2); }
	| '<' numeric					{ $$ = makeItemUnary(jpiLess, $2); }
	| '>' numeric					{ $$ = makeItemUnary(jpiGreater, $2); }
	| '<' '=' numeric				{ $$ = makeItemUnary(jpiLessOrEqual, $3); }
	| '>' '=' numeric				{ $$ = makeItemUnary(jpiGreaterOrEqual, $3); }
	;

jsonpath:
	absolute_path						{ $$ = makeItemList($1); }
	| absolute_path '?' '(' expr ')'	{ $$ = makeItemExpression($1, $4); }
	| relative_path '?' '(' expr ')'	{ $$ = makeItemExpression($1, $4); }
	;

expr:
	any_key right_expr					{ $$ = makeItemList(list_make2($1, $2)); }
	| '.' any_key right_expr			{ $$ = makeItemList(list_make2($2, $3)); }
	| '@' right_expr					
						{ $$ = makeItemList(list_make2(makeItemType(jpiCurrent), $2)); }
	| '@' '.' any_key right_expr
						{ $$ = makeItemList(list_make3(makeItemType(jpiCurrent),$3, $4)); }
	| relative_path '?' '(' expr ')'	{ $$ = makeItemExpression($1, $4); }
	| '(' expr ')'						{ $$ = $2; }
	| expr AND_P expr					{ $$ = makeItemBinary(jpiAnd, $1, $3); }
	| expr OR_P expr					{ $$ = makeItemBinary(jpiOr, $1, $3); }
	| NOT_P expr 						{ $$ = makeItemUnary(jpiNot, $2); }
	;

any_key:
	key								{ $$ = $1; }
	| '*'							{ $$ = makeItemType(jpiAnyKey); }
	| '[' '*' ']'					{ $$ = makeItemType(jpiAnyArray); }
	;

joined_key:
	any_key							{ $$ = list_make1($1); }
	| joined_key '[' '*' ']'		{ $$ = lappend($1, makeItemType(jpiAnyArray)); }
	;
key:
	STRING_P						{ $$ = makeItemKey(&$1); }
	| TO_P							{ $$ = makeItemKey(&$1); }
	| NULL_P						{ $$ = makeItemKey(&$1); }
	| TRUE_P						{ $$ = makeItemKey(&$1); }
	| FALSE_P						{ $$ = makeItemKey(&$1); }
	;

absolute_path:
	'$' '.' 						{ $$ = list_make1(makeItemType(jpiRoot)); }
	| '$'  							{ $$ = list_make1(makeItemType(jpiRoot)); }
	| '$' '.' path					{ $$ = lcons(makeItemType(jpiRoot), $3); }
	;

relative_path:
	joined_key '.' joined_key			{ $$ = list_concat($1, $3); }
	| '.' joined_key '.' joined_key		{ $$ = list_concat($2, $4); }
	| '@' '.' joined_key '.' joined_key	{ $$ = list_concat($3, $5); }
	| relative_path '.' joined_key		{ $$ = list_concat($1, $3); }

path:
	joined_key						{ $$ = $1; }
	| path '.' joined_key			{ $$ = list_concat($1, $3); }
	;

%%

