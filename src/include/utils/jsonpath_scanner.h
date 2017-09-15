/*-------------------------------------------------------------------------
 *
 * jsonpath_scanner.h
 *	jsonpath scanner & parser support
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/include/utils/jsonpath_scanner.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_SCANNER_H
#define JSONPATH_SCANNER_H

/* struct string is shared between scan and gram */
typedef struct string {
	char	*val;
	int		len;
	int		total;
} string;

#include "utils/jsonpath.h"
#include "utils/jsonpath_gram.h"

/* flex 2.5.4 doesn't bother with a decl for this */
extern int jsonpath_yylex(YYSTYPE * yylval_param);
extern void jsonpath_yyerror(JsonPathParseResult **result, const char *message);

#endif
