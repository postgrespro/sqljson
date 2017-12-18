/* contrib/jsonpathx/jsonpathx--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION jsonpathx" to load this file. \quit

CREATE FUNCTION map(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_map'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION flatmap(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_flatmap'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION reduce(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_reduce' 
LANGUAGE C STRICT STABLE;

CREATE FUNCTION fold(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_fold'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION foldl(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_foldl'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION foldr(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_foldr'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION min(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_min'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION max(jsonpath_fcxt)
RETURNS int8
AS 'MODULE_PATHNAME', 'jsonpath_max'
LANGUAGE C STRICT STABLE;
