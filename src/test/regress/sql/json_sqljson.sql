-- JSON_EXISTS

SELECT JSON_EXISTS(NULL FORMAT JSON, '$');
SELECT JSON_EXISTS(NULL::text FORMAT JSON, '$');
SELECT JSON_EXISTS(NULL::bytea FORMAT JSON, '$');
SELECT JSON_EXISTS(NULL::json FORMAT JSON, '$');
SELECT JSON_EXISTS(NULL::jsonb FORMAT JSON, '$');
SELECT JSON_EXISTS(NULL::json, '$');

SELECT JSON_EXISTS('' FORMAT JSON, '$');
SELECT JSON_EXISTS('' FORMAT JSON, '$' TRUE ON ERROR);
SELECT JSON_EXISTS('' FORMAT JSON, '$' FALSE ON ERROR);
SELECT JSON_EXISTS('' FORMAT JSON, '$' UNKNOWN ON ERROR);
SELECT JSON_EXISTS('' FORMAT JSON, '$' ERROR ON ERROR);


SELECT JSON_EXISTS(bytea '' FORMAT JSON, '$' ERROR ON ERROR);

SELECT JSON_EXISTS(json '[]', '$');
SELECT JSON_EXISTS('[]' FORMAT JSON, '$');
SELECT JSON_EXISTS(JSON_OBJECT(RETURNING bytea FORMAT JSON) FORMAT JSON, '$');

SELECT JSON_EXISTS(json '1', '$');
SELECT JSON_EXISTS(json 'null', '$');
SELECT JSON_EXISTS(json '[]', '$');

SELECT JSON_EXISTS(json '1', '$.a');
SELECT JSON_EXISTS(json '1', 'strict $.a');
SELECT JSON_EXISTS(json '1', 'strict $.a' ERROR ON ERROR);
SELECT JSON_EXISTS(json 'null', '$.a');
SELECT JSON_EXISTS(json '[]', '$.a');
SELECT JSON_EXISTS(json '[1, "aaa", {"a": 1}]', 'strict $.a');
SELECT JSON_EXISTS(json '[1, "aaa", {"a": 1}]', 'lax $.a');
SELECT JSON_EXISTS(json '{}', '$.a');
SELECT JSON_EXISTS(json '{"b": 1, "a": 2}', '$.a');

SELECT JSON_EXISTS(json '1', '$.a.b');
SELECT JSON_EXISTS(json '{"a": {"b": 1}}', '$.a.b');
SELECT JSON_EXISTS(json '{"a": 1, "b": 2}', '$.a.b');

SELECT JSON_EXISTS(json '{"a": 1, "b": 2}', '$.* ? (@ > $x)' PASSING 1 AS x);
SELECT JSON_EXISTS(json '{"a": 1, "b": 2}', '$.* ? (@ > $x)' PASSING '1' AS x);
SELECT JSON_EXISTS(json '{"a": 1, "b": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 2 AS y);
SELECT JSON_EXISTS(json '{"a": 1, "b": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 1 AS y);

-- extension: boolean expressions
SELECT JSON_EXISTS(json '1', '$ > 2');
SELECT JSON_EXISTS(json '1', '$.a > 2' ERROR ON ERROR);

-- JSON_VALUE

SELECT JSON_VALUE(NULL, '$');
SELECT JSON_VALUE(NULL FORMAT JSON, '$');
SELECT JSON_VALUE(NULL::text, '$');
SELECT JSON_VALUE(NULL::bytea, '$');
SELECT JSON_VALUE(NULL::json, '$');
SELECT JSON_VALUE(NULL::jsonb FORMAT JSON, '$');

SELECT JSON_VALUE('' FORMAT JSON, '$');
SELECT JSON_VALUE('' FORMAT JSON, '$' NULL ON ERROR);
SELECT JSON_VALUE('' FORMAT JSON, '$' DEFAULT '"default value"' ON ERROR);
SELECT JSON_VALUE('' FORMAT JSON, '$' ERROR ON ERROR);

SELECT JSON_VALUE(json 'null', '$');
SELECT JSON_VALUE(json 'null', '$' RETURNING int);

SELECT JSON_VALUE(json 'true', '$');
SELECT JSON_VALUE(json 'true', '$' RETURNING bool);

SELECT JSON_VALUE(json '123', '$');
SELECT JSON_VALUE(json '123', '$' RETURNING int) + 234;
SELECT JSON_VALUE(json '123', '$' RETURNING text);
/* jsonb bytea ??? */
SELECT JSON_VALUE(json '123', '$' RETURNING bytea);

SELECT JSON_VALUE(json '1.23', '$');
SELECT JSON_VALUE(json '1.23', '$' RETURNING int);
SELECT JSON_VALUE(json '"1.23"', '$' RETURNING numeric);
SELECT JSON_VALUE(json '"1.23"', '$' RETURNING int ERROR ON ERROR);

SELECT JSON_VALUE(json '"aaa"', '$');
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING text);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING char(5));
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING char(2));
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING json);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING jsonb);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING json ERROR ON ERROR);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING jsonb ERROR ON ERROR);
SELECT JSON_VALUE(json '"\"aaa\""', '$' RETURNING json);
SELECT JSON_VALUE(json '"\"aaa\""', '$' RETURNING jsonb);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING int);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING int ERROR ON ERROR);
SELECT JSON_VALUE(json '"aaa"', '$' RETURNING int DEFAULT 111 ON ERROR);
SELECT JSON_VALUE(json '"123"', '$' RETURNING int) + 234;

SELECT JSON_VALUE(json '"2017-02-20"', '$' RETURNING date) + 9;

-- Test NULL checks execution in domain types
CREATE DOMAIN sqljson_int_not_null AS int NOT NULL;
SELECT JSON_VALUE(json '1', '$.a' RETURNING sqljson_int_not_null);
SELECT JSON_VALUE(json '1', '$.a' RETURNING sqljson_int_not_null NULL ON ERROR);
SELECT JSON_VALUE(json '1', '$.a' RETURNING sqljson_int_not_null DEFAULT NULL ON ERROR);

SELECT JSON_VALUE(json '[]', '$');
SELECT JSON_VALUE(json '[]', '$' ERROR ON ERROR);
SELECT JSON_VALUE(json '{}', '$');
SELECT JSON_VALUE(json '{}', '$' ERROR ON ERROR);

SELECT JSON_VALUE(json '1', '$.a');
SELECT JSON_VALUE(json '1', 'strict $.a' ERROR ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' ERROR ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' ERROR ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(json '1', '$.a' DEFAULT 2 ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' DEFAULT 2 ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' DEFAULT '2' ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' NULL ON EMPTY DEFAULT '2' ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' DEFAULT '2' ON EMPTY DEFAULT '3' ON ERROR);
SELECT JSON_VALUE(json '1', 'lax $.a' ERROR ON EMPTY DEFAULT '3' ON ERROR);

SELECT JSON_VALUE(json '[1,2]', '$[*]' ERROR ON ERROR);
SELECT JSON_VALUE(json '[1,2]', '$[*]' DEFAULT '0' ON ERROR);
SELECT JSON_VALUE(json '[" "]', '$[*]' RETURNING int ERROR ON ERROR);
SELECT JSON_VALUE(json '[" "]', '$[*]' RETURNING int DEFAULT 2 + 3 ON ERROR);
SELECT JSON_VALUE(json '["1"]', '$[*]' RETURNING int DEFAULT 2 + 3 ON ERROR);

SELECT
	x,
	JSON_VALUE(
		json '{"a": 1, "b": 2}',
		'$.* ? (@ > $x)' PASSING x AS x
		RETURNING int
		DEFAULT -1 ON EMPTY
		DEFAULT -2 ON ERROR
	) y
FROM
	generate_series(0, 2) x;

SELECT JSON_VALUE(json 'null', '$a' PASSING point ' (1, 2 )' AS a);
SELECT JSON_VALUE(json 'null', '$a' PASSING point ' (1, 2 )' AS a RETURNING point);

-- JSON_QUERY

SELECT
	JSON_QUERY(js FORMAT JSON, '$'),
	JSON_QUERY(js FORMAT JSON, '$' WITHOUT WRAPPER),
	JSON_QUERY(js FORMAT JSON, '$' WITH CONDITIONAL WRAPPER),
	JSON_QUERY(js FORMAT JSON, '$' WITH UNCONDITIONAL ARRAY WRAPPER),
	JSON_QUERY(js FORMAT JSON, '$' WITH ARRAY WRAPPER)
FROM
	(VALUES
		('null'),
		('12.3'),
		('true'),
		('"aaa"'),
		('[1, null, "2"]'),
		('{"a": 1, "b": [2]}')
	) foo(js);

SELECT
	JSON_QUERY(js FORMAT JSON, 'strict $[*]') AS "unspec",
	JSON_QUERY(js FORMAT JSON, 'strict $[*]' WITHOUT WRAPPER) AS "without",
	JSON_QUERY(js FORMAT JSON, 'strict $[*]' WITH CONDITIONAL WRAPPER) AS "with cond",
	JSON_QUERY(js FORMAT JSON, 'strict $[*]' WITH UNCONDITIONAL ARRAY WRAPPER) AS "with uncond",
	JSON_QUERY(js FORMAT JSON, 'strict $[*]' WITH ARRAY WRAPPER) AS "with"
FROM
	(VALUES
		('1'),
		('[]'),
		('[null]'),
		('[12.3]'),
		('[true]'),
		('["aaa"]'),
		('[[1, 2, 3]]'),
		('[{"a": 1, "b": [2]}]'),
		('[1, "2", null, [3]]')
	) foo(js);

SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING text);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING text KEEP QUOTES);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING text KEEP QUOTES ON SCALAR STRING);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING text OMIT QUOTES);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING text OMIT QUOTES ON SCALAR STRING);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING json OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY('"aaa"' FORMAT JSON, '$' RETURNING bytea FORMAT JSON OMIT QUOTES ERROR ON ERROR);

-- QUOTES behavior should not be specified when WITH WRAPPER used:
-- Should fail
SELECT JSON_QUERY(json '[1]', '$' WITH WRAPPER OMIT QUOTES);
SELECT JSON_QUERY(json '[1]', '$' WITH WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(json '[1]', '$' WITH CONDITIONAL WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(json '[1]', '$' WITH CONDITIONAL WRAPPER OMIT QUOTES);
-- Should succeed
SELECT JSON_QUERY(json '[1]', '$' WITHOUT WRAPPER OMIT QUOTES);
SELECT JSON_QUERY(json '[1]', '$' WITHOUT WRAPPER KEEP QUOTES);

SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]');
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' NULL ON EMPTY);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' EMPTY ARRAY ON EMPTY);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' EMPTY OBJECT ON EMPTY);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' ERROR ON EMPTY);

SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' ERROR ON EMPTY NULL ON ERROR);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' ERROR ON EMPTY EMPTY ARRAY ON ERROR);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' ERROR ON EMPTY EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' ERROR ON EMPTY ERROR ON ERROR);
SELECT JSON_QUERY('[]' FORMAT JSON, '$[*]' ERROR ON ERROR);

SELECT JSON_QUERY('[1,2]' FORMAT JSON, '$[*]' ERROR ON ERROR);

SELECT JSON_QUERY(json '[1,2]', '$' RETURNING json);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING json FORMAT JSON);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING jsonb);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING jsonb FORMAT JSON);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING text);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING char(10));
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING char(3));
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING text FORMAT JSON);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING bytea);
SELECT JSON_QUERY(json '[1,2]', '$' RETURNING bytea FORMAT JSON);

SELECT JSON_QUERY(json '[1,2]', '$[*]' RETURNING bytea EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(json '[1,2]', '$[*]' RETURNING bytea FORMAT JSON EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(json '[1,2]', '$[*]' RETURNING json EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(json '[1,2]', '$[*]' RETURNING jsonb EMPTY OBJECT ON ERROR);

SELECT
	x, y,
	JSON_QUERY(
		json '[1,2,3,4,5,null]',
		'$[*] ? (@ >= $x && @ <= $y)'
		PASSING x AS x, y AS y
		WITH CONDITIONAL WRAPPER
		EMPTY ARRAY ON EMPTY
	) list
FROM
	generate_series(0, 4) x,
	generate_series(0, 4) y;

-- Conversion to record types
CREATE TYPE sqljson_rec AS (a int, t text, js json, jb jsonb, jsa json[]);
CREATE TYPE sqljson_reca AS (reca sqljson_rec[]);

SELECT JSON_QUERY(json '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljson_rec);
SELECT * FROM unnest((JSON_QUERY(json '{"jsa":  [{"a": 1, "b": ["foo"]}, {"a": 2, "c": {}}, 123]}', '$' RETURNING sqljson_rec)).jsa);
SELECT * FROM unnest((JSON_QUERY(json '{"reca": [{"a": 1, "t": ["foo", []]}, {"a": 2, "jb": [{}, true]}]}', '$' RETURNING sqljson_reca)).reca);

-- Conversion to array types
SELECT JSON_QUERY(json '[1,2,null,"3"]', '$[*]' RETURNING int[] WITH WRAPPER);
SELECT * FROM unnest(JSON_QUERY(json '[{"a": 1, "t": ["foo", []]}, {"a": 2, "jb": [{}, true]}]', '$' RETURNING sqljson_rec[]));

-- Conversion to domain types
SELECT JSON_QUERY(json '{"a": 1}', '$.a' RETURNING sqljson_int_not_null);
SELECT JSON_QUERY(json '{"a": 1}', '$.b' RETURNING sqljson_int_not_null);

-- Test constraints

CREATE TABLE test_json_constraints (
	js text,
	i int,
	x jsonb DEFAULT JSON_QUERY(json '[1,2]', '$[*]' WITH WRAPPER)
	CONSTRAINT test_json_constraint1
		CHECK (js IS JSON)
	CONSTRAINT test_json_constraint2
		CHECK (JSON_EXISTS(js FORMAT JSON, '$.a' PASSING i + 5 AS int, i::text AS txt, array[1,2,3] as arr))
	CONSTRAINT test_json_constraint3
		CHECK (JSON_VALUE(js::json, '$.a' RETURNING int DEFAULT ('12' || i)::int ON EMPTY ERROR ON ERROR) > i)
	CONSTRAINT test_json_constraint4
		CHECK (JSON_QUERY(js FORMAT JSON, '$.a' RETURNING jsonb WITH CONDITIONAL WRAPPER EMPTY OBJECT ON ERROR) < jsonb '[10]')
	CONSTRAINT test_json_constraint5
		CHECK (JSON_QUERY(js FORMAT JSON, '$.a' RETURNING char(5) OMIT QUOTES EMPTY ARRAY ON EMPTY) >  'a')
);

\d test_json_constraints

SELECT check_clause
FROM information_schema.check_constraints
WHERE constraint_name LIKE 'test_json_constraint%';

SELECT adsrc FROM pg_attrdef WHERE adrelid = 'test_json_constraints'::regclass;

INSERT INTO test_json_constraints VALUES ('', 1);
INSERT INTO test_json_constraints VALUES ('1', 1);
INSERT INTO test_json_constraints VALUES ('[]');
INSERT INTO test_json_constraints VALUES ('{"b": 1}', 1);
INSERT INTO test_json_constraints VALUES ('{"a": 1}', 1);
INSERT INTO test_json_constraints VALUES ('{"a": 7}', 1);
INSERT INTO test_json_constraints VALUES ('{"a": 10}', 1);

DROP TABLE test_json_constraints;

-- JSON_TABLE

-- Should fail (JSON_TABLE can be used only in FROM clause)
SELECT JSON_TABLE('[]', '$');

-- Should fail (no columns)
SELECT * FROM JSON_TABLE(NULL, '$' COLUMNS ());

-- NULL => empty table
SELECT * FROM JSON_TABLE(NULL, '$' COLUMNS (foo int)) bar;

-- invalid json => empty table
SELECT * FROM JSON_TABLE('', '$' COLUMNS (foo int)) bar;
SELECT * FROM JSON_TABLE('' FORMAT JSON,  '$' COLUMNS (foo int)) bar;

-- invalid json => error
SELECT * FROM JSON_TABLE('' FORMAT JSON, '$' COLUMNS (foo int) ERROR ON ERROR) bar;

--
SELECT * FROM JSON_TABLE('123' FORMAT JSON, '$'
	COLUMNS (item int PATH '$', foo int)) bar;

SELECT * FROM JSON_TABLE(json '123', '$'
	COLUMNS (item int PATH '$', foo int)) bar;

-- JSON_TABLE: basic functionality
SELECT *
FROM
	(VALUES
		('1'),
		('[]'),
		('{}'),
		('[1, 1.23, "2", "aaaaaaa", null, false, true, {"aaa": 123}, "[1,2]", "\"str\""]'),
		('err')
	) vals(js)
	LEFT OUTER JOIN
-- JSON_TABLE is implicitly lateral
	JSON_TABLE(
		vals.js FORMAT json, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			id2 FOR ORDINALITY, -- allowed additional ordinality columns
			"int" int PATH '$',
			"text" text PATH '$',
			"char(4)" char(4) PATH '$',
			"bool" bool PATH '$',
			"numeric" numeric PATH '$',
			js json PATH '$',
			jb jsonb PATH '$',
			jst text    FORMAT JSON  PATH '$',
			jsc char(4) FORMAT JSON  PATH '$',
			jsv varchar(4) FORMAT JSON  PATH '$',
			jsb jsonb   FORMAT JSON PATH '$',
			aaa int, -- implicit path '$."aaa"',
			aaa1 int PATH '$.aaa'
		)
	) jt
	ON true;

-- JSON_TABLE: Test backward parsing

CREATE VIEW json_table_view AS
SELECT * FROM
	JSON_TABLE(
		'null' FORMAT JSON, 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			id FOR ORDINALITY,
			id2 FOR ORDINALITY, -- allowed additional ordinality columns
			"int" int PATH '$',
			"text" text PATH '$',
			"char(4)" char(4) PATH '$',
			"bool" bool PATH '$',
			"numeric" numeric PATH '$',
			js json PATH '$',
			jb jsonb PATH '$',
			jst text    FORMAT JSON  PATH '$',
			jsc char(4) FORMAT JSON  PATH '$',
			jsv varchar(4) FORMAT JSON  PATH '$',
			jsb jsonb   FORMAT JSON PATH '$',
			aaa int, -- implicit path '$."aaa"',
			aaa1 int PATH '$.aaa',
			NESTED PATH '$[1]' AS p1 COLUMNS (
				a1 int,
				NESTED PATH '$[*]' AS "p1 1" COLUMNS (
					a11 text
				),
				b1 text
			),
			NESTED PATH '$[2]' AS p2 COLUMNS (
				NESTED PATH '$[*]' AS "p2:1" COLUMNS (
					a21 text
				),
				NESTED PATH '$[*]' AS p22 COLUMNS (
					a22 text
				)
			)
		)
	);

\sv json_table_view

EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM json_table_view;

-- JSON_TABLE: ON EMPTY/ON ERROR behavior
SELECT *
FROM
	(VALUES ('1'), ('err'), ('"err"')) vals(js),
	JSON_TABLE(vals.js FORMAT JSON, '$' COLUMNS (a int PATH '$')) jt;

SELECT *
FROM
	(VALUES ('1'), ('err'), ('"err"')) vals(js)
		LEFT OUTER JOIN
	JSON_TABLE(vals.js FORMAT JSON, '$' COLUMNS (a int PATH '$') ERROR ON ERROR) jt
		ON true;

SELECT *
FROM
	(VALUES ('1'), ('err'), ('"err"')) vals(js)
		LEFT OUTER JOIN
	JSON_TABLE(vals.js FORMAT JSON, '$' COLUMNS (a int PATH '$' ERROR ON ERROR)) jt
		ON true;

SELECT * FROM JSON_TABLE('1', '$' COLUMNS (a int PATH '$.a' ERROR ON EMPTY)) jt;
SELECT * FROM JSON_TABLE('1', '$' COLUMNS (a int PATH 'strict $.a' ERROR ON EMPTY) ERROR ON ERROR) jt;
SELECT * FROM JSON_TABLE('1', '$' COLUMNS (a int PATH 'lax $.a' ERROR ON EMPTY) ERROR ON ERROR) jt;

SELECT * FROM JSON_TABLE(json '"a"', '$' COLUMNS (a int PATH '$'   DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;
SELECT * FROM JSON_TABLE(json '"a"', '$' COLUMNS (a int PATH 'strict $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;
SELECT * FROM JSON_TABLE(json '"a"', '$' COLUMNS (a int PATH 'lax $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;

-- JSON_TABLE: nested paths and plans

-- Should fail (JSON_TABLE columns shall contain explicit AS path
-- specifications if explicit PLAN clause is used)
SELECT * FROM JSON_TABLE(
	json '[]', '$' -- AS <path name> required here
	COLUMNS (
		foo int PATH '$'
	)
	PLAN DEFAULT (UNION)
) jt;

SELECT * FROM JSON_TABLE(
	json '[]', '$' AS path1
	COLUMNS (
		NESTED PATH '$' COLUMNS ( -- AS <path name> required here
			foo int PATH '$'
		)
	)
	PLAN DEFAULT (UNION)
) jt;

-- Should fail (column names anf path names shall be distinct)
SELECT * FROM JSON_TABLE(
	json '[]', '$' AS a
	COLUMNS (
		a int
	)
) jt;

SELECT * FROM JSON_TABLE(
	json '[]', '$' AS a
	COLUMNS (
		b int,
		NESTED PATH '$' AS a
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	json '[]', '$'
	COLUMNS (
		b int,
		NESTED PATH '$' AS b
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	json '[]', '$'
	COLUMNS (
		NESTED PATH '$' AS a
		COLUMNS (
			b int
		),
		NESTED PATH '$'
		COLUMNS (
			NESTED PATH '$' AS a
			COLUMNS (
				c int
			)
		)
	)
) jt;

-- JSON_TABLE: plan validation

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p1)
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0)
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER p3)
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER (p1 CROSS p13))
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER (p1 CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 UNION p11) CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 INNER p11) CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', '$[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 INNER (p12 CROSS p11)) CROSS p2))
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', 'strict $[*]' AS p0
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN (p0 OUTER ((p1 INNER (p12 CROSS p11)) CROSS (p2 INNER p21)))
) jt;

SELECT * FROM JSON_TABLE(
	json 'null', 'strict $[*]' -- without root path name
	COLUMNS (
		NESTED PATH '$' AS p1 COLUMNS (
			NESTED PATH '$' AS p11 COLUMNS ( foo int ),
			NESTED PATH '$' AS p12 COLUMNS ( bar int )
		),
		NESTED PATH '$' AS p2 COLUMNS (
			NESTED PATH '$' AS p21 COLUMNS ( baz int )
		)
	)
	PLAN ((p1 INNER (p12 CROSS p11)) CROSS (p2 INNER p21))
) jt;

-- JSON_TABLE: plan execution

CREATE TEMP TABLE json_table_test (js text);

INSERT INTO json_table_test
VALUES (
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []}, 
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
);

-- unspecified plan (outer, union)
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
	) jt;

-- default plan (outer, union)
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (outer, union)
	) jt;

-- specific plan (p outer (pb union pc))
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pb union pc))
	) jt;

-- specific plan (p outer (pc union pb))
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pc union pb))
	) jt;

-- default plan (inner, union)
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (inner)
	) jt;

-- specific plan (p inner (pb union pc))
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p inner (pb union pc))
	) jt;

-- default plan (inner, cross)
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (cross, inner)
	) jt;

-- specific plan (p inner (pb cross pc))
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p inner (pb cross pc))
	) jt;

-- default plan (outer, cross)
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (outer, cross)
	) jt;

-- specific plan (p outer (pb cross pc))
select
	jt.*
from
	json_table_test jtt,
	json_table (
		jtt.js FORMAT JSON,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pb cross pc))
	) jt;


select
	jt.*, b1 + 100 as b
from
	json_table (json
		'[
			{"a":  1,  "b": [[1, 10], [2], [3, 30, 300]], "c": [1, null, 2]},
			{"a":  2,  "b": [10, 20], "c": [1, null, 2]}, 
			{"x": "3", "b": [11, 22, 33, 44]}
		 ]', 
		'$[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on error,
			nested path 'strict $.b[*]' as pb columns (
				b text format json path '$', 
				nested path 'strict $[*]' as pb1 columns (
					b1 int path '$'
				)
			),
			nested path 'strict $.c[*]' as pc columns (
				c text format json path '$',
				nested path 'strict $[*]' as pc1 columns (
					c1 int path '$'
				)
			)
		)
		--plan default(outer, cross)
		plan(p outer ((pb inner pb1) cross (pc outer pc1)))
	) jt;

-- Should succeed (JSON arguments are passed to root and nested paths)
SELECT *
FROM
	generate_series(1, 4) x,
	generate_series(1, 3) y,
	JSON_TABLE(json
		'[[1,2,3],[2,3,4,5],[3,4,5,6]]',
		'strict $[*] ? (@.[*] < $x)'
		PASSING x AS x, y AS y
		COLUMNS (
			y text FORMAT JSON PATH '$',
			NESTED PATH 'strict $[*] ? (@ >= $y)'
			COLUMNS (
				z int PATH '$'
			)
		)
	) jt;

-- Should fail (JSON arguments are not passed to column paths)
SELECT *
FROM JSON_TABLE(
	json '[1,2,3]',
	'$[*] ? (@ < $x)'
		PASSING 10 AS x
		COLUMNS (y text FORMAT JSON PATH '$ ? (@ < $x)')
	) jt;
