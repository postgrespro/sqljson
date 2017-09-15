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
