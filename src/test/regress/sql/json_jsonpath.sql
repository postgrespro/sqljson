select _jsonpath_exists(json '{"a": 12}', '$.a.b');
select _jsonpath_exists(json '{"a": 12}', '$.b');
select _jsonpath_exists(json '{"a": {"a": 12}}', '$.a.a');
select _jsonpath_exists(json '{"a": {"a": 12}}', '$.*.a');
select _jsonpath_exists(json '{"b": {"a": 12}}', '$.*.a');
select _jsonpath_exists(json '{}', '$.*');
select _jsonpath_exists(json '{"a": 1}', '$.*');
select _jsonpath_exists(json '{"a": {"b": 1}}', 'lax $.**{1}');
select _jsonpath_exists(json '{"a": {"b": 1}}', 'lax $.**{2}');
select _jsonpath_exists(json '{"a": {"b": 1}}', 'lax $.**{3}');
select _jsonpath_exists(json '[]', '$.[*]');
select _jsonpath_exists(json '[1]', '$.[*]');
select _jsonpath_exists(json '[1]', '$.[1]');
select _jsonpath_exists(json '[1]', 'strict $.[1]');
select _jsonpath_exists(json '[1]', '$.[0]');
select _jsonpath_exists(json '[1]', '$.[0.3]');
select _jsonpath_exists(json '[1]', '$.[0.5]');
select _jsonpath_exists(json '[1]', '$.[0.9]');
select _jsonpath_exists(json '[1]', '$.[1.2]');
select _jsonpath_exists(json '[1]', 'strict $.[1.2]');
select _jsonpath_exists(json '{}', 'strict $.[0.3]');
select _jsonpath_exists(json '{}', 'lax $.[0.3]');
select _jsonpath_exists(json '{}', 'strict $.[1.2]');
select _jsonpath_exists(json '{}', 'lax $.[1.2]');
select _jsonpath_exists(json '{}', 'strict $.[-2 to 3]');
select _jsonpath_exists(json '{}', 'lax $.[-2 to 3]');

select _jsonpath_exists(json '{"a": [1,2,3], "b": [3,4,5]}', '$ ? (@.a[*] >  @.b[*])');
select _jsonpath_exists(json '{"a": [1,2,3], "b": [3,4,5]}', '$ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(json '{"a": [1,2,3], "b": [3,4,"5"]}', '$ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(json '{"a": [1,2,3], "b": [3,4,"5"]}', 'strict $ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(json '{"a": [1,2,3], "b": [3,4,null]}', '$ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(json '1', '$ ? ((@ == "1") is unknown)');
select _jsonpath_exists(json '1', '$ ? ((@ == 1) is unknown)');
select _jsonpath_exists(json '[{"a": 1}, {"a": 2}]', '$[0 to 1] ? (@.a > 1)');

select * from _jsonpath_query(json '{"a": 12, "b": {"a": 13}}', '$.a');
select * from _jsonpath_query(json '{"a": 12, "b": {"a": 13}}', '$.b');
select * from _jsonpath_query(json '{"a": 12, "b": {"a": 13}}', '$.*');
select * from _jsonpath_query(json '{"a": 12, "b": {"a": 13}}', 'lax $.*.a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[*].a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[*].*');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[0].a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[1].a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[2].a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[0,1].a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}]', 'lax $.[0 to 10].a');
select * from _jsonpath_query(json '[12, {"a": 13}, {"b": 14}, "ccc", true]', '$.[2.5 - 1 to @.size() - 2]');
select * from _jsonpath_query(json '1', 'lax $[0]');
select * from _jsonpath_query(json '1', 'lax $[*]');
select * from _jsonpath_query(json '{}', 'lax $[0]');
select * from _jsonpath_query(json '[1]', 'lax $[0]');
select * from _jsonpath_query(json '[1]', 'lax $[*]');
select * from _jsonpath_query(json '[1,2,3]', 'lax $[*]');
select * from _jsonpath_query(json '[]', '$[last]');
select * from _jsonpath_query(json '[]', 'strict $[last]');
select * from _jsonpath_query(json '[1]', '$[last]');
select * from _jsonpath_query(json '{}', 'lax $[last]');
select * from _jsonpath_query(json '[1,2,3]', '$[last]');
select * from _jsonpath_query(json '[1,2,3]', '$[last - 1]');
select * from _jsonpath_query(json '[1,2,3]', '$[last ? (@.type() == "number")]');
select * from _jsonpath_query(json '[1,2,3]', '$[last ? (@.type() == "string")]');

select * from _jsonpath_query(json '{"a": 10}', '$');
select * from _jsonpath_query(json '{"a": 10}', '$ ? (.a < $value)');
select * from _jsonpath_query(json '{"a": 10}', '$ ? (.a < $value)', '{"value" : 13}');
select * from _jsonpath_query(json '{"a": 10}', '$ ? (.a < $value)', '{"value" : 8}');
select * from _jsonpath_query(json '{"a": 10}', '$.a ? (@ < $value)', '{"value" : 13}');
select * from _jsonpath_query(json '[10,11,12,13,14,15]', '$.[*] ? (@ < $value)', '{"value" : 13}');
select * from _jsonpath_query(json '[10,11,12,13,14,15]', '$.[0,1] ? (@ < $value)', '{"value" : 13}');
select * from _jsonpath_query(json '[10,11,12,13,14,15]', '$.[0 to 2] ? (@ < $value)', '{"value" : 15}');
select * from _jsonpath_query(json '[1,"1",2,"2",null]', '$.[*] ? (@ == "1")');
select * from _jsonpath_query(json '[1,"1",2,"2",null]', '$.[*] ? (@ == $value)', '{"value" : "1"}');

select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{1}');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{1,}');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{2}');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{2,}');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{3,}');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{0}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{1}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{0,}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{1,}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"b": 1}}', 'lax $.**{1,2}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**{0}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**{1}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**{0,}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**{1,}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**{1,2}.b ? (@ > 0)');
select * from _jsonpath_query(json '{"a": {"c": {"b": 1}}}', 'lax $.**{2,3}.b ? (@ > 0)');

select * from _jsonpath_exists(json '{"a": {"b": 1}}', '$.**.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"b": 1}}', '$.**{0}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"b": 1}}', '$.**{1}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"b": 1}}', '$.**{0,}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"b": 1}}', '$.**{1,}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"b": 1}}', '$.**{1,2}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**{0}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**{1}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**{0,}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**{1,}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**{1,2}.b ? ( @ > 0)');
select * from _jsonpath_exists(json '{"a": {"c": {"b": 1}}}', '$.**{2,3}.b ? ( @ > 0)');

select _jsonpath_query(json '{"g": {"x": 2}}', '$.g ? (exists (@.x))');
select _jsonpath_query(json '{"g": {"x": 2}}', '$.g ? (exists (@.y))');
select _jsonpath_query(json '{"g": {"x": 2}}', '$.g ? (exists (@.x ? (@ >= 2) ))');

--test ternary logic
select
	x, y,
	_jsonpath_query(
		json '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true && $y == true) ||
				 @ == false && !($x == true && $y == true) ||
				 @ == null  &&  ($x == true && $y == true) is unknown)',
		json_build_object('x', x, 'y', y)
	) as "x && y"
from
	(values (json 'true'), ('false'), ('"null"')) x(x),
	(values (json 'true'), ('false'), ('"null"')) y(y);

select
	x, y,
	_jsonpath_query(
		json '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true || $y == true) ||
				 @ == false && !($x == true || $y == true) ||
				 @ == null  &&  ($x == true || $y == true) is unknown)',
		json_build_object('x', x, 'y', y)
	) as "x || y"
from
	(values (json 'true'), ('false'), ('"null"')) x(x),
	(values (json 'true'), ('false'), ('"null"')) y(y);

select _jsonpath_exists(json '{"a": 1, "b": 1}', '$ ? (.a == .b)');
select _jsonpath_exists(json '{"c": {"a": 1, "b": 1}}', '$ ? (.a == .b)');
select _jsonpath_exists(json '{"c": {"a": 1, "b": 1}}', '$.c ? (.a == .b)');
select _jsonpath_exists(json '{"c": {"a": 1, "b": 1}}', '$.c ? ($.c.a == .b)');
select _jsonpath_exists(json '{"c": {"a": 1, "b": 1}}', '$.* ? (.a == .b)');
select _jsonpath_exists(json '{"a": 1, "b": 1}', '$.** ? (.a == .b)');
select _jsonpath_exists(json '{"c": {"a": 1, "b": 1}}', '$.** ? (.a == .b)');

select _jsonpath_query(json '{"c": {"a": 2, "b": 1}}', '$.** ? (.a == 1 + 1)');
select _jsonpath_query(json '{"c": {"a": 2, "b": 1}}', '$.** ? (.a == (1 + 1))');
select _jsonpath_query(json '{"c": {"a": 2, "b": 1}}', '$.** ? (.a == .b + 1)');
select _jsonpath_query(json '{"c": {"a": 2, "b": 1}}', '$.** ? (.a == (.b + 1))');
select _jsonpath_exists(json '{"c": {"a": -1, "b": 1}}', '$.** ? (.a == - 1)');
select _jsonpath_exists(json '{"c": {"a": -1, "b": 1}}', '$.** ? (.a == -1)');
select _jsonpath_exists(json '{"c": {"a": -1, "b": 1}}', '$.** ? (.a == -.b)');
select _jsonpath_exists(json '{"c": {"a": -1, "b": 1}}', '$.** ? (.a == - .b)');
select _jsonpath_exists(json '{"c": {"a": 0, "b": 1}}', '$.** ? (.a == 1 - .b)');
select _jsonpath_exists(json '{"c": {"a": 2, "b": 1}}', '$.** ? (.a == 1 - - .b)');
select _jsonpath_exists(json '{"c": {"a": 0, "b": 1}}', '$.** ? (.a == 1 - +.b)');
select _jsonpath_exists(json '[1,2,3]', '$ ? (+@[*] > +2)');
select _jsonpath_exists(json '[1,2,3]', '$ ? (+@[*] > +3)');
select _jsonpath_exists(json '[1,2,3]', '$ ? (-@[*] < -2)');
select _jsonpath_exists(json '[1,2,3]', '$ ? (-@[*] < -3)');
select _jsonpath_exists(json '1', '$ ? ($ > 0)');

-- unwrapping of operator arguments in lax mode
select _jsonpath_query(json '{"a": [2]}', 'lax $.a * 3');
select _jsonpath_query(json '{"a": [2, 3, 4]}', 'lax -$.a');
-- should fail
select _jsonpath_query(json '{"a": [1, 2]}', 'lax $.a * 3');
-- should fail (by standard unwrapped only arguments of multiplicative expressions)
select _jsonpath_query(json '{"a": [2]}', 'lax $.a + 3');

-- extension: boolean expressions
select _jsonpath_query(json '2', '$ > 1');
select _jsonpath_query(json '2', '$ <= 1');
select _jsonpath_query(json '2', '$ == "2"');

select _jsonpath_predicate(json '2', '$ > 1');
select _jsonpath_predicate(json '2', '$ <= 1');
select _jsonpath_predicate(json '2', '$ == "2"');
select _jsonpath_predicate(json '2', '1');
select _jsonpath_predicate(json '{}', '$');
select _jsonpath_predicate(json '[]', '$');
select _jsonpath_predicate(json '[1,2,3]', '$[*]');
select _jsonpath_predicate(json '[]', '$[*]');
select _jsonpath_predicate(json '[[1, true], [2, false]]', 'strict $[*] ? (@[0] > $x) [1]', '{"x": 1}');
select _jsonpath_predicate(json '[[1, true], [2, false]]', 'strict $[*] ? (@[0] < $x) [1]', '{"x": 2}');

select _jsonpath_query(json '[null,1,true,"a",[],{}]', '$.type()');
select _jsonpath_query(json '[null,1,true,"a",[],{}]', 'lax $.type()');
select _jsonpath_query(json '[null,1,true,"a",[],{}]', '$[*].type()');
select _jsonpath_query(json 'null', 'null.type()');
select _jsonpath_query(json 'null', 'true.type()');
select _jsonpath_query(json 'null', '123.type()');
select _jsonpath_query(json 'null', '"123".type()');
select _jsonpath_query(json 'null', 'aaa.type()');

select _jsonpath_query(json '{"a": 2}', '($.a - 5).abs() + 10');
select _jsonpath_query(json '{"a": 2.5}', '-($.a * $.a).floor() + 10');
select _jsonpath_query(json '[1, 2, 3]', '($[*] > 2) ? (@ == true)');
select _jsonpath_query(json '[1, 2, 3]', '($[*] > 3).type()');
select _jsonpath_query(json '[1, 2, 3]', '($[*].a > 3).type()');
select _jsonpath_query(json '[1, 2, 3]', 'strict ($[*].a > 3).type()');

select _jsonpath_query(json '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]', 'strict $[*].size()');
select _jsonpath_query(json '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]', 'lax $[*].size()');

select _jsonpath_query(json '[0, 1, -2, -3.4, 5.6]', '$[*].abs()');
select _jsonpath_query(json '[0, 1, -2, -3.4, 5.6]', '$[*].floor()');
select _jsonpath_query(json '[0, 1, -2, -3.4, 5.6]', '$[*].ceiling()');
select _jsonpath_query(json '[0, 1, -2, -3.4, 5.6]', '$[*].ceiling().abs()');
select _jsonpath_query(json '[0, 1, -2, -3.4, 5.6]', '$[*].ceiling().abs().type()');

select _jsonpath_query(json '[{},1]', '$[*].keyvalue()');
select _jsonpath_query(json '{}', '$.keyvalue()');
select _jsonpath_query(json '{"a": 1, "b": [1, 2], "c": {"a": "bbb"}}', '$.keyvalue()');
select _jsonpath_query(json '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', '$[*].keyvalue()');
select _jsonpath_query(json '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'strict $.keyvalue()');
select _jsonpath_query(json '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'lax $.keyvalue()');

select _jsonpath_query(json 'null', '$.double()');
select _jsonpath_query(json 'true', '$.double()');
select _jsonpath_query(json '[]', '$.double()');
select _jsonpath_query(json '[]', 'strict $.double()');
select _jsonpath_query(json '{}', '$.double()');
select _jsonpath_query(json '1.23', '$.double()');
select _jsonpath_query(json '"1.23"', '$.double()');
select _jsonpath_query(json '"1.23aaa"', '$.double()');

select _jsonpath_query(json '["", "a", "abc", "abcabc"]', '$[*] ? (@ starts with "abc")');
select _jsonpath_query(json '["", "a", "abc", "abcabc"]', 'strict $ ? (@[*] starts with "abc")');
select _jsonpath_query(json '["", "a", "abd", "abdabc"]', 'strict $ ? (@[*] starts with "abc")');
select _jsonpath_query(json '["abc", "abcabc", null, 1]', 'strict $ ? (@[*] starts with "abc")');
select _jsonpath_query(json '["abc", "abcabc", null, 1]', 'strict $ ? ((@[*] starts with "abc") is unknown)');
select _jsonpath_query(json '[[null, 1, "abc", "abcabc"]]', 'lax $ ? (@[*] starts with "abc")');
select _jsonpath_query(json '[[null, 1, "abd", "abdabc"]]', 'lax $ ? ((@[*] starts with "abc") is unknown)');
select _jsonpath_query(json '[null, 1, "abd", "abdabc"]', 'lax $[*] ? ((@ starts with "abc") is unknown)');

select _jsonpath_query(json 'null', '$.datetime()');
select _jsonpath_query(json 'true', '$.datetime()');
select _jsonpath_query(json '[]', '$.datetime()');
select _jsonpath_query(json '[]', 'strict $.datetime()');
select _jsonpath_query(json '{}', '$.datetime()');
select _jsonpath_query(json '""', '$.datetime()');

-- Standard extension: UNIX epoch to timestamptz
select _jsonpath_query(json '0', '$.datetime()');
select _jsonpath_query(json '0', '$.datetime().type()');
select _jsonpath_query(json '1490216035.5', '$.datetime()');

select _jsonpath_query(json '"10-03-2017"',       '$.datetime("dd-mm-yyyy")');
select _jsonpath_query(json '"10-03-2017"',       '$.datetime("dd-mm-yyyy").type()');
select _jsonpath_query(json '"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy")');
select _jsonpath_query(json '"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy").type()');

select _jsonpath_query(json '"10-03-2017 12:34"', '       $.datetime("dd-mm-yyyy HH24:MI").type()');
select _jsonpath_query(json '"10-03-2017 12:34 +05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM").type()');
select _jsonpath_query(json '"12:34:56"',                '$.datetime("HH24:MI:SS").type()');
select _jsonpath_query(json '"12:34:56 +05:20"',         '$.datetime("HH24:MI:SS TZH:TZM").type()');

set time zone '+00';

select _jsonpath_query(json '"10-03-2017 12:34"',        '$.datetime("dd-mm-yyyy HH24:MI")');
select _jsonpath_query(json '"10-03-2017 12:34"',        '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select _jsonpath_query(json '"10-03-2017 12:34 +05"',    '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select _jsonpath_query(json '"10-03-2017 12:34 -05"',    '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select _jsonpath_query(json '"10-03-2017 12:34 +05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select _jsonpath_query(json '"10-03-2017 12:34 -05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select _jsonpath_query(json '"12:34"',       '$.datetime("HH24:MI")');
select _jsonpath_query(json '"12:34"',       '$.datetime("HH24:MI TZH")');
select _jsonpath_query(json '"12:34 +05"',    '$.datetime("HH24:MI TZH")');
select _jsonpath_query(json '"12:34 -05"',    '$.datetime("HH24:MI TZH")');
select _jsonpath_query(json '"12:34 +05:20"', '$.datetime("HH24:MI TZH:TZM")');
select _jsonpath_query(json '"12:34 -05:20"', '$.datetime("HH24:MI TZH:TZM")');

set time zone '+10';

select _jsonpath_query(json '"10-03-2017 12:34"',       '$.datetime("dd-mm-yyyy HH24:MI")');
select _jsonpath_query(json '"10-03-2017 12:34"',        '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select _jsonpath_query(json '"10-03-2017 12:34 +05"',    '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select _jsonpath_query(json '"10-03-2017 12:34 -05"',    '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select _jsonpath_query(json '"10-03-2017 12:34 +05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select _jsonpath_query(json '"10-03-2017 12:34 -05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select _jsonpath_query(json '"12:34"',        '$.datetime("HH24:MI")');
select _jsonpath_query(json '"12:34"',        '$.datetime("HH24:MI TZH")');
select _jsonpath_query(json '"12:34 +05"',    '$.datetime("HH24:MI TZH")');
select _jsonpath_query(json '"12:34 -05"',    '$.datetime("HH24:MI TZH")');
select _jsonpath_query(json '"12:34 +05:20"', '$.datetime("HH24:MI TZH:TZM")');
select _jsonpath_query(json '"12:34 -05:20"', '$.datetime("HH24:MI TZH:TZM")');

set time zone default;

select _jsonpath_query(json '"2017-03-10"', '$.datetime().type()');
select _jsonpath_query(json '"2017-03-10"', '$.datetime()');
select _jsonpath_query(json '"2017-03-10 12:34:56"', '$.datetime().type()');
select _jsonpath_query(json '"2017-03-10 12:34:56"', '$.datetime()');
select _jsonpath_query(json '"2017-03-10 12:34:56 +3"', '$.datetime().type()');
select _jsonpath_query(json '"2017-03-10 12:34:56 +3"', '$.datetime()');
select _jsonpath_query(json '"2017-03-10 12:34:56 +3:10"', '$.datetime().type()');
select _jsonpath_query(json '"2017-03-10 12:34:56 +3:10"', '$.datetime()');
select _jsonpath_query(json '"12:34:56"', '$.datetime().type()');
select _jsonpath_query(json '"12:34:56"', '$.datetime()');
select _jsonpath_query(json '"12:34:56 +3"', '$.datetime().type()');
select _jsonpath_query(json '"12:34:56 +3"', '$.datetime()');
select _jsonpath_query(json '"12:34:56 +3:10"', '$.datetime().type()');
select _jsonpath_query(json '"12:34:56 +3:10"', '$.datetime()');

-- date comparison
select _jsonpath_query(json
	'["10.03.2017", "11.03.2017", "09.03.2017"]',
	'$[*].datetime("dd.mm.yyyy") ? (@ == "10.03.2017".datetime("dd.mm.yyyy"))'
);
select _jsonpath_query(json
	'["10.03.2017", "11.03.2017", "09.03.2017"]',
	'$[*].datetime("dd.mm.yyyy") ? (@ >= "10.03.2017".datetime("dd.mm.yyyy"))'
);
select _jsonpath_query(json
	'["10.03.2017", "11.03.2017", "09.03.2017"]',
	'$[*].datetime("dd.mm.yyyy") ? (@ <  "10.03.2017".datetime("dd.mm.yyyy"))'
);

-- time comparison
select _jsonpath_query(json
	'["12:34", "12:35", "12:36"]',
	'$[*].datetime("HH24:MI") ? (@ == "12:35".datetime("HH24:MI"))'
);
select _jsonpath_query(json
	'["12:34", "12:35", "12:36"]',
	'$[*].datetime("HH24:MI") ? (@ >= "12:35".datetime("HH24:MI"))'
);
select _jsonpath_query(json
	'["12:34", "12:35", "12:36"]',
	'$[*].datetime("HH24:MI") ? (@ <  "12:35".datetime("HH24:MI"))'
);

-- timetz comparison
select _jsonpath_query(json
	'["12:34 +1", "12:35 +1", "12:36 +1", "12:35 +2", "12:35 -2"]',
	'$[*].datetime("HH24:MI TZH") ? (@ == "12:35 +1".datetime("HH24:MI TZH"))'
);
select _jsonpath_query(json
	'["12:34 +1", "12:35 +1", "12:36 +1", "12:35 +2", "12:35 -2"]',
	'$[*].datetime("HH24:MI TZH") ? (@ >= "12:35 +1".datetime("HH24:MI TZH"))'
);
select _jsonpath_query(json
	'["12:34 +1", "12:35 +1", "12:36 +1", "12:35 +2", "12:35 -2"]',
	'$[*].datetime("HH24:MI TZH") ? (@ <  "12:35 +1".datetime("HH24:MI TZH"))'
);

-- timestamp comparison
select _jsonpath_query(json
	'["10.03.2017 12:34", "10.03.2017 12:35", "10.03.2017 12:36"]',
	'$[*].datetime("dd.mm.yyyy HH24:MI") ? (@ == "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))'
);
select _jsonpath_query(json
	'["10.03.2017 12:34", "10.03.2017 12:35", "10.03.2017 12:36"]',
	'$[*].datetime("dd.mm.yyyy HH24:MI") ? (@ >= "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))'
);
select _jsonpath_query(json
	'["10.03.2017 12:34", "10.03.2017 12:35", "10.03.2017 12:36"]',
	'$[*].datetime("dd.mm.yyyy HH24:MI") ? (@ < "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))'
);

-- timestamptz compasison
select _jsonpath_query(json
	'["10.03.2017 12:34 +1", "10.03.2017 12:35 +1", "10.03.2017 12:36 +1", "10.03.2017 12:35 +2", "10.03.2017 12:35 -2"]',
	'$[*].datetime("dd.mm.yyyy HH24:MI TZH") ? (@ == "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))'
);
select _jsonpath_query(json
	'["10.03.2017 12:34 +1", "10.03.2017 12:35 +1", "10.03.2017 12:36 +1", "10.03.2017 12:35 +2", "10.03.2017 12:35 -2"]',
	'$[*].datetime("dd.mm.yyyy HH24:MI TZH") ? (@ >= "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))'
);
select _jsonpath_query(json
	'["10.03.2017 12:34 +1", "10.03.2017 12:35 +1", "10.03.2017 12:36 +1", "10.03.2017 12:35 +2", "10.03.2017 12:35 -2"]',
	'$[*].datetime("dd.mm.yyyy HH24:MI TZH") ? (@ < "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))'
);

-- jsonpath operators

SELECT json '[{"a": 1}, {"a": 2}]' @* '$[*]';
SELECT json '[{"a": 1}, {"a": 2}]' @* '$[*] ? (@.a > 10)';
SELECT json '[{"a": 1}, {"a": 2}]' @* '[$[*].a]';

SELECT json '[{"a": 1}, {"a": 2}]' @? '$[*].a > 1';
SELECT json '[{"a": 1}, {"a": 2}]' @? '$[*].a > 2';

-- extension: map item method
select _jsonpath_query(json '1', 'strict $.map(@ + 10)');
select _jsonpath_query(json '1', 'lax $.map(@ + 10)');
select _jsonpath_query(json '[1, 2, 3]', '$.map(@ + 10)');
select _jsonpath_query(json '[[1, 2], [3, 4, 5], [], [6, 7]]', '$.map(@.map(@ + 10))');

-- extension: reduce/fold item methods
select _jsonpath_query(json '1', 'strict $.reduce($1 + $2)');
select _jsonpath_query(json '1', 'lax $.reduce($1 + $2)');
select _jsonpath_query(json '1', 'strict $.fold($1 + $2, 10)');
select _jsonpath_query(json '1', 'lax $.fold($1 + $2, 10)');
select _jsonpath_query(json '[1, 2, 3]', '$.reduce($1 + $2)');
select _jsonpath_query(json '[1, 2, 3]', '$.fold($1 + $2, 100)');
select _jsonpath_query(json '[]', '$.reduce($1 + $2)');
select _jsonpath_query(json '[]', '$.fold($1 + $2, 100)');
select _jsonpath_query(json '[1]', '$.reduce($1 + $2)');
select _jsonpath_query(json '[1, 2, 3]', '$.foldl([$1, $2], [])');
select _jsonpath_query(json '[1, 2, 3]', '$.foldr([$2, $1], [])');
select _jsonpath_query(json '[[1, 2], [3, 4, 5], [], [6, 7]]', '$.fold($1 + $2.fold($1 + $2, 100), 1000)');

-- extension: min/max item methods
select _jsonpath_query(json '1', 'strict $.min()');
select _jsonpath_query(json '1', 'lax $.min()');
select _jsonpath_query(json '[]', '$.min()');
select _jsonpath_query(json '[]', '$.max()');
select _jsonpath_query(json '[1, 2, 3]', '$.min()');
select _jsonpath_query(json '[1, 2, 3]', '$.max()');
select _jsonpath_query(json '[2, 3, 5, 1, 4]', '$.min()');
select _jsonpath_query(json '[2, 3, 5, 1, 4]', '$.max()');

-- extension: path sequences
select _jsonpath_query(json '[1,2,3,4,5]', '10, 20, $[*], 30');
select _jsonpath_query(json '[1,2,3,4,5]', 'lax    10, 20, $[*].a, 30');
select _jsonpath_query(json '[1,2,3,4,5]', 'strict 10, 20, $[*].a, 30');
select _jsonpath_query(json '[1,2,3,4,5]', '-(10, 20, $[1 to 3], 30)');
select _jsonpath_query(json '[1,2,3,4,5]', 'lax (10, 20, $[1 to 3], 30).map(@ + 100)');
select _jsonpath_query(json '[1,2,3,4,5]', '$[(0, $[*], 5) ? (@ == 3)]');
select _jsonpath_query(json '[1,2,3,4,5]', '$[(0, $[*], 3) ? (@ == 3)]');

-- extension: array constructors
select _jsonpath_query(json '[1, 2, 3]', '[]');
select _jsonpath_query(json '[1, 2, 3]', '[1, 2, $.map(@ + 100)[*], 4, 5]');
select _jsonpath_query(json '[1, 2, 3]', '[1, 2, $.map(@ + 100)[*], 4, 5][*]');
select _jsonpath_query(json '[1, 2, 3]', '[(1, (2, $.map(@ + 100)[*])), (4, 5)]');
select _jsonpath_query(json '[1, 2, 3]', '[[1, 2], [$.map(@ + 100)[*], 4], 5, [(1,2)?(@ > 5)]]');
select _jsonpath_query(json '[1, 2, 3]', 'strict [1, 2, $.map(@.a)[*], 4, 5]');
select _jsonpath_query(json '[[1, 2], [3, 4, 5], [], [6, 7]]', '[$[*].map(@ + 10)[*] ? (@ > 13)]');

-- extension: object constructors
select _jsonpath_query(json '[1, 2, 3]', '{}');
select _jsonpath_query(json '[1, 2, 3]', '{a: 2 + 3, "b": [$[*], 4, 5]}');
select _jsonpath_query(json '[1, 2, 3]', '{a: 2 + 3, "b": [$[*], 4, 5]}.*');
select _jsonpath_query(json '[1, 2, 3]', '{a: 2 + 3, "b": ($[*], 4, 5)}');
select _jsonpath_query(json '[1, 2, 3]', '{a: 2 + 3, "b": [$.map({x: @, y: @ < 3})[*], {z: "foo"}]}');

-- extension: object subscripting
select _jsonpath_exists(json '{"a": 1}', '$["a"]');
select _jsonpath_exists(json '{"a": 1}', '$["b"]');
select _jsonpath_exists(json '{"a": 1}', 'strict $["b"]');
select _jsonpath_exists(json '{"a": 1}', '$["b", "a"]');

select * from _jsonpath_query(json '{"a": 1}', '$["a"]');
select * from _jsonpath_query(json '{"a": 1}', 'strict $["b"]');
select * from _jsonpath_query(json '{"a": 1}', 'lax $["b"]');
select * from _jsonpath_query(json '{"a": 1, "b": 2}', 'lax $["b", "c", "b", "a", 0 to 3]');

-- extension: outer item reference (@N)
select _jsonpath_query(json '[2,4,1,5,3]', '$[*] ? (!exists($[*] ? (@ < @1)))');
select _jsonpath_query(json '[2,4,1,5,3]', '$.map(@ + @1[0])');
-- the first @1 and @2 reference array, the second @1 -- current mapped array element
select _jsonpath_query(json '[2,4,1,5,3]', '$.map(@ + @1[@1 - @2[2]])');
select _jsonpath_query(json '[[2,4,1,5,3]]', '$.map(@.reduce($1 + $2 + @2[0][2] + @1[3]))');

-- extension: including subpaths into result
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.(a[*].b)');
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.(a[*]).b');
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.a.([*].b)');
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.(a)[*].b');
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.a[*].(b)');
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.(a)[*].(b)');
select _jsonpath_query(json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}', '$.(a.[0 to 1].b)');

-- extension: custom operators and type casts
select _jsonpath_query(json '"aaa"', '$::text || "bbb"::text || $::text');
select _jsonpath_query(json '"aaa"', '$::text || "bbb" || $');
select _jsonpath_query(json '[null, true, 1, "aaa",  {"a": 1}, [1, 2]]', '$.map(@::text || "xyz"::text)');

select _jsonpath_query(json '123.45', '$::int4');
select _jsonpath_query(json '123.45', '$::float4');
select _jsonpath_query(json '123.45', '$::text');
select _jsonpath_query(json '123.45', '$::text::int4');
select _jsonpath_query(json '123.45', '$::text::float4');
select _jsonpath_query(json '123.45', '$::text::float4::int4');
select _jsonpath_query(json '4000000000', '$::int8');

select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::text');
select _jsonpath_query(json '[123.45, null, 0.67, "8.9"]', '$[*]::text::float4::int4');


select _jsonpath_query(json '[123.45, 0.67]', '$[*]::int4 > $[0]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4 > $[0]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4 > $[1]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4 > $[2]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[0]::int4 > $[*]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4 > $[2]::text::float4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::text::float4 > $[2]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::text::float4 > $[2]::text::float4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4 > $[0 to 1]::int4');
select _jsonpath_query(json '[123.45, null, 0.67]', '$[*]::int4 > $[1 to 2]::int4');
select _jsonpath_query(json '[123.45, 100000.2, 10000.67, "1"]', '$[0]::int8 > $[*]::int4::int8');

select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}]', '$[*] -> "a"::text');
select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}]', '$[0] -> "a"::text');
select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}]', '$[1] -> $[0].a::text');
select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}]', '$[0] \? "a"::text');
select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}]', '$[*] \? "b"::text');
select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}]', '$[*] \? "c"::text');
select _jsonpath_query(json '[{"a": "b"}, {"b": [1, "2"]}, null, 1]', '$[*] ? (@ \? "a"::text)');

select _jsonpath_query(json '[1, "t", 0, "f", null]', '$[*] ? (@::int4)');
select _jsonpath_query(json '[1, "t", 0, "f", null]', '$[*] ? (@::bool)');
select _jsonpath_query(json '[1, "t", 0, "f", null]', '$[*] ? (!(@::bool))');
select _jsonpath_query(json '[1, "t", 0, "f", null]', '$[*] ? (@::bool == false::bool)');
select _jsonpath_query(json '[1, "t", 0, "f", null]', '$[*] ? (@::bool || !(@::bool))');


select _jsonpath_query(json '[1, 2, 3]', '$[*] ? (@::int4 > 1::int4)');

select json '1' @* '$ ? (@ \@> 1)';
select json '1' @* '$ ? (@ \@> 2)';

select jsonpath '$::a' + '1';

select jsonpath '$ ? ($.a::jsonb || 1)';
select jsonpath '$ ? ($.a::jsonb || ==== 1)';
select jsonpath '$::jsonb';

select json '{"tags": [{"term": "a"}, {"term": "NYC"}]}' @? 'strict $.tags \@> [{term: "NYC"}]';
select json '{"tags": [{"term": "a"}, {"term": "NYC"}]}' @? 'lax    $.tags \@>  {term: "NYC"}';

select json '"str"' @? '$ == "str"::jsonb';
