select json '{"a": 12}' @? '$.a.b';
select json '{"a": 12}' @? '$.b';
select json '{"a": {"a": 12}}' @? '$.a.a';
select json '{"a": {"a": 12}}' @? '$.*.a';
select json '{"b": {"a": 12}}' @? '$.*.a';
select json '{}' @? '$.*';
select json '{"a": 1}' @? '$.*';
select json '{"a": {"b": 1}}' @? 'lax $.**{1}';
select json '{"a": {"b": 1}}' @? 'lax $.**{2}';
select json '{"a": {"b": 1}}' @? 'lax $.**{3}';
select json '[]' @? '$.[*]';
select json '[1]' @? '$.[*]';
select json '[1]' @? '$.[1]';
select json '[1]' @? 'strict $.[1]';
select json '[1]' @? '$.[0]';
select json '[1]' @? '$.[0.3]';
select json '[1]' @? '$.[0.5]';
select json '[1]' @? '$.[0.9]';
select json '[1]' @? '$.[1.2]';
select json '[1]' @? 'strict $.[1.2]';
select json '{}' @? 'strict $.[0.3]';
select json '{}' @? 'lax $.[0.3]';
select json '{}' @? 'strict $.[1.2]';
select json '{}' @? 'lax $.[1.2]';
select json '{}' @? 'strict $.[-2 to 3]';
select json '{}' @? 'lax $.[-2 to 3]';

select json '{"a": [1,2,3], "b": [3,4,5]}' @? '$ ? (@.a[*] >  @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,5]}' @? '$ ? (@.a[*] >= @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,"5"]}' @? '$ ? (@.a[*] >= @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,"5"]}' @? 'strict $ ? (@.a[*] >= @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,null]}' @? '$ ? (@.a[*] >= @.b[*])';
select json '1' @? '$ ? ((@ == "1") is unknown)';
select json '1' @? '$ ? ((@ == 1) is unknown)';
select json '[{"a": 1}, {"a": 2}]' @? '$[0 to 1] ? (@.a > 1)';

select json '{"a": 12, "b": {"a": 13}}' @* '$.a';
select json '{"a": 12, "b": {"a": 13}}' @* '$.b';
select json '{"a": 12, "b": {"a": 13}}' @* '$.*';
select json '{"a": 12, "b": {"a": 13}}' @* 'lax $.*.a';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[*].a';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[*].*';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[0].a';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[1].a';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[2].a';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[0,1].a';
select json '[12, {"a": 13}, {"b": 14}]' @* 'lax $.[0 to 10].a';
select json '[12, {"a": 13}, {"b": 14}, "ccc", true]' @* '$.[2.5 - 1 to @.size() - 2]';
select json '1' @* 'lax $[0]';
select json '1' @* 'lax $[*]';
select json '{}' @* 'lax $[0]';
select json '[1]' @* 'lax $[0]';
select json '[1]' @* 'lax $[*]';
select json '[1,2,3]' @* 'lax $[*]';
select json '[]' @* '$[last]';
select json '[]' @* 'strict $[last]';
select json '[1]' @* '$[last]';
select json '{}' @* 'lax $[last]';
select json '[1,2,3]' @* '$[last]';
select json '[1,2,3]' @* '$[last - 1]';
select json '[1,2,3]' @* '$[last ? (@.type() == "number")]';
select json '[1,2,3]' @* '$[last ? (@.type() == "string")]';

select * from jsonpath_query(json '{"a": 10}', '$');
select * from jsonpath_query(json '{"a": 10}', '$ ? (.a < $value)');
select * from jsonpath_query(json '{"a": 10}', '$ ? (.a < $value)', '{"value" : 13}');
select * from jsonpath_query(json '{"a": 10}', '$ ? (.a < $value)', '{"value" : 8}');
select * from jsonpath_query(json '{"a": 10}', '$.a ? (@ < $value)', '{"value" : 13}');
select * from jsonpath_query(json '[10,11,12,13,14,15]', '$.[*] ? (@ < $value)', '{"value" : 13}');
select * from jsonpath_query(json '[10,11,12,13,14,15]', '$.[0,1] ? (@ < $value)', '{"value" : 13}');
select * from jsonpath_query(json '[10,11,12,13,14,15]', '$.[0 to 2] ? (@ < $value)', '{"value" : 15}');
select * from jsonpath_query(json '[1,"1",2,"2",null]', '$.[*] ? (@ == "1")');
select * from jsonpath_query(json '[1,"1",2,"2",null]', '$.[*] ? (@ == $value)', '{"value" : "1"}');
select json '[1, "2", null]' @* '$[*] ? (@ != null)';
select json '[1, "2", null]' @* '$[*] ? (@ == null)';

select json '{"a": {"b": 1}}' @* 'lax $.**';
select json '{"a": {"b": 1}}' @* 'lax $.**{1}';
select json '{"a": {"b": 1}}' @* 'lax $.**{1,}';
select json '{"a": {"b": 1}}' @* 'lax $.**{2}';
select json '{"a": {"b": 1}}' @* 'lax $.**{2,}';
select json '{"a": {"b": 1}}' @* 'lax $.**{3,}';
select json '{"a": {"b": 1}}' @* 'lax $.**.b ? (@ > 0)';
select json '{"a": {"b": 1}}' @* 'lax $.**{0}.b ? (@ > 0)';
select json '{"a": {"b": 1}}' @* 'lax $.**{1}.b ? (@ > 0)';
select json '{"a": {"b": 1}}' @* 'lax $.**{0,}.b ? (@ > 0)';
select json '{"a": {"b": 1}}' @* 'lax $.**{1,}.b ? (@ > 0)';
select json '{"a": {"b": 1}}' @* 'lax $.**{1,2}.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**{0}.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**{1}.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**{0,}.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**{1,}.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**{1,2}.b ? (@ > 0)';
select json '{"a": {"c": {"b": 1}}}' @* 'lax $.**{2,3}.b ? (@ > 0)';

select json '{"a": {"b": 1}}' @? '$.**.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{0}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{1}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{0,}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{1,}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{1,2}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{0}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{1}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{0,}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{1,}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{1,2}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{2,3}.b ? ( @ > 0)';

select json '{"g": {"x": 2}}' @* '$.g ? (exists (@.x))';
select json '{"g": {"x": 2}}' @* '$.g ? (exists (@.y))';
select json '{"g": {"x": 2}}' @* '$.g ? (exists (@.x ? (@ >= 2) ))';

--test ternary logic
select
	x, y,
	jsonpath_query(
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
	jsonpath_query(
		json '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true || $y == true) ||
				 @ == false && !($x == true || $y == true) ||
				 @ == null  &&  ($x == true || $y == true) is unknown)',
		json_build_object('x', x, 'y', y)
	) as "x || y"
from
	(values (json 'true'), ('false'), ('"null"')) x(x),
	(values (json 'true'), ('false'), ('"null"')) y(y);

select json '{"a": 1, "b": 1}' @? '$ ? (.a == .b)';
select json '{"c": {"a": 1, "b": 1}}' @? '$ ? (.a == .b)';
select json '{"c": {"a": 1, "b": 1}}' @? '$.c ? (.a == .b)';
select json '{"c": {"a": 1, "b": 1}}' @? '$.c ? ($.c.a == .b)';
select json '{"c": {"a": 1, "b": 1}}' @? '$.* ? (.a == .b)';
select json '{"a": 1, "b": 1}' @? '$.** ? (.a == .b)';
select json '{"c": {"a": 1, "b": 1}}' @? '$.** ? (.a == .b)';

select json '{"c": {"a": 2, "b": 1}}' @* '$.** ? (.a == 1 + 1)';
select json '{"c": {"a": 2, "b": 1}}' @* '$.** ? (.a == (1 + 1))';
select json '{"c": {"a": 2, "b": 1}}' @* '$.** ? (.a == .b + 1)';
select json '{"c": {"a": 2, "b": 1}}' @* '$.** ? (.a == (.b + 1))';
select json '{"c": {"a": -1, "b": 1}}' @? '$.** ? (.a == - 1)';
select json '{"c": {"a": -1, "b": 1}}' @? '$.** ? (.a == -1)';
select json '{"c": {"a": -1, "b": 1}}' @? '$.** ? (.a == -.b)';
select json '{"c": {"a": -1, "b": 1}}' @? '$.** ? (.a == - .b)';
select json '{"c": {"a": 0, "b": 1}}' @? '$.** ? (.a == 1 - .b)';
select json '{"c": {"a": 2, "b": 1}}' @? '$.** ? (.a == 1 - - .b)';
select json '{"c": {"a": 0, "b": 1}}' @? '$.** ? (.a == 1 - +.b)';
select json '[1,2,3]' @? '$ ? (+@[*] > +2)';
select json '[1,2,3]' @? '$ ? (+@[*] > +3)';
select json '[1,2,3]' @? '$ ? (-@[*] < -2)';
select json '[1,2,3]' @? '$ ? (-@[*] < -3)';
select json '1' @? '$ ? ($ > 0)';

-- unwrapping of operator arguments in lax mode
select json '{"a": [2]}' @* 'lax $.a * 3';
select json '{"a": [2]}' @* 'lax $.a + 3';
select json '{"a": [2, 3, 4]}' @* 'lax -$.a';
-- should fail
select json '{"a": [1, 2]}' @* 'lax $.a * 3';

-- extension: boolean expressions
select json '2' @* '$ > 1';
select json '2' @* '$ <= 1';
select json '2' @* '$ == "2"';

select json '2' @~ '$ > 1';
select json '2' @~ '$ <= 1';
select json '2' @~ '$ == "2"';
select json '2' @~ '1';
select json '{}' @~ '$';
select json '[]' @~ '$';
select json '[1,2,3]' @~ '$[*]';
select json '[]' @~ '$[*]';
select jsonpath_predicate(json '[[1, true], [2, false]]', 'strict $[*] ? (@[0] > $x) [1]', '{"x": 1}');
select jsonpath_predicate(json '[[1, true], [2, false]]', 'strict $[*] ? (@[0] < $x) [1]', '{"x": 2}');

select json '[null,1,true,"a",[],{}]' @* '$.type()';
select json '[null,1,true,"a",[],{}]' @* 'lax $.type()';
select json '[null,1,true,"a",[],{}]' @* '$[*].type()';
select json 'null' @* 'null.type()';
select json 'null' @* 'true.type()';
select json 'null' @* '123.type()';
select json 'null' @* '"123".type()';

select json '{"a": 2}' @* '($.a - 5).abs() + 10';
select json '{"a": 2.5}' @* '-($.a * $.a).floor() + 10';
select json '[1, 2, 3]' @* '($[*] > 2) ? (@ == true)';
select json '[1, 2, 3]' @* '($[*] > 3).type()';
select json '[1, 2, 3]' @* '($[*].a > 3).type()';
select json '[1, 2, 3]' @* 'strict ($[*].a > 3).type()';

select json '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]' @* 'strict $[*].size()';
select json '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]' @* 'lax $[*].size()';

select json '[0, 1, -2, -3.4, 5.6]' @* '$[*].abs()';
select json '[0, 1, -2, -3.4, 5.6]' @* '$[*].floor()';
select json '[0, 1, -2, -3.4, 5.6]' @* '$[*].ceiling()';
select json '[0, 1, -2, -3.4, 5.6]' @* '$[*].ceiling().abs()';
select json '[0, 1, -2, -3.4, 5.6]' @* '$[*].ceiling().abs().type()';

select json '[{},1]' @* '$[*].keyvalue()';
select json '{}' @* '$.keyvalue()';
select json '{"a": 1, "b": [1, 2], "c": {"a": "bbb"}}' @* '$.keyvalue()';
select json '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]' @* '$[*].keyvalue()';
select json '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]' @* 'strict $.keyvalue()';
select json '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]' @* 'lax $.keyvalue()';

select json 'null' @* '$.double()';
select json 'true' @* '$.double()';
select json '[]' @* '$.double()';
select json '[]' @* 'strict $.double()';
select json '{}' @* '$.double()';
select json '1.23' @* '$.double()';
select json '"1.23"' @* '$.double()';
select json '"1.23aaa"' @* '$.double()';

select json '["", "a", "abc", "abcabc"]' @* '$[*] ? (@ starts with "abc")';
select json '["", "a", "abc", "abcabc"]' @* 'strict $ ? (@[*] starts with "abc")';
select json '["", "a", "abd", "abdabc"]' @* 'strict $ ? (@[*] starts with "abc")';
select json '["abc", "abcabc", null, 1]' @* 'strict $ ? (@[*] starts with "abc")';
select json '["abc", "abcabc", null, 1]' @* 'strict $ ? ((@[*] starts with "abc") is unknown)';
select json '[[null, 1, "abc", "abcabc"]]' @* 'lax $ ? (@[*] starts with "abc")';
select json '[[null, 1, "abd", "abdabc"]]' @* 'lax $ ? ((@[*] starts with "abc") is unknown)';
select json '[null, 1, "abd", "abdabc"]' @* 'lax $[*] ? ((@ starts with "abc") is unknown)';

select json '[null, 1, "abc", "abd", "aBdC", "abdacb", "babc"]' @* 'lax $[*] ? (@ like_regex "^ab.*c")';
select json '[null, 1, "abc", "abd", "aBdC", "abdacb", "babc"]' @* 'lax $[*] ? (@ like_regex "^ab.*c" flag "i")';

select json 'null' @* '$.datetime()';
select json 'true' @* '$.datetime()';
select json '[]' @* '$.datetime()';
select json '[]' @* 'strict $.datetime()';
select json '{}' @* '$.datetime()';
select json '""' @* '$.datetime()';

-- Standard extension: UNIX epoch to timestamptz
select json '0' @* '$.datetime()';
select json '0' @* '$.datetime().type()';
select json '1490216035.5' @* '$.datetime()';

select json '"10-03-2017"' @*       '$.datetime("dd-mm-yyyy")';
select json '"10-03-2017"' @*       '$.datetime("dd-mm-yyyy").type()';
select json '"10-03-2017 12:34"' @* '$.datetime("dd-mm-yyyy")';
select json '"10-03-2017 12:34"' @* '$.datetime("dd-mm-yyyy").type()';

select json '"10-03-2017 12:34"' @* '       $.datetime("dd-mm-yyyy HH24:MI").type()';
select json '"10-03-2017 12:34 +05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM").type()';
select json '"12:34:56"' @*                '$.datetime("HH24:MI:SS").type()';
select json '"12:34:56 +05:20"' @*         '$.datetime("HH24:MI:SS TZH:TZM").type()';

set time zone '+00';

select json '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI")';
select json '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select json '"10-03-2017 12:34 +05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select json '"10-03-2017 12:34 -05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select json '"10-03-2017 12:34 +05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select json '"10-03-2017 12:34 -05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select json '"12:34"' @*       '$.datetime("HH24:MI")';
select json '"12:34"' @*       '$.datetime("HH24:MI TZH")';
select json '"12:34 +05"' @*    '$.datetime("HH24:MI TZH")';
select json '"12:34 -05"' @*    '$.datetime("HH24:MI TZH")';
select json '"12:34 +05:20"' @* '$.datetime("HH24:MI TZH:TZM")';
select json '"12:34 -05:20"' @* '$.datetime("HH24:MI TZH:TZM")';

set time zone '+10';

select json '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI")';
select json '"10-03-2017 12:34"' @*        '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select json '"10-03-2017 12:34 +05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select json '"10-03-2017 12:34 -05"' @*    '$.datetime("dd-mm-yyyy HH24:MI TZH")';
select json '"10-03-2017 12:34 +05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select json '"10-03-2017 12:34 -05:20"' @* '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")';
select json '"12:34"' @*        '$.datetime("HH24:MI")';
select json '"12:34"' @*        '$.datetime("HH24:MI TZH")';
select json '"12:34 +05"' @*    '$.datetime("HH24:MI TZH")';
select json '"12:34 -05"' @*    '$.datetime("HH24:MI TZH")';
select json '"12:34 +05:20"' @* '$.datetime("HH24:MI TZH:TZM")';
select json '"12:34 -05:20"' @* '$.datetime("HH24:MI TZH:TZM")';

set time zone default;

select json '"2017-03-10"' @* '$.datetime().type()';
select json '"2017-03-10"' @* '$.datetime()';
select json '"2017-03-10 12:34:56"' @* '$.datetime().type()';
select json '"2017-03-10 12:34:56"' @* '$.datetime()';
select json '"2017-03-10 12:34:56 +3"' @* '$.datetime().type()';
select json '"2017-03-10 12:34:56 +3"' @* '$.datetime()';
select json '"2017-03-10 12:34:56 +3:10"' @* '$.datetime().type()';
select json '"2017-03-10 12:34:56 +3:10"' @* '$.datetime()';
select json '"12:34:56"' @* '$.datetime().type()';
select json '"12:34:56"' @* '$.datetime()';
select json '"12:34:56 +3"' @* '$.datetime().type()';
select json '"12:34:56 +3"' @* '$.datetime()';
select json '"12:34:56 +3:10"' @* '$.datetime().type()';
select json '"12:34:56 +3:10"' @* '$.datetime()';

set time zone '+00';

-- date comparison
select json '["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]'
	@* '$[*].datetime() ? (@ == "10.03.2017".datetime("dd.mm.yyyy"))';
select json '["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]'
	@* '$[*].datetime() ? (@ >= "10.03.2017".datetime("dd.mm.yyyy"))';
select json '["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]'
	@* '$[*].datetime() ? (@ <  "10.03.2017".datetime("dd.mm.yyyy"))';

-- time comparison
select json '["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]'
	@* '$[*].datetime() ? (@ == "12:35".datetime("HH24:MI"))';
select json '["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]'
	@* '$[*].datetime() ? (@ >= "12:35".datetime("HH24:MI"))';
select json '["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]'
	@* '$[*].datetime() ? (@ <  "12:35".datetime("HH24:MI"))';

-- timetz comparison
select json '["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]'
	@* '$[*].datetime() ? (@ == "12:35 +1".datetime("HH24:MI TZH"))';
select json '["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]'
	@* '$[*].datetime() ? (@ >= "12:35 +1".datetime("HH24:MI TZH"))';
select json '["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]'
	@* '$[*].datetime() ? (@ <  "12:35 +1".datetime("HH24:MI TZH"))';

-- timestamp comparison
select json '["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]'
	@* '$[*].datetime() ? (@ == "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))';
select json '["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]'
	@* '$[*].datetime() ? (@ >= "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))';
select json '["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]'
	@* '$[*].datetime() ? (@ < "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))';

-- timestamptz comparison
select json '["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]'
	@* '$[*].datetime() ? (@ == "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))';
select json '["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]'
	@* '$[*].datetime() ? (@ >= "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))';
select json '["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]'
	@* '$[*].datetime() ? (@ < "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))';

set time zone default;

-- jsonpath operators

SELECT json '[{"a": 1}, {"a": 2}]' @* '$[*]';
SELECT json '[{"a": 1}, {"a": 2}]' @* '$[*] ? (@.a > 10)';
SELECT json '[{"a": 1}, {"a": 2}]' @* '[$[*].a]';

SELECT json '[{"a": 1}, {"a": 2}]' @? '$[*] ? (@.a > 1)';
SELECT json '[{"a": 1}, {"a": 2}]' @? '$[*].a ? (@ > 2)';

SELECT json '[{"a": 1}, {"a": 2}]' @~ '$[*].a > 1';
SELECT json '[{"a": 1}, {"a": 2}]' @~ '$[*].a > 2';

-- extension: map item method
select json '1' @* 'strict $.map(@ + 10)';
select json '1' @* 'lax $.map(@ + 10)';
select json '[1, 2, 3]' @* '$.map(@ + 10)';
select json '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '$.map(@.map(@ + 10))';

-- extension: reduce/fold item methods
select json '1' @* 'strict $.reduce($1 + $2)';
select json '1' @* 'lax $.reduce($1 + $2)';
select json '1' @* 'strict $.fold($1 + $2, 10)';
select json '1' @* 'lax $.fold($1 + $2, 10)';
select json '[1, 2, 3]' @* '$.reduce($1 + $2)';
select json '[1, 2, 3]' @* '$.fold($1 + $2, 100)';
select json '[]' @* '$.reduce($1 + $2)';
select json '[]' @* '$.fold($1 + $2, 100)';
select json '[1]' @* '$.reduce($1 + $2)';
select json '[1, 2, 3]' @* '$.foldl([$1, $2], [])';
select json '[1, 2, 3]' @* '$.foldr([$2, $1], [])';
select json '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '$.fold($1 + $2.fold($1 + $2, 100), 1000)';

-- extension: min/max item methods
select json '1' @* 'strict $.min()';
select json '1' @* 'lax $.min()';
select json '[]' @* '$.min()';
select json '[]' @* '$.max()';
select json '[null]' @* '$.min()';
select json '[null]' @* '$.max()';
select json '[1, 2, 3]' @* '$.min()';
select json '[1, 2, 3]' @* '$.max()';
select json '[2, 3, 5, null, 1, 4, null]' @* '$.min()';
select json '[2, 3, 5, null, 1, 4, null]' @* '$.max()';
select json '["aa", null, "a", "bbb"]' @* '$.min()';
select json '["aa", null, "a", "bbb"]' @* '$.max()';
select json '[1, null, "2"]' @* '$.max()';

-- extension: path sequences
select json '[1,2,3,4,5]' @* '10, 20, $[*], 30';
select json '[1,2,3,4,5]' @* 'lax    10, 20, $[*].a, 30';
select json '[1,2,3,4,5]' @* 'strict 10, 20, $[*].a, 30';
select json '[1,2,3,4,5]' @* '-(10, 20, $[1 to 3], 30)';
select json '[1,2,3,4,5]' @* 'lax (10, 20, $[1 to 3], 30).map(@ + 100)';
select json '[1,2,3,4,5]' @* '$[(0, $[*], 5) ? (@ == 3)]';
select json '[1,2,3,4,5]' @* '$[(0, $[*], 3) ? (@ == 3)]';

-- extension: array constructors
select json '[1, 2, 3]' @* '[]';
select json '[1, 2, 3]' @* '[1, 2, $.map(@ + 100)[*], 4, 5]';
select json '[1, 2, 3]' @* '[1, 2, $.map(@ + 100)[*], 4, 5][*]';
select json '[1, 2, 3]' @* '[(1, (2, $.map(@ + 100)[*])), (4, 5)]';
select json '[1, 2, 3]' @* '[[1, 2], [$.map(@ + 100)[*], 4], 5, [(1,2)?(@ > 5)]]';
select json '[1, 2, 3]' @* 'strict [1, 2, $.map(@.a)[*], 4, 5]';
select json '[[1, 2], [3, 4, 5], [], [6, 7]]' @* '[$[*].map(@ + 10)[*] ? (@ > 13)]';

-- extension: object constructors
select json '[1, 2, 3]' @* '{}';
select json '[1, 2, 3]' @* '{a: 2 + 3, "b": [$[*], 4, 5]}';
select json '[1, 2, 3]' @* '{a: 2 + 3, "b": [$[*], 4, 5]}.*';
select json '[1, 2, 3]' @* '{a: 2 + 3, "b": [$[*], 4, 5]}[*]';
select json '[1, 2, 3]' @* '{a: 2 + 3, "b": ($[*], 4, 5)}';
select json '[1, 2, 3]' @* '{a: 2 + 3, "b": [$.map({x: @, y: @ < 3})[*], {z: "foo"}]}';

-- extension: object subscripting
select json '{"a": 1}' @? '$["a"]';
select json '{"a": 1}' @? '$["b"]';
select json '{"a": 1}' @? 'strict $["b"]';
select json '{"a": 1}' @? '$["b", "a"]';

select json '{"a": 1}' @* '$["a"]';
select json '{"a": 1}' @* 'strict $["b"]';
select json '{"a": 1}' @* 'lax $["b"]';
select json '{"a": 1, "b": 2}' @* 'lax $["b", "c", "b", "a", 0 to 3]';

select json 'null' @* '{"a": 1}["a"]';
select json 'null' @* '{"a": 1}["b"]';

-- extension: outer item reference (@N)
select json '[2,4,1,5,3]' @* '$[*] ? (!exists($[*] ? (@ < @1)))';
select json '[2,4,1,5,3]' @* '$.map(@ + @1[0])';
-- the first @1 and @2 reference array, the second @1 -- current mapped array element
select json '[2,4,1,5,3]' @* '$.map(@ + @1[@1 - @2[2]])';
select json '[[2,4,1,5,3]]' @* '$.map(@.reduce($1 + $2 + @2[0][2] + @1[3]))';

-- extension: including subpaths into result
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a[*].b)';
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a[*]).b';
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.a.([*].b)';
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a)[*].b';
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.a[*].(b)';
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a)[*].(b)';
select json '{"a": [{"b": 1, "c": 10}, {"b": 2, "c": 20}]}' @* '$.(a.[0 to 1].b)';

-- extension: custom operators and type casts
select json '"aaa"' @* '$::text || "bbb"::text || $::text';
select json '"aaa"' @* '$::text || "bbb" || $';
select json '[null, true, 1, "aaa",  {"a": 1}, [1, 2]]' @* '$.map(@::text || "xyz"::text)';

select json '123.45' @* '$::int4';
select json '123.45' @* '$::float4';
select json '123.45' @* '$::text';
select json '123.45' @* '$::text::int4';
select json '123.45' @* '$::text::float4';
select json '123.45' @* '$::text::float4::int4';
select json '4000000000' @* '$::int8';

select json '[123.45, null, 0.67]' @* '$[*]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::text';
select json '[123.45, null, 0.67, "8.9"]' @* '$[*]::text::float4::int4';


select json '[123.45, 0.67]' @* '$[*]::int4 > $[0]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::int4 > $[0]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::int4 > $[1]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::int4 > $[2]::int4';
select json '[123.45, null, 0.67]' @* '$[0]::int4 > $[*]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::int4 > $[2]::text::float4';
select json '[123.45, null, 0.67]' @* '$[*]::text::float4 > $[2]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::text::float4 > $[2]::text::float4';
select json '[123.45, null, 0.67]' @* '$[*]::int4 > $[0 to 1]::int4';
select json '[123.45, null, 0.67]' @* '$[*]::int4 > $[1 to 2]::int4';
select json '[123.45, 100000.2, 10000.67, "1"]' @* '$[0]::int8 > $[*]::int4::int8';

select json '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[*] -> "a"::text';
select json '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[0] -> "a"::text';
select json '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[1] -> $[0].a::text';
select json '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[0] \? "a"::text';
select json '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[*] \? "b"::text';
select json '[{"a": "b"}, {"b": [1, "2"]}]' @* '$[*] \? "c"::text';
select json '[{"a": "b"}, {"b": [1, "2"]}, null, 1]' @* '$[*] ? (@ \? "a"::text)';

select json '[1, "t", 0, "f", null]' @* '$[*] ? (@::int4)';
select json '[1, "t", 0, "f", null]' @* '$[*] ? (@::bool)';
select json '[1, "t", 0, "f", null]' @* '$[*] ? (!(@::bool))';
select json '[1, "t", 0, "f", null]' @* '$[*] ? (@::bool == false::bool)';
select json '[1, "t", 0, "f", null]' @* '$[*] ? (@::bool || !(@::bool))';

select json '[1, 2, 3]' @* '$[*] ? (@::int4 > 1::int4)';

select json '"str"' @* '$::json';
select json '"str"' @* '$::jsonb';
