select json '{"a": 12}' @? '$';
select json '{"a": 12}' @? '1';
select json '{"a": 12}' @? '$.a.b';
select json '{"a": 12}' @? '$.b';
select json '{"a": 12}' @? '$.a + 2';
select json '{"a": 12}' @? '$.b + 2';
select json '{"a": {"a": 12}}' @? '$.a.a';
select json '{"a": {"a": 12}}' @? '$.*.a';
select json '{"b": {"a": 12}}' @? '$.*.a';
select json '{"b": {"a": 12}}' @? '$.*.b';
select json '{"b": {"a": 12}}' @? 'strict $.*.b';
select json '{}' @? '$.*';
select json '{"a": 1}' @? '$.*';
select json '{"a": {"b": 1}}' @? 'lax $.**{1}';
select json '{"a": {"b": 1}}' @? 'lax $.**{2}';
select json '{"a": {"b": 1}}' @? 'lax $.**{3}';
select json '[]' @? '$[*]';
select json '[1]' @? '$[*]';
select json '[1]' @? '$[1]';
select json '[1]' @? 'strict $[1]';
select json_path_query('[1]', 'strict $[1]');
select json '[1]' @? 'lax $[10000000000000000]';
select json '[1]' @? 'strict $[10000000000000000]';
select json_path_query('[1]', 'lax $[10000000000000000]');
select json_path_query('[1]', 'strict $[10000000000000000]');
select json '[1]' @? '$[0]';
select json '[1]' @? '$[0.3]';
select json '[1]' @? '$[0.5]';
select json '[1]' @? '$[0.9]';
select json '[1]' @? '$[1.2]';
select json '[1]' @? 'strict $[1.2]';
select json_path_query('[1]', 'strict $[1.2]');
select json_path_query('{}', 'strict $[0.3]');
select json '{}' @? 'lax $[0.3]';
select json_path_query('{}', 'strict $[1.2]');
select json '{}' @? 'lax $[1.2]';
select json_path_query('{}', 'strict $[-2 to 3]');
select json '{}' @? 'lax $[-2 to 3]';

select json '{"a": [1,2,3], "b": [3,4,5]}' @? '$ ? (@.a[*] >  @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,5]}' @? '$ ? (@.a[*] >= @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,"5"]}' @? '$ ? (@.a[*] >= @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,"5"]}' @? 'strict $ ? (@.a[*] >= @.b[*])';
select json '{"a": [1,2,3], "b": [3,4,null]}' @? '$ ? (@.a[*] >= @.b[*])';
select json '1' @? '$ ? ((@ == "1") is unknown)';
select json '1' @? '$ ? ((@ == 1) is unknown)';
select json '[{"a": 1}, {"a": 2}]' @? '$[0 to 1] ? (@.a > 1)';

select json_path_query('1', 'lax $.a');
select json_path_query('1', 'strict $.a');
select json_path_query('1', 'strict $.*');
select json_path_query('[]', 'lax $.a');
select json_path_query('[]', 'strict $.a');
select json_path_query('{}', 'lax $.a');
select json_path_query('{}', 'strict $.a');

select json_path_query('1', 'strict $[1]');
select json_path_query('1', 'strict $[*]');
select json_path_query('[]', 'strict $[1]');
select json_path_query('[]', 'strict $["a"]');

select json_path_query('{"a": 12, "b": {"a": 13}}', '$.a');
select json_path_query('{"a": 12, "b": {"a": 13}}', '$.b');
select json_path_query('{"a": 12, "b": {"a": 13}}', '$.*');
select json_path_query('{"a": 12, "b": {"a": 13}}', 'lax $.*.a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[*].a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[*].*');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[0].a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[1].a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[2].a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[0,1].a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[0 to 10].a');
select json_path_query('[12, {"a": 13}, {"b": 14}]', 'lax $[0 to 10 / 0].a');
select json_path_query('[12, {"a": 13}, {"b": 14}, "ccc", true]', '$[2.5 - 1 to $.size() - 2]');
select json_path_query('1', 'lax $[0]');
select json_path_query('1', 'lax $[*]');
select json_path_query('{}', 'lax $[0]');
select json_path_query('[1]', 'lax $[0]');
select json_path_query('[1]', 'lax $[*]');
select json_path_query('[1,2,3]', 'lax $[*]');
select json_path_query('[1,2,3]', 'strict $[*].a');
select json_path_query('[]', '$[last]');
select json_path_query('[]', '$[last ? (exists(last))]');
select json_path_query('[]', 'strict $[last]');
select json_path_query('[1]', '$[last]');
select json_path_query('{}', '$[last]');
select json_path_query('[1,2,3]', '$[last]');
select json_path_query('[1,2,3]', '$[last - 1]');
select json_path_query('[1,2,3]', '$[last ? (@.type() == "number")]');
select json_path_query('[1,2,3]', '$[last ? (@.type() == "string")]');

select * from json_path_query('{"a": 10}', '$');
select * from json_path_query('{"a": 10}', '$ ? (@.a < $value)');
select * from json_path_query('{"a": 10}', '$ ? (@.a < $value)', '1');
select * from json_path_query('{"a": 10}', '$ ? (@.a < $value)', '[{"value" : 13}]');
select * from json_path_query('{"a": 10}', '$ ? (@.a < $value)', '{"value" : 13}');
select * from json_path_query('{"a": 10}', '$ ? (@.a < $value)', '{"value" : 8}');
select * from json_path_query('{"a": 10}', '$.a ? (@ < $value)', '{"value" : 13}');
select * from json_path_query('[10,11,12,13,14,15]', '$[*] ? (@ < $value)', '{"value" : 13}');
select * from json_path_query('[10,11,12,13,14,15]', '$[0,1] ? (@ < $x.value)', '{"x": {"value" : 13}}');
select * from json_path_query('[10,11,12,13,14,15]', '$[0 to 2] ? (@ < $value)', '{"value" : 15}');
select * from json_path_query('[1,"1",2,"2",null]', '$[*] ? (@ == "1")');
select * from json_path_query('[1,"1",2,"2",null]', '$[*] ? (@ == $value)', '{"value" : "1"}');
select * from json_path_query('[1,"1",2,"2",null]', '$[*] ? (@ == $value)', '{"value" : null}');
select * from json_path_query('[1, "2", null]', '$[*] ? (@ != null)');
select * from json_path_query('[1, "2", null]', '$[*] ? (@ == null)');
select * from json_path_query('{}', '$ ? (@ == @)');
select * from json_path_query('[]', 'strict $ ? (@ == @)');

select json_path_query('{"a": {"b": 1}}', 'lax $.**');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{0}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{0 to last}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{1}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{1 to last}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{2}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{2 to last}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{3 to last}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{last}');
select json_path_query('{"a": {"b": 1}}', 'lax $.**.b ? (@ > 0)');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{0}.b ? (@ > 0)');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{1}.b ? (@ > 0)');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{0 to last}.b ? (@ > 0)');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{1 to last}.b ? (@ > 0)');
select json_path_query('{"a": {"b": 1}}', 'lax $.**{1 to 2}.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**{0}.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**{1}.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**{0 to last}.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**{1 to last}.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**{1 to 2}.b ? (@ > 0)');
select json_path_query('{"a": {"c": {"b": 1}}}', 'lax $.**{2 to 3}.b ? (@ > 0)');


select json '{"a": {"b": 1}}' @? '$.**.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{0}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{1}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{0 to last}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{1 to last}.b ? ( @ > 0)';
select json '{"a": {"b": 1}}' @? '$.**{1 to 2}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{0}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{1}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{0 to last}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{1 to last}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{1 to 2}.b ? ( @ > 0)';
select json '{"a": {"c": {"b": 1}}}' @? '$.**{2 to 3}.b ? ( @ > 0)';

select json_path_query('{"g": {"x": 2}}', '$.g ? (exists (@.x))');
select json_path_query('{"g": {"x": 2}}', '$.g ? (exists (@.y))');
select json_path_query('{"g": {"x": 2}}', '$.g ? (exists (@.x ? (@ >= 2) ))');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'lax $.g ? (exists (@.x))');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'lax $.g ? (exists (@.x + "3"))');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'lax $.g ? ((exists (@.x + "3")) is unknown)');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'strict $.g[*] ? (exists (@.x))');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'strict $.g[*] ? ((exists (@.x)) is unknown)');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'strict $.g ? (exists (@[*].x))');
select json_path_query('{"g": [{"x": 2}, {"y": 3}]}', 'strict $.g ? ((exists (@[*].x)) is unknown)');

--test ternary logic
select
	x, y,
	json_path_query(
		'[true, false, null]',
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
	json_path_query(
		'[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true || $y == true) ||
				 @ == false && !($x == true || $y == true) ||
				 @ == null  &&  ($x == true || $y == true) is unknown)',
		json_build_object('x', x, 'y', y)
	) as "x || y"
from
	(values (json 'true'), ('false'), ('"null"')) x(x),
	(values (json 'true'), ('false'), ('"null"')) y(y);

select json '{"a": 1, "b":1}' @? '$ ? (@.a == @.b)';
select json '{"c": {"a": 1, "b":1}}' @? '$ ? (@.a == @.b)';
select json '{"c": {"a": 1, "b":1}}' @? '$.c ? (@.a == @.b)';
select json '{"c": {"a": 1, "b":1}}' @? '$.c ? ($.c.a == @.b)';
select json '{"c": {"a": 1, "b":1}}' @? '$.* ? (@.a == @.b)';
select json '{"a": 1, "b":1}' @? '$.** ? (@.a == @.b)';
select json '{"c": {"a": 1, "b":1}}' @? '$.** ? (@.a == @.b)';

select json_path_query('{"c": {"a": 2, "b":1}}', '$.** ? (@.a == 1 + 1)');
select json_path_query('{"c": {"a": 2, "b":1}}', '$.** ? (@.a == (1 + 1))');
select json_path_query('{"c": {"a": 2, "b":1}}', '$.** ? (@.a == @.b + 1)');
select json_path_query('{"c": {"a": 2, "b":1}}', '$.** ? (@.a == (@.b + 1))');
select json '{"c": {"a": -1, "b":1}}' @? '$.** ? (@.a == - 1)';
select json '{"c": {"a": -1, "b":1}}' @? '$.** ? (@.a == -1)';
select json '{"c": {"a": -1, "b":1}}' @? '$.** ? (@.a == -@.b)';
select json '{"c": {"a": -1, "b":1}}' @? '$.** ? (@.a == - @.b)';
select json '{"c": {"a": 0, "b":1}}' @? '$.** ? (@.a == 1 - @.b)';
select json '{"c": {"a": 2, "b":1}}' @? '$.** ? (@.a == 1 - - @.b)';
select json '{"c": {"a": 0, "b":1}}' @? '$.** ? (@.a == 1 - +@.b)';
select json '[1,2,3]' @? '$ ? (+@[*] > +2)';
select json '[1,2,3]' @? '$ ? (+@[*] > +3)';
select json '[1,2,3]' @? '$ ? (-@[*] < -2)';
select json '[1,2,3]' @? '$ ? (-@[*] < -3)';
select json '1' @? '$ ? ($ > 0)';

-- arithmetic errors
select json_path_query('[1,2,0,3]', '$[*] ? (2 / @ > 0)');
select json_path_query('[1,2,0,3]', '$[*] ? ((2 / @ > 0) is unknown)');
select json_path_query('0', '1 / $');
select json_path_query('0', '1 / $ + 2');
select json_path_query('0', '-(3 + 1 % $)');
select json_path_query('1', '$ + "2"');
select json_path_query('[1, 2]', '3 * $');
select json_path_query('"a"', '-$');
select json_path_query('[1,"2",3]', '+$');
select json '["1",2,0,3]' @? '-$[*]';
select json '[1,"2",0,3]' @? '-$[*]';
select json '["1",2,0,3]' @? 'strict -$[*]';
select json '[1,"2",0,3]' @? 'strict -$[*]';

-- unwrapping of operator arguments in lax mode
select json_path_query('{"a": [2]}', 'lax $.a * 3');
select json_path_query('{"a": [2]}', 'lax $.a + 3');
select json_path_query('{"a": [2, 3, 4]}', 'lax -$.a');
-- should fail
select json_path_query('{"a": [1, 2]}', 'lax $.a * 3');

-- extension: boolean expressions
select json_path_query('2', '$ > 1');
select json_path_query('2', '$ <= 1');
select json_path_query('2', '$ == "2"');
select json '2' @? '$ == "2"';

select json '2' @@ '$ > 1';
select json '2' @@ '$ <= 1';
select json '2' @@ '$ == "2"';
select json '2' @@ '1';
select json '{}' @@ '$';
select json '[]' @@ '$';
select json '[1,2,3]' @@ '$[*]';
select json '[]' @@ '$[*]';
select json_path_match('[[1, true], [2, false]]', 'strict $[*] ? (@[0] > $x) [1]', '{"x": 1}');
select json_path_match('[[1, true], [2, false]]', 'strict $[*] ? (@[0] < $x) [1]', '{"x": 2}');

select json_path_query('[null,1,true,"a",[],{}]', '$.type()');
select json_path_query('[null,1,true,"a",[],{}]', 'lax $.type()');
select json_path_query('[null,1,true,"a",[],{}]', '$[*].type()');
select json_path_query('null', 'null.type()');
select json_path_query('null', 'true.type()');
select json_path_query('null', '123.type()');
select json_path_query('null', '"123".type()');

select json_path_query('{"a": 2}', '($.a - 5).abs() + 10');
select json_path_query('{"a": 2.5}', '-($.a * $.a).floor() % 4.3');
select json_path_query('[1, 2, 3]', '($[*] > 2) ? (@ == true)');
select json_path_query('[1, 2, 3]', '($[*] > 3).type()');
select json_path_query('[1, 2, 3]', '($[*].a > 3).type()');
select json_path_query('[1, 2, 3]', 'strict ($[*].a > 3).type()');

select json_path_query('[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]', 'strict $[*].size()');
select json_path_query('[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]', 'lax $[*].size()');

select json_path_query('[0, 1, -2, -3.4, 5.6]', '$[*].abs()');
select json_path_query('[0, 1, -2, -3.4, 5.6]', '$[*].floor()');
select json_path_query('[0, 1, -2, -3.4, 5.6]', '$[*].ceiling()');
select json_path_query('[0, 1, -2, -3.4, 5.6]', '$[*].ceiling().abs()');
select json_path_query('[0, 1, -2, -3.4, 5.6]', '$[*].ceiling().abs().type()');

select json_path_query('[{},1]', '$[*].keyvalue()');
select json_path_query('{}', '$.keyvalue()');
select json_path_query('{"a": 1, "b": [1, 2], "c": {"a": "bbb"}}', '$.keyvalue()');
select json_path_query('[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', '$[*].keyvalue()');
select json_path_query('[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'strict $.keyvalue()');
select json_path_query('[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'lax $.keyvalue()');
select json_path_query('[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'strict $.keyvalue().a');
select json '{"a": 1, "b": [1, 2]}' @? 'lax $.keyvalue()';
select json '{"a": 1, "b": [1, 2]}' @? 'lax $.keyvalue().key';

select json_path_query('null', '$.double()');
select json_path_query('true', '$.double()');
select json_path_query('[]', '$.double()');
select json_path_query('[]', 'strict $.double()');
select json_path_query('{}', '$.double()');
select json_path_query('1.23', '$.double()');
select json_path_query('"1.23"', '$.double()');
select json_path_query('"1.23aaa"', '$.double()');
select json_path_query('"nan"', '$.double()');
select json_path_query('"NaN"', '$.double()');
select json_path_query('"inf"', '$.double()');
select json_path_query('"-inf"', '$.double()');

select json_path_query('{}', '$.abs()');
select json_path_query('true', '$.floor()');
select json_path_query('"1.2"', '$.ceiling()');

select json_path_query('["", "a", "abc", "abcabc"]', '$[*] ? (@ starts with "abc")');
select json_path_query('["", "a", "abc", "abcabc"]', 'strict $ ? (@[*] starts with "abc")');
select json_path_query('["", "a", "abd", "abdabc"]', 'strict $ ? (@[*] starts with "abc")');
select json_path_query('["abc", "abcabc", null, 1]', 'strict $ ? (@[*] starts with "abc")');
select json_path_query('["abc", "abcabc", null, 1]', 'strict $ ? ((@[*] starts with "abc") is unknown)');
select json_path_query('[[null, 1, "abc", "abcabc"]]', 'lax $ ? (@[*] starts with "abc")');
select json_path_query('[[null, 1, "abd", "abdabc"]]', 'lax $ ? ((@[*] starts with "abc") is unknown)');
select json_path_query('[null, 1, "abd", "abdabc"]', 'lax $[*] ? ((@ starts with "abc") is unknown)');

select json_path_query('[null, 1, "abc", "abd", "aBdC", "abdacb", "adc\nabc", "babc"]', 'lax $[*] ? (@ like_regex "^ab.*c")');
select json_path_query('[null, 1, "abc", "abd", "aBdC", "abdacb", "adc\nabc", "babc"]', 'lax $[*] ? (@ like_regex "^a  b.*  c " flag "ix")');
select json_path_query('[null, 1, "abc", "abd", "aBdC", "abdacb", "adc\nabc", "babc"]', 'lax $[*] ? (@ like_regex "^ab.*c" flag "m")');
select json_path_query('[null, 1, "abc", "abd", "aBdC", "abdacb", "adc\nabc", "babc"]', 'lax $[*] ? (@ like_regex "^ab.*c" flag "s")');

select json_path_query('null', '$.datetime()');
select json_path_query('true', '$.datetime()');
select json_path_query('[]', '$.datetime()');
select json_path_query('[]', 'strict $.datetime()');
select json_path_query('{}', '$.datetime()');
select json_path_query('""', '$.datetime()');
select json_path_query('"12:34"', '$.datetime("aaa")');
select json_path_query('"12:34"', '$.datetime("aaa", 1)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", 1)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", 10000000000000)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", 10000000000000)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", 2147483647)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", 2147483648)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", -2147483647)');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", -2147483648)');
select json_path_query('"aaaa"', '$.datetime("HH24")');

-- Standard extension: UNIX epoch to timestamptz
select json_path_query('0', '$.datetime()');
select json_path_query('0', '$.datetime().type()');
select json_path_query('1490216035.5', '$.datetime()');

select json '"10-03-2017"' @? '$.datetime("dd-mm-yyyy")';
select json_path_query('"10-03-2017"', '$.datetime("dd-mm-yyyy")');
select json_path_query('"10-03-2017"', '$.datetime("dd-mm-yyyy").type()');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy").type()');

select json_path_query('"10-03-2017 12:34"', '       $.datetime("dd-mm-yyyy HH24:MI").type()');
select json_path_query('"10-03-2017 12:34 +05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM").type()');
select json_path_query('"12:34:56"', '$.datetime("HH24:MI:SS").type()');
select json_path_query('"12:34:56 +05:20"', '$.datetime("HH24:MI:SS TZH:TZM").type()');

set time zone '+00';

select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", "+00")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", "+00:12")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", "-00:12:34")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", "UTC")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", 12 * 60)');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", -(5 * 3600 + 12 * 60 + 34))');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", $tz)',
                        json_build_object('tz', extract(timezone from now())));
select json_path_query('"10-03-2017 12:34 +05"', '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select json_path_query('"10-03-2017 12:34 -05"', '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select json_path_query('"10-03-2017 12:34 +05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select json_path_query('"10-03-2017 12:34 -05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select json_path_query('"12:34"', '$.datetime("HH24:MI")');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH")');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", "+00")');
select json_path_query('"12:34 +05"', '$.datetime("HH24:MI TZH")');
select json_path_query('"12:34 -05"', '$.datetime("HH24:MI TZH")');
select json_path_query('"12:34 +05:20"', '$.datetime("HH24:MI TZH:TZM")');
select json_path_query('"12:34 -05:20"', '$.datetime("HH24:MI TZH:TZM")');

set time zone '+10';

select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", "+10")');
select json_path_query('"10-03-2017 12:34"', '$.datetime("dd-mm-yyyy HH24:MI TZH", $tz)',
                        json_build_object('tz', extract(timezone from now())));
select json_path_query('"10-03-2017 12:34 +05"', '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select json_path_query('"10-03-2017 12:34 -05"', '$.datetime("dd-mm-yyyy HH24:MI TZH")');
select json_path_query('"10-03-2017 12:34 +05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select json_path_query('"10-03-2017 12:34 -05:20"', '$.datetime("dd-mm-yyyy HH24:MI TZH:TZM")');
select json_path_query('"12:34"', '$.datetime("HH24:MI")');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH")');
select json_path_query('"12:34"', '$.datetime("HH24:MI TZH", "+10")');
select json_path_query('"12:34 +05"', '$.datetime("HH24:MI TZH")');
select json_path_query('"12:34 -05"', '$.datetime("HH24:MI TZH")');
select json_path_query('"12:34 +05:20"', '$.datetime("HH24:MI TZH:TZM")');
select json_path_query('"12:34 -05:20"', '$.datetime("HH24:MI TZH:TZM")');

set time zone default;

select json_path_query('"2017-03-10"', '$.datetime().type()');
select json_path_query('"2017-03-10"', '$.datetime()');
select json_path_query('"2017-03-10 12:34:56"', '$.datetime().type()');
select json_path_query('"2017-03-10 12:34:56"', '$.datetime()');
select json_path_query('"2017-03-10 12:34:56 +3"', '$.datetime().type()');
select json_path_query('"2017-03-10 12:34:56 +3"', '$.datetime()');
select json_path_query('"2017-03-10 12:34:56 +3:10"', '$.datetime().type()');
select json_path_query('"2017-03-10 12:34:56 +3:10"', '$.datetime()');
select json_path_query('"12:34:56"', '$.datetime().type()');
select json_path_query('"12:34:56"', '$.datetime()');
select json_path_query('"12:34:56 +3"', '$.datetime().type()');
select json_path_query('"12:34:56 +3"', '$.datetime()');
select json_path_query('"12:34:56 +3:10"', '$.datetime().type()');
select json_path_query('"12:34:56 +3:10"', '$.datetime()');

set time zone '+00';

-- date comparison
select json_path_query(
	'["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]',
	'$[*].datetime() ? (@ == "10.03.2017".datetime("dd.mm.yyyy"))');
select json_path_query(
	'["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]',
	'$[*].datetime() ? (@ >= "10.03.2017".datetime("dd.mm.yyyy"))');
select json_path_query(
	'["2017-03-10", "2017-03-11", "2017-03-09", "12:34:56", "01:02:03 +04", "2017-03-10 00:00:00", "2017-03-10 12:34:56", "2017-03-10 01:02:03 +04", "2017-03-10 03:00:00 +03"]',
	'$[*].datetime() ? (@ <  "10.03.2017".datetime("dd.mm.yyyy"))');

-- time comparison
select json_path_query(
	'["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]',
	'$[*].datetime() ? (@ == "12:35".datetime("HH24:MI"))');
select json_path_query(
	'["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]',
	'$[*].datetime() ? (@ >= "12:35".datetime("HH24:MI"))');
select json_path_query(
	'["12:34:00", "12:35:00", "12:36:00", "12:35:00 +00", "12:35:00 +01", "13:35:00 +01", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +01"]',
	'$[*].datetime() ? (@ <  "12:35".datetime("HH24:MI"))');

-- timetz comparison
select json_path_query(
	'["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]',
	'$[*].datetime() ? (@ == "12:35 +1".datetime("HH24:MI TZH"))');
select json_path_query(
	'["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]',
	'$[*].datetime() ? (@ >= "12:35 +1".datetime("HH24:MI TZH"))');
select json_path_query(
	'["12:34:00 +01", "12:35:00 +01", "12:36:00 +01", "12:35:00 +02", "12:35:00 -02", "10:35:00", "11:35:00", "12:35:00", "2017-03-10", "2017-03-10 12:35:00", "2017-03-10 12:35:00 +1"]',
	'$[*].datetime() ? (@ <  "12:35 +1".datetime("HH24:MI TZH"))');

-- timestamp comparison
select json_path_query(
	'["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]',
	'$[*].datetime() ? (@ == "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))');
select json_path_query(
	'["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]',
	'$[*].datetime() ? (@ >= "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))');
select json_path_query(
	'["2017-03-10 12:34:00", "2017-03-10 12:35:00", "2017-03-10 12:36:00", "2017-03-10 12:35:00 +01", "2017-03-10 13:35:00 +01", "2017-03-10 12:35:00 -01", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]',
	'$[*].datetime() ? (@ < "10.03.2017 12:35".datetime("dd.mm.yyyy HH24:MI"))');

-- timestamptz comparison
select json_path_query(
	'["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]',
	'$[*].datetime() ? (@ == "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))');
select json_path_query(
	'["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]',
	'$[*].datetime() ? (@ >= "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))');
select json_path_query(
	'["2017-03-10 12:34:00 +01", "2017-03-10 12:35:00 +01", "2017-03-10 12:36:00 +01", "2017-03-10 12:35:00 +02", "2017-03-10 12:35:00 -02", "2017-03-10 10:35:00", "2017-03-10 11:35:00", "2017-03-10 12:35:00", "2017-03-10", "2017-03-11", "12:34:56", "12:34:56 +01"]',
	'$[*].datetime() ? (@ < "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))');

set time zone default;

-- jsonpath operators

SELECT json_path_query('[{"a": 1}, {"a": 2}]', '$[*]');
SELECT json_path_query('[{"a": 1}, {"a": 2}]', '$[*] ? (@.a > 10)');
SELECT json_path_query('[{"a": 1}, {"a": 2}]', '[$[*].a]');

SELECT json_path_query_array('[{"a": 1}, {"a": 2}, {}]', 'strict $[*].a');
SELECT json_path_query_array('[{"a": 1}, {"a": 2}]', '$[*].a');
SELECT json_path_query_array('[{"a": 1}, {"a": 2}]', '$[*].a ? (@ == 1)');
SELECT json_path_query_array('[{"a": 1}, {"a": 2}]', '$[*].a ? (@ > 10)');
SELECT json_path_query_array('[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}]', '$[*].a ? (@ > $min && @ < $max)', '{"min": 1, "max": 4}');
SELECT json_path_query_array('[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}]', '$[*].a ? (@ > $min && @ < $max)', '{"min": 3, "max": 4}');
SELECT json_path_query_array('[{"a": 1}, {"a": 2}]', '[$[*].a]');

SELECT json_path_query_first('[{"a": 1}, {"a": 2}, {}]', 'strict $[*].a');
SELECT json_path_query_first('[{"a": 1}, {"a": 2}]', '$[*].a');
SELECT json_path_query_first('[{"a": 1}, {"a": 2}]', '$[*].a ? (@ == 1)');
SELECT json_path_query_first('[{"a": 1}, {"a": 2}]', '$[*].a ? (@ > 10)');
SELECT json_path_query_first('[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}]', '$[*].a ? (@ > $min && @ < $max)', '{"min": 1, "max": 4}');
SELECT json_path_query_first('[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}]', '$[*].a ? (@ > $min && @ < $max)', '{"min": 3, "max": 4}');

SELECT json '[{"a": 1}, {"a": 2}]' @? '$[*].a ? (@ > 1)';
SELECT json '[{"a": 1}, {"a": 2}]' @? '$[*] ? (@.a > 2)';
SELECT json_path_exists('[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}]', '$[*] ? (@.a > $min && @.a < $max)', '{"min": 1, "max": 4}');
SELECT json_path_exists('[{"a": 1}, {"a": 2}, {"a": 3}, {"a": 5}]', '$[*] ? (@.a > $min && @.a < $max)', '{"min": 3, "max": 4}');

SELECT json '[{"a": 1}, {"a": 2}]' @@ '$[*].a > 1';
SELECT json '[{"a": 1}, {"a": 2}]' @@ '$[*].a > 2';

-- extension: path sequences
select json_path_query('[1,2,3,4,5]', '10, 20, $[*], 30');
select json_path_query('[1,2,3,4,5]', 'lax    10, 20, $[*].a, 30');
select json_path_query('[1,2,3,4,5]', 'strict 10, 20, $[*].a, 30');
select json_path_query('[1,2,3,4,5]', '-(10, 20, $[1 to 3], 30)');
select json_path_query('[1,2,3,4,5]', 'lax (10, 20.5, $[1 to 3], "30").double()');
select json_path_query('[1,2,3,4,5]', '$[(0, $[*], 5) ? (@ == 3)]');
select json_path_query('[1,2,3,4,5]', '$[(0, $[*], 3) ? (@ == 3)]');

-- extension: array constructors
select json_path_query('[1, 2, 3]', '[]');
select json_path_query('[1, 2, 3]', '[1, 2, $[*], 4, 5]');
select json_path_query('[1, 2, 3]', '[1, 2, $[*], 4, 5][*]');
select json_path_query('[1, 2, 3]', '[(1, (2, $[*])), (4, 5)]');
select json_path_query('[1, 2, 3]', '[[1, 2], [$[*], 4], 5, [(1,2)?(@ > 5)]]');
select json_path_query('[1, 2, 3]', 'strict [1, 2, $[*].a, 4, 5]');
select json_path_query('[[1, 2], [3, 4, 5], [], [6, 7]]', '[$[*][*] ? (@ > 3)]');

-- extension: object constructors
select json_path_query('[1, 2, 3]', '{}');
select json_path_query('[1, 2, 3]', '{a: 2 + 3, "b": [$[*], 4, 5]}');
select json_path_query('[1, 2, 3]', '{a: 2 + 3, "b": [$[*], 4, 5]}.*');
select json_path_query('[1, 2, 3]', '{a: 2 + 3, "b": [$[*], 4, 5]}[*]');
select json_path_query('[1, 2, 3]', '{a: 2 + 3, "b": ($[*], 4, 5)}');
select json_path_query('[1, 2, 3]', '{a: 2 + 3, "b": {x: $, y: $[1] > 2, z: "foo"}}');

-- extension: object subscripting
select json '{"a": 1}' @? '$["a"]';
select json '{"a": 1}' @? '$["b"]';
select json '{"a": 1}' @? 'strict $["b"]';
select json '{"a": 1}' @? '$["b", "a"]';

select json_path_query('{"a": 1}', '$["a"]');
select json_path_query('{"a": 1}', 'strict $["b"]');
select json_path_query('{"a": 1}', 'lax $["b"]');
select json_path_query('{"a": 1, "b": 2}', 'lax $["b", "c", "b", "a", 0 to 3]');

select json_path_query('null', '{"a": 1}["a"]');
select json_path_query('null', '{"a": 1}["b"]');
