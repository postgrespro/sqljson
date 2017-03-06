select _jsonpath_exists(jsonb '{"a": 12}', '$.a.b');
select _jsonpath_exists(jsonb '{"a": 12}', '$.b');
select _jsonpath_exists(jsonb '{"a": {"a": 12}}', '$.a.a');
select _jsonpath_exists(jsonb '{"a": {"a": 12}}', '$.*.a');
select _jsonpath_exists(jsonb '{"b": {"a": 12}}', '$.*.a');
select _jsonpath_exists(jsonb '{}', '$.*');
select _jsonpath_exists(jsonb '{"a": 1}', '$.*');
select _jsonpath_exists(jsonb '{"a": {"b": 1}}', 'lax $.**{1}');
select _jsonpath_exists(jsonb '{"a": {"b": 1}}', 'lax $.**{2}');
select _jsonpath_exists(jsonb '{"a": {"b": 1}}', 'lax $.**{3}');

select _jsonpath_exists(jsonb '[]', '$.[*]');
select _jsonpath_exists(jsonb '[1]', '$.[*]');
select _jsonpath_exists(jsonb '[1]', '$.[1]');
select _jsonpath_exists(jsonb '[1]', '$.[0]');
select _jsonpath_exists(jsonb '{"a": [1,2,3], "b": [3,4,5]}', '$ ? (@.a[*] >  @.b[*])');
select _jsonpath_exists(jsonb '{"a": [1,2,3], "b": [3,4,5]}', '$ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(jsonb '{"a": [1,2,3], "b": [3,4,"5"]}', 'lax $ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(jsonb '{"a": [1,2,3], "b": [3,4,"5"]}', 'strict $ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(jsonb '{"a": [1,2,3], "b": [3,4,null]}', '$ ? (@.a[*] >= @.b[*])');
select _jsonpath_exists(jsonb '1', '$ ? ((@ == "1") is unknown)');
select _jsonpath_exists(jsonb '1', '$ ? ((@ == 1) is unknown)');

select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.a');
select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.b');
select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.*');
select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', 'lax $.*.a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[*].a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[*].*');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[0].a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[1].a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[2].a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[0,1].a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', 'lax $.[0 to 10].a');
select * from _jsonpath_query(jsonb '1', 'lax $[0]');
select * from _jsonpath_query(jsonb '1', 'lax $[*]');
select * from _jsonpath_query(jsonb '[1]', 'lax $[0]');
select * from _jsonpath_query(jsonb '[1]', 'lax $[*]');
select * from _jsonpath_query(jsonb '[1,2,3]', 'lax $[*]');

select * from _jsonpath_query(jsonb '{"a": 10}', '$');
select * from _jsonpath_query(jsonb '{"a": 10}', '$ ? (.a < $value)');
select * from _jsonpath_query(jsonb '{"a": 10}', '$ ? (.a < $value)', '{"value" : 13}');
select * from _jsonpath_query(jsonb '{"a": 10}', '$ ? (.a < $value)', '{"value" : 8}');
select * from _jsonpath_query(jsonb '{"a": 10}', '$.a ? (@ < $value)', '{"value" : 13}');
select * from _jsonpath_query(jsonb '[10,11,12,13,14,15]', '$.[*] ? (@ < $value)', '{"value" : 13}');
select * from _jsonpath_query(jsonb '[10,11,12,13,14,15]', '$.[0,1] ? (@ < $value)', '{"value" : 13}');
select * from _jsonpath_query(jsonb '[10,11,12,13,14,15]', '$.[0 to 2] ? (@ < $value)', '{"value" : 15}');
select * from _jsonpath_query(jsonb '[1,"1",2,"2",null]', '$.[*] ? (@ == "1")');
select * from _jsonpath_query(jsonb '[1,"1",2,"2",null]', '$.[*] ? (@ == $value)', '{"value" : "1"}');
select * from _jsonpath_query(jsonb '[1, "2", null]', '$[*] ? (@ != null)');
select * from _jsonpath_query(jsonb '[1, "2", null]', '$[*] ? (@ == null)');

select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{1}');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{1,}');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{2}');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{2,}');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{3,}');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{0}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{1}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{0,}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{1,}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"b": 1}}', 'lax $.**{1,2}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**{0}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**{1}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**{0,}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**{1,}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**{1,2}.b ? (@ > 0)');
select * from _jsonpath_query(jsonb '{"a": {"c": {"b": 1}}}', 'lax $.**{2,3}.b ? (@ > 0)');

select * from _jsonpath_exists(jsonb '{"a": {"b": 1}}', '$.**.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"b": 1}}', '$.**{0}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"b": 1}}', '$.**{1}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"b": 1}}', '$.**{0,}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"b": 1}}', '$.**{1,}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"b": 1}}', '$.**{1,2}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**{0}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**{1}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**{0,}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**{1,}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**{1,2}.b ? (@ > 0)');
select * from _jsonpath_exists(jsonb '{"a": {"c": {"b": 1}}}', '$.**{2,3}.b ? (@ > 0)');

select _jsonpath_query(jsonb '{"g": {"x": 2}}', '$.g ? (exists (@.x))');
select _jsonpath_query(jsonb '{"g": {"x": 2}}', '$.g ? (exists (@.y))');
select _jsonpath_query(jsonb '{"g": {"x": 2}}', '$.g ? (exists (@.x ? (@ >= 2) ))');

--test ternary logic
select
	x, y,
	_jsonpath_query(
		jsonb '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true && $y == true) ||
				 @ == false && !($x == true && $y == true) ||
				 @ == null  &&  ($x == true && $y == true) is unknown)',
		jsonb_build_object('x', x, 'y', y)
	) as "x && y"
from
	(values (jsonb 'true'), ('false'), ('"null"')) x(x),
	(values (jsonb 'true'), ('false'), ('"null"')) y(y);

select
	x, y,
	_jsonpath_query(
		jsonb '[true, false, null]',
		'$[*] ? (@ == true  &&  ($x == true || $y == true) ||
				 @ == false && !($x == true || $y == true) ||
				 @ == null  &&  ($x == true || $y == true) is unknown)',
		jsonb_build_object('x', x, 'y', y)
	) as "x || y"
from
	(values (jsonb 'true'), ('false'), ('"null"')) x(x),
	(values (jsonb 'true'), ('false'), ('"null"')) y(y);

select _jsonpath_exists(jsonb '{"a": 1, "b":1}', '$ ? (.a == .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 1, "b":1}}', '$ ? (.a == .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 1, "b":1}}', '$.c ? (.a == .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 1, "b":1}}', '$.* ? (.a == .b)');
select _jsonpath_exists(jsonb '{"a": 1, "b":1}', '$.** ? (.a == .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 1, "b":1}}', '$.** ? (.a == .b)');

select _jsonpath_query(jsonb '{"c": {"a": 2, "b":1}}', '$.** ? (.a == 1 + 1)');
select _jsonpath_query(jsonb '{"c": {"a": 2, "b":1}}', '$.** ? (.a == (1 + 1))');
select _jsonpath_query(jsonb '{"c": {"a": 2, "b":1}}', '$.** ? (.a == .b + 1)');
select _jsonpath_query(jsonb '{"c": {"a": 2, "b":1}}', '$.** ? (.a == (.b + 1))');
select _jsonpath_exists(jsonb '{"c": {"a": -1, "b":1}}', '$.** ? (.a == - 1)');
select _jsonpath_exists(jsonb '{"c": {"a": -1, "b":1}}', '$.** ? (.a == -1)');
select _jsonpath_exists(jsonb '{"c": {"a": -1, "b":1}}', '$.** ? (.a == -.b)');
select _jsonpath_exists(jsonb '{"c": {"a": -1, "b":1}}', '$.** ? (.a == - .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 0, "b":1}}', '$.** ? (.a == 1 - .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 2, "b":1}}', '$.** ? (.a == 1 - - .b)');
select _jsonpath_exists(jsonb '{"c": {"a": 0, "b":1}}', '$.** ? (.a == 1 - +.b)');
select _jsonpath_exists(jsonb '[1,2,3]', '$ ? (+@[*] > +2)');
select _jsonpath_exists(jsonb '[1,2,3]', '$ ? (+@[*] > +3)');
select _jsonpath_exists(jsonb '[1,2,3]', '$ ? (-@[*] < -2)');
select _jsonpath_exists(jsonb '[1,2,3]', '$ ? (-@[*] < -3)');

-- unwrapping of operator arguments in lax mode
select _jsonpath_query(jsonb '{"a": [2]}', 'lax $.a * 3');
select _jsonpath_query(jsonb '{"a": [2]}', 'lax $.a + 3');
select _jsonpath_query(jsonb '{"a": [2, 3, 4]}', 'lax -$.a');
-- should fail
select _jsonpath_query(jsonb '{"a": [1, 2]}', 'lax $.a * 3');

select _jsonpath_query(jsonb '[null,1,true,"a",[],{}]', '$.type()');
select _jsonpath_query(jsonb '[null,1,true,"a",[],{}]', 'lax $.type()');
select _jsonpath_query(jsonb '[null,1,true,"a",[],{}]', '$[*].type()');

select _jsonpath_query(jsonb '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]', 'strict $[*].size()');
select _jsonpath_query(jsonb '[1,null,true,"11",[],[1],[1,2,3],{},{"a":1,"b":2}]', 'lax $[*].size()');

select _jsonpath_query(jsonb '[0, 1, -2, -3.4, 5.6]', '$[*].abs()');
select _jsonpath_query(jsonb '[0, 1, -2, -3.4, 5.6]', '$[*].floor()');
select _jsonpath_query(jsonb '[0, 1, -2, -3.4, 5.6]', '$[*].ceiling()');
select _jsonpath_query(jsonb '[0, 1, -2, -3.4, 5.6]', '$[*].ceiling().abs()');
select _jsonpath_query(jsonb '[0, 1, -2, -3.4, 5.6]', '$[*].ceiling().abs().type()');

select _jsonpath_query(jsonb '[{},1]', '$[*].keyvalue()');
select _jsonpath_query(jsonb '{}', '$.keyvalue()');
select _jsonpath_query(jsonb '{"a": 1, "b": [1, 2], "c": {"a": "bbb"}}', '$.keyvalue()');
select _jsonpath_query(jsonb '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', '$[*].keyvalue()');
select _jsonpath_query(jsonb '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'strict $.keyvalue()');
select _jsonpath_query(jsonb '[{"a": 1, "b": [1, 2]}, {"c": {"a": "bbb"}}]', 'lax $.keyvalue()');

select _jsonpath_query(jsonb 'null', '$.double()');
select _jsonpath_query(jsonb 'true', '$.double()');
select _jsonpath_query(jsonb '[]', '$.double()');
select _jsonpath_query(jsonb '[]', 'strict $.double()');
select _jsonpath_query(jsonb '{}', '$.double()');
select _jsonpath_query(jsonb '1.23', '$.double()');
select _jsonpath_query(jsonb '"1.23"', '$.double()');
select _jsonpath_query(jsonb '"1.23aaa"', '$.double()');
