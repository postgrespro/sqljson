select _jsonpath_exists(jsonb '{"a": 12}', '$.a.b');
select _jsonpath_exists(jsonb '{"a": 12}', '$.b');
select _jsonpath_exists(jsonb '{"a": {"a": 12}}', '$.a.a');
select _jsonpath_exists(jsonb '{"a": {"a": 12}}', '$.*.a');
select _jsonpath_exists(jsonb '{"b": {"a": 12}}', '$.*.a');
select _jsonpath_exists(jsonb '{}', '$.*');
select _jsonpath_exists(jsonb '{"a": 1}', '$.*');
select _jsonpath_exists(jsonb '[]', '$.[*]');
select _jsonpath_exists(jsonb '[1]', '$.[*]');

select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.a');
select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.b');
select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.*');
select * from _jsonpath_query(jsonb '{"a": 12, "b": {"a": 13}}', '$.*.a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', '$.[*].a');
select * from _jsonpath_query(jsonb '[12, {"a": 13}, {"b": 14}]', '$.[*].*');
