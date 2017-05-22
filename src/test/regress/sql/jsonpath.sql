--jsonpath io

select ''::jsonpath;
select '$'::jsonpath;
select 'strict $'::jsonpath;
select 'lax $'::jsonpath;
select '$.a'::jsonpath;
select '$.a.v'::jsonpath;
select '$.a.*'::jsonpath;
select '$.*.[*]'::jsonpath;
select '$.*[*]'::jsonpath;
select '$.a.[*]'::jsonpath;
select '$.a[*]'::jsonpath;
select '$.a.[*][*]'::jsonpath;
select '$.a.[*].[*]'::jsonpath;
select '$.a[*][*]'::jsonpath;
select '$.a[*].[*]'::jsonpath;
select '$[*]'::jsonpath;
select '$[0]'::jsonpath;
select '$[*][0]'::jsonpath;
select '$[*].a'::jsonpath;
select '$[*][0].a.b'::jsonpath;
select '$.a.**.b'::jsonpath;
select '$.a.**{2}.b'::jsonpath;
select '$.a.**{2,2}.b'::jsonpath;
select '$.a.**{2,5}.b'::jsonpath;
select '$.a.**{,5}.b'::jsonpath;
select '$.a.**{5,}.b'::jsonpath;
select '$+1'::jsonpath;
select '$-1'::jsonpath;
select '$--+1'::jsonpath;
select '$.a/+-1'::jsonpath;

select '$.g ? ($.a == 1)'::jsonpath;
select '$.g ? (@ == 1)'::jsonpath;
select '$.g ? (.a == 1)'::jsonpath;
select '$.g ? (@.a == 1)'::jsonpath;
select '$.g ? (@.a == 1 || @.a == 4)'::jsonpath;
select '$.g ? (@.a == 1 && @.a == 4)'::jsonpath;
select '$.g ? (@.a == 1 || @.a == 4 && @.b == 7)'::jsonpath;
select '$.g ? (@.a == 1 || !(@.a == 4) && @.b == 7)'::jsonpath;
select '$.g ? (@.a == 1 || !(@.x >= 123 || @.a == 4) && @.b == 7)'::jsonpath;
select '$.g ? (.x >= @[*]?(@.a > "abc"))'::jsonpath;
select '$.g ? ((@.x >= 123 || @.a == 4) is unknown)'::jsonpath;
select '$.g ? (exists (.x))'::jsonpath;
select '$.g ? (exists (@.x ? (@ == 14)))'::jsonpath;
select '$.g ? (exists (.x ? (@ == 14)))'::jsonpath;
select '$.g ? ((@.x >= 123 || @.a == 4) && exists (.x ? (@ == 14)))'::jsonpath;
select '$.g ? (+@.x >= +-(+@.a + 2))'::jsonpath;

select '$a'::jsonpath;
select '$a.b'::jsonpath;
select '$a[*]'::jsonpath;
select '$.g ? (@.zip == $zip)'::jsonpath;
select '$.a.[1,2, 3 to 16]'::jsonpath;
select '$.a[1,2, 3 to 16]'::jsonpath;
select '$.a[$a + 1, ($b[*]) to -(@[0] * 2)]'::jsonpath;
select '$.a[$.a.size() - 3]'::jsonpath;
select 'last'::jsonpath;
select '"last"'::jsonpath;
select '$.last'::jsonpath;
select '$ ? (last > 0)'::jsonpath;
select '$[last]'::jsonpath;
select '$[@ ? (last > 0)]'::jsonpath;

select 'null.type()'::jsonpath;
select '1.type()'::jsonpath;
select '"aaa".type()'::jsonpath;
select 'true.type()'::jsonpath;
select '$.datetime()'::jsonpath;
select '$.datetime("datetime template")'::jsonpath;
select '$.reduce($1 + $2 + @[1])'::jsonpath;
select '$.fold($1 + $2 + @[1], 2 + 3)'::jsonpath;
select '$.min().abs() + 5'::jsonpath;
select '$.max().floor()'::jsonpath;

select '$ ? (@ starts with "abc")'::jsonpath;
select '$ ? (@ starts with $var)'::jsonpath;

select '$ ? (@ like_regex "pattern")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "i")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "is")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "isim")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "xsms")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "a")'::jsonpath;

select '$ < 1'::jsonpath;
select '($ < 1) || $.a.b <= $x'::jsonpath;
select '@ + 1'::jsonpath;

select '($).a.b'::jsonpath;
select '($.a.b).c.d'::jsonpath;
select '($.a.b + -$.x.y).c.d'::jsonpath;
select '(-+$.a.b).c.d'::jsonpath;
select '1 + ($.a.b + 2).c.d'::jsonpath;
select '1 + ($.a.b > 2).c.d'::jsonpath;

select '1, 2 + 3, $.a[*] + 5'::jsonpath;
select '(1, 2, $.a)'::jsonpath;
select '(1, 2, $.a).a[*]'::jsonpath;
select '(1, 2, $.a) == 5'::jsonpath;
select '$[(1, 2, $.a) to (3, 4)]'::jsonpath;
select '$[(1, (2, $.a)), 3, (4, 5)]'::jsonpath;

select '[]'::jsonpath;
select '[[1, 2], ([(3, 4, 5), 6], []), $.a[*]]'::jsonpath;

select '{}'::jsonpath;
select '{a: 1 + 2}'::jsonpath;
select '{a: 1 + 2, b : (1,2), c: [$[*],4,5], d: { "e e e": "f f f" }}'::jsonpath;

select '$ ? (@.a < 1)'::jsonpath;
select '$ ? (@.a < -1)'::jsonpath;
select '$ ? (@.a < +1)'::jsonpath;
select '$ ? (@.a < .1)'::jsonpath;
select '$ ? (@.a < -.1)'::jsonpath;
select '$ ? (@.a < +.1)'::jsonpath;
select '$ ? (@.a < 0.1)'::jsonpath;
select '$ ? (@.a < -0.1)'::jsonpath;
select '$ ? (@.a < +0.1)'::jsonpath;
select '$ ? (@.a < 10.1)'::jsonpath;
select '$ ? (@.a < -10.1)'::jsonpath;
select '$ ? (@.a < +10.1)'::jsonpath;
select '$ ? (@.a < 1e1)'::jsonpath;
select '$ ? (@.a < -1e1)'::jsonpath;
select '$ ? (@.a < +1e1)'::jsonpath;
select '$ ? (@.a < .1e1)'::jsonpath;
select '$ ? (@.a < -.1e1)'::jsonpath;
select '$ ? (@.a < +.1e1)'::jsonpath;
select '$ ? (@.a < 0.1e1)'::jsonpath;
select '$ ? (@.a < -0.1e1)'::jsonpath;
select '$ ? (@.a < +0.1e1)'::jsonpath;
select '$ ? (@.a < 10.1e1)'::jsonpath;
select '$ ? (@.a < -10.1e1)'::jsonpath;
select '$ ? (@.a < +10.1e1)'::jsonpath;
select '$ ? (@.a < 1e-1)'::jsonpath;
select '$ ? (@.a < -1e-1)'::jsonpath;
select '$ ? (@.a < +1e-1)'::jsonpath;
select '$ ? (@.a < .1e-1)'::jsonpath;
select '$ ? (@.a < -.1e-1)'::jsonpath;
select '$ ? (@.a < +.1e-1)'::jsonpath;
select '$ ? (@.a < 0.1e-1)'::jsonpath;
select '$ ? (@.a < -0.1e-1)'::jsonpath;
select '$ ? (@.a < +0.1e-1)'::jsonpath;
select '$ ? (@.a < 10.1e-1)'::jsonpath;
select '$ ? (@.a < -10.1e-1)'::jsonpath;
select '$ ? (@.a < +10.1e-1)'::jsonpath;
select '$ ? (@.a < 1e+1)'::jsonpath;
select '$ ? (@.a < -1e+1)'::jsonpath;
select '$ ? (@.a < +1e+1)'::jsonpath;
select '$ ? (@.a < .1e+1)'::jsonpath;
select '$ ? (@.a < -.1e+1)'::jsonpath;
select '$ ? (@.a < +.1e+1)'::jsonpath;
select '$ ? (@.a < 0.1e+1)'::jsonpath;
select '$ ? (@.a < -0.1e+1)'::jsonpath;
select '$ ? (@.a < +0.1e+1)'::jsonpath;
select '$ ? (@.a < 10.1e+1)'::jsonpath;
select '$ ? (@.a < -10.1e+1)'::jsonpath;
select '$ ? (@.a < +10.1e+1)'::jsonpath;

select '@1'::jsonpath;
select '@-1'::jsonpath;
select '$ ? (@0 > 1)'::jsonpath;
select '$ ? (@1 > 1)'::jsonpath;
select '$.a ? (@.b ? (@1 > @) > 5)'::jsonpath;
select '$.a ? (@.b ? (@2 > @) > 5)'::jsonpath;

-- jsonpath combination operators

select jsonpath '$.a' == jsonpath '$[*] + 1';
-- should fail
select jsonpath '$.a' == jsonpath '$.b == 1';
--select jsonpath '$.a' != jsonpath '$[*] + 1';
select jsonpath '$.a' >  jsonpath '$[*] + 1';
select jsonpath '$.a' <  jsonpath '$[*] + 1';
select jsonpath '$.a' >= jsonpath '$[*] + 1';
select jsonpath '$.a' <= jsonpath '$[*] + 1';
select jsonpath '$.a' +  jsonpath '$[*] + 1';
select jsonpath '$.a' -  jsonpath '$[*] + 1';
select jsonpath '$.a' *  jsonpath '$[*] + 1';
select jsonpath '$.a' /  jsonpath '$[*] + 1';
select jsonpath '$.a' %  jsonpath '$[*] + 1';

select jsonpath '$.a' == jsonb '"aaa"';
--select jsonpath '$.a' != jsonb '1';
select jsonpath '$.a' >   jsonb '12.34';
select jsonpath '$.a' <   jsonb '"aaa"';
select jsonpath '$.a' >=  jsonb 'true';
select jsonpath '$.a' <=  jsonb 'false';
select jsonpath '$.a' +   jsonb 'null';
select jsonpath '$.a' -   jsonb '12.3';
select jsonpath '$.a' *   jsonb '5';
select jsonpath '$.a' /   jsonb '0';
select jsonpath '$.a' %   jsonb '"1.23"';
select jsonpath '$.a' ==  jsonb '[]';
select jsonpath '$.a' >=  jsonb '[1, "2", true, null, [], {"a": [1], "b": 3}]';
select jsonpath '$.a' +   jsonb '{}';
select jsonpath '$.a' /   jsonb '{"a": 1, "b": [1, {}], "c": {}, "d": {"e": true, "f": {"g": "abc"}}}';


select jsonpath '$' -> 'a';
select jsonpath '$' -> 1;
select jsonpath '$' -> 'a' -> 1;
select jsonpath '$.a' ? jsonpath '$.x ? (@.y ? (@ > 3 + @1.b + $) == $) > $.z';

select jsonpath '$.a.b[(@[*]?(@ > @1).c + 1.23).**{2,5}].map({a: @, b: [$.x, [], @ % 5]})' ?
       jsonpath '$.**[@.size() + 3].map(@ + $?(@ > @1.reduce($1 + $2 * @ - $) / $)) > true';

select jsonpath '$.a + $a' @ jsonb '"aaa"';
select jsonpath '$.a + $a' @ jsonb '{"b": "abc"}';
select jsonpath '$.a + $a' @ jsonb '{"a": "abc"}';
select jsonpath '$.a + $a.double()' @ jsonb '{"a": "abc"}';
select jsonpath '$.a + $a.x.double()' @ jsonb '{"a": {"x": -12.34}}';
select jsonpath '$[*] ? (@ > $min && @ <= $max)' @ jsonb '{"min": -1.23, "max": 5.0}';
select jsonpath '$[*] ? (@ > $min && @ <= $max)' @ jsonb '{"min": -1.23}' @ jsonb '{"max": 5.0}';
