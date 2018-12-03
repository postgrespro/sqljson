#define JSONPATH_JSON_C

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

#include "utils/jsonpath_json.h"

#define jsonb_jsonpath_exists2		json_jsonpath_exists2
#define jsonb_jsonpath_exists3		json_jsonpath_exists3
#define jsonb_jsonpath_predicate2	json_jsonpath_predicate2
#define jsonb_jsonpath_predicate3	json_jsonpath_predicate3
#define jsonb_jsonpath_query2		json_jsonpath_query2
#define jsonb_jsonpath_query3		json_jsonpath_query3
#define jsonb_jsonpath_query_wrapped2	json_jsonpath_query_wrapped2
#define jsonb_jsonpath_query_wrapped3	json_jsonpath_query_wrapped3

#include "jsonpath_exec.c"
