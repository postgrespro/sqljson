#define JSONPATH_JSON_C

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

#include "utils/jsonpath_json.h"

#define jsonb_path_exists				json_path_exists
#define jsonb_path_exists_opr			json_path_exists_opr
#define jsonb_path_match				json_path_match
#define jsonb_path_match_opr			json_path_match_opr
#define jsonb_path_query				json_path_query
#define jsonb_path_query_novars			json_path_query_novars
#define jsonb_path_query_array			json_path_query_array
#define jsonb_path_query_array_novars	json_path_query_array_novars
#define jsonb_path_query_first			json_path_query_first
#define jsonb_path_query_first_novars	json_path_query_first_novars
#define jsonb_path_query_first_text			json_path_query_first_text
#define jsonb_path_query_first_text_novars	json_path_query_first_text_novars

#include "jsonpath_exec.c"
