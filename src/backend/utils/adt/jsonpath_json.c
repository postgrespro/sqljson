#define JSONPATH_JSON_C

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

/* redefine jsonb structures */
#define Jsonb Json
#define JsonbContainer JsonContainer
#define JsonbIterator JsonIterator

/* redefine jsonb functions */
#define findJsonbValueFromContainer(jc, flags, jbv) \
		findJsonValueFromContainer((JsonContainer *)(jc), flags, jbv)
#define getIthJsonbValueFromContainer(jc, i) \
		getIthJsonValueFromContainer((JsonContainer *)(jc), i)
#define pushJsonbValue pushJsonValue
#define JsonbIteratorInit(jc) JsonIteratorInit((JsonContainer *)(jc))
#define JsonbIteratorNext JsonIteratorNext
#define JsonbValueToJsonb JsonbValueToJson
#define JsonbToCString JsonToCString
#define JsonbUnquote JsonUnquote
#define JsonbExtractScalar(jc, jbv) JsonExtractScalar((JsonContainer *)(jc), jbv)

/* redefine jsonb macros */
#undef JsonContainerSize
#define JsonContainerSize(jc) \
	((((JsonContainer *)(jc))->header & JB_CMASK) == JB_CMASK && \
	 JsonContainerIsArray(jc) \
		? JsonGetArraySize((JsonContainer *)(jc)) \
		: ((JsonContainer *)(jc))->header & JB_CMASK)


#undef DatumGetJsonbP
#define DatumGetJsonbP(d)	DatumGetJsonP(d)

#undef DatumGetJsonbPCopy
#define DatumGetJsonbPCopy(d)	DatumGetJsonPCopy(d)

#undef JsonbPGetDatum
#define JsonbPGetDatum(json)	JsonPGetDatum(json)

#undef PG_GETARG_JSONB_P
#define PG_GETARG_JSONB_P(n)	DatumGetJsonP(PG_GETARG_DATUM(n))

#undef PG_GETARG_JSONB_P_COPY
#define PG_GETARG_JSONB_P_COPY(n)	DatumGetJsonPCopy(PG_GETARG_DATUM(n))


#ifdef DatumGetJsonb
#undef DatumGetJsonb
#define DatumGetJsonb(d)	DatumGetJsonbP(d)
#endif

#ifdef DatumGetJsonbCopy
#undef DatumGetJsonbCopy
#define DatumGetJsonbCopy(d)	DatumGetJsonbPCopy(d)
#endif

#ifdef JsonbGetDatum
#undef JsonbGetDatum
#define JsonbGetDatum(json)	JsonbPGetDatum(json)
#endif

#ifdef PG_GETARG_JSONB
#undef PG_GETARG_JSONB
#define PG_GETARG_JSONB(n)	PG_GETARG_JSONB_P(n)
#endif

#ifdef PG_GETARG_JSONB_COPY
#undef PG_GETARG_JSONB_COPY
#define PG_GETARG_JSONB_COPY(n)	PG_GETARG_JSONB_P_COPY(n)
#endif

/* redefine global jsonpath functions */
#define executeJsonPath		executeJsonPathJson
#define JsonbPathExists		JsonPathExists
#define JsonbPathQuery		JsonPathQuery
#define JsonbPathValue		JsonPathValue
#define JsonbTableRoutine	JsonTableRoutine

#define jsonb_jsonpath_exists2		json_jsonpath_exists2
#define jsonb_jsonpath_exists3		json_jsonpath_exists3
#define jsonb_jsonpath_predicate2	json_jsonpath_predicate2
#define jsonb_jsonpath_predicate3	json_jsonpath_predicate3
#define jsonb_jsonpath_query2		json_jsonpath_query2
#define jsonb_jsonpath_query3		json_jsonpath_query3
#define jsonb_jsonpath_query_safe2	json_jsonpath_query_safe2
#define jsonb_jsonpath_query_safe3	json_jsonpath_query_safe3

static inline JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Json *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = (void *) &jb->root;
	jbv->val.binary.len = jb->root.len;

	return jbv;
}

#include "jsonpath_exec.c"
