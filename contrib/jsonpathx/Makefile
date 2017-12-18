# contrib/jsonpathx/Makefile

MODULE_big = jsonpathx
OBJS = jsonpathx.o

EXTENSION = jsonpathx
DATA = jsonpathx--1.0.sql
PGFILEDESC = "jsonpathx - extended JSON path item methods"

REGRESS = jsonpathx_jsonb jsonpathx_json

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/jsonpathx
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

