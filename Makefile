# contrib/pg_tm_aux/Makefile

MODULE_big	= pg_tm_aux
OBJS = \
	$(WIN32RES) \
	pg_tm_aux.o

EXTENSION = pg_tm_aux
DATA = pg_tm_aux--1.0.sql pg_tm_aux--1.0--1.1.sql
PGFILEDESC = "pg_tm_aux - transfer manager auxilary functions"

REGRESS = check

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tm_aux
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
