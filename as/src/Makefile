# Aerospike Server
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

ifeq ($(USE_EE),1)
  include $(EEREPO)/as/make_in/Makefile.vars
endif

BASE_HEADERS += aggr.h asm.h cfg.h cluster_config.h datamodel.h feature.h index.h job_manager.h json_init.h
BASE_HEADERS += ldt.h ldt_aerospike.h ldt_record.h monitor.h packet_compression.h
BASE_HEADERS += proto.h rec_props.h scan.h secondary_index.h security.h security_config.h system_metadata.h
BASE_HEADERS += thr_batch.h thr_info.h thr_proxy.h thr_rw_internal.h thr_scan.h thr_sindex.h
BASE_HEADERS += thr_tsvc.h thr_write.h transaction.h transaction_policy.h
BASE_HEADERS += udf_aerospike.h udf_arglist.h udf_cask.h
BASE_HEADERS += udf_memtracker.h udf_record.h udf_rw.h udf_timer.h
BASE_HEADERS += write_request.h xdr_serverside.h

BASE_SOURCES += aggr.c as.c asm.c bin.c cfg.c cluster_config.c index.c job_manager.c json_init.c
BASE_SOURCES += ldt.c ldt_record.c ldt_aerospike.c monitor.c namespace.c packet_compression.c
BASE_SOURCES += particle.c proto.c rec_props.c record.c scan.c signal.c secondary_index.c system_metadata.c
BASE_SOURCES += thr_batch.c thr_demarshal.c thr_info.c thr_info_port.c thr_nsup.c thr_proxy.c
BASE_SOURCES += thr_query.c thr_rw.c thr_sindex.c thr_tscan.c thr_tsvc.c transaction.c
BASE_SOURCES += udf_aerospike.c udf_arglist.c udf_cask.c
BASE_SOURCES += udf_memtracker.c udf_record.c udf_rw.c udf_timer.c
ifneq ($(USE_XDR),1)
  BASE_SOURCES += xdr_serverside_stubs.c
endif
ifneq ($(USE_WARM),1)
  BASE_SOURCES += namespace_cold.c
endif
ifneq ($(USE_SEC),1)
  BASE_SOURCES += security_stubs.c
endif

FABRIC_HEADERS += hb.h fabric.h fb_health.h migrate.h paxos.h
FABRIC_SOURCES += hb.c fabric.c fb_health.c migrate.c partition.c paxos.c

STORAGE_HEADERS += storage.h drv_ssd.h
STORAGE_SOURCES += storage.c drv_kv.c drv_memory.c drv_ssd.c
ifneq ($(USE_WARM),1)
  STORAGE_SOURCES += drv_ssd_cold.c
endif

HEADERS = $(BASE_HEADERS:%=base/%) $(FABRIC_HEADERS:%=fabric/%) $(STORAGE_HEADERS:%=storage/%)
SOURCES = $(BASE_SOURCES:%=base/%) $(FABRIC_SOURCES:%=fabric/%) $(STORAGE_SOURCES:%=storage/%)

SERVER = $(BIN_DIR)/asd

INCLUDES += $(INCLUDE_DIR:%=-I%) -I$(XDR_INCLUDE_DIR)
ifeq ($(USE_KV),1)
  INCLUDES += -I$(KV_INCLUDE_DIR)
endif
INCLUDES += -I$(CF)/include
INCLUDES += -I$(AI)/include
INCLUDES += -I$(COMMON)/target/$(PLATFORM)/include
INCLUDES += -I$(MOD_LUA)/target/$(PLATFORM)/include
INCLUDES += -I$(JANSSON)/src

ifeq ($(USE_LUAJIT),1)
  INCLUDES += -I$(LUAJIT)/src
else
  INCLUDE_LUA_5_1 = /usr/include/lua5.1
  ifneq ($(wildcard $(INCLUDE_LUA_5_1)),)
    INCLUDES += -I$(INCLUDE_LUA_5_1)
    LUA_SUFFIX = 5.1
  endif
endif

AS_LIBRARIES += $(LIBRARY_DIR)/libcf.a
AS_LIBRARIES += $(LIBRARY_DIR)/libai.a
AS_LIBRARIES += $(MOD_LUA)/target/$(PLATFORM)/lib/libmod_lua.a
AS_LIBRARIES += $(COMMON)/target/$(PLATFORM)/lib/libaerospike-common.a
AS_LIBRARIES += $(LIBRARY_DIR)/libxdrconfig.a

ifeq ($(DOPROFILE),1)
  LIBRARIES += -pg -fprofile-arcs -lgcov
endif

# Add either the LuaJIT or Lua library
ifeq ($(USE_LUAJIT),1)
  ifeq ($(LD_LUAJIT),static)
    AS_LIBRARIES += $(LUAJIT)/src/libluajit.a
  else
    LIBRARIES += -L$(LUAJIT)/src -lluajit
  endif
else
  ifeq ($(LD_LUA),static)
    # Find and add the static Lua library.
    AS_LIBRARIES += $(or \
      $(wildcard /usr/local/lib/liblua.a), \
      $(wildcard /usr/lib64/liblua$(LUA_SUFFIX).a), \
      $(wildcard /usr/lib/x86_64-linux-gnu/liblua$(LUA_SUFFIX).a), \
      $(wildcard /usr/lib/liblua.a), \
      $(error Cannot find "liblua.a"))
  else
    LIBRARIES += -llua$(LUA_SUFFIX)
  endif
endif

ifeq ($(LD_JANSSON),static)
  AS_LIBRARIES += $(JANSSON)/src/.libs/libjansson.a
else
  LIBRARIES += -L$(JANSSON)/src/.libs -ljansson
endif

LIBRARIES := $(AS_LIBRARIES) $(LIBRARIES)

ifneq ($(PREPRO),1)
  ifneq ($(MEXP_PHASE),1)
    AS_LIB_DEPS = $(AS_LIBRARIES)
  endif
endif

ifeq ($(USE_KV),1)
  LIBRARIES += -lvsl -lvsldp
  CFLAGS += -DUSE_KV
endif

OBJECTS = $(SOURCES:%.c=$(OBJECT_DIR)/%.o) $(VERSION_OBJ)
DEPENDENCIES = $(OBJECTS:%.o=%.d)
MEXP_SOURCES = $(SOURCES:%=$(MEXP_DIR)/%)
PREPROS = $(OBJECTS:%=%$(PREPRO_SUFFIX))

.PHONY: all
all: $(SERVER)

.PHONY: clean
clean:
	$(RM) $(OBJECTS) $(SERVER){,.stripped}
	$(RM) $(DEPENDENCIES)
	$(RM) $(MEXP_SOURCES) $(PREPROS)

$(SERVER): $(OBJECTS) $(AS_LIB_DEPS)
ifneq ($(PREPRO),1)
  ifneq ($(MEXP_PHASE),1)
	$(LINK.c) -o $(SERVER) $(OBJECTS) $(LIBRARIES)
  endif
endif

include $(DEPTH)/make_in/Makefile.targets
