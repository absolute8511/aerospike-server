# Citrusleaf Foundation
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

ifeq ($(USE_EE),1)
  include $(EEREPO)/cf/make_in/Makefile.vars
endif

HEADERS += arenax.h cf_str.h dynbuf.h
HEADERS += enhanced_alloc.h fault.h hist.h hist_track.h mem_count.h
HEADERS += meminfo.h msg.h olock.h rchash.h socket.h util.h
HEADERS += vmapx.h

SOURCES += alloc.c arenax.c cf_str.c daemon.c dynbuf.c fault.c
SOURCES += hist.c hist_track.c id.c meminfo.c msg.c olock.c
SOURCES += socket.c vmapx.c
ifneq ($(USE_WARM),1)
  SOURCES += arenax_cold.c
endif

ifeq ($(USE_JEM),1)
  HEADERS += jem.h
  SOURCES += jem.c
endif

LIBRARY = $(LIBRARY_DIR)/libcf.a

INCLUDES += $(INCLUDE_DIR:%=-I%) -I$(COMMON)/src/include

OBJECTS = $(SOURCES:%.c=$(OBJECT_DIR)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)
MEXP_SOURCES = $(SOURCES:%=$(MEXP_DIR)/%)
PREPROS = $(OBJECTS:%=%$(PREPRO_SUFFIX))

.PHONY: all
all: $(LIBRARY)

.PHONY: clean
clean:
	$(RM) $(OBJECTS) $(LIBRARY)
	$(RM) $(DEPENDENCIES)
	$(RM) $(MEXP_SOURCES) $(PREPROS)

$(LIBRARY): $(OBJECTS)
ifneq ($(PREPRO),1)
  ifneq ($(MEXP_PHASE),1)
	$(AR) rs $(LIBRARY) $(OBJECTS)
  endif
endif

include $(DEPTH)/make_in/Makefile.targets
