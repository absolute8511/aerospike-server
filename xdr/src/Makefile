# Aerospike XDR
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

ifeq ($(USE_XDR),1)
  include $(XDR)/make_in/Makefile.vars
endif

XDR_CONFIG_HEADERS = xdr_config.h

XDR_CONFIG_SOURCES = xdr_config.c

XDR_CONFIG_OBJECTS = $(XDR_CONFIG_SOURCES:%.c=$(OBJECT_DIR)/%.o)

XDR_CONFIG_LIBRARY = $(LIBRARY_DIR)/libxdrconfig.a

XDR_ALL_OBJECTS += $(XDR_CONFIG_OBJECTS)

INCLUDES += -I$(CF)/include
INCLUDES += -I$(COMMON)/target/$(PLATFORM)/include
INCLUDES += $(XDR_INCLUDES)
INCLUDES += -I$(XDR_INCLUDE_DIR)

DEPENDENCIES = $(XDR_ALL_OBJECTS:%.o=%.d)
MEXP_SOURCES += $(XDR_CONFIG_SOURCES:%=$(MEXP_DIR)/%)
PREPROS = $(XDR_ALL_OBJECTS:%=%$(PREPRO_SUFFIX))

# Allow common globals:
CFLAGS := $(CFLAGS:-fno-common=)

.PHONY: all
ifeq ($(USE_XDR),1)
  ifeq ($(EEREPO), )
    $(error "To build with XDR support, the EEREPO environment variable must be set appropriately.")
  endif
  all: $(XDR_LIBRARY)
else
  all: $(XDR_CONFIG_LIBRARY)
endif

.PHONY: clean
clean:
	$(RM) $(XDR_ALL_OBJECTS) $(XDR_CONFIG_LIBRARY) $(SERVER){,.stripped}
	$(RM) $(DEPENDENCIES)
	$(RM) $(MEXP_SOURCES) $(PREPROS)

$(XDR_CONFIG_LIBRARY): $(XDR_CONFIG_OBJECTS)
ifneq ($(PREPRO),1)
  ifneq ($(MEXP_PHASE),1)
	$(AR) rs $(XDR_CONFIG_LIBRARY) $(XDR_CONFIG_OBJECTS)
  endif
endif

ifeq ($(USE_XDR),1)
  include $(XDR)/make_in/Makefile.targets
endif

include $(DEPTH)/make_in/Makefile.targets
