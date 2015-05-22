# Aerospike Server - Aerospike Index
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

HEADERS = ai.h ai_btree.h ai_globals.h ai_types.h ai_obj.h bt.h bt_iterator.h bt_output.h btree.h btreepriv.h find.h stream.h

SOURCES = ai.c ai_btree.c ai_obj.c bt.c bt_code.c bt_iterator.c bt_output.c find.c stream.c

INCLUDES += $(INCLUDE_DIR:%=-I%)
INCLUDES += -I$(CF)/include -I$(AS)/include
INCLUDES += -I$(AS)/include
INCLUDES += -I$(XDR_INCLUDE_DIR)
INCLUDES += -I$(COMMON)/target/$(PLATFORM)/include
INCLUDES += -I$(MOD_LUA)/target/$(PLATFORM)/include

LIBRARY = $(LIBRARY_DIR)/libai.a

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
