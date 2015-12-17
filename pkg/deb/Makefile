# Build Aerospike Server ".deb" Distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version)
OS = $(shell $(DEPTH)/build/os_version)
SIZE = $(shell du -k $(BIN_DIR)/asd | cut -f1)


DEB = $(PKG)/packages/aerospike-server-$(EDITION)-$(REV).$(OS).x86_64.deb

ifeq ($(USE_XDR),1)
all:	dist-xdr package clean
else
all:	dist package clean
endif

.PHONY: dist
dist:
	install -d $(BUILD_ROOT)/DEBIAN
	install -d $(BUILD_ROOT)/etc/aerospike
	install -d $(BUILD_ROOT)/etc/init.d
	install -d $(BUILD_ROOT)/etc/logrotate.d
	install -d $(BUILD_ROOT)/usr/bin
	install -d $(BUILD_ROOT)/var/log/aerospike
	install -d $(BUILD_ROOT)/var/run/aerospike

	install -pm 644 $(PKG)/deb/conffiles $(BUILD_ROOT)/DEBIAN
	install -pm 755 $(PKG)/deb/postinst.server $(BUILD_ROOT)/DEBIAN/postinst

	install -pm 755 $(BIN_DIR)/asd $(BUILD_ROOT)/usr/bin/asd
	install -pm 755 $(DEPTH)/tools/citrus2aero/upgrade2to3 $(BUILD_ROOT)/usr/bin/asmigrate2to3
	install -pm 755 $(DEPTH)/tools/fixownership/fixownership.py $(BUILD_ROOT)/usr/bin/asfixownership

	install -pm 644 $(DEPTH)/as/etc/aerospike.conf $(BUILD_ROOT)/etc/aerospike
	install -pm 644 $(DEPTH)/as/etc/aerospike_ssd.conf $(BUILD_ROOT)/etc/aerospike
	install -pm 644 $(DEPTH)/as/etc/aerospike_mesh.conf $(BUILD_ROOT)/etc/aerospike

	install -pm 755 $(DEPTH)/as/etc/init-script.deb $(BUILD_ROOT)/etc/init.d/aerospike
	sed -i 's/@EDITION@/$(EDITION)/g' $(BUILD_ROOT)/etc/init.d/aerospike
	install -pm 644 $(DEPTH)/as/etc/logrotate_asd $(BUILD_ROOT)/etc/logrotate.d/aerospike

ifeq ($(USE_ASM),1)
	install -pm 644 $(DEPTH)/as/etc/initfns $(BUILD_ROOT)/etc/aerospike
	install -d $(OPT_AS)/bin
	install -pm 755 $(DEPTH)/as/etc/run-under-gdb $(OPT_AS)/bin
	install -d $(OPT_AS)/lib
	install -pm 644 $(DEPTH)/modules/asmalloc/lib/asmalloc.jem.so $(OPT_AS)/lib
	install -pm 644 $(DEPTH)/modules/jemalloc/lib/libjemalloc.so $(OPT_AS)/lib
endif

	install -d $(OPT_AS)/data
	install -d $(OPT_AS)/smd
	install -d $(OPT_AS)/sys/udf/lua/external
	install -d $(OPT_AS)/sys/udf/lua/ldt
	install -d $(OPT_AS)/usr/udf/lua
	install -pm 644 $(DEPTH)/modules/lua-core/src/*.lua $(OPT_AS)/sys/udf/lua
	install -pm 644 $(DEPTH)/modules/lua-core/src/external/* $(OPT_AS)/sys/udf/lua/external
	install -pm 644 $(DEPTH)/modules/lua-core/src/ldt/* $(OPT_AS)/sys/udf/lua/ldt

	install -d $(OPT_AS)/doc
	install -pm 644 $(DEPTH)/LICENSE $(OPT_AS)/doc

	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/deb/server-64 > $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@EDITION@/'$(EDITION)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@SIZE@/'$(SIZE)'/g' $(BUILD_ROOT)/DEBIAN/control

package:
	fakeroot dpkg-deb --build $(BUILD_ROOT) $(DEB)

clean:
	rm -rf $(SOURCE_ROOT)/*

ifeq ($(USE_XDR),1)
  include $(XDR)/make_in/Makefile.deb.in
endif
