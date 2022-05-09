COMMON_CFLAGS+=-I$(SPDK_HEADER_DIR)
COMMON_CFLAGS+=-L$(SPDK_LIB_DIR)
COMMON_CFLAGS+=-L$(DPDK_LIB_DIR)
COMMON_CFLAGS+=-L$(VFIO_LIB_DIR)

PKG_CONFIG_PATH = $(SPDK_LIB_DIR)/pkgconfig

DPDK_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_env_dpdk)
SPDK_DPDK_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_event spdk_event_bdev spdk_env_dpdk spdk_event_nvmf)
SYS_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs --static spdk_syslibs)

# Shows how to compile and external bdev and application against the SPDK archives.
vda_dataplane:
	$(CC) $(COMMON_CFLAGS) -march=$(TARGET_ARCHITECTURE) -o vda_dataplane ./app.c -pthread -Wl,--whole-archive,-Bstatic $(SPDK_DPDK_LIB) \
	-Wl,--no-whole-archive,-Bdynamic $(SYS_LIB)
