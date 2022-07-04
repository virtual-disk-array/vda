COMMON_CFLAGS += -I$(SPDK_HEADER_DIR)
COMMON_CFLAGS += -L$(SPDK_LIB_DIR)
COMMON_CFLAGS += -L$(DPDK_LIB_DIR)
COMMON_CFLAGS += -L$(VFIO_LIB_DIR)

ifeq ($(VDA_DATAPLANE_DEBUG),y)
COMMON_CFLAGS += -DDEBUG -O0 -fno-omit-frame-pointer
else
COMMON_CFLAGS += -DNDEBUG -O2
endif

PKG_CONFIG_PATH = $(SPDK_LIB_DIR)/pkgconfig

DPDK_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_env_dpdk)
SPDK_DPDK_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_event spdk_event_bdev spdk_env_dpdk spdk_event_nvmf)
SYS_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs --static spdk_syslibs)

.PHONY: app
app:
	$(CC) $(COMMON_CFLAGS) -march=$(TARGET_ARCHITECTURE) -L../raid1 -o vda_dataplane ./app.c -pthread -Wl,--whole-archive,-Bstatic -lraid1_external $(SPDK_DPDK_LIB) \
	-Wl,--no-whole-archive,-Bdynamic $(SYS_LIB)
