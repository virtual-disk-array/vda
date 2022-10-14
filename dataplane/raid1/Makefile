src=vbdev_raid1_rpc.c vbdev_raid1.c

COMMON_CFLAGS += -I$(SPDK_HEADER_DIR)
COMMON_CFLAGS += -L$(SPDK_LIB_DIR)
COMMON_CFLAGS += -L$(DPDK_LIB_DIR)
COMMON_CFLAGS += -L$(VFIO_LIB_DIR)

ifeq ($(VDA_DATAPLANE_DEBUG),y)
COMMON_CFLAGS += -DDEBUG -O0 -fno-omit-frame-pointer -g
else
COMMON_CFLAGS += -DNDEBUG -O2
endif

.PHONY: raid1
raid1:
	$(CC) $(COMMON_CFLAGS) -march=$(TARGET_ARCHITECTURE) -c ./vbdev_raid1_rpc.c -o ./vbdev_raid1_rpc.o
	$(CC) $(COMMON_CFLAGS) -march=$(TARGET_ARCHITECTURE) -c ./vbdev_raid1.c -o ./vbdev_raid1.o
	$(AR) rcs ./libraid1.a ./vbdev_raid1_rpc.o ./vbdev_raid1.o
