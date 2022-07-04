src=vbdev_susres_rpc.c vbdev_susres.c

COMMON_CFLAGS += -I$(SPDK_HEADER_DIR)
COMMON_CFLAGS += -L$(SPDK_LIB_DIR)
COMMON_CFLAGS += -L$(DPDK_LIB_DIR)
COMMON_CFLAGS += -L$(VFIO_LIB_DIR)

ifeq ($(VDA_DATAPLANE_DEBUG),y)
COMMON_CFLAGS += -DDEBUG -O0 -fno-omit-frame-pointer
else
COMMON_CFLAGS += -DNDEBUG -O2
endif

.PHONY: susres
susres:
	$(CC) $(COMMON_CFLAGS) -march=$(TARGET_ARCHITECTURE) -c ./vbdev_susres_rpc.c -o ./vbdev_susres_rpc.o
	$(CC) $(COMMON_CFLAGS) -march=$(TARGET_ARCHITECTURE) -c ./vbdev_susres.c -o ./vbdev_susres.o
	$(AR) rcs ./libsusres.a ./vbdev_susres_rpc.o ./vbdev_susres.o
