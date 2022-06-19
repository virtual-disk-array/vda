src=vbdev_raid1_rpc.c vbdev_raid1.c

COMMON_CFLAGS+=-I$(SPDK_HEADER_DIR)
COMMON_CFLAGS+=-L$(SPDK_LIB_DIR)
COMMON_CFLAGS+=-L$(DPDK_LIB_DIR)
COMMON_CFLAGS+=-L$(VFIO_LIB_DIR)
COMMON_CFLAGS+=-DDEBUG

raid1:
	$(CC) $(COMMON_CFLAGS) -c ./vbdev_raid1_rpc.c -o ./vbdev_raid1_rpc.o
	$(CC) $(COMMON_CFLAGS) -c ./vbdev_raid1.c -o ./vbdev_raid1.o
	$(AR) rcs ./libraid1_external.a ./vbdev_raid1_rpc.o ./vbdev_raid1.o
