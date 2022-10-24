#ifndef VBDEV_RAID1_H
#define VBDEV_RAID1_H

#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/endian.h"
#include "spdk/bdev_module.h"

#define RAID1_MAX_NAME_LEN (128)
#define RAID1_DEFAULT_STRIP_SIZE (4*1024*1024)
#define RAID1_DEFAULT_WRITE_DELAY (1000)
#define RAID1_DEFAULT_CLEAN_RATIO (1000)
#define RAID1_DEFAULT_MAX_DELAY (1024)
#define RAID1_DEFAULT_MAX_RESYNC (10)

#define RAID1_SB_START_PAGE (0)
#define RAID1_SB_START_BYTE (RAID1_SB_START_PAGE * PAGE_SIZE)
#define RAID1_SB_PAGES (1)
#define RAID1_SB_SIZE (RAID1_SB_PAGES * PAGE_SIZE)
#define RAID1_BM_START_PAGE (RAID1_SB_START_PAGE + RAID1_SB_PAGES)
#define RAID1_BM_START_BYTE (RAID1_BM_START_PAGE * PAGE_SIZE)
#define RAID1_MAJOR_VERSION (1)
#define RAID1_MINOR_VERSION (0)
#define RAID1_STRIP_PER_REGION (PAGE_SIZE * RAID1_BYTESZ)

#define RAID1_MAX_DELAY_CNT (255)

#define RAID1_MAGIC_STRING "SPDK_RAID1"
#define RAID1_MAGIC_STRING_LEN (11)
SPDK_STATIC_ASSERT(sizeof(RAID1_MAGIC_STRING) == RAID1_MAGIC_STRING_LEN,
    "RAID1_MAGIC_STRING_LEN incorrect");

struct raid1_sb {
	char magic[RAID1_MAGIC_STRING_LEN];
	struct spdk_uuid uuid;
	uint64_t meta_size;
	uint64_t data_size;
	uint64_t counter;
	uint32_t major_version;
	uint32_t minor_version;
	uint64_t strip_size;
}__attribute__((packed));

SPDK_STATIC_ASSERT(sizeof(struct raid1_sb)<RAID1_SB_SIZE, "sb size is too large");

struct raid1_create_param {
	char *bdev0_name;
	char *bdev1_name;
	uint64_t strip_size;
	uint64_t write_delay;
	uint64_t clean_ratio;
	uint64_t max_delay;
	uint64_t max_resync;
	uint64_t meta_size;
	bool synced;
	bool ignore_zero_block;
};

typedef void (*raid1_create_cb)(void *arg, int rc);
typedef void (*raid1_delete_cb)(void *arg, int rc);
typedef void (*raid1_dump_cb)(void *arg, struct raid1_sb *sb, int rc);

void raid1_bdev_create(const char *raid1_name, struct raid1_create_param *param, raid1_create_cb cb_fn, void *cb_arg);
void raid1_bdev_delete(const char *raid1_name, raid1_delete_cb cb_fn, void *cb_arg);
void raid1_bdev_dump(const char *bdev_name, raid1_dump_cb cb_fn, void *cb_arg);

#endif
