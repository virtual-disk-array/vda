#ifndef VBDEV_RAID1_H
#define VBDEV_RAID1_H

#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"

#define RAID1_MAX_NAME_LEN (128)
#define RAID1_DEFAULT_STRIP_SIZE (4*1024*1024)
#define RAID1_DEFAULT_WRITE_DELAY (1000)
#define RAID1_DEFAULT_CLEAN_RATIO (1000*1000)
#define RAID1_DEFAULT_MAX_PENDING (1024)
#define RAID1_DEFAULT_MAX_RESYNC (10)

struct raid1_create_param {
	char *bdev0_name;
	char *bdev1_name;
	uint64_t strip_size;
	uint64_t write_delay;
	uint64_t clean_ratio;
	uint64_t max_pending;
	uint64_t max_resync;
	bool synced;
};

typedef void (*raid1_create_cb)(void *arg, int rc);
typedef void (*raid1_delete_cb)(void *arg, int rc);
typedef void (*raid1_rebuild_cb)(void *arg, int rc);

void raid1_bdev_create(const char *raid1_name, struct raid1_create_param *param, raid1_create_cb cb_fn, void *cb_arg);
void raid1_bdev_delete(const char *raid1_name, raid1_delete_cb cb_fn, void *cb_arg);
void raid1_bdev_rebuild(const char *raid1_name, const char *bdev0_name, const char *bdev1_name, raid1_rebuild_cb cb_fn, void *cb_arg);

#endif
