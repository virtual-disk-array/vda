#include "spdk/stdinc.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/bdev_module.h"
#include "spdk/log.h"

#include "vbdev_raid1.h"

void
raid1_bdev_create(const char *raid1_name)
{
		SPDK_ERRLOG("raid1_bdev_create: %s\n", raid1_name);
}
