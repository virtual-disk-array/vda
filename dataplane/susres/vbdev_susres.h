#ifndef VBDEV_SUSRES_H
#define VBDEV_SUSRES_H

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

/**
 * Create new pass through bdev.
 *
 * \param bdev_name Bdev on which pass through vbdev will be created.
 * \param vbdev_name Name of the pass through bdev.
 * \return 0 on success, other on failure.
 */
int bdev_susres_create_disk(const char *bdev_name, const char *vbdev_name);

/**
 * Delete susres bdev.
 *
 * \param bdev Pointer to pass through bdev.
 * \param cb_fn Function to call after deletion.
 * \param cb_arg Argument to pass to cb_fn.
 */
void bdev_susres_delete_disk(struct spdk_bdev *bdev,
	spdk_bdev_unregister_cb cb_fn, void *cb_arg);

typedef void (*susres_rpc_cb)(void *arg, int bdeverrno);

void
bdev_susres_suspend_disk(const char *vbdev_name,
	susres_rpc_cb cb_fn, void *cb_arg);

void
bdev_susres_resume_disk(const char *vbdev_name,
	susres_rpc_cb cb_fn, void *cb_arg);

#endif
