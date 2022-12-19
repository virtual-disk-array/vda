#include "spdk/stdinc.h"

#include "vbdev_susres.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"

static int vbdev_susres_init(void);
static int vbdev_susres_get_ctx_size(void);
static void vbdev_susres_examine(struct spdk_bdev *bdev);
static void vbdev_susres_finish(void);
static int vbdev_susres_config_json(struct spdk_json_write_ctx *w);

static struct spdk_bdev_module susres_if = {
	.name = "susres",
	.module_init = vbdev_susres_init,
	.get_ctx_size = vbdev_susres_get_ctx_size,
	.examine_config = vbdev_susres_examine,
	.module_fini = vbdev_susres_finish,
	.config_json = vbdev_susres_config_json
};

SPDK_BDEV_MODULE_REGISTER(susres, &susres_if)

/* List of pt_bdev names and their base bdevs via configuration file.
 * Used so we can parse the conf once at init and use this list in examine().
 */
#define SUSRES_MAX_NAME_LEN (256)
struct bdev_names {
	char			vbdev_name[SUSRES_MAX_NAME_LEN+1];
	char			bdev_name[SUSRES_MAX_NAME_LEN+1];
	TAILQ_ENTRY(bdev_names)	link;
};
static TAILQ_HEAD(, bdev_names) g_bdev_names = TAILQ_HEAD_INITIALIZER(g_bdev_names);

enum susres_status {
	SUSRES_STATUS_RESUMED = 0,
	SUSRES_STATUS_RESUMING,
	SUSRES_STATUS_SUSPENDED,
	SUSRES_STATUS_SUSPENDING,
};

static inline const char *
susres_status_to_string(enum susres_status status)
{
	switch(status) {
	case SUSRES_STATUS_RESUMED:
		return "resumed";
	case SUSRES_STATUS_RESUMING:
		return "resuming";
	case SUSRES_STATUS_SUSPENDED:
		return "suspended";
	case SUSRES_STATUS_SUSPENDING:
		return "suspending";
	default:
		assert(false);
	}
}

/* List of virtual bdevs and associated info for each. */
struct vbdev_susres {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc		*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		pt_bdev;    /* the PT virtual bdev */
	TAILQ_ENTRY(vbdev_susres)	link;
	struct spdk_thread		*thread;    /* thread where base device is opened */
	enum susres_status		status;
	int 				ack_cnt;
	struct susres_thread_ctx	*thread_ctx_array;
	void				*rpc_arg;
	susres_rpc_cb			rpc_cb_fn;
};
static TAILQ_HEAD(, vbdev_susres) g_pt_nodes = TAILQ_HEAD_INITIALIZER(g_pt_nodes);

/* The pt vbdev channel struct. It is allocated and freed on my behalf by the io channel code.
 * If this vbdev needed to implement a poller or a queue for IO, this is where those things
 * would be defined. This susres bdev doesn't actually need to allocate a channel, it could
 * simply pass back the channel of the bdev underneath it but for example purposes we will
 * present its own to the upper layers.
 */
struct pt_io_channel {
	struct spdk_io_channel	*base_ch; /* IO channel of base device */
};

struct susres_bdev_io {
	/* bdev related */
	struct spdk_io_channel *ch;
	/* for bdev_io_wait */
	struct spdk_bdev_io_wait_entry bdev_io_wait;
	TAILQ_ENTRY(susres_bdev_io) link;
};

struct susres_thread_ctx {
	enum susres_status status;
	uint64_t inflight;
	TAILQ_HEAD(, susres_bdev_io) io_queue;
	struct spdk_thread *rpc_thread;
	struct pt_io_channel *pt_ch;
};

static inline struct susres_thread_ctx *
vbdev_susres_get_thread_ctx(struct vbdev_susres *pt_node)
{
	struct spdk_thread *thread = spdk_get_thread();
	int idx = spdk_thread_get_id(thread) - 1;
	assert(idx >= 0);
	return &pt_node->thread_ctx_array[idx];
}

static void vbdev_susres_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);

static void
vbdev_susres_base_bdev_event_cb(enum spdk_bdev_event_type type,
	struct spdk_bdev *bdev, void *event_ctx);

static int
susres_open_base_bdev(struct vbdev_susres *pt_node, const char *bdev_name)
{
	struct bdev_names *name;
	int rc = 0;

	assert(pt_node->thread == spdk_get_thread());
	rc = spdk_bdev_open_ext(bdev_name, true, vbdev_susres_base_bdev_event_cb,
		NULL, &pt_node->base_desc);
	if (rc) {
		SPDK_ERRLOG("Reopen base bdev failed: %s %s %d\n",
			pt_node->pt_bdev.name, pt_node->base_bdev->name, rc);
		return rc;
	}
	pt_node->base_bdev = spdk_bdev_desc_get_bdev(pt_node->base_desc);
	rc = spdk_bdev_module_claim_bdev(pt_node->base_bdev, pt_node->base_desc,
		pt_node->pt_bdev.module);
	if (rc) {
		SPDK_ERRLOG("Reclaim base bdev failed: %s %s %d\n",
			pt_node->pt_bdev.name, pt_node->base_bdev->name, rc);
		spdk_bdev_close(pt_node->base_desc);
		return rc;
	}
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(pt_node->pt_bdev.name, name->vbdev_name) == 0) {
			assert(strlen(bdev_name) <= SUSRES_MAX_NAME_LEN);
			strncpy(name->bdev_name, bdev_name, SUSRES_MAX_NAME_LEN);
			break;
		}
	}
	SPDK_DEBUGLOG(vbdev_susres, "open_base_bdev pt_bdev: %s base_bdev: %s\n",
		pt_node->pt_bdev.name, pt_node->base_bdev->name);
	return 0;
}

static void
susres_close_base_bdev(struct vbdev_susres *pt_node)
{
	struct bdev_names *name;
	SPDK_DEBUGLOG(vbdev_susres, "close_base_bdev pt_bdev: %s base_bdev: %s\n",
		pt_node->pt_bdev.name, pt_node->base_bdev->name);
	assert(pt_node->thread == spdk_get_thread());
	spdk_bdev_module_release_bdev(pt_node->base_bdev);
	spdk_bdev_close(pt_node->base_desc);
	pt_node->base_bdev = NULL;
	pt_node->base_desc = NULL;
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(pt_node->pt_bdev.name, name->vbdev_name) == 0) {
			memset(name->bdev_name, 0, SUSRES_MAX_NAME_LEN);
			break;
		}
	}
}

/* Callback for unregistering the IO device. */
static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_susres *pt_node  = io_device;

	/* Done with this pt_node. */
	free(pt_node->thread_ctx_array);
	free(pt_node->pt_bdev.name);
	free(pt_node);
}

/* Called after we've unregistered following a hot remove callback.
 * Our finish entry point will be called next.
 */
static int
vbdev_susres_destruct(void *ctx)
{
	struct vbdev_susres *pt_node = (struct vbdev_susres *)ctx;

	if (pt_node->rpc_cb_fn) {
		pt_node->rpc_cb_fn(pt_node->rpc_arg, 0);
	}

	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */

	TAILQ_REMOVE(&g_pt_nodes, pt_node, link);

	if (pt_node->status != SUSRES_STATUS_SUSPENDED) {
		/* Unclaim the underlying bdev. */
		spdk_bdev_module_release_bdev(pt_node->base_bdev);
		assert(pt_node->thread == spdk_get_thread());
		spdk_bdev_close(pt_node->base_desc);
	}

	/* Unregister the io_device. */
	spdk_io_device_unregister(pt_node, _device_unregister_cb);

	return 0;
}

static void
susres_thread_ack(void *arg)
{
	struct vbdev_susres *pt_node, *tmp;

	pt_node = NULL;
	TAILQ_FOREACH(tmp, &g_pt_nodes, link) {
		if (tmp == arg) {
			pt_node = tmp;
			break;
		}
	}

	if (!pt_node) {
		SPDK_NOTICELOG("Received ack after the susres bdev is deleted: %p", arg);
		return;
	}

	pt_node->ack_cnt++;
	if (pt_node->ack_cnt >= spdk_thread_get_count()) {
		assert(pt_node->rpc_cb_fn);
		if (pt_node->status == SUSRES_STATUS_SUSPENDING) {
			pt_node->status = SUSRES_STATUS_SUSPENDED;
			susres_close_base_bdev(pt_node);
		} else if (pt_node->status == SUSRES_STATUS_RESUMING) {
			pt_node->status = SUSRES_STATUS_RESUMED;
		} else {
			assert(false);
		}
		pt_node->ack_cnt = 0;
		pt_node->rpc_cb_fn(pt_node->rpc_arg, 0);
		pt_node->rpc_cb_fn = NULL;
	}
}

static inline void
susres_io_complete_hook(struct vbdev_susres *pt_node)
{
	struct susres_thread_ctx *thread_ctx = vbdev_susres_get_thread_ctx(pt_node);
	assert(thread_ctx->inflight > 0);
	thread_ctx->inflight--;
	if (thread_ctx->inflight == 0 && thread_ctx->status == SUSRES_STATUS_SUSPENDING) {
		thread_ctx->status = SUSRES_STATUS_SUSPENDED;
		if (thread_ctx->pt_ch) {
			spdk_put_io_channel(thread_ctx->pt_ch->base_ch);
		}
		spdk_thread_send_msg(thread_ctx->rpc_thread, susres_thread_ack, pt_node);
		thread_ctx->rpc_thread = NULL;
	}
}

/* Completion callback for IO that were issued from this bdev. The original bdev_io
 * is passed in as an arg so we'll complete that one with the appropriate status
 * and then free the one that this module issued.
 */
static void
_pt_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	struct vbdev_susres *pt_node = SPDK_CONTAINEROF(orig_io->bdev,
		struct vbdev_susres, pt_bdev);
	/* Complete the original IO and then free the one that we created here
	 * as a result of issuing an IO via submit_request.
	 */
	spdk_bdev_io_complete(orig_io, status);
	susres_io_complete_hook(pt_node);
	spdk_bdev_free_io(bdev_io);
}

static void
_pt_complete_zcopy_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	struct susres_bdev_io *io_ctx = (struct susres_bdev_io *)orig_io->driver_ctx;
	struct vbdev_susres *pt_node = SPDK_CONTAINEROF(orig_io->bdev,
		struct vbdev_susres, pt_bdev);

	/* Complete the original IO and then free the one that we created here
	 * as a result of issuing an IO via submit_request.
	 */
	spdk_bdev_io_set_buf(orig_io, bdev_io->u.bdev.iovs[0].iov_base, bdev_io->u.bdev.iovs[0].iov_len);
	spdk_bdev_io_complete(orig_io, status);
	susres_io_complete_hook(pt_node);
	spdk_bdev_free_io(bdev_io);
}

static void
vbdev_susres_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct susres_bdev_io *io_ctx = (struct susres_bdev_io *)bdev_io->driver_ctx;

	vbdev_susres_submit_request(io_ctx->ch, bdev_io);
}

static void
vbdev_susres_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct susres_bdev_io *io_ctx = (struct susres_bdev_io *)bdev_io->driver_ctx;
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct vbdev_susres *pt_node = SPDK_CONTAINEROF(bdev_io->bdev,
		struct vbdev_susres, pt_bdev);
	int rc;

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_susres_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	/* Queue the IO using the channel of the base device. */
	rc = spdk_bdev_queue_io_wait(bdev_io->bdev, pt_ch->base_ch, &io_ctx->bdev_io_wait);
	if (rc != 0) {
		SPDK_ERRLOG("Queue io failed in vbdev_susres_queue_io, rc=%d.\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		susres_io_complete_hook(pt_node);
	}
}

/* Callback for getting a buf from the bdev pool in the event that the caller passed
 * in NULL, we need to own the buffer so it doesn't get freed by another vbdev module
 * beneath us before we're done with it. That won't happen in this example but it could
 * if this example were used as a template for something more complex.
 */
static void
pt_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct vbdev_susres *pt_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_susres,
					 pt_bdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct susres_bdev_io *io_ctx = (struct susres_bdev_io *)bdev_io->driver_ctx;
	int rc;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		susres_io_complete_hook(pt_node);
		return;
	}

	if (bdev_io->u.bdev.md_buf == NULL) {
		rc = spdk_bdev_readv_blocks(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
					    bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks, _pt_complete_io,
					    bdev_io);
	} else {
		rc = spdk_bdev_readv_blocks_with_md(pt_node->base_desc, pt_ch->base_ch,
						    bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						    bdev_io->u.bdev.md_buf,
						    bdev_io->u.bdev.offset_blocks,
						    bdev_io->u.bdev.num_blocks,
						    _pt_complete_io, bdev_io);
	}

	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for susres.\n");
			io_ctx->ch = ch;
			vbdev_susres_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
			susres_io_complete_hook(pt_node);
		}
	}
}

/* Called when someone above submits IO to this pt vbdev. We're simply passing it on here
 * via SPDK IO calls which in turn allocate another bdev IO and call our cpl callback provided
 * below along with the original bdev_io so that we can complete it once this IO completes.
 */
static void
vbdev_susres_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_susres *pt_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_susres, pt_bdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct susres_bdev_io *io_ctx = (struct susres_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;
	struct susres_thread_ctx *thread_ctx = vbdev_susres_get_thread_ctx(pt_node);
	if (thread_ctx->status != SUSRES_STATUS_RESUMED) {
		TAILQ_INSERT_TAIL(&thread_ctx->io_queue, io_ctx, link);
		return;
	}

	thread_ctx->inflight++;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, pt_read_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		if (bdev_io->u.bdev.md_buf == NULL) {
			rc = spdk_bdev_writev_blocks(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
						     bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
						     bdev_io->u.bdev.num_blocks, _pt_complete_io,
						     bdev_io);
		} else {
			rc = spdk_bdev_writev_blocks_with_md(pt_node->base_desc, pt_ch->base_ch,
							     bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
							     bdev_io->u.bdev.md_buf,
							     bdev_io->u.bdev.offset_blocks,
							     bdev_io->u.bdev.num_blocks,
							     _pt_complete_io, bdev_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		rc = spdk_bdev_write_zeroes_blocks(pt_node->base_desc, pt_ch->base_ch,
						   bdev_io->u.bdev.offset_blocks,
						   bdev_io->u.bdev.num_blocks,
						   _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(pt_node->base_desc, pt_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = spdk_bdev_flush_blocks(pt_node->base_desc, pt_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_RESET:
		rc = spdk_bdev_reset(pt_node->base_desc, pt_ch->base_ch,
				     _pt_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		rc = spdk_bdev_zcopy_start(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.bdev.iovs,
					   bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
					   bdev_io->u.bdev.num_blocks, bdev_io->u.bdev.zcopy.populate,
					   _pt_complete_zcopy_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = spdk_bdev_abort(pt_node->base_desc, pt_ch->base_ch, bdev_io->u.abort.bio_to_abort,
				     _pt_complete_io, bdev_io);
		break;
	default:
		SPDK_ERRLOG("susres: unknown I/O type %d\n", bdev_io->type);
		thread_ctx->inflight--;
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}
	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for susres.\n");
			thread_ctx->inflight--;
			io_ctx->ch = ch;
			vbdev_susres_queue_io(bdev_io);
		} else {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			thread_ctx->inflight--;
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

/* We'll just call the base bdev and let it answer however if we were more
 * restrictive for some reason (or less) we could get the response back
 * and modify according to our purposes.
 */
static bool
vbdev_susres_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_susres *pt_node = (struct vbdev_susres *)ctx;

	if (pt_node->status == SUSRES_STATUS_RESUMED) {
		return spdk_bdev_io_type_supported(pt_node->base_bdev, io_type);
	} else {
		/* Do not support any io type if we are not
		 * in the resumed status.
		 */
		return false;
	}
}

/* We supplied this as an entry point for upper layers who want to communicate to this
 * bdev.  This is how they get a channel. We are passed the same context we provided when
 * we created our PT vbdev in examine() which, for this bdev, is the address of one of
 * our context nodes. From here we'll ask the SPDK channel code to fill out our channel
 * struct and we'll keep it in our PT node.
 */
static struct spdk_io_channel *
vbdev_susres_get_io_channel(void *ctx)
{
	struct vbdev_susres *pt_node = (struct vbdev_susres *)ctx;
	struct spdk_io_channel *pt_ch = NULL;

	/* The IO channel code will allocate a channel for us which consists of
	 * the SPDK channel structure plus the size of our pt_io_channel struct
	 * that we passed in when we registered our IO device. It will then call
	 * our channel create callback to populate any elements that we need to
	 * update.
	 */
	pt_ch = spdk_get_io_channel(pt_node);

	return pt_ch;
}

/* This is the output for bdev_get_bdevs() for this vbdev */
static int
vbdev_susres_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_susres *pt_node = (struct vbdev_susres *)ctx;

	spdk_json_write_name(w, "susres");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&pt_node->pt_bdev));
	if (pt_node->status != SUSRES_STATUS_SUSPENDED) {
		spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(pt_node->base_bdev));
	}
	spdk_json_write_named_string(w, "status",
		susres_status_to_string(pt_node->status));
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vbdev_susres_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_susres *pt_node;

	TAILQ_FOREACH(pt_node, &g_pt_nodes, link) {
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "construct_susres_bdev");
		spdk_json_write_named_object_begin(w, "params");
		spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(pt_node->base_bdev));
		spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&pt_node->pt_bdev));
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

/* We provide this callback for the SPDK channel code to create a channel using
 * the channel struct we provided in our module get_io_channel() entry point. Here
 * we get and save off an underlying base channel of the device below us so that
 * we can communicate with the base bdev on a per channel basis.  If we needed
 * our own poller for this vbdev, we'd register it here.
 */
static int
pt_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;
	struct vbdev_susres *pt_node = io_device;
	struct susres_thread_ctx *thread_ctx = vbdev_susres_get_thread_ctx(pt_node);

	SPDK_DEBUGLOG(vbdev_susres, "ch_create_cb: %s %" PRIu64 "\n",
		pt_node->pt_bdev.name, spdk_thread_get_id(spdk_get_thread()));

	if (thread_ctx->status != SUSRES_STATUS_SUSPENDED) {
		assert(!pt_ch->base_ch);
		pt_ch->base_ch = spdk_bdev_get_io_channel(pt_node->base_desc);
	}
	thread_ctx->pt_ch = pt_ch;

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this bdev used its own poller, we'd unregister it here.
 */
static void
pt_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;
	struct vbdev_susres *pt_node = io_device;
	struct susres_bdev_io *io_ctx;
	struct susres_thread_ctx *thread_ctx = vbdev_susres_get_thread_ctx(pt_node);

	SPDK_DEBUGLOG(vbdev_susres, "ch_destroy_cb: %s %" PRIu64 "\n",
		pt_node->pt_bdev.name, spdk_thread_get_id(spdk_get_thread()));
	while (!TAILQ_EMPTY(&thread_ctx->io_queue)) {
		io_ctx = TAILQ_FIRST(&thread_ctx->io_queue);
		TAILQ_REMOVE(&thread_ctx->io_queue, io_ctx, link);
		struct spdk_bdev_io *orig_io = SPDK_CONTAINEROF(io_ctx,
			struct spdk_bdev_io, driver_ctx);
		spdk_bdev_io_complete(orig_io, SPDK_BDEV_IO_STATUS_FAILED);
		spdk_bdev_free_io(orig_io);
	}

	if (thread_ctx->status != SUSRES_STATUS_SUSPENDED) {
		assert(pt_ch->base_ch);
		spdk_put_io_channel(pt_ch->base_ch);
		pt_ch->base_ch = NULL;
	}
	thread_ctx->pt_ch = NULL;
}

/* Create the susres association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_susres_insert_name(const char *bdev_name, const char *vbdev_name)
{
	struct bdev_names *name;

	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(vbdev_name, name->vbdev_name) == 0) {
			SPDK_ERRLOG("susres bdev %s already exists\n", vbdev_name);
			return -EEXIST;
		}
	}

	name = calloc(1, sizeof(struct bdev_names));
	if (!name) {
		SPDK_ERRLOG("could not allocate bdev_names\n");
		return -ENOMEM;
	}

	assert(strlen(bdev_name) <= SUSRES_MAX_NAME_LEN);
	assert(strlen(vbdev_name) <= SUSRES_MAX_NAME_LEN);
	strncpy(name->bdev_name, bdev_name, SUSRES_MAX_NAME_LEN);
	strncpy(name->vbdev_name, vbdev_name, SUSRES_MAX_NAME_LEN);

	TAILQ_INSERT_TAIL(&g_bdev_names, name, link);

	return 0;
}

static int
vbdev_susres_init(void)
{
	return 0;
}

/* Called when the entire module is being torn down. */
static void
vbdev_susres_finish(void)
{
	struct bdev_names *name;

	while ((name = TAILQ_FIRST(&g_bdev_names))) {
		TAILQ_REMOVE(&g_bdev_names, name, link);
		free(name);
	}
}

/* During init we'll be asked how much memory we'd like passed to us
 * in bev_io structures as context. Here's where we specify how
 * much context we want per IO.
 */
static int
vbdev_susres_get_ctx_size(void)
{
	return sizeof(struct susres_bdev_io);
}

/* Where vbdev_susres_config_json() is used to generate per module JSON config data, this
 * function is called to output any per bdev specific methods. For the PT module, there are
 * none.
 */
static void
vbdev_susres_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	/* No config per bdev needed */
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_susres_fn_table = {
	.destruct		= vbdev_susres_destruct,
	.submit_request		= vbdev_susres_submit_request,
	.io_type_supported	= vbdev_susres_io_type_supported,
	.get_io_channel		= vbdev_susres_get_io_channel,
	.dump_info_json		= vbdev_susres_dump_info_json,
	.write_config_json	= vbdev_susres_write_config_json,
};

static void
vbdev_susres_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_susres *pt_node, *tmp;

	SPDK_DEBUGLOG(vbdev_susres, "Removing: %s\n", bdev_find->name);
	TAILQ_FOREACH_SAFE(pt_node, &g_pt_nodes, link, tmp) {
		if (bdev_find == pt_node->base_bdev) {
			SPDK_NOTICELOG("Hot remove: %s\n", pt_node->base_bdev->name);
			spdk_bdev_unregister(&pt_node->pt_bdev, NULL, NULL);
		}
	}
}

/* Called when the underlying base bdev triggers asynchronous event such as bdev removal. */
static void
vbdev_susres_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
				  void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		vbdev_susres_base_bdev_hotremove_cb(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

/* Create and register the susres vbdev if we find it in our list of bdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vbdev_susres_register(const char *bdev_name)
{
	struct bdev_names *name;
	struct vbdev_susres *pt_node;
	struct spdk_bdev *bdev;
	struct susres_thread_ctx *thread_ctx;
	int thread_count, i;
	int rc = 0;

	/* Check our list of names from config versus this bdev and if
	 * there's a match, create the pt_node & bdev accordingly.
	 */
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(name->bdev_name, bdev_name) != 0) {
			continue;
		}

		SPDK_NOTICELOG("Match on %s\n", bdev_name);
		pt_node = calloc(1, sizeof(struct vbdev_susres));
		if (!pt_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_node\n");
			break;
		}

		pt_node->pt_bdev.name = strdup(name->vbdev_name);
		if (!pt_node->pt_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_bdev name\n");
			free(pt_node);
			break;
		}
		pt_node->pt_bdev.product_name = "susres";

		thread_count = spdk_thread_get_count();
		SPDK_NOTICELOG("thread_count: %d\n", thread_count);
		pt_node->thread_ctx_array = malloc(
			thread_count * sizeof(struct susres_thread_ctx));
		if (!pt_node->thread_ctx_array) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate thread_ctx_array %d\n", thread_count);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		for (i = 0; i < thread_count; i++) {
			thread_ctx = &pt_node->thread_ctx_array[i];
			thread_ctx->status = SUSRES_STATUS_RESUMED;
			thread_ctx->inflight = 0;
			TAILQ_INIT(&thread_ctx->io_queue);
			thread_ctx->rpc_thread = NULL;
			thread_ctx->pt_ch = NULL;
		}

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_susres_base_bdev_event_cb,
					NULL, &pt_node->base_desc);
		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(pt_node->thread_ctx_array);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("base bdev opened\n");

		bdev = spdk_bdev_desc_get_bdev(pt_node->base_desc);
		pt_node->base_bdev = bdev;

		/* Copy some properties from the underlying base bdev. */
		pt_node->pt_bdev.write_cache = bdev->write_cache;
		pt_node->pt_bdev.required_alignment = bdev->required_alignment;
		pt_node->pt_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		pt_node->pt_bdev.blocklen = bdev->blocklen;
		pt_node->pt_bdev.blockcnt = bdev->blockcnt;

		pt_node->pt_bdev.md_interleave = bdev->md_interleave;
		pt_node->pt_bdev.md_len = bdev->md_len;
		pt_node->pt_bdev.dif_type = bdev->dif_type;
		pt_node->pt_bdev.dif_is_head_of_md = bdev->dif_is_head_of_md;
		pt_node->pt_bdev.dif_check_flags = bdev->dif_check_flags;

		/* This is the context that is passed to us when the bdev
		 * layer calls in so we'll save our pt_bdev node here.
		 */
		pt_node->pt_bdev.ctxt = pt_node;
		pt_node->pt_bdev.fn_table = &vbdev_susres_fn_table;
		pt_node->pt_bdev.module = &susres_if;
		TAILQ_INSERT_TAIL(&g_pt_nodes, pt_node, link);

		spdk_io_device_register(pt_node, pt_bdev_ch_create_cb, pt_bdev_ch_destroy_cb,
					sizeof(struct pt_io_channel),
					name->vbdev_name);
		SPDK_NOTICELOG("io_device created at: 0x%p\n", pt_node);

		/* Save the thread where the base device is opened */
		pt_node->thread = spdk_get_thread();

		rc = spdk_bdev_module_claim_bdev(bdev, pt_node->base_desc, pt_node->pt_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			spdk_bdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			free(pt_node->thread_ctx_array);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("bdev claimed\n");

		rc = spdk_bdev_register(&pt_node->pt_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register pt_bdev\n");
			spdk_bdev_module_release_bdev(&pt_node->pt_bdev);
			spdk_bdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			free(pt_node->thread_ctx_array);
			free(pt_node->pt_bdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("pt_bdev registered\n");
		SPDK_NOTICELOG("created pt_bdev for: %s\n", name->vbdev_name);
		break;
	}

	return rc;
}

/* Create the susres disk from the given bdev and vbdev name. */
int
bdev_susres_create_disk(const char *bdev_name, const char *vbdev_name)
{
	int rc;

	/* Insert the bdev name into our global name list even if it doesn't exist yet,
	 * it may show up soon...
	 */
	rc = vbdev_susres_insert_name(bdev_name, vbdev_name);
	if (rc) {
		return rc;
	}

	rc = vbdev_susres_register(bdev_name);
	if (rc == -ENODEV) {
		/* This is not an error, we tracked the name above and it still
		 * may show up later.
		 */
		SPDK_NOTICELOG("vbdev creation deferred pending base bdev arrival\n");
		rc = 0;
	}

	return rc;
}

void
bdev_susres_delete_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn,
				   void *cb_arg)
{
	struct bdev_names *name;

	if (!bdev || bdev->module != &susres_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	/* Remove the association (vbdev, bdev) from g_bdev_names. This is required so that the
	 * vbdev does not get re-created if the same bdev is constructed at some other time,
	 * unless the underlying bdev was hot-removed.
	 */
	TAILQ_FOREACH(name, &g_bdev_names, link) {
		if (strcmp(name->vbdev_name, bdev->name) == 0) {
			TAILQ_REMOVE(&g_bdev_names, name, link);
			free(name);
			break;
		}
	}

	/* Additional cleanup happens in the destruct callback. */
	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

/* Because we specified this function in our pt bdev function table when we
 * registered our pt bdev, we'll get this call anytime a new bdev shows up.
 * Here we need to decide if we care about it and if so what to do. We
 * parsed the config file at init so we check the new bdev against the list
 * we built up at that time and if the user configured us to attach to this
 * bdev, here's where we do it.
 */
static void
vbdev_susres_examine(struct spdk_bdev *bdev)
{
	vbdev_susres_register(bdev->name);
	spdk_bdev_module_examine_done(&susres_if);
}

static void
susres_msg_cpl(void *arg)
{
	return;
}

static void
susres_do_suspend(void *arg)
{
	struct vbdev_susres *pt_node = arg;
	struct susres_thread_ctx *thread_ctx;
	thread_ctx = vbdev_susres_get_thread_ctx(pt_node);
	assert(thread_ctx->status == SUSRES_STATUS_RESUMED);
	assert(!thread_ctx->rpc_thread);
	if (thread_ctx->inflight == 0) {
		thread_ctx->status = SUSRES_STATUS_SUSPENDED;
		if (thread_ctx->pt_ch) {
			assert(thread_ctx->pt_ch->base_ch);
			spdk_put_io_channel(thread_ctx->pt_ch->base_ch);
			thread_ctx->pt_ch->base_ch = NULL;
		}
		spdk_thread_send_msg(pt_node->thread, susres_thread_ack, pt_node);
	} else {
		thread_ctx->status = SUSRES_STATUS_SUSPENDING;
		thread_ctx->rpc_thread = pt_node->thread;
	}
}

void
bdev_susres_suspend_disk(const char *vbdev_name,
	susres_rpc_cb cb_fn, void *cb_arg)
{
	struct vbdev_susres *pt_node, *tmp;
	struct spdk_thread *thread;

	pt_node = NULL;
	TAILQ_FOREACH(tmp, &g_pt_nodes, link) {
		if (!strcmp(tmp->pt_bdev.name, vbdev_name)) {
			pt_node = tmp;
		}
	}
	if (!pt_node) {
		SPDK_ERRLOG("Can not find susres vbdev: %s\n", vbdev_name);
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	if (pt_node->status != SUSRES_STATUS_RESUMED) {
		SPDK_ERRLOG("The susres vbdev is not in resumed status: %s %s\n",
			vbdev_name, susres_status_to_string(pt_node->status));
		cb_fn(cb_arg, -EINVAL);
		return;
	}
	pt_node->status = SUSRES_STATUS_SUSPENDING;
	assert(!pt_node->rpc_cb_fn);
	assert(pt_node->ack_cnt == 0);
	thread = spdk_get_thread();
	if (pt_node->thread != thread) {
		SPDK_ERRLOG("different_thread: [%s] [%s]\n",  spdk_thread_get_name(pt_node->thread),
			spdk_thread_get_name(thread));
	}
	assert(pt_node->thread == thread);
	pt_node->rpc_cb_fn = cb_fn;
	pt_node->rpc_arg = cb_arg;
	spdk_for_each_thread(susres_do_suspend, pt_node, susres_msg_cpl);
}

static void
susres_do_resume(void *arg)
{
	struct vbdev_susres *pt_node = arg;
	struct susres_thread_ctx *thread_ctx;
	struct susres_bdev_io *io_ctx;

	thread_ctx = vbdev_susres_get_thread_ctx(pt_node);
	assert(thread_ctx->status == SUSRES_STATUS_SUSPENDED);
	assert(!thread_ctx->rpc_thread);
	thread_ctx->status = SUSRES_STATUS_RESUMED;
	if (thread_ctx->pt_ch) {
		assert(!thread_ctx->pt_ch->base_ch);
		thread_ctx->pt_ch->base_ch = spdk_bdev_get_io_channel(pt_node->base_desc);
	}
	while (!TAILQ_EMPTY(&thread_ctx->io_queue)) {
		io_ctx = TAILQ_FIRST(&thread_ctx->io_queue);
		TAILQ_REMOVE(&thread_ctx->io_queue, io_ctx, link);
		struct spdk_bdev_io *orig_io = SPDK_CONTAINEROF(io_ctx,
			struct spdk_bdev_io, driver_ctx);
		vbdev_susres_submit_request(io_ctx->ch, orig_io);
	}
	spdk_thread_send_msg(pt_node->thread, susres_thread_ack, pt_node);
}

void
bdev_susres_resume_disk(const char *bdev_name, const char *vbdev_name,
	susres_rpc_cb cb_fn, void *cb_arg)
{
	int rc;
	struct vbdev_susres *pt_node, *tmp;
	struct spdk_thread *thread = spdk_get_thread();

	pt_node = NULL;
	TAILQ_FOREACH(tmp, &g_pt_nodes, link) {
		if (!strcmp(tmp->pt_bdev.name, vbdev_name)) {
			pt_node = tmp;
		}
	}
	if (!pt_node) {
		SPDK_ERRLOG("Can not find susres vbdev: %s\n", vbdev_name);
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	if (pt_node->status != SUSRES_STATUS_SUSPENDED) {
		SPDK_ERRLOG("The susres vbdev is not in suspended status: %s %s\n",
			vbdev_name, susres_status_to_string(pt_node->status));
		cb_fn(cb_arg, -EINVAL);
		return;
	}

	rc = susres_open_base_bdev(pt_node, bdev_name);
	if (rc) {
		cb_fn(cb_arg, rc);
		return;
	}

	pt_node->status = SUSRES_STATUS_RESUMING;
	assert(!pt_node->rpc_cb_fn);
	assert(pt_node->ack_cnt == 0);
	assert(pt_node->thread == thread);
	pt_node->rpc_cb_fn = cb_fn;
	pt_node->rpc_arg = cb_arg;
	spdk_for_each_thread(susres_do_resume, pt_node, susres_msg_cpl);
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_susres)
