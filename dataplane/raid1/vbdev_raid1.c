#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/thread.h"

#include "vbdev_raid1.h"

static int raid1_bdev_initialize(void);
static void raid1_bdev_finish(void);
static int raid1_bdev_get_ctx_size(void);
static struct spdk_bdev_module g_raid1_if = {
	.name = "raid1",
	.async_init	= false,
	.module_init = raid1_bdev_initialize,
	.module_fini = raid1_bdev_finish,
	.get_ctx_size = raid1_bdev_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(raid1, &g_raid1_if)

static inline const char *
raid1_bdev_io_type_to_string(enum spdk_bdev_io_type io_type)
{
	switch(io_type) {
	case SPDK_BDEV_IO_TYPE_INVALID:
		return "IO_TYPE_INVALID";
	case SPDK_BDEV_IO_TYPE_READ:
		return "IO_TYPE_READ";
	case SPDK_BDEV_IO_TYPE_WRITE:
		return "IO_TYPE_WRITE";
	case SPDK_BDEV_IO_TYPE_UNMAP:
		return "IO_TYPE_UNMAP";
	case SPDK_BDEV_IO_TYPE_FLUSH:
		return "IO_TYPE_FLUSH";
	case SPDK_BDEV_IO_TYPE_RESET:
		return "IO_TYPE_RESET";
	case SPDK_BDEV_IO_TYPE_NVME_ADMIN:
		return "TYPE_NVME_ADMIN";
	case SPDK_BDEV_IO_TYPE_NVME_IO:
		return "TYPE_NVME_IO";
	case SPDK_BDEV_IO_TYPE_NVME_IO_MD:
		return "NVME_IO_MD";
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		return "TYPE_WRITE_ZEROES";
	case SPDK_BDEV_IO_TYPE_ZCOPY:
		return "IO_TYPE_ZCOPY";
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
		return "IO_TYPE_GET_ZONE_INFO";
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
		return "IO_TYPE_ZONE_MANAGEMENT";
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		return "IO_TYPE_ZONE_APPEND";
	case SPDK_BDEV_IO_TYPE_COMPARE:
		return "IO_TYPE_COMPARE";
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
		return "IO_TYPE_COMPARE_AND_WRITE";
	case SPDK_BDEV_IO_TYPE_ABORT:
		return "IO_TYPE_ABORT";
	default:
		assert(false);
	}
}

#define RAID1_BYTESZ (8)
static inline void raid1_bm_set(uint8_t *bm, int idx)
{
	int pos, offset;
	pos = idx / RAID1_BYTESZ;
	offset = idx % RAID1_BYTESZ;
	bm[pos] |= (0x01 << offset);
}

static inline void raid1_bm_clear(uint8_t *bm, int idx)
{
	int pos, offset;
	pos = idx / RAID1_BYTESZ;
	offset = idx % RAID1_BYTESZ;
	bm[pos] &= ~(0x01 << offset);
}

static inline bool raid1_bm_test(uint8_t *bm, int idx)
{
	int pos, offset;
	pos = idx / RAID1_BYTESZ;
	offset = idx % RAID1_BYTESZ;
	if (((bm[pos] >> offset) & 0x1) == 0x1)
		return true;
	else
		return false;
}

static inline void
raid1_bm_show(uint8_t *bm, int length)
{
	int i, start, step;
	uint8_t buf[8];
	start = 0;
	while (start < length) {
		step = spdk_min(8, length-start);
		for (i = 0; i < step; i++) {
			buf[i] = bm[start+i];
		}
		for (i = step; i < 8; i++) {
			buf[i] = 0;
		}
		SPDK_DEBUGLOG(bdev_raid1, "0x%02x 0x%02x 0x%02x 0x%02x 0x%02x 0x%02x 0x%02x 0x%02x\n",
			buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]);
		start += step;
	}
}

static uint8_t g_raid1_zero_buf[PAGE_SIZE] = {0};
static inline bool
raid1_is_buffer_all_zero(const uint8_t *buf, size_t n)
{
	int i;
	for (i = 0; i < (n/PAGE_SIZE); i++) {
		if (memcmp(buf, g_raid1_zero_buf, PAGE_SIZE)) {
			return false;
		}
		buf += PAGE_SIZE;
	}
	if (memcmp(buf, g_raid1_zero_buf, n%PAGE_SIZE)) {
		return false;
	}
	return true;
}

struct raid1_per_bdev {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *desc;
	uint8_t required_alignment;
	size_t buf_align;
	uint32_t block_size;
	uint64_t num_blocks;
	bool closed;
};

struct raid1_per_thread
{
	struct raid1_per_bdev *per_bdev;
	struct spdk_io_channel *io_channel;
	bool closed;
};

enum raid1_io_type {
	RAID1_IO_READ = 0,
	RAID1_IO_WRITE,
};

typedef void (*raid1_io_cb)(void *arg, int rc);

struct raid1_io_info {
	void *buf;
	uint64_t offset;
	uint64_t nbytes;
	enum raid1_io_type io_type;
};

struct raid1_per_io {
	struct raid1_io_info io_info;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
	struct raid1_per_thread *per_thread;
	raid1_io_cb cb_fn;
	void *cb_arg;
};

struct raid1_iov_info {
	struct iovec *iovs;
	int iovcnt;
	uint64_t offset_blocks;
	uint64_t num_blocks;
	enum raid1_io_type io_type;
};

struct raid1_per_iov {
	struct raid1_iov_info iov_info;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
	struct raid1_per_thread *per_thread;
	raid1_io_cb cb_fn;
	void *cb_arg;
};

static void
raid1_bdev_event_cb(enum spdk_bdev_event_type type,  struct spdk_bdev *bdev,
	void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		/* FIXME support revoving base bdev */
		SPDK_ERRLOG("Removing base bdev: %s\n", bdev->name);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
	return;
}

static int
raid1_per_bdev_open(const char *bdev_name, struct raid1_per_bdev *per_bdev)
{
	int rc;

	rc = spdk_bdev_open_ext(bdev_name, true, raid1_bdev_event_cb, NULL, &per_bdev->desc);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev: %s %s %d\n",
			bdev_name, spdk_strerror(-rc), rc);
		goto err_out;
	}

	per_bdev->bdev = spdk_bdev_desc_get_bdev(per_bdev->desc);

	rc = spdk_bdev_module_claim_bdev(per_bdev->bdev, per_bdev->desc, &g_raid1_if);
	if (rc) {
		SPDK_ERRLOG("Could not claim bdev: %s %s %d\n",
			bdev_name, spdk_strerror(-rc), rc);
		goto close_bdev;
	}

	per_bdev->required_alignment = per_bdev->bdev->required_alignment;
	per_bdev->buf_align = spdk_bdev_get_buf_align(per_bdev->bdev);
	per_bdev->block_size = spdk_bdev_get_block_size(per_bdev->bdev);
	per_bdev->num_blocks = spdk_bdev_get_num_blocks(per_bdev->bdev);
	per_bdev->closed = false;

	return 0;

close_bdev:
	spdk_bdev_close(per_bdev->desc);
err_out:
	per_bdev->closed = true;
	return rc;
}

static void
raid1_per_bdev_close(struct raid1_per_bdev *per_bdev)
{
	if (!per_bdev->closed) {
		spdk_bdev_module_release_bdev(per_bdev->bdev);
		spdk_bdev_close(per_bdev->desc);
		per_bdev->closed = true;
	}
}

static int
raid1_per_thread_open(struct raid1_per_bdev *per_bdev, struct raid1_per_thread *per_thread)
{
	per_thread->per_bdev = per_bdev;
	per_thread->io_channel = spdk_bdev_get_io_channel(per_bdev->desc);
	per_thread->closed = false;
	if (per_thread->io_channel == NULL) {
		SPDK_ERRLOG("Could not open io channel for bdev: %s\n",
			per_bdev->bdev->name);
		per_thread->closed = true;
		return -EIO;
	}
	return 0;
}

static void
raid1_per_thread_close(struct raid1_per_thread *per_thread)
{
	if (!per_thread->closed) {
		per_thread->per_bdev = NULL;
		spdk_put_io_channel(per_thread->io_channel);
		per_thread->closed = true;
	}
}

static inline void
raid1_per_io_init(struct raid1_per_io *per_io, struct raid1_per_thread *per_thread,
	void *buf, uint64_t offset, uint64_t nbytes, enum raid1_io_type io_type,
	raid1_io_cb cb_fn, void *cb_arg)
{
	per_io->per_thread = per_thread;
	per_io->io_info.buf = buf;
	per_io->io_info.offset = offset;
	per_io->io_info.nbytes = nbytes;
	per_io->io_info.io_type = io_type;
	per_io->cb_fn = cb_fn;
	per_io->cb_arg = cb_arg;
}

static void
raid1_per_io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid1_per_io *per_io = cb_arg;
	int rc;

	spdk_bdev_free_io(bdev_io);
	if (success) {
		SPDK_DEBUGLOG(bdev_raid1, "per_io_complete success %s %p\n",
			per_io->per_thread->per_bdev->bdev->name, per_io);
		rc = 0;
	} else {
		SPDK_ERRLOG("per_io_complete error %s %p\n",
			per_io->per_thread->per_bdev->bdev->name, per_io);
		rc = -EIO;
	}
	per_io->cb_fn(per_io->cb_arg, rc);
}

static void
raid1_per_io_submit(void *arg)
{
	struct raid1_per_io *per_io = arg;
	struct raid1_per_thread *per_thread = per_io->per_thread;
	struct raid1_per_bdev *per_bdev = per_thread->per_bdev;
	struct raid1_io_info *io_info = &per_io->io_info;
	struct spdk_bdev_io_wait_entry *bdev_io_wait = &per_io->bdev_io_wait;
	int rc;

	SPDK_DEBUGLOG(bdev_raid1, "per_io_submit %s %p %p %d %" PRIu64 " %" PRIu64 "\n",
		per_bdev->bdev->name, per_io, per_io->cb_arg,
		io_info->io_type, io_info->offset, io_info->nbytes);
	switch (io_info->io_type) {
	case RAID1_IO_READ:
		rc = spdk_bdev_read(per_bdev->desc, per_thread->io_channel, 
			io_info->buf, io_info->offset, io_info->nbytes,
			raid1_per_io_complete, per_io);
		break;
	case RAID1_IO_WRITE:
		rc = spdk_bdev_write(per_bdev->desc, per_thread->io_channel,
			io_info->buf, io_info->offset, io_info->nbytes,
			raid1_per_io_complete, per_io);
		break;
	default:
		assert(false);
		return;
	}

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("per_io_submit queueing %s %p\n",
			per_bdev->bdev->name, per_io);
		bdev_io_wait->bdev = per_bdev->bdev;
		bdev_io_wait->cb_fn = raid1_per_io_submit;
		bdev_io_wait->cb_arg = per_io;
		rc = spdk_bdev_queue_io_wait(per_bdev->bdev,
			per_thread->io_channel, bdev_io_wait);
	}
	if (rc) {
		SPDK_ERRLOG("per_io_submit io err: %s %p %d\n",
			per_bdev->bdev->name, per_io, rc);
		per_io->cb_fn(per_io->cb_arg, rc);
	}
}

static inline void
raid1_per_iov_init(struct raid1_per_iov *per_iov, struct raid1_per_thread *per_thread,
        struct iovec *iovs, int iovcnt, uint64_t offset_blocks, uint64_t num_blocks,
        enum raid1_io_type io_type, raid1_io_cb cb_fn, void *cb_arg)
{
	per_iov->per_thread = per_thread;
	per_iov->iov_info.iovs = iovs;
	per_iov->iov_info.iovcnt = iovcnt;
	per_iov->iov_info.offset_blocks = offset_blocks;
	per_iov->iov_info.num_blocks = num_blocks;
	per_iov->iov_info.io_type = io_type;
	per_iov->cb_fn = cb_fn;
	per_iov->cb_arg = cb_arg;
}

static void
raid1_per_iov_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid1_per_iov *per_iov = cb_arg;
	int rc;

	spdk_bdev_free_io(bdev_io);
	if (success) {
		SPDK_DEBUGLOG(bdev_raid1, "per_iov_complete success %s %p\n",
			per_iov->per_thread->per_bdev->bdev->name, per_iov);
		rc = 0;
	} else {
		SPDK_ERRLOG("per_iov_complete error %s %p\n",
			per_iov->per_thread->per_bdev->bdev->name, per_iov);
		rc = -EIO;
	}
	per_iov->cb_fn(per_iov->cb_arg, rc);
}

static void
raid1_per_iov_submit(void *arg)
{
	struct raid1_per_iov *per_iov = arg;
	struct raid1_per_thread *per_thread = per_iov->per_thread;
	struct raid1_per_bdev *per_bdev = per_thread->per_bdev;
	struct raid1_iov_info *iov_info = &per_iov->iov_info;
	struct spdk_bdev_io_wait_entry *bdev_io_wait = &per_iov->bdev_io_wait;
	int rc;

	SPDK_DEBUGLOG(bdev_raid1, "per_iov_submit %s %p %p %d %" PRIu64 " %" PRIu64 " \n",
		per_bdev->bdev->name, per_iov, per_iov->cb_arg,
		iov_info->io_type, iov_info->offset_blocks, iov_info->num_blocks);

	switch (iov_info->io_type) {
	case RAID1_IO_READ:
		rc = spdk_bdev_readv_blocks(per_bdev->desc, per_thread->io_channel,
			iov_info->iovs, iov_info->iovcnt,
			iov_info->offset_blocks, iov_info->num_blocks,
			raid1_per_iov_complete, per_iov);
		break;
	case RAID1_IO_WRITE:
		rc = spdk_bdev_writev_blocks(per_bdev->desc, per_thread->io_channel,
			iov_info->iovs, iov_info->iovcnt,
			iov_info->offset_blocks, iov_info->num_blocks,
			raid1_per_iov_complete, per_iov);
		break;
	default:
		assert(false);
		return;
	}

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("per_iov_submit queueing %s %p\n",
			per_bdev->bdev->name, per_iov);
		bdev_io_wait->bdev = per_bdev->bdev;
		bdev_io_wait->cb_fn = raid1_per_iov_submit;
		bdev_io_wait->cb_arg = per_iov;
		rc = spdk_bdev_queue_io_wait(per_bdev->bdev,
			per_thread->io_channel, bdev_io_wait);
	}
	if (rc) {
		SPDK_ERRLOG("per_iov_submit io err: %s %p %d\n",
			per_bdev->bdev->name, per_iov, rc);
		per_iov->cb_fn(per_iov->cb_arg, rc);
	}
}

typedef void (*raid1_multi_io_cb)(void *arg, uint8_t err_mask);

struct raid1_multi_io;

struct raid1_io_leg {
	struct raid1_per_io per_io;
	struct raid1_multi_io *multi_io;
	uint8_t idx;
};

struct raid1_multi_io {
	struct raid1_io_leg io_leg[2];
	uint8_t err_mask;
	uint8_t complete_cnt;
	raid1_multi_io_cb cb_fn;
	void *cb_arg;
};

static void
raid1_multi_io_complete(void *arg, int rc)
{
	struct raid1_io_leg *io_leg = arg;
	struct raid1_multi_io *multi_io = io_leg->multi_io;
	if (rc) {
		multi_io->err_mask |= (0x1 << io_leg->idx);
	}
	assert(multi_io->complete_cnt < 2);
	multi_io->complete_cnt++;
	if (multi_io->complete_cnt == 2) {
		multi_io->cb_fn(multi_io->cb_arg, multi_io->err_mask);
	}
}

static void
raid1_multi_io_read(struct raid1_multi_io *multi_io,
        struct raid1_per_thread *per_thread[2],
        uint8_t *buf[2], uint64_t offset, uint64_t nbytes,
        raid1_multi_io_cb cb_fn, void *cb_arg)
{
	struct raid1_io_leg *io_leg;
	uint8_t i;

	multi_io->err_mask = 0;
	multi_io->complete_cnt = 0;
	multi_io->cb_fn = cb_fn;
	multi_io->cb_arg = cb_arg;

	for (i = 0; i < 2; i++) {
		io_leg = &multi_io->io_leg[i];
		io_leg->idx = i;
		io_leg->multi_io = multi_io;
		raid1_per_io_init(&io_leg->per_io, per_thread[i], buf[i],
			offset, nbytes, RAID1_IO_READ,
			raid1_multi_io_complete, io_leg);
		raid1_per_io_submit(&io_leg->per_io);
	}
}

static void
raid1_multi_io_write(struct raid1_multi_io *multi_io,
        struct raid1_per_thread *per_thread[2],
        uint8_t *buf, uint64_t offset, uint64_t nbytes,
        raid1_multi_io_cb cb_fn, void *cb_arg)
{
	struct raid1_io_leg *io_leg;
	uint8_t i;

	multi_io->err_mask = 0;
	multi_io->complete_cnt = 0;
	multi_io->cb_fn = cb_fn;
	multi_io->cb_arg = cb_arg;

	for (i = 0; i < 2; i++) {
		io_leg = &multi_io->io_leg[i];
		io_leg->idx = i;
		io_leg->multi_io = multi_io;
		raid1_per_io_init(&io_leg->per_io, per_thread[i], buf,
			offset, nbytes, RAID1_IO_WRITE,
			raid1_multi_io_complete, io_leg);
		raid1_per_io_submit(&io_leg->per_io);
	}
}

typedef void (*raid1_multi_iov_cb)(void *arg, uint8_t err_mask);

struct raid1_multi_iov;

struct raid1_iov_leg {
	struct raid1_per_iov per_iov;
	struct raid1_multi_iov *multi_iov;
	uint8_t idx;
};

struct raid1_multi_iov {
	struct raid1_iov_leg iov_leg[2];
	uint8_t err_mask;
	uint8_t complete_cnt;
	raid1_multi_iov_cb cb_fn;
	void *cb_arg;
};

static void
raid1_multi_iov_complete(void *arg, int rc)
{
	struct raid1_iov_leg *iov_leg = arg;
	struct raid1_multi_iov *multi_iov = iov_leg->multi_iov;
	if (rc) {
		multi_iov->err_mask |= (0x1 << iov_leg->idx);
	}
	assert(multi_iov->complete_cnt < 2);
	multi_iov->complete_cnt++;
	if (multi_iov->complete_cnt == 2) {
		multi_iov->cb_fn(multi_iov->cb_arg, multi_iov->err_mask);
	}
}

static void
raid1_multi_iov_write(struct raid1_multi_iov *multi_iov,
        struct raid1_per_thread *per_thread[2],
        struct iovec *iovs, int iovcnt, uint64_t offset_blocks, uint64_t num_blocks,
        raid1_multi_iov_cb cb_fn, void *cb_arg)
{
	struct raid1_iov_leg *iov_leg;
	uint8_t i;
	multi_iov->err_mask = 0;
	multi_iov->complete_cnt = 0;
	multi_iov->cb_fn = cb_fn;
	multi_iov->cb_arg = cb_arg;

	for (i = 0; i < 2; i++) {
		iov_leg = &multi_iov->iov_leg[i];
		iov_leg->idx = i;
		iov_leg->multi_iov = multi_iov;
		raid1_per_iov_init(&iov_leg->per_iov, per_thread[i],
			iovs, iovcnt, offset_blocks, num_blocks,
			RAID1_IO_WRITE, raid1_multi_iov_complete, iov_leg);
		raid1_per_iov_submit(&iov_leg->per_iov);
	}
}

typedef int (*raid1_msg_ping)(void *arg);
typedef void (*raid1_msg_pong)(void *arg, int rc);

struct raid1_msg_ctx {
	raid1_msg_ping ping_fn;
	void *ping_arg;
	raid1_msg_pong pong_fn;
	void *pong_arg;
	int rc;
	struct spdk_thread *orig_thread;
};

static void
raid1_msg_pong_wrapper(void *arg)
{
	struct raid1_msg_ctx *msg_ctx = arg;
	msg_ctx->pong_fn(msg_ctx->pong_arg, msg_ctx->rc);
}

static void
raid1_msg_ping_wrapper(void *arg)
{
	struct raid1_msg_ctx *msg_ctx = arg;
	msg_ctx->rc = msg_ctx->ping_fn(msg_ctx->ping_arg);
	spdk_thread_send_msg(msg_ctx->orig_thread, raid1_msg_pong_wrapper, msg_ctx);
}

static void
raid1_msg_submit(struct raid1_msg_ctx *msg_ctx,
        struct spdk_thread *target_thread,
        raid1_msg_ping ping_fn, void *ping_arg,
        raid1_msg_pong pong_fn, void *pong_arg)
{
	msg_ctx->ping_fn = ping_fn;
	msg_ctx->ping_arg = ping_arg;
	msg_ctx->pong_fn = pong_fn;
	msg_ctx->pong_arg = pong_arg;
	msg_ctx->orig_thread = spdk_get_thread();
	spdk_thread_send_msg(target_thread, raid1_msg_ping_wrapper, msg_ctx);
}

#define RAID1_MAX_IO_THREAD (256)
#define RAID1_IO_THREAD_PREFIX "nvmf_tgt_poll_group"

struct raid1_thread_info {
	uint32_t cnt;
	uint32_t idx;
	struct spdk_thread *p[RAID1_MAX_IO_THREAD];
};

static struct raid1_thread_info g_raid1_thread_info = {
	.cnt = 0,
	.idx = 0,
};

static inline struct spdk_thread *
raid1_choose_thread(void)
{
	struct spdk_thread *thread;
	if (g_raid1_thread_info.cnt == 0) {
		int total_thread_cnt = spdk_thread_get_count();
		int i;
		const char *name;
		for(i = 0; i < total_thread_cnt; i++) {
			thread = spdk_thread_get_by_id(i+1);
			name = spdk_thread_get_name(thread);
			if (strncmp(name, RAID1_IO_THREAD_PREFIX,
					strlen(RAID1_IO_THREAD_PREFIX)) == 0) {
				g_raid1_thread_info.p[g_raid1_thread_info.cnt] = thread;
				g_raid1_thread_info.cnt++;
				SPDK_DEBUGLOG(bdev_raid1, "io_thread: %s\n", name);
			}
		}
		assert(g_raid1_thread_info.cnt > 0);
	}
	thread = g_raid1_thread_info.p[g_raid1_thread_info.idx];
	g_raid1_thread_info.idx++;
	g_raid1_thread_info.idx %= g_raid1_thread_info.cnt;
	SPDK_DEBUGLOG(bdev_raid1, "choosing thread: %s\n", spdk_thread_get_name(thread));
	return thread;
}

static int
raid1_bdev_initialize(void)
{
	return 0;
}

static void
raid1_bdev_finish(void)
{
	return;
}

enum raid1_bdev_status {
	RAID1_BDEV_NORMAL = 0,
	RAID1_BDEV_DEGRADED,
	RAID1_BDEV_FAILED,
};

static inline const char *
raid1_bdev_status_to_str(enum raid1_bdev_status status)
{
	switch (status) {
	case RAID1_BDEV_NORMAL:
		return "RAID1_BDEV_NORMAL";
	case RAID1_BDEV_DEGRADED:
		return "RAID1_BDEV_DEGRADED";
	case RAID1_BDEV_FAILED:
		return "RAID1_BDEV_FAILED";
	default:
		assert(false);
	}
}

struct raid1_bdev;

enum raid1_queue_type {
	RAID1_QUEUE_NONE = 0,
	RAID1_QUEUE_SET,
	RAID1_QUEUE_CLEAR,
	RAID1_QUEUE_SB_WRITING,
};

struct raid1_region {
	struct raid1_bdev *r1_bdev;
	struct raid1_multi_io region_io;
	uint64_t delay_cnt;
	uint64_t idx;
	uint8_t *bm_buf;
	uint64_t bdev_offset;
	enum raid1_queue_type queue_type;
	TAILQ_ENTRY(raid1_region) link;
	TAILQ_HEAD(, raid1_bdev_io) delay_queue;
	TAILQ_HEAD(, raid1_bdev_io) bm_writing_queue;
	bool bm_writing;
};

struct raid1_resync_ctx {
	struct raid1_bdev *r1_bdev;
	uint64_t bit_idx;
	uint64_t offset;
	uint64_t nbytes;
	struct raid1_per_io per_io;
	uint8_t *buf;
	TAILQ_ENTRY(raid1_resync_ctx) link;
	TAILQ_HEAD(, raid1_bdev_io) waiting_queue;
};

TAILQ_HEAD(raid1_resync_head, raid1_resync_ctx);

struct raid1_resync {
	uint64_t curr_bit;
	uint64_t num_inflight;
	uint8_t *needed_bm;
	uint8_t *active_bm;
	uint64_t hash_size;
	struct raid1_resync_head *running_hash;
	struct raid1_resync_head *pending_hash;
	struct raid1_resync_ctx *resync_ctx_array;
	struct raid1_resync_head available_queue;
	struct raid1_resync_head read_complete_queue;
	struct raid1_resync_head write_complete_queue;
	struct raid1_resync_head bm_writing_queue;
	bool stopped;
	bool finished;
};

struct raid1_delete_ctx {
    bool deleted;
    struct spdk_thread *orig_thread;
};

#define RAID1_MAX_INFLIGHT_PER_BIT (255)

struct raid1_bdev_io {
	struct spdk_thread *orig_thread;
	enum spdk_bdev_io_status status;
	uint64_t bit_idx;
	uint64_t region_idx;
	uint64_t bit_in_region;
	union {
	    struct {
	        struct raid1_per_iov per_iov;
	        uint8_t read_idx;
	    } read_ctx;
	    struct {
	        struct raid1_multi_iov multi_iov;
	    } write_ctx;
	} u;
	TAILQ_ENTRY(raid1_bdev_io) link;
};

TAILQ_HEAD(raid1_io_head, raid1_bdev_io);

struct raid1_bit_and_region {
	uint64_t bit_cnt;
	uint64_t region_cnt;
};

static inline void
raid1_calc_bit_and_region(uint64_t data_size, uint64_t bit_size, uint64_t *bit_cnt, uint64_t *region_cnt)
{
	*bit_cnt = SPDK_CEIL_DIV(data_size, bit_size);
	*region_cnt = SPDK_CEIL_DIV(*bit_cnt, RAID1_BIT_PER_REGION);
}

static int
raid1_bdev_get_ctx_size(void)
{
	return sizeof(struct raid1_bdev_io);
}

#define RAID1_MAX_INFLIGHT_PER_BIT (255)

struct raid1_bdev {
	char raid1_name[RAID1_MAX_NAME_LEN];
	char bdev0_name[RAID1_MAX_NAME_LEN];
	char bdev1_name[RAID1_MAX_NAME_LEN];
	struct spdk_bdev bdev;
	struct raid1_per_bdev per_bdev[2];
	struct raid1_per_thread per_thread[2];
	struct raid1_per_thread *per_thread_ptr[2];
	size_t buf_align;
	uint8_t *sb_buf;
	uint64_t start_blocks;
	struct raid1_sb *sb;
	struct raid1_multi_io sb_io;
	uint64_t bit_size;
	uint64_t write_delay;
	uint64_t clean_ratio;
	uint64_t max_delay;
	uint64_t max_resync;
	uint64_t region_size;
	uint64_t region_cnt;
	uint64_t bit_cnt;
	uint64_t bm_size;
	uint8_t *bm_buf;
	uint8_t *inflight_cnt;
	struct raid1_region *regions;
	struct raid1_resync *resync;
	bool sb_writing;
	bool sb_sync;
	bool multi;
	bool ignore_zero_block;
	uint64_t bm_io_cnt;
	uint64_t data_io_cnt;
	uint64_t read_delivered;
	uint64_t write_delivered;
	uint64_t clean_counter;
	uint64_t poller_counter;
	struct spdk_thread *r1_thread;
	enum raid1_bdev_status status;
	uint8_t health_idx;
	bool online[2];
	uint8_t read_idx;
	uint32_t pending_close;
	TAILQ_ENTRY(raid1_bdev) link;
	TAILQ_HEAD(, raid1_region) set_queue;
	TAILQ_HEAD(, raid1_region) clear_queue;
	TAILQ_HEAD(, raid1_region) sb_region_queue;
	TAILQ_HEAD(, raid1_bdev_io) sb_io_queue;
	uint64_t pending_hash_size;
	struct raid1_io_head *pending_io_hash;
	struct spdk_poller *poller;
	struct raid1_delete_ctx delete_ctx;
};

static TAILQ_HEAD(, raid1_bdev) g_raid1_bdev_head = TAILQ_HEAD_INITIALIZER(g_raid1_bdev_head);

static inline struct raid1_bdev *
raid1_find_by_name(const char *raid1_name)
{
	struct raid1_bdev *r1_bdev;
	TAILQ_FOREACH(r1_bdev, &g_raid1_bdev_head, link) {
		if (!strncmp(r1_bdev->raid1_name, raid1_name, RAID1_MAX_NAME_LEN)) {
			return r1_bdev;
		}
	}
	return NULL;
}

static inline struct raid1_bdev *
raid1_find_and_remove(const char *raid1_name)
{
	struct raid1_bdev *r1_bdev;
	r1_bdev = raid1_find_by_name(raid1_name);
	if (r1_bdev) {
		TAILQ_REMOVE(&g_raid1_bdev_head, r1_bdev, link);
	}
	return r1_bdev;
}

static inline void
raid1_delete_from_queue(struct raid1_bdev *r1_bdev)
{
	TAILQ_REMOVE(&g_raid1_bdev_head, r1_bdev, link);
}

static void raid1_io_failed(struct raid1_bdev_io *raid1_io);
static void raid1_write_complete_hook(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io);
static void raid1_bdev_write_handler(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io);
static void raid1_bdev_read_hander(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io);
static void raid1_deliver_region_degraded(struct raid1_bdev *r1_bdev, struct raid1_region *region);
static void raid1_update_status(struct raid1_bdev *r1_bdev);
static void raid1_resync_free(struct raid1_bdev *r1_bdev);
static void raid1_bdev_release_in_thread(struct raid1_bdev *r1_bdev);
static void raid1_release_ack_send(struct raid1_bdev *r1_bdev);
static int raid1_io_poller(void *arg);

static uint8_t
raid1_read_choose_bdev(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	if (r1_bdev->status == RAID1_BDEV_DEGRADED) {
		return r1_bdev->health_idx;
	}
	if (raid1_bm_test(r1_bdev->resync->needed_bm,
			raid1_io->bit_idx)) {
		return 0;
	}
	r1_bdev->read_idx = 1 - r1_bdev->read_idx;
	return r1_bdev->read_idx;
}

static inline void
raid1_sb_counter_add(struct raid1_sb *sb, uint64_t n)
{
	uint64_t counter = from_le64(&sb->counter);
	counter += n;
	to_le64(&sb->counter, counter);
}

static inline void
raid1_resync_hash_add(struct raid1_resync_head *hash, uint64_t hash_size, struct raid1_resync_ctx *resync_ctx)
{
	uint64_t key = resync_ctx->bit_idx % hash_size;
	struct raid1_resync_head *resync_head = &hash[key];
	TAILQ_INSERT_TAIL(resync_head, resync_ctx, link);
}

static inline void
raid1_resync_hash_del(struct raid1_resync_head *hash, uint64_t hash_size, struct raid1_resync_ctx *resync_ctx)
{
	uint64_t key = resync_ctx->bit_idx % hash_size;
	struct raid1_resync_head *resync_head = &hash[key];
	TAILQ_REMOVE(resync_head, resync_ctx, link);
}

static inline struct raid1_resync_ctx *
raid1_resync_hash_get(struct raid1_resync_head *hash, uint64_t hash_size, uint64_t bit_idx)
{
	uint64_t key = bit_idx % hash_size;
	struct raid1_resync_head *resync_head = &hash[key];
	struct raid1_resync_ctx *tmp, *resync_ctx;

	resync_ctx = NULL;
	TAILQ_FOREACH(tmp, resync_head, link) {
		if (tmp->bit_idx == bit_idx) {
			resync_ctx = tmp;
			break;
		}
	}
	return resync_ctx;
}

static inline void
raid1_resync_flush_io(struct raid1_bdev *r1_bdev, struct raid1_resync_ctx *resync_ctx)
{
	struct raid1_bdev_io *raid1_io;
	while (!TAILQ_EMPTY(&resync_ctx->waiting_queue)) {
		raid1_io = TAILQ_FIRST(&resync_ctx->waiting_queue);
		TAILQ_REMOVE(&resync_ctx->waiting_queue, raid1_io, link);
		raid1_bdev_write_handler(r1_bdev, raid1_io);
	}
}

static inline void
raid1_resync_bm_writing_hook(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	struct raid1_resync *resync = r1_bdev->resync;
	struct raid1_resync_ctx *resync_ctx, *tmp;
	TAILQ_FOREACH_SAFE(resync_ctx, &resync->bm_writing_queue, link, tmp) {
		uint64_t region_idx = resync_ctx->bit_idx / (PAGE_SIZE * RAID1_BYTESZ);
		if (region_idx == region->idx) {
			uint64_t idx_in_region = resync_ctx->bit_idx % (PAGE_SIZE * RAID1_BYTESZ);
			uint8_t inflight_cnt = r1_bdev->inflight_cnt[resync_ctx->bit_idx];
			TAILQ_REMOVE(&resync->bm_writing_queue, resync_ctx, link);
			assert(raid1_bm_test(resync->needed_bm, resync_ctx->bit_idx));
			assert(raid1_bm_test(region->bm_buf, idx_in_region));
			raid1_bm_clear(resync->needed_bm, resync_ctx->bit_idx);
			if (inflight_cnt == 0) {
				raid1_bm_clear(region->bm_buf, idx_in_region);
				if (region->queue_type == RAID1_QUEUE_NONE) {
					region->queue_type = RAID1_QUEUE_CLEAR;
					// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
					TAILQ_INSERT_TAIL(&r1_bdev->clear_queue, region, link);
				}
			}
			TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
			assert(resync->num_inflight > 0);
			resync->num_inflight--;
		}
	}
}

static void
raid1_resync_write_complete(void *arg, int rc)
{
	struct raid1_resync_ctx *resync_ctx = arg;
	struct raid1_bdev *r1_bdev = resync_ctx->r1_bdev;
	struct raid1_resync *resync = r1_bdev->resync;
	SPDK_DEBUGLOG(bdev_raid1, "raid1_resync_write_complete %s %" PRIu64 " %d\n",
		r1_bdev->raid1_name, resync_ctx->bit_idx, rc);
	raid1_resync_hash_del(resync->running_hash, resync->hash_size, resync_ctx);
	assert(raid1_bm_test(resync->active_bm, resync_ctx->bit_idx));
	raid1_bm_clear(resync->active_bm, resync_ctx->bit_idx);
	if (rc) {
		if (r1_bdev->sb_writing) {
			TAILQ_INSERT_TAIL(&resync->write_complete_queue, resync_ctx, link);
		} else {
			if (r1_bdev->online[1] == true) {
				r1_bdev->online[1] = false;
				raid1_update_status(r1_bdev);
			}
			TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
			resync->num_inflight--;
		}
	} else {
		uint64_t region_idx = resync_ctx->bit_idx / (PAGE_SIZE * RAID1_BYTESZ);
		uint64_t idx_in_region = resync_ctx->bit_idx % (PAGE_SIZE * RAID1_BYTESZ);
		uint8_t inflight_cnt = r1_bdev->inflight_cnt[resync_ctx->bit_idx];
		struct raid1_region *region = &r1_bdev->regions[region_idx];
		if (region->bm_writing) {
			TAILQ_INSERT_TAIL(&resync->bm_writing_queue, resync_ctx, link);
		} else {
			assert(raid1_bm_test(resync->needed_bm, resync_ctx->bit_idx));
			assert(raid1_bm_test(region->bm_buf, idx_in_region));
			raid1_bm_clear(resync->needed_bm, resync_ctx->bit_idx);
			if (inflight_cnt == 0 && r1_bdev->status == RAID1_BDEV_NORMAL) {
				raid1_bm_clear(region->bm_buf, idx_in_region);
				if (region->queue_type == RAID1_QUEUE_NONE) {
					region->queue_type = RAID1_QUEUE_CLEAR;
					// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
					TAILQ_INSERT_TAIL(&r1_bdev->clear_queue, region, link);
				}
			}
			TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
			resync->num_inflight--;
		}
	}
	raid1_resync_flush_io(r1_bdev, resync_ctx);
}

static void
raid1_resync_read_complete(void *arg, int rc)
{
	struct raid1_resync_ctx *resync_ctx = arg;
	struct raid1_bdev *r1_bdev = resync_ctx->r1_bdev;
	struct raid1_resync *resync = r1_bdev->resync;
	SPDK_DEBUGLOG(bdev_raid1, "raid1_resync_read_complete %s %" PRIu64 " %d\n",
		r1_bdev->raid1_name, resync_ctx->bit_idx, rc);
	if (rc) {
		raid1_resync_hash_del(resync->running_hash, resync->hash_size, resync_ctx);
		assert(raid1_bm_test(resync->active_bm, resync_ctx->bit_idx));
		raid1_bm_clear(resync->active_bm, resync_ctx->bit_idx);
		if (r1_bdev->sb_writing) {
			TAILQ_INSERT_TAIL(&resync->read_complete_queue, resync_ctx, link);
		} else {
			if (r1_bdev->online[0] == true) {
				r1_bdev->online[0] = false;
				raid1_update_status(r1_bdev);
			}
			TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
			resync->num_inflight--;
		}
		raid1_resync_flush_io(r1_bdev, resync_ctx);
	} else {
		if (r1_bdev->ignore_zero_block && raid1_is_buffer_all_zero(resync_ctx->buf, resync_ctx->nbytes)) {
			raid1_resync_write_complete(resync_ctx, 0);
		} else {
			struct raid1_per_io *per_io = &resync_ctx->per_io;
			struct raid1_per_thread *per_thread = r1_bdev->per_thread_ptr[1];
			assert(raid1_bm_test(resync->needed_bm, resync_ctx->bit_idx));
			assert(raid1_bm_test(resync->active_bm, resync_ctx->bit_idx));
			assert(r1_bdev->inflight_cnt[resync_ctx->bit_idx] == 0);
			raid1_per_io_init(per_io, per_thread, resync_ctx->buf, resync_ctx->offset,
				resync_ctx->nbytes, RAID1_IO_WRITE,
				raid1_resync_write_complete, resync_ctx);
			raid1_per_io_submit(per_io);
		}
	}
}

static void
raid1_resync_handler(struct raid1_bdev *r1_bdev,
        struct raid1_resync *resync, struct raid1_resync_ctx *resync_ctx)
{
	struct raid1_per_io *per_io = &resync_ctx->per_io;
	struct raid1_per_thread *per_thread = r1_bdev->per_thread_ptr[0];
	SPDK_DEBUGLOG(bdev_raid1, "raid1_resync_handler %s %" PRIu64 "\n",
		r1_bdev->raid1_name, resync_ctx->bit_idx);
	assert(raid1_bm_test(resync->needed_bm, resync_ctx->bit_idx));
	assert(!raid1_bm_test(resync->active_bm, resync_ctx->bit_idx));
	raid1_bm_set(resync->active_bm, resync_ctx->bit_idx);
	resync_ctx->offset = from_le64(&r1_bdev->sb->meta_size) + r1_bdev->bit_size * resync_ctx->bit_idx;
	if (resync_ctx->bit_idx == r1_bdev->bit_cnt - 1) {
		/* In the last bit might be smaller than the bit size */
		uint64_t whole_size = from_le64(&r1_bdev->sb->meta_size) + from_le64(&r1_bdev->sb->data_size);
		resync_ctx->nbytes = spdk_min(r1_bdev->bit_size, whole_size - resync_ctx->offset);
	} else {
		resync_ctx->nbytes = r1_bdev->bit_size;
	}
	raid1_per_io_init(per_io, per_thread, resync_ctx->buf, resync_ctx->offset,
		resync_ctx->nbytes, RAID1_IO_READ,
		raid1_resync_read_complete, resync_ctx);
	raid1_per_io_submit(per_io);
}

static void
raid1_set_delete_flag(void *ctx)
{
	struct raid1_bdev *r1_bdev = ctx;
	r1_bdev->delete_ctx.deleted = true;
}

static inline void
raid1_bdev_failed_hook(struct raid1_bdev *r1_bdev)
{
	struct raid1_bdev_io *raid1_io;
	struct raid1_region *region;
	while (!TAILQ_EMPTY(&r1_bdev->sb_io_queue)) {
		raid1_io = TAILQ_FIRST(&r1_bdev->sb_io_queue);
		TAILQ_REMOVE(&r1_bdev->sb_io_queue, raid1_io, link);
		raid1_io_failed(raid1_io);
	}
	while (!TAILQ_EMPTY(&r1_bdev->sb_region_queue)) {
		region = TAILQ_FIRST(&r1_bdev->sb_region_queue);
		TAILQ_REMOVE(&r1_bdev->sb_region_queue, region, link);
		while (!TAILQ_EMPTY(&region->delay_queue)) {
			raid1_io = TAILQ_FIRST(&region->delay_queue);
			TAILQ_REMOVE(&region->delay_queue, raid1_io, link);
			raid1_write_complete_hook(r1_bdev, raid1_io);
			raid1_io_failed(raid1_io);
		}
	}
}

static void
raid1_update_sb_complete(void *arg, int rc)
{
	struct raid1_bdev *r1_bdev = arg;
	assert(r1_bdev->sb_writing);
	assert(r1_bdev->resync->stopped);
	r1_bdev->sb_writing = false;
	if (rc) {
		r1_bdev->online[0] = false;
		r1_bdev->online[1] = false;
		r1_bdev->status = RAID1_BDEV_FAILED;
		raid1_bdev_failed_hook(r1_bdev);
	} else {
		struct raid1_resync *resync = r1_bdev->resync;
		struct raid1_resync_ctx *resync_ctx;
		while (!TAILQ_EMPTY(&resync->read_complete_queue)) {
			resync_ctx = TAILQ_FIRST(&resync->read_complete_queue);
			TAILQ_REMOVE(&resync->read_complete_queue, resync_ctx, link);
			if (r1_bdev->online[0] == true) {
				r1_bdev->online[0] = false;
				raid1_update_status(r1_bdev);
			}
			TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
			resync->num_inflight--;
		}
		while (!TAILQ_EMPTY(&resync->write_complete_queue)) {
			resync_ctx = TAILQ_FIRST(&resync->write_complete_queue);
			TAILQ_REMOVE(&resync->write_complete_queue, resync_ctx, link);
			if (r1_bdev->online[1] == true) {
				r1_bdev->online[1] = false;
				raid1_update_status(r1_bdev);
			}
			TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
			resync->num_inflight--;
		}
		struct raid1_bdev_io *raid1_io;
		while (!TAILQ_EMPTY(&r1_bdev->sb_io_queue)) {
			raid1_io = TAILQ_FIRST(&r1_bdev->sb_io_queue);
			TAILQ_REMOVE(&r1_bdev->sb_io_queue, raid1_io, link);
			raid1_bdev_write_handler(r1_bdev, raid1_io);
		}
		while (!TAILQ_EMPTY(&r1_bdev->sb_region_queue)) {
			struct raid1_region *region = TAILQ_FIRST(&r1_bdev->sb_region_queue);
			TAILQ_REMOVE(&r1_bdev->sb_region_queue, region, link);
			assert(region->queue_type == RAID1_QUEUE_SB_WRITING);
			region->queue_type = RAID1_QUEUE_NONE;
			// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
			raid1_deliver_region_degraded(r1_bdev, region);
		}
		raid1_io_poller(r1_bdev);
	}
}

static void
raid1_syncup_sb_complete(void *arg, uint8_t err_mask)
{
	struct raid1_bdev *r1_bdev = arg;
	assert(r1_bdev->sb_writing);
	r1_bdev->sb_writing = false;
	assert(!r1_bdev->sb_sync);
	r1_bdev->sb_sync = true;
	if (err_mask) {
		bool status_changed = false;
		if ((err_mask & 0x1) && (r1_bdev->online[0] == true)) {
			r1_bdev->online[0] = false;
			status_changed = true;
		}
		if ((err_mask & 0x2) && (r1_bdev->online[1] == true)) {
			r1_bdev->online[1] = false;
			status_changed = true;
		}
		if (status_changed) {
			raid1_update_status(r1_bdev);
		}
	} else {
		assert(r1_bdev->resync->stopped);
		assert(TAILQ_EMPTY(&r1_bdev->sb_io_queue));
		TAILQ_EMPTY(&r1_bdev->sb_region_queue);
		raid1_io_poller(r1_bdev);
	}
}

static void
raid1_syncup_sb(struct raid1_bdev *r1_bdev)
{
	raid1_sb_counter_add(r1_bdev->sb, 1);
	SPDK_DEBUGLOG(bdev_raid1, "sb_counter: %s %" PRIu64 "\n",
		r1_bdev->raid1_name, from_le64(&r1_bdev->sb->counter));
	assert(!r1_bdev->sb_writing);
	r1_bdev->sb_writing = true;
	raid1_multi_io_write(&r1_bdev->sb_io, r1_bdev->per_thread_ptr,
		r1_bdev->sb_buf, RAID1_SB_START_BYTE, RAID1_SB_SIZE,
		raid1_syncup_sb_complete, r1_bdev);
}

static void
raid1_update_sb(struct raid1_bdev *r1_bdev)
{
	struct raid1_per_thread *per_thread = r1_bdev->per_thread_ptr[r1_bdev->health_idx];
	struct raid1_per_io *per_io = &r1_bdev->sb_io.io_leg[r1_bdev->health_idx].per_io;
	raid1_sb_counter_add(r1_bdev->sb, 1);
	assert(!r1_bdev->sb_writing);
	r1_bdev->sb_writing = true;
	raid1_per_io_init(per_io, per_thread, r1_bdev->sb_buf, RAID1_SB_START_BYTE,
		RAID1_SB_SIZE, RAID1_IO_WRITE, raid1_update_sb_complete, r1_bdev);
	raid1_per_io_submit(per_io);
}

static void
raid1_close_bdev_done(void *ctx)
{
	struct raid1_bdev *r1_bdev = ctx;
	assert(r1_bdev->pending_close > 0);
	r1_bdev->pending_close--;
}

static void
raid1_close_bdev(struct raid1_bdev *r1_bdev, uint8_t idx)
{
	raid1_per_bdev_close(&r1_bdev->per_bdev[idx]);
	spdk_thread_send_msg(r1_bdev->r1_thread, raid1_close_bdev_done, r1_bdev);
}

static void
raid1_close_bdev0(void *ctx)
{
	struct raid1_bdev *r1_bdev = ctx;
	raid1_close_bdev(r1_bdev, 0);
}

static void
raid1_close_bdev1(void *ctx)
{
	struct raid1_bdev *r1_bdev = ctx;
	raid1_close_bdev(r1_bdev, 1);
}

static void
raid1_update_status(struct raid1_bdev *r1_bdev)
{
	assert(!r1_bdev->online[0] || !r1_bdev->online[1]);
	if (!r1_bdev->online[0] && !r1_bdev->online[1]) {
		SPDK_ERRLOG("Two underling bdevs failed: %s\n",
			r1_bdev->raid1_name);
		r1_bdev->status = RAID1_BDEV_FAILED;
		r1_bdev->resync->stopped = true;
		raid1_bdev_failed_hook(r1_bdev);
		goto close_per_thread;
	}
	if (!r1_bdev->online[0] && !r1_bdev->resync->stopped) {
		SPDK_ERRLOG("Primary failed during sync: %s\n",
			r1_bdev->raid1_name);
		assert(r1_bdev->status == RAID1_BDEV_NORMAL);
		r1_bdev->online[0] = false;
		r1_bdev->status = RAID1_BDEV_FAILED;
		r1_bdev->resync->stopped = true;
		raid1_bdev_failed_hook(r1_bdev);
		goto close_per_thread;
	}
	if (r1_bdev->online[0]) {
		r1_bdev->health_idx = 0;
	} else {
		r1_bdev->health_idx = 1;
	}
	assert(r1_bdev->status == RAID1_BDEV_NORMAL);
	SPDK_ERRLOG("One bdev failed: %s %d %d\n",
		r1_bdev->raid1_name, r1_bdev->online[0], r1_bdev->online[1]);
	r1_bdev->status = RAID1_BDEV_DEGRADED;
	r1_bdev->resync->stopped = true;
	raid1_update_sb(r1_bdev);
close_per_thread:
	/* if (!r1_bdev->online[0]) { */
	/* 	raid1_per_thread_close(&r1_bdev->per_thread[0]); */
	/* 	r1_bdev->pending_close++; */
	/* 	spdk_thread_send_msg(r1_bdev->delete_ctx.orig_thread, */
	/* 		raid1_close_bdev0, r1_bdev); */
	/* } */
	/* if (!r1_bdev->online[1]) { */
	/* 	raid1_per_thread_close(&r1_bdev->per_thread[1]); */
	/* 	r1_bdev->pending_close++; */
	/* 	spdk_thread_send_msg(r1_bdev->delete_ctx.orig_thread, */
	/* 		raid1_close_bdev1, r1_bdev); */
	/* } */
}

static int
raid1_bdev_destruct(void *ctx)
{
	struct raid1_bdev *r1_bdev = ctx;
	assert(r1_bdev->delete_ctx.orig_thread == spdk_get_thread());
	spdk_thread_send_msg(r1_bdev->r1_thread, raid1_set_delete_flag, r1_bdev);
	return 1;
}

static inline struct raid1_region *
raid1_bit_to_region(struct raid1_bdev *r1_bdev, uint64_t bit_idx)
{
	uint64_t region_idx = bit_idx / RAID1_BIT_PER_REGION;
	return &r1_bdev->regions[region_idx];
}

static inline void
raid1_init_pos(struct raid1_bdev *r1_bdev, struct spdk_bdev_io *bdev_io,
        struct raid1_bdev_io *raid1_io)
{
	uint64_t offset = bdev_io->u.bdev.offset_blocks * r1_bdev->bdev.blocklen;
	raid1_io->bit_idx = offset / r1_bdev->bit_size;
	assert(raid1_io->bit_idx < r1_bdev->bit_cnt);
	raid1_io->region_idx = offset / r1_bdev->region_size;
	raid1_io->bit_in_region = raid1_io->bit_idx % (RAID1_BYTESZ * PAGE_SIZE);
	SPDK_DEBUGLOG(bdev_raid1, "raid1_init_pos %s %p %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
		r1_bdev->raid1_name, raid1_io,
		raid1_io->bit_idx, raid1_io->region_idx, raid1_io->bit_in_region);
}

static void
raid1_bdev_io_complete(void *ctx)
{
	struct raid1_bdev_io *raid1_io = ctx;
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	SPDK_DEBUGLOG(bdev_raid1, "io_complete: %p %p %d\n",
		bdev_io, raid1_io, raid1_io->status);
	spdk_bdev_io_complete(bdev_io, raid1_io->status);
}

static void
raid1_read_complete_on_success(struct raid1_bdev_io *raid1_io)
{
	raid1_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	spdk_thread_send_msg(raid1_io->orig_thread, raid1_bdev_io_complete, raid1_io);
}

static void
raid1_read_complete_on_err(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	if (r1_bdev->online[raid1_io->u.read_ctx.read_idx]) {
		r1_bdev->online[raid1_io->u.read_ctx.read_idx] = false;
		raid1_update_status(r1_bdev);
	}
	raid1_bdev_read_hander(r1_bdev, raid1_io);
}

static void
raid1_read_complete(void *arg, int rc)
{
	struct raid1_bdev_io *raid1_io = arg;
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);
	r1_bdev->data_io_cnt--;
	if (rc) {
		raid1_read_complete_on_err(r1_bdev, raid1_io);
	} else {
		raid1_read_complete_on_success(raid1_io);
	}
}

static void
raid1_read_deliver(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	raid1_io->u.read_ctx.read_idx = raid1_read_choose_bdev(r1_bdev, raid1_io);
	struct raid1_per_thread *per_thread = r1_bdev->per_thread_ptr[raid1_io->u.read_ctx.read_idx];
	struct raid1_per_iov *per_iov = &raid1_io->u.read_ctx.per_iov;
	r1_bdev->data_io_cnt++;
	r1_bdev->read_delivered++;
	raid1_per_iov_init(per_iov, per_thread,
		bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
		bdev_io->u.bdev.offset_blocks+r1_bdev->start_blocks, bdev_io->u.bdev.num_blocks,
		RAID1_IO_READ, raid1_read_complete, raid1_io);
	raid1_per_iov_submit(per_iov);
}

static void
raid1_io_failed(struct raid1_bdev_io *raid1_io)
{
	raid1_io->status = SPDK_BDEV_IO_STATUS_FAILED;
	spdk_thread_send_msg(raid1_io->orig_thread, raid1_bdev_io_complete, raid1_io);
}

static void
raid1_io_success(struct raid1_bdev_io *raid1_io)
{
	raid1_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	spdk_thread_send_msg(raid1_io->orig_thread, raid1_bdev_io_complete, raid1_io);
}

static void
raid1_bdev_read_hander(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	if (r1_bdev->delete_ctx.deleted) {
		raid1_io_failed(raid1_io);
		return;
	}
	if (r1_bdev->status == RAID1_BDEV_FAILED) {
		raid1_io_failed(raid1_io);
		return;
	}
	raid1_read_deliver(r1_bdev, raid1_io);
}

static void
raid1_bdev_read(void *ctx)
{
	struct raid1_bdev_io *raid1_io = ctx;
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);
	raid1_init_pos(r1_bdev, bdev_io, raid1_io);
	raid1_bdev_read_hander(r1_bdev, raid1_io);
}

static inline void
raid1_write_sb_pending(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	TAILQ_INSERT_TAIL(&r1_bdev->sb_io_queue, raid1_io, link);
}

static inline void
raid1_write_resync_pending(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	struct raid1_resync *resync = r1_bdev->resync;
	struct raid1_resync_ctx *resync_ctx;
	resync_ctx = raid1_resync_hash_get(
		resync->running_hash, resync->hash_size, raid1_io->bit_idx);
	assert(resync_ctx != NULL);
	TAILQ_INSERT_TAIL(&resync_ctx->waiting_queue, raid1_io, link);
}

static inline void
raid1_write_bm_writing_pending(struct raid1_region *region, struct raid1_bdev_io *raid1_io)
{
	TAILQ_INSERT_TAIL(&region->bm_writing_queue, raid1_io, link);
}

static inline void
raid1_write_delay(struct raid1_bdev *r1_bdev,
        struct raid1_region *region, struct raid1_bdev_io *raid1_io)
{
	r1_bdev->inflight_cnt[raid1_io->bit_idx]++;
	r1_bdev->data_io_cnt++;
	r1_bdev->write_delivered++;
	region->delay_cnt++;
	if (r1_bdev->inflight_cnt[raid1_io->bit_idx] > 1) {
		assert(raid1_bm_test(region->bm_buf, raid1_io->bit_in_region));
	} else {
		if (raid1_bm_test(r1_bdev->resync->needed_bm, raid1_io->bit_idx)) {
			assert(raid1_bm_test(region->bm_buf, raid1_io->bit_in_region));
		} else {
			if (r1_bdev->status == RAID1_BDEV_NORMAL) {
				assert(!raid1_bm_test(region->bm_buf, raid1_io->bit_in_region));
			}
			raid1_bm_set(region->bm_buf, raid1_io->bit_in_region);
		}
	}
	TAILQ_INSERT_TAIL(&region->delay_queue, raid1_io, link);
	switch (region->queue_type) {
	case RAID1_QUEUE_NONE:
		TAILQ_INSERT_TAIL(&r1_bdev->set_queue, region, link);
		region->queue_type = RAID1_QUEUE_SET;
		// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
		break;
	case RAID1_QUEUE_CLEAR:
		TAILQ_REMOVE(&r1_bdev->clear_queue, region, link);
		TAILQ_INSERT_TAIL(&r1_bdev->set_queue, region, link);
		region->queue_type = RAID1_QUEUE_SET;
		// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
		break;
	case RAID1_QUEUE_SET:
	case RAID1_QUEUE_SB_WRITING:
		break;
	default:
		assert(false);
	}
}

static inline void
raid1_write_inflight_pending(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	uint64_t key = raid1_io->bit_idx % r1_bdev->pending_hash_size;
	struct raid1_io_head *io_head = &r1_bdev->pending_io_hash[key];
	TAILQ_INSERT_TAIL(io_head, raid1_io, link);
}

static void
raid1_abort_region(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	struct raid1_bdev_io *raid1_io;
	while (!TAILQ_EMPTY(&region->delay_queue)) {
		r1_bdev->data_io_cnt--;
		raid1_io = TAILQ_FIRST(&region->delay_queue);
		TAILQ_REMOVE(&region->delay_queue, raid1_io, link);
		raid1_io_failed(raid1_io);
	}
}

static void
raid1_write_complete_hook(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	uint8_t inflight_cnt = r1_bdev->inflight_cnt[raid1_io->bit_idx];
	struct raid1_resync *resync = r1_bdev->resync;
	assert(inflight_cnt > 0);
	inflight_cnt--;
	r1_bdev->inflight_cnt[raid1_io->bit_idx] = inflight_cnt;
	assert(r1_bdev->data_io_cnt > 0);
	r1_bdev->data_io_cnt--;
	if (inflight_cnt == (RAID1_MAX_INFLIGHT_PER_BIT - 1)) {
		uint64_t key = raid1_io->bit_idx % r1_bdev->pending_hash_size;
		struct raid1_io_head *io_head = &r1_bdev->pending_io_hash[key];
		struct raid1_bdev_io *tmp, *next_raid1_io;
		next_raid1_io = NULL;
		TAILQ_FOREACH_SAFE(next_raid1_io, io_head, link, tmp) {
			if (next_raid1_io->bit_idx == raid1_io->bit_idx) {
				TAILQ_REMOVE(io_head, next_raid1_io, link);
				raid1_bdev_write_handler(r1_bdev, next_raid1_io);
				if (r1_bdev->inflight_cnt[raid1_io->bit_idx] == RAID1_MAX_INFLIGHT_PER_BIT) {
					break;
				}
			}
		}
	} else if (inflight_cnt == 0) {
		struct raid1_resync_ctx *resync_ctx;
		resync_ctx = raid1_resync_hash_get(resync->pending_hash, resync->hash_size, raid1_io->bit_idx);
		if (resync_ctx) {
			SPDK_DEBUGLOG(bdev_raid1, "Handle resync in write complete hook\n");
			assert(raid1_bm_test(resync->needed_bm, raid1_io->bit_idx));
			raid1_resync_hash_del(resync->pending_hash, resync->hash_size, resync_ctx);
			raid1_resync_hash_add(resync->running_hash, resync->hash_size, resync_ctx);
			raid1_resync_handler(r1_bdev, resync, resync_ctx);
		}
		struct raid1_region *region = &r1_bdev->regions[raid1_io->region_idx];
		if (!raid1_bm_test(resync->needed_bm, raid1_io->bit_idx) &&
			r1_bdev->status == RAID1_BDEV_NORMAL) {
			assert(!raid1_bm_test(resync->active_bm, raid1_io->bit_idx));
			assert(raid1_bm_test(region->bm_buf, raid1_io->bit_in_region));
			raid1_bm_clear(region->bm_buf, raid1_io->bit_in_region);
			if (region->queue_type == RAID1_QUEUE_NONE) {
				TAILQ_INSERT_TAIL(&r1_bdev->clear_queue, region, link);
				region->queue_type = RAID1_QUEUE_CLEAR;
				// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
			}
		}
	}
}

static void
raid1_deliver_region_degraded_complete(void *ctx, int rc)
{
	struct raid1_bdev_io *raid1_io = ctx;
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);
	if (rc) {
		if (r1_bdev->online[r1_bdev->health_idx] == true) {
			assert(r1_bdev->status == RAID1_BDEV_DEGRADED);
			r1_bdev->online[r1_bdev->health_idx] = false;
			TAILQ_INSERT_TAIL(&r1_bdev->sb_io_queue, raid1_io, link);
			raid1_update_status(r1_bdev);
		} else {
			assert(r1_bdev->status == RAID1_BDEV_FAILED);
			raid1_write_complete_hook(r1_bdev, raid1_io);
			raid1_io_failed(raid1_io);
		}
	} else {
		raid1_write_complete_hook(r1_bdev, raid1_io);
		raid1_io_success(raid1_io);
	}
}

static void
raid1_deliver_region_degraded(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	struct raid1_bdev_io *raid1_io;
	struct raid1_per_thread *per_thread = r1_bdev->per_thread_ptr[r1_bdev->health_idx];
	while (!TAILQ_EMPTY(&region->delay_queue)) {
		raid1_io = TAILQ_FIRST(&region->delay_queue);
		TAILQ_REMOVE(&region->delay_queue, raid1_io, link);
		struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
			struct spdk_bdev_io, driver_ctx);
		struct raid1_per_iov *per_iov = &raid1_io->u.write_ctx.multi_iov.iov_leg[r1_bdev->health_idx].per_iov;
		raid1_per_iov_init(per_iov, per_thread,
			bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
			bdev_io->u.bdev.offset_blocks+r1_bdev->start_blocks, bdev_io->u.bdev.num_blocks,
			RAID1_IO_WRITE, raid1_deliver_region_degraded_complete, raid1_io);
		raid1_per_iov_submit(per_iov);
	}
}

static void
raid1_deliver_region_multi_complete(void *ctx, uint8_t err_mask)
{
	struct raid1_bdev_io *raid1_io = ctx;
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);
	if (err_mask) {
		bool status_changed = false;
		if ((err_mask & 0x1) && (r1_bdev->online[0] == true)) {
			r1_bdev->online[0] = false;
			status_changed = true;
		}
		if ((err_mask & 0x2) && (r1_bdev->online[1] == true)) {
			r1_bdev->online[1] = false;
			status_changed = true;
		}
		if (status_changed) {
			TAILQ_INSERT_TAIL(&r1_bdev->sb_io_queue, raid1_io, link);
			raid1_update_status(r1_bdev);
		} else {
			if (r1_bdev->status == RAID1_BDEV_FAILED) {
				raid1_write_complete_hook(r1_bdev, raid1_io);
				raid1_io_failed(raid1_io);
			} else {
				assert(r1_bdev->status == RAID1_BDEV_DEGRADED);
				if (r1_bdev->sb_writing) {
					TAILQ_INSERT_TAIL(&r1_bdev->sb_io_queue, raid1_io, link);
				} else {
					raid1_write_complete_hook(r1_bdev, raid1_io);
					raid1_io_success(raid1_io);
				}
			}
		}
	} else {
		raid1_write_complete_hook(r1_bdev, raid1_io);
		raid1_io_success(raid1_io);
	}
}

static void
raid1_deliver_region_multi(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	struct raid1_bdev_io *raid1_io;
	while (!TAILQ_EMPTY(&region->delay_queue)) {
		raid1_io = TAILQ_FIRST(&region->delay_queue);
		TAILQ_REMOVE(&region->delay_queue, raid1_io, link);
		struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
			struct spdk_bdev_io, driver_ctx);
		struct raid1_multi_iov *multi_iov = &raid1_io->u.write_ctx.multi_iov;
		assert(!raid1_bm_test(r1_bdev->resync->active_bm, raid1_io->bit_idx));
		raid1_multi_iov_write(multi_iov, r1_bdev->per_thread_ptr,
			bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
			bdev_io->u.bdev.offset_blocks+r1_bdev->start_blocks, bdev_io->u.bdev.num_blocks,
			raid1_deliver_region_multi_complete, raid1_io);
	}
}

static inline void
raid1_region_bm_writing_hook(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	struct raid1_bdev_io *raid1_io;
	while (!TAILQ_EMPTY(&region->bm_writing_queue)) {
		raid1_io = TAILQ_FIRST(&region->bm_writing_queue);
		TAILQ_REMOVE(&region->bm_writing_queue, raid1_io, link);
		raid1_bdev_write_handler(r1_bdev, raid1_io);
	}
}

static void
raid1_write_bm_degraded_complete(void *ctx, int rc)
{
	struct raid1_region *region = ctx;
	struct raid1_bdev *r1_bdev = region->r1_bdev;
	r1_bdev->bm_io_cnt--;
	region->bm_writing = false;
	raid1_resync_bm_writing_hook(r1_bdev, region);
	raid1_region_bm_writing_hook(r1_bdev, region);
	if (rc) {
		if (r1_bdev->online[r1_bdev->health_idx] == true) {
			r1_bdev->online[r1_bdev->health_idx] = false;
			TAILQ_INSERT_TAIL(&r1_bdev->sb_region_queue, region, link);
			region->queue_type = RAID1_QUEUE_SB_WRITING;
			// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
			raid1_update_status(r1_bdev);
			return;
		} else {
			assert(r1_bdev->status == RAID1_BDEV_FAILED);
			if (r1_bdev->sb_writing) {
				TAILQ_INSERT_TAIL(&r1_bdev->sb_region_queue, region, link);
				region->queue_type = RAID1_QUEUE_SB_WRITING;
				// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
				return;
			} else {
				raid1_abort_region(r1_bdev, region);
				return;
			}
		}
	} else {
		raid1_deliver_region_degraded(r1_bdev, region);
		return;
	}
	assert(false);
}

static void
raid1_write_bm_multi_complete(void *ctx, uint8_t err_mask)
{
	struct raid1_region *region = ctx;
	struct raid1_bdev *r1_bdev = region->r1_bdev;
	bool status_changed;
	r1_bdev->bm_io_cnt--;
	region->bm_writing = false;
	raid1_resync_bm_writing_hook(r1_bdev, region);
	raid1_region_bm_writing_hook(r1_bdev, region);
	if (err_mask) {
		status_changed = false;
		if ((err_mask & 0x1) && (r1_bdev->online[0] == true)) {
			r1_bdev->online[0] = false;
			status_changed = true;
		}
		if ((err_mask & 0x2) && (r1_bdev->online[1] == true)) {
			r1_bdev->online[1] = false;
			status_changed = true;
		}
		if (status_changed) {
			if (region->queue_type != RAID1_QUEUE_SB_WRITING) {
				if (region->queue_type == RAID1_QUEUE_CLEAR) {
					TAILQ_REMOVE(&r1_bdev->clear_queue, region, link);
				} else if (region->queue_type == RAID1_QUEUE_SET) {
					TAILQ_REMOVE(&r1_bdev->set_queue, region, link);
				} else {
					assert(region->queue_type == RAID1_QUEUE_NONE);
				}
				TAILQ_INSERT_TAIL(&r1_bdev->sb_region_queue, region, link);
				region->queue_type = RAID1_QUEUE_SB_WRITING;
				// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
			}
			raid1_update_status(r1_bdev);
			return;
		} else {
			if (r1_bdev->status == RAID1_BDEV_FAILED) {
				raid1_abort_region(r1_bdev, region);
				return;
			} else {
				assert(r1_bdev->status == RAID1_BDEV_DEGRADED);
				if (r1_bdev->sb_writing) {
					TAILQ_INSERT_TAIL(&r1_bdev->sb_region_queue, region, link);
					region->queue_type = RAID1_QUEUE_SB_WRITING;
					// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
					return;
				} else {
					raid1_deliver_region_degraded(r1_bdev, region);
					return;
				}
			}
		}
	} else {
		raid1_deliver_region_multi(r1_bdev, region);
		return;
	}
	assert(false);
}

static void
raid1_write_bm(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	r1_bdev->bm_io_cnt++;
	region->bm_writing = true;
	region->delay_cnt = 0;
	if (r1_bdev->status == RAID1_BDEV_DEGRADED) {
		struct raid1_per_io *per_io = &region->region_io.io_leg[r1_bdev->health_idx].per_io;
		struct raid1_per_thread *per_thread = r1_bdev->per_thread_ptr[r1_bdev->health_idx];
		raid1_per_io_init(per_io, per_thread, region->bm_buf, region->bdev_offset,
			PAGE_SIZE, RAID1_IO_WRITE,
			raid1_write_bm_degraded_complete, region);
		raid1_per_io_submit(per_io);
	} else {
		raid1_multi_io_write(&region->region_io, r1_bdev->per_thread_ptr, region->bm_buf,
			region->bdev_offset, PAGE_SIZE,
			raid1_write_bm_multi_complete, region);
	}
}

static void
raid1_clear_trigger(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	assert(region->queue_type == RAID1_QUEUE_CLEAR);
	TAILQ_REMOVE(&r1_bdev->clear_queue, region, link);
	region->queue_type = RAID1_QUEUE_NONE;
	// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
	assert(TAILQ_EMPTY(&region->delay_queue));
	raid1_write_bm(r1_bdev, region);
}
static void
raid1_write_trigger(struct raid1_bdev *r1_bdev, struct raid1_region *region)
{
	assert(region->queue_type == RAID1_QUEUE_SET);
	TAILQ_REMOVE(&r1_bdev->set_queue, region, link);
	region->queue_type = RAID1_QUEUE_NONE;
	// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
	raid1_write_bm(r1_bdev, region);
}

static void
raid1_bdev_write_handler(struct raid1_bdev *r1_bdev, struct raid1_bdev_io *raid1_io)
{
	struct raid1_region *region;
	uint8_t inflight_cnt;

	if (r1_bdev->delete_ctx.deleted) {
		raid1_io_failed(raid1_io);
		return;
	}
	if (r1_bdev->status == RAID1_BDEV_FAILED) {
		raid1_io_failed(raid1_io);
		return;
	}
	if (r1_bdev->sb_writing) {
		raid1_write_sb_pending(r1_bdev, raid1_io);
		return;
	}
	if (raid1_bm_test(r1_bdev->resync->active_bm,
			raid1_io->bit_idx)) {
		raid1_write_resync_pending(r1_bdev, raid1_io);
		return;
	}
	region = &r1_bdev->regions[raid1_io->region_idx];
	if (region->bm_writing) {
		raid1_write_bm_writing_pending(region, raid1_io);
		return;
	}
	inflight_cnt = r1_bdev->inflight_cnt[raid1_io->bit_idx];
	if (inflight_cnt == RAID1_MAX_INFLIGHT_PER_BIT) {
		assert(raid1_bm_test(region->bm_buf, raid1_io->bit_in_region));
		raid1_write_inflight_pending(r1_bdev, raid1_io);
		raid1_write_trigger(r1_bdev, region);
	} else {
		raid1_write_delay(r1_bdev, region, raid1_io);
		if (region->delay_cnt >= r1_bdev->max_delay) {
			raid1_write_trigger(r1_bdev, region);
		}
	}
}

static void
raid1_bdev_write(void *ctx)
{
	struct raid1_bdev_io *raid1_io = ctx;
	struct spdk_bdev_io *bdev_io = SPDK_CONTAINEROF(raid1_io,
		struct spdk_bdev_io, driver_ctx);
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);

	raid1_init_pos(r1_bdev, bdev_io, raid1_io);
	raid1_bdev_write_handler(r1_bdev, raid1_io);
}

static void
raid1_bdev_get_buf_cb(struct spdk_io_channel *ch,
        struct spdk_bdev_io *bdev_io, bool success)
{
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);
	if (!success) {
		SPDK_ERRLOG("get_buf_cb failed: %s %p\n",
			r1_bdev->raid1_name, bdev_io);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	} else {
		spdk_thread_send_msg(r1_bdev->r1_thread, raid1_bdev_read, bdev_io);
	}
}

static void
raid1_bdev_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct raid1_bdev *r1_bdev = SPDK_CONTAINEROF(bdev_io->bdev,
		struct raid1_bdev, bdev);
	struct raid1_bdev_io *raid1_io = (struct raid1_bdev_io *)bdev_io->driver_ctx;
	raid1_io->orig_thread = spdk_get_thread();
	SPDK_DEBUGLOG(bdev_raid1, "submit_request %s %p %p %" PRIu64 " %d %s\n",
		r1_bdev->raid1_name, bdev_io, raid1_io,
		bdev_io->u.bdev.num_blocks, bdev_io->bdev->blocklen,
		raid1_bdev_io_type_to_string(bdev_io->type));
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		if (bdev_io->u.bdev.iovs == NULL ||
			bdev_io->u.bdev.iovs[0].iov_base == NULL) {
			spdk_bdev_io_get_buf(bdev_io, raid1_bdev_get_buf_cb,
				bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		} else {
			spdk_thread_send_msg(r1_bdev->r1_thread, raid1_bdev_read, raid1_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		spdk_thread_send_msg(r1_bdev->r1_thread, raid1_bdev_write, raid1_io);
		break;
	default:
		SPDK_ERRLOG("submit_request unspport io type: %s %p %d\n",
			r1_bdev->raid1_name, bdev_io, bdev_io->type);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}
}

static bool
raid1_bdev_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return true;
	default:
		return false;
	}
}

static struct spdk_io_channel *
raid1_bdev_get_io_channel(void *ctx)
{
	struct raid1_bdev *r1_bdev = ctx;
	struct spdk_io_channel *ch = NULL;
	ch = spdk_get_io_channel(r1_bdev);
	return ch;
}

/* This is the output for bdev_get_bdevs() for this vbdev */
static int
raid1_bdev_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct raid1_bdev *r1_bdev = ctx;

	spdk_json_write_name(w, "raid1");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "bdev0_name", r1_bdev->bdev0_name);
	spdk_json_write_named_string(w, "bdev1_name", r1_bdev->bdev1_name);
	spdk_json_write_named_bool(w, "bdev0_online", r1_bdev->online[0]);
	spdk_json_write_named_bool(w, "bdev1_online", r1_bdev->online[1]);
	spdk_json_write_named_uint64(w, "meta_szie", from_le64(&r1_bdev->sb->meta_size));
	spdk_json_write_named_uint64(w, "data_szie", from_le64(&r1_bdev->sb->data_size));
	spdk_json_write_named_uint64(w, "counter", from_le64(&r1_bdev->sb->counter));
	spdk_json_write_named_uint32(w, "major_version", from_le32(&r1_bdev->sb->major_version));
	spdk_json_write_named_uint32(w, "minor_version", from_le32(&r1_bdev->sb->minor_version));
	spdk_json_write_named_uint64(w, "bit_size", r1_bdev->bit_size);
	spdk_json_write_named_uint64(w, "write_delay", r1_bdev->write_delay);
	spdk_json_write_named_uint64(w, "clean_ratio", r1_bdev->clean_ratio);
	spdk_json_write_named_uint64(w, "max_delay", r1_bdev->max_delay);
	spdk_json_write_named_uint64(w, "max_resync", r1_bdev->max_resync);
	spdk_json_write_named_uint64(w, "total_bit", r1_bdev->bit_cnt);
	spdk_json_write_named_uint64(w, "synced_bit", r1_bdev->resync->curr_bit);
	spdk_json_write_named_uint64(w, "bm_io_cnt", r1_bdev->bm_io_cnt);
	spdk_json_write_named_uint64(w, "data_io_cnt", r1_bdev->data_io_cnt);
	spdk_json_write_named_uint64(w, "resync_io_cnt", r1_bdev->resync->num_inflight);
	spdk_json_write_named_uint64(w, "read_delivered", r1_bdev->read_delivered);
	spdk_json_write_named_uint64(w, "write_delivered", r1_bdev->write_delivered);
	spdk_json_write_named_uint64(w, "poller_counter", r1_bdev->poller_counter);
	spdk_json_write_named_bool(w, "sb_writing", r1_bdev->sb_writing);
	spdk_json_write_named_bool(w, "resync_stopped", r1_bdev->resync->stopped);
	spdk_json_write_named_string(w, "status",
		raid1_bdev_status_to_str(r1_bdev->status));
	spdk_json_write_object_end(w);
	return 0;
}

static const struct spdk_bdev_fn_table g_raid1_bdev_fn_table = {
	.destruct = raid1_bdev_destruct,
	.submit_request = raid1_bdev_submit_request,
	.io_type_supported = raid1_bdev_io_type_supported,
	.get_io_channel = raid1_bdev_get_io_channel,
	.dump_info_json = raid1_bdev_dump_info_json,
};

static int
raid1_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
    return 0;
}

static void
raid1_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
    return;
}

#define RAID1_PENDING_HASH_RATIO (PAGE_SIZE)
#define RAID1_PRODUCT_NAME "raid1"

static int
raid1_deleting_poller(struct raid1_bdev *r1_bdev)
{
	int event_cnt = 0;

	r1_bdev->resync->stopped = true;
	if (!r1_bdev->sb_writing) {
		while (!TAILQ_EMPTY(&r1_bdev->clear_queue)) {
			struct raid1_region *region = TAILQ_FIRST(&r1_bdev->clear_queue);
			if (!region->bm_writing) {
				raid1_clear_trigger(r1_bdev, region);
				event_cnt++;
			}
		}
	}
	if (!r1_bdev->sb_writing
		&& r1_bdev->resync->num_inflight == 0
		&& TAILQ_EMPTY(&r1_bdev->clear_queue)
		&& r1_bdev->bm_io_cnt == 0 && r1_bdev->data_io_cnt == 0
		&& !r1_bdev->sb_sync) {
		if (r1_bdev->status == RAID1_BDEV_NORMAL) {
			SPDK_DEBUGLOG(bdev_raid1, "Syncup sb before deleting\n");
			raid1_syncup_sb(r1_bdev);
			event_cnt++;
		} else {
			SPDK_DEBUGLOG(bdev_raid1, "Skip sb syncup due to raid1 status: %d\n",
				r1_bdev->status);
			r1_bdev->sb_sync = true;
		}
	}
	if (r1_bdev->sb_sync && !r1_bdev->sb_writing && r1_bdev->pending_close == 0) {
		SPDK_DEBUGLOG(bdev_raid1, "Release in thread for deleting: %s\n",
			r1_bdev->raid1_name);
		assert(r1_bdev->resync->num_inflight == 0);
		assert(r1_bdev->bm_io_cnt == 0);
		assert(r1_bdev->data_io_cnt == 0);
		raid1_bdev_release_in_thread(r1_bdev);
		raid1_release_ack_send(r1_bdev);
		event_cnt++;
	}
	return event_cnt;
}

static int
raid1_resync_poller(struct raid1_bdev *r1_bdev)
{
	int event_cnt = 0;
	struct raid1_resync *resync = r1_bdev->resync;
	uint32_t i;
	SPDK_DEBUGLOG(bdev_raid1, "In poller max_resync: %" PRIu64 " curr_bit: %" PRIu64 " bit_cnt: %" PRIu64 "\n",
		r1_bdev->max_resync, resync->curr_bit, r1_bdev->bit_cnt);
	for (i = 0; i < r1_bdev->max_resync; i++) {
		if (resync->curr_bit == r1_bdev->bit_cnt) {
			if (resync->num_inflight == 0) {
				resync->stopped = true;
			}
			break;
		}
		assert(resync->curr_bit < r1_bdev->bit_cnt);
		if (TAILQ_EMPTY(&resync->available_queue)) {
			SPDK_DEBUGLOG(bdev_raid1, "In poller available_queue empty\n");
			break;
		}
		if (raid1_bm_test(resync->needed_bm, resync->curr_bit)) {
			assert(!raid1_bm_test(resync->active_bm, resync->curr_bit));
			struct raid1_resync_ctx *resync_ctx = TAILQ_FIRST(&resync->available_queue);
			TAILQ_REMOVE(&resync->available_queue, resync_ctx, link);
			resync->num_inflight++;
			resync_ctx->bit_idx = resync->curr_bit;
			uint8_t inflight_cnt = r1_bdev->inflight_cnt[resync->curr_bit];
			struct raid1_region *region = raid1_bit_to_region(
				r1_bdev, resync->curr_bit);
			if (inflight_cnt > 0) {
				SPDK_DEBUGLOG(bdev_raid1, "Queue resync_ctx, curr_bit=%" PRIu64 " inflight_cnt=%d\n",
					resync_ctx->bit_idx, inflight_cnt);
				raid1_resync_hash_add(resync->pending_hash, resync->hash_size, resync_ctx);
			} else {
				SPDK_DEBUGLOG(bdev_raid1, "Handle resync in poller\n");
				raid1_resync_hash_add(resync->running_hash, resync->hash_size, resync_ctx);
				raid1_resync_handler(r1_bdev, resync, resync_ctx);
			}
			event_cnt++;
		} else {
			SPDK_DEBUGLOG(bdev_raid1, "Resync bit is 0: %" PRIu64 "\n", resync->curr_bit);
		}
		resync->curr_bit++;
	}
	return event_cnt;
}

static int
raid1_normal_poller(struct raid1_bdev *r1_bdev)
{
	int event_cnt = 0;
	if (!r1_bdev->resync->stopped) {
		event_cnt += raid1_resync_poller(r1_bdev);
	}
	while (!TAILQ_EMPTY(&r1_bdev->set_queue)) {
		struct raid1_region *region = TAILQ_FIRST(&r1_bdev->set_queue);
		raid1_write_trigger(r1_bdev, region);
		event_cnt++;
	}
	r1_bdev->clean_counter++;
	r1_bdev->clean_counter %= r1_bdev->clean_ratio;
	if (r1_bdev->clean_counter == 0) {
		struct raid1_region *region, *tmp;
		TAILQ_FOREACH_SAFE(region, &r1_bdev->clear_queue, link, tmp) {
			if (!region->bm_writing) {
				raid1_clear_trigger(r1_bdev, region);
				event_cnt++;
			}
		}
	}
	return event_cnt;
}

static int
raid1_io_poller(void *arg)
{
	struct raid1_bdev *r1_bdev = arg;
	r1_bdev->poller_counter++;
	if (r1_bdev->delete_ctx.deleted) {
		return raid1_deleting_poller(r1_bdev);
	} else {
		if (!r1_bdev->sb_writing) {
			return raid1_normal_poller(r1_bdev);
		}
	}
	return 0;
}

struct raid1_init_params {
	const char *raid1_name;
	const char *bdev0_name;
	const char *bdev1_name;
	struct raid1_per_bdev *per_bdev0;
	struct raid1_per_bdev *per_bdev1;
	struct raid1_sb *sb;
	const uint8_t *bm_buf;
};

#define RAID1_RESYNC_HASH_RATIO (10)

static void
raid1_resync_allocate(struct raid1_bdev *r1_bdev)
{
	struct raid1_resync *resync;
	struct raid1_resync_ctx *resync_ctx;
	uint32_t i, j;

	resync = malloc(sizeof(*resync));
	if (resync == NULL) {
		SPDK_ERRLOG("Could not allocate raid1_resync\n");
		goto err_out;
	}

	resync->curr_bit = 0;
	resync->num_inflight = 0;

	resync->needed_bm = malloc(r1_bdev->bm_size);
	if (resync->needed_bm == NULL) {
		SPDK_ERRLOG("Could not allocate needed_bm\n");
		goto free_resync;
	}
	memcpy(resync->needed_bm, r1_bdev->bm_buf, r1_bdev->bm_size);

	resync->active_bm = calloc(1, r1_bdev->bm_size);
	if (resync->active_bm == NULL) {
		SPDK_ERRLOG("Could not allocate active_bm\n");
		goto free_needed_bm;
	}

	resync->hash_size = SPDK_CEIL_DIV(r1_bdev->max_resync, RAID1_RESYNC_HASH_RATIO);

	resync->running_hash = calloc(resync->hash_size,
		sizeof(struct raid1_resync_head));
	if (resync->running_hash == NULL) {
		SPDK_ERRLOG("Could not allocate resync->running_hash\n");
		goto free_active_bm;
	}
	for (i = 0; i < resync->hash_size; i++) {
		TAILQ_INIT(&resync->running_hash[i]);
	}

	resync->pending_hash = calloc(resync->hash_size,
		sizeof(struct raid1_resync_head));
	if (resync->pending_hash == NULL) {
		SPDK_ERRLOG("Could not allocate resync->running_hash\n");
		goto free_running_hash;
	}
	for (i = 0; i < resync->hash_size; i++) {
		TAILQ_INIT(&resync->pending_hash[i]);
	}

	resync->resync_ctx_array = malloc(r1_bdev->max_resync * sizeof(struct raid1_resync_ctx));
	if (resync->resync_ctx_array == NULL) {
		SPDK_ERRLOG("Could not allocate resync_ctx_array\n");
		goto free_pending_hash;
	}

	TAILQ_INIT(&resync->available_queue);
	for (i = 0; i < r1_bdev->max_resync; i++) {
		resync_ctx = &resync->resync_ctx_array[i];
		resync_ctx->buf = spdk_dma_malloc(r1_bdev->bit_size,
			r1_bdev->buf_align, NULL);
		if (resync_ctx->buf == NULL) {
			for (j = 0; j < i; j++) {
				resync_ctx = &resync->resync_ctx_array[j];
				spdk_dma_free(resync_ctx->buf);
			}
			goto free_resync_ctx_array;
		}
		resync_ctx->r1_bdev = r1_bdev;
		TAILQ_INSERT_TAIL(&resync->available_queue, resync_ctx, link);
		TAILQ_INIT(&resync_ctx->waiting_queue);
	}

	TAILQ_INIT(&resync->read_complete_queue);
	TAILQ_INIT(&resync->write_complete_queue);
	TAILQ_INIT(&resync->bm_writing_queue);

	r1_bdev->resync = resync;
	return;

free_resync_ctx_array:
	free(resync->resync_ctx_array);
free_pending_hash:
	free(resync->pending_hash);
free_running_hash:
	free(resync->running_hash);
free_active_bm:
	free(resync->active_bm);
free_needed_bm:
	free(resync->needed_bm);
free_resync:
	free(resync);
err_out:
	r1_bdev->resync = NULL;
}

static void
raid1_resync_free(struct raid1_bdev *r1_bdev)
{
	struct raid1_resync *resync = r1_bdev->resync;
	struct raid1_resync_ctx *resync_ctx;
	uint32_t i;

	SPDK_DEBUGLOG(bdev_raid1, "raid1_resync_free: %s\n", r1_bdev->raid1_name);

	for (i = 0; i < r1_bdev->max_resync; i++) {
		resync_ctx = &resync->resync_ctx_array[i];
		spdk_dma_free(resync_ctx->buf);
	}
	free(resync->resync_ctx_array);
	free(resync->running_hash);
	free(resync->pending_hash);
	free(resync->active_bm);
	free(resync->needed_bm);
	free(resync);
	r1_bdev->resync = NULL;
}

static int
raid1_bdev_init_in_thread(void *arg)
{
	struct raid1_bdev *r1_bdev = arg;
	int rc;

	rc = raid1_per_thread_open(
		&r1_bdev->per_bdev[0], &r1_bdev->per_thread[0]);
	if (rc) {
		SPDK_ERRLOG("Could not open meta0 per thread\n");
		goto err_out;
	}
	r1_bdev->per_thread_ptr[0] = &r1_bdev->per_thread[0];

	if (r1_bdev->multi) {
		rc = raid1_per_thread_open(
			&r1_bdev->per_bdev[1], &r1_bdev->per_thread[1]);
		if (rc) {
			SPDK_ERRLOG("Could not open meta1 per thread\n");
			goto close_bdev0_per_thread;
		}
		r1_bdev->per_thread_ptr[1] = &r1_bdev->per_thread[1];
	}

	r1_bdev->poller = spdk_poller_register(raid1_io_poller,
		r1_bdev, r1_bdev->write_delay);
	if (r1_bdev->poller == NULL) {
		SPDK_ERRLOG("Could not register raid1_io_poller\n");
		rc = -EIO;
		goto close_bdev1_per_thread;
	}

	return 0;

close_bdev1_per_thread:
	if (r1_bdev->multi) {
		raid1_per_thread_close(&r1_bdev->per_thread[1]);
	}
close_bdev0_per_thread:
	raid1_per_thread_close(&r1_bdev->per_thread[0]);
err_out:
	return rc;
}

static void
raid1_bdev_release_in_thread(struct raid1_bdev *r1_bdev)
{
	SPDK_DEBUGLOG(bdev_raid1, "raid1_bdev_release_in_thread: %s\n",
		r1_bdev->raid1_name);
	raid1_per_thread_close(&r1_bdev->per_thread[0]);
	if (r1_bdev->multi) {
		raid1_per_thread_close(&r1_bdev->per_thread[1]);
	}
	spdk_poller_unregister(&r1_bdev->poller);
}

static void raid1_release(struct raid1_bdev *r1_bdev)
{
	raid1_per_bdev_close(&r1_bdev->per_bdev[0]);
	if (r1_bdev->multi) {
		raid1_per_bdev_close(&r1_bdev->per_bdev[1]);
	}
	spdk_io_device_unregister(r1_bdev, NULL);
	free(r1_bdev->pending_io_hash);
	raid1_resync_free(r1_bdev);
	free(r1_bdev->regions);
	free(r1_bdev->inflight_cnt);
	spdk_dma_free(r1_bdev->bm_buf);
	spdk_dma_free(r1_bdev->sb_buf);
	spdk_bdev_destruct_done(&r1_bdev->bdev, 0);
	free(r1_bdev);
}

static void
raid1_release_ack_recv(void *arg)
{
	struct raid1_bdev *r1_bdev = arg;
	raid1_find_and_remove(r1_bdev->raid1_name);
	raid1_release(r1_bdev);
}

static void
raid1_release_ack_send(struct raid1_bdev *r1_bdev)
{
	assert(r1_bdev->delete_ctx.orig_thread);
	spdk_thread_send_msg(r1_bdev->delete_ctx.orig_thread,
		raid1_release_ack_recv, r1_bdev);
}

struct raid1_create_ctx {
	char raid1_name[RAID1_MAX_NAME_LEN];
	char bdev0_name[RAID1_MAX_NAME_LEN];
	char bdev1_name[RAID1_MAX_NAME_LEN];
	uint64_t bit_size;
	uint64_t write_delay;
	uint64_t clean_ratio;
	uint64_t max_delay;
	uint64_t max_resync;
	bool synced;
	bool multi;
	bool ignore_zero_block;
	int primary_idx;
	uint64_t meta_size;
	uint64_t data_size;
	size_t buf_align;
	uint64_t region_cnt;
	uint64_t bit_cnt;
	uint64_t bm_size;
	struct raid1_per_bdev per_bdev[2];
	struct raid1_per_thread per_thread[2];
	struct raid1_per_thread *per_thread_ptr[2];
	struct raid1_multi_io multi_io;
	uint8_t *meta_buf[2];
	raid1_create_cb cb_fn;
	void *cb_arg;
	struct raid1_bdev *r1_bdev;
	struct raid1_msg_ctx msg_ctx;
	int rc;
};

static void
raid1_create_finish(struct raid1_create_ctx *create_ctx, int rc)
{
	create_ctx->cb_fn(create_ctx->cb_arg, rc);
	spdk_dma_free(create_ctx->meta_buf[0]);
	if (create_ctx->multi) {
		spdk_dma_free(create_ctx->meta_buf[1]);
	}
	free(create_ctx);
}

static int
raid1_bdev_init_rollback(void *arg)
{
	struct raid1_bdev *r1_bdev = arg;
	raid1_bdev_release_in_thread(r1_bdev);
	return 0;
}

static void
raid1_bdev_init_rollback_complete(void *arg, int rc)
{
	struct raid1_create_ctx *create_ctx = arg;
	assert(rc == 0);
	raid1_release(create_ctx->r1_bdev);
	raid1_create_finish(create_ctx, create_ctx->rc);
}

static void
raid1_bdev_init_complete(void *arg, int rc)
{
	struct raid1_create_ctx *create_ctx = arg;
	struct raid1_bdev *r1_bdev = create_ctx->r1_bdev;

	if (rc) {
		raid1_release(r1_bdev);
		raid1_create_finish(create_ctx, rc);
	} else {
		rc = spdk_bdev_register(&r1_bdev->bdev);
		if (rc) {
			SPDK_ERRLOG("Could not register bdev: %s %s %d\n",
				r1_bdev->bdev.name, spdk_strerror(-rc), rc);
			create_ctx->rc = rc;
			raid1_msg_submit(&create_ctx->msg_ctx, r1_bdev->r1_thread,
				raid1_bdev_init_rollback, r1_bdev,
				raid1_bdev_init_rollback_complete, create_ctx);
		} else {
			raid1_create_finish(create_ctx, rc);
		}
	}
}

static void
raid1_bdev_init(struct raid1_create_ctx *create_ctx)
{
	struct spdk_thread *target_thread;
	struct raid1_bdev *r1_bdev;
	struct raid1_region *region;
	struct raid1_io_head *io_head;
	uint8_t *meta_buf;
	int i;
	int rc;

	/* We have finished all IOs in the RPC thread */
	raid1_per_thread_close(&create_ctx->per_thread[0]);
	if (create_ctx->multi) {
		raid1_per_thread_close(&create_ctx->per_thread[1]);
	}

	r1_bdev = raid1_find_by_name(create_ctx->raid1_name);
	if (r1_bdev) {
		SPDK_ERRLOG("The raid1 bdev exists: %s\n", create_ctx->raid1_name);
		rc = -EEXIST;
		goto err_out;
	}

	r1_bdev = calloc(1, sizeof(*r1_bdev));
	if (r1_bdev == NULL) {
		SPDK_ERRLOG("Could not allocate raid1_bdev\n");
		rc = -ENOMEM;
		goto err_out;
	}

	strncpy(r1_bdev->raid1_name, create_ctx->raid1_name, RAID1_MAX_NAME_LEN);
	if (create_ctx->primary_idx == 0) {
		strncpy(r1_bdev->bdev0_name, create_ctx->bdev0_name, RAID1_MAX_NAME_LEN);
		memcpy(&r1_bdev->per_bdev[0], &create_ctx->per_bdev[0], sizeof(struct raid1_per_bdev));
		if (create_ctx->multi) {
			strncpy(r1_bdev->bdev1_name, create_ctx->bdev1_name, RAID1_MAX_NAME_LEN);
			memcpy(&r1_bdev->per_bdev[1], &create_ctx->per_bdev[1], sizeof(struct raid1_per_bdev));
		}
		meta_buf = create_ctx->meta_buf[0];
	} else {
		assert(create_ctx->multi);
		strncpy(r1_bdev->bdev0_name, create_ctx->bdev1_name, RAID1_MAX_NAME_LEN);
		strncpy(r1_bdev->bdev1_name, create_ctx->bdev0_name, RAID1_MAX_NAME_LEN);
		memcpy(&r1_bdev->per_bdev[0], &create_ctx->per_bdev[1], sizeof(struct raid1_per_bdev));
		memcpy(&r1_bdev->per_bdev[1], &create_ctx->per_bdev[0], sizeof(struct raid1_per_bdev));
		meta_buf = create_ctx->meta_buf[1];
	}
	r1_bdev->multi = create_ctx->multi;
	r1_bdev->bit_size = create_ctx->bit_size;
	r1_bdev->write_delay = create_ctx->write_delay;
	r1_bdev->clean_ratio = create_ctx->clean_ratio;
	r1_bdev->clean_counter = 0;
	r1_bdev->max_delay = create_ctx->max_delay;
	r1_bdev->max_resync = create_ctx->max_resync;
	r1_bdev->buf_align = create_ctx->buf_align;
	r1_bdev->region_cnt = create_ctx->region_cnt;
	r1_bdev->bit_cnt = create_ctx->bit_cnt;
	r1_bdev->bm_size = create_ctx->bm_size;
	r1_bdev->region_size = RAID1_BYTESZ * PAGE_SIZE * r1_bdev->bit_size;
	r1_bdev->start_blocks = create_ctx->meta_size / r1_bdev->per_bdev[0].block_size;
	r1_bdev->ignore_zero_block = create_ctx->ignore_zero_block;

	r1_bdev->sb_buf = spdk_dma_zmalloc(RAID1_SB_SIZE, r1_bdev->buf_align, NULL);
	if (r1_bdev->sb_buf == NULL) {
		SPDK_ERRLOG("Could not allocate sb_buf, size=%ld align=%ld\n",
			RAID1_SB_SIZE, r1_bdev->buf_align);
		rc = -ENOMEM;
		goto free_r1_bdev;
	}
	r1_bdev->sb = (struct raid1_sb *)r1_bdev->sb_buf;
	memcpy(r1_bdev->sb_buf, meta_buf, sizeof(struct raid1_sb));

	r1_bdev->bm_buf = spdk_dma_zmalloc(r1_bdev->bm_size, r1_bdev->buf_align, NULL);
	if (r1_bdev->bm_buf == NULL) {
		SPDK_ERRLOG("Could not allocate bm_buf\n");
		rc = -ENOMEM;
		goto free_sb_buf;
	}
	memcpy(r1_bdev->bm_buf, meta_buf+RAID1_SB_SIZE, r1_bdev->bm_size);
	SPDK_DEBUGLOG(bdev_raid1, "r1_bdev->bm_buf: %p\n", r1_bdev->bm_buf);
	SPDK_DEBUGLOG(bdev_raid1, "r1_bdev->bm_size: %" PRIu64 "\n", r1_bdev->bm_size);
	raid1_bm_show(r1_bdev->bm_buf, r1_bdev->bm_size);

	r1_bdev->inflight_cnt = calloc(r1_bdev->bit_cnt, sizeof(uint8_t));
	if (r1_bdev->inflight_cnt == NULL) {
		SPDK_ERRLOG("Could not allocate inflight_cnt, size: %" PRIu64 "\n", r1_bdev->bit_cnt);
		rc = -ENOMEM;
		goto free_bm_buf;
	}

	r1_bdev->regions = calloc(r1_bdev->region_cnt, sizeof(struct raid1_region));
	if (r1_bdev->regions == NULL) {
		SPDK_ERRLOG("Could not allocate regions\n");
		rc = -ENOMEM;
		goto free_inflight_cnt;
	}

	SPDK_DEBUGLOG(bdev_raid1, "r1_bdev: %p\n", r1_bdev);
	SPDK_DEBUGLOG(bdev_raid1, "r1_bdev->regions: %p\n", r1_bdev->regions);
	SPDK_DEBUGLOG(bdev_raid1, "region_cnt: %" PRIu64 "\n", r1_bdev->region_cnt);
	for(i = 0; i < r1_bdev->region_cnt; i++) {
		region = &r1_bdev->regions[i];
		region->r1_bdev = r1_bdev;
		region->idx = i;
		region->bm_buf = r1_bdev->bm_buf + PAGE_SIZE * i;
		region->bdev_offset = RAID1_BM_START_BYTE + PAGE_SIZE * i;
		region->queue_type = RAID1_QUEUE_NONE;
		// SPDK_ERRLOG("change_queue_type: %p %d\n", region, region->queue_type);
		TAILQ_INIT(&region->delay_queue);
		TAILQ_INIT(&region->bm_writing_queue);
		region->bm_writing = false;
		SPDK_DEBUGLOG(bdev_raid1, "region: %p %p %" PRIu64 "\n",
			region, region->bm_buf, region->idx);
	}

	raid1_resync_allocate(r1_bdev);
	if (r1_bdev->resync == NULL) {
		SPDK_ERRLOG("Could not allocate r1_bdev->resync\n");
		rc = -ENOMEM;
		goto free_regions;
	}

	r1_bdev->sb_writing = false;
	r1_bdev->bm_io_cnt = 0;
	r1_bdev->data_io_cnt = 0;

	r1_bdev->r1_thread = raid1_choose_thread();
	if (create_ctx->multi) {
		r1_bdev->status = RAID1_BDEV_NORMAL;
		r1_bdev->online[0] = true;
		r1_bdev->online[1] = true;
		if (create_ctx->synced) {
			r1_bdev->resync->stopped = true;
			r1_bdev->resync->curr_bit = r1_bdev->bit_cnt;
		} else {
			r1_bdev->resync->stopped = false;
			r1_bdev->resync->curr_bit = 0;
		}
	} else {
		r1_bdev->status = RAID1_BDEV_DEGRADED;
		r1_bdev->online[0] = true;
		r1_bdev->online[1] = false;
		r1_bdev->resync->stopped = true;
		r1_bdev->resync->curr_bit = 0;
	}

	r1_bdev->read_idx = 0;
	r1_bdev->pending_close = 0;

	TAILQ_INIT(&r1_bdev->set_queue);
	TAILQ_INIT(&r1_bdev->clear_queue);
	TAILQ_INIT(&r1_bdev->sb_region_queue);
	TAILQ_INIT(&r1_bdev->sb_io_queue);

	r1_bdev->pending_hash_size = SPDK_CEIL_DIV(
		r1_bdev->bm_size, RAID1_PENDING_HASH_RATIO);
	r1_bdev->pending_io_hash = malloc(
		r1_bdev->pending_hash_size * sizeof(struct raid1_io_head));
	if (r1_bdev->pending_io_hash == NULL) {
		SPDK_ERRLOG("Could not allocate pending_io_hash\n");
		rc = -ENOMEM;
		goto free_resync;
	}
	for (i = 0; i < r1_bdev->pending_hash_size; i++) {
		io_head = &r1_bdev->pending_io_hash[i];
		TAILQ_INIT(io_head);
	}
	r1_bdev->bdev.name = r1_bdev->raid1_name;
	r1_bdev->bdev.product_name = RAID1_PRODUCT_NAME;
	r1_bdev->bdev.write_cache = false;
	if (create_ctx->multi) {
		r1_bdev->bdev.required_alignment = spdk_max(
			r1_bdev->per_bdev[0].required_alignment,
			r1_bdev->per_bdev[1].required_alignment);
		r1_bdev->bdev.blocklen = spdk_min(r1_bdev->per_bdev[0].block_size,
			r1_bdev->per_bdev[1].block_size);
	} else {
		r1_bdev->bdev.required_alignment = r1_bdev->per_bdev[0].required_alignment;
		r1_bdev->bdev.blocklen = r1_bdev->per_bdev[0].block_size;
	}
	r1_bdev->bdev.blockcnt = from_le64(&r1_bdev->sb->data_size) / r1_bdev->bdev.blocklen;
	r1_bdev->bdev.optimal_io_boundary = r1_bdev->bit_size;
	r1_bdev->bdev.split_on_optimal_io_boundary = true;
	r1_bdev->bdev.ctxt = r1_bdev;
	r1_bdev->bdev.fn_table = &g_raid1_bdev_fn_table;
	r1_bdev->bdev.module = &g_raid1_if;

	r1_bdev->sb_sync = false;
	r1_bdev->delete_ctx.orig_thread = spdk_get_thread();
	r1_bdev->delete_ctx.deleted = false;

	spdk_io_device_register(r1_bdev, raid1_bdev_ch_create_cb,
		raid1_bdev_ch_destroy_cb, 0, r1_bdev->raid1_name);

	create_ctx->r1_bdev = r1_bdev;
	TAILQ_INSERT_TAIL(&g_raid1_bdev_head, r1_bdev, link);
	raid1_msg_submit(&create_ctx->msg_ctx, r1_bdev->r1_thread,
		raid1_bdev_init_in_thread, r1_bdev,
		raid1_bdev_init_complete, create_ctx);
	return;

free_resync:
	raid1_resync_free(r1_bdev);
free_regions:
	free(r1_bdev->regions);
free_inflight_cnt:
	free(r1_bdev->inflight_cnt);
free_bm_buf:
	spdk_dma_free(r1_bdev->bm_buf);
free_sb_buf:
	spdk_dma_free(r1_bdev->sb_buf);
free_r1_bdev:
	free(r1_bdev);
err_out:
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
	if (create_ctx->multi) {
		raid1_per_bdev_close(&create_ctx->per_bdev[1]);
	}
	raid1_create_finish(create_ctx, rc);
}

static bool
raid1_good_sb(struct raid1_create_ctx *create_ctx, struct raid1_sb *sb)
{
	if (from_le32(&sb->major_version) != RAID1_MAJOR_VERSION) {
		SPDK_ERRLOG("raid1 major_verison mismatch\n");
		return false;
	}
	if (from_le64(&sb->meta_size) != create_ctx->meta_size) {
		SPDK_ERRLOG("raid1 meta_size mismatch\n");
		return false;
	}
	if (from_le64(&sb->data_size) != create_ctx->data_size) {
		SPDK_ERRLOG("raid1 data_size mismatch\n");
		return false;
	}
	if (from_le64(&sb->bit_size) != create_ctx->bit_size) {
		SPDK_ERRLOG("raid1 bit_size mismatch\n");
		return false;
	}
	return true;
}

static void
raid1_create_update_secondary_complete(void *arg, int rc)
{
	struct raid1_create_ctx *create_ctx = arg;
	if (rc) {
		SPDK_ERRLOG("Update secondary err: %d\n", rc);
		goto err_out;
	}
	raid1_bdev_init(create_ctx);
	return;

err_out:
	raid1_per_thread_close(&create_ctx->per_thread[0]);
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
	raid1_per_thread_close(&create_ctx->per_thread[1]);
	raid1_per_bdev_close(&create_ctx->per_bdev[1]);
	raid1_create_finish(create_ctx, rc);
	return;
}

static void
raid1_create_update_secondary(struct raid1_create_ctx *create_ctx)
{
	struct raid1_per_io *per_io;
	int idx = 1 - create_ctx->primary_idx;
	per_io = &create_ctx->multi_io.io_leg[idx].per_io;
	raid1_per_io_init(per_io, create_ctx->per_thread_ptr[idx],
		create_ctx->meta_buf[idx], RAID1_SB_START_BYTE,
		create_ctx->meta_size, RAID1_IO_WRITE,
		raid1_create_update_secondary_complete, create_ctx);
	raid1_per_io_submit(per_io);
}

static void
raid1_create_update_primary_complete(void *arg, int rc)
{
	struct raid1_create_ctx *create_ctx = arg;
	if (rc) {
		SPDK_ERRLOG("Update primary err: %d\n", rc);
		goto err_out;
	}
	raid1_create_update_secondary(create_ctx);
	return;

err_out:
	raid1_per_thread_close(&create_ctx->per_thread[0]);
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
	raid1_per_thread_close(&create_ctx->per_thread[1]);
	raid1_per_bdev_close(&create_ctx->per_bdev[1]);
	raid1_create_finish(create_ctx, rc);
	return;
}

static void
raid1_create_update_primary(struct raid1_create_ctx *create_ctx)
{
	struct raid1_per_io *per_io;
	int idx = create_ctx->primary_idx;
	per_io = &create_ctx->multi_io.io_leg[idx].per_io;
	raid1_per_io_init(per_io, create_ctx->per_thread_ptr[idx],
		create_ctx->meta_buf[idx], RAID1_SB_START_BYTE,
		create_ctx->meta_size, RAID1_IO_WRITE,
		raid1_create_update_primary_complete, create_ctx);
	raid1_per_io_submit(per_io);
}

static void
raid1_set_all_bm(uint8_t *bm, uint64_t bit_cnt)
{
	int written_cnt = (bit_cnt / RAID1_BYTESZ) * RAID1_BYTESZ;
	memset(bm, 0xff, bit_cnt / RAID1_BYTESZ);
	for (int i = written_cnt; i < bit_cnt; i++) {
		raid1_bm_set(bm, i);
	}
}

static void
raid1_create_meta_multi_read_complete(void *arg, uint8_t err_mask)
{
	struct raid1_create_ctx *create_ctx = arg;
	int rc;
	struct raid1_sb *sb0, *sb1;
	bool valid_sb0, valid_sb1;
	struct raid1_init_params params;
	int primary_idx;
	if (err_mask) {
		rc = -EIO;
		goto err_out;
	}
	sb0 = (struct raid1_sb *)create_ctx->meta_buf[0];
	sb1 = (struct raid1_sb *)create_ctx->meta_buf[1];
	if (strncmp(sb0->magic, RAID1_MAGIC_STRING, RAID1_MAGIC_STRING_LEN)) {
		/* if (!raid1_is_buffer_all_zero(create_ctx->meta_buf[0], */
		/* 		create_ctx->meta_size)) { */
		/* 	SPDK_ERRLOG("meta_buf[0] is not all zero\n"); */
		/* 	rc = -EINVAL; */
		/* 	goto err_out; */
		/* } */
		valid_sb0 = false;
	} else {
		if (!raid1_good_sb(create_ctx, sb0)) {
			rc = -EINVAL;
			goto err_out;
		}
		valid_sb0 = true;
	}
	if (strncmp(sb1->magic, RAID1_MAGIC_STRING, RAID1_MAGIC_STRING_LEN)) {
		/* if (!raid1_is_buffer_all_zero(create_ctx->meta_buf[1], */
		/* 		create_ctx->meta_size)) { */
		/* 	SPDK_ERRLOG("meta_buf[1] is not all zero\n"); */
		/* 	rc = -EINVAL; */
		/* 	goto err_out; */
		/* } */
		valid_sb1 = false;
	} else {
		if (!raid1_good_sb(create_ctx, sb1)) {
			rc = -EINVAL;
			goto err_out;
		}
		valid_sb1 = true;
	}
	params.raid1_name = create_ctx->raid1_name;
	if (valid_sb0 && valid_sb1) {
		if (spdk_uuid_compare(&sb0->uuid, &sb1->uuid)) {
			SPDK_ERRLOG("raid1 uuid mismatch\n");
			rc = -EINVAL;
			goto err_out;
		}
		uint64_t counter0 = from_le64(&sb0->counter);
		uint64_t counter1 = from_le64(&sb1->counter);
		if (counter0 >= counter1) {
			create_ctx->primary_idx = 0;
		} else {
			create_ctx->primary_idx = 1;
		}
		create_ctx->synced = false;
		raid1_bdev_init(create_ctx);
	} else if (valid_sb0) {
		create_ctx->primary_idx = 0;
		create_ctx->synced = false;
		raid1_set_all_bm(create_ctx->meta_buf[0] + RAID1_SB_SIZE,
			create_ctx->bit_cnt);
		memcpy(create_ctx->meta_buf[1], create_ctx->meta_buf[0],
			create_ctx->meta_size);
		raid1_sb_counter_add(sb0, 1);
		raid1_create_update_primary(create_ctx);
	} else if (valid_sb1) {
		create_ctx->primary_idx = 1;
		create_ctx->synced = false;
		raid1_set_all_bm(create_ctx->meta_buf[1] + RAID1_SB_SIZE,
			create_ctx->bit_cnt);
		memcpy(create_ctx->meta_buf[0], create_ctx->meta_buf[1],
			create_ctx->meta_size);
		raid1_sb_counter_add(sb1, 1);
		raid1_create_update_primary(create_ctx);
	} else {
		create_ctx->primary_idx = 0;
		memset(create_ctx->meta_buf[0], 0, create_ctx->meta_size);
		strncpy(sb0->magic, RAID1_MAGIC_STRING, RAID1_MAGIC_STRING_LEN);
		spdk_uuid_generate(&sb0->uuid);
		to_le64(&sb0->meta_size, create_ctx->meta_size);
		to_le64(&sb0->data_size, create_ctx->data_size);
		to_le64(&sb0->counter, 1);
		to_le32(&sb0->major_version, RAID1_MAJOR_VERSION);
		to_le32(&sb0->minor_version, RAID1_MINOR_VERSION);
		to_le64(&sb0->bit_size, create_ctx->bit_size);
		if (!create_ctx->synced) {
			raid1_set_all_bm(create_ctx->meta_buf[0] + RAID1_SB_SIZE,
				create_ctx->bit_cnt);
		}
		memcpy(create_ctx->meta_buf[1], create_ctx->meta_buf[0],
			create_ctx->meta_size);
		raid1_sb_counter_add(sb0, 1);
		raid1_create_update_primary(create_ctx);
	}
	return;

err_out:
	raid1_per_thread_close(&create_ctx->per_thread[0]);
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
	raid1_per_thread_close(&create_ctx->per_thread[1]);
	raid1_per_bdev_close(&create_ctx->per_bdev[1]);
	raid1_create_finish(create_ctx, rc);
	return;
}

static void
raid1_create_single_sb_update_complete(void *arg, int rc)
{
	struct raid1_create_ctx *create_ctx = arg;
	if (rc) {
		SPDK_ERRLOG("Update single sb err: %d\n", rc);
		goto err_out;
	}
	raid1_bdev_init(create_ctx);
	return;

err_out:
	raid1_per_thread_close(&create_ctx->per_thread[0]);
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
	raid1_create_finish(create_ctx, rc);
	return;
}

static void
raid1_create_single_sb_update(struct raid1_create_ctx *create_ctx)
{
	struct raid1_per_io *per_io;
	per_io = &create_ctx->multi_io.io_leg[0].per_io;
	raid1_per_io_init(per_io, create_ctx->per_thread_ptr[0],
		create_ctx->meta_buf[0], RAID1_SB_START_BYTE,
		create_ctx->meta_size, RAID1_IO_WRITE,
		raid1_create_single_sb_update_complete, create_ctx);
	raid1_per_io_submit(per_io);
}

static void
raid1_create_meta_single_read_complete(void *arg, int rc)
{
	struct raid1_create_ctx *create_ctx = arg;
	struct raid1_sb *sb;
	if (rc) {
		SPDK_ERRLOG("Read meta error: %d\n", rc);
		goto err_out;
	}

	sb = (struct raid1_sb *)create_ctx->meta_buf[0];
	if (strncmp(sb->magic, RAID1_MAGIC_STRING, RAID1_MAGIC_STRING_LEN)) {
		/* if (!raid1_is_buffer_all_zero(create_ctx->meta_buf[0], */
		/* 		create_ctx->meta_size)) { */
		/* 	SPDK_ERRLOG("meta_buf is not all zero\n"); */
		/* 	rc = -EINVAL; */
		/* 	goto err_out; */
		/* } */
		memset(create_ctx->meta_buf[0], 0, create_ctx->meta_size);
		strncpy(sb->magic, RAID1_MAGIC_STRING, RAID1_MAGIC_STRING_LEN);
		spdk_uuid_generate(&sb->uuid);
		to_le64(&sb->meta_size, create_ctx->meta_size);
		to_le64(&sb->data_size, create_ctx->data_size);
		to_le64(&sb->counter, 1);
		to_le32(&sb->major_version, RAID1_MAJOR_VERSION);
		to_le32(&sb->minor_version, RAID1_MINOR_VERSION);
		to_le64(&sb->bit_size, create_ctx->bit_size);
	} else {
		raid1_sb_counter_add(sb, 1);
	}
	create_ctx->synced = false;
	raid1_create_single_sb_update(create_ctx);
	return;

err_out:
	raid1_per_thread_close(&create_ctx->per_thread[0]);
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
	raid1_create_finish(create_ctx, rc);
	return;
}

void
raid1_bdev_create(const char *raid1_name, struct raid1_create_param *param,
	raid1_create_cb cb_fn, void *cb_arg)
{
	struct raid1_create_ctx *create_ctx;
	uint64_t whole_size, meta_size, data_size, bm_size, bit_cnt, region_cnt;
	size_t buf_align;
	int rc;

	SPDK_DEBUGLOG(bdev_raid1, "raid1_bdev_create: %s\n", raid1_name);

	create_ctx = calloc(1, sizeof(*create_ctx));
	if (create_ctx == NULL) {
		SPDK_ERRLOG("Could not allocate raid1 create_ctx\n");
		rc = -ENOMEM;
		goto call_cb;
	}

	if (strncmp(param->bdev1_name, "-", 1)) {
		create_ctx->multi = true;
	} else {
		create_ctx->multi = false;
	}

	strncpy(create_ctx->raid1_name, raid1_name, RAID1_MAX_NAME_LEN);
	strncpy(create_ctx->bdev0_name, param->bdev0_name, RAID1_MAX_NAME_LEN);
	if (create_ctx->multi) {
		strncpy(create_ctx->bdev1_name, param->bdev1_name, RAID1_MAX_NAME_LEN);
	}

	create_ctx->bit_size = param->bit_size;
	create_ctx->write_delay = param->write_delay;
	create_ctx->clean_ratio = param->clean_ratio;
	create_ctx->max_delay = param->max_delay;
	create_ctx->max_resync = param->max_resync;
	create_ctx->synced = param->synced;
	create_ctx->ignore_zero_block = param->ignore_zero_block;

	rc = raid1_per_bdev_open(create_ctx->bdev0_name, &create_ctx->per_bdev[0]);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev0 per_bdev\n");
		goto free_ctx;
	}

	rc = raid1_per_thread_open(&create_ctx->per_bdev[0], &create_ctx->per_thread[0]);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev0 per_thread\n");
		goto close_bdev0_per_bdev;
	}

	if (create_ctx->multi) {
		rc = raid1_per_bdev_open(create_ctx->bdev1_name, &create_ctx->per_bdev[1]);
		if (rc) {
			SPDK_ERRLOG("Could not open bdev1 per_bdev\n");
			goto close_bdev0_per_thread;
		}

		rc = raid1_per_thread_open(&create_ctx->per_bdev[1], &create_ctx->per_thread[1]);
		if (rc) {
			SPDK_ERRLOG("Could not open bdev1 per_thread\n");
			goto close_bdev1_per_bdev;
		}
	}

	create_ctx->per_thread_ptr[0] = &create_ctx->per_thread[0];
	create_ctx->per_thread_ptr[1] = &create_ctx->per_thread[1];

	if (create_ctx->bit_size % PAGE_SIZE) {
		SPDK_ERRLOG("bit_size is not alignment to %lu\n", PAGE_SIZE);
		rc = -EINVAL;
		goto close_bdev1_per_thread;
	}

	if (create_ctx->multi) {
		buf_align = spdk_max(create_ctx->per_bdev[0].buf_align,
			create_ctx->per_bdev[1].buf_align);
		whole_size = spdk_min(create_ctx->per_bdev[0].block_size * create_ctx->per_bdev[0].num_blocks,
			create_ctx->per_bdev[1].block_size * create_ctx->per_bdev[1].num_blocks);
	} else {
		buf_align = create_ctx->per_bdev[0].buf_align;
		whole_size = create_ctx->per_bdev[0].block_size * create_ctx->per_bdev[0].num_blocks;
	}

	raid1_calc_bit_and_region(whole_size, param->bit_size, &bit_cnt, &region_cnt);
	bm_size = region_cnt * PAGE_SIZE;
	meta_size = RAID1_SB_SIZE + bm_size;
	if (param->meta_size != 0) {
		if (param->meta_size < meta_size) {
			rc = -EINVAL;
			SPDK_ERRLOG("meta_size is too small: %" PRIu64 " < %" PRIu64 "\n",
				param->meta_size, meta_size);
			goto close_bdev1_per_thread;
		}
		meta_size = param->meta_size;
	}
	data_size = whole_size - meta_size;
	raid1_calc_bit_and_region(data_size, param->bit_size, &bit_cnt, &region_cnt);

	create_ctx->meta_buf[0] = spdk_dma_zmalloc(meta_size, buf_align, NULL);
	if (!create_ctx->meta_buf[0]) {
		rc = -ENOMEM;
		SPDK_ERRLOG("Could not allocate meta_buf[0]\n");
		goto close_bdev1_per_thread;
	}
	if (create_ctx->multi) {
		create_ctx->meta_buf[1] = spdk_dma_zmalloc(meta_size, buf_align, NULL);
		if (!create_ctx->meta_buf[1]) {
			rc = -ENOMEM;
			SPDK_ERRLOG("Could not allocate meta_buf[1]\n");
			goto free_meta_buf0;
		}
	}

	create_ctx->meta_size = meta_size;
	create_ctx->data_size = data_size;
	create_ctx->region_cnt = region_cnt;
	create_ctx->bit_cnt = bit_cnt;
	create_ctx->bm_size = bm_size;
	create_ctx->cb_fn = cb_fn;
	create_ctx->cb_arg = cb_arg;
	if (create_ctx->multi) {
		raid1_multi_io_read(&create_ctx->multi_io, create_ctx->per_thread_ptr,
			create_ctx->meta_buf, RAID1_SB_START_BYTE, create_ctx->meta_size,
			raid1_create_meta_multi_read_complete, create_ctx);
	} else {
		struct raid1_per_io *per_io = &create_ctx->multi_io.io_leg[0].per_io;
		raid1_per_io_init(per_io, create_ctx->per_thread_ptr[0],
			create_ctx->meta_buf[0], RAID1_SB_START_BYTE,
			create_ctx->meta_size, RAID1_IO_READ,
			raid1_create_meta_single_read_complete, create_ctx);
		raid1_per_io_submit(per_io);
	}

	return;

free_meta_buf0:
	spdk_dma_free(create_ctx->meta_buf[0]);
close_bdev1_per_thread:
	if (create_ctx->multi) {
		raid1_per_thread_close(&create_ctx->per_thread[1]);
	}
close_bdev1_per_bdev:
	if (create_ctx->multi) {
		raid1_per_bdev_close(&create_ctx->per_bdev[1]);
	}
close_bdev0_per_thread:
	raid1_per_thread_close(&create_ctx->per_thread[0]);
close_bdev0_per_bdev:
	raid1_per_bdev_close(&create_ctx->per_bdev[0]);
free_ctx:
	free(create_ctx);
call_cb:
	cb_fn(cb_arg, rc);

	return;
}

void
raid1_bdev_delete(const char *raid1_name, raid1_delete_cb cb_fn, void *cb_arg)
{
	struct raid1_bdev *r1_bdev;
	int i;
	int rc;
	SPDK_DEBUGLOG(bdev_raid1, "raid1_bdev_delete: %s\n", raid1_name);

	r1_bdev = raid1_find_by_name(raid1_name);
	if (r1_bdev == NULL) {
		SPDK_ERRLOG("raid1 bdev %s is not found\n", raid1_name);
		rc = -ENODEV;
		goto err_out;
	}

	spdk_bdev_unregister(&r1_bdev->bdev, cb_fn, cb_arg);
	return;

err_out:
	cb_fn(cb_arg, rc);
}

struct raid1_dump_ctx {
	char bdev_name[RAID1_MAX_NAME_LEN];
	struct raid1_per_bdev per_bdev;
	struct raid1_per_thread per_thread;
	struct raid1_per_io per_io;
	uint8_t *sb_buf;
	raid1_dump_cb cb_fn;
	void *cb_arg;
};

static void
raid1_dump_sb_read_complete(void *arg, int rc)
{
	struct raid1_dump_ctx *dump_ctx = arg;
	struct raid1_sb *sb = NULL;

	if (rc) {
		SPDK_ERRLOG("Read sb error: %d\n", rc);
		goto out;
	}
	sb = (struct raid1_sb *)dump_ctx->sb_buf;
out:
	dump_ctx->cb_fn(dump_ctx->cb_arg, sb, rc);
	raid1_per_thread_close(&dump_ctx->per_thread);
	raid1_per_bdev_close(&dump_ctx->per_bdev);
	free(dump_ctx);
}
void
raid1_bdev_dump(const char *bdev_name, raid1_dump_cb cb_fn, void *cb_arg)
{
	struct raid1_dump_ctx *dump_ctx;
	int rc;

	dump_ctx = calloc(1, sizeof(*dump_ctx));
	if (dump_ctx == NULL) {
		SPDK_ERRLOG("Could not allocate raid1 dump_ctx\n");
		rc = -ENOMEM;
		goto call_cb;
	}
	strncpy(dump_ctx->bdev_name, bdev_name, RAID1_MAX_NAME_LEN);
	dump_ctx->cb_fn = cb_fn;
	dump_ctx->cb_arg = cb_arg;

	rc = raid1_per_bdev_open(bdev_name, &dump_ctx->per_bdev);
	if (rc) {
		SPDK_ERRLOG("Could not open per_bdev\n");
		goto free_ctx;
	}

	rc = raid1_per_thread_open(&dump_ctx->per_bdev, &dump_ctx->per_thread);
	if (rc) {
		SPDK_ERRLOG("Could not open per_thread\n");
		goto close_per_bdev;
	}

	dump_ctx->sb_buf = spdk_dma_zmalloc(RAID1_SB_SIZE,
		dump_ctx->per_bdev.buf_align, NULL);
	if (!dump_ctx->sb_buf) {
		rc = -ENOMEM;
		SPDK_ERRLOG("Could not allocate sb_buf\n");
		goto close_per_thread;
	}

	raid1_per_io_init(&dump_ctx->per_io, &dump_ctx->per_thread, dump_ctx->sb_buf,
		RAID1_SB_START_BYTE, RAID1_SB_SIZE, RAID1_IO_READ,
		raid1_dump_sb_read_complete, dump_ctx);
	raid1_per_io_submit(&dump_ctx->per_io);
	return;

close_per_thread:
	raid1_per_thread_close(&dump_ctx->per_thread);
close_per_bdev:
	raid1_per_bdev_close(&dump_ctx->per_bdev);
free_ctx:
	free(dump_ctx);
call_cb:
	cb_fn(cb_arg, NULL, rc);

	return;
}

SPDK_LOG_REGISTER_COMPONENT(bdev_raid1)
