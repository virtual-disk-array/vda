#include "spdk/rpc.h"

#include "vbdev_raid1.h"

struct rpc_bdev_raid1_create {
	char *raid1_name;
	char *bdev0_name;
	char *bdev1_name;
	uint64_t strip_size_kb;
	uint64_t write_delay;
	uint64_t clean_ratio;
	uint64_t max_pending;
	uint64_t max_resync;
	bool synced;
};

static void
free_rpc_bdev_raid1_create(struct rpc_bdev_raid1_create *r)
{
	free(r->raid1_name);
	free(r->bdev0_name);
	free(r->bdev1_name);
}

static void
bdev_raid1_create_cb(void *cb_arg, int rc)
{
	struct spdk_jsonrpc_request *request = cb_arg;
	struct spdk_json_write_ctx *w;

	if (rc) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
	} else {
		w = spdk_jsonrpc_begin_result(request);
		spdk_json_write_bool(w, true);
		spdk_jsonrpc_end_result(request, w);
	}
}

static const struct spdk_json_object_decoder rpc_bdev_raid1_create_decoders[] = {
	{"raid1_name",  offsetof(struct rpc_bdev_raid1_create, raid1_name), spdk_json_decode_string},
	{"bdev0_name",  offsetof(struct rpc_bdev_raid1_create, bdev0_name), spdk_json_decode_string},
	{"bdev1_name",  offsetof(struct rpc_bdev_raid1_create, bdev1_name), spdk_json_decode_string},
	{"strip_size_kb",  offsetof(struct rpc_bdev_raid1_create, strip_size_kb), spdk_json_decode_uint64},
	{"write_delay",  offsetof(struct rpc_bdev_raid1_create, write_delay), spdk_json_decode_uint64, true},
	{"clean_ratio",  offsetof(struct rpc_bdev_raid1_create, clean_ratio), spdk_json_decode_uint64, true},
	{"max_pending",  offsetof(struct rpc_bdev_raid1_create, max_pending), spdk_json_decode_uint64, true},
	{"max_resync",  offsetof(struct rpc_bdev_raid1_create, max_resync), spdk_json_decode_uint64, true},
	{"synced", offsetof(struct rpc_bdev_raid1_create, synced), spdk_json_decode_bool, true},
};

static void
rpc_bdev_raid1_create(struct spdk_jsonrpc_request *request,
	const struct spdk_json_val *params)
{
	struct rpc_bdev_raid1_create req = {0};
	struct raid1_create_param param;

	if (spdk_json_decode_object(params, rpc_bdev_raid1_create_decoders,
			SPDK_COUNTOF(rpc_bdev_raid1_create_decoders),
			&req)) {
		SPDK_DEBUGLOG(vbdev_passthru, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
			"spdk_json_decode_object failed");
		goto cleanup;
	}

	param.bdev0_name = req.bdev0_name;
	param.bdev1_name = req.bdev1_name;
	param.strip_size = req.strip_size_kb * 1024;
	param.write_delay = req.write_delay == 0 ? RAID1_DEFAULT_WRITE_DELAY : req.write_delay;

	raid1_bdev_create(req.raid1_name, &param, bdev_raid1_create_cb, request);

cleanup:
	free_rpc_bdev_raid1_create(&req);
}
SPDK_RPC_REGISTER("bdev_raid1_create", rpc_bdev_raid1_create, SPDK_RPC_RUNTIME)

struct rpc_delete_raid1_bdev {
	char *raid1_name;
};

static void
free_rpc_delete_raid1_bdev(struct rpc_delete_raid1_bdev *r)
{
	free(r->raid1_name);
}

static void
raid1_bdev_delete_cb(void *cb_arg, int rc)
{
	struct spdk_jsonrpc_request *request = cb_arg;
	struct spdk_json_write_ctx *w;

	if (rc) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
	} else {
		w = spdk_jsonrpc_begin_result(request);
		spdk_json_write_bool(w, true);
		spdk_jsonrpc_end_result(request, w);
	}
}

static const struct spdk_json_object_decoder rpc_delete_raid1_bdev_decoders[] = {
	{"raid1_name", offsetof(struct rpc_delete_raid1_bdev, raid1_name), spdk_json_decode_string},
};

static void
spdk_rpc_delete_raid1_bdev(struct spdk_jsonrpc_request *request,
	const struct spdk_json_val *params)
{
	struct rpc_delete_raid1_bdev req = {};
	if (spdk_json_decode_object(params, rpc_delete_raid1_bdev_decoders,
			SPDK_COUNTOF(rpc_delete_raid1_bdev_decoders),
			&req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
			"spdk_json_decode_object failed");
		goto cleanup;
	}

	raid1_bdev_delete(req.raid1_name, raid1_bdev_delete_cb, request);

cleanup:
	free_rpc_delete_raid1_bdev(&req);
}
SPDK_RPC_REGISTER("delete_raid1_bdev", spdk_rpc_delete_raid1_bdev, SPDK_RPC_RUNTIME)
