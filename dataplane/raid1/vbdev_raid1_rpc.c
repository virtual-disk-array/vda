#include "vbdev_raid1.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"

struct rpc_bdev_raid1_create {
		char *raid1_name;
};

static void
free_rpc_bdev_raid1_create(struct rpc_bdev_raid1_create *r)
{
		free(r->raid1_name);
}

static const struct spdk_json_object_decoder rpc_bdev_raid1_create_decoders[] = {
		{"raid1_name",  offsetof(struct rpc_bdev_raid1_create, raid1_name), spdk_json_decode_string},
};

static void
rpc_bdev_raid1_create(struct spdk_jsonrpc_request *request,
		const struct spdk_json_val *params)
{
		struct rpc_bdev_raid1_create req = {NULL};
		struct spdk_json_write_ctx *w;

		if (spdk_json_decode_object(params, rpc_bdev_raid1_create_decoders,
						SPDK_COUNTOF(rpc_bdev_raid1_create_decoders),
						&req)) {
				SPDK_DEBUGLOG(vbdev_passthru, "spdk_json_decode_object failed\n");
				spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						"spdk_json_decode_object failed");
				goto cleanup;
		}

		raid1_bdev_create(req.raid1_name);
		w = spdk_jsonrpc_begin_result(request);
		spdk_json_write_string(w, req.raid1_name);
		spdk_jsonrpc_end_result(request, w);

cleanup:
		free_rpc_bdev_raid1_create(&req);
}
SPDK_RPC_REGISTER("bdev_raid1_create", rpc_bdev_raid1_create, SPDK_RPC_RUNTIME)
