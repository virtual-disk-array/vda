#include "vbdev_susres.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"

/* Structure to hold the parameters for this RPC method. */
struct rpc_bdev_susres_create {
	char *base_bdev_name;
	char *name;
};

/* Free the allocated memory resource after the RPC handling. */
static void
free_rpc_bdev_susres_create(struct rpc_bdev_susres_create *r)
{
	free(r->base_bdev_name);
	free(r->name);
}

/* Structure to decode the input parameters for this RPC method. */
static const struct spdk_json_object_decoder rpc_bdev_susres_create_decoders[] = {
	{"base_bdev_name", offsetof(struct rpc_bdev_susres_create, base_bdev_name), spdk_json_decode_string},
	{"name", offsetof(struct rpc_bdev_susres_create, name), spdk_json_decode_string},
};

/* Decode the parameters for this RPC method and properly construct the susres
 * device. Error status returned in the failed cases.
 */
static void
rpc_bdev_susres_create(struct spdk_jsonrpc_request *request,
			 const struct spdk_json_val *params)
{
	struct rpc_bdev_susres_create req = {NULL};
	struct spdk_json_write_ctx *w;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_susres_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_susres_create_decoders),
				    &req)) {
		SPDK_DEBUGLOG(vbdev_susres, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = bdev_susres_create_disk(req.base_bdev_name, req.name);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_susres_create(&req);
}
SPDK_RPC_REGISTER("bdev_susres_create", rpc_bdev_susres_create, SPDK_RPC_RUNTIME)

struct rpc_bdev_susres_delete {
	char *name;
};

static void
free_rpc_bdev_susres_delete(struct rpc_bdev_susres_delete *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_susres_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_susres_delete, name), spdk_json_decode_string},
};

static void
rpc_bdev_susres_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	spdk_jsonrpc_send_bool_response(request, bdeverrno == 0);
}

static void
rpc_bdev_susres_delete(struct spdk_jsonrpc_request *request,
			 const struct spdk_json_val *params)
{
	struct rpc_bdev_susres_delete req = {NULL};
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_bdev_susres_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_susres_delete_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	bdev_susres_delete_disk(bdev, rpc_bdev_susres_delete_cb, request);

cleanup:
	free_rpc_bdev_susres_delete(&req);
}
SPDK_RPC_REGISTER("bdev_susres_delete", rpc_bdev_susres_delete, SPDK_RPC_RUNTIME)

struct rpc_bdev_susres_suspend {
	char *name;
};

static void
free_rpc_bdev_susres_suspend(struct rpc_bdev_susres_suspend *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_susres_suspend_decoders[] = {
	{"name", offsetof(struct rpc_bdev_susres_suspend, name), spdk_json_decode_string},
};

static void
rpc_bdev_susres_suspend_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	spdk_jsonrpc_send_bool_response(request, bdeverrno == 0);
}

static void
rpc_bdev_susres_suspend(struct spdk_jsonrpc_request *request,
			 const struct spdk_json_val *params)
{
	struct rpc_bdev_susres_suspend req = {NULL};
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_bdev_susres_suspend_decoders,
				    SPDK_COUNTOF(rpc_bdev_susres_suspend_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev_susres_suspend_disk(req.name, rpc_bdev_susres_suspend_cb, request);

cleanup:
	free_rpc_bdev_susres_suspend(&req);
}
SPDK_RPC_REGISTER("bdev_susres_suspend", rpc_bdev_susres_suspend, SPDK_RPC_RUNTIME)

struct rpc_bdev_susres_resume {
	char *base_bdev_name;
	char *name;
};

static void
free_rpc_bdev_susres_resume(struct rpc_bdev_susres_resume *req)
{
	free(req->base_bdev_name);
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_susres_resume_decoders[] = {
	{"base_bdev_name", offsetof(struct rpc_bdev_susres_resume, base_bdev_name), spdk_json_decode_string},
	{"name", offsetof(struct rpc_bdev_susres_resume, name), spdk_json_decode_string},
};

static void
rpc_bdev_susres_resume_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	spdk_jsonrpc_send_bool_response(request, bdeverrno == 0);
}

static void
rpc_bdev_susres_resume(struct spdk_jsonrpc_request *request,
			 const struct spdk_json_val *params)
{
	struct rpc_bdev_susres_resume req = {NULL};
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_bdev_susres_resume_decoders,
				    SPDK_COUNTOF(rpc_bdev_susres_resume_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev_susres_resume_disk(req.base_bdev_name, req.name, rpc_bdev_susres_resume_cb, request);

cleanup:
	free_rpc_bdev_susres_resume(&req);
}
SPDK_RPC_REGISTER("bdev_susres_resume", rpc_bdev_susres_resume, SPDK_RPC_RUNTIME)
