#include "spdk/stdinc.h"

#include "spdk/config.h"
#include "spdk/env.h"
#include "spdk/event.h"

static void
vda_dataplane_started(void *arg1)
{
	SPDK_NOTICELOG("vda_dataplane is running\n");
}

int
main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	int rc;

	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "vda_dataplane";
	if ((rc = spdk_app_parse_args(argc, argv, &opts,
				NULL, NULL, NULL, NULL)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		return rc;
	}
	rc = spdk_app_start(&opts, vda_dataplane_started, NULL);
	spdk_app_fini();

	return rc;
}
