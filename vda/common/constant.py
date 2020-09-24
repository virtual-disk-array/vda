from collections import namedtuple


ReplyInfo = namedtuple("ReplyInfo", ["reply_code", "reply_msg"])


class Constant:
    PACKAGE_NAME = "vda"
    CLI_NAME = "vda"
    CLI_DEFAULT_ADDR_PORT = "localhost:9520"
    CLI_DEFAULT_TIMEOUT = 10

    LOGGING_FMT = "%(asctime)s - %(process)d %(thread)d %(name)s - %(levelname)s - %(message)s"  # noqa: E501

    DEFAULT_PORTAL_LISTENER = "127.0.0.1"
    DEFAULT_PORTAL_MAX_WORKERS = 10
    DEFAULT_PORTAL_PORT = 9520
    DEFAULT_MONITOR_LISTENER = "127.0.0.1"
    DEFAULT_MONITOR_MAX_WORKERS = 10
    DEFAULT_MONITOR_PORT = 9620
    DEFAULT_DN_LISTENER = "127.0.0.1"
    DEFAULT_DN_MAX_WORKERS = 10
    DEFAULT_DN_PORT = 9720
    DEFAULT_DN_SOCK_PATH = "/var/tmp/spdk.sock"
    DEFAULT_DN_SOCK_TIMEOUT = 60
    DEFAULT_CN_LISTENER = "127.0.0.1"
    DEFAULT_CN_MAX_WORKERS = 10
    DEFAULT_CN_PORT = 9820
    DEFAULT_CN_SOCK_PATH = "/var/tmp/spdk.sock"
    DEFAULT_CN_SOCK_TIMEOUT = 60
    DEFAULT_DB_URI = "sqlite:////tmp/vda.db"

    MAX_HASH_CODE = 65536
    RES_NAME_LENGTH = 256
    RES_ID_LENGTH = 32
    PAGE_SIZE = 4096
    CLUSTER_SIZE_MB = 4
    CLUSTER_SIZE = CLUSTER_SIZE_MB * 1024 * 1024
    BDEV_PREFIX = "vda"
    ALLOCATE_BATCH_SIZE = 10
    DN_NVMF_PORT = 4420
    CN_NVMF_PORT = 4421
    NVMF_SN_LENGTH = 20
    NVMF_MODULE_NUMBER = "VDA_CONTROLLER"
    RAID0_STRIP_SIZE_KB = 64
    MAX_DN_LIST_SIZE = 100
    MAX_PD_LIST_SIZE = 100
    MAX_CN_LIST_SIZE = 100
    MAX_DA_LIST_SIZE = 100
    MAX_EXP_LIST_SIZE = 100
    MAX_SNAP_LIST_SIZE = 100

    DEFAULT_DN_TRANSPORT_CONF_STR = '{"trtype":"TCP"}'
    DEFAULT_CN_TRANSPORT_CONF_STR = '{"trtype":"TCP"}'
    DEFAULT_DN_LISTENER_CONF_STR = '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4420"}'  # noqa: E501
    DEFAULT_CN_LISTENER_CONF_STR = '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}'  # noqa: E501

    DEFAULT_MONITOR_METRIC_CONF = {
        "default": {
            "backlog_min_val": 0,
            "backlog_width": 100,
            "backlog_steps": 10,
            "process_min_val": 0,
            "process_width": 100,
            "process_steps": 10,
        },
    }

    dn_success = ReplyInfo(0, "success")
    dn_id_mismatch = ReplyInfo(1, "dn_id mismatch")
    dn_old_version = ReplyInfo(2, "old version")

    cn_success = ReplyInfo(0, "success")
    cn_id_mismatch = ReplyInfo(1, "cn_id mismatch")
    cn_old_version = ReplyInfo(2, "old version")

    UNINIT_MSG = "uninit"
    DEFAULT_LOCATION = "__default_location__"
