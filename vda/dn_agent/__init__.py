import argparse
import json

from vda.common.constant import Constant
from vda.common.utils import loginit
from vda.dn_agent.launcher import launch_server


def main():
    parser = argparse.ArgumentParser(
        prog="vda_dn_agent",
        add_help=True,
    )
    parser.add_argument(
        "--listener",
        default=Constant.DEFAULT_DN_LISTENER,
        help="dn agent grpc listener",
    )
    parser.add_argument(
        "--port",
        default=Constant.DEFAULT_DN_PORT,
        type=int,
        help="dn agent grpc port",
    )
    parser.add_argument(
        "--max-workers",
        default=Constant.DEFAULT_DN_MAX_WORKERS,
        type=int,
        help="dn agent grpc worker count",
    )
    parser.add_argument(
        "--sock-path",
        default=Constant.DEFAULT_DN_SOCK_PATH,
        help="spdk sock path for dn",
    )
    parser.add_argument(
        "--sock-timeout",
        default=Constant.DEFAULT_DN_SOCK_TIMEOUT,
        help="spdk sock timeout for dn",
    )
    parser.add_argument(
        "--transport-conf",
        default=Constant.DEFAULT_DN_TRANSPORT_CONF_STR,
        help="nvmeof transport configuration",
    )
    parser.add_argument(
        "--listener-conf",
        default=Constant.DEFAULT_DN_LISTENER_CONF_STR,
        help="nvmeof listener configuration"
    )
    parser.add_argument(
        "--local-store",
        default=None,
        help="local file path for storing the syncup request",
    )

    loginit()

    args = parser.parse_args()
    transport_conf = json.loads(args.transport_conf)
    listener_conf = json.loads(args.listener_conf)
    launch_server(
        listener=args.listener,
        port=args.port,
        max_workers=args.max_workers,
        sock_path=args.sock_path,
        sock_timeout=args.sock_timeout,
        transport_conf=transport_conf,
        listener_conf=listener_conf,
        local_store=args.local_store,
    )
