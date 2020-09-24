import argparse

from vda.common.constant import Constant
from vda.common.utils import loginit, create_db_args, get_db_args
from vda.portal.launcher import launch_server


def main():
    parser = argparse.ArgumentParser(
        prog="vda_portal",
        add_help=True,
    )
    parser.add_argument(
        "--listener",
        default=Constant.DEFAULT_PORTAL_LISTENER,
        help="portal grpc listener",
    )
    parser.add_argument(
        "--port",
        default=Constant.DEFAULT_PORTAL_PORT,
        type=int,
        help="portal grpc port",
    )
    parser.add_argument(
        "--max-workers",
        default=Constant.DEFAULT_PORTAL_MAX_WORKERS,
        type=int,
        help="portal grpc worker count",
    )
    create_db_args(parser)

    loginit()

    args = parser.parse_args()
    db_uri, db_kwargs = get_db_args(args)
    launch_server(
        listener=args.listener,
        port=args.port,
        max_workers=args.max_workers,
        db_uri=db_uri,
        db_kwargs=db_kwargs,
    )
