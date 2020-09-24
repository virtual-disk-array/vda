import argparse
import json

from vda.common.constant import Constant
from vda.common.utils import loginit, create_db_args, get_db_args
from vda.monitor.launcher import launch_server


def main():
    parser = argparse.ArgumentParser(
        prog="vda_monitor",
        add_help=True,
    )

    parser.add_argument(
        "--listener",
        default=Constant.DEFAULT_MONITOR_LISTENER,
        help="monitor grpc listener",
    )
    parser.add_argument(
        "--port",
        default=Constant.DEFAULT_MONITOR_PORT,
        type=int,
        help="monitor grpc port",
    )
    parser.add_argument(
        "--max-workers",
        default=Constant.DEFAULT_MONITOR_MAX_WORKERS,
        type=int,
        help="monitor grpc worker count",
    )

    create_db_args(parser)

    parser.add_argument(
        "--total",
        type=int,
        default=1,
        help="total monitor instance count",
    )
    parser.add_argument(
        "--current",
        type=int,
        default=0,
        help="the current monitor number, from 0 to total-1",
    )
    parser.add_argument(
        "--dn-heartbeat-interval",
        type=int,
        default=5,
        help="dn heartbeat interval",
    )
    parser.add_argument(
        "--dn-heartbeat-workers",
        type=int,
        default=10,
        help="dn heartbeat thread pool size",
    )
    parser.add_argument(
        "--cn-heartbeat-interval",
        type=int,
        default=5,
        help="cn heartbeat interval",
    )
    parser.add_argument(
        "--cn-heartbeat-workers",
        type=int,
        default=10,
        help="cn heartbeat thread pool size",
    )
    parser.add_argument(
        "--da-syncup-interval",
        type=int,
        default=5,
        help="da syncup interval",
    )
    parser.add_argument(
        "--da-syncup-workers",
        type=int,
        default=10,
        help="da syncup thread pool size",
    )
    parser.add_argument(
        "--dn-syncup-interval",
        type=int,
        default=120,
        help="dn syncup interval",
    )
    parser.add_argument(
        "--dn-syncup-workers",
        type=int,
        default=10,
        help="dn syncup thread pool size",
    )
    parser.add_argument(
        "--cn-syncup-interval",
        type=int,
        default=120,
        help="cn syncup interval",
    )
    parser.add_argument(
        "--cn-syncup-workers",
        type=int,
        default=10,
        help="cn syncup thread pool size",
    )
    parser.add_argument(
        "--monitor-metric-conf",
        default="{}",
        help="monitor metric configuration",
    )

    loginit()

    args = parser.parse_args()
    db_uri, db_kwargs = get_db_args(args)
    launch_server(
        listener=args.listener,
        port=args.port,
        max_workers=args.max_workers,
        db_uri=db_uri,
        db_kwargs=db_kwargs,
        total=args.total,
        current=args.current,
        dn_heartbeat_interval=args.dn_heartbeat_interval,
        dn_heartbeat_workers=args.dn_heartbeat_workers,
        cn_heartbeat_interval=args.cn_heartbeat_interval,
        cn_heartbeat_workers=args.cn_heartbeat_workers,
        da_syncup_interval=args.da_syncup_interval,
        da_syncup_workers=args.da_syncup_workers,
        dn_syncup_interval=args.dn_syncup_interval,
        dn_syncup_workers=args.dn_syncup_workers,
        cn_syncup_interval=args.cn_syncup_interval,
        cn_syncup_workers=args.cn_syncup_workers,
        monitor_metric_conf=json.loads(args.monitor_metric_conf),
    )
