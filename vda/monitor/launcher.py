from concurrent import futures
import logging

import grpc

from vda.common.constant import Constant
from vda.grpc import monitor_pb2_grpc, monitor_pb2
from vda.monitor.basic_handler import HandlerMetricConf
from vda.monitor.heartbeat_handler import (
    DnHeartbeatHandler,
    CnHeartbeatHandler,
)
from vda.monitor.syncup_handler import (
    DaSyncupHandler,
    DnSyncupHandler,
    CnSyncupHandler,
)


logger = logging.getLogger(__name__)


class MonitorServicer(monitor_pb2_grpc.MonitorServicer):

    def __init__(self, dhh, chh, da_sh, dn_sh, cn_sh):
        self.dhh = dhh
        self.chh = chh
        self.da_sh = da_sh
        self.dn_sh = dn_sh
        self.cn_sh = cn_sh

    def GetMonitorMetric(self, request, context):
        ddh_handler_metric = self.ddh.get_handler_metric()
        ddh_metric = monitor_pb2.HandlerMetric(**ddh_handler_metric)
        chh_handler_metric = self.chh.get_handler_metric()
        chh_metric = monitor_pb2.HandlerMetric(**chh_handler_metric)
        da_sh_handler_metric = self.da_sh.get_handler_metric()
        da_sh_metric = monitor_pb2.HandlerMetric(**da_sh_handler_metric)
        dn_sh_handler_metric = self.dn_sh.get_handler_metric()
        dn_sh_metric = monitor_pb2.HandlerMetric(**dn_sh_handler_metric)
        cn_sh_handler_metric = self.cn_sh.get_handler_metric()
        cn_sh_metric = monitor_pb2.HandlerMetric(**cn_sh_handler_metric)
        return monitor_pb2.GetMonitorMetricReply(
            ddh_metric=ddh_metric,
            chh_metric=chh_metric,
            da_sh_metric=da_sh_metric,
            dn_sh_metric=dn_sh_metric,
            cn_sh_metric=cn_sh_metric,
        )


def get_handler_metric_conf(name, default_conf, custom_conf):
    conf = {}
    if "default" in default_conf:
        for key in default_conf["default"]:
            conf[key] = default_conf["default"][key]

    if name in default_conf:
        for key in default_conf[name]:
            conf[key] = default_conf[name][key]

    if "default" in custom_conf:
        for key in custom_conf["default"]:
            conf[key] = custom_conf["default"][key]

    if name in custom_conf:
        for key in custom_conf[name]:
            conf[key] = custom_conf[name][key]

    return HandlerMetricConf(
        backlog_min_val=conf["backlog_min_val"],
        backlog_width=conf["backlog_width"],
        backlog_steps=conf["backlog_steps"],
        process_min_val=conf["process_min_val"],
        process_width=conf["process_width"],
        process_steps=conf["process_steps"],
    )


def launch_server(
        listener,
        port,
        max_workers,
        db_uri,
        db_kwargs,
        total,
        current,
        dn_heartbeat_interval,
        dn_heartbeat_workers,
        cn_heartbeat_interval,
        cn_heartbeat_workers,
        da_syncup_interval,
        da_syncup_workers,
        dn_syncup_interval,
        dn_syncup_workers,
        cn_syncup_interval,
        cn_syncup_workers,
        monitor_metric_conf,
):
    dhh_metric_conf = get_handler_metric_conf(
        "dhh",
        Constant.DEFAULT_MONITOR_METRIC_CONF,
        monitor_metric_conf,
    )
    dhh = DnHeartbeatHandler(
        db_uri=db_uri,
        db_kwargs=db_kwargs,
        total=total,
        current=current,
        interval=dn_heartbeat_interval,
        max_workers=dn_heartbeat_workers,
        handler_metric_conf=dhh_metric_conf,
    )
    dhh.start()

    chh_metric_conf = get_handler_metric_conf(
        "chh",
        Constant.DEFAULT_MONITOR_METRIC_CONF,
        monitor_metric_conf,
    )
    chh = CnHeartbeatHandler(
        db_uri=db_uri,
        db_kwargs=db_kwargs,
        total=total,
        current=current,
        interval=cn_heartbeat_interval,
        max_workers=cn_heartbeat_workers,
        handler_metric_conf=chh_metric_conf,
    )
    chh.start()

    da_sh_metric_conf = get_handler_metric_conf(
        "da_sh",
        Constant.DEFAULT_MONITOR_METRIC_CONF,
        monitor_metric_conf,
    )
    da_sh = DaSyncupHandler(
        db_uri=db_uri,
        db_kwargs=db_kwargs,
        total=total,
        current=current,
        interval=da_syncup_interval,
        max_workers=da_syncup_workers,
        handler_metric_conf=da_sh_metric_conf,
    )
    da_sh.start()

    dn_sh_metric_conf = get_handler_metric_conf(
        "dn_sh",
        Constant.DEFAULT_MONITOR_METRIC_CONF,
        monitor_metric_conf,
    )
    dn_sh = DnSyncupHandler(
        db_uri=db_uri,
        db_kwargs=db_kwargs,
        total=total,
        current=current,
        interval=dn_syncup_interval,
        max_workers=dn_syncup_workers,
        handler_metric_conf=dn_sh_metric_conf,
    )
    dn_sh.start()

    cn_sh_metric_conf = get_handler_metric_conf(
        "cn_sh",
        Constant.DEFAULT_MONITOR_METRIC_CONF,
        monitor_metric_conf,
    )
    cn_sh = CnSyncupHandler(
        db_uri=db_uri,
        db_kwargs=db_kwargs,
        total=total,
        current=current,
        interval=cn_syncup_interval,
        max_workers=cn_syncup_workers,
        handler_metric_conf=cn_sh_metric_conf,
    )
    cn_sh.start()

    server = grpc.server(futures.ThreadPoolExecutor(
        max_workers=max_workers))
    monitor_pb2_grpc.add_MonitorServicer_to_server(
        MonitorServicer(
            dhh=dhh,
            chh=chh,
            da_sh=da_sh,
            dn_sh=dn_sh,
            cn_sh=cn_sh,
        ),
        server,
    )
    addr_port = f"{listener}:{port}"
    server.add_insecure_port(addr_port)
    server.start()
    logger.info("vda_monitor is running")
    server.wait_for_termination()
