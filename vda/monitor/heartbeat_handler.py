from collections import namedtuple
import logging

import grpc

from vda.common.modules import DiskNode, ControllerNode, DiskArray
from vda.common.syncup import syncup_dn, syncup_cn, SyncupCtx
from vda.monitor.basic_handler import BasicHandler
from vda.grpc import (
    dn_agent_pb2,
    dn_agent_pb2_grpc,
    cn_agent_pb2,
    cn_agent_pb2_grpc,
)

logger = logging.getLogger(__name__)

DnBacklog = namedtuple("DnBacklog", [
    "dn_id", "dn_name", "error", "error_msg"])
CnBacklog = namedtuple("CnBacklog", [
    "cn_id", "cn_name", "error", "error_msg"])


class DnHeartbeatHandler(BasicHandler):

    def get_backlog_list(self, session, begin, end):
        dns = session \
            .query(DiskNode) \
            .filter(DiskNode.hash_code >= begin) \
            .filter(DiskNode.hash_code < end) \
            .filter(DiskNode.online == True) \
            .with_entities(
                DiskNode.dn_id,
                DiskNode.dn_name,
                DiskNode.error,
                DiskNode.error_msg,
            ) \
            .all()  # noqa: E712
        dn_backlog_list = []
        for dn in dns:
            dn_backlog = DnBacklog(
                dn_id=dn.dn_id,
                dn_name=dn.dn_name,
                error=dn.error,
                error_msg=dn.error_msg,
            )
            dn_backlog_list.append(dn_backlog)
        return dn_backlog_list

    def process_backlog(self, session, dn_backlog):
        with grpc.insecure_channel(dn_backlog.dn_name) as channel:
            stub = dn_agent_pb2_grpc.DnAgentStub(channel)
            request = dn_agent_pb2.DnHeartbeatRequest(
                dn_id=dn_backlog.dn_id,
            )
            try:
                reply = stub.DnHeartbeat(request)
            except Exception:
                logger.error("DnHeartbeat failed", exc_info=True)
                error = True
                error_msg = "dn_heartbeat_failed"
            else:
                if reply.reply_info.reply_code == 0:
                    error = False
                    error_msg = None
                else:
                    error = True
                    error_msg = reply.reply_info.reply_msg
            if error != dn_backlog.error \
               or error_msg != dn_backlog.error_msg:
                dn = session \
                    .query(DiskNode) \
                    .filter_by(dn_id=dn_backlog.dn_id) \
                    .with_lockmode("update") \
                    .one()
                dn.error = error
                dn.error_msg = dn.error_msg
                session.add(dn)
                session.commit()

                if not error:
                    syncup_ctx = SyncupCtx(session=session, req_id=None)
                    syncup_dn(dn.dn_id, syncup_ctx)


class CnHeartbeatHandler(BasicHandler):

    def get_backlog_list(self, session, begin, end):
        cns = session \
            .query(ControllerNode) \
            .filter(ControllerNode.hash_code >= begin) \
            .filter(ControllerNode.hash_code < end) \
            .filter(ControllerNode.online == True) \
            .with_entities(
                ControllerNode.cn_id,
                ControllerNode.cn_name,
                ControllerNode.error,
                ControllerNode.error_msg,
            ) \
            .all()  # noqa: E712
        cn_backlog_list = []
        for cn in cns:
            cn_backlog = CnBacklog(
                cn_id=cn.cn_id,
                cn_name=cn.cn_name,
                error=cn.error,
                error_msg=cn.error_msg,
            )
            cn_backlog_list.append(cn_backlog)
        return cn_backlog_list

    def process_backlog(self, session, cn_backlog):
        with grpc.insecure_channel(cn_backlog.cn_name) as channel:
            stub = cn_agent_pb2_grpc.CnAgentStub(channel)
            request = cn_agent_pb2.CnHeartbeatRequest(
                cn_id=cn_backlog.cn_id,
            )
            try:
                reply = stub.CnHeartbeat(request)
            except Exception:
                logger.error("CnHeartbeat failed", exc_info=True)
                error = True
                error_msg = "cn_heartbeat_failed"
            else:
                if reply.reply_info.reply_code == 0:
                    error = False
                    error_msg = None
                else:
                    error = True
                    error_msg = reply.reply_info.reply_msg
            if error != cn_backlog.error \
               or error_msg != cn_backlog.error_msg:
                cn = session \
                    .query(ControllerNode) \
                    .filter_by(cn_id=cn_backlog.cn_id) \
                    .with_lockmode("update") \
                    .one()

                cn_id_list = []
                if error:
                    # failover
                    for cntlr in cn.cntlrs:
                        if cntlr.primary:
                            da = session \
                                .query(DiskArray) \
                                .filter_by(da_id=cntlr.da_id) \
                                .with_lockmode("update") \
                                .one()
                            for cntlr1 in da.cntlrs:
                                if cntlr1.cntlr_idx != cntlr.cntlr_idx \
                                   and cntlr1.cn.online \
                                   and not cntlr1.cn.error:
                                    cntlr.primary = False
                                    cntlr1.primary = True
                                    cn.version += 1
                                    cntlr1.cn.version += 1
                                    session.add(cntlr)
                                    session.add(cntlr1)
                                    session.add(cntlr1.cn)
                                    cn_id_list.append(cntlr1.cn.cn_id)

                cn.error = error
                cn.error_msg = cn.error_msg
                session.add(cn)
                session.commit()

                syncup_ctx = SyncupCtx(session=session, req_id=None)
                if error:
                    for cn_id in cn_id_list:
                        syncup_cn(cn_id, syncup_ctx)
                else:
                    syncup_cn(cn.cn_id, syncup_ctx)
