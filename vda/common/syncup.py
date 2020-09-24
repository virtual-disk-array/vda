from collections import namedtuple
import logging

import grpc

from vda.common.constant import Constant
from vda.grpc import (
    dn_agent_pb2,
    dn_agent_pb2_grpc,
    cn_agent_pb2,
    cn_agent_pb2_grpc,
)
from vda.common.modules import (
    DiskNode,
    ControllerNode,
)


SyncupCtx = namedtuple("SyncupCtx", ["session", "req_id"])


logger = logging.getLogger(__name__)


def syncup_dn(dn_id, syncup_ctx):
    session = syncup_ctx.session
    req_id = syncup_ctx.req_id
    dn = session \
        .query(DiskNode) \
        .filter_by(dn_id=dn_id) \
        .with_lockmode("update") \
        .one()
    with grpc.insecure_channel(dn.dn_name) as channel:
        stub = dn_agent_pb2_grpc.DnAgentStub(channel)
        pd_cfg_list = []
        pd_dict = {}
        vd_dict = {}
        for pd in dn.pds:
            pd_dict[pd.pd_id] = pd
            vd_cfg_list = []
            for vd in pd.vds:
                vd_dict[vd.vd_id] = vd
                primary_cntlr = None
                for cntlr in vd.grp.da.cntlrs:
                    if cntlr.primary:
                        primary_cntlr = cntlr
                        break
                assert(primary_cntlr)
                vd_cfg = dn_agent_pb2.VdBackendCfg(
                    vd_id=vd.vd_id,
                    vd_clusters=vd.vd_clusters,
                    cntlr_id=primary_cntlr.cntlr_id,
                )
                vd_cfg_list.append(vd_cfg)
            pd_cfg = dn_agent_pb2.PdCfg(
                pd_id=pd.pd_id,
                pd_conf=pd.pd_conf,
                vd_cfg_list=vd_cfg_list,
            )
            pd_cfg_list.append(pd_cfg)
        dn_cfg = dn_agent_pb2.DnCfg(
            dn_id=dn.dn_id,
            version=dn.version,
            pd_cfg_list=pd_cfg_list,
        )
        request = dn_agent_pb2.SyncupDnRequest(
            req_id=req_id,
            dn_cfg=dn_cfg,
        )
        logger.info("SyncupDnRequest: %s", request)
        try:
            reply = stub.SyncupDn(request)
        except Exception:
            logger.error("SyncupDn failed", exc_info=True)
            dn.error = True
            dn.error_msg = "syncup_failed"
            session.add(dn)
            session.commit()
        else:
            logger.info("SyncupDnReply: %s", reply)
            if reply.reply_info.reply_code == Constant.dn_success.reply_code:
                dn_info = reply.dn_info
                dn.error = dn_info.error
                dn.error_msg = dn_info.error_msg
                session.add(dn)
                for pd_info in dn_info.pd_info_list:
                    pd = pd_dict[pd_info.pd_id]
                    pd.total_clusters = pd_info.total_clusters
                    pd.free_clusters = pd_info.free_clusters
                    pd.error = pd_info.error
                    pd.error_msg = pd_info.error_msg
                    session.add(pd)
                    for vd_info in pd_info.vd_info_list:
                        vd = vd_dict[vd_info.vd_id]
                        vd.dn_error = vd_info.error
                        vd.dn_error_msg = vd_info.error_msg
                        session.add(vd)
            else:
                dn.error = True
                dn.error_msg = reply.reply_info.reply_msg
                session.add(dn)
            session.commit()


def syncup_cn(cn_id, syncup_ctx):
    session = syncup_ctx.session
    req_id = syncup_ctx.req_id
    cn = session \
        .query(ControllerNode) \
        .filter_by(cn_id=cn_id) \
        .with_lockmode("update") \
        .one()
    cntlr_dict = {}
    grp_dict = {}
    exp_dict = {}
    vd_dict = {}
    with grpc.insecure_channel(cn.cn_name) as channel:
        stub = cn_agent_pb2_grpc.CnAgentStub(channel)
        da_cntlr_cfg_list = []
        for cntlr in cn.cntlrs:
            cntlr_dict[cntlr.cntlr_id] = cntlr
            da = cntlr.da
            cntlr_cfg_list = []
            for cntlr1 in da.cntlrs:
                cntlr_cfg = cn_agent_pb2.CntlrCfg(
                    cntlr_id=cntlr1.cntlr_id,
                    cntlr_idx=cntlr1.cntlr_idx,
                    primary=cntlr1.primary,
                    cn_name=cntlr1.cn.cn_name,
                    cn_listener_conf=cntlr1.cn.cn_listener_conf,
                )
                cntlr_cfg_list.append(cntlr_cfg)
            grp_cfg_list = []
            for grp in da.grps:
                grp_dict[grp.grp_id] = grp
                vd_cfg_list = []
                for vd in grp.vds:
                    vd_dict[vd.vd_id] = vd
                    vd_cfg = cn_agent_pb2.VdFrontendCfg(
                        vd_id=vd.vd_id,
                        vd_idx=vd.vd_idx,
                        vd_clusters=vd.vd_clusters,
                        dn_name=vd.pd.dn.dn_name,
                        dn_listener_conf=vd.pd.dn.dn_listener_conf,
                    )
                    vd_cfg_list.append(vd_cfg)
                grp_cfg = cn_agent_pb2.GrpCfg(
                    grp_id=grp.grp_id,
                    grp_idx=grp.grp_idx,
                    grp_size=grp.grp_size,
                    vd_cfg_list=vd_cfg_list,
                )
                grp_cfg_list.append(grp_cfg)
            exp_cfg_list = []
            for exp in da.exps:
                exp_dict[exp.exp_id] = exp
                exp_cfg = cn_agent_pb2.ExpCfg(
                    exp_id=exp.exp_id,
                    exp_name=exp.exp_name,
                    initiator_nqn=exp.initiator_nqn,
                    snap_name=exp.snap_name,
                )
                exp_cfg_list.append(exp_cfg)
            da_cntlr_cfg = cn_agent_pb2.DaCntlrCfg(
                da_id=da.da_id,
                da_name=da.da_name,
                cntlr_id=cntlr.cntlr_id,
                da_size=da.da_size,
                da_conf=da.da_conf,
                cntlr_cfg_list=cntlr_cfg_list,
                grp_cfg_list=grp_cfg_list,
                exp_cfg_list=exp_cfg_list,
            )
            da_cntlr_cfg_list.append(da_cntlr_cfg)
        cn_cfg = cn_agent_pb2.CnCfg(
            cn_id=cn.cn_id,
            version=cn.version,
            cn_listener_conf=cn.cn_listener_conf,
            da_cntlr_cfg_list=da_cntlr_cfg_list,
        )
        request = cn_agent_pb2.SyncupCnRequest(
            req_id=req_id,
            cn_cfg=cn_cfg,
        )
        logger.info("SyncupCnRequest: %s", request)
        try:
            reply = stub.SyncupCn(request)
        except Exception:
            logger.error("SyncupCn failed", exc_info=True)
            cn.error = True
            cn.error_msg = "syncup_failed"
            session.add(cn)
            session.commit()
        else:
            logger.info("SyncupCnReply: %s", reply)
            if reply.reply_info.reply_code == Constant.cn_success.reply_code:
                cn_info = reply.cn_info
                cn.error = cn_info.error
                cn.error_msg = cn_info.error_msg
                for da_cntlr_info in cn_info.da_cntlr_info_list:
                    cntlr = cntlr_dict[da_cntlr_info.cntlr_id]
                    cntlr.error = da_cntlr_info.error
                    cntlr.error_msg = da_cntlr_info.error_msg
                    session.add(cntlr)
                    da = cntlr.da
                    da_error = False
                    if da_cntlr_info.grp_info_list is not None:
                        # primary
                        da.da_details = da_cntlr_info.da_details
                        for grp_info in da_cntlr_info.grp_info_list:
                            grp = grp_dict[grp_info.grp_id]
                            grp.error = grp_info.error
                            grp.error_msg = grp_info.error_msg
                            session.add(grp)
                            if grp.error:
                                da_error = True
                            for vd_info in grp_info.vd_info_list:
                                vd = vd_dict[vd_info.vd_id]
                                vd.cn_error = vd_info.error
                                vd.cn_error_msg = vd_info.error_msg
                                session.add(vd)
                                if vd.cn_error or vd.dn_error:
                                    da_error = True
                    for cntlr1 in da.cntlrs:
                        if cntlr1.error:
                            da_error = True
                    for exp_info in da_cntlr_info.exp_info_list:
                        exp = exp_dict[exp_info.exp_id]
                        for es in exp.ess:
                            if es.cntlr_id == da_cntlr_info.cntlr_id:
                                es.error = exp_info.error
                                es.error_msg = exp_info.error_msg
                                session.add(es)
                                if es.error:
                                    da_error = True
                                break
                    da.error = da_error
                    session.add(da)
            else:
                cn.error = True
                cn.error_msg = reply.reply_info.reply_msg
                session.add(cn)
            session.commit()
