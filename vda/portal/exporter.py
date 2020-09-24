import uuid

from vda.grpc import portal_pb2
from vda.common.modules import (
    DiskArray,
    ControllerNode,
    Exporter,
    ExpState,
)
from vda.common.constant import Constant
from vda.common.name_fmt import NameFmt
from vda.common.syncup import syncup_cn, SyncupCtx


def create_exp(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    exp_id = uuid.uuid4().hex

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=request.da_name) \
        .with_lockmode("update") \
        .one()

    exp = Exporter(
        exp_id=exp_id,
        exp_name=request.exp_name,
        da_id=da.da_id,
        initiator_nqn=request.initiator_nqn,
        snap_name=request.snap_name,
    )
    session.add(exp)

    cn_id_list = []
    for cntlr in da.cntlrs:
        es_id = uuid.uuid4().hex
        es = ExpState(
            es_id=es_id,
            exp_id=exp_id,
            cntlr_id=cntlr.cntlr_id,
            error=True,
            error_msg=Constant.UNINIT_MSG,
        )
        session.add(es)

        cn = session \
            .query(ControllerNode) \
            .filter_by(cn_id=cntlr.cn.cn_id) \
            .with_lockmode("update") \
            .one()
        cn.version += 1
        session.add(cn)
        cn_id_list.append(cn.cn_id)

    da.error = True
    session.add(da)

    session.commit()

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    for cn_id in cn_id_list:
        syncup_cn(cn_id, syncup_ctx)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.CreateExpReply(reply_info=reply_info)


def delete_exp(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name
    exp_name = request.exp_name

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=da_name) \
        .with_lockmode("update") \
        .one()

    exp = session \
        .query(Exporter) \
        .filter_by(da_id=da.da_id) \
        .filter_by(exp_name=exp_name) \
        .with_lockmode("update") \
        .one()

    cn_id_list = []
    for es in exp.ess:
        cn_id = es.cntlr.cn.cn_id
        session.delete(es)
        cn = session \
            .query(ControllerNode) \
            .filter_by(cn_id=cn_id) \
            .with_lockmode("update") \
            .one()
        cn.version += 1
        session.add(cn)
        cn_id_list.append(cn_id)

    da.error = True
    session.add(da)
    session.delete(exp)

    session.commit()

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    for cn_id in cn_id_list:
        syncup_cn(cn_id, syncup_ctx)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.DeleteExpReply(reply_info=reply_info)


def modify_exp(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name
    exp_name = request.exp_name

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=da_name) \
        .with_lockmode("update") \
        .one()

    exp = session \
        .query(Exporter) \
        .filter_by(da_id=da.da_id) \
        .filter_by(exp_name=exp_name) \
        .with_lockmode("update") \
        .one()

    attr = request.WhichOneof("attr")
    if attr == "new_exp_name":
        exp.exp_name = request.new_exp_name
    else:
        assert(False)
    session.add(exp)
    session.commit()

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ModifyExpReply(reply_info=reply_info)


def list_exp(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=da_name) \
        .with_lockmode("update") \
        .one()

    query = session.query(Exporter).filter_by(da_id=da.da_id)
    if request.offset:
        query = query.offset(request.offset)
    if request.limit:
        query = query.limit(request.limit)
    if request.set_initiator_nqn:
        query = query.filter_by(initiator_nqn=request.initiator_nqn)
    exps = query.all()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    exp_msg_list = []
    for exp in exps:
        es_msg_list = []
        for es in exp.ess:
            es_msg = portal_pb2.EsMsg(
                es_id=es.es_id,
                cntlr_idx=es.cntlr.cntlr_idx,
                cn_name=es.cntlr.cn.cn_name,
                cn_listener_conf=es.cntlr.cn.cn_listener_conf,
                error=es.error,
                error_msg=es.error_msg,
            )
            es_msg_list.append(es_msg)
        exp_nqn = NameFmt.exp_nqn_name(da.da_name, exp.exp_name)
        exp_msg = portal_pb2.ExpMsg(
            exp_id=exp.exp_id,
            exp_name=exp.exp_name,
            exp_nqn=exp_nqn,
            da_name=da.da_name,
            initiator_nqn=exp.initiator_nqn,
            snap_name=exp.snap_name,
            es_msg_list=es_msg_list,
        )
        exp_msg_list.append(exp_msg)
    return portal_pb2.ListExpReply(
        reply_info=reply_info,
        exp_msg_list=exp_msg_list,
    )


def get_exp(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name
    exp_name = request.exp_name

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=da_name) \
        .with_lockmode("update") \
        .one()

    exp = session \
        .query(Exporter) \
        .filter_by(da_id=da.da_id) \
        .filter_by(exp_name=exp_name) \
        .with_lockmode("update") \
        .one()

    es_msg_list = []
    for es in exp.ess:
        es_msg = portal_pb2.EsMsg(
            es_id=es.es_id,
            cntlr_idx=es.cntlr.cntlr_idx,
            cn_name=es.cntlr.cn.cn_name,
            cn_listener_conf=es.cntlr.cn.cn_listener_conf,
            error=es.error,
            error_msg=es.error_msg,
        )
        es_msg_list.append(es_msg)
    exp_nqn = NameFmt.exp_nqn_name(da.da_name, exp.exp_name)
    exp_msg = portal_pb2.ExpMsg(
        exp_id=exp.exp_id,
        exp_name=exp.exp_name,
        exp_nqn=exp_nqn,
        da_name=da.da_name,
        initiator_nqn=exp.initiator_nqn,
        snap_name=exp.snap_name,
        es_msg_list=es_msg_list,
    )
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.GetExpReply(
        reply_info=reply_info,
        exp_msg=exp_msg,
    )
