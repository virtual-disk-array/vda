from vda.grpc import portal_pb2
from vda.common.modules import DiskNode, ControllerNode, DiskArray
from vda.common.syncup import syncup_dn, syncup_cn, SyncupCtx


def manual_syncup_dn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn_name = request.dn_name
    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=dn_name) \
        .with_lockmode("update") \
        .one()
    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    syncup_dn(dn.dn_id, syncup_ctx)
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ManualSyncupDnReply(reply_info=reply_info)


def manual_syncup_cn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    cn_name = request.cn_name
    cn = session \
        .query(ControllerNode) \
        .filter_by(cn_name=cn_name) \
        .with_lockmode("update") \
        .one()
    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    syncup_cn(cn.cn_id, syncup_ctx)
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ManualSyncupCnReply(reply_info=reply_info)


def manual_syncup_da(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=da_name) \
        .with_lockmode("update") \
        .one()

    dn_id_list = []
    for grp in da.grps:
        for vd in grp.vds:
            dn_id_list.append(vd.pd.dn.dn_id)

    cn_id_list = []
    primary_cn_id = None
    for cntlr in da.cntlrs:
        if cntlr.primary:
            primary_cn_id = cntlr.cn.cn_id
        else:
            cn_id_list.append(cntlr.cn.cn_id)

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)

    for dn_id in dn_id_list:
        syncup_dn(dn_id, syncup_ctx)

    syncup_cn(primary_cn_id, syncup_ctx)

    for cn_id in cn_id_list:
        syncup_cn(cn_id, syncup_ctx)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ManualSyncupDaReply(reply_info=reply_info)
