import uuid

from vda.grpc import portal_pb2
from vda.common.modules import ControllerNode
from vda.common.constant import Constant
from vda.common.syncup import syncup_cn, SyncupCtx


def create_cn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    cn_id = uuid.uuid4().hex
    cn = ControllerNode(
        cn_id=cn_id,
        cn_name=request.cn_name,
        cn_listener_conf=request.cn_listener_conf,
        online=request.online,
        location=request.location,
        hash_code=request.hash_code,
        version=0,
        error=True,
        error_msg=Constant.UNINIT_MSG,
    )
    session.add(cn)
    session.commit()

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    syncup_cn(cn.cn_id, syncup_ctx)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.CreateCnReply(reply_info=reply_info)


def delete_cn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    cn = session \
        .query(ControllerNode) \
        .filter_by(cn_name=request.cn_name) \
        .with_lockmode("update") \
        .one()
    session.delete(cn)
    session.commit()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.DeleteCnReply(reply_info=reply_info)


def modify_cn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    cn = session \
        .query(ControllerNode) \
        .filter_by(cn_name=request.cn_name) \
        .with_lockmode("update") \
        .one()
    attr = request.WhichOneof("attr")
    if attr == "new_online":
        cn.online = request.new_online
    elif attr == "new_hash_code":
        cn.hash_code = request.new_hash_code
    else:
        assert(False)
    session.add(cn)
    session.commit()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ModifyCnReply(reply_info=reply_info)


def list_cn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    query = session.query(ControllerNode)
    if request.offset:
        query = query.offset(request.offset)
    if request.limit:
        query = query.limit(request.limit)
    if request.set_online:
        query = query.filter_by(online=request.online)
    if request.set_location:
        query = query.filter_by(location=request.location)
    if request.set_hash_code:
        query = query.filter_by(hash_code=request.hash_code)
    if request.set_error:
        query = query.filter_by(error=request.error)
    cns = query.all()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    cn_msg_list = []
    for cn in cns:
        cn_msg = portal_pb2.CnMsg(
            cn_id=cn.cn_id,
            cn_name=cn.cn_name,
            cn_listener_conf=cn.cn_listener_conf,
            online=cn.online,
            location=cn.location,
            hash_code=cn.hash_code,
            version=cn.version,
            error=cn.error,
            error_msg=cn.error_msg,
        )
        cn_msg_list.append(cn_msg)
    return portal_pb2.ListCnReply(
        reply_info=reply_info,
        cn_msg_list=cn_msg_list,
    )


def get_cn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    cn = session \
        .query(ControllerNode) \
        .filter_by(cn_name=request.cn_name) \
        .one()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    cn_msg = portal_pb2.CnMsg(
        cn_id=cn.cn_id,
        cn_name=cn.cn_name,
        cn_listener_conf=cn.cn_listener_conf,
        online=cn.online,
        location=cn.location,
        hash_code=cn.hash_code,
        version=cn.version,
        error=cn.error,
        error_msg=cn.error_msg,
    )
    cntlr_msg_list = []
    for cntlr in cn.cntlrs:
        cntlr_msg = portal_pb2.CntlrMsg(
            cntlr_id=cntlr.cntlr_id,
            da_name=cntlr.da.da_name,
            cntlr_idx=cntlr.cntlr_idx,
            cn_name=cntlr.cn.cn_name,
            primary=cntlr.primary,
            error=cntlr.error,
            error_msg=cntlr.error_msg,
        )
        cntlr_msg_list.append(cntlr_msg)
    return portal_pb2.GetCnReply(
        reply_info=reply_info,
        cn_msg=cn_msg,
        cntlr_msg_list=cntlr_msg_list,
    )
