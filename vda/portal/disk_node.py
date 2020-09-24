import uuid

from vda.common.constant import Constant
from vda.grpc import portal_pb2
from vda.common.modules import DiskNode
from vda.common.syncup import syncup_dn, SyncupCtx


def create_dn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn_id = uuid.uuid4().hex
    dn_name = request.dn_name

    dn = DiskNode(
        dn_id=dn_id,
        dn_name=dn_name,
        dn_listener_conf=request.dn_listener_conf,
        online=request.online,
        location=request.location,
        hash_code=request.hash_code,
        version=0,
        error=True,
        error_msg=Constant.UNINIT_MSG,
    )
    session.add(dn)
    session.commit()

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    syncup_dn(dn.dn_id, syncup_ctx)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.CreateDnReply(reply_info=reply_info)


def delete_dn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=request.dn_name) \
        .with_lockmode("update") \
        .one()
    session.delete(dn)
    session.commit()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.DeleteDnReply(reply_info=reply_info)


def modify_dn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=request.dn_name) \
        .with_lockmode("update") \
        .one()
    attr = request.WhichOneof("attr")
    if attr == "new_online":
        dn.online = request.new_online
    elif attr == "new_hash_code":
        dn.hash_code = request.new_hash_code
    else:
        assert(False)
    session.add(dn)
    session.commit()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ModifyDnReply(reply_info=reply_info)


def list_dn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    query = session.query(DiskNode)
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
    dns = query.all()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    dn_msg_list = []
    for dn in dns:
        dn_msg = portal_pb2.DnMsg(
            dn_id=dn.dn_id,
            dn_name=dn.dn_name,
            dn_listener_conf=dn.dn_listener_conf,
            online=dn.online,
            location=dn.location,
            hash_code=dn.hash_code,
            version=dn.version,
            error=dn.error,
            error_msg=dn.error_msg,
        )
        dn_msg_list.append(dn_msg)
    return portal_pb2.ListDnReply(
        reply_info=reply_info, dn_msg_list=dn_msg_list)


def get_dn(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=request.dn_name) \
        .one()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    dn_msg = portal_pb2.DnMsg(
        dn_id=dn.dn_id,
        dn_name=dn.dn_name,
        dn_listener_conf=dn.dn_listener_conf,
        online=dn.online,
        location=dn.location,
        hash_code=dn.hash_code,
        version=dn.version,
        error=dn.error,
        error_msg=dn.error_msg,
    )
    pd_name_list = []
    for pd in dn.pds:
        pd_name_list.append(pd.pd_name)
    return portal_pb2.GetDnReply(
        reply_info=reply_info,
        dn_msg=dn_msg,
        pd_name_list=pd_name_list,
    )
