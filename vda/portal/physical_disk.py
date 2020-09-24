import uuid

from vda.grpc import portal_pb2
from vda.common.modules import DiskNode, PhysicalDisk
from vda.common.constant import Constant
from vda.common.syncup import syncup_dn, SyncupCtx


def create_pd(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    pd_id = uuid.uuid4().hex
    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=request.dn_name) \
        .with_lockmode("update") \
        .one()
    pd = PhysicalDisk(
        pd_id=pd_id,
        pd_name=request.pd_name,
        total_clusters=0,
        free_clusters=0,
        pd_conf=request.pd_conf,
        online=request.online,
        dn_id=dn.dn_id,
        error=True,
        error_msg=Constant.UNINIT_MSG,
    )
    session.add(pd)
    dn.version += 1
    session.add(dn)
    session.commit()

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    syncup_dn(dn.dn_id, syncup_ctx)
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.CreatePdReply(reply_info=reply_info)


def delete_pd(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn_name = request.dn_name
    pd_name = request.pd_name

    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=dn_name) \
        .with_lockmode("update") \
        .one()

    pd = session \
        .query(PhysicalDisk) \
        .filter_by(dn_id=dn.dn_id) \
        .filter_by(pd_name=pd_name) \
        .with_lockmode("update") \
        .one()

    session.delete(pd)
    dn.version += 1
    session.add(dn)
    session.commit()

    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    syncup_dn(dn.dn_id, syncup_ctx)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.DeletePdReply(reply_info=reply_info)


def modify_pd(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn_name = request.dn_name
    pd_name = request.pd_name

    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=dn_name) \
        .with_lockmode("update") \
        .one()

    pd = session \
        .query(PhysicalDisk) \
        .filter_by(dn_id=dn.dn_id) \
        .filter_by(pd_name=pd_name) \
        .with_lockmode("update") \
        .one()

    attr = request.WhichOneof("attr")
    if attr == "new_pd_name":
        pd.pd_name = request.new_pd_name
    elif attr == "new_online":
        pd.online = request.online
    else:
        assert(False)
    session.add(pd)
    session.commit()

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ModifyPdReply(reply_info=reply_info)


def list_pd(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn_name = request.dn_name

    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=dn_name) \
        .with_lockmode("update") \
        .one()

    query = session.query(PhysicalDisk).filter_by(dn_id=dn.dn_id)
    if request.offset:
        query = query.offset(request.offset)
    if request.limit:
        query = query.limit(request.limit)
    if request.set_online:
        query = query.filter_by(online=request.online)
    if request.set_error:
        query = query.filter_by(error=request.set_error)
    pds = query.all()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    pd_msg_list = []
    for pd in pds:
        pd_msg = portal_pb2.PdMsg(
            pd_id=pd.pd_id,
            dn_name=dn.dn_name,
            pd_name=pd.pd_name,
            total_size=pd.total_clusters*Constant.CLUSTER_SIZE,
            free_size=pd.free_clusters*Constant.CLUSTER_SIZE,
            pd_conf=pd.pd_conf,
            online=pd.online,
            error=pd.error,
            error_msg=pd.error_msg,
        )
        pd_msg_list.append(pd_msg)
    return portal_pb2.ListPdReply(
        reply_info=reply_info,
        pd_msg_list=pd_msg_list,
    )


def get_pd(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    dn_name = request.dn_name
    pd_name = request.pd_name

    dn = session \
        .query(DiskNode) \
        .filter_by(dn_name=dn_name) \
        .with_lockmode("update") \
        .one()

    pd = session \
        .query(PhysicalDisk) \
        .filter_by(dn_id=dn.dn_id) \
        .filter_by(pd_name=pd_name) \
        .with_lockmode("update") \
        .one()

    pd_msg = portal_pb2.PdMsg(
        pd_id=pd.pd_id,
        dn_name=dn.dn_name,
        pd_name=pd.pd_name,
        total_size=pd.total_clusters*Constant.CLUSTER_SIZE,
        free_size=pd.free_clusters*Constant.CLUSTER_SIZE,
        pd_conf=pd.pd_conf,
        online=pd.online,
        error=pd.error,
        error_msg=pd.error_msg,
    )

    vd_msg_list = []
    for vd in pd.vds:
        vd_msg = portal_pb2.VdMsg(
            vd_id=vd.vd_id,
            da_name=vd.grp.da.da_name,
            grp_idx=vd.grp.grp_idx,
            dn_name=dn.dn_name,
            pd_name=pd.pd_name,
            vd_size=vd.vd_clusters*Constant.CLUSTER_SIZE,
        )
        vd_msg_list.append(vd_msg)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.GetPdReply(
        reply_info=reply_info,
        pd_msg=pd_msg,
        vd_msg_list=vd_msg_list,
    )
