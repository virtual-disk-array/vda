import uuid
import json
import math

from vda.grpc import portal_pb2
from vda.common.modules import (
    DiskArray,
    Group,
    PhysicalDisk,
    VirtualDisk,
    DiskNode,
    Controller,
    ControllerNode,
)
from vda.common.constant import Constant
from vda.common.syncup import syncup_dn, syncup_cn, SyncupCtx


def get_pd_dn_candidates(session, required_clusters, locations):
    candidates = session \
        .query(PhysicalDisk) \
        .join(DiskNode) \
        .filter(DiskNode.error == False) \
        .filter(DiskNode.online == True) \
        .filter(DiskNode.location.notin_(locations)) \
        .filter(PhysicalDisk.error == False) \
        .filter(PhysicalDisk.online == True) \
        .filter(PhysicalDisk.free_clusters >= required_clusters) \
        .order_by(PhysicalDisk.free_clusters.desc()) \
        .with_entities(
            DiskNode.location,
            DiskNode.dn_id,
            PhysicalDisk.pd_id,
        ) \
        .all()  # noqa: E712
    return candidates


def get_cn_candidates(session, locations):
    candidates = session \
        .query(ControllerNode) \
        .filter(ControllerNode.error == False) \
        .filter(ControllerNode.online == True) \
        .filter(ControllerNode.location.notin_(locations)) \
        .order_by(ControllerNode.version) \
        .limit(Constant.ALLOCATE_BATCH_SIZE) \
        .with_entities(ControllerNode.cn_id, ControllerNode.location) \
        .all()  # noqa: E712
    return candidates


def create_da(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name
    cntlr_cnt = request.cntlr_cnt
    da_size = request.da_size
    physical_size = request.physical_size
    hash_code = request.hash_code
    da_conf_str = request.da_conf
    da_conf = json.loads(da_conf_str)
    stripe_count = da_conf["stripe_count"]
    vd_count = stripe_count
    da_id = uuid.uuid4().hex
    grp_id = uuid.uuid4().hex
    grp_idx = 0
    grp_size = physical_size

    da = DiskArray(
        da_id=da_id,
        da_name=da_name,
        cntlr_cnt=cntlr_cnt,
        da_size=da_size,
        da_conf=da_conf_str,
        hash_code=hash_code,
        da_details="{}",
        error=True,
    )
    session.add(da)

    group = Group(
        grp_id=grp_id,
        grp_idx=grp_idx,
        grp_size=grp_size,
        da_id=da_id,
        error=True,
        error_msg=Constant.UNINIT_MSG,
    )
    session.add(group)
    vd_size = math.ceil(grp_size/vd_count)
    required_clusters = math.ceil(vd_size/Constant.CLUSTER_SIZE)
    locations = set()
    pd_dn_list = []
    while True:
        prev_count = len(pd_dn_list)
        candidates = get_pd_dn_candidates(
            session=session,
            required_clusters=required_clusters,
            locations=list(locations),
        )
        for c in candidates:
            if c.location:
                if c.location in locations:
                    continue
                else:
                    locations.add(c.location)
            pd = session \
                .query(PhysicalDisk) \
                .filter_by(pd_id=c.pd_id) \
                .with_lockmode("update") \
                .one()
            if pd.free_clusters < required_clusters or \
               pd.error or \
               pd.online is False:
                continue
            dn = session \
                .query(DiskNode) \
                .filter_by(dn_id=c.dn_id) \
                .with_lockmode("update") \
                .one()
            if dn.error or not dn.online:
                continue
            pd_dn_list.append((pd, dn))
            if len(pd_dn_list) >= vd_count:
                break
        if len(pd_dn_list) >= vd_count:
            break
        if len(pd_dn_list) == prev_count:
            raise Exception("no enough dn")
        assert(len(pd_dn_list) > prev_count)
    assert(len(pd_dn_list) == vd_count)
    dn_id_list = []
    for i in range(vd_count):
        vd_id = uuid.uuid4().hex
        pd, dn = pd_dn_list[i]
        vd = VirtualDisk(
            vd_id=vd_id,
            vd_idx=i,
            vd_clusters=required_clusters,
            grp_id=grp_id,
            pd_id=pd.pd_id,
            dn_error=True,
            dn_error_msg=Constant.UNINIT_MSG,
            cn_error=True,
            cn_error_msg=Constant.UNINIT_MSG,
        )
        dn.version += 1
        pd.free_clusters -= required_clusters
        assert(pd.free_clusters >= 0)
        session.add(vd)
        session.add(pd)
        session.add(dn)
        dn_id_list.append(dn.dn_id)

    cn_count = cntlr_cnt
    locations = set()
    cn_list = []
    while True:
        prev_count = len(cn_list)
        candidates = get_cn_candidates(session, list(locations))
        for c in candidates:
            if c.location:
                if c.location in locations:
                    continue
                else:
                    locations.add(c.location)
            cn = session \
                .query(ControllerNode) \
                .filter_by(cn_id=c.cn_id) \
                .with_lockmode("update") \
                .one()
            if cn.error or not cn.online:
                continue
            cn_list.append(cn)
            if len(cn_list) >= cn_count:
                break
        if len(cn_list) >= cn_count:
            break
        if len(cn_list) == prev_count:
            raise Exception("no enough cn")
        assert(len(cn_list) > prev_count)
    assert(len(cn_list) == cn_count)
    cn_id_list = []
    primary_cn_id = None
    for i in range(cn_count):
        cntlr_id = uuid.uuid4().hex
        cn = cn_list[i]
        if i == 0:
            primary = True
        else:
            primary = False
        cntlr = Controller(
            cntlr_id=cntlr_id,
            cntlr_idx=i,
            primary=primary,
            da_id=da_id,
            cn_id=cn.cn_id,
            error=True,
            error_msg=Constant.UNINIT_MSG,
        )
        session.add(cntlr)
        cn.version += 1
        session.add(cn)
        if primary:
            primary_cn_id = cn.cn_id
        else:
            cn_id_list.append(cn.cn_id)

    session.commit()

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
    return portal_pb2.CreateDaReply(reply_info=reply_info)


def delete_da(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da = session \
        .query(DiskArray) \
        .filter_by(da_name=request.da_name) \
        .with_lockmode("update") \
        .one()
    dn_id_list = []
    for grp in da.grps:
        for vd in grp.vds:
            pd = session \
                .query(PhysicalDisk) \
                .filter_by(pd_id=vd.pd_id) \
                .with_lockmode("update") \
                .one()
            dn = session \
                .query(DiskNode) \
                .filter_by(dn_id=pd.dn_id) \
                .with_lockmode("update") \
                .one()
            pd.free_clusters += vd.vd_clusters
            dn.version += 1
            dn_id_list.append(dn.dn_id)
            session.delete(vd)
            session.add(pd)
            session.add(dn)
        session.delete(grp)
    cn_id_list = []
    for cntlr in da.cntlrs:
        cn = session \
            .query(ControllerNode) \
            .filter_by(cn_id=cntlr.cn_id) \
            .with_lockmode("update") \
            .one()
        cn.version += 1
        session.delete(cntlr)
        session.add(cn)
        cn_id_list.append(cn.cn_id)
    session.delete(da)
    session.commit()
    syncup_ctx = SyncupCtx(session=session, req_id=req_id)
    for dn_id in dn_id_list:
        syncup_dn(dn_id, syncup_ctx)
    for cn_id in cn_id_list:
        syncup_cn(cn_id, syncup_ctx)
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.DeleteDaReply(reply_info=reply_info)


def modify_da(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da_name = request.da_name

    da = session \
        .query(DiskArray) \
        .filter_by(da_name=da_name) \
        .with_lockmode("update") \
        .one()

    attr = request.WhichOneof("attr")
    if attr == "new_hash_code":
        da.hash_code = request.new_hash_code
    else:
        assert(False)
    session.add(da)
    session.commit()

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.ModifyDaReply(reply_info=reply_info)


def list_da(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    query = session.query(DiskArray)
    if request.offset:
        query = query.offset(request.offset)
    if request.limit:
        query = query.limit(request.limit)
    if request.set_hash_code:
        query = query.filter_by(hash_code=request.hash_code)
    das = query.all()
    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    da_msg_list = []
    for da in das:
        da_msg = portal_pb2.DaMsg(
            da_id=da.da_id,
            da_name=da.da_name,
            cntlr_cnt=da.cntlr_cnt,
            da_size=da.da_size,
            da_conf=da.da_conf,
            da_details=da.da_details,
            hash_code=da.hash_code,
            error=da.error,
        )
        da_msg_list.append(da_msg)

    return portal_pb2.ListDaReply(
        reply_info=reply_info, da_msg_list=da_msg_list)


def get_da(request, portal_ctx):
    session = portal_ctx.session
    req_id = portal_ctx.req_id
    da = session \
        .query(DiskArray) \
        .filter_by(da_name=request.da_name) \
        .one()
    da_msg = portal_pb2.DaMsg(
        da_id=da.da_id,
        da_name=da.da_name,
        cntlr_cnt=da.cntlr_cnt,
        da_size=da.da_size,
        da_conf=da.da_conf,
        da_details=da.da_details,
        hash_code=da.hash_code,
        error=da.error,
    )

    grp_msg_list = []
    for grp in da.grps:
        vd_msg_list = []
        for vd in grp.vds:
            vd_msg = portal_pb2.VdMsg(
                vd_id=vd.vd_id,
                da_name=da.da_name,
                grp_idx=grp.grp_idx,
                vd_idx=vd.vd_idx,
                dn_name=vd.pd.dn.dn_name,
                pd_name=vd.pd.pd_name,
                vd_size=vd.vd_clusters*Constant.CLUSTER_SIZE,
            )
            vd_msg_list.append(vd_msg)
        grp_msg = portal_pb2.GrpMsg(
            grp_id=grp.grp_id,
            da_name=da.da_name,
            grp_idx=grp.grp_idx,
            grp_size=grp.grp_size,
            vd_msg_list=vd_msg_list,
        )
        grp_msg_list.append(grp_msg)

    cntlr_msg_list = []
    for cntlr in da.cntlrs:
        cntlr_msg = portal_pb2.CntlrMsg(
            cntlr_id=cntlr.cntlr_id,
            da_name=da.da_name,
            cntlr_idx=cntlr.cntlr_idx,
            cn_name=cntlr.cn.cn_name,
            primary=cntlr.primary,
            error=cntlr.error,
            error_msg=cntlr.error_msg,
        )
        cntlr_msg_list.append(cntlr_msg)

    reply_info = portal_pb2.PortalReplyInfo(
        req_id=req_id,
        reply_code=0,
        reply_msg="success",
    )
    return portal_pb2.GetDaReply(
        reply_info=reply_info,
        da_msg=da_msg,
        grp_msg_list=grp_msg_list,
        cntlr_msg_list=cntlr_msg_list,
    )
