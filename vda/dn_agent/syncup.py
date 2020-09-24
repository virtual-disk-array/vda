import os
import json

from vda.grpc import dn_agent_pb2

from vda.common.constant import Constant
from vda.common.spdk_client import SpdkError
from vda.common.name_fmt import NameFmt
from vda.common.operation_client import OperationClient
from vda.common.dn_agent_schema import DnCfgSchema


g_dn_cfg = None


def do_dn_syncup(dn_cfg, spdk_client, local_store, dn_listener_conf):
    global g_dn_cfg
    if g_dn_cfg is None:
        g_dn_cfg = dn_cfg
    if g_dn_cfg.dn_id != dn_cfg.dn_id:
        reply_info = dn_agent_pb2.DnAgentReplyInfo(
            reply_code=Constant.dn_id_mismatch.reply_code,
            reply_msg=Constant.dn_id_mismatch.reply_msg,
        )
        return dn_agent_pb2.SyncupDnReply(reply_info=reply_info)
    if g_dn_cfg.version > dn_cfg.version:
        reply_info = dn_agent_pb2.DnAgentReplyInfo(
            reply_code=Constant.dn_old_version.reply_code,
            reply_msg=Constant.dn_old_version.reply_msg,
        )
        return dn_agent_pb2.SyncupDnReply(reply_info=reply_info)
    g_dn_cfg = dn_cfg
    if local_store:
        schema = DnCfgSchema()
        with open(local_store, "w") as f:
            f.write(schema.dumps(dn_cfg))

    oc = OperationClient(spdk_client)

    pd_bdev_set = set()
    pd_lvs_set = set()
    backend_nqn_set = set()
    backend_lvol_set = set()

    oc.load_nvmfs()

    pd_info_list = []
    for pd_cfg in dn_cfg.pd_cfg_list:
        pd_conf = json.loads(pd_cfg.pd_conf)
        pd_bdev_name = NameFmt.pd_bdev_name(pd_cfg.pd_id)
        if pd_conf["type"] == "nvme":
            pd_bdev_realname = f"{pd_bdev_name}n1"
        else:
            pd_bdev_realname = pd_bdev_name
        pd_bdev_set.add(pd_bdev_realname)
        pd_lvs_name = NameFmt.pd_lvs_name(pd_cfg.pd_id)
        pd_lvs_set.add(pd_lvs_name)
        try:
            oc.create_pd_bdev(pd_bdev_name, pd_conf)
            oc.examine_bdev(pd_bdev_realname)
            oc.create_pd_lvs(pd_lvs_name, pd_bdev_realname)
        except SpdkError as e:
            pd_error = True
            pd_error_msg = e.message
        else:
            pd_error = False
            pd_error_msg = None

        vd_info_list = []
        for vd_cfg in pd_cfg.vd_cfg_list:
            backend_lvol_name = NameFmt.backend_lvol_name(vd_cfg.vd_id)
            backend_lvol_fullname = NameFmt.backend_lvol_fullname(
                pd_id=pd_cfg.pd_id,
                vd_id=vd_cfg.vd_id,
            )
            backend_lvol_set.add(backend_lvol_fullname)
            backend_nqn_name = NameFmt.backend_nqn_name(vd_cfg.vd_id)
            backend_nqn_set.add(backend_nqn_name)
            frontend_nqn_name = NameFmt.frontend_nqn_name(vd_cfg.cntlr_id)
            if pd_error:
                vd_error = True
                vd_error_msg = "stop"
            else:
                try:
                    oc.create_backend_lvol(
                        pd_lvs_name=pd_lvs_name,
                        backend_lvol_name=backend_lvol_name,
                        vd_clusters=vd_cfg.vd_clusters,
                    )
                    oc.create_backend_nvmf(
                        backend_nqn_name=backend_nqn_name,
                        backend_lvol_fullname=backend_lvol_fullname,
                        frontend_nqn_name=frontend_nqn_name,
                        dn_listener_conf=dn_listener_conf,
                    )
                except SpdkError as e:
                    vd_error = True
                    vd_error_msg = e.message
                else:
                    vd_error = False
                    vd_error_msg = None
            vd_info = dn_agent_pb2.VdBackendInfo(
                vd_id=vd_cfg.vd_id,
                error=vd_error,
                error_msg=vd_error_msg,
            )
            vd_info_list.append(vd_info)
        if pd_error:
            total_clusters = 0
            free_clusters = 0
        else:
            total_clusters, free_clusters = oc.get_pd_lvs_size(pd_lvs_name)
        pd_info = dn_agent_pb2.PdInfo(
            pd_id=pd_cfg.pd_id,
            total_clusters=total_clusters,
            free_clusters=free_clusters,
            error=pd_error,
            error_msg=pd_error_msg,
            vd_info_list=vd_info_list,
        )
        pd_info_list.append(pd_info)

    oc.load_bdevs()

    backend_nqn_prefix = NameFmt.backend_nqn_prefix()
    backend_nqn_list = oc.get_backend_nqn_list(backend_nqn_prefix)
    for backend_nqn_name in backend_nqn_list:
        if backend_nqn_name not in backend_nqn_set:
            oc.delete_backend_nvmf(backend_nqn_name)

    backend_lvol_prefix = NameFmt.backend_lvol_prefix()
    backend_lvol_list = oc.get_backend_lvol_list(backend_lvol_prefix)
    for backend_lvol_fullname in backend_lvol_list:
        if backend_lvol_fullname not in backend_lvol_set:
            oc.delete_backend_lvol(backend_lvol_fullname)

    pd_lvs_prefix = NameFmt.pd_lvs_prefix()
    pd_lvs_list = oc.get_pd_lvs_list(pd_lvs_prefix)
    for pd_lvs_name in pd_lvs_list:
        if pd_lvs_name not in pd_lvs_set:
            oc.delete_pd_lvs(pd_lvs_name)

    pd_bdev_prefix = NameFmt.pd_bdev_prefix()
    pd_bdev_list = oc.get_pd_bdev_list(pd_bdev_prefix)
    for pd_bdev_name in pd_bdev_list:
        if pd_bdev_name not in pd_bdev_set:
            oc.delete_pd_bdev(pd_bdev_name)

    dn_info = dn_agent_pb2.DnInfo(
        dn_id=dn_cfg.dn_id,
        error=False,
        error_msg=None,
        pd_info_list=pd_info_list,
    )
    reply_info = dn_agent_pb2.DnAgentReplyInfo(
        reply_code=Constant.dn_success.reply_code,
        reply_msg=Constant.dn_success.reply_msg,
    )
    reply = dn_agent_pb2.SyncupDnReply(
        reply_info=reply_info,
        dn_info=dn_info,
    )
    return reply


def syncup_init(client, local_store, listener_conf):
    global g_dn_cfg
    if local_store and os.path.isfile(local_store):
        with open(local_store) as f:
            data = f.read()
        schema = DnCfgSchema()
        g_dn_cfg = schema.loads(data)
        do_dn_syncup(g_dn_cfg, client, local_store, listener_conf)


def syncup_dn(request, context):
    return do_dn_syncup(
        request.dn_cfg,
        context.client,
        context.local_store,
        context.listener_conf,
    )
