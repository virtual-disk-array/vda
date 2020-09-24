import os
from collections import namedtuple
import json

from vda.grpc import cn_agent_pb2

from vda.common.constant import Constant
from vda.common.spdk_client import SpdkError
from vda.common.name_fmt import NameFmt
from vda.common.operation_client import OperationClient
from vda.common.cn_agent_schema import CnCfgSchema


g_cn_cfg = None

GrpCtx = namedtuple("GrpCtx", [
    "grp_bdev_set",
    "raid0_bdev_set",
])


class GrpManager():

    def __init__(self, oc):
        self.oc = oc
        self.grp_bdev_set = set()
        self.raid0_bdev_set = set()

    def create_grp(
            self, grp_bdev_name, da_conf_str, frontend_bdev_list, grp_cfg):
        da_conf = json.loads(da_conf_str)
        sorted_list = sorted(frontend_bdev_list, key=lambda x: x[0])
        bdev_name_list = []
        for _, bdev_name, vd_error in sorted_list:
            if vd_error:
                raise SpdkError(1, "vd_error")
            bdev_name_list.append(bdev_name)
        stripe_size_kb = da_conf["stripe_size_kb"]
        raid0_bdev_name = NameFmt.raid0_bdev_name(grp_cfg.grp_id)
        self.raid0_bdev_set.add(raid0_bdev_name)
        self.oc.create_raid0_bdev(
            raid0_bdev_name, bdev_name_list, stripe_size_kb)
        self.grp_bdev_set.add(grp_bdev_name)
        self.oc.create_grp_bdev(grp_bdev_name, raid0_bdev_name)

    def cleanup_grps(self):
        grp_bdev_prefix = NameFmt.grp_bdev_prefix()
        grp_bdev_list = self.oc.get_grp_bdev_list(grp_bdev_prefix)
        for grp_bdev_name in grp_bdev_list:
            if grp_bdev_name not in self.grp_bdev_set:
                self.oc.delete_grp_bdev(grp_bdev_name)

        raid0_bdev_prefix = NameFmt.raid0_bdev_prefix()
        raid0_bdev_list = self.oc.get_raid0_bdev_list(raid0_bdev_prefix)
        for raid0_bdev_name in raid0_bdev_list:
            if raid0_bdev_name not in self.raid0_bdev_set:
                self.oc.delete_raid0_bdev(raid0_bdev_name)


class SyncupManager():

    def __init__(self, spdk_client, cn_listener_conf):
        self.oc = OperationClient(spdk_client)
        self.cn_listener_conf = cn_listener_conf
        self.frontend_nvme_set = set()
        self.agg_bdev_set = set()
        self.da_lvs_set = set()
        self.exp_nqn_set = set()
        self.secondary_nvme_set = set()
        self.gm = GrpManager(self.oc)
        self.da_cntlr_info_list = []

        self.oc.load_nvmfs()

    def process_da(self, da_cntlr_cfg):
        this_cntlr_cfg = None
        primary_cntlr_cfg = None
        secondary_cntlr_cfg_list = []
        for cntlr_cfg in da_cntlr_cfg.cntlr_cfg_list:
            if cntlr_cfg.cntlr_id == da_cntlr_cfg.cntlr_id:
                this_cntlr_cfg = cntlr_cfg
            if cntlr_cfg.primary:
                primary_cntlr_cfg = cntlr_cfg
            else:
                secondary_cntlr_cfg_list.append(cntlr_cfg)
        if this_cntlr_cfg is None:
            raise Exception("Can not find this_cntlr_cfg")
        if primary_cntlr_cfg is None:
            raise Exception("Can not find primary_cntlr_cfg")
        if this_cntlr_cfg.primary:
            self._create_primary(
                da_cntlr_cfg, this_cntlr_cfg, secondary_cntlr_cfg_list)
        else:
            self._create_secondary(
                da_cntlr_cfg, this_cntlr_cfg, primary_cntlr_cfg)

    def _create_primary(
            self, da_cntlr_cfg, this_cntlr_cfg, secondary_cntlr_cfg_list):
        grp_info_list = []
        grp_bdev_list = []
        stop = False
        for grp_cfg in da_cntlr_cfg.grp_cfg_list:
            vd_info_list = []
            frontend_bdev_list = []
            for vd_cfg in grp_cfg.vd_cfg_list:
                backend_nqn_name = NameFmt.backend_nqn_name(vd_cfg.vd_id)
                frontend_nvme_name = NameFmt.frontend_nvme_name(
                    vd_cfg.vd_id)
                frontend_bdev_name = f"{frontend_nvme_name}n1"
                frontend_nqn_name = NameFmt.frontend_nqn_name(
                    this_cntlr_cfg.cntlr_id)
                try:
                    self.oc.create_frontend_nvme(
                        frontend_nvme_name=frontend_nvme_name,
                        backend_nqn_name=backend_nqn_name,
                        frontend_nqn_name=frontend_nqn_name,
                        dn_listener_conf_str=vd_cfg.dn_listener_conf,
                    )
                except SpdkError as e:
                    vd_error = True
                    vd_error_msg = e.message
                else:
                    vd_error = False
                    vd_error_msg = None
                vd_info = cn_agent_pb2.VdFrontendInfo(
                    vd_id=vd_cfg.vd_id,
                    error=vd_error,
                    error_msg=vd_error_msg,
                )
                vd_info_list.append(vd_info)
                self.frontend_nvme_set.add(frontend_nvme_name)
                frontend_bdev_list.append(
                    (vd_cfg.vd_idx, frontend_bdev_name, vd_error))
            grp_bdev_name = NameFmt.grp_bdev_name(grp_cfg.grp_id)
            try:
                self.gm.create_grp(
                    grp_bdev_name=grp_bdev_name,
                    da_conf_str=da_cntlr_cfg.da_conf,
                    frontend_bdev_list=frontend_bdev_list,
                    grp_cfg=grp_cfg,
                )
            except SpdkError as e:
                grp_error = True
                grp_error_msg = e.message
            else:
                grp_error = False
                grp_error_msg = None
            grp_info = cn_agent_pb2.GrpInfo(
                grp_id=grp_cfg.grp_id,
                error=grp_error,
                error_msg=grp_error_msg,
                vd_info_list=vd_info_list,
            )
            grp_info_list.append(grp_info)
            grp_bdev_list.append((grp_cfg.grp_idx, grp_bdev_name))
            if grp_error:
                stop = True
        da_details = None
        agg_bdev_name = NameFmt.agg_bdev_name(da_cntlr_cfg.da_id)
        da_lvs_name = NameFmt.da_lvs_name(da_cntlr_cfg.da_id)
        if stop:
            da_cntlr_error = True
            da_cntlr_error_msg = "stop"
        else:
            try:
                self.oc.create_agg_bdev(agg_bdev_name, grp_bdev_list)
                self.oc.examine_bdev(agg_bdev_name)
                self.oc.create_da_lvs(da_lvs_name, agg_bdev_name)
                main_snap_name = NameFmt.main_snap_name(da_cntlr_cfg.da_id)
                self.oc.create_main_snap(
                    da_lvs_name, main_snap_name, da_cntlr_cfg.da_size)
                da_details = self.oc.get_da_details(da_lvs_name)
            except SpdkError as e:
                da_cntlr_error = True
                da_cntlr_error_msg = e.message
            else:
                da_cntlr_error = False
                da_cntlr_error_msg = None
        self.agg_bdev_set.add(agg_bdev_name)
        self.da_lvs_set.add(da_lvs_name)
        exp_info_list = []
        secondary_nqn_list = []
        for cntlr_cfg in secondary_cntlr_cfg_list:
            secondary_nqn_name = NameFmt.secondary_nqn_name(
                cntlr_cfg.cntlr_id)
            secondary_nqn_list.append(secondary_nqn_name)
        for exp_cfg in da_cntlr_cfg.exp_cfg_list:
            exp_nqn_name = NameFmt.exp_nqn_name(
                da_cntlr_cfg.da_name,
                exp_cfg.exp_name,
            )
            snap_full_name = NameFmt.snap_full_name(
                da_id=da_cntlr_cfg.da_id,
                snap_name=exp_cfg.snap_name,
            )
            if stop:
                exp_error = True
                exp_error_msg = "stop"
            else:
                try:
                    self.oc.create_exp_primary_nvmf(
                        exp_nqn_name=exp_nqn_name,
                        snap_full_name=snap_full_name,
                        initiator_nqn=exp_cfg.initiator_nqn,
                        secondary_nqn_list=secondary_nqn_list,
                        cn_listener_conf=self.cn_listener_conf,
                    )
                except SpdkError as e:
                    exp_error = True
                    exp_error_msg = e.message
                else:
                    exp_error = False
                    exp_error_msg = None
            exp_info = cn_agent_pb2.ExpInfo(
                exp_id=exp_cfg.exp_id,
                error=exp_error,
                error_msg=exp_error_msg,
            )
            exp_info_list.append(exp_info)
            self.exp_nqn_set.add(exp_nqn_name)
        da_cntlr_info = cn_agent_pb2.DaCntlrInfo(
            da_id=da_cntlr_cfg.da_id,
            cntlr_id=da_cntlr_cfg.cntlr_id,
            da_details=da_details,
            error=da_cntlr_error,
            error_msg=da_cntlr_error_msg,
            grp_info_list=grp_info_list,
            exp_info_list=exp_info_list,
        )
        self.da_cntlr_info_list.append(da_cntlr_info)

    def _create_secondary(
            self, da_cntlr_cfg, this_cntlr_cfg, primary_cntlr_cfg):
        exp_info_list = []
        for exp_cfg in da_cntlr_cfg.exp_cfg_list:
            secondary_nvme_name = NameFmt.secondary_nvme_name(
                primary_cntlr_cfg.cntlr_id,
            )
            secondary_bdev_name = f"{secondary_nvme_name}n1"
            exp_nqn_name = NameFmt.exp_nqn_name(
                da_cntlr_cfg.da_name,
                exp_cfg.exp_name,
            )
            secondary_nqn_name = NameFmt.secondary_nqn_name(
                this_cntlr_cfg.cntlr_id)
            try:
                self.oc.create_secondary_nvme(
                    secondary_nvme_name,
                    exp_nqn_name,
                    secondary_nqn_name,
                    primary_cntlr_cfg.cn_listener_conf,
                )
                self.oc.create_exp_secondary_nvmf(
                    exp_nqn_name=exp_nqn_name,
                    secondary_bdev_name=secondary_bdev_name,
                    initiator_nqn=exp_cfg.initiator_nqn,
                    cn_listener_conf=self.cn_listener_conf,
                )
            except SpdkError as e:
                exp_error = True
                exp_error_msg = e.message
            else:
                exp_error = False
                exp_error_msg = None
            exp_info = cn_agent_pb2.ExpInfo(
                exp_id=exp_cfg.exp_id,
                error=exp_error,
                error_msg=exp_error_msg,
            )
            exp_info_list.append(exp_info)
            self.secondary_nvme_set.add(secondary_nvme_name)
            self.exp_nqn_set.add(exp_nqn_name)
        da_cntlr_info = cn_agent_pb2.DaCntlrInfo(
            da_id=da_cntlr_cfg.da_id,
            cntlr_id=da_cntlr_cfg.cntlr_id,
            da_details=None,
            error=False,
            error_msg=None,
            grp_info_list=None,
            exp_info_list=exp_info_list,
        )
        self.da_cntlr_info_list.append(da_cntlr_info)

    def remove_unused_resources(self):
        self.oc.load_bdevs()
        exp_nqn_prefix = NameFmt.exp_nqn_prefix()
        exp_nqn_list = self.oc.get_exp_nqn_list(exp_nqn_prefix)
        for exp_nqn_name in exp_nqn_list:
            if exp_nqn_name not in self.exp_nqn_set:
                self.oc.delete_exp_nvmf(exp_nqn_name)

        secondary_nvme_prefix = NameFmt.secondary_nvme_prefix()
        secondary_nvme_list = self.oc.get_secondary_nvme_list(
            secondary_nvme_prefix)
        for secondary_nvme_name in secondary_nvme_list:
            if secondary_nvme_name not in self.secondary_nvme_set:
                self.oc.delete_secondary_nvme(secondary_nvme_name)

        da_lvs_prefix = NameFmt.da_lvs_prefix()
        da_lvs_list = self.oc.get_da_lvs_list(da_lvs_prefix)
        for da_lvs_name in da_lvs_list:
            if da_lvs_name not in self.da_lvs_set:
                self.oc.delete_da_lvs(da_lvs_name)

        agg_bdev_prefix = NameFmt.agg_bdev_prefix()
        agg_bdev_list = self.oc.get_agg_bdev_list(agg_bdev_prefix)
        for agg_bdev_name in agg_bdev_list:
            if agg_bdev_name not in self.agg_bdev_set:
                self.oc.delete_agg_bdev(agg_bdev_name)

        self.gm.cleanup_grps()

        frontend_nvme_prefix = NameFmt.frontend_nvme_prefix()
        frontend_nvme_list = self.oc.get_frontend_nvme_list(
            frontend_nvme_prefix)
        for frontend_nvme_name in frontend_nvme_list:
            if frontend_nvme_name not in self.frontend_nvme_set:
                self.oc.delete_frontend_nvme(frontend_nvme_name)

    def get_da_cntlr_info_list(self):
        return self.da_cntlr_info_list


def do_cn_syncup(cn_cfg, spdk_client, local_store, cn_listener_conf):
    global g_cn_cfg
    if g_cn_cfg is None:
        g_cn_cfg = cn_cfg
    if g_cn_cfg.cn_id != cn_cfg.cn_id:
        reply_info = cn_agent_pb2.CnAgentReplyInfo(
            reply_code=Constant.cn_id_mismatch.reply_code,
            reply_msg=Constant.cn_id_mismatch.reply_msg,
        )
        return cn_agent_pb2.SyncupCnReply(reply_info=reply_info)
    if g_cn_cfg.version > cn_cfg.version:
        reply_info = cn_agent_pb2.CnAgentReplyInfo(
            reply_code=Constant.cn_old_version.reply_code,
            reply_msg=Constant.cn_old_version.reply_msg,
        )
        return cn_agent_pb2.SyncupCnReply(reply_info=reply_info)
    g_cn_cfg = cn_cfg
    if local_store:
        schema = CnCfgSchema()
        with open(local_store, "w") as f:
            f.wirte(schema.dumps(cn_cfg))

    sm = SyncupManager(spdk_client, cn_listener_conf)

    for da_cntlr_cfg in cn_cfg.da_cntlr_cfg_list:
        sm.process_da(da_cntlr_cfg)

    sm.remove_unused_resources()

    cn_info = cn_agent_pb2.CnInfo(
        cn_id=cn_cfg.cn_id,
        error=False,
        error_msg=None,
        da_cntlr_info_list=sm.get_da_cntlr_info_list(),
    )
    reply_info = cn_agent_pb2.CnAgentReplyInfo(
        reply_code=Constant.cn_success.reply_code,
        reply_msg=Constant.cn_success.reply_msg,
    )
    reply = cn_agent_pb2.SyncupCnReply(
        reply_info=reply_info,
        cn_info=cn_info,
    )
    return reply


def syncup_init(client, local_store, listener_conf):
    global g_cn_cfg
    if local_store and os.path.isfile(local_store):
        with open(local_store) as f:
            data = f.read()
        schema = CnCfgSchema()
        g_cn_cfg = schema.loads(data)
        do_cn_syncup(g_cn_cfg, client, local_store, listener_conf)


def syncup_cn(request, context):
    return do_cn_syncup(
        request.cn_cfg,
        context.client,
        context.local_store,
        context.listener_conf,
    )
