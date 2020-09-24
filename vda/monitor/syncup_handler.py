import logging

from vda.common.modules import DiskNode, ControllerNode, DiskArray
from vda.common.syncup import syncup_dn, syncup_cn, SyncupCtx
from vda.monitor.basic_handler import BasicHandler


logger = logging.getLogger(__name__)


class DaSyncupHandler(BasicHandler):

    def get_backlog_list(self, session, begin, end):
        das = session \
            .query(DiskArray) \
            .filter(DiskArray.hash_code >= begin) \
            .filter(DiskArray.hash_code < end) \
            .filter(DiskArray.error == True) \
            .with_entities(DiskArray.da_id) \
            .all()  # noqa: E712
        da_id_list = []
        for da in das:
            da_id_list.append(da.da_id)
        return da_id_list

    def process_backlog(self, session, da_id):
        da = session \
            .query(DiskArray) \
            .filter_by(da_id=da_id) \
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
        syncup_ctx = SyncupCtx(session=session, req_id=None)
        for dn_id in dn_id_list:
            syncup_dn(dn_id, syncup_ctx)
        syncup_cn(primary_cn_id, syncup_ctx)
        for cn_id in cn_id_list:
            syncup_cn(cn_id, syncup_ctx)


class DnSyncupHandler(BasicHandler):

    def get_backlog_list(self, session, begin, end):
        dns = session \
            .query(DiskNode) \
            .filter(DiskNode.hash_code >= begin) \
            .filter(DiskNode.hash_code < end) \
            .filter(DiskNode.error == False) \
            .filter(DiskNode.online == True) \
            .with_entities(DiskNode.dn_id) \
            .all()  # noqa: E712
        dn_id_list = []
        for dn in dns:
            dn_id_list.append(dn.dn_id)
        return dn_id_list

    def process_backlog(self, session, dn_id):
        syncup_ctx = SyncupCtx(session=session, req_id=None)
        syncup_dn(dn_id, syncup_ctx)


class CnSyncupHandler(BasicHandler):

    def get_backlog_list(self, session, begin, end):
        cns = session \
            .query(ControllerNode) \
            .filter(ControllerNode.hash_code >= begin) \
            .filter(ControllerNode.hash_code < end) \
            .filter(ControllerNode.error == False) \
            .filter(ControllerNode.online == True) \
            .with_entities(ControllerNode.cn_id) \
            .all()  # noqa: E712
        cn_id_list = []
        for cn in cns:
            cn_id_list.append(cn.cn_id)
        return cn_id_list

    def process_backlog(self, session, cn_id):
        syncup_ctx = SyncupCtx(session=session, req_id=None)
        syncup_cn(cn_id, syncup_ctx)
