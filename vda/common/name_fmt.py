VDA_PREFIX = "vda"

BACKEND_NQN_PREFIX = "nqn.2016-06.io.spdk:vda-be"
FRONTEND_NQN_PREFIX = "nqn.2016-06.io.spdk:vda-fe"
EXP_NQN_PREFIX = "nqn.2016-06.io.spdk:vda-exp"
SECONDARY_NQN_PREFIX = "nqn.2016-06.io.spdk:vda-se"

PD_BDEV_TYPE = "000"
PD_LVS_TYPE = "001"
BACKEND_BDEV_TYPE = "002"
FRONTEND_NVME_TYPE = "003"
AGG_BDEV_TYPE = "004"
DA_LVS_TYPE = "005"
SECONDARY_NVME_TYPE = "006"
GRP_BDEV_TYPE = "007"

RAID0_BDEV_TYPE = "100"


class NameFmt:

    @classmethod
    def pd_bdev_prefix(cls):
        return f"{VDA_PREFIX}-{PD_BDEV_TYPE}"

    @classmethod
    def pd_lvs_prefix(cls):
        return f"{VDA_PREFIX}-{PD_LVS_TYPE}"

    @classmethod
    def backend_lvol_prefix(cls):
        return f"{VDA_PREFIX}-{PD_LVS_TYPE}"

    @classmethod
    def backend_nqn_prefix(cls):
        return f"{BACKEND_NQN_PREFIX}"

    @classmethod
    def frontend_nqn_prefix(cls):
        return f"{FRONTEND_NQN_PREFIX}"

    @classmethod
    def frontend_nvme_prefix(cls):
        return f"{VDA_PREFIX}-{FRONTEND_NVME_TYPE}"

    @classmethod
    def agg_bdev_prefix(cls):
        return f"{VDA_PREFIX}-{AGG_BDEV_TYPE}"

    @classmethod
    def da_lvs_prefix(cls):
        return f"{VDA_PREFIX}-{DA_LVS_TYPE}"

    @classmethod
    def secondary_nvme_prefix(cls):
        return f"{VDA_PREFIX}-{SECONDARY_NVME_TYPE}"

    @classmethod
    def grp_bdev_prefix(cls):
        return f"{VDA_PREFIX}-{GRP_BDEV_TYPE}"

    @classmethod
    def raid0_bdev_prefix(cls):
        return f"{VDA_PREFIX}-{RAID0_BDEV_TYPE}"

    @classmethod
    def exp_nqn_prefix(cls):
        return f"{EXP_NQN_PREFIX}"

    @classmethod
    def secondary_nqn_prefix(cls):
        return f"{SECONDARY_NQN_PREFIX}"

    @classmethod
    def pd_bdev_name(cls, pd_id):
        prefix = cls.pd_bdev_prefix()
        return f"{prefix}-{pd_id}"

    @classmethod
    def pd_lvs_name(cls, pd_id):
        prefix = cls.pd_lvs_prefix()
        return f"{prefix}-{pd_id}"

    @classmethod
    def backend_lvol_name(cls, vd_id):
        return f"{vd_id}"

    @classmethod
    def backend_lvol_fullname(cls, pd_id, vd_id):
        lvs_name = cls.pd_lvs_name(pd_id)
        lvol_name = cls.backend_lvol_name(vd_id)
        return f"{lvs_name}/{lvol_name}"

    @classmethod
    def backend_nqn_name(cls, vd_id):
        prefix = cls.backend_nqn_prefix()
        return f"{prefix}-{vd_id}"

    @classmethod
    def frontend_nqn_name(cls, cntlr_id):
        prefix = cls.frontend_nqn_prefix()
        return f"{prefix}-{cntlr_id}"

    @classmethod
    def frontend_nvme_name(cls, vd_id):
        prefix = cls.frontend_nvme_prefix()
        return f"{prefix}-{vd_id}"

    @classmethod
    def agg_bdev_name(cls, da_id):
        prefix = cls.agg_bdev_prefix()
        return f"{prefix}-{da_id}"

    @classmethod
    def da_lvs_name(cls, da_id):
        prefix = cls.da_lvs_prefix()
        return f"{prefix}-{da_id}"

    @classmethod
    def raid0_bdev_name(cls, grp_id):
        prefix = cls.raid0_bdev_prefix()
        return f"{prefix}-{grp_id}"

    @classmethod
    def grp_bdev_name(cls, grp_id):
        prefix = cls.grp_bdev_prefix()
        return f"{prefix}-{grp_id}"

    @classmethod
    def main_snap_name(cls, da_id):
        return f"{da_id}"

    @classmethod
    def snap_full_name(cls, da_id, snap_name):
        da_lvs_name = cls.da_lvs_name(da_id)
        if not snap_name:
            snap_name = cls.main_snap_name(da_id)
        return f"{da_lvs_name}/{snap_name}"

    @classmethod
    def exp_nqn_name(cls, da_name, exp_name):
        prefix = cls.exp_nqn_prefix()
        return f"{prefix}-{da_name}-{exp_name}"

    @classmethod
    def secondary_nqn_name(cls, cntlr_id):
        prefix = cls.secondary_nqn_prefix()
        return f"{prefix}-{cntlr_id}"

    @classmethod
    def secondary_nvme_name(cls, primary_cntlr_id):
        prefix = cls.secondary_nvme_prefix()
        return f"{prefix}-{primary_cntlr_id}"
