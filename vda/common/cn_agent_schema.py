from marshmallow import Schema, fields, post_load

from vda.grpc import cn_agent_pb2


class CntlrCfgSchema(Schema):
    cntlr_id = fields.String()
    cntlr_idx = fields.Integer()
    primary = fields.Boolean()
    cn_name = fields.String()
    cn_listener_conf = fields.String()

    @post_load
    def make_cntlr(self, data, **kwargs):
        return cn_agent_pb2.CntlrCfg(**data)


class VdFrontendCfgSchema(Schema):
    vd_id = fields.String()
    vd_idx = fields.Integer()
    vd_clusters = fields.Integer()
    dn_name = fields.String()
    dn_listener_conf = fields.String()

    @post_load
    def make_vd(self, data, **kwargs):
        return cn_agent_pb2.VdFrontendCfg(**data)


class GrpCfgSchema(Schema):
    grp_id = fields.String()
    grp_idx = fields.Integer()
    grp_size = fields.Integer()
    vd_cfg_list = fields.List(fields.Nested(VdFrontendCfgSchema))

    @post_load
    def make_grp(self, data, **kwargs):
        return cn_agent_pb2.GrpCfg(**data)


class ExpCfgSchema(Schema):
    exp_id = fields.String()
    exp_name = fields.String()
    initiator_nqn = fields.String()
    snap_name = fields.String()

    @post_load
    def make_exp(self, data, **kwargs):
        return cn_agent_pb2.ExpCfg(**data)


class DaCntlrCfgSchema(Schema):
    da_id = fields.String()
    da_name = fields.String()
    cntlr_id = fields.String()
    da_size = fields.Integer()
    da_conf = fields.String()
    cntlr_cfg_list = fields.List(fields.Nested(CntlrCfgSchema))
    grp_cfg_list = fields.List(fields.Nested(GrpCfgSchema))
    exp_cfg_list = fields.List(fields.Nested(ExpCfgSchema))

    @post_load
    def make_da_cntlr(self, data, **kwargs):
        return cn_agent_pb2.DaCntlrCfg(**data)


class CnCfgSchema(Schema):
    cn_id = fields.String()
    version = fields.Integer()
    da_cntlr_cfg_list = fields.List(fields.Nested(DaCntlrCfgSchema))

    @post_load
    def make_cn(self, data, **kwargs):
        return cn_agent_pb2.CnCfg(**data)
