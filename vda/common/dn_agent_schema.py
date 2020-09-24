from marshmallow import Schema, fields, post_load

from vda.grpc import dn_agent_pb2


class VdBackendCfgSchema(Schema):
    vd_id = fields.String()
    vd_clusters = fields.Integer()
    cntlr_id = fields.String()

    @post_load
    def make_vd(self, data, **kwargs):
        return dn_agent_pb2.VdBackendCfg(**data)


class PdCfgSchema(Schema):
    pd_id = fields.String()
    pd_conf = fields.String()
    vd_cfg_list = fields.List(fields.Nested(VdBackendCfgSchema))

    @post_load
    def make_pd(self, data, **kwargs):
        return dn_agent_pb2.PdCfg(**data)


class DnCfgSchema(Schema):
    dn_id = fields.String()
    version = fields.Integer()
    dn_listener_conf = fields.String()
    pd_cfg_list = fields.List(fields.Nested(PdCfgSchema))

    @post_load
    def make_dn(self, data, **kwargs):
        return dn_agent_pb2.DnCfg(**data)
