from marshmallow import Schema, fields


class PortalReplyInfoSchema(Schema):
    req_id = fields.String()
    reply_code = fields.Integer()
    reply_msg = fields.String()

    class Meta:
        ordered = True


class DnMsgSchema(Schema):
    dn_id = fields.String()
    dn_name = fields.String()
    dn_listener_conf = fields.String()
    online = fields.Boolean()
    location = fields.String()
    hash_code = fields.Integer()
    version = fields.Integer()
    error = fields.Boolean()
    error_msg = fields.String()

    class Meta:
        ordered = True


class PdMsgSchema(Schema):
    pd_id = fields.String()
    dn_name = fields.String()
    pd_name = fields.String()
    total_size = fields.Integer()
    free_size = fields.Integer()
    pd_conf = fields.String()
    online = fields.Boolean()
    error = fields.Boolean()
    error_msg = fields.String()

    class Meta:
        ordered = True


class VdMsgSchema(Schema):
    vd_id = fields.String()
    da_name = fields.String()
    grp_idx = fields.Integer()
    vd_idx = fields.Integer()
    dn_name = fields.String()
    pd_name = fields.String()
    vd_size = fields.Integer()

    class Meta:
        ordered = True


class CnMsgSchema(Schema):
    cn_id = fields.String()
    cn_name = fields.String()
    cn_listener_conf = fields.String()
    online = fields.Boolean()
    hash_code = fields.Integer()
    version = fields.Integer()
    error = fields.Boolean()
    error_msg = fields.String()

    class Meta:
        ordered = True


class CntlrMsgSchema(Schema):
    cntlr_id = fields.String()
    da_name = fields.String()
    cntlr_idx = fields.Integer()
    cn_name = fields.String()
    primary = fields.Boolean()
    error = fields.Boolean()
    error_msg = fields.String()

    class Meta:
        ordered = True


class EsMsgSchema(Schema):
    es_id = fields.String()
    cntlr_idx = fields.Integer()
    cn_name = fields.String()
    cn_listener_conf = fields.String()
    error = fields.Boolean()
    error_msg = fields.String()

    class Meta:
        ordered = True


class ExpMsgSchema(Schema):
    exp_id = fields.String()
    exp_name = fields.String()
    exp_nqn = fields.String()
    da_name = fields.String()
    initiator_nqn = fields.String()
    snap_name = fields.String()
    es_msg_list = fields.List(fields.Nested(EsMsgSchema))

    class Meta:
        ordered = True


class GrpMsgSchema(Schema):
    grp_id = fields.String()
    da_name = fields.String()
    grp_idx = fields.Integer()
    grp_size = fields.Integer()
    vd_msg_list = fields.List(fields.Nested(VdMsgSchema))

    class Meta:
        ordered = True


class DaMsgSchema(Schema):
    da_id = fields.String()
    da_name = fields.String()
    cntlr_cnt = fields.Integer()
    da_size = fields.Integer()
    da_conf = fields.String()
    da_details = fields.String()
    hash_code = fields.Integer()
    error = fields.Boolean()

    class Meta:
        ordered = True


class SnapMsgSchema(Schema):
    da_name = fields.String()
    snap_name = fields.String()
    snap_size = fields.Integer()
    snapshot = fields.Boolean()
    clone = fields.Boolean()
    base_snapshot = fields.String()
    clone_list = fields.List(fields.String())

    class Meta:
        ordered = True


class CreateDnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class DeleteDnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ModifyDnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ListDnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    dn_msg_list = fields.List(fields.Nested(DnMsgSchema))

    class Meta:
        ordered = True


class GetDnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    dn_msg = fields.Nested(DnMsgSchema)
    pd_name_list = fields.List(fields.String())

    class Meta:
        ordered = True


class CreatePdReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class DeletePdReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ModifyPdReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ListPdReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    pd_msg_list = fields.List(fields.Nested(PdMsgSchema))

    class Meta:
        ordered = True


class GetPdReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    pd_msg = fields.Nested(PdMsgSchema)
    vd_msg_list = fields.List(fields.Nested(VdMsgSchema))

    class Meta:
        ordered = True


class CreateCnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class DeleteCnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ModifyCnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ListCnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    cn_msg_list = fields.List(fields.Nested(CnMsgSchema))

    class Meta:
        ordered = True


class GetCnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    cn_msg = fields.Nested(CnMsgSchema)
    cntlr_msg_list = fields.List(fields.Nested(CntlrMsgSchema))

    class Meta:
        ordered = True


class CreateDaReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class DeleteDaReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ModifyDaReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ListDaReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    da_msg_list = fields.List(fields.Nested(DaMsgSchema))

    class Meta:
        ordered = True


class GetDaReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    da_msg = fields.Nested(DaMsgSchema)
    grp_msg_list = fields.List(fields.Nested(GrpMsgSchema))
    cntlr_msg_list = fields.List(fields.Nested(CntlrMsgSchema))

    class Meta:
        ordered = True


class CreateExpReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class DeleteExpReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ModifyExpReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ListExpReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    exp_msg_list = fields.List(fields.Nested(ExpMsgSchema))

    class Meta:
        ordered = True


class GetExpReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)
    exp_msg = fields.Nested(ExpMsgSchema)

    class Meta:
        ordered = True


class ManualSyncupDnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ManualSyncupCnReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True


class ManualSyncupDaReplySchema(Schema):
    reply_info = fields.Nested(PortalReplyInfoSchema)

    class Meta:
        ordered = True
