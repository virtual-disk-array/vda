import random
import argparse
import logging

import grpc

from vda.common.constant import Constant
from vda.common.portal_schema import (
    CreateDnReplySchema,
    DeleteDnReplySchema,
    ModifyDnReplySchema,
    ListDnReplySchema,
    GetDnReplySchema,
    CreatePdReplySchema,
    DeletePdReplySchema,
    ModifyPdReplySchema,
    ListPdReplySchema,
    GetPdReplySchema,
    CreateCnReplySchema,
    DeleteCnReplySchema,
    ModifyCnReplySchema,
    ListCnReplySchema,
    GetCnReplySchema,
    CreateDaReplySchema,
    DeleteDaReplySchema,
    ModifyDaReplySchema,
    ListDaReplySchema,
    GetDaReplySchema,
    CreateExpReplySchema,
    DeleteExpReplySchema,
    ModifyExpReplySchema,
    ListExpReplySchema,
    GetExpReplySchema,
    ManualSyncupDnReplySchema,
    ManualSyncupCnReplySchema,
    ManualSyncupDaReplySchema,
)
from vda.grpc import portal_pb2, portal_pb2_grpc


logger = logging.getLogger(__name__)


def bool_arg(arg):
    if arg == "1":
        return True
    elif arg == "0":
        return False
    else:
        raise Exception(f"invalid arg {arg}")


def show_reply(reply, schema):
    print(schema().dumps(reply, indent=2))


def dn_create_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.CreateDnRequest(
        dn_name=args.dn_name,
        dn_listener_conf=args.dn_listener_conf,
        online=args.online,
        location=args.location,
        hash_code=args.hash_code,
    )
    reply = stub.CreateDn(request)
    show_reply(reply, CreateDnReplySchema)


def dn_delete_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.DeleteDnRequest(
        dn_name=args.dn_name,
    )
    reply = stub.DeleteDn(request)
    show_reply(reply, DeleteDnReplySchema)


def dn_modify_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.new_online is not None:
        request = portal_pb2.ModifyDnRequest(
            dn_name=args.dn_name,
            new_online=args.new_online,
        )
    elif args.new_hash_code is not None:
        request = portal_pb2.ModifyDnRequest(
            dn_name=args.dn_name,
            new_hash_code=args.new_hash_code,
        )
    else:
        assert(False)
    reply = stub.ModifyDn(request)
    show_reply(reply, ModifyDnReplySchema)


def dn_list_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.online is None:
        set_online = False
    else:
        set_online = True
    if args.hash_code is None:
        set_hash_code = False
    else:
        set_hash_code = True
    if args.error is None:
        set_error = False
    else:
        set_error = True
    request = portal_pb2.ListDnRequest(
        offset=args.offset,
        limit=args.limit,
        online=args.online,
        set_online=set_online,
        location=args.location,
        hash_code=args.hash_code,
        set_hash_code=set_hash_code,
        error=args.error,
        set_error=set_error,
    )
    reply = stub.ListDn(request)
    show_reply(reply, ListDnReplySchema)


def dn_get_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.GetDnRequest(
        dn_name=args.dn_name,
    )
    reply = stub.GetDn(request)
    show_reply(reply, GetDnReplySchema)


def pd_create_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.CreatePdRequest(
        dn_name=args.dn_name,
        pd_name=args.pd_name,
        pd_conf=args.pd_conf,
        online=args.online,
    )
    reply = stub.CreatePd(request)
    show_reply(reply, CreatePdReplySchema)


def pd_delete_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.DeletePdRequest(
        dn_name=args.dn_name,
        pd_name=args.pd_name,
    )
    reply = stub.DeletePd(request)
    show_reply(reply, DeletePdReplySchema)


def pd_modify_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.new_pd_name is not None:
        request = portal_pb2.ModifyPdRequest(
            dn_name=args.dn_name,
            pd_name=args.pd_name,
            new_pd_name=args.new_pd_name,
        )
    elif args.new_online is not None:
        request = portal_pb2.ModifyPdRequest(
            dn_name=args.dn_name,
            pd_name=args.pd_name,
            new_online=args.new_online,
        )
    else:
        assert(False)
    reply = stub.ModifyPd(request)
    show_reply(reply, ModifyPdReplySchema)


def pd_list_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.ListPdRequest(
        offset=args.offset,
        limit=args.limit,
        dn_name=args.dn_name,
        online=args.online,
        error=args.error,
    )
    reply = stub.ListPd(request)
    show_reply(reply, ListPdReplySchema)


def pd_get_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.GetPdRequest(
        dn_name=args.dn_name,
        pd_name=args.pd_name,
    )
    reply = stub.GetPd(request)
    show_reply(reply, GetPdReplySchema)


def cn_create_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.CreateCnRequest(
        cn_name=args.cn_name,
        cn_listener_conf=args.cn_listener_conf,
        online=args.online,
        location=args.location,
        hash_code=args.hash_code,
    )
    reply = stub.CreateCn(request)
    show_reply(reply, CreateCnReplySchema)


def cn_delete_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.DeleteCnRequest(
        cn_name=args.cn_name,
    )
    reply = stub.DeleteCn(request)
    show_reply(reply, DeleteCnReplySchema)


def cn_modify_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.new_online is not None:
        request = portal_pb2.ModifyCnRequest(
            cn_name=args.cn_name,
            new_online=args.new_online,
        )
    elif args.new_hash_code is not None:
        request = portal_pb2.ModifyCnRequest(
            cn_name=args.cn_name,
            new_hash_code=args.new_hash_code,
        )
    else:
        assert(False)
    reply = stub.ModifyCn(request)
    show_reply(reply, ModifyCnReplySchema)


def cn_list_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.online is None:
        set_online = False
    else:
        set_online = True
    if args.hash_code is None:
        set_hash_code = False
    else:
        set_hash_code = True
    if args.error is None:
        set_error = False
    else:
        set_error = True
    request = portal_pb2.ListCnRequest(
        offset=args.offset,
        limit=args.limit,
        online=args.online,
        set_online=set_online,
        location=args.location,
        hash_code=args.hash_code,
        set_hash_code=set_hash_code,
        error=args.error,
        set_error=set_error,
    )
    reply = stub.ListCn(request)
    show_reply(reply, ListCnReplySchema)


def cn_get_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.GetCnRequest(
        cn_name=args.cn_name,
    )
    reply = stub.GetCn(request)
    show_reply(reply, GetCnReplySchema)


def da_create_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.CreateDaRequest(
        da_name=args.da_name,
        cntlr_cnt=args.cntlr_cnt,
        da_size=args.da_size,
        physical_size=args.physical_size,
        hash_code=args.hash_code,
        da_conf=args.da_conf,
    )
    reply = stub.CreateDa(request)
    show_reply(reply, CreateDaReplySchema)


def da_delete_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.DeleteDaRequest(
        da_name=args.da_name,
    )
    reply = stub.DeleteDa(request)
    show_reply(reply, DeleteDaReplySchema)


def da_modify_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.new_hash_code is not None:
        request = portal_pb2.ModifyDaRequest(
            da_name=args.da_name,
            new_hash_code=args.new_hash_code,
        )
    else:
        assert(False)
    reply = stub.ModifyDa(request)
    show_reply(reply, ModifyDaReplySchema)


def da_list_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.hash_code is None:
        set_hash_code = False
    else:
        set_hash_code is True
    request = portal_pb2.ListDaRequest(
        offset=args.offset,
        limit=args.limit,
        hash_code=args.hash_code,
        set_hash_code=set_hash_code,
    )
    reply = stub.ListDa(request)
    show_reply(reply, ListDaReplySchema)


def da_get_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.GetDaRequest(
        da_name=args.da_name,
    )
    reply = stub.GetDa(request)
    show_reply(reply, GetDaReplySchema)


def exp_create_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.CreateExpRequest(
        da_name=args.da_name,
        exp_name=args.exp_name,
        initiator_nqn=args.initiator_nqn,
        snap_name=args.snap_name,
    )
    reply = stub.CreateExp(request)
    show_reply(reply, CreateExpReplySchema)


def exp_delete_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.DeleteExpRequest(
        da_name=args.da_name,
        exp_name=args.exp_name,
    )
    reply = stub.DeleteExp(request)
    show_reply(reply, DeleteExpReplySchema)


def exp_modify_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.new_exp_name is not None:
        request = portal_pb2.ModifyExpRequest(
            da_name=args.da_name,
            exp_name=args.exp_name,
            new_exp_name=args.new_exp_name,
        )
    else:
        assert(False)
    reply = stub.ModifyExp(request)
    show_reply(reply, ModifyExpReplySchema)


def exp_list_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    if args.initiator_nqn is None:
        set_initiator_nqn = False
    else:
        set_initiator_nqn = True
    request = portal_pb2.ListExpRequest(
        offset=args.offset,
        limit=args.limit,
        da_name=args.da_name,
        initiator_nqn=args.initiator_nqn,
        set_initiator_nqn=set_initiator_nqn,
    )
    reply = stub.ListExp(request)
    show_reply(reply, ListExpReplySchema)


def exp_get_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.GetExpRequest(
        da_name=args.da_name,
        exp_name=args.exp_name,
    )
    reply = stub.GetExp(request)
    show_reply(reply, GetExpReplySchema)


def syncup_dn_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.ManualSyncupDnRequest(
        dn_name=args.dn_name,
    )
    reply = stub.ManualSyncupDn(request)
    show_reply(reply, ManualSyncupDnReplySchema)


def syncup_cn_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.ManualSyncupCnRequest(
        cn_name=args.cn_name,
    )
    reply = stub.ManualSyncupCn(request)
    show_reply(reply, ManualSyncupCnReplySchema)


def syncup_da_handler(args):
    channel = grpc.insecure_channel(args.addr_port)
    stub = portal_pb2_grpc.PortalStub(channel)
    request = portal_pb2.ManualSyncupDaRequest(
        da_name=args.da_name,
    )
    reply = stub.ManualSyncupDa(request)
    show_reply(reply, ManualSyncupDaReplySchema)


parser = argparse.ArgumentParser(
    prog=Constant.CLI_NAME,
    add_help=True,
)

parser.add_argument(
    "--addr-port",
    default=Constant.CLI_DEFAULT_ADDR_PORT,
    help="vda portal grpc server address:port",
)

parser.add_argument(
    "--timeout",
    type=int,
    default=Constant.CLI_DEFAULT_TIMEOUT,
    help="vda portal grpc timeout",
)

main_subparsers = parser.add_subparsers(help="vda commands")

dn_parser = main_subparsers.add_parser(
    "dn",
    help="manage data node",
)

dn_sub_parser = dn_parser.add_subparsers(help="manage data node")

dn_create_parser = dn_sub_parser.add_parser(
    "create",
    help="create data node",
)
dn_create_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
dn_create_parser.add_argument(
    "--dn-listener-conf",
    default=Constant.DEFAULT_DN_LISTENER_CONF_STR,
    help="disk node listener configuration string",
)
dn_create_parser.add_argument(
    "--online",
    type=bool_arg,
    default=True,
    help="disk node online status, 1 means online, 0 menas not online",
)
dn_create_parser.add_argument(
    "--location",
    default="",
    help="disk node location",
)
dn_create_parser.add_argument(
    "--hash-code",
    type=int,
    default=random.randrange(0, Constant.MAX_HASH_CODE),
    help="disk node hash code, 0 - 65535 included",
)
dn_create_parser.set_defaults(func=dn_create_handler)

dn_delete_parser = dn_sub_parser.add_parser(
    "delete",
    help="delete disk node",
)
dn_delete_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
dn_delete_parser.set_defaults(func=dn_delete_handler)

dn_modify_parser = dn_sub_parser.add_parser(
    "modify",
    help="modify disk node",
)
dn_modify_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
dn_modify_group = dn_modify_parser.add_mutually_exclusive_group(
    required=True)
dn_modify_group.add_argument(
    "--new-online",
    type=bool_arg,
    help="disk node new online status, 1 for online, 0 for not online",
)
dn_modify_group.add_argument(
    "--new-hash-code",
    type=int,
    help="disk node new hash code, 0 - 65535 included",
)
dn_modify_parser.set_defaults(func=dn_modify_handler)

dn_list_parser = dn_sub_parser.add_parser(
    "list",
    help="list disk node",
)
dn_list_parser.add_argument(
    "--offset",
    type=int,
    help="offset",
)
dn_list_parser.add_argument(
    "--limit",
    type=int,
    help="limit",
)
dn_list_parser.add_argument(
    "--online",
    type=bool_arg,
    help="filter by online status",
)
dn_list_parser.add_argument(
    "--location",
    help="filter by location",
)
dn_list_parser.add_argument(
    "--hash-code",
    type=int,
    help="filter by hash code",
)
dn_list_parser.add_argument(
    "--error",
    type=bool_arg,
    help="filter by error status, 1 means True, 0 means False",
)
dn_list_parser.set_defaults(func=dn_list_handler)

dn_get_parser = dn_sub_parser.add_parser(
    "get",
    help="get disk node",
)
dn_get_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
dn_get_parser.set_defaults(func=dn_get_handler)

pd_parser = main_subparsers.add_parser(
    "pd",
    help="manage physical disk",
)

pd_sub_parser = pd_parser.add_subparsers(help="manage physical disk")

pd_create_parser = pd_sub_parser.add_parser(
    "create",
    help="create physical disk",
)
pd_create_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
pd_create_parser.add_argument(
    "--pd-name",
    required=True,
    help="physical disk name",
)
pd_create_parser.add_argument(
    "--pd-conf",
    required=True,
    help="physical disk configurate string",
)
pd_create_parser.add_argument(
    "--online",
    type=bool_arg,
    default=True,
    help="physical disk online status, 1 means online, 0 means not online",
)
pd_create_parser.set_defaults(func=pd_create_handler)

pd_delete_parser = pd_sub_parser.add_parser(
    "delete",
    help="delete data node",
)
pd_delete_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
pd_delete_parser.add_argument(
    "--pd-name",
    required=True,
    help="physical disk name",
)
pd_delete_parser.set_defaults(func=pd_delete_handler)

pd_modify_parser = pd_sub_parser.add_parser(
    "modify",
    help="modify physical disk",
)
pd_modify_parser.add_argument(
    "--dn-name",
    required=True,
    help="data node hostname:port",
)
pd_modify_parser.add_argument(
    "--pd-name",
    required=True,
    help="physical disk name",
)
pd_modify_group = pd_modify_parser.add_mutually_exclusive_group(
    required=True)
pd_modify_group.add_argument(
    "--new-pd-name",
    help="new physical disk name",
)
pd_modify_group.add_argument(
    "--new-online",
    type=bool_arg,
    help="new online status, 1 means True, 0 means False",
)
pd_modify_parser.set_defaults(func=pd_modify_handler)

pd_list_parser = pd_sub_parser.add_parser(
    "list",
    help="list physical disks on a disk node"
)
pd_list_parser.add_argument(
    "--offset",
    type=int,
    help="offset",
)
pd_list_parser.add_argument(
    "--limit",
    type=int,
    help="limit",
)
pd_list_parser.add_argument(
    "--dn-name",
    required=True,
    help="filter by disk node hostname:port",
)
pd_list_parser.add_argument(
    "--online",
    type=bool_arg,
    help="filter by online status, 1 means True, 0 means False",
)
pd_list_parser.add_argument(
    "--error",
    type=bool_arg,
    help="filter by error status, 1 means True, 0 means False",
)
pd_list_parser.set_defaults(func=pd_list_handler)

pd_get_parser = pd_sub_parser.add_parser(
    "get",
    help="get physical disk",
)
pd_get_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
pd_get_parser.add_argument(
    "--pd-name",
    required=True,
    help="physical disk name",
)
pd_get_parser.set_defaults(func=pd_get_handler)

cn_parser = main_subparsers.add_parser(
    "cn",
    help="manage controller node",
)

cn_sub_parser = cn_parser.add_subparsers(help="manage controller node")

cn_create_parser = cn_sub_parser.add_parser(
    "create",
    help="create controller node",
)
cn_create_parser.add_argument(
    "--cn-name",
    required=True,
    help="controller node hostname:port",
)
cn_create_parser.add_argument(
    "--cn-listener-conf",
    default=Constant.DEFAULT_CN_LISTENER_CONF_STR,
    help="controller node listener configuration string",
)
cn_create_parser.add_argument(
    "--online",
    type=bool_arg,
    default=True,
    help="controller node online status, 1 means True, 0 means False",
)
cn_create_parser.add_argument(
    "--location",
    default="",
    help="controller node location",
)
cn_create_parser.add_argument(
    "--hash-code",
    type=int,
    default=random.randrange(0, Constant.MAX_HASH_CODE),
    help="controller node hash code, 0 - 65535",
)
cn_create_parser.set_defaults(func=cn_create_handler)

cn_delete_parser = cn_sub_parser.add_parser(
    "delete",
    help="delete controller node",
)
cn_delete_parser.add_argument(
    "--cn-name",
    required=True,
    help="controller node hostname:port",
)
cn_delete_parser.set_defaults(func=cn_delete_handler)

cn_modify_parser = cn_sub_parser.add_parser(
    "modify",
    help="modify controller node",
)
cn_modify_parser.add_argument(
    "--cn-name",
    required=True,
    help="controller node hostname:port",
)
cn_modify_group = cn_modify_parser.add_mutually_exclusive_group(
    required=True)
cn_modify_group.add_argument(
    "--new-online",
    type=bool_arg,
    help="new online status, 1 means True, 0 means False",
)
cn_modify_group.add_argument(
    "--new-hash-code",
    type=int,
    help="new hash code, 0 - 65535",
)
cn_modify_parser.set_defaults(func=cn_modify_handler)

cn_list_parser = cn_sub_parser.add_parser(
    "list",
    help="list controller node",
)
cn_list_parser.add_argument(
    "--offset",
    type=int,
    help="offset",
)
cn_list_parser.add_argument(
    "--limit",
    type=int,
    help="limit",
)
cn_list_parser.add_argument(
    "--online",
    type=bool_arg,
    help="filter by online status, 1 means True, 0 means False",
)
cn_list_parser.add_argument(
    "--location",
    help="filter by location",
)
cn_list_parser.add_argument(
    "--hash-code",
    type=int,
    help="filter by hash code",
)
cn_list_parser.add_argument(
    "--error",
    type=bool_arg,
    help="filter by error status, 1 means True, 0 means False",
)
cn_list_parser.set_defaults(func=cn_list_handler)

cn_get_parser = cn_sub_parser.add_parser(
    "get",
    help="get controller node",
)
cn_get_parser.add_argument(
    "--cn-name",
    required=True,
    help="controller node hostname:port",
)
cn_get_parser.set_defaults(func=cn_get_handler)

da_parser = main_subparsers.add_parser(
    "da",
    help="manage disk array",
)

da_sub_parser = da_parser.add_subparsers(help="manage disk array")

da_create_parser = da_sub_parser.add_parser(
    "create",
    help="create disk array",
)
da_create_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
da_create_parser.add_argument(
    "--cntlr-cnt",
    type=int,
    required=True,
    help="controller count",
)
da_create_parser.add_argument(
    "--da-size",
    type=int,
    required=True,
    help="disk arrary size",
)
da_create_parser.add_argument(
    "--physical-size",
    type=int,
    required=True,
    help="physical disk size of the disk array",
)
da_create_parser.add_argument(
    "--hash-code",
    type=int,
    default=random.randrange(0, Constant.MAX_HASH_CODE),
    help="disk array hash code, 0 - 65535 included",
)
da_create_parser.add_argument(
    "--da-conf",
    help="disk array configuration string",
)
da_create_parser.set_defaults(func=da_create_handler)

da_delete_parser = da_sub_parser.add_parser(
    "delete",
    help="delete disk array",
)
da_delete_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
da_delete_parser.set_defaults(func=da_delete_handler)

da_modify_parser = da_sub_parser.add_parser(
    "modify",
    help="modify disk array",
)
da_modify_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
da_modify_group = da_modify_parser.add_mutually_exclusive_group(
    required=True)
da_modify_group.add_argument(
    "--new-hash-code",
    type=int,
    help="new hash code",
)
da_modify_parser.set_defaults(func=da_modify_handler)

da_list_parser = da_sub_parser.add_parser(
    "list",
    help="list disk array",
)
da_list_parser.add_argument(
    "--offset",
    type=int,
    help="offset",
)
da_list_parser.add_argument(
    "--limit",
    type=int,
    help="limit",
)
da_list_parser.add_argument(
    "--hash-code",
    type=int,
    help="filter by hash code",
)
da_list_parser.set_defaults(func=da_list_handler)

da_get_parser = da_sub_parser.add_parser(
    "get",
    help="get disk array",
)
da_get_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
da_get_parser.set_defaults(func=da_get_handler)

exp_parser = main_subparsers.add_parser(
    "exp",
    help="manage exporter",
)

exp_sub_parser = exp_parser.add_subparsers(help="manage exporter")

exp_create_parser = exp_sub_parser.add_parser(
    "create",
    help="create exporter",
)
exp_create_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
exp_create_parser.add_argument(
    "--exp-name",
    required=True,
    help="exporter name",
)
exp_create_parser.add_argument(
    "--initiator-nqn",
    required=True,
    help="initiator nqn",
)
exp_create_parser.add_argument(
    "--snap-name",
    required=False,
    help="snap name",
)
exp_create_parser.set_defaults(func=exp_create_handler)

exp_delete_parser = exp_sub_parser.add_parser(
    "delete",
    help="delete exporter",
)
exp_delete_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
exp_delete_parser.add_argument(
    "--exp-name",
    required=True,
    help="exporter name",
)
exp_delete_parser.set_defaults(func=exp_delete_handler)

exp_modify_parser = exp_sub_parser.add_parser(
    "modify",
    help="modify exporter",
)
exp_modify_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
exp_modify_parser.add_argument(
    "--exp-name",
    help="exporter name",
)
exp_modify_group = exp_modify_parser.add_mutually_exclusive_group(
    required=True)
exp_modify_group.add_argument(
    "--new-exp-name",
    help="new exporter name",
)
exp_modify_parser.set_defaults(func=exp_modify_handler)

exp_list_parser = exp_sub_parser.add_parser(
    "list",
    help="list exporters on a disk array"
)
exp_list_parser.add_argument(
    "--offset",
    type=int,
    help="offset",
)
exp_list_parser.add_argument(
    "--limit",
    type=int,
    help="limit",
)
exp_list_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
exp_list_parser.add_argument(
    "--initiator-nqn",
    help="filter by snap initiator nqn",
)
exp_list_parser.set_defaults(func=exp_list_handler)

exp_get_parser = exp_sub_parser.add_parser(
    "get",
    help="get exporter",
)
exp_get_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
exp_get_parser.add_argument(
    "--exp-name",
    required=True,
    help="exporter name",
)
exp_get_parser.set_defaults(func=exp_get_handler)


syncup_parser = main_subparsers.add_parser(
    "syncup",
    help="syncup manually",
)

syncup_sub_parser = syncup_parser.add_subparsers(help="syncup manually")

syncup_dn_parser = syncup_sub_parser.add_parser(
    "dn",
    help="syncup disk node manually",
)
syncup_dn_parser.add_argument(
    "--dn-name",
    required=True,
    help="disk node hostname:port",
)
syncup_dn_parser.set_defaults(func=syncup_dn_handler)

syncup_cn_parser = syncup_sub_parser.add_parser(
    "cn",
    help="syncup controller node manually",
)
syncup_cn_parser.add_argument(
    "--cn-name",
    required=True,
    help="controller node hostname:port",
)
syncup_cn_parser.set_defaults(func=syncup_cn_handler)

syncup_da_parser = syncup_sub_parser.add_parser(
    "da",
    help="syncup disk array manually",
)
syncup_da_parser.add_argument(
    "--da-name",
    required=True,
    help="disk array name",
)
syncup_da_parser.set_defaults(func=syncup_da_handler)


def main():
    args = parser.parse_args()
    return args.func(args)
