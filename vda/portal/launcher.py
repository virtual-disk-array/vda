from concurrent import futures
import logging
from collections import namedtuple
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import grpc

from vda.grpc import portal_pb2_grpc
from vda.portal.disk_node import (
    create_dn,
    delete_dn,
    modify_dn,
    list_dn,
    get_dn,
)
from vda.portal.physical_disk import (
    create_pd,
    delete_pd,
    modify_pd,
    list_pd,
    get_pd,
)
from vda.portal.controller_node import (
    create_cn,
    delete_cn,
    modify_cn,
    list_cn,
    get_cn,
)
from vda.portal.disk_array import (
    create_da,
    delete_da,
    modify_da,
    list_da,
    get_da,
)
from vda.portal.exporter import (
    create_exp,
    delete_exp,
    modify_exp,
    list_exp,
    get_exp,
)
from vda.portal.manual_syncup import (
    manual_syncup_dn,
    manual_syncup_cn,
    manual_syncup_da,
)


logger = logging.getLogger(__name__)

PortalCtx = namedtuple("PortalCtx", [
    "req_id",
    "grpc_ctx",
    "session",
])


class PortalError(Exception):
    pass


class PortalServicer(portal_pb2_grpc.PortalServicer):

    def __init__(self, db_uri, db_kwargs):
        self.engine = create_engine(db_uri, **db_kwargs)
        self.Session = sessionmaker(bind=self.engine)

    def invoke(self, func, request, context):
        req_id = uuid.uuid4().hex
        session = self.Session()
        portal_ctx = PortalCtx(
            req_id=req_id,
            grpc_ctx=context,
            session=session,
        )
        logger.info(
            "grpc request: %s [%s] [%s]", func.__name__, request, portal_ctx)
        try:
            reply = func(request, portal_ctx)
        except Exception:
            logger.error("grpc failed", exc_info=True)
            session.rollback()
            e = PortalError(f"interanl error, req_id={req_id}")
            raise e
        else:
            logger.info("grpc reply: [%s]", reply)
            return reply
        finally:
            session.close()

    def CreateDn(self, request, context):
        return self.invoke(create_dn, request, context)

    def DeleteDn(self, request, context):
        return self.invoke(delete_dn, request, context)

    def ModifyDn(self, request, context):
        return self.invoke(modify_dn, request, context)

    def ListDn(self, request, context):
        return self.invoke(list_dn, request, context)

    def GetDn(self, request, context):
        return self.invoke(get_dn, request, context)

    def CreatePd(self, request, context):
        return self.invoke(create_pd, request, context)

    def DeletePd(self, request, context):
        return self.invoke(delete_pd, request, context)

    def ModifyPd(self, request, context):
        return self.invoke(modify_pd, request, context)

    def ListPd(self, request, context):
        return self.invoke(list_pd, request, context)

    def GetPd(self, request, context):
        return self.invoke(get_pd, request, context)

    def CreateCn(self, request, context):
        return self.invoke(create_cn, request, context)

    def DeleteCn(self, request, context):
        return self.invoke(delete_cn, request, context)

    def ModifyCn(self, request, context):
        return self.invoke(modify_cn, request, context)

    def ListCn(self, request, context):
        return self.invoke(list_cn, request, context)

    def GetCn(self, request, context):
        return self.invoke(get_cn, request, context)

    def CreateDa(self, request, context):
        return self.invoke(create_da, request, context)

    def DeleteDa(self, request, context):
        return self.invoke(delete_da, request, context)

    def ModifyDa(self, request, context):
        return self.invoke(modify_da, request, context)

    def ListDa(self, request, context):
        return self.invoke(list_da, request, context)

    def GetDa(self, request, context):
        return self.invoke(get_da, request, context)

    def CreateExp(self, request, context):
        return self.invoke(create_exp, request, context)

    def DeleteExp(self, request, context):
        return self.invoke(delete_exp, request, context)

    def ModifyExp(self, request, context):
        return self.invoke(modify_exp, request, context)

    def ListExp(self, request, context):
        return self.invoke(list_exp, request, context)

    def GetExp(self, request, context):
        return self.invoke(get_exp, request, context)

    def ManualSyncupDn(self, request, context):
        return self.invoke(manual_syncup_dn, request, context)

    def ManualSyncupCn(self, request, context):
        return self.invoke(manual_syncup_cn, request, context)

    def ManualSyncupDa(self, request, context):
        return self.invoke(manual_syncup_da, request, context)


def launch_server(
        listener,
        port,
        max_workers,
        db_uri,
        db_kwargs,
):
    server = grpc.server(futures.ThreadPoolExecutor(
        max_workers=max_workers))
    portal_pb2_grpc.add_PortalServicer_to_server(
        PortalServicer(db_uri, db_kwargs),
        server,
    )
    addr_port = f"{listener}:{port}"
    server.add_insecure_port(addr_port)
    server.start()
    logger.info("vda_portal is running")
    server.wait_for_termination()
