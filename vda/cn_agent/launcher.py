from concurrent import futures
import logging
from collections import namedtuple
from threading import Lock

import grpc

from vda.common.constant import Constant
from vda.common.spdk_client import SpdkClient
from vda.grpc import cn_agent_pb2_grpc, cn_agent_pb2
from vda.cn_agent.syncup import syncup_cn, syncup_init


logger = logging.getLogger(__name__)


CnAgentCtx = namedtuple("CnAgentCtx", [
    "grpc_ctx",
    "client",
    "local_store",
    "listener_conf",
])


class CnAgentServicer(cn_agent_pb2_grpc.CnAgentServicer):

    def __init__(
            self,
            sock_path,
            sock_timeout,
            transport_conf,
            listener_conf,
            local_store,
    ):
        self.sock_path = sock_path
        self.sock_timeout = sock_timeout
        self.client = SpdkClient(self.sock_path, self.sock_timeout)
        self.listener_conf = {}
        for key in listener_conf:
            self.listener_conf[key] = listener_conf[key]
        self.local_store = local_store
        params = {}
        for key in transport_conf:
            params[key] = transport_conf[key]
        self.client.call("nvmf_create_transport", params)
        self.lock = Lock()
        syncup_init(self.client, local_store, listener_conf)
        logger.info("cn is running")

    def invoke(self, func, request, context):
        cn_agent_ctx = CnAgentCtx(
            context,
            self.client,
            self.local_store,
            self.listener_conf,
        )
        logger.info(
            "grpc request: %s [%s] [%s]",
            func.__name__, request, cn_agent_ctx,
        )
        try:
            reply = func(request, cn_agent_ctx)
        except Exception as e:
            logger.error("grpc failed", exc_info=True)
            self.client.close()
            self.client = SpdkClient(
                self.sock_path, self.sock_timeout)
            raise e
        else:
            logger.info("grpc reply: [%s]", reply)
            return reply

    def SyncupCn(self, request, context):
        with self.lock:
            return self.invoke(syncup_cn, request, context)

    def CnHeartbeat(self, request, context):
        reply_info = cn_agent_pb2.CnAgentReplyInfo(
            reply_code=Constant.cn_success.reply_code,
            reply_msg=Constant.cn_success.reply_msg,
        )
        reply = cn_agent_pb2.CnHeartbeatReply(
            reply_info=reply_info,
        )
        locked = self.lock.acquire(blocking=False)
        if locked:
            try:
                self.client.call_and_check("bdev_get_bdevs")
            finally:
                self.lock.release()
        return reply


def launch_server(
        listener,
        port,
        max_workers,
        sock_path,
        sock_timeout,
        transport_conf,
        listener_conf,
        local_store,
):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    cn_agent_servicer = CnAgentServicer(
        sock_path=sock_path,
        sock_timeout=sock_timeout,
        transport_conf=transport_conf,
        listener_conf=listener_conf,
        local_store=local_store,
    )
    cn_agent_pb2_grpc.add_CnAgentServicer_to_server(
        cn_agent_servicer,
        server,
    )
    addr_port = f"{listener}:{port}"
    server.add_insecure_port(addr_port)
    server.start()
    server.wait_for_termination()
