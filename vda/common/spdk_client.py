import copy
import socket
import json
from threading import local
import logging


logger = logging.getLogger(__name__)


class JsonRpcError(Exception):
    pass


class SpdkError(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message


class SpdkClient(local):

    def __init__(self, addr, timeout):
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(addr)
        self.req_id = 0

    def close(self):
        if self.sock is not None:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def send(self, method, params):
        self.req_id += 1
        req = {
            "jsonrpc": "2.0",
            "method": method,
            "id": self.req_id,
        }

        if params:
            req["params"] = copy.deepcopy(params)

        req_str = json.dumps(req)
        logger.info("json rpc request: %s", req_str)
        self.sock.sendall(req_str.encode("utf-8"))

    def recv(self):
        full_data = ""
        reply = None
        while reply is None:
            self.sock.settimeout(self.timeout)
            data = self.sock.recv(4096)
            if not data:
                self.sock.close()
                raise JsonRpcError("incomplete response")
            full_data += data.decode("utf-8")
            try:
                logger.info("json rpc reply: %s", full_data)
                reply = json.loads(full_data)
            except json.decoder.JSONDecodeError:
                pass
        return reply

    def call(self, method, params=None):
        self.send(method, params)
        return self.recv()

    def call_and_check(self, method, params=None):
        self.send(method, params)
        ret = self.recv()
        if "error" in ret:
            raise SpdkError(ret["error"]["code"], ret["error"]["message"])
        return ret
