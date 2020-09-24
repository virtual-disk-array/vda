import unittest
from unittest.mock import patch, Mock


from vda.dn_agent.launcher import launch_server, DnAgentServicer


class LaunchServerTest(unittest.TestCase):

    @patch("vda.dn_agent.launcher.SpdkClient")
    @patch("vda.dn_agent.launcher.grpc")
    def test_launch_server(self, grpc_mock, SpdkClient_mock):

        server_mock = grpc_mock.server.return_value

        listener = "127.0.0.1"
        port = 9521
        max_workers = 10
        sock_path = "/tmp/dn.sock"
        sock_timeout = 10
        transport_conf = {}
        listener_conf = {}
        local_store = None
        launch_server(
            listener=listener,
            port=port,
            max_workers=max_workers,
            sock_path=sock_path,
            sock_timeout=sock_timeout,
            transport_conf=transport_conf,
            listener_conf=listener_conf,
            local_store=local_store,
        )
        grpc_mock.server.return_value.wait_for_termination.assert_called_once()


class DnAgentServicerTest(unittest.TestCase):

    @patch("vda.dn_agent.launcher.SpdkClient")
    @patch("vda.dn_agent.launcher.syncup_init")
    def test_invoke(
            self,
            syncup_init_mock,
            SpdkClient_mock,
    ):
        sock_path = "/tmp/dn.sock"
        sock_timeout = 10
        transport_conf = {}
        listener_conf = {}
        local_store = None

        dn_agent_servicer = DnAgentServicer(
            sock_path=sock_path,
            sock_timeout=sock_timeout,
            transport_conf=transport_conf,
            listener_conf=listener_conf,
            local_store=local_store,
        )
        SpdkClient_mock.assert_called_once()
        SpdkClient_mock.reset_mock()

        class MyErr(Exception):
            pass

        mock_func = Mock(side_effect=MyErr)
        mock_func.__name__ = "mock_func"
        with self.assertRaises(MyErr):
            dn_agent_servicer.invoke(mock_func, None, None)
            SpdkClient_mock.assert_called_once()

    @patch("vda.dn_agent.launcher.SpdkClient")
    @patch("vda.dn_agent.launcher.syncup_init")
    @patch("vda.dn_agent.launcher.syncup_dn")
    def test_SyncupDn(
            self,
            syncup_dn_mock,
            syncup_init_mock,
            SpdkClient_mock,
    ):
        sock_path = "/tmp/dn.sock"
        sock_timeout = 10
        transport_conf = {}
        listener_conf = {}
        local_store = None
        syncup_dn_mock.__name__ = "syncup_dn_mock"
        dn_agent_servicer = DnAgentServicer(
            sock_path=sock_path,
            sock_timeout=sock_timeout,
            transport_conf=transport_conf,
            listener_conf=listener_conf,
            local_store=local_store,
        )
        dn_agent_servicer.SyncupDn(None, None)
        syncup_dn_mock.assert_called_once()
