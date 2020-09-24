import unittest
from unittest.mock import Mock, MagicMock
import json

from vda.common.cn_agent_schema import CnCfgSchema
from vda.cn_agent.syncup import (
    syncup_cn,
    syncup_init,
)


def spdk_side_effect(method, params=None):
    if method == "bdev_get_bdevs":
        return {
            "result": [{
                "name": "abc",
            }]
        }
    elif method == "nvmf_get_subsystems":
        return {
            "result": [{
                "nqn": "abc",
            }]
        }
    elif method == "bdev_nvme_get_controllers":
        return {
            "result": [{
                "name": "abc",
            }]
        }
    else:
        return {
            "result": [{}]
        }

class SyncupCnTest(unittest.TestCase):

    def _get_cn_cfg(self, primary):
        if primary:
            first_role = True
            second_role = False
        else:
            first_role = False
            second_role = True

        schema = CnCfgSchema()
        cn_listener_conf_1 = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4421
        }
        cn_listener_conf_2 = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4422
        }
        dn_listener_conf = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4420
        }
        da_conf = {
            "stripe_size_kb": 64,
        }
        data = {
            "cn_id": "83d47e55-6e7d-45b2-a411-48b1f67ada99",
            "version": 1,
            "da_cntlr_cfg_list": [{
                "da_id": "9b9adc4e-c2e8-4aae-802e-0f12f72b7fa3",
                "da_name": "da0",
                "cntlr_id": "a6c39db7-0a16-454f-802f-f6b88433705b",
                "da_size": 200*1024*1024*1024,
                "da_conf": json.dumps(da_conf),
                "cntlr_cfg_list": [{
                    "cntlr_id": "a6c39db7-0a16-454f-802f-f6b88433705b",
                    "cntlr_idx": 0,
                    "primary": first_role,
                    "cn_name": "localhost",
                    "cn_listener_conf": json.dumps(cn_listener_conf_1),
                    
                }, {
                    "cntlr_id": "d5c539aa-3d54-4820-a976-44808a98d3f7",
                    "cntlr_idx": 1,
                    "primary": second_role,
                    "cn_name": "localhost",
                    "cn_listener_conf": json.dumps(cn_listener_conf_2),
                }],
                "grp_cfg_list": [{
                    "grp_idx": "c9798e53-e65d-4fe7-9f3c-9f1a61adc536",
                    "grp_idx": 0,
                    "grp_size": 10*1024*1024*1024,
                    "vd_cfg_list": [{
                        "vd_id": "f1edba9a-94aa-48c1-b8c1-45f70dc3d70c",
                        "vd_idx": 0,
                        "vd_clusters": 10240,
                        "dn_name": "localhost",
                        "dn_listener_conf": json.dumps(dn_listener_conf)
                    }],
                }],
                "exp_cfg_list": [{
                    "exp_id": "b4783ba2-8405-4c75-9341-7883f5891dac",
                    "exp_name": "exp0",
                    "initiator_nqn": "nqn.2016-06.io.spdk:host0",
                    "snap_name": "snap_name",
                }]
            }]
        }
        cn_cfg = schema.loads(json.dumps(data))
        return cn_cfg

    def test_syncup_cn_primary(self):
        cn_cfg = self._get_cn_cfg(True)
        request = Mock()
        request.cn_cfg = cn_cfg
        context = Mock()
        context.client = MagicMock()
        context.client.call_and_check.side_effect = spdk_side_effect
        context.local_store = False
        context.listener_conf = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4421
        }
        syncup_cn(request, context)

    def test_syncup_cn_secondary(self):
        cn_cfg = self._get_cn_cfg(False)
        request = Mock()
        request.cn_cfg = cn_cfg
        context = Mock()
        context.client = MagicMock()
        context.client.call_and_check.side_effect = spdk_side_effect
        context.local_store = False
        context.listener_conf = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4422
        }
        syncup_cn(request, context)
