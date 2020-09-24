import unittest
from unittest.mock import Mock, MagicMock
import json

from vda.common.dn_agent_schema import DnCfgSchema
from vda.dn_agent.syncup import (
    syncup_dn,
    syncup_init,
)


class SyncupDnTest(unittest.TestCase):

    def test_syncup_dn(self):
        schema = DnCfgSchema()
        dn_listener_conf = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4420
        }
        pd_conf = {
            "type": "malloc",
            "size": 1048576,
        }
        data = {
            "dn_id": "ccb967ee-3736-4075-9806-eadab6197626",
            "version": 1,
            "pd_cfg_list": [{
                "pd_id": "2a014196-2efc-47a7-adcb-6d7b909886a5",
                "pd_conf": json.dumps(pd_conf),
                "vd_cfg_list": [{
                    "vd_id": "13a0da86-a090-4fc2-af24-550e38968407",
                    "vd_clusters": "2048",
                    "cntlr_id": "8566a637-3118-4161-8724-d2d9fbafbe80",
                }]
            }]
        }
        dn_cfg = schema.loads(json.dumps(data))
        request = Mock()
        request.dn_cfg = dn_cfg
        context = Mock()
        context.client = MagicMock()
        context.local_store = False
        context.listener_conf = dn_listener_conf
        syncup_dn(request, context)

    def test_syncup_init(self):
        schema = DnCfgSchema()
        dn_listener_conf = {
            "trtype": "tcp",
            "traddr": "127.0.0.1",
            "adrfam":  "ipv4",
            "trsvcid": 4420
        }
        pd_conf = {
            "type": "malloc",
            "size": 1048576,
        }
        data = {
            "dn_id": "ccb967ee-3736-4075-9806-eadab6197626",
            "version": 1,
            "pd_cfg_list": [{
                "pd_id": "2a014196-2efc-47a7-adcb-6d7b909886a5",
                "pd_conf": json.dumps(pd_conf),
                "vd_cfg_list": [{
                    "vd_id": "13a0da86-a090-4fc2-af24-550e38968407",
                    "vd_clusters": "2048",
                    "cntlr_id": "8566a637-3118-4161-8724-d2d9fbafbe80",
                }]
            }]
        }
        dn_cfg = schema.loads(json.dumps(data))
        local_store = "/tmp/vda_test_local_store"
        with open(local_store, "w") as f:
            f.write(schema.dumps(dn_cfg))
        client = MagicMock()
        syncup_init(client, local_store, dn_listener_conf)
