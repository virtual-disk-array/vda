import json
import uuid
import math
import hashlib

from vda.common.constant import Constant


class OperationClient:

    def __init__(self, spdk_client):
        self.spdk_client = spdk_client
        self.bdev_list = None
        self.bdev_dict = None
        self.nvmf_dict = None

    def load_bdevs(self):
        ret = self.spdk_client.call_and_check("bdev_get_bdevs")
        self.bdev_list = ret["result"]
        self.bdev_dict = {}
        for bdev_conf in self.bdev_list:
            name = bdev_conf["name"]
            self.bdev_dict[name] = bdev_conf

    def load_nvmfs(self):
        ret = self.spdk_client.call_and_check("nvmf_get_subsystems")
        self.nvmf_dict = {}
        for nvmf_conf in ret["result"]:
            nqn = nvmf_conf["nqn"]
            self.nvmf_dict[nqn] = nvmf_conf

    def _get_bdev_name_list(self, bdev_prefix):
        bdev_name_list = []
        for bdev_conf in self.bdev_list:
            name = bdev_conf["name"]
            if name.startswith(bdev_prefix):
                bdev_name_list.append(name)
        return bdev_name_list

    def examine_bdev(self, name):
        params = {
            "name": name,
        }
        self.spdk_client.call("bdev_examine", params)

    def create_pd_bdev(self, pd_bdev_name, pd_conf):
        params = {
            "name": pd_bdev_name,
        }
        ret = self.spdk_client.call("bdev_get_bdevs", params)
        if "result" in ret:
            return

        bdev_type = pd_conf["type"]
        if bdev_type == "malloc":
            size = pd_conf["size"]
            block_size = Constant.PAGE_SIZE
            num_blocks = math.ceil(size/block_size)
            params = {
                "name": pd_bdev_name,
                "block_size": block_size,
                "num_blocks": num_blocks,
            }
            self.spdk_client.call_and_check("bdev_malloc_create", params)
        elif bdev_type == "aio":
            block_size = Constant.PAGE_SIZE
            filename = pd_conf["filename"]
            params = {
                "name": pd_bdev_name,
                "block_size": block_size,
                "filename": filename,
            }
            self.spdk_client.call_and_check("bdev_aio_create", params)
        elif bdev_type == "nvme":
            params = {
                "trtype": "pcie",
                "name": pd_bdev_name,
                "traddr": pd_conf["traddr"],
            }
            self.spdk_client.call_and_check(
                "bdev_nvme_attach_controller", params)
        else:
            raise Exception(f"Unsupport bdev type: {bdev_type}")

    def delete_pd_bdev(self, pd_bdev_name):
        bdev_conf = self.bdev_dict[pd_bdev_name]
        product_name = bdev_conf["product_name"]
        if product_name == "Malloc disk":
            params = {
                "name": pd_bdev_name,
            }
            self.spdk_client.call("bdev_malloc_delete", params)
        elif product_name == "AIO disk":
            params = {
                "name": pd_bdev_name,
            }
            self.spdk_client.call("bdev_aio_delete", params)
        elif product_name == "NVMe disk":
            params = {
                "nqn": bdev_conf["driver_specific"]["nvme"]["trid"]["subnqn"]
            }
            self.spdk_client.call("nvmf_delete_subsystem", params)
        else:
            raise Exception(f"Unknow product_name: {product_name}")

    def get_pd_bdev_list(self, pd_bdev_prefix):
        return self._get_bdev_name_list(pd_bdev_prefix)

    def create_pd_lvs(self, pd_lvs_name, pd_bdev_name):
        params = {
            "lvs_name": pd_lvs_name,
        }
        ret = self.spdk_client.call("bdev_lvol_get_lvstores", params)
        if "result" in ret:
            return

        params = {
            "lvs_name": pd_lvs_name,
            "bdev_name": pd_bdev_name,
            "clear_method": "none",
            "cluster_sz": Constant.CLUSTER_SIZE,
        }
        self.spdk_client.call_and_check("bdev_lvol_create_lvstore", params)

    def delete_pd_lvs(self, pd_lvs_name):
        params = {
            "lvs_name": pd_lvs_name,
        }
        self.spdk_client.call("bdev_lvol_delete_lvstore", params)

    def get_pd_lvs_list(self, pd_lvs_prefix):
        ret = self.spdk_client.call("bdev_lvol_get_lvstores")
        pd_lvs_list = []
        for lvs_conf in ret["result"]:
            lvs_name = lvs_conf["name"]
            if lvs_name.startswith(pd_lvs_prefix):
                pd_lvs_list.append(lvs_name)
        return pd_lvs_list

    def create_backend_lvol(
            self, pd_lvs_name, backend_lvol_name, vd_clusters):
        params = {
            "name": f"{pd_lvs_name}/{backend_lvol_name}",
        }
        ret = self.spdk_client.call("bdev_get_bdevs", params)
        if "result" in ret:
            return

        params = {
            "lvol_name": backend_lvol_name,
            "size": vd_clusters * Constant.CLUSTER_SIZE,
            "lvs_name":  pd_lvs_name,
            "clear_method": "none",
            "thin_provision": False,
        }
        self.spdk_client.call_and_check("bdev_lvol_create", params)

    def delete_backend_lvol(self, backend_lvol_fullname):
        params = {
            "name": backend_lvol_fullname,
        }
        self.spdk_client.call("bdev_lvol_delete", params)

    def get_backend_lvol_list(self, backend_lvol_prefix):
        backend_lvol_list = []
        for bdev_conf in self.bdev_list:
            for alias in bdev_conf["aliases"]:
                if alias.startswith(backend_lvol_prefix):
                    backend_lvol_list.append(alias)
        return backend_lvol_list

    def _create_nvmf_subsystem(self, nqn):
        m = hashlib.md5()
        m.update(nqn.encode())
        serial_number = m.hexdigest()[:Constant.NVMF_SN_LENGTH]
        params = {
            "nqn": nqn,
            "allow_any_host": False,
            "serial_number": serial_number,
            "model_number": Constant.NVMF_MODULE_NUMBER,
        }
        self.spdk_client.call_and_check("nvmf_create_subsystem", params)

    def _create_nvmf_ns(self, nqn, bdev_name):
        m = hashlib.md5()
        m.update(nqn.encode())
        params = {
            "nqn": nqn,
            "namespace": {
                "nsid": 1,
                "uuid": str(uuid.UUID(int=int(m.hexdigest(), 16))),
                "bdev_name": bdev_name,
            }
        }
        self.spdk_client.call_and_check("nvmf_subsystem_add_ns", params)

    def _create_nvmf_listener(self, nqn, trtype, adrfam, traddr, trsvcid):
        params = {
            "nqn": nqn,
            "listen_address": {
                "trtype": trtype,
                "adrfam": adrfam,
                "traddr": traddr,
                "trsvcid": trsvcid,
            }
        }
        self.spdk_client.call_and_check("nvmf_subsystem_add_listener", params)

    def _create_nvmf_host(self, nqn, host):
        params = {
            "nqn": nqn,
            "host": host,
        }
        self.spdk_client.call_and_check("nvmf_subsystem_add_host", params)

    def _delete_nvmf_host(self, nqn, host):
        params = {
            "nqn": nqn,
            "host": host,
        }
        self.spdk_client.call_and_check("nvmf_subsystem_remove_host", params)

    def create_backend_nvmf(
            self, backend_nqn_name, backend_lvol_fullname,
            frontend_nqn_name, dn_listener_conf):
        nvmf_conf = self.nvmf_dict.get(backend_nqn_name)
        # The nvmf subsystem should only allow frontend_nqn_name to access it.
        # If the allowed host nqn does not match frontend_nqn_name,
        # we should delete the subsystem and create a new one.
        # Because if we do not delete the subsystem,
        # the old host will still be able to access this subsystem
        # even we delete it from the nvmf_conf["host"]
        if nvmf_conf is not None \
           and len(nvmf_conf["hosts"]) > 0 \
           and nvmf_conf["hosts"][0]["nqn"] != frontend_nqn_name:
            params = {
                "nqn": backend_nqn_name,
            }
            self.spdk_client.call_and_check("nvmf_delete_subsystem", params)
            nvmf_conf = None
        if nvmf_conf is None:
            self._create_nvmf_subsystem(backend_nqn_name)
            self._create_nvmf_ns(backend_nqn_name, backend_lvol_fullname)
            self._create_nvmf_listener(
                nqn=backend_nqn_name,
                trtype=dn_listener_conf["trtype"],
                adrfam=dn_listener_conf["adrfam"],
                traddr=dn_listener_conf["traddr"],
                trsvcid=dn_listener_conf["trsvcid"],
            )
            self._create_nvmf_host(backend_nqn_name, frontend_nqn_name)
        else:
            if len(nvmf_conf["namespaces"]) == 0:
                self._create_nvmf_ns(backend_nqn_name, backend_lvol_fullname)
            if len(nvmf_conf["listen_addresses"]) == 0:
                self._create_nvmf_listener(
                    nqn=backend_nqn_name,
                    trtype=dn_listener_conf["trtype"],
                    adrfam=dn_listener_conf["adrfam"],
                    traddr=dn_listener_conf["traddr"],
                    trsvcid=dn_listener_conf["trsvcid"],
                )
            if len(nvmf_conf["hosts"]) == 0:
                self._create_nvmf_host(backend_nqn_name, frontend_nqn_name)

    def delete_backend_nvmf(self, backend_nqn_name):
        params = {
            "nqn": backend_nqn_name,
        }
        self.spdk_client.call("nvmf_delete_subsystem", params)

    def get_backend_nqn_list(self, backend_nqn_prefix):
        backend_nqn_list = []
        for nqn in self.nvmf_dict:
            if nqn.startswith(backend_nqn_prefix):
                backend_nqn_list.append(nqn)
        return backend_nqn_list

    def get_pd_lvs_size(self, pd_lvs_name):
        params = {
            "lvs_name": pd_lvs_name,
        }
        ret = self.spdk_client.call("bdev_lvol_get_lvstores", params)
        total_clusters = ret["result"][0]["total_data_clusters"]
        free_clusters = ret["result"][0]["free_clusters"]
        return total_clusters, free_clusters

    def create_raid0_bdev(
            self, raid0_bdev_name, bdev_name_list,  stripe_size_kb):
        params = {
            "name": raid0_bdev_name,
        }
        ret = self.spdk_client.call("bdev_get_bdevs", params)
        if "result" in ret:
            return

        params = {
            "name": raid0_bdev_name,
            "raid_level": "raid0",
            "base_bdevs": bdev_name_list,
            "strip_size_kb": stripe_size_kb,
        }
        self.spdk_client.call_and_check("bdev_raid_create", params)

    def delete_raid0_bdev(self, raid0_bdev_name):
        params = {
            "name": raid0_bdev_name,
        }
        self.spdk_client.call("bdev_raid_delete", params)

    def get_raid0_bdev_list(self, raid0_bdev_prefix):
        return self._get_bdev_name_list(raid0_bdev_prefix)

    def create_grp_bdev(self, grp_bdev_name, underlying_bdev_name):
        params = {
            "name": grp_bdev_name,
        }
        ret = self.spdk_client.call("bdev_get_bdevs", params)
        if "result" in ret:
            return

        params = {
            "name": grp_bdev_name,
            "base_bdev_name": underlying_bdev_name,
        }
        self.spdk_client.call_and_check("bdev_passthru_create", params)

    def delete_grp_bdev(self, grp_bdev_name):
        params = {
            "name": grp_bdev_name,
        }
        self.spdk_client.call("bdev_passthru_delete", params)

    def get_grp_bdev_list(self, grp_bdev_prefix):
        return self._get_bdev_name_list(grp_bdev_prefix)

    def create_frontend_nvme(
            self, frontend_nvme_name, backend_nqn_name,
            frontend_nqn_name, dn_listener_conf_str):
        dn_listener_conf = json.loads(dn_listener_conf_str)

        params = {
            "name": frontend_nvme_name,
        }
        ret = self.spdk_client.call("bdev_nvme_get_controllers", params)
        if "result" in ret:
            return

        params = {
            "name": frontend_nvme_name,
            "trtype": dn_listener_conf["trtype"],
            "traddr": dn_listener_conf["traddr"],
            "adrfam": dn_listener_conf["adrfam"],
            "trsvcid": dn_listener_conf["trsvcid"],
            "subnqn": backend_nqn_name,
            "hostnqn": frontend_nqn_name,
        }
        self.spdk_client.call_and_check("bdev_nvme_attach_controller", params)

    def delete_frontend_nvme(self, frontend_nvme_name):
        params = {
            "name": frontend_nvme_name,
        }
        self.spdk_client.call("bdev_nvme_detach_controller", params)

    def get_frontend_nvme_list(self, frontend_nvme_prefix):
        ret = self.spdk_client.call_and_check("bdev_nvme_get_controllers")
        frontend_nvme_list = []
        for nvme_conf in ret["result"]:
            name = nvme_conf["name"]
            if name.startswith(frontend_nvme_prefix):
                frontend_nvme_list.append(name)
        return frontend_nvme_list

    def create_agg_bdev(self,  agg_bdev_name, grp_bdev_list):
        params = {
            "name": agg_bdev_name,
        }
        ret = self.spdk_client.call("bdev_get_bdevs", params)
        if "result" in ret:
            return

        if len(grp_bdev_list) > 1:
            msg = f"Invalid grp len: {agg_bdev_name} {grp_bdev_list}"
            raise Exception(msg)
        params = {
            "name": agg_bdev_name,
            "base_bdev_name": grp_bdev_list[0][1]
        }
        self.spdk_client.call_and_check("bdev_passthru_create", params)

    def delete_agg_bdev(self, agg_bdev_name):
        params = {
            "name": agg_bdev_name,
        }
        self.spdk_client.call("bdev_passthru_delete", params)

    def get_agg_bdev_list(self, agg_bdev_prefix):
        return self._get_bdev_name_list(agg_bdev_prefix)

    def create_da_lvs(self, da_lvs_name, agg_bdev_name):
        params = {
            "lvs_name": da_lvs_name,
        }
        ret = self.spdk_client.call("bdev_lvol_get_lvstores", params)
        if "result" in ret:
            return

        params = {
            "lvs_name": da_lvs_name,
            "bdev_name": agg_bdev_name,
            "clear_method": "unmap",
            "cluster_sz": Constant.CLUSTER_SIZE,
        }
        self.spdk_client.call_and_check("bdev_lvol_create_lvstore", params)

    def delete_da_lvs(self, da_lvs_name):
        params = {
            "lvs_name": da_lvs_name,
        }
        self.spdk_client.call("bdev_lvol_delete_lvstore", params)

    def get_da_lvs_list(self, da_lvs_prefix):
        ret = self.spdk_client.call("bdev_lvol_get_lvstores")
        da_lvs_list = []
        for lvs_conf in ret["result"]:
            lvs_name = lvs_conf["name"]
            if lvs_name.startswith(da_lvs_prefix):
                da_lvs_list.append(lvs_name)
        return da_lvs_list

    def create_exp_primary_nvmf(
            self,  exp_nqn_name, snap_full_name,
            initiator_nqn, secondary_nqn_list, cn_listener_conf):
        nvmf_conf = self.nvmf_dict.get(exp_nqn_name)
        if nvmf_conf is None:
            self._create_nvmf_subsystem(exp_nqn_name)
            self._create_nvmf_ns(exp_nqn_name, snap_full_name)
            self._create_nvmf_listener(
                nqn=exp_nqn_name,
                trtype=cn_listener_conf["trtype"],
                adrfam=cn_listener_conf["adrfam"],
                traddr=cn_listener_conf["traddr"],
                trsvcid=cn_listener_conf["trsvcid"],
            )
            self._create_nvmf_host(exp_nqn_name, initiator_nqn)
            for secondary_nqn in secondary_nqn_list:
                self._create_nvmf_host(exp_nqn_name, secondary_nqn)
        else:
            if len(nvmf_conf["namespaces"]) == 0:
                self._create_nvmf_ns(exp_nqn_name, snap_full_name)
            if len(nvmf_conf["listen_addresses"]) == 0:
                self._create_nvmf_listener(
                    nqn=exp_nqn_name,
                    trtype=cn_listener_conf["trtype"],
                    adrfam=cn_listener_conf["adrfam"],
                    traddr=cn_listener_conf["traddr"],
                    trsvcid=cn_listener_conf["trsvcid"],
                )
            host_nqn_set_1 = set()
            for host_conf in nvmf_conf["hosts"]:
                host_nqn = host_conf["nqn"]
                host_nqn_set_1.add(host_nqn)
            host_nqn_set_2 = set()
            host_nqn_set_2.add(initiator_nqn)
            for secondary_nqn in secondary_nqn_list:
                host_nqn_set_2.add(secondary_nqn)
            for host_nqn in host_nqn_set_1:
                if host_nqn not in host_nqn_set_2:
                    self._delete_nvmf_host(exp_nqn_name, host_nqn)
            for host_nqn in host_nqn_set_2:
                if host_nqn not in host_nqn_set_1:
                    self._create_nvmf_host(exp_nqn_name, host_nqn)

    def create_exp_secondary_nvmf(
            self, exp_nqn_name, secondary_bdev_name,
            initiator_nqn, cn_listener_conf):
        nvmf_conf = self.nvmf_dict.get(exp_nqn_name)
        if nvmf_conf is not None \
           and len(nvmf_conf["namespaces"]) > 0 \
           and nvmf_conf["namespaces"][0]["name"] != secondary_bdev_name:
            params = {
                "nqn": exp_nqn_name,
            }
            self.spdk_client.call_and_check("nvmf_delete_subsystem", params)
            nvmf_conf = None
        if nvmf_conf is None:
            params = {
                "nqn": exp_nqn_name,
            }
            self._create_nvmf_subsystem(exp_nqn_name)
            self._create_nvmf_ns(exp_nqn_name, secondary_bdev_name)
            self._create_nvmf_listener(
                nqn=exp_nqn_name,
                trtype=cn_listener_conf["trtype"],
                adrfam=cn_listener_conf["adrfam"],
                traddr=cn_listener_conf["traddr"],
                trsvcid=cn_listener_conf["trsvcid"],
            )
            self._create_nvmf_host(exp_nqn_name, initiator_nqn)
        else:
            # we have checked the namespace
            # so we do not need to check it again
            if len(nvmf_conf["listen_addresses"]) == 0:
                self._create_nvmf_listener(
                    nqn=exp_nqn_name,
                    trtype=cn_listener_conf["trtype"],
                    adrfam=cn_listener_conf["adrfam"],
                    traddr=cn_listener_conf["traddr"],
                    trsvcid=cn_listener_conf["trsvcid"],
                )
            find = False
            for host_conf in nvmf_conf["hosts"]:
                host_nqn = host_conf["nqn"]
                if host_nqn != initiator_nqn:
                    self._delete_nvmf_host(exp_nqn_name, host_nqn)
                else:
                    find = True
            if find is False:
                self._create_nvmf_host(exp_nqn_name, initiator_nqn)

    def delete_exp_nvmf(self, exp_nqn_name):
        params = {
            "nqn": exp_nqn_name,
        }
        self.spdk_client.call("nvmf_delete_subsystem", params)

    def get_exp_nqn_list(self,  exp_nqn_prefix):
        exp_nqn_list = []
        for nqn in self.nvmf_dict:
            if nqn.startswith(exp_nqn_prefix):
                exp_nqn_list.append(nqn)
        return exp_nqn_list

    def create_secondary_nvme(
            self, secondary_nvme_name, exp_nqn_name,
            secondary_nqn_name, primary_listener_conf_str):
        primary_listener_conf = json.loads(primary_listener_conf_str)

        params = {
            "name": secondary_nvme_name,
        }
        ret = self.spdk_client.call("bdev_nvme_get_controllers", params)
        if "result" in ret:
            return

        params = {
            "name": secondary_nvme_name,
            "trtype": primary_listener_conf["trtype"],
            "traddr": primary_listener_conf["traddr"],
            "adrfam": primary_listener_conf["adrfam"],
            "trsvcid": primary_listener_conf["trsvcid"],
            "subnqn": exp_nqn_name,
            "hostnqn": secondary_nqn_name,
        }
        self.spdk_client.call_and_check("bdev_nvme_attach_controller", params)

    def delete_secondary_nvme(self, secondary_nvme_name):
        params = {
            "name": secondary_nvme_name,
        }
        self.spdk_client.call("bdev_nvme_detach_controller", params)

    def get_secondary_nvme_list(self, secondary_nvme_prefix):
        ret = self.spdk_client.call_and_check("bdev_nvme_get_controllers")
        secondary_nvme_list = []
        for nvme_conf in ret["result"]:
            name = nvme_conf["name"]
            if name.startswith(secondary_nvme_prefix):
                secondary_nvme_list.append(name)
        return secondary_nvme_list

    def create_main_snap(self, da_lvs_name, main_snap_name, da_size):
        params = {
            "name": f"{da_lvs_name}/{main_snap_name}"
        }
        ret = self.spdk_client.call("bdev_get_bdevs", params)
        if "result" in ret:
            return

        params = {
            "lvol_name": main_snap_name,
            "size": da_size,
            "lvs_name": da_lvs_name,
            "clear_method": "unmap",
            "thin_provision": True,
        }
        self.spdk_client.call_and_check("bdev_lvol_create", params)

    def get_da_details(self, da_lvs_name):
        params = {
            "lvs_name": da_lvs_name,
        }
        ret = self.spdk_client.call_and_check(
            "bdev_lvol_get_lvstores", params)
        details = {
            "lvs_conf": ret["result"][0],
        }
        details_str = json.dumps(details)
        return details_str
