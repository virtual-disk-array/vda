#!/bin/bash
set -x
set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

ROOT_DIR=$CURR_DIR/../..
BIN_DIR=$ROOT_DIR/_out/linux_amd64
FIO_JOBFILE=$CURR_DIR/basic-verify.fio

NVMF_NQN="nqn.2016-06.io.vda:exp-da0-exp0a"
HOST_NQN="nqn.2016-06.io.spdk:host0"
NVMF_MODEL_NUMBER="VDA_CONTROLLER"
NVMF_SERIAL_NUMBER="SPDK00000000000001"
NVMF_PORT=4430
NVMF_DEV_PATH="/dev/disk/by-id/nvme-${NVMF_MODEL_NUMBER}_${NVMF_SERIAL_NUMBER}"

sudo rm -rf $WORK_DIR
mkdir -p $WORK_DIR

echo "launch vda_dataplane"
sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/vda_dp.sock > $WORK_DIR/vda_dp.log 2>&1 &"
sleep 1
sudo chown $(id -u):$(id -g) $WORK_DIR/vda_dp.sock
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_transport --trtype TCP

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_malloc_create --name vd0 128 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_malloc_create --name vd1 128 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_raid_create --name raid0a --raid-level raid0 --base-bdevs "vd0 vd1" --strip-size-kb 4
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_passthru_create --name grp0 --base-bdev-name raid0a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_raid_create --name agg0 --raid-level concat --base-bdevs "grp0" --strip-size-kb 4
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_lvol_create_lvstore --cluster-sz 4194304 --clear-method unmap --md-pages-per-cluster-ratio 300 agg0 lvs0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_lvol_create --lvs-name lvs0 --thin-provision --clear-method unmap lv0 8192
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_susres_create --name susres0 --base-bdev-name lvs0/lv0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_subsystem --serial-number ${NVMF_SERIAL_NUMBER} --model-number ${NVMF_MODEL_NUMBER} ${NVMF_NQN}
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_ns ${NVMF_NQN} susres0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_listener --trtype tcp --traddr 127.0.0.1 --adrfam ipv4 --trsvcid ${NVMF_PORT} ${NVMF_NQN}
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_host ${NVMF_NQN} ${HOST_NQN}

total_data_clusters=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_lvol_get_lvstores --lvs-name lvs0 | jq -rM '.[0].total_data_clusters')
if [ ${total_data_clusters} -ne 63 ]; then
    echo "incorrect total_data_clusters before extend: ${total_data_clusters}"
    exit 1
fi

sudo nvme connect -t tcp -n ${NVMF_NQN} -a 127.0.0.1 -s ${NVMF_PORT} --hostnqn ${HOST_NQN}
wait_for_nvme ${NVMF_DEV_PATH}
sudo fio --filename=$NVMF_DEV_PATH --runtime=10 --aux-path $WORK_DIR $FIO_JOBFILE &
sleep 3

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_malloc_create --name vd2 128 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_malloc_create --name vd3 128 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_raid_create --name raid0b --raid-level raid0 --base-bdevs "vd2 vd3" --strip-size-kb 4
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_passthru_create --name grp1 --base-bdev-name raid0b

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_susres_suspend --name susres0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_raid_delete agg0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_raid_create --name agg0 --raid-level concat --base-bdevs "grp0 grp1" --strip-size-kb 4
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_examine --name agg0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_lvol_grow_lvstore --lvs-name lvs0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_susres_resume --name susres0 --base-bdev-name lvs0/lv0

total_data_clusters=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_lvol_get_lvstores --lvs-name lvs0 | jq -rM '.[0].total_data_clusters')
if [ ${total_data_clusters} -ne 127 ]; then
    echo "incorrect total_data_clusters after extend: ${total_data_clusters}"
    exit 1
fi

wait

sudo nvme disconnect -n nqn.2016-06.io.vda:exp-da0-exp0a

cleanup

echo "susres_test succeed"
