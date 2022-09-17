#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

ROOT_DIR=$CURR_DIR/../..
BIN_DIR=$ROOT_DIR/_out/linux_amd64
FIO_JOBFILE=$CURR_DIR/basic-verify.fio

sudo rm -rf $WORK_DIR
mkdir -p $WORK_DIR

echo "test sync"
sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/vda_dp.sock > $WORK_DIR/vda_dp.log 2>&1 &"

sleep 5

sudo chown $(id -u):$(id -g) $WORK_DIR/vda_dp.sock

echo "creating random disk"
dd if=/dev/random of=$WORK_DIR/disk0.img bs=1M count=1024
echo "creating zero disk"
dd if=/dev/zero of=$WORK_DIR/disk1.img bs=1M count=1024

echo "creating loop dev"
sudo losetup /dev/loop240 $WORK_DIR/disk0.img --sector-size 4096
sudo losetup /dev/loop241 $WORK_DIR/disk1.img --sector-size 4096

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create /dev/loop240 aio0 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create /dev/loop241 aio1 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name raid1a --bdev0-name aio0 --bdev1-name aio1

function wait_for_raid1() {
    total_strip=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name raid1a | jq '.[0].driver_specific.raid1.total_strip')
    max_retry=5
    retry_cnt=0
    while true; do
        synced_strip=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name raid1a | jq '.[0].driver_specific.raid1.synced_strip')
        echo "raid1 sync ${synced_strip}/${total_strip}"
        if [ $synced_strip -eq $total_strip ]; then
            break
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "raid1 sync timeout"
            exit 1
        fi
        sleep 5
        ((retry_cnt=retry_cnt+1))
    done
}

wait_for_raid1

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_delete --raid1-name raid1a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio1
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock spdk_kill_instance SIGTERM

sudo losetup --detach /dev/loop240
sudo losetup --detach /dev/loop241

md5_0=$(md5sum $WORK_DIR/disk0.img | awk '{print $1}')
md5_1=$(md5sum $WORK_DIR/disk1.img | awk '{print $1}')

if [ "${md5_0}" == "${md5_1}" ]; then
    echo "raid1 sync correct"
else
    echo "raid1 sync error"
    exit 1
fi

echo "test normal rw"

sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/vda_dp.sock > $WORK_DIR/vda_dp.log 2>&1 &"

sleep 5

sudo chown $(id -u):$(id -g) $WORK_DIR/vda_dp.sock

echo "creating random disk"
dd if=/dev/random of=$WORK_DIR/disk0.img bs=1M count=1024
echo "creating zero disk"
dd if=/dev/zero of=$WORK_DIR/disk1.img bs=1M count=1024

echo "creating loop dev"
sudo losetup /dev/loop240 $WORK_DIR/disk0.img --sector-size 4096
sudo losetup /dev/loop241 $WORK_DIR/disk1.img --sector-size 4096

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create /dev/loop240 aio0 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create /dev/loop241 aio1 4096
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name raid1a --bdev0-name aio0 --bdev1-name aio1

wait_for_raid1

$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_subsystem --serial-number SPDK00000000000001 --model-number VDA_CONTROLLER nqn.2016-06.io.vda:exp-da0-exp0a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_ns nqn.2016-06.io.vda:exp-da0-exp0a raid1a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_listener --trtype tcp --traddr 127.0.0.1 --adrfam ipv4 --trsvcid 4430 nqn.2016-06.io.vda:exp-da0-exp0a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_host nqn.2016-06.io.vda:exp-da0-exp0a nqn.2016-06.io.spdk:host0
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_subsystem --serial-number SPDK00000000000001 --model-number VDA_CONTROLLER nqn.2016-06.io.vda:exp-da0-exp0a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_ns nqn.2016-06.io.vda:exp-da0-exp0a raid1a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_listener --trtype tcp --traddr 127.0.0.1 --adrfam ipv4 --trsvcid 4430 nqn.2016-06.io.vda:exp-da0-exp0a
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_host nqn.2016-06.io.vda:exp-da0-exp0a nqn.2016-06.io.spdk:host0

sudo nvme connect -t tcp -n nqn.2016-06.io.vda:exp-da0-exp0a -a 127.0.0.1 -s 4430 --hostnqn nqn.2016-06.io.spdk:host0

DEV_PATH="/dev/disk/by-id/nvme-VDA_CONTROLLER_SPDK00000000000001"

function wait_for_nvme() {
    dev_path=$1
    max_retry=10
    retry_cnt=0
    while true; do
        if [ -e $dev_path ]; then
            break
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "nvmf check timeout: $dev_path"
            exit 1
        fi
        sleep 1
        ((retry_cnt=retry_cnt+1))
    done
}

wait_for_nvme $DEV_PATH

exit 0

cleanup

echo "succeed"
