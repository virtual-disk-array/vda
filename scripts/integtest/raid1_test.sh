#!/bin/bash
set -x
set -e

ulimit -c unlimited

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

ROOT_DIR=$CURR_DIR/../..
BIN_DIR=$ROOT_DIR/_out/linux_amd64
FIO_JOBFILE=$CURR_DIR/basic-verify.fio
RAID1_NAME="raid1a"
NVMF_NQN="nqn.2016-06.io.vda:exp-da0-exp0a"
HOST_NQN="nqn.2016-06.io.spdk:host0"
NVMF_MODEL_NUMBER="VDA_CONTROLLER"
NVMF_SERIAL_NUMBER="SPDK00000000000001"
NVMF_PORT=4430
NVMF_DEV_PATH="/dev/disk/by-id/nvme-${NVMF_MODEL_NUMBER}_${NVMF_SERIAL_NUMBER}"

function wait_for_raid1() {
    max_retry=$1
    total_strip=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.total_strip')
    retry_cnt=0
    while true; do
        synced_strip=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.synced_strip')
        echo "raid1 sync ${synced_strip}/${total_strip}"
        if [ $synced_strip -eq $total_strip ]; then
            resync_io_cnt=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.resync_io_cnt')
            if [ $resync_io_cnt -eq 0 ]; then
                break
            else
                echo "resync_io_cnt: $resync_io_cnt"
            fi
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "raid1 sync timeout"
            exit 1
        fi
        sleep 1
        ((retry_cnt=retry_cnt+1))
    done
}

function wait_for_nvme() {
    max_retry=10
    retry_cnt=0
    while true; do
        if [ -e ${NVMF_DEV_PATH} ]; then
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

MD5_4K_ZERO="620f0b67a91f7f74151bc5be745b7110"
function verify_disk_file() {
    md5_0=$(md5sum $WORK_DIR/disk0.img | awk '{print $1}')
    md5_1=$(md5sum $WORK_DIR/disk1.img | awk '{print $1}')
    if [ "${md5_0}" == "${md5_1}" ]; then
        echo "raid1 disk same"
    else
        echo "raid1 disk different"
        exit 1
    fi
    dd if=$WORK_DIR/disk0.img of=$WORK_DIR/bitmap0.img bs=4k count=1 skip=1
    # dd if=$WORK_DIR/disk1.img of=$WORK_DIR/bitmap1.img bs=4k count=1 skip=1
    md5_bitmap=$(md5sum $WORK_DIR/bitmap0.img | awk '{print $1}')
    if [ "${md5_bitmap}" == "${MD5_4K_ZERO}" ]; then
        echo "bitmap is all zero"
    else
        echo "bitmap is not all zero"
        exit 1
    fi
}

function check_raid1_counter() {
    bdev_name=$1
    target=$2
    counter=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_dump --bdev-name $bdev_name | jq '.counter')
    if [ $counter -ne $target ]; then
        echo"counter incorrect: $bdev_name $counter $target"
        exit 1
    fi
}

function retry() {
    cmd=$@
    max_retry=600
    retry_cnt=0
    while ! ${cmd}
    do
        if [ $retry_cnt -ge $max_retry ]; then
            echo "failed"
            exit 1
        fi
        sleep 1
        ((retry_cnt=retry_cnt+1))
    done
}

function test_sync() {
    echo "test sync"
    cp $WORK_DIR/random.img $WORK_DIR/disk0.img
    dd if=/dev/zero of=$WORK_DIR/disk1.img bs=1M count=1024
    sleep 1
    retry sudo losetup $LOOP_NAME0 $WORK_DIR/disk0.img --sector-size 4096
    retry sudo losetup $LOOP_NAME1 $WORK_DIR/disk1.img --sector-size 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create $LOOP_NAME0 aio0 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create $LOOP_NAME1 aio1 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name $RAID1_NAME --bdev0-name aio0 --bdev1-name aio1
    wait_for_raid1 10
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_delete --raid1-name $RAID1_NAME
    check_raid1_counter aio0 3
    check_raid1_counter aio1 3
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio0
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio1
    sudo losetup --detach $LOOP_NAME0
    sudo losetup --detach $LOOP_NAME1
    verify_disk_file
    rm $WORK_DIR/disk0.img
    rm $WORK_DIR/disk1.img
}

function test_normal_rw() {
    echo "test normal rw"
    cp $WORK_DIR/random.img $WORK_DIR/disk0.img
    dd if=/dev/zero of=$WORK_DIR/disk1.img bs=1M count=1024
    sleep 1
    retry sudo losetup $LOOP_NAME0 $WORK_DIR/disk0.img --sector-size 4096
    retry sudo losetup $LOOP_NAME1 $WORK_DIR/disk1.img --sector-size 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create $LOOP_NAME0 aio0 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create $LOOP_NAME1 aio1 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name $RAID1_NAME --bdev0-name aio0 --bdev1-name aio1
    wait_for_raid1 10
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_subsystem --serial-number $NVMF_SERIAL_NUMBER --model-number VDA_CONTROLLER $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_ns $NVMF_NQN $RAID1_NAME
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_listener --trtype tcp --traddr 127.0.0.1 --adrfam ipv4 --trsvcid $NVMF_PORT $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_host $NVMF_NQN $HOST_NQN
    sudo nvme connect -t tcp -n $NVMF_NQN -a 127.0.0.1 -s $NVMF_PORT --hostnqn $HOST_NQN
    wait_for_nvme 5
    sudo fio --filename=$NVMF_DEV_PATH --runtime=60 --aux-path $WORK_DIR $FIO_JOBFILE
    sudo nvme disconnect -n $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_delete_subsystem $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_delete --raid1-name $RAID1_NAME
    check_raid1_counter aio0 3
    check_raid1_counter aio1 3
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio0
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio1
    sudo losetup --detach $LOOP_NAME0
    sudo losetup --detach $LOOP_NAME1
    verify_disk_file
}

function test_rw_during_sync() {
    echo "test rw during sync"
    rm -f $WORK_DIR/disk0.img
    for i in $(seq 4); do cat $WORK_DIR/random.img >> $WORK_DIR/disk0.img; done
    dd if=/dev/zero of=$WORK_DIR/disk1.img bs=1M count=4096
    sleep 1
    retry sudo losetup $LOOP_NAME0 $WORK_DIR/disk0.img --sector-size 4096
    retry sudo losetup $LOOP_NAME1 $WORK_DIR/disk1.img --sector-size 4096
    sudo dmsetup create ${DELAY_NAME0} --table "0 $(sudo blockdev --getsz ${LOOP_NAME0}) delay ${LOOP_NAME0} 0 100"
    sudo dmsetup create ${DELAY_NAME1} --table "0 $(sudo blockdev --getsz ${LOOP_NAME1}) delay ${LOOP_NAME1} 0 100"
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create "/dev/mapper/$DELAY_NAME0" aio0 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create "/dev/mapper/$DELAY_NAME1" aio1 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name $RAID1_NAME --bdev0-name aio0 --bdev1-name aio1
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_subsystem --serial-number $NVMF_SERIAL_NUMBER --model-number VDA_CONTROLLER $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_ns $NVMF_NQN $RAID1_NAME
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_listener --trtype tcp --traddr 127.0.0.1 --adrfam ipv4 --trsvcid $NVMF_PORT $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_host $NVMF_NQN $HOST_NQN
    sudo nvme connect -t tcp -n $NVMF_NQN -a 127.0.0.1 -s $NVMF_PORT --hostnqn $HOST_NQN
    wait_for_nvme 100
    total_strip=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.total_strip')
    synced_strip=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.synced_strip')
    echo "raid1 sync ${synced_strip}/${total_strip}"
    sudo fio --filename=$NVMF_DEV_PATH --runtime=60 --aux-path $WORK_DIR $FIO_JOBFILE
    wait_for_raid1 600
    sudo nvme disconnect -n $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_delete_subsystem $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_delete --raid1-name $RAID1_NAME
    check_raid1_counter aio0 3
    check_raid1_counter aio1 3
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio0
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio1
    sleep 1
    retry sudo dmsetup remove $DELAY_NAME0
    retry sudo dmsetup remove $DELAY_NAME1
    sudo losetup --detach $LOOP_NAME0
    sudo losetup --detach $LOOP_NAME1
    verify_disk_file
}

function test_secondary_fail_and_add_old_secondary_after_sync() {
    echo "test secondary fail and add old secondary after sync"
    dd if=/dev/zero of=$WORK_DIR/disk0.img bs=1M count=1024
    dd if=/dev/zero of=$WORK_DIR/disk1.img bs=1M count=1024
    sleep 1
    retry sudo losetup $LOOP_NAME0 $WORK_DIR/disk0.img --sector-size 4096
    retry sudo losetup $LOOP_NAME1 $WORK_DIR/disk1.img --sector-size 4096
    # sudo dmsetup create ${FLAKEY_NAME0} --table "0 $(sudo blockdev --getsz ${LOOP_NAME0}) flakey ${LOOP_NAME0} 0 10 3600"
    sudo dmsetup create ${FLAKEY_NAME1} --table "0 $(sudo blockdev --getsz ${LOOP_NAME1}) flakey ${LOOP_NAME0} 0 10 3600"
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create "$LOOP_NAME0" aio0 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create "/dev/mapper/$FLAKEY_NAME1" aio1 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name $RAID1_NAME --bdev0-name aio0 --bdev1-name aio1
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_subsystem --serial-number $NVMF_SERIAL_NUMBER --model-number VDA_CONTROLLER $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_ns $NVMF_NQN $RAID1_NAME
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_listener --trtype tcp --traddr 127.0.0.1 --adrfam ipv4 --trsvcid $NVMF_PORT $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_subsystem_add_host $NVMF_NQN $HOST_NQN
    sudo nvme connect -t tcp -n $NVMF_NQN -a 127.0.0.1 -s $NVMF_PORT --hostnqn $HOST_NQN
    wait_for_nvme 5
    wait_for_raid1 5
    sudo fio --filename=$NVMF_DEV_PATH --runtime=60 --aux-path $WORK_DIR $FIO_JOBFILE
    sudo nvme disconnect -n $NVMF_NQN
    bdev0_online=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.bdev0_online')
    bdev1_online=$($BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_get_bdevs --name $RAID1_NAME | jq '.[0].driver_specific.raid1.bdev1_online')
    if [ $bdev0_online != "true" ]; then
        echo "bdev0 status incorrect: $bdev0_online"
        exit 1
    fi
    if [ $bdev1_online != "false" ]; then
        echo "bdev1 status incorrect: $bdev1_online"
        exit 1
    fi
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_delete_subsystem $NVMF_NQN
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_delete --raid1-name $RAID1_NAME
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio1
    retry sudo dmsetup remove $FLAKEY_NAME1
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_create "$LOOP_NAME1" aio1 4096
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_create --raid1-name $RAID1_NAME --bdev0-name aio0 --bdev1-name aio1
    wait_for_raid1 600
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock --plugin vda_rpc_plugin bdev_raid1_delete --raid1-name $RAID1_NAME
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio0
    $BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock bdev_aio_delete aio1
    sudo losetup --detach $LOOP_NAME0
    sudo losetup --detach $LOOP_NAME1
    verify_disk_file
}

sudo rm -rf $WORK_DIR
mkdir -p $WORK_DIR

echo "launch vda_dataplane"
sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/vda_dp.sock > $WORK_DIR/vda_dp.log 2>&1 &"
sleep 1
sudo chown $(id -u):$(id -g) $WORK_DIR/vda_dp.sock
$BIN_DIR/spdk/scripts/rpc.py -s $WORK_DIR/vda_dp.sock nvmf_create_transport --trtype TCP

echo "creating random disk"
dd if=/dev/random of=$WORK_DIR/random.img bs=1M count=1024

test_sync
test_normal_rw
test_rw_during_sync
test_secondary_fail_and_add_old_secondary_after_sync
cleanup

echo "succeed"
