#!/bin/bash

function cleanup() {
    set +e
    echo "nvme disconnect-all"
    sudo nvme disconnect-all
    echo "stop minikube"
    minikube status > /dev/null && minikube stop
    echo "delete minikube"
    minikube delete --all
    echo "stop vda_portal"
    ps -f -C vda_portal > /dev/null && killall vda_portal
    echo "stop vda_monitor"
    ps -f -C vda_monitor > /dev/null && killall vda_monitor
    echo "stop vda_dn_agent"
    ps -f -C vda_dn_agent > /dev/null && killall vda_dn_agent
    echo "stop vda_cn_agent"
    ps -f -C vda_cn_agent > /dev/null && killall vda_cn_agent
    echo "stop etcd"
    ps -f -C etcd > /dev/null && killall etcd
    echo "stop spdk"
    ps -f -C reactor_0 > /dev/null && sudo killall reactor_0
    echo "stop flakey devices"
    sudo dmsetup status $FLAKEY_NAME0 > /dev/null 2>&1 && retry sudo dmsetup remove $FLAKEY_NAME0
    sudo dmsetup status $FLAKEY_NAME1 > /dev/null 2>&1 && retry sudo dmsetup remove $FLAKEY_NAME1
    echo "stop delay devices"
    sudo dmsetup status $DELAY_NAME0 > /dev/null 2>&1 && retry sudo dmsetup remove $DELAY_NAME0
    sudo dmsetup status $DELAY_NAME1 > /dev/null 2>&1 && retry sudo dmsetup remove $DELAY_NAME1
    echo "stop loop devices"
    losetup $LOOP_NAME0 > /dev/null 2>&1 && retry sudo losetup --detach $LOOP_NAME0
    losetup $LOOP_NAME1 > /dev/null 2>&1 && retry sudo losetup --detach $LOOP_NAME1
    set -e
}

function force_cleanup() {
    set +e
    sudo nvme disconnect-all
    ps -f -C vda_portal > /dev/null && killall -9 vda_portal
    ps -f -C vda_monitor > /dev/null && killall -9 vda_monitor
    ps -f -C vda_dn_agent > /dev/null && killall -9 vda_dn_agent
    ps -f -C vda_cn_agent > /dev/null && killall -9 vda_cn_agent
    ps -f -C etcd > /dev/null && killall -9 etcd
    ps -f -C reactor_0 > /dev/null && sudo killall -9 reactor_0
    sudo dmsetup status $FLAKEY_NAME0 > /dev/null 2>&1 && retry sudo dmsetup remove $FLAKEY_NAME0
    sudo dmsetup status $FLAKEY_NAME1 > /dev/null 2>&1 && retry sudo dmsetup remove $FLAKEY_NAME1
    sudo dmsetup status $DELAY_NAME0 > /dev/null 2>&1 && retry sudo dmsetup remove $DELAY_NAME0
    sudo dmsetup status $DELAY_NAME1 > /dev/null 2>&1 && retry sudo dmsetup remove $DELAY_NAME1
    losetup $LOOP_NAME0 > /dev/null 2>&1 && retry sudo losetup --detach $LOOP_NAME0
    losetup $LOOP_NAME1 > /dev/null 2>&1 && retry sudo losetup --detach $LOOP_NAME1
    set -e
}

function cleanup_check() {
    set +e
    minikube status > /dev/null && echo "minikube is still runing"
    ps -f -C vda_portal > /dev/null && echo "vda_portal is still running"
    ps -f -C vda_monitor > /dev/null && echo "vda_monitor is still running"
    ps -f -C vda_dn_agent > /dev/null && echo "vda_dn_agent is still running"
    ps -f -C vda_cn_agent > /dev/null && echo "vda_cn_agent is still running"
    ps -f -C etcd > /dev/null && echo "etcd is still running"
    ps -f -C reactor_0 > /dev/null && echo "reactor_0 is still running"
    sudo dmsetup status delay0 > /dev/null 2>&1 && echo "delay0 still exists"
    sudo dmsetup status delay1 > /dev/null 2>&1 && echo "delay1 still exists"
    losetup /dev/loop240 > /dev/null 2>&1 && echo "loop240 still exist"
    losetup /dev/loop241 > /dev/null 2>&1 && echo "loop241 still exist"
    set -e
}

function umount_dir() {
    dir=$1
    mountpoint $dir && sudo umount $dir
}

function cntlr_verify() {
    da_name=$1
    da_rsp=$($BIN_DIR/vda_cli da get --da-name $da_name)
    cntlr_cnt=$(echo $da_rsp | jq ".disk_array.cntlr_list | length")
    if [ "$cntlr_cnt" == "" ]; then
        echo "cntlr_cnt is empty, da_name: $da_name"
        echo "$da_rsp"
        return
    fi
    if [ $cntlr_cnt -eq 0 ]; then
        echo "cntlr_cnt is 0, da_name: $da_name"
        echo "$da_rsp"
        return
    fi
    for i in $(seq 0 $[cntlr_cnt - 1]); do
        is_err=$(echo $da_rsp | jq -r ".disk_array.cntlr_list[$i].err_info.is_err")
        if [ "$is_err" != "null" ]; then
            echo "cntlr is err, da_name: $da_name cntlr: $i"
            echo "$da_rsp"
            return
        fi
        timestamp=$(echo $da_rsp | jq -r ".disk_array.cntlr_list[$i].err_info.timestamp")
        if [ "$timestamp" == "null" ]; then
            echo "cntlr timestmap is null, da_name: $da_name cntlr: $i"
            echo "$da_rsp"
            return
        fi
    done
}

function grp_verify() {
    da_name=$1
    da_rsp=$($BIN_DIR/vda_cli da get --da-name $da_name)
    grp_cnt=$(echo $da_rsp | jq -r ".disk_array.grp_list | length")
    if [ "$grp_cnt" == "" ]; then
        echo "grp_cnt is empty"
        echo "$da_rsp"
        return
    fi
    if [ $grp_cnt -eq 0 ]; then
        echo "grp_cnt is 0"
        echo "$da_rsp"
        return
    fi
    for i in $(seq 0 $[grp_cnt - 1]); do
        is_err=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].err_info.is_err")
        if [ "$is_err" != "null" ]; then
            echo "grp is err, grp: $i"
            echo "$da_rsp"
            return
        fi
        timestmap=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].err_info.timestamp")
        if [ "$timestmap" == "null" ]; then
            echo "grp timestamp is null, grp: $i"
            echo "$da_rsp"
            return
        fi
        vd_cnt=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].vd_list | length")
        if [ "$vd_cnt" == "" ]; then
            echo "vd_cnt is empty, grp: $i"
            echo "$da_rsp"
            return
        fi
        if [ $vd_cnt -eq 0 ]; then
            echo "vd_cnt is 0, grp: $i"
            echo "$da_rsp"
            return
        fi
        for j in $(seq 0 $[vd_cnt - 1]); do
            is_err=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].vd_list[$j].be_err_info.is_err")
            if [ "$is_err" != "null" ]; then
                echo "vd_be is err,  grp: $i vd: $j"
                echo "$da_rsp"
                return
            fi
            timestamp=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].vd_list[$j].be_err_info.timestamp")
            if [ "$timestamp" == "null" ]; then
                echo "vd_be timestamp is null, grp: $i vd: $j"
                echo "$da_rsp"
                return
            fi
            is_err=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].vd_list[$j].fe_err_info.is_err")
            if [ "$is_err" != "null" ]; then
                echo "vd_fe is err,  grp: $i vd: $j"
                echo "$da_rsp"
                return
            fi
            timestamp=$(echo $da_rsp | jq -r ".disk_array.grp_list[$i].vd_list[$j].fe_err_info.timestamp")
            if [ "$timestamp" == "null" ]; then
                echo "vd_fe timestamp is null, grp: $i vd: $j"
                echo "$da_rsp"
                return
            fi
        done
    done
}

function da_verify() {
    da_name=$1
    ret=$(cntlr_verify $da_name)
    if [ "$ret" != "" ]; then
        echo "$ret"
        return
    fi
    ret=$(grp_verify $da_name)
    if [ "$ret" != "" ]; then
        echo "$ret"
        return
    fi
}

function vda_api_verify() {
    cli_rsp=$1
    echo $cli_rsp | jq -rM '.'
    reply_msg=$(echo $cli_rsp | jq -rM '.reply_info.reply_msg')
    if [ "$reply_msg" != "succeed" ]; then
        echo "vda api err"
        exit 1
    fi
}

function exp_verify() {
    da_name=$1
    exp_name=$2
    exp_rsp=$($BIN_DIR/vda_cli exp get --da-name $da_name --exp-name $exp_name)
    exp_info_cnt=$(echo $exp_rsp | jq ".exporter.exp_info_list | length")
    if [ "$exp_info_cnt" == "" ]; then
        echo "exp_infno_cnt is empty, da_name: $da_name exp_name: $exp_name"
        echo "$exp_rsp"
        return
    fi
    if [ $exp_info_cnt -eq 0 ]; then
        echo "exp_info_cnt is 0, da_name: $da_name exp_name: $exp_name"
        echo "$exp_rsp"
        return
    fi
    for i in $(seq 0 $[exp_info_cnt - 1]); do
        is_err=$(echo $exp_rsp | jq ".exporter.exp_info_list[$i].err_info.is_err")
        if [ "$is_err" != "null" ]; then
            echo "exp_info is err, da_name: $da_name exp_name: $exp_name exp_info: $i"
            echo "$exp_rsp"
            return
        fi
        timestamp=$(echo $exp_rsp | jq ".exporter.exp_info_list[$i].err_info.timestamp")
        if [ "$timestamp" == "null" ]; then
            echo "exp_info timestamp is null, da_name: $da_name exp_name: $exp_name exp_info: $i"
            echo "$exp_rsp"
            return
        fi
    done
}

function nvmf_connect() {
    da_name=$1
    exp_name=$2
    exp_rsp=$($BIN_DIR/vda_cli exp get --da-name $da_name --exp-name $exp_name)
    host_nqn=$3
    exp_info_cnt=$(echo $exp_rsp | jq ".exporter.exp_info_list | length")
    if [ "$exp_info_cnt" == "" ]; then
        echo "exp_info_cnt is empty, da_name: $da_name exp_name: $exp_name"
        echo $exp_rsp | jq -rM '.'
        exit 1
    fi
    if [ $exp_info_cnt -eq 0 ]; then
        echo "exp_info_cnt is 0, da_name: $da_name exp_name: $exp_name"
        echo $exp_rsp | jq -rM '.'
        exit 1
    fi
    for i in $(seq $[exp_info_cnt - 1] -1 0); do
        tr_svc_id=$(echo $exp_rsp | jq -r ".exporter.exp_info_list[$i].nvmf_listener.tr_svc_id")
        sudo nvme connect -t tcp -n nqn.2016-06.io.vda:exp-$da_name-$exp_name -a 127.0.0.1 -s $tr_svc_id --hostnqn $host_nqn
        serial_number=$(echo $exp_rsp | jq -r ".exporter.serial_number")
        dev_path="/dev/disk/by-id/nvme-VDA_CONTROLLER_$serial_number"
        max_retry=10
        retry_cnt=0
        while true; do
            if [ -e $dev_path ]; then
                break
            fi
            if [ $retry_cnt -ge $max_retry ]; then
                echo "nvmf check timeout: $da_name $exp_name $dev_path"
                exit 1
            fi
            sleep 1
            ((retry_cnt=retry_cnt+1))
        done
    done
}

function nvmf_format() {
    da_name=$1
    exp_name=$2
    exp_rsp=$($BIN_DIR/vda_cli exp get --da-name $da_name --exp-name $exp_name)
    echo "nvmf_format"
    echo $exp_rsp | jq -rM '.'
    serial_number=$(echo $exp_rsp | jq -r ".exporter.serial_number")
    dev_path="/dev/disk/by-id/nvme-VDA_CONTROLLER_$serial_number"
    # sudo mkfs.ext4 $dev_path
    sudo mkfs.xfs $dev_path
}

function nvmf_mount() {
    da_name=$1
    exp_name=$2
    dir=$3
    exp_rsp=$($BIN_DIR/vda_cli exp get --da-name $da_name --exp-name $exp_name)
    echo "nvmf_mount"
    echo $exp_rsp | jq -rM '.'
    serial_number=$(echo $exp_rsp | jq -r ".exporter.serial_number")
    dev_path="/dev/disk/by-id/nvme-VDA_CONTROLLER_$serial_number"
    mkdir -p $dir
    echo "dev_path: $dev_path"
    echo "dir: $dir"
    sudo mount $dev_path $dir
}

function retry() {
    cmd=$@
    max_retry=600
    retry_cnt=0
    set +e
    while true; do
        ret=$($cmd 2>&1)
        if [ "$ret" == "" ]; then
            set -e
            return
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "failed"
            exit 1
        fi
        sleep 1
        ((retry_cnt=retry_cnt+1))
    done
}

function wait_for_nvme() {
    nvmf_dev_path=$1
    max_retry=10
    retry_cnt=0
    while true; do
        if [ -e ${nvmf_dev_path} ]; then
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
