#!/bin/bash

set -e

curr_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $curr_dir/conf.sh
source $curr_dir/utils.sh

rm -rf $work_dir
mkdir -p $work_dir

echo "launch spdk"

cd $spdk_dir
sudo ./build/bin/spdk_tgt --rpc-socket $work_dir/dn0.sock --wait-for-rpc > $work_dir/dn0.log 2>&1 &
sudo ./build/bin/spdk_tgt --rpc-socket $work_dir/dn1.sock --wait-for-rpc > $work_dir/dn1.log 2>&1 &
sudo ./build/bin/spdk_tgt --rpc-socket $work_dir/cn0.sock --wait-for-rpc > $work_dir/cn0.log 2>&1 &
sudo ./build/bin/spdk_tgt --rpc-socket $work_dir/cn1.sock --wait-for-rpc > $work_dir/cn1.log 2>&1 &

sleep 1

sudo ./scripts/rpc.py -s $work_dir/dn0.sock bdev_set_options -d
sudo ./scripts/rpc.py -s $work_dir/dn0.sock framework_start_init
sudo ./scripts/rpc.py -s $work_dir/dn0.sock framework_wait_init
sudo chmod 777 $work_dir/dn0.sock

sudo ./scripts/rpc.py -s $work_dir/dn1.sock bdev_set_options -d
sudo ./scripts/rpc.py -s $work_dir/dn1.sock framework_start_init
sudo ./scripts/rpc.py -s $work_dir/dn1.sock framework_wait_init
sudo chmod 777 $work_dir/dn1.sock

sudo ./scripts/rpc.py -s $work_dir/cn0.sock bdev_set_options -d
sudo ./scripts/rpc.py -s $work_dir/cn0.sock framework_start_init
sudo ./scripts/rpc.py -s $work_dir/cn0.sock framework_wait_init
sudo chmod 777 $work_dir/cn0.sock

sudo ./scripts/rpc.py -s $work_dir/cn1.sock bdev_set_options -d
sudo ./scripts/rpc.py -s $work_dir/cn1.sock framework_start_init
sudo ./scripts/rpc.py -s $work_dir/cn1.sock framework_wait_init
sudo chmod 777 $work_dir/cn1.sock

echo "launch etcd"
cd $etcd_dir
./etcd --listen-client-urls "http://localhost:$etcd_port" \
       --advertise-client-urls "http://localhost:$etcd_port" \
       --listen-peer-urls "http://localhost:$etcd_peer_port" \
       --name etcd0 --data-dir $work_dir/etcd0.data \
       > $work_dir/etcd0.log 2>&1 &

echo "launch vda services"

$vda_dir/vda_dn_agent --network tcp --address '127.0.0.1:9720' \
               --sock-path $work_dir/dn0.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4420"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/dn_agent_0.log 2>&1 &

$vda_dir/vda_dn_agent --network tcp --address '127.0.0.1:9721' \
               --sock-path $work_dir/dn1.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4421"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/dn_agent_1.log 2>&1 &

$vda_dir/vda_cn_agent --network tcp --address '127.0.0.1:9820' \
               --sock-path $work_dir/cn0.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/cn_agent_0.log 2>&1 &

$vda_dir/vda_cn_agent --network tcp --address '127.0.0.1:9821' \
               --sock-path $work_dir/cn1.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4431"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/cn_agent_1.log 2>&1 &

$vda_dir/vda_portal --portal-address '127.0.0.1:9520' --portal-network tcp \
             --etcd-endpoints localhost:$etcd_port \
             > $work_dir/portal_0.log 2>&1 &

$vda_dir/vda_monitor --etcd-endpoints localhost:$etcd_port \
              > $work_dir/monitor_0.log 2>&1 &


echo "prepare pd file"
dd if=/dev/zero of=$work_dir/pd0.img bs=1M count=512
dd if=/dev/zero of=$work_dir/pd1.img bs=1M count=512

echo "create vda resources"
$vda_dir/vda_cli dn create --sock-addr localhost:9720 --tr-svc-id 4420
$vda_dir/vda_cli pd create --sock-addr localhost:9720 --pd-name pd0 \
                 --bdev-type-key aio --bdev-type-value $work_dir/pd0.img
$vda_dir/vda_cli dn create --sock-addr localhost:9721 --tr-svc-id 4421
$vda_dir/vda_cli pd create --sock-addr localhost:9721 --pd-name pd1 \
                 --bdev-type-key aio --bdev-type-value $work_dir/pd1.img
$vda_dir/vda_cli cn create --sock-addr localhost:9820 --tr-svc-id 4430
$vda_dir/vda_cli cn create --sock-addr localhost:9821 --tr-svc-id 4431

$vda_dir/vda_cli da create --da-name da0 --size-mb 64 --physical-size-mb 64 \
                 --cntlr-cnt 1 --strip-cnt 1 --strip-size-kb 64

$vda_dir/vda_cli da create --da-name da1 --size-mb 64 --physical-size-mb 64 \
                 --cntlr-cnt 2 --strip-cnt 1 --strip-size-kb 64

$vda_dir/vda_cli da create --da-name da2 --size-mb 64 --physical-size-mb 64 \
                 --cntlr-cnt 1 --strip-cnt 2 --strip-size-kb 64

$vda_dir/vda_cli da create --da-name da3 --size-mb 64 --physical-size-mb 64 \
                 --cntlr-cnt 2 --strip-cnt 2 --strip-size-kb 64


host_nqn="nqn.2016-06.io.spdk:host0"

$vda_dir/vda_cli exp create --da-name da0 --exp-name exp0a \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da1 --exp-name exp1a \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da2 --exp-name exp2a \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da3 --exp-name exp3a \
                 --initiator-nqn $host_nqn


da_verify da0
da_verify da1
da_verify da2
da_verify da3

exp_verify da0 exp0a
exp_verify da1 exp1a
exp_verify da2 exp2a
exp_verify da3 exp3a

nvmf_connect da0 exp0a $host_nqn
nvmf_connect da1 exp1a $host_nqn
nvmf_connect da2 exp2a $host_nqn
nvmf_connect da3 exp3a $host_nqn

nvmf_format da0 exp0a
nvmf_format da1 exp1a
nvmf_format da2 exp2a
nvmf_format da3 exp3a

nvmf_mount da0 exp0a "$work_dir/da0"
nvmf_mount da1 exp1a "$work_dir/da1"
nvmf_mount da2 exp2a "$work_dir/da2"
nvmf_mount da3 exp3a "$work_dir/da3"

sudo touch "$work_dir/da0/foo"
sudo touch "$work_dir/da1/foo"
sudo touch "$work_dir/da2/foo"
sudo touch "$work_dir/da3/foo"

if [ ! -f "$work_dir/da0/foo" ]; then
    echo "can not create file: $work_dir/da0/foo"
    exit 1
fi

if [ ! -f "$work_dir/da1/foo" ]; then
    echo "can not create file: $work_dir/da1/foo"
    exit 1
fi

if [ ! -f "$work_dir/da2/foo" ]; then
    echo "can not create file: $work_dir/da2/foo"
    exit 1
fi

if [ ! -f "$work_dir/da3/foo" ]; then
    echo "can not create file: $work_dir/da3/foo"
    exit 1
fi

umount_dir "$work_dir/da0"
umount_dir "$work_dir/da1"
umount_dir "$work_dir/da2"
umount_dir "$work_dir/da3"

sudo nvme disconnect-all

$vda_dir/vda_cli exp delete --da-name da0 --exp-name exp0a
$vda_dir/vda_cli exp delete --da-name da1 --exp-name exp1a
$vda_dir/vda_cli exp delete --da-name da2 --exp-name exp2a
$vda_dir/vda_cli exp delete --da-name da3 --exp-name exp3a

$vda_dir/vda_cli exp create --da-name da0 --exp-name exp0b \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da1 --exp-name exp1b \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da2 --exp-name exp2b \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da3 --exp-name exp3b \
                 --initiator-nqn $host_nqn

exp_verify da0 exp0b
exp_verify da1 exp1b
exp_verify da2 exp2b
exp_verify da3 exp3b

nvmf_connect da0 exp0b $host_nqn
nvmf_connect da1 exp1b $host_nqn
nvmf_connect da2 exp2b $host_nqn
nvmf_connect da3 exp3b $host_nqn

nvmf_mount da0 exp0b "$work_dir/da0"
nvmf_mount da1 exp1b "$work_dir/da1"
nvmf_mount da2 exp2b "$work_dir/da2"
nvmf_mount da3 exp3b "$work_dir/da3"

if [ ! -f "$work_dir/da0/foo" ]; then
    echo "can not find file: $work_dir/da0/foo"
    exit 1
fi

if [ ! -f "$work_dir/da1/foo" ]; then
    echo "can not find file: $work_dir/da1/foo"
    exit 1
fi

if [ ! -f "$work_dir/da2/foo" ]; then
    echo "can not find file: $work_dir/da2/foo"
    exit 1
fi

if [ ! -f "$work_dir/da3/foo" ]; then
    echo "can not find file: $work_dir/da3/foo"
    exit 1
fi

umount_dir "$work_dir/da0"
umount_dir "$work_dir/da1"
umount_dir "$work_dir/da2"
umount_dir "$work_dir/da3"

sudo nvme disconnect-all

$vda_dir/vda_cli exp delete --da-name da0 --exp-name exp0b
$vda_dir/vda_cli exp delete --da-name da1 --exp-name exp1b
$vda_dir/vda_cli exp delete --da-name da2 --exp-name exp2b
$vda_dir/vda_cli exp delete --da-name da3 --exp-name exp3b

echo "testing failover"
$vda_dir/vda_cli exp create --da-name da3 --exp-name exp3c \
                 --initiator-nqn $host_nqn
exp_verify da3 exp3c
nvmf_connect da3 exp3c $host_nqn
nvmf_mount da3 exp3c "$work_dir/da3"

sock_addr=`$vda_dir/vda_cli da get --da-name da3 | jq -r ".disk_array.cntlr_list[] | select(.is_primary==true).sock_addr"`
echo "primary sock_addr: $sock_addr"
if [ "$sock_addr" != "localhost:9820" ] && [ "$sock_addr" != "localhost:9821" ]; then
    echo "get primary sock_addr err"
    exit 1
fi
if [ "$sock_addr" == "localhost:9820" ]; then
    port="9820"
    new_primary="localhost:9821"
else
    port="9821"
    new_primary="localhost:9820"
fi
primary_pid=`ps -ef | grep vda_cn_agent | grep $port | awk '{print $2}'`
if [ "$primary_pid" == "" ]; then
    echo "can not find primary_pid"
    exit 1
fi
kill $primary_pid
echo "waiting for failover"
max_retry=10
retry_cnt=0
while true; do
    sock_addr=`$vda_dir/vda_cli da get --da-name da3 | jq -r ".disk_array.cntlr_list[] | select(.is_primary==true).sock_addr"`
    if [ "$sock_addr" == "$new_primary" ]; then
        break
    fi
    if [ $retry_cnt -ge $max_retry ]; then
        echo "failover timeout"
        exit 1
    fi
    sleep 5
    ((retry_cnt=retry_cnt+1))
done

grp_verify da3

sudo touch "$work_dir/da3/bar"

if [ ! -f "$work_dir/da3/bar" ]; then
    echo "can not create file: $work_dir/da3/bar"
    exit 1
fi

if [ "$new_primary" == "localhost:9820" ]; then
    $vda_dir/vda_cn_agent --network tcp --address '127.0.0.1:9821' \
                          --sock-path $work_dir/cn1.sock --sock-timeout 10 \
                          --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4431"}' \
                          --tr-conf '{"trtype":"TCP"}' \
                          > $work_dir/cn_agent_1.log 2>&1 &
else
    $vda_dir/vda_cn_agent --network tcp --address '127.0.0.1:9820' \
                          --sock-path $work_dir/cn0.sock --sock-timeout 10 \
                          --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}' \
                          --tr-conf '{"trtype":"TCP"}' \
                          > $work_dir/cn_agent_0.log 2>&1 &
fi

echo "waiting for da3 recover"
max_retry=10
retry_cnt=0
while true; do
    cntlr_cnt=`$vda_dir/vda_cli da get --da-name $da_name | jq ".disk_array.cntlr_list | length"`
    if [ "$cntlr_cnt" == "" ]; then
        echo "cntlr_cnt is empty, da_name: $da_name"
        exit 1
    fi
    if [ $cntlr_cnt -eq 0 ]; then
        echo "cntlr_cnt is 0, da_name: $da_name"
        exit 1
    fi
    good_cntlr_cnt=0
    for i in `seq 0 $[cntlr_cnt - 1]`; do
        timestamp=`$vda_dir/vda_cli da get --da-name $da_name | jq -r ".disk_array.cntlr_list[$i].err_info.timestamp"`
        if [ "$timestamp" == "null" ]; then
            echo "cntlr timestmap is null, da_name: $da_name cntlr: $i"
            exit 1
        fi
        is_err=`$vda_dir/vda_cli da get --da-name $da_name | jq -r ".disk_array.cntlr_list[$i].err_info.is_err"`
        if [ "$is_err" == "null" ]; then
            ((good_cntlr_cnt=good_cntlr_cnt+1))
        fi
    done
    if [ $good_cntlr_cnt -eq 2 ]; then
        break
    fi
    if [ $retry_cnt -ge $max_retry ]; then
        echo "da3 recover timeout"
        exit 1
    fi
    sleep 5
    ((retry_cnt=retry_cnt+1))
done

sleep 15

echo "da3 recovered"

da_verify da3

# echo "sleep"
# sleep infinity

umount_dir "$work_dir/da3"

$vda_dir/vda_cli exp delete --da-name da3 --exp-name exp3c


$vda_dir/vda_cli da delete --da-name da0
$vda_dir/vda_cli da delete --da-name da1
$vda_dir/vda_cli da delete --da-name da2
$vda_dir/vda_cli da delete --da-name da3

$vda_dir/vda_cli cn delete --sock-addr localhost:9820
$vda_dir/vda_cli cn delete --sock-addr localhost:9821

$vda_dir/vda_cli pd delete --sock-addr localhost:9720 --pd-name pd0
$vda_dir/vda_cli pd delete --sock-addr localhost:9721 --pd-name pd1

$vda_dir/vda_cli dn delete --sock-addr localhost:9720
$vda_dir/vda_cli dn delete --sock-addr localhost:9721

cleanup

echo "succeed"
