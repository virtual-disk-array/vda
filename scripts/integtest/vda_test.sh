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

$vda_dir/vda_cli exp create --da-name da0 --exp-name exp0 \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da1 --exp-name exp1 \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da2 --exp-name exp2 \
                 --initiator-nqn $host_nqn
$vda_dir/vda_cli exp create --da-name da3 --exp-name exp3 \
                 --initiator-nqn $host_nqn


da_verify da0
da_verify da1
da_verify da2
da_verify da3

exp_verify da0 exp0
exp_verify da1 exp1
exp_verify da2 exp2
exp_verify da3 exp3

nvmf_connect da0 exp0 $host_nqn
sleep 1
nvmf_format da0 exp0
nvmf_mount da0 exp0 "$work_dir/da0"

echo "sleep"
sleep infinity

# $vda_dir/vda_cli exp delete --da-name da0 --exp-name exp0
# $vda_dir/vda_cli exp delete --da-name da1 --exp-name exp1
# $vda_dir/vda_cli exp delete --da-name da2 --exp-name exp2
# $vda_dir/vda_cli exp delete --da-name da3 --exp-name exp3

# $vda_dir/vda_cli da delete --da-name da0
# $vda_dir/vda_cli da delete --da-name da1
# $vda_dir/vda_cli da delete --da-name da2
# $vda_dir/vda_cli da delete --da-name da3

# $vda_dir/vda_cli cn delete --sock-addr localhost:9820
# $vda_dir/vda_cli cn delete --sock-addr localhost:9821

# $vda_dir/vda_cli pd delete --sock-addr localhost:9720 --pd-name pd0
# $vda_dir/vda_cli pd delete --sock-addr localhost:9721 --pd-name pd1

# $vda_dir/vda_cli dn delete --sock-addr localhost:9720
# $vda_dir/vda_cli dn delete --sock-addr localhost:9721

# cleanup

# echo "succeed"
