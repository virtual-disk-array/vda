#!/bin/bash

set -e

curr_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $curr_dir/conf.sh

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
cd $vda_dir

./vda_dn_agent --network tcp --address '127.0.0.1:9720' \
               --sock-path $work_dir/dn0.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4420"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/dn_agent_0.log 2>&1 &

./vda_dn_agent --network tcp --address '127.0.0.1:9721' \
               --sock-path $work_dir/dn1.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4421"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/dn_agent_1.log 2>&1 &

./vda_cn_agent --network tcp --address '127.0.0.1:9820' \
               --sock-path $work_dir/cn0.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/cn_agent_0.log 2>&1 &

./vda_cn_agent --network tcp --address '127.0.0.1:9821' \
               --sock-path $work_dir/cn1.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4431"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $work_dir/cn_agent_1.log 2>&1 &

./vda_portal --portal-address '127.0.0.1:9520' --portal-network tcp \
             --etcd-endpoints localhost:$etcd_port \
             > $work_dir/portal_0.log 2>&1 &

./vda_monitor --etcd-endpoints localhost:$etcd_port \
              > $work_dir/monitor_0.log 2>&1 &


echo "create vda resources"
./vda_cli dn create --sock-addr localhost:9720 --tr-svc-id 4420
./vda_cli pd create --sock-addr localhost:9720 --pd-name pd0 \
          --bdev-type-key malloc --bdev-type-value 256
./vda_cli cn create --sock-addr localhost:9820 --tr-svc-id 4430

echo "create kubernetes cluster"
cd $curr_dir

minikube start --vm-driver=none

echo "create kubernetes resoruces"

$kubectl_cmd apply -f controller-rbac.yaml
$kubectl_cmd apply -f controller.yaml
$kubectl_cmd apply -f node-rbac.yaml
$kubectl_cmd apply -f node.yaml
$kubectl_cmd apply -f storageclass.yaml

function wait_for_pod() {
    target_cnt=$1
    max_retry=$2
    retry_cnt=0
    while true; do
        cnt=`$kubectl_cmd get pod -o json | jq ".items[].status.containerStatuses[].ready" | grep true | wc -l`
        if [ $cnt -eq $target_cnt ]; then
            break
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "kubernetes resoruces timeout"
            exit 1
        fi
        echo "kubernetes resources wait retry cnt: $retry_cnt"
        sleep 5
        ((retry_cnt=retry_cnt+1))
    done
}

wait_for_pod 5 10

$kubectl_cmd apply -f testpod.yaml

wait_for_pod 6 20

echo "sleep"
sleep infinity
