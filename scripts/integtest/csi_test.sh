#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

rm -rf $WORK_DIR
mkdir -p $WORK_DIR

ROOT_DIR=$CURR_DIR/../..
BIN_DIR=$ROOT_DIR/_out/linux_amd64
ETCD_BIN=$ROOT_DIR/bin/etcd

echo "launch etcd"
$ETCD_BIN --listen-client-urls "http://localhost:$ETCD_PORT" \
          --advertise-client-urls "http://localhost:$ETCD_PORT" \
          --listen-peer-urls "http://localhost:$ETCD_PEER_PORT" \
          --name etcd0 --data-dir $WORK_DIR/etcd0.data \
          > $WORK_DIR/etcd0.log 2>&1 &

echo "launch vda_dataplane"

sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/dn0.sock > $WORK_DIR/dn0.log 2>&1 &"
sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/dn1.sock > $WORK_DIR/dn1.log 2>&1 &"
sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/cn0.sock > $WORK_DIR/cn0.log 2>&1 &"
sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/cn1.sock > $WORK_DIR/cn1.log 2>&1 &"

sleep 5

sudo chown $(id -u):$(id -g) $WORK_DIR/dn0.sock
sudo chown $(id -u):$(id -g) $WORK_DIR/dn1.sock
sudo chown $(id -u):$(id -g) $WORK_DIR/cn0.sock
sudo chown $(id -u):$(id -g) $WORK_DIR/cn1.sock

echo "launch vda services"

$BIN_DIR/vda_dn_agent --network tcp --address '127.0.0.1:9720' \
               --sock-path $WORK_DIR/dn0.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4420"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $WORK_DIR/dn_agent_0.log 2>&1 &

$BIN_DIR/vda_dn_agent --network tcp --address '127.0.0.1:9721' \
               --sock-path $WORK_DIR/dn1.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4421"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $WORK_DIR/dn_agent_1.log 2>&1 &

$BIN_DIR/vda_cn_agent --network tcp --address '127.0.0.1:9820' \
               --sock-path $WORK_DIR/cn0.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $WORK_DIR/cn_agent_0.log 2>&1 &

$BIN_DIR/vda_cn_agent --network tcp --address '127.0.0.1:9821' \
               --sock-path $WORK_DIR/cn1.sock --sock-timeout 10 \
               --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4431"}' \
               --tr-conf '{"trtype":"TCP"}' \
               > $WORK_DIR/cn_agent_1.log 2>&1 &

$BIN_DIR/vda_portal --portal-address '0.0.0.0:9520' --portal-network tcp \
             --etcd-endpoints localhost:$ETCD_PORT \
             > $WORK_DIR/portal_0.log 2>&1 &

$BIN_DIR/vda_monitor --etcd-endpoints localhost:$ETCD_PORT \
              > $WORK_DIR/monitor_0.log 2>&1 &


echo "create vda resources"
$BIN_DIR/vda_cli dn create --sock-addr localhost:9720 --tr-svc-id 4420
$BIN_DIR/vda_cli pd create --sock-addr localhost:9720 --pd-name pd0 \
                 --bdev-type-key malloc --bdev-type-value 256
$BIN_DIR/vda_cli cn create --sock-addr localhost:9820 --tr-svc-id 4430

echo "create kubernetes cluster"

minikube start --driver=none --kubernetes-version=v1.25.0

minikube kubectl -- apply -f https://docs.projectcalico.org/manifests/calico.yaml

# eval $(minikube docker-env)
# docker build -t virtualdiskarray/vdacsi:dev -f $ROOT_DIR/scripts/csi/Dockerfile $ROOT_DIR

echo "create kubernetes resoruces"

minikube kubectl -- apply -f $CURR_DIR/controller-rbac.yaml
minikube kubectl -- apply -f $CURR_DIR/controller.yaml
minikube kubectl -- apply -f $CURR_DIR/node-rbac.yaml
minikube kubectl -- apply -f $CURR_DIR/node.yaml
minikube kubectl -- apply -f $CURR_DIR/storageclass.yaml

function wait_for_pod() {
    target_cnt=$1
    max_retry=$2
    retry_cnt=0
    while true; do
        cnt=$(minikube kubectl -- get pod -o json | jq ".items[].status.containerStatuses[].ready" | grep true | wc -l)
        if [ $cnt -eq $target_cnt ]; then
            break
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "kubernetes resoruces timeout"
            exit 1
        fi
        echo "kubernetes pod retry cnt: $retry_cnt"
        sleep 5
        ((retry_cnt=retry_cnt+1))
    done
}

function wait_for_pvc() {
    max_retry=10
    retry_cnt=0
    while true; do
        phase=$(minikube kubectl -- get pvc -o json | jq -r ".items[0].status.phase")
        if [ $phase == "Bound" ]; then
            break
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "kubernetes pvc timeout"
            exit 1
        fi
        echo "kubernetes pvc retry cnt: $retry_cnt"
        sleep 5
        ((retry_cnt=retry_cnt+1))
    done
}

function wait_for_deleting_pvc() {
    max_retry=10
    retry_cnt=0
    while true; do
        pvc_cnt=$(minikube kubectl -- get pvc -o json | jq ".items | length")
        if [ $pvc_cnt -eq 0 ]; then
            break
        fi
        if [ $retry_cnt -ge $max_retry ]; then
            echo "kubernetes deleting pvc timeout"
            exit 1
        fi
        echo "kubernetes pvc deleting retry cnt: $retry_cnt"
        sleep 5
        ((retry_cnt=retry_cnt+1))
    done
}

wait_for_pod 5 10

echo "create testpvc"
minikube kubectl -- apply -f $CURR_DIR/testpvc.yaml
wait_for_pvc

da_name=$($BIN_DIR/vda_cli da list | jq -r ".da_summary_list[0].da_name")
if [ $da_name == "null" ]; then
    echo "can not find da"
    exit 1
fi

exp_cnt=$($BIN_DIR/vda_cli exp list --da-name $da_name | jq ".exp_summary_list | length")
if [ $exp_cnt -ne 0 ]; then
    echo "exp_cnt is not 0: $exp_cnt"
    exit 1
fi

echo "create testpod"
minikube kubectl -- apply -f $CURR_DIR/testpod.yaml

wait_for_pod 6 20

exp_cnt=$($BIN_DIR/vda_cli exp list --da-name $da_name | jq ".exp_summary_list | length")
if [ $exp_cnt -ne 1 ]; then
    echo "exp_cnt is not 1: $exp_cnt"
    exit 1
fi

echo "delete testpod"
minikube kubectl -- delete pod vdacsi-test

wait_for_pod 5 10

echo "check exp_cnt after deleting testpod"
retry_cnt=0
max_retry=10
while true; do
    exp_cnt=$($BIN_DIR/vda_cli exp list --da-name $da_name | jq ".exp_summary_list | length")
    if [ $exp_cnt -eq 0 ]; then
	echo "exp_cnt is 0"
	break
    fi
    echo "exp_cnt is not 0: $exp_cnt"
    if [ $retry_cnt -ge $max_retry ]; then
        echo "check exp_cnt timeout"
        exit 1
    fi
    sleep 1
    ((retry_cnt=retry_cnt+1))
done

echo "deleve pvc"
minikube kubectl -- delete pvc vdacsi-pvc

wait_for_deleting_pvc

da_cnt=$($BIN_DIR/vda_cli da list | jq ".da_summary_list | length")
if [ $da_cnt -ne 0 ]; then
    echo "da_cnt is not 0: $da_cnt"
    exit 1
fi

cleanup

echo "succeed"
