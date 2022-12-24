#!/bin/bash

set -x
set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

ROOT_DIR=$CURR_DIR/../..
BIN_DIR=$ROOT_DIR/_out/linux_amd64
ETCD_BIN=$ROOT_DIR/bin/etcd

sudo rm -rf $WORK_DIR
mkdir -p $WORK_DIR

function launch_processes() {
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

    $BIN_DIR/vda_portal --portal-address '127.0.0.1:9520' --portal-network tcp \
                        --etcd-endpoints localhost:$ETCD_PORT \
                        > $WORK_DIR/portal_0.log 2>&1 &
}

launch_processes

echo "prepare pd file"
dd if=/dev/zero of=$WORK_DIR/pd0.img bs=1M count=2048
dd if=/dev/zero of=$WORK_DIR/pd1.img bs=1M count=2048

echo "create dn localhost:9720"
$BIN_DIR/vda_cli dn create --sock-addr localhost:9720 --tr-svc-id 4420
echo "create pd localhost:9720 pd0"
$BIN_DIR/vda_cli pd create --sock-addr localhost:9720 --pd-name pd0 \
                 --bdev-type-key aio --bdev-type-value $WORK_DIR/pd0.img
echo "create dn localhost:9721"
$BIN_DIR/vda_cli dn create --sock-addr localhost:9721 --tr-svc-id 4421
echo "create pd localhost:9721 pd1"
$BIN_DIR/vda_cli pd create --sock-addr localhost:9721 --pd-name pd1 \
                 --bdev-type-key aio --bdev-type-value $WORK_DIR/pd1.img
echo "create cn localhost:9820"
$BIN_DIR/vda_cli cn create --sock-addr localhost:9820 --tr-svc-id 4430
echo "create cn localhost:9821"
$BIN_DIR/vda_cli cn create --sock-addr localhost:9821 --tr-svc-id 4431

echo "create da0"
$BIN_DIR/vda_cli da create --da-name da0 --size-mb 64 --init-grp-size-mb 64 \
                 --cntlr-cnt 1 --strip-cnt 1 --strip-size-kb 64
echo "create da1"
$BIN_DIR/vda_cli da create --da-name da1 --size-mb 64 --init-grp-size-mb 64 \
                 --cntlr-cnt 2 --strip-cnt 1 --strip-size-kb 64
echo "create da2"
$BIN_DIR/vda_cli da create --da-name da2 --size-mb 64 --init-grp-size-mb 64 \
                 --cntlr-cnt 1 --strip-cnt 2 --strip-size-kb 64
echo "create da3"
$BIN_DIR/vda_cli da create --da-name da3 --size-mb 512 --init-grp-size-mb 512 \
                 --cntlr-cnt 2 --strip-cnt 1 --strip-size-kb 64
host_nqn="nqn.2016-06.io.spdk:host0"

echo "create exp da0 exp0a"
$BIN_DIR/vda_cli exp create --da-name da0 --exp-name exp0a \
                 --initiator-nqn $host_nqn
echo "create exp da1 exp1a"
$BIN_DIR/vda_cli exp create --da-name da1 --exp-name exp1a \
                 --initiator-nqn $host_nqn
echo "create exp da2 exp2a"
$BIN_DIR/vda_cli exp create --da-name da2 --exp-name exp2a \
                 --initiator-nqn $host_nqn
echo "create exp da3 exp3a"
$BIN_DIR/vda_cli exp create --da-name da3 --exp-name exp3a \
                 --initiator-nqn $host_nqn

da_verify da0
da_verify da1
da_verify da2
da_verify da3

exp_verify da0 exp0a
exp_verify da1 exp1a
exp_verify da2 exp2a
exp_verify da3 exp3a

killall vda_portal
killall vda_dn_agent
killall vda_cn_agent
killall etcd
sudo killall reactor_0

launch_processes

$BIN_DIR/vda_cli dn syncup --sock-addr '127.0.0.1:9720'
$BIN_DIR/vda_cli dn syncup --sock-addr '127.0.0.1:9721'
for i in $(seq 2); do
    $BIN_DIR/vda_cli cn syncup --sock-addr '127.0.0.1:9820'
    $BIN_DIR/vda_cli cn syncup --sock-addr '127.0.0.1:9821'
done

da_verify da0
da_verify da1
da_verify da2
da_verify da3

exp_verify da0 exp0a
exp_verify da1 exp1a
exp_verify da2 exp2a
exp_verify da3 exp3a

cleanup

echo "succeed"
