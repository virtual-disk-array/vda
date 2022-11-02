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

$BIN_DIR/vda_monitor --etcd-endpoints localhost:$ETCD_PORT \
              > $WORK_DIR/monitor_0.log 2>&1 &


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

nvmf_connect da0 exp0a $host_nqn
nvmf_connect da1 exp1a $host_nqn
nvmf_connect da2 exp2a $host_nqn
nvmf_connect da3 exp3a $host_nqn

nvmf_format da0 exp0a
nvmf_format da1 exp1a
nvmf_format da2 exp2a
nvmf_format da3 exp3a

nvmf_mount da0 exp0a "$WORK_DIR/da0"
nvmf_mount da1 exp1a "$WORK_DIR/da1"
nvmf_mount da2 exp2a "$WORK_DIR/da2"
nvmf_mount da3 exp3a "$WORK_DIR/da3"

sudo touch "$WORK_DIR/da0/foo"
sudo touch "$WORK_DIR/da1/foo"
sudo touch "$WORK_DIR/da2/foo"
sudo touch "$WORK_DIR/da3/foo"

if [ ! -f "$WORK_DIR/da0/foo" ]; then
    echo "can not create file: $WORK_DIR/da0/foo"
    exit 1
fi

if [ ! -f "$WORK_DIR/da1/foo" ]; then
    echo "can not create file: $WORK_DIR/da1/foo"
    exit 1
fi

if [ ! -f "$WORK_DIR/da2/foo" ]; then
    echo "can not create file: $WORK_DIR/da2/foo"
    exit 1
fi

if [ ! -f "$WORK_DIR/da3/foo" ]; then
    echo "can not create file: $WORK_DIR/da3/foo"
    exit 1
fi

umount_dir "$WORK_DIR/da0"
umount_dir "$WORK_DIR/da1"
umount_dir "$WORK_DIR/da2"
umount_dir "$WORK_DIR/da3"

sudo nvme disconnect-all

echo "delete exp da0 exp0a"
$BIN_DIR/vda_cli exp delete --da-name da0 --exp-name exp0a
echo "delete exp da1 exp1a"
$BIN_DIR/vda_cli exp delete --da-name da1 --exp-name exp1a
echo "delete exp da2 exp2a"
$BIN_DIR/vda_cli exp delete --da-name da2 --exp-name exp2a
echo "delete exp da3 exp3a"
$BIN_DIR/vda_cli exp delete --da-name da3 --exp-name exp3a

echo "create exp da0 exp0b"
$BIN_DIR/vda_cli exp create --da-name da0 --exp-name exp0b \
                 --initiator-nqn $host_nqn
echo "create exp da1 exp1b"
$BIN_DIR/vda_cli exp create --da-name da1 --exp-name exp1b \
                 --initiator-nqn $host_nqn
echo "create exp da2 exp2b"
$BIN_DIR/vda_cli exp create --da-name da2 --exp-name exp2b \
                 --initiator-nqn $host_nqn
echo "create exp da3 exp3b"
$BIN_DIR/vda_cli exp create --da-name da3 --exp-name exp3b \
                 --initiator-nqn $host_nqn

exp_verify da0 exp0b
exp_verify da1 exp1b
exp_verify da2 exp2b
exp_verify da3 exp3b

nvmf_connect da0 exp0b $host_nqn
nvmf_connect da1 exp1b $host_nqn
nvmf_connect da2 exp2b $host_nqn
nvmf_connect da3 exp3b $host_nqn

nvmf_mount da0 exp0b "$WORK_DIR/da0"
nvmf_mount da1 exp1b "$WORK_DIR/da1"
nvmf_mount da2 exp2b "$WORK_DIR/da2"
nvmf_mount da3 exp3b "$WORK_DIR/da3"

if [ ! -f "$WORK_DIR/da0/foo" ]; then
    echo "can not find file: $WORK_DIR/da0/foo"
    exit 1
fi

if [ ! -f "$WORK_DIR/da1/foo" ]; then
    echo "can not find file: $WORK_DIR/da1/foo"
    exit 1
fi

if [ ! -f "$WORK_DIR/da2/foo" ]; then
    echo "can not find file: $WORK_DIR/da2/foo"
    exit 1
fi

if [ ! -f "$WORK_DIR/da3/foo" ]; then
    echo "can not find file: $WORK_DIR/da3/foo"
    exit 1
fi

umount_dir "$WORK_DIR/da0"
umount_dir "$WORK_DIR/da1"
umount_dir "$WORK_DIR/da2"
umount_dir "$WORK_DIR/da3"

sudo nvme disconnect-all

echo "delete exp da0 exp0b"
$BIN_DIR/vda_cli exp delete --da-name da0 --exp-name exp0b
echo "delete exp da1 exp1b"
$BIN_DIR/vda_cli exp delete --da-name da1 --exp-name exp1b
echo "delete exp da2 exp2b"
$BIN_DIR/vda_cli exp delete --da-name da2 --exp-name exp2b
echo "delete exp da3 exp3b"
$BIN_DIR/vda_cli exp delete --da-name da3 --exp-name exp3b

echo "testing cn failover"
echo "create exp da3 exp3c"
$BIN_DIR/vda_cli exp create --da-name da3 --exp-name exp3c \
                 --initiator-nqn $host_nqn
exp_verify da3 exp3c
nvmf_connect da3 exp3c $host_nqn

nvmf_mount da3 exp3c "$WORK_DIR/da3"

sock_addr=`$BIN_DIR/vda_cli da get --da-name da3 | jq -r ".disk_array.cntlr_list[] | select(.is_primary==true).sock_addr"`
echo "primary sock_addr: $sock_addr"
if [ "$sock_addr" != "localhost:9820" ] && [ "$sock_addr" != "localhost:9821" ]; then
    echo "get primary sock_addr err"
    exit 1
fi
if [ "$sock_addr" == "localhost:9820" ]; then
    port="9820"
    new_primary="localhost:9821"
    cn_name="cn0"
else
    port="9821"
    new_primary="localhost:9820"
    cn_name="cn1"
fi
primary_pid=$(ps -ef | grep vda_cn_agent | grep $port | awk '{print $2}')
if [ "$primary_pid" == "" ]; then
    echo "can not find primary_pid"
    exit 1
fi
kill $primary_pid
spdk_pid=$(ps -ef | grep vda_dataplane | grep $cn_name | grep -v sudo | awk '{print $2}')
sudo kill -9 $spdk_pid
echo "waiting for cn failover"
max_retry=10
retry_cnt=0
while true; do
    sock_addr=$($BIN_DIR/vda_cli da get --da-name da3 | jq -r ".disk_array.cntlr_list[] | select(.is_primary==true).sock_addr")
    if [ "$sock_addr" == "$new_primary" ]; then
        break
    fi
    if [ $retry_cnt -ge $max_retry ]; then
        echo "cn failover timeout"
        exit 1
    fi
    sleep 5
    ((retry_cnt=retry_cnt+1))
done

echo "cn failover done"

max_retry=10
retry_cnt=0
while true; do
    is_err=$($BIN_DIR/vda_cli da get --da-name da3 | jq -r  ".disk_array.cntlr_list[] | select(.is_primary==true).err_info.is_err")
    if [ $is_err == "null" ]; then
        break
    fi
    if [ $retry_cnt -ge $max_retry ]; then
        echo "waiting for primary ready timeout"
        exit 1
    fi
    sleep 5
    ((retry_cnt=retry_cnt+1))
done

echo "new primary ready"

grp_verify da3

sudo touch "$WORK_DIR/da3/bar"

if [ ! -f "$WORK_DIR/da3/bar" ]; then
    echo "can not create file: $WORK_DIR/da3/bar"
    exit 1
fi

if [ "$new_primary" == "localhost:9820" ]; then
    sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/cn1.sock > $WORK_DIR/cn1.log 2>&1 &"
    sleep 5
    sudo chown $(id -u):$(id -g) $WORK_DIR/cn1.sock
    $BIN_DIR/vda_cn_agent --network tcp --address '127.0.0.1:9821' \
                          --sock-path $WORK_DIR/cn1.sock --sock-timeout 10 \
                          --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4431"}' \
                          --tr-conf '{"trtype":"TCP"}' \
                          > $WORK_DIR/cn_agent_1.log 2>&1 &
else
    sudo bash -c "$BIN_DIR/vda_dataplane --config $BIN_DIR/dataplane_config.json --rpc-socket $WORK_DIR/cn0.sock > $WORK_DIR/cn0.log 2>&1 &"
    sleep 5
    sudo chown $(id -u):$(id -g) $WORK_DIR/cn0.sock
    $BIN_DIR/vda_cn_agent --network tcp --address '127.0.0.1:9820' \
                          --sock-path $WORK_DIR/cn0.sock --sock-timeout 10 \
                          --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}' \
                          --tr-conf '{"trtype":"TCP"}' \
                          > $WORK_DIR/cn_agent_0.log 2>&1 &
fi

echo "waiting for da3 recover"
max_retry=10
retry_cnt=0
while true; do
    cntlr_cnt=$($BIN_DIR/vda_cli da get --da-name $da_name | jq ".disk_array.cntlr_list | length")
    if [ "$cntlr_cnt" == "" ]; then
        echo "cntlr_cnt is empty, da_name: $da_name"
        exit 1
    fi
    if [ $cntlr_cnt -eq 0 ]; then
        echo "cntlr_cnt is 0, da_name: $da_name"
        exit 1
    fi
    good_cntlr_cnt=0
    for i in $(seq 0 $[cntlr_cnt - 1]); do
        timestamp=$($BIN_DIR/vda_cli da get --da-name $da_name | jq -r ".disk_array.cntlr_list[$i].err_info.timestamp")
        if [ "$timestamp" == "null" ]; then
            echo "cntlr timestmap is null, da_name: $da_name cntlr: $i"
            exit 1
        fi
        is_err=$($BIN_DIR/vda_cli da get --da-name $da_name | jq -r ".disk_array.cntlr_list[$i].err_info.is_err")
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

sleep 20

echo "da3 recovered"

da_verify da0
da_verify da1
da_verify da2
da_verify da3
exp_verify da3 exp3c

umount_dir "$WORK_DIR/da3"

$BIN_DIR/vda_cli exp delete --da-name da3 --exp-name exp3c


$BIN_DIR/vda_cli da delete --da-name da0
$BIN_DIR/vda_cli da delete --da-name da1
$BIN_DIR/vda_cli da delete --da-name da2
$BIN_DIR/vda_cli da delete --da-name da3

$BIN_DIR/vda_cli cn delete --sock-addr localhost:9820
$BIN_DIR/vda_cli cn delete --sock-addr localhost:9821

$BIN_DIR/vda_cli pd delete --sock-addr localhost:9720 --pd-name pd0
$BIN_DIR/vda_cli pd delete --sock-addr localhost:9721 --pd-name pd1

$BIN_DIR/vda_cli dn delete --sock-addr localhost:9720
$BIN_DIR/vda_cli dn delete --sock-addr localhost:9721

cleanup

echo "succeed"
