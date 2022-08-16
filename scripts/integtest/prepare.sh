#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
ROOT_DIR=$CURR_DIR/../..
BIN_DIR=$ROOT_DIR/_out/linux_amd64

sudo modprobe nvme-tcp

sudo sysctl fs.protected_regular=0

sudo HUGEMEM=$HUGE_MEM $BIN_DIR/spdk/scripts/setup.sh

mkdir -p $WORK_DIR
