#!/bin/bash

set -e

curr_dir=$(dirname $0)
source $curr_dir/conf.sh

sudo modprobe nvme-tcp

sudo sysctl fs.protected_regular=0

cd $spdk_dir
sudo HUGEMEM=$huge_mem scripts/setup.sh

mkdir -p $work_dir
