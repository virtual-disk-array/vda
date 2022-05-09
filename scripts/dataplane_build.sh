#!/bin/bash

set -e

curr_dir=$(readlink -f $(dirname $0))
dataplane_dir=$curr_dir/../dataplane
cd $dataplane_dir
SPDK_DIR=$dataplane_dir/spdk

export SPDK_LIB_DIR="$SPDK_DIR/build/lib"
export DPDK_LIB_DIR="$SPDK_DIR/dpdk/build/lib"
export VFIO_LIB_DIR="$SPDK_DIR/libvfio-user/build/release/lib"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SPDK_LIB_DIR:$DPDK_LIB_DIR:$VFIO_LIB_DIR

export SPDK_HEADER_DIR="$SPDK_DIR/include"

rm -f $dataplane_dir/app/vda_dataplane
make -C $dataplane_dir/app vda_dataplane
