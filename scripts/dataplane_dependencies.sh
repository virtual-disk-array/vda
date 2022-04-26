#!/bin/bash

set -e

SPDK_VERSION=v22.01
DPDK_VERSION=v21.02
INTEL_IPSEC_MB_VERSION=v0.50
ISA_L_VERSION=v2.24.0
LIBVFIO_USER_VERSION=v0.1
OCF_VERSION=v22.3

curr_dir=$(readlink -f $(dirname $0))
dataplane_dir=$curr_dir/../dataplane
cd $dataplane_dir
rm -rf spdk
git clone https://github.com/spdk/spdk
cd $dataplane_dir/spdk
git checkout $SPDK_VERSION
sudo scripts/pkgdep.sh
git submodule update --init
cd $dataplane_dir/spdk/dpdk
git checkout $DPDK_VERSION
cd $dataplane_dir/spdk/intel-ipsec-mb
git checkout $INTEL_IPSEC_MB_VERSION
cd $dataplane_dir/spdk/isa-l
git checkout $ISA_L_VERSION
cd $dataplane_dir/spdk/libvfio-user
git checkout $LIBVFIO_USER_VERSION
cd $dataplane_dir/spdk/ocf
git checkout $OCF_VERSION
cd $dataplane_dir/spdk
./configure
make
