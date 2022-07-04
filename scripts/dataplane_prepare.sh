#!/bin/bash

set -e

SPDK_VERSION=09897660602615f7a2b2a8b78c09d3382dca075d
DPDK_VERSION=d0470b2491529499eab84608b41a715f4c240be6
INTEL_IPSEC_MB_VERSION=5b6f01f1d52c5a2a577b47094b8d358541fdcb5e
ISA_L_VERSION=2df39cf5f1b9ccaa2973f6ef273857e4dc46f0cf
LIBVFIO_USER_VERSION=d307dbcab74aef3680ba99d7f836f2bc0b4bc81e
OCF_VERSION=4477cb55a0bcd313a5ebcfdf877ca76a31695df7

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
