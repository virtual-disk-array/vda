#!/bin/bash

set -e

SPDK_VERSION=v22.01
DPDK_VERSION=2e2390492155e15f506ca4b3e4456c7e53ed1f68
INTEL_IPSEC_MB_VERSION=5b6f01f1d52c5a2a577b47094b8d358541fdcb5e
ISA_L_VERSION=2df39cf5f1b9ccaa2973f6ef273857e4dc46f0cf
LIBVFIO_USER_VERSION=17769cf1af093dfb4b9bc3347ae39324029989ac
OCF_VERSION=865d29d0cb93a71ce37a8410914c35005aa6ed54

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
