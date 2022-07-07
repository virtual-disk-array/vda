#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$CURR_DIR/../..
DATAPLANE_DIR=$ROOT_DIR/dataplane

SPDK_VERSION=09897660602615f7a2b2a8b78c09d3382dca075d
DPDK_VERSION=d0470b2491529499eab84608b41a715f4c240be6
INTEL_IPSEC_MB_VERSION=5b6f01f1d52c5a2a577b47094b8d358541fdcb5e
ISA_L_VERSION=2df39cf5f1b9ccaa2973f6ef273857e4dc46f0cf
LIBVFIO_USER_VERSION=d307dbcab74aef3680ba99d7f836f2bc0b4bc81e
OCF_VERSION=4477cb55a0bcd313a5ebcfdf877ca76a31695df7

if [ ! -d $DATAPLANE_DIR/spdk ]; then
    cd $DATAPLANE_DIR
    git clone https://github.com/spdk/spdk
    cd $DATAPLANE_DIR/spdk
    git checkout $SPDK_VERSION
    sudo scripts/pkgdep.sh
    git submodule update --init
    cd $DATAPLANE_DIR/spdk/dpdk
    git checkout $DPDK_VERSION
    cd $DATAPLANE_DIR/spdk/intel-ipsec-mb
    git checkout $INTEL_IPSEC_MB_VERSION
    cd $DATAPLANE_DIR/spdk/isa-l
    git checkout $ISA_L_VERSION
    cd $DATAPLANE_DIR/spdk/libvfio-user
    git checkout $LIBVFIO_USER_VERSION
    cd $DATAPLANE_DIR/spdk/ocf
    git checkout $OCF_VERSION
fi
