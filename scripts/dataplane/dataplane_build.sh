#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/dataplane_dependencies.sh
ROOT_DIR=$CURR_DIR/../..
DATAPLANE_DIR=$ROOT_DIR/dataplane

export TARGET_ARCHITECTURE=${VDA_DATAPLANE_ARCHITECTURE:-core2}

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

if [ ! -d $DATAPLANE_DIR/spdk/build ]; then
    cd $DATAPLANE_DIR/spdk
    configure_params="--target-arch=${VDA_DATAPLANE_ARCHITECTURE:-core2}"
    if [ "${VDA_DATAPLANE_DEBUG}" == "y" ]; then
        configure_params="${configure_params} --enable-debug"
    fi
    ./configure $configure_params
    procnr=$(cat /proc/cpuinfo | grep processor | wc -l)
    make -j $procnr
fi

cd $DATAPLANE_DIR

export SPDK_HEADER_DIR="$DATAPLANE_DIR/spdk/include"
export SPDK_LIB_DIR="$DATAPLANE_DIR/spdk/build/lib"
export DPDK_LIB_DIR="$DATAPLANE_DIR/spdk/dpdk/build/lib"
export VFIO_LIB_DIR="$DATAPLANE_DIR/spdk/libvfio-user/build/release/lib"

make -C $DATAPLANE_DIR/raid1 raid1
make -C $DATAPLANE_DIR/susres susres
make -C $DATAPLANE_DIR/app app

cp $DATAPLANE_DIR/app/vda_dataplane $ROOT_DIR/_out/linux_amd64/vda_dataplane
cp $CURR_DIR/dataplane_config.json $ROOT_DIR/_out/linux_amd64/dataplane_config.json
mkdir -p $ROOT_DIR/_out/linux_amd64/spdk
cp -r $DATAPLANE_DIR/spdk/scripts $ROOT_DIR/_out/linux_amd64/spdk/scripts
cp -r $DATAPLANE_DIR/spdk/include $ROOT_DIR/_out/linux_amd64/spdk/include
cp -r $DATAPLANE_DIR/spdk/python $ROOT_DIR/_out/linux_amd64/spdk/python
cp $DATAPLANE_DIR/rpc_plugin/vda_rpc_plugin.py $ROOT_DIR/_out/linux_amd64/spdk/python/vda_rpc_plugin.py
