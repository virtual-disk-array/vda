#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$CURR_DIR/../..
DATAPLANE_DIR=$ROOT_DIR/dataplane

export TARGET_ARCHITECTURE=${VDA_DATAPLANE_ARCHITECTURE:-core2}

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
