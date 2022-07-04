#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$CURR_DIR/../..
DATAPLANE_DIR=$ROOT_DIR/dataplane

export VDA_DATAPLANE_ARCHITECTURE=core2
export VDA_DATAPLANE_DEBUG=n

# bash $CURR_DIR/dataplane_clean.sh
bash $CURR_DIR/dataplane_build.sh

cp $DATAPLANE_DIR/app/vda_dataplane $ROOT_DIR/_out/linux_amd64/vda_dataplane
cp $CURR_DIR/dataplane_config.json $ROOT_DIR/_out/linux_amd64/dataplane_config.json
rm -rf $ROOT_DIR/_out/linux_amd64/spdk
mkdir -p $ROOT_DIR/_out/linux_amd64/spdk
cp -r $DATAPLANE_DIR/spdk/scripts $ROOT_DIR/_out/linux_amd64/spdk/scripts
cp -r $DATAPLANE_DIR/spdk/include $ROOT_DIR/_out/linux_amd64/spdk/include
