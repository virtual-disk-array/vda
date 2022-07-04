#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$CURR_DIR/../..
DATAPLANE_DIR=$ROOT_DIR/dataplane

if [ "$1" == "all" ]; then
    rm -rf $DATAPLANE_DIR/spdk
else
    cd $DATAPLANE_DIR/spdk
    make clean
fi
rm -f $DATAPLANE_DIR/raid1/*.[oa]
rm -f $DATAPLANE_DIR/susres/*.[oa]
rm -f $DATAPLANE_DIR/app/vda_dataplane
