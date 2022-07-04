#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$CURR_DIR/../..
DATAPLANE_DIR=$ROOT_DIR/dataplane

rm -rf $DATAPLANE_DIR/spdk
rm -f $DATAPLANE_DIR/*.[oa]
rm -f $DATAPLANE_DIR/vda_dataplane
