#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))

$CURR_DIR/vda_test.sh
$CURR_DIR/csi_test.sh
