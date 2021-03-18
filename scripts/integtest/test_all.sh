#!/bin/bash

set -e

curr_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $curr_dir/conf.sh

$curr_dir/vda_test.sh
$curr_dir/csi_test.sh
