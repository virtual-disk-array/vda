#!/bin/bash

set -e

curr_dir=$(readlink -f $(dirname $0))
dataplane_dir=$curr_dir/../dataplane
out_dir=$curr_dir/../_out

export TARGET_ARCHITECTURE=core2
bash $curr_dir/dataplane_prepare.sh
bash $curr_dir/dataplane_build.sh

cp $dataplane_dir/app/vda_dataplane $out_dir/linux_amd64/vda_dataplane
cp $curr_dir/dataplane_config.json $out_dir/linux_amd64/dataplane_config.json
rm -rf $out_dir/linux_amd64/spdk
mkdir -p $out_dir/linux_amd64/spdk
cp -r $dataplane_dir/spdk/scripts $out_dir/linux_amd64/spdk/scripts
cp -r $dataplane_dir/spdk/include $out_dir/linux_amd64/spdk/include
