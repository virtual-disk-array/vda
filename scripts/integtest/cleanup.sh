#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

cleanup

set +e
umount_dir "$work_dir/da0"
umount_dir "$work_dir/da1"
umount_dir "$work_dir/da2"
umount_dir "$work_dir/da3"
set -e

sleep 1

force_cleanup

sleep 1

cleanup_check
