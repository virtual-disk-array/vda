#!/bin/bash

curr_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $curr_dir/conf.sh
source $curr_dir/utils.sh

cleanup

umount_dir "$work_dir/da0"
umount_dir "$work_dir/da1"
umount_dir "$work_dir/da2"
umount_dir "$work_dir/da3"

sleep 1

force_cleanup

sleep 1

cleanup_check
