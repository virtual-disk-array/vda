#!/bin/bash

set -e

CURR_DIR=$(readlink -f $(dirname $0))
source $CURR_DIR/conf.sh
source $CURR_DIR/utils.sh

cleanup

set +e
umount_dir "$WORK_DIR/da0"
umount_dir "$WORK_DIR/da1"
umount_dir "$WORK_DIR/da2"
umount_dir "$WORK_DIR/da3"
set -e

sleep 1

force_cleanup

sleep 1

cleanup_check
