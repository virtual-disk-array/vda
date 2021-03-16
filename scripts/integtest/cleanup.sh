#!/bin/bash

curr_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $curr_dir/utils.sh

cleanup
cleanup_check
