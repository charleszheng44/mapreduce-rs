#!/usr/bin/env bash

set -eu

declare -r NUM_WORKER=${1:-5}
declare -r MR_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

for i in $(seq $NUM_WORKER);
do
    echo start worker $i
    $MR_ROOT/target/debug/mrworker $MR_ROOT/target/debug/libmrapps_wc.dylib &
done
