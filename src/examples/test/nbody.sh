#!/bin/bash

set -e
source $(dirname $0)/run_util.sh

CHECKPOINT_WRITE_DIR=/scratch/checkpoints/
CHECKPOINT_READ_DIR=/scratch/cp-union/checkpoints/
ITERATIONS=10
PARALLELISM=10
#BUILD_TYPE=debug
BUILD_TYPE=release

function run_test() {
  run_command 'NBody' "\
 --particles=1000000 \
 --log_prefix=false \
 --sleep_time=0.001 \
 --iterations=$ITERATIONS \
 --work_stealing=true $1 $2 $3 $4 $5 $6 $7 $8 "
}


run_test 
