#!/bin/bash

set -e
source $(dirname $0)/run_util.sh

CHECKPOINT_WRITE_DIR=/scratch/checkpoints/
CHECKPOINT_READ_DIR=/scratch/cp-union/
GRAPHSIZE=10
SHARDS=100
ITERATIONS=100
BUILD_TYPE=release

PARALLELISM=$(awk -F= '{s+=$2} END {print s-1}' mpi_hostfile)
MACHINES=$(cat mpi_hostfile | wc -l)

function cleanup() {
  pdsh -g muppets rm -rf ${CHECKPOINT_WRITE_DIR}/${GRAPHSIZE}M
  pdsh -g muppets mkdir -p ${CHECKPOINT_WRITE_DIR}/${GRAPHSIZE}M
}

function make_graph() {
  ~/share/bin/mpirun \
          -hostfile mpi_hostfile \
          -bynode \
          -n $((1 + MACHINES)) \
          bash -c "LD_LIBRARY_PATH=/home/power/share/lib \
          bin/release/examples/example-dsm\
          --runner=Pagerank \
          --build_graph \
          --nodes=$((GRAPHSIZE*1000*1000))\
          --shards=$SHARDS\
          --iterations=0 \
          --work_stealing=false\
          --graph_prefix=/scratch/pagerank_test/${GRAPHSIZE}M/pr"
}

function run_test_bg() {
  run_command 'Pagerank' "\
 --nodes=$((GRAPHSIZE*1000*1000)) \
 --shards=$SHARDS \
 --sleep_time=0.001 \
 --iterations=$ITERATIONS \
 --checkpoint_write_dir=${CHECKPOINT_WRITE_DIR}/${GRAPHSIZE}M/ \
 --checkpoint_read_dir=${CHECKPOINT_READ_DIR}/${GRAPHSIZE}M/ \
 --work_stealing=true \
 --graph_prefix=/scratch/pagerank_test/${GRAPHSIZE}M/pr" $1 $2 $3 $4 $5 $6 $7 $8 & 
}

function run_test() {
  run_test_bg $1 $2 $3 $4 $5 $6 $7 $8
  wait
}

#make_graph

cleanup
RESULTS_DIR=results.checkpoint/
run_test '--checkpoint=true'

cleanup
RESULTS_DIR=results.no_checkpoint/
run_test '--checkpoint=false'

# test terminating job and trying to restore from checkpoint
cleanup
RESULTS_DIR=results.checkpoint_fault/
run_test_bg '--checkpoint=true'
sleep 180
pkill mpirun

RESULTS_DIR=results.restore_fault
run_test '--checkpoint=true' '--dead_workers=5,6,10'
