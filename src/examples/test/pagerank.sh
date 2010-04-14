#!/bin/bash

set -e
source $(dirname $0)/run_util.sh

PARALLELISM=$(awk -F= '{s+=$2} END {print s-1}' mpi_hostfile)
MACHINES=$(cat mpi_hostfile | wc -l)

GRAPHSIZE=500
SHARDS=100

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

function run_test() {

pdsh -f 100 -g muppets "rm -rf /scratch/checkpoints/${GRAPHSIZE}M/"
pdsh -f 100 -g muppets "mkdir -p /scratch/checkpoints/${GRAPHSIZE}M"

  run_command 'Pagerank' "\
 --nodes=$((GRAPHSIZE*1000*1000)) \
 --shards=$SHARDS \
 --sleep_time=0.001 \
 --iterations=5 \
 --checkpoint_dir=/scratch/checkpoints/${GRAPHSIZE}M/ \
 --work_stealing=true \
 --checkpoint=$1 \
 --graph_prefix=/scratch/pagerank_test/${GRAPHSIZE}M/pr"
}

make_graph

RESULTS_DIR=results.cp/
run_test true

RESULTS_DIR=results.no_cp/
run_test false
