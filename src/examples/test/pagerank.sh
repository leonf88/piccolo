#!/bin/bash

set -e
source $(dirname $0)/run_util.sh

GRAPHSIZE=10
MACHINES=$(cat mpi_hostfile | wc -l)
SHARDS=$((MACHINES * 10))

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
  run_command 'Pagerank' "\
 --nodes=$((GRAPHSIZE*1000*1000)) \
 --shards=$SHARDS \
 --sleep_time=0.001 \
 --iterations=50 \
 --work_stealing=$1 \
 --graph_prefix=/scratch/pagerank_test/${GRAPHSIZE}M/pr"
}

#make_graph

PARALLELISM=$(awk -F= '{s+=$2} END {print s-1}' mpi_hostfile)
RESULTS_DIR=results.workstealing/
run_test true

RESULTS_DIR=results.noworkstealing/
run_test false
