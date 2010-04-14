#!/bin/bash

set -e
source $(dirname $0)/run_util.sh

CHECKPOINT_DIR=/home/power/w/hashdsm/checkpoints
GRAPHSIZE=10
SHARDS=100
ITERATIONS=25

PARALLELISM=$(awk -F= '{s+=$2} END {print s-1}' mpi_hostfile)
MACHINES=$(cat mpi_hostfile | wc -l)


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

pdsh -f 100 -g muppets "rm -rf ${CHECKPOINT_DIR}/${GRAPHSIZE}M/ 2>/dev/null"
pdsh -f 100 -g muppets "mkdir -p ${CHECKPOINT_DIR}/${GRAPHSIZE}M"

  run_command 'Pagerank' "\
 --nodes=$((GRAPHSIZE*1000*1000)) \
 --shards=$SHARDS \
 --sleep_time=0.001 \
 --iterations=$ITERATIONS \
 --checkpoint_dir=${CHECKPOINT_DIR}/${GRAPHSIZE}M/ \
 --work_stealing=true \
 --graph_prefix=/scratch/pagerank_test/${GRAPHSIZE}M/pr" $1 $2 $3 $4 $5 $6 $7
}

#make_graph

#RESULTS_DIR=results.cp/
#run_test '--checkpoint=true'

#RESULTS_DIR=results.no_cp/
#run_test '--checkpoint=false'

RESULTS_DIR=results.fault/
run_test '--checkpoint=true' '--failure_simulation_interval=15'
