#!/bin/bash

NODES=$((90 * 1000 * 1000))

source $(dirname $0)/run_util.sh

function make_graph() {
  ~/share/bin/mpirun -hostfile mpi_hostfile -bynode -tag-output -n 7 \
          bash -c "LD_LIBRARY_PATH=/home/power/share/lib \
          bin/examples/example-dsm\
          --runner=Pagerank \
          --build_graph \
          --nodes=$NODES\
          --shards=72\
          --iterations=0 \
          --graph_prefix=/scratch/pagerank_test/pr"
}

function run_test() {
  run_command 'Pagerank' "--nodes=$NODES \
              --shards=72 \
              --sleep_time=0.001 \
              --iterations=10 \
              --graph_prefix=/scratch/pagerank_test/pr"
}

#make_graph
run_test
