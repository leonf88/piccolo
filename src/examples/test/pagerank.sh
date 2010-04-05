#!/bin/bash

source $(dirname $0)/run_util.sh

~/share/bin/mpirun -hostfile mpi_hostfile -bynode -tag-output -n 16 \
        bash -c "LD_LIBRARY_PATH=/home/power/share/lib bin/examples/example-dsm --runner=Pagerank --build_graph --nodes=100000000 --shards=64 --graph_prefix=/scratch/100M/testgraph"
        
#run_command 'Pagerank' '--nodes=50000000 --shards=64 --graph_prefix=/scratch/100M/testgraph'
