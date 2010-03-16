#!/bin/bash

set -e

rm -f matmul.n*

for n in 1 2 4 8 16 20; do 
  for j in 0; do
    echo $n $j
    /usr/bin/time ~/share/bin/mpirun -hostfile mpi_hostfile -bynode  -tag-output -n $((n + 1)) \
      bash -c 'LD_LIBRARY_PATH=/home/power/share/lib bin/example-dsm --runner=MatrixMultiplication --edge_size=2000' &> matmul.n$n.j$j
  done
done
