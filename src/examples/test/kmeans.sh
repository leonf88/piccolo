#!/bin/bash

set -e

rm -f kmeans.n*

for n in 4 8 16 20; do 
  for j in 0; do
    echo $n $j
    /usr/bin/time ~/share/bin/mpirun -hostfile mpi_hostfile -bynode  -tag-output -n $n \
      bash -c 'LD_LIBRARY_PATH=/home/power/share/lib bin/example-dsm --runner=KMeans --iterations=50 --num_dists=10000 --num_points=500000 --sleep_time=0' &> kmeans.n_$n.r_$j
  done
done
