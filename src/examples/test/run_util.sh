#!/bin/bash

set -e

mkdir -p results/

function run_command() {
  runner=$1
  args=$2
  for n in 1 2 4 8 16; do 
      echo > results/$runner.n_$n
      echo "$runner :: $n"
      IFS=$(echo -e '\n')
      
      /usr/bin/time ~/share/bin/mpirun -hostfile mpi_hostfile -bynode -tag-output -n $((n + 1)) \
        bash -c "LD_LIBRARY_PATH=/home/power/share/lib bin/examples/example-dsm --runner=$runner $args --sleep_time=0.001"\
        2>&1 | while read line; do 
          echo $line >> results/$runner.n_$n
          echo $line
        done
  done
}
