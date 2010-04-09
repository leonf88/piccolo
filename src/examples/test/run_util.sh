#!/bin/bash

set -e

mkdir -p results/
#strace -c -f -o results/trace.$n.\$BASHPID 

function run_command() {
  runner=$1
  args=$2
  for n in 6 12 18 23; do 
      pdsh -g muppets -f 100 -l root 'echo 3 > /proc/sys/vm/drop_caches'
      echo > results/$runner.n_$n
      echo "$runner :: $n"
      IFS=$(echo -e '\n')
      
      /usr/bin/time ~/share/bin/mpirun \
         --mca mpi_paffinity_alone 1 \
         -hostfile mpi_hostfile\
         -bynode \
         -tag-output -n $((n + 1)) \
        bash -c "\
              LD_LIBRARY_PATH=/home/power/share/lib \
              bin/examples/example-dsm \
        			--runner=$runner \
        			$args "\
        2>&1 | while read line; do 
          echo $line >> results/$runner.n_$n
          echo $line
        done
      if [[ $? -ne 0 ]]; then
        exit 1
      fi
  done
}
