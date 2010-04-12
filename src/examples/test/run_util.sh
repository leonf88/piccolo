#!/bin/bash

NUM_CORES=24
RESULTS_DIR=results/
PARALLELISM="6 12 18 23"

#strace -c -f -o results/trace.$n.\$BASHPID 

function run_command() {
  echo $RESULTS_DIR
  mkdir -p $RESULTS_DIR
  runner=$1
  args=$2
  for n in $PARALLELISM; do 
      pdsh -g muppets -f 100 -l root 'echo 3 > /proc/sys/vm/drop_caches'
      echo > $RESULTS_DIR/$runner.n_$n
      echo "$runner :: $n"
      IFS=$(echo -e '\n')

      if [[ $n != $NUM_CORES ]]; then 
        AFFINITY=1
      else
        AFFINITY=0
      fi

      /usr/bin/time ~/share/bin/mpirun \
         -mca mpi_paffinity_alone $AFFINITY \
         -hostfile mpi_hostfile\
         -byslot \
         -tag-output -n $((n + 1)) \
        bash -c "\
              LD_LIBRARY_PATH=/home/power/share/lib \
              bin/debug/examples/example-dsm \
        			--runner=$runner \
        			$args "\
        2>&1 | while read line; do 
          echo $line >> $RESULTS_DIR/$runner.n_$n
          echo $line
        done
      if [[ $? -ne 0 ]]; then
        exit 1
      fi
  done
}
