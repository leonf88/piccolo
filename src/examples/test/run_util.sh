#!/bin/bash

BUILD_TYPE=release

NUM_CORES=$(awk -F= '{s+=$2} END {print s}' mpi_hostfile)
MACHINES=$(awk '{print $1}' mpi_hostfile | sort | uniq | wc -l)

echo "Running with $MACHINES machines, $NUM_CORES cores"

RESULTS_DIR=results/
PARALLELISM="71"
#PARALLELISM="6 12 24 48"
#strace -c -f -o results/trace.$n.\$BASHPID 

function run_command() {
  echo "Writing output to: $RESULTS_DIR"
  mkdir -p $RESULTS_DIR
  runner=$1
  
  for n in $PARALLELISM; do 
      #pdsh -g muppets -f 100 -l root 'echo 3 > /proc/sys/vm/drop_caches'
      pdsh -g muppets -f 100 'pkill -9 -f example-dsm' 
      echo > $RESULTS_DIR/$runner.n_$n
      echo "$runner :: $n"
      IFS=$(echo -e '\n')

      if [[ $n != $NUM_CORES ]]; then 
        AFFINITY=1
      else
        AFFINITY=0
      fi

      echo "Processor affinity: " $AFFINITY

        # --output-filename $RESULTS_DIR/stderr.$n \
      /usr/bin/time ~/share/bin/mpirun \
         -mca mpi_paffinity_alone $AFFINITY \
         -hostfile mpi_hostfile\
         -bycore \
         -display-map \
         -tag-output \
         -n $((n + 1)) \
	        bash -c "\
	              LD_LIBRARY_PATH=/home/power/share/lib \
	              bin/$BUILD_TYPE/examples/example-dsm \
	        			--runner=$runner \
	        			$2 $3 $4 $5 $6 $7 " 2> $RESULTS_DIR/$runner.n_$n
  done
}
