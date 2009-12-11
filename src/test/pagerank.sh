#!/bin/bash

set -e
set -x

nodes=4096
shards=64
export PYTHONPATH=bin/master
export LD_LIBRARY_PATH=extlib/mpich2/lib

trap 
for i in 4 8 16 32; do
  for s in 0.1 1 10; do
     pkill -9 -f test-pagerank || true;
     ldir=pagerank.p$i.s$s.async
     mkdir -p $ldir
     rm -f $ldir/*
     FLAGS="--graph_size=$nodes --shards=$shards --log_dir=$ldir --localtest --workers=$i --cpu_profile=$ldir/prof --sleep_time=$s"
     bin/test/test-pagerank $FLAGS 2>$ldir/err > $ldir/convergence
     
     pkill -9 -f test-pagerank || true;
     ldir=pagerank.p$i.s$s.sync
     mkdir -p $ldir
     rm -f $ldir/*
     FLAGS="--graph_size=$nodes --shards=$shards --log_dir=$ldir --localtest --workers=$i --cpu_profile=$ldir/prof --sleep_time=$s"
     bin/test/test-pagerank $FLAGS --synchronous 2>$ldir/err > $ldir/convergence 
  done
done

