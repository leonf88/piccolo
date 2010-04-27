#!/usr/bin/python

import os
import sys; sys.path += ['src/examples/test']
import runutil, math

checkpoint_write_dir="/scratch/checkpoints/"
checkpoint_read_dir="/scratch/cp-union/checkpoints/"
base_size=100
shards=256
iterations=5

def cleanup(size):
  print "Removing old checkpoints..."
  os.system('pdsh -f20 -g muppets mkdir -p %s/%sM' % (checkpoint_write_dir, size))
  os.system('pdsh -f20 -g muppets rm -rf %s/%sM' % (checkpoint_write_dir, size))

def system(cmd):
  print cmd
  os.system(cmd)

def make_graph(size):
  system(' '.join(
                  ['/home/power/share/bin/mpirun',
                   '-hostfile mpi_hostfile',
                   '-bynode',
                   '-n %s' % (runutil.machines + 1),
                   'bash -c "LD_LIBRARY_PATH=/home/power/share/lib',
                   'bin/release/examples/example-dsm',
                   '--runner=Pagerank',
                   '--build_graph',
                   '--nodes=%s' % (size * 1000 * 1000),
                   '--shards=%s' % shards,
                   '--iterations=0',
                   '--work_stealing=false',
                   '--graph_prefix=/scratch/pagerank_test/%sM/pr"' % size]))

def run_pr(fname, size, args):
  runutil.run_command('Pagerank', 
                      n=n,
                      logfile_name=fname,
                      build_type='debug',
                      args=['--iterations=%s' % iterations,
                            '--sleep_time=0.001',
#                            '--sleep_hack=1',
                            '--nodes=%s' % (size * 1000 * 1000),
                            '--shards=%s' % shards,
                            '--work_stealing=true',
                            '--checkpoint_write_dir=%s/%sM' % (checkpoint_write_dir, size),
                            '--checkpoint_read_dir=%s/%sM' % (checkpoint_read_dir, size),
                            '--graph_prefix=/scratch/pagerank_test/%sM/pr' % (size),
                            ] + args)

make_graph(base_size)
for n in runutil.parallelism[:1]:
  graphsize = base_size
  #cleanup(graphsize)
  run_pr('Pagerank.no_checkpoint', graphsize, ['--checkpoint=false'])
  
#  cleanup(graphsize)
#  run_pr('Pagerank.checkpoint', graphsize, ['--checkpoint=true'])


# test terminating job and trying to restore from checkpoint
#cleanup()
#run_pr('Pagerank.checkpoint_fault', ['--checkpoint=true'])

#sleep 180
#pkill mpirun

#RESULTS_DIR=results.restore_fault
#run_test '--checkpoint=true' '--dead_workers=5,6,10'
#run_pr('Pagerank.restore_fault', ['--checkpoint=true', '--dead_workers=5,6,10'])