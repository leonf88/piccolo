#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math

checkpoint_write_dir="/scratch/checkpoints/"
checkpoint_read_dir="/scratch/cp-union/checkpoints/"
graphsize=900
shards=384
iterations=10
#build_type=debug
build_type=release


def cleanup():
  print "Removing old checkpoints..."
  os.system('pdsh -f20 -g muppets mkdir -p %s/%sM' % (checkpoint_write_dir, graphsize))
  os.system('pdsh -f20 -g muppets rm -rf %s/%sM' % (checkpoint_write_dir, graphsize))

def make_graph():
  os.system(' '.join(
                    ['~/share/bin/mpirun'
                     '-hostfile mpi_hostfile',
                     '-bynode',
                     '-n %s' % (runutil.machines + 1),
                     'bash -c "LD_LIBRARY_PATH=/home/power/share/lib',
                     'bin/release/examples/example-dsm',
                     '--runner=Pagerank',
                     '--build_graph',
                     '--nodes=%s' % (graphsize * 1000 * 1000),
                     '--shards=%s' % shards,
                     '--iterations=0',
                     '--work_stealing=false',
                     '--graph_prefix=/scratch/pagerank_test/%sM/pr"' % graphsize]))

#make_graph()

def run_pr(fname, args):
  runutil.run_command('Pagerank', 
                      n=n,
                      logfile_name=fname,
                      args=['--iterations=%s' % iterations,
                            '--sleep_time=0.0001',
                            '--nodes=%s' % graphsize * 1000 * 1000,
                            '--shards=%s' % shards,
                            '--checkpoint_write_dir=%s/%sM' % (checkpoint_write_dir, graphsize),
                            '--checkpoint_read_dir=%s/%sM' % (checkpoint_read_dir, graphsize),
                            '--work_stealing=true',
                            '--graph_prefix=/scratch/pagerank_test/%sM/pr' % (graphsize),
                            ] + args)

for n in runutil.parallelism:
  cleanup()
  run_pr('Pagerank.no_checkpoint', ['--checkpoint=false'])
  
  cleanup()
  run_pr('Pagerank.checkpoint', ['--checkpoint=true'])


# test terminating job and trying to restore from checkpoint
#cleanup()
#run_pr('Pagerank.checkpoint_fault', ['--checkpoint=true'])

#sleep 180
#pkill mpirun

#RESULTS_DIR=results.restore_fault
#run_test '--checkpoint=true' '--dead_workers=5,6,10'
#run_pr('Pagerank.restore_fault', ['--checkpoint=true', '--dead_workers=5,6,10'])
