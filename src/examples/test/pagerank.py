#!/usr/bin/python

import traceback
import os
import sys; sys.path += ['src/examples/test']
import runutil, math

checkpoint_write_dir="/scratch/checkpoints/"
checkpoint_read_dir="/scratch/cp-union/checkpoints/"
scaled_base_size=5
fixed_base_size=100
shards=256
memory_graph=1

def cleanup(size):
  print "Removing old checkpoints..."
  os.system('pdsh -f20 -g muppets mkdir -p %s/%sM' % (checkpoint_write_dir, size))
  os.system('pdsh -f20 -g muppets rm -rf %s/%sM' % (checkpoint_write_dir, size))

def system(cmd):
  print cmd
  os.system(cmd)

def make_graph(size, hostfile='fast_hostfile'):
  if memory_graph: return
  system(' '.join(
                  ['/home/power/share/bin/mpirun',
                   '-hostfile %s' % hostfile,
                   '-bynode',
                   '-n %s' % runutil.hostfile_info(hostfile)[1],
                   'bash -c "LD_LIBRARY_PATH=/home/power/share/lib',
                   'bin/release/examples/example-dsm',
                   '--runner=Pagerank',
                   '--build_graph',
                   '--nodes=%s' % (size * 1000 * 1000),
                   '--shards=%s' % shards,
                   '--iterations=0',
                   '--work_stealing=false',
                   '--graph_prefix=/scratch/pagerank_test/%sM/pr"' % size]))

def run_pr(fname, size, n, args=None, **kw):
  try:
    runutil.run_example('Pagerank', 
                        n=n,
                        logfile_name=fname,
                        build_type='release',
                        args=['--iterations=%s' % 3,
                              '--sleep_time=0.001',
                              '--nodes=%s' % (size * 1000 * 1000),
                              '--memory_graph=%d' % memory_graph,
                              '--shards=%s' % shards,
                              '--work_stealing=true',
                              '--checkpoint_write_dir=%s/%sM' % (checkpoint_write_dir, size),
                              '--checkpoint_read_dir=%s/%sM' % (checkpoint_read_dir, size),
                              '--graph_prefix=/scratch/pagerank_test/%sM/pr' % (size),
                              ] + args,
                        **kw)
  except SystemError:
    traceback.print_exc()
    return

# test scaling with work size, and with a fixed size of data
def test_scaled_perf():
  for n in runutil.parallelism[5:]:
    graphsize = scaled_base_size * n
    make_graph(graphsize)
    run_pr('Pagerank.scaled_size', graphsize, n, ['--checkpoint=false'])


def test_fixed_perf():
  for n in runutil.parallelism:  
    graphsize = fixed_base_size
    make_graph(graphsize)
    run_pr('Pagerank.fixed_size', graphsize, n, ['--checkpoint=false'])


def test_work_stealing():
  make_graph(graphsize, hostfile='slow_hostfile')
  run_pr('Pagerank.with_stealing', 
         graphsize,
         n,
         hostfile='slow_hostfile',
         args=['--checkpoint=false', '--work_stealing=true'])

def test_checkpointing():    
  cleanup()
  os.popen('sleep 120; pkill mpirun')
  run_pr('Pagerank.checkpoint_fault', ['--checkpoint=true'])  
  run_pr('Pagerank.restore_fault', ['--checkpoint=true', '--dead_workers=5,6,10'])

test_fixed_perf()
#test_scaled_perf()
