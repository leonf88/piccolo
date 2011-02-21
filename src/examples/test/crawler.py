#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math, os

for n in [2]:#runutil.parallelism:
  os.system('mkdir -p logs.%d' % n)
  os.system('rm logs.%d/*' % n)
  cmd = ' '.join(['mpirun',
                  '-bynode',
                  '-hostfile conf/mpi-cluster',
#                  '-nooversubscribe',
                  '-n %d' % (1 + n),
                  'python src/examples/crawler/crawler.py',
                  '--log_prefix=false',
                  '--crawler_runtime=300'])
  
  runutil.run_command(cmd, n, logfile_name='Crawler')
