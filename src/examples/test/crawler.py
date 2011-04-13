#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math, os

usetriggers = "false"
if len(sys.argv) > 1 and (sys.argv[1] == "-t" or sys.argv[1] == "--crawler_triggers"):
  usetriggers = "true"

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
                  '--crawler_triggers=%s' % usetriggers,
                  '--crawler_runtime=30'])
  
  runutil.run_command(cmd, n, logfile_name='Crawler')
