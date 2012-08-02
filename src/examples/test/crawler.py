#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math, os

usetriggers = "false"
if len(sys.argv) > 1 and (sys.argv[1] == "-t" or sys.argv[1] == "--crawler_triggers"):
  usetriggers = "true"

for n in [144]:#runutil.parallelism:
  os.system('mkdir -p logs.%d' % n)
  os.system('rm logs.%d/*' % n)
  cmd = ' '.join(['mpirun',
                  '-bynode',
                  '-x LD_PRELOAD=/usr/lib/openmpi/lib/libmpi.so',
                  '-hostfile conf/mpi-cluster-crawl',
#                  '-nooversubscribe',
                  '-n %d' % (1 + n),
#                  'xterm -display beaker-2:1 -hold -e gdb -ex run --args python src/examples/crawler/crawler.py',
#                  'valgrind',
                  'python src/examples/crawler/crawler.py',
                  '--log_prefix=false',
                  '-v=0',
                  '--crawler_triggers=%s' % usetriggers,
                  '--crawler_runtime=30'])
  
  runutil.run_command(cmd, n, logfile_name='Crawler')
