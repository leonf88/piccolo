#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=2

for n in runutil.parallelism:
  runutil.run_command('NBody', 
                      n=n,
                      args=['--particles=%s' % 5000000,
                            '--log_prefix=false',
                            '--sleep_time=0.00001',
                            '--iterations=%s' % iterations,
                            '--work_stealing=false'])

