#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5

for n in [32, 16, 8, 4, 2]:
  runutil.run_command('NBody', 
                      n=n,
                      args=['--particles=%s' % 5000000,
                            'log_prefix=false',
                            'sleep_time=0.0001',
                            'iterations=%s' % iterations,
                            'work_stealing=false'])

