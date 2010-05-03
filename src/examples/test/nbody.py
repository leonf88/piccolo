#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5
base_size=5000000

for n in runutil.parallelism:
  runutil.run_example('nbody', 
                      n=n,
                      logfile_name='nbody.scaled_size',
                      args=['--particles=%s' % (base_size * n),
                            '--log_prefix=false',
                            '--sleep_time=0.00001',
                            '--iterations=%s' % iterations,
                            '--work_stealing=false'])

  runutil.run_example('nbody', 
                      n=n,
                      logfile_name='nbody.fixed_size',
                      args=['--particles=%s' % base_size,
                            '--log_prefix=false',
                            '--sleep_time=0.00001',
                            '--iterations=%s' % iterations,
                            '--work_stealing=false'])

