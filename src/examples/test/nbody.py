#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5
base_size=100000

def particles(n):
  return int(math.sqrt(n) * base_size)

for n in runutil.parallelism:
  runutil.run_example('NBody', 
                      n=n,
                      logfile_name='nbody.scaled_size',
                      args=['--particles=%s' % particles(n),
                            '--log_prefix=false',
                            '--sleep_time=0.00001',
                            '--iterations=%s' % iterations,
                            '--work_stealing=false'])

for n in runutil.parallelism:
  runutil.run_example('NBody', 
                      n=n,
                      logfile_name='nbody.fixed_size',
                      args=['--particles=%s' % base_size,
                            '--log_prefix=false',
                            '--sleep_time=0.00001',
                            '--iterations=%s' % iterations,
                            '--work_stealing=false'])

