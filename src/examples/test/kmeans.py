#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5

for n in runutil.parallelism:
  runutil.run_command('KMeans', 
                      n=n,
                      args=['--iterations=%s' % iterations,
                            '--sleep_time=0.0001',
                            '--work_stealing=false',
                            '--num_dists=100',
                            '--num_points=%d' % (50000000*n)
                            ])


