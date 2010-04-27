#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5

for n in runutil.parallelism:
  runutil.run_command('MatrixMultiplication', 
                      n=n,
                      args=['--iterations=%s' % iterations,
                            '--sleep_time=0.0001',
                            '--work_stealing=false',
                            '--edge_size=%d' % (250 * int(pow(n, 1./3.)))
                            ])
