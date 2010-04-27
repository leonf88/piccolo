#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5
base = 2500

def edge(n):
    v = round(base * pow(n, 1./3.) / 250)
    return int(v) * 250

for n in runutil.parallelism:                            
 runutil.run_command('MatrixMultiplication', 
                      n=n,
                      args=['--iterations=%s' % iterations,
                            '--sleep_time=0.0001',
                            '--work_stealing=false',
                            '--edge_size=%d' % edge(n),
                            ])
