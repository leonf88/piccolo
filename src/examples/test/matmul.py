#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5
base = 3000

def edge(n):
    v = round(base * pow(n, 1./3.) / 250)
    return int(v) * 250

for n in runutil.parallelism:                            
   runutil.run_example('MatrixMultiplication', 
                        n=n,
                        logfile_name='MatrixMultiplication.scaled_size',
                        args=['--iterations=%s' % iterations,
                              '--sleep_time=0.0001',
                              '--work_stealing=false',
                              '--edge_size=%d' % edge(n),
                              ])
   
   runutil.run_example('MatrixMultiplication', 
                        n=n,
                        logfile_name='MatrixMultiplication.fixed_size',
                        args=['--iterations=%s' % iterations,
                              '--sleep_time=0.0001',
                              '--work_stealing=false',
                              '--edge_size=%d' % base_size,
                              ])
