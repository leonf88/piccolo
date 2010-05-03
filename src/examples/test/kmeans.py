#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=5
base_size=100 * 1000 * 1000
#
#for n in runutil.parallelism:
#  try:
#    runutil.run_example('KMeans',
#                        logfile_name='KMeans.scaled_size', 
#                        n=n,
#                        #build_type='debug',
#                        args=['--iterations=%s' % iterations,
#                              '--sleep_time=0.001',
#                              '--work_stealing=false',
#                              '--num_dists=100',
#                              '--num_points=%d' % (base_size*n)
#                              ])
#  except: 
#    print 'Error running command!'

for n in runutil.parallelism:
  try:
    runutil.run_example('KMeans',
                        logfile_name='KMeans.fixed_size', 
                        n=n,
                        args=['--iterations=%s' % iterations,
                              '--sleep_time=0.001',
                              '--work_stealing=false',
                              '--num_dists=100',
                              '--num_points=%d' % (base_size)
                              ])
  except: 
    print 'Error running command!'
