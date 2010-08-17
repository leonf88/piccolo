#!/usr/bin/python

import sys; sys.path += ['src/examples/test']
import runutil, math
iterations=10
base_size=250 * 1000 * 1000

def test_scaled_perf():
  for n in runutil.parallelism[4:]:
      runutil.run_example('KMeans',
                          logfile_name='KMeans.scaled_size', 
                          n=n,
                          #build_type='debug',
                          args=['--iterations=%s' % iterations,
                                '--sleep_time=0.00001',
                                '--work_stealing=false',
                                '--num_dists=100',
                                '--num_points=%d' % (base_size*n)
                                ])

def test_fixed_perf():
  for n in runutil.parallelism:
      runutil.run_example('KMeans',
                          logfile_name='KMeans.fixed_size', 
                          n=n,
                          args=['--iterations=%s' % iterations,
                                '--sleep_time=0.001',
                                '--work_stealing=false',
                                '--num_dists=64',
                                '--num_points=%d' % (base_size)
                                ])

def test_checkpoint():
  runutil.run_example('KMeans',
                      logfile_name='KMeans.checkpoint', 
                      n=64,
                      args=['--iterations=%s' % iterations,
                            '--sleep_time=0.001',
                            '--checkpoint_read_dir=/scratch/power/checkpoints/kmeans.%d' % base_size,
                            '--checkpoint_write_dir=/scratch/power/checkpoints/kmeans.%d' % base_size,
                            '--checkpoint=true',
                            '--restore=true',
                            '--work_stealing=false',
                            '--num_dists=64',
                            '--num_points=%d' % (base_size),
                            '--cpu_profile',
                            ])


#test_scaled_perf()
#test_fixed_perf()
test_checkpoint()
