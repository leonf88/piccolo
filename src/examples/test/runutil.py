#!/usr/bin/python

import time
import os, sys, re, subprocess

import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)

parallelism = [64, 32, 16, 8, 4, 2]
num_cores = machines = 0
mdict = {}
for l in open('mpi_hostfile').readlines():
  num_cores += int(l.split('=')[1])
  mdict[l.split()[0]] = 1
machines = len(mdict)


def system(*args):
  os.system(*args)

logfile = None
def log(fmt, *args):
  print fmt % args
  print >>logfile, fmt % args
  logfile.flush()
    
def run_command(runner, 
                n=64, 
                build_type='release',
                results_dir='results',
                logfile_name=None,
                args=None):
  if not logfile_name: logfile_name = runner
  if not args: args = []

  output_dir="%s.%s" % (results_dir, n)
  system('mkdir -p %s' % output_dir)
  global logfile

  logfile = open('%s/%s' % (output_dir, logfile_name), 'w')

  log("Running with %s machines, %s cores" % (machines, num_cores))

  system('rm -f profile/*')
  log("Writing output to: %s", output_dir)
  log("Wiping cache...")
  #system("pdsh -f 100 -g muppets -l root 'echo 3 > /proc/sys/vm/drop_caches'")
  log("Killing existing workers...")
  #system("pdsh -f 100 -g muppets 'pkill -9 example-dsm || true'")

  affinity = 0 if n >= num_cores else 1

  log("Runner: %s", runner)
  log("Parallelism: %s", n)
  log("Processor affinity: %s", affinity)

  cmd = ' '.join(['/home/power/share/bin/mpirun',
                  '-mca mpi_paffinity_alone %s' % affinity,
                  '-hostfile mpi_hostfile',
                  '-bynode',
                  '-nooversubscribe',
                  '-tag-output ',
                  '-n %s ' % (n + 1),
                  'bash -c "',
                  'LD_LIBRARY_PATH=/home/power/share/lib',
                  'bin/%s/examples/example-dsm' % build_type,
                  '--runner=%s' % runner,
                  '--log_prefix=false']
                  + args + 
                  ['"'])
  
  log('Command: %s', cmd)
  start = time.time()
  handle = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            shell=True)

  while handle.returncode == None:
    handle.poll()
    l = handle.stdout.readline().strip()
    log(l)

  end = time.time()
  log('Finished in: %s', end - start)
