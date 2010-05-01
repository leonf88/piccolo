#!/usr/bin/python

import time
import os, sys, re, subprocess

import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)

parallelism = [64, 32, 16, 8, 4, 2]

def hostfile_info(f):
  cores = machines = 0
  mdict = {}
  for l in open(f).readlines():
    cores += int(l.split('=')[1])
    mdict[l.split()[0]] = 1
    machines = len(mdict)
  return cores, machines


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
                hostfile='fast_hostfile',
                logfile_name=None,
                args=None):
  if not logfile_name: logfile_name = runner
  if not args: args = []

  output_dir="%s.%s" % (results_dir, n)
  system('mkdir -p %s' % output_dir)
  global logfile

  logfile = open('%s/%s' % (output_dir, logfile_name), 'w')

  machines, cores = hostfile_info(hostfile)
  log("Running with %s machines, %s cores" % (machines, cores))

  system('rm -f profile/*')
  log("Writing output to: %s", output_dir)
  log("Wiping cache...")
  #system("pdsh -f 100 -g muppets -l root 'echo 3 > /proc/sys/vm/drop_caches'")
  log("Killing existing workers...")
  #system("pdsh -f 100 -g muppets 'pkill -9 example-dsm || true'")

  affinity = 0 if n >= cores else 1

  log("Runner: %s", runner)
  log("Parallelism: %s", n)
  log("Processor affinity: %s", affinity)

  cmd = ' '.join(['/home/power/share/bin/mpirun',
                  '-mca mpi_paffinity_alone %s' % affinity,
                  '-hostfile %s' % hostfile,
                  '-bynode',
                  '-nooversubscribe',
                  '-tag-output ',
                  '-display-map',
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
    if l: log(l)

  end = time.time()
  log('Finished in: %s', end - start)

  if handle.returncode != 0:
    raise SystemError, 'Command failed'
