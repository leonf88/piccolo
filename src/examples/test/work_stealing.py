#!/usr/bin/python

import os, sys, math
import numpy as N

shards = [int(v) for v in open('/home/power/shard_sizes').read().split() if v]
average = 0

class Bucket:
    def __init__(self, id):
        self.id = id
        self.shards = []
        self.bad = 0

    def __lt__(self, other):
        if self.bad and not other.bad: return 0
        return sum(self.shards) < sum(other.shards)
    
    def steal(self, other, reverse=0):
        other.shards.sort()
        other.shards.reverse()
        
        cutoff = sum(self.shards)
        b = 0
        i = 0
        while b < cutoff:
            b += other.shards[i]
            i += 1

        if i >= len(other.shards) - 1:
            return False
        
        if reverse:
            self.shards += [other.shards[-1]]
            del other.shards[-1]
        else:
            self.shards += [other.shards[i + 1]]
            del other.shards[i + 1]
        self.bad = 1
        return True

    def __repr__(self):
        return '%d' % sum(self.shards)

    def skew(self):
        return (sum(self.shards) - average) / average


def steal_one(buckets, rev, tries=1):
    buckets.sort()
    fastest = buckets[0]
    for i in range(tries):
        slowest = buckets[len(buckets) - i - 1]
        if fastest.steal(slowest, rev): return True
    return False


def TestStealing(bucket_count, tries):
  buckets = [Bucket(i) for i in range(bucket_count)]
  for i in range(len(shards)): buckets[i % bucket_count].shards += [shards[i]]

  for i in range(tries):
      if not steal_one(buckets, False, tries=1):
          print 'Stole: ', i
          break

  skew = sorted([b.skew() for b in buckets])
  open('workstealing.%d.fit.tries=%d' % (bucket_count, tries), 'w').write('\n'.join([str(v) for v in skew]))

def TestAssignments(bucket_count):
  global average, shards
  average = sum(shards) / float(bucket_count)
  TestStealing(bucket_count, 0)
  TestStealing(bucket_count, 10)

  shards = sorted(shards)
  shards.reverse()

  fit = [0 for i in range(bucket_count)]
  for s in shards:
      fit.sort()
      fit[0] += s

  fit = sorted([(v - average)/average for v in fit])
  open('preselect.%d.fit' % bucket_count, 'w').write('\n'.join([str(v) for v in fit]))

TestAssignments(4)
TestAssignments(12)
TestAssignments(64)
