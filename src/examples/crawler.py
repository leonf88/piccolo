#!/usr/bin/python

import os, sys

import urllib, urlparse, cgi, httplib, urllib2
import crawler_support as cs
import time
import re

import logging
from urllib2 import HTTPError
logging.basicConfig(level=logging.DEBUG)

def debug(fmt, *args, **kwargs): logging.debug(str(fmt) % args)
def info(fmt, *args, **kwargs): logging.info(str(fmt) % args)
def warn(fmt, *args, **kwargs): logging.warn(str(fmt) % args)
def error(fmt, *args, **kwargs): logging.error(str(fmt) % args)
def fatal(fmt, *args, **kwargs): logging.fatal(str(fmt) % args)

from threading import *
from Queue import PriorityQueue, Queue, Empty, Full  

C_SHOULD_FETCH = 0
C_FETCHING = 1
C_FETCH_DONE = 2
C_PARSE_DONE = 3
C_DONE = 4
C_ROBOTS_BLACKLIST = 5

crawl_queue = Queue()
robots_queue = Queue()

fetch_table = None
crawltime_table = None
robots_table = None

output_log = open('crawler.out', 'w')

R_FETCHING = 'FETCHING'
ROBOTS_REJECT_ALL = '/'
ROBOTS_TIMEOUT = 10

DISALLOW_RE = re.compile('Disallow:(.*)')


def initialize():
    global fetch_table, crawltime_table, robots_table
    fetch_table = cs.kernel().crawl_table(0)
    crawltime_table = cs.kernel().crawl_table(1)
    robots_table = cs.kernel().robots_table(2)
    fetch_table.put("http://rjpower.org/gitweb", C_SHOULD_FETCH)
    return 0

def check_robots(url):
    disallowed = robots_table.get(url.netloc).split('\n')
    for d in disallowed:
        if not d: continue
        if url.path.startswith(d): return False
    return True

def fetch_robots(site):
    info('Fetching robots... %s', site)
    disallow = {}
    try:
        rtxt = urllib2.urlopen('http://%s/robots.txt' % site, timeout=ROBOTS_TIMEOUT).read()
    except HTTPError, e:
        if e.code == 404: rtxt = ''
        else: raise e
    except:
        info('Failed robots fetch for %s: %s', site, e)
        robots_table.put(site, ROBOTS_REJECT_ALL)
        return
    
    for l in rtxt.split('\n'):
        g = DISALLOW_RE.search(l)
        if g: 
            path = g.group(1).strip()
            if path: disallow[path] = 1
            
    robots_table.put(site, '\n'.join(disallow.keys()))
    info('Robots fetch of %s successful: %s', site, ','.join(disallow.keys()))

def fetch_page(url):
    url_s = url.geturl()
    info('Fetching... %s', url_s)
    try:
        return urllib2.urlopen(url_s).read()
    except:
        warn('Fetch of %s failed.', url_s.geturl())
        return ''


def extract_links(url, content):
    info('Extracting... %s', url)

class CrawlThread(Thread):
    def __init__(self): 
        Thread.__init__(self)

    def run(self):
        while 1:
            try:
                site = robots_queue.get(0)
                fetch_robots(site)
                continue # keep reading until we run out of robot fetches to do.
            except Empty: pass

            try:
                url = crawl_queue.get(1, 0.1)
                data = fetch_page(url)
                outlinks = extract_links(data)
                
            except Empty: pass
                            
            #data = urllib2.urlopen(url).read(
            
def crawl():
    global fetch_table, robots_table    
    threads = [CrawlThread() for i in range(50)]
    for t in threads: t.start()
        
    while 1:
        it = fetch_table.get_typed_iterator(cs.kernel().current_shard())
        while not it.done():
            time.sleep(0.1)
            url_s = it.key()
            if it.value() == C_SHOULD_FETCH:
                info('Checking... %s', it.key())
                url = urlparse.urlparse(url_s)
                site = url.netloc
                if not robots_table.contains(site):
                    info('Fetching robots: %s', site)
                    robots_queue.put(site)
                    robots_table.put(site, R_FETCHING)
                    it.Next()
                    continue         
                
                if robots_table.get(site) == R_FETCHING:
                    info('Waiting for robot fetch: %s', url_s)
                    it.Next()
                    continue
                
                if not check_robots(url):
                    info('Blocked by robots %s', url_s)
                    fetch_table.put(url_s, C_ROBOTS_BLACKLIST)
                    it.Next()
                    continue
                  
                last_crawl = 0
                if crawltime_table.contains(site):
                    last_crawl = crawltime_table.get(site)
                
                if time.time() - last_crawl < 60:
                    info('Waiting for politeness: %s, %d', url_s, time.time() - last_crawl)
                else:
                    info('Queueing: %s', url_s)
                    crawl_queue.put(url)
                    fetch_table.put(url_s, C_FETCHING)
                    crawltime_table.put(site, int(time.time()))                
                    it.Next()
    