#!/usr/bin/python

import os, sys

import urllib, urlparse, cgi, httplib, urllib2
import crawler_support as cs
import time

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
ROBOTS_REJECT_ALL = '''User-agent: *
Disallow: /*
'''

class CrawlThread(Thread):
    def __init__(self): 
        Thread.__init__(self)
    
    def run(self):
        while 1:
            try:
                site = robots_queue.get(0)
                print 'Fetching... ', site
                robots_table.put(site, urllib2.urlopen('http://%s/robots.txt' % site).read())
                continue # keep reading until we run out of robot fetches to do.
            except Empty: pass
            except:
                print 'Failed robots fetch for ', site
                robots_table.put(site, ROBOTS_REJECT_ALL)

            try:
                url = crawl_queue.get(1, 0.1)
                print 'Fetching... ', url                
            except Empty: pass
                            
            #data = urllib2.urlopen(url).read(

def initialize():
    global fetch_table, crawltime_table, robots_table
    fetch_table = cs.kernel().crawl_table(0)
    crawltime_table = cs.kernel().crawl_table(1)
    robots_table = cs.kernel().robots_table(2)
    fetch_table.put("http://www.yahoo.com", C_SHOULD_FETCH)
    return 0

def check_robots(site, url):
    robots_data = robots_table.get(site)
    

def crawl():
    global fetch_table, robots_table    
    threads = [CrawlThread() for i in range(50)]
    for t in threads: t.start()
        
    while 1:
        it = fetch_table.get_typed_iterator(cs.kernel().current_shard())
        while not it.done():
            print 'Checking... ', it.key(), it.value()
        
            url = it.key()
            if it.value() == C_SHOULD_FETCH:
                site = urlparse.urlparse(url).netloc   
                if not robots_table.contains(site):
                    print 'Adding to robots queue ', site
                    robots_queue.put(site)
                    robots_table.put(site, R_FETCHING)
                    it.Next()
                    continue         
                
                if robots_table.get(site) == R_FETCHING:
                    it.Next()
                    continue
                
                if not check_robots(site, url):
                    fetch_table.put(url, C_ROBOTS_BLACKLIST)
                    it.Next()
                    continue
                  
                if crawltime_table.contains(site):
                    last_crawl = crawltime_table.get(site)
                    if time.time() - last_crawl > 60:
                        crawl_queue.put(url)
                        fetch_table.put(url, C_FETCHING)
                        crawltime_table.put(int(time.time()))                
                    it.Next()
                                        
        time.sleep(1)
    