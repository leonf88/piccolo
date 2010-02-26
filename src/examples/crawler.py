#!/usr/bin/python

import os, re, sys, time
import urllib, urlparse, cgi, httplib, urllib2

from collections import defaultdict

import cStringIO

from lxml import etree
html_parser = etree.HTMLParser()

href_xpath = etree.XPath('//a/@href')
base_xpath = etree.XPath('//base/@href')

import threading
from threading import Thread, Lock
from Queue import PriorityQueue, Queue, Empty, Full  

import logging; logging.basicConfig(level=logging.INFO)

from tlds import domain_from_site

def debug(fmt, *args, **kwargs): logging.debug(str(fmt) % args, **kwargs)
def info(fmt, *args, **kwargs): logging.info(str(fmt) % args, **kwargs)
def warn(fmt, *args, **kwargs): logging.warn(str(fmt) % args, **kwargs)
def error(fmt, *args, **kwargs): logging.error(str(fmt) % args, **kwargs)
def fatal(fmt, *args, **kwargs): logging.fatal(str(fmt) % args, **kwargs)

class CrawlOpener(object):
    def __init__(self):
        self.o = urllib2.build_opener()
    
    def open(self, url_s):    
        req = urllib2.Request(url_s)
        req.add_header('User-Agent', 'MPICrawler/0.1 +http://kermit.news.cs.nyu.edu/crawler.html')
        return self.o.open(req, timeout=10)

crawl_opener = CrawlOpener()

CRAWLER_THREADS = 25

class FetchStatus:
    SHOULD_FETCH = 0
    FETCHING = 1
    FETCH_DONE = 2
    PARSE_DONE = 3
    DONE = 4
    ROBOTS_BLACKLIST = 5
    ERROR = 6    

class ThreadStatus:
    IDLE = 1
    FETCHING = 2
    EXTRACTING = 3
    ADDING = 4
    ROBOT_FETCH = 5 


class RobotStatus:
    FETCHING = 'FETCHING'

ROBOTS_REJECT_ALL = '/'

crawl_queue = Queue(10000)
robots_queue = Queue(1000)

class LockedFile(file):
    def __init__(self, *args, **kw): 
        file.__init__(self, *args, **kw)
        self.lock = Lock()
    
    def write(self, s):
        with self.lock:
            file.write(self, s)
        self.flush()
        
    def writelines(self, seq):
        self.write(''.join(seq))

url_log = LockedFile('/media/raid/urls.out', 'w')
data_log = LockedFile('/media/raid/pages.out', 'w')

class Page(object):
    @staticmethod
    def Create(url):
        p = Page()
        p.domain = domain_from_site(url.netloc)
        p.url = url
        p.url_s = url.geturl()
        p.content = None
        p.outlinks = []
        return p


import crawler_support as cs

fetch_table = None
crawltime_table = None
robots_table = None

def initialize():
    global fetch_table, crawltime_table, robots_table
    fetch_table = cs.kernel().crawl_table(0)
    crawltime_table = cs.kernel().crawl_table(1)
    robots_table = cs.kernel().robots_table(2)
    fetch_table.put("http://www.rjpower.org/gitweb", FetchStatus.SHOULD_FETCH)
    return 0

def check_robots(url):
    disallowed = robots_table.get(url.netloc).split('\n')
    for d in disallowed:
        if not d: continue
        if url.path.startswith(d): return False
    return True

DISALLOW_RE = re.compile('Disallow:(.*)')
def fetch_robots(site):
    info('Fetching robots... %s', site)
    disallow = {}
    try:
        rtxt = crawl_opener.open('http://%s/robots.txt' % site).read()
    except urllib2.HTTPError, e:
        if e.code == 404: 
            rtxt = ''
        else:
            info('Failed robots fetch for %s.', site, exc_info=1) 
            robots_table.put(site, ROBOTS_REJECT_ALL)
            return
    except:
        info('Failed robots fetch for %s.', site, exc_info=1)
        robots_table.put(site, ROBOTS_REJECT_ALL)
        return
    
    for l in rtxt.split('\n'):
        g = DISALLOW_RE.search(l)
        if g:
            path = g.group(1).strip()
            if path.endswith('*'): path = path[:-1]
            if path: disallow[path] = 1
            
    robots_table.put(site, '\n'.join(disallow.keys()))
    info('Robots fetch of %s successful.', site)

def fetch_page(page):
    info('Fetching... %s', page.url_s)
    try:
        page.content = crawl_opener.open(page.url_s).read()
        print >>url_log, page.url_s
        print >>data_log, page.content
        fetch_table.put(page.url_s, FetchStatus.FETCH_DONE)
    except:
        warn('Fetch of %s failed.', page.url_s, exc_info=1)
        page.content = None
        fetch_table.put(page.url_s, FetchStatus.ERROR)

def extract_links(page):
    info('Extracting... %s [%d]', page.url_s, len(page.content))
    try:
        root = etree.parse(cStringIO.StringIO(page.content), html_parser)
        base_href = base_xpath(root)
        if base_href: base_href = base_href[0].strip()
        else: base_href = page.url_s.strip()
        page.outlinks = set([urlparse.urljoin(base_href, l) for l in href_xpath(root)]) 
    except:
        warn('Parse of %s failed.', page.url_s, exc_info=1)

def add_links(page):
    for l in page.outlinks:
        debug('Adding link to fetch queue: %s', l)
        fetch_table.put(l, FetchStatus.SHOULD_FETCH)

class CrawlThread(Thread):    
    def __init__(self): 
        Thread.__init__(self)
        self.status = ThreadStatus.IDLE
        self.url = ''

    def run(self):
        while 1:
            self.status = ThreadStatus.IDLE
            self.url = ''
            
            try:
                site = robots_queue.get(0)
                self.status = ThreadStatus.ROBOT_FETCH                
                self.url = site
                fetch_robots(site)
                continue # keep reading until we run out of robot fetches to do.
            except Empty: pass

            try:
                page = crawl_queue.get(1, 0.1)
                self.url = page.url_s
                self.status = ThreadStatus.FETCHING
                fetch_page(page)
                self.status = ThreadStatus.EXTRACTING
                extract_links(page)
                self.status = ThreadStatus.ADDING
                add_links(page)                
            except Empty: pass
            
                            
            #data = urllib2.urlopen(url).read(
            
def crawl():
    global fetch_table, robots_table    
    threads = [CrawlThread() for i in range(CRAWLER_THREADS)]
    for t in threads: t.start()
        
    while 1:
        it = fetch_table.get_typed_iterator(cs.kernel().current_shard())
        time.sleep(1)
        counts = defaultdict(int)
        while not it.done():
            url_s = it.key()
            status = it.value()
            it.Next()
            
            counts[status] += 1
            
            if status != FetchStatus.SHOULD_FETCH:
                continue
            
            debug('Queue thread running... %s -> %s', url_s, status)
        
            url = urlparse.urlparse(url_s)
            site = url.netloc
            if not robots_table.contains(site):
                info('Fetching robots: %s', site)
                robots_queue.put(site)
                robots_table.put(site, RobotStatus.FETCHING)
                continue         
            
            if robots_table.get(site) == RobotStatus.FETCHING:
                debug('Waiting for robot fetch: %s', url_s)
                continue
            
            if not check_robots(url):
                info('Blocked by robots "%s"', url_s)
                fetch_table.put(url_s, FetchStatus.ROBOTS_BLACKLIST)
                info("%s -> %s", url_s, fetch_table.get(url_s))
                continue
              
            last_crawl = 0
            domain = domain_from_site(site)
            if crawltime_table.contains(domain):
                last_crawl = crawltime_table.get(domain)
            
            if time.time() - last_crawl < 60:
                debug('Waiting for politeness: %s, %d', url_s, time.time() - last_crawl)
            else:
                debug('Queueing: %s', url_s)
                crawl_queue.put(Page.Create(url))
                fetch_table.put(url_s, FetchStatus.FETCHING)
                crawltime_table.put(domain, int(time.time()))
        
        for k, v in sorted(counts.items()):
            info('Page status [%s]: %s', k, v)
        
        for i in range(len(threads)):
            info('Thread status [%d]: %s %s', i, threads[i].status, threads[i].url)
