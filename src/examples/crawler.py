#!/usr/bin/python

import os, re, sys, time
import urllib, urlparse, cgi, httplib, urllib2

import cStringIO

from lxml import etree
html_parser = etree.HTMLParser()

href_xpath = etree.XPath('//a/@href')
base_xpath = etree.XPath('//base/@href')

from threading import Thread
from Queue import PriorityQueue, Queue, Empty, Full  

import logging; logging.basicConfig(level=logging.DEBUG)

def debug(fmt, *args, **kwargs): logging.debug(str(fmt) % args, **kwargs)
def info(fmt, *args, **kwargs): logging.info(str(fmt) % args, **kwargs)
def warn(fmt, *args, **kwargs): logging.warn(str(fmt) % args, **kwargs)
def error(fmt, *args, **kwargs): logging.error(str(fmt) % args, **kwargs)
def fatal(fmt, *args, **kwargs): logging.fatal(str(fmt) % args, **kwargs)

class CrawlStatus:
    SHOULD_FETCH = 0
    FETCHING = 1
    FETCH_DONE = 2
    PARSE_DONE = 3
    DONE = 4
    ROBOTS_BLACKLIST = 5

class RobotStatus:
    FETCHING = 'FETCHING'

ROBOTS_REJECT_ALL = '/'
ROBOTS_TIMEOUT = 10

crawl_queue = Queue()
robots_queue = Queue()

output_log = open('crawler.out', 'w')

class Page(object):
    @staticmethod
    def Create(url):
        p = Page()
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
    fetch_table.put("http://rjpower.org/gitweb", CrawlStatus.SHOULD_FETCH)
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
        rtxt = urllib2.urlopen('http://%s/robots.txt' % site, timeout=ROBOTS_TIMEOUT).read()
    except urllib2.HTTPError, e:
        if e.code == 404: rtxt = ''
        else: raise e
    except:
        info('Failed robots fetch for %s.', site, exc_info=1)
        robots_table.put(site, ROBOTS_REJECT_ALL)
        return
    
    for l in rtxt.split('\n'):
        g = DISALLOW_RE.search(l)
        if g: 
            path = g.group(1).strip()
            if path: disallow[path] = 1
            
    robots_table.put(site, '\n'.join(disallow.keys()))
    info('Robots fetch of %s successful: %s', site, ','.join(disallow.keys()))

def fetch_page(page):
    info('Fetching... %s', page.url_s)
    try:
        page.content = urllib2.urlopen(page.url_s).read()
    except:
        warn('Fetch of %s failed.', page.url_s, exc_info=1)
        page.content = None

def extract_links(page):
    info('Extracting... %s [%d]', page.url_s, len(page.content))
    try:
        root = etree.parse(cStringIO.StringIO(page.content), html_parser)
        base_href = base_xpath(root)
        if not base_href: base_href = page.url_s        
        page.outlinks = set([urlparse.urljoin(base_href, l) for l in href_xpath(root)]) 
    except:
        warn('Parse of %s failed.', page.url_s, exc_info=1)

def add_links(page):
    for l in page.outlinks:
        info('Adding link to fetch queue: %s', l)
        fetch_table.put(l, CrawlStatus.SHOULD_FETCH)

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
                page = crawl_queue.get(1, 0.1)
                fetch_table.put(page.url_s, CrawlStatus.FETCH_DONE)
                fetch_page(page)
                extract_links(page)
                add_links(page)                
            except Empty: pass
                            
            #data = urllib2.urlopen(url).read(
            
def crawl():
    global fetch_table, robots_table    
    threads = [CrawlThread() for i in range(50)]
    for t in threads: t.start()
        
    while 1:
        it = fetch_table.get_typed_iterator(cs.kernel().current_shard())
        time.sleep(1)
        while not it.done():
            url_s = it.key()
            it.Next()
            
            debug('Queue thread running... %s', url_s)
            if it.value() != CrawlStatus.SHOULD_FETCH:
                continue
        
            url = urlparse.urlparse(url_s)
            site = url.netloc
            if not robots_table.contains(site):
                info('Fetching robots: %s', site)
                robots_queue.put(site)
                robots_table.put(site, RobotStatus.FETCHING)
                continue         
            
            if robots_table.get(site) == RobotStatus.FETCHING:
                info('Waiting for robot fetch: %s', url_s)
                continue
            
            if not check_robots(url):
                info('Blocked by robots %s', url_s)
                fetch_table.put(url_s, CrawlStatus.ROBOTS_BLACKLIST)
                continue
              
            last_crawl = 0
            if crawltime_table.contains(site):
                last_crawl = crawltime_table.get(site)
            
            if time.time() - last_crawl < 60:
                info('Waiting for politeness: %s, %d', url_s, time.time() - last_crawl)
            else:
                info('Queueing: %s', url_s)
                crawl_queue.put(Page.Create(url))
                fetch_table.put(url_s, CrawlStatus.FETCHING)
                crawltime_table.put(site, int(time.time()))
