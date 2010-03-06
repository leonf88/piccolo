#!/usr/bin/python

import json, os, re, socket, sys, time, types
import urllib, cgi, httplib, urllib2

from urlparse import urlparse, urljoin 

from collections import defaultdict

import cStringIO

from lxml import etree
html_parser = etree.HTMLParser()

href_xpath = etree.XPath('//a/@href')
base_xpath = etree.XPath('//base/@href')

import threading
from threading import Thread, Lock
from Queue import PriorityQueue, Queue, Empty, Full  

import logging
logging.basicConfig(filename='logs/crawl.log.%s.%d' % (socket.gethostname(), os.getpid()), 
                    level=logging.INFO)

console = logging.StreamHandler()
console.setLevel(logging.WARN)
logging.getLogger('').addHandler(console)

from tlds import domain_from_site

def now(): return time.time()

def debug(fmt, *args, **kwargs): logging.debug(str(fmt) % args, **kwargs)
def info(fmt, *args, **kwargs): logging.info(str(fmt) % args, **kwargs)
def warn(fmt, *args, **kwargs): logging.warn(str(fmt) % args, **kwargs)
def error(fmt, *args, **kwargs): logging.error(str(fmt) % args, **kwargs)
def fatal(fmt, *args, **kwargs): logging.fatal(str(fmt) % args, **kwargs)

CRAWLER_THREADS = 10
ROBOTS_REJECT_ALL = '/'
MAX_ROBOTS_SIZE = 50000
MAX_PAGE_SIZE = 500000
CRAWL_TIMEOUT = 10
RECORD_HEADER = '*' * 30 + 'BEGIN_RECORD' + '*' * 30 + '\n'

class CrawlOpener(object):
    def __init__(self):
        self.o = urllib2.build_opener()
    
    def open(self, url_s):    
        req = urllib2.Request(url_s)
        req.add_header('User-Agent', 'MPICrawler/0.1 +http://kermit.news.cs.nyu.edu/crawler.html')
        return self.o.open(req, timeout=CRAWL_TIMEOUT)
crawl_opener = CrawlOpener()


def enum(c):
    c.as_str = dict([(v, k) for k, v in c.__dict__.items() if not k.startswith('_')])
    return c
    
@enum
class FetchStatus:
    SHOULD_FETCH = 0
    FETCHING = 1
    FETCH_DONE = 2
    PARSE_DONE = 3
    DONE = 4
    ROBOTS_BLACKLIST = 5
    ERROR = 6    

@enum
class ThreadStatus:
    IDLE = 1
    FETCHING = 2
    EXTRACTING = 3
    ADDING = 4
    ROBOT_FETCH = 5 

@enum
class RobotStatus:
    FETCHING = 'FETCHING'


crawl_queue = Queue(10000)
robots_queue = Queue(1000)

class LockedFile(object):
    def __init__(self, f, *args, **kw): 
        self.lock = Lock()
        self.f = open(f, 'w')
    
    def writeRecord(self, data, **kw):
        with self.lock:
            self.f.write(RECORD_HEADER)
            for k, v in kw.iteritems(): self.f.write('%s: %s\n' % (k, v))
            self.f.write('length: %s\n' % len(data))
            self.f.write(data)
            self.f.write('\n')
    
    def write(self, s):
        with self.lock: self.f.write(s)

url_log = LockedFile('/scratch/urls.out')
data_log = LockedFile('/scratch/pages.out')

def key_from_site(site): return domain_from_site(site) + ' ' + site
    
def key_from_url(url): return domain_from_site(url.netloc) + ' ' + url.geturl()
def url_from_key(key): return urlparse(key[key.find(' ') + 1:])

class Page(object):
    @staticmethod
    def create(url):
        p = Page()
        p.domain = domain_from_site(url.netloc)
        p.url = url
        p.url_s = url.geturl()
        p.content = None
        p.outlinks = []
        return p
    
    def key(self): return key_from_url(self.url)

try:
    import crawler_support as cs
except: pass

fetch_table = None
crawltime_table = None
domain_counts = None
robots_table = None

def initialize():
    global fetch_table, crawltime_table, robots_table, domain_counts
    fetch_table = cs.kernel().crawl_table(0)
    crawltime_table = cs.kernel().crawl_table(1)
    robots_table = cs.kernel().robots_table(2)
    domain_counts = cs.kernel().crawl_table(3)
    fetch_table.put(key_from_url(urlparse("http://www.rjpower.org/crawlstart.html")), 
                    FetchStatus.SHOULD_FETCH)
    return 0

def check_robots(url):
    disallowed = robots_table.get(key_from_site(url.netloc)).split('\n')
    for d in disallowed:
        if not d: continue
        if url.path.startswith(d): return False
    return True

DISALLOW_RE = re.compile('Disallow:(.*)')
def fetch_robots(site):
    info('Fetching robots... %s', site)
    try:
        return crawl_opener.open('http://%s/robots.txt' % site).read(MAX_ROBOTS_SIZE)
    except urllib2.HTTPError, e:
        if e.code == 404: return ''
    except: pass
    
    info('Failed robots fetch for %s.', site, exc_info=1)
    warn('Failed robots fetch for %s.', site)
    return ROBOTS_REJECT_ALL
        
def parse_robots(site, rtxt):
    info('Parsing robots: %s', site)
    try:
        disallow = {}
        for l in rtxt.split('\n'):
            g = DISALLOW_RE.search(l)
            if g:
                path = g.group(1).strip()
                if path.endswith('*'): path = path[:-1]
                if path: disallow[path] = 1
                
        robots_table.put(key_from_site(site), '\n'.join(disallow.keys()))
        info('Robots fetch of %s successful.', site)
    except:
        warn('Failed to parse robots file!', exc_info=1)

def fetch_page(page):
    info('Fetching... %s', page.url_s)
    try:
        page.content = crawl_opener.open(page.url_s).read(MAX_PAGE_SIZE)
        url_log.write(page.url_s + '\n')
        data_log.writeRecord(page.content, url=page.url_s, domain=page.domain)
        fetch_table.put(page.key(), FetchStatus.FETCH_DONE)
    except (urllib2.URLError, socket.timeout):
        warn('Fetch of %s failed.', page.url_s)
        page.content = ''
        fetch_table.put(page.key(), FetchStatus.ERROR)

def join_url(base, l):
    if l.find('#') != -1: l = l[:l.find('#')]    
    j = urljoin(base, l)
    if len(j) > 200 or len(j) < 6 or ' ' in j or not j.startswith('http://') or j.count('&') > 2: return None
    return j

def extract_links(page):
    info('Extracting... %s [%d]', page.url_s, len(page.content))
    try:
        root = etree.parse(cStringIO.StringIO(page.content), html_parser)
        base_href = base_xpath(root)
        if base_href: base_href = base_href[0].strip()
        else: base_href = page.url_s.strip()
        
        outlinks = [join_url(base_href, l) for l in href_xpath(root)]
        page.outlinks = set([l for l in outlinks if l])
    except:
        warn('Parse of %s failed.', page.url_s)

def add_links(page):
    for l in page.outlinks:
        debug('Adding link to fetch queue: %s', l)
        if not isinstance(l, types.StringType): info('Garbage link: %s', l)
        else: fetch_table.put(key_from_url(urlparse(l)), FetchStatus.SHOULD_FETCH)

class CrawlThread(Thread):    
    def __init__(self): 
        Thread.__init__(self)
        self.status = ThreadStatus.IDLE
        self.url = ''
        self.last_active = now()
        
    def run(self):
        while 1:
            self.status = ThreadStatus.IDLE
            self.url = ''
            self.last_active = now()
            
            if robots_queue.empty() and crawl_queue.empty():
                time.sleep(1)
            
            try:
                site = robots_queue.get(0)
                self.status = ThreadStatus.ROBOT_FETCH                
                self.url = site                
                parse_robots(site, fetch_robots(site))
            except Empty: pass
            except:
                warn('Error while processing robots fetch!', exc_info=1)

            try:
                page = crawl_queue.get(0)
                self.url = page.url_s
                self.status = ThreadStatus.FETCHING
                fetch_page(page)
                self.status = ThreadStatus.EXTRACTING
                extract_links(page)
                self.status = ThreadStatus.ADDING
                add_links(page)
            except Empty: pass
            except:
                warn('Error when processing page %s', page.url_s, exc_info=1)
                fetch_table.put(page.key(), FetchStatus.ERROR)

class StatusThread(Thread):
    def __init__(self, threadlist):
        Thread.__init__(self)
        self.threads = threadlist
    
    def run(self):
        while 1:
            try:
                it = fetch_table.get_typed_iterator(cs.kernel().current_shard())
                counts = defaultdict(int)
                while not it.done():                        
                    counts[it.value()] += 1
                    it.Next()
                
                for k, v in sorted(counts.items()):
                    warn('Fetched [%s]: %s', FetchStatus.as_str[k], v)
                
                for t in sorted(self.threads, key=lambda t: t.last_active):
                    if t.status != ThreadStatus.IDLE:
                        warn('Thread [%s]: %.2f %s', ThreadStatus.as_str[t.status], now() - t.last_active, t.url)
                    
                it = domain_counts.get_typed_iterator(cs.kernel().current_shard())
                dcounts = []            
                while not it.done(): 
                    dcounts += [(it.value(), it.key())]
                    it.Next()
                
                warn('Top sites:')
                dcounts.sort(reverse=1)
                for v, k in dcounts[:100]:
                    warn('%s: %s', k, v) 
            except:
                warn('Failed to print status!?', exc_info=1)
                            
            time.sleep(10)


def crawl():    
    threads = [CrawlThread() for i in range(CRAWLER_THREADS)]
    for t in threads: t.start()
    
    status = StatusThread(threads)
    status.start()
    
    warn('Starting crawl!')
    last_t = time.time()
        
    while 1:
        it = fetch_table.get_typed_iterator(cs.kernel().current_shard())
        fetch_table.SendUpdates()
        fetch_table.CheckForUpdates()
        
        time.sleep(0.1)
         
        if time.time() - last_t > 10:
            warn('Queue thread running...')
            last_t = time.time()
        
        while not it.done():
            url = url_from_key(it.key())
            status = it.value()
            it.Next()

            if status != FetchStatus.SHOULD_FETCH:
                continue            
        
            check_url(url, status)        


def check_url(url, status):
    url_s = url.geturl()
    site = url.netloc
    rkey = key_from_site(site)

    debug('Checking: %s %s', url_s, status)
        
    if not robots_table.contains(rkey):
        info('Fetching robots: %s', site)
        robots_table.put(rkey, RobotStatus.FETCHING)
        robots_queue.put(site)
        if robots_table.get(rkey) != RobotStatus.FETCHING:
            fatal("WTF????")
            exit(1)
        return         
    
    if robots_table.get(rkey) == RobotStatus.FETCHING:
        debug('Waiting for robot fetch: %s', url_s)
        return
    
    if not check_robots(url):
        info('Blocked by robots "%s"', url_s)
        fetch_table.put(key_from_url(url), FetchStatus.ROBOTS_BLACKLIST)
        return
      
    last_crawl = 0
    domain = domain_from_site(site)
    if crawltime_table.contains(domain):
        last_crawl = crawltime_table.get(domain)
    
    if now() - last_crawl < 60:
        debug('Waiting for politeness: %s, %d', url_s, now() - last_crawl)
    else:
        debug('Queueing: %s', url_s)
        crawl_queue.put(Page.create(url))
        fetch_table.put(key_from_url(url), FetchStatus.FETCHING)
        domain_counts.put(domain, 1)
        crawltime_table.put(domain, int(now()))
