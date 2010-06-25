#!/usr/bin/python

import json, os, re, socket, sys, time, types, threading, traceback
import gzip, urllib, cgi, httplib, urllib2, cStringIO

from urlparse import urlparse, urljoin 
from collections import defaultdict

from lxml import etree
html_parser = etree.HTMLParser()

href_xpath = etree.XPath('//a/@href')
base_xpath = etree.XPath('//base/@href')

from threading import Thread, Lock
from Queue import PriorityQueue, Queue, Empty, Full  

from tlds import domain_from_site

try:
  from piccolo import *
except:
  print 'Failed to import crawler support module!'
  traceback.print_exc()
  sys.exit(1)

num_crawlers = NetworkThread.Get().size() - 1
crawler_id = NetworkThread.Get().id()

import logging

os.system('mkdir -p logs.%d' % num_crawlers)
logging.basicConfig(level=logging.WARN)
                    #filename='logs.%d/crawl.log.%s.%d' % (num_crawlers, socket.gethostname(), os.getpid()),
                    
                  
def now(): return time.time()

def debug(fmt, *args, **kwargs): logging.debug(str(fmt) % args, **kwargs)
def info(fmt, *args, **kwargs): logging.info(str(fmt) % args, **kwargs)
def warn(fmt, *args, **kwargs): logging.warn(str(fmt) % args, **kwargs)
def error(fmt, *args, **kwargs): logging.error(str(fmt) % args, **kwargs)
def fatal(fmt, *args, **kwargs): logging.fatal(str(fmt) % args, **kwargs)
def console(fmt, *args, **kwargs):
  info(fmt, *args, **kwargs)
  print >> sys.stderr, str(fmt) % args 

CRAWLER_THREADS = 5
ROBOTS_REJECT_ALL = '/'
MAX_ROBOTS_SIZE = 50000
MAX_PAGE_SIZE = 100000
CRAWL_TIMEOUT = 10
RECORD_HEADER = '*' * 30 + 'BEGIN_RECORD' + '*' * 30 + '\n'
PROFILING = False
RUNTIME = -1
crawl_queue = Queue(100000)
robots_queue = Queue(100000)
running = True

fetch_table = None
crawltime_table = None
robots_table = None
domain_counts = None
fetch_counts = None

class CrawlOpener(object):
  def __init__(self):
    self.o = urllib2.build_opener()
  
  def read(self, url_s):  
    req = urllib2.Request(url_s)
    req.add_header('User-Agent', 'MPICrawler/0.1 +http://kermit.news.cs.nyu.edu/crawler.html')
    req.add_header('Accept-encoding', 'gzip')
    url_f = self.o.open(req, timeout=CRAWL_TIMEOUT)
    sf = cStringIO.StringIO(url_f.read())    
    if url_f.headers.get('Content-Encoding') and 'gzip' in url_f.headers.get('Content-Encoding'):
    	return gzip.GzipFile(fileobj=sf).read()
    return sf.read()
       
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
  FETCH_ERROR = 6  
  PARSE_ERROR = 7
  GENERIC_ERROR = 8
  # used for reporting bytes downloaded
  FETCHED_BYTES = 10

  
@enum
class ThreadStatus:
  IDLE = 1
  FETCHING = 2
  EXTRACTING = 3
  ADDING = 4
  ROBOT_FETCH = 5 
  ROBOT_PARSE = 6

@enum
class RobotStatus:
  FETCHING = 'FETCHING'

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

  def sync(self):
    with self.lock: os.fsync(self.f)

url_log = LockedFile('/scratch/urls.out.%d' % crawler_id)
data_log = LockedFile('/scratch/pages.out.%d' % crawler_id)

def key_from_site(site): return domain_from_site(site) + ' ' + site
  
def key_from_url(url): return domain_from_site(url.netloc) + ' ' + url.geturl()
def url_from_key(key): return urlparse(key[key.find(' ') + 1:])

import cPickle
def CrawlPickle(v): 
  return cPickle.dumps(v)

def CrawlUnpickle(v):
  return cPickle.loads(v)

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

def update_tables():
  for t in [fetch_table, fetch_counts, crawltime_table, domain_counts, robots_table]:
    t.SendUpdates()
    t.HandlePutRequests()

def check_robots(url):
  k  = key_from_site(url.netloc)
  if not robots_table.contains(k):
    fatal('Robots error!!!! %s', url)
  
  disallowed = robots_table.get(k).split('\n')
  for d in disallowed:
    if not d: continue
    if url.path.startswith(d): return False
  return True

DISALLOW_RE = re.compile('Disallow:(.*)')
def fetch_robots(site):
  info('Fetching robots: %s', site)
  try:
    return crawl_opener.read('http://%s/robots.txt' % site)
  except urllib2.HTTPError, e:
    if e.code == 404: return ''
  except: pass
  
  info('Failed robots fetch for %s.', site, exc_info=1)
  warn('Failed robots fetch for %s: %s', site, sys.exc_info()[1])
  return ROBOTS_REJECT_ALL
    
def parse_robots(site, rtxt):
  debug('Parsing robots: %s', site)
  try:
    disallow = {}
    for l in rtxt.split('\n'):
      g = DISALLOW_RE.search(l)
      if g:
        path = g.group(1).strip()
        if path.endswith('*'): path = path[:-1]
        if path: disallow[path] = 1
        
    robots_table.update(key_from_site(site), '\n'.join(disallow.keys()))
    debug('Robots fetch of %s successful.', site)
  except:
    warn('Failed to parse robots file!', exc_info=1)

def update_fetch_table(key, status, byte_count=0):
    fetch_table.update(key, status)
    fetch_counts.update(FetchStatus.as_str[status], 1)
    if byte_count > 0:
      fetch_counts.update(FetchStatus.as_str[FetchStatus.FETCHED_BYTES], byte_count)

def fetch_page(page):
  info('Fetching page: %s', page.url_s)
  try:
    page.content = crawl_opener.read(page.url_s)
    url_log.write(page.url_s + '\n')
    data_log.writeRecord(page.content, url=page.url_s, domain=page.domain)
    update_fetch_table(page.key(), FetchStatus.FETCH_DONE, len(page.content))
  except (urllib2.URLError, socket.timeout):
    info('Fetch of %s failed: %s', page.url_s, sys.exc_info()[1])
    warn('Fetch of %s failed.', page.url_s, exc_info=1)
    page.content = ''
    update_fetch_table(page.key(), FetchStatus.FETCH_ERROR)

def join_url(base, l):
  '''Join a link with the base href extracted from the page; throwing away garbage.'''   
  if l.find('#') != -1: l = l[:l.find('#')]
  j = urljoin(base, l)
  if len(j) > 200 or \
     len(j) < 6 or \
     ' ' in j or not \
     j.startswith('http://') or \
     j.count('&') > 2: return None
  return j

def extract_links(page):
  debug('Extracting... %s [%d]', page.url_s, len(page.content))
  if not page.content:
  	page.outlinks = []
  	return
  
  try:
    root = etree.parse(cStringIO.StringIO(page.content), html_parser)
    base_href = base_xpath(root)
    if base_href: base_href = base_href[0].strip()
    else: base_href = page.url_s.strip()
    
    outlinks = [join_url(base_href, l) for l in href_xpath(root)]
    page.outlinks = set([l for l in outlinks if l])
  except:
    info('Parse of %s failed: %s', page.url_s, sys.exc_info()[1])
    warn('Parse of %s failed.', page.url_s, exc_info=1)
    update_fetch_table(page.key(), FetchStatus.PARSE_ERROR)
    

def add_links(page):
  for l in page.outlinks:
    debug('Adding link to fetch queue: %s', l)
    if not isinstance(l, types.StringType): info('Garbage link: %s', l)
    else: fetch_table.update(key_from_url(urlparse(l)), FetchStatus.SHOULD_FETCH)

class CrawlThread(Thread):  
  def __init__(self, id): 
    Thread.__init__(self)
    self.status = ThreadStatus.IDLE
    self.url = ''
    self.last_active = now()
    self.id = id
    
  def ping(self, new_status):
    self.last_active = now()
    self.status = new_status
  
  def run(self):
    while 1:
      self.url = ''
      self.ping(ThreadStatus.IDLE)
      
      if robots_queue.empty() and crawl_queue.empty():
        time.sleep(1)
      
      for i in range(10):
        try:
          page = crawl_queue.get(block=False)
          self.url = page.url_s          
          self.ping(ThreadStatus.FETCHING) 
          fetch_page(page)
          self.ping(ThreadStatus.EXTRACTING)
          extract_links(page)
          self.ping(ThreadStatus.ADDING)
          add_links(page)          
        except Empty: pass
        except:
          warn('Error when processing page %s', page.url_s, exc_info=1)
          update_fetch_table(page.key(), FetchStatus.GENERIC_ERROR)
      
      try:
        site = robots_queue.get(block=False)        
        self.url = site
        self.ping(ThreadStatus.ROBOT_FETCH)
        robots_data = fetch_robots(site)
        self.ping(ThreadStatus.ROBOT_PARSE)
        parse_robots(site, robots_data)
      except Empty: pass
      except:
        warn('Error while processing robots fetch!', exc_info=1)
      

class StatusThread(Thread):
  def __init__(self, threadlist):
    Thread.__init__(self)
    self.threads = threadlist
    self.start_time = time.time()
    self.end_time = time.time() + RUNTIME
  
  def print_status(self):
      console('Crawler status [running for: %.1f, remaining: %.1f]',
          time.time() - self.start_time, self.end_time - time.time())
      
      console('Page queue: %d; robot queue %d',
          crawl_queue.qsize(), robots_queue.qsize())
      
      it = fetch_counts.get_iterator(kernel().current_shard())
      while not it.done():            
        console('Fetch[%s] :: %d', it.key(), it.value())
        it.Next()
      
      console('Threads (ordered by last ping time)')
      for t in sorted(self.threads, key=lambda t: t.last_active):
        console('>> T(%02d) last ping: %.2f status: %s last url:%s',
                t.id, now() - t.last_active, ThreadStatus.as_str[t.status], t.url)
        
      it = domain_counts.get_iterator(kernel().current_shard())
      dcounts = []
      while not it.done(): 
        dcounts += [(it.key(), it.value())]
        it.Next()
      
      console('Top sites:')
      dcounts.sort(key=lambda t: t[1], reverse=1)
      for k, v in dcounts[:10]:
        console('%s: %s', k, v)
    
  def run(self):
    global running
    while 1:
      try:
        if crawler_id == 1:
          self.print_status()
      except:
        warn('Failed to print status!?', exc_info=1)
            
      update_tables()
      if RUNTIME > 0 and time.time() - self.start_time > RUNTIME:
        running = False
      
      url_log.sync()
      data_log.sync()
      time.sleep(5)


def do_crawl():  
  global RUNTIME
  RUNTIME = crawler_runtime()
  
  threads = [CrawlThread(i) for i in range(CRAWLER_THREADS)]
  for t in threads: t.start()
  
  status = StatusThread(threads)
  status.start()
  
  warn('Starting crawl!')
  last_t = time.time()
  
  while running:
    it = fetch_table.get_iterator(kernel().current_shard())
    if time.time() - last_t > 10:
      warn('Queue thread running...')
      last_t = time.time()
    
    while not it.done() and running:
      url = url_from_key(it.key())
      status = it.value()
      
      debug('Looking AT: %s %s', url, status)      
      if status == FetchStatus.SHOULD_FETCH:
        check_url(url, status)
      it.Next()
    
    time.sleep(0.1)


def check_url(url, status):
  url_s = url.geturl()
  site = url.netloc
  robots_key = key_from_site(site)

  info('Checking: %s %s', url_s, status)
    
  if not robots_table.contains(robots_key):
    debug('Queueing robots fetch: %s', site)    
    robots_table.update(robots_key, RobotStatus.FETCHING)
    robots_queue.put(site)
    return
  
  if robots_table.get(robots_key) == RobotStatus.FETCHING:
    debug('Waiting for robot fetch: %s', url_s)
    return
  
  if not check_robots(url):
    debug('Blocked by robots "%s"', url_s)
    update_fetch_table(key_from_url(url), FetchStatus.ROBOTS_BLACKLIST, 1)
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
    fetch_table.update(key_from_url(url), FetchStatus.FETCHING)
    domain_counts.update(domain, 1)
    crawltime_table.update(domain, int(now()))
    
    
def initialize():
  console('Initializing...')
  global fetch_table, crawltime_table, robots_table, domain_counts, fetch_counts
  fetch_table = kernel().GetIntTable(0)
  crawltime_table = kernel().GetIntTable(1)
  robots_table = kernel().GetStringTable(2) 
  domain_counts = kernel().GetIntTable(3)
  fetch_counts = kernel().GetIntTable(4)
  
  if kernel().current_shard() == 0:
    fetch_table.update(
      key_from_url(urlparse("http://kermit.news.cs.nyu.edu/crawlstart.html")),
      FetchStatus.SHOULD_FETCH)
  
  return 0

def crawl():
  console('Crawling...')
  do_crawl()
  return 0

def main():
  global fetch_table, crawltime_table, robots_table, domain_counts, fetch_counts
  num_workers = NetworkThread.Get().size() - 1
  tr = TableRegistry.Get()

  fetch_table = tr.CreateIntTable(0,  num_workers, DomainSharding(), IntAccum.Max())
  crawltime_table = tr.CreateIntTable(1,  num_workers, DomainSharding(), IntAccum.Max())
  robots_table = tr.CreateStringTable(2,  num_workers, DomainSharding(), StringAccum.Replace())
  domain_counts = tr.CreateIntTable(3,  num_workers, DomainSharding(), IntAccum.Sum())
  fetch_counts = tr.CreateIntTable(4,  num_workers, DomainSharding(), IntAccum.Sum())

  conf = ConfigData()
  conf.set_num_workers(num_workers)
  if not StartWorker(conf):
    m = Master(conf)
    print fetch_table
    m.py_run_all('initialize()', fetch_table)
    m.py_run_all('crawl()', fetch_table)
  
if __name__ == '__main__':
  main()
