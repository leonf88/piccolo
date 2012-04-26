#!/usr/bin/python

__doc__ = '''
ppp: The Piccolo Pre-Processor.  

Yes, alliteration is fun.

Transforms .pp files to C++, replacing instances of the PMap and PReduce
calls with calls to generated kernels and barriers. Now also works with 
the PSwapAccumulator call.
'''

import os, sys, re, _sre

class PrettyPattern(object):
  def __init__(self, txt, p):
    self._txt = txt
    self._p = p

  def __repr__(self): return self._txt

  def match(self, *args): return self._p.match(*args)
  def search(self, *args): return self._p.search(*args)

def compile(p, name=None, flags=0):
  if isinstance(p, PrettyPattern): return p
  if not name: name = p
  return PrettyPattern(name, re.compile(p, flags))

class Scanner(object):
  ws = compile(r'[ \t\n\r]+', name='whitespace')
  comment = compile(r'(//[^\n]*)|(/\*.*?\*/)', name='comment', flags=re.DOTALL)
  id = compile(r'([a-zA-Z][a-z0-9_]*)', name='identifier')

  def __init__(self, f):
    self.file = f
    self.content = open(f).read()
    self.output = ''
    self.line = 1
    self.offset = 0
    self._stack = []

  def code_prefix(self):
    filename = os.path.basename(self.file)
    prefix = os.path.splitext(filename)[0]
    return re.sub(r'\W', '_', prefix)

  def update(self, g, update_pos):
    if update_pos:
      self.output += self.content[:g.start()]
      s = g.end()

      # this is horrible...
      self.line += self.content.count('\n', 0, s)
      self.offset = s - self.content.rfind('\n', 0, s)

      self.content = self.content[g.end():]


  def slurp(self, *patterns):
    matched = True
    while matched:
      matched = False
      for p in patterns:
        p = compile(p)
        g = p.match(self.content)
        if g:
          matched = True
          self.update(g, True)

  def match(self, pattern, update_pos=True, must_match=True):
    g = compile(pattern).match(self.content)
    if not g and must_match:
      raise ValueError, 'Expected `%s\' in input at line %d (offset %d), "...%s...", while parsing:\n >>>%s' % \
                        (pattern, self.line, self.offset, self.content[:20], '\n >>>'.join([s[0] for s in self._stack]))
    self.update(g, update_pos)
    return g

  def search(self, pattern, update_pos=True):
    g = compile(pattern).search(self.content)
    if g: self.update(g, update_pos)
    return g

  def read(self, *patterns):
    g = []
    for p in patterns:
      self.slurp(Scanner.ws, Scanner.comment)
      g.append(self.match(p).group(0))
    return g

  def peek(self, pattern):
    self.slurp(Scanner.ws, Scanner.comment)
    return self.match(pattern, update_pos=False, must_match=False) != None

  def push(self, fname):
    self._stack += [(fname, self.line, self.offset)]

  def pop(self): self._stack.pop()


HEADER = '''
#include "client/client.h"

using namespace piccolo;
'''

MAP_KERNEL = '''
class %(prefix)s_Map_%(id)s : public DSMKernel {
public:
  virtual ~%(prefix)s_Map_%(id)s() {}
  template <class K, %(klasses)s>
  void run_iter(const K& k, %(decl)s) {
#line %(line)s "%(filename)s"
    %(code)s;
  }
  
  template <class TableA>
  void run_loop(TableA* a) {
    typename TableA::Iterator *it =  a->get_typed_iterator(current_shard());
    for (; !it->done(); it->Next()) {
      run_iter(it->key(), %(calls)s);
    }
    delete it;
  }
  
  void map() {
      run_loop(%(main_table)s);
  }
};

REGISTER_KERNEL(%(prefix)s_Map_%(id)s);
REGISTER_METHOD(%(prefix)s_Map_%(id)s, map);
'''

RUN_KERNEL = '''
class %(prefix)s_Run_%(id)s : public DSMKernel {
public:
  virtual ~%(prefix)s_Run_%(id)s () {}
  void run() {
#line %(line)s "%(filename)s"
      %(code)s;
  }
};

REGISTER_KERNEL(%(prefix)s_Run_%(id)s);
REGISTER_METHOD(%(prefix)s_Run_%(id)s, run);
'''

SWAP_ACCUM = '''
class %(prefix)s_Swap_%(id)s : public DSMKernel_Swap {
public:
  virtual ~%(prefix)s_Swap_%(id)s () {}
  void swap() {
#line %(line)s "%(filename)s"
      %(code)s;
  }
};

REGISTER_KERNEL(%(prefix)s_Swap_%(id)s);
REGISTER_METHOD(%(prefix)s_Swap_%(id)s, swap);
'''

class Counter(object):
  def __init__(self):
    self.c = 0

  def __call__(self):
    self.c += 1
    return self.c

get_id = Counter()

def ParseKeys(s):
  s.push('ParseKeys')
  k, _, t = s.read(Scanner.id, ':', Scanner.id)
  result = [(k, t)]

  if s.peek(','):
    s.read(',')
    result += ParseKeys(s)

  s.pop()
  return result

# match brackets
def ParseCode(s):
  s.push('ParseCode')
  s.read('{')
  code = ''
  while 1:
    code += s.search(r'[^{}]*', re.DOTALL).group(0)
    if s.peek('{'): code += '{' + ParseCode(s) + '}'
    if s.peek('}'):
      s.read('}')
      s.pop()
      return code

def ParsePMap(s):
  s.push('ParsePMap')
  _, keys, _, code, _ = s.read(r'\(', '{'), ParseKeys(s), s.read('}', ','), ParseCode(s), s.read(r'\)', ';')

  id = get_id()
  main_table = keys[0][1]

  s.output += 'm.run_all("%s_Map_%d", "map", %s);' % (s.code_prefix(), id, main_table)

  i = 0
  klasses, decls, calls = [], [], []
  for k, v in keys:
    klasses += ['class Value%d' % i]
    if i == 0:
      decls += ['Value%d &%s' % (i, k)]
      calls += ['it->value()']
    else:
      decls += ['const Value%d &%s' % (i, k)]
      calls += ['%s->get(it->key())' % v]
    i += 1

  s.content += MAP_KERNEL % dict(filename=s.file,
                            prefix=s.code_prefix(),
                            line=s._stack[-1][1],
                            id=id,
                            decl=','.join(decls),
                            calls=','.join(calls),
                            klasses=','.join(klasses),
                            code=code,
                            main_table=main_table) + '\n'
  s.pop()


def ParsePRunOne(s):
  s.push('ParsePRunOne')
  _, table, _, code, _ = s.read(r'\(',), s.read(Scanner.id)[0], s.read(','), ParseCode(s), s.read(r'\)', ';')

  id = get_id()
  s.output += 'm.run_one("%s_Run_%d", "run", %s);' % (s.code_prefix(), id, table)

  s.content += RUN_KERNEL % dict(filename=s.file,
                            prefix=s.code_prefix(),
                            line=s._stack[-1][1],
                            id=id,
                            code=code) + '\n'
  s.pop()
  return code

def ParsePRunAll(s):
  s.push('ParsePRunAll')
  _, table, _, code, _ = s.read(r'\(',), s.read(Scanner.id)[0], s.read(','), ParseCode(s), s.read(r'\)', ';')

  id = get_id()
  s.output += 'm.run_all("%s_Run_%d", "run", %s);' % (s.code_prefix(), id, table)

  s.content += RUN_KERNEL % dict(filename=s.file,
                            prefix=s.code_prefix() ,
                            line=s._stack[-1][1],
                            id=id,
                            code=code) + '\n'
  s.pop()
  return code

def ParsePSwapAccumulator(s):
  s.push('ParsePSwapAccumulator')
  _, table, _, accum, _ = s.read(r'\(',), s.read(Scanner.id)[0], s.read(','), ParseCode(s), s.read(r'\)', ';')

  code = table + '->swap_accumulator(' + accum + ');'

  id = get_id()
  s.output += 'm.run_all("%s_Swap_%d", "swap", %s);' % (s.code_prefix() , id, table)
  s.content += SWAP_ACCUM % dict(filename=s.file,
                            prefix=s.code_prefix() ,
                            line=s._stack[-1][1],
                            id=id,
                            code=code) + '\n'
  s.pop()
  return code;

def ProcessFile(f_in, f_out):
  global c
  s = Scanner(f_in)
  print >> f_out, HEADER
  print >> f_out, '#line 1 "%s"' % f_in

  while 1:
    g = s.search('PMap|PRunOne|PRunAll|PSwapAccumulator')
    if not g: break
    if g.group(0) == 'PMap': ParsePMap(s)
    elif g.group(0) == 'PRunOne': ParsePRunOne(s)
    elif g.group(0) == 'PRunAll': ParsePRunAll(s)
    elif g.group(0) == 'PSwapAccumulator': ParsePSwapAccumulator(s)

  print >> f_out, s.output
  print >> f_out, s.content

if __name__ == '__main__':
  n_in = sys.argv[1]
  try:
    ProcessFile(n_in, sys.stdout)
  except ValueError, e:
    print >> sys.stderr, 'Parse error:', e
    sys.exit(1)

