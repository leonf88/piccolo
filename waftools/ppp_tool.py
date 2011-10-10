#!/usr/bin/env python

from waflib import TaskGen
TaskGen.declare_chain(
        name='ppp',
        rule='${PPP} ${SRC} ${TGT}',
        shell=False,
        ext_in='.pp',
        ext_out='.cc',
        reentrant=False,
)

def configure(ctx):
  ctx.env['PPP'] = ctx.path.find_resource('util/ppp.py').abspath()
