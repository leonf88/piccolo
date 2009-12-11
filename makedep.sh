#!/bin/bash

for f in `find src -name '*.cc'`; do 
  gcc $CPPFLAGS -MM -MG -MT ${f/.cc/.o} $f
done

for f in `find src -name '*.c'`; do 
  gcc $CPPFLAGS -MM -MG -MT ${f/.c/.o} $f
done
