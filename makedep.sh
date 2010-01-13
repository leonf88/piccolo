#!/bin/bash

mkdir -p .deps
for f in `find src -name '*.cc'` `find src -name '*.c'`; do
  depfile=.deps/$f.d
  if [[ $depfile -ot $f ]]; then
    mkdir -p .deps/`dirname $f`
    gcc $CPPFLAGS -MM -MG -MF $depfile -MT ${f/.cc/.o} $f
  fi
done

find .deps/ -type f | xargs cat > Makefile.dep
