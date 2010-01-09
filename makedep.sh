#!/bin/bash

mkdir -p .deps
for f in `find src -name '*.cc'` `find src -name '*.c'`; do
  if [[ .deps/$f -ot $f ]]; then
    mkdir -p .deps/`dirname $f`
    gcc $CPPFLAGS -MM -MG -MF .deps/$f.d -MT ${f/.cc/.o} $f
  fi
done

find .deps/ -type f | xargs cat > Makefile.dep
