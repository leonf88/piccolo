#!/bin/bash

echo -n "checking dependencies: "
mkdir -p .deps
for f in `find src -name '*.cc'` `find src -name '*.c'`; do
  depfile=.deps/$f.d
  if [[ $depfile -ot $f ]]; then
    echo -n 'N'
    mkdir -p .deps/`dirname $f`
    o=${f/.cc/.o}
    o=${o/src/bin}
    gcc $CPPFLAGS -MM -MG -MF $depfile -MT $o $f
  else
    echo -n 'O'
  fi
done

echo

find .deps/ -type f | xargs cat > Makefile.dep
