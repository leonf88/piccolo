#!/bin/bash

export CC='distcc gcc' 
export CXX='distcc g++'

CONFIG="./configure --disable-shared --enable-static --disable-dependency-tracking"

(
cd gflags
$CONFIG && make clean && make libgflags.la -j32
)

(
cd glog
CPPFLAGS=-I../gflags/src/ LDFLAGS=-L../gflags/.libs $CONFIG && make clean && make -j32 
)

(
cd gperftools
CPPFLAGS=-I../gflags/src/ LDFLAGS=-L../gflags/.libs $CONFIG && make clean && make -j32 
)

