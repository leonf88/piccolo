#!/bin/bash

CONFIG="./configure --enable-static --disable-dependency-tracking "

(
cd gflags
$CONFIG && make clean && make libgflags.la -j8
)

(
cd glog
CPPFLAGS=-I../gflags/src/ LDFLAGS=-L../gflags/.libs $CONFIG && make clean && make libglog.la -j8
)

