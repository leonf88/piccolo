#!/bin/bash

CONFIG="./configure --enable-static --disable-dependency-tracking "

pushd gflags
$CONFIG && make clean && make libgflags.la -j8
popd

pushd glog
CPPFLAGS=-I../gflags/src/ LDFLAGS=-L../gflags/.libs $CONFIG && make clean && make libglog.la -j8
popd

