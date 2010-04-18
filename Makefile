CXX = distcc g++
CC = distcc gcc
CMAKE = cmake
export CXX CC

MAKE := $(MAKE) --no-print-directory

all: release debug

release:
	mkdir -p bin/release
	cd bin/release && $(CMAKE) -DCMAKE_BUILD_TYPE=Release ../../src && $(MAKE) example-dsm

debug:
	mkdir -p bin/debug
	cd bin/debug && $(CMAKE) -DCMAKE_BUILD_TYPE=Debug ../../src && $(MAKE) example-dsm

clean:
	rm -rf bin/*

