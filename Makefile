CXX = distcc g++
CC = distcc gcc
CMAKE = cmake
export CXX CC

MAKE := $(MAKE) --no-print-directory

all: release debug

bin/release/Makefile:
	@mkdir -p bin/release
	@cd bin/release && $(CMAKE) -DCMAKE_BUILD_TYPE=Release ../../src
	
bin/debug/Makefile:
	@mkdir -p bin/debug
	@cd bin/debug && $(CMAKE) -DCMAKE_BUILD_TYPE=Debug ../../src


release: bin/release/Makefile
	@cd bin/release && $(MAKE) example-dsm

debug: bin/debug/Makefile
	@cd bin/debug  && $(MAKE) example-dsm

clean:
	rm -rf bin/*

.DEFAULT: bin/debug/Makefile bin/release/Makefile
	@cd bin/release && $(MAKE) $@
	@cd bin/debug && $(MAKE) $@
	