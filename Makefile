CXX = distcc g++
CC = distcc gcc
CMAKE = cmake

export CXX CC
example-dsm:
	cd bin && $(CMAKE) ../src && $(MAKE) example-dsm

all:
	cd bin && $(CMAKE) ../src && $(MAKE) example-dsm
	
clean:
	rm -rf bin/*


