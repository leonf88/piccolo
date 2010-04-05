CMAKE=CC="distcc gcc" CXX="distcc g++" cmake

example-dsm:
	cd bin && $(CMAKE) ../src && $(MAKE) example-dsm

all:
	cd bin && $(CMAKE) ../src && $(MAKE) all
	
clean:
	cd bin && make clean


