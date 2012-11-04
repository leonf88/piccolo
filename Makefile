all: build/.configured
	cd src && ./waf.py build

build/.configured: src/wscript
	cd src && ./waf.py configure
	touch build/.configured

clean:
	rm -rf build/

