all: configure
        cd src && ./waf.py build

configure: src/wscript
        cd src && ./waf.py configure

clean:
        rm -rf build/

