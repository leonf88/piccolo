all:
	cd bin && cmake ../src && $(MAKE)
	
clean:
	cd bin && make clean


