.PRECIOUS : %.pb.cc %.pb.h

MPI_INC := -I/usr/include/mpi
VPATH := src/

MPI_LINK := mpic++
MPI_LIBDIR := 

#MPI_LINK := /home/power/local/mpich2/bin/mpic++
#MPI_INC := -I/home/power/local/mpich2/include
#MPI_LIBDIR := -L/home/power/local/mpich2/lib

CPPFLAGS := $(CPPFLAGS) -I. -Isrc -Iextlib/glog/src/ -Iextlib/gflags/src/  $(MPI_INC)
CFLAGS := -ggdb2 -O0 -Wall -Wno-unused-function -Wno-sign-compare $(CPPFLAGS)
CXXFLAGS := $(CFLAGS)

UPCC := upcc
UPCFLAGS := $(CPPFLAGS) --uses-mpi --network=mpi
UPC_LIBDIR := -L/home/power/local/upc/opt/lib

LDFLAGS := 
LDDIRS := -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/ $(MPI_LIBDIR) $(UPC_LIBDIR)

DYNAMIC_LIBS := -lboost_thread -lprotobuf
STATIC_LIBS := -Wl,-Bstatic -lglog -lgflags -Wl,-Bdynamic 
UPC_LIBS := -lgasnet-mpi-par -lupcr-mpi-par -lumalloc -lammpi
MPI_LIBS := -lmpi_cxx

LINK_LIB := ld --eh-frame-hdr -r
LINK_BIN := $(MPI_LINK) 


LIBCOMMON_OBJS := src/util/common.pb.o src/util/file.o src/util/common.o 
LIBRPC_OBJS := src/util/rpc.o
LIBTEST_OBJS := src/test/file-helper.o src/test/test.pb.o
LIBWORKER_OBJS := src/worker/worker.pb.o src/worker/worker.o src/worker/kernel.o src/master/master.o

%.o: %.cc
	@echo CC :: $<
	@$(CXX) $(CXXFLAGS) $(TARGET_ARCH) -c $< -o $@


all: bin/test-shortest-path bin/test-tables bin/test-shortest-path-upc bin/test-pr

ALL_SOURCES := $(shell find src -name '*.h' -o -name '*.cc' -o -name '*.proto')

depend:
	CPPFLAGS="$(CPPFLAGS)" ./makedep.sh > Makefile.dep

Makefile.dep: $(ALL_SOURCES)
	CPPFLAGS="$(CPPFLAGS)" ./makedep.sh > Makefile.dep

bin/libcommon.a : $(LIBCOMMON_OBJS)
	$(LINK_LIB) $^ -o $@

bin/libworker.a : $(LIBWORKER_OBJS)
	$(LINK_LIB) $^ -o $@

bin/librpc.a : $(LIBRPC_OBJS)
	$(LINK_LIB) $^ -o $@
	
bin/libtest.a : $(LIBTEST_OBJS)
	$(LINK_LIB) $^ -o $@
		
bin/test-shortest-path: bin/libworker.a bin/libcommon.a bin/librpc.a bin/libtest.a src/test/test-shortest-path.o
	$(LINK_BIN) $(LDDIRS) $(DYNAMIC_LIBS) $^ -o $@ $(STATIC_LIBS)

bin/test-tables: bin/libworker.a bin/libcommon.a bin/librpc.a src/test/test-tables.o
	$(LINK_BIN) $(LDDIRS) $(DYNAMIC_LIBS) $^ -o $@ $(STATIC_LIBS) 
	
bin/test-shortest-path-upc: bin/libcommon.a bin/libtest.a src/test/test-shortest-path.upc	 
		$(UPCC) $(UPCFLAGS) $(LDDIRS) $(DYNAMIC_LIBS) $^ -o $@ $(STATIC_LIBS) $(MPI_LIBS)

bin/test-pr: bin/libworker.a bin/libcommon.a bin/librpc.a bin/libtest.a src/test/test-pr.o 
	$(LINK_BIN) $(LDDIRS) $(DYNAMIC_LIBS) $^ -o $@ $(STATIC_LIBS)

clean:
	find src -name '*.o' -exec rm {} \;
	rm -f bin/*

%.pb.cc %.pb.h : %.proto
	protoc -Isrc/ --cpp_out=$(CURDIR)/src $<

%.upc.o: %.upc	 

$(shell mkdir -p bin/)
-include Makefile.dep
