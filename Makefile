.PRECIOUS : %.pb.cc %.pb.h

VPATH := src/

MPI_INC := -I/usr/lib/openmpi/include/
MPI_LINK := mpic++.openmpi
MPI_LIBDIR := -L/usr/local/lib
MPI_LIBS := -lmpi_cxx -lmpi  -lopen-rte -lopen-pal -ldl -lutil -lpthread

#MPI_LINK := /home/power/local/mpich2/bin/mpic++ 
#MPI_INC := -I/home/power/local/mpich2/include
#MPI_LIBDIR := -L/home/power/local/mpich2/lib
#MPI_LIBS := -lmpichcxx -lmpich

CDEBUG := -ggdb2
COPT := -O3 
CPPFLAGS := $(CPPFLAGS) -I. -Isrc -Iextlib/glog/src/ -Iextlib/gflags/src/  $(MPI_INC)
CFLAGS := $(CDEBUG) $(COPT) -Wall -Wno-unused-function -Wno-sign-compare $(CPPFLAGS)
CXXFLAGS := $(CFLAGS)

UPCC := upcc
UPCFLAGS := $(CPPFLAGS) --network=udp
UPC_LIBDIR := -L/home/power/local/upc/opt/lib
UPC_THREADS := 2

LDFLAGS := 
LDDIRS := -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/ -L/usr/local/lib/ $(MPI_LIBDIR) $(UPC_LIBDIR)

DYNAMIC_LIBS := -lprotobuf
STATIC_LIBS := -lglog -lgflags -lprofiler -lunwind -lboost_thread-mt
UPC_LIBS := -lgasnet-mpi-par -lupcr-mpi-par -lumalloc -lammpi

LINK_LIB := ld -r
LINK_BIN := $(MPI_LINK) 


LIBCOMMON_OBJS := src/util/common.pb.o src/util/file.o src/util/common.o src/util/coder.o
LIBRPC_OBJS := src/util/rpc.o
LIBTEST_OBJS := src/test/file-helper.o src/test/test.pb.o
LIBWORKER_OBJS := src/worker/worker.pb.o src/worker/worker.o src/worker/kernel.o src/master/master.o src/worker/table-msgs.o

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(TARGET_ARCH) -c $< -o $@


all: bin/test-shortest-path bin/test-tables bin/test-shortest-path-upc bin/test-pr bin/pr-upc

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
	
bin/test-shortest-path-upc: bin/libtest.a bin/libcommon.a src/test/test-shortest-path.upc	 
	$(UPCC) $(UPCFLAGS) $(LDDIRS)  $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) $(MPI_LIBS) 

bin/pr-upc: bin/libcommon.a bin/libtest.a src/test/pagerank.upc
	$(UPCC) -T $(UPC_THREADS) $(UPCFLAGS) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) $(MPI_LIBS)

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
