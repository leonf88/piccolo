.PRECIOUS : %.pb.cc %.pb.h

VPATH := src/

MPI_INC :=  -I/home/power/share/include
MPI_LINK := /home/power/share/bin/mpic++
MPI_LIBDIR := -L/home/power/share/lib/ 
MPI_LIBS := 

PY_INC := -I/usr/include/python2.6/

CXX := distcc g++
CDEBUG := -ggdb2
COPT :=  -O2
CPPFLAGS := $(CPPFLAGS) -I. -Isrc -Iextlib/glog/src/ -Iextlib/gflags/src/ $(MPI_INC) $(PY_INC)

USE_CPU_PROFILE := 1
USE_TCMALLOC := 
USE_OPROFILE := 

ifneq ($(USE_CPU_PROFILE),)
	PROF_LIBS := -lprofiler -lunwind
	CPPFLAGS := $(CPPFLAGS) -DCPUPROF=1 
endif

ifneq ($(USE_TCMALLOC),)
	PROF_LIBS := $(PROF_LIBS) -ltcmalloc
	CPPFLAGS := $(CPPFLAGS) -DHEAPPROF=1
endif

ifneq ($(USE_OPROFILE),)
	CFLAGS := $(CFLAGS) -fno-omit-frame-pointer
endif

CFLAGS := $(CFLAGS) $(CDEBUG) $(COPT) -Wall -Wno-unused-function -Wno-sign-compare $(CPPFLAGS)
CXXFLAGS := $(CFLAGS)

UPCC := /home/power/share/bupc/bin/upcc
UPCFLAGS := $(CPPFLAGS) --network=udp -O
UPC_LIBDIR := -L/home/power/share/upc/opt/lib
UPC_THREADS := -T 20
#UPC_THREADS :=

LDFLAGS := 
LDDIRS := $(LDDIRS) -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/ $(MPI_LIBDIR) $(UPC_LIBDIR)

DYNAMIC_LIBS := -Wl,-Bdynamic -ldl -lutil -lpthread -lrt -lprotobuf -lnuma $(PROF_LIBS)  
STATIC_LIBS := -Wl,-Bstatic -lglog -lgflags -lboost_thread-mt -llzo2 $(MPI_LIBS) 
UPC_LIBS := -lgasnet-mpi-par -lupcr-mpi-par -lumalloc -lammpi

LINK_LIB := ld -r $(LDFLAGS)
LINK_BIN := $(MPI_LINK)  $(LDFLAGS)


LIBCOMMON_OBJS := src/util/common.pb.o \
									src/util/file.o \
									src/util/common.o \
									src/util/coder.o

LIBRPC_OBJS := src/util/rpc.o

LIBEXAMPLE_OBJS := src/examples/upc/file-helper.o \
									 src/examples/graph.pb.o

LIBKERNEL_OBJS := src/kernel/table.o\
									src/kernel/table-registry.o \
									src/kernel/kernel-registry.o

LIBWORKER_OBJS := src/worker/worker.pb.o src/worker/worker.o\
								  src/master/master.o $(LIBKERNEL_OBJS)

all: bin/shortest-path\
	 bin/mpi-test \
	 bin/pagerank\
	 bin/k-means\
	 bin/test-tables\
	 bin/test-hashmap\
	 bin/crawler_support.so
#  bin/shortest-path-upc\
#	 bin/pr-upc\

ALL_SOURCES := $(shell find src -name '*.h' -o -name '*.cc' -o -name '*.proto')

CORE_LIBS := bin/libworker.a bin/libcommon.a bin/librpc.a
EXAMPLE_LIBS := $(CORE_LIBS) bin/libexample.a

depend: Makefile.dep

Makefile.dep: $(ALL_SOURCES)
	CPPFLAGS="$(CPPFLAGS)" ./makedep.sh

bin/libcommon.a : $(LIBCOMMON_OBJS)
	$(LINK_LIB) $^ -o $@

bin/libworker.a : $(LIBWORKER_OBJS)
	$(LINK_LIB) $^ -o $@

bin/librpc.a : $(LIBRPC_OBJS)
	$(LINK_LIB) $^ -o $@
	
bin/libexample.a : $(LIBEXAMPLE_OBJS)
	$(LINK_LIB) $^ -o $@
		
bin/test-tables: $(CORE_LIBS) src/test/test-tables.o
	$(LINK_BIN) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) 
	
bin/shortest-path: $(EXAMPLE_LIBS) src/examples/shortest-path.o
	$(LINK_BIN) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) 

bin/pagerank: $(EXAMPLE_LIBS) src/examples/pagerank.o 
	$(LINK_BIN) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) 

bin/k-means: $(EXAMPLE_LIBS) src/examples/k-means.o 
	$(LINK_BIN) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) 

bin/test-hashmap: $(EXAMPLE_LIBS) src/test/test-hashmap.o
	$(LINK_BIN) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) 

bin/mpi-test: src/test/mpi-test.o bin/libcommon.a
	$(LINK_BIN) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) 

bin/shortest-path-upc: bin/libexample.a bin/libcommon.a src/examples/upc/shortest-path.upc	 
	$(UPCC) $(UPCFLAGS) $(LDDIRS)  $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) $(MPI_LIBS) 

bin/pagerank-upc: bin/libexample.a bin/libcommon.a src/examples/upc/pagerank.upc
	$(UPCC) $(UPC_THREADS) $(UPCFLAGS) $(LDDIRS) $^ -o $@ $(STATIC_LIBS) $(DYNAMIC_LIBS) $(MPI_LIBS)

bin/crawler_support.so: src/examples/crawler_support_wrap.o src/examples/crawler_support.o
	touch bin/crawler_support.so

clean:
	rm -f bin/*
	find src -name '*.o' -exec rm {} \;
	find src -name '*.pb.h' -exec rm {} \;
	find src -name '*.pb.cc' -exec rm {} \;

%.pb.cc %.pb.h : %.proto
	protoc -Isrc/ --cpp_out=$(CURDIR)/src $<

%.upc.o: %.upc	 

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(TARGET_ARCH) -c $< -o $@

%.o: %.cxx
	$(CXX) $(CXXFLAGS) $(TARGET_ARCH) -c $< -o $@

%_wrap.cxx : %.swig Makefile %.h
	swig -c++ -python $(CPPFLAGS)  $<


$(shell mkdir -p bin/)
-include Makefile.dep
