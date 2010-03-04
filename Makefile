.PRECIOUS : %.pb.cc %.pb.h %_wrap.cc

VPATH := src/

MPI_INC :=  -I/home/power/share/include
MPI_LINK := /home/power/share/bin/mpic++
MPI_LIBDIR := -L/home/power/share/lib/ 
MPI_LIBS := -L/home/power/share/lib -lmpi_cxx -lmpi -lopen-rte -lopen-pal -lnuma 

PY_INC := -I/usr/include/python2.6/

DISTCC := distcc
CXX := g++
CDEBUG := -ggdb1
COPT :=
COPT := -O2 -DNDEBUG

CPPFLAGS := $(CPPFLAGS) -Isrc -Ibin -Iextlib/glog/src/ -Iextlib/gflags/src/ $(MPI_INC) $(PY_INC)

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

DYNAMIC_LIBS := -ldl -lutil -lpthread -lrt -lprotobuf -lnuma  $(PROF_LIBS)
STATIC_LIBS := -lglog -lgflags -lboost_thread-mt -llzo2 

UPC_LIBS := -lgasnet-mpi-par -lupcr-mpi-par -lumalloc -lammpi

LDDIRS := $(LDDIRS) -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/ $(MPI_LIBDIR) $(UPC_LIBDIR)

LINK_LIB := ld -r 
LINK_BIN := $(MPI_LINK) 
LINK_BIN_FLAGS := $(LDDIRS) -Wl,-Bstatic $(STATIC_LIBS) -Wl,-Bdynamic $(DYNAMIC_LIBS) 

LIBCOMMON_OBJS := bin/util/common.pb.o \
		  bin/util/file.o \
		  bin/util/common.o

LIBRPC_OBJS := bin/util/rpc.o

LIBEXAMPLE_OBJS := bin/examples/upc/file-helper.o \
		   bin/examples/graph.pb.o

LIBKERNEL_OBJS := bin/kernel/table.o\
		  bin/kernel/table-registry.o \
		  bin/kernel/kernel-registry.o

LIBWORKER_OBJS := bin/worker/worker.pb.o bin/worker/worker.o\
		  bin/master/master.o $(LIBKERNEL_OBJS)

all: 	 setup\
         bin/shortest-path\
	 bin/mpi-test \
	 bin/pagerank\
	 bin/k-means\
	 bin/test-tables\
	 bin/test-hashmap\
	 bin/crawler\
	 dsm_paper
#  bin/shortest-path-upc\
#	 bin/pr-upc\

setup:
	@cd src && find . -type d -exec mkdir -p ../bin/{} \;

dsm_paper:
	cd paper && make

ALL_SOURCES := $(shell find src -name '*.h' -o -name '*.cc' -o -name '*.proto')

CORE_LIBS := bin/libworker.a bin/libcommon.a bin/librpc.a
EXAMPLE_LIBS := $(CORE_LIBS) bin/libexample.a

depend: Makefile.dep

Makefile.dep: $(ALL_SOURCES)
	CPPFLAGS="$(CPPFLAGS)" ./makedep.sh

bin/libcommon.a : $(LIBCOMMON_OBJS)
	$(LINK_LIB) $(LIBCOMMON_OBJS) -o $@

bin/librpc.a : $(LIBRPC_OBJS) $(LIBCOMMON_OBJS)
	$(LINK_LIB) $(LIBRPC_OBJS) -o $@

bin/libworker.a :  $(LIBCOMMON_OBJS) $(LIBRPC_OBJS) $(LIBWORKER_OBJS)
	$(LINK_LIB) $(LIBWORKER_OBJS) -o $@

bin/libexample.a : $(LIBEXAMPLE_OBJS)
	$(LINK_LIB) $^ -o $@

bin/test-tables: $(CORE_LIBS) bin/test/test-tables.o
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS)

bin/shortest-path: $(EXAMPLE_LIBS) bin/examples/shortest-path.o
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS)

bin/pagerank: $(EXAMPLE_LIBS) bin/examples/pagerank.o 
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS)

bin/k-means: $(EXAMPLE_LIBS) bin/examples/k-means.o 
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS)
	
bin/crawler: bin/examples/crawler_support_wrap.o bin/examples/crawler_support.o $(EXAMPLE_LIBS)
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS) -lpython2.6 -lboost_python-mt

bin/test-hashmap: $(EXAMPLE_LIBS) bin/test/test-hashmap.o
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS)

bin/mpi-test: bin/test/mpi-test.o bin/libcommon.a
	$(LINK_BIN) $(LDDIRS) $^ -o $@  $(LINK_BIN_FLAGS)

bin/shortest-path-upc: bin/libexample.a bin/libcommon.a src/examples/upc/shortest-path.upc	 
	$(UPCC) $(UPCFLAGS) $(LDDIRS)  $^ -o $@ $(MPI_LIBS)  $(LINK_BIN_FLAGS)

bin/pagerank-upc: bin/libexample.a bin/libcommon.a src/examples/upc/pagerank.upc
	$(UPCC) $(UPC_THREADS) $(UPCFLAGS) $(LDDIRS) $^ -o $@ $(MPI_LIBS) $(LINK_BIN_FLAGS)

clean:
	rm -rf bin/
	find src -name '*.pb.h' -exec rm {} \;
	find src -name '*.pb.cc' -exec rm {} \;
	find src -name '*_wrap.cc' -exec rm {} \;

%.pb.cc %.pb.h : %.proto
	protoc -Isrc/ --cpp_out=$(CURDIR)/src/ $<

%_wrap.cc : %.h
	swig -O -c++ -python $(CPPFLAGS) -o $@ $< 

%.upc.o: %.upc	 

bin/%.o: src/%.cc
	$(DISTCC) $(CXX) $(CXXFLAGS) $(TARGET_ARCH) -c $< -o $@


$(shell mkdir -p bin/)
-include Makefile.dep
