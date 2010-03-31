$(shell mkdir -p bin/)

.PRECIOUS : %.pb.cc %.pb.h

MPI_INC :=  -I/home/power/share/include
MPI_LIBS := -pthread -L/home/power/share/lib -lmpi_cxx -lmpi -lopen-rte -lopen-pal -lnuma 
#-Wl,--whole-archive -libverbs -lmthca -Wl,--no-whole-archive
MPI_LINK := g++ 

VPATH := src/

PY_INC := -I/usr/include/python2.6/

DISTCC := distcc
CXX := g++
CDEBUG := -ggdb2
#COPT :=
COPT := -O2 -DNDEBUG
CPPFLAGS := $(CPPFLAGS) -Isrc -Ibin -Iextlib/glog/src/ -Iextlib/gflags/src/ $(MPI_INC) $(PY_INC)

USE_CPU_PROFILE := 1
USE_TCMALLOC := 
USE_OPROFILE :=

ifneq ($(USE_CPU_PROFILE),)
	PROF_LIBS := -Lextlib/gperftools/.libs/ -lprofiler -lunwind
	CPPFLAGS := $(CPPFLAGS) -Iextlib/gperftools/src/ -DCPUPROF=1 
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
UPC_LIBS := -lgasnet-mpi-par -lupcr-mpi-par -lumalloc -lammpi

LDDIRS := $(LDDIRS) -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/ $(UPC_LIBDIR)

DYNAMIC_LIBS :=  $(PROF_LIBS)
STATIC_LIBS :=   $(MPI_LIBS) -lblas -lprotobuf -lglog -lgflags -lboost_thread-mt -llzo2 -Wl,-Bdynamic -ldl -lutil -lpthread -lrt

LINK_LIB := ld -r 
LINK_BIN := $(MPI_LINK) $(LDDIRS)
LINK_BIN_FLAGS := $(DYNAMIC_LIBS) $(STATIC_LIBS) 

LIBCOMMON_OBJS := bin/util/common.pb.o \
		  bin/util/file.o \
		  bin/util/common.o

LIBRPC_OBJS := bin/util/rpc.o

LIBEXAMPLE_OBJS := bin/examples/examples.pb.o \
                   bin/examples/upc/file-helper.o
                   

LIBKERNEL_OBJS := bin/kernel/table.o\
		  bin/kernel/table-registry.o \
		  bin/kernel/kernel-registry.o

LIBWORKER_OBJS := bin/worker/worker.pb.o\
                  bin/worker/worker.o\
		  bin/master/master.o $(LIBKERNEL_OBJS)

#  bin/shortest-path-upc\
#	 bin/pr-upc\

all:   setup\
       bin/example-dsm\
       bin/crawler\
       bin/py-shell\
       bin/test-hashmap\
       dsm_paper

dsm_paper:
	cd paper && $(MAKE)

setup:
	@cd src && find . -type d -exec mkdir -p ../bin/{} \;

ALL_SOURCES := $(shell find src -name '*.h' -o -name '*.cc' -o -name '*.proto')

CORE_LIBS := bin/libworker.a bin/libcommon.a bin/librpc.a
EXAMPLE_LIBS := $(CORE_LIBS) bin/libexample.a
EXAMPLE_OBJS := bin/examples/example-main.o \
		bin/examples/shortest-path.o \
		bin/examples/pagerank.o \
		bin/examples/k-means.o \
		bin/examples/matmul.o \
		bin/examples/nas/isort.o \
		bin/examples/nas/n-body.o \
		bin/test/test-tables.o

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

bin/example-dsm: $(EXAMPLE_LIBS) $(EXAMPLE_OBJS) 
	$(LINK_BIN) $^ -o $@ $(LINK_BIN_FLAGS) 

bin/py-shell: bin/examples/python_support_wrap.o bin/examples/py-shell.o $(EXAMPLE_LIBS)
	$(LINK_BIN) $^ -o $@ -lpython2.6 -lboost_python-mt $(LINK_BIN_FLAGS) 
	
bin/crawler: bin/examples/python_support_wrap.o bin/examples/crawler/crawler_support.o $(EXAMPLE_LIBS)
	$(LINK_BIN) $^ -o $@ -lpython2.6 -lboost_python-mt $(LINK_BIN_FLAGS) 

bin/test-hashmap: $(EXAMPLE_LIBS) bin/test/test-hashmap.o
	$(LINK_BIN) $^ -o $@  $(LINK_BIN_FLAGS)

bin/mpi-test: bin/test/mpi-test.o bin/libcommon.a
	$(LINK_BIN) $^ -o $@  $(LINK_BIN_FLAGS)

bin/shortest-path-upc: bin/libexample.a bin/libcommon.a src/examples/upc/shortest-path.upc	 
	$(UPCC) $(UPCFLAGS) $(LDDIRS)  $^ -o $@ $(MPI_LIBS) $(LINK_BIN_FLAGS)

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
	swig -ignoremissing -O -c++ -python -I/usr/include $(CPPFLAGS) -o $@ $< 

%.upc.o: %.upc	 

bin/%.o: src/%.cc
	$(DISTCC) $(CXX) $(CXXFLAGS) $(TARGET_ARCH) -c $< -o $@

-include Makefile.dep
