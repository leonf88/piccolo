MYD := $(notdir $(CURDIR))
.PRECIOUS : %.pb.cc %.pb.h

VPATH := .:src
CPPFLAGS := $(CPPFLAGS) -I. -Isrc -Iextlib/glog/src/ -Iextlib/gflags/src/
LDFLAGS := 
LDDIRS := -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/
LIBS := -Wl,-Bstatic -lglog -lgflags -Wl,-Bdynamic -lboost_thread -lprotobuf 

AR_LD := ld --eh-frame-hdr -r 

all: bin/test-shortest-path

SRCS := $(shell find src -name '*.c' -o -name '*.cc')

depend:
	CPPFLAGS="$(CPPFLAGS)" ./makedep.sh > Makefile.dep

LIBCOMMON_OBJS := src/util/common.pb.o src/util/jenkins-hash.o src/util/file.o \
									src/util/fake-mpi.o src/util/common.o

LIBKERNEL_OBS := src/worker/kernels/pagerank.o src/worker/kernels/shortest-path.o
LIBWORKER_OBJS := src/worker/worker.pb.o src/worker/worker.o src/worker/kernel.o 

bin/libcommon.a : $(LIBCOMMON_OBJS)
	$(AR_LD) $^ -o $@

bin/libworker.a : $(LIBWORKER_OBJS)
	$(AR_LD) $^ -o $@

bin/libkernel.a : $(LIBKERNEL_OBS) 
	$(AR_LD) $^ -o $@

bin/test-shortest-path: bin/libkernel.a bin/libworker.a bin/libcommon.a test/test-shortest-path.o
	g++ $(LDFLAGS) $(LDDIRS) `mpic++ -showme:link` -o $@ $^ $(LIBS) 

clean:
	find src -name '*.o' -exec rm {} \;
	rm -f bin/*

%.pb.cc %.pb.h : %.proto
	protoc -Isrc/ --cpp_out=$(CURDIR)/src $<

$(shell mkdir -p bin/)
-include Makefile.dep
