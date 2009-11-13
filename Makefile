MYD := $(notdir $(CURDIR))
.PRECIOUS : %.pb.cc %.pb.h

VPATH := .:src
CPPFLAGS := $(CPPFLAGS) -I. -Isrc -Iextlib/glog/src/ -Iextlib/gflags/src/
LDFLAGS := 
LDDIRS := -Lextlib/glog/.libs/ -Lextlib/gflags/.libs/
DYNAMIC_LIBS := -lboost_thread -lprotobuf
STATIC_LIBS := -Wl,-Bstatic -lglog -lgflags -Wl,-Bdynamic 

LINK_LIB := ld --eh-frame-hdr -r
LINK_BIN := $(CXX) $(LDDIRS) `mpic++ -showme:link`

LIBCOMMON_OBJS := src/util/common.pb.o src/util/jenkins-hash.o src/util/file.o \
									src/util/fake-mpi.o src/util/common.o

LIBWORKER_OBJS := src/worker/worker.pb.o src/worker/worker.o src/worker/kernel.o src/worker/accumulator.o


all: bin/test-shortest-path

depend:
	CPPFLAGS="$(CPPFLAGS)" ./makedep.sh > Makefile.dep


bin/libcommon.a : $(LIBCOMMON_OBJS)
	$(LINK_LIB) $^ -o $@

bin/libworker.a : $(LIBWORKER_OBJS)
	$(LINK_LIB) $^ -o $@
	
bin/test-shortest-path: bin/libworker.a bin/libcommon.a src/test/test-shortest-path.o
	$(LINK_BIN) $(DYNAMIC_LIBS) $^ -o $@ $(STATIC_LIBS)

clean:
	find src -name '*.o' -exec rm {} \;
	rm -f bin/*

%.pb.cc %.pb.h : %.proto
	protoc -Isrc/ --cpp_out=$(CURDIR)/src $<

$(shell mkdir -p bin/)
-include Makefile.dep
