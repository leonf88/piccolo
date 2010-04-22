set(MPILIBS open-rte open-pal boost_thread-mt util )
set(OTHERLIBS mpi_cxx mpi lzo2 pthread rt dl numa protobuf)

protobuf_generate_cpp( EXAMPLE_PB_SRC EXAMPLE_PB_HDR examples.proto )

add_library(example 
            k-means.cc matmul.cc pagerank.cc shortest-path.cc 
            nas/isort.cc nas/n-body.cc
            ${EXAMPLE_PB_HDR} ${EXAMPLE_PB_SRC})
add_dependencies(example worker_proto common_proto)

add_executable(example-dsm example-main.cc)

# we need to resolve static initializers, so glob all the symbols in
target_link_libraries(example-dsm -Wl,--whole-archive common worker kernel master example -Wl,--no-whole-archive )

# link statically if we can
if ($ENV{TCMALLOC})
  target_link_libraries(example-dsm -Wl,-Bstatic tcmalloc -Wl,-Bdynamic )
endif()

if ($ENV{JEMALLOC})
  target_link_libraries(example-dsm -Wl,--whole-archive jemalloc_pic -Wl,--no-whole-archive)
endif()

target_link_libraries(example-dsm -Wl,-Bstatic ${MPILIBS} gflags glog profiler blas unwind -Wl,-Bdynamic )
target_link_libraries(example-dsm ${OTHERLIBS})		      

swig_add_module(python_support python python_support.i)
swig_link_libraries(python_support ${PYTHON_LIBRARIES})

add_executable(dsm-crawler crawler/crawler_support.cc)

target_link_libraries(dsm-crawler -Wl,--whole-archive common worker kernel master -Wl,--no-whole-archive )
target_link_libraries(dsm-crawler -Wl,-Bstatic ${MPILIBS} gflags glog profiler blas unwind -Wl,-Bdynamic )
target_link_libraries(dsm-crawler ${OTHERLIBS})
target_link_libraries(dsm-crawler boost_python-mt ${PYTHON_LIBRARIES})
