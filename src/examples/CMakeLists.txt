protobuf_generate_cpp( EXAMPLE_PB_SRC EXAMPLE_PB_HDR examples.proto )
add_executable(example-dsm example-main.cc 
                           k-means.cc  
			   matmul.cc
			   pagerank.cc
			   shortest-path.cc 
			   nas/isort.cc
			   nas/n-body.cc
			   ${WORKER_PB_HDR}
			   ${COMMON_PB_HDR}
			   ${EXAMPLE_PB_HDR}
			   ${EXAMPLE_PB_SRC})

# we need to resolve static initializers, so glob all the symbols in
FOREACH(lib common worker kernel master)
  TARGET_LINK_LIBRARIES(example-dsm -Wl,--whole-archive ${lib} -Wl,--no-whole-archive )
ENDFOREACH()

# link statically if we can
TARGET_LINK_LIBRARIES(example-dsm -Wl,-Bstatic gflags glog blas -Wl,-Bdynamic )

target_link_libraries(example-dsm 
		      protobuf
		      mpi_cxx mpi
		      open-rte open-pal
		      boost_thread-mt
		      lzo2
		      pthread
		      rt dl)