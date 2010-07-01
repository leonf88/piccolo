set(MPILIBS mpi_cxx mpi open-rte open-pal)
set(STATICLIBS protobuf boost_thread-mt util)
set(DYNAMICLIBS lzo2 pthread rt dl numa SDL)

protobuf_generate_cpp(EXAMPLE_PB_SRC EXAMPLE_PB_HDR examples.proto )
add_custom_target(example_proto DEPENDS ${EXAMLE_PB_HDR})

add_library(example 
            k-means.cc 
            matmul.cc 
            pagerank.cc 
            raytrace.cc
            shortest-path.cc
            test-tables.cc
            wordcount.cc
            nas/isort.cc 
            nas/n-body.cc
            ${EXAMPLE_PB_HDR} 
            ${EXAMPLE_PB_SRC})
            
add_dependencies(example worker_proto common_proto)
add_executable(example-dsm main.cc)

# we need to resolve static initializers, so glob all the symbols in
target_link_libraries(example-dsm -Wl,-whole-archive common worker kernel master example -Wl,-no-whole-archive)
target_link_libraries(example-dsm gflags glog gtest profiler gperfbase unwind)

# link statically if possible
target_link_libraries(example-dsm -Wl,-Bstatic ${STATICLIBS} blas -Wl,-Bdynamic )
target_link_libraries(example-dsm ${DYNAMICLIBS} ${MPI_LINK_FLAGS})    
