set(MPILIBS )
set(STATICLIBS protobuf boost_thread-mt util z mpi_cxx mpi open-rte open-pal SDL profiler unwind)
set(DYNAMICLIBS lzo2 pthread rt dl numa)

protobuf_generate_cpp(EXAMPLE_PB_SRC EXAMPLE_PB_HDR ${PICCOLO_SOURCE_DIR} examples.proto )
piccolo_pp(k-means.pp 
		   matmul.pp
		   pagerank.pp
		   raytrace.pp
		   shortest-path.pp
		   wordcount.pp)
		    

add_custom_target(example_proto DEPENDS ${EXAMLE_PB_HDR})

add_library(example 
            matmul.pp.cc             
            k-means.pp.cc
            shortest-path.pp.cc
            pagerank.pp.cc
            test-tables.cc
            test-tables2.cc
            wordcount.pp.cc
            raytrace.pp.cc
            facedet/cpp/pgmimage.c
            facedet/cpp/imagenet.cpp
            facedet/cpp/backprop.cpp
            faceclass.cc
            nas/isort.cc 
            nas/n-body.cc            
			bipartmatch-trigger.cc
            ${EXAMPLE_PB_HDR} 
            ${EXAMPLE_PB_SRC})
            
add_dependencies(example worker_proto common_proto)
add_executable(example-dsm main.cc)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/facedet/cpp)

# we need to resolve static initializers, so glob all the symbols in
target_link_libraries(example-dsm -Wl,-whole-archive common worker kernel master example -Wl,-no-whole-archive)
target_link_libraries(example-dsm gflags glog webgraph)

# link statically if possible
target_link_libraries(example-dsm -Wl,-Bstatic ${STATICLIBS} blas -Wl,-Bdynamic )
target_link_libraries(example-dsm ${DYNAMICLIBS} ${MPI_LINK_FLAGS})    

add_executable(matmul-mpi matmul-mpi.cc)
target_link_libraries(matmul-mpi glog gflags blas rt ${MPI_LINK_FLAGS})
