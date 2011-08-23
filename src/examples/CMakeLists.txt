set(EXAMPLE_LIBS 
	${EXAMPLE_LIBS} 
	${MPI_LIBS}
	${BLAS_LIBS}
  	${PROTOBUF_LIBS})  	  	
set(EXAMPLE_LIBS ${EXAMPLE_LIBS} boost_thread-mt util z lzo2 pthread rt)

protobuf_generate_cpp(EXAMPLE_PB_SRC EXAMPLE_PB_HDR ${PICCOLO_SOURCE_DIR} examples.proto )
piccolo_pp(k-means.pp 
           matmul.pp 
           pagerank.pp
#           accelpagerank.pp
           raytrace.pp
           shortest-path.pp
           shortest-path-trigger.pp
           wordcount.pp)

add_custom_target(example_proto DEPENDS ${EXAMPLE_PB_HDR})

set (EXTRA_SRC "")
if (SDL_FOUND)
  set (EXTRA_SRC raytrace.pp.cc)
  set (EXAMPLE_LIBS ${EXAMPLE_LIBS} SDL)
endif()

add_library(example 
            matmul.pp.cc             
            k-means.pp.cc
            shortest-path.pp.cc
            shortest-path-trigger.pp.cc
            pagerank.pp.cc
#			accelpagerank.pp.cc
            test-tables.cc
            test-tables2.cc
            wordcount.pp.cc
            facedet/cpp/pgmimage.c
            facedet/cpp/imagenet.cpp
            facedet/cpp/backprop.cpp
            faceclass.cc
            nas/isort.cc 
            nas/n-body.cc
            bipartmatch-trigger.cc
            bipartmatch.cc
            ${EXTRA_SRC}
            ${EXAMPLE_PB_HDR} 
            ${EXAMPLE_PB_SRC})
            
add_dependencies(example worker_proto common_proto)
add_executable(example-dsm main.cc)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/facedet/cpp)

# we need to resolve static initializers, so glob all the symbols together with -whole-archive
target_link_libraries(example-dsm -Wl,-whole-archive common worker kernel master example -Wl,-no-whole-archive)
target_link_libraries(example-dsm gflags glog webgraph)

target_link_libraries(example-dsm ${EXAMPLE_LIBS})

add_executable(matmul-mpi matmul-mpi.cc)
target_link_libraries(matmul-mpi glog gflags blas rt ${MPI_LINK_FLAGS})
