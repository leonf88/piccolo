set(MPILIBS mpi_cxx mpi open-rte open-pal)
set(STATICLIBS protobuf boost_thread-mt util)
set(DYNAMICLIBS lzo2 pthread rt dl numa)

protobuf_generate_cpp( EXAMPLE_PB_SRC EXAMPLE_PB_HDR examples.proto )

add_library(example 
            k-means.cc matmul.cc pagerank.cc shortest-path.cc
            test-tables.cc 
            nas/isort.cc nas/n-body.cc
            ${EXAMPLE_PB_HDR} ${EXAMPLE_PB_SRC})
add_dependencies(example worker_proto common_proto)

add_executable(example-dsm example-main.cc)

# we need to resolve static initializers, so glob all the symbols in
target_link_libraries(example-dsm -Wl,--whole-archive common worker kernel master example -Wl,--no-whole-archive )

if ($ENV{TCMALLOC})
  target_link_libraries(example-dsm -Wl,-Bstatic tcmalloc -Wl,-Bdynamic )
endif()

if ($ENV{JEMALLOC})
  target_link_libraries(example-dsm -Wl,--whole-archive jemalloc_pic -Wl,--no-whole-archive)
endif()

# link statically if possible
target_link_libraries(example-dsm -Wl,-Bstatic ${MPILIBS} ${STATICLIBS} gflags glog profiler blas unwind -Wl,-Bdynamic )
target_link_libraries(example-dsm ${DYNAMICLIBS})		      


GET_DIRECTORY_PROPERTY(cmake_include_directories INCLUDE_DIRECTORIES)
SET(swig_include_dirs)
FOREACH(it ${cmake_include_directories})
  SET(swig_include_dirs ${swig_include_dirs} "-I${it}")
ENDFOREACH(it)


ADD_CUSTOM_COMMAND(
 OUTPUT python_support_wrap.cc
 COMMAND swig
 ARGS 
 -python 
 -ignoremissing 
 -O 
 -c++ 
 -outdir ${CMAKE_CURRENT_BINARY_DIR} 
 ${swig_include_dirs}
 -o ${CMAKE_CURRENT_BINARY_DIR}/python_support_wrap.cc
 ${CMAKE_CURRENT_SOURCE_DIR}/python_support.i
 DEPENDS python_support.i)
 
add_executable(dsm-crawler 
			   python_support_wrap.cc
			   crawler/crawler-main.cc
			   ${EXAMPLE_PB_HDR} ${EXAMPLE_PB_SRC})

target_link_libraries(dsm-crawler -Wl,--whole-archive common worker kernel master -Wl,--no-whole-archive )
target_link_libraries(dsm-crawler -Wl,-Bstatic ${STATICLIBS} ${MPILIBS} gflags glog profiler blas unwind boost_python-mt-py26 -Wl,-Bdynamic )
target_link_libraries(dsm-crawler ${DYNAMICLIBS})
target_link_libraries(dsm-crawler ${PYTHON_LIBRARIES})
