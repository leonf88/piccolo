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
target_link_libraries(example-dsm -Wl,-Bstatic ${STATICLIBS} gflags glog profiler blas unwind -Wl,-Bdynamic )
target_link_libraries(example-dsm ${DYNAMICLIBS} ${MPILIBS})		      


GET_DIRECTORY_PROPERTY(cmake_include_directories INCLUDE_DIRECTORIES)
SET(include_flag)
FOREACH(it ${cmake_include_directories})
  SET(include_flag ${include_flag} "-I${it}")
ENDFOREACH(it)

GET_DIRECTORY_PROPERTY(cmake_link_directories LINK_DIRECTORIES)
SET(link_flag)
FOREACH(it ${cmake_link_directories})
  SET(link_flag ${link_flag} "-L${it}")
ENDFOREACH(it)

SET(swig_out ${CMAKE_CURRENT_BINARY_DIR}/piccolo_wrap.cc)
SET(swig_shim ${CMAKE_CURRENT_SOURCE_DIR}/python_support.cc)

ADD_CUSTOM_COMMAND(
 OUTPUT ${swig_out}
 COMMAND swig
 ARGS 
 -python -ignoremissing -O  -c++ 
 -w315 -w317 -w362 -w503 -w451 -w401
 -outdir ${CMAKE_CURRENT_BINARY_DIR} 
 ${include_flag}
 -o ${swig_out}
 ${CMAKE_CURRENT_SOURCE_DIR}/python_support.swig
 DEPENDS python_support.swig worker master common kernel)
 
ADD_CUSTOM_COMMAND(
  OUTPUT _piccolo.so
  COMMAND g++
  ARGS
  -shared
  -fPIC
  ${include_flag}
  ${link_flag}
  -o ${CMAKE_CURRENT_BINARY_DIR}/_piccolo.so
  ${swig_out} ${swig_shim} ${EXAMPLE_PB_HDR} ${EXAMPLE_PB_SRC}
  ${CMAKE_BINARY_DIR}/util/libcommon.a
  ${CMAKE_BINARY_DIR}/worker/libworker.a
  ${CMAKE_BINARY_DIR}/kernel/libkernel.a
  ${CMAKE_BINARY_DIR}/master/libmaster.a
  -Wl,-Bstatic 
  -lgflags -lglog -lprofiler -lunwind -lprotobuf  
  -lmpi_cxx -lmpi -lopen-rte -lopen-pal
  -Wl,-Bdynamic
  -lpthread -lutil -lnuma
  -llzo2 -lboost_python-mt-py26 -lboost_thread-mt -lblas 
  DEPENDS ${swig_out}
 )
  
 ADD_CUSTOM_TARGET(_piccolo ALL DEPENDS _piccolo.so)
