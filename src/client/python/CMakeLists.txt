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

SET(SWIG_LIBS ${PROTOBUF_LIBS}
               boost_python-py26 boost_thread-mt util z lzo2 pthread rt)
FOREACH(lib ${SWIG_LIBS})
  SET(link_flag ${link_flag} "-l${lib}")
ENDFOREACH(lib)

SET(swig_out ${CMAKE_CURRENT_BINARY_DIR}/piccolo_wrap.cc)
SET(swig_shim ${CMAKE_CURRENT_SOURCE_DIR}/python_support.cc)

ADD_CUSTOM_COMMAND(
 OUTPUT ${swig_out}
 COMMAND swig
 ARGS 
 -python -ignoremissing -c++
 -w315 -w317 -w362 -w503 -w451 -w401
 -outdir ${CMAKE_CURRENT_BINARY_DIR} 
 ${include_flag}
 ${MPI_COMPILE_FLAGS}
 -o ${swig_out}
 ${CMAKE_CURRENT_SOURCE_DIR}/python_support.swig
 DEPENDS python_support.swig ${swig_shim})
 
ADD_CUSTOM_COMMAND(
  OUTPUT _piccolo.so
  COMMAND g++
  ARGS
  -ggdb2
  -fPIC
  -shared
  ${include_flag}
  -Wl,--start-group
  ${CMAKE_BINARY_DIR}/util/libcommon.a
  ${CMAKE_BINARY_DIR}/worker/libworker.a
  ${CMAKE_BINARY_DIR}/kernel/libkernel.a
  ${CMAKE_BINARY_DIR}/master/libmaster.a
  ${CMAKE_BINARY_DIR}/external/google-flags/libgflags.a    
  ${CMAKE_BINARY_DIR}/external/google-logging/libglog.a
  ${swig_out} ${swig_shim} 
  ${MPI_LINK_FLAGS}  
  ${link_flag}
  -Wl,--end-group
  -o ${CMAKE_CURRENT_BINARY_DIR}/_piccolo.so
  ${EXAMPLE_PB_HDR} ${EXAMPLE_PB_SRC}
  DEPENDS ${swig_out} worker master common kernel example
)
  
ADD_CUSTOM_TARGET(_piccolo ALL DEPENDS _piccolo.so)