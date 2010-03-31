cmake_minimum_required (VERSION 2.6) 
set(CMAKE_CXX_COMPILER "distcc g++")
project (PICCOLO) 

set(CMAKE_MODULE_PATH "${PICCOLO_SOURCE_DIR}/../extlib/")

find_package(ProtoBuf REQUIRED)
find_package(SWIG REQUIRED)
find_package(PythonLibs REQUIRED)

include(${SWIG_USE_FILE})
 
set(CMAKE_CXX_FLAGS "-ggdb2")
set(CMAKE_SWIG_FLAGS "-ignoremissing -O -c++ -python")

include_directories( ${PROTOBUF_INCLUDE_DIRS} )
include_directories( ${PYTHON_INCLUDE_PATH} )
include_directories( .
                     ${PICCOLO_SOURCE_DIR}/../extlib/glog/src 
                     ${PICCOLO_SOURCE_DIR}/../extlib/gflags/src 
                     ${PICCOLO_SOURCE_DIR}/../extlib/gperftools/src 
                     /home/power/share/include ) 

link_directories( ${PICCOLO_SOURCE_DIR}/../extlib/glog/.libs/
                  ${PICCOLO_SOURCE_DIR}/../extlib/gflags/.libs/
		  ${PICCOLO_SOURCE_DIR}/../extlib/gperftools/
		  /home/power/share/lib )

add_subdirectory (util)
add_subdirectory (worker)
add_subdirectory (kernel)
add_subdirectory (master)
add_subdirectory (test)
add_subdirectory (examples)
add_subdirectory (examples/crawler)
add_subdirectory (examples/nas)
