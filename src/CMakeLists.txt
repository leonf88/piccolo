cmake_minimum_required (VERSION 2.6) 
project (PICCOLO) 

set(CMAKE_BINARY_DIR "bin/")
set(CMAKE_MODULE_PATH "${PICCOLO_SOURCE_DIR}/../extlib/")
set(CMAKE_CXX_FLAGS "-ggdb2")

find_package(ProtoBuf REQUIRED)

include_directories( ${PROTOBUF_INCLUDE_DIRS} )
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
