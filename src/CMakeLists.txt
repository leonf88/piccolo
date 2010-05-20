cmake_minimum_required (VERSION 2.6) 
project (PICCOLO) 

set(ENV{LANG} "C")
cmake_policy(SET CMP0014 OLD)

set(CMAKE_MODULE_PATH "${PICCOLO_SOURCE_DIR}/../extlib/")
find_package(ProtoBuf REQUIRED)
find_package(SWIG REQUIRED)
find_package(PythonLibs REQUIRED)

include(${SWIG_USE_FILE})

set(CMAKE_CXX_FLAGS "-fPIC -march=core2 -ggdb2 -Wall -Wno-sign-compare -Wno-unused-function")
if ($ENV{TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHEAPPROF")
endif()

if ($ENV{OPROFILE})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCPUPROF")
endif()

set(CMAKE_SWIG_FLAGS -ignoremissing -O -c++)

include_directories( ${PYTHON_INCLUDE_PATH} )
include_directories( ${CMAKE_CURRENT_SOURCE_DIR}
                     ${PICCOLO_BINARY_DIR}
                     ${PROTOBUF_INCLUDE_DIRS}
                     /home/power/share/include ) 
                     
SET(CMAKE_INCLUDE_CURRENT_DIR ON)

link_directories( /home/power/share/lib )

add_subdirectory (external/google-flags)
add_subdirectory (external/google-logging)
add_subdirectory (external/google-perftools)
add_subdirectory (external/libunwind)

include_directories( ${PICCOLO_SOURCE_DIR}/external/google-flags )
include_directories( ${PICCOLO_SOURCE_DIR}/external/google-logging )
include_directories( ${PICCOLO_SOURCE_DIR}/external/google-perftools )

add_subdirectory (util)
add_subdirectory (worker)
add_subdirectory (kernel)
add_subdirectory (master)
add_subdirectory (test)
add_subdirectory (examples)
add_subdirectory (examples/crawler)
add_subdirectory (examples/nas)
