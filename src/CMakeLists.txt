cmake_minimum_required (VERSION 2.6) 
cmake_policy(SET CMP0014 OLD)

project (PICCOLO) 

set(ENV{LANG} "C")

include(../cmake/PiccoloPP.cmake)
include(../cmake/FindProtobuf.cmake)
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckLibraryExists)
include(CheckFunctionExists)

find_package(PythonLibs)
find_package(SWIG)
find_package(Boost REQUIRED)
find_package(SDL)

#check_include_file_cxx("google/profiler.h" HAVE_GOOGLE_PROF)
#check_include_file_cxx("google/tcmalloc.h" HAVE_GOOGLE_TCMALLOC)
check_include_file_cxx("google/protobuf/descriptor.h" HAVE_PROTOBUF)

check_include_file("lzo/lzo1x.h" HAVE_LZO1X_H)
check_include_file("libunwind.h"  HAVE_LIBUNWIND_H)

find_program(HAVE_PROTOC "protoc")
find_program(HAVE_MPICC "mpic++")
find_library(HAVE_LIBUNWIND unwind)

if (NOT HAVE_LZO1X_H)
  message(FATAL_ERROR "Required lzo header file not found.")
endif()

if (NOT HAVE_PROTOBUF OR NOT HAVE_PROTOC)
  message(FATAL_ERROR "Required protocol buffer headers and compiler not found.")
endif()
 
if (NOT HAVE_MPICC) 
  message(FATAL_ERROR "Required OpenMPI development libraries and compiler not found")
endif()

set(CXX_DEFAULT "${CMAKE_CXX_FLAGS}")
set(CXX_DEFINE "-DHAVE_LIB_GFLAGS ")
set(CXX_WARN "-Wall -Wno-sign-compare -Wno-unused-function -Wno-unused-result")
 
if (HAVE_LIBUNWIND AND HAVE_LIBUNWIND_H)
  set(CXX_DEFINE "${CXX_DEFINE} -DHAVE_LIBUNWIND_H")
endif()

if (HAVE_LIB_UNWIND)
  set(CXX_DEFINE "${CXX_DEFINE} -DHAVE_LIBUNWIND")
endif()

if ("$ENV{TCMALLOC}" AND HAVE_GOOGLE_TCMALLOC)
  message(STATUS "Enabling TCMalloc")
  set(CXX_DEFINE "${CXX_DEFINE} -DHEAPPROF ")
endif()

if ("$ENV{OPROFILE}")
  message(STATUS "Enabling OProfile")
else()
  if (HAVE_GOOGLE_PROF)
    message(STATUS "Enabling internal CPU profiler")
    set(CXX_DEFINE "${CXX_DEFINE} -DCPUPROF ")
  endif()
endif()

set(CMAKE_INCLUDE_CURRENT_DIR ON)

exec_program("mpic++ -showme:compile" OUTPUT_VARIABLE MPI_COMPILE_FLAGS)
exec_program("mpic++ -showme:incdirs" OUTPUT_VARIABLE MPI_INCDIRS)
exec_program("mpic++ -showme:link" OUTPUT_VARIABLE MPI_LINK_FLAGS)
exec_program("mpic++ -showme:libdirs" OUTPUT_VARIABLE MPI_LIBDIRS)

string(REPLACE " " ";" MPI_INCDIRS "${MPI_INCDIRS}")
string(REPLACE " " ";" MPI_LINK_FLAGS "${MPI_LINK_FLAGS}")

set(CMAKE_CXX_FLAGS "${CXX_DEFAULT} -fPIC -fno-omit-frame-pointer -g0 ${CXX_WARN} ${CXX_DEFINE}")

add_subdirectory (external/google-flags)
add_subdirectory (external/google-logging)
add_subdirectory (external/webgraph)

include_directories( ${CMAKE_CURRENT_SOURCE_DIR}
                     ${PICCOLO_BINARY_DIR}
                     ${PROTOBUF_INCLUDE_DIRS}
                     ${PYTHON_INCLUDE_PATH}
                     ${MPI_INCDIRS}
                     external/google-flags
		     external/google-logging
		     external/webgraph)

                     

link_directories( ${MPI_LIBDIRS} )
include_directories(${MPI_INCDIRS})

set(CMAKE_CXX_FLAGS "${CXX_DEFAULT} -fPIC -fno-omit-frame-pointer -ggdb1 ${CXX_WARN} ${CXX_DEFINE}")
add_subdirectory (util)
add_subdirectory (worker)
add_subdirectory (kernel)
add_subdirectory (master)
add_subdirectory (examples)
add_subdirectory (examples/crawler)
add_subdirectory (examples/nas)

if (NOT SWIG_FOUND)
  message(STATUS "Swig not found; skipping python build.")
endif()

if (NOT PYTHONLIBS_FOUND)
  message(STATUS "Python dev not found; skipping python build.")
endif()

if (SWIG_FOUND AND PYTHONLIBS_FOUND)
  set(CMAKE_SWIG_FLAGS -ignoremissing -O -c++)
  include(${SWIG_USE_FILE})
  add_subdirectory (client/python)
endif()
