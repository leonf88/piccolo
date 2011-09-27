cmake_minimum_required (VERSION 2.6) 
cmake_policy(SET CMP0014 OLD)

project (PICCOLO) 

set(ENV{LANG} "C")

include(../cmake/PiccoloPP.cmake)
include(../cmake/FindProtobuf.cmake)
include(../cmake/PcFromUcontext.cmake)

include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckLibraryExists)
include(CheckFunctionExists)

find_package(PythonLibs)
find_package(SWIG)
find_package(SDL)
find_package(Boost REQUIRED)

check_include_file_cxx("google/protobuf/descriptor.h" HAVE_PROTOBUF)

check_include_file("lzo/lzo1x.h" HAVE_LZO1X_H)
check_include_file("libunwind.h"  HAVE_LIBUNWIND_H)

find_program(HAVE_PROTOC "protoc")
find_program(HAVE_MPICC "mpic++")

find_library(HAVE_LIBUNWIND unwind)
find_library(HAVE_BLAS blas)
find_library(HAVE_PROTOBUF protobuf)

if (NOT HAVE_LZO1X_H)
  message(FATAL_ERROR "Required lzo header file not found.")
endif()

if (NOT HAVE_PROTOBUF OR NOT HAVE_PROTOC)
  message(FATAL_ERROR "Required protocol buffer headers and compiler not found.")
else()
  set(PROTOBUF_LIBS protobuf)
endif()
 
if (NOT HAVE_MPICC) 
  message(FATAL_ERROR "Required OpenMPI development libraries and compiler not found")
endif()

if (NOT HAVE_BLAS) 
  message(FATAL_ERROR "Required library blas not found")
else()
  set (BLAS_LIBS blas)
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


if ("$ENV{OPROFILE}")
  message(STATUS "Enabling OProfile")
endif()

set(CMAKE_INCLUDE_CURRENT_DIR ON)

exec_program("mpic++ -showme:compile" OUTPUT_VARIABLE MPI_COMPILE_FLAGS RETURN_VALUE MPI_COMPILE_RV)
exec_program("mpic++ -showme:incdirs" OUTPUT_VARIABLE MPI_INCDIRS RETURN_VALUE MPI_INCDIR_RV)
exec_program("mpic++ -showme:link" OUTPUT_VARIABLE MPI_LINK_FLAGS RETURN_VALUE MPI_LINK_RV)
exec_program("mpic++ -showme:libdirs" OUTPUT_VARIABLE MPI_LIBDIRS RETURN_VALUE MPI_LIBDIR_RV)
exec_program("mpic++ -showme:libs" OUTPUT_VARIABLE MPI_LIBS RETURN_VALUE MPI_LIB_RC)

if (MPI_COMPILE_RV)
message(FATAL_ERROR "Failed to execute mpic++ to gather environment information.")
endif()

if (MPI_INCDIR_RV)
message(FATAL_ERROR "Failed to execute mpic++ to gather environment information.")
endif()

if (MPI_LINK_RV)
message(FATAL_ERROR "Failed to execute mpic++ to gather environment information.")
endif()

if (MPI_LIBDIR_RV)
message(FATAL_ERROR "Failed to execute mpic++ to gather environment information.")
endif()

if (MPI_LIB_RV)
message(FATAL_ERROR "Failed to execute mpic++ to gather environment information.")
endif()

string(REPLACE " " ";" MPI_INCDIRS "${MPI_INCDIRS}")
string(REPLACE " " ";" MPI_LINK_FLAGS "${MPI_LINK_FLAGS}")
string(REPLACE " " ";" MPI_LIBDIRS "${MPI_LIBDIRS}")
string(REPLACE " " ";" MPI_LIBS "${MPI_LIBS}")

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
	             external/webgraph )
		     

link_directories(${MPI_LIBDIRS})
include_directories(${MPI_INCDIRS})

set(CMAKE_CXX_FLAGS "${CXX_DEFAULT} -fPIC -fno-omit-frame-pointer -ggdb2 ${CXX_WARN} ${CXX_DEFINE}")
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
