set(CMAKE_CXX_FLAGS "-fPIC -march=core2 -Wall -Wno-sign-compare -Wno-unused-function -fno-omit-frame-pointer -ggdb1")

include_directories( ${PICCOLO_SOURCE_DIR}/external/google-flags/)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_LIB_GFLAGS")

add_library(glog
            utilities.cc
            vlog_is_on.cc
            demangle.cc
            logging.cc
            symbolize.cc
            signalhandler.cc
            raw_logging.cc)
