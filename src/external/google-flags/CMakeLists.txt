set(CMAKE_CXX_FLAGS "-fPIC -march=core2 -Wall -Wno-sign-compare -Wno-unused-function -fno-omit-frame-pointer -ggdb1")

add_library(gflags
            gflags.cc
            gflags_reporting.cc
            gflags_nc.cc)

