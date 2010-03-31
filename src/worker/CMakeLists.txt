protobuf_generate_cpp( WORKER_PB_SRC WORKER_PB_HDR worker.proto )
add_library(worker worker.cc ${WORKER_PB_SRC} ${WORKER_PB_HDR})
