protobuf_generate_cpp( WORKER_PB_SRC WORKER_PB_HDR ${PICCOLO_SOURCE_DIR} worker.proto )
add_custom_target(worker_proto DEPENDS ${WORKER_PB_HDR})

add_library(worker worker.cc ${WORKER_PB_SRC} ${WORKER_PB_HDR})
add_dependencies(worker common_proto)
