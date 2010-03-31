protobuf_generate_cpp( COMMON_PB_SRC COMMON_PB_HDR common.proto)
add_library(common common.cc file.cc rpc.cc ${COMMON_PB_SRC} ${COMMON_PB_HDR})
