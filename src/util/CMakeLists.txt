protobuf_generate_cpp(COMMON_PB_SRC COMMON_PB_HDR common.proto)
add_custom_target(common_proto DEPENDS ${COMMON_PB_HDR})
add_library(common
            common.cc 
            file.cc 
            logging.cc
            string.cc
            rpc.cc 
            static-initializers.cc 
            hashmap.cc 
            ${COMMON_PB_SRC} 
            ${COMMON_PB_HDR})
