# master
add_library(master master.cc ${WORKER_PB_SRC} ${WORKER_PB_HDR})
add_dependencies(master worker_proto)

# worker
protobuf_generate_cpp( WORKER_PB_SRC WORKER_PB_HDR worker.proto )
add_custom_target(worker_proto DEPENDS ${WORKER_PB_HDR})

add_library(worker worker.cc ${WORKER_PB_SRC} ${WORKER_PB_HDR})
add_dependencies(worker common_proto)
protobuf_generate_cpp(COMMON_PB_SRC COMMON_PB_HDR common.proto)

# common
add_custom_target(common_proto DEPENDS ${COMMON_PB_HDR})
add_library(common
            common.cc 
            file.cc 
            stringpiece.cc
            rpc.cc 
            static-initializers.cc 
            ${COMMON_PB_SRC} 
            ${COMMON_PB_HDR})

# kernel          
add_library(kernel 
            kernel.cc 
            table-registry.cc 
            disk-table.cc
            local-table.cc
            global-table.cc)
add_dependencies(kernel worker_proto)
