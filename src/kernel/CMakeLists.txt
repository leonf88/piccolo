add_library(kernel 
            kernel.cc 
            table-registry.cc 
            disk-table.cc
            table.cc)
add_dependencies(kernel worker_proto)
