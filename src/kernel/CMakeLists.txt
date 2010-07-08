add_library(kernel 
            kernel.cc 
            table-registry.cc 
            disk-table.cc
            map-test.cc
            global-table.cc
            local-table.cc)
add_dependencies(kernel worker_proto)
