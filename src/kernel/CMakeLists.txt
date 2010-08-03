add_library(kernel 
            kernel.cc 
            table-registry.cc 
            disk-table.cc
            local-table.cc
            global-table.cc
            test-tables.cc)
add_dependencies(kernel worker_proto)
