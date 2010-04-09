add_library(kernel kernel-registry.cc table-registry.cc table.cc ${WORKER_PB_HDR})
add_dependencies(kernel worker)
