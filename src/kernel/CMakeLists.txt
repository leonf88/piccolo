add_library(kernel kernel-registry.cc table-registry.cc table.cc)
add_dependencies(kernel worker_proto)
