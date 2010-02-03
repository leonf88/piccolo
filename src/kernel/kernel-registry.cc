#include <stdio.h>

#include "kernel/kernel-registry.h"
#include "kernel/table-registry.h"

namespace dsm {

class Worker;
void DSMKernel::Init(Worker* w, int table_id, int shard) {
  w_ = w;
  table_id_ = table_id;
  shard_ = shard;
}

Table* DSMKernel::get_table(int id) {
  return Registry::get_table(id);
}

namespace Registry {
static map<string, KernelInfo*> *kernels = NULL;

map<string, KernelInfo*>& get_kernels() {
  if (kernels == NULL) {
    kernels = new map<string, KernelInfo*>;
  }
  return *kernels;
}

KernelInfo* get_kernel(const string& name) {
  return get_kernels()[name];
}
}

}
