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
static KernelMap *kernels = NULL;
static RunnerMap *runners = NULL;

KernelMap& get_kernels() {
  if (kernels == NULL) {
    kernels = new KernelMap;
  }
  return *kernels;
}

KernelInfo* get_kernel(const string& name) {
  return get_kernels()[name];
}

RunnerMap& get_runners() {
  if (runners == NULL) {
    runners = new RunnerMap;
  }
  return *runners;
}

KernelRunner get_runner(const string& name) {
  return get_runners()[name];
}

}

}
