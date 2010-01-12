#include <stdio.h>

#include "worker/registry.h"
#include "worker/worker.h"
#include "worker/table.h"

namespace upc {

void DSMKernel::Init(Worker* w, int table_id, int shard) {
  w_ = w;
  table_id_ = table_id;
  shard_ = shard;
}

Table* DSMKernel::get_table(int id) {
  return w_->get_table(id);
}

namespace Registry {

static map<string, KernelHelperBase*> *kernels = NULL;

map<string, KernelHelperBase*>* get_mapping() {
  if (kernels == NULL) {
    kernels = new map<string, KernelHelperBase*>;
  }
  return kernels;
}

DSMKernel* create_kernel(const string& name) {
  return get_helper(name)->create();
}

KernelHelperBase* get_helper(const string& name) {
  return (*get_mapping())[name];
}

}

}
