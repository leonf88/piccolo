#include <stdio.h>

#include "worker/registry.h"
#include "worker/worker.h"
#include "worker/table.h"

namespace dsm {

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
static map<int, GlobalTable*> *tables = NULL;

map<string, KernelInfo*>* get_kernels() {
  if (kernels == NULL) {
    kernels = new map<string, KernelInfo*>;
  }
  return kernels;
}

map<int, GlobalTable*>* get_tables() {
  if (tables == NULL) {
    tables = new map<int, GlobalTable*>;
  }
  return tables;
}

KernelInfo* get_kernel_info(const string& name) {
  return (*get_kernels())[name];
}

GlobalTable* get_table(int id) {
  return (*get_tables())[id];
}


}

}
