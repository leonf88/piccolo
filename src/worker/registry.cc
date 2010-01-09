#include <stdio.h>

#include "worker/registry.h"
#include "worker/table.h"

namespace upc {

static map<int, KernelFunction> *kernels = NULL;
static map<int, TableRegistry::TableCreator*> *tables = new map<int, TableRegistry::TableCreator*>;

map<int, KernelFunction>* KernelRegistry::get_mapping() {
  if (kernels == NULL) {
    kernels = new map<int, KernelFunction>;
  }
  return kernels;
}

KernelFunction KernelRegistry::get_kernel(int id) {
  return (*kernels)[id];
}

int KernelRegistry::get_id(KernelFunction kf) {
  map<int, KernelFunction> &k = *KernelRegistry::get_mapping();
  for (map<int, KernelFunction>::iterator i = k.begin(); i != k.end(); ++i) {
    if (i->second == kf) { return i->first; }
  }
  LOG(FATAL) << "Failed to find kernel " << kf << " in mapping.";
}

KernelRegistry::StaticHelper::StaticHelper(const char* name, KernelFunction kf) {
//  fprintf(stderr, "Registering... %s\n", name);
  map<int, KernelFunction> &k = *KernelRegistry::get_mapping();
  k[k.size()] = kf;
}

void TableRegistry::register_table_creator(int id, TableRegistry::TableCreator* t) {
  (*tables)[id] = t;
}

static Table* get_table(int id, int shard) {
  CHECK(tables->find(id) != tables->end());
  Table *t = (*tables)[id]->create_table();
  t->info_.shard = shard;
  return t;
}

}
