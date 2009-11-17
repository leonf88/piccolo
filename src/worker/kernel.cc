#include "worker/kernel.h"

namespace upc {

static map<int, KernelFunction> *kernels = NULL;

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
  fprintf(stderr, "Registering... %s\n", name);
  map<int, KernelFunction> &k = *KernelRegistry::get_mapping();
  k[k.size()] = kf;
}

}
