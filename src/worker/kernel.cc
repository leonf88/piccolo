#include "worker/kernel.h"

namespace upc {
KernelRegistry::StaticHelper::StaticHelper(KernelFunction kf) {
  map<int, KernelFunction> &kernels = KernelRegistry::get()->kernels;
  kernels[kernels.size()] = kf;
}

static KernelRegistry *theRegistry = NULL;
KernelRegistry* KernelRegistry::get() {
  if (theRegistry == NULL) {
    theRegistry = new KernelRegistry;
  }
  return theRegistry;
}
}
