#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"

namespace upc {

typedef void (*KernelFunction)(void);

struct KernelRegistry {
  static KernelRegistry* get();

  struct StaticHelper {
    StaticHelper(KernelFunction kf);
  };

  KernelFunction GetKernel(int id) {
    return kernels[id];
  }

  map<int, KernelFunction> kernels;
};

#define REGISTER_KERNEL(kf)\
  namespace {\
    struct MyStaticHelper : public KernelRegistry::StaticHelper {\
      MyStaticHelper() : StaticHelper(kf) {}\
      static MyStaticHelper registryHelper;\
    };\
  }

} // end namespace
#endif /* KERNEL_H_ */
