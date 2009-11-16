#ifndef KERNEL_H_
#define KERNEL_H_

#include "util/common.h"

namespace upc {

typedef void (*KernelFunction)(void);

struct KernelRegistry {
  struct StaticHelper {
    StaticHelper(const char* name, KernelFunction kf);
  };

  static KernelFunction get_kernel(int id);
  static int get_id(KernelFunction kf);

  static map<int, KernelFunction>* get_mapping();
};



#define REGISTER_KERNEL(kf)\
  namespace {\
    struct StaticHelper_ ## kf : public KernelRegistry::StaticHelper {\
      StaticHelper_ ## kf () : StaticHelper(#kf, kf) {}\
    };\
    static StaticHelper_ ## kf reg_helper_ ## kf;\
  }

} // end namespace
#endif /* KERNEL_H_ */
