#ifndef MASTER_H_
#define MASTER_H_

#include "worker/kernel.h"
#include "worker/worker.h"
#include "worker/worker.pb.h"
#include "util/common.h"
#include "util/rpc.h"

namespace upc {

class Master {
public:
  Master(const ConfigData &conf);
  ~Master();

  // N.B.  All run_* methods are blocking.

  // Run the given kernel function on all worker nodes.
  void run_all(KernelFunction f);

  // Run the given kernel function on one (arbitrary) worker node.
  void run_one(KernelFunction f);

  // Run the kernel function on the given set of nodes.
  void run_range(KernelFunction f, vector<int> nodes);

private:
  ConfigData config_;
  RPCHelper *rpc_;
  MPI::Intracomm world_;
};

}

#endif /* MASTER_H_ */
