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
  void run(KernelFunction f);

private:
  ConfigData config_;
  RPCHelper *rpc_;
  MPI::Intracomm world_;
};

}

#endif /* MASTER_H_ */
