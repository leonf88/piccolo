#ifndef MASTER_H_
#define MASTER_H_

#include "worker/registry.h"
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
  typedef void* KernelFunction;

  void run_all(const string& kernel, int method);

  // Run the given kernel function on one (arbitrary) worker node.
  void run_one(const string& kernel, int method);

  // Run the kernel function on the given set of shards.
  void run_range(const string& kernel, int method, vector<int> shards);

private:
  ConfigData config_;
  RPCHelper *rpc_;
  MPI::Intracomm world_;

  int worker_for_shard(int shard);
};

#define METHOD_ID(klass, method)\
    ((KernelHelper<klass>*)Registry::get_helper(#klass))->method_id(&klass::method)

#define RUN_ONE(m, klass, method)\
  m.run_one(#klass, METHOD_ID(klass, method))

#define RUN_ALL(m, klass, method)\
  m.run_all(#klass, METHOD_ID(klass, method))

#define RUN_RANGE(m, klass, method)\
  m.run_range(#klass, METHOD_ID(klass, method))

}

#endif /* MASTER_H_ */
