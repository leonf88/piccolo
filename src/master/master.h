#ifndef MASTER_H_
#define MASTER_H_

#include "worker/registry.h"
#include "worker/worker.h"
#include "worker/worker.pb.h"
#include "util/common.h"
#include "util/rpc.h"

namespace dsm {

class Master {
public:
  Master(const ConfigData &conf);
  ~Master();

  // N.B.  All run_* methods are blocking.

  struct RunDescriptor {
    RunDescriptor(const string& k, const string& m, int t) :
      kernel(k), method(m), table(t) {}

    string kernel;
    string method;

    int table;
  };

  void run_all(const RunDescriptor& r);

  // Run the given kernel function on one (arbitrary) worker node.
  void run_one(const RunDescriptor& r);

  // Run the kernel function on the given set of shards.
  void run_range(const RunDescriptor& r, vector<int> shards);

private:
  ConfigData config_;
  RPCHelper *rpc_;
  MPI::Intracomm world_;

  enum WorkerStatus {
    IDLE =       0,
    WORKING =    1,
    MIGRATING =  2
  };

  struct WorkerState {
    vector<int> assigned;
    vector<int> pending;
    vector<int> finished;

    double last_ping_time;
    int status;

    bool is_assigned(int shard) {
      return find(assigned.begin(), assigned.end(), shard) != assigned.end();
    }
  };

  vector<WorkerState> workers_;

  int worker_for_shard(int shard);
  int assign_worker(int shard);
};

#define RUN_ONE(m, klass, method, table)\
  m.run_one(Master::RunDescriptor(#klass, #method, table))

#define RUN_ALL(m, klass, method, table)\
  m.run_all(Master::RunDescriptor(#klass, #method, table))

#define RUN_RANGE(m, klass, method, table, shards)\
  m.run_range(Master::RunDescriptor(#klass, #method, table), shards)

}

#endif /* MASTER_H_ */
