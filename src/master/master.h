#ifndef MASTER_H_
#define MASTER_H_

#include "kernel/kernel-registry.h"
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

  typedef deque<int> TaskList;

  struct WorkerState {
    WorkerState(int id);

    vector<TaskList> assigned;
    vector<TaskList> pending;
    vector<TaskList> finished;

    double last_ping_time;
    int status;
    int id;

    bool is_assigned(int table, int shard) { return IN(assigned[table], shard);  }
    bool idle(int table) { return pending[table].empty(); }

    void assign(int task);
    void deassign(int task);

    bool get_next(const RunDescriptor& r, KernelRequest* msg);
  };

  vector<WorkerState> workers_;

  int worker_for_shard(int table, int shard);
  int assign_worker(int table, int shard);
  void send_assignments();

  void steal_work(const RunDescriptor& r, int idle_worker);
};

#define RUN_ONE(m, klass, method, table)\
  m.run_one(Master::RunDescriptor(#klass, #method, table))

#define RUN_ALL(m, klass, method, table)\
  m.run_all(Master::RunDescriptor(#klass, #method, table))

#define RUN_RANGE(m, klass, method, table, shards)\
  m.run_range(Master::RunDescriptor(#klass, #method, table), shards)

}

#endif /* MASTER_H_ */
