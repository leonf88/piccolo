#ifndef MASTER_H_
#define MASTER_H_

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

  enum TaskStatus {
    ASSIGNED  = 0,
    WORKING   = 1,
    FINISHED  = 2
  };

  struct Task {
    Task() { table = shard = 0; }
    Task(int t, int sh) : table(t), shard(sh), status(ASSIGNED) {}
    int table;
    int shard;
    int status;
  };

  struct ShardInfo {};

  typedef pair<int, int> Taskid;
  typedef map<Taskid, Task*> TaskMap;
  typedef map<Taskid, ShardInfo> ShardMap;

  struct WorkerState {
    WorkerState(int id);

    // Pending tasks to work on.
    TaskMap assigned;
    TaskMap pending;
    TaskMap active;

    // Table shards this worker is responsible for serving.
    ShardMap shards;

    double last_ping_time;

    int status;
    int id;

    int slots;

    bool is_assigned(int table, int shard) {
      return assigned.find(MP(table, shard)) != assigned.end();
    }

    bool finished() {
      return assigned.size() - pending.size() - active.size();
    }

    bool idle() { return pending.empty(); }

    void set_serves(int shard, bool should_service);
    bool serves(int table, int shard);

    bool get_next(const RunDescriptor& r, KernelRequest* msg);
  };

  vector<WorkerState> workers_;

  WorkerState* worker_for_shard(int table, int shard);

  // Find a worker to run a kernel on the given table and shard.  If a worker
  // already serves the given shard, return it.  Otherwise, find an eligible
  // worker and assign it to them.
  WorkerState* assign_worker(int table, int shard);

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
