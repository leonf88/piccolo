#ifndef MASTER_H_
#define MASTER_H_

#include "worker/worker.pb.h"
#include "util/common.h"
#include "util/rpc.h"

namespace dsm {


class WorkerState;

class Master {
public:
  Master(const ConfigData &conf);
  ~Master();

  struct RunDescriptor {
    string kernel;
    string method;

    int table;
    int checkpoint_interval;
    int epoch;

    static RunDescriptor C(const string& k, const string& m, int t, int c_interval=-1) {
      RunDescriptor r = { k, m, t, c_interval };
      return r;
    }
  };

  // N.B.  All run_* methods are blocking.
  void run_all(const RunDescriptor& r);

  // Run the given kernel function on one (arbitrary) worker node.
  void run_one(const RunDescriptor& r);

  // Run the kernel function on the given set of shards.
  void run_range(const RunDescriptor& r, vector<int> shards);

  // Blocking.  Instruct workers to save all table state.  When this call returns,
  // all active tables in the system will have been committed to disk.
  void checkpoint(bool compute_deltas=false);

  // Attempt restore from a previous checkpoint for this job.  If none exists,
  // the process is left in the original state.
  void restore();

private:
  ConfigData config_;
  RPCHelper *rpc_;
  MPI::Intracomm world_;
  int restored_checkpoint_epoch_;
  int restored_kernel_epoch_;
  int checkpoint_epoch_;
  int kernel_epoch_;

  // Used for interval checkpointing.
  double last_checkpoint_;

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

  typedef pair<int, int> Taskid;
  typedef map<Taskid, Task*> TaskMap;
  typedef map<Taskid, bool> ShardMap;
  typedef map<int, map<int, ShardInfo> > TableInfo;

  friend class WorkerState;

  WorkerState* worker_for_shard(int table, int shard);

  // Find a worker to run a kernel on the given table and shard.  If a worker
  // already serves the given shard, return it.  Otherwise, find an eligible
  // worker and assign it to them.
  WorkerState* assign_worker(int table, int shard);

  void send_table_assignments();
  void steal_work(const RunDescriptor& r, int idle_worker);
  void assign_tables();
  void assign_tasks(const RunDescriptor& r, vector<int> shards);
  void dispatch_work(const RunDescriptor& r);
  vector<WorkerState*> workers_;

  // Global table information, as reported by workers.
  TableInfo tables_;

  typedef map<string, MethodStats> MethodStatsMap;
  MethodStatsMap method_stats_;

  // The list of tasks that have been stolen already, to avoid
  // moving things too often.
  set<Taskid> stolen_;
};

#define RUN_ONE(m, klass, method, table)\
  m.run_one(Master::RunDescriptor::C(#klass, #method, table))

#define RUN_ALL(m, klass, method, table)\
  m.run_all(Master::RunDescriptor::C(#klass, #method, table))

#define RUN_RANGE(m, klass, method, table, shards)\
  m.run_range(Master::RunDescriptor::C(#klass, #method, table), shards)

}

#endif /* MASTER_H_ */
