#ifndef MASTER_H_
#define MASTER_H_

#include "worker/worker.pb.h"
#include "util/common.h"
#include "util/rpc.h"
#include "kernel/kernel.h"
#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/local-table.h"
#include "kernel/table-registry.h"

namespace dsm {

class WorkerState;
class TaskState;

struct RunDescriptor {
   string kernel;
   string method;

   GlobalTable *table;
   CheckpointType checkpoint_type;
   int checkpoint_interval;

   // Tables to checkpoint.  If empty, commit all tables.
   vector<int> checkpoint_tables;
   vector<int> shards;

   int epoch;

   // Parameters to be passed to the individual kernel functions.  These are
   // also saved when checkpointing.
   ArgMap params;

   RunDescriptor() {}
   RunDescriptor(const string& kernel,
                 const string& method,
                 GlobalTable *table,
                 CheckpointType c_type=CP_NONE, int c_interval=-1) {
     this->kernel = kernel;
     this->method = method;
     this->table = table;
     this->checkpoint_type = c_type;
     this->checkpoint_interval = c_interval;
   }
 };

class Master {
public:
  Master(const ConfigData &conf);
  ~Master();

  void run_all(RunDescriptor r);
  void run_one(RunDescriptor r);
  void run_range(RunDescriptor r, vector<int> shards);

  // N.B.  All run_* methods are blocking.
  void run_all(const string& kernel, const string& method, GlobalTable* locality) {
    run_all(RunDescriptor(kernel, method, locality));
  }

  // Run the given kernel function on one (arbitrary) worker node.
  void run_one(const string& kernel, const string& method, GlobalTable* locality) {
    run_one(RunDescriptor(kernel, method, locality));
  }

  // Run the kernel function on the given set of shards.
  void run_range(const string& kernel, const string& method,
                 GlobalTable* locality, vector<int> shards) {
    run_range(RunDescriptor(kernel, method, locality), shards);
  }

  void run(RunDescriptor r);

  // Blocking.  Instruct workers to save all table state.  When this call returns,
  // all active tables in the system will have been committed to disk.
  void checkpoint();

  // Attempt restore from a previous checkpoint for this job.  If none exists,
  // the process is left in the original state, and this function returns NULL.
  bool restore(ArgMap *args);

private:
  void start_checkpoint();
  void start_worker_checkpoint(int worker_id, const RunDescriptor& r);
  void finish_worker_checkpoint(int worker_id, const RunDescriptor& r);
  void flush_checkpoint(const ArgMap& params);

  ConfigData config_;
  int checkpoint_epoch_;
  int kernel_epoch_;

  RunDescriptor current_run_;

  Timer cp_timer_;
  bool checkpointing_;

  // Used for interval checkpointing.
  double last_checkpoint_;

  WorkerState* worker_for_shard(int table, int shard);

  // Find a worker to run a kernel on the given table and shard.  If a worker
  // already serves the given shard, return it.  Otherwise, find an eligible
  // worker and assign it to them.
  WorkerState* assign_worker(int table, int shard);

  void send_table_assignments();
  bool steal_work(const RunDescriptor& r, int idle_worker, double avg_time);
  void assign_tables();
  void assign_tasks(const RunDescriptor& r, vector<int> shards);
  void dispatch_work(const RunDescriptor& r);
  vector<WorkerState*> workers_;

  typedef map<string, MethodStats> MethodStatsMap;
  MethodStatsMap method_stats_;

  TableRegistry::Map& tables_;

  NetworkThread* network_;
};
}

#endif /* MASTER_H_ */
