#ifndef MASTER_H_
#define MASTER_H_

#include "worker/worker.pb.h"
#include "util/common.h"
#include "util/rpc.h"

namespace dsm {

class WorkerState;

class ParamMap {
public:
  string& operator[](const string& key) {
    return p_[key];
  }

  void set_int(const string& key, int v) {
    p_[key] = StringPrintf("%d", v);
  }

  int get_int(const string& key) {
    return (int)strtol(p_[key].c_str(), NULL, 10);
  }

  double get_double(const string& key) {
    return strtod(p_[key].c_str(), NULL);
  }

  Params* to_params() {
    Params* out = new Params;
    for (unordered_map<string, string>::iterator i = p_.begin(); i != p_.end(); ++i) {
      Param *p = out->add_param();
      p->set_key(i->first);
      p->set_value(i->second);
    }
    return out;
  }

  static ParamMap* from_params(const Params& p) {
    ParamMap *pm = new ParamMap;
    for (int i = 0; i < p.param_size(); ++i) {
      (*pm)[p.param(i).key()] = p.param(i).value();
    }
    return pm;
  }

private:
  unordered_map<string, string> p_;
};

class Master {
public:
  Master(const ConfigData &conf);
  ~Master();

  struct RunDescriptor {
    string kernel;
    string method;

    int table;
    CheckpointType checkpoint_type;
    int checkpoint_interval;

    // Tables to checkpoint.  If empty, commit all tables.
    vector<int> checkpoint_tables;

    int epoch;

    // Parameters to save when checkpointing.
    Params* params;

    static RunDescriptor Create(const string& kernel,
                                const string& method,
                                int table,
                                CheckpointType c_type=CP_NONE, int c_interval=-1) {
      RunDescriptor r = { kernel, method, table, c_type, c_interval };
      r.params = NULL;
      return r;
    }
  };

  // N.B.  All run_* methods are blocking.
  void run_all(RunDescriptor r);

  // Run the given kernel function on one (arbitrary) worker node.
  void run_one(RunDescriptor r);

  // Run the kernel function on the given set of shards.
  void run_range(RunDescriptor r, vector<int> shards);

  // Blocking.  Instruct workers to save all table state.  When this call returns,
  // all active tables in the system will have been committed to disk.
  void checkpoint(RunDescriptor r);

  // Attempt restore from a previous checkpoint for this job.  If none exists,
  // the process is left in the original state, and this function returns NULL.
  ParamMap* restore();

private:
  void start_checkpoint();
  void start_worker_checkpoint(int worker_id, const RunDescriptor& r);
  void finish_worker_checkpoint(int worker_id, const RunDescriptor& r);
  void flush_checkpoint(Params* params);

  ConfigData config_;
  RPCHelper *rpc_;
  MPI::Intracomm world_;
  int checkpoint_epoch_;
  int kernel_epoch_;

  Timer cp_timer_;
  bool checkpointing_;

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
  m.run_one(Master::RunDescriptor::Create(#klass, #method, table))

#define RUN_ALL(m, klass, method, table)\
  m.run_all(Master::RunDescriptor::Create(#klass, #method, table))

#define RUN_RANGE(m, klass, method, table, shards)\
  m.run_range(Master::RunDescriptor::Create(#klass, #method, table), shards)

}

#endif /* MASTER_H_ */
