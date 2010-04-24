#include "master/master.h"
#include "kernel/table-registry.h"
#include "kernel/kernel.h"

DEFINE_bool(work_stealing, true, "");
DEFINE_string(dead_workers, "",
              "Comma delimited list of workers to pretend have died.");

DECLARE_string(checkpoint_write_dir);
DECLARE_string(checkpoint_read_dir);

namespace dsm {

static unordered_set<int> dead_workers;

struct Taskid {
  int table;
  int shard;

  Taskid(int t, int s) : table(t), shard(s) {}

  bool operator<(const Taskid& b) const {
    return table < b.table || (table == b.table && shard < b.shard);
  }
};

struct TaskState : private boost::noncopyable {
  enum Status {
    PENDING  = 0,
    WORKING   = 1,
    FINISHED  = 2
  };

  TaskState(Taskid id, int64_t size) : id(id), status(PENDING), size(size) {}

  static bool IdCompare(TaskState *a, TaskState *b) {
    return a->id < b->id;
  }

  static bool WeightCompare(TaskState *a, TaskState *b) {
    return a->size < b->size;
  }

  Taskid id;
  int status;
  int size;
};

typedef map<Taskid, TaskState*> TaskMap;
typedef set<Taskid> ShardSet;
struct WorkerState : private boost::noncopyable {
  WorkerState(int w_id) : id(w_id), slots(0) {
    last_ping_time = Now();
    last_task_start = 0;
    total_runtime = 0;
    checkpointing = false;
  }

  TaskMap work;

  // Table shards this worker is responsible for serving.
  ShardSet shards;

  double last_ping_time;

  int status;
  int id;

  int slots;

  double last_task_start;
  double total_runtime;

  bool checkpointing;

  // Order by number of pending tasks and last update time.
  static bool PendingCompare(WorkerState *a, WorkerState* b) {
    return (a->num_pending() < b->num_pending()) ||
           (a->num_pending() == b->num_pending() &&
            a->last_ping_time > b->last_ping_time);
  }

  bool alive() const {
    return dead_workers.find(id) == dead_workers.end();
  }

  bool is_assigned(Taskid id) {
    return work.find(id) != work.end();
  }

  void ping() {
    last_ping_time = Now();
  }

  bool idle(double avg_completion_time) {
    return num_finished() == work.size() &&
           Now() - last_ping_time > 5.0 + avg_completion_time * 3 / 4;
  }

  bool full() const { return work.size() >= slots; }

  void assign_shard(int shard, bool should_service) {
    Registry::TableMap &tables = Registry::get_tables();
    for (Registry::TableMap::iterator i = tables.begin(); i != tables.end(); ++i) {
      Taskid t(i->first, shard);
      if (should_service) {
        shards.insert(t);
      } else {
        shards.erase(shards.find(t));
      }
    }
  }

  bool serves(Taskid id) const {
    return shards.find(id) != shards.end();
  }

  void assign_task(TaskState *s) {
    work[s->id] = s;
  }

  void remove_task(TaskState* s) {
    work.erase(work.find(s->id));
  }

  void clear_tasks() {
    work.clear();
  }

  void set_finished(const Taskid& id) {
    CHECK(work.find(id) != work.end());
    TaskState *t = work[id];
    CHECK(t->status == TaskState::WORKING);
    t->status = TaskState::FINISHED;
  }

  vector<TaskState*> pending() const {
    vector<TaskState*> out;
    for (TaskMap::const_iterator i = work.begin(); i != work.end(); ++i) {
      if (i->second->status == TaskState::PENDING) {
        out.push_back(i->second);
      }
    }
    return out;
  }

#define COUNT_TASKS(type)\
  int c = 0;\
  for (TaskMap::const_iterator i = work.begin(); i != work.end(); ++i)\
    if (i->second->status == TaskState::type) { ++c; }\
  return c;

  int num_pending() const { COUNT_TASKS(PENDING); }
  int num_active() const { COUNT_TASKS(WORKING); }
  int num_finished() const { COUNT_TASKS(FINISHED); }
  int num_assigned() const { return work.size(); }

#undef COUNT_TASKS

  // Order pending tasks by our guess of how large they are
  bool get_next(const Master::RunDescriptor& r,
                KernelRequest* msg) {
    vector<TaskState*> p = pending();

    if (p.empty()) {
      return false;
    }

    TaskState* best = *max_element(p.begin(), p.end(), &TaskState::WeightCompare);

    msg->set_kernel(r.kernel);
    msg->set_method(r.method);
    msg->set_table(r.table);
    msg->set_shard(best->id.shard);

    best->status = TaskState::WORKING;
    last_task_start = Now();

    return true;
  }
};

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = MPI::COMM_WORLD;
  checkpoint_epoch_ = 0;
  kernel_epoch_ = 0;
  last_checkpoint_ = Now();
  checkpointing_ = false;

  CHECK_GT(world_.Get_size(), 1) << "At least one master and one worker required!";

  rpc_ = new RPCHelper(&world_);
  for (int i = 0; i < config_.num_workers(); ++i) {
    workers_.push_back(new WorkerState(i));
  }

  for (int i = 0; i < config_.num_workers(); ++i) {
    RegisterWorkerRequest req;
    int src = 0;
    rpc_->ReadAny(&src, MTYPE_REGISTER_WORKER, &req);
    workers_[src - 1]->slots = req.slots();
    LOG(INFO) << "Registered worker " << src - 1 << "; " << config_.num_workers() - 1 - i << " remaining.";
  }

  vector<StringPiece> bits = StringPiece::split(FLAGS_dead_workers, ",");
//  LOG(INFO) << "dead workers: " << FLAGS_dead_workers;
  for (int i = 0; i < bits.size(); ++i) {
    LOG(INFO) << MP(i, bits[i].AsString());
    dead_workers.insert(strtod(bits[i].AsString().c_str(), NULL));
  }
}

Master::~Master() {
  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    LOG(INFO) << StringPrintf("Worker %2d: %.3f", i, w.total_runtime);
  }

  for (MethodStatsMap::iterator i = method_stats_.begin(); i != method_stats_.end(); ++i) {
    LOG(INFO) << "Kernel stats: " << i->first << " :: " << i->second;
  }

  LOG(INFO) << "Shutting down workers.";
  EmptyMessage msg;
  for (int i = 1; i < world_.Get_size(); ++i) {
    rpc_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
  }
}

void Master::checkpoint(RunDescriptor r) {
  // Pause any other kind of activity until the workers all confirm the checkpoint is done; this is
  // to avoid changing the state of the system (via new shard or task assignments) until the checkpoint
  // is complete.

  start_checkpoint();

  for (int i = 0; i < workers_.size(); ++i) {
    start_worker_checkpoint(i, r);
  }

  for (int i = 0; i < workers_.size(); ++i) {
    finish_worker_checkpoint(i, r);
  }

  flush_checkpoint(r.params);
}

void Master::start_checkpoint() {
  if (checkpointing_) {
    return;
  }

  cp_timer_.Reset();
  checkpoint_epoch_ += 1;
  checkpointing_ = true;

  LOG(INFO) << "Starting new checkpoint: " << checkpoint_epoch_;
}

void Master::start_worker_checkpoint(int worker_id, const RunDescriptor &r) {
  start_checkpoint();

  LOG(INFO) << "Starting checkpoint on: " << worker_id;

  CHECK_EQ(workers_[worker_id]->checkpointing, false);

  workers_[worker_id]->checkpointing = true;

  CheckpointRequest req;
  req.set_epoch(checkpoint_epoch_);
  req.set_checkpoint_type(r.checkpoint_type);

  for (int i = 0; i < r.checkpoint_tables.size(); ++i) {
    req.add_table(r.checkpoint_tables[i]);
  }

  rpc_->Send(1 + worker_id, MTYPE_START_CHECKPOINT, req);
}

void Master::finish_worker_checkpoint(int worker_id, const RunDescriptor& r) {
  CHECK_EQ(workers_[worker_id]->checkpointing, true);

  if (r.checkpoint_type == CP_MASTER_CONTROLLED) {
    EmptyMessage req;
    rpc_->Send(1 + worker_id, MTYPE_FINISH_CHECKPOINT, req);
  }

  LOG(INFO) << "Waiting for " << worker_id << " to finish checkpointing.";

  EmptyMessage resp;
  rpc_->Read(1 + worker_id, MTYPE_CHECKPOINT_DONE, &resp);

  workers_[worker_id]->checkpointing = false;
}

void Master::flush_checkpoint(Params* params) {
  RecordFile rf(StringPrintf("%s/epoch_%05d/checkpoint.finished",
                            FLAGS_checkpoint_write_dir.c_str(), checkpoint_epoch_), "w");

  CheckpointInfo cinfo;
  cinfo.set_checkpoint_epoch(checkpoint_epoch_);
  cinfo.set_kernel_epoch(kernel_epoch_);

  rf.write(cinfo);
  rf.write(*params);
  rf.sync();

  LOG(INFO) << "Checkpoint: " << cp_timer_.elapsed() << " seconds elapsed; ";
  checkpointing_ = false;
  last_checkpoint_ = Now();
}

ParamMap* Master::restore() {
  vector<string> matches = File::Glob(FLAGS_checkpoint_read_dir + "/*/checkpoint.finished");
  if (matches.empty()) {
    return NULL;
  }

  // Glob returns results in sorted order, so our last checkpoint will be the last.
  const char* fname = matches.back().c_str();
  int epoch = -1;
  CHECK_EQ(sscanf(fname, (FLAGS_checkpoint_read_dir + "/epoch_%05d/checkpoint.finished").c_str(), &epoch),
           1) << "Unexpected filename: " << fname;

  LOG(INFO) << "Restoring from file: " << matches.back();

  RecordFile rf(matches.back(), "r");
  CheckpointInfo info;
  Params params;
  CHECK(rf.read(&info));
  CHECK(rf.read(&params));

  LOG(INFO) << "Restoring state from checkpoint " << MP(info.kernel_epoch(), info.checkpoint_epoch());

  kernel_epoch_ = info.kernel_epoch();
  checkpoint_epoch_ = info.checkpoint_epoch();

  StartRestore req;
  req.set_epoch(epoch);
  rpc_->Broadcast(MTYPE_RESTORE, req);

  for (int i = 0; i < config_.num_workers(); ++i) {
    EmptyMessage resp;
    LOG(INFO) << "Waiting for restore to finish... " << i + 1 << " of " << config_.num_workers();
    rpc_->ReadAny(NULL, MTYPE_RESTORE_DONE, &resp);
  }

  return ParamMap::from_params(params);
}

void Master::run_all(RunDescriptor r) {
  vector<int> shards;
  for (int i = 0; i < Registry::get_table(r.table)->info().num_shards; ++i) {
    shards.push_back(i);
  }
  run_range(r, shards);
}

void Master::run_one(RunDescriptor r) {
  vector<int> shards;
  shards.push_back(0);
  run_range(r, shards);
}

WorkerState* Master::worker_for_shard(int table, int shard) {
  for (int i = 0; i < workers_.size(); ++i) {
    if (workers_[i]->serves(Taskid(table, shard))) { return workers_[i]; }
  }

  return NULL;
}

WorkerState* Master::assign_worker(int table, int shard) {
  WorkerState* ws = worker_for_shard(table, shard);
  int64_t work_size = tables_[table][shard].entries();

  if (ws) {
//    LOG(INFO) << "Worker for shard: " << MP(table, shard, ws->id);
    ws->assign_task(new TaskState(Taskid(table, shard), work_size));
    return ws;
  }

  WorkerState* best = NULL;
  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    if (w.alive() && !w.full() &&
       (best == NULL || w.shards.size() < best->shards.size())) {
      best = workers_[i];
    }
  }

//  LOG(INFO) << "Assigned " << MP(table, shard, best->id);
  CHECK(best->alive());

  if (best->full()) {
    LOG(FATAL) << "Failed to assign work - no available workers!";
  }

  VLOG(1) << "Assigning " << MP(table, shard) << " to " << best->id;
  best->assign_shard(shard, true);
  best->assign_task(new TaskState(Taskid(table, shard), work_size));
  return best;
}

void Master::send_table_assignments() {
  ShardAssignmentRequest req;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    for (ShardSet::iterator j = w.shards.begin(); j != w.shards.end(); ++j) {
      ShardAssignment* s  = req.add_assign();
      s->set_new_worker(i);
      s->set_table(j->table);
      s->set_shard(j->shard);
//      s->set_old_worker(-1);
    }
  }

  rpc_->SyncBroadcast(MTYPE_SHARD_ASSIGNMENT, req);
}

void Master::steal_work(const RunDescriptor& r, int idle_worker) {
  if (!FLAGS_work_stealing) {
    return;
  }

  WorkerState &dst = *workers_[idle_worker];

  if (!dst.alive()) {
    return;
  }

  // Find the worker with the largest number of pending tasks.
  WorkerState& src = **max_element(workers_.begin(), workers_.end(), &WorkerState::PendingCompare);
  if (src.num_pending() == 0) {
    return;
  }

  vector<TaskState*> pending = src.pending();

  TaskState *task = *max_element(pending.begin(), pending.end(), TaskState::WeightCompare);
  const Taskid& tid = task->id;

  LOG(INFO) << "Worker " << idle_worker << " is stealing task "
            << MP(tid.shard, task->size) << " from worker " << src.id;
  dst.assign_shard(tid.shard, true);
  src.assign_shard(tid.shard, false);

  src.remove_task(task);
  dst.assign_task(task);

  // Update the table assignments.
  send_table_assignments();
}

void Master::assign_tables() {
  // Assign workers for all table shards, to ensure every shard has an owner.
  Registry::TableMap &tables = Registry::get_tables();
  for (Registry::TableMap::iterator i = tables.begin(); i != tables.end(); ++i) {
    for (int j = 0; j < i->second->num_shards(); ++j) {
      assign_worker(i->first, j);
    }
  }

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    w.clear_tasks();
  }
}

void Master::assign_tasks(const RunDescriptor& r, vector<int> shards) {
  for (int i = 0; i < shards.size(); ++i) {
    assign_worker(r.table, shards[i]);
  }
}

void Master::dispatch_work(const RunDescriptor& r) {
  KernelRequest w_req;
  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    if (w.num_pending() > 0 && w.num_active() == 0) {
      w.get_next(r, &w_req);
      rpc_->Send(w.id + 1, MTYPE_RUN_KERNEL, w_req);
    }
  }
}

void Master::run_range(RunDescriptor r, vector<int> shards) {
  KernelInfo *k = Registry::get_kernel(r.kernel);
  CHECK(k != NULL) << "Invalid kernel class " << r.kernel;
  CHECK(k->has_method(r.method)) << "Invalid method: " << MP(r.kernel, r.method);

  MethodStats &mstats = method_stats_[r.kernel + ":" + r.method];
  mstats.set_invocations(mstats.invocations() + 1);

  // Fill in the list of tables to checkpoint, if it was left empty.
  if (r.checkpoint_tables.empty()) {
    for (TableInfo::iterator i = tables_.begin(); i != tables_.end(); ++i) {
      r.checkpoint_tables.push_back(i->first);
    }
  }

  Timer t;

  assign_tables();
  assign_tasks(r, shards);
  send_table_assignments();
  dispatch_work(r);

  KernelDone k_done;

  int count = 0;
  while (count < shards.size()) {
    PERIODIC(5, {
       string status;
       for (int k = 0; k < config_.num_workers(); ++k) {
         status += StringPrintf("%d/%d ",
                                workers_[k]->num_finished(),
                                workers_[k]->num_assigned());
       }
       LOG(INFO) << StringPrintf("Running %s; %s; left: %d", r.method.c_str(), status.c_str(), shards.size() - count);
    });



    if (r.checkpoint_type == CP_ROLLING &&
        Now() - last_checkpoint_ > r.checkpoint_interval) {
      checkpoint(r);
    }

    if (rpc_->HasData(MPI_ANY_SOURCE, MTYPE_KERNEL_DONE)) {
      int w_id = 0;
      rpc_->ReadAny(&w_id, MTYPE_KERNEL_DONE, &k_done);
      w_id -= 1;

      Taskid task_id(k_done.kernel().table(), k_done.kernel().shard());

      VLOG(1) << "Finished: " << MP(task_id.table, task_id.shard);
      ++count;

      for (int i = 0; i < k_done.shards_size(); ++i) {
        const ShardInfo &si = k_done.shards(i);
        tables_[si.table()][si.shard()].CopyFrom(si);
      }

      WorkerState& w = *workers_[w_id];
      w.set_finished(task_id);

      w.total_runtime += Now() - w.last_task_start;
      mstats.set_total_shard_time(mstats.total_shard_time() + Now() - w.last_task_start);
      mstats.set_shard_invocations(mstats.shard_invocations() + 1);
      w.ping();
    } else {
      Sleep(0.001);
    }

    for (int i = 0; i < workers_.size(); ++i) {
      WorkerState& w = *workers_[i];
      double avg_completion_time = mstats.total_shard_time() / mstats.shard_invocations();
      if (w.idle(avg_completion_time) &&
          !w.full() && !checkpointing_) {
        steal_work(r, w.id);
      }

      if (r.checkpoint_type == CP_MASTER_CONTROLLED &&
          0.7 * shards.size() < count &&
          w.idle(avg_completion_time) &&
          !w.checkpointing) {
        start_worker_checkpoint(w.id, r);
      }

      // Just restore when the job is restarted by MPI.
//      if (!w.alive()) {
//        LOG(FATAL) << "Worker " << i << " died, restoring from last checkpoint.";
//        exit(1);
//        restore();
//
//        count = 0;
//        w.shards.clear();
//        assign_tables();
//        assign_tasks(r, shards);
//        send_table_assignments();
//        break;
//      }
    }

    dispatch_work(r);
  }

  mstats.set_total_time(mstats.total_time() + t.elapsed());

  if (r.checkpoint_type == CP_MASTER_CONTROLLED) {
    for (int i = 0; i < workers_.size(); ++i) {
      WorkerState& w = *workers_[i];
      if (!w.checkpointing) {
        start_worker_checkpoint(w.id, r);
      }
    }

    for (int i = 0; i < workers_.size(); ++i) {
      WorkerState& w = *workers_[i];
      finish_worker_checkpoint(w.id, r);
    }

    flush_checkpoint(r.params);
  }
  kernel_epoch_++;
  LOG(INFO) << "Kernel '" << r.method << "' finished in " << t.elapsed();
}

static void TestTaskSort() {
  vector<TaskState*> t;
  for (int i = 0; i < 100; ++i) {
    t.push_back(new TaskState(Taskid(0, i), rand()));
  }

  sort(t.begin(), t.end(), &TaskState::WeightCompare);
  for (int i = 1; i < 100; ++i) {
    CHECK_LE(t[i-1]->size, t[i]->size);
  }
}

REGISTER_TEST(TaskSort, TestTaskSort());
}
