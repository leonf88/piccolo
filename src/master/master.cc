#include "master/master.h"
#include "kernel/table-registry.h"
#include "kernel/kernel.h"

DEFINE_bool(work_stealing, true, "");
DEFINE_string(dead_workers, "",
              "Comma delimited list of workers to pretend have died.");
DECLARE_string(checkpoint_dir);

namespace dsm {

static unordered_set<int> dead_workers;

struct WorkerState : private boost::noncopyable {
  WorkerState(int w_id) : id(w_id), slots(0) {
    last_ping_time = Now();
    last_task_start = 0;
    total_runtime = 0;
  }

  // Pending tasks to work on.
  Master::TaskMap assigned;
  Master::TaskMap pending;
  Master::TaskMap active;

  // Table shards this worker is responsible for serving.
  Master::ShardMap shards;

  double last_ping_time;

  int status;
  int id;

  int slots;

  double last_task_start;
  double total_runtime;

  bool alive() {
    return dead_workers.find(id) == dead_workers.end();
  }

  bool is_assigned(int table, int shard) {
    return assigned.find(MP(table, shard)) != assigned.end();
  }

  int num_finished() {
    return assigned.size() - pending.size() - active.size();
  }

  void ping() {
    last_ping_time = Now();
  }

  bool idle(double avg_completion_time) {
    return pending.empty() &&
           active.empty() &&
           Now() - last_ping_time > avg_completion_time / 2;
  }

  bool full() { return assigned.size() >= slots; }

  void set_serves(int shard, bool should_service) {
    Registry::TableMap &tables = Registry::get_tables();
    for (Registry::TableMap::iterator i = tables.begin(); i != tables.end(); ++i) {
      Master::Taskid t = MP(i->first, shard);
      if (should_service) {
        shards[MP(i->first, shard)] = true;
      } else {
        shards.erase(shards.find(t));
      }
    }
  }

  bool serves(int table, int shard) {
    return shards.find(MP(table, shard)) != shards.end();
  }

  bool get_next(const Master::RunDescriptor& r,
                const Master::TableInfo& tin,
                KernelRequest* msg) {
    if (pending.empty()) {
      return false;
    }

    Master::TableInfo& tables = const_cast<Master::TableInfo&>(tin);
    Master::TaskMap::iterator best = pending.begin();
    for (Master::TaskMap::iterator i = pending.begin(); i != pending.end(); ++i) {
      if (tables[r.table][i->second->shard].entries() >
          tables[r.table][best->second->shard].entries()) {
  //      LOG(INFO) << "Choosing: " << i->second->shard << " : "
  //                << tables[r.table][i->second->shard].entries();
        best = i;
      }
    }

    msg->set_kernel(r.kernel);
    msg->set_method(r.method);
    msg->set_table(r.table);
    msg->set_shard(best->second->shard);

    active[best->first] = best->second;
    pending.erase(best);

    last_task_start = Now();

    return true;
  }
};

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = MPI::COMM_WORLD;
  restored_checkpoint_epoch_ = 0;
  restored_kernel_epoch_ = 0;
  checkpoint_epoch_ = 0;
  kernel_epoch_ = 0;
  last_checkpoint_ = Now();

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
  LOG(INFO) << "dead workers: " << FLAGS_dead_workers;
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

void Master::checkpoint(Params *params, bool compute_deltas) {
  checkpoint_epoch_ += 1;

  File::Mkdirs(StringPrintf("%s/epoch_%05d/",
                            FLAGS_checkpoint_dir.c_str(), checkpoint_epoch_));

  StartCheckpoint req;
  req.set_epoch(checkpoint_epoch_);
  req.set_compute_deltas(compute_deltas);
  rpc_->Broadcast(MTYPE_CHECKPOINT, req);

  Timer t;

  // Pause any other kind of activity until the workers all confirm the checkpoint is done; this is
  // to avoid changing the state of the system (via new shard or task assignments) until the checkpoint
  // is complete.
  for (int i = 0; i < config_.num_workers(); ++i) {
    EmptyMessage resp;
    int src;
    rpc_->ReadAny(&src, MTYPE_CHECKPOINT_DONE, &resp);
    PERIODIC(5,
             LOG(INFO) << "Checkpoint: " << src - 1 << " finished in " << t.elapsed()
                       << "; " << config_.num_workers() - i << " tasks remaining.");
  }


  RecordFile rf(StringPrintf("%s/epoch_%05d/checkpoint.finished",
                            FLAGS_checkpoint_dir.c_str(), checkpoint_epoch_), "w");

  CheckpointInfo cinfo;
  cinfo.set_checkpoint_epoch(checkpoint_epoch_);
  cinfo.set_kernel_epoch(kernel_epoch_);

  rf.write(cinfo);
  rf.write(*params);
  rf.sync();
}

Params* Master::restore() {
  vector<string> matches = File::Glob(FLAGS_checkpoint_dir + "/*/checkpoint.finished");
  if (matches.empty()) {
    return NULL;
  }

  // Glob returns results in sorted order, so our last checkpoint will be the last.
  const char* fname = matches.back().c_str();
  int epoch = -1;
  CHECK_EQ(sscanf(fname, (FLAGS_checkpoint_dir + "/epoch_%05d/checkpoint.finished").c_str(), &epoch),
           1) << "Unexpected filename: " << fname;

  LOG(INFO) << "Restoring from file: " << matches.back();

  RecordFile rf(matches.back(), "r");
  CheckpointInfo info;
  Params *params = new Params;
  CHECK(rf.read(&info));
  CHECK(rf.read(params));

  restored_kernel_epoch_ = info.kernel_epoch();
  restored_checkpoint_epoch_ = info.checkpoint_epoch();
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

  return params;
}

void Master::run_all(const RunDescriptor& r) {
  vector<int> shards;
  for (int i = 0; i < Registry::get_table(r.table)->info().num_shards; ++i) {
    shards.push_back(i);
  }
  run_range(r, shards);
}

void Master::run_one(const RunDescriptor& r) {
  vector<int> shards;
  shards.push_back(0);
  run_range(r, shards);
}

WorkerState* Master::worker_for_shard(int table, int shard) {
  for (int i = 0; i < workers_.size(); ++i) {
    if (workers_[i]->serves(table, shard)) { return workers_[i]; }
  }

  return NULL;
}

WorkerState* Master::assign_worker(int table, int shard) {
  WorkerState* ws = worker_for_shard(table, shard);
  if (ws) {
//    LOG(INFO) << "Worker for shard: " << MP(table, shard, ws->id);
    ws->assigned[MP(table, shard)] = new Task(table, shard);
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

  LOG(INFO) << "Assigned " << MP(table, shard, best->id);
  CHECK(best->alive());

  if (best->full()) {
    LOG(FATAL) << "Failed to assign work - no available workers!";
  }

  VLOG(1) << "Assigning " << MP(table, shard) << " to " << best->id;
  best->set_serves(shard, true);
  best->assigned[MP(table, shard)] = new Task(table, shard);
  return best;
}

void Master::send_table_assignments() {
  ShardAssignmentRequest req;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    for (ShardMap::iterator j = w.shards.begin(); j != w.shards.end(); ++j) {
      ShardAssignment* s  = req.add_assign();
      s->set_new_worker(i);
      s->set_table(j->first.first);
      s->set_shard(j->first.second);
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

  // Find a worker with an idle task.
  int busy_worker = -1;
  int t_count = 0;
  for (int i = 0; i < workers_.size(); ++i) {
    const WorkerState &w = *workers_[i];
    if (w.pending.size() > t_count) {
      busy_worker = i;
      t_count = w.pending.size();
    }
  }

  if (busy_worker == -1) { return; }

  WorkerState& src = *workers_[busy_worker];
  Taskid tid;
  Task *task = NULL;
  for (TaskMap::iterator i = src.pending.begin(); i != src.pending.end(); ++i) {
    if (stolen_.find(i->first) == stolen_.end()) {
      tid = i->first;
      task = i->second;
      break;
    }
  }

  if (task == NULL)
    return;

  LOG(INFO) << "Worker " << idle_worker << " is stealing task " << task->shard << " from " << busy_worker;
  dst.set_serves(task->shard, true);
  src.set_serves(task->shard, false);

  src.pending.erase(tid);
  src.assigned.erase(tid);

  dst.assigned[tid] = task;
  dst.pending[tid] = task;

  stolen_.insert(tid);

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

    w.assigned.clear();
    w.pending.clear();
  }
}

void Master::assign_tasks(const RunDescriptor& r, vector<int> shards) {
  for (int i = 0; i < shards.size(); ++i) {
    assign_worker(r.table, shards[i]);
  }

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    w.pending = w.assigned;
  }
}

void Master::dispatch_work(const RunDescriptor& r) {
  KernelRequest w_req;
  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = *workers_[i];
    if (!w.pending.empty() && w.active.empty()) {
      w.get_next(r, tables_, &w_req);
      rpc_->Send(w.id + 1, MTYPE_RUN_KERNEL, w_req);
    }
  }
}

void Master::run_range(const RunDescriptor& r, vector<int> shards) {
  KernelInfo *k = Registry::get_kernel(r.kernel);
  CHECK(k != NULL) << "Invalid kernel class " << r.kernel;
  CHECK(k->has_method(r.method)) << "Invalid method: " << MP(r.kernel, r.method);

  MethodStats &mstats = method_stats_[r.kernel + ":" + r.method];
  mstats.set_invocations(mstats.invocations() + 1);

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
                                workers_[k]->assigned.size());
       }
       LOG(INFO) << StringPrintf("Running %s; %s; left: %d", r.method.c_str(), status.c_str(), shards.size() - count);
    });

    if (r.checkpoint_interval > 0 && Now() - last_checkpoint_ > r.checkpoint_interval) {
      checkpoint(NULL, true /* require delta updates */);
      last_checkpoint_ = Now();
    }

    if (rpc_->HasData(MPI_ANY_SOURCE, MTYPE_KERNEL_DONE)) {
      int w_id = 0;
      rpc_->ReadAny(&w_id, MTYPE_KERNEL_DONE, &k_done);
      w_id -= 1;

      pair<int, int> task_id = MP(k_done.kernel().table(), k_done.kernel().shard());

      VLOG(1) << "Finished: " << task_id;
      ++count;

      for (int i = 0; i < k_done.shards_size(); ++i) {
        const ShardInfo &si = k_done.shards(i);
        tables_[si.table()][si.shard()].CopyFrom(si);
      }

      WorkerState& w = *workers_[w_id];
      CHECK(w.active.find(task_id) != w.active.end());
      w.active.erase(task_id);
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
      if (w.idle(avg_completion_time) && !w.full()) {
        steal_work(r, w.id);
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

  kernel_epoch_++;
  LOG(INFO) << "Kernel '" << r.method << "' finished in " << t.elapsed();
}

}
