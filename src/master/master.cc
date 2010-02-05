#include "master/master.h"
#include "kernel/table-registry.h"

namespace dsm {

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = MPI::COMM_WORLD;
  rpc_ = new RPCHelper(&world_);
  for (int i = 0; i < config_.num_workers(); ++i) {
    workers_.push_back(WorkerState(i));
  }
}

Master::~Master() {
  EmptyMessage msg;
  LOG(INFO) << "Shutting down workers.";
  for (int i = 1; i < world_.Get_size(); ++i) {
    rpc_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
  }
}

Master::WorkerState::WorkerState(int w_id) : id(w_id) {}

bool Master::WorkerState::get_next(const RunDescriptor& r, KernelRequest* msg) {
  if (pending.empty()) {
    return false;
  }

  Task t = pending.begin()->second;

  msg->set_kernel(r.kernel);
  msg->set_method(r.method);
  msg->set_table(r.table);
  msg->set_shard(t.shard);

  pending.erase(pending.begin());

  return true;
}

void Master::WorkerState::set_serves(int shard, bool should_service) {
  for (int i = 0; i < Registry::get_tables().size(); ++i) {
    Taskid t = MP(i, shard);
    if (should_service) {
      shards[MP(i, shard)] = ShardInfo();
    } else {
      shards.erase(shards.find(t));
    }
  }
}

bool Master::WorkerState::serves(int table, int shard) {
  return shards.find(MP(table, shard)) != shards.end();
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

Master::WorkerState* Master::worker_for_shard(int table, int shard) {
  for (int i = 0; i < workers_.size(); ++i) {
    if (workers_[i].serves(table, shard)) { return &workers_[i]; }
  }

  return NULL;
}

Master::WorkerState* Master::assign_worker(int table, int shard) {
  WorkerState* w = worker_for_shard(table, shard);
  if (w) {
    w->assigned[MP(table, shard)] = Task(table, shard);
    return w;
  }

  WorkerState* best = &workers_[0];
  for (int i = 0; i < workers_.size(); ++i) {
    if (workers_[i].shards.size() < best->shards.size()) {
      best = &workers_[i];
    }
  }

  VLOG(1) << "Assigning " << MP(table, shard) << " to " << best->id;
  best->set_serves(shard, true);
  best->assigned[MP(table, shard)] = Task(table, shard);
  return best;
}

void Master::send_assignments() {
  ShardAssignmentRequest req;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];
    for (ShardMap::iterator j = w.shards.begin(); j != w.shards.end(); ++j) {
      ShardAssignment* s  = req.add_assign();
      s->set_new_worker(i);
      s->set_table(j->first.first);
      s->set_shard(j->first.second);
//      s->set_old_worker(-1);
    }
  }

  rpc_->Broadcast(MTYPE_SHARD_ASSIGNMENT, req);
  world_.Barrier();
}

void Master::steal_work(const RunDescriptor& r, int idle_worker) {
  WorkerState &dst = workers_[idle_worker];

  // Find a worker with an idle task.
  int busy_worker = -1;
  int t_count = 0;
  for (int i = 0; i < workers_.size(); ++i) {
    const WorkerState &w = workers_[i];
    if (w.pending.size() > t_count) {
      busy_worker = i;
      t_count = w.pending.size();
    }
  }

  if (busy_worker == -1) { return; }

  WorkerState& src = workers_[busy_worker];
  Taskid tid = src.pending.begin()->first;
  Task task = src.pending.begin()->second;

  LOG(INFO) << "Worker " << idle_worker << " is stealing task " << task.shard << " from " << busy_worker;
  dst.set_serves(task.shard, true);
  src.set_serves(task.shard, false);

  src.pending.erase(tid);
  dst.assigned[tid] = task;
  dst.pending[tid] = task;

  // Update the table assignments.
  send_assignments();
}

void Master::run_range(const RunDescriptor& r, vector<int> shards) {
  Timer t;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];

    w.assigned.clear();
    w.pending.clear();
  }

  for (int i = 0; i < shards.size(); ++i) {
    assign_worker(r.table, i);
  }

  send_assignments();

  KernelRequest w_req;
  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];
    w.finished = 0;
    w.pending = w.assigned;

    if (!w.pending.empty()) {
      w.get_next(r, &w_req);
      rpc_->Send(w.id + 1, MTYPE_RUN_KERNEL, w_req);
    }
  }

  KernelRequest k_done;

  int count = 0;
  for (int j = 0; j < shards.size(); ++j) {
    int w_id = 0;
    rpc_->ReadAny(&w_id, MTYPE_KERNEL_DONE, &k_done);
    w_id -= 1;

    WorkerState& w = workers_[w_id];
    w.finished += 1;
    ++count;

    string status;
    for (int k = 0; k < config_.num_workers(); ++k) {
      status += StringPrintf("%2d/%2d; ", workers_[k].finished, workers_[k].assigned.size());
    }

    if (w.idle()) {
      steal_work(r, w_id);
    }

    if (!w.pending.empty()) {
      w.get_next(r, &w_req);
      rpc_->Send(w.id + 1, MTYPE_RUN_KERNEL, w_req);
    }

    LOG(INFO) << "Progress: " << status << " left " << shards.size() - count;
  }

  LOG(INFO) << "Kernel run finished in " << t.elapsed();
}

}
