#include "master/master.h"

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

Master::WorkerState::WorkerState(int w_id) : id(w_id) {
  assigned.resize(Registry::get_tables().size());
  pending.resize(Registry::get_tables().size());
  finished.resize(Registry::get_tables().size());
}

void Master::WorkerState::assign(int task) {
  for (int i = 0; i < Registry::get_tables().size(); ++i) {
    assigned[i].push_back(task);
    pending[i].push_back(task);
  }
}

bool Master::WorkerState::get_next(const RunDescriptor& r, KernelRequest* msg) {
  if (pending[r.table].empty()) {
    return false;
  }

  int shard = pending[r.table].back();
  pending[r.table].pop_back();

  msg->set_kernel(r.kernel);
  msg->set_method(r.method);
  msg->set_table(r.table);
  msg->set_shard(shard);

  return true;
}

void Master::WorkerState::deassign(int task) {
  for (int i = 0; i < Registry::get_tables().size(); ++i) {
    TaskList::iterator it = find(assigned[i].begin(), assigned[i].end(), task);
    if (it != assigned[i].end()) { assigned[i].erase(it); }
    it = find(pending[i].begin(), pending[i].end(), task);

    if (it != pending[i].end()) { pending[i].erase(it); }
  }
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

int Master::worker_for_shard(int table, int shard) {
  for (int i = 0; i < workers_.size(); ++i) {
    if (workers_[i].is_assigned(table, shard)) { return i; }
  }

  return -1;
}

int Master::assign_worker(int table, int shard) {
  if (worker_for_shard(table, shard) >= 0) {
    return worker_for_shard(table, shard);
  }

  int best = 0;
  int best_v = 1e6;
  for (int i = 0; i < workers_.size(); ++i) {
    if (workers_[i].assigned[table].size() < best_v) {
      best_v = workers_[i].assigned[table].size();
      best = i;
    }
  }

  VLOG(1) << "Assigning " << shard << " to " << best;
  workers_[best].assigned[table].push_back(shard);
  return best;
}

void Master::send_assignments() {
  ShardAssignmentRequest req;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];
    for (int j = 0; j < w.assigned.size(); ++j) {
      TaskList& t = w.assigned[j];
      for (TaskList::iterator k = t.begin(); k != t.end(); ++k) {
        ShardAssignment* s  = req.add_assign();
        s->set_new_worker(i);
        s->set_shard(*k);
        s->set_table(j);
        s->set_old_worker(-1);
      }
    }
  }

  rpc_->SyncBroadcast(MTYPE_SHARD_ASSIGNMENT, req);
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
  int task = src.pending[r.table].back();

  LOG(INFO) << "Worker " << idle_worker << " is stealing task " << task << " from " << busy_worker;
  dst.assign(task);
  src.deassign(task);

  // Update the table assignments.
  send_assignments();

  // Send the new kernel assignments.
//  KernelRequest msg;
//  msg.set_kernel(r.kernel);
//  msg.set_method(r.method);
//  msg.set_table(r.table);
//  msg.set_shard(task);
//  rpc_->Send(1 + busy_worker, MTYPE_STOP_KERNEL, msg);
}

void Master::run_range(const RunDescriptor& r, vector<int> shards) {
  Timer t;

  for (int i = 0; i < shards.size(); ++i) {
    for (int j = 0; j < Registry::get_tables().size(); ++j) {
      assign_worker(j, shards[i]);
    }
  }

  send_assignments();

  KernelRequest w_req;
  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];
    w.finished[r.table].clear();
    w.pending[r.table] = w.assigned[r.table];

    if (!w.pending[r.table].empty()) {
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
    w.finished[k_done.table()].push_back(k_done.shard());
    ++count;

    string status;
    for (int k = 0; k < config_.num_workers(); ++k) {
      status += StringPrintf("%d: %d; ", k, workers_[k].finished[r.table].size());
    }

    if (w.idle(r.table) && shards.size() - count > 3) {
      steal_work(r, w_id);
    }

    if (!w.pending[r.table].empty()) {
      w.get_next(r, &w_req);
      rpc_->Send(w.id + 1, MTYPE_RUN_KERNEL, w_req);
    }

    LOG(INFO) << "Progress: " << status << " left " << shards.size() - count;
  }

  LOG(INFO) << "Kernel run finished in " << t.elapsed();
}

}
