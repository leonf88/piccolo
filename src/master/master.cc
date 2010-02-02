#include "master/master.h"

namespace dsm {

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = MPI::COMM_WORLD;
  rpc_ = new RPCHelper(&world_);
  workers_.resize(config_.num_workers());
}
Master::~Master() {
  EmptyMessage msg;
  LOG(INFO) << "Shutting down workers.";
  for (int i = 1; i < world_.Get_size(); ++i) {
    rpc_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
  }
}

Master::WorkerState::WorkerState() {
  assigned.resize(Registry::get_tables().size());
  pending.resize(Registry::get_tables().size());
  finished.resize(Registry::get_tables().size());
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
  workers_[best].assigned[table].insert(shard);
  return best;
}

void Master::send_assignments() {
  ShardAssignmentRequest req;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];
    for (int j = 0; j < w.assigned.size(); ++j) {
      unordered_set<int>& t = w.assigned[j];
      for (unordered_set<int>::iterator k = t.begin(); k != t.end(); ++k) {
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

void Master::steal_work(int idle_worker, const RunDescriptor& r) {
  WorkerState &dst = workers_[idle_worker];

  int busy_worker = -1;
  int t_count = 1;
  for (int i = 0; i < workers_.size(); ++i) {
    const WorkerState &w = workers_[i];
    if (w.pending.size() > t_count) {
      busy_worker = i;
      t_count = w.pending.size();
    }
  }

  if (busy_worker == -1) { return; }

  WorkerState& src = workers_[busy_worker];
  int task = *src.pending[r.table].begin();

  LOG(INFO) << "Worker " << idle_worker << " is stealing task " << task << " from " << busy_worker;
  dst.assigned[r.table].insert(task);
  dst.pending[r.table].insert(task);
  src.assigned[r.table].erase(task);
  src.pending[r.table].erase(task);

  // Update the table assignments.
  send_assignments();

  // Send the new kernel assignments.
  KernelRequest msg;
  msg.set_kernel(r.kernel);
  msg.set_method(r.method);
  msg.set_table(r.table);
  msg.set_shard(task);

  rpc_->Send(1 + idle_worker, MTYPE_RUN_KERNEL, msg);
  rpc_->Send(1 + busy_worker, MTYPE_STOP_KERNEL, msg);
}

void Master::run_range(const RunDescriptor& r, vector<int> shards) {
  KernelRequest msg;
  msg.set_kernel(r.kernel);
  msg.set_method(r.method);
  msg.set_table(r.table);

  Timer t;

  for (int i = 0; i < shards.size(); ++i) {
    for (int j = 0; j < Registry::get_tables().size(); ++j) {
      assign_worker(j, shards[i]);
    }
  }

  send_assignments();

  for (int i = 0; i < shards.size(); ++i) {
    int widx = worker_for_shard(0, shards[i]);
    msg.set_shard(shards[i]);
    WorkerState& w = workers_[widx];
    w.pending[r.table].insert(i);
    VLOG(1) << "Running shard: " << shards[i] << " on " << worker_for_shard(0, shards[i]);
    rpc_->Send(1 + widx, MTYPE_RUN_KERNEL, msg);
  }

  KernelRequest k_done;

  int count = 0;
  for (int j = 0; j < shards.size(); ++j) {
    int w_id = 0;
    rpc_->ReadAny(&w_id, MTYPE_KERNEL_DONE, &k_done);
    w_id -= 1;

    WorkerState& w = workers_[w_id];
    w.pending[r.table].erase(k_done.shard());
    w.finished[k_done.table()].insert(k_done.shard());
    ++count;

    string status;
    for (int k = 0; k < config_.num_workers(); ++k) {
      status += StringPrintf("%d: %d; ", k, w.finished[r.table].size());
    }

    if (w.idle(r.table) && shards.size() - count > 3) {
      steal_work(w_id, r);
    }

    LOG(INFO) << "Progress: " << status << " left " << shards.size() - count;
  }

  LOG(INFO) << "Kernel run finished in " << t.elapsed();
}

}
