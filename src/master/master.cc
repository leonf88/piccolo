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
  workers_[best].assigned[table].push_back(shard);
  return best;
}

void Master::send_assignments() {
  ShardAssignmentRequest req;

  for (int i = 0; i < workers_.size(); ++i) {
    WorkerState& w = workers_[i];
    for (int j = 0; j < w.assigned.size(); ++j) {
      vector<int>& t = w.assigned[j];
      for (int k = 0; k < t.size(); ++k) {
        ShardAssignment* s  = req.add_assign();
        s->set_new_worker(i);
        s->set_shard(t[k]);
        s->set_table(j);
        s->set_old_worker(-1);
      }
    }
  }

  rpc_->SyncBroadcast(MTYPE_SHARD_ASSIGNMENT, req);
}

void Master::run_range(const RunDescriptor& r, vector<int> shards) {
  KernelRequest msg;
  msg.set_kernel(r.kernel);
  msg.set_method(r.method);

  Timer t;
  vector<int> done(config_.num_workers());
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
    w.pending[r.table].push_back(i);
    VLOG(1) << "Running shard: " << shards[i] << " on " << worker_for_shard(0, shards[i]);
    rpc_->Send(1 + widx, MTYPE_RUN_KERNEL, msg);
  }

  KernelRequest kernel_done;

  int count = 0;
  for (int j = 0; j < shards.size(); ++j) {
    int peer = 0;
    rpc_->ReadAny(&peer, MTYPE_KERNEL_DONE, &kernel_done);

    done[peer - 1] += 1;
    ++count;

    string status;
    for (int k = 0; k < config_.num_workers(); ++k) {
      status += StringPrintf("%d: %d; ", k, done[k]);
    }

    LOG(INFO) << "Kernels finished: " << status << " left " << shards.size() - count;
  }

  LOG(INFO) << "Kernel run finished in " << t.elapsed();
}

}
