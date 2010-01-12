#include "master/master.h"

namespace upc {

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = MPI::COMM_WORLD;
  rpc_ = new RPCHelper(&world_);
}

Master::~Master() {
  EmptyMessage msg;
  LOG(INFO) << "Shutting down workers.";
  for (int i = 1; i < world_.Get_size(); ++i) {
    rpc_->Send(i, MTYPE_WORKER_SHUTDOWN, ProtoWrapper(msg));
  }
}

void Master::run_all(const string& kernel, int method) {
  vector<int> nodes;
  for (int i = 1; i < world_.Get_size(); ++i) {
    nodes.push_back(i);
  }
  run_range(kernel, method, nodes);
}

void Master::run_one(const string& kernel, int method) {
  vector<int> nodes;
  nodes.push_back(1);
  run_range(kernel, method, nodes);
}

int Master::worker_for_shard(int shard) {
  return shard % config_.num_workers();
}

void Master::run_range(const string& kernel, int method, vector<int> shards) {
  RunKernelRequest msg;
  msg.set_kernel(kernel);
  msg.set_method(method);

  Timer t;
  for (int i = 0; i < shards.size(); ++i) {
    rpc_->Send(worker_for_shard(shards[i]), MTYPE_RUN_KERNEL, ProtoWrapper(msg));
  }

  string waiting(shards.size(), '0');
//  int peer = 0;
//  for (int i = 0; i < nodes.size(); ++i) {
//    EmptyMessage msg;
//    ProtoWrapper wrapper(msg);
//    rpc_->ReadAny(&peer, MTYPE_KERNEL_DONE, &wrapper);
//    waiting[peer - 1] = '1';
//    LOG(INFO) << "Finished kernel: " << waiting;
//  }

  EmptyMessage empty;
  ProtoWrapper wrapper(empty);

  while (!shards.empty()) {
    bool found = false;
    for (int j = 0; j < shards.size(); ++j) {
      if (rpc_->TryRead(worker_for_shard(shards[j]), MTYPE_KERNEL_DONE, &wrapper)) {
        waiting[shards[j] - 1] = '1';

        shards.erase(shards.begin() + j);
        found = true;
//        LOG(INFO) << "Kernels finished: " << waiting;
        break;
      }
    }

    if (!found) { Sleep(0.0001); }
  }

  LOG(INFO) << "Kernels finished in " << t.elapsed();
}

}
