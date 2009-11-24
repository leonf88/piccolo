#include "master/master.h"

namespace upc {

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = MPI::COMM_WORLD;
  rpc_ = new RPCHelper(&world_);

  world_.Split(MASTER_COLOR, world_.Get_rank());
}

Master::~Master() {
  EmptyMessage msg;
  LOG(INFO) << "Shutting down workers.";
  for (int i = 1; i < world_.Get_size(); ++i) {
    rpc_->Send(i, MTYPE_WORKER_SHUTDOWN, msg);
  }
}

void Master::run_all(KernelFunction f) {
  vector<int> nodes;
  for (int i = 1; i < world_.Get_size(); ++i) {
    nodes.push_back(i);
  }
  run_range(f, nodes);
}

void Master::run_one(KernelFunction f) {
  vector<int> nodes;
  nodes.push_back(1);
  run_range(f, nodes);
}

void Master::run_range(KernelFunction f, const vector<int> &nodes) {
  int id = KernelRegistry::get_id(f);
  RunKernelRequest msg;
  msg.set_kernel_id(id);

  for (int i = 0; i < nodes.size(); ++i) {
    rpc_->Send(nodes[i], MTYPE_RUN_KERNEL, msg);
  }

  LOG(INFO) << "Waiting for response.";

  string waiting(nodes.size(), '0');
  int peer = 0;
  for (int i = 0; i < nodes.size(); ++i) {
    EmptyMessage msg;
    rpc_->ReadAny(&peer, MTYPE_KERNEL_DONE, &msg);
    waiting[peer - 1] = '1';
    LOG(INFO) << "Finished kernel: " << waiting;
  }

  LOG(INFO) << "All kernels finished.";
}

}
