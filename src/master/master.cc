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

void Master::run_range(KernelFunction f, vector<int> nodes) {
  int id = KernelRegistry::get_id(f);
  RunKernelRequest msg;
  msg.set_kernel_id(id);

  Timer t;

  for (int i = 0; i < nodes.size(); ++i) {
    rpc_->Send(nodes[i], MTYPE_RUN_KERNEL, ProtoWrapper(msg));
  }

  string waiting(nodes.size(), '0');
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

  while (!nodes.empty()) {
    bool found = false;
    for (int j = 0; j < nodes.size(); ++j) {
      if (rpc_->TryRead(nodes[j], MTYPE_KERNEL_DONE, &wrapper)) {
        waiting[nodes[j] - 1] = '1';

        nodes.erase(nodes.begin() + j);
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
