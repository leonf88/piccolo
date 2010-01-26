#include "master/master.h"

namespace dsm {

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

int Master::worker_for_shard(int shard) {
  return shard % config_.num_workers();
}

void Master::run_range(const RunDescriptor& r, vector<int> shards) {
  KernelRequest msg;
  msg.set_kernel(r.kernel);
  msg.set_method(r.method);

  Timer t;
  vector<int> done(config_.num_workers());
  for (int i = 0; i < shards.size(); ++i) {
    msg.set_shard(shards[i]);
    VLOG(1) << "Sending shard: " << shards[i] << " to " << worker_for_shard(shards[i]);
    rpc_->Send(1 + worker_for_shard(shards[i]), MTYPE_RUN_KERNEL, ProtoWrapper(msg));
  }

  KernelRequest kernel_done;
  ProtoWrapper wrapper(kernel_done);

  int count = 0;
  for (int j = 0; j < shards.size(); ++j) {
    int peer = 0;
    rpc_->ReadAny(&peer, MTYPE_KERNEL_DONE, &wrapper);

    done[peer - 1] += 1;
    ++count;

    string status;
    for (int k = 0; k < config_.num_workers(); ++k) {
      status += StringPrintf("%d: %d; ", k, done[k]);
    }

    VLOG(1) << "Kernels finished: " << status << " left " << shards.size() - count;
  }

  LOG(INFO) << "Kernel run finished in " << t.elapsed();
}

}
