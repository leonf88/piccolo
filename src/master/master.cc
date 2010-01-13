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
  RunKernelRequest msg;
  msg.set_kernel(r.kernel);
  msg.set_method(r.method);

  Timer t;
  for (int i = 0; i < shards.size(); ++i) {
    msg.set_shard(shards[i]);
    VLOG(1) << "Sending shard: " << shards[i] << " to " << worker_for_shard(shards[i]);
    rpc_->Send(1 + worker_for_shard(shards[i]), MTYPE_RUN_KERNEL, ProtoWrapper(msg));
  }

  string waiting(shards.size(), '0');

  EmptyMessage empty;
  ProtoWrapper wrapper(empty);

  while (!shards.empty()) {
    bool found = false;
    for (int j = 0; j < shards.size(); ++j) {
      if (rpc_->TryRead(1 + worker_for_shard(shards[j]),
                        MTYPE_KERNEL_DONE, &wrapper)) {
        waiting[shards[j] - 1] = '1';

        shards.erase(shards.begin() + j);
        found = true;
        LOG(INFO) << "Kernels finished: " << waiting << " : " << waiting.size() - shards.size();
        break;
      }
    }

    if (!found) { Sleep(0.0001); }
  }

  LOG(INFO) << "Kernels finished in " << t.elapsed();
}

}
