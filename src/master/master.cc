#include "master/master.h"

namespace upc {

Master::Master(const ConfigData &conf) {
  config_.CopyFrom(conf);
  world_ = GetMPIWorld();
  rpc_ = GetRPCHelper();
}

void Master::run(KernelFunction f) {
  int id = KernelRegistry::get_id(f);
  RunKernelRequest msg;
  msg.set_kernel_id(id);
  rpc_->Broadcast(MTYPE_RUN_KERNEL, msg);

  LOG(INFO) << "Waiting for response.";


  string waiting(world_->Get_size() - 1, '0');
  int peer = 0;
  for (int i = 1; i < world_->Get_size(); ++i) {
    EmptyMessage msg;
    rpc_->ReadAny(&peer, MTYPE_KERNEL_DONE, &msg);
    waiting[peer - 1] = '1';
    LOG(INFO) << "Finished kernel: " << waiting;
  }

  LOG(INFO) << "All kernels finished.";
}

}
