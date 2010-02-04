#include "util/rpc.h"

DECLARE_bool(localtest);
DEFINE_bool(rpc_log, false, "");

namespace dsm {

#define rpc_log(msg, src, target, rpc) do { if (FLAGS_rpc_log) { LOG(INFO) << StringPrintf("%d - > %d (%d)", src, target, rpc) << " :: " << msg; } } while(0)
#define rpc_lock

bool RPCHelper::HasData(int target, int rpc) {
  rpc_lock;

  rpc_log("IProbe", my_rank_, target, rpc);
  return mpi_world_->Iprobe(target, rpc);
}

bool RPCHelper::HasData(int target, int rpc, MPI::Status &status) {
  rpc_lock;

  rpc_log("IProbe", my_rank_, target, rpc);
  return mpi_world_->Iprobe(target, rpc, status);
}

bool RPCHelper::TryRead(int target, int rpc, Message *msg) {
  rpc_lock;
  bool success = false;
  string scratch;
  MPI::Status status;

//  rpc_log("IProbeStart", my_rank_, target, rpc);
  MPI::Status probe_result;
  if (mpi_world_->Iprobe(target, rpc, probe_result)) {
    success = true;

    rpc_log("IProbeSuccess", my_rank_, target, rpc);

    scratch.resize(probe_result.Get_count(MPI::BYTE));

    mpi_world_->Recv(&scratch[0], scratch.size(), MPI::BYTE, target, rpc, status);
    VLOG(2) << "Read message of size: " << scratch.size();

    msg->ParseFromString(scratch);
  }

//  rpc_log("IProbeDone", my_rank_, target, rpc);
  return success;
}

int RPCHelper::Read(int target, int rpc, Message *msg) {
  rpc_lock;

  int r_size = 0;
  string scratch;
  MPI::Status status;

  rpc_log("BProbeStart", my_rank_, target, rpc);
  MPI::Status probe_result;
  mpi_world_->Probe(target, rpc, probe_result);
  rpc_log("BProbeDone", my_rank_, target, rpc);

  r_size = probe_result.Get_count(MPI::BYTE);
  scratch.resize(r_size);

  VLOG(2) << "Reading message of size: " << r_size << " :: " << &scratch[0];

  mpi_world_->Recv(&scratch[0], r_size, MPI::BYTE, target, rpc, status);
  msg->ParseFromString(scratch);
  return r_size;
}

int RPCHelper::ReadAny(int *target, int rpc, Message *msg) {
  rpc_lock;
  int r_size = 0;
  string scratch;

  while (!HasData(MPI_ANY_SOURCE, rpc)) {
    Sleep(0.001);
  }

  MPI::Status probe_result;
  mpi_world_->Probe(MPI_ANY_SOURCE, rpc, probe_result);

  r_size = probe_result.Get_count(MPI::BYTE);
  *target = probe_result.Get_source();

  scratch.resize(r_size);
  mpi_world_->Recv(&scratch[0], r_size, MPI::BYTE, *target, rpc);
  msg->ParseFromString(scratch);
  return r_size;
}

void RPCHelper::Send(int target, int rpc, const Message &msg) {
  rpc_lock;
  rpc_log("SendStart", my_rank_, target, rpc);
  string scratch;

  scratch.clear();
  msg.AppendToString(&scratch);
  mpi_world_->Send(&scratch[0], scratch.size(), MPI::BYTE, target, rpc);
  rpc_log("SendDone", my_rank_, target, rpc);
}

void RPCHelper::SyncSend(int target, int rpc, const Message &msg) {
  rpc_lock;
  rpc_log("SyncSendStart", my_rank_, target, rpc);
  string scratch;

  scratch.clear();
  msg.AppendToString(&scratch);
  mpi_world_->Ssend(&scratch[0], scratch.size(), MPI::BYTE, target, rpc);
  rpc_log("SyncSendDone", my_rank_, target, rpc);
}


MPI::Request RPCHelper::SendData(int target, int rpc, const string& msg) {
  rpc_lock;
  rpc_log("SendData", my_rank_, target, rpc);
  return mpi_world_->Isend(&msg[0], msg.size(), MPI::BYTE, target, rpc);
}


// For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
// here.
void RPCHelper::Broadcast(int rpc, const Message &msg) {
  rpc_lock;
  for (int i = 1; i < mpi_world_->Get_size(); ++i) {
    Send(i, rpc, msg);
  }
}

void RPCHelper::SyncBroadcast(int rpc, const Message &msg) {
  rpc_lock;
  for (int i = 1; i < mpi_world_->Get_size(); ++i) {
    SyncSend(i, rpc, msg);
  }
}

}
