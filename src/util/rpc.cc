#include "util/rpc.h"

DECLARE_bool(localtest);
DEFINE_bool(rpc_log, false, "");

namespace dsm {

void ProtoWrapper::AppendToCoder(Encoder *e) const {
  string s = p_->SerializeAsString();
  e->write_bytes(s.data(), s.size());
}

void ProtoWrapper::ParseFromCoder(Decoder *d) {
  Clear();
  p_->ParseFromArray(&d->data()[0], d->data().size());
}

#define rpc_log(msg, src, target, rpc) do { if (FLAGS_rpc_log) { LOG(INFO) << StringPrintf("%d - > %d (%d)", src, target, rpc) << " :: " << msg; } } while(0)
#define rpc_lock

bool RPCHelper::HasData(int peerId, int rpcId) {
  rpc_lock;

  rpc_log("IProbe", my_rank_, peerId, rpcId);
  return mpi_world_->Iprobe(peerId, rpcId);
}

bool RPCHelper::HasData(int peerId, int rpcId, MPI::Status &status) {
  rpc_lock;

  rpc_log("IProbe", my_rank_, peerId, rpcId);
  return mpi_world_->Iprobe(peerId, rpcId, status);
}


bool RPCHelper::TryRead(int peerId, int rpcId, RPCMessage *msg) {
  rpc_lock;
  bool success = false;
  string scratch;
  MPI::Status status;

//  rpc_log("IProbeStart", my_rank_, peerId, rpcId);
  MPI::Status probe_result;
  if (mpi_world_->Iprobe(peerId, rpcId, probe_result)) {
    success = true;

    rpc_log("IProbeSuccess", my_rank_, peerId, rpcId);

    scratch.resize(probe_result.Get_count(MPI::BYTE));

    mpi_world_->Recv(&scratch[0], scratch.size(), MPI::BYTE, peerId, rpcId, status);
    VLOG(2) << "Read message of size: " << scratch.size();

    msg->ParseFromString(scratch);
  }

//  rpc_log("IProbeDone", my_rank_, peerId, rpcId);
  return success;
}

int RPCHelper::Read(int peerId, int rpcId, RPCMessage *msg) {
  rpc_lock;

  int r_size = 0;
  string scratch;
  MPI::Status status;

  rpc_log("BProbeStart", my_rank_, peerId, rpcId);
  MPI::Status probe_result;
  mpi_world_->Probe(peerId, rpcId, probe_result);
  rpc_log("BProbeDone", my_rank_, peerId, rpcId);

  r_size = probe_result.Get_count(MPI::BYTE);
  scratch.resize(r_size);

  VLOG(2) << "Reading message of size: " << r_size << " :: " << &scratch[0];

  mpi_world_->Recv(&scratch[0], r_size, MPI::BYTE, peerId, rpcId, status);
  msg->ParseFromString(scratch);
  return r_size;
}

int RPCHelper::ReadAny(int *peerId, int rpcId, RPCMessage *msg) {
  rpc_lock;
  int r_size = 0;
  string scratch;

  while (!HasData(MPI_ANY_SOURCE, rpcId)) {
    Sleep(0.001);
  }

  MPI::Status probe_result;
  mpi_world_->Probe(MPI_ANY_SOURCE, rpcId, probe_result);

  r_size = probe_result.Get_count(MPI::BYTE);
  *peerId = probe_result.Get_source();

  scratch.resize(r_size);
  mpi_world_->Recv(&scratch[0], r_size, MPI::BYTE, *peerId, rpcId);
  msg->ParseFromString(scratch);
  return r_size;
}

void RPCHelper::Send(int peerId, int rpcId, const RPCMessage &msg) {
  rpc_lock;
  rpc_log("SendStart", my_rank_, peerId, rpcId);
  string scratch;

  scratch.clear();
  msg.AppendToString(&scratch);
  mpi_world_->Send(&scratch[0], scratch.size(), MPI::BYTE, peerId, rpcId);
  rpc_log("SendDone", my_rank_, peerId, rpcId);
}

MPI::Request RPCHelper::SendData(int peerId, int rpcId, const string& msg) {
  rpc_lock;
  rpc_log("SendData", my_rank_, peerId, rpcId);
  return mpi_world_->Isend(&msg[0], msg.size(), MPI::BYTE, peerId, rpcId);
}


// For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
// here.
void RPCHelper::Broadcast(int rpcId, const RPCMessage &msg) {
  rpc_lock;
  for (int i = 0; i < mpi_world_->Get_size(); ++i) {
    Send(i, rpcId, msg);
  }
}

}
