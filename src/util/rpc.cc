#include "util/rpc.h"
#include "util/fake-mpi.h"

DECLARE_bool(localtest);

namespace upc {

RPCHelper *GetRPCHelper() {
  return new RPCHelper(GetMPIWorld());
}

MPI::Comm *GetMPIWorld() {
  MPI::Comm *world;
  if (FLAGS_localtest) {
    world = new FakeMPIComm(MPI_ANY_SOURCE);
  } else {
    world = &MPI::COMM_WORLD;
  }
  return world;
}

static void rpc_log(string msg, int src, int target, int rpc) {
  LOG(INFO) << StringPrintf("%d - > %d (%d)", src, target, rpc) << " :: " << msg;
}

// Try to read a message from the given peer and rpc channel.  Return
// the number of bytes read, 0 if no message was available.
int RPCHelper::TryRead(int peerId, int rpcId, google::protobuf::Message *msg, string *scratch) {
  string buf;
  int rSize = 0;

  if (!scratch) {
    scratch = &buf;
  }

  rpc_log("IProbeStart", mpiWorld->Get_rank(), peerId, rpcId);
  MPI::Status probeResult;
  if (mpiWorld->Iprobe(peerId, rpcId, probeResult)) {
    rSize = probeResult.Get_count(MPI::BYTE);
    scratch->resize(rSize);
    mpiWorld->Recv(&(*scratch)[0], rSize, MPI::BYTE, peerId, rpcId);
    msg->ParseFromString(*scratch);
  }

  rpc_log("IProbeDone", mpiWorld->Get_rank(), peerId, rpcId);
  return 0;
}

int RPCHelper::Read(int peerId, int rpcId, google::protobuf::Message *msg, string *scratch) {
  string buf;
  int rSize = 0;

  if (!scratch) {
    scratch = &buf;
  }

  rpc_log("BProbeStart", mpiWorld->Get_rank(), peerId, rpcId);
  MPI::Status probeResult;
  mpiWorld->Probe(peerId, rpcId, probeResult);
  rpc_log("BProbeDone", mpiWorld->Get_rank(), peerId, rpcId);

  rSize = probeResult.Get_count(MPI::BYTE);
  scratch->resize(rSize);
  mpiWorld->Recv(&(*scratch)[0], rSize, MPI::BYTE, peerId, rpcId);
  msg->ParseFromString(*scratch);
  return rSize;
}

int RPCHelper::ReadAny(int *peerId, int rpcId, google::protobuf::Message *msg, string *scratch) {
  string buf;
  int rSize = 0;

  if (!scratch) {
    scratch = &buf;
  }

  rpc_log("BProbeStart", mpiWorld->Get_rank(), MPI_ANY_SOURCE, rpcId);
  MPI::Status probeResult;
  mpiWorld->Probe(MPI_ANY_SOURCE, rpcId, probeResult);

  rpc_log("BProbeDone", mpiWorld->Get_rank(), MPI_ANY_SOURCE, rpcId);

  rSize = probeResult.Get_count(MPI::BYTE);
  *peerId = probeResult.Get_source();

  scratch->resize(rSize);
  mpiWorld->Recv(&(*scratch)[0], rSize, MPI::BYTE, *peerId, rpcId);
  msg->ParseFromString(*scratch);
  return rSize;
}

void RPCHelper::Send(int peerId, int rpcId, const google::protobuf::Message &msg, string *scratch) {
  string buf;

  if (!scratch) {
    scratch = &buf;
  }

  rpc_log("SendStart", mpiWorld->Get_rank(), peerId, rpcId);
  msg.AppendToString(scratch);
  mpiWorld->Send(&(*scratch)[0], scratch->size(), MPI::BYTE, peerId, rpcId);
  rpc_log("SendDone", mpiWorld->Get_rank(), peerId, rpcId);
}

// For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
// here.
void RPCHelper::Broadcast(int rpcId, const google::protobuf::Message &msg) {
  for (int i = 0; i < mpiWorld->Get_size(); ++i) {
    Send(i, rpcId, msg);
  }
}

}
