#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include <mpi.h>
#include <google/protobuf/message.h>

namespace upc {

class RPCHelper {
public:
  RPCHelper(MPI::Comm *mpi) :
    mpiWorld(mpi) {
  }

  // Try to read a message from the given peer and rpc channel.  Return
  // the number of bytes read, 0 if no message was available.
  int TryRead(int peerId, int rpcId, google::protobuf::Message *msg,
              string *scratch = NULL) {
    string buf;
    int rSize = 0;

    if (!scratch) {
      scratch = &buf;
    }

    MPI::Status probeResult;
    if (mpiWorld->Iprobe(peerId, rpcId, probeResult)) {
      rSize = probeResult.Get_count(MPI::BYTE);
      scratch->resize(rSize);
      mpiWorld->Recv(&(*scratch)[0], rSize, MPI::BYTE, peerId, rpcId);
      msg->ParseFromString(*scratch);
      return rSize;
    }
  }

  int Read(int peerId, int rpcId, google::protobuf::Message *msg,
           string *scratch = NULL) {
    string buf;
    int rSize = 0;

    if (!scratch) {
      scratch = &buf;
    }

    MPI::Status probeResult;
    mpiWorld->Probe(peerId, rpcId, probeResult);
    rSize = probeResult.Get_count(MPI::BYTE);
    scratch->resize(rSize);
    mpiWorld->Recv(&(*scratch)[0], rSize, MPI::BYTE, peerId, rpcId);
    msg->ParseFromString(*scratch);
    return rSize;
  }

  void Send(int peerId, int rpcId, const google::protobuf::Message &msg) {
    string scratch;
    msg.AppendToString(&scratch);
    mpiWorld->Send(&scratch[0], scratch.size(), MPI::BYTE, peerId, rpcId);
  }

private:
  MPI::Comm *mpiWorld;
};
}

#endif // UTIL_RPC_H
