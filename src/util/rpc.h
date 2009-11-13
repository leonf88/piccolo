#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include <mpi.h>
#include <google/protobuf/message.h>

namespace upc {

class RPCHelper;

// Return pointers to mpi or rpc helpers.
RPCHelper *GetRPCHelper();
MPI::Comm *GetMPIWorld();

class RPCHelper {
public:
  RPCHelper(MPI::Comm *mpi) :
    mpiWorld(mpi) {
  }

  // Try to read a message from the given peer and rpc channel.  Return
  // the number of bytes read, 0 if no message was available.
  int TryRead(int peerId, int rpcId, google::protobuf::Message *msg, string *scratch = NULL);
  int Read(int peerId, int rpcId, google::protobuf::Message *msg, string *scratch = NULL);
  int ReadAny(int *peerId, int rpcId, google::protobuf::Message *msg, string *scratch = NULL);


  void Send(int peerId, int rpcId, const google::protobuf::Message &msg, string *scratch = NULL);

  // For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
  // here.
  void Broadcast(int rpcId, const google::protobuf::Message &msg);
private:
  MPI::Comm *mpiWorld;
};
}

#endif // UTIL_RPC_H
