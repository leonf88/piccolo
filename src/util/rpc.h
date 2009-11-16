#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include <mpi.h>
#include <google/protobuf/message.h>

namespace upc {

class RPCHelper;

class RPCHelper {
public:
  typedef google::protobuf::Message Message;

  RPCHelper(MPI::Comm *mpi) :
    mpiWorld(mpi) {
  }

  // Try to read a message from the given peer and rpc channel.  Return
  // the number of bytes read, 0 if no message was available.
  int TryRead(int peerId, int rpcId, Message *msg);
  int Read(int peerId, int rpcId, Message *msg);
  int ReadAny(int *peerId, int rpcId, Message *msg);
  void Send(int peerId, int rpcId, const Message &msg);

  void Invoke(int peerId, const Message& req, Message *resp);

  // For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
  // here.
  void Broadcast(int rpcId, const Message &msg);
private:
  MPI::Comm *mpiWorld;
  string scratch;
};
}

#endif // UTIL_RPC_H
