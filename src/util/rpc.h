#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include "util/file.h"
#include <boost/thread.hpp>
#include <google/protobuf/message.h>
#include <mpi.h>

namespace dsm {

typedef google::protobuf::Message Message;

class RPCHelper {
public:
  // Try to read a message from the given peer and rpc channel = 0; return false if no
  // message is immediately available.
  virtual bool TryRead(int target, int method, Message *msg) = 0;
  virtual bool HasData(int target, int method) = 0;
  virtual bool HasData(int target, int method, MPI::Status &status) = 0;

  virtual int Read(int src, int method, Message *msg) = 0;
  virtual int ReadAny(int *src, int method, Message *msg) = 0;
  virtual int ReadBytes(int desired_src, int method, string* data, int *actual_src) = 0;
  virtual void Send(int target, int method, const Message &msg) = 0;
  virtual void SyncSend(int target, int method, const Message &msg) = 0;

  virtual void SendData(int peer_id, int rpc_id, const string& data) = 0;
  virtual MPI::Request ISendData(int peer_id, int rpc_id, const string& data) = 0;

  virtual void Broadcast(int method, const Message &msg) = 0;
  virtual void SyncBroadcast(int method, const Message &msg) = 0;
};

RPCHelper *get_rpc_helper();

}

#endif // UTIL_RPC_H
