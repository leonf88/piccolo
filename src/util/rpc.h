#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include "util/coder.h"
#include <mpi.h>
#include <google/protobuf/message.h>

namespace upc {

class RPCHelper;

class RPCMessage {
private:
public:
  virtual void AppendToCoder(Encoder *e) const = 0;
  virtual void ParseFromCoder(Decoder *d) = 0;

	void ParseFromString(const string &s) { Clear(); Decoder d(s); ParseFromCoder(&d); }
	void AppendToString(string *s) const { Encoder e(s); AppendToCoder(&e); }

	virtual void Clear() = 0;
};


// Simple wrapper to allow protocol buffers to be sent through the RPC system.
class ProtoWrapper : public RPCMessage {
private:
  google::protobuf::Message *p_;
public:
  ProtoWrapper(const google::protobuf::Message& p) : p_((google::protobuf::Message*)&p) {}

  void Clear() { p_->Clear(); }
  void AppendToCoder(Encoder *e) const;
  void ParseFromCoder(Decoder *d);
};

class RPCHelper {
public:
  typedef google::protobuf::Message Message;

  RPCHelper(MPI::Comm *mpi) :
    mpiWorld(mpi) {
  }

  // Try to read a message from the given peer and rpc channel.  Return
  // the number of bytes read, 0 if no message was available.
  int TryRead(int peerId, int rpcId, RPCMessage *msg);
  int Read(int peerId, int rpcId, RPCMessage *msg);
  int ReadAny(int *peerId, int rpcId, RPCMessage *msg);
  void Send(int peerId, int rpcId, const RPCMessage &msg);

  // For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
  // here.
  void Broadcast(int rpcId, const RPCMessage &msg);
private:
  MPI::Comm *mpiWorld;
};
}

#endif // UTIL_RPC_H
