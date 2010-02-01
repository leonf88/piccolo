#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include "util/coder.h"
#include <mpi.h>
#include <google/protobuf/message.h>

namespace dsm {

class RPCHelper;

class RPCMessage {
private:
public:
  virtual void AppendToCoder(Encoder *e) const = 0;
  virtual void ParseFromCoder(Decoder *d) = 0;

	void ParseFromString(const string &s) {
	  Clear();
	  Decoder d(s);
	  ParseFromCoder(&d);
	}

	void AppendToString(string *s) const {
	  Encoder e(s);
	  AppendToCoder(&e);
	}

	virtual void Clear() = 0;
};

class RPCHelper {
public:
  typedef google::protobuf::Message Message;

  RPCHelper(MPI::Comm *mpi) :
    mpi_world_(mpi), my_rank_(mpi->Get_rank()) {
  }

  // Try to read a message from the given peer and rpc channel; return false if no
  // message is immediately available.
  bool TryRead(int peerId, int rpcId, RPCMessage *msg);
  bool HasData(int peerId, int rpcId);
  bool HasData(int peerId, int rpcId, MPI::Status &status);

  int Read(int peerId, int rpcId, RPCMessage *msg);
  int ReadAny(int *peerId, int rpcId, RPCMessage *msg);
  void Send(int peerId, int rpcId, const RPCMessage &msg);
  void SyncSend(int peerId, int rpcId, const RPCMessage &msg);

  MPI::Request SendData(int peer_id, int rpc_id, const string& data);

  // For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
  // here.
  void Broadcast(int rpcId, const RPCMessage &msg);
  void SyncBroadcast(int rpcId, const RPCMessage &msg);


  // Simple wrapper to allow protocol buffers to be sent through the RPC system.
  class ProtoWrapper : public RPCMessage {
  typedef google::protobuf::Message Message;
  private:
    Message *p_;
  public:
    ProtoWrapper(Message* p) : p_(p) {}

    void Clear() { p_->Clear(); }
    void AppendToCoder(Encoder *e) const;
    void ParseFromCoder(Decoder *d);
  };

#define WRAP(f, p, r, msg)\
    ProtoWrapper w(msg);\
    return f(p, r, &w);\

  bool TryRead(int peerId, int rpcId, Message* msg) { WRAP(TryRead, peerId, rpcId, msg); }
  bool Read(int peerId, int rpcId, Message* msg) { WRAP(Read, peerId, rpcId, msg); }
  bool ReadAny(int *peerId, int rpcId, Message* msg) { WRAP(ReadAny, peerId, rpcId, msg); }
#undef WRAP

  void Send(int peerId, int rpcId, const Message& msg) {
    ProtoWrapper w((Message*)&msg);
    Send(peerId, rpcId, w);
  }

  void SyncSend(int peerId, int rpcId, const Message& msg) {
    ProtoWrapper w((Message*)&msg);
    SyncSend(peerId, rpcId, w);
  }

  void Broadcast(int rpcId, const Message& msg) {
    ProtoWrapper w((Message*)&msg);
    Broadcast(rpcId, w);
  }

  void SyncBroadcast(int rpcId, const Message& msg) {
      ProtoWrapper w((Message*)&msg);
      SyncBroadcast(rpcId, w);
    }


private:
  boost::recursive_mutex mpi_lock_;

  MPI::Comm *mpi_world_;
  int my_rank_;
};
}

#endif // UTIL_RPC_H
