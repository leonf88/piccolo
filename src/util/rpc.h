#ifndef UTIL_RPC_H
#define UTIL_RPC_H

#include "util/common.h"
#include "util/file.h"
#include "util/common.pb.h"

#include <boost/thread.hpp>
#include <google/protobuf/message.h>
#include <mpi.h>

namespace dsm {

typedef google::protobuf::Message Message;

struct RPCRequest;

// XXX
//stats_.set_bytes_out(stats_.bytes_out() + p->payload.size());
//stats_.set_put_out(stats_.put_out() + 1);

// Hackery to get around mpi's unhappiness with threads.  This thread
// simply polls MPI continuously for any kind of update and adds it to
// a local queue.
class NetworkThread {
public:
  bool active() const;
  int64_t pending_bytes() const;
  
  // Blocking read for the given source and message type.
  void Read(int desired_src, int type, Message* data, int *source=NULL);
  bool TryRead(int desired_src, int type, Message* data, int *source=NULL);

  // Enqueue the given request for transmission.
  void Send(RPCRequest *req);
  void Send(int dst, int method, const Message &msg);

  void Broadcast(int method, const Message& msg);
  void SyncBroadcast(int method, int reply, const Message& msg);
  void WaitForSync(int method, int count);

  void Flush();
  void Shutdown();

  int id() { return id_; }
  int size() const;

  static NetworkThread *Get();

  Stats stats;
private:
  static const int kMaxHosts = 512;
  static const int kMaxMethods = 32;

  typedef deque<string> Queue;

  bool running;

  vector<RPCRequest*> pending_sends_;
  unordered_set<RPCRequest*> active_sends_;

  Queue incoming[kMaxMethods][kMaxHosts];

  MPI::Comm *world_;
  mutable boost::recursive_mutex send_lock;
  mutable boost::recursive_mutex q_lock[kMaxHosts];
  mutable boost::thread *t_;
  int id_;

  bool check_queue(int src, int type, Message* data);

  void CollectActive();
  void Run();

  NetworkThread();
};


}

#endif // UTIL_RPC_H
