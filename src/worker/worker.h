#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "util/rpc.h"
#include "worker/worker.pb.h"
#include "worker/accumulator.h"

#include <boost/thread.hpp>
#include <mpi.h>

using boost::shared_ptr;

namespace upc {

class Worker : private boost::noncopyable {
public:
  Worker(const ConfigData &c);
  ~Worker();

  void Start();

  void KernelLoop();
  void NetworkLoop();

  struct Peer;

  SharedTable *CreateTable(ShardingFunction sf, HashFunction hf, AccumFunction af);
private:
  // The largest amount of data we'll send over the network as a single piece.
  static const int64_t kNetworkChunkSize = 1 << 20;
  static const int32_t kNetworkTimeout = 10;

  deque<LocalHash*> pendingWrites;

  boost::thread *kernelThread, *networkThread;
  MPI::Comm *world;
  RPCHelper *rpc;

  int64_t bytesIn, bytesOut;

  int numPeers;
  bool running;

  ConfigData config;

  // The status of other workers.
  vector<Peer*> peers;

  // Tables registered in the system.
  vector<PartitionedHash*> tables;

  // Network operations.
  void ProcessUpdates(Peer *p);
  void GetIncomingUpdates();
  void ComputeUpdates(Peer *p, LocalHash::Iterator *it);
  void SendAndReceive();

  int64_t pending_network_bytes() const;
  int64_t pending_kernel_bytes() const;
};

}

#endif /* WORKER_H_ */
