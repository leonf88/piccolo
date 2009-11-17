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

  void Run();

  void KernelLoop();
  void NetworkLoop();

  struct Peer;

  Table *CreateTable(ShardingFunction sf, HashFunction hf, AccumFunction af);
private:
  // The largest amount of data we'll send over the network as a single piece.
  static const int64_t kNetworkChunkSize = 1 << 20;
  static const int32_t kNetworkTimeout = 10;

  deque<LocalTable*> pending_writes_;

  boost::thread *kernel_thread_, *network_thread_;

  MPI::Intracomm worker_comm_;

  int num_peers_;
  bool running;

  ConfigData config;

  // The status of other workers.
  vector<Peer*> peers;

  // Tables registered in the system.
  vector<PartitionedTable*> tables;

  // Network operations.
  void ProcessUpdates(Peer *p);
  void GetIncomingUpdates();
  void ComputeUpdates(Peer *p, LocalTable::Iterator *it);
  void SendAndReceive();

  int64_t pending_network_bytes() const;
  int64_t pending_kernel_bytes() const;
};

}

#endif /* WORKER_H_ */
