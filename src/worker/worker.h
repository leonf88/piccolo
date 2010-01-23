#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "util/rpc.h"
#include "worker/registry.h"
#include "worker/worker.pb.h"
#include "worker/table-internal.h"

#include <boost/thread.hpp>
#include <mpi.h>

using boost::shared_ptr;

namespace dsm {

class Worker : private boost::noncopyable {
public:
  Worker(const ConfigData &c);
  ~Worker();

  void Run();

  void KernelLoop();
  void NetworkLoop();

  struct Peer;

  int peer_for_shard(int table_id, int shard) {
    return shard % config.num_workers();
  }

  Stats get_stats() {
    return stats_;
  }
private:
  // The largest amount of data we'll send over the network as a single piece.
  static const int64_t kNetworkChunkSize = 500 << 10;
  static const int32_t kNetworkTimeout = 20;

  deque<Table*> pending_writes_;
  deque<KernelRequest> kernel_requests_;
  deque<KernelRequest> kernel_done_;

  boost::recursive_mutex kernel_lock_;

  boost::thread *kernel_thread_, *network_thread_;

  MPI::Intracomm world_;
  RPCHelper *rpc_;

  int num_peers_;
  bool running_;

  ConfigData config;

  // The status of other workers.
  vector<Peer*> peers_;

  struct KernelId {
    string kname_;
    int table_;
    int shard_;

    KernelId(string kname, int table, int shard) :
      kname_(kname), table_(table), shard_(shard) {}

#define CMP_LESS(a, b, member)\
  if ((a).member < (b).member) { return true; }\
  if ((b).member < (a).member) { return false; }

    bool operator<(const KernelId& o) const {
      CMP_LESS(*this, o, kname_);
      CMP_LESS(*this, o, table_);
      CMP_LESS(*this, o, shard_);
      return false;
    }
  };

  map<KernelId, DSMKernel*> kernels_;

  // Network operations.
  void ProcessUpdates(Peer *p);
  void ComputeUpdates(Peer *p, Table::Iterator *it);
  void Poll();

  int64_t pending_network_bytes() const;
  int64_t pending_kernel_bytes() const;

  Stats stats_;
};

}

#endif /* WORKER_H_ */
