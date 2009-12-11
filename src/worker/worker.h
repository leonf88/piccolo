#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "util/rpc.h"
#include "worker/worker.pb.h"
#include "worker/table-internal.h"

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

  template <class K, class V>
  TypedTable<K, V>* CreateTable(typename TypedTable<K, V>::ShardingFunction sf,
                                typename TypedTable<K, V>::AccumFunction af) {
    TableInfo tinfo;
    tinfo.af = (void*)af;
    tinfo.sf = (void*)sf;
    tinfo.num_threads = config.num_workers();
    tinfo.table_id = tables.size();
    tinfo.rpc = new RPCHelper(&world_);
    tinfo.owner_thread = config.worker_id();

    TypedPartitionedTable<K, V> *t = new TypedPartitionedTable<K, V>(tinfo);
    tables.push_back(t);
    return t;
  }

  Stats get_stats() { return stats_; }

private:
  // The largest amount of data we'll send over the network as a single piece.
  static const int64_t kNetworkChunkSize = 500 << 10;
  static const int32_t kNetworkTimeout = 20;

  deque<Table*> pending_writes_;
  deque<RunKernelRequest> kernel_requests_;

  boost::recursive_mutex kernel_lock_;

  boost::thread *kernel_thread_, *network_thread_;

  MPI::Intracomm world_;
  RPCHelper *rpc_;

  int num_peers_;
  bool running_;
  bool kernel_done_;

  ConfigData config;

  // The status of other workers.
  vector<Peer*> peers;

  // Tables registered in the system.
  vector<PartitionedTable*> tables;

  // Network operations.
  void ProcessUpdates(Peer *p);
  void ComputeUpdates(Peer *p, Table::Iterator *it);
  void Poll();

  bool pending_network_writes() const;
  int64_t pending_kernel_bytes() const;

  Stats stats_;
};

}

#endif /* WORKER_H_ */
