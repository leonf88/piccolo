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
  TypedTable<K, V>* create_table(int id,
                                 int shards,
                                 typename TypedTable<K, V>::ShardingFunction sharding=&ModSharding,
                                 typename TypedTable<K, V>::AccumFunction accum=&Accumulator<V>::replace) {
    TableInfo info;
    info.num_shards = shards;
    info.accum_function = (void*)accum;
    info.sharding_function = (void*)sharding;

    return this->create_table<K, V>(id, info);
  }

  // Creates the table indicated by 'id' and registers it for use on this node.
  template <class K, class V>
  TypedTable<K, V>* create_table(int id, TableInfo info) {
    if (tables_.find(id) != tables_.end()) {
      LOG(FATAL) << "Requested duplicate table with id: " << id;
    }

    info.table_id = id;
    info.rpc = rpc_;

    TypedTable<K, V> *t = new TypedPartitionedTable<K, V>(info);
    tables_[id] = t;

    return t;
  }

  int peer_for_shard(int table_id, int shard) {
    return shard % config.num_workers();
  }

  Stats get_stats() {
    return stats_;
  }

  Table* get_table(int id) {
    if (tables_.find(id) == tables_.end()) {
      LOG(FATAL) << "Invalid table: " << id;
    }

    return tables_[id];
  }

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
  vector<Peer*> peers_;

  // Tables registered in the system.
  typedef unordered_map<int, Table*> TableMap;
  TableMap tables_;

  map<string, DSMKernel*> kernels_;

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
