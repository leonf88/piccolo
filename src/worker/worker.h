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

  // Returns a local reference to table indicated by 'id'.  Writes to 'shard'
  // are performed locally; other writes will be forwarded to the appropriate
  // foreign table.
  Table* get_table(int id, int shard) {
    if (tables[make_pair(id, shard)]) {
      return tables[make_pair(id, shard)];
    }

    Table *t = TableRegistry::get_instance(id, shard);
    t->info_.rpc = rpc_;
    tables[make_pair(id, shard)] = t;
    return t;
  }

  // Convenience wrapper for get_table.
  template <class K, class V>
  TypedTable<K, V>* get_typed_table(int id, int shard) {
    return (TypedTable<K, V>*)get_table(id, shard);
  }

  int peer_for_shard(int table_id, int shard);

  Stats get_stats() {
    return stats_;
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
  vector<Peer*> peers;

  // Tables registered in the system.
  typedef unordered_map<pair<int, int>, Table*> TableMap;
  TableMap tables;

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
