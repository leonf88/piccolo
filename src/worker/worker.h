#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "util/rpc.h"
#include "kernel/kernel-registry.h"
#include "kernel/table.h"
#include "worker/worker.pb.h"

#include <boost/thread.hpp>
#include <mpi.h>

using boost::shared_ptr;

namespace dsm {

class Worker : private boost::noncopyable {
struct Stub;
public:
  Worker(const ConfigData &c);
  ~Worker();

  void Run();

  void KernelLoop();
  void TableLoop();

  Stats get_stats() {
    return stats_;
  }

  // Non-blocking send and blocking read.
  void Send(int peer, int type, const Message& msg);
  void Read(int peer, int type, Message* msg);

  void CheckForMasterUpdates();

  // Returns true if any non-trivial operations were performed.
  bool CheckForWorkerUpdates();
  void CollectPending();

  void release_shard(GlobalTable *t, int shard);
  void acquire_shard(GlobalTable *t, int shard);

  int peer_for_shard(int table_id, int shard) const;
  int id() const { return config_.worker_id(); };
  int epoch() const { return epoch_; }

  int64_t pending_network_bytes() const;
  int64_t pending_kernel_bytes() const;
  bool network_idle() const;

private:
  void Checkpoint(int epoch);
  void Restore(int epoch);
  void UpdateEpoch(int peer, int peer_epoch);

  RecordFile *checkpoint_delta_;

  struct SendRequest;

  deque<KernelRequest> kernel_queue_;
  deque<KernelRequest> kernel_done_;
  unordered_set<SendRequest*> outgoing_requests_;

  boost::recursive_mutex state_lock_;
  boost::thread *table_thread_;

  MPI::Intracomm world_;
  RPCHelper *rpc_;

  // The current epoch this worker is running within.
  int epoch_;

  int num_peers_;
  bool running_;

  ConfigData config_;

  // The status of other workers.
  vector<Stub*> peers_;

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

  Stats stats_;
};

}

#endif /* WORKER_H_ */
