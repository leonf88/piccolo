#ifndef WORKER_H_
#define WORKER_H_

#include "util/common.h"
#include "worker/kernel.h"
#include "worker/worker.h"
#include "worker/worker.pb.h"
#include "worker/accumulator.h"

#include <boost/thread.hpp>
#include <mpi.h>

using boost::shared_ptr;

namespace asyncgraph {

class Worker : private boost::noncopyable, public KernelRunner {
public:
  // The largest amount of data we'll send over the network as a single piece.
  static const int64_t kNetworkChunkSize = 1 << 20;
  static const int32_t kNetworkTimeout = 10;
  static const int32_t kMaxPeers = 8192;
private:

  Kernel *kernel;

  // New accumulators are created by the network thread when sending out updates,
  // and by the kernel thread when a new iteration starts.  An accumulator cannot
  // span 2 iterations; this allows the network thread to determine when it has
  // sent the complete data for an iteration.
  bool volatile accumWriting[kMaxPeers];

  vector<Accumulator*> activeAccum;

  deque<Accumulator*> pendingWrites;
  mutable boost::recursive_mutex pendingLock;

  boost::thread *kernelThread, *networkThread;
  MPI::Comm *world;

  int64_t bytesIn, bytesOut;

  // The current iteration being computed and distributed, respectively.
  int32_t kernelIteration;

  int numPeers;
  bool running;

  ConfigData config;

  struct Peer;

  // The status of other workers.
  vector<Peer*> peers;

  void processUpdates();

  void computeUpdates(Peer *p, Accumulator::Iterator* i, bool final);
  void getIncomingUpdates();
  void sendAndReceive();

  int64_t pendingNetworkBytes() const;
  int64_t pendingKernelBytes() const;

  void pushAccumulators();
  bool popAccumulators(deque<Accumulator*>* out);
public:
  Worker(const ConfigData &c);
  ~Worker();
  
  void status(WorkerStatus* r);
  void kernelLoop();
  void networkLoop();

  void output(int shard, const StringPiece& k, const StringPiece &v);

  Kernel* getKernel() { return kernel; }
  void wait();
};

}

#endif /* WORKER_H_ */
