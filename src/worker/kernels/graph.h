#ifndef KERNEL_GRAPH_H_
#define KERNEL_GRAPH_H_

#include "util/common.h"
#include "util/file.h"
#include "worker/kernel.h"
#include "worker/worker.pb.h"
#include "worker/accumulator.h"

namespace asyncgraph {
template <class Derived, class NType, class KType, class VType>
class GraphKernel : public Kernel {
public:
  typedef GraphKernel<Derived, NType, KType, VType> Base;

  GraphKernel() : iteration(0) {}

  virtual ~GraphKernel() {
    for (int i = 0; i < localNodes.size(); ++i) {
      delete localNodes[i];
    }
  }

  void loadGraph(RecordFile& f) {
    do {
      NType *n = new NType;
      if (!f.read(n)) {
        delete n;
        break;
      }

      localNodes.push_back(n);
    } while (1);
  }

  virtual void init(const ConfigData& conf) {
    for (int i = 0; i < conf.num_shards(); ++i) {
      if (i % conf.num_workers() != conf.worker_id())
        continue;

      RecordFile f(conf.shard_prefix() + StringPrintf("%05d-of-%05d", i, conf.num_shards()), "r");

      LOG(INFO) << "Loading graph file " << f.name();
      static_cast<Derived*>(this)->loadGraph(f);
    }

    config.CopyFrom(conf);
    LOG(INFO) << "Graph kernel started with " << localNodes.size() << " local nodes.";
  }


  // Returns false if updates for this source should not be processed.
  bool startUpdates(const UpdateRequest& req) { return true; }
  void finishUpdates() {}
  void handleUpdates(const UpdateRequest& updates) {
    int count = 0;

    CastingIterator<KType, VType> c(updates);
    if (static_cast<Derived*>(this)->startUpdates(updates)) {
      while (!c.done()) {
        static_cast<Derived*>(this)->handleUpdate(c.key(), c.value());
        c.next();

        ++count;
      }
    }
    static_cast<Derived*>(this)->finishUpdates();
  }

  void startIteration() {}
  void finishIteration() {}
  void runIteration() {
    static_cast<Derived*>(this)->startIteration();

    for (int i = 0; i < localNodes.size(); ++i) {
      PERIODIC(30, LOG(INFO) << StringPrintf("Evaluating kernel %d / %zd...", i, localNodes.size()));
      NType &n = *localNodes[i];
      static_cast<Derived*>(this)->handlePropagate(n);
    }

    static_cast<Derived*>(this)->finishIteration();

    ++iteration;
  }

protected:
  int iteration;
  ConfigData config;
  vector<NType*> localNodes;
};
}

#endif /* KERNEL_GRAPH_H_ */
