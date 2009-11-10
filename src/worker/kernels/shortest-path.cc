#include "util/common.h"
#include "util/file.h"
#include "worker/kernels/graph.h"
#include "worker/worker.pb.h"


namespace asyncgraph {
class ShortestPathKernel : public GraphKernel<ShortestPathKernel, PathNode, int32_t, int32_t> {
public:
  Accumulator* newAccumulator() {
    return new MinAccumulator<int32_t, int32_t>;
  }

  void init(const ConfigData& conf) {
    Base::init(conf);
  }

  void startIteration() {
    distanceCounts.clear();
    dirty = updates = 0;
  }

  void finishIteration() {
    for (map<int, int>::iterator i = distanceCounts.begin(); i != distanceCounts.end(); ++i) {
      LOG(INFO) << "Distance: " << i->first << " count " << i->second;
    }

    LOG(INFO) << "Sent " << updates << " updates from " << dirty << " nodes.";
  }

  void handlePropagate(PathNode& n) {
    distanceCounts[n.distance()]++;

    if (n.dirty()) {
      n.set_dirty(false);
      ++dirty;

      for (int j = 0; j < n.target_size(); ++j) {
        ++updates;
        output(n.target(j), n.distance() + 1);
      }
    }
  }

  void handleUpdate(const int32_t& k, const int32_t& v) {
    int32_t id = k;
    int32_t dist = v;

    LOG_EVERY_N(INFO, 10000) << "Updating..." << id << " with distance " << dist;

    if (localNodes[id]->distance() > dist) {
      localNodes[id]->set_distance(dist);
      localNodes[id]->set_dirty(true);
    }
  }
private:
  map<int, int> distanceCounts;
  int dirty, updates;
};
REGISTER_KERNEL(ShortestPathKernel);

}
