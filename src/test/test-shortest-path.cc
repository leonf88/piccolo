#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "worker/kernel.h"
#include "worker/worker.pb.h"

namespace upc {
void BuildTestGraph(int nodes, int density) {
  RecordFile out("nodes.rec", "w");

  for (int i = 0; i < nodes; ++i) {
    PathNode n;
    n.set_id(i);
    n.set_distance(i == 0 ? 0 : 10000000);
    n.set_dirty(i == 0);

    for (int j = 0; j < density; ++j) {
      n.add_target(random() % nodes);
    }

    out.write(n);
    LOG_EVERY_N(INFO, 10000) << "Working; created " << i << " nodes.";
  }
}

void RunSPIteration() {

}

REGISTER_KERNEL(RunSPIteration);

}

using namespace upc;
int main(int argc, char **argv) {
  Init(argc, argv);
  BuildTestGraph(10000, 3);

  ConfigData conf;
  conf.set_kernel("ShortestPathKernel");
  conf.set_shard_prefix("nodes.rec");
  conf.set_num_workers(1);
  Worker w(conf);

  sleep(100);
}

