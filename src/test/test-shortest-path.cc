#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

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

static SharedTable* distanceHash;

void RunSPIteration() {
  LOG(INFO) << "Running... " << distanceHash;
  for (int i = 0; i < 1000; ++i) {
    distanceHash->put(StringPrintf("key.%d", i), StringPiece((char*)new int(i + 10000), sizeof(i)));
  }

  for (int i = 0; i < 1000; ++i) {
    distanceHash->put(StringPrintf("key.%d", i), StringPiece((char*)new int(i), sizeof(i)));
  }
}
REGISTER_KERNEL(RunSPIteration);

}

using namespace upc;
int main(int argc, char **argv) {
  Init(argc, argv);
//  BuildTestGraph(10000, 3);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    m.run(&RunSPIteration);
  } else {
    Worker w(conf);
    distanceHash = w.CreateTable(&ShardStr, &HashStr, &AccumMin);
    w.Start();
  }

  sleep(100);
}

