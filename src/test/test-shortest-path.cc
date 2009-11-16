#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

namespace upc {
static SharedTable* distanceHash;

void RunSPIteration() {
  LOG(INFO) << "Running... " << distanceHash;
  for (int i = 0; i < 1000; ++i) {
    distanceHash->put(StringPrintf("key.%d", i), StringPiece((char*)new int(i + 10000), sizeof(i)));
  }

  for (int i = 0; i < 1000; ++i) {
    distanceHash->put(StringPrintf("key.%d", i), StringPrintf("v: %d", i));
    StringPiece a = distanceHash->get(StringPrintf("key.%d", i));
    LOG(INFO) << "v : " << a.AsString();
  }
}
REGISTER_KERNEL(RunSPIteration);
}

using namespace upc;
int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    m.run(&RunSPIteration);
  } else {
    Worker w(conf);
    distanceHash = w.CreateTable(&ShardStr, &HashStr, &AccumMin);
    w.Run();
  }
}

