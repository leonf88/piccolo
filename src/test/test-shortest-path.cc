#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

namespace upc {
static SharedTable* distanceHash;

void TestPut() {
  for (int i = 0; i < 100; ++i) {
    distanceHash->put(StringPrintf("key.%d", i), StringPrintf("v: %d", i));
  }
}
REGISTER_KERNEL(TestPut);

void TestGet() {
  for (int i = 0; i < 100; ++i) {
    string a = distanceHash->get(StringPrintf("key.%d", i));
    LOG(INFO) << "v : " << a;
  }
}
REGISTER_KERNEL(TestGet);

}

using namespace upc;
int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    m.run(&TestPut);
    m.run(&TestGet);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    distanceHash = w.CreateTable(&ShardStr, &HashStr, &AccumMin);
    w.Run();
  }
}

