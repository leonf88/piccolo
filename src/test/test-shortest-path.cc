#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

namespace upc {
static SharedTable* min_hash;
static SharedTable* max_hash;
static SharedTable* sum_hash;

void TestPut() {
  for (int i = 0; i < 100; ++i) {
    min_hash->put(StringPrintf("key.%d", i), double_as_str(i));
    max_hash->put(StringPrintf("key.%d", i), double_as_str(i));
    sum_hash->put(StringPrintf("key.%d", i), double_as_str(i));
  }
}
REGISTER_KERNEL(TestPut);

void TestGet() {
  for (int i = 0; i < 100; ++i) {
    LOG(INFO) << "min_v : " << str_as_double(min_hash->get(StringPrintf("key.%d", i)));
    LOG(INFO) << "max_v : " << str_as_double(max_hash->get(StringPrintf("key.%d", i)));
    LOG(INFO) << "sum_v : " << str_as_double(sum_hash->get(StringPrintf("key.%d", i)));
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
    min_hash = w.CreateTable(&ShardStr, &HashStr, &AccumMin);
    max_hash = w.CreateTable(&ShardStr, &HashStr, &AccumMax);
    sum_hash = w.CreateTable(&ShardStr, &HashStr, &AccumSum);
    w.Run();
  }
}

