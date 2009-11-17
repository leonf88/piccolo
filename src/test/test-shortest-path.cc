#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

namespace upc {
static Table* min_hash;
static Table* max_hash;
static Table* sum_hash;

void TestPut() {
  for (int i = 0; i < 100; ++i) {
    min_hash->put(StringPrintf("key.%d", i), double_as_str(i));
    max_hash->put(StringPrintf("key.%d", i), double_as_str(i));
    sum_hash->put(StringPrintf("key.%d", i), double_as_str(i));
  }
}
REGISTER_KERNEL(TestPut);

void TestGet() {
  int my_thread = min_hash->get_local()->info().owner_thread;
  for (int i = 0; i < 100; ++i) {
    LOG(INFO) << my_thread << " min_v : " << str_as_double(min_hash->get(StringPrintf("key.%d", i)));
    LOG(INFO) << my_thread << " max_v : " << str_as_double(max_hash->get(StringPrintf("key.%d", i)));
    LOG(INFO) << my_thread << " sum_v : " << str_as_double(sum_hash->get(StringPrintf("key.%d", i)));
  }
}
REGISTER_KERNEL(TestGet);

void TestGetLocal() {
  LocalTable *local = min_hash->get_local();
  LocalTable::Iterator *it = local->get_iterator();
  int my_thread = local->info().owner_thread;

  while (!it->done()) {
    const string& k = it->key();
    LOG(INFO) << my_thread << " min_v : " << k << " : " << str_as_double(min_hash->get(k));
    LOG(INFO) << my_thread << " max_v : " << k << " : " << str_as_double(max_hash->get(k));
    LOG(INFO) << my_thread << " sum_v : " << k << " : " << str_as_double(sum_hash->get(k));
    it->next();
  }
}
REGISTER_KERNEL(TestGetLocal);
}

using namespace upc;
int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    m.run(&TestPut);
    m.run(&TestGetLocal);
    m.run(&TestGet);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    min_hash = w.CreateTable(&ShardStr, &HashStr, &AccumMin);
    max_hash = w.CreateTable(&ShardStr, &HashStr, &AccumMax);
    sum_hash = w.CreateTable(&ShardStr, &HashStr, &AccumSum);
    w.Run();
  }

  LOG(INFO) << "Exiting.";
}

