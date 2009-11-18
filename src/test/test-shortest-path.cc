#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

namespace upc {
static TypedTable<string, double>* min_hash;
static TypedTable<string, double>* max_hash;
static TypedTable<string, double>* sum_hash;

void TestPut() {
  for (int i = 0; i < 100; ++i) {
    min_hash->put(StringPrintf("key.%d", i), i);
    max_hash->put(StringPrintf("key.%d", i), i);
    sum_hash->put(StringPrintf("key.%d", i), i);
  }
}
REGISTER_KERNEL(TestPut);

void TestGet() {
  int my_thread = min_hash->get_typed_iterator()->owner()->info().owner_thread;
  for (int i = 0; i < 100; ++i) {
    LOG(INFO) << my_thread << " min_v : " << min_hash->get(StringPrintf("key.%d", i));
    LOG(INFO) << my_thread << " max_v : " << max_hash->get(StringPrintf("key.%d", i));
    LOG(INFO) << my_thread << " sum_v : " << sum_hash->get(StringPrintf("key.%d", i));
  }
}
REGISTER_KERNEL(TestGet);

void TestGetLocal() {
  TypedTableIterator<string, double> *it = min_hash->get_typed_iterator();
  TypedTable<string, double>* local = (TypedTable<string, double>*)it->owner();

  int my_thread = local->info().owner_thread;

  while (!it->done()) {
    const string& k = it->key();
    LOG(INFO) << my_thread << " min_v : " << k << " : " << min_hash->get(k);
    LOG(INFO) << my_thread << " max_v : " << k << " : " << max_hash->get(k);
    LOG(INFO) << my_thread << " sum_v : " << k << " : " << sum_hash->get(k);
    it->Next();
  }
}
REGISTER_KERNEL(TestGetLocal);
}

using namespace upc;
int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

  LOG(INFO) << Marshal<double>::from_string(Marshal<double>::to_string(10));

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    m.run(&TestPut);
    m.run(&TestGetLocal);
    m.run(&TestGet);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    min_hash = w.CreateTable<string, double>(&StringSharding, &Accumulator<double>::min);
    max_hash = w.CreateTable<string, double>(&StringSharding, &Accumulator<double>::max);
    sum_hash = w.CreateTable<string, double>(&StringSharding, &Accumulator<double>::sum);
    w.Run();
  }

  LOG(INFO) << "Exiting.";
}

