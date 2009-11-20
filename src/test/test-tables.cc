#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

namespace upc {
static TypedTable<int, double>* min_hash;
static TypedTable<int, double>* max_hash;
static TypedTable<int, double>* sum_hash;
static TypedTable<int, double>* replace_hash;

void TestPut() {
  for (int i = 0; i < 100; ++i) {
    min_hash->put(i, i);
    max_hash->put(i, i);
    sum_hash->put(i, i);
    replace_hash->put(i, i);
  }
}
REGISTER_KERNEL(TestPut);

void TestGet() {
  int num_threads = min_hash->info().num_threads;
  for (int i = 0; i < 100; ++i) {
    CHECK_EQ((int)min_hash->get(i), i);
    CHECK_EQ((int)max_hash->get(i), i);
    CHECK_EQ((int)replace_hash->get(i), i);
    CHECK_EQ((int)sum_hash->get(i), i * num_threads);
  }
}
REGISTER_KERNEL(TestGet);

void TestGetLocal() {
  TypedTable<int, double>::Iterator *it = min_hash->get_typed_iterator();
  int num_threads = min_hash->info().num_threads;

  while (!it->done()) {
    const int& k = it->key();
    CHECK_EQ((int)min_hash->get(k), k);
    CHECK_EQ((int)max_hash->get(k), k);
    CHECK_EQ((int)replace_hash->get(k), k);
    CHECK_EQ((int)sum_hash->get(k), k * num_threads);
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
    m.run_all(&TestPut);
    m.run_all(&TestGetLocal);
    m.run_all(&TestGet);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    min_hash = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::min);
    max_hash = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::max);
    sum_hash = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::sum);
    replace_hash = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::replace);
    w.Run();
  }

  LOG(INFO) << "Exiting.";
}

