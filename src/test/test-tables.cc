#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"
using namespace upc;


static TypedGlobalTable<int, double>* min_hash = NULL;
static TypedGlobalTable<int, double>* max_hash = NULL;
static TypedGlobalTable<int, double>* sum_hash = NULL;
static TypedGlobalTable<int, double>* replace_hash = NULL;
static TypedGlobalTable<int, Pair>* pair_hash = NULL;

class TableKernel : public DSMKernel {
public:
  void TestPut() {
    Pair p;
    for (int i = 0; i < 100; ++i) {
      min_hash->put(i, i);
      max_hash->put(i, i);
      sum_hash->put(i, i);
      replace_hash->put(i, i);
      p.set_key(StringPrintf("%d", i));
      p.set_value(StringPrintf("%d", i));
      pair_hash->put(i, p);
    }
  }

  void TestGet() {
    int num_threads = min_hash->info().num_shards;
    for (int i = 0; i < 100; ++i) {
      CHECK_EQ((int)min_hash->get(i), i);
      CHECK_EQ((int)max_hash->get(i), i);
      CHECK_EQ((int)replace_hash->get(i), i);
      CHECK_EQ((int)sum_hash->get(i), i * num_threads);
      CHECK_EQ(pair_hash->get(i).value(), StringPrintf("%d", i));
    }
  }

  void TestGetLocal() {
    TypedTable<int, double>::Iterator *it = min_hash->get_typed_iterator(shard());
    int num_threads = min_hash->info().num_shards;

    while (!it->done()) {
      const int& k = it->key();
      CHECK_EQ((int)min_hash->get(k), k);
      CHECK_EQ((int)max_hash->get(k), k);
      CHECK_EQ((int)replace_hash->get(k), k);
      CHECK_EQ((int)sum_hash->get(k), k * num_threads);
      CHECK_EQ(pair_hash->get(k).value(), StringPrintf("%d", k));
      it->Next();
    }
  }
};

REGISTER_KERNEL(TableKernel);
REGISTER_METHOD(TableKernel, TestPut);
REGISTER_METHOD(TableKernel, TestGet);
REGISTER_METHOD(TableKernel, TestGetLocal);

static void TestMarshalling() {
  ConfigData c;
  c.set_num_workers(10000);
  c.set_worker_id(0);
  c.set_master_id(0);

  LOG(INFO) << c.DebugString();

  string cdata = Data::to_string<ConfigData>(c);
  ConfigData c2 = Data::from_string<ConfigData>(cdata);

  CHECK_EQ(c2.DebugString(), c.DebugString());
}

int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

  TestMarshalling();



  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, TableKernel, TestPut, 0);
    RUN_ALL(m, TableKernel, TestGetLocal, 0);
    RUN_ALL(m, TableKernel, TestGet, 0);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    min_hash = w.create_table<int, double>(0, 10, &ModSharding, &Accumulator<double>::min);
    max_hash = w.create_table<int, double>(1, 10, &ModSharding, &Accumulator<double>::max);
    sum_hash = w.create_table<int, double>(2, 10, &ModSharding, &Accumulator<double>::sum);
    replace_hash = w.create_table<int, double>(3, 10, &ModSharding, &Accumulator<double>::replace);
    pair_hash = w.create_table<int, Pair>(4, 10, &ModSharding, &Accumulator<Pair>::replace);
    w.Run();
  }

  LOG(INFO) << "Exiting.";
}

