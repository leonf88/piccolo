#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"
using namespace dsm;


DEFINE_int32(table_size, 100000, "");

static TypedGlobalTable<int, int>* min_hash = NULL;
static TypedGlobalTable<int, int>* max_hash = NULL;
static TypedGlobalTable<int, int>* sum_hash = NULL;
static TypedGlobalTable<int, int>* replace_hash = NULL;
//static TypedGlobalTable<int, Pair>* pair_hash = NULL;

class TableKernel : public DSMKernel {
public:
  void TestPut() {
    LOG(INFO) << "Here: " << current_shard() << " : " << min_hash->num_shards();
    Pair p;
    for (int i = 0; i < FLAGS_table_size; ++i) {
      min_hash->put(i, i);
      max_hash->put(i, i);
      sum_hash->put(i, 1);
      replace_hash->put(i, i);
//      p.set_key(StringPrintf("%d", i));
//      p.set_value(StringPrintf("%d", i));
//      pair_hash->put(i, p);
    }
  }

  void TestGet() {
    int num_shards = min_hash->num_shards();
    for (int i = 0; i < FLAGS_table_size; ++i) {
      PERIODIC(1, LOG(INFO) << "Fetching... " << i; );
      CHECK_EQ(min_hash->get(i), i) << " i= " << i;
      CHECK_EQ(max_hash->get(i), i) << " i= " << i;
      CHECK_EQ(replace_hash->get(i), i) << " i= " << i;
      CHECK_EQ(sum_hash->get(i), num_shards) << " i= " << i;
//      CHECK_EQ(pair_hash->get(i).value(), StringPrintf("%d", i));
    }
  }

  void TestGetLocal() {
    TypedTable<int, int>::Iterator *it = min_hash->get_typed_iterator(current_shard());
    int num_shards = min_hash->num_shards();

    while (!it->done()) {
      const int& k = it->key();
      CHECK_EQ(min_hash->get(k), k) << " k= " << k;
      CHECK_EQ(max_hash->get(k), k) << " k= " << k;
      CHECK_EQ(replace_hash->get(k), k) << " k= " << k;
      CHECK_EQ(sum_hash->get(k), num_shards) << " k= " << k;
//      CHECK_EQ(pair_hash->get(k).value(), StringPrintf("%d", k));
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

  min_hash = Registry::create_table<int, int>(0, 10, &ModSharding, &Accumulator<int>::min);
  max_hash = Registry::create_table<int, int>(1, 10, &ModSharding, &Accumulator<int>::max);
  sum_hash = Registry::create_table<int, int>(2, 10, &ModSharding, &Accumulator<int>::sum);
  replace_hash = Registry::create_table<int, int>(3, 10, &ModSharding, &Accumulator<int>::replace);
//  pair_hash = Registry::create_table<int, Pair>(4, 10, &ModSharding, &Accumulator<Pair>::replace);


  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, TableKernel, TestPut, 0);
    RUN_ALL(m, TableKernel, TestGetLocal, 0);
    RUN_ALL(m, TableKernel, TestGet, 0);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    w.Run();
  }

  LOG(INFO) << "Exiting.";
}

