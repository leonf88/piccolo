#include "client.h"

using namespace dsm;


DEFINE_int32(table_size, 100000, "");
DEFINE_int32(shards, 10, "");

static TypedGlobalTable<int, int>* min_hash = NULL;
static TypedGlobalTable<int, int>* max_hash = NULL;
static TypedGlobalTable<int, int>* sum_hash = NULL;
static TypedGlobalTable<int, int>* replace_hash = NULL;
static TypedGlobalTable<int, string>* string_hash = NULL;

//static TypedGlobalTable<int, Pair>* pair_hash = NULL;

class TableKernel : public DSMKernel {
public:
  void TestPut() {
    Pair p;
    for (int i = 0; i < FLAGS_table_size; ++i) {
      LOG_EVERY_N(INFO, 100000) << "Writing... " << LOG_OCCURRENCES;
      min_hash->put(i, i);
      max_hash->put(i, i);
      sum_hash->put(i, 1);
      replace_hash->put(i, i);
      string_hash->put(i, StringPrintf("%d", i));
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
      CHECK_EQ(string_hash->get(i), StringPrintf("%d", i)) << " i= " << i;
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
      CHECK_EQ(string_hash->get(k), StringPrintf("%d", k)) << " i= " << k;
//      CHECK_EQ(pair_hash->get(k).value(), StringPrintf("%d", k));
      it->Next();
    }
  }

  void TestClear() {
    min_hash->clear(current_shard());
    max_hash->clear(current_shard());
    sum_hash->clear(current_shard());
    replace_hash->clear(current_shard());
    string_hash->clear(current_shard());
  }
};

REGISTER_KERNEL(TableKernel);
REGISTER_METHOD(TableKernel, TestPut);
REGISTER_METHOD(TableKernel, TestGet);
REGISTER_METHOD(TableKernel, TestGetLocal);
REGISTER_METHOD(TableKernel, TestClear);

int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_slots(FLAGS_shards * 2 / conf.num_workers());

  min_hash = Registry::create_table<int, int>(0, FLAGS_shards, &ModSharding, &Accumulator<int>::min);
  max_hash = Registry::create_table<int, int>(1, FLAGS_shards, &ModSharding, &Accumulator<int>::max);
  sum_hash = Registry::create_table<int, int>(2, FLAGS_shards, &ModSharding, &Accumulator<int>::sum);
  replace_hash = Registry::create_table<int, int>(3, FLAGS_shards, &ModSharding, &Accumulator<int>::replace);
  string_hash = Registry::create_table<int, string>(4, FLAGS_shards, &ModSharding, &Accumulator<string>::replace);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    m.run_all(Master::RunDescriptor::C("TableKernel", "TestPut", 0, 5));
    m.checkpoint();

    // wipe all the tables and then restore from the previous checkpoint.
    m.run_all(Master::RunDescriptor::C("TableKernel", "TestClear", 0, 0));
    m.restore();

    m.run_all(Master::RunDescriptor::C("TableKernel", "TestGetLocal", 0, 0));
    m.checkpoint();
    m.run_all(Master::RunDescriptor::C("TableKernel", "TestGet", 0, 0));
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    w.Run();
  }

  LOG(INFO) << "Exiting.";
}

