#include "kernel/local-table.h"

namespace dsm {

void LocalTable::write_delta(const TableData& req) {
  if (!delta_file_) {
    LOG_EVERY_N(ERROR, 100) << "Shard: " << this->info().shard << " is somehow missing it's delta file?";
  } else {
    delta_file_->write(req);
  }
}
}

using namespace dsm;
struct TableTestRGB { int r; int g; int b; };

static void TestLocalTable() {
  TableDescriptor td;
  td.accum = new Accumulators<TableTestRGB>::Replace;
  td.num_shards = 1;
  td.shard = 0;
  td.default_shard_size = 1;
  td.table_id = 0;
  td.key_marshal = td.value_marshal = td.sharder = NULL;
  vector<tuple2<int, int> > source;
  for (int i = 0; i < 500; ++i) {
    for (int j = 0; j < 500; ++j) {
      source.push_back(MP(i, j));
    }
  }

  TableTestRGB b = { 1, 2, 3 };


  TypedLocalTable<tuple2<int, int>, TableTestRGB> *h = new TypedLocalTable<tuple2<int, int>, TableTestRGB>;
  h->Init(td);
  h->resize(500000);

  for (int j = 0; j < 2500; ++j) {
    h->update(source[j], b);
  }

  Timer t;
  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 100; ++j) {
      h->update(source[j], b);
    }

    for (int j = 0; j < 1000; ++j) {
      h->update(source[j], b);
    }

    for (int j = 0; j < 1000; ++j) {
      TableTestRGB foo = h->get(source[j]);
      CHECK(memcmp(&foo, &b, sizeof(b)) == 0);
    }
  }
  LOG(INFO) << "Time: " << t.elapsed();
}
REGISTER_TEST(LocalTable, TestLocalTable());
