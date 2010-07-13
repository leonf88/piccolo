#include "util/common.h"
#include "kernel/sparse-table.h"
#include "kernel/dense-table.h"

#include "util/static-initializers.h"
#include <gflags/gflags.h>

using std::tr1::unordered_map;
using namespace dsm;

int optimizer_hack;

DEFINE_int32(test_table_size, 100000, "");
#define START_TEST_PERF { Timer t; for (int i = 0; i < FLAGS_test_table_size; ++i) {

#define END_TEST_PERF(name)\
  }\
  fprintf(stderr, "%s: %d ops in %.3f seconds; %.0f/s %.0f cycles\n",\
          #name, FLAGS_test_table_size, t.elapsed(), t.rate(FLAGS_test_table_size), t.cycle_rate(FLAGS_test_table_size)); }

#define TEST_PERF(name, op)\
    START_TEST_PERF \
    op; \
    END_TEST_PERF(name)

namespace {
struct MapTestRGB {
  uint16_t r;
  uint16_t g;
  uint16_t b;
};

static void TestSparseTable() {
  TableDescriptor td;
  td.accum = new Accumulators<int>::Replace();

  SparseTable<tuple2<int, int>, int> h(1);
  h.Init(&td);

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      h.put(MP(i, j), 1);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(h.contains(MP(i, j)));
      CHECK_EQ(h.get(MP(i, j)), 1);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(h.contains(MP(i, j)));
      h.update(MP(i, j), 2);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(h.contains(MP(i, j)));
      CHECK_EQ(h.get(MP(i, j)), 2);
    }
  }
}
REGISTER_TEST(SparseTable, TestSparseTable());

static void TestMapPerf() {
  TableDescriptor td;
  td.accum = new Accumulators<int>::Replace();

  TypedTable<int, double> *h = new SparseTable<int, double>(FLAGS_test_table_size * 2);
  ((SparseTable<int, double>*)h)->Init(&td);

//  DenseMap<int, double> d(FLAGS_test_table_size * 2);

  vector<double> array_test(FLAGS_test_table_size * 2);


  vector<int> source(FLAGS_test_table_size);
  for (int i = 0; i < source.size(); ++i) {
    source[i] = random() % FLAGS_test_table_size;
  }

  TEST_PERF(SparseTablePut, h->put(source[i], i));
  TEST_PERF(SparseTableGet, h->get(source[i]));

//  TEST_PERF(DenseMapPut, d.put(source[i], i));
//  TEST_PERF(DenseMapGet, d.get(source[i]));

  std::tr1::hash<int> hasher;
  TEST_PERF(ArrayPut, array_test[hasher(i) % FLAGS_test_table_size] = i);

  // Need to have some kind of side effect or this gets eliminated entirely.
  optimizer_hack = 0;
  TEST_PERF(ArrayPut, optimizer_hack += array_test[hasher(i) % FLAGS_test_table_size]);
}
REGISTER_TEST(SparseTablePerf, TestMapPerf());

}
