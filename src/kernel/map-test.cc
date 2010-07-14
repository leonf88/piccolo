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

static void TestTable(TypedTable<int, int> *t) {
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      t->put(10 * i + j, 1);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(t->contains(10 * i + j));
      CHECK_EQ(t->get(10 * i + j), 1);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(t->contains(10 * i + j));
      t->update(10 * i + j, 2);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(t->contains(10 * i + j));
      CHECK_EQ(t->get(10 * i + j), 2);
    }
  }
}

static void TestDenseTable() {
  TableDescriptor td;
  td.accum = new Accumulators<int>::Replace();

  DenseTable<int, int> *t = new DenseTable<int, int>();
  t->Init(&td);
  TestTable(t);
}

static void TestSparseTable() {
  TableDescriptor td;
  td.accum = new Accumulators<int>::Replace();

  SparseTable<int, int> *t = new SparseTable<int, int>();
  t->Init(&td);
  TestTable(t);
}

REGISTER_TEST(DenseTable, TestDenseTable());
REGISTER_TEST(SparseTable, TestSparseTable());

static void TestMapPerf() {
  TableDescriptor td;
  td.accum = new Accumulators<int>::Replace();

  SparseTable<int, double> *h = new SparseTable<int, double>(FLAGS_test_table_size * 2);
  h->Init(&td);

  DenseTable<int, double> d(FLAGS_test_table_size * 2);
  d.Init(&td);

  vector<double> array_test(FLAGS_test_table_size * 2);


  vector<int> source(FLAGS_test_table_size);
  for (int i = 0; i < source.size(); ++i) {
    source[i] = random() % FLAGS_test_table_size;
  }

  TEST_PERF(SparseTablePut, h->put(source[i], i));
  TEST_PERF(SparseTableGet, h->get(source[i]));

  TEST_PERF(DenseTablePut, d.put(source[i], i));
  TEST_PERF(DenseTableGet, d.get(source[i]));

  std::tr1::hash<int> hasher;
  TEST_PERF(ArrayPut, array_test[hasher(i) % FLAGS_test_table_size] = i);

  // Need to have some kind of side effect or this gets eliminated entirely.
  optimizer_hack = 0;
  TEST_PERF(ArrayPut, optimizer_hack += array_test[hasher(i) % FLAGS_test_table_size]);
}
REGISTER_TEST(SparseTablePerf, TestMapPerf());

}
