#include "util/common.h"
#include "kernel/sparse-map.h"
#include "kernel/dense-map.h"

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

static void TestSparseMapRGB() {
   SparseMap<tuple2<int, int>, MapTestRGB> h;
   vector<tuple2<int, int> > source;
   for (int i = 0; i < 50; ++i) {
     for (int j = 0; j < 50; ++j) {
       source.push_back(MP(i, j));
     }
   }

   MapTestRGB b = { 1, 2, 3 };
   TEST_PERF(SparseMapPutRGB, h.put(source[i % source.size()], b));

   for (int rep = 0; rep < 10; ++rep) {
     START_TEST_PERF
     SparseMap<tuple2<int, int>, MapTestRGB>::iterator it = h.find(source[i % source.size()]);
     it->second = b;
     END_TEST_PERF(SparseMapFindRGB);

     TEST_PERF(SparseMapGetRGB, h.get(source[i % source.size()]));
   }
}
REGISTER_TEST(SparseMapRGB, TestSparseMapRGB());

static void TestSparseMap() {
  SparseMap<pair<int, int>, int> h(1);
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      h.put(make_pair(i, j), 1);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(h.contains(make_pair(i, j)));
      CHECK_EQ(h.get(make_pair(i, j)), 1);
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(h.find(make_pair(i, j)) != h.end());
      h.find(make_pair(i, j))->second = 2;
    }
  }

  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      CHECK(h.contains(make_pair(i, j)));
      CHECK_EQ(h.get(make_pair(i, j)), 2);
    }
  }
}
REGISTER_TEST(SparseMap, TestSparseMap());

static void TestMapPerf() {
  SparseMap<int, double> h(FLAGS_test_table_size * 2);
  DenseMap<int, double> d(FLAGS_test_table_size * 2);

  unordered_map<int, double> umap(FLAGS_test_table_size * 2);
  vector<double> array_test(FLAGS_test_table_size * 2);


  vector<int> source(FLAGS_test_table_size);
  for (int i = 0; i < source.size(); ++i) {
    source[i] = random() % FLAGS_test_table_size;
  }

  TEST_PERF(SparseMapPut, h.put(source[i], i));
  TEST_PERF(SparseMapGet, h.get(source[i]));

  TEST_PERF(DenseMapPut, d.put(source[i], i));
  TEST_PERF(DenseMapGet, d.get(source[i]));

  TEST_PERF(STLHashPut, umap.insert(make_pair(source[i], i)));
  TEST_PERF(STLHashGet, umap.find(source[i]));

  std::tr1::hash<int> hasher;
  TEST_PERF(ArrayPut, array_test[hasher(i) % FLAGS_test_table_size] = i);

  // Need to have some kind of side effect or this gets eliminated entirely.
  optimizer_hack = 0;
  TEST_PERF(ArrayPut, optimizer_hack += array_test[hasher(i) % FLAGS_test_table_size]);
}
REGISTER_TEST(SparseMapPerf, TestMapPerf());

}
