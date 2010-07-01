#include "util/common.h"
#include "util/hashmap.h"
#include "util/static-initializers.h"
#include <gflags/gflags.h>

using std::tr1::unordered_map;
using namespace dsm;

int optimizer_hack;

DEFINE_int32(test_table_size, 100000, "");
#define TEST_PERF(name, op)\
{\
  Timer t;\
  for (int i = 0; i < FLAGS_test_table_size; ++i) {\
    op;\
  }\
  fprintf(stderr, "%s: %d inserts in %.3f seconds; %.0f/s %.0f cycles\n",\
          #op, FLAGS_test_table_size, t.elapsed(), t.rate(FLAGS_test_table_size), t.cycle_rate(FLAGS_test_table_size));\
}

struct HashMapTestRGB {
  uint16_t r;
  uint16_t g;
  uint16_t b;
};

typedef HashMap<tuple2<int, int>, HashMapTestRGB>::iterator HIterator;

static void TestHashMapRGB() {
   HashMap<tuple2<int, int>, HashMapTestRGB> h;
   vector<tuple2<int, int> > source;
   for (int i = 0; i < 500; ++i) {
     for (int j = 0; j < 500; ++j) {
       source.push_back(MP(i, j));
     }
   }

   HashMapTestRGB b = { 1, 2, 3 };
   TEST_PERF(HashPut, h.put(source[i], b));

   for (int rep = 0; rep < 10; ++rep) {
     TEST_PERF(HashPut, { HIterator it = h.find(source[i]); it->second = b; } );
     TEST_PERF(HashPut, h.get(source[i]));
   }
}

static void TestHashMap() {
  HashMap<pair<int, int>, int> h(1);
  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 100; ++j) {
      h.put(make_pair(i, j), 1);
    }
  }

  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 100; ++j) {
      CHECK(h.contains(make_pair(i, j)));
      CHECK_EQ(h.get(make_pair(i, j)), 1);
    }
  }

  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 100; ++j) {
      CHECK(h.find(make_pair(i, j)) != h.end());
      h.find(make_pair(i, j))->second = 2;
    }
  }

  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 100; ++j) {
      CHECK(h.contains(make_pair(i, j)));
      CHECK_EQ(h.get(make_pair(i, j)), 2);
    }
  }
}

static void TestHashMapPerf() {
  HashMap<int, double> h(FLAGS_test_table_size * 2);
  unordered_map<int, double> umap(FLAGS_test_table_size * 2);
  vector<double> array_test(FLAGS_test_table_size * 2);

  vector<int> source(FLAGS_test_table_size);
  for (int i = 0; i < source.size(); ++i) {
    source[i] = random() % FLAGS_test_table_size;
  }

  TEST_PERF(HashPut, h.put(source[i], i));
  TEST_PERF(HashGet, h.get(source[i]));
  TEST_PERF(STLHashPut, umap[source[i]] = i);
  TEST_PERF(ArrayPut, array_test[source[i]] = i);

  std::tr1::hash<int> hasher;
  TEST_PERF(ArrayPut, array_test[hasher(i) % FLAGS_test_table_size] = i);

  // Need to have some kind of side effect or this gets eliminated entirely.
  optimizer_hack = 0;
  TEST_PERF(ArrayPut, optimizer_hack += array_test[hasher(i) % FLAGS_test_table_size]);
}
REGISTER_TEST(HashMap, TestHashMap());
REGISTER_TEST(HashMapPerf, TestHashMapPerf());
REGISTER_TEST(HashMapRGB, TestHashMapRGB());
