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

struct FooBar {
  uint16_t r;
  uint16_t g;
  uint16_t b;
};

static void TestHashMap() {
  {
    HashMap<int, int> h(1);
    h.put(1, 1);
    h.put(2, 2);
    CHECK_EQ(h.get(1), 1);
    CHECK_EQ(h.get(2), 2);
  }
  HashMap<int, double> h(FLAGS_test_table_size * 2);
  unordered_map<int, double> umap(FLAGS_test_table_size * 2);
  vector<double> array_test(FLAGS_test_table_size * 2);

  {
    vector<int> source(FLAGS_test_table_size);
    for (int i = 0; i < source.size(); ++i) {
      source[i] = random() % FLAGS_test_table_size;
    }

    TEST_PERF(HashPut, h.put(source[i], i));
    TEST_PERF(HashReplace, h.put(source[i], i));
    TEST_PERF(HashGet, h.get(source[i]));
    TEST_PERF(STLHashPut, umap[source[i]] = i);
    TEST_PERF(ArrayPut, array_test[source[i]] = i);
  }

  {
    HashMap<tuple2<int, int>, FooBar> h;
    FooBar b = { 1, 2, 3 };
    vector<tuple2<int, int> > source;
    for (int i = 0; i < 500; ++i) {
      for (int j = 0; j < 500; ++j) {
        source.push_back(MP(i, j));
      }
    }

    TEST_PERF(HashPut, h.put(source[i], b));

    for (int rep = 0; rep < 10; ++rep) {
      TEST_PERF(HashPut, h.find(source[i]));
      TEST_PERF(HashPut, h.get(source[i]));
    }
  }


  std::tr1::hash<int> hasher;
  TEST_PERF(ArrayPut, array_test[hasher(i) % FLAGS_test_table_size] = i);

  optimizer_hack = 0;
  TEST_PERF(ArrayPut, optimizer_hack += array_test[hasher(i) % FLAGS_test_table_size]);
}

REGISTER_TEST(HashMap, TestHashMap());
