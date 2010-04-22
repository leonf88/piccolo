#include "util/hashmap.h"
#include "util/static-initializers.h"
#include <gflags/gflags.h>

#include <tr1/unordered_map>

using std::tr1::unordered_map;
using namespace dsm;

int optimizer_hack;

DEFINE_int32(test_table_size, 1000000, "");
#define TEST_PERF(name, op)\
{\
  Timer t;\
  for (int i = 0; i < FLAGS_test_table_size; ++i) {\
    op;\
  }\
  fprintf(stderr, "%s: %d inserts in %.3f seconds; %.0f/s %.0f cycles\n",\
          #op, FLAGS_test_table_size, t.elapsed(), t.rate(FLAGS_test_table_size), t.cycle_rate(FLAGS_test_table_size));\
}

static void TestHashMapSpeed() {
  HashMap<int, double> h(FLAGS_test_table_size * 2);
  unordered_map<int, double> umap(FLAGS_test_table_size * 2);
  vector<double> array_test(FLAGS_test_table_size * 2);

  vector<int> source(FLAGS_test_table_size);
  for (int i = 0; i < source.size(); ++i) {
    source[i] = random() % FLAGS_test_table_size;
  }

  TEST_PERF(HashPut, h.put(source[i], i));
  TEST_PERF(HashReplace, h.put(source[i], i));
  TEST_PERF(HashGet, h.get(source[i]));
  TEST_PERF(STLHashPut, umap[source[i]] = i);
  TEST_PERF(ArrayPut, array_test[source[i]] = i);
  TEST_PERF(ArrayPut, array_test[data::hash<int>(i) % FLAGS_test_table_size] = i);

  optimizer_hack = 0;
  TEST_PERF(ArrayPut, optimizer_hack += array_test[data::hash<int>(i) % FLAGS_test_table_size]);
}

REGISTER_TEST(HashMapSpeed, TestHashMapSpeed());
