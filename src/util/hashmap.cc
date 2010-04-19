#include "util/hashmap.h"
#include "util/static-initializers.h"

using namespace dsm;

static const int kTestSize=5000000;
static void TestHashMapSpeed() {
  HashMap<int, double> h(kTestSize * 2);
  Timer t;
  for (int i = 0; i < kTestSize; ++i) {
    h.put(i, i);
  }
  fprintf(stderr, "%d inserts in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    h.put(i, i);
  }
  fprintf(stderr, "%d replaces in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    h.get(i);
  }
  fprintf(stderr, "%d gets in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));
}

REGISTER_TEST(HashMapSpeed, TestHashMapSpeed());
