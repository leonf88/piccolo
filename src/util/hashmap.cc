#include "util/hashmap.h"
#include "util/static-initializers.h"

using namespace dsm;

int optimizer_hack;

static const int kTestSize=50000000;
static const int kHashSize=kTestSize * 2;
static void TestHashMapSpeed() {
  HashMap<int, double> h(kHashSize);
  Timer t;
  for (int i = 0; i < kTestSize; ++i) {
    h.put(i, i);
  }
  fprintf(stderr, "Hash: %d inserts in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    h.put(i, i);
  }
  fprintf(stderr, "Hash: %d replaces in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    h.get(i);
  }
  fprintf(stderr, "Hash: %d gets in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  vector<double> array_test(kHashSize);
  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    array_test[random() % kTestSize] = 1;
  }
  fprintf(stderr, "Array: %d random puts in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    array_test[data::hash<int>(i) % kTestSize] = 1;
  }
  fprintf(stderr, "Array: %d hashed puts in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));

  optimizer_hack = 0;
  t.Reset();
  for (int i = 0; i < kTestSize; ++i) {
    optimizer_hack += array_test[data::hash<int>(i) % kTestSize];
  }
  fprintf(stderr, "Array: %d hashed gets in %.3f seconds; %.0f/s %.0f cycles\n",
          kTestSize, t.elapsed(), t.rate(kTestSize), t.cycle_rate(kTestSize));
}

REGISTER_TEST(HashMapSpeed, TestHashMapSpeed());
