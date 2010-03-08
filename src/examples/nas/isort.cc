#include "client.h"

using namespace dsm;
typedef uint32_t KeyType;

struct KeyGen {
  KeyGen() : x_(314159625), a_(1220703125) {}
  KeyType next() {
    uint64_t n = a_ * x_ % (2ll << 46);
    x_ = n;
    return x_;
  }

  uint64_t x_;
  uint64_t a_;
};

static const int kNumBuckets = 8 * 256;

static vector<int> src;
static TypedGlobalTable<KeyType, KeyType> *dst = NULL;

DEFINE_int32(sort_size, 1000000, "");

class SortKernel : public DSMKernel {
public:
  void Init() {
    KeyGen k;
    for (int i = 0; i < FLAGS_sort_size / kNumBuckets; ++i) {
      src.push_back(k.next());
    }
  }

  void Sort() {
    for (int i = 0; i < src.size(); ++i) {
      dst->put(src[i], 1);
    }
  }
};
REGISTER_KERNEL(SortKernel);
REGISTER_METHOD(SortKernel, Init);
REGISTER_METHOD(SortKernel, Sort);

int IntegerSort(ConfigData& conf) {
  conf.set_slots(1 + kNumBuckets / conf.num_workers());
  dst = Registry::create_table<KeyType, KeyType>(0, kNumBuckets, &UintModSharding, &Accumulator<KeyType>::sum);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, SortKernel, Init, 0);
    RUN_ALL(m, SortKernel, Sort, 0);
  } else {
    Worker w(conf);
    w.Run();
  }

  return 0;
}
REGISTER_RUNNER(IntegerSort);
