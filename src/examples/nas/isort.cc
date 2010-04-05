#include "client.h"
#include "examples/examples.h"
#include <algorithm>

using namespace dsm;
typedef uint32_t KeyType;
typedef Bucket ValueType;

static void BucketMerge(Bucket *l, const Bucket &r) {
  l->MergeFrom(r);
}

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

static vector<int> src;
static TypedGlobalTable<KeyType, ValueType> *dst = NULL;

DEFINE_int64(sort_size, 1000000, "");

class SortKernel : public DSMKernel {
public:
  void Init() {
    KeyGen k;
    for (int i = 0; i < FLAGS_sort_size / dst->num_shards(); ++i) {
      src.push_back(k.next());
    }
  }

  void Partition() {
    Bucket b;
    b.mutable_value()->Add(0);
    for (int i = 0; i < src.size(); ++i) {
      PERIODIC(1.0, LOG(INFO) << "Partitioning...." << 100. * i / src.size());
      b.set_value(0, src[i]);
      dst->put(src[i] & 0xffff, b);
    }
  }

  void Sort() {
    TypedTable<KeyType, ValueType>::Iterator *i = dst->get_typed_iterator(current_shard());
    while (!i->done()) {
      Bucket b = i->value();
      uint32_t* t = b.mutable_value()->mutable_data();
      std::sort(t, t + b.value_size());
      i->Next();
    }
  }

};
REGISTER_KERNEL(SortKernel);
REGISTER_METHOD(SortKernel, Init);
REGISTER_METHOD(SortKernel, Partition);
REGISTER_METHOD(SortKernel, Sort);

int IntegerSort(ConfigData& conf) {
  dst = Registry::create_table<KeyType, ValueType>(0, conf.num_workers(), &UintModSharding, &BucketMerge);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, SortKernel, Init, 0);
    RUN_ALL(m, SortKernel, Partition, 0);
    RUN_ALL(m, SortKernel, Sort, 0);
  } else {
    Worker w(conf);
    w.Run();

    LOG(INFO) << "Worker " << conf.worker_id() << " :: " << w.get_stats();
  }

  return 0;
}
REGISTER_RUNNER(IntegerSort);
