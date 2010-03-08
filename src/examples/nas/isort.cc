#include "client.h"
#include "examples/graph.pb.h"
#include <algorithm>

using namespace dsm;
typedef uint32_t KeyType;
typedef Bucket ValueType;

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

namespace dsm { namespace data {
  template <>
  void marshal(const Bucket& t, string *out) {
    t.SerializePartialToString(out);
  }

  template <>
  void unmarshal(const StringPiece& s, Bucket* t) {
    t->ParseFromArray(s.data, s.len);
  }

} }

static const int kNumBuckets = 8 * 256;

static vector<int> src;
static TypedGlobalTable<KeyType, ValueType> *dst = NULL;

DEFINE_int64(sort_size, 1000000, "");

static void BucketMerge(ValueType *l, const ValueType &r) {
  l->MergeFrom(r);

//  uint32_t* data = l->mutable_value()->mutable_data();
//  std::inplace_merge(data, data + le, data + l->value_size());
}

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
      LOG_EVERY_N(INFO, 10000000) << "Partitioning...." << i;
      b.set_value(0, src[i]);
      dst->put(src[i] & 0x0fff, b);
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
//  conf.set_slots(1 + kNumBuckets / conf.num_workers());
  dst = Registry::create_table<KeyType, ValueType>(0, conf.num_workers(), &UintModSharding, &BucketMerge);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, SortKernel, Init, 0);
    RUN_ALL(m, SortKernel, Partition, 0);
    RUN_ALL(m, SortKernel, Sort, 0);
  } else {
    Worker w(conf);
    w.Run();
  }

  return 0;
}
REGISTER_RUNNER(IntegerSort);
