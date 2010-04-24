#include "client.h"
#include "examples/examples.h"

DEFINE_int64(particles, 1000000, "");

using namespace dsm;

// Each box is 64*64*64
static const int kEdgeSize = 64;

// A partition is 8*8*8 boxes.
static const int kPartitionSize = 8;

struct pos {
  double x, y, z;

  static int sharding(const pos& p, int shard) {
    return (int)(p.x * 1000000 + p.y * 10000 + p.z);
  }

  static pos Create(int x, int y, int z) {
    pos p = { x, y, z};
    return p;
  }

  pos get_bucket() {

  }

  pos operator/(double d) { return pos::Create(x / d, y / d, z / d); }
  double magnitude_squared() { return x * x + y * y + z * z; }
  double magnitude() { return sqrt(magnitude_squared()); }
};

#define POS_OP(op)\
  pos operator op (const pos& a, const pos& b) {\
    return pos::Create(a.x op b.x, a.y op b.y, a.z op b.z);\
  }

POS_OP(-)
POS_OP(+)
#undef POS_OP


static bool operator==(const pos& a, const pos& b) {
  return memcmp(&a, &b, sizeof(pos)) == 0;
}

namespace dsm { namespace data {
  template <>
  uint32_t hash(pos p) {
    return (int)(p.x * 1000000 + p.y * 10000 + p.z);
  }
} }

static void append_merge(string* a, const string& b) {
  a->append(b);
}

class NBodyKernel : public DSMKernel {
public:
  TypedGlobalTable<pos, string> *curr;
  TypedGlobalTable<pos, string> *next;

  void Init() {
    for (int i = 0; i < FLAGS_particles; ++i) {
      pos pt = pos::Create(rand_double(), rand_double(), rand_double());
      curr->put(pt.get_bucket(), string((char*)&pt, sizeof(pt)));
    }
  }

  pos compute_force(pos p1, pos p2) {
    return (p1 - p2) / (p1 - p2).magnitude_squared();
  }

  void compute_pos(pos bucket, pos particle) {
    // iterate over points in the surrounding boxes, and compute a
    // new position.
    for (int dx = -1; dx <= 1; ++dx) {
      for (int dy = -1; dy <= 1; ++dy) {
        for (int dz = -1; dz <= 1; ++dz) {
          pos bk = bucket;
          bk.x = (bk.x + dx);
          bk.y = (bk.y + dy);
          bk.z = (bk.z + dz);
          const string& b = curr->get(bk);
        }
      }
    }
  }

  void SimulateRound() {
    // Iterate over each bucket in this partition.
    TypedTable<pos, string>::Iterator* it = curr->get_typed_iterator(current_shard());
    while (!it->done()) {
      const pos& bucket_pos = it->key();
      const pos* points = (pos*)it->value().data();
      for (int i = 0; i < it->value().size() / sizeof(pos); ++i) {
        compute_pos(bucket_pos, points[i]);
      }
      it->Next();
    }
    delete it;

    curr->clear(current_shard());
    swap(curr, next);
  }
};
REGISTER_KERNEL(NBodyKernel);
REGISTER_METHOD(NBodyKernel, Init);
REGISTER_METHOD(NBodyKernel, SimulateRound);

int NBody(ConfigData& conf) {
  Registry::create_table<pos, string>(0, conf.num_workers(), &pos::sharding, &append_merge);
  Registry::create_table<pos, string>(1, conf.num_workers(), &pos::sharding, &append_merge);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, NBodyKernel, Init, 0);
    RUN_ALL(m, NBodyKernel, Partition, 0);
    RUN_ALL(m, NBodyKernel, NBody, 0);
  } else {
    Worker w(conf);
    w.Run();
  }

  return 0;
}
REGISTER_RUNNER(NBody);
