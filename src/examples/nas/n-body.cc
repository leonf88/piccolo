#include "client.h"
#include "examples/examples.h"

DEFINE_int64(particles, 1000000, "");

using namespace dsm;

static const int kEdgeSize = 64;

struct pos {
  int x, y, z;

  static int sharding(const pos& p, int shard) {
    return (p.x >> 3) | ((p.y >> 3) << 8) | ((p.z >> 3) << 8);
  }
};

static bool operator==(const pos& a, const pos& b) {
  return memcmp(&a, &b, sizeof(pos)) == 0;
}

namespace dsm { namespace data {
  template <>
  int hash(pos p) {
    return hash(p.x | (p.y << 10) | (p.z << 20));
  }
} }

static void append_merge(string* a, const string& b) {
  a->append(b);
}

static TypedGlobalTable<pos, string> *a = NULL;
static TypedGlobalTable<pos, string> *b = NULL;

class NBodyKernel : public DSMKernel {
public:
  void Init() {
  }

  void update_particle(pos bucket, pos particle) {
    // iterate over points in the surrounding boxes, and add their contribution
    // to our particle.
    for (int dx = -1; dx <= 1; ++dx) {
      for (int dy = -1; dy <= 1; ++dy) {
        for (int dz = -1; dz <= 1; ++dz) {
          pos bk = bucket;
          bk.x = (bk.x + dx) % kEdgeSize;
          bk.y = (bk.y + dy) % kEdgeSize;
          bk.z = (bk.z + dz) % kEdgeSize;
          const string& b = a->get(bk);
        }
      }
    }
  }

  void SimulateRound() {
    TypedTable<pos, string>::Iterator* it = a->get_typed_iterator(current_shard());
    while (!it->done()) {
      const pos& p = it->key();
      const string& b = it->value();
      for (int i = 0; i < b.size() / sizeof(pos); i += sizeof(pos)) {
        update_particle(p, *(pos*)(b.data() + i));
      }
      it->Next();
    }
    delete it;

    a->clear(current_shard());
    swap(a, b);
  }
};
REGISTER_KERNEL(NBodyKernel);
REGISTER_METHOD(NBodyKernel, Init);
REGISTER_METHOD(NBodyKernel, SimulateRound);

int NBody(ConfigData& conf) {
  a = Registry::create_table<pos, string>(0, conf.num_workers(), &pos::sharding, &append_merge);

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
