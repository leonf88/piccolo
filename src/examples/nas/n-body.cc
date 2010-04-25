#include "examples/examples.h"

DEFINE_int64(particles, 1000000, "");

using namespace dsm;

static const double kCutoffRadius = 1.0;
static const double kTimestep = 1e-6;

// Each box is 1*1*1
static const int kBoxSize = (int)ceil(kCutoffRadius);

// A partition is 8*8*8 boxes.
static const int kPartitionSize = 1;

// World is a cube of boxes.
static const int kWorldSize = 1;

static const int kNumPartitions = kWorldSize / (kPartitionSize * kBoxSize);

struct pos {
  double x, y, z;

  pos() : x(0), y(0), z(0) {}
  pos(double x, double y, double z) : x(x), y(y), z(z) {}
  pos(const pos& b) : x(b.x), y(b.y), z(b.z) {}

  // Find the partition corresponding to this box.
  static int shard_for_pos(const pos& p) {
    const pos& grid = p.get_box() / kPartitionSize;
    return int(grid.z) * kNumPartitions * kNumPartitions +
           int(grid.y) * kNumPartitions +
           int(grid.x);
  }
  
  static pos pos_for_shard(int id) {
    int z = id / (kNumPartitions * kNumPartitions);
    int y = (id % (kNumPartitions * kNumPartitions)) / kNumPartitions;
    int x = id % kNumPartitions;

    return pos(x, y, z) * kPartitionSize;
  }

  static int hash(const pos& p) {
    const pos& grid = p.get_box();
    return grid.z * kWorldSize * kWorldSize +
           grid.y * kWorldSize +
           grid.x;
  }

  static int sharding(const pos& p, int shards) { return shard_for_pos(p); }

  // Return the upper left corner of the box containing this position.
  pos get_box() const {
    return pos(int(x / kBoxSize) * kBoxSize,
               int(y / kBoxSize) * kBoxSize,
               int(z / kBoxSize) * kBoxSize);
  }

  void clip() {
    if (x < 0) { x = kWorldSize + x; }
    if (y < 0) { y = kWorldSize + y; }
    if (z < 0) { z = kWorldSize + z; }
    if (x > kWorldSize) { x -= kWorldSize; } 
    if (y > kWorldSize) { y -= kWorldSize; } 
    if (z > kWorldSize) { z -= kWorldSize; } 
  }

  bool operator==(const pos& b) const { return x == b.x && y == b.y && z == b.z; }
  pos operator*(double d) { return pos(x * d, y * d, z * d); }
  pos operator/(double d) { return pos(x / d, y / d, z / d); }
  pos& operator+= (const pos& b) { x += b.x; y += b.y; z += b.z; return *this; }
  pos& operator-= (const pos& b) { x -= b.x; y -= b.y; z -= b.z; return *this; }

  pos operator+ (const pos& b) const { return pos(x + b.x, y + b.y, z + b.z); }
  pos operator- (const pos& b) const { return pos(x - b.x, y - b.y, z - b.z); }

  double magnitude_squared() { return x * x + y * y + z * z; }
  double magnitude() { return sqrt(magnitude_squared()); }

  tuple4<int, double, double, double> as_tuple() { return MP(shard_for_pos(*this), x,y,z); }
};

static pos kZero(0, 0, 0);

namespace dsm { namespace data {
template <>
uint32_t hash(pos p) {
  return pos::hash(p);
}

template <>
void marshal(const pos& t, string *out) {
  out->append((char*)&t, sizeof(pos));  
}

template <>
void unmarshal(const StringPiece& s, pos* t) {
  *t = *(pos*)(s.data);
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
    curr = this->get_table<pos, string>(0);
    next = this->get_table<pos, string>(1);
    pos ul = pos::pos_for_shard(current_shard());

    // Create randomly distributed particles inside of this shard.
    for (int i = 0; i < FLAGS_particles / curr->num_shards(); ++i) {
      pos pt = ul + pos(rand_double() * kBoxSize * kPartitionSize, 
                        rand_double() * kBoxSize * kPartitionSize, 
                        rand_double() * kBoxSize * kPartitionSize);

      LOG_EVERY_N(INFO, 10000) << "Creating: " << pt.as_tuple();
      curr->update(pt.get_box(), string((char*)&pt, sizeof(pt)));
    }
  }

  pos compute_force(pos p1, pos p2) {
    LOG(INFO) << "COMPUTING::: " << p1.as_tuple() << " : " << p2.as_tuple();
    double dist = (p1 - p2).magnitude_squared();
    if (dist > kCutoffRadius) { return kZero; }
    if (dist < 1e-8) { return kZero; }
    return (p1 - p2) / dist;
  }

  void compute_update(pos box, const pos *points, int count) {
    // iterate over points in the surrounding boxes, and compute a
    // new position.

    for (int i = 0; i < count; ++i) {
      pos a = points[i];
      pos a_force = kZero;
      for (int dx = -1; dx <= 1; ++dx) {
        for (int dy = -1; dy <= 1; ++dy) {
          for (int dz = -1; dz <= 1; ++dz) {
            pos bk = box + pos(dx, dy, dz);
            bk = bk.get_box();
            bk.clip();
            string pstring = curr->get(bk);
            const pos* pb = (pos*)pstring.data();
            
            LOG(INFO) << "Fetched: " << bk.as_tuple() << " size: " << pstring.size() / sizeof(pos);

            for (int j = 0; j < pstring.size() / sizeof(pos); ++j) {
              const pos& b = pb[j];
              a_force += compute_force(a, b);
            }
          }
        }
      }

      a = a + (a_force * kTimestep);
      next->update(a.get_box(), string((char*)&a, sizeof(pos)));
    }
  }

  void Simulate() {
    // Iterate over each box in this partition.
    TypedTable<pos, string>::Iterator* it = curr->get_typed_iterator(current_shard());
    while (!it->done()) {
      const pos& box_pos = it->key();
      const pos* points = (pos*)it->value().data();
      compute_update(box_pos, points, it->value().size() / sizeof(pos));
      it->Next();
    }
    delete it;

    curr->clear(current_shard());
    swap(curr, next);
  }
};
REGISTER_KERNEL(NBodyKernel);
REGISTER_METHOD(NBodyKernel, Init);
REGISTER_METHOD(NBodyKernel, Simulate);

int NBody(ConfigData& conf) {
  conf.set_slots(256);
  Registry::create_table<pos, string>(0, kNumPartitions * kNumPartitions * kNumPartitions, 
                                      &pos::sharding, &append_merge);
  
  Registry::create_table<pos, string>(1, kNumPartitions * kNumPartitions * kNumPartitions, 
                                      &pos::sharding, &append_merge);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, NBodyKernel, Init, 0);
    for (int i = 0; i < 100; ++i) {
      RUN_ALL(m, NBodyKernel, Simulate, 0);
    }
  } else {
    Worker w(conf);
    w.Run();
  }

  return 0;
}
REGISTER_RUNNER(NBody);

static void TestPos() {
  for (int i = 0; i < 100; ++i) {
    pos p(i, i, i);
    int shard = pos::shard_for_pos(p);
    pos ps = pos::pos_for_shard(shard);
    int shard2 = pos::shard_for_pos(ps);
    CHECK_EQ(shard, 
             (i / kPartitionSize) * kNumPartitions * kNumPartitions +
             (i / kPartitionSize) * kNumPartitions +
             (i / kPartitionSize));
    CHECK_EQ(shard, shard2) << p.as_tuple() << " : " << ps.as_tuple();
  }
}
REGISTER_TEST(TestPos, TestPos());
