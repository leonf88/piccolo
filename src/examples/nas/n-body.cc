#include "examples/examples.h"

DEFINE_int64(particles, 1000000, "");

using namespace dsm;

static const double kCutoffRadius = 1.0;
static const double kTimestep = 1e-6;

// Each box is 1*1*1
static const int kBoxSize = (int)ceil(kCutoffRadius);

// A partition is 4*4*4 boxes.
static const int kPartitionSize = 4;

// World is a cube of boxes.
static const int kWorldSize = 20;

static const int kNumPartitions = kWorldSize / (kPartitionSize * kBoxSize);

struct pos {
  double x, y, z;

  pos() : x(0), y(0), z(0) {}
  pos(double nx, double ny, double nz) : x(nx), y(ny), z(nz) {}
  pos(const pos& b) : x(b.x), y(b.y), z(b.z) {}

  // Find the partition corresponding to this point
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

  uint32_t hash() const {
    return uint32_t(z * kWorldSize * kWorldSize) +
           uint32_t(y * kWorldSize) +
           uint32_t(x);
  }

  // Return the upper left corner of the box containing this position.
  pos get_box() const {
    return pos(int(x / kBoxSize) * kBoxSize,
               int(y / kBoxSize) * kBoxSize,
               int(z / kBoxSize) * kBoxSize);
  }

  bool out_of_bounds() {
    return x < 0 || y < 0 || z < 0 || 
           x >= kWorldSize || 
           y >= kWorldSize ||
           z >= kWorldSize;
  }

  bool operator==(const pos& b) const {
    return x == b.x && y == b.y && z == b.z;
  }

  bool operator!=(const pos& b) const {
    return !(*this == b);
  }

  pos operator*(double d) { return pos(x * d, y * d, z * d); }
  pos operator/(double d) { return pos(x / d, y / d, z / d); }
  pos& operator+= (const pos& b) { x += b.x; y += b.y; z += b.z; return *this; }
  pos& operator-= (const pos& b) { x -= b.x; y -= b.y; z -= b.z; return *this; }

  pos operator+ (const pos& b) const { return pos(x + b.x, y + b.y, z + b.z); }
  pos operator- (const pos& b) const { return pos(x - b.x, y - b.y, z - b.z); }

  double magnitude_squared() { return x * x + y * y + z * z; }
  double magnitude() { return sqrt(magnitude_squared()); }
};

namespace std {
static ostream & operator<< (ostream &out, const pos& p) {
  out << MP(p.x, p.y, p.z);
  return out;
}

namespace tr1 {
template <>
struct hash<pos> {
  size_t operator()(const pos& k) { return k.hash(); }
};
}

}

namespace dsm {
template <>
struct Marshal<pos> {
  static void marshal(const pos& t, string *out) { out->append((char*)&t, sizeof(pos)); }
  static void unmarshal(const StringPiece& s, pos* t) { *t = *(pos*)(s.data); }
};

}

struct PosSharding : public Sharder<pos> {
  int operator()(const pos& p, int shards) {
    return pos::shard_for_pos(p);
  }
};

struct AppendAccum : public Accumulator<string> {
  void operator()(string* a, const string& b) {
    a->append(b);
  }
};

static pos kZero(0, 0, 0);

class NBodyKernel : public DSMKernel {
public:
  TypedGlobalTable<pos, string> *curr;
  TypedGlobalTable<pos, string> *next;

  HashMap<pos, string> cache;

  void CreatePoints() {
    curr = this->get_table<pos, string>(0);
    next = this->get_table<pos, string>(1);
    pos ul = pos::pos_for_shard(current_shard());
    
    // Create randomly distributed particles for each box inside of this shard
    for (int dx = 0; dx < kPartitionSize; ++dx) {
      for (int dy = 0; dy < kPartitionSize; ++dy) {
        for (int dz = 0; dz < kPartitionSize; ++dz) {
          int num_points = max(1, int(FLAGS_particles / pow(kWorldSize, 3)));
          pos b = ul + pos(dx, dy, dz) * kBoxSize;
          for (int i = 0; i < num_points; ++i) {
            pos pt = b + pos(rand_double() * kBoxSize, 
                             rand_double() * kBoxSize, 
                             rand_double() * kBoxSize);
            
            curr->update(pt.get_box(), string((char*)&pt, sizeof(pt)));
//            LOG(INFO) << "Adding: " << pt << " ; " << pt.get_box();
//            LOG(INFO) << "Created " <<
//                      curr->get(pt.get_box()).size() / sizeof(pos) << " at " << b;
          }
        }
      }
    }
  }

  pos compute_force(pos p1, pos p2) {
    double dist = (p1 - p2).magnitude_squared();
    if (dist > kCutoffRadius) { return kZero; }
    if (dist < 1e-8) { return kZero; }

    ++interaction_count;
    
//    LOG(INFO) << "COMPUTING::: " 
//              << p1 << " : " << p2 
//              << " ;; " << dist;
    return (p1 - p2) / dist;
  }

  void compute_update(pos box, const pos *points, int count) {
    // iterate over points in the surrounding boxes, and compute a
    // new position.

    string neighbors;
    for (int dx = -1; dx <= 1; ++dx) {
      for (int dy = -1; dy <= 1; ++dy) {
        for (int dz = -1; dz <= 1; ++dz) {
          pos bk = box + pos(dx, dy, dz);
          bk = bk.get_box();
          if (bk.out_of_bounds()) {
            continue;
          }

//          LOG_EVERY_N(INFO, 1000)
//            << "Fetched: " << MP(dx, dy, dz) << " : " << bk;

          if (cache.contains(bk)) {
            neighbors += cache.get(bk);
          } else {
            string v = curr->get(bk);
            neighbors += v;
            cache.put(bk, v);
          }
        }
      }
    }

    for (int i = 0; i < count; ++i) {
      pos a = points[i];
      pos a_force = kZero;

      const pos* pb = (pos*) neighbors.data();
      for (int j = 0; j < neighbors.size() / sizeof(pos); ++j) {
        const pos& b = pb[j];
        a_force += compute_force(a, b);
      }

      a = a + (a_force * kTimestep);
      if (a.get_box().out_of_bounds()) { continue; }

      next->update(a.get_box(), string((char*) &a, sizeof(pos)));
    }
  }

  void Simulate() {
    cache.clear();

    // Iterate over each box in this partition.
    TypedIterator<pos, string>* it = curr->get_typed_iterator(current_shard());
//    int total = curr->get_partition(current_shard())->size();

    for (int count = 0; !it->done(); ++count) {
      interaction_count = 0;
      const pos& box_pos = it->key();
      const pos* points = (pos*)it->value().data();
      compute_update(box_pos, points, it->value().size() / sizeof(pos));
//      LOG(INFO) << "Scanned: " << count << " of " << total
//                << " local points: " << it->value().size() / sizeof(pos)
//                << " interactions: " << interaction_count;
      it->Next();
    }
    delete it;
  }

  void Swap() {
    curr->clear(current_shard());
    swap(curr, next);
  }

  int interaction_count;
};
REGISTER_KERNEL(NBodyKernel);
REGISTER_METHOD(NBodyKernel, CreatePoints);
REGISTER_METHOD(NBodyKernel, Simulate);
REGISTER_METHOD(NBodyKernel, Swap);

int NBody(ConfigData& conf) {
  conf.set_slots(256);
  CreateTable(0, kNumPartitions * kNumPartitions * kNumPartitions, 
                                      new PosSharding, new AppendAccum);
  
  CreateTable(1, kNumPartitions * kNumPartitions * kNumPartitions, 
                                      new PosSharding, new AppendAccum);

  if (!StartWorker(conf)) {
    Master m(conf);
    m.run_all("NBodyKernel", " CreatePoints",  TableRegistry::Get()->table(0));
    for (int i = 0; i < FLAGS_iterations; ++i) {
      LOG(INFO) << "Running iteration: " << MP(i, FLAGS_iterations);
      m.run_all("NBodyKernel", " Simulate",  TableRegistry::Get()->table(0));
    }
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
    CHECK_EQ(shard, shard2) << p << " : " << ps;

    pos pb(i + 0.1, i + 0.1, i + 0.1);
    CHECK_EQ(pos::shard_for_pos(pb), pos::shard_for_pos(p));
    CHECK_EQ(pb.get_box(), p);
    CHECK_EQ(pb.get_box(), p.get_box());
  }

  HashMap<pos, int> t(1000);
  for (int i = 0; i < 100; ++i) {
    t.put(pos(0, 0, i + 0.5), i);
  }

  for (int i = 0; i < 100; ++i) {
   CHECK_EQ(t.get(pos(0, 0, i + 0.5)), i);
  }
}
REGISTER_TEST(TestPos, TestPos());
