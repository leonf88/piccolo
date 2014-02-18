#include "examples.h"
#include <cblas.h>

using namespace dsm;

static int bRows = -1;
static int bCols = -1;

struct Block {
  float *d;

  Block() : d(new float[FLAGS_block_size*FLAGS_block_size]) {}
  Block(const Block& other) : d(new float[FLAGS_block_size*FLAGS_block_size])  {
    memcpy(d, other.d, sizeof(float)*FLAGS_block_size*FLAGS_block_size);
  }

  Block& operator=(const Block& other) {
    memcpy(d, other.d, sizeof(float)*FLAGS_block_size*FLAGS_block_size);
    return *this;
  }

  ~Block() { delete [] d; }
};

namespace dsm {
template <>
struct Marshal<Block> {
  void marshal(const Block& t, string *out) {
    out->assign((const char*)t.d,
                sizeof(float) * FLAGS_block_size * FLAGS_block_size);
  }

  void unmarshal(const StringPiece& s, Block *t) {
    CHECK_EQ(s.len, sizeof(float) * FLAGS_block_size * FLAGS_block_size);
    memcpy(t->d, s.data, s.len);
  }
};
}

static TypedGlobalTable<int, Block>* matrix_a = NULL;
static TypedGlobalTable<int, Block>* matrix_b = NULL;
static TypedGlobalTable<int, Block>* matrix_c = NULL;

struct BlockSum : public Accumulator<Block> {
  void Accumulate(Block *a, const Block& b) {
    for (int i = 0; i < FLAGS_block_size * FLAGS_block_size; ++i) {
      a->d[i] += b.d[i];
    }
  }
};

struct MatrixMultiplicationKernel : public DSMKernel {
  int num_shards, my_shard;

  int block_id(int y, int x) {
    return (y * bCols + x);
  }

  bool is_local(int y, int x) {
    return block_id(y, x) % num_shards == my_shard;
  }

  void Initialize() {
    num_shards = matrix_a->num_shards();
    my_shard = current_shard();

    Block b, z;
    for (int i = 0; i < FLAGS_block_size * FLAGS_block_size; ++i) {
      b.d[i] = 2;
      z.d[i] = 0;
    }

    int bcount = 0;

    for (int by = 0; by < bRows; by ++) {
      for (int bx = 0; bx < bCols; bx ++) {
        if (!is_local(by, bx)) { continue; }
        ++bcount;
        CHECK(matrix_a->get_shard(block_id(by, bx)) == current_shard());
        matrix_a->update(block_id(by, bx), b);
        matrix_b->update(block_id(by, bx), b);
        matrix_c->update(block_id(by, bx), z);
      }
    }
  }

  void Multiply() {
    Block a, b, c;

    // If work stealing occurs, this could be a kernel instance that
    // didn't run Initialize, so fetch parameters again.
    num_shards = matrix_a->num_shards();
    my_shard = current_shard();

    for (int k = 0; k < bRows; k++) {
      for (int i = 0; i < bRows; i++) {
        for (int j = 0; j < bCols; j++) {
          if (!is_local(i, k)) { continue; }
          a = matrix_a->get(block_id(i, k));
          b = matrix_b->get(block_id(k, j));
          cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                      FLAGS_block_size, FLAGS_block_size, FLAGS_block_size, 1,
                      a.d, FLAGS_block_size, b.d, FLAGS_block_size, 1, c.d, FLAGS_block_size);
          matrix_c->update(block_id(i, j), c);
        }
      }
    }
  }

  void Print() {
    Block b = matrix_c->get(block_id(0, 0));
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 5; ++j) {
        printf("%.2f ", b.d[FLAGS_block_size*i+j]);
      }
      printf("\n");
    }
  }
};

REGISTER_KERNEL(MatrixMultiplicationKernel);
REGISTER_METHOD(MatrixMultiplicationKernel, Initialize);
REGISTER_METHOD(MatrixMultiplicationKernel, Multiply);
REGISTER_METHOD(MatrixMultiplicationKernel, Print);

int MatrixMultiplication(ConfigData& conf) {
  bCols = FLAGS_edge_size / FLAGS_block_size;
  bRows = FLAGS_edge_size / FLAGS_block_size;

  matrix_a = CreateTable(0, bCols * bRows, new Sharding::Mod, new BlockSum);
  matrix_b = CreateTable(1, bCols * bRows, new Sharding::Mod, new BlockSum);
  matrix_c = CreateTable(2, bCols * bRows, new Sharding::Mod, new BlockSum);

  StartWorker(conf);
  Master m(conf);

  for (int i = 0; i < FLAGS_iterations; ++i) {
    m.run_all("MatrixMultiplicationKernel", "Initialize", matrix_a);
    m.run_all("MatrixMultiplicationKernel", "Multiply", matrix_a);
    m.run_one("MatrixMultiplicationKernel", "Print", matrix_c);
  }
  return 0;
}
REGISTER_RUNNER(MatrixMultiplication);
