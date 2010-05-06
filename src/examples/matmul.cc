#include "client.h"
#include <cblas.h>

using namespace dsm;

DEFINE_int32(edge_size, 1000, "");

static const int kBlockSize = 250;
static int bRows = -1;
static int bCols = -1;

struct Block { double d[kBlockSize*kBlockSize]; };

static TypedGlobalTable<int, Block>* matrix_a = NULL;
static TypedGlobalTable<int, Block>* matrix_b = NULL;
static TypedGlobalTable<int, Block>* matrix_c = NULL;

static void block_sum(Block *a, const Block& b) {
  for (int i = 0; i < kBlockSize * kBlockSize; ++i) {
    a->d[i] += b.d[i];
  }
}

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
    memset(b.d, 2, kBlockSize * kBlockSize * sizeof(double));
    memset(z.d, 0, kBlockSize * kBlockSize * sizeof(double));

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

    LOG(INFO) << NetworkThread::Get()->id() << " assigned " << bcount;
  }

  void Multiply() {
    Block a, b, c;

    for (int k = 0; k < bRows; k++) {
      for (int i = 0; i < bRows; i++) {
        for (int j = 0; j < bCols; j++) {
          if (!is_local(i, k)) { continue; }
          a = matrix_a->get(block_id(i, k));
          b = matrix_b->get(block_id(k, j));
          cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                      kBlockSize, kBlockSize, kBlockSize, 1,
                      a.d, kBlockSize, b.d, kBlockSize, 1, c.d, kBlockSize);
          matrix_c->update(block_id(i, j), c);
        }
      }
    }
  }
};

REGISTER_KERNEL(MatrixMultiplicationKernel);
REGISTER_METHOD(MatrixMultiplicationKernel, Initialize);
REGISTER_METHOD(MatrixMultiplicationKernel, Multiply);

int MatrixMultiplication(ConfigData& conf) {
  bCols = FLAGS_edge_size / kBlockSize;
  bRows = FLAGS_edge_size / kBlockSize;

  LOG(INFO) << "Create matrices with " << conf.num_workers() << " shards.";
  matrix_a = Registry::create_table<int, Block>(0, 4 * conf.num_workers(), &ModSharding, &block_sum);
  matrix_b = Registry::create_table<int, Block>(1, 4 * conf.num_workers(), &ModSharding, &block_sum);
  matrix_c = Registry::create_table<int, Block>(2, 4 * conf.num_workers(), &ModSharding, &block_sum);

  StartWorker(conf);
  Master m(conf);

  for (int i = 0; i < FLAGS_iterations; ++i) {
    m.run_all(Master::RunDescriptor::Create("MatrixMultiplicationKernel", "Initialize", matrix_a));
    m.run_all(Master::RunDescriptor::Create("MatrixMultiplicationKernel", "Multiply", matrix_a));
  }
  return 0;
}
REGISTER_RUNNER(MatrixMultiplication);
