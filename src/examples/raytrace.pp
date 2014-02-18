#include "examples.h"
#include <SDL/SDL.h>

using namespace dsm;

DEFINE_int32(width, 800, "");
DEFINE_int32(height, 600, "");
DEFINE_int32(frames, 1, "");

DEFINE_string(source, "", "");

struct RGB {
  uint16_t r;
  uint16_t g;
  uint16_t b;
};

struct Pixel {
  int a_, b_;
  static Pixel New(int a, int b) {
    Pixel p = { a, b };
    return p;
  }

  Pixel operator+(const int& offset) const {
    return Pixel::New(a_ + offset / FLAGS_block_size, b_ + offset % FLAGS_block_size);
  }

  bool operator==(const Pixel& o) const {
    return o.a_ == a_ && o.b_ == b_;
  }
};

// Always write pixels to the first partition.
struct PixelSharder : public Sharder<Pixel> {
  int operator()(const Pixel& k, int shards) { return 0; }
};

namespace std { namespace tr1 {
template <>
struct hash<Pixel> {
  hash<uint32_t> h;
  size_t operator()(Pixel p) const {
    return h(p.a_) ^ h(p.b_);
  }
};
} }

// Align map blocks with blocks from the picture
struct PixelBlock : public BlockInfo<Pixel> {
  Pixel block_id(const Pixel& k, int block_size)  {
    return Pixel::New(k.a_ - (k.a_ % FLAGS_block_size),
                       k.b_ - (k.b_ % FLAGS_block_size));
  }

  int block_pos(const Pixel& k, int block_size) {
    return (k.a_ % FLAGS_block_size) * FLAGS_block_size + k.b_ % FLAGS_block_size;
  }
};

static TypedGlobalTable<Pixel, RGB>* pixels = NULL;
static TypedGlobalTable<int, int>* geom = NULL;
static SDL_Surface *screen = NULL;

class RayTraceKernel : public DSMKernel {
public:
  void InitKernel() {
    GlobalTable *t = get_table(0);
    if (t->is_local_shard(0)) {
      t->get_partition(0)->resize(FLAGS_width * FLAGS_height);

      SDL_Init(SDL_INIT_AUDIO|SDL_INIT_VIDEO);
      screen = SDL_SetVideoMode(FLAGS_width, FLAGS_height, 32, SDL_SWSURFACE);
    }
  }

  void TraceFrame() {
    int s = current_shard();
    int chunks_per_row = FLAGS_width / FLAGS_block_size;
    int r = FLAGS_block_size * (s / chunks_per_row);
    int c = FLAGS_block_size * (s % chunks_per_row);

    int frame = get_arg<int>("frame");
    Timer t;

    string cmd = StringPrintf("povray +O- -D +FP24 +SC%d +EC%d +SR%d +ER%d  +Q8 +SF%d +EF%d +KFI1 +KFF%d +W%d +H%d %s 2>/dev/null",
                              c, min(c + FLAGS_block_size, FLAGS_width),
                              r, min(r + FLAGS_block_size, FLAGS_height),
                              frame, frame, FLAGS_frames,
                              FLAGS_width, FLAGS_height,
                              FLAGS_source.c_str(),
                              current_shard());

    FILE *f = popen(cmd.c_str(), "r");

    int h, w, maxval;
    CHECK_EQ(fscanf(f, "P6\n"), 0); /* Magic number */
    CHECK_EQ(fscanf(f, "%d %d\n", &w, &h), 2); /* Width, height */
    CHECK_EQ(fscanf(f, "%d\n", &maxval), 1); /* Maximum value */

    RGB up;
    for (int i = 0; i < FLAGS_block_size; ++i) {
      for (int j = 0; j < FLAGS_width; ++j) {
        CHECK_EQ(fread(&up.r, 2, 1, f), 1);
        CHECK_EQ(fread(&up.g, 2, 1, f), 1);
        CHECK_EQ(fread(&up.b, 2, 1, f), 1);

        if (j >= c && j < c + FLAGS_block_size) {
          pixels->update(Pixel::New(r + i, j), up);
        }
      }
    }

    pclose(f);
  }

  void DrawFrame() {
    DenseTable<Pixel, RGB>* p = (DenseTable<Pixel, RGB>*)pixels->get_partition(0);
    for (int i = 0; i < FLAGS_height; ++i) {
      for (int j = 0; j < FLAGS_width; ++j) {
        RGB b = p->get(Pixel::New(i, j));
        Uint32 *bufp = (Uint32 *)screen->pixels + i*screen->pitch/4 + j;
        *bufp = SDL_MapRGB(screen->format, b.r, b.g, b.b);
      }
    }

    SDL_UpdateRect(screen, 0, 0, FLAGS_width, FLAGS_height);
  }
};
REGISTER_KERNEL(RayTraceKernel);
REGISTER_METHOD(RayTraceKernel, TraceFrame);
REGISTER_METHOD(RayTraceKernel, DrawFrame);

static int RayTrace(ConfigData &conf) {
  int shards = (FLAGS_height * FLAGS_width) / (FLAGS_block_size * FLAGS_block_size);
  TableDescriptor* pixel_desc = new TableDescriptor(0, 1);
  pixel_desc->key_marshal = new Marshal<Pixel>;
  pixel_desc->value_marshal = new Marshal<RGB>;

  pixel_desc->partition_factory = new DenseTable<Pixel, RGB>::Factory;
  pixel_desc->block_size = FLAGS_block_size * FLAGS_block_size;
  pixel_desc->block_info = new PixelBlock;
  pixel_desc->sharder = new PixelSharder;
  pixel_desc->accum = new Accumulators<RGB>::Replace;
  pixels = CreateTable<Pixel, RGB>(pixel_desc);

  geom = CreateTable(1, shards, new Sharding::Mod, new Accumulators<int>::Replace);

  MarshalledMap args;
  if (!StartWorker(conf)) {
    Master m(conf);
    for (int i = 1; i <= FLAGS_frames; ++i) {
      args.put<int>("frame", i);
      RunDescriptor r("RayTraceKernel", "TraceFrame",  geom);
      r.params = args;
      m.run_all(r);

      m.run_one("RayTraceKernel", "DrawFrame",  pixels);
    }
  }
  return 0;
}
REGISTER_RUNNER(RayTrace);
