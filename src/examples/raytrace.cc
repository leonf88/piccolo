#include "client/client.h"

using namespace dsm;

DEFINE_int32(width, 800, "");
DEFINE_int32(height, 600, "");
DEFINE_int32(block_size, 10, "");
DEFINE_int32(frames, 1, "");

DEFINE_string(source, "", "");

struct RGB {
  uint16_t r;
  uint16_t g;
  uint16_t b;
};
typedef tuple2<int, int> Pixel;


// Always write pixels to the first partition.
struct PixelSharder : public Sharder<Pixel> {
  int operator()(const Pixel& k, int shards) { return 0; }
};

static TypedGlobalTable<Pixel, RGB>* pixels = NULL;
static TypedGlobalTable<int, int>* geom = NULL;

class RayTraceKernel : public DSMKernel {
public:
  void InitKernel() {
    GlobalTable *t = get_table(0);
    if (t->is_local_shard(0)) {
      t->get_partition(0)->resize(FLAGS_width * FLAGS_height);
    }
  }

  void TraceFrame() {
    int s = current_shard();
    int chunks_per_row = FLAGS_width / FLAGS_block_size;
    int r = FLAGS_block_size * (s / chunks_per_row);
    int c = FLAGS_block_size * (s % chunks_per_row);

    int frame = args().get<int>("frame");

    string cmd = StringPrintf("povray +O- -D +FP24 +SC%d +EC%d +SR%d +ER%d  +Q8 +SF%d +EF%d +KFI1 +KFF%d +W%d +H%d %s 2>/dev/null",
                              c, min(c + FLAGS_block_size, FLAGS_width),
                              r, min(r + FLAGS_block_size, FLAGS_height),
                              frame, frame, FLAGS_frames,
                              FLAGS_width, FLAGS_height,
                              FLAGS_source.c_str(),
                              current_shard());

//    LOG(INFO) << cmd;
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
          pixels->update(MP(r + i, j), up);
        }
      }
    }

    pclose(f);
  }

  void DrawFrame() {
    return;
    FILE* f = fopen("frame.ppm", "w");
    Pixel k;
    fprintf(f, "P6\n%d %d\n65535\n", FLAGS_width, FLAGS_height);
    for (int i = 0; i < FLAGS_height; ++i) {
      for (int j = 0; j < FLAGS_width; ++j) {
        k.a_  = i; k.b_ = j;
        RGB r = pixels->get_local(k);
//        LOG(INFO) << i << " : " << j << " : " << r.r << ", " << r.g << ", " << r.b;
        fwrite(&r.r, 2, 1, f);
        fwrite(&r.g, 2, 1, f);
        fwrite(&r.b, 2, 1, f);
      }
    }
    fclose(f);

    system("display -remote frame.ppm");
  }
};
REGISTER_KERNEL(RayTraceKernel);
REGISTER_METHOD(RayTraceKernel, TraceFrame);
REGISTER_METHOD(RayTraceKernel, DrawFrame);

static int RayTrace(ConfigData &conf) {
  int shards = (FLAGS_height * FLAGS_width) / (FLAGS_block_size * FLAGS_block_size);
  pixels = CreateTable(0, 1, new PixelSharder, new Accumulators<RGB>::Replace);
  geom = CreateTable(1, shards, new Sharding::Mod, new Accumulators<int>::Replace);

  ArgMap args;
  if (!StartWorker(conf)) {
    Master m(conf);
    for (int i = 1; i <= FLAGS_frames; ++i) {
      args.set<int>("frame", i);
      RunDescriptor r("RayTraceKernel", "TraceFrame",  geom);
      r.params = args;
      m.run_all(r);

      m.run_one("RayTraceKernel", "DrawFrame",  pixels);
    }
  }
  return 0;
}
REGISTER_RUNNER(RayTrace);
