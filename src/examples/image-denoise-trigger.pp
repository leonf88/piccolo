#include "examples/examples.h"
#include "kernel/disk-table.h"

#include "examples/imglib/pgmimage.h"

using std::vector;
using namespace piccolo;

DEFINE_string(tidn_image, "input_image.pgm", "Input image to denoise");
DEFINE_int32(tidn_width, 0, "Input image width");
DEFINE_int32(tidn_height, 0, "Input image height");
DEFINE_double(tidn_bound, 0, "Termination tolerance");
DEFINE_int32(tidn_colors, 0, "Input image color depth");
DEFINE_double(tidn_sigma, 0, "Stddev of noise to add to img");
DEFINE_double(tidn_lambda, 0, "Smoothing parameter");
DEFINE_string(tidn_smoothing, "sq", "Smoothing type ([sq]uare or [la]place)");

static int NUM_WORKERS = 0;
static TypedGlobalTable<int, vector<double> >* potentials;
static TypedGlobalTable<int, vector<double> >* beliefs;

namespace piccolo {
template<> struct Marshal<vector<double> > : MarshalBase {
  static void marshal(const vector<double>& t, string *out) {
    int i;
    double j;
    int len = t.size();
    out->append((char*) &len, sizeof(int));
    for (i = 0; i < len; i++) {
      j = t[i];
      out->append((char*) &j, sizeof(double));
    }
  }
  static void unmarshal(const StringPiece &s, vector<double>* t) {
    int i;
    double j;
    int len;
    memcpy(&len, s.data, sizeof(int));
    if (len < 0) LOG(FATAL) << "Unmarshalled vector of size < 0";
    t->clear();
    for (i = 0; i < len; i++) {
      memcpy(&j, s.data + i*sizeof(double) + sizeof(int), sizeof(double));
      t->push_back(j);
    }
  }
};
}

// Trigger that handles applying incoming updates
struct idn_trigger: public HybridTrigger<int, vector<double> > {
public:
  bool Accumulate(vector<double>* a, const vector<double>& b) {
    return true;
  }
  bool LongFire(const vector<double> key, bool lastrun) {
    return false;
  }
};

inline int getVertID(int row, int col) {
  return row*FLAGS_tidn_width+col;
}

static int PopulateTables(int shards, string im_path, int colors) {
  image cleanim(im_path);
  cleanim.corrupt(FLAGS_tidn_sigma);
  for(int i=0; i<FLAGS_tidn_height; i++) {
    for(int j=0; j<FLAGS_tidn_width; j++) {
      int idx = getVertID(i,j);
      vector<double> initval;
      beliefs->put(idx, initval);
    }
  }
  return 0;
}

int ImageDenoiseTrigger(const ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  //initialize tables
  potentials = CreateTable(0, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
  beliefs    = CreateTable(1, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);

  StartWorker(conf);
  Master m(conf);

  CHECK_GT(FLAGS_tidn_width,0) << "Image must have a positive width";
  CHECK_GT(FLAGS_tidn_height,0) << "Image must have a positive height";

  if (!m.restore()) {
    //do non-restore setup (populate tables from image data)
    if (PopulateTables(FLAGS_shards, FLAGS_tidn_image, FLAGS_tidn_colors)) {
      LOG(FATAL) << "Failed to turn image into an in-memory database";
    }
  }

  //Start the timer!
  struct timeval start_time, end_time;
  gettimeofday(&start_time, NULL);

  //run the application!


  //Finish the timer!
  gettimeofday(&end_time, NULL);
  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec)
      * 1000000 + (end_time.tv_usec - start_time.tv_usec);
  fprintf(stderr, "Total denoise time: %.3f seconds \n", totaltime / 1000000.0);

  return 0;
}
REGISTER_RUNNER(ImageDenoiseTrigger);
