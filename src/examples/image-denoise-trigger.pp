#include "examples/examples.h"
#include "kernel/disk-table.h"

#include "examples/imglib/pgmimage.h"

using std::vector;
using namespace piccolo;

// The math is based on Graphlab's dist_loopybg_denoise
// application, but the data and control flow bear no
// resemblance to that reference.

// Notes to self:
// - Following the example of GraphLab, it seems as though it makes sense to 
//   store data in logarithmic format. Some kind of magnitude or BP issue?
// - In order to make the algorithm work properly, the _beliefs_ table will
//   actually have values packed as [B][edge 1][edge 2][edge 3][edge 4].
// - Updates will have to take the form of [vertex #][belief]
// - Need to switch to a matrix-mult-style block sharding!

DEFINE_string(tidn_image, "input_image.pgm", "Input image to denoise");
DEFINE_int32(tidn_width, 0, "Input image width");
DEFINE_int32(tidn_height, 0, "Input image height");
DEFINE_double(tidn_bound, 0, "Termination tolerance");
DEFINE_int32(tidn_colors, 0, "Input image color depth");
DEFINE_double(tidn_sigma, 0, "Stddev of noise to add to img");
DEFINE_double(tidn_lambda, 0, "Smoothing parameter");
DEFINE_string(tidn_smoothing, "sq", "Smoothing type ([sq]uare or [la]place)");
DEFINE_double(tidn_propthresh, 1e-10, "Threshold to propagate updates");
DEFINE_double(tidn_damping, 0.1, "Edge damping value");

static int NUM_WORKERS = 0;
static TypedGlobalTable<int, vector<double> >* potentials;
static TypedGlobalTable<int, vector<double> >* edges_up;
static TypedGlobalTable<int, vector<double> >* edges_down;
static TypedGlobalTable<int, vector<double> >* edges_left;
static TypedGlobalTable<int, vector<double> >* edges_right;
//static TypedGlobalTable<int, vector<double> >* beliefs;

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

inline int getVertID(int row, int col) {
  return row*FLAGS_tidn_width+col;
}
inline int getRowFromID(int ID) {
  return (ID/FLAGS_tidn_width);
}
inline int getColFromID(int ID) {
  return (ID%FLAGS_tidn_width);
}

double factorNormalize(vector<double>& vec) {
  // Following the GraphLab example, this is done in terms
  // of log-stored values, so we need to sum their exponents
  // to properly normalize.
  double max=vec[0],total=0.;
  for(int i=0; i<vec.size(); i++) {
    if (std::isnan(vec[i]) || std::isinf(vec[i])) {
      LOG(FATAL) << "Infinite/NaN pixel detected, aborting.";
    }
    max = std::max(max,vec[i]);
  }
  for(int i=0; i<vec.size(); i++) {
    total += exp(vec[i] -= max);
  }
  if (std::isnan(total) || std::isinf(total) || total <= 0.) {
    LOG(FATAL) << "Normalization leadd to fin/NaN problem";
  }
  total = log(total);
  for(int i=0; i<vec.size(); i++) {
    vec[i] -= total;
  }
  return total;
}

void factorTimes(vector<double>& a, vector<double>& b) {
  // Remember, factors are stored logarithmically
  CHECK_EQ(a.size(),b.size()) << "factorTimes() requires dimensionally-identical vectors";
  for(int i=0; i<a.size(); i++) {
    a[i] += b[i];
  }
}

void factorDivide(vector<double>& a, vector<double>& b) {
  CHECK_EQ(a.size(),b.size()) << "factorDivide() requires dimensionally-identical vectors";
  for(int i=0; i<a.size(); i++) {
    a[i] -= b[i];
  }
}

void factorDivideToDivisor(vector<double>& a, vector<double>& b) {
  CHECK_EQ(a.size(),b.size()) << "factorDivide() requires dimensionally-identical vectors";
  for(int i=0; i<a.size(); i++) {
    b[i] = a[i] - b[i];
  }
}

// Note: this has dest_it and src_it reversed in the inner loop
// compared with GraphLab for Oolong to save a vector copy step.
void factorDamp(vector<double>& dest, vector<double>& src, double dampfac) {
  CHECK_EQ(dest.size(),src.size()) << "factorDamp() requires dimensionally-identical vectors";
  vector<double>::iterator dest_it = dest.begin();
  vector<double>::iterator src_it = src.begin();
  for(; dest_it != dest.end(); dest_it++, src_it++) {
    *dest_it = std::log(dampfac * std::exp(*dest_it) +
               (1.0 - dampfac) * std::exp(*src_it));
  }
}

void factorUniform(vector<double>& vec, double value = 0.) {              // remember factors are logs!
  for(vector<double>::iterator it = vec.begin(); it != vec.end(); it++) { // and e^0 = 1 (log(1) = 0)
    *it = value;
  }
}

void factorConvolve(vector<double>* a, double factor) {

}

static int PopulateTables(int shards, string im_path, int colors) {
  // Fetch image and initialize vectors
  image cleanim(im_path);
  cleanim.corrupt(FLAGS_tidn_sigma); //well, now cleanim is something of a misnomer.
  vector<double> initval;
  initval.resize(FLAGS_tidn_colors);
  factorUniform(initval);
  factorNormalize(initval);
  vector<double> potential(initval);

  // Set up all potentials and edges
  double sigma_squared = FLAGS_tidn_sigma*FLAGS_tidn_sigma;
  for(int i=0; i<FLAGS_tidn_height; i++) {
    for(int j=0; j<FLAGS_tidn_width; j++) {
      int idx = getVertID(i,j);

      // Set up potential from pixel
      double obs = (double)cleanim.getpixel(i,j);
      for(size_t pred = 0; pred < FLAGS_tidn_colors; ++pred) {
        potential[pred] = 
          -(obs - pred)*(obs - pred) / (2.0 * sigma_squared);
      }
      factorNormalize(potential);
      potentials->put(idx,potential);
      //beliefs->put(idx, initval);

      // Set up edges
      edges_up->put(idx,initval);
      edges_down->put(idx,initval);
      edges_left->put(idx,initval);
      edges_right->put(idx,initval);
    }
  }
  return 0;
}

// Trigger that handles applying incoming updates to the belief
// for the particular pixel, then propagates via the LongFire
struct idn_trigger: public HybridTrigger<int, vector<double> > {
public:
  bool Accumulate(vector<double>* a, const vector<double>& b) {
    LOG(FATAL) << "Must implement edge_factor!";
//    factorConvolve((vector<double>&)b, edge_factor);
    factorNormalize((vector<double>&)b);
    factorDamp((vector<double>&)*a,(vector<double>&)b,FLAGS_tidn_damping);
    return true;								//always run the long trigger for now
  }
  bool LongFire(const int key, bool lastrun) {
    // Get the potentials and incoming edges
    vector<double> P = potentials->get(key);
    vector<double> A = edges_up->get(key);
    vector<double> B = edges_down->get(key);
    vector<double> C = edges_left->get(key);
    vector<double> D = edges_right->get(key);

	// Calculate the new Belief [B = n(PABCD)]
    factorTimes(P,A);
    factorTimes(P,B);
    factorTimes(P,C);
    factorTimes(P,D);
    factorNormalize(P);
    //beliefs->put(key,P);

    // Notify neighbors
    if (0 != getRowFromID(key)) {
      factorDivide(P,A);
      factorNormalize(A);
      edges_down->update(key-FLAGS_tidn_width,A);
    }
    if (FLAGS_tidn_height-1 != getRowFromID(key)) {
      factorDivide(P,B);
      factorNormalize(B);
      edges_up->update(key+FLAGS_tidn_width,B);
    }
    if (0 != getColFromID(key)) {
      factorDivide(P,C);
      factorNormalize(C);
      edges_right->update(key-1,C);
    }
    if (FLAGS_tidn_width-1 != getColFromID(key)) {
      factorDivide(P,D);
      //beliefs->put(idx, initval);
      factorNormalize(D);
      edges_left->update(key+1,D);
    }
    return false;		// don't re-run this long trigger unless another
                        // Accumulator says to do so.
  }
};

int ImageDenoiseTrigger(const ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  //initialize tables
  potentials  = CreateTable(0, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
  edges_up    = CreateTable(1, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
  edges_down  = CreateTable(2, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
  edges_left  = CreateTable(3, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
  edges_right = CreateTable(4, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
//  beliefs     = CreateTable(5, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
  //all of these edges are the INCOMING edges for a pixel. The only actual processing that has been done on
  //them is a cavity and normalization; the edge_factor convolution, normalization, and damping must be
  //performed in the receiver's Accumulator. With the processed edges, the trigger will take responsibility
  //for combining the potentials and edges into a new belief, taking the normalized cavity, and propagating.

  StartWorker(conf);
  Master m(conf);

  //assertions on arguments
  CHECK_GT(FLAGS_tidn_width,0) << "Image must have a positive width";
  CHECK_GT(FLAGS_tidn_height,0) << "Image must have a positive height";
  CHECK_GT(FLAGS_tidn_colors,0) << "Image must have a positive color depth";

  CHECK_GE(FLAGS_tidn_damping,0) << "Damping factor must be >= 0";
  CHECK_LT(FLAGS_tidn_damping,1) << "Damping factor must be < 1";

  if (!m.restore()) {
    //do non-restore setup (populate tables from image data)
    if (PopulateTables(FLAGS_shards, FLAGS_tidn_image, FLAGS_tidn_colors)) {
      LOG(FATAL) << "Failed to turn image into an in-memory database";
    }
  }

  PSwapAccumulator(edges_up,   {(Trigger<int,vector<double> >*)new idn_trigger});
  PSwapAccumulator(edges_down, {(Trigger<int,vector<double> >*)new idn_trigger});
  PSwapAccumulator(edges_left, {(Trigger<int,vector<double> >*)new idn_trigger});
  PSwapAccumulator(edges_right,{(Trigger<int,vector<double> >*)new idn_trigger});

  //Start the timer!
  struct timeval start_time, end_time;
  gettimeofday(&start_time, NULL);

  //run the application!


  //Finish the timer!
  gettimeofday(&end_time, NULL);
  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec)
      * 1000000 + (end_time.tv_usec - start_time.tv_usec);
  fprintf(stderr, "Total denoise time: %.3f seconds \n", totaltime / 1000000.0);

  PSwapAccumulator(edges_up,   {new Triggers<int,vector<double> >::ReadOnlyTrigger});
  PSwapAccumulator(edges_down, {new Triggers<int,vector<double> >::ReadOnlyTrigger});
  PSwapAccumulator(edges_left, {new Triggers<int,vector<double> >::ReadOnlyTrigger});
  PSwapAccumulator(edges_right,{new Triggers<int,vector<double> >::ReadOnlyTrigger});

  return 0;
}
REGISTER_RUNNER(ImageDenoiseTrigger);
