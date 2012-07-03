#include "examples/examples.h"
#include "kernel/disk-table.h"

#include "examples/imglib/pgmimage.h"

using std::vector;
using namespace piccolo;

// The math is based on Graphlab's dist_loopybg_denoise
// application, but the data and control flow bear no
// resemblance to that reference.

DEFINE_string(tidn_image, "input_image.pgm", "Input image to denoise");
DEFINE_int32(tidn_width, 0, "Input image width");
DEFINE_int32(tidn_height, 0, "Input image height");
DEFINE_int32(tidn_colors, 5, "Input image color depth");
DEFINE_double(tidn_sigma, 1, "Stddev of noise to add to img");
DEFINE_double(tidn_lambda, 2, "Smoothing parameter");
DEFINE_string(tidn_smoothing, "la", "Smoothing type ([sq]uare or [la]place)");
DEFINE_double(tidn_propthresh, 1e-4, "Threshold to propagate updates");
DEFINE_double(tidn_damping, 0.9, "Edge damping value");
DEFINE_int32(tidn_passes, 1, "Number of async denoising passes");
DEFINE_bool(tidn_corrupt, true, "Set to false if pre-corrupted image");
DEFINE_double(tidn_corrupt_pct, 1.0, "When corruption is specified, pick a random area to corrupt");
DEFINE_bool(tidn_useblock, true, "Set to true for Block sharding, false for Mod sharding");

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

struct BlockShard : public Sharder<int> {
  BlockShard() { num_shards = -1; naive = 0; }
  int operator()(const int& key, int shards) {
    if (shards != num_shards) reblock(shards);
    int retval = (naive?(key%shards):(
      _BlockCols*((getRowFromID(key)*_BlockRows)/FLAGS_tidn_height)+
      ((getColFromID(key)*_BlockCols)/FLAGS_tidn_width)
      ));
    return retval;
  }
private:
  void reblock(int shards) {
    CHECK_GT(shards,0) << "reblock() was called for BlockSharding with a shards value of 0";
    num_shards = shards;
	for (_BlockRows = sqrt(num_shards); _BlockRows > 0; _BlockRows--) {
      if ((num_shards/_BlockRows)*_BlockRows == num_shards) {
        _BlockCols = num_shards/_BlockRows;
        VLOG(1) << "Found non-naive blocking scheme " << _BlockCols << " blocks wide and "
                  << _BlockRows << " blocks tall.";
        return;
      }
    }
    naive = 1;
  }
  int num_shards;
  int _BlockRows, _BlockCols;
  int naive;
};

// Tools for Unary Factors
double factorNormalize(vector<double>& vec) {
  // Following the GraphLab example, this is done in terms
  // of log-stored values, so we need to sum their exponents
  // to properly normalize.
  double max=vec[0],total=0.;
  for(int i=0; i<vec.size(); i++) {
    if (std::isnan(vec[i]) || std::isinf(vec[i])) {
      LOG(FATAL) << "Infinite/NaN pixel " << vec[i] << " detected in Normalize (1), aborting.";
    }
    max = std::max(max,vec[i]);
  }
  for(int i=0; i<vec.size(); i++) {
    total += exp(vec[i] -= max);
  }
  if (std::isnan(total) || std::isinf(total) || total <= 0.) {
    LOG(FATAL) << "Normalization (2) led to fin/NaN problem, total = " << total;
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

class binfac {
public:
  binfac(unsigned int rows = 0, unsigned int cols = 0) : data_(rows*cols), rows_(rows), cols_(cols) {}
  double get(int i, int j) { return data_[i*cols_+j]; }
  void set(int i, int j, double v) { data_[i*cols_+j] = v; }
  void resize(unsigned int rows, unsigned int cols) {
    data_.resize(rows*cols);
    rows_ = rows;
    cols_ = cols;
  }

  void set_as_agreement(double lambda) {
    for(unsigned int i = 0; i < rows_; i++) {
      for(unsigned int j = 0; j < cols_; j++) {
        set(i,j,(i == j)?0.:-lambda);
      }
    }
  }

  void set_as_laplace(double lambda) {
    for(unsigned int i = 0; i < rows_; i++) {
      for(unsigned int j = 0; j < cols_; j++) {
        set(i,j,-std::abs((double)i-(double)j)*lambda);
      }
    }
  }
  int cols() { return cols_; }
  int rows() { return rows_; }

private:
  vector<double> data_;
  unsigned int rows_;
  unsigned int cols_;

};

binfac edge_factor;

void factorConvolve(vector<double> a, binfac b) {
  CHECK_EQ(a.size(),b.cols()) << "factorConvolve() requires dimensionally-identical vectors";
  for(int i=0; i<a.size(); i++) {
    double sum = 0.;
    for(int j=0; j<a.size(); j++) {
      sum += std::exp(b.get(i,j)+a[j]);
    }
    if (sum == 0.) sum = std::numeric_limits<double>::min();
    a[i] = std::log(sum);
    if (std::isnan(a[i]) || std::isinf(a[i])) LOG(FATAL) << "Detected NaN/inf sum " << a[i] << " in convolution";
  }
}

double factorResidual(vector<double>& a, vector<double>& b) {
  double residual = 0;
  for(vector<double>::iterator it1 = a.begin(), it2 = b.begin(); it1 != a.end(); it1++, it2++) {
    residual += std::abs(std::exp(*it1) - std::exp(*it2));
  }
  return residual/a.size();
}

static int PopulateTables(int shards, string im_path, int colors) {
  // Fetch image and add noise to it
  image cleanim(im_path);
  image noisyim(cleanim);
  if (FLAGS_tidn_corrupt) {
    if (FLAGS_tidn_corrupt_pct < 1.0) {
      double totalarea = noisyim.rows()*noisyim.cols();
      double corruptarea = totalarea*FLAGS_tidn_corrupt_pct;
      int x0, y0, width, height;
      do {
        x0 = rand() % noisyim.cols();
        width = rand() % (noisyim.cols()-x0);
        y0 = rand() % noisyim.rows();
        height = floor(corruptarea/width);
      } while(x0+width > noisyim.cols() || y0+height > noisyim.rows());
      LOG(INFO) << "Corrupting " << FLAGS_tidn_corrupt_pct*100 << "% of image from ("
                << x0 << ", " << y0 << ") to (" << (x0+width-1) << ", " << (y0+height-1) << ")";
      noisyim.corrupt_area(x0,y0,width,height,FLAGS_tidn_sigma,(float)((double)255./(double)FLAGS_tidn_colors));
    } else {
      noisyim.corrupt(FLAGS_tidn_sigma,(float)((double)255./(double)FLAGS_tidn_colors));
    }
    noisyim.tofile(im_path + ".noisy");
    double mse = noisyim.calcMSEfrom(cleanim);
    LOG(INFO) << "Noisy image has MSE=" << mse << " from original";
  }

  // Set up initialization vectors (potential and initval)
  vector<double> initval;
  initval.resize(FLAGS_tidn_colors);
  factorUniform(initval);
  factorNormalize(initval);
  vector<double> potential(initval);

  // Set up all potentials and edges
  int k=0;
  double sigma_squared = FLAGS_tidn_sigma*FLAGS_tidn_sigma;

  int minval,maxval;
  minval = maxval = noisyim.getpixel(0,0);
  for(int i=0; i<FLAGS_tidn_height; i++) {
    for(int j=0; j<FLAGS_tidn_width; j++) {
      if (noisyim.getpixel(i,j) < minval) minval = noisyim.getpixel(i,j);
      if (noisyim.getpixel(i,j) > maxval) maxval = noisyim.getpixel(i,j);
    }
  }

  for(int i=0; i<FLAGS_tidn_height; i++) {
    for(int j=0; j<FLAGS_tidn_width; j++) {
      int idx = getVertID(i,j);

      // Set up potential from pixel
      double obs = (double)noisyim.getpixel(i,j);
      //map 0..255 color to 0...[FLAGS_tidn_colors-1]
      obs = (double)(FLAGS_tidn_colors-1)*((obs-(double)minval)/((double)maxval-(double)minval));
      //printf("%d %d = %f -> %f\n",i,j,(double)noisyim.getpixel(i,j),obs);

      for(size_t pred = 0; pred < FLAGS_tidn_colors; ++pred) {
        potential[pred] = 
          -(obs - pred)*(obs - pred) / (2.0 * sigma_squared);
      }
      factorNormalize(potential);
   	  //VLOG(3) << "Putting vertex ID " << idx << "'s update in potentials";
      potentials->update(idx,potential);
      //beliefs->update(idx, initval);

      // Set up edges
	  //VLOG(3) << "Putting vertex ID " << idx << "'s update in edges";
      edges_up->update(idx,initval);
      edges_down->update(idx,initval);
      edges_left->update(idx,initval);
      edges_right->update(idx,initval);
      k++;
      CHECK_EQ(initval.size(),FLAGS_tidn_colors) << "!!BUG!! initval got corrupted.";
    }
  }
  LOG(INFO) << "Set up " << k << " pixels in potential and edge tables";
  return 0;
}

// Trigger that handles applying incoming updates to the belief
// for the particular pixel, then propagates via the LongFire
struct idn_trigger: public HybridTrigger<int, vector<double> > {
public:
  bool Accumulate(vector<double>* a, const vector<double>& b) {
    vector<double> old(*a);
    factorConvolve((vector<double>&)b, edge_factor);
    factorNormalize((vector<double>&)b);
    factorDamp((vector<double>&)*a,(vector<double>&)b,FLAGS_tidn_damping);
    double resid = factorResidual((vector<double>&)*a,old);
    return (resid > FLAGS_tidn_propthresh);			//always run the long trigger for now
  }
  bool LongFire(const int key, bool lastrun) {
    // Get the potentials and incoming edges
    vector<double> P = potentials->get(key);
    vector<double> old(P);
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
    //beliefs->update(key,P);

    // Notify neighbors
    if (0 != getRowFromID(key)) {
      factorDivideToDivisor(P,A);
      factorNormalize(A);
      edges_down->update(key-FLAGS_tidn_width,A);
    }
    if (FLAGS_tidn_height-1 != getRowFromID(key)) {
      factorDivideToDivisor(P,B);
      factorNormalize(B);
      edges_up->update(key+FLAGS_tidn_width,B);
    }
    if (0 != getColFromID(key)) {
      factorDivideToDivisor(P,C);
      factorNormalize(C);
      edges_right->update(key-1,C);
    }
    if (FLAGS_tidn_width-1 != getColFromID(key)) {
      factorDivideToDivisor(P,D);
      //beliefs->update(idx, initval);
      factorNormalize(D);
      edges_left->update(key+1,D);
    }
    return false;                       	// don't re-run this long trigger unless another
                                            // Accumulator says to do so.
  }
};

int ImageDenoiseTrigger(const ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  {
    image testim(FLAGS_tidn_image);
    FLAGS_tidn_width = testim.cols();
    FLAGS_tidn_height = testim.rows();
    LOG(INFO) << "Opened image " << FLAGS_tidn_image << " with dimensions " << testim.cols() << " x "
              << testim.rows();
  }

  //assertions on arguments
  CHECK_GT(FLAGS_tidn_width,0) << "Image must have a positive width";
  CHECK_GT(FLAGS_tidn_height,0) << "Image must have a positive height";
  CHECK_GT(FLAGS_tidn_colors,0) << "Image must have a positive color depth";

  CHECK_GE(FLAGS_tidn_damping,0) << "Damping factor must be >= 0";
  CHECK_LT(FLAGS_tidn_damping,1) << "Damping factor must be < 1";

  //initialize tables
  if (FLAGS_tidn_useblock) {
    potentials  = CreateTable(0, FLAGS_shards, new BlockShard, new Accumulators<vector<double> >::Replace);
    edges_up    = CreateTable(1, FLAGS_shards, new BlockShard, new Accumulators<vector<double> >::Replace,1);
    edges_down  = CreateTable(2, FLAGS_shards, new BlockShard, new Accumulators<vector<double> >::Replace,1);
    edges_left  = CreateTable(3, FLAGS_shards, new BlockShard, new Accumulators<vector<double> >::Replace,1);
    edges_right = CreateTable(4, FLAGS_shards, new BlockShard, new Accumulators<vector<double> >::Replace,1);
  } else {
    potentials  = CreateTable(0, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);
    edges_up    = CreateTable(1, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace,1);
    edges_down  = CreateTable(2, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace,1);
    edges_left  = CreateTable(3, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace,1);
    edges_right = CreateTable(4, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace,1);
  }
  //beliefs     = CreateTable(5, FLAGS_shards, new Sharding::Mod, new Accumulators<vector<double> >::Replace);

  //set up edge factor on all workers
  edge_factor.resize(FLAGS_tidn_colors,FLAGS_tidn_colors);
  if (FLAGS_tidn_smoothing == "sq") {
    edge_factor.set_as_agreement(FLAGS_tidn_lambda);
  } else {
    edge_factor.set_as_laplace(FLAGS_tidn_lambda);
  }

  //all of these edges are the INCOMING edges for a pixel. The only actual processing that has been done on
  //them is a cavity and normalization; the edge_factor convolution, normalization, and damping must be
  //performed in the receiver's Accumulator. With the processed edges, the trigger will take responsibility
  //for combining the potentials and edges into a new belief, taking the normalized cavity, and propagating.
  StartWorker(conf);
  Master m(conf);

  if (!m.restore()) {

    //do non-restore setup (populate tables from image data)
    PRunOne(potentials, {
      if (PopulateTables(FLAGS_shards, FLAGS_tidn_image, FLAGS_tidn_colors)) {
        LOG(FATAL) << "Failed to turn image into an in-memory database";
      }
    });
  }

  PSwapAccumulator(edges_up,   {(Trigger<int,vector<double> >*)new idn_trigger});
  PSwapAccumulator(edges_down, {(Trigger<int,vector<double> >*)new idn_trigger});
  PSwapAccumulator(edges_left, {(Trigger<int,vector<double> >*)new idn_trigger});
  PSwapAccumulator(edges_right,{(Trigger<int,vector<double> >*)new idn_trigger});

  //Start the timer!
  struct timeval start_time, end_time;
  gettimeofday(&start_time, NULL);

  //run the application!
  PRunAll(potentials, {

    vector<double> initval;
    TypedGlobalTable<int, vector<double> >::Iterator *it =
      potentials->get_typed_iterator(current_shard());
    for(; !it->done(); it->Next()) {
      if (0 != getColFromID(it->key())) {
        initval = it->value();
        edges_right->update(it->key()-1, initval);
      }
    }

  });

  while (--FLAGS_tidn_passes > 0) {

    PSwapAccumulator(edges_up,   {new Accumulators<vector<double> >::Replace});
    PSwapAccumulator(edges_down, {new Accumulators<vector<double> >::Replace});
    PSwapAccumulator(edges_left, {new Accumulators<vector<double> >::Replace});
    PSwapAccumulator(edges_right,{new Accumulators<vector<double> >::Replace});

    PRunAll(potentials, {
      TypedGlobalTable<int, vector<double> >::Iterator *it = 
        potentials->get_typed_iterator(current_shard());
      vector<double> edgereset;
      edgereset.resize(FLAGS_tidn_colors);
      factorUniform(edgereset);
      factorNormalize(edgereset);
      for(; !it->done(); it->Next()) {
        int key = it->key();
        vector<double> P = it->value();
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
        potentials->update(key,P);

        // By doing this, we can then poke each edge with a "null" (raw potential) update
        edges_up->update(key,edgereset);
		edges_down->update(key,edgereset);
        edges_left->update(key,edgereset);
        edges_right->update(key,edgereset);
      }

    });

    PSwapAccumulator(edges_up,   {(Trigger<int,vector<double> >*)new idn_trigger});
    PSwapAccumulator(edges_down, {(Trigger<int,vector<double> >*)new idn_trigger});
    PSwapAccumulator(edges_left, {(Trigger<int,vector<double> >*)new idn_trigger});
    PSwapAccumulator(edges_right,{(Trigger<int,vector<double> >*)new idn_trigger});

    PRunAll(potentials, {

      vector<double> initval;
      TypedGlobalTable<int, vector<double> >::Iterator *it =
        potentials->get_typed_iterator(current_shard());
      for(; !it->done(); it->Next()) {
        if (0 != getColFromID(it->key())) {
          initval = it->value();
          edges_right->update(it->key()-1, initval);
        }
      }

    });
  }

  //Finish the timer!
  gettimeofday(&end_time, NULL);
  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec)
      * 1000000 + (end_time.tv_usec - start_time.tv_usec);
  fprintf(stderr, "Total denoise time: %.3f seconds \n", totaltime / 1000000.0);

  PSwapAccumulator(edges_up,   {new Triggers<int,vector<double> >::ReadOnlyTrigger});
  PSwapAccumulator(edges_down, {new Triggers<int,vector<double> >::ReadOnlyTrigger});
  PSwapAccumulator(edges_left, {new Triggers<int,vector<double> >::ReadOnlyTrigger});
  PSwapAccumulator(edges_right,{new Triggers<int,vector<double> >::ReadOnlyTrigger});

  //Construct output image
  PRunOne(potentials, {
    image outim(FLAGS_tidn_image + ".output", FLAGS_tidn_height, FLAGS_tidn_width);
    double sumresid = 0.;
    for(int shard=0; shard<potentials->num_shards(); shard++) {
      TypedTableIterator<int, vector<double> >* it_P = potentials->get_typed_iterator(shard);
      TypedTableIterator<int, vector<double> >* it_A = edges_up->get_typed_iterator(shard);
      TypedTableIterator<int, vector<double> >* it_B = edges_down->get_typed_iterator(shard);
      TypedTableIterator<int, vector<double> >* it_C = edges_left->get_typed_iterator(shard);
      TypedTableIterator<int, vector<double> >* it_D = edges_right->get_typed_iterator(shard);

      for(; !it_P->done() && !it_A->done() && !it_B->done() && !it_C->done() && !it_D->done(); 
            it_P->Next(), it_A->Next(), it_B->Next(), it_C->Next(), it_D->Next()) {

        if (it_P->key() != it_A->key() || it_A->key() != it_B->key() || 
            it_B->key() != it_C->key() || it_C->key() != it_D->key()
        ) {
          LOG(FATAL) << "Rebuilding image: key iteration mismatch";
        }

        vector<double> P = it_P->value();
        vector<double> Pold(P);
        vector<double> A = it_A->value();
        vector<double> B = it_B->value();
        vector<double> C = it_C->value();
        vector<double> D = it_D->value();
    	// Calculate the new Belief [B = n(PABCD)]
        factorTimes(P,A);
        factorTimes(P,B);
        factorTimes(P,C);
        factorTimes(P,D);
        factorNormalize(P);
        //belief == P now
        sumresid += factorResidual(P,Pold);

        double maxval = P[0];
        unsigned int maxidx = 0;

        // Find index of max vector element
        int k = 0;
        for(vector<double>::iterator it = P.begin(); it != P.end(); it++, k++) {
          if (*it > maxval) {
            maxval = *it;
            maxidx = k;
          }
        }

        outim.setpixel(getRowFromID(it_P->key()),getColFromID(it_P->key()),maxidx);
      } 
    }
    outim.tofile_graphlab(FLAGS_tidn_image + ".output");
    if (FLAGS_tidn_corrupt) {
      image cleanim(FLAGS_tidn_image);
      double mse = outim.calcMSEfrom(cleanim);
      LOG(INFO) << "Reconstructed image has MSE=" << mse << " from original";
    }
    LOG(INFO) << "Sum of residuals between Ps and Bs is " << sumresid;
  });

  return 0;
}
REGISTER_RUNNER(ImageDenoiseTrigger);
