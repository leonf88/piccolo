#include "client.h"
#include "examples/examples.pb.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <algorithm>
#include <libgen.h>

using namespace dsm;
using namespace std;

static double TOTALRANK = 0;
static int NUM_WORKERS = 2;

static const double kPropagationFactor = 0.8;
static const int kBlocksize = 1000;
static const char kTestPrefix[] = "testdata/pr-graph.rec";

DEFINE_bool(build_graph, false, "");
DEFINE_int32(nodes, 10000, "");
DEFINE_string(graph_prefix, kTestPrefix, "Path to web graph.");

typedef pair<uint32_t, uint32_t> PageId;

namespace dsm { namespace data {
template<>
  uint32_t hash(PageId p) {
    return hash(p.first) ^ hash(p.second);
  }
}
}

static int SiteSharding(const PageId& p, int nshards) {
  return p.first % nshards;
}

static double powerlaw_random(double dmin, double dmax, double n) {
  double r = (double)random() / RAND_MAX;
  return pow((pow(dmax, n) - pow(dmin, n)) * pow(r, 3) + pow(dmin, n), 1.0/n);
}

static boost::recursive_mutex file_lock;

static vector<int> site_sizes;
static void BuildGraph(int shard, int nshards, int nodes, int density) {
  boost::recursive_mutex::scoped_lock sl(file_lock);

  char* d = strdup(FLAGS_graph_prefix.c_str());
  File::Mkdirs(dirname(d));
  if (site_sizes.empty()) {
    for (int n = 0; n < FLAGS_nodes; ) {
      int c = powerlaw_random(10, 500000, 0.001);
      site_sizes.push_back(c);
      LOG_EVERY_N(INFO, 100) << "Site size: " << c;
      n += c;
    }
  }

  string target = StringPrintf("%s-%05d-of-%05d-N%05d", FLAGS_graph_prefix.c_str(), shard, nshards, nodes);
  //FILE* lzo = popen(StringPrintf("lzop -f -q -1 -o%s", target.c_str()).c_str(), "w");

//  if (File::Exists(target + ".lzo")) {
//    return;
//  }

  Page n;
  RecordFile out(target, "w", RecordFile::LZO);
  for (int i = shard; i < site_sizes.size(); i += nshards) {
    for (int j = 0; j < site_sizes[i]; ++j) {
      n.Clear();
      n.set_site(i);
      n.set_id(j);
      for (int k = 0; k < density; k++) {
        int target_site = (random() % 10 != 0) ? i : (random() % site_sizes.size());
        n.add_target_site(target_site);
        n.add_target_id(random() % site_sizes[target_site]);
      }
      out.write(n);
    }
  }
  //pclose(lzo);
}

static double random_restart_seed() {
  return (1-kPropagationFactor)*(TOTALRANK/FLAGS_nodes);
}

class PRKernel : public DSMKernel {
public:
  int iter;
  vector<Page> nodes;
  TypedGlobalTable<PageId, double>* curr_pr_hash;
  TypedGlobalTable<PageId, double>* next_pr_hash;

  void KernelInit() {
    curr_pr_hash = this->get_table<PageId, double>(0);
    next_pr_hash = this->get_table<PageId, double>(1);
    iter = 0;
  }

  void BuildGraph() {
    srand(0);
    for (int i = 0; i < FLAGS_shards; ++i) {
      ::BuildGraph(i, FLAGS_shards, FLAGS_nodes, 15);
    }
  }

  RecordFile* get_reader() {
    string file = StringPrintf("%s-%05d-of-%05d-N%05d",
        FLAGS_graph_prefix.c_str(), current_shard(), FLAGS_shards, FLAGS_nodes);
    //FILE* lzo = popen(StringPrintf("lzop -d -c %s", file.c_str()).c_str(), "r");
    //RecordFile * r = new RecordFile(lzo, "r");
    return new RecordFile(file, "r", RecordFile::LZO);
  }

  void free_reader(RecordFile* r) {
    //pclose(r->fp.filePointer());
    delete r;
  }

  void Initialize() {
    next_pr_hash->resize(FLAGS_nodes);
    curr_pr_hash->resize(FLAGS_nodes);

    Page n;
    RecordFile *r = get_reader();
    while (r->read(&n)) {
      curr_pr_hash->update(MP(n.site(), n.id()), random_restart_seed());
    }
    free_reader(r);
  }

  void WriteStatus() {
    fprintf(stderr, "Iteration %d, PR:: ", iter);
    fprintf(stderr, "%.2f\n", curr_pr_hash->get(MP(0, 0)));
  }

  void PageRankIter() {
    ++iter;

    Page n;
    Timer t;

    RecordFile *r = get_reader();
    while (r->read(&n)) {
      double v = curr_pr_hash->get_local(MP(n.site(), n.id()));
      double contribution = kPropagationFactor * v / n.target_site_size();
      for (int i = 0; i < n.target_site_size(); ++i) {
        next_pr_hash->update(MP(n.target_site(i), n.target_id(i)), contribution);
      }
    }
    free_reader(r);

    char host[1024];
    gethostname(host, 1024);
    VLOG(1) << "Finished shard " << current_shard() << " on " << host << " in " << t.elapsed();
  }

  void ResetTable() {
    // Move the values computed from the last iteration into the current table, and reset
    // the values local to our node to the random restart value.
    swap(curr_pr_hash, next_pr_hash);
    next_pr_hash->clear(current_shard());

    TypedTable<PageId, double>::Iterator *it = curr_pr_hash->get_typed_iterator(current_shard());
    while (!it->done()) {
      next_pr_hash->update(it->key(), random_restart_seed());
      it->Next();
    }
  }
};
REGISTER_KERNEL(PRKernel);
REGISTER_METHOD(PRKernel, BuildGraph);
REGISTER_METHOD(PRKernel, Initialize);
REGISTER_METHOD(PRKernel, WriteStatus);
REGISTER_METHOD(PRKernel, PageRankIter);
REGISTER_METHOD(PRKernel, ResetTable);

int Pagerank(ConfigData& conf) {
  conf.set_slots(256);

  NUM_WORKERS = conf.num_workers();
  TOTALRANK = FLAGS_nodes;

  Registry::create_table<PageId, double>(0, FLAGS_shards, &SiteSharding, &Accumulator<double>::sum);
  Registry::create_table<PageId, double>(1, FLAGS_shards, &SiteSharding, &Accumulator<double>::sum);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    if (FLAGS_build_graph) {
      vector<int> shards;
      for (int i = 0; i < conf.num_workers(); ++i) {
        shards.push_back(i);
      }

      RUN_RANGE(m, PRKernel, BuildGraph, 0, shards);
    }

    RUN_ALL(m, PRKernel, Initialize, 0);
    for (int i = 0; i < FLAGS_iterations; i++) {
      RUN_ALL(m, PRKernel, PageRankIter, 0);
      RUN_ALL(m, PRKernel, ResetTable, 0);
      RUN_ONE(m, PRKernel, WriteStatus, 0);
    }
  } else {
    Worker w(conf);
    w.Run();
    LOG(INFO) << "Worker stats: " << conf.worker_id() << " :: " << w.get_stats();
  }

  return 0;
}
REGISTER_RUNNER(Pagerank);
