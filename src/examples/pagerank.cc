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

static vector<int> site_sizes;
static void BuildGraph(int shard, int nshards, int nodes, int density) {
  char* d = strdup(FLAGS_graph_prefix.c_str());
  File::Mkdirs(dirname(d));
  RecordFile out(StringPrintf("%s-%05d-of-%05d-N%05d.lzo", FLAGS_graph_prefix.c_str(), shard, nshards, nodes), "w", 1);

  if (site_sizes.empty()) {
    for (int n = 0; n < FLAGS_nodes; ) {
      int c = 50 + random() % 10000;
      site_sizes.push_back(c);
      n += c;
    }
  }

  Page n;
  for (int i = 0; i < site_sizes.size(); ++i) {
    if (i % nshards != shard) {
      continue;
    }

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
    ::BuildGraph(current_shard(), FLAGS_shards, FLAGS_nodes, 15);
  }

  RecordFile* get_reader() {
    string file = StringPrintf("%s-%05d-of-%05d-N%05d.lzo",
        FLAGS_graph_prefix.c_str(), current_shard(), FLAGS_shards, FLAGS_nodes);
    RecordFile * r = new RecordFile(file, "r");
    return r;
  }

  void Initialize() {
    Page n;
    RecordFile *r = get_reader();

    next_pr_hash->resize((int)(FLAGS_nodes * 1.5));
    curr_pr_hash->resize((int)(FLAGS_nodes * 1.5));

    while (r->read(&n)) {
      curr_pr_hash->put(MP(n.site(), n.id()), random_restart_seed());
    }
    delete r;
  }

  void WriteStatus() {
    fprintf(stderr, "Iteration %d, PR:: ", iter);
    fprintf(stderr, "%.2f\n", curr_pr_hash->get(MP(0, 0)));
  }

  void PageRankIter() {
    ++iter;

    RecordFile *r = get_reader();
    Page n;
    Timer t;

    while (r->read(&n)) {
      double v = curr_pr_hash->get_local(MP(n.site(), n.id()));
      double contribution = kPropagationFactor * v / n.target_site_size();
      for (int i = 0; i < n.target_site_size(); ++i) {
        next_pr_hash->put(MP(n.target_site(i), n.target_id(i)), contribution);
      }
    }
    delete r;

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
      next_pr_hash->put(it->key(), random_restart_seed());
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
  conf.set_slots(128);

  // Cap address space at 2G.
  struct rlimit rl;
  rl.rlim_cur = 1l << 31;
  rl.rlim_max = 1l << 31;
  setrlimit(RLIMIT_AS, &rl);

  NUM_WORKERS = conf.num_workers();
  TOTALRANK = FLAGS_nodes;

  Registry::create_table<PageId, double>(0, FLAGS_shards, &SiteSharding, &Accumulator<double>::sum);
  Registry::create_table<PageId, double>(1, FLAGS_shards, &SiteSharding, &Accumulator<double>::sum);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    if (FLAGS_build_graph) {
      RUN_ALL(m, PRKernel, BuildGraph, 0);
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
