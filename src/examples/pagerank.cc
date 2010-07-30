#include "client/client.h"
#include "examples/examples.pb.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <algorithm>
#include <libgen.h>

using namespace dsm;
using namespace std;

static float TOTALRANK = 0;
static int NUM_WORKERS = 2;

static const float kPropagationFactor = 0.8;
static const int kBlocksize = 1000;
static const char kTestPrefix[] = "testdata/pr-graph.rec";

DEFINE_bool(memory_graph, false,
            "If true, the web graph will be generated on-demand.");

DEFINE_string(graph_prefix, kTestPrefix, "Path to web graph.");
DEFINE_bool(build_graph, false, "");
DEFINE_int32(nodes, 10000, "");

static float powerlaw_random(float dmin, float dmax, float n) {
  float r = (float)random() / RAND_MAX;
  return pow((pow(dmax, n) - pow(dmin, n)) * pow(r, 3) + pow(dmin, n), 1.0/n);
}

static vector<int> site_sizes;


// I'd like to use a pair here, but for some reason they fail to count
// as POD types according to C++.  Sigh.
struct PageId {
  uint32_t site;
  uint32_t page;
} __attribute__((packed));

PageId P(int s, int p) {
  PageId pid = { s, p };
  return pid;
}

bool operator==(const PageId& a, const PageId& b) {
  return a.site == b.site && a.page == b.page;
}

PageId operator+(const PageId& a, int offset) {
  PageId r = P(a.site, a.page + offset);
  while (r.page > site_sizes[r.site]) {
    r.page -= site_sizes[r.site];
    ++r.site;
  }

  return r;
}

namespace std {
static ostream & operator<< (ostream &out, const PageId& p) {
  out << MP(p.site, p.page);
  return out;
}

namespace tr1 {
template <>
struct hash<PageId> {
  size_t operator()(const PageId& p) const {
    return SuperFastHash((const char*)&p, sizeof p);
  }
};
}
}

struct SiteSharding : public Sharder<PageId> {
  int operator()(const PageId& p, int nshards) {
    return p.site % nshards;
  }
};

struct PageIdBlockInfo : public BlockInfo<PageId> {
  PageId block_id(const PageId& k, int block_size)  {
    return P(k.site, k.page - (k.page % block_size));
  }

  int block_pos(const PageId& k, int block_size) {
    return k.page % block_size;
  }
};

static void InitSites() {
  if (site_sizes.empty()) {
    srand(0);
    for (int n = 0; n < FLAGS_nodes; ) {
      int c = powerlaw_random(1, min(50000,
                                     (int)(100000. * FLAGS_nodes / 100e6)), 0.001);
      site_sizes.push_back(c);
//      LOG_EVERY_N(INFO, 100) << "Site size: " << c;
      n += c;
    }
  }
}

static void BuildGraph(int shard, int nshards, int nodes, int density) {
  InitSites();
  char* d = strdup(FLAGS_graph_prefix.c_str());
  File::Mkdirs(dirname(d));

  string target = StringPrintf("%s-%05d-of-%05d-N%05d", FLAGS_graph_prefix.c_str(), shard, nshards, nodes);

  if (File::Exists(target)) {
    return;
  }

  srand(shard);
  Page n;
  RecordFile out(target, "w", RecordFile::LZO);
  // Only sites with site_id % nshards == shard are in this shard.
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
}

static float random_restart_seed() {
  return (1-kPropagationFactor)*(TOTALRANK/FLAGS_nodes);
}

// Generate a graph on-demand rather then reading from disk.
class InMemoryGraph : public RecordFile {
public:
  InMemoryGraph(int shard, int num_shards)
    : shard_(shard), site_(shard), site_pos_(0), num_shards_(num_shards) {
    InitSites();
    srand(shard);
  }

  bool read(google::protobuf::Message *m) {
    if (site_pos_ >= site_sizes[site_]) {
      site_ += num_shards_;
      site_pos_ = 0;
    }

    if (site_ >= site_sizes.size()) {
      return false;
    }

    Page* p = static_cast<Page*>(m);
    p->Clear();
    p->set_site(site_);
    p->set_id(site_pos_);
    for (int k = 0; k < 15; k++) {
      int target_site = (random() % 10 != 0) ? site_ : (random() % site_sizes.size());
      p->add_target_site(target_site);
      p->add_target_id(random() % site_sizes[target_site]);
    }

    ++site_pos_;
    return true;
  }

private:
  int shard_;
  int site_;
  int site_pos_;
  int num_shards_;
};

class PRKernel : public DSMKernel {
public:
  int iter;
  vector<Page> nodes;
  TypedGlobalTable<PageId, float>* curr_pr_hash;
  TypedGlobalTable<PageId, float>* next_pr_hash;

  void InitKernel() {
    curr_pr_hash = this->get_table<PageId, float>(0);
    next_pr_hash = this->get_table<PageId, float>(1);
  }

  void BuildGraph() {
    if (FLAGS_memory_graph) { return; }
    for (int i = 0; i < FLAGS_shards; ++i) {
       ::BuildGraph(i, FLAGS_shards, FLAGS_nodes, 15);
    }
  }

  RecordFile* get_reader() {
    if (!FLAGS_memory_graph) {
      string file = StringPrintf("%s-%05d-of-%05d-N%05d",
          FLAGS_graph_prefix.c_str(), current_shard(), FLAGS_shards, FLAGS_nodes);
      //FILE* lzo = popen(StringPrintf("lzop -d -c %s", file.c_str()).c_str(), "r");
      //RecordFile * r = new RecordFile(lzo, "r");
      return new RecordFile(file, "r", RecordFile::LZO);
    }

    return new InMemoryGraph(current_shard(), FLAGS_shards);
  }

  void free_reader(RecordFile* r) {
    //pclose(r->fp.filePointer());
    delete r;
  }

  void Initialize() {
    iter = 0;

    next_pr_hash->resize((int)(2 * FLAGS_nodes));
    curr_pr_hash->resize((int)(2 * FLAGS_nodes));
  }

  void WriteStatus() {
    fprintf(stderr, "Iteration %d, PR:: ", get_arg<int>("iteration"));
    fprintf(stderr, "%.2f\n", curr_pr_hash->get(P(0, 0)));
  }

  void PageRankIter() {
    ++iter;

    Page n;
    Timer t;

    RecordFile *r = get_reader();
    while (r->read(&n)) {
      struct PageId p = P(n.site(), n.id());
      next_pr_hash->update(p, random_restart_seed());

      float v = 0;
      if (curr_pr_hash->contains(p)) {
        v = curr_pr_hash->get_local(P(n.site(), n.id()));
      }

      float contribution = kPropagationFactor * v / n.target_site_size();
      for (int i = 0; i < n.target_site_size(); ++i) {
        next_pr_hash->update(P(n.target_site(i), n.target_id(i)), contribution);
      }
    }
    free_reader(r);

    char host[1024];
    gethostname(host, 1024);
    VLOG(1) << "Finished shard " << current_shard() << " on " << host << " in " << t.elapsed();
  }

  void ResetTable() {
    // Move the values computed from the last iteration into the current table.
    swap(curr_pr_hash, next_pr_hash);
    next_pr_hash->clear(current_shard());
  }
};
REGISTER_KERNEL(PRKernel);
REGISTER_METHOD(PRKernel, BuildGraph);
REGISTER_METHOD(PRKernel, Initialize);
REGISTER_METHOD(PRKernel, WriteStatus);
REGISTER_METHOD(PRKernel, PageRankIter);
REGISTER_METHOD(PRKernel, ResetTable);

int Pagerank(ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();
  TOTALRANK = FLAGS_nodes;

  TableDescriptor* pr_desc = new TableDescriptor(0, FLAGS_shards);
  pr_desc->key_marshal = new Marshal<PageId>;
  pr_desc->value_marshal = new Marshal<float>;

  pr_desc->partition_factory = new SparseTable<PageId, float>::Factory;
  pr_desc->block_size = 1000;
  pr_desc->block_info = new PageIdBlockInfo;
  pr_desc->sharder = new SiteSharding;
  pr_desc->accum = new Accumulators<float>::Sum;

  CreateTable<PageId, float>(pr_desc);
  pr_desc->table_id = 1;
  CreateTable<PageId, float>(pr_desc);

  StartWorker(conf);

  Master m(conf);
  if (FLAGS_build_graph) {
    vector<int> shards;
    for (int i = 0; i < conf.num_workers(); ++i) {
      shards.push_back(i);
    }

    m.run_range("PRKernel", "BuildGraph", TableRegistry::Get()->table(0), shards);
    return 0;
  }

  m.restore();

  int &i = m.get_cp_var<int>("iteration", 0);
  if (i == 0) {
    m.run_all("PRKernel", "Initialize",  TableRegistry::Get()->table(0));
  }

  for (; i < FLAGS_iterations; ++i) {
    int curr_pr = (i % 2 == 0) ? 0 : 1;
    int next_pr = (i % 2 == 0) ? 1 : 0;

    {
      RunDescriptor r("PRKernel", "PageRankIter", TableRegistry::Get()->table(curr_pr));
      r.checkpoint_type = CP_MASTER_CONTROLLED;
      // We only need to save the next_pr table, which alternates each iteration.
      r.checkpoint_tables = MakeVector(next_pr);
      r.shards = range(FLAGS_shards);
      m.run_all(r);
    }

    m.run_all("PRKernel", "ResetTable",  TableRegistry::Get()->table(curr_pr));

    {
      RunDescriptor status("PRKernel", "WriteStatus",  TableRegistry::Get()->table(curr_pr));
      status.params.put<int>("iteration", i);
      m.run_one(status);
    }
  }

  return 0;
}
REGISTER_RUNNER(Pagerank);
