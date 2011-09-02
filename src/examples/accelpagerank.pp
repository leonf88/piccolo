// Accelerated PageRan k Runner for Oolong/Piccolo
#include "examples/examples.h"
#include "webgraph.h"

#include <algorithm>
#include <libgen.h>

using namespace dsm;
using namespace std;

static float TOTALRANK = 0;
static int NUM_WORKERS = 2;

static const float kPropagationFactor = 0.8;
static const int kBlocksize = 1000;
static const char kTestPrefix[] = "testdata/pr-graph.rec";

//Not an option for this runner
//DEFINE_bool(apr_memory_graph, false,
//            "If true, the web graph will be generated on-demand.");

DEFINE_string(apr_graph_prefix, kTestPrefix, "Path to web graph.");
DEFINE_int32(apr_nodes, 10000, "");
DEFINE_double(apr_tol, 1e-6, "threshold for updates");
DEFINE_double(apr_d, 0.85, "alpha/restart probability");
DEFINE_int32(ashow_top, 10, "number of top results to display");

#define PREFETCH 1024

DEFINE_string(apr_convert_graph, "", "Path to WebGraph .graph.gz database to convert");

static float powerlaw_random(float dmin, float dmax, float n) {
  float r = (float)random() / RAND_MAX;
  return pow((pow(dmax, n) - pow(dmin, n)) * pow(r, 3) + pow(dmin, n), 1.0/n);
}

static float random_restart_seed() {
  return (1-kPropagationFactor)*(TOTALRANK/FLAGS_apr_nodes);
}

// I'd like to use a pair here, but for some reason they fail to count
// as POD types according to C++.  Sigh.
struct APageId {
  int64_t site : 32;
  int64_t page : 32;
};

struct APageInfo {
  string url;
  vector<APageId> adj;
};

//-----------------------------------------------
// Marshalling for APageInfo type
//-----------------------------------------------
namespace dsm {
	template <> struct Marshal<APageInfo> : MarshalBase {
		static void marshal(const APageInfo& t, string *out) {
			int i;
			struct APageId j;
			int len = t.url.size();
			out->append((char*)&len,sizeof(int));
			out->append(t.url.c_str(),len);
			len = t.adj.size();
			out->append((char*)&len,sizeof(int));
			for(i = 0; i < len; i++) {
				j = t.adj[i];
				out->append((char*)&j,sizeof(struct APageId));
			}
		}
		static void unmarshal(const StringPiece &s, APageInfo* t) {
			int i;
			struct APageId j;
			int len,len2;
			memcpy(&len,s.data,sizeof(int));
			t->url.clear();
			t->url.append(s.data+1*sizeof(int),len);
			t->adj.clear();
			memcpy(&len2,s.data+1*sizeof(int)+len,sizeof(int));
			for(i = 0; i < len2; i++) {
				memcpy(&j,s.data+(2)*sizeof(int)+len+i*sizeof(struct APageId),sizeof(struct APageId));
				t->adj.push_back(j);
			}
		}
	};
}
			
bool operator==(const APageId& a, const APageId& b) {
  return a.site == b.site && a.page == b.page;
}

struct AccelPRStruct {
  int64_t L;				//number of outgoing links
  float pr_int;			//internal pagerank
  float pr_ext;			//external pagerank
};

namespace std { namespace tr1 {
template <>
struct hash<APageId> {
  size_t operator()(const APageId& p) const {
    return SuperFastHash((const char*)&p, sizeof p);
  }
};
} }

struct SiteSharding : public Sharder<APageId> {
  int operator()(const APageId& p, int nshards) {
    return p.site % nshards;
  }
};

struct APageIdBlockInfo : public BlockInfo<APageId> {
  APageId start(const APageId& k, int block_size)  {
    APageId p = { k.site, k.page - (k.page % block_size) };
    return p;
  }

  int offset(const APageId& k, int block_size) {
    return k.page % block_size;
  }
};

TypedGlobalTable<APageId, AccelPRStruct> *prs;
TypedGlobalTable<APageId, APageInfo>* apages;
DiskTable<uint64_t, Page> *pagedb;

static vector<int> InitSites() {
  vector<int> site_sizes;
  srand(0);
  for (int n = 0; n < FLAGS_apr_nodes; ) {
    int c = powerlaw_random(1, min(50000,
                                   (int)(100000. * FLAGS_apr_nodes / 100e6)), 0.001);
    site_sizes.push_back(c);
    n += c;
  }
  return site_sizes;
}

static vector<int> site_sizes;

static void BuildGraph(int shard, int nshards, int nodes, int density) {
  char* d = strdup(FLAGS_apr_graph_prefix.c_str());
  File::Mkdirs(dirname(d));

  string target = StringPrintf("%s-%05d-of-%05d-N%05d", FLAGS_apr_graph_prefix.c_str(), shard, nshards, nodes);

  if (File::Exists(target)) {
    return;
  }

  srand(shard);
  Page n;
  RecordFile out(target, "w", RecordFile::NONE);
  // Only sites with site_id % nshards == shard are in this shard.
  for (int i = shard; i < site_sizes.size(); i += nshards) {
    PERIODIC(1, LOG(INFO) << "Working: Shard -- " << shard << " of " << nshards 
                          << "; site " << i << " of " << site_sizes.size());
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

static void WebGraphAPageIds(WebGraph::Reader *wgr, vector<APageId> *out) {
  WebGraph::URLReader *r = wgr->newURLReader();
  struct APageId pid = {-1, -1};
  string prev, url;
  int prevHostLen = 0;
  int i = 0;

  out->reserve(wgr->nodes);

  while (r->readURL(&url)) {
    if (i++ % 100000 == 0)
      LOG(INFO) << "Reading URL " << i-1 << " of " << wgr->nodes;

    // Get host part
    int hostLen = url.find('/', 8);
    CHECK(hostLen != url.npos) << "Failed to split host in URL " << url;
    ++hostLen;

    if (prev.compare(0, prevHostLen, url, 0, hostLen) == 0) {
      // Same site
      ++pid.page;
    } else {
      // Different site
      ++pid.site;
      pid.page = 0;

      swap(prev, url);
      prevHostLen = hostLen;
    }

    out->push_back(pid);
  }

  delete r;

  LOG(INFO) << pid.site+1 << " total sites read";
}

static void ConvertGraph(string path, int nshards) {
  WebGraph::Reader r(path);
  vector<APageId> APageIds;
  WebGraphAPageIds(&r, &APageIds);

  char* d = strdup(FLAGS_apr_graph_prefix.c_str());
  File::Mkdirs(dirname(d));

  RecordFile *out[nshards];
  for (int i = 0; i < nshards; ++i) {
    string target = StringPrintf("%s-%05d-of-%05d-N%05d", FLAGS_apr_graph_prefix.c_str(), i, nshards, r.nodes);
    out[i] = new RecordFile(target, "w", RecordFile::NONE);
  }

  // XXX Maybe we should take at most FLAGS_apr_nodes nodes
  const WebGraph::Node *node;
  Page n;
  while ((node = r.readNode())) {
    if (node->node % 100000 == 0)
      LOG(INFO) << "Reading node " << node->node << " of " << r.nodes;
    APageId src = APageIds.at(node->node);
    n.Clear();
    n.set_site(src.site);
    n.set_id(src.page);
    for (unsigned int i = 0; i < node->links.size(); ++i) {
      APageId dest = APageIds.at(node->links[i]);
      n.add_target_site(dest.site);
      n.add_target_id(dest.page);
    }
    out[src.site % nshards]->write(n);
  }

  for (int i = 0; i < nshards; ++i)
    delete out[i];
}


namespace dsm {
struct AccelPRTrigger : public Trigger<APageId, AccelPRStruct> {
  public:
    void Fire(const APageId* key, AccelPRStruct* value, const AccelPRStruct& newvalue, bool* doUpdate, bool isNew) {
//      fprintf(stderr,"Trigger fired");
      *doUpdate = true;
      if (isNew) {
        value->L = newvalue.L;
        value->pr_int = FLAGS_apr_d*newvalue.pr_ext;
      } else {
        value->pr_int += (FLAGS_apr_d*newvalue.pr_ext)/newvalue.L;
      }
      fprintf(stderr,"diff %f tol %f\n",abs(value->pr_int-value->pr_ext),FLAGS_apr_tol);
      if (abs(value->pr_int-value->pr_ext) >= FLAGS_apr_tol) {
        fprintf(stderr,"Propagating trigger for %ld-%ld\n",key->site,key->page);
        APageInfo p = apages->get(*key);
        struct AccelPRStruct updval = { p.adj.size(), 0, value->pr_int-value->pr_ext };
        vector<APageId>::iterator it = p.adj.begin();
        for(; it != p.adj.end(); it++) {
          struct APageId neighbor = { it->site, it->page };
          prs->enqueue_update(neighbor,updval);
        }
        value->pr_ext = value->pr_int;
      }
      return;
    }
    bool LongFire(const APageId key) {
      return false;
    }
}; };

int AccelPagerank(ConfigData& conf) {
  site_sizes = InitSites();

  NUM_WORKERS = conf.num_workers();
  TOTALRANK = FLAGS_apr_nodes;

  prs = CreateTable(0, FLAGS_shards, new SiteSharding, (Trigger<APageId, AccelPRStruct>*)new AccelPRTrigger);

  if (FLAGS_build_graph) {
    if (NetworkThread::Get()->id() == 0) {
      LOG(INFO) << "Building graph with " << FLAGS_shards << " shards; " 
                << FLAGS_apr_nodes << " nodes.";
      for (int i = 0; i < FLAGS_shards; ++i) {
         BuildGraph(i, FLAGS_shards, FLAGS_apr_nodes, 15);
      }
    }
    return 0;
  }

  //no RecordTable option in this runner, MemoryTable only
  apages = CreateTable(1, FLAGS_shards, new SiteSharding, new Accumulators<APageInfo>::Replace);

  //Also need to load pages
  if (FLAGS_apr_convert_graph.empty()) {
    pagedb = CreateRecordTable<Page>(2, FLAGS_apr_graph_prefix + "*", false);
  }

  StartWorker(conf);
  Master m(conf);

  if (!FLAGS_apr_convert_graph.empty()) {
    ConvertGraph(FLAGS_apr_convert_graph, FLAGS_shards);
    return 0;
  }

  m.restore();

  PMap({ n : pagedb },  {
    struct APageId p = { n.site(), n.id() };
    struct APageInfo info;
    info.adj.clear();
    for(int i=0; i<n.target_site_size(); i++) {
      struct APageId neigh = { n.target_site(i), n.target_id(i) };
      info.adj.push_back(neigh);
    }
    apages->update(p,info);
  });

  PRunAll(apages, {
    TypedTableIterator<APageId, APageInfo> *it =
      apages->get_typed_iterator(current_shard());
    for(; !it->done(); it->Next()) {
      struct AccelPRStruct initval = { 1, 0, (1-FLAGS_apr_d)/(FLAGS_apr_nodes*FLAGS_apr_d) };
      prs->update(it->key(),initval);
    }
  });

  PRunOne(prs, {
    fprintf(stdout,"PageRank complete, tabulating results...\n");
    float pr_min = 1, pr_max = 0, pr_sum = 0;
    struct APageId toplist[FLAGS_ashow_top];
    float topscores[FLAGS_ashow_top];
    int totalpages = 0;

    for(int shard=0; shard < prs->num_shards(); shard++) {
      TypedTableIterator<APageId, AccelPRStruct> *it = prs->get_typed_iterator(shard,PREFETCH);

      for(; !it->done(); it->Next()) {
        totalpages++;
        if (it->value().pr_ext > pr_max)
          pr_max = it->value().pr_ext;
        if (it->value().pr_ext > topscores[FLAGS_ashow_top-1]) {
          topscores[FLAGS_ashow_top-1] = it->value().pr_ext;
          toplist[FLAGS_ashow_top-1] = it->key();
          for(int i=FLAGS_ashow_top-2; i>=0; i--) {
            if (topscores[i] < topscores[i+1]) {
              float a = topscores[i];
              struct APageId b = toplist[i];
              topscores[i] = topscores[i+1];
              toplist[i] = toplist[i+1];
              topscores[i+1] = a;
              toplist[i+1] = b;
            } else {
              break;
            }
          }
        }
        if (it->value().pr_ext < pr_min)
          pr_min = it->value().pr_ext;
        pr_sum += it->value().pr_ext;
      }
    }
    float pr_avg = pr_sum/totalpages;
    fprintf(stdout,"RESULTS: min=%f, max=%f, sum=%f, avg=%f [%d pages in %d shards]\n",pr_min,pr_max,pr_sum,pr_avg,totalpages,prs->num_shards());
    fprintf(stdout,"Top Pages:\n");
    for(int i=0;i<FLAGS_ashow_top;i++) {
      fprintf(stdout,"%d\t%f\t%ld-%ld\n",i+1,topscores[i],toplist[i].site,toplist[i].page);
    }
  });

  return 0;
}
REGISTER_RUNNER(AccelPagerank);
