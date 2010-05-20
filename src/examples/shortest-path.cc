#include "client.h"
#include "examples/examples.pb.h"

using namespace dsm;
DEFINE_int32(num_nodes, 10000, "");

DEFINE_bool(dump_output, false, "");

static int NUM_WORKERS = 0;
static TypedGlobalTable<int, double>* distance;

static void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  File::Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(StringPrintf("testdata/sp-graph.rec-%05d-of-%05d", i, shards), "w");
  }

  fprintf(stderr, "Building graph: ");

  for (int i = 0; i < nodes; i++) {
    PathNode n;
    n.set_id(i);

    for (int j = 0; j < density; j++) {
      n.add_target(random() % nodes);
    }

    out[i % shards]->write(n);
    if (i % (nodes / 50) == 0) { fprintf(stderr, "."); }
  }
  fprintf(stderr, "\n");

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

struct ShortestPathKernel : public DSMKernel {
  vector<PathNode> local_nodes;
  void Initialize() {
    for (int i = 0; i < FLAGS_num_nodes; ++i) {
      distance->put(i, 1e9);
    }

    // Initialize a root node.
    distance->put(0, 0);
  }

  void Propagate() {
    if (local_nodes.empty()) {
      RecordFile r(StringPrintf("testdata/sp-graph.rec-%05d-of-%05d", current_shard(), FLAGS_shards), "r");
      PathNode n;
      while (r.read(&n)) {
        local_nodes.push_back(n);
      }
    }

    for (int i = 0; i < local_nodes.size(); ++i) {
      const PathNode &n = local_nodes[i];
      for (int j = 0; j < n.target_size(); ++j) {
        distance->put(n.target(j), distance->get(n.id()) + 1);
      }
    }
  }

  void DumpDistances() {
    for (int i = 0; i < FLAGS_num_nodes; ++i) {
      if (i % 30 == 0) {
        fprintf(stderr, "\n%5d: ", i);
      }

      int d = (int)distance->get(i);
      if (d >= 1000) { d = -1; }
      fprintf(stderr, "%3d ", d);
    }
  }
};

REGISTER_KERNEL(ShortestPathKernel);
REGISTER_METHOD(ShortestPathKernel, Initialize);
REGISTER_METHOD(ShortestPathKernel, Propagate);
REGISTER_METHOD(ShortestPathKernel, DumpDistances);

int ShortestPath(ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  distance = CreateTable(0, FLAGS_shards, new Sharding::Mod, new Accumulators<double>::Min);

  if (!StartWorker(conf)) {
    BuildGraph(FLAGS_shards, FLAGS_num_nodes, 4);

    Master m(conf);
    m.run_one("ShortestPathKernel", " Initialize",  0);
    for (int i = 0; i < 20; ++i) {
      m.run_all("ShortestPathKernel", " Propagate",  0);
    }

    if (FLAGS_dump_output) {
      m.run_one("ShortestPathKernel", " DumpDistances",  0);
    }
  }
  return 0;
}
REGISTER_RUNNER(ShortestPath);
