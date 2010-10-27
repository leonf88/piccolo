#include "examples/examples.h"
#include "kernel/disk-table.h"

using namespace dsm;
DEFINE_int32(num_nodes, 10000, "");
DEFINE_bool(dump_output, false, "");

static int NUM_WORKERS = 0;
static TypedGlobalTable<int, double>* distance_map;
static RecordTable<PathNode>* nodes;

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

int ShortestPath(ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  distance_map = CreateTable(0, FLAGS_shards, new Sharding::Mod, new Accumulators<double>::Min);
  nodes = CreateRecordTable<PathNode>(1, "testdata/sp-graph.rec*", false);

  StartWorker(conf);
  Master m(conf);

  if (FLAGS_build_graph) {
    BuildGraph(FLAGS_shards, FLAGS_num_nodes, 4);
    return 0;
  }

  PRunOne(distance_map, {
    for (int i = 0; i < FLAGS_num_nodes; ++i) {
      distance_map->update(i, 1e9);
    }

    // Initialize a root node.
    distance_map->update(0, 0);
  });

  for (int i = 0; i < 20; ++i) {
    PMap({n : nodes}, {
        for (int j = 0; j < n.target_size(); ++j) {
          distance_map->update(n.target(j), distance_map->get(n.id()) + 1);
        }
    });
  }

  PRunOne(distance_map, {
    for (int i = 0; i < FLAGS_num_nodes; ++i) {
      if (i % 30 == 0) {
        fprintf(stderr, "\n%5d: ", i);
      }

      int d = (int)distance_map->get(i);
      if (d >= 1000) { d = -1; }
      fprintf(stderr, "%3d ", d);
    }
    fprintf(stderr, "\n");
  });
  return 0;
}
REGISTER_RUNNER(ShortestPath);
