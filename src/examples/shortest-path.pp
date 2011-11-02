#include "examples/examples.h"
#include "kernel/disk-table.h"

using std::vector;

using namespace piccolo;

DEFINE_int32(num_nodes, 10000, "");
DEFINE_bool(dump_output, false, "");

static int NUM_WORKERS = 0;
static TypedGlobalTable<int, double>* distance_map;
static RecordTable<PathNode>* nodes;

static void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  File::Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(
        StringPrintf("testdata/sp-graph.rec-%05d-of-%05d", i, shards), "w");
  }

  srandom(nodes);	//repeatable graphs
  fprintf(stderr, "Building graph: ");

  for (int i = 0; i < nodes; i++) {
    PathNode n;
    n.set_id(i);

    for (int j = 0; j < density; j++) {
      n.add_target(random() % nodes);
    }

    out[i % shards]->write(n);
    if (i % (nodes / 50) == 0) {
      fprintf(stderr, ".");
    }
  }
  fprintf(stderr, "\n");

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

class sssp_kern : public DSMKernel {
public:
  void sssp_driver() {
    for(int i=0;i<100;i++) {
    TypedTableIterator<long unsigned int, PathNode>* it = nodes->get_typed_iterator(current_shard());
    for(;!it->done(); it->Next()) {
        PathNode n = it->value();
        for (int j = 0; j < n.target_size(); ++j) {
          distance_map->update(n.target(j), distance_map->get(n.id()) + 1);
        }
    }
    delete it;
    }
  }
};

REGISTER_KERNEL(sssp_kern);
REGISTER_METHOD(sssp_kern,sssp_driver);

int ShortestPath(const ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  distance_map = CreateTable(0, FLAGS_shards, new Sharding::Mod,
                             new Accumulators<double>::Min);
  if (!FLAGS_build_graph) {
  nodes = CreateRecordTable < PathNode > (1, "testdata/sp-graph.rec*", false);
  }

  StartWorker(conf);
  Master m(conf);

  if (FLAGS_build_graph) {
    BuildGraph(FLAGS_shards, FLAGS_num_nodes, 4);
    return 0;
  }

  if (!m.restore()) {
  PRunAll(distance_map, {
          vector<double> v;
    for(int i=current_shard();i<FLAGS_num_nodes;i+=FLAGS_shards) {
      distance_map->update(i, 1e9);   //Initialize all distances to very large.
    }

    // Initialize a root node.
    distance_map->update(0, 0);
  });
  }

  //Start the timer!
  struct timeval start_time, end_time;
  gettimeofday(&start_time, NULL);

  vector<int> cptables;
  for (int i = 0; i < 100; ++i) {
    cptables.clear();
    cptables.push_back(0);

    RunDescriptor sp_rd("sssp_kern", "sssp_driver", nodes, cptables);

    //Switched to a RunDescriptor so that checkpointing can be used.
    sp_rd.checkpoint_type = CP_CONTINUOUS;
    m.run_all(sp_rd);

  }

//Finish the timer!
  gettimeofday(&end_time, NULL);
  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec)
      * 1000000 + (end_time.tv_usec - start_time.tv_usec);
  fprintf(stderr, "Total SSSP time: %.3f seconds\n", totaltime / 1000000.0);

  if (FLAGS_dump_output) {
    PRunOne(distance_map, {
        for (int i = 0; i < FLAGS_num_nodes; ++i) {
          if (i % 30 == 0) {
            fprintf(stderr, "\n%5d: ", i);
          }

          int d = (int)distance_map->get(i);
          if (d >= 1000) {d = -1;}
          fprintf(stderr, "%3d ", d);
        }
        fprintf(stderr, "\n");
    });
  }
  return 0;
}
REGISTER_RUNNER(ShortestPath);
