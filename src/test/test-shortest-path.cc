#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"
#include "test/test.pb.h"

using namespace upc;
DEFINE_int32(num_nodes, 100000, "");

static int NUM_WORKERS = 0;
static TypedTable<int, double>* distance;

void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(StringPrintf("testdata/graph.rec-%05d-of-%05d", i, shards), "w");
  }

  for (int i = 0; i < nodes; i++) {
    PathNode n;
    n.set_id(i);

    for (int j = 0; j < density; j++) {
      n.add_target(random() % nodes);
    }

    out[i % shards]->write(n);
    LOG_EVERY_N(INFO, 10000) << "Working; created " << i << " nodes.";
  }

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

void Initialize() {
  for (int i = 0; i < FLAGS_num_nodes; ++i) {
    distance->put(i, 1e9);
  }

  // Initialize a root node.
  distance->put(0, 0);
}

void Propagate() {
  int my_thread = distance->info().owner_thread;
  RecordFile r(StringPrintf("testdata/graph.rec-%05d-of-%05d", my_thread, NUM_WORKERS), "r");
  PathNode n;
  while (r.read(&n)) {
    for (int i = 0; i < n.target_size(); ++i) {
      distance->put(n.target(i), distance->get(n.id()) + 1);
    }
  }
}

void DumpDistances() {
  LocalFile f("testdata/distances.dot", "w");

  f.Printf("digraph G{\n");
  for (int i = 0; i < NUM_WORKERS; ++i) {
    RecordFile r(StringPrintf("testdata/graph.rec-%05d-of-%05d", i, NUM_WORKERS), "r");
    PathNode n;
    while (r.read(&n)) {
      f.Printf("node_%d [label=\"d=%d\"];\n", n.id(), (int)distance->get(n.id()));
      for (int i = 0; i < n.target_size(); ++i) {
        if (distance->get(n.target(i)) == distance->get(n.id()) + 1) {
          f.Printf("node_%d -> node_%d;\n", n.id(), n.target(i));
        }
      }
    }
  }

  f.Printf("}");
}

REGISTER_KERNEL(Initialize);
REGISTER_KERNEL(Propagate);
REGISTER_KERNEL(DumpDistances);

int main(int argc, char **argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  NUM_WORKERS = conf.num_workers();

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    BuildGraph(NUM_WORKERS, FLAGS_num_nodes, 4);

    Master m(conf);
    m.run_one(&Initialize);
    for (int i = 0; i < 20; ++i) {
      m.run_all(&Propagate);
    }
    m.run_one(&DumpDistances);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    distance = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::min);
    w.Run();
  }

  LOG(INFO) << "Exiting.";

}
