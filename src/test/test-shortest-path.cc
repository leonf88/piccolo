#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

using namespace upc;

static int NUM_NODES = 10000;
static int NUM_WORKERS = 0;
static TypedTable<int, double>* distance;

void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(StringPrintf("testdata/nodes.rec-%05d-of-%05d", i, shards), "w");
  }

  for (int i = 0; i < nodes; i++) {
    PathNode n;
    n.set_id(i);
    n.set_dirty(i == 0);

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
  for (int i = 0; i < NUM_NODES; ++i) {
    distance->put(i, 1e9);
  }

  // Initialize a root node.
  distance->put(0, 0);
}

void Propagate() {
  int my_thread = distance->info().owner_thread;
  RecordFile r(StringPrintf("testdata/nodes.rec-%05d-of-%05d", my_thread, NUM_WORKERS), "r");
  PathNode n;
  while (r.read(&n)) {
    for (int i = 0; i < n.target_size(); ++i) {
      distance->put(n.target(i), distance->get(n.id()) + 1);
    }
  }
}

void DumpDistances() {
  for (int i = 0; i < NUM_NODES; ++i) {
    LOG(INFO) << "node: " << i << " :: " << distance->get(i);
  }
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
    BuildGraph(NUM_WORKERS, NUM_NODES, 10);

    Master m(conf);
    m.run(&Initialize);
    for (int i = 0; i < 10; ++i) {
      m.run(&Propagate);
    }
    m.run(DumpDistances);
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
    distance = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::min);
    w.Run();
  }

  LOG(INFO) << "Exiting.";

}
