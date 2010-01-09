#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

#include "test/test.pb.h"

#include <algorithm>

using std::swap;

static double PROP = 0.8;
static int BLK = 10000;
static double TOTALRANK = 0;

DEFINE_int32(num_nodes, 64, "");
DEFINE_bool(build_graph, false, "");

static int NUM_WORKERS = 2;

using namespace upc;

static TypedTable<int, double>* curr_pr_hash;
static TypedTable<int, double>* next_pr_hash;

static int BlkModSharding(const int& key, int shards) { return (key/BLK) % shards; }

void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(StringPrintf("testdata/pr-graph.rec-%05d-of-%05d-N%05d", i, shards, nodes), "w");
  }

  fprintf(stderr, "Building graph: ");
  PathNode n;
  for (int i = 0; i < nodes; i++) {
    n.Clear();
    n.set_id(i);

    for (int j = 0; j < density; j++) {
      n.add_target(random() % nodes);
    }
    
    out[BlkModSharding(i,shards)]->write(n);
    EVERY_N((nodes / 50), fprintf(stderr, "."));
  }

  fprintf(stderr, " done.\n");

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

void Initialize() {
  TypedTable<int, double>* curr_pr_hash(w->get_typed_table<int, double>(0, shard));

  for (int i = 0; i < FLAGS_num_nodes; i++) {
		curr_pr_hash->put(i, (1-PROP)*(TOTALRANK/FLAGS_num_nodes));
  }
}

static int iter = 0;

void WriteStatus() {
  TypedTable<int, double>* curr_pr_hash(w->get_typed_table<int, double>(0, shard));
  LOG(INFO) << "iter: " << iter;

  fprintf(stderr, "PR:: ");
  for (int i = 0; i < 10; ++i) {
    fprintf(stderr, "%.2f ", curr_pr_hash->get(i));
  }
  fprintf(stderr, "\n");
}

void PageRankIter(int shard) {
  static vector<PathNode> nodes;

  ++iter;

  if (nodes.empty()) {
    RecordFile r(StringPrintf("testdata/pr-graph.rec-%05d-of-%05d-N%05d", shard, NUM_WORKERS, FLAGS_num_nodes), "r");
    PathNode n;
    while (r.read(&n)) {
      nodes.push_back(n);
    }
  }

  for (int i = 0; i < nodes.size(); ++i) {
    PathNode &n = nodes[i];
	  double v = curr_pr_hash->get(n.id());
//	  LOG(INFO) << n.id();
    for (int i = 0; i < n.target_size(); ++i) {
//      LOG(INFO) << "Adding: " << PROP * v / n.target_size() << " to " << n.target(i);
      next_pr_hash->put(n.target(i), PROP*v/n.target_size());
    }
	}
}

void ClearTable(int shard) {
  // Move the values computed from the last iteration into the current table, and reset
  // the values local to our node to the random restart value.
  TypedTable<int, double>* curr_pr_hash(w->get_typed_table<int, double>(0, shard));
  TypedTable<int, double>* next_pr_hash(w->get_typed_table<int, double>(1, shard));

  curr_pr_hash->clear();
	TypedTable<int, double>::Iterator *it = next_pr_hash->get_typed_iterator();
	while (!it->done()) {
//	  LOG(INFO) << "Setting: " << it->key() << " :: " << (1-PROP)*(TOTALRANK/FLAGS_num_nodes);
	  curr_pr_hash->put(it->key(), it->value());
		it->Next();
	}
	delete it;

	next_pr_hash->clear();
}

REGISTER_KERNEL(Initialize);
REGISTER_KERNEL(WriteStatus);
REGISTER_KERNEL(PageRankIter);
REGISTER_KERNEL(ClearTable);

int main(int argc, char **argv) {
	Init(argc, argv);
  
  ConfigData conf;                                                            
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);

	NUM_WORKERS = conf.num_workers();
	TOTALRANK = FLAGS_num_nodes;

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    if (FLAGS_build_graph) {
      BuildGraph(NUM_WORKERS, FLAGS_num_nodes, 10);
    }

    Master m(conf);
		m.run_one(&Initialize);
		for (int i = 0; i < 10; i++) {
			m.run_all(&PageRankIter);
			m.run_all(&ClearTable);
//			m.run_one(&WriteStatus);
		}
  } else {
    Worker w(conf);
    w.Run();
    curr_pr_hash = w->get_typed_table<int, double>(0);
    next_pr_hash = w->get_typed_table<int, double>(1);

    LOG(INFO) << "Worker " << conf.worker_id() << " :: " << w->get_stats();
  }
}

