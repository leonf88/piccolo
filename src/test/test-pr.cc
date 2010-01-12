//#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

#include "test/test.pb.h"

#include <algorithm>

using std::swap;

static double TOTALRANK = 0;
static int NUM_WORKERS = 2;

static const double kPropagationFactor = 0.8;
static const int kBlocksize = 10000;

DEFINE_int32(num_nodes, 64, "");
DEFINE_bool(build_graph, false, "");

using namespace upc;

static int BlkModSharding(const int& key, int shards) {
  return (key/kBlocksize) % shards;
}

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

class PRKernel : public DSMKernel {
public:
  int iter;
  TypedTable<int, double>* curr_pr_hash;
  TypedTable<int, double>* next_pr_hash;

  void KernelInit() {
    curr_pr_hash = this->get_table<int, double>(0);
    next_pr_hash = this->get_table<int, double>(1);
  }

  void Initialize() {
    for (int i = 0; i < FLAGS_num_nodes; i++) {
      curr_pr_hash->put(i, (1-kPropagationFactor)*(TOTALRANK/FLAGS_num_nodes));
    }
  }

  void WriteStatus() {
    LOG(INFO) << "iter: " << iter;

    fprintf(stderr, "PR:: ");
    for (int i = 0; i < 10; ++i) {
      fprintf(stderr, "%.2f ", curr_pr_hash->get(i));
    }
    fprintf(stderr, "\n");
  }

  void PageRankIter() {
    static vector<PathNode> nodes;

    ++iter;

    if (nodes.empty()) {
      RecordFile r(StringPrintf("testdata/pr-graph.rec-%05d-of-%05d-N%05d",
                                shard(), NUM_WORKERS, FLAGS_num_nodes), "r");
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
        next_pr_hash->put(n.target(i), kPropagationFactor*v/n.target_size());
      }
    }
  }

  void ClearTable() {
    // Move the values computed from the last iteration into the current table, and reset
    // the values local to our node to the random restart value.
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
};

REGISTER_KERNEL(PRKernel);
REGISTER_METHOD(PRKernel, Initialize);
REGISTER_METHOD(PRKernel, WriteStatus);
REGISTER_METHOD(PRKernel, PageRankIter);
REGISTER_METHOD(PRKernel, ClearTable);


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
    RUN_ONE(m, PRKernel, Initialize);
		for (int i = 0; i < 10; i++) {
			RUN_ALL(m, PRKernel, PageRankIter);
			RUN_ALL(m, PRKernel, ClearTable);
			RUN_ONE(m, PRKernel, WriteStatus);
		}
  } else {
    Worker w(conf);
    w.create_table<int, double>(0, NUM_WORKERS, &BlkModSharding, &Accumulator<double>::sum);
    w.create_table<int, double>(1, NUM_WORKERS, &BlkModSharding, &Accumulator<double>::sum);
    w.Run();
    LOG(INFO) << "Worker " << conf.worker_id() << " :: " << w.get_stats();
  }
}

