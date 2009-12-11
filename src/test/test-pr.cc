#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

#include "test/test.pb.h"

#include <algorithm>

using std::swap;

#define PROP 0.8
#define BLK 2
static double TOTALRANK = 0;

DEFINE_int32(num_nodes, 64, "");

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

  PathNode n;
  for (int i = 0; i < nodes; i++) {
    n.Clear();
    n.set_id(i);
    for (int j = 0; j < density; j++) {
      n.add_target(random() % nodes);
    }

		/* build complete graph
		for (int j = 0; j < FLAGS_num_nodes; j++) {
			n.add_target(j);
		}
		*/

    out[BlkModSharding(i,shards)]->write(n);
    LOG_EVERY_N(INFO, 10000) << "Working; created " << i << " nodes.";
  }

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

void Initialize() {
  for (int i = 0; i < FLAGS_num_nodes; i++) {
		curr_pr_hash->put(i, (1-PROP)*(TOTALRANK/FLAGS_num_nodes));
  }
}

static int iter = 0;

void WriteStatus() {
  LOG(INFO) << "iter: " << iter;

  fprintf(stderr, "PR:: ");
  for (int i = 0; i < 10; ++i) {
    fprintf(stderr, "%.2f ", curr_pr_hash->get(i));
  }
  fprintf(stderr, "\n");
}

void PageRankIter() {
	int my_thread = curr_pr_hash->get_typed_iterator()->owner()->info().owner_thread;
  ++iter;

	RecordFile r(StringPrintf("testdata/pr-graph.rec-%05d-of-%05d-N%05d", my_thread, NUM_WORKERS, FLAGS_num_nodes), "r");
  PathNode n;
	while (r.read(&n)) {
	  double v = curr_pr_hash->get(n.id());
//	  LOG(INFO) << n.id();
    for (int i = 0; i < n.target_size(); ++i) {
//      LOG(INFO) << "Adding: " << PROP * v / n.target_size() << " to " << n.target(i);
      next_pr_hash->put(n.target(i), PROP*v/n.target_size());
    }
	}
}

void ClearTable() {
  // Move the values computed from the last iteration into the current table, and reset
  // the values local to our node to the random restart value.

	swap(curr_pr_hash, next_pr_hash);
	next_pr_hash->clear();
	TypedTable<int, double>::Iterator *it = curr_pr_hash->get_typed_iterator();
	while (!it->done()) {
//	  LOG(INFO) << "Setting: " << it->key() << " :: " << (1-PROP)*(TOTALRANK/FLAGS_num_nodes);
		next_pr_hash->put(it->key(), (1-PROP)*(TOTALRANK/FLAGS_num_nodes));
		it->Next();
	}
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
		BuildGraph(NUM_WORKERS, FLAGS_num_nodes, 10); 
    Master m(conf);
		m.run_one(&Initialize);
		m.run_one(&WriteStatus);
		for (int i = 0; i < 50; i++) {
			m.run_all(&PageRankIter);
			m.run_all(&ClearTable);
			m.run_one(&WriteStatus);
		}
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
		curr_pr_hash = w.CreateTable<int, double>(&BlkModSharding, &Accumulator<double>::sum);
		next_pr_hash = w.CreateTable<int, double>(&BlkModSharding, &Accumulator<double>::sum);
    w.Run(); 
  }
}

