#include "client.h"
#include "test/test.pb.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <algorithm>

using namespace dsm;
using namespace std;

static double TOTALRANK = 0;
static int NUM_WORKERS = 2;

static const double kPropagationFactor = 0.8;
static const int kBlocksize = 1000;
static const char kTestPrefix[] = "testdata/pr-graph.rec";

DEFINE_int32(shards, 10, "");
DEFINE_int32(iterations, 10, "");

DEFINE_bool(build_graph, false, "");
DEFINE_int32(nodes, 10000, "");

DEFINE_bool(use_block_sharding, true, "");

DEFINE_string(graph_prefix, kTestPrefix, "Path to web graph.");

static int (*sharding)(const int& key, int shards);
static int BlkModSharding(const int& key, int shards) { return (key/kBlocksize) % shards; }

void BuildGraph(int shards, int nodes, int density) {
  fprintf(stderr, "Building graph: ");
  vector<RecordFile*> out(shards);
  File::Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(StringPrintf("%s-%05d-of-%05d-N%05d",
                                         kTestPrefix, i, shards, nodes), "w");
  }

  Page n;
  for (int i = 0; i < nodes; i++) {
    n.Clear();
    n.set_id(i);

    for (int j = 0; j < density; j++) { n.add_target((i + j * 1000) % nodes); }
    for (int j = 0; j < density; j++) { n.add_target(j); }

    out[sharding(i,shards)]->write(n);
    EVERY_N(max(1, (nodes / 50)), fprintf(stderr, "."));
  }

  fprintf(stderr, " done.\n");

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

static double random_restart_seed() {
  return (1-kPropagationFactor)*(TOTALRANK/FLAGS_nodes);
}

class PRKernel : public DSMKernel {
public:
  int iter;
  vector<Page> nodes;
  TypedGlobalTable<int, double>* curr_pr_hash;
  TypedGlobalTable<int, double>* next_pr_hash;

  void KernelInit() {
    curr_pr_hash = this->get_table<int, double>(0);
    next_pr_hash = this->get_table<int, double>(1);
    iter = 0;
  }

  void Initialize() {
    for (int i = current_shard(); i < FLAGS_nodes; i += get_table(0)->num_shards()) {
      next_pr_hash->put(i, random_restart_seed());
    }
  }

  void WriteStatus() {
    fprintf(stderr, "Iteration %d, PR:: ", iter);
    for (int i = 0; i < 30; ++i) {
      fprintf(stderr, "%.2f ", curr_pr_hash->get(i));
    }
    fprintf(stderr, "\n");
  }

  void PageRankIter() {
    ++iter;

    RecordFile r(StringPrintf(FLAGS_graph_prefix + "-%05d-of-%05d-N%05d",
                              current_shard(), FLAGS_shards, FLAGS_nodes), "r");
    Page n;
    Timer t;
    while (r.read(&n)) {
      double v = curr_pr_hash->get_local(n.id());
      double contribution = kPropagationFactor * v / n.target_size();
      for (int i = 0; i < n.target_size(); ++i) {
//        LOG_EVERY_N(INFO, 1000) << "Adding: " <<  MP(n.id(), n.target(i))
//                             << " : " << MP(n.target(i), next_pr_hash->get(n.target(i)), contribution);
        next_pr_hash->put(n.target(i), contribution);
      }
    }

    char host[1024];
    gethostname(host, 1024);
    VLOG(1) << "Finished shard " << current_shard() << " on " << host << " in " << t.elapsed();
  }

  void ResetTable() {
    // Move the values computed from the last iteration into the current table, and reset
    // the values local to our node to the random restart value.
    swap(curr_pr_hash, next_pr_hash);
    next_pr_hash->clear(current_shard());

    TypedTable<int, double>::Iterator *it = curr_pr_hash->get_typed_iterator(current_shard());
    while (!it->done()) {
      next_pr_hash->put(it->key(), random_restart_seed());
      it->Next();
    }
  }
};
REGISTER_KERNEL(PRKernel);
REGISTER_METHOD(PRKernel, Initialize);
REGISTER_METHOD(PRKernel, WriteStatus);
REGISTER_METHOD(PRKernel, PageRankIter);
REGISTER_METHOD(PRKernel, ResetTable);

int main(int argc, char **argv) {
	Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_slots(16);

  // Cap address space at 2G.
  struct rlimit rl;
  rl.rlim_cur = 1l << 31;
  rl.rlim_max = 1l << 31;
  setrlimit(RLIMIT_AS, &rl);

  sharding = FLAGS_use_block_sharding ? &BlkModSharding : &ModSharding;

	NUM_WORKERS = conf.num_workers();
	TOTALRANK = FLAGS_nodes;

	Registry::create_table<int, double>(0, FLAGS_shards, sharding, &Accumulator<double>::sum);
	Registry::create_table<int, double>(1, FLAGS_shards, sharding, &Accumulator<double>::sum);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    if (FLAGS_build_graph) {
      BuildGraph(FLAGS_shards, FLAGS_nodes, 10);
    }

    Master m(conf);
    RUN_ALL(m, PRKernel, Initialize, 0);
		for (int i = 0; i < FLAGS_iterations; i++) {
			RUN_ALL(m, PRKernel, PageRankIter, 0);
			RUN_ALL(m, PRKernel, ResetTable, 0);
			RUN_ONE(m, PRKernel, WriteStatus, 0);
		}
  } else {
    Worker w(conf);
    w.Run();
    LOG(INFO) << "Worker stats: " << conf.worker_id() << " :: " << w.get_stats();
  }
}

