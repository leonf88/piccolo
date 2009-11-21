#include "util/common.h"
#include "util/file.h"
#include "worker/worker.h"
#include "master/master.h"

#include "test/test.pb.h"

#define PROP 0.8
#define TOTALRANK 10.0

static int NUM_NODES = 100;
static int NUM_WORKERS = 2;

using namespace upc;

static TypedTable<int, double>* curr_pr_hash;
static TypedTable<int, double>* next_pr_hash;

void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(StringPrintf("testdata/nodes.rec-%05d-of-%05d", i, shards), "w");
  }

  for (int i = 0; i < nodes; i++) {
    PathNode n;
    n.set_id(i);
/*
    for (int j = 0; j < density; j++) {
      n.add_target(random() % nodes);
    }
		*/
		for (int j = 0; j < nodes; j++) {
			n.add_target(j);
		}

    out[i % shards]->write(n);
    LOG_EVERY_N(INFO, 10000) << "Working; created " << i << " nodes.";
  }

  for (int i = 0; i < shards; ++i) {
    delete out[i];
  }
}

void Initialize() {
  for (int i = 0; i < NUM_NODES; i++) {
		curr_pr_hash->put(i,TOTALRANK/NUM_NODES);
  }
}


void PageRankIter() {
	int my_thread = curr_pr_hash->get_typed_iterator()->owner()->info().owner_thread;
	LOG(INFO) << "PageRankIter " << my_thread << " iter ??";

	LOG(INFO) << "Iter pr[0] " << curr_pr_hash->get(0);

	RecordFile r(StringPrintf("testdata/nodes.rec-%05d-of-%05d", my_thread, NUM_WORKERS), "r");
  PathNode n;
	while (r.read(&n)) {
   for (int i = 0; i < n.target_size(); ++i) {
     next_pr_hash->put(n.target(i), PROP*curr_pr_hash->get(n.id())/n.target_size());
    }
	}
}

void ClearTable() {
	TypedTable<int, double>* tmp;

	curr_pr_hash->clear();
	tmp = curr_pr_hash;
	curr_pr_hash = next_pr_hash;
	next_pr_hash = tmp;

	TypedTable<int, double>::Iterator *it = curr_pr_hash->get_typed_iterator();
	while (!it->done()) {
		curr_pr_hash->put(it->key(), (1-PROP)*(TOTALRANK/NUM_NODES));
		it->Next();
	}

}

REGISTER_KERNEL(Initialize);
REGISTER_KERNEL(PageRankIter);
REGISTER_KERNEL(ClearTable);

int main(int argc, char **argv) {

	Init(argc, argv);

	ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
	NUM_WORKERS = conf.num_workers();

  if (MPI::COMM_WORLD.Get_rank() == 0) {

		if (argc > 1) { 
			BuildGraph(NUM_WORKERS, NUM_NODES, 4); //only create new graph when there's extra argument
		}
    Master m(conf);
		m.run_one(&Initialize);
		for (int i = 0; i < 10; i++) { 
			m.run_all(&PageRankIter);
			m.run_all(&ClearTable);
		}
  } else {
    conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);
    Worker w(conf);
		curr_pr_hash = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::sum);
		next_pr_hash = w.CreateTable<int, double>(&ModSharding, &Accumulator<double>::sum);
    w.Run(); 
  }
}

