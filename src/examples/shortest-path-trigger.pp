#include "examples/examples.h"
#include "kernel/disk-table.h"

using std::vector;

using namespace piccolo;

DEFINE_int32(tnum_nodes, 10000, "");
DEFINE_int32(tdump_nodes, 0, "");

static int NUM_WORKERS = 0;
static TypedGlobalTable<int, double>* distance_map;
static RecordTable<PathNode>* nodes_record;
static TypedGlobalTable<int, vector<int> >* nodes;

namespace piccolo {
template<> struct Marshal<vector<int> > : MarshalBase {
  static void marshal(const vector<int>& t, string *out) {
    int i;
    int j;
    int len = t.size();
    out->append((char*) &len, sizeof(int));
    for (i = 0; i < len; i++) {
      j = t[i];
      out->append((char*) &j, sizeof(int));
    }
  }
  static void unmarshal(const StringPiece &s, vector<int>* t) {
    int i;
    int j;
    int len;
    memcpy(&len, s.data, sizeof(int));
    if (len < 0) LOG(FATAL) << "Unmarshalled vector of size < 0";
    t->clear();
    for (i = 0; i < len; i++) {
      memcpy(&j, s.data + (i + 1) * sizeof(int), sizeof(int));
      t->push_back(j);
    }
  }
};
}
// This is the trigger. In order to experiment with non-trigger version,
// I limited the maximum distance will be 20.

struct SSSPTrigger: public HybridTrigger<int, double> {
public:
  bool Accumulate(double* a, const double& b) {
    if (*a <= b)		//not better
		return false;
    *a = b;
    return true;
  }
  bool LongFire(const int key, bool lastrun) {
    double distance = distance_map->get(key);
    vector<int> thisnode = nodes->get(key);
    vector<int>::iterator it = thisnode.begin();
    for (; it != thisnode.end(); it++)
      if ((*it) != key)
   { if ((*it) < 0) { printf("BAD KEY for neighbor of %d with %d neighbors\n",key,thisnode.size()); } else
        distance_map->update((*it), distance + 1);
   }
    return false;
  }
};

static void BuildGraph(int shards, int nodes, int density) {
  vector<RecordFile*> out(shards);
  File::Mkdirs("testdata/");
  for (int i = 0; i < shards; ++i) {
    out[i] = new RecordFile(
        StringPrintf("testdata/sp-graph.rec-%05d-of-%05d", i, shards), "w");
  }

  srandom(nodes);	//repeatable graphs
  fprintf(stderr, "Building graph with %d nodes and %d shards:\n", nodes, shards);

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

class ssspt_kern : public DSMKernel {
public:
  void ssspt_driver() {
    if (current_shard() == 0) {
      distance_map->update(0, 0);
    }
  }
  void dump_adj_to_file() {
    FILE* fh = fopen("adj_dumpT","w");
    printf("Dumping Adj data to file\n");
    
    for(int i=0; i < nodes->num_shards(); i++) {
      printf("-- Dumping shard %d...\n",i);
    TypedGlobalTable<int, vector<int> >::Iterator *it =
      nodes->get_typed_iterator(i);
      for(; !it->done(); it->Next()) {
        //printf("dumping key %d\n",it->key());
        fprintf(fh,"%d: {",it->key());
        
        vector<int>::iterator it2 = (it->value()).begin();
        for (; it2 != (it->value()).end(); it2++) {
          fprintf(fh,"%d,",*it2);
        }
        fprintf(fh,"}\n");
      }
    }
    fclose(fh);

  }
};


REGISTER_KERNEL(ssspt_kern);
REGISTER_METHOD(ssspt_kern,ssspt_driver);
REGISTER_METHOD(ssspt_kern,dump_adj_to_file);

int ShortestPathTrigger(const ConfigData& conf) {
  NUM_WORKERS = conf.num_workers();

  distance_map = CreateTable(0, FLAGS_shards, new Sharding::Mod,
                             (Trigger<int,double>*)new SSSPTrigger, 1);
  if (!FLAGS_build_graph) {
    nodes_record = CreateRecordTable<PathNode>(1, "testdata/sp-graph.rec*", false);
  } 
  nodes = CreateTable(2, FLAGS_shards, new Sharding::Mod,
                      new Accumulators<vector<int> >::Replace);

  distance_map->resize((int)(2.52f*(float)FLAGS_tnum_nodes));
  nodes->resize((int)(2.52f*(float)FLAGS_tnum_nodes));

  StartWorker(conf);
  Master m(conf);

  if (FLAGS_build_graph) {
    BuildGraph(FLAGS_shards, FLAGS_tnum_nodes, 4);
    return 0;
  }

  if (!m.restore()) {

    //This is actually a kernel-like thing
    PSwapAccumulator(distance_map,{new Accumulators<double>::Replace});

    PRunAll(distance_map, {
       //vector<double> v;
       //v.clear();
       for(int i=current_shard();i<FLAGS_tnum_nodes;i+=FLAGS_shards) {
         distance_map->update(i, 1e9);	//Initialize all distances to very large.
       //  nodes->update(i,v);	//Start all vectors with empty adjacency lists
       }
    });

    //Build adjacency lists by appending RecordTables' contents
    PMap({n: nodes_record}, {
      vector<int> v;//=nodes->get(n.id());
      for(int i=0; i < n.target_size(); i++) {
        v.push_back(n.target(i));
        nodes->update(n.id(),v);
      }
    });
  }

  //RunDescriptor pr_dump("ssspt_kern","dump_adj_to_file", nodes);
  //m.run_one(pr_dump);

  //This is actually a kernel-like thing
  PSwapAccumulator(distance_map,{(Trigger<int,double>*)new SSSPTrigger});

  //Start the timer!
  struct timeval start_time, end_time;
  gettimeofday(&start_time, NULL);

  //Start it all up by poking the thresholding process with a "null" update on the newly-initialized nodes.
  vector<int> cptables;
  cptables.clear();	
  cptables.push_back(0);
  cptables.push_back(2);

  RunDescriptor pr_rd("ssspt_kern", "ssspt_driver", distance_map, cptables);

  //Switched to a RunDescriptor so that checkpointing can be used.
  pr_rd.checkpoint_type = CP_CONTINUOUS;
  m.run_all(pr_rd);

  //This is actually a kernel-like thing
  PSwapAccumulator(distance_map,{new Triggers<int,double>::NullTrigger});

  //Finish the timer!
  gettimeofday(&end_time, NULL);
  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec)
      * 1000000 + (end_time.tv_usec - start_time.tv_usec);
  fprintf(stderr, "Total SSSP time: %.3f seconds \n", totaltime / 1000000.0);

  if (FLAGS_tdump_nodes > 0) {
    FILE* fh = fopen("SSSPT_dump","w");
    /*PDontRunOne(distance_map, {*/
      for (int i = 0; i < FLAGS_tdump_nodes; ++i) {
        int d = (int)distance_map->get(i);
        if (d >= 1000) {d = -1;}
        fprintf(fh, "%8d:\t%3d\n", i, d);
      }
    /*});*/
    fclose(fh);
  }
  return 0;
}
REGISTER_RUNNER(ShortestPathTrigger);
