#include "client/client.h"
#include "examples/examples.pb.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <algorithm>
#include <libgen.h>

using namespace dsm;
using namespace std;

static int NUM_WORKERS = 2;
#define MAXCOST RAND_MAX

DEFINE_int32(left_vertices, 100, "Number of left-side vertices");
DEFINE_int32(right_vertices, 100, "Number of right-side vertices");
DEFINE_double(edge_probability, 0.5, "Probability of edge between vertices");
DEFINE_bool(edge_costs, false, "Set to true to have edges have costs");

static TypedGlobalTable<int, vector<int> >* leftoutedges = NULL;
static TypedGlobalTable<int, vector<int> >* leftoutcosts = NULL;
static TypedGlobalTable<int, int>*           leftmatches = NULL;
static TypedGlobalTable<int, int>*          rightmatches = NULL;
static TypedGlobalTable<int, int>*		      rightcosts = NULL;
static TypedGlobalTable<string, string>*      StatsTable = NULL;

//-----------------------------------------------
namespace dsm{
	template <> struct Marshal<vector<int> > : MarshalBase {
		static void marshal(const vector<int>& t, string *out) {
			int i,j;
			int len = t.size();
			out->append((char*)&len,sizeof(int));
			for(i = 0; i < len; i++) {
				j = t[i];
				out->append((char*)&j,sizeof(int));
			}
		}
		static void unmarshal(const StringPiece &s, vector<int>* t) {
			int i,j;
			int len;
			memcpy(&len,s.data,sizeof(int));
			t->clear();
			for(i = 0; i < len; i++) {
				memcpy(&j,s.data+(i+1)*sizeof(int),sizeof(int));
				t->push_back(j);
			}
		}
	};
}
			
//-----------------------------------------------
class BPMKernel : public DSMKernel {
	public:
		void InitTables() {
			vector<int> v;		//for left nodes' neighbors
			vector<int> v2;	//for left nodes' edge costs

			v.clear();
			v2.clear();

			leftmatches->resize(FLAGS_left_vertices);
			rightmatches->resize(FLAGS_right_vertices);
			rightcosts->resize(FLAGS_right_vertices);
			leftoutedges->resize(FLAGS_left_vertices);
			leftoutcosts->resize(FLAGS_left_vertices);
			for(int i=0; i<FLAGS_left_vertices; i++) {
				leftmatches->update(i,-1);
				leftoutedges->update(i,v);
				leftoutcosts->update(i,v2);
			}
			for(int i=0; i<FLAGS_right_vertices; i++) {
				rightmatches->update(i,-1);
				rightcosts->update(i,MAXCOST);
			}

			//One-off status value
			StatsTable->resize(leftmatches->num_shards());
			for(int i=0; i<leftmatches->num_shards(); i++) {
				char key[32];
				sprintf(key,"quiescent%d",i);
				StatsTable->update(key,"t");
			}
			StatsTable->SendUpdates();		
		}

		void PopulateLeft() {
			TypedTableIterator<int, vector<int> > *it = 
				leftoutedges->get_typed_iterator(current_shard());
            CHECK(it != NULL);
			TypedTableIterator<int, vector<int> > *it2 = 
				leftoutcosts->get_typed_iterator(current_shard());
            CHECK(it2 != NULL);
			int cost = 0;
			for(; !it->done() && !it2->done(); it->Next(),it2->Next()) {
				vector<int> v  =  it->value();
				vector<int> v2 = it2->value();
				for(int i=0; i<FLAGS_right_vertices; i++) {
					if ((float)rand()/(float)RAND_MAX < 
							FLAGS_edge_probability) {
						v.push_back(i);					//add neighbor
						cost = ((FLAGS_edge_costs)?rand():(RAND_MAX));
						v2.push_back(cost);
					}
				}
				leftoutedges->update(it->key(),v);		//store list of neighboring edges
				leftoutcosts->update(it2->key(),v2);	//store list of neighbor edge costs
			}
		}

		//Set a random right neighbor of each left vertex to be
		//matched.  If multiple lefts set the same right, the triggers
		//will sort it out.
		void BPMRoundLeft() {
			bool quiescent = true;

			char qkey[32];
			sprintf(qkey,"quiescent%d",current_shard());

			TypedTableIterator<int, vector<int> > *it = 
				leftoutedges->get_typed_iterator(current_shard());
			TypedTableIterator<int, vector<int> > *it2 = 
				leftoutcosts->get_typed_iterator(current_shard());
			TypedTableIterator<int, int> *it3 = 
				leftmatches->get_typed_iterator(current_shard());
			for(; !it->done() && !it2->done() && !it3->done(); it->Next(),it2->Next(),it3->Next()) {

				vector<int>  v =  it->value();	//leftoutedges
				vector<int> v2 = it2->value();	//leftoutcosts

				//only try to match if this left node is unmatched and has candidate right nodes,
				//represented respectively by leftmatches == -1 and v/v2 having >0 items.
				if (v.size() <= 0 || it3->value() != -1)
					continue;

				//don't stop until nothing happens in a round
				quiescent = false;

				//try to find a random or best match
				int j;
				float mincost = MAXCOST;
				if (FLAGS_edge_costs) {
					//edges have associated costs
					vector<int>::iterator  inner_it =  v.begin();
					vector<int>::iterator inner_it2 = v2.begin();
					j = -1;
					for(; inner_it != v.end() && inner_it2 != v2.end(); inner_it++, inner_it2++) {
						if ((*inner_it2) < mincost) {
							mincost = *inner_it2;
							j = *inner_it;
						}
					}
				} else {
					//all edges equal; pick one at random
					j = v.size()*((float)rand()/(float)RAND_MAX);
					j = (j>=v.size())?v.size()-1:j;
					j = v[j];
					//leave mincost at MAXCOST; all edges same cost
				}
				VLOG(2) << "Attempted match: left " << it->key() << " <--> right " << j << endl;

				int rightmatch = rightmatches->get(j);
				if (rightmatch == -1) {
					//if right is unmatched, this is perfect, gogogo
					rightcosts->update(j,mincost);
					rightmatches->update(j,it->key());
					leftmatches->update(it->key(),j);
				} else {
					//if right is already matched, check costs
					if (mincost < rightcosts->get(j)) {
						//found better match!
						leftmatches->update(rightmatch,-1);	//remove old match
						rightcosts->update(j,mincost);
						rightmatches->update(j,it->key());
						leftmatches->update(it->key(),j);
					} else {
						//left match denied.  Left node is sad.
						VLOG(2) << "Denying match on " << j << " from " << it->key() << endl;
						//Leftmatch stays at -1
						vector<int>::iterator inner_it = find(v.begin(), v.end(), j);
						vector<int>::iterator inner_it2;

						if (inner_it != v.end()) {		//remove possible match
							inner_it2 = v2.begin() + (inner_it-v.begin()); //index into cost list
							v.erase(inner_it);
							v2.erase(inner_it2);
						}

						//Enqueue the removal
						leftoutedges->update((int)it->key(),v);
						leftoutcosts->update((int)it->key(),v2);
					}
				}
			}
			StatsTable->update(qkey,(quiescent?"t":"f"));
			VLOG(2) << "Shard " << current_shard() << " is quiescent? " << StatsTable->get(qkey).c_str() << endl;
		}

		void BPMRoundRight() {
			int rightset[FLAGS_right_vertices];
			bool quiescent = (0 == strcmp(StatsTable->get("quiescent0").c_str(),"t"))?true:false;

			for(int i=0; i<FLAGS_right_vertices; i++)
				rightset[i] = 0;

			//Check if parallelization set multiple lefts matching the same right
			int i=0;
			for(int j=0; j<leftoutedges->num_shards(); j++) {
				TypedTableIterator<int, vector<int> > *it = 
					leftoutedges->get_typed_iterator(j);
				TypedTableIterator<int, vector<int> > *it2 = 
					leftoutcosts->get_typed_iterator(j);
				TypedTableIterator<int, int> *it3 = 
					leftmatches->get_typed_iterator(j);
				for(; !it->done() && !it2->done() && !it3->done(); it->Next(),it2->Next(),it3->Next()) {
					int rightmatch = it3->value();
					vector<int>  v =  it->value();	//leftoutedges
					vector<int> v2 = it2->value();	//leftoutcosts
					if (-1 < rightmatch) {
						rightset[rightmatch]++;
						if (rightset[rightmatch] > 1) {
							//left match denied.  Left node is sad.
							VLOG(2) << "Denying match on " << rightmatch << " from " << it->key() << endl;
							//Leftmatch stays at -1
							vector<int>::iterator inner_it = find(v.begin(), v.end(), rightmatch);
							vector<int>::iterator inner_it2;

							if (inner_it != v.end()) {		//remove possible match
								inner_it2 = v2.begin() + (inner_it-v.begin()); //index into cost list
								v.erase(inner_it);
								v2.erase(inner_it2);
							}

							//Enqueue the removal
							leftmatches->update(it->key(),-1);
							leftoutedges->update(it->key(),v);
							leftoutcosts->update(it->key(),v2);

							//State and stuff
							quiescent = false;
							i++;
						} else {
							rightmatches->update(it3->value(),it3->key());
						}
					}
				}
			}
			VLOG(0) << "Total of " << i << " left nodes were overlapped and fixed." << endl;
			StatsTable->update("quiescent0",(quiescent?"t":"f"));
		}

		void EvalPerformance() {
			int left_matched=0, right_matched=0;
			int rightset[FLAGS_right_vertices];

			//float edgecost = 0.f;
			//float worstedgecost = 0.f;

			for(int i=0; i<FLAGS_right_vertices; i++) {
				rightset[i] = 0;
				right_matched += (-1 < rightmatches->get(i));

				//TODO calculate how the costs worked out
			}

			for(int i=0; i<FLAGS_left_vertices; i++) {
				int rightmatch = leftmatches->get(i);
				if (-1 < rightmatch) {
					left_matched++;
					rightset[rightmatch]++;
					if (rightset[rightmatch] > 1)
						cout << rightset[rightmatch] << " left vertices have right vertex " <<
							rightmatch << " as a match: one is " << i << endl;
				}
			}
			printf("Performance: [LEFT]  %d of %d matched.\n",left_matched,FLAGS_left_vertices);
			printf("Performance: [RIGHT] %d of %d matched.\n",right_matched,FLAGS_right_vertices);
		}
};

//-----------------------------------------------

REGISTER_KERNEL(BPMKernel);
REGISTER_METHOD(BPMKernel, InitTables);
REGISTER_METHOD(BPMKernel, PopulateLeft);
REGISTER_METHOD(BPMKernel, BPMRoundLeft);
REGISTER_METHOD(BPMKernel, BPMRoundRight);
REGISTER_METHOD(BPMKernel, EvalPerformance);

int Bipartmatch(ConfigData& conf) {

	leftoutedges  = CreateTable(0,conf.num_workers(),new Sharding::Mod, 
		new Accumulators<vector<int> >::Replace);
	leftmatches   = CreateTable(1,conf.num_workers(),new Sharding::Mod,
		new Accumulators<int>::Replace);
	rightmatches  = CreateTable(2,conf.num_workers(),new Sharding::Mod,
		new Accumulators<int>::Replace);
	leftoutcosts  = CreateTable(3,conf.num_workers(),new Sharding::Mod,
		new Accumulators<vector<int> >::Replace);
	rightcosts    = CreateTable(4,conf.num_workers(),new Sharding::Mod,
		new Accumulators<int>::Replace);
	StatsTable    = CreateTable(10000,1,new Sharding::String, new Accumulators<string>::Replace);//CreateStatsTable();

	StartWorker(conf);
	Master m(conf);

	NUM_WORKERS = conf.num_workers();
	printf("---- Initializing Bipartmatch on %d workers ----\n",NUM_WORKERS);

	//Fill in all necessary keys
	m.run_one("BPMKernel","InitTables",  leftoutedges);
	//Populate edges left<->right
	m.run_all("BPMKernel","PopulateLeft",  leftoutedges);
	m.barrier();

	bool unstable;
	do {
		unstable = false;
		m.run_all("BPMKernel","BPMRoundLeft",leftoutedges);
		m.run_one("BPMKernel","BPMRoundRight",rightmatches);
		for(int i=0; i<conf.num_workers(); i++) {
			char qkey[32];
			sprintf(qkey,"quiescent%d",i);
			if (0 == strcmp(StatsTable->get(qkey).c_str(),"f"))
				unstable = true;
		}
	} while(unstable);

	m.run_one("BPMKernel","EvalPerformance",leftmatches);

	return 0;
}
REGISTER_RUNNER(Bipartmatch);
