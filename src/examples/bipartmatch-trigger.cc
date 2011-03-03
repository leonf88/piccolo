#include "client/client.h"
#include "examples/examples.pb.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <algorithm>
#include <libgen.h>

using namespace dsm;
using namespace std;

static int NUM_WORKERS = 2;

DEFINE_int32(left_vertices, 1000, "Number of left-side vertices");
DEFINE_int32(right_vertices, 1000, "Number of right-side vertices");
DEFINE_double(edge_probability, 0.5, "Probability of edge between vertices");

static TypedGlobalTable<int, vector<int> >*  leftoutedges = NULL;
static TypedGlobalTable<int, int>*            leftmatches = NULL;
static TypedGlobalTable<int, int>*           rightmatches = NULL;

//-----------------------------------------------
namespace dsm{
	template <> struct Marshal<vector<int> > : MarshalBase {
		static void marshal(const vector<int>& t, string *out) {
//			LOG(INFO) << "Marshalling vector<int> to string" << endl;
			int i,j;
			int len = t.size();
			out->append((char*)&len,sizeof(int));
			for(i = 0; i < len; i++) {
				j = t[i];
				out->append((char*)&j,sizeof(int));
			}
//			LOG(INFO) << "Marshalled vector<int> to string of size " << out->length() << endl;
		}
		static void unmarshal(const StringPiece &s, vector<int>* t) {
			int i,j;
			int len;
//			LOG(INFO) << "Unmarshalling vector<int> from string" << endl;
			memcpy(&len,s.data,sizeof(int));
			t->clear();
			for(i = 0; i < len; i++) {
				memcpy(&j,s.data+(i+1)*sizeof(int),sizeof(int));
				t->push_back(j);
			}
//			LOG(INFO) << "Unmarshalled vector<int> from string" << endl;
		}
	};
}
			

//-----------------------------------------------

class BPMTKernel : public DSMKernel {
	public:
		void InitTables() {
			vector<int> v;
			v.clear();
			for(int i=0; i<FLAGS_left_vertices; i++) {
				leftmatches->update(i,-1);
				leftoutedges->update(i,v);
			}
			leftmatches->SendUpdates();
			for(int i=0; i<FLAGS_right_vertices; i++) {
				rightmatches->update(i,-1);
			}
			rightmatches->SendUpdates();
		}

		void PopulateLeft() {
			TypedTableIterator<int, vector<int> > *it = 
				leftoutedges->get_typed_iterator(current_shard());
            CHECK(it != NULL);
			for(; !it->done(); it->Next()) {
				vector<int> v = it->value();
				for(int i=0; i<FLAGS_right_vertices; i++) {
					if ((float)rand()/(float)RAND_MAX < 
							FLAGS_edge_probability) {
						v.push_back(i);
					}
				}
				leftoutedges->update(it->key(),v);
			}
			leftoutedges->SendUpdates();
		}

		//Set a random right neighbor of each left vertex to be
		//matched.  If multiple lefts set the same right, the triggers
		//will sort it out.
		void BeginBPMT() {
			TypedTableIterator<int, vector<int> > *it = 
				leftoutedges->get_typed_iterator(current_shard());
			for(; !it->done(); it->Next()) {
				vector<int> v = it->value();
				if (v.size() <= 0) continue;
				int j = v.size()*((float)rand()/(float)RAND_MAX);
				j = (j>=v.size())?v.size()-1:j;
				rightmatches->update(v[j],it->key());
				leftmatches->update(it->key(),v[j]);
				rightmatches->SendUpdates();
				leftmatches->SendUpdates();
			}
		}

		void EvalPerformance() {
			int matched=0;
			for(int i=0; i<FLAGS_left_vertices; i++) {
				if (-1 < leftmatches->get(i))
					matched++;
			}
			printf("Performance: %d of %d matched.\n",matched,FLAGS_left_vertices);
		}
};

class MatchRequestTrigger : public Trigger<int, int> {
	public:
		bool Fire(const int& key, const int& value, int& newvalue ) {
			if (value != -1) {
//				printf("request: updating %d to %d\n",newvalue,-1);
				leftmatches->enqueue_update(newvalue,-1);
				return false;
			} else {
			// else this match is acceptable.
			}
			return true;
		}
};

class MatchDenyTrigger : public Trigger<int, int> {
	public:
		bool Fire(const int& key, const int& value, int& newvalue ) {
			if (newvalue == -1) {
				vector<int> v = leftoutedges->get(key);
				vector<int>::iterator it = find(v.begin(), v.end(), value);
				if (it != v.end())		//remove possible match
					v.erase(it);
				leftoutedges->enqueue_update((int)key,v);
				if (v.size() == 0)		//forget it if no more candidates
					return true;
				int j = v.size()*((float)rand()/(float)RAND_MAX);
				j = (j>=v.size())?v.size()-1:j;
				printf("deny a: updating %d to %d\n",v[j],key);
				rightmatches->enqueue_update(v[j],key);
				printf("deny b: updating %d to %d\n",v[j],key);
				newvalue = v[j];
				printf("deny c: updating %d to %d\n",v[j],key);
				return false;
			}
			return true;
		}
};



//-----------------------------------------------

REGISTER_KERNEL(BPMTKernel);
REGISTER_METHOD(BPMTKernel, InitTables);
REGISTER_METHOD(BPMTKernel, PopulateLeft);
REGISTER_METHOD(BPMTKernel, BeginBPMT);
REGISTER_METHOD(BPMTKernel, EvalPerformance);

int Bipartmatch_trigger(ConfigData& conf) {

	leftoutedges  = CreateTable(0,conf.num_workers(),new Sharding::Mod, 
		new Accumulators<vector<int> >::Replace);
	leftmatches   = CreateTable(1,conf.num_workers(),new Sharding::Mod,
		new Accumulators<int>::Replace);
	rightmatches  = CreateTable(2,conf.num_workers(),new Sharding::Mod,
		new Accumulators<int>::Replace);

	TriggerID matchreqid = rightmatches->register_trigger(new MatchRequestTrigger);
	TriggerID matchdenyid = leftmatches->register_trigger(new MatchDenyTrigger);

	StartWorker(conf);
	Master m(conf);

	NUM_WORKERS = conf.num_workers();
	printf("---- Initializing Bipartmatch-trigger on %d workers ----\n",NUM_WORKERS);

	//Disable triggers
	m.enable_trigger(matchreqid,2,false);
	m.enable_trigger(matchdenyid,1,false);

	//Fill in all necessary keys
	m.run_one("BPMTKernel","InitTables",  leftoutedges);
	//Populate edges left<->right
	m.run_all("BPMTKernel","PopulateLeft",  leftoutedges);

	//Enable triggers
	m.enable_trigger(matchreqid,2,true);
	m.enable_trigger(matchdenyid,1,true);

	m.run_all("BPMTKernel","BeginBPMT", leftoutedges);

	//Disable triggers
	m.enable_trigger(matchreqid,2,false);
	m.enable_trigger(matchdenyid,1,false);

	m.run_one("BPMTKernel","EvalPerformance",leftmatches);

	return 0;
}
REGISTER_RUNNER(Bipartmatch_trigger);
