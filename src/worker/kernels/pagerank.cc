#include "util/common.h"
#include "util/file.h"
#include "worker/kernel.h"
#include "worker/kernels/graph.h"
#include "worker/worker.pb.h"
#include <math.h>

DEFINE_double(min_convergence, 1e-5, "If our divergence is less then this, finish up.");

namespace asyncgraph {

static const double kPropagationFactor = 0.8;

struct Node {
  int64_t id;
  int64_t *target;
  int32_t target_size;

  Node(int max_size) { target = new int64_t[max_size]; }

  Node(const PagerankNode& n) {
    id = n.id();
    target = new int64_t[n.target_size()];
    for (int i = 0; i < n.target_size(); ++i) {
      target[i] = n.target(i);
    }
    target_size = n.target_size();
  }

  ~Node() {
    delete [] target;
  }
};

class FakeGraph {
public:
  FakeGraph() : n(100000) {}
  virtual Node* getNode(int i) = 0;
  virtual double expectedRank(int i) = 0;
protected:
  Node n;
};

class TreeGraph : public FakeGraph {
public:
  int size;
  TreeGraph(int size) : size(size) {}

  virtual Node* getNode(int i) {
    if (i > 0) {
      n.target_size = 1;
      n.target[0] = (i - 1) / 2;
    } else {
      n.target_size = 0;
    }

    n.id = i;

    return &n;
  }

  virtual double expectedRank(int i) {
    // Probably an analytic solution to this, but I'm lazy...
    int tRank = (int)log2(size) - (int)log2(i + 1);
    double rank = 1.0 / size;
    for (int i = 0; i < tRank; ++i) {
      rank = 1.0 / size + 2 * rank * kPropagationFactor;
    }

    return rank;
  }
};

class FullyConnectedGraph : public FakeGraph {
public:
  int size;
  FullyConnectedGraph(int size) : size(size) {
    CHECK_LT(size, 100000);
    for (int i = 0; i < size; ++i) {
      n.target[i] = i;
    }
    n.target_size = size;
  }

  virtual Node* getNode(int i) {
    n.id = i;
    return &n;
  }

  virtual double expectedRank(int i) {
    return 1.0;
  }
};

class PsuedoWebGraph : public FakeGraph {
public:
  int size;
  PsuedoWebGraph(int size) : size(size) {}

  virtual Node* getNode(int nodeId) {
    n.id = nodeId;

    // leaf nodes
    if (nodeId > size / 4) {
      n.target_size = 0;
      return &n;
    }

    n.target_size = 10;
    for (int i = 0; i < 10; ++i) {
      n.target[i] = random() % (size / 3);
    }
    return &n;
  }

  virtual double expectedRank(int i) {
    return 1.0;
  }
};

// Simple pagerank kernel with modifications to support efficient asynchronous computation.
class PagerankKernel : public GraphKernel<PagerankKernel, Node, int32_t, double> {
public:
  // Rank values to be propagated for the next iteration.
  typedef unordered_map<int32_t, double> RankMap;
  RankMap currentRanks;

  // Accumulated values from other nodes.  These are copied into currentRanks at intervals.
  RankMap newRanks;

  // The cumulative rank value from the previous and current update.
  double previousTotal, currentTotal;
  double startTime;
  double divergence;

  FakeGraph *graph;

  int64_t totalNodes;

  struct Peer {
    Peer() {
      ready = false;
      lastUpdate = 0;
      totalRank = 0;
      lastTotalRank = 0;
      iteration = -1;
    }

    // the total amount of rank that any of our local nodes received from this peer.
    double totalRank;
    double lastTotalRank;
    double lastUpdate;
    bool ready;
    int id;
    int iteration;
  };

  Peer *currentPeer;
  vector<Peer> peers;

  Accumulator* newAccumulator() {
    Accumulator* a = new SumAccumulator<int64_t, double>;
    return a;
  }

  PagerankKernel() : Base() {
    previousTotal = currentTotal = 0;
    totalNodes = 0;
    currentPeer = NULL;
  }

  bool done() {
    return divergence < FLAGS_min_convergence;
  }

  Node *getNode(int id) {
    return graph->getNode(id);
  }

  void init(const ConfigData& conf) {
    //Base::init(conf);
    config.CopyFrom(conf);

    totalNodes = config.graph_size();
    graph = new FullyConnectedGraph(totalNodes);
    divergence = -1;

    for (int i = config.worker_id(); i < totalNodes; i += config.num_workers()) {
      newRanks[i] = 0;
      currentRanks[i] = (1.0 - kPropagationFactor);
    }

    peers.resize(conf.num_workers());
    for (int i = 0; i < conf.num_workers(); ++i) {
      peers[i].id = i;
    }
    
    previousTotal = 0;
    startTime = Now();
  }

  void loadGraph(RecordFile& f) {
//    do {
//      PagerankNode n;
//      if (!f.read(&n)) {
//        break;
//      }
//
//      localNodes.push_back(new Node(n));
//    } while (1);
  }

  void startIteration() {
    Histogram hist;

    maybeUpdateRanks();

    divergence = 0;
    for (RankMap::iterator i = currentRanks.begin(); i != currentRanks.end(); ++i) {
      hist.add(i->second);
      divergence += fabs(graph->expectedRank(i->first) - i->second);
    }

    PERIODIC(10,
             LOG(INFO) << "Divergence at  " << Now() - startTime << " -> " << divergence);

    for (int i = config.worker_id(); i < totalNodes; i += config.num_workers()) {
      handlePropagate(*graph->getNode(i));
    }
  }

  void maybeUpdateRanks() {
    int ready = 0;
    for (int i = 0; i < peers.size(); ++i) {
      if (peers[i].ready) { ++ready; }
    }

    // If we have previously received a complete update set from a peer, but don't have
    // information for this round, then we use the last total rank to estimate it's
    // contribution.
    double estimatedRank = 1e-12;
    double totalReceived = 1e-12;

    // As our computation progresses, we require more complete updates from other peers
    // before proceeding.  We initially require at least 50% of our peers to be available.
    double needed = 1.0 - (1.0 / (1. + iteration));

    VLOG(1) << "Ready? " << ready << " needed? " << needed * peers.size();
    if (ready > needed * peers.size()) {
      for (int i = 0; i < peers.size(); ++i) {
        Peer &p = peers[i];

        VLOG(1) << StringPrintf("%d %d %.8f %.8f %d", i, p.ready, p.totalRank, p.lastTotalRank, p.iteration);
        estimatedRank += max(p.totalRank, p.lastTotalRank);

        if (p.ready) {
          totalReceived += p.totalRank;
          p.ready = false;
          p.lastTotalRank = max(p.totalRank, p.lastTotalRank);
          p.totalRank = 0;
        }
      }

      // Scale the new 'current' values by the amount we estimate we would have received
      // from our missing peer updates.
      // double scaling = estimatedRank / totalReceived;
      double scaling = 1.0 * peers.size() / ready;
      double t = 0;
      for (RankMap::iterator i = newRanks.begin(); i != newRanks.end(); ++i) {
        VLOG(3) << StringPrintf("Setting new rank for %d = %f (%f * %f + 1.0 / %ld); old was %f",
                                i->first, (i->second * scaling) + 1.0, i->second, scaling, totalNodes, currentRanks[i->first]);

        currentRanks[i->first] = (i->second * scaling) + (1.0 - kPropagationFactor);
        t += currentRanks[i->first];
        i->second = 0;
      }

      previousTotal = currentTotal;
      currentTotal = t;

      PERIODIC(5,
               LOG(INFO) << StringPrintf("Received quorum of ranks %d/%zd (%.2f%%), scaling estimate %.8f, %.8f, %f -> %f",
                                         ready, peers.size(), ready * 100. / peers.size(), scaling, scaling - (1.0 * peers.size() / ready), previousTotal, currentTotal));
    }
  }

  void handlePropagate(Node& n) {
    double propFactor = kPropagationFactor / n.target_size;
    double myRank = currentRanks[n.id];

    for (int i = 0; i < n.target_size; ++i) {
      output(n.target[i], myRank * propFactor);
    }
  }

  bool startUpdates(const UpdateRequest &u) {
    currentPeer = &peers[u.source()];
    if (currentPeer->ready) {
      return false;
    }

    VLOG(1) << "Accepting update from peer " << u.source() << " done? " << u.done();
    currentPeer->iteration = u.iteration();
    return true;
  }

  void handleUpdate(const int32_t& k, const double& v) {
    VLOG(3) << "Applying update from peer " << currentPeer->id << " it "
            << currentPeer->iteration << " k " << k << " v  " << v << " old " << newRanks[k];

    DCHECK(newRanks.find(k) != newRanks.end()) << "Got update for: " << k << " on peer " << config.worker_id();

    newRanks[k] += v;
    currentPeer->totalRank += v;
  }

  void finishUpdates() {
    currentPeer->lastUpdate = Now();
    currentPeer->ready = true;
  }

  void status(KernelStatus *status) {
    status->set_divergence(divergence);
    Pair *p = status->add_extra();
    p->set_key("foo");
    p->set_value("bar");
  }

  void flush() {
    if (totalNodes > 1000) { return; }

    LocalFile rankOut(StringPrintf("pageranks.out.dot.%d", config.worker_id()), "w+");
    double totalRank = 0;

    for (RankMap::iterator i = currentRanks.begin(); i != currentRanks.end(); ++i) {
      totalRank += i->second;
    }

    for (RankMap::iterator i = currentRanks.begin(); i != currentRanks.end(); ++i) {
      int64_t id = i->first;
      double rank = i->second;
      double color = min(0.99, rank * 10.0 / totalRank);
      rankOut.Printf("Node%d [label=\"%d :: %.2f\",fontsize=%d,color=\"0.5 %.8f 0.5\"];\n",
                     id, id, rank, 8, color);


      Node *n = getNode(id);
      for (int j = 0; j < n->target_size; ++j) {
        rankOut.Printf("Node%d -> Node%d;\n", n, n->target[j]);
      }
    }

  }
};

REGISTER_KERNEL(PagerankKernel);
}
