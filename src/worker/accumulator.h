#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include "util/rpc.h"

#include "worker/worker.pb.h"
#include <algorithm>

// Accumulated hashes are hash tables partitioned across the address space of
// all worker threads involved in a computation.  Updates to a hash from a
// local thread are applied immediately; updates against keys stored on
// remote threads are queued and sent when network bandwidth is available.
#define POD_CAST(type, d) ( *((type*)d.data) )

namespace upc {

typedef unordered_map<StringPiece, StringPiece> StringMap;

class LocalHash;
typedef int (*ShardingFunction)(StringPiece);
typedef int (*HashFunction)(StringPiece);
typedef void
    (*AccumFunction)(StringPiece in1, StringPiece in2, StringPiece out);

class AccumHash {
public:
  AccumHash(ShardingFunction sf, HashFunction hf, AccumFunction af) :
    sf_(sf), hf_(hf), af_(af) {
  }

  // Process network updates for this table, applying the accumulator as necessary.
  virtual void applyUpdates(const HashUpdateRequest& req) = 0;

  int ownerThread;
protected:
  AccumFunction af_;
  HashFunction hf_;
  ShardingFunction sf_;
};

// A local accumulated hash table.
class LocalHash: public AccumHash {
private:
   StringMap data;
public:
  struct Iterator {
    Iterator(LocalHash *owner);
    StringPiece key();
    StringPiece value();
    void next();
    bool done();

  private:
    StringMap::iterator it_;
    LocalHash *owner_;
  };


  LocalHash(ShardingFunction sf, HashFunction hf, AccumFunction af);

  StringPiece get(const StringPiece &k);
  void put(const StringPiece &k, const StringPiece &v);
  void remove(const StringPiece &k);
  void clear();
  bool empty();
  int64_t size();
  Iterator *getIterator();

  void applyUpdates(const HashUpdateRequest& req);
};

// A set of accumulated hashes.
class PartitionedHash: public AccumHash {
private:
  static const int32_t kMaxPeers = 8192;

  // New accumulators are created by the network thread when sending out updates,
  // and by the kernel thread when a new iteration starts.
  bool volatile accumWriting[kMaxPeers];

  vector<LocalHash*> partitions;
  mutable boost::recursive_mutex pendingLock;
public:
  PartitionedHash(int numThreads,
                  ShardingFunction sf,
                  HashFunction hf,
                  AccumFunction af,
                  RPCHelper rpc) :
    AccumHash(sf, hf, af) {
    bzero((void*) accumWriting, sizeof(bool) * kMaxPeers);
  }

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  StringPiece get(const StringPiece &k);

  // Store the given key-value pair in this hash, applying the accumulation
  // policy set at construction time.  If 'k' has affinity for a remote thread,
  // the application occurs immediately on the local host, and the update is
  // queued for transmission to the owning thread.
  void put(const StringPiece &k, const StringPiece &v);

  // Remove this entry from the local and master table.
  void remove(const StringPiece &k);

  // Append to 'out' the list of accumulators that have pending network data.
  bool getPendingUpdates(deque<LocalHash*> *out);
  int pendingBytes();

};

}

#endif
