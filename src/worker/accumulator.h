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

typedef int (*ShardingFunction)(StringPiece, int);
typedef int (*HashFunction)(StringPiece);

// Accumulate new_val into current.
typedef void (*AccumFunction)(StringPiece current, StringPiece new_val);

// Sharding functions
static int ShardInt(StringPiece k, int shards) {
  return POD_CAST(int, k) % shards;
}
static int ShardStr(StringPiece k, int shards) {
  return k.hash() % shards;
}

// Hash functions
static int HashInt(StringPiece k) {
  return POD_CAST(int, k);
}
static int HashStr(StringPiece k) {
  return k.hash();
}

// Accumulation functions
static void AccumMin(StringPiece current, StringPiece new_val) {
  current.data = (char*) min(POD_CAST(int, current), POD_CAST(int, new_val));
}
static void AccumSum(StringPiece current, StringPiece new_val) {
  current.data = (char*) (POD_CAST(int, current) + POD_CAST(int, new_val));
}
static void AccumProd(StringPiece current, StringPiece new_val) {
  current.data = (char*) (POD_CAST(int, current) * POD_CAST(int, new_val));
}

typedef unordered_map<StringPiece, StringPiece> StringMap;

class LocalHash;

class SharedTable {
public:
  SharedTable(ShardingFunction sf, HashFunction hf, AccumFunction af,
              int owner, int id) :
    sf_(sf), hf_(hf), af_(af), owner(owner), hash_id(id) {
  }

  virtual StringPiece get(const StringPiece &k) = 0;
  virtual void put(const StringPiece &k, const StringPiece &v) = 0;
  virtual void remove(const StringPiece &k) = 0;

  // The thread with ownership over this data.
  int owner;

  // The table to which this partition belongs.
  int hash_id;

protected:
  AccumFunction af_;
  HashFunction hf_;
  ShardingFunction sf_;
};

// A local accumulated hash table.
class LocalHash: public SharedTable {
public:
  struct Iterator {
    Iterator(LocalHash *owner);
    StringPiece key();
    StringPiece value();
    void next();
    bool done();

    LocalHash *owner() {
      return owner_;
    }

  private:
    LocalHash *owner_;
    StringMap::iterator it_;
  };

  LocalHash(ShardingFunction sf, HashFunction hf, AccumFunction af, int owner,
            int id);

  StringPiece get(const StringPiece &k);
  void put(const StringPiece &k, const StringPiece &v);
  void remove(const StringPiece &k);
  void clear();
  bool empty();
  int64_t size();
  Iterator *get_iterator();

  void applyUpdates(const HashUpdateRequest& req);
private:
  StringMap data;
};

// A set of accumulated hashes.
class PartitionedHash: public SharedTable {
private:
  static const int32_t kMaxPeers = 8192;

  bool volatile accum_working[kMaxPeers];

  vector<LocalHash*> partitions;
  mutable boost::recursive_mutex pending_lock;

public:
  PartitionedHash(ShardingFunction sf, HashFunction hf, AccumFunction af,
                  int my_thread, int hash_id,
                  int num_threads, RPCHelper *rpc) : SharedTable(sf, hf, af, my_thread, hash_id) {
    partitions.resize(num_threads);

    bzero((void*) accum_working, sizeof(bool) * kMaxPeers);
    for (int i = 0; i < partitions.size(); ++i) {
      partitions[i] = new LocalHash(sf_, hf_, af_, i, hash_id);
    }
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
  bool GetPendingUpdates(deque<LocalHash*> *out);
  void ApplyUpdates(const upc::HashUpdateRequest& req);

  int pending_write_bytes();
};

}

#endif
