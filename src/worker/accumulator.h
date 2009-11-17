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

static inline double str_as_double(const string& s) { return *reinterpret_cast<const double*>(s.data()); }
static inline string double_as_str(double d) { return string((char*)&d, sizeof(d)); }

typedef int (*ShardingFunction)(StringPiece, int);
typedef int (*HashFunction)(StringPiece);
typedef string (*AccumFunction)(const string& a, const string& b);

// Sharding functions
static int ShardInt(StringPiece k, int shards) { return POD_CAST(int, k) % shards; }
static int ShardStr(StringPiece k, int shards) { return k.hash() % shards; }

// Hash functions
static int HashInt(StringPiece k) { return POD_CAST(int, k); }
static int HashStr(StringPiece k) { return k.hash(); }

// Accumulation functions
static string AccumMin(const string& a, const string& b) { return double_as_str(min(str_as_double(a), str_as_double(b))); }
static string AccumMax(const string& a, const string& b) { return double_as_str(max(str_as_double(a), str_as_double(b))); }
static string AccumSum(const string& a, const string& b) { return double_as_str(str_as_double(a) + str_as_double(b)); }
static string AccumMul(const string& a, const string& b) { return double_as_str(str_as_double(a) * str_as_double(b)); }

static string AccumRep(const string& a, const string& b) { return b; }

class LocalHash;

class SharedTable {
public:
  SharedTable(ShardingFunction sf, HashFunction hf, AccumFunction af,
              int owner, int id) :
    sf_(sf), hf_(hf), af_(af), owner_thread_(owner), table_id(id) {
  }

  virtual string get(const StringPiece &k) = 0;
  virtual void put(const StringPiece &k, const StringPiece &v) = 0;
  virtual void remove(const StringPiece &k) = 0;

  // The thread with ownership over this data.
  int owner_thread_;

  // The table to which this partition belongs.
  int table_id;

protected:
  AccumFunction af_;
  HashFunction hf_;
  ShardingFunction sf_;
};

// A local accumulated hash table.
class LocalHash: public SharedTable {
public:
  typedef unordered_map<string, string> StringMap;
  struct Iterator {
    Iterator(LocalHash *owner);
    string key();
    string value();
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

  string get(const StringPiece &k);
  void put(const StringPiece &k, const StringPiece &v);
  void put_no_accum(const StringPiece &k, const StringPiece &v);
  void remove(const StringPiece &k);
  void clear();
  bool empty();
  bool contains(const StringPiece &k);

  int64_t size();
  Iterator *get_iterator();

  void applyUpdates(const HashUpdate& req);
private:
  StringMap data_;
};

// A set of accumulated hashes.
class PartitionedHash: public SharedTable {
private:
  static const int32_t kMaxPeers = 8192;

  vector<LocalHash*> partitions_;
  mutable boost::recursive_mutex pending_lock_;
  bool volatile accum_working_[kMaxPeers];
  RPCHelper *rpc_;

public:
  PartitionedHash(ShardingFunction sf, HashFunction hf, AccumFunction af,
                  int my_thread, int table_id,
                  int num_threads, RPCHelper *rpc) : SharedTable(sf, hf, af, my_thread, table_id), rpc_(rpc) {
    partitions_.resize(num_threads);

    bzero((void*) accum_working_, sizeof(bool) * kMaxPeers);
    for (int i = 0; i < partitions_.size(); ++i) {
      partitions_[i] = new LocalHash(sf_, hf_, af_, i, table_id);
    }
  }

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  string get(const StringPiece &k);

  // Check only the local table for 'k'.  Abort if lookup would case a remote fetch.
  string get_local(const StringPiece &k);

  // Store the given key-value pair in this hash, applying the accumulation
  // policy set at construction time.  If 'k' has affinity for a remote thread,
  // the application occurs immediately on the local host, and the update is
  // queued for transmission to the owning thread.
  void put(const StringPiece &k, const StringPiece &v);

  // Remove this entry from the local and master table.
  void remove(const StringPiece &k);

  // Append to 'out' the list of accumulators that have pending network data.
  bool GetPendingUpdates(deque<LocalHash*> *out);
  void ApplyUpdates(const upc::HashUpdate& req);

  int pending_write_bytes();
};

}

#endif
