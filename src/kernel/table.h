#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include "util/hashmap.h"
#include "util/file.h"
#include "worker/worker.pb.h"
#include <boost/thread.hpp>

namespace dsm {

class HashPut;

static int StringSharding(const string& k, int shards) { return StringPiece(k).hash() % shards; }
static int ModSharding(const int& key, int shards) { return key % shards; }
static int UintModSharding(const uint32_t& key, int shards) { return key % shards; }
class Worker;

struct TableDescriptor {
public:
  int table_id;
  int num_shards;

  // For local tables, the shard of the global table they represent.
  int shard;
  int default_shard_size;

  // We use void* to pass around the various accumulation and sharding
  // functions; they are cast to the appropriate type at the time of use.
  void *accum_function;
  void *sharding_function;
};

struct Table_Iterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;
};

template <class K, class V>
struct TypedTable_Iterator : public Table_Iterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;
};

// Accumulator interface
//template <class InternalV, class ExternalV>
//class Accumulator {
//public:
//  virtual void Accumulate(InternalV* out, const ExternalV& in) = 0;
//  virtual void Merge(InternalV* out, vector<InternalV>& in) = 0;
//  virtual ExternalV View(const InternalV& in) = 0;
//};

template <class K, class V>
class TypedTable {
public:
  typedef TypedTable_Iterator<K, V> Iterator;

  // Functions for locating and accumulating data.
  typedef int (*ShardingFunction)(const K& k, int num_shards);
  typedef void (*AccumFunction)(V* a, const V& b);
};

// Methods common to both global table views and local shards
class TableView {
public:
  typedef Table_Iterator Iterator;
  
  virtual const TableDescriptor& info() const = 0;
  virtual void set_info(const TableDescriptor& t) = 0;

  int id() const { return info().table_id; }
  int shard() const { return info().shard; }
  int num_shards() const { return info().num_shards; }

  // Generic routines to fetch and set entries as serialized strings.

  // Put replaces the current value (if any) with the new value specified.  Update
  // applies the accumulation function for this table to merge the existing and
  // new value.
  virtual string get_str(const StringPiece &k) = 0;
  virtual void put_str(const StringPiece &k, const StringPiece& v) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece& v) = 0;
  virtual bool contains_str(const StringPiece &k) = 0;

  virtual bool empty() = 0;
  virtual int64_t size() = 0;

  virtual void resize(int64_t new_size) = 0;

  // Checkpoint and restoration.
  virtual void start_checkpoint(const string& f) = 0;
  virtual void write_delta(const HashPut& put) = 0;
  virtual void finish_checkpoint() = 0;
  virtual void restore(const string& f) = 0;
 
  // Handle incoming network data. 
  virtual void ApplyUpdates(const HashPut& req) = 0;
};

// Operations needed on a local shard of a table.
class LocalView {
public:
  virtual void ApplyUpdates(const HashPut& up) = 0;
  virtual TableView::Iterator* get_iterator() = 0;
  virtual void clear() = 0;
};

class LocalTable : public LocalView, public TableView {
public:
  void Init(const TableDescriptor &tinfo) { 
    info_ = tinfo; 
    delta_file_ = NULL;
  }

  const TableDescriptor& info() const { return info_; }
  void set_info(const TableDescriptor& info) { info_ = info; }
  void ApplyUpdates(const HashPut& req);
  void write_delta(const HashPut& put);
protected:
  friend class GlobalTable;
  TableDescriptor info_;
  bool dirty;
  bool tainted;
  int16_t owner;
  RecordFile *delta_file_;
};

class GlobalView {
public:
  virtual LocalTable *get_partition(int shard) = 0;
  virtual TableView::Iterator* get_iterator(int shard) = 0;
  virtual bool is_local_shard(int shard) = 0;
  virtual bool is_local_key(const StringPiece &k) = 0;
  virtual void set_owner(int shard, int worker) = 0;
  virtual int get_owner(int shard) = 0;

  virtual int get_shard_str(StringPiece k) = 0;

  // Fetch the given key, using only local information.
  virtual void get_local(const StringPiece &k, string *v) = 0;

  // Fetch key k from the node owning it.  Returns true if the key exists.
  virtual bool get_remote(int shard, const StringPiece &k, string* v) = 0;

  // Fill in a response from a remote worker for the given key.
  virtual void handle_get(const StringPiece& key, HashPut* resp) = 0;

  // Transmit any buffered update data to remote peers.
  virtual void SendUpdates() = 0;

  virtual int pending_write_bytes() = 0;

  // Clear any local data for which this table has ownership.  Pending updates
  // are *not* cleared.
  virtual void clear(int shard) = 0;
  virtual bool empty() = 0;
  virtual int64_t size() = 0;
  virtual void resize(int64_t new_size) = 0;
};

class GlobalTable : public GlobalView, public TableView {
public:
  void Init(const TableDescriptor& tinfo);
  const TableDescriptor& info() const { return info_; }
  void set_info(const TableDescriptor& info) { info_ = info; }

  virtual ~GlobalTable() {
    for (int i = 0; i < partitions_.size(); ++i) {
      delete partitions_[i];
    }
  }

  LocalTable *get_partition(int shard);
  Table_Iterator* get_iterator(int shard);
  bool is_local_shard(int shard);
  bool is_local_key(const StringPiece &k);
  void set_owner(int shard, int worker);
  int get_owner(int shard);

  // Fetch the given key, using only local information.
  void get_local(const StringPiece &k, string *v);

  // Fetch key k from the node owning it.  Returns true if the key exists.
  bool get_remote(int shard, const StringPiece &k, string* v);

  // Fill in a response from a remote worker for the given key.
  void handle_get(const StringPiece& key, HashPut* resp);

  // Transmit any buffered update data to remote peers.
  void SendUpdates();
  void ApplyUpdates(const HashPut& req);

  int pending_write_bytes();

  // Clear any local data for which this table has ownership.  Pending updates
  // are *not* cleared.
  void clear(int shard);
  bool empty();
  int64_t size() { return 1; }
  void resize(int64_t new_size);

  void start_checkpoint(const string& f);
  void write_delta(const HashPut& d);
  void finish_checkpoint();
  void restore(const string& f);

protected:
  virtual LocalTable* create_local(int shard) = 0;
  boost::recursive_mutex& mutex() { return m_; }
  vector<LocalTable*> partitions_;
  vector<LocalTable*> get_cache_;
  TableDescriptor info_;

  volatile int pending_writes_;
  boost::recursive_mutex m_;

  friend class Worker;
  Worker *w_;
  int worker_id_;

  void set_worker(Worker *w);
  void HandlePutRequests();

  void set_dirty(int shard) { partitions_[shard]->dirty = true; }
  bool dirty(int shard) { return partitions_[shard]->dirty || !partitions_[shard]->empty(); }

  void set_tainted(int shard) { partitions_[shard]->tainted = true; }
  void clear_tainted(int shard) { partitions_[shard]->tainted = false; }
  bool tainted(int shard) { return partitions_[shard]->tainted; }
};

// Wrapper to add string methods based on key/value typed methods.
#define WRAPPER_FUNCTION_DECL \
bool contains_str(const StringPiece& k);\
string get_str(const StringPiece &k);\
void put_str(const StringPiece &k, const StringPiece &v);\
void remove_str(const StringPiece &k);\
void update_str(const StringPiece &k, const StringPiece &v);

template <class K, class V>
class TypedLocalTable : public LocalTable, private boost::noncopyable {
public:
  typedef HashMap<K, V> DataMap;
  struct Iterator;

  void Init(const TableDescriptor &tinfo); 

  bool empty(); 
  int64_t size(); 

  Table_Iterator* get_iterator();
  Iterator* get_typed_iterator();

  bool contains(const K &k);
  V get(const K &k); 
  void put(const K &k, const V &v); 
  void update(const K &k, const V &v);
  void remove(const K &k); 

  void clear(); 
  void resize(int64_t new_size); 

  void start_checkpoint(const string& f);
  void finish_checkpoint(); 
  void restore(const string& f);

  WRAPPER_FUNCTION_DECL;

private:
  DataMap data_;
};

static const int kWriteFlushCount = 100000;

template <class K, class V>
class TypedGlobalTable : public GlobalTable, private boost::noncopyable {
private:
  static const int32_t kMaxPeers = 8192;
  typedef typename TypedTable<K, V>::ShardingFunction ShardingFunction;
protected:
  LocalTable* create_local(int shard);
public:
  int get_shard(const K& k);
  int get_shard_str(StringPiece k);
  V get_local(const K& k);

  void Init(const TableDescriptor& tinfo);

  // Store the given key-value pair in this hash. If 'k' has affinity for a
  // remote thread, the application occurs immediately on the local host,
  // and the update is queued for transmission to the owner.
  void put(const K &k, const V &v);
  void update(const K &k, const V &v);

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k);
  bool contains(const K &k);
  void remove(const K &k);
  Table_Iterator* get_iterator(int shard);
  TypedTable_Iterator<K, V>* get_typed_iterator(int shard);

  WRAPPER_FUNCTION_DECL;
};


#include "table-internal.h"

}
#endif
