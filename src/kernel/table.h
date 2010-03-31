#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include "util/hashmap.h"
#include "worker/worker.pb.h"
#include <boost/thread.hpp>

namespace dsm {

class HashPut;

static int StringSharding(const string& k, int shards) { return StringPiece(k).hash() % shards; }
static int ModSharding(const int& key, int shards) { return key % shards; }
static int UintModSharding(const uint32_t& key, int shards) { return key % shards; }

template <class V>
struct Accumulator {
  static void min(V* a, const V& b) { *a = std::min(*a, b); }
  static void max(V* a, const V& b) { *a = std::max(*a, b); }
  static void sum(V* a, const V& b) { *a = *a + b; }
  static void replace(V* a, const V& b) { *a = b; }
};

class Worker;

struct TableInfo {
public:
  int table_id;
  int num_shards;

  // For local tables, the shard of the global table they represent.
  int shard;

  // We use void* to pass around the various accumulation and sharding
  // functions; they are cast to the appropriate type at the time of use.
  void *accum_function;
  void *sharding_function;

  // Used for remote sends, and to trigger polling when needed.
  Worker *worker;
};

struct Table_Iterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;
};

// Methods common to both global table views and local shards
class Table {
public:
  typedef Table_Iterator Iterator;

  Table(TableInfo tinfo) : info_(tinfo) {}
  virtual ~Table() {}

  const TableInfo& info() const { return info_; }
  void set_info(const TableInfo& t) { info_ = t; }

  int id() const { return info_.table_id; }
  int shard() const { return info_.shard; }
  int num_shards() const { return info_.num_shards; }

  // Generic routines to fetch and set entries as serialized strings.
  virtual string get_str(const StringPiece &k) = 0;
  virtual void put_str(const StringPiece &k, const StringPiece& v) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece& v) {}

  virtual bool empty() = 0;
  virtual int64_t size() = 0;

  virtual void resize(int64_t new_size) = 0;

  // Checkpoint and restoration.
  virtual void start_checkpoint(const string& f) = 0;
  virtual void write_delta(const HashPut& put) = 0;
  virtual void finish_checkpoint() = 0;
  virtual void restore(const string& f) = 0;

  TableInfo info_;
};

// A local partition of a global table.
class TableShard : public Table {
public:
  TableShard(const TableInfo& info) :
    Table(info), dirty(false), tainted(false), owner(-1) {}

  // Log the given put for checkpointing
  void write_delta(const HashPut& put);

  virtual Table::Iterator* get_iterator() = 0;

  void ApplyUpdates(const HashPut& up);

  // Serialize a portion of the given iterator, up to the network batch limit.
  static void SerializePartial(HashPut& r, Table::Iterator *it);

  virtual void clear() = 0;
protected:
  friend class GlobalTable;
  bool dirty;
  bool tainted;
  int16_t owner;

  RecordFile *delta_file_;
};

class GlobalTable : public Table {
public:
  GlobalTable(const TableInfo& info);

  virtual ~GlobalTable() {
    for (int i = 0; i < partitions_.size(); ++i) {
      delete partitions_[i];
    }
  }

  TableShard *get_partition(int shard) {
    return partitions_[shard];
  }

  Table_Iterator* get_iterator(int shard) {
    return partitions_[shard]->get_iterator();
  }

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

protected:
  friend class Worker;
  virtual TableShard* create_local(int shard) = 0;
  boost::recursive_mutex& mutex() { return m_; }
  vector<TableShard*> partitions_;

  volatile int pending_writes_;
  boost::recursive_mutex m_;

  // Generic methods against serialized strings.
  virtual bool contains_str(StringPiece k) = 0;
  virtual int get_shard_str(StringPiece k) = 0;

  void set_dirty(int shard) { partitions_[shard]->dirty = true; }
  bool dirty(int shard) { return partitions_[shard]->dirty || !partitions_[shard]->empty(); }

  void set_tainted(int shard) { partitions_[shard]->tainted = true; }
  void clear_tainted(int shard) { partitions_[shard]->tainted = false; }
  bool tainted(int shard) { return partitions_[shard]->tainted; }

  void start_checkpoint(const string& f);
  void write_delta(const HashPut& d);
  void finish_checkpoint();
  void restore(const string& f);
};

template <class K, class V>
struct TypedTable_Iterator : public Table_Iterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;
};

template <class K, class V>
class TypedTable {
public:
  typedef TypedTable_Iterator<K, V> Iterator;

  // Functions for locating and accumulating data.
  typedef void (*AccumFunction)(V* a, const V& b);
  typedef int (*ShardingFunction)(const K& k, int num_shards);
};

static const int kWriteFlushCount = 1000000;

// A local accumulated hash table.
template <class K, class V>
class TypedTableShard : public TableShard {
public:
  typedef HashMap<K, V> DataMap;

  struct Iterator : public TypedTable<K, V>::Iterator {
    Iterator(TypedTableShard<K, V> *t) : it_(t->data_.begin()) {
      t_ = t;
    }

    void key_str(string *out) { data::marshal<K>(key(), out); }
    void value_str(string *out) { data::marshal<V>(value(), out); }

    bool done() { return  it_ == t_->data_.end(); }
    void Next() { ++it_; }

    const K& key() { return it_.key(); }
    V& value() { return it_.value(); }

    TableShard* owner() { return t_; }

  private:
    typename DataMap::iterator it_;
    TypedTableShard<K, V> *t_;
  };

  TypedTableShard(TableInfo tinfo, int size=5) :
    TableShard(tinfo), data_(size) {
  }

  bool contains(const K &k) { return data_.contains(k); }
  bool empty() { return data_.empty(); }
  int64_t size() { return data_.size(); }

  Iterator* get_iterator() { return new Iterator(this); }
  Iterator* get_typed_iterator() { return new Iterator(this); }

  V get(const K &k) { return data_[k]; }

  void put(const K &k, const V &v) {
    data_.accumulate(k, v, ((typename TypedTable<K, V>::AccumFunction)this->info_.accum_function));
  }

  void remove(const K &k) {
//    data_.erase(data_.find(k));
  }

  void clear() { data_.clear(); }

  string get_str(const StringPiece &k) {
    return data::to_string<V>(get(data::from_string<K>(k)));
  }

  void resize(int64_t new_size) {
    data_.resize(new_size);
  }

  void put_str(const StringPiece &k, const StringPiece &v) {
    const K& kt = data::from_string<K>(k);
    const V& vt = data::from_string<V>(v);
    put(kt, vt);
  }

  void remove_str(const StringPiece &k) {
    remove(data::from_string<K>(k));
  }

  void start_checkpoint(const string& f) {
    data_.checkpoint(f);
    delta_file_ = new RecordFile(f + ".delta", "w");
  }

  void finish_checkpoint() {
    delete delta_file_;
    delta_file_ = NULL;
  }

  void restore(const string& f) {
    data_.restore(f);

    // Replay delta log.
    RecordFile rf(f + ".delta", "r");
    HashPut p;
    while (rf.read(&p)) {
      ApplyUpdates(p);
    }
  }

private:
  DataMap data_;
};

template <class K, class V>
class TypedGlobalTable : public GlobalTable {
private:
  static const int32_t kMaxPeers = 8192;
  typedef typename TypedTable<K, V>::ShardingFunction ShardingFunction;
protected:
  TableShard* create_local(int shard) {
    TableInfo linfo = ((GlobalTable*)this)->info();
    linfo.shard = shard;
    return new TypedTableShard<K, V>(linfo);
  }
public:
  TypedGlobalTable(TableInfo tinfo);

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k);

  bool contains(const K& k);
  bool contains_str(StringPiece k) {
    return contains(data::from_string<K>(k));
  }

  // Store the given key-value pair in this hash, applying the accumulation
  // policy set at construction.  If 'k' has affinity for a remote thread,
  // the application occurs immediately on the local host, and the update is
  // queued for transmission to the owning thread.
  void put(const K &k, const V &v);

  // Remove this entry from the local and master table.
  void remove(const K &k);

  Table_Iterator* get_iterator(int shard);
  TypedTable_Iterator<K, V>* get_typed_iterator(int shard);

  const TableInfo& info() { return this->info_; }

  int get_shard(const K& k) {
    ShardingFunction sf = (ShardingFunction)info().sharding_function;
    int shard = sf(k, info().num_shards);
    DCHECK_GE(shard, 0);
    DCHECK_LT(shard, num_shards());
    return shard;
  }

  int get_shard_str(StringPiece k) {
    return get_shard(data::from_string<K>(k));
  }

  string get_str(const StringPiece &k) {
    return data::to_string<V>(get(data::from_string<K>(k)));
  }

  void put_str(const StringPiece &k, const StringPiece &v) {
    const K& kt = data::from_string<K>(k);
    const V& vt = data::from_string<V>(v);
    put(kt, vt);
  }

  void remove_str(const StringPiece &k) {
    remove(data::from_string<K>(k));
  }

  V get_local(const K& k) {
    int shard = this->get_shard(k);

    CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

    return static_cast<TypedTableShard<K, V>*>(partitions_[shard])->get(k);
  }
};

template <class K, class V>
TypedGlobalTable<K, V>::TypedGlobalTable(TableInfo tinfo) : GlobalTable(tinfo) {
  for (int i = 0; i < partitions_.size(); ++i) {
    TableInfo linfo = info();
    linfo.shard = i;
    partitions_[i] = new TypedTableShard<K, V>(linfo);
  }

  pending_writes_ = 0;
}

template <class K, class V>
void TypedGlobalTable<K, V>::put(const K &k, const V &v) {
  int shard = this->get_shard(k);

//  boost::recursive_mutex::scoped_lock sl(mutex());
  static_cast<TypedTableShard<K, V>*>(partitions_[shard])->put(k, v);

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  if (pending_writes_ > kWriteFlushCount) {
    SendUpdates();
  }

  PERIODIC(0.1, { info().worker->HandlePutRequests(); });
}


template <class K, class V>
V TypedGlobalTable<K, V>::get(const K &k) {
  int shard = this->get_shard(k);

  // If we received a get for this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  while (tainted(shard)) {
    sched_yield();
  }

  PERIODIC(0.1, info().worker->HandlePutRequests());

  if (is_local_shard(shard)) {
//    boost::recursive_mutex::scoped_lock sl(mutex());
    return static_cast<TypedTableShard<K, V>*>(partitions_[shard])->get(k);
  }

  string v_str;
  get_remote(shard, data::to_string<K>(k), &v_str);
  return data::from_string<V>(v_str);
}

template <class K, class V>
bool TypedGlobalTable<K, V>::contains(const K &k) {
    int shard = this->get_shard(k);

  // If we received a requestfor this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  while (tainted(shard)) {
    sched_yield();
  }

  if (is_local_shard(shard)) {
//    boost::recursive_mutex::scoped_lock sl(mutex());
    return static_cast<TypedTableShard<K, V>*>(partitions_[shard])->contains(k);
  }

  string v_str;
  return get_remote(shard, data::to_string<K>(k), &v_str);
}

template <class K, class V>
void TypedGlobalTable<K, V>::remove(const K &k) {
  LOG(FATAL) << "Not implemented!";
}

template <class K, class V>
Table_Iterator* TypedGlobalTable<K, V>::get_iterator(int shard) {
  return partitions_[shard]->get_iterator();
}

template <class K, class V>
TypedTable_Iterator<K, V>* TypedGlobalTable<K, V>::get_typed_iterator(int shard) {
  return (typename TypedTable<K, V>::Iterator*)partitions_[shard]->get_iterator();
}

}
#endif
