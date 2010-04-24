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

// Wrapper to convert from string methods to key/value typed methods.
template <class K, class V, class ParentTable >
class TypeWrapper : public ParentTable {
public:
  bool contains_str(const StringPiece& k) {
    return contains(data::from_string<K>(k));
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

  void update_str(const StringPiece &k, const StringPiece &v) {
    const K& kt = data::from_string<K>(k);
    const V& vt = data::from_string<V>(v);
    update(kt, vt);
  }
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

template <class K, class V>
class TypedLocalTable_ : public LocalTable {
public:
  typedef HashMap<K, V> DataMap;

  void Init(const TableDescriptor &tinfo) {
    LocalTable::Init(tinfo);
    data_.rehash(3);//tinfo.default_shard_size);
    dirty = false;
    tainted = false;
    owner = -1;
  }

  bool contains(const K &k) { return data_.find(k) != data_.end(); }
  bool empty() { return data_.empty(); }
  int64_t size() { return data_.size(); }

  Iterator* get_iterator() { return new Iterator(this); }
  Iterator* get_typed_iterator() { return new Iterator(this); }

  V get(const K &k) { return data_[k]; }
  void put(const K &k, const V &v) { data_[k] = v; }

  void update(const K &k, const V &v) {
    ((typename TypedTable<K, V>::AccumFunction)this->info_.accum_function)(&data_[k], v);
  }

  void remove(const K &k) { data_.erase(data_.find(k)); }
  void clear() { data_.clear(); }
  void resize(int64_t new_size) { data_.rehash(new_size); }

  void start_checkpoint(const string& f) {
    data_.checkpoint(f);
    delta_file_ = new RecordFile(f + ".delta", "w");
  }

  void finish_checkpoint() {
    if (delta_file_) {
      delete delta_file_;
      delta_file_ = NULL;
    }
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

  struct Iterator : public TypedTable<K, V>::Iterator {
    Iterator(TypedLocalTable_<K, V> *t) : it_(t->data_.begin()) {
      t_ = t;
    }

    void key_str(string *out) { data::marshal<K>(key(), out); }
    void value_str(string *out) { data::marshal<V>(value(), out); }

    bool done() { return  it_ == t_->data_.end(); }
    void Next() { ++it_; }

    const K& key() { return it_->first; }
    V& value() { return it_->second; }

    LocalTable* owner() { return t_; }

  private:
    typename DataMap::iterator it_;
    TypedLocalTable_<K, V> *t_;
  };

private:
  DataMap data_;
};

template <class K, class V>
class TypedLocalTable : public TypeWrapper<K, V, TypedLocalTable_<K, V>  > {
public:
  TypedLocalTable(const TableDescriptor& d) {
    TypedLocalTable_<K, V>::Init(d);
  }
};



static const int kWriteFlushCount = 100000;

template <class K, class V>
class TypedGlobalTable_ : public GlobalTable {
private:
  static const int32_t kMaxPeers = 8192;
  typedef typename TypedTable<K, V>::ShardingFunction ShardingFunction;
protected:
  LocalTable* create_local(int shard) {
    TableDescriptor linfo = ((GlobalTable*)this)->info();
    linfo.shard = shard;
    return new TypedLocalTable<K, V>(linfo);
  }
public:
  bool contains_str(StringPiece k) {
    return contains(data::from_string<K>(k));
  }

  const TableDescriptor& info() { return this->info_; }

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

  V get_local(const K& k) {
    int shard = this->get_shard(k);

    CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

    return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->get(k);
  }

  void Init(const TableDescriptor& tinfo) {
    GlobalTable::Init(tinfo);
    for (int i = 0; i < partitions_.size(); ++i) {
      TableDescriptor linfo = info();
      linfo.shard = i;
      partitions_[i] = new TypedLocalTable<K, V>(linfo);
    }

    pending_writes_ = 0;
  }

  // Store the given key-value pair in this hash. If 'k' has affinity for a
  // remote thread, the application occurs immediately on the local host,
  // and the update is queued for transmission to the owner.
  void put(const K &k, const V &v) {
    LOG(FATAL) << "Need to implement.";
    int shard = this->get_shard(k);

  //  boost::recursive_mutex::scoped_lock sl(mutex());
    static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->put(k, v);

    if (!is_local_shard(shard)) {
      ++pending_writes_;
    }

    if (pending_writes_ > kWriteFlushCount) {
      SendUpdates();
    }

    PERIODIC(0.1, { this->HandlePutRequests(); });
  }

  void update(const K &k, const V &v) {
    int shard = this->get_shard(k);

  //  boost::recursive_mutex::scoped_lock sl(mutex());
    static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->update(k, v);

    if (!is_local_shard(shard)) {
      ++pending_writes_;
    }

    if (pending_writes_ > kWriteFlushCount) {
      SendUpdates();
    }

    PERIODIC(0.1, { this->HandlePutRequests(); });
  }

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k) {
    int shard = this->get_shard(k);

    // If we received a get for this shard; but we haven't received all of the
    // data for it yet. Continue reading from other workers until we do.
    while (tainted(shard)) {
      sched_yield();
    }

    PERIODIC(0.1, this->HandlePutRequests());

    if (is_local_shard(shard)) {
  //    boost::recursive_mutex::scoped_lock sl(mutex());
      return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->get(k);
    }

    string v_str;
    get_remote(shard, data::to_string<K>(k), &v_str);
    return data::from_string<V>(v_str);
  }

  bool contains(const K &k) {
      int shard = this->get_shard(k);

    // If we received a requestfor this shard; but we haven't received all of the
    // data for it yet. Continue reading from other workers until we do.
    while (tainted(shard)) {
      sched_yield();
    }

    if (is_local_shard(shard)) {
  //    boost::recursive_mutex::scoped_lock sl(mutex());
      return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->contains(k);
    }

    string v_str;
    return get_remote(shard, data::to_string<K>(k), &v_str);
  }

  void remove(const K &k) {
    LOG(FATAL) << "Not implemented!";
  }

  Table_Iterator* get_iterator(int shard) {
    return partitions_[shard]->get_iterator();
  }

  TypedTable_Iterator<K, V>* get_typed_iterator(int shard) {
    return (typename TypedTable<K, V>::Iterator*)partitions_[shard]->get_iterator();
  }
};

template <class K, class V>
class TypedGlobalTable : public TypeWrapper<K, V, TypedGlobalTable_<K, V> >, private boost::noncopyable {
private:
  TypedGlobalTable() {}
public:
  static TypedGlobalTable<K, V>* Create(const TableDescriptor &d) {
    TypedGlobalTable<K, V>* t = new TypedGlobalTable<K, V>;
    t->Init(d);
    return t;
  }
};

}
#endif
