#ifndef TABLEINTERNAL_H_
#define TABLEINTERNAL_H_

#include "util/hashmap.h"
#include "util/file.h"

#include "kernel/table.h"
#include "worker/worker.pb.h"

namespace dsm {

static const int kWriteFlushCount = 10000000;

// A local accumulated hash table.
template <class K, class V>
class TypedLocalTable : public LocalTable {
public:
  typedef HashMap<K, V> DataMap;

  struct Iterator : public TypedTable<K, V>::Iterator {
    Iterator(TypedLocalTable<K, V> *t) : it_(t->data_.begin()) {
      t_ = t;
    }

    void key_str(string *out) { data::marshal<K>(key(), out); }
    void value_str(string *out) { data::marshal<V>(value(), out); }

    bool done() { return  it_ == t_->data_.end(); }
    void Next() { ++it_; }

    const K& key() { return it_.key(); }
    V& value() { return it_.value(); }

    Table* owner() { return t_; }

  private:
    typename DataMap::iterator it_;
    TypedLocalTable<K, V> *t_;
  };

  TypedLocalTable(TableInfo tinfo, int size=10) :
    LocalTable(tinfo), data_(size) {
  }

  V accumulate(const V& a, const V& b) {
    return ((typename TypedTable<K, V>::AccumFunction)info_.accum_function)(a, b);
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

  void put_str(const StringPiece &k, const StringPiece &v) {
    const K& kt = data::from_string<K>(k);
    const V& vt = data::from_string<V>(v);

//    LOG(INFO) << MP(kt, vt);

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

  Table::Iterator* get_iterator(int shard);
  TypedTable_Iterator<K, V>* get_typed_iterator(int shard);

  const TableInfo& info() { return this->info_; }

  LocalTable* create_local(int shard) {
    TableInfo linfo = ((Table*)this)->info();
    linfo.shard = shard;
    return new TypedLocalTable<K, V>(linfo);
  }

  int get_shard(const K& k) {
    ShardingFunction sf = (ShardingFunction)info().sharding_function;
    return sf(k, info().num_shards);
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

    return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->get(k);
  }
};

template <class K, class V>
TypedGlobalTable<K, V>::TypedGlobalTable(TableInfo tinfo) : GlobalTable(tinfo) {
  for (int i = 0; i < partitions_.size(); ++i) {
    TableInfo linfo = info();
    linfo.shard = i;
    partitions_[i] = new TypedLocalTable<K, V>(linfo);
  }

  pending_writes_ = 0;
}

template <class K, class V>
void TypedGlobalTable<K, V>::put(const K &k, const V &v) {
  int shard = this->get_shard(k);
  static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->put(k, v);

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  PERIODIC(0.1, { CheckForUpdates(); });

  if (pending_writes_ > kWriteFlushCount) {
    SendUpdates();
  }
}


template <class K, class V>
V TypedGlobalTable<K, V>::get(const K &k) {
  int shard = this->get_shard(k);

  // If we received a get for this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  while (tainted(shard)) {
    CheckForUpdates();
  }

  if (is_local_shard(shard)) {
    PERIODIC(0.1, { CheckForUpdates(); });
    return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->get(k);
  }

  string v_str;
  get_remote(shard, data::to_string<K>(k), &v_str);
  return data::from_string<V>(v_str);
}

template <class K, class V>
bool TypedGlobalTable<K, V>::contains(const K &k) {
  int shard = this->get_shard(k);

  // If we received a get for this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  while (tainted(shard)) {
    CheckForUpdates();
  }

  if (is_local_shard(shard)) {
    PERIODIC(0.1, { CheckForUpdates(); });
    return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->contains(k);
  }

  string v_str;
  return get_remote(shard, data::to_string<K>(k), &v_str);
}

template <class K, class V>
void TypedGlobalTable<K, V>::remove(const K &k) {
  LOG(FATAL) << "Not implemented!";
}

template <class K, class V>
Table::Iterator* TypedGlobalTable<K, V>::get_iterator(int shard) {
  return partitions_[shard]->get_iterator();
}

template <class K, class V>
TypedTable_Iterator<K, V>* TypedGlobalTable<K, V>::get_typed_iterator(int shard) {
  return (typename TypedTable<K, V>::Iterator*)partitions_[shard]->get_iterator();
}


} // end namespace dsm

#endif /* TABLEINTERNAL_H_ */
