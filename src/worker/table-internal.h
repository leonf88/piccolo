#ifndef TABLEINTERNAL_H_
#define TABLEINTERNAL_H_

#include "worker/table.h"
#include "worker/hash-msgs.h"
#include "util/hashmap.h"

namespace dsm {

// A local accumulated hash table.
template <class K, class V>
class TypedLocalTable : public LocalTable {
public:
  typedef HashMap<K, V> DataMap;

  struct Iterator : public TypedTable<K, V>::Iterator {
    Iterator(TypedLocalTable<K, V> *t) : it_(t->data_.begin()) {
      t_ = t;
    }

    void key_str(string *out) { Data::marshal<K>(key(), out); }
    void value_str(string *out) { Data::marshal<V>(value(), out); }

    bool done() { return  it_ == t_->data_.end(); }
    void Next() { ++it_; }

    const K& key() { return it_.key(); }
    V& value() { return it_.value(); }

    Table* owner() { return t_; }

  private:
    typename DataMap::iterator it_;
    TypedLocalTable<K, V> *t_;
  };

  TypedLocalTable(TableInfo tinfo, int size=10000) :
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

  void ApplyUpdates(const HashUpdate& req) {
    for (int i = 0; i < req.put_size(); ++i) {
      StringPiece k = req.key(i);
      StringPiece v = req.value(i);
      this->put_str(k, v);
    }
  }

  string get_str(const StringPiece &k) {
    return Data::to_string<V>(get(Data::from_string<K>(k)));
  }

  void put_str(const StringPiece &k, const StringPiece &v) {
    const K& kt = Data::from_string<K>(k);
    const V& vt = Data::from_string<V>(v);
    put(kt, vt);
  }

  void remove_str(const StringPiece &k) {
    remove(Data::from_string<K>(k));
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

  // Store the given key-value pair in this hash, applying the accumulation
  // policy set at construction.  If 'k' has affinity for a remote thread,
  // the application occurs immediately on the local host, and the update is
  // queued for transmission to the owning thread.
  void put(const K &k, const V &v);

  // Remove this entry from the local and master table.
  void remove(const K &k);

  Table::Iterator* get_iterator(int shard);
  typename TypedTable<K, V>::Iterator* get_typed_iterator(int shard);

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
    return get_shard(Data::from_string<K>(k));
  }

  string get_str(const StringPiece &k) {
    return Data::to_string<V>(get(Data::from_string<K>(k)));
  }

  void put_str(const StringPiece &k, const StringPiece &v) {
    const K& kt = Data::from_string<K>(k);
    const V& vt = Data::from_string<V>(v);
    put(kt, vt);
  }

  void remove_str(const StringPiece &k) {
    remove(Data::from_string<K>(k));
  }
};

template <class K, class V>
TypedGlobalTable<K, V>::TypedGlobalTable(TableInfo tinfo) : GlobalTable(tinfo) {
  partitions_.resize(info().num_shards);
  local_shards_.resize(info().num_shards);

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

  {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);
    static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->put(k, v);
  }

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  while (pending_writes_ > 1000000) { sched_yield(); }
}


template <class K, class V>
V TypedGlobalTable<K, V>::get(const K &k) {
  int shard = this->get_shard(k);
  if (is_local_shard(shard)) {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);
    return static_cast<TypedLocalTable<K, V>*>(partitions_[shard])->get(k);
  }

  VLOG(1) << "Fetching non-local key " << k << " from shard " << shard << " : " << info().table_id;

//  LOG(FATAL) << "Disabled.";

  HashRequest req;
  HashUpdate resp;

  req.set_key(Data::to_string<K>(k));
  req.set_table_id(info().table_id);

  rpc_->Send(shard + 1, MTYPE_GET_REQUEST, req);
  rpc_->Read(shard + 1, MTYPE_GET_RESPONSE, &resp);

  V v = Data::from_string<V>(resp.value(0));
//  VLOG(1) << "Got result " << Data::to_string<K>(k) << " : " << v;

  return v;
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
typename TypedTable<K, V>::Iterator* TypedGlobalTable<K, V>::get_typed_iterator(int shard) {
  return (typename TypedTable<K, V>::Iterator*)partitions_[shard]->get_iterator();
}


} // end namespace dsm

#endif /* TABLEINTERNAL_H_ */
