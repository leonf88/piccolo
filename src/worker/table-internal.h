#ifndef TABLEINTERNAL_H_
#define TABLEINTERNAL_H_

#include "worker/table.h"

namespace upc {

// A set of LocalTables, one for each shard in the computation.
class PartitionedTable  {
public:
  // Check only the local table for 'k'.  Abort if lookup would case a remote fetch.
  virtual string get_local(const StringPiece &k) = 0;

  // Append to 'out' the list of accumulators that have pending network data.  Return
  // true if any updates were appended.
  virtual bool GetPendingUpdates(deque<Table*> *out) = 0;
  virtual void ApplyUpdates(const upc::HashUpdate& req) = 0;
  virtual int pending_write_bytes() = 0;
};

// A local accumulated hash table.
template <class K, class V>
class LocalTable : public TypedTable<K, V> {
public:
  typedef unordered_map<K, V> DataMap;

  struct Iterator : public TypedTable<K, V>::Iterator {
    Iterator(LocalTable<K, V> *owner) {
      owner_ = owner;
      it_ = owner->data_.begin();
    }

    string key_str() {
//      LOG(INFO) << key() << " : " << value();
      return Data::to_string<K>(key());
    }
    string value_str() { return Data::to_string<V>(value()); }
    bool done() { return it_ == owner_->data_.end(); }
    void Next() { ++it_; }

    const K& key() { return it_->first; }
    const V& value() { return it_->second; }

    Table* owner() { return owner_; }

  private:
    typename unordered_map<K, V>::iterator it_;
    LocalTable<K, V> *owner_;
  };

  LocalTable(TableInfo tinfo) : TypedTable<K, V>(tinfo) {
    data_.rehash(100000);
  }

  bool contains(const K &k) { return data_.find(k) != data_.end(); }
  bool empty() { return data_.empty(); }
  int64_t size() { return data_.size(); }

  Iterator* get_iterator() {
    return new Iterator(this);
  }

  Iterator* get_typed_iterator() {
    return new Iterator(this);
  }

  V get(const K &k) {
    boost::recursive_mutex::scoped_lock sl(write_lock_);
    return data_[k];
  }

  void put(const K &k, const V &v) {
    boost::recursive_mutex::scoped_lock sl(write_lock_);
    typename DataMap::iterator i = data_.find(k);

    if (i != data_.end()) {
      i->second = this->accumulate(i->second, v);
    } else {
      data_.insert(make_pair(k, v));
    }
  }

  void remove(const K &k) {
    boost::recursive_mutex::scoped_lock sl(write_lock_);
    data_.erase(data_.find(k));
  }

  void clear() {
    boost::recursive_mutex::scoped_lock sl(write_lock_);
    data_.clear();
  }

  void applyUpdates(const HashUpdate& req) {
    boost::recursive_mutex::scoped_lock sl(write_lock_);
    for (int i = 0; i < req.put_size(); ++i) {
      const Pair &p = req.put(i);
      this->put_str(p.key(), p.value());
    }

    for (int i = 0; i < req.remove_size(); ++i) {
      const string& k = req.remove(i);
      this->remove_str(k);
    }
  }
  mutable boost::recursive_mutex write_lock_;
private:
  DataMap data_;
};

template <class K, class V>
class TypedPartitionedTable : public PartitionedTable, public TypedTable<K, V> {
private:
  static const int32_t kMaxPeers = 8192;

  vector<LocalTable<K, V>*> partitions_;
  mutable boost::recursive_mutex pending_lock_;
public:
  TypedPartitionedTable(TableInfo tinfo);

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k);

  // Check only the local table for 'k'.  Abort if lookup would case a remote fetch.
  string get_local(const StringPiece &k);

  // Store the given key-value pair in this hash, applying the accumulation
  // policy set at construction.  If 'k' has affinity for a remote thread,
  // the application occurs immediately on the local host, and the update is
  // queued for transmission to the owning thread.
  void put(const K &k, const V &v);

  // Remove this entry from the local and master table.
  void remove(const K &k);

  void clear() {
    partitions_[info().owner_thread]->clear();
  }

  // Append to 'out' the list of accumulators that have pending network data.  Return
  // true if any updates were appended.
  bool GetPendingUpdates(deque<Table*> *out);
  void ApplyUpdates(const upc::HashUpdate& req);
  int pending_write_bytes();

  Table::Iterator* get_iterator();
  typename TypedTable<K, V>::Iterator* get_typed_iterator();

  const TableInfo& info() { return this->info_; }

  int64_t size() { return 1; }
};

template <class K, class V>
TypedPartitionedTable<K, V>::TypedPartitionedTable(TableInfo tinfo) : TypedTable<K, V>(tinfo) {
  partitions_.resize(info().num_threads);

  for (int i = 0; i < partitions_.size(); ++i) {
    TableInfo linfo = info();
    linfo.owner_thread = i;
    partitions_[i] = new LocalTable<K, V>(linfo);
  }
}

template <class K, class V>
void TypedPartitionedTable<K, V>::ApplyUpdates(const upc::HashUpdate& req) {
  partitions_[info().owner_thread]->applyUpdates(req);
}

template <class K, class V>
bool TypedPartitionedTable<K, V>::GetPendingUpdates(deque<Table*> *out) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable<K, V> *a = partitions_[i];
    if (i != info().owner_thread && !a->empty()) {
      TableInfo linfo = info();
      linfo.owner_thread = i;
      partitions_[i] = new LocalTable<K, V>(linfo);
      out->push_back(a);
    }
  }

  return out->size() > 0;
}

template <class K, class V>
int TypedPartitionedTable<K, V>::pending_write_bytes() {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int64_t s = 0;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable<K, V> *a = partitions_[i];
    if (i != info().owner_thread) {
      s += a->size();
    }
  }

  return s;
}

template <class K, class V>
void TypedPartitionedTable<K, V>::put(const K &k, const V &v) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int shard = this->get_shard(k);
  LocalTable<K, V> *h = partitions_[shard];
  h->put(k, v);
}

template <class K, class V>
string TypedPartitionedTable<K, V>::get_local(const StringPiece &k) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int shard = this->get_shard(Data::from_string<K>(k));
  CHECK_EQ(shard, info().owner_thread);

  LocalTable<K, V> *h = partitions_[shard];

  VLOG(1) << "Returning local result : " <<  h->get(Data::from_string<K>(k))
          << " : " << Data::from_string<V>(h->get_str(k));

  return h->get_str(k);
}

template <class K, class V>
V TypedPartitionedTable<K, V>::get(const K &k) {
  int shard = ((typename TypedTable<K, V>::ShardingFunction)info().sf)(k, partitions_.size());
  if (shard == info().owner_thread || partitions_[shard]->contains(k)) {
    return partitions_[shard]->get(k);
  }

  LOG(INFO) << "Non-local fetch for " << k;
  VLOG(1) << "Requesting key " << Data::to_string<K>(k) << " from shard " << shard;

  HashRequest req;
  HashUpdate resp;
  req.set_key(Data::to_string<K>(k));
  req.set_table_id(info().table_id);

  info().rpc->Send(shard, MTYPE_GET_REQUEST, req);
  info().rpc->Read(shard, MTYPE_GET_RESPONSE, &resp);

  V v = Data::from_string<V>(resp.put(0).value());
  VLOG(1) << "Got key " << Data::to_string<K>(k) << " : " << v;

  return v;
}

template <class K, class V>
void TypedPartitionedTable<K, V>::remove(const K &k) {
  LOG(FATAL) << "Not implemented!";
}

template <class K, class V>
Table::Iterator* TypedPartitionedTable<K, V>::get_iterator() {
  return partitions_[info().owner_thread]->get_iterator();
}

template <class K, class V>
typename TypedTable<K, V>::Iterator* TypedPartitionedTable<K, V>::get_typed_iterator() {
  return partitions_[info().owner_thread]->get_typed_iterator();
}


} // end namespace upc

#endif /* TABLEINTERNAL_H_ */
