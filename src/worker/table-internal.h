#ifndef TABLEINTERNAL_H_
#define TABLEINTERNAL_H_

#include "worker/table.h"
#include "worker/hash-msgs.h"
#include "util/hashmap.h"

namespace upc {

// A local accumulated hash table.
template <class K, class V>
class LocalTable : public TypedTable<K, V> {
public:
  typedef HashMap<K, V> DataMap;

  struct Iterator : public TypedTable<K, V>::Iterator {
    Iterator(LocalTable<K, V> *t) : it_(t->data_.begin()) {
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
    LocalTable<K, V> *t_;
  };

  LocalTable(TableInfo tinfo, int size=10000) :
    TypedTable<K, V>(tinfo), data_(size) {
  }

  bool contains(const K &k) { return data_.contains(k); }
  bool empty() { return data_.empty(); }
  int64_t size() { return data_.size(); }

  Iterator* get_iterator() {
    return new Iterator(this);
  }

  Iterator* get_typed_iterator() {
    return new Iterator(this);
  }

  V get(const K &k) {
    return data_[k];
  }

  void put(const K &k, const V &v) {
    data_.accumulate(k, v, ((typename TypedTable<K, V>::AccumFunction)this->info_.accum_function));
  }

  void remove(const K &k) {
//    data_.erase(data_.find(k));
  }

  void clear() {
    data_.clear();
  }

  void ApplyUpdates(const HashUpdate& req) {
    for (int i = 0; i < req.put_size(); ++i) {
      StringPiece k = req.key(i);
      StringPiece v = req.value(i);
      this->put_str(k, v);
    }
  }

private:
  DataMap data_;
};

template <class K, class V>
class TypedPartitionedTable : public TypedTable<K, V> {
private:
  static const int32_t kMaxPeers = 8192;

  vector<LocalTable<K, V>*> partitions_;
  mutable boost::recursive_mutex pending_lock_;
  volatile int pending_writes_;

  vector<LocalTable<K, V>*> table_pool_;

  vector<bool> local_shards_;

public:
  TypedPartitionedTable(TableInfo tinfo);

  ~TypedPartitionedTable() {
    for (int i = 0; i < partitions_.size(); ++i) {
      delete partitions_[i];
    }
  }

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

  // Clear any local data for which this table has ownership.  Pending updates
  // are *not* cleared.
  void clear() {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);
    for (int i = 0; i < local_shards_.size(); ++i) {
      if (local_shards_[i]) {
        partitions_[i]->clear();
      }
    }
  }

  // Append to 'out' the list of local tables that have pending network data.
  // Return true if any updates were appended.
  bool GetPendingUpdates(deque<Table*> *out);
  void ApplyUpdates(const upc::HashUpdate& req);
  void Free(Table *t);

  int pending_write_bytes();

  Table::Iterator* get_iterator();
  typename TypedTable<K, V>::Iterator* get_typed_iterator();

  const TableInfo& info() { return this->info_; }

  bool is_local_shard(int shard) {
    return local_shards_[shard];
  }

  bool is_local_key(const StringPiece &k) {
    return is_local_shard(this->get_shard(k));
  }

  void local_shards(vector<int>* v) {
    v->clear();
    for (int i = 0; i < local_shards_.size(); ++i) {
      if (local_shards_[i]) {
        v->push_back(i);
      }
    }
  }
};

template <class K, class V>
TypedPartitionedTable<K, V>::TypedPartitionedTable(TableInfo tinfo) : TypedTable<K, V>(tinfo) {
  partitions_.resize(info().num_shards);
  local_shards_.resize(info().num_shards);

  for (int i = 0; i < partitions_.size(); ++i) {
    TableInfo linfo = info();
    linfo.shard = i;
    partitions_[i] = new LocalTable<K, V>(linfo);
  }

  pending_writes_ = 0;
}

template <class K, class V>
void TypedPartitionedTable<K, V>::ApplyUpdates(const upc::HashUpdate& req) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);
  partitions_[req.shard()]->ApplyUpdates(req);
}

template <class K, class V>
bool TypedPartitionedTable<K, V>::GetPendingUpdates(deque<Table*> *out) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable<K, V> *t = partitions_[i];

    if (!is_local_shard(i) && !t->empty()) {
      out->push_back(t);

      TableInfo linfo = info();
      linfo.shard = i;

      if (table_pool_.empty()) {
        partitions_[i] = new LocalTable<K, V>(linfo);
      } else {
        partitions_[i] = table_pool_.back();
        partitions_[i]->set_info(linfo);
        table_pool_.pop_back();
      }
    }
  }

  pending_writes_ = 0;
  return out->size() > 0;
}

template <class K, class V>
void TypedPartitionedTable<K, V>::Free(Table *used) {
  delete used;
  return;

  if (table_pool_.size() > partitions_.size() * 2) {
    delete used;
    return;
  }

  LocalTable<K, V>* t = static_cast<LocalTable<K, V>* >(used);
  t->clear();

  boost::recursive_mutex::scoped_lock sl(pending_lock_);
  table_pool_.push_back(t);
}


template <class K, class V>
int TypedPartitionedTable<K, V>::pending_write_bytes() {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int64_t s = 0;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable<K, V> *t = partitions_[i];
    if (!is_local_shard(i)) {
      s += t->size();
    }
  }

  return s;
}

template <class K, class V>
void TypedPartitionedTable<K, V>::put(const K &k, const V &v) {
  int shard = this->get_shard(k);

  {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);
    partitions_[shard]->put(k, v);
  }

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  while (pending_writes_ > 1000000) { sched_yield(); }
}

template <class K, class V>
string TypedPartitionedTable<K, V>::get_local(const StringPiece &k) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int shard = this->get_shard(Data::from_string<K>(k));
  CHECK(is_local_shard(shard));

  LocalTable<K, V> *h = partitions_[shard];

//  VLOG(1) << "Returning local result : " <<  h->get(Data::from_string<K>(k))
//          << " : " << Data::from_string<V>(h->get_str(k));

  return h->get_str(k);
}

template <class K, class V>
V TypedPartitionedTable<K, V>::get(const K &k) {
  int shard = this->get_shard(k);
  if (is_local_shard(shard)) {
    return partitions_[shard]->get(k);
  }

  VLOG(1) << "Fetching non-local key " << k << " from shard " << shard << " : " << info().table_id;

  HashRequest req;
  HashUpdate resp;

  req.set_key(Data::to_string<K>(k));
  req.set_table_id(info().table_id);

  info().rpc->Send(shard + 1, MTYPE_GET_REQUEST, req);
  info().rpc->Read(shard + 1, MTYPE_GET_RESPONSE, &resp);

  V v = Data::from_string<V>(resp.value(0));
//  VLOG(1) << "Got result " << Data::to_string<K>(k) << " : " << v;

  return v;
}

template <class K, class V>
void TypedPartitionedTable<K, V>::remove(const K &k) {
  LOG(FATAL) << "Not implemented!";
}

template <class K, class V>
Table::Iterator* TypedPartitionedTable<K, V>::get_iterator() {
  return partitions_[info().shard]->get_iterator();
}

template <class K, class V>
typename TypedTable<K, V>::Iterator* TypedPartitionedTable<K, V>::get_typed_iterator() {
  return partitions_[info().shard]->get_typed_iterator();
}


} // end namespace upc

#endif /* TABLEINTERNAL_H_ */
