static const int kWriteFlushCount = 100000;

template<class K, class V>
TypedLocalTable<K, V>::TypedLocalTable(const TableDescriptor &tinfo) : LocalTable(tinfo) {
  data_.rehash(1);//tinfo.default_shard_size);
  owner = -1;
}

template<class K, class V>
bool TypedLocalTable<K, V>::contains(const K &k) {
  return data_.find(k) != data_.end();
}

template<class K, class V>
bool TypedLocalTable<K, V>::empty() {
  return data_.empty();
}

template<class K, class V>
int64_t TypedLocalTable<K, V>::size() {
  return data_.size();
}

template<class K, class V>
Table_Iterator* TypedLocalTable<K, V>::get_iterator() {
  return new Iterator(this);
}

template<class K, class V>
typename TypedLocalTable<K, V>::Iterator* TypedLocalTable<K, V>::get_typed_iterator() {
  return new Iterator(this);
}

template<class K, class V>
V TypedLocalTable<K, V>::get(const K &k) {
  return data_[k];
}

template<class K, class V>
void TypedLocalTable<K, V>::put(const K &k, const V &v) {
  data_[k] = v;
}

template<class K, class V> void TypedLocalTable<K, V>::update(const K &k, const V &v) {
  data_.accumulate(k, v,
                   ((typename TypedTable<K, V>::AccumFunction) this->info_.accum_function));
}

template<class K, class V>
void TypedLocalTable<K, V>::remove(const K &k) {
  data_.erase(data_.find(k));
}

template<class K, class V>
void TypedLocalTable<K, V>::clear() {
  data_.clear();
}

template<class K, class V>
void TypedLocalTable<K, V>::resize(int64_t new_size) {
  data_.rehash(new_size);
}

template<class K, class V>
void TypedLocalTable<K, V>::start_checkpoint(const string& f) {
  data_.checkpoint(f);
  delta_file_ = new RecordFile(f + ".delta", "w");
}

template<class K, class V>
void TypedLocalTable<K, V>::finish_checkpoint() {
  if (delta_file_) {
    delete delta_file_;
    delta_file_ = NULL;
  }
}

template<class K, class V> void TypedLocalTable<K, V>::restore(const string& f) {

  data_.restore(f);

  // Replay delta log.
  RecordFile rf(f + ".delta", "r");
  HashPut p;
  while (rf.read(&p)) {
    ApplyUpdates(p);
  }
}

template<class K, class V>
struct TypedLocalTable<K, V>::Iterator: public TypedTable<K, V>::Iterator {
  Iterator(TypedLocalTable<K, V> *t) :
    it_(t->data_.begin()) {
    t_ = t;
  }

  void key_str(string *out) {
    data::marshal<K>(key(), out);
  }
  void value_str(string *out) {
    data::marshal<V>(value(), out);
  }

  bool done() {
    return it_ == t_->data_.end();
  }
  void Next() {
    ++it_;
  }

  const K& key() {
    return it_->first;
  }
  V& value() {
    return it_->second;
  }

  LocalTable* owner() {
    return t_;
  }

private:
  typename DataMap::iterator it_;
  TypedLocalTable<K, V> *t_;
};

template<class K, class V>
int TypedGlobalTable<K, V>::get_shard(const K& k) {
  DCHECK(this != NULL);
  DCHECK(info().sharding_function != NULL);

  ShardingFunction sf = (ShardingFunction) info().sharding_function;
  int shard = sf(k, info().num_shards);
  DCHECK_GE(shard, 0);
  DCHECK_LT(shard, this->num_shards());
  return shard;
}

template<class K, class V>
int TypedGlobalTable<K, V>::get_shard_str(StringPiece k) {
  return get_shard(data::from_string<K>(k));
}

template<class K, class V>
V TypedGlobalTable<K, V>::get_local(const K& k) {
  int shard = this->get_shard(k);

  CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

  return static_cast<TypedLocalTable<K, V>*> (partitions_[shard])->get(k);
}

template<class K, class V>
TypedGlobalTable<K, V>::TypedGlobalTable(const TableDescriptor& tinfo) : GlobalTable(tinfo) {
  for (int i = 0; i < partitions_.size(); ++i) {
    partitions_[i] = (TypedLocalTable<K, V>*)create_local(i);
  }

  pending_writes_ = 0;
}

// Store the given key-value pair in this hash. If 'k' has affinity for a
// remote thread, the application occurs immediately on the local host,
// and the update is queued for transmission to the owner.
template<class K, class V>
void TypedGlobalTable<K, V>::put(const K &k, const V &v) {
  LOG(FATAL) << "Need to implement.";
  int shard = this->get_shard(k);

  //  boost::recursive_mutex::scoped_lock sl(mutex());
  static_cast<TypedLocalTable<K, V>*> (partitions_[shard])->put(k, v);

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  if (pending_writes_ > kWriteFlushCount) {
    SendUpdates();
  }

PERIODIC(0.1, {this->HandlePutRequests();});
}

template<class K, class V>
void TypedGlobalTable<K, V>::update(const K &k, const V &v) {
  int shard = this->get_shard(k);

  //  boost::recursive_mutex::scoped_lock sl(mutex());
  static_cast<TypedLocalTable<K, V>*> (partitions_[shard])->update(k, v);

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  if (pending_writes_ > kWriteFlushCount) {
    SendUpdates();
  }

PERIODIC(0.1, {this->HandlePutRequests();});
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V>
V TypedGlobalTable<K, V>::get(const K &k) {
  int shard = this->get_shard(k);

  // If we received a get for this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  while (tainted(shard)) {
    this->HandlePutRequests();
    sched_yield();
  }

  PERIODIC(0.1, this->HandlePutRequests());

  if (is_local_shard(shard)) {
    //    boost::recursive_mutex::scoped_lock sl(mutex());
    return static_cast<TypedLocalTable<K, V>*> (partitions_[shard])->get(k);
  }

  string v_str;
  get_remote(shard, data::to_string<K>(k), &v_str);
  return data::from_string<V>(v_str);
}

template<class K, class V>
bool TypedGlobalTable<K, V>::contains(const K &k) {
  int shard = this->get_shard(k);

  // If we received a requestfor this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  while (tainted(shard)) {
    this->HandlePutRequests();
    sched_yield();
  }

  if (is_local_shard(shard)) {
    //    boost::recursive_mutex::scoped_lock sl(mutex());
    return static_cast<TypedLocalTable<K, V>*> (partitions_[shard])->contains(
                                                                               k);
  }

  string v_str;
  return get_remote(shard, data::to_string<K>(k), &v_str);
}

template<class K, class V>
void TypedGlobalTable<K, V>::remove(const K &k) {
  LOG(FATAL) << "Not implemented!";
}

template<class K, class V>
Table_Iterator* TypedGlobalTable<K, V>::get_iterator(int shard) {
  return partitions_[shard]->get_iterator();
}

template<class K, class V>
LocalTable* TypedGlobalTable<K, V>::create_local(int shard) {
  TableDescriptor linfo = ((GlobalTable*) this)->info();
  linfo.shard = shard;
  TypedLocalTable<K, V>* t = new TypedLocalTable<K, V>(linfo);
  return t;
}

template<class K, class V>
TypedTable_Iterator<K, V>* TypedGlobalTable<K, V>::get_typed_iterator(
                                                                       int shard) {
  return (typename TypedTable<K, V>::Iterator*) partitions_[shard]->get_iterator();
}

#define WRAPPER_IMPL(klass)\
template<class K, class V>\
bool klass<K, V>::contains_str(const StringPiece& k) {\
  return contains(data::from_string<K>(k));\
}\
template<class K, class V>\
string klass<K, V>::get_str(const StringPiece &k) {\
  return data::to_string<V>(get(data::from_string<K>(k)));\
}\
template<class K, class V>\
void klass<K, V>::put_str(const StringPiece &k, const StringPiece &v) {\
  const K& kt = data::from_string<K>(k);\
  const V& vt = data::from_string<V>(v);\
  put(kt, vt);\
}\
template<class K, class V>\
void klass<K, V>::remove_str(const StringPiece &k) {\
  remove(data::from_string<K>(k));\
}\
template<class K, class V>\
void klass<K, V>::update_str(const StringPiece &k, const StringPiece &v) {\
  const K& kt = data::from_string<K>(k);\
  const V& vt = data::from_string<V>(v);\
  update(kt, vt);\
}

WRAPPER_IMPL(TypedLocalTable);
WRAPPER_IMPL(TypedGlobalTable);
