#ifndef LOCALTABLE_H_
#define LOCALTABLE_H_

#include "table.h"

#include "util/file.h"
#include "util/rpc.h"
#include "kernel/sparse-map.h"
#include "kernel/dense-map.h"

namespace dsm {

// Represents a single shard of a global table.
class LocalTable : public TableBase, public Checkpointable {
public:
  void Init(const TableDescriptor &tinfo) {
    TableBase::Init(tinfo);
    delta_file_ = NULL;
  }

  virtual TableBase::Iterator *get_iterator() = 0;

  void write_delta(const TableData& put);

  virtual void resize(int64_t new_size) = 0;
  virtual void clear() = 0;

  virtual int64_t size() = 0;
  bool empty() { return size() == 0; }

  virtual bool contains_str(const StringPiece& k) = 0;
  virtual string get_str(const StringPiece &k) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece &v) = 0;

  virtual void SerializePartial(TableData *req) = 0;
  virtual void ApplyUpdates(const TableData& req) = 0;

protected:
  friend class GlobalTable;
  int16_t owner;
  RecordFile *delta_file_;
};

template <class K, class V, class Map=SparseMap<K, V> >
class TypedLocalTable : public LocalTable,
                         public TypedTable<K, V>,
                         private boost::noncopyable {
public:
  struct LocalIterator;

  void Init(const TableDescriptor &tinfo) {
    LocalTable::Init(tinfo);
    data_.rehash(1);//tinfo.default_shard_size);
    owner = -1;
  }

  bool empty();
  int64_t size();

  TableIterator* get_iterator();
  Iterator* get_typed_iterator();

  bool contains(const K &k);
  V get(const K &k);
  void put(const K &k, const V &v);
  void update(const K &k, const V &v);
  void remove(const K &k);
  void resize(int64_t new_size);

  void start_checkpoint(const string& f);
  void finish_checkpoint();
  void restore(const string& f);

  void clear();

  void SerializePartial(TableData *req) {
    data_.SerializePartial(req, *(Marshal<K>*)info_.key_marshal, *(Marshal<V>*)info_.value_marshal);
    req->set_done(true);
  }

  void ApplyUpdates(const TableData& req) {
    data_.ApplyUpdates(req,
                       *(Marshal<K>*)info_.key_marshal,
                       *(Marshal<V>*)info_.value_marshal,
                       *(Accumulator<V>*)info_.accum);
  }

  K key_from_string(StringPiece k) { return unmarshal(static_cast<Marshal<K>* >(this->info().key_marshal), k); }
  V value_from_string(StringPiece v) { return unmarshal(static_cast<Marshal<V>* >(this->info().value_marshal), v); }
  string key_to_string(const K& k) { return marshal(static_cast<Marshal<K>* >(this->info().key_marshal), k); }
  string value_to_string(const V& v) { return marshal(static_cast<Marshal<V>* >(this->info().value_marshal), v); }

  // String wrappers
  bool contains_str(const StringPiece& k);
  string get_str(const StringPiece &k);
  void update_str(const StringPiece &k, const StringPiece &v);
private:
  Map data_;
};

template<class K, class V, class Map>
struct TypedLocalTable<K, V, Map>::LocalIterator  : public TypedTableIterator<K, V> {
  LocalIterator(TypedLocalTable<K, V, Map> *t) :
    it_(t->data_.begin()) {
    t_ = t;
  }

  void key_str(string *out) {
    static_cast<Marshal<K>*>(this->owner()->info().key_marshal)->marshal(key(), out);
  }
  void value_str(string *out) {
    static_cast<Marshal<V>*>(this->owner()->info().value_marshal)->marshal(value(), out);
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
  typename Map::iterator it_;
  TypedLocalTable<K, V, Map> *t_;
};

template<class K, class V, class Map>
bool TypedLocalTable<K, V, Map>::contains(const K &k) {
  return data_.find(k) != data_.end();
}

template<class K, class V, class Map>
bool TypedLocalTable<K, V, Map>::empty() {
  return data_.empty();
}

template<class K, class V, class Map>
int64_t TypedLocalTable<K, V, Map>::size() {
  return data_.size();
}

template<class K, class V, class Map>
TableIterator* TypedLocalTable<K, V, Map>::get_iterator() {
  return new LocalIterator(this);
}

template<class K, class V, class Map>
typename TypedLocalTable<K, V, Map>::Iterator* TypedLocalTable<K, V, Map>::get_typed_iterator() {
  return new LocalIterator(this);
}

template<class K, class V, class Map>
V TypedLocalTable<K, V, Map>::get(const K &k) {
  return data_.get(k);
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::put(const K &k, const V &v) {
  data_[k] = v;
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::update(const K &k, const V &v) {
  typename Map::iterator pos = data_.find(k);
  if (pos == data_.end()) {
//    LOG(INFO) << "Inserting: " << k;
    data_.put(k, v);
  } else {
//    LOG(INFO) << "Accumulating: " << k;
    Accumulator<V>* a = (Accumulator<V>*)this->info_.accum;
    (*a)(&pos->second, v);
  }
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::remove(const K &k) {
  data_.erase(data_.find(k));
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::clear() {
  data_.clear();
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::resize(int64_t new_size) {
  data_.rehash(new_size);
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::start_checkpoint(const string& f) {
  Timer t;
  LZOFile lz(f, "w");
  Encoder e(&lz);
  e.write(data_.size());

  for (typename Map::iterator i = data_.begin(); i != data_.end(); ++i) {
    e.write_string(this->key_to_string(i->first));
    e.write_string(this->value_to_string(i->second));
  }

  lz.sync();
  delta_file_ = new RecordFile(f + ".delta", "w");
  //  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::finish_checkpoint() {
  if (delta_file_) {
    delete delta_file_;
    delta_file_ = NULL;
  }
}

template<class K, class V, class Map> void TypedLocalTable<K, V, Map>::restore(const string& f) {
  LZOFile lz(f, "r");
  Decoder d(&lz);
  int size;
  d.read(&size);
  data_.clear();
  data_.rehash(size);

  string k, v;
  for (uint32_t i = 0; i < size; ++i) {
    d.read_string(&k);
    d.read_string(&v);
    data_.put(this->key_from_string(k), this->value_from_string(v));
  }

  // Replay delta log.
  RecordFile rf(f + ".delta", "r");
  TableData p;
  while (rf.read(&p)) {
    ApplyUpdates(p);
  }
}


template<class K, class V, class Map>
bool TypedLocalTable<K, V, Map>::contains_str(const StringPiece& k) {
//  LOG(INFO) << "Contains: " << key_from_string(k);
  return contains(key_from_string(k));
}

template<class K, class V, class Map>
string TypedLocalTable<K, V, Map>::get_str(const StringPiece &k) {
  return value_to_string(get(key_from_string(k)));
}

template<class K, class V, class Map>
void TypedLocalTable<K, V, Map>::update_str(const StringPiece &k,
                                             const StringPiece &v) {
  update(key_from_string(k), value_from_string(v));
}

typedef TypedLocalTable<int, int> LIntTable;

}

#endif /* LOCALTABLE_H_ */
