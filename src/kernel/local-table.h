#ifndef LOCALTABLE_H_
#define LOCALTABLE_H_

#include "table.h"

#include "util/file.h"
#include "util/rpc.h"

namespace dsm {

struct HashPutCoder {
  HashPutCoder(HashPut *h);
  HashPutCoder(const HashPut& h);

  void add_pair(const string& k, const string& v);
  StringPiece key(int idx);
  StringPiece value(int idx);

  int size();

  HashPut *h_;
};


// Represents a single shard of a global table.
class LocalTable : public TableBase, public Checkpointable {
public:
  void Init(const TableDescriptor &tinfo) {
    TableBase::Init(tinfo);
    delta_file_ = NULL;
  }

  virtual TableBase::Iterator *get_iterator() = 0;

  void ApplyUpdates(const HashPut& req);
  void write_delta(const HashPut& put);

  virtual void resize(int64_t new_size) = 0;
  virtual void clear() = 0;

  virtual int64_t size() = 0;
  bool empty() { return size() == 0; }

  virtual bool contains_str(const StringPiece& k) = 0;
  virtual string get_str(const StringPiece &k) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece &v) = 0;

protected:
  friend class GlobalTable;
  int16_t owner;
  RecordFile *delta_file_;
};

template <class K, class V>
class TypedLocalTable : public LocalTable,
                         public TypedTable<K, V>,
                         private boost::noncopyable {
public:
  struct LocalIterator;

  typedef HashMap<K, V> DataMap;
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

  K key_from_string(StringPiece k) { return unmarshal(static_cast<Marshal<K>* >(this->info().key_marshal), k); }
  V value_from_string(StringPiece v) { return unmarshal(static_cast<Marshal<V>* >(this->info().value_marshal), v); }
  string key_to_string(const K& k) { return marshal(static_cast<Marshal<K>* >(this->info().key_marshal), k); }
  string value_to_string(const V& v) { return marshal(static_cast<Marshal<V>* >(this->info().value_marshal), v); }

  // String wrappers
  bool contains_str(const StringPiece& k);
  string get_str(const StringPiece &k);
  void update_str(const StringPiece &k, const StringPiece &v);
private:
  DataMap data_;
};

template<class K, class V>
struct TypedLocalTable<K, V>::LocalIterator  : public TypedTableIterator<K, V> {
  LocalIterator(TypedLocalTable<K, V> *t) :
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
  typename TypedLocalTable<K, V>::DataMap::iterator it_;
  TypedLocalTable<K, V> *t_;
};

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
TableIterator* TypedLocalTable<K, V>::get_iterator() {
  return new LocalIterator(this);
}

template<class K, class V>
typename TypedLocalTable<K, V>::Iterator* TypedLocalTable<K, V>::get_typed_iterator() {
  return new LocalIterator(this);
}

template<class K, class V>
V TypedLocalTable<K, V>::get(const K &k) {
  return data_.get(k);
}

template<class K, class V>
void TypedLocalTable<K, V>::put(const K &k, const V &v) {
  data_[k] = v;
}

template<class K, class V> void TypedLocalTable<K, V>::update(const K &k, const V &v) {
  typename HashMap<K, V>::iterator pos = data_.find(k);
  if (pos == data_.end()) {
    data_.put(k, v);
  } else {
    Accumulator<V>* a = (Accumulator<V>*)this->info_.accum;
    (*a)(&pos->second, v);
  }
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
  Timer t;
  LZOFile lz(f, "w");
  Encoder e(&lz);
  e.write(data_.size());

  for (typename DataMap::iterator i = data_.begin(); i != data_.end(); ++i) {
    e.write_string(this->key_to_string(i->first));
    e.write_string(this->value_to_string(i->second));
  }

  lz.sync();
  delta_file_ = new RecordFile(f + ".delta", "w");
  //  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
}

template<class K, class V>
void TypedLocalTable<K, V>::finish_checkpoint() {
  if (delta_file_) {
    delete delta_file_;
    delta_file_ = NULL;
  }
}

template<class K, class V> void TypedLocalTable<K, V>::restore(const string& f) {
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
  HashPut p;
  while (rf.read(&p)) {
    ApplyUpdates(p);
  }
}


template<class K, class V>
bool TypedLocalTable<K, V>::contains_str(const StringPiece& k) {
  return contains(key_from_string(k));
}

template<class K, class V>
string TypedLocalTable<K, V>::get_str(const StringPiece &k) {
  return value_to_string(get(key_from_string(k)));
}

template<class K, class V>
void TypedLocalTable<K, V>::update_str(const StringPiece &k,
                                             const StringPiece &v) {
  update(key_from_string(k), value_from_string(v));
}

typedef TypedLocalTable<int, int> LIntTable;

}

#endif /* LOCALTABLE_H_ */
