#ifndef LOCALTABLE_H_
#define LOCALTABLE_H_

#include "table.h"

namespace dsm {

// Represents a single shard of a global table.
class LocalTable : public Table, public Checkpointable, public UntypedTable {
public:
  void Init(const TableDescriptor &tinfo) {
    Table::Init(tinfo);
    delta_file_ = NULL;
  }

  virtual Table::Iterator *get_iterator() = 0;

  void ApplyUpdates(const HashPut& req);
  void write_delta(const HashPut& put);

  virtual void resize(int64_t new_size) = 0;
  virtual void clear() = 0;

  virtual int64_t size() = 0;
  bool empty() { return size() == 0; }
protected:
  friend class GlobalTable;
  int16_t owner;
  RecordFile *delta_file_;
};

template <class K, class V>
class TypedLocalTable : public LocalTable, private boost::noncopyable {
public:
  typedef HashMap<K, V> DataMap;
  struct Iterator;

  void Init(const TableDescriptor &tinfo) {
    LocalTable::Init(tinfo);
    data_.rehash(1);//tinfo.default_shard_size);
    owner = -1;
  }

  bool empty();
  int64_t size();

  Table_Iterator* get_iterator();
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
  bool contains_str(const StringPiece& k) { return contains(key_from_string(k)); }
  string get_str(const StringPiece &k) { return value_to_string(get(key_from_string(k))); }
  void put_str(const StringPiece &k, const StringPiece &v) { put(key_from_string(k), value_from_string(v)); }
  void remove_str(const StringPiece &k) { remove(key_from_string(k)); }
  void update_str(const StringPiece &k, const StringPiece &v) { update(key_from_string(k), value_from_string(v)); }
private:
  DataMap data_;
};
}

#endif /* LOCALTABLE_H_ */
