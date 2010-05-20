#ifndef LOCALTABLE_H_
#define LOCALTABLE_H_

#include "table.h"

namespace dsm {
// Operations needed on a local shard of a table.
class LocalView : public TableView, public Checkpointable {
public:
  LocalView(const TableDescriptor &info) : TableView(info) {}
  virtual TableView::Iterator* get_iterator() = 0;
};

class LocalTable : public LocalView {
public:
  LocalTable(const TableDescriptor &tinfo) : LocalView(tinfo) {
    delta_file_ = NULL;
  }

  void ApplyUpdates(const HashPut& req);
  void write_delta(const HashPut& put);

  virtual void resize(int64_t new_size) = 0;
  virtual void clear() = 0;

  virtual int64_t size() = 0;
  bool empty() { return size() == 0; }

  // Generic routines to fetch and set entries as serialized strings.

  // Put replaces the current value (if any) with the new value specified.  Update
  // applies the accumulation function for this table to merge the existing and
  // new value.
  virtual string get_str(const StringPiece &k) = 0;
  virtual void put_str(const StringPiece &k, const StringPiece& v) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece& v) = 0;
  virtual bool contains_str(const StringPiece &k) = 0;
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

  TypedLocalTable(const TableDescriptor &tinfo);

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

  WRAPPER_FUNCTION_DECL;

private:
  DataMap data_;
};
}

#endif /* LOCALTABLE_H_ */
