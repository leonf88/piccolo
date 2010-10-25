#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include "util/file.h"
#include "worker/worker.pb.h"
#include <boost/thread.hpp>

namespace dsm {

#ifndef SWIG
// Commonly used accumulation and sharding operators.
template <class V>
struct Accumulators {
  struct Min : public Accumulator<V> {
    void Accumulate(V* a, const V& b) { *a = std::min(*a, b); }
  };

  struct Max : public Accumulator<V> {
    void Accumulate(V* a, const V& b) { *a = std::max(*a, b); }
  };

  struct Sum : public Accumulator<V> {
    void Accumulate(V* a, const V& b) { *a = *a + b; }
  };

  struct Replace : public Accumulator<V> {
    void Accumulate(V* a, const V& b) { *a = b; }
  };
};

struct Sharding {
  struct String  : public Sharder<string> {
    int operator()(const string& k, int shards) { return StringPiece(k).hash() % shards; }
  };

  struct Mod : public Sharder<int> {
    int operator()(const int& key, int shards) { return key % shards; }
  };

  struct UintMod : public Sharder<uint32_t> {
    int operator()(const uint32_t& key, int shards) { return key % shards; }
  };
};
#endif

struct TableBase;

struct TableFactory {
  virtual TableBase* New() = 0;
};

struct TableDescriptor {
public:
  TableDescriptor(int id, int shards) {
    table_id = id;
    num_shards = shards;
    block_size = 500;
  }

  TableDescriptor(const TableDescriptor& t) {
    memcpy(this, &t, sizeof(t));
  }

  int table_id;
  int num_shards;

  // For local tables, the shard of the global table they represent.
  int shard;
  int default_shard_size;

  void *accum;
  void *sharder;
  void *key_marshal;
  void *value_marshal;
  TableFactory *partition_factory;

  // for dense tables
  int block_size;
  void *block_info;
};

class TableIterator;

// Methods common to both global and local table views.
class TableBase {
  public:
  typedef TableIterator Iterator;
  virtual void Init(const TableDescriptor* info) {
    info_ = new TableDescriptor(*info);

    CHECK(info_->key_marshal != NULL);
    CHECK(info_->value_marshal != NULL);
  }

  const TableDescriptor& info() const { return *info_; }

  int id() const { return info().table_id; }
  int num_shards() const { return info().num_shards; }

protected:
  TableDescriptor *info_;
};

class UntypedTable {
public:
  virtual bool contains_str(const StringPiece& k) = 0;
  virtual string get_str(const StringPiece &k) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece &v) = 0;
};

// Key/value typed interface.
template <class K, class V>
class TypedTable : public UntypedTable {
public:
  virtual bool contains(const K &k) = 0;
  virtual V get(const K &k) = 0;
  virtual void put(const K &k, const V &v) = 0;
  virtual void update(const K &k, const V &v) = 0;
  virtual void remove(const K &k) = 0;

  // Default specialization for untyped methods
  virtual bool contains_str(const StringPiece& s) {
    K k;
    kmarshal()->unmarshal(s, &k);
    return contains(k);
  }

  virtual string get_str(const StringPiece &s) {
    K k;
    string out;

    kmarshal()->unmarshal(s, &k);
    vmarshal()->marshal(get(k), &out);
    return out;
  }

  virtual void update_str(const StringPiece& kstr, const StringPiece &vstr) {
    K k; V v;
    kmarshal()->unmarshal(kstr, &k);
    vmarshal()->unmarshal(vstr, &v);
    update(k, v);
  }
protected:
  virtual Marshal<K> *kmarshal() = 0;
  virtual Marshal<V> *vmarshal() = 0;
};

struct TableIterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;
};

template <class K, class V>
struct TypedTableIterator : public TableIterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;

  virtual void key_str(string *out) { kmarshal()->marshal(key(), out); }
  virtual void value_str(string *out) { vmarshal()->marshal(value(), out); }

protected:
  virtual Marshal<K> *kmarshal() {
    static Marshal<K> m;
    return &m;
  }

  virtual Marshal<V> *vmarshal() {
    static Marshal<V> m;
    return &m;
  }
};


class TableData;

// Checkpoint and restoration.
class Checkpointable {
public:
  virtual void start_checkpoint(const string& f) = 0;
  virtual void write_delta(const TableData& put) = 0;
  virtual void finish_checkpoint() = 0;
  virtual void restore(const string& f) = 0;
};

// Interface for serializing tables, either to disk or for transmitting via RPC.
struct TableCoder {
  virtual void WriteEntry(StringPiece k, StringPiece v) = 0;
  virtual bool ReadEntry(string* k, string *v) = 0;

  virtual ~TableCoder() {}
};

class Serializable {
public:
  virtual void ApplyUpdates(TableCoder *in) = 0;
  virtual void Serialize(TableCoder* out) = 0;
};
}

#endif
