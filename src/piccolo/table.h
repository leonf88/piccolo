#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "piccolo/common.h"
#include "piccolo/file.h"
#include "piccolo/worker.pb.h"
#include <boost/thread.hpp>

namespace dsm {

#ifndef SWIG
// Commonly used accumulation operators.
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

struct TableIterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;
};

// Methods common to both global table views and local shards
class TableBase {
public:
  typedef TableIterator Iterator;
  virtual void Init(const TableDescriptor* info) {
    info_ = new TableDescriptor(*info);

//    CHECK(info_->accum != NULL);
    CHECK(info_->key_marshal != NULL);
    CHECK(info_->value_marshal != NULL);
  }

  const TableDescriptor& info() const { return *info_; }

  int id() const { return info().table_id; }
  int shard() const { return info().shard; }
  int num_shards() const { return info().num_shards; }

protected:
  TableDescriptor *info_;
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

class UntypedTable {
public:
  virtual bool contains_str(const StringPiece& k) = 0;
  virtual string get_str(const StringPiece &k) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece &v) = 0;
};


template <class K, class V>
struct TypedTableIterator : public TableIterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;
};

// Key/value typed interface.
template <class K, class V>
class TypedTable {
public:
  virtual bool contains(const K &k) = 0;
  virtual V get(const K &k) = 0;
  virtual void put(const K &k, const V &v) = 0;
  virtual void update(const K &k, const V &v) = 0;
  virtual void remove(const K &k) = 0;
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

struct RPCTableCoder : public TableCoder {
  RPCTableCoder(const TableData* in);
  virtual void WriteEntry(StringPiece k, StringPiece v);
  virtual bool ReadEntry(string* k, string *v);

  int read_pos_;
  TableData *t_;
};

struct LocalTableCoder : public TableCoder {
  LocalTableCoder(const string &f, const string& mode);
  virtual ~LocalTableCoder();

  virtual void WriteEntry(StringPiece k, StringPiece v);
  virtual bool ReadEntry(string* k, string *v);

  RecordFile *f_;
};
}

#endif
