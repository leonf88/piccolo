#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include "util/file.h"
#include "worker/worker.pb.h"
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

    CHECK(info_->accum != NULL);
    CHECK(info_->key_marshal != NULL);
    CHECK(info_->value_marshal != NULL);
//    CHECK_NE(info_->block_info, NULL);
//    CHECK_NE(info_->partition_factory, NULL);
  }

  const TableDescriptor& info() const { return *info_; }

  int id() const { return info().table_id; }
  int shard() const { return info().shard; }
  int num_shards() const { return info().num_shards; }

protected:
  TableDescriptor *info_;
};

// Interface that typed tables should support.
template <class K, class V>
class TypedTable {
public:
  virtual bool contains(const K &k) = 0;
  virtual V get(const K &k) = 0;
  virtual void put(const K &k, const V &v) = 0;
  virtual void update(const K &k, const V &v) = 0;
  virtual void remove(const K &k) = 0;
};

template <class K, class V>
struct TypedTableIterator : public TableIterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;
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

}

#endif
