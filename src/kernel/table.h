#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include <boost/thread.hpp>

namespace dsm {

class HashPut;

static int StringSharding(const string& k, int shards) { return StringPiece(k).hash() % shards; }
static int ModSharding(const int& key, int shards) { return key % shards; }
static int UintModSharding(const uint32_t& key, int shards) { return key % shards; }

template <class V>
struct Accumulator {
  static void min(V* a, const V& b) { *a = std::min(*a, b); }
  static void max(V* a, const V& b) { *a = std::max(*a, b); }
  static void sum(V* a, const V& b) { *a = *a + b; }
  static void replace(V* a, const V& b) { *a = b; }
};

struct HashPutCoder {
  HashPutCoder(HashPut *h);
  HashPutCoder(const HashPut& h);

  void add_pair(const string& k, const string& v);
  StringPiece key(int idx);
  StringPiece value(int idx);

  int size();

  HashPut *h_;
};

class Worker;
class RecordFile;

struct TableInfo {
public:
  int table_id;
  int num_shards;

  // For local tables, the shard of the global table they represent.
  int shard;

  // We use void* to pass around the various accumulation and sharding
  // functions; they are cast to the appropriate type at the time of use.
  void *accum_function;
  void *sharding_function;

  // Used for remote sends, and to trigger polling when needed.
  Worker *worker;
};

class Table;
struct Table_Iterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;

  virtual Table *owner() = 0;
};

class Table {
public:
  typedef Table_Iterator Iterator;

  Table(TableInfo tinfo) : info_(tinfo) {}
  virtual ~Table() {}

  // Generic routines to fetch and set entries as serialized strings.
  virtual string get_str(const StringPiece &k) = 0;
  virtual void put_str(const StringPiece &k, const StringPiece& v) = 0;

  virtual bool empty() = 0;
  virtual int64_t size() = 0;

  const TableInfo& info() const { return info_; }
  void set_info(const TableInfo& t) { info_ = t; }

  int id() const { return info_.table_id; }
  int shard() const { return info_.shard; }
  int num_shards() const { return info_.num_shards; }

  // Checkpoint and restoration.
  virtual void start_checkpoint(const string& f) = 0;
  virtual void write_delta(const HashPut& put) = 0;
  virtual void finish_checkpoint() = 0;

  virtual void restore(const string& f) = 0;

  TableInfo info_;
};

// A local partition of a global table.
class LocalTable : public Table {
public:
  LocalTable(const TableInfo& info) :
    Table(info), dirty(false), tainted(false), owner(-1) {}

  void write_delta(const HashPut& put);

  virtual void clear() = 0;
  virtual Table::Iterator* get_iterator() = 0;

  void ApplyUpdates(const HashPut& up);

  // Serialize a portion of the given iterator, up to the network batch limit.
  static void SerializePartial(HashPut& r, Table::Iterator *it);

protected:
  friend class GlobalTable;
  bool dirty;
  bool tainted;
  int16_t owner;

  RecordFile *delta_file_;
};

class GlobalTable : public Table {
public:
  GlobalTable(const TableInfo& info);

  virtual ~GlobalTable() {
    for (int i = 0; i < partitions_.size(); ++i) {
      delete partitions_[i];
    }
  }

  LocalTable *get_partition(int shard) { return partitions_[shard]; }
  Table::Iterator* get_iterator(int shard) {
    return partitions_[shard]->get_iterator();
  }

  // Generic methods against serialized strings.
  virtual bool contains_str(StringPiece k) = 0;
  virtual int get_shard_str(StringPiece k) = 0;

  bool is_local_shard(int shard);
  bool is_local_key(const StringPiece &k);

  void set_owner(int shard, int worker);
  int get_owner(int shard);

  // Fetch the given key, using only local information.
  void get_local(const StringPiece &k, string *v);

  // Fetch key k from the node owning it.  Returns true if the key exists.
  bool get_remote(int shard, const StringPiece &k, string* v);

  // Transmit any buffered update data to remote peers.
  void SendUpdates();
  void ApplyUpdates(const HashPut& req);

  int pending_write_bytes();

  // Clear any local data for which this table has ownership.  Pending updates
  // are *not* cleared.
  void clear(int shard);
  bool empty();
  int64_t size() { return 1; }

  void set_dirty(int shard) { partitions_[shard]->dirty = true; }
  bool dirty(int shard) { return partitions_[shard]->dirty || !partitions_[shard]->empty(); }

  void set_tainted(int shard) { partitions_[shard]->tainted = true; }
  void clear_tainted(int shard) { partitions_[shard]->tainted = false; }
  bool tainted(int shard) { return partitions_[shard]->tainted; }

  void start_checkpoint(const string& f);
  void write_delta(const HashPut& d);
  void finish_checkpoint();

  void restore(const string& f);

  boost::recursive_mutex& mutex() { return m_; }

protected:
  friend class Worker;
  virtual LocalTable* create_local(int shard) = 0;

  vector<LocalTable*> partitions_;

  volatile int pending_writes_;
  boost::recursive_mutex m_;
};

template <class K, class V>
struct TypedTable_Iterator : public Table_Iterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;
};

template <class K, class V>
class TypedTable {
public:
  typedef TypedTable_Iterator<K, V> Iterator;

  // Functions for locating and accumulating data.
  typedef void (*AccumFunction)(V* a, const V& b);
  typedef int (*ShardingFunction)(const K& k, int num_shards);
};

}
#endif
