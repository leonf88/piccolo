#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"

namespace dsm {

class HashUpdate;

static int StringSharding(const string& k, int shards) { return StringPiece(k).hash() % shards; }
static int ModSharding(const int& key, int shards) { return key % shards; }

template <class V>
struct Accumulator {
  static V min(const V& a, const V& b) { return std::min(a, b); }
  static V max(const V& a, const V& b) { return std::max(a, b); }
  static V sum(const V& a, const V& b) { return a + b; }
  static V replace(const V& a, const V& b) { return b; }
};

struct Data {
  // strings
  static void marshal(const string& t, string *out) { *out = t; }
  static void unmarshal(const StringPiece& s, string *t) { t->assign(s.data, s.len); }

  // protocol messages
  static void marshal(const google::protobuf::Message& t, string *out) { t.SerializePartialToString(out); }
  static void unmarshal(const StringPiece& s, google::protobuf::Message* t) { t->ParseFromArray(s.data, s.len); }

  template <class T>
  static void marshal(const T& t, string* out) {
    out->assign(reinterpret_cast<const char*>(&t), sizeof(t));
  }

  template <class T>
  static void unmarshal(const StringPiece& s, T *t) {
    *t = *reinterpret_cast<const T*>(s.data);
  }

  template <class T>
  static string to_string(const T& t) {
    string t_marshal;
    marshal(t, &t_marshal);
    return t_marshal;
  }

  template <class T>
  static T from_string(const StringPiece& t) {
    T t_marshal;
    unmarshal(t, &t_marshal);
    return t_marshal;
  }
};

struct HashUpdateCoder {
  HashUpdateCoder(HashUpdate *h);
  HashUpdateCoder(const HashUpdate& h);

  void add_pair(const string& k, const string& v);
  StringPiece key(int idx);
  StringPiece value(int idx);

  int size();

  HashUpdate *h_;
};

class Worker;

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

  TableInfo info_;
};

// A local partition of a global table.
class LocalTable : public Table {
public:
  LocalTable(const TableInfo& info) : Table(info), dirty(false), tainted(false), owner(-1) {}

  virtual void clear() = 0;

  // Returns a view on the global table containing values only from 'shard'.
  // 'shard' must be local.
  virtual Table::Iterator* get_iterator() = 0;
  void ApplyUpdates(const HashUpdate& up);

  static void SerializePartial(HashUpdate& r, Table::Iterator *it);

protected:
  friend class GlobalTable;
  bool dirty;
  bool tainted;
  int16_t owner;
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

  virtual int get_shard_str(StringPiece k) = 0;

  bool is_local_shard(int shard);
  bool is_local_key(const StringPiece &k);

  void set_owner(int shard, int worker);
  int get_owner(int shard);

  void get_local(const StringPiece &k, string *v);
  void get_remote(int shard, const StringPiece &k, string* v);

  // Transmit any buffered update data to remote peers.
  void SendUpdates();
  void ApplyUpdates(const HashUpdate& req);
  void CheckForUpdates();

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

protected:
  friend class Worker;
  virtual LocalTable* create_local(int shard) = 0;

  vector<LocalTable*> partitions_;

  volatile int pending_writes_;
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
  typedef V (*AccumFunction)(const V& a, const V& b);
  typedef int (*ShardingFunction)(const K& k, int num_shards);
};

}
#endif
