#ifndef GLOBALTABLE_H_
#define GLOBALTABLE_H_

#include "table.h"

namespace dsm {
class LocalTable;

class GlobalTable  : public Table,  public UntypedTable {
public:
  void Init(const TableDescriptor& tinfo);
  virtual ~GlobalTable();

  struct PartitionInfo {
    PartitionInfo() : dirty(false), tainted(false), owner(-1) {}
    bool dirty;
    bool tainted;
    int owner;
    ShardInfo sinfo;
  };

  virtual PartitionInfo* get_partition_info(int shard) {
    return &partinfo_[shard];
  }

  bool tainted(int shard) { return get_partition_info(shard)->tainted; }
  bool owner(int shard) { return get_partition_info(shard)->owner; }

  LocalTable *get_partition(int shard);
  virtual Table_Iterator* get_iterator(int shard);

  bool is_local_shard(int shard);
  bool is_local_key(const StringPiece &k);

  // Fill in a response from a remote worker for the given key.
  void handle_get(const HashGet& req, HashPut* resp);

  // Handle updates from the master or other workers.
  void SendUpdates();
  void ApplyUpdates(const HashPut& req);
  void HandlePutRequests();
  void UpdatePartitions(const ShardInfo& sinfo);

  int pending_write_bytes();

  // Clear any local data for which this table has ownership.  Pending updates
  // are *not* cleared.
  void clear(int shard);
  bool empty();
  void resize(int64_t new_size);

  virtual void start_checkpoint(const string& f);
  virtual void write_delta(const HashPut& d);
  virtual void finish_checkpoint();
  virtual void restore(const string& f);

  int64_t shard_size(int shard);

  virtual int get_shard_str(StringPiece k) = 0;

protected:
  vector<PartitionInfo> partinfo_;

  virtual LocalTable* create_local(int shard) = 0;
  boost::recursive_mutex& mutex() { return m_; }
  vector<LocalTable*> partitions_;
  vector<LocalTable*> cache_;

  volatile int pending_writes_;
  boost::recursive_mutex m_;

  friend class Worker;
  Worker *w_;
  int worker_id_;

  void set_worker(Worker *w);

  // Fetch the given key, using only local information.
  void get_local(const StringPiece &k, string *v);

  // Fetch key k from the node owning it.  Returns true if the key exists.
  bool get_remote(int shard, const StringPiece &k, string* v);
};

template <class K, class V>
class TypedLocalTable;

template <class K, class V>
class TypedGlobalTable : public GlobalTable, private boost::noncopyable {
public:
  void Init(const TableDescriptor &tinfo) {
    GlobalTable::Init(tinfo);
    for (int i = 0; i < partitions_.size(); ++i) {
      partitions_[i] = (TypedLocalTable<K, V>*)create_local(i);
    }

    pending_writes_ = 0;
  }

  int get_shard(const K& k);
  int get_shard_str(StringPiece k);
  V get_local(const K& k);

  // Store the given key-value pair in this hash. If 'k' has affinity for a
  // remote thread, the application occurs immediately on the local host,
  // and the update is queued for transmission to the owner.
  void put(const K &k, const V &v);
  void update(const K &k, const V &v);

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k);
  bool contains(const K &k);
  void remove(const K &k);
  Table_Iterator* get_iterator(int shard);
  TypedIterator<K, V>* get_typed_iterator(int shard);

  K key_from_string(StringPiece k) { return unmarshal(static_cast<Marshal<K>* >(this->info().key_marshal), k); }
  V value_from_string(StringPiece v) { return unmarshal(static_cast<Marshal<V>* >(this->info().value_marshal), v); }
  string key_to_string(const K& k) { return marshal(static_cast<Marshal<K>* >(this->info().key_marshal), k); }
  string value_to_string(const V& v) { return marshal(static_cast<Marshal<V>* >(this->info().value_marshal), v); }

  // String wrappers
  bool contains_str(const StringPiece& k) { return contains(key_from_string(k)); }
  string get_str(const StringPiece &k) { return value_to_string(get(key_from_string(k))); }
  void put_str(const StringPiece &k, const StringPiece &v) { return put(key_from_string(k), value_from_string(v)); }
  void remove_str(const StringPiece &k) { remove(key_from_string(k)); }
  void update_str(const StringPiece &k, const StringPiece &v) { return update(key_from_string(k), value_from_string(v)); }

protected:
  LocalTable* create_local(int shard);
};

};

#endif /* GLOBALTABLE_H_ */
