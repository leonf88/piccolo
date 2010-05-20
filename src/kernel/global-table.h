#ifndef GLOBALTABLE_H_
#define GLOBALTABLE_H_

#include "table.h"

namespace dsm {
class LocalTable;

class GlobalView : public TableView, public Checkpointable {
public:
  GlobalView(const TableDescriptor& info) : TableView(info) {
    partinfo_.resize(num_shards());
  }

  void clear_tainted(int shard);
  void set_tainted(int shard);
  bool tainted(int shard);
  bool dirty(int shard);
  void set_dirty(int shard);
  void set_owner(int shard, int worker);
  int get_owner(int shard);
  void UpdateShardinfo(const ShardInfo& sinfo);

  virtual TableView::Iterator* get_iterator(int shard) = 0;
  virtual int64_t shard_size(int shard) = 0;
  virtual int pending_write_bytes() = 0;

  virtual void ApplyUpdates(const HashPut& req) = 0;
  virtual void SendUpdates() = 0;
  virtual void HandlePutRequests() = 0;

  virtual int get_shard_str(StringPiece k) = 0;
  virtual bool is_local_shard(int shard) = 0;
  virtual void handle_get(const HashGet& req, HashPut* resp) = 0;
  virtual void set_worker(Worker* w) = 0;


protected:
  struct PartitionInfo {
    PartitionInfo() : dirty(false), tainted(false), owner(-1) {}
    bool dirty;
    bool tainted;
    int owner;
    ShardInfo sinfo;
  };

  vector<PartitionInfo> partinfo_;
};

class GlobalTable : public GlobalView {
public:
  GlobalTable(const TableDescriptor& tinfo);
  virtual ~GlobalTable();

  LocalTable *get_partition(int shard);
  Table_Iterator* get_iterator(int shard);
  bool is_local_shard(int shard);
  bool is_local_key(const StringPiece &k);

  // Fill in a response from a remote worker for the given key.
  void handle_get(const HashGet& req, HashPut* resp);

  // Transmit any buffered update data to remote peers.
  void SendUpdates();
  void ApplyUpdates(const HashPut& req);

  int pending_write_bytes();

  // Clear any local data for which this table has ownership.  Pending updates
  // are *not* cleared.
  void clear(int shard);
  bool empty();
  void resize(int64_t new_size);

  void start_checkpoint(const string& f);
  void write_delta(const HashPut& d);
  void finish_checkpoint();
  void restore(const string& f);

  int64_t shard_size(int shard);

  virtual int get_shard_str(StringPiece k) = 0;

  void HandlePutRequests();
protected:
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
class TypedGlobalTable : public GlobalTable, private boost::noncopyable {
public:
  TypedGlobalTable(const TableDescriptor& tinfo);
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

  WRAPPER_FUNCTION_DECL;
protected:
  LocalTable* create_local(int shard);
};
}

#endif /* GLOBALTABLE_H_ */
