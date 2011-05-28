#ifndef GLOBALTABLE_H_
#define GLOBALTABLE_H_

#include "table.h"
#include "local-table.h"

#include "util/file.h"
#include "util/rpc.h"

#include <queue>

//#define GLOBAL_TABLE_USE_SCOPEDLOCK

#define RETRIGGER_MODE_EXPLICIT 0
#define RETRIGGER_MODE_IMPLICIT 1
#define RETRIGGER_SCAN_INTERVAL 0.2

namespace dsm {

class Worker;
class Master;

// Encodes table entries using the passed in TableData protocol buffer.
struct ProtoTableCoder : public TableCoder {
  ProtoTableCoder(const TableData* in);
  virtual void WriteEntry(StringPiece k, StringPiece v);
  virtual bool ReadEntry(string* k, string *v);

  int read_pos_;
  TableData *t_;
};


struct PartitionInfo {
  PartitionInfo() : dirty(false), tainted(false) {}
  bool dirty;
  bool tainted;
  ShardInfo sinfo;
};

class GlobalTable : virtual public TableBase {
public:
  virtual void UpdatePartitions(const ShardInfo& sinfo) = 0;
  virtual TableIterator* get_iterator(int shard,unsigned int fetch_num = FETCH_NUM) = 0;

  virtual bool is_local_shard(int shard) = 0;
  virtual bool is_local_key(const StringPiece &k) = 0;

  virtual PartitionInfo* get_partition_info(int shard) = 0;
  virtual LocalTable* get_partition(int shard) = 0;

  virtual bool tainted(int shard) = 0;
  virtual int owner(int shard) = 0;

protected:
  friend class Worker;
  friend class Master;

  // Fill in a response from a remote worker for the given key.
  virtual void handle_get(const HashGet& req, TableData* resp) = 0;
  virtual int64_t shard_size(int shard) = 0;
};

class MutableGlobalTable : virtual public GlobalTable {
public:
  MutableGlobalTable(): applyingupdates(false) {}

  // Handle updates from the master or other workers.
  virtual void SendUpdates() = 0;
  virtual void SendUpdates(int* count) = 0;
  virtual void ApplyUpdates(const TableData& req) = 0;
  virtual void HandlePutRequests() = 0;

  virtual int pending_write_bytes() = 0;
  virtual int clearUpdateQueue() = 0;

  virtual void clear() = 0;
  virtual void resize(int64_t new_size) = 0;

  // Exchange the content of this table with that of table 'b'.
  virtual void swap(GlobalTable *b) = 0;
protected:
  friend class Worker;
  virtual void local_swap(GlobalTable *b) = 0;

  bool applyingupdates;
};

class GlobalTableBase : virtual public GlobalTable {
public:
  virtual ~GlobalTableBase();

  void Init(const TableDescriptor *tinfo);

  void UpdatePartitions(const ShardInfo& sinfo);

  virtual TableIterator* get_iterator(int shard, unsigned int fetch_num = FETCH_NUM) = 0;

  virtual bool is_local_shard(int shard);
  virtual bool is_local_key(const StringPiece &k);

  int64_t shard_size(int shard);

  // Fill in a response from a remote worker for the given key.
  void handle_get(const HashGet& req, TableData* resp);

  PartitionInfo* get_partition_info(int shard) { return &partinfo_[shard]; }
  LocalTable* get_partition(int shard) { return partitions_[shard]; }

  bool tainted(int shard) { return get_partition_info(shard)->tainted; }
  int owner(int shard) { return get_partition_info(shard)->sinfo.owner(); }
protected:
  virtual int shard_for_key_str(const StringPiece& k) = 0;

  // Fetch the given key, using only local information.
  void get_local(const StringPiece &k, string *v);

  // Fetch key k from the node owning it.  Returns true if the key exists.
  bool get_remote(int shard, const StringPiece &k, string* v);

  int worker_id_;

  // partitions_ for buffering remote writes to non-Trigger tables,
  // or writebufs_ for Trigger tables
  vector<LocalTable*> partitions_;
  vector<TableData*> writebufs_;
  vector<ProtoTableCoder*> writebufcoders_;

  vector<LocalTable*> cache_;

  boost::recursive_mutex& mutex() { return m_; }
  boost::recursive_mutex m_;

  boost::recursive_mutex& trigger_mutex() { return m_trig_; }
  boost::recursive_mutex m_trig_;

  boost::mutex& retrigger_mutex() { return m_retrig_; }
  boost::mutex m_retrig_;

  vector<PartitionInfo> partinfo_;

  struct CacheEntry {
    double last_read_time;
    string value;
  };

  unordered_map<StringPiece, CacheEntry> remote_cache_;
};

class MutableGlobalTableBase :
  virtual public GlobalTableBase,
  virtual public MutableGlobalTable,
  virtual public Checkpointable {
public:
  MutableGlobalTableBase() : pending_writes_(0) {}

  void SendUpdates();
  void SendUpdates(int* count);
  virtual void ApplyUpdates(const TableData& req) = 0;
  void HandlePutRequests();

  int pending_write_bytes();

  void clear();
  void resize(int64_t new_size);

  void start_checkpoint(const string& f);
  void write_delta(const TableData& d);
  void finish_checkpoint();
  void restore(const string& f);

  void swap(GlobalTable *b);

protected:
  int64_t pending_writes_;
  void local_swap(GlobalTable *b);
};

template <class K, class V>
class TypedGlobalTable :
  virtual public GlobalTable,
  public MutableGlobalTableBase,
  public TypedTable<K, V>,
  private boost::noncopyable {
public:
  //update queuing support
  typedef pair<K, V> KVPair;
  deque<KVPair> update_queue;
  bool clearingUpdateQueue;

  //long trigger support
  typedef pair<K,TriggerID> RetriggerPair;
  queue<RetriggerPair> retrigger_map;	// which keys still
										// request long triggers
  int retrigger_mode;		// 0 = scan retrigger_map for flags
							// 1 = scan table for flags

  typedef TypedTableIterator<K, V> Iterator;
  typedef DecodeIterator<K, V> UpdateDecoder;
  virtual void Init(const TableDescriptor *tinfo, int retrigt_count) {
    GlobalTableBase::Init(tinfo);
    for (int i = 0; i < partitions_.size(); ++i) {
      // For non-triggered tables that allow remote accumulation
      partitions_[i] = create_local(i);

      // For triggered tables that do not allow remote accumulation
      writebufs_[i] = new TableData;
      writebufcoders_[i] = new ProtoTableCoder(writebufs_[i]);
    }
    
    //Clear the update queue, just in case
    clearingUpdateQueue = false;
    update_queue.clear();

	//Clear the long/retrigger map, just in case, and
    //then start up the retrigger thread
    if (retrigt_count != 0) {
      while (!retrigger_map.empty()) retrigger_map.pop();
	  retrigger_mode = RETRIGGER_MODE_EXPLICIT; //separate map to start
      for (int i=0; i<retrigt_count; i++)
        boost::thread(boost::bind(&TypedGlobalTable<K, V>::retrigger_thread,this));
    }
  }

  int get_shard(const K& k);
  V get_local(const K& k);

  // Store the given key-value pair in this hash. If 'k' has affinity for a
  // remote thread, the application occurs immediately on the local host,
  // and the update is queued for transmission to the owner.
  void put(const K &k, const V &v);
  void update(const K &k, const V &v);
  void enqueue_update(K k, V v);
  int clearUpdateQueue();

  // Provide a mechanism to enable a long trigger / retrigger, as well as
  // a function from which to create a retrigger thread
  void enable_retrigger(K k, TriggerID id);
  void retrigger_thread(void);

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k);
  bool contains(const K &k);
  void remove(const K &k);
  TableIterator* get_iterator(int shard, unsigned int fetch_num = FETCH_NUM);
  TypedTable<K, V>* partition(int idx) {
    return dynamic_cast<TypedTable<K, V>* >(partitions_[idx]);
  }
  ProtoTableCoder* writebufcoder(int idx) {
    return writebufcoders_[idx];
  }

  virtual TypedTableIterator<K, V>* get_typed_iterator(int shard,unsigned int fetch_num = FETCH_NUM) {
    return static_cast<TypedTableIterator<K, V>* >(get_iterator(shard,fetch_num));
  }

  void ApplyUpdates(const dsm::TableData& req) {
    boost::recursive_mutex::scoped_lock sl(mutex());
    if (applyingupdates == true) {
      VLOG(2) << "Avoiding recursive ApplyUpdate()." << endl;
      return;						//prevent recursive applyupdates
    }

    applyingupdates = true;
    VLOG(3) << "Performing non-recursive ApplyUpdate()." << endl;
    if (!is_local_shard(req.shard())) {
      LOG_EVERY_N(INFO, 1000)
          << "Forwarding push request from: " << MP(id(), req.shard())
          << " to " << owner(req.shard());
    }

    // Changes to support locality of triggers <CRM>
    ProtoTableCoder c(&req);
    UpdateDecoder it;
    partitions_[req.shard()]->DecodeUpdates(&c, &it);
    for(;!it.done(); it.Next()) {
      update(it.key(),it.value());
    }
    applyingupdates = false;
  }

  Marshal<K> *kmarshal() { return ((Marshal<K>*)info_.key_marshal); }
  Marshal<V> *vmarshal() { return ((Marshal<V>*)info_.value_marshal); }

protected:
  int shard_for_key_str(const StringPiece& k);
  virtual LocalTable* create_local(int shard);

};

static const int kWriteFlushCount = 1000000;

template<class K, class V>
class RemoteIterator : public TypedTableIterator<K, V> {
public:
  RemoteIterator(TypedGlobalTable<K, V> *table, int shard, unsigned int fetch_num = FETCH_NUM) :
    owner_(table), shard_(shard), done_(false), fetch_num_(fetch_num) {
    request_.set_table(table->id());
    request_.set_shard(shard_);
    request_.set_row_count(fetch_num_);
    int target_worker = table->owner(shard);

    // << CRM 2011-01-18 >>
    while (!cached_results.empty()) cached_results.pop();

    VLOG(3) << "Created RemoteIterator on table " << table->id() << ", shard " << shard <<" @" << this << endl;
    NetworkThread::Get()->Call(target_worker+1, MTYPE_ITERATOR, request_, &response_);
    for(int i=1; i<=response_.row_count(); i++) {
      pair<string, string> row;
      row = make_pair(response_.key(i-1),response_.value(i-1));
      cached_results.push(row);
    }

    request_.set_id(response_.id());
  }

  void key_str(string *out) {
    if (!cached_results.empty())
		VLOG(4) << "Pulling first of " << cached_results.size() << " results" << endl;
    if (!cached_results.empty())
        *out = cached_results.front().first;
  }

  void value_str(string *out) {
    if (!cached_results.empty())
		VLOG(4) << "Pulling first of " << cached_results.size() << " results" << endl;
    if (!cached_results.empty())
        *out = cached_results.front().second;
  }

  bool done() {
    return response_.done() && cached_results.empty();
  }

  void Next() {
    int target_worker = dynamic_cast<GlobalTable*>(owner_)->owner(shard_);
    if (!cached_results.empty()) cached_results.pop();
    if (cached_results.empty()) {
      if (response_.done())								//if the last response indicated no more
        return;											//data and now no cache, don't try.
      NetworkThread::Get()->Call(target_worker+1, MTYPE_ITERATOR, request_, &response_);
      if (response_.row_count() < 1 && !response_.done())
        LOG(ERROR) << "Call to server requesting " << request_.row_count() <<
			" rows returned " << response_.row_count() << " rows." << endl;
      for(int i=1; i<=response_.row_count(); i++) {
        pair<string, string> row;
		row = make_pair(response_.key(i-1),response_.value(i-1));
        cached_results.push(row);
      }
    } else {
      VLOG(4) << "[PREFETCH] Using cached key for Next()" << endl;
    }
    ++index_;
  }

  const K& key() {
    if (cached_results.empty())
      LOG(FATAL) << "Cache miss on key!" << endl;
    ((Marshal<K>*)(owner_->info().key_marshal))->unmarshal((cached_results.front().first), &key_);
    return key_;
  }

  V& value() {
    if (cached_results.empty())
      LOG(FATAL) << "Cache miss on key!" << endl;
    ((Marshal<V>*)(owner_->info().value_marshal))->unmarshal((cached_results.front().second), &value_);
    return value_;
  }

private:
  TableBase* owner_;
  IteratorRequest request_;
  IteratorResponse response_;
  int id_;

  int shard_;
  int index_;
  K key_;
  V value_;
  bool done_;

  // << CRM 2011-01-18 >>
  queue<pair<string, string> > cached_results;
  unsigned int fetch_num_;
};


template<class K, class V>
int TypedGlobalTable<K, V>::get_shard(const K& k) {
  DCHECK(this != NULL);
  DCHECK(this->info().sharder != NULL);

  Sharder<K> *sharder = (Sharder<K>*)(this->info().sharder);
  int shard = (*sharder)(k, this->info().num_shards);
  DCHECK_GE(shard, 0);
  DCHECK_LT(shard, this->num_shards());
  return shard;
}

template<class K, class V>
int TypedGlobalTable<K, V>::shard_for_key_str(const StringPiece& k) {
  return get_shard(unmarshal(static_cast<Marshal<K>* >(this->info().key_marshal), k));
}

template<class K, class V>
V TypedGlobalTable<K, V>::get_local(const K& k) {
  int shard = this->get_shard(k);

  CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

  return partition(shard)->get(k);
}

// Store the given key-value pair in this hash. If 'k' has affinity for a
// remote thread, the application occurs immediately on the local host,
// and the update is queued for transmission to the owner.
template<class K, class V>
void TypedGlobalTable<K, V>::put(const K &k, const V &v) {
  LOG(FATAL) << "Need to implement.";
  int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif
  partition(shard)->put(k, v);

  if (!is_local_shard(shard)) {
    ++pending_writes_;
  }

  if (pending_writes_ > kWriteFlushCount) {
    SendUpdates();
  }

  PERIODIC(0.1, {this->HandlePutRequests();});
}

template<class K, class V>
void TypedGlobalTable<K, V>::update(const K &k, const V &v) {
  int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif

  if (is_local_shard(shard)) {

    // invoke any registered triggers.
    bool doUpdate = true;
    V v2 = v;
    V v1;

    boost::recursive_mutex::scoped_lock sl(trigger_mutex());

    if (partition(shard)->contains(k))
      v1 = partition(shard)->get(k);

    for (int i = 0; i < num_triggers(); ++i) {
      if (reinterpret_cast<Trigger<K, V>*>(trigger(i))->enabled()) {
        doUpdate = doUpdate && reinterpret_cast<Trigger<K, V>*>(trigger(i))->Fire(k, v1, v2);
      }
      //for now, let NACKS disallow chained triggers (?)
      if (!doUpdate) break;
    }

    sl.unlock();

    // Only update if no triggers NACKed
    if (doUpdate) {
      partition(shard)->update(k, v2);
    }

    //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  } else {

    if (num_triggers() == 0) {
      //No triggers, remote accumulation is OK
      partition(shard)->update(k, v);

    } else {
      //Triggers, no remote accumulation allowed
      string sk, sv;
      ((Marshal<K>*)(info_.key_marshal))->marshal((k), &sk);
      ((Marshal<V>*)(info_.value_marshal))->marshal((v), &sv);
      writebufcoder(shard)->WriteEntry(sk,sv);

    }
    ++pending_writes_;
    if (pending_writes_ > kWriteFlushCount) {
      SendUpdates();
    }

    PERIODIC(0.1, {this->HandlePutRequests();});
  }

  //Deal with updates enqueued inside triggers
  clearUpdateQueue();
}

template<class K, class V>
int TypedGlobalTable<K, V>::clearUpdateQueue(void) {

  int i=0;
  deque<KVPair> removed_items;
  {
    boost::recursive_mutex::scoped_lock sl(mutex());
	if (clearingUpdateQueue)
		return 0;
	clearingUpdateQueue = true;		//turn recursion into iteration

  }
  int lastqueuesize = 0;
  do {
    {
      boost::recursive_mutex::scoped_lock sl(mutex());
      //Swap queue with an empty queue so we don't recurse way down
      VLOG(3) << "clearing update queue for table " << this->id() << " of " << update_queue.size() << " items" << endl;

      removed_items.clear();
      update_queue.swap(removed_items);
      lastqueuesize = removed_items.size();
    }

    while(!removed_items.empty()) {
      KVPair thispair(removed_items.front());
      VLOG(3) << "Removed pair (" << (i-removed_items.size()) << " of " << i << ")" << endl;
      update(thispair.first,thispair.second);
      removed_items.pop_front();
      i++;
    }
  } while(lastqueuesize != 0);
  {
    boost::recursive_mutex::scoped_lock sl(mutex());
	clearingUpdateQueue = false;		//turn recursion into iteration
  }
  return i;
}

template<class K, class V>
void TypedGlobalTable<K, V>::enable_retrigger(K k, TriggerID id) {
  boost::mutex::scoped_lock sl(retrigger_mutex());
  if (retrigger_mode == RETRIGGER_MODE_EXPLICIT) {
    //insert or update item in retrigger map
    RetriggerPair rtpair(k,id);

    //Set new one
    retrigger_map.push(rtpair);
  } else { //RETRIGGER_MODE_IMPLICIT
    //Set bit in table instead of in retrigger map
    LOG(FATAL) << "Retrigger mode 1 not yet implemented." << endl;
  }
}

template<class K, class V>
void TypedGlobalTable<K, V>::retrigger_thread(void) {
  while(1) {
    {
 //     boost::mutex::scoped_lock sl(retrigger_mutex());
      if (retrigger_mode == RETRIGGER_MODE_EXPLICIT) {
/*
        typename map<K, RetriggerPair>::iterator it = retrigger_map.begin();
        typename map<K, RetriggerPair>::iterator oldit;
        for(; it != retrigger_map.end();) {
          if (it->second.first == true) {
            V v = get(it->first);
            it->second.first = 
				reinterpret_cast<Trigger<K, V>*>(trigger(it->second.second))->
				Fire(it->first, v, v);
          }
          if (it->second.first == false) {
            oldit = it;
            it++;
            retrigger_map.erase(oldit);
          } else
            it++;
        }
*/
      } else { //RETRIGGER_MODE_IMPLICIT
        LOG(FATAL) << "Retrigger mode 1 not yet implemented!" << endl;
      }
    }
    Sleep(RETRIGGER_SCAN_INTERVAL);
  }
}

template<class K, class V>
void TypedGlobalTable<K, V>::enqueue_update(K k, V v) {
  boost::recursive_mutex::scoped_lock sl(mutex());

  const KVPair thispair(k,v);
  update_queue.push_back(thispair);
  VLOG(2) << "Enqueing table id " << this->id() << " update (" << update_queue.size() << " pending pairs)" << endl;
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V>
V TypedGlobalTable<K, V>::get(const K &k) {
  int shard = this->get_shard(k);

  // If we received a request for this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  // New for triggers: be sure to not recursively apply updates.
  if (tainted(shard)) {
    boost::recursive_mutex::scoped_lock sl(mutex());
    if (applyingupdates == false) {
      applyingupdates = true;
      while (tainted(shard)) {
        this->HandlePutRequests();
        sched_yield();
      }
      applyingupdates = false;
    }
  }

  PERIODIC(0.1, this->HandlePutRequests());

  if (is_local_shard(shard)) {
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->get(k);
  }

  string v_str;
  get_remote(shard,
             marshal(static_cast<Marshal<K>* >(this->info().key_marshal), k),
             &v_str);
  return unmarshal(static_cast<Marshal<V>* >(this->info().value_marshal), v_str);
}

template<class K, class V>
bool TypedGlobalTable<K, V>::contains(const K &k) {
  int shard = this->get_shard(k);

  // If we received a request for this shard; but we haven't received all of the
  // data for it yet. Continue reading from other workers until we do.
  // New for triggers: be sure to not recursively apply updates.
  if (tainted(shard)) {
    boost::recursive_mutex::scoped_lock sl(mutex());
    if (applyingupdates == false) {
      applyingupdates = true;
      while (tainted(shard)) {
        this->HandlePutRequests();
        sched_yield();
      }
      applyingupdates = false;
    }
  }

  if (is_local_shard(shard)) {
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->contains(k);
  }

  string v_str;
  return get_remote(shard, marshal(static_cast<Marshal<K>* >(info_.key_marshal), k), &v_str);
}

template<class K, class V>
void TypedGlobalTable<K, V>::remove(const K &k) {
  LOG(FATAL) << "Not implemented!";
}

template<class K, class V>
LocalTable* TypedGlobalTable<K, V>::create_local(int shard) {
  TableDescriptor *linfo = new TableDescriptor(info());
  linfo->shard = shard;
  LocalTable* t = (LocalTable*)info_.partition_factory->New();
  t->Init(linfo);

  return t;
}

template<class K, class V>
TableIterator* TypedGlobalTable<K, V>::get_iterator(int shard, unsigned int fetch_num) {
  if (this->is_local_shard(shard)) {
    return (TypedTableIterator<K, V>*) partitions_[shard]->get_iterator();
  } else {
    return new RemoteIterator<K, V>(this, shard, fetch_num);
  }
}

}

#endif /* GLOBALTABLE_H_ */
