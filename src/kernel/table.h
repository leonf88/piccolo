#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#define FETCH_NUM 2048

#include "util/common.h"
#include "util/file.h"
#include "worker/worker.pb.h"
#include <boost/thread.hpp>

namespace dsm {

struct TableBase;
struct Table;

class TableData;

// This interface is used by global tables to communicate with the outside
// world and determine the current state of a computation.
struct TableHelper {
  virtual int id() const = 0;
  virtual int epoch() const = 0;
  virtual int peer_for_shard(int table, int shard) const = 0;
  virtual void HandlePutRequest() = 0;
};

struct SharderBase {};
enum AccumulatorType { ACCUMULATOR, TRIGGER };
struct AccumulatorBase {
	AccumulatorType accumtype;
};

struct BlockInfoBase {};


typedef int TriggerID;
struct TriggerBase {
  Table *table;
  TableHelper *helper;
  TriggerID triggerid;

  TriggerBase() {
    enabled_ = true;
  }
  virtual void enable(bool enabled__) { enabled_ = enabled__; }
  virtual bool enabled() { return enabled_; }

private:
  bool enabled_;
};


// Triggers are registered at table initialization time, and
// are executed in response to changes to a table.s
//
// When firing, triggers are activated in the order specified at
// initialization time.
/*
template <class K, class V>
struct Trigger : public TriggerBase {
  virtual bool Fire(const K& k, const V& current, V& update) = 0;
  virtual bool LongFire(const K& k) = 0;
};
*/

//#ifdef SWIGPYTHON
//template <class K, class V> class TriggerDescriptor : public Trigger;
//#endif

#ifndef SWIG

// Each table is associated with a single accumulator.  Accumulators are
// applied whenever an update is supplied for an existing key-value cell.
template <class V>
struct Accumulator : public AccumulatorBase {
  Accumulator() {
    accumtype = ACCUMULATOR;
  }
  virtual void Accumulate(V* a, const V& b) = 0;
};

// <<CRM 2011-06-09
template <class K, class V>
struct Trigger : public AccumulatorBase {
  Trigger() {
    accumtype = TRIGGER;
  }
  virtual void Fire(const K* key, V* value, const V& updateval, bool* doUpdate) = 0;
};
// CRM 2001-06-09>>

template <class K>
struct Sharder : public SharderBase {
  virtual int operator()(const K& k, int shards) = 0;
};

// Commonly-used trigger operators.
template <class K, class V>
struct Triggers {
  struct NullTrigger : public Trigger<K, V> {
    void Fire(const K* key, V* value, const V& updateval, bool* doUpdate) {
      *value = updateval;
      *doUpdate = true;
      return;
    }
  };
  struct ReadOnlyTrigger : public Trigger<K, V> {
    void Fire(const K* key, V* value, const V& updateval, bool* doUpdate) {
      *doUpdate = false;
      return;
    }
  };
};

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
#endif		//#ifdef SWIG / #else

struct TableFactory {
  virtual TableBase* New() = 0;
};

struct TableDescriptor {
public:
  TableDescriptor() { Reset(); }

  TableDescriptor(int id, int shards) {
    Reset();
    table_id = id;
    num_shards = shards; 
  }

  void Reset() {
    table_id = -1;
    num_shards = -1;
    block_size = 500;
    max_stale_time = 0.;
    helper = NULL;
    partition_factory = NULL;
    block_info = NULL;
    key_marshal = value_marshal = NULL;
    accum = NULL;
    sharder = NULL;
    //triggers.clear();
  }

  void swap_accumulator(AccumulatorBase* newaccum) {
    //delete accum;
    accum = newaccum;
    return;
  }

  int table_id;
  int num_shards;

  // For local tables, the shard of the global table they represent.
  int shard;
  int default_shard_size;

//  vector<TriggerBase*> triggers;

  AccumulatorBase *accum;
  SharderBase *sharder;

  MarshalBase *key_marshal;
  MarshalBase *value_marshal;

  // For global tables, factory for constructing new partitions.
  TableFactory *partition_factory;

  // For dense tables, information on block layout and size.
  int block_size;
  BlockInfoBase *block_info;

  // For global tables, the maximum amount of time to cache remote values
  double max_stale_time;

  // For global tables, reference to the local worker.  Used for passing
  // off remote access requests.
  TableHelper *helper;
};

class TableIterator;

struct Table {
  virtual const TableDescriptor& info() const = 0;
  virtual TableDescriptor& mutable_info() = 0;
  virtual int id() const = 0;
  virtual int num_shards() const = 0;
};

struct UntypedTable {
  virtual bool contains_str(const StringPiece& k) = 0;
  virtual string get_str(const StringPiece &k) = 0;
  virtual void update_str(const StringPiece &k, const StringPiece &v) = 0;
};

struct TableIterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;
};

// Methods common to both global and local table views.
class TableBase : public Table {
public:
  typedef TableIterator Iterator;
  virtual void Init(const TableDescriptor * info) {
	info_ = *info;
    CHECK(info_.key_marshal != NULL);
    CHECK(info_.value_marshal != NULL);
  }

  const TableDescriptor & info() const { return info_; }
  TableDescriptor& mutable_info() { return info_; }
  int id() const { return info().table_id; }
  int num_shards() const { return info().num_shards; }

  TableHelper *helper() { return info().helper; }
  int helper_id() { return helper()->id(); }

  //int num_triggers() { return info_.triggers.size(); }
  //TriggerBase *trigger(int idx) { return info_.triggers[idx]; }

/*
  TriggerID register_trigger(TriggerBase *t) {
    if (helper()) {
      t->helper = helper();
    }
    t->table = this;
	t->triggerid = info_.triggers.size();

    info_.triggers.push_back(t);
    return t->triggerid;
  }
*/

  void set_helper(TableHelper *w) {
/*
    for (int i = 0; i < info_.triggers.size(); ++i) {
      trigger(i)->helper = w;
    }
*/

    info_.helper = w;
  }

//protected:
  TableDescriptor info_;
};

// Key/value typed interface.
template <class K, class V>
class TypedTable : virtual public UntypedTable {
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

template <class K, class V> struct TypedTableIterator;
template <class K, class V> struct Trigger;
template <class K, class V> struct PythonTrigger;

struct DecodeIteratorBase {
};

// Added for the sake of triggering on remote updates/puts <CRM>
template <typename K, typename V>
struct DecodeIterator : public TypedTableIterator<K, V>, public DecodeIteratorBase {

  Marshal<K>* kmarshal() { return NULL; }
  Marshal<V>* vmarshal() { return NULL; }

  DecodeIterator() {
    clear();
    rewind();
  }
  void append(K k, V v) {
    kvpair thispair(k, v);
    decodedeque.push_back(thispair);
  }
  void clear() {
    decodedeque.clear();
  }
  void rewind() {
    intit = decodedeque.begin();
  }
  bool done() {
    return intit == decodedeque.end();
  }
  void Next() {
    intit++;
  }
  const K& key() {
    static K k2;
    if (intit != decodedeque.end()) {
      k2 = intit->first;
    }
    return k2;
  }
  V& value() {
    static V v2;
    if (intit != decodedeque.end()) {
      v2 = intit->second;
    }
    return v2;
  }
    
private:
  typedef std::pair<K, V> kvpair;
  std::vector<kvpair> decodedeque;
  typename std::vector<kvpair>::iterator intit;
  
};

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
  virtual void DecodeUpdates(TableCoder *in, DecodeIteratorBase *it) = 0;
  virtual void Serialize(TableCoder* out) = 0;
};
}

#endif
