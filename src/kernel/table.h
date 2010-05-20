#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

#include "util/common.h"
#include "util/hashmap.h"
#include "util/file.h"
#include "worker/worker.pb.h"
#include <boost/thread.hpp>

namespace dsm {

class HashPut;

template <class V>
struct Accumulator {
  virtual void operator()(V* a, const V& b) = 0;
};

template <class K>
struct Sharder {
  virtual int operator()(const K& k, int shards) = 0;
};

template <class T>
struct Marshal {
  virtual void marshal(const T& t, string* out) {
    GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
    out->assign(reinterpret_cast<const char*>(&t), sizeof(t));
  }

  virtual void unmarshal(const StringPiece& s, T *t) {
    GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
    *t = *reinterpret_cast<const T*>(s.data);
  }
};

template <>
struct Marshal<string> {
  void marshal(const string& t, string *out) { *out = t; }
  void unmarshal(const StringPiece& s, string *t) { t->assign(s.data, s.len); }
};

template <>
struct Marshal<google::protobuf::Message> {
  void marshal(const google::protobuf::Message& t, string *out) { t.SerializePartialToString(out); }
  void unmarshal(const StringPiece& s, google::protobuf::Message* t) { t->ParseFromArray(s.data, s.len); }
};


#ifndef SWIG
// Commonly used accumulation operators.
template <class V>
struct Accumulators {
  struct Min : public Accumulator<V> {
    void operator()(V* a, const V& b) { *a = std::min(*a, b); }
  };

  struct Max : public Accumulator<V> {
    void operator()(V* a, const V& b) { *a = std::max(*a, b); }
  };

  struct Sum : public Accumulator<V> {
    void operator()(V* a, const V& b) { *a = *a + b; }
  };

  struct Replace : public Accumulator<V> {
    void operator()(V* a, const V& b) { *a = b; }
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

class Worker;

struct TableDescriptor {
public:
  int table_id;
  int num_shards;

  // For local tables, the shard of the global table they represent.
  int shard;
  int default_shard_size;

  // We use void* to pass around the various accumulation and sharding
  // objects; they are cast to the appropriate type at the time of use.
  void *accum;
  void *sharder;
  void *key_marshal;
  void *value_marshal;
};

struct Table_Iterator {
  virtual void key_str(string *out) = 0;
  virtual void value_str(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;
};

template <class K, class V>
struct TypedIterator : public Table_Iterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;
};

// Methods common to both global table views and local shards
class TableView {
public:
  typedef Table_Iterator Iterator;
  TableView(const TableDescriptor& info) : info_(info) {}

  const TableDescriptor& info() const { return info_; }
  void set_info(const TableDescriptor& t) { info_ = t; }

  int id() const { return info().table_id; }
  int shard() const { return info().shard; }
  int num_shards() const { return info().num_shards; }
protected:
  TableDescriptor info_;
};

class Checkpointable {
public:
  // Checkpoint and restoration.
  virtual void start_checkpoint(const string& f) = 0;
  virtual void write_delta(const HashPut& put) = 0;
  virtual void finish_checkpoint() = 0;
  virtual void restore(const string& f) = 0;
};


// Wrapper to add string methods based on key/value typed methods.
#define WRAPPER_FUNCTION_DECL \
bool contains_str(const StringPiece& k);\
string get_str(const StringPiece &k);\
void put_str(const StringPiece &k, const StringPiece &v);\
void remove_str(const StringPiece &k);\
void update_str(const StringPiece &k, const StringPiece &v);\
string key_to_string(const K& t);\
string value_to_string(const V& t);\
K key_from_string(const StringPiece& t);\
V value_from_string(const StringPiece& t);

}
#endif
