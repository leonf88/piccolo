#ifndef DENSE_MAP_H_
#define DENSE_MAP_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include <boost/noncopyable.hpp>

namespace dsm {

// Provides fast lookups for dense key-spaces.  Keys are divided into 'blocks' of
// contiguous elements; users can operate on single entries or blocks at a time for
// more efficient access.  Modifying a single entry in a block marks the entire
// block as dirty, triggering a future of the block if non-local.
template<class K, class V>
class DenseTable:
  public LocalTable,
  public TypedTable<K, V>,
  private boost::noncopyable {
public:
  static const int kBlockSize = 512;

  struct Value {
    Value() : entries(0) {}
    Value(int count) : entries(count) {}

    vector<V> entries;
    bool dirty;
  };

  struct Iterator {
    Iterator(DenseTable<K, V> parent, typename unordered_map<K, Value>::iterator it, int idx) :
      parent_(parent), it_(it), idx_(idx) {
    }

    void Next() {
      ++idx_;
      if (idx_ >= parent_.block_size_) {
        ++it_;
        idx_ = 0;
      }
    }

    bool done() { return it_ == parent_.m_.end(); }

    const K& key() { return it_->first + idx_; }
    const V& value() { return it_->second.entries[idx_]; }

    void key_str(string* k) {
      return ((Marshal<K>*)parent_.info_->key_marshal)->marshal(key(), k);
    }

    void value_str(string *v) {
      return ((Marshal<V>*)parent_.info_->value_marshal)->marshal(value(), v);
    }

    DenseTable<K, V> &parent_;
    typename unordered_map<K, Value>::iterator it_;
    int idx_;
  };

  // return the first key in a bucket
  K start_key(const K& k) {
    return k - (k % block_size_);
  }

  // Construct a hashmap with the given initial size; it will be expanded as necessary.
  DenseTable(int size = 1) : m_(size) {
    block_size_ = kBlockSize;
  }

  ~DenseTable() {}

  bool contains(const K& k) {
    return m_.contains(start_key(k));
  }

  V get(const K& k) {
    return get_block(k)[k % block_size_];
  }

  V* get_block(const K& k) {
    Value &vb = m_[start_key(k)];
    if (vb.entries.size() != block_size_) {
      vb.entries.resize(block_size_);
    }

    return &vb.entries[0];
  }

  void put(const K& k, const V& v) {
    V* vb = get_block(k);
    vb[k % block_size_] = v;

    return vb[k % block_size_];
  }


  void remove(const K& k) { LOG(FATAL) << "Not implemented."; }


  bool contains_str(const StringPiece& s) {
    K k;
    ((Marshal<K>*)info_->key_marshal)->unmarshal(s, &k);
    return contains(k);
  }

  string get_str(const StringPiece &s) {
    K k;
    ((Marshal<K>*)info_->key_marshal)->unmarshal(s, &k);
    string out;
    ((Marshal<V>*)info_->value_marshal)->marshal(get(k), &out);
    return out;
  }

  void update_str(const StringPiece& kstr, const StringPiece &vstr) {
    K k; V v;
    ((Marshal<K>*)info_->key_marshal)->unmarshal(kstr, &k);
    ((Marshal<V>*)info_->value_marshal)->unmarshal(vstr, &v);
    update(k, v);
  }


  bool empty() { return size() == 0; }
  int size() { return m_.size() * block_size_; }
  void clear() { m_.clear(); }
  void resize(int64_t s) {}

  void SerializePartial(TableData *out);
  void ApplyUpdates(const TableData& req);

private:
  typename std::tr1::unordered_map<K, Value> m_;
  int block_size_;
};
}
#endif
