#ifndef DENSE_MAP_H_
#define DENSE_MAP_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include <boost/noncopyable.hpp>

#include "kernel/sparse-map.h"

namespace dsm {

// Provides fast lookups for dense key-spaces.  Keys are divided into 'blocks' of
// contiguous elements; users can operate on single entries or blocks at a time for
// more efficient access.  Modifying a single entry in a block marks the entire
// block as dirty, triggering a future of the block if non-local.
template<class K, class V>
class DenseMap: private boost::noncopyable {
public:
  static const int kBlockSize = 512;

  struct Value {
    Value() : entries(0) {}
    Value(int count) : entries(count) {}

    vector<V> entries;
    bool dirty;
  };

  struct STLView {
    K first;
    V second;
  };

  struct iterator {
    iterator(DenseMap<K, V> parent, typename dsm::SparseMap<K, Value>::iterator it,
             int idx) :
      parent_(parent), it_(it), idx_(idx) {
    }

    bool operator==(const iterator &o) {
      return o.it_ == it_ && o.idx_ == idx_;
    }

    bool operator!=(const iterator &o) {
      return !(*this == o);
    }

    iterator& operator++() {
      ++idx_;
      if (idx_ >= parent_.block_size_) {
        ++it_;
        idx_ = 0;
      }
    }

    STLView* operator->() {
      stl_.first = it_->first + idx_;
      stl_.second = it_->second.entries[idx_];
      return &stl_;
    }

    DenseMap<K, V> &parent_;
    typename dsm::SparseMap<K, Value>::iterator it_;
    int idx_;

    STLView stl_;
  };

  // return the first key in a bucket
  K start_key(const K& k) {
    return k - (k % block_size_);
  }

  // Construct a hashmap with the given initial size; it will be expanded as necessary.
  DenseMap(int size = 1) : m_(size) {
    block_size_ = kBlockSize;
  }

  ~DenseMap() {}

  V& operator[](const K& k) {
    return get(k);
  }

  bool contains(const K& k) {
    return m_.contains(start_key(k));
  }

  V& get(const K& k) {
    return get_block(k)[k % block_size_];
  }

  V* get_block(const K& k) {
    Value &vb = m_[start_key(k)];
    if (vb.entries.size() != block_size_) {
      vb.entries.resize(block_size_);
    }

    return &vb.entries[0];
  }

  V& put(const K& k, const V& v) {
    V* vb = get_block(k);
    vb[k % block_size_] = v;

    return vb[k % block_size_];
  }

  bool empty() { return size() == 0; }
  int size() { return m_.size() * block_size_; }
  void clear() { m_.clear(); }

  iterator begin() { return iterator(*this, m_.begin(), 0); }
  const iterator& end() { return *end_; }

  iterator find(const K& k) {
    iterator i = m_.find(start_key(k));
    if (i != m_.end()) {
      return iterator(*this, i, k % block_size_);
    }
    return *end_;
  }

  void erase(iterator pos) { LOG(FATAL) << "Not implemented."; }

  void SerializePartial(TableData *out, Marshal<K> &kmarshal, Marshal<V> &vmarshal);
  void ApplyUpdates(const TableData& req, Marshal<K> &kmarshal, Marshal<V> &vmarshal);

private:
  typename dsm::SparseMap<K, Value> m_;
  iterator *end_;
  int block_size_;
};
}
#endif
