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

  struct Bucket {
    Bucket() : entries(0) {}
    Bucket(int count) : entries(count) {}

    vector<V> entries;
    bool dirty;
  };

  typedef typename std::tr1::unordered_map<K, Bucket> BucketMap;

  struct Iterator : public TypedTableIterator<K, V> {
    Iterator(DenseTable<K, V> &parent) : parent_(parent), it_(parent_.m_.begin()), idx_(0) { }

    void Next() {
      ++idx_;
      if (idx_ >= parent_.block_size_) {
        ++it_;
        idx_ = 0;
      }
    }

    bool done() { return it_ == parent_.m_.end(); }

    const K& key() { k_ = it_->first + idx_; return k_; }
    V& value() { return it_->second.entries[idx_]; }

    void key_str(string* k) {
      return ((Marshal<K>*)parent_.info_->key_marshal)->marshal(key(), k);
    }

    void value_str(string *v) {
      return ((Marshal<V>*)parent_.info_->value_marshal)->marshal(value(), v);
    }

    DenseTable<K, V> &parent_;
    K k_;
    typename unordered_map<K, Bucket>::iterator it_;
    int idx_;
  };

  struct Factory : public TableFactory {
    TableBase* New() { return new DenseTable<K, V>(); }
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

  void Init(const TableDescriptor* td) {
    TableBase::Init(td);
  }

  bool contains(const K& k) {
    return m_.find(start_key(k)) != m_.end();
  }

  V get(const K& k) {
    return get_block(k)[k % block_size_];
  }

  V* get_block(const K& k) {
    Bucket &vb = m_[start_key(k)];
    if (vb.entries.size() != block_size_) {
      vb.entries.resize(block_size_);
    }

    return &vb.entries[0];
  }

  void update(const K& k, const V& v) {
    V* vb = get_block(k);
    ((Accumulator<V>*)info_->accum)->Accumulate(&vb[k % block_size_], v);
  }

  void put(const K& k, const V& v) {
    V* vb = get_block(k);
    vb[k % block_size_] = v;
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

  TableIterator* get_iterator() { return new Iterator(*this); }

  bool empty() { return size() == 0; }
  int64_t size() { return m_.size() * block_size_; }
  void clear() { m_.clear(); }
  void resize(int64_t s) {}

  void SerializePartial(TableData *out) {
    for (typename BucketMap::iterator i = m_.begin(); i != m_.end(); ++i) {
      Bucket &b = i->second;

      // For the purposes of serialization, all values in a bucket are assumed to take on the
      // same size.
      Arg *kv = out->add_kv_data();
      ((Marshal<K>*)info_->key_marshal)->marshal(i->first, kv->mutable_key());

      string tmp;
      for (int j = 0; j < b.entries.size(); ++j) {
        ((Marshal<V>*)info_->value_marshal)->marshal(b.entries[j], &tmp);
        kv->mutable_value()->append(tmp);
      }
    }
  }

  void ApplyUpdates(const TableData& req) {
    K k;
    for (int i = 0; i < req.kv_data_size(); ++i) {
      const Arg &kv = req.kv_data(i);
      ((Marshal<K>*)info_->key_marshal)->unmarshal(kv.key(), &k);

      V* block = get_block(k);
      const int value_size = kv.value().size() / kBlockSize;
      for (int j = 0; j < kBlockSize; ++j) {
        ((Marshal<V>*)info_->value_marshal)->unmarshal(
            StringPiece(kv.value().data() + (value_size * j), value_size),
            &block[j]);
      }
    }
  }

private:
  BucketMap m_;
  int block_size_;
};
}
#endif
