#ifndef DENSE_MAP_H_
#define DENSE_MAP_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include <boost/noncopyable.hpp>

namespace dsm {

template <class K>
struct BlockInfo {
  virtual K block_id(const K& k, int block_size) = 0;
  virtual int block_pos(const K& k, int block_size) = 0;
};

struct IntBlockInfo : public BlockInfo<int> {
  int block_id(const int& k, int block_size) {
    return k - (k % block_size);
  }

  int block_pos(const int& k, int block_size) {
    return k % block_size;
  }
};


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
      if (idx_ >= parent_.info_->block_size) {
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

  BlockInfo<K>& block_info() {
    return *(BlockInfo<K>*)info_->block_info;
  }

  // return the first key in a bucket
  K start_key(const K& k) {
    return block_info().block_id(k, info_->block_size);
  }

  int block_pos(const K& k) {
    return block_info().block_pos(k, info_->block_size);
  }

  // Construct a hashmap with the given initial size; it will be expanded as necessary.
  DenseTable(int size = 1) : m_(size) {
  }

  ~DenseTable() {}

  void Init(const TableDescriptor* td) {
    TableBase::Init(td);
  }

  bool contains(const K& k) {
    return m_.find(start_key(k)) != m_.end();
  }

  // We anticipate a strong locality relationship between successive operations.
  // The last accessed block is cached, and can be returned immediately if the
  // subsequent operation(s) also access the same block.
  V* get_block(const K& k) {
    K start = start_key(k);

    if (start == last_block_start_ && last_block_) {
      return last_block_;
    }

    Bucket &vb = m_[start];
    if (vb.entries.size() != info_->block_size) {
      vb.entries.resize(info_->block_size);
    }

    last_block_ = &vb.entries[0];
    last_block_start_ = start;

    return last_block_;
  }

  V get(const K& k) {
    return get_block(k)[block_pos(k)];
  }

  void update(const K& k, const V& v) {
    V* vb = get_block(k);
    ((Accumulator<V>*)info_->accum)->Accumulate(&vb[block_pos(k)], v);
  }

  void put(const K& k, const V& v) {
    V* vb = get_block(k);
    vb[block_pos(k)] = v;
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

  bool empty() {
    return size() == 0;
  }

  int64_t size() {
    return m_.size() * info_->block_size;
  }

  void clear() {
    m_.clear();
    last_block_ = NULL;
  }

  void resize(int64_t s) {}

  void Serialize(TableData *out) {
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

    out->set_done(true);
  }

  void ApplyUpdates(const TableData& req) {
    K k;
    for (int i = 0; i < req.kv_data_size(); ++i) {
      const Arg &kv = req.kv_data(i);
      ((Marshal<K>*)info_->key_marshal)->unmarshal(kv.key(), &k);

      V* block = get_block(k);
      const int value_size = kv.value().size() / info_->block_size;

      V tmp;
      for (int j = 0; j < info_->block_size; ++j) {
        ((Marshal<V>*)info_->value_marshal)->unmarshal(
                    StringPiece(kv.value().data() + (value_size * j), value_size),
                    &tmp);
        ((Accumulator<V>*)info_->accum)->Accumulate(&block[j], tmp);
      }
    }
  }

private:
  BucketMap m_;
  V* last_block_;
  K last_block_start_;
};
}
#endif
