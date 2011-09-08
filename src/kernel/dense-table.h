#ifndef DENSE_MAP_H_
#define DENSE_MAP_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include "kernel/table.h"
#include <boost/noncopyable.hpp>

namespace dsm {

template <class K>
struct BlockInfo : BlockInfoBase {
  // Returns the key representing the first element in the block
  // containing 'k'.
  virtual K start(const K& k, int block_size) = 0;

  // Returns the index offset of 'k' within it's block.
  virtual int offset(const K& k, int block_size) = 0;
};

struct IntBlockInfo : public BlockInfo<int> {
  int start(const int& k, int block_size) {
    return k - (k % block_size);
  }

  int offset(const int& k, int block_size) {
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
  typedef DecodeIterator<K, V> UpdateDecoder;

  struct Iterator : public TypedTableIterator<K, V> {
    Iterator(DenseTable<K, V> &parent) : parent_(parent), it_(parent_.m_.begin()), idx_(0) {}

    Marshal<K> *kmarshal() { return ((Marshal<K>*)parent_.kmarshal()); }
    Marshal<V> *vmarshal() { return ((Marshal<V>*)parent_.vmarshal()); }

    void Next() {
      ++idx_;
      if (idx_ >= parent_.info_.block_size) {
        ++it_;
        idx_ = 0;
      }
    }

    bool done() { return it_ == parent_.m_.end(); }

    const K& key() { k_ = it_->first + idx_; return k_; }
    V& value() { return it_->second.entries[idx_]; }

    DenseTable<K, V> &parent_;
    K k_;
    typename unordered_map<K, Bucket>::iterator it_;
    int idx_;
  };

  struct Factory : public TableFactory {
    TableBase* New() { return new DenseTable<K, V>(); }
  };

  BlockInfo<K>& block_info() {
    return *(BlockInfo<K>*)info_.block_info;
  }

  // return the first key in a bucket
  K start_key(const K& k) {
    return block_info().start(k, info_.block_size);
  }

  int block_pos(const K& k) {
    return block_info().offset(k, info_.block_size);
  }

  // Construct a hashmap with the given initial size; it will be expanded as necessary.
  DenseTable(int size = 1) : m_(size) {
    last_block_ = NULL;
  }

  ~DenseTable() {}

  void Init(const TableDescriptor * td) {
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

    if (last_block_ && start == last_block_start_) {
      return last_block_;
    }

    Bucket &vb = m_[start];
    if (vb.entries.size() != info_.block_size) {
      vb.entries.resize(info_.block_size);
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

    if (info_.accum->accumtype == ACCUMULATOR) {
      ((Accumulator<V>*)info_.accum)->Accumulate(&vb[block_pos(k)], v);
    } else if (info_.accum->accumtype == TRIGGER) {
      V v2 = vb[block_pos(k)];
      bool doUpdate = false;
      ((Trigger<K,V>*)info_.accum)->Fire(&k,&v2,v,&doUpdate);
      if (doUpdate)
        vb[block_pos(k)] = v2;
    } else {
      LOG(FATAL) << "update() called with neither TRIGGER nor ACCUMULATOR";
    }

//    K k2(k);
//    ((Accumulator<V>*)info_.accum)->Accumulate(&k2, v);
  }

  void put(const K& k, const V& v) {
    V* vb = get_block(k);
    vb[block_pos(k)] = v;
  }

  void remove(const K& k) { LOG(FATAL) << "Not implemented."; }

  TableIterator* get_iterator() { return new Iterator(*this); }

  bool empty() {
    return size() == 0;
  }

  int64_t size() {
    return m_.size() * info_.block_size;
  }

  void clear() {
    m_.clear();
    last_block_ = NULL;
  }

  void resize(int64_t s) {}

  void Serialize(TableCoder* out) {
    string k, v;
    for (typename BucketMap::iterator i = m_.begin(); i != m_.end(); ++i) {
      v.clear();

      // For the purposes of serialization, all values in a bucket are assumed
      // to be the same number of bytes.
      ((Marshal<K>*)info_.key_marshal)->marshal(i->first, &k);

      string tmp;
      Bucket &b = i->second;
      for (int j = 0; j < b.entries.size(); ++j) {
        ((Marshal<V>*)info_.value_marshal)->marshal(b.entries[j], &tmp);
        v += tmp;
      }

      out->WriteEntry(k, v);
    }
  }

void DecodeUpdates(TableCoder *in, DecodeIteratorBase *itbase) {
    UpdateDecoder *it = static_cast<UpdateDecoder*>(itbase);
    K k;
    string kt, vt;

    it->clear();
    while (in->ReadEntry(&kt, &vt)) {
      ((Marshal<K>*)info_.key_marshal)->unmarshal(kt, &k);
      const int value_size = vt.size() / info_.block_size;

      V tmp;
      for (int j = 0; j < info_.block_size; ++j) {
        ((Marshal<V>*)info_.value_marshal)->unmarshal(
            StringPiece(vt.data() + (value_size * j), value_size),
            &tmp);
        it->append(k + j, tmp);
      }
    }
    it->rewind();
    return;
  }

  Marshal<K>* kmarshal() { return ((Marshal<K>*)info_.key_marshal); }
  Marshal<V>* vmarshal() { return ((Marshal<V>*)info_.value_marshal); }

private:
  BucketMap m_;
  V* last_block_;
  K last_block_start_;
};
}
#endif
