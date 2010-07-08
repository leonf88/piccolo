#ifndef SPARSE_MAP_H_
#define SPARSE_MAP_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include <boost/noncopyable.hpp>

namespace dsm {

static const double kLoadFactor = 0.8;

template <class K, class V>
class SparseMap : private boost::noncopyable {
public:
  struct STLView {
     K first;
     V second;
     bool in_use;
   };

  struct iterator {
    iterator(SparseMap<K, V>& parent) : pos(-1), parent_(parent) { ++(*this); }
    iterator(SparseMap<K, V>& parent, int p) : pos(p), parent_(parent) {}

    bool operator==(const iterator &o) { return o.pos == pos; }
    bool operator!=(const iterator &o) { return o.pos != pos; }

    iterator& operator++();
    STLView* operator->();

    int pos;
    SparseMap<K, V> &parent_;
  };

  // Construct a SparseMap with the given initial size; it will be expanded as necessary.
  SparseMap(int size=1);
  ~SparseMap() { delete end_; }

  V& operator[](const K& k);
  bool contains(const K& k);

  V& get(const K& k);
  V& put(const K& k, const V& v);

  void rehash(uint32_t size);

  bool empty() { return size() == 0; }
  int size() { return entries_; }

  void clear() {
    for (int i = 0; i < size_; ++i) { buckets_[i].in_use = 0; }
    entries_ = 0;
  }

  iterator begin() { return iterator(*this); }
  const iterator& end() { return *end_; }

  iterator find(const K& k);
  void erase(iterator pos);

  void SerializePartial(TableData *out, Marshal<K> &kmarshal, Marshal<V> &vmarshal);
  void ApplyUpdates(const TableData& req,
                      Marshal<K> &kmarshal, Marshal<V> &vmarshal,
                      Accumulator<V> &accum);

private:
  uint32_t bucket_idx(K k) {
    return hashobj_(k) % size_;
  }

  int bucket_for_key(const K& k) {
    int start = bucket_idx(k);
    int b = start;

    do {
      if (buckets_[b].in_use) {
        if (buckets_[b].k == k) {
          return b;
        }
      } else {
        return -1;
      }

       b = (b + 1) % size_;
    } while (b != start);

    return -1;
  }

#pragma pack(push, 1)
  struct Bucket {
    K k;
    V v;
    bool in_use;
  };
#pragma pack(pop)

  std::vector<Bucket> buckets_;

  uint32_t entries_;
  uint32_t size_;
  iterator *end_;

  std::tr1::hash<K> hashobj_;
};

template <class K, class V>
typename SparseMap<K, V>::iterator SparseMap<K, V>::find(const K& k) {
  int b = bucket_for_key(k);
  if (b == -1) { return *end_; }
  return iterator(*this, b);
}

template <class K, class V>
void SparseMap<K, V>::erase(typename SparseMap<K, V>::iterator pos) {
  pos->in_use = false;
  --entries_;
}

template <class K, class V>
typename SparseMap<K, V>::iterator& SparseMap<K, V>::iterator::operator++() {
  do {
    ++pos;
  } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);
  return *this;
}

template <class K, class V>
typename SparseMap<K, V>::STLView* SparseMap<K, V>::iterator::operator->()  {
  return (STLView*)&parent_.buckets_[pos];
}

template <class K, class V>
SparseMap<K, V>::SparseMap(int size)
  : buckets_(0), entries_(0), size_(0) {
  clear();

  end_ = new iterator(*this);
  end_->pos = size_;

  rehash(size);
}

template <class K, class V>
void SparseMap<K, V>::SerializePartial(TableData *out, Marshal<K> &kmarshal, Marshal<V> &vmarshal) {
  iterator i = begin();
  while (i != end()) {
    Arg *kv = out->add_kv_data();
    kmarshal.marshal(i->first, kv->mutable_key());
    vmarshal.marshal(i->second, kv->mutable_value());
    ++i;
  }

  clear();
}

template <class K, class V>
void SparseMap<K, V>::ApplyUpdates(const TableData& req,
                                      Marshal<K> &kmarshal,
                                      Marshal<V> &vmarshal,
                                      Accumulator<V> &accum) {
  K k;
  V v;
  for (int i = 0; i < req.kv_data_size(); ++i) {
    const Arg& kv = req.kv_data(i);
    kmarshal.unmarshal(kv.key(), &k);
    vmarshal.unmarshal(kv.value(), &v);

    int b = bucket_for_key(k);
    if (b == -1) {
      put(k, v);
    } else {
      accum(&buckets_[b].v, v);
    }
  }
}


static int log2(int s) {
  int l = 0;
  while (s >>= 1) { ++l; }
  return l;
}

template <class K, class V>
void SparseMap<K, V>::rehash(uint32_t size) {
  if (size_ == size)
    return;

  size = std::max(size_, size);

  std::vector<Bucket> old_b = buckets_;

  int old_entries = entries_;

//  LOG(INFO) << "Rehashing... " << entries_ << " : " << size_ << " -> " << size;

  buckets_.resize(size);
  size_ = size;
  clear();

  for (int i = 0; i < old_b.size(); ++i) {
    if (old_b[i].in_use) {
      put(old_b[i].k, old_b[i].v);
    }
  }

  CHECK_EQ(old_entries, entries_);

  end_->pos = size_;
}

template <class K, class V>
V& SparseMap<K, V>::operator[](const K& k) {
  if (contains(k)) {
    return get(k);
  }

  return put(k, V());
}

template <class K, class V>
bool SparseMap<K, V>::contains(const K& k) {
  return bucket_for_key(k) != -1;
}

template <class K, class V>
V& SparseMap<K, V>::get(const K& k) {
  int b = bucket_for_key(k);
  CHECK_NE(b, -1) << "No entry for requested key: " << k;

  return buckets_[b].v;
}

template <class K, class V>
V& SparseMap<K, V>::put(const K& k, const V& v) {
  int start = bucket_idx(k);
  int b = start;
  bool found = false;

  do {
    if (!buckets_[b].in_use) {
      break;
    }

    if (buckets_[b].k == k) {
      found = true;
      break;
    }

    b = (b + 1) % size_;
  } while(b != start);

  // Inserting a new entry:
  if (!found) {
    if (entries_ > size_ * kLoadFactor) {
      rehash((int)(1 + size_ / kLoadFactor));
      put(k, v);
    } else {
      buckets_[b].in_use = 1;
      buckets_[b].k = k;
      buckets_[b].v = v;
      ++entries_;
    }
  } else {
    // Replacing an existing entry
    buckets_[b].v = v;
  }

  return buckets_[b].v;
}
}
#endif /* SPARSE_MAP_H_ */
