#ifndef HASHMAP_H_
#define HASHMAP_H_

#include "util/common.h"
#include "util/file.h"
#include "util/hash.h"
#include <boost/noncopyable.hpp>
#include <tr1/type_traits>

namespace dsm {

namespace data {
template <class K>
static uint32_t hash(K k) {
  k = (k ^ 61) ^ (k >> 16);
  k = k + (k << 3);
  k = k ^ (k >> 4);
  k = k * 0x27d4eb2d;
  return k ^ (k >> 15);
}

template<>
uint32_t hash(string s) {
  return SuperFastHash(s.data(), s.size());
}

} }


namespace dsm {
template <class K, class V>
class HashMap : private boost::noncopyable {
public:
  struct iterator;
  typedef void (*AccumFunction)(V* v1, const V& v2);
  typedef void (*KMarshal)(const K& t, string *out);
  typedef void (*VMarshal)(const V& t, string *out);

  KMarshal key_marshaller;
  VMarshal value_marshaller;
private:
  static const double kLoadFactor = 0.8;

  uint32_t bucket_idx(K k) {
    return dsm::data::hash<K>(k) % size_;
  }

  int bucket_for_key(const K& k) {
    int start = bucket_idx(k);
    int b = start;
    int i = 1;

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

  struct Bucket {
    K k;
    V v;
    bool in_use;
  };

  vector<Bucket> buckets_;

  uint32_t entries_;
  uint32_t size_;
  iterator *end_;
public:
  struct iterator {
    iterator(HashMap<K, V>& parent) : pos(-1), parent_(parent) { ++(*this); }
    iterator(HashMap<K, V>& parent, int p) : pos(p), parent_(parent) {}

     bool operator==(const iterator &o) { return o.pos == pos; }
     bool operator!=(const iterator &o) { return o.pos != pos; }

     iterator& operator++() {
       do { ++pos; } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);
       return *this;
     }

     struct BucketPair {
       K first;
       V second;
     };

     BucketPair* operator->() {
       return (BucketPair*)&parent_.buckets_[pos];
     }

     int pos;
     HashMap<K, V> &parent_;
  };

  // Construct a hashmap with the given initial size; it will be expanded as necessary.
  HashMap(int size);
  ~HashMap() {
    delete end_;
  }

  V& operator[](const K& k);
  bool contains(const K& k);

  V& get(const K& k);
  V& put(const K& k, const V& v);

  void accumulate(const K& k, const V& v, AccumFunction f);

  void rehash(uint32_t size);

  bool empty() { return size() == 0; }
  int size() { return entries_; }

  void remove(const K& k) {}

  void clear() {
    for (int i = 0; i < size_; ++i) { buckets_[i].in_use = 0; }
    entries_ = 0;
  }

  iterator find(const K& k) {
    int b = bucket_for_key(k);
    if (b == -1) { return *end_; }
    return iterator(*this, b);
  }


  iterator begin() { return iterator(*this); }
  const iterator& end() { return *end_; }

  void checkpoint(const string& file);
  void restore(const string& file);
};

template <class K, class V>
HashMap<K, V>::HashMap(int size)
  : buckets_(0), entries_(0), size_(0) {
  clear();

  end_ = new iterator(*this);
  end_->pos = size_;

  rehash(size);
  key_marshaller = &data::marshal<K>;
  value_marshaller = &data::marshal<V>;
}

static int log2(int s) {
  int l = 0;
  while (s >>= 1) { ++l; }
  return l;
}

template <class K, class V>
void HashMap<K, V>::rehash(uint32_t size) {
  if (size_ == size)
    return;

  size = max(size_, size);

  vector<Bucket> old_b = buckets_;

  int old_entries = entries_;

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
V& HashMap<K, V>::operator[](const K& k) {
  if (contains(k)) {
    return get(k);
  }

  return put(k, V());
}

template <class K, class V>
void HashMap<K, V>::accumulate(const K& k, const V& v, AccumFunction f) {
  int b = bucket_for_key(k);
  if (b != -1) {
    f(&buckets_[b].v, v);
  } else {
    put(k, v);
  }
}

template <class K, class V>
bool HashMap<K, V>::contains(const K& k) {
  return bucket_for_key(k) != -1;
}

template <class K, class V>
V& HashMap<K, V>::get(const K& k) {
  int b = bucket_for_key(k);
  if (b == -1) {
    LOG(FATAL) << "No entry for key.";
  }

  return buckets_[b].v;
}

template <class K, class V>
V& HashMap<K, V>::put(const K& k, const V& v) {
  int start = bucket_idx(k);
  int b = start;

  int i = 1;
  do {
    if (!buckets_[b].in_use || buckets_[b].k == k) {
      break;
    }

//    b = (b + i * i + i) % size_;
//    ++i;
    b = (b + 1) % size_;
  } while(b != start);

  if (!buckets_[b].in_use) {
    if (entries_ > size_ * kLoadFactor) {
      rehash((int)(size_ / kLoadFactor));
      put(k, v);
    } else {
      buckets_[b].in_use = 1;
      buckets_[b].k = k;
      buckets_[b].v = v;
      ++entries_;
    }
  } else {
    buckets_[b].v = v;
  }

  return buckets_[b].v;
}

template <class K, class V>
void HashMap<K, V>::checkpoint(const string& file) {
  Timer t;

  LZOFile f(file, "w");
  Encoder e(&f);
  e.write(size_);
  e.write(entries_);

  for (uint32_t i = 0; i < size_; ++i) {
    if (buckets_[i].in_use) {
      e.write(i);
      e.write_bytes((char*)&buckets_[i], sizeof(Bucket));
    }
  }

  f.sync();
//  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
}

template <class K, class V>
void HashMap<K, V>::restore(const string& file) {
  LZOFile f(file, "r");
  Decoder d(&f);
  d.read(&size_);
  d.read(&entries_);

  buckets_.resize(size_);

  for (uint32_t i = 0; i < entries_; ++i) {
    uint32_t idx;
    d.read(&idx);
    buckets_[idx].in_use = 1;
    d.read_bytes((char*)&buckets_[idx], sizeof(Bucket));
  }
}

}
#endif /* HASHMAP_H_ */
