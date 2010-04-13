/*
 * hashmap.h
 *
 * Simple generic hashmap using linear probing.
 *
 */

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

}

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
  static const double kLoadFactor = 0.6;

  uint32_t bucket_idx(K k) {
    return dsm::data::hash<K>(k) % size_;
  }

  int bucket_for_key(const K& k) {
    int b = bucket_idx(k);

    while(1) {
      if (in_use_[b]) {
        if (keys_[b] == k) {
          return b;
        }
      } else {
        return -1;
      }

      b = (b + 1) % size_;
    }

    return -1;
  }

  vector<K> keys_;
  vector<V> values_;
  vector<uint8_t> in_use_;

  uint32_t entries_;
  uint32_t size_;
  iterator *end_;
public:
  struct iterator {
    iterator(HashMap<K, V>& parent) : pos(-1), parent_(parent) { ++(*this); }

     bool operator==(const iterator &o) { return o.pos == pos; }
     bool operator!=(const iterator &o) { return o.pos != pos; }

     iterator& operator++() {
       do { ++pos; } while (pos < parent_.keys_.size() && !parent_.in_use_[pos]);
       return *this;
     }

     const K& key() { return parent_.keys_[pos]; }
     V& value() { return parent_.values_[pos]; }

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

  void resize(uint32_t size);

  bool empty() { return size() == 0; }
  int size() { return entries_; }

  void remove(const K& k) {}

  void clear() {
    for (int i = 0; i < size_; ++i) { in_use_[i] = 0; }
    entries_ = 0;
  }

  iterator begin() { return iterator(*this); }
  const iterator& end() { return *end_; }

  void checkpoint(const string& file);
  void restore(const string& file);
};

template <class K, class V>
HashMap<K, V>::HashMap(int size)
  : keys_(0), values_(0), in_use_(0), entries_(0), size_(0) {
  clear();

  end_ = new iterator(*this);
  end_->pos = size_;

  resize(size);
  key_marshaller = &data::marshal<K>;
  value_marshaller = &data::marshal<V>;
}

static int log2(int s) {
  int l = 0;
  while (s >>= 1) { ++l; }
  return l;
}

template <class K, class V>
void HashMap<K, V>::resize(uint32_t size) {
  if (size_ == size)
    return;

  size = max(size_, size);

  vector<K> old_k = keys_;
  vector<V> old_v = values_;
  vector<uint8_t> old_inuse = in_use_;

  int old_entries = entries_;

  keys_.resize(size);
  values_.resize(size);
  in_use_.resize(size);
  size_ = size;
  clear();

  for (int i = 0; i < old_inuse.size(); ++i) {
    if (old_inuse[i]) {
      put(old_k[i], old_v[i]);
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
    f(&values_[b], v);
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

  return values_[b];
}

template <class K, class V>
V& HashMap<K, V>::put(const K& k, const V& v) {
  int start = bucket_idx(k);
  int b = start;

  do {
    if (!in_use_[b] || keys_[b] == k) {
      break;
    }
    b = (b + 1) % size_;
  } while(b != start);

  if (!in_use_[b]) {
    if (entries_ > size_ * kLoadFactor) {
      resize((int)(size_ * 1.5));
      put(k, v);
    } else {
      in_use_[b] = 1;
      keys_[b] = k;
      values_[b] = v;
      ++entries_;
    }
  } else {
    values_[b] = v;
  }

  return values_[b];
}

template <class K, class V>
void HashMap<K, V>::checkpoint(const string& file) {
  Timer t;

  LocalFile f(file, "w");
  Encoder e(&f);
  e.write(size_);
  e.write(entries_);

  for (uint32_t i = 0; i < size_; ++i) {
    if (in_use_[i]) {
      e.write(i);
      e.write_bytes((char*)&keys_[i], sizeof(K));
      e.write_bytes((char*)&values_[i], sizeof(V));
    }
  }

  f.sync();
//  LOG(INFO) << "Flushed " << file << " to disk in: " << t.elapsed();
}

template <class K, class V>
void HashMap<K, V>::restore(const string& file) {
  LocalFile f(file, "r");
  Decoder d(&f);
  d.read(&size_);
  d.read(&entries_);

  keys_.resize(size_);
  values_.resize(size_);
  in_use_.resize(size_);

  for (uint32_t i = 0; i < entries_; ++i) {
    uint32_t idx;
    d.read(&idx);
    d.read_bytes((char*)&keys_[idx], sizeof(K));
    d.read_bytes((char*)&values_[i], sizeof(V));
  }
}

}
#endif /* HASHMAP_H_ */
