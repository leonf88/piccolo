/*
 * hashmap.h
 *
 * Simple generic hashmap using quadratic probing.
 *
 */

#ifndef HASHMAP_H_
#define HASHMAP_H_

#include "util/common.h"
#include "util/file.h"
#include "util/hash.h"
#include <tr1/type_traits>

namespace dsm {


namespace data {
template <class K>
static int hash(K k) {
  k = (k ^ 61) ^ (k >> 16);
  k = k + (k << 3);
  k = k ^ (k >> 4);
  k = k * 0x27d4eb2d;
  return k ^ (k >> 15);
}

template<>
int hash(string s) {
  return SuperFastHash(s.data(), s.size());
}

}

template <class K, class V>
class HashMap {
public:
  struct iterator;
  typedef void (*AccumFunction)(V* v1, const V& v2);
  typedef void (*KMarshal)(const K& t, string *out);
  typedef void (*VMarshal)(const V& t, string *out);

  KMarshal key_marshaller;
  VMarshal value_marshaller;
private:
  struct Bucket {
    bool in_use;

    K key;
    V value;
  };

  static const double kLoadFactor = 0.7;

  int bucket_idx(K k) {
    return dsm::data::hash<K>(k) & (size_ - 1);
  }

  Bucket* bucket_for_key(const K& k) {
    int b = bucket_idx(k);

    while(1) {
      if (buckets_[b].in_use) {
        if (buckets_[b].key == k) {
          return &buckets_[b];
        }
      } else {
        return NULL;
      }

      b = (b + 1) & (size_ - 1);
    }

    return NULL;
  }

  vector<Bucket> buckets_;

  int entries_;
  int size_;
  iterator *end_;

public:
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

  void rehash(int size);

  bool empty() { return size() == 0; }
  int size() { return entries_; }

  void remove(const K& k) {}

  void clear() {
    for (int i = 0; i < buckets_.size(); ++i) {
      buckets_[i].in_use = 0;
    }

    entries_ = 0;
  }

  struct iterator {
    iterator(vector<Bucket>& b) : pos(-1), b_(b) { ++(*this); }

     bool operator==(const iterator &o) { return o.pos == pos; }
     bool operator!=(const iterator &o) { return o.pos != pos; }

     iterator& operator++() {
       do { ++pos; } while (pos < b_.size() && !b_[pos].in_use);
       return *this;
     }

     const K& key() { return b_[pos].key; }
     V& value() { return b_[pos].value; }

     int pos;
     vector<Bucket>& b_;
  };

  iterator begin() { return iterator(buckets_); }
  const iterator& end() { return *end_; }

  void checkpoint(const string& file);
  void restore(const string& file);
};

template <class K, class V>
HashMap<K, V>::HashMap(int size) : buckets_(0), entries_(0), size_(0) {
  clear();

  end_ = new iterator(buckets_);
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
void HashMap<K, V>::rehash(int size) {
  if (size == 1 << log2(size)) {
    size = 1 << log2(size);
  } else {
    size = 1 << (log2(size) + 1);
  }

  vector<Bucket> old_buckets = buckets_;

  buckets_.resize(size);
  size_ = size;
  clear();

  for (int i = 0; i < old_buckets.size(); ++i) {
    if (old_buckets[i].in_use) { put(old_buckets[i].key, old_buckets[i].value); }
  }

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
  Bucket *b = bucket_for_key(k);
  if (b) {
    f(&b->value, v);
  } else {
    put(k, v);
  }
}

template <class K, class V>
bool HashMap<K, V>::contains(const K& k) {
  return bucket_for_key(k) != NULL;
}

template <class K, class V>
V& HashMap<K, V>::get(const K& k) {
  Bucket *b = bucket_for_key(k);
  if (!b) {
    LOG(FATAL) << "No entry for key.";
  }

  return b->value;
}

template <class K, class V>
V& HashMap<K, V>::put(const K& k, const V& v) {
  int start = bucket_idx(k);
  int b = start;

  do {
    if (!buckets_[b].in_use || buckets_[b].key == k) {
      break;
    }
    b = (b + 1) % size_;
  } while(b != start);

  if (!buckets_[b].in_use) {
    if (entries_ > size_ * kLoadFactor) {
      rehash((int)size_ * 3);
      put(k, v);
    } else {
      buckets_[b].in_use = 1;
      buckets_[b].key = k;
      buckets_[b].value = v;
      ++entries_;
    }
  } else {
    buckets_[b].value = v;
  }

  return buckets_[b].value;
}

template <class K, class V>
void HashMap<K, V>::checkpoint(const string& file) {
  if (std::tr1::is_pod<K>::value && std::tr1::is_pod<V>::value) {
    LocalFile lf(file, "w");
    Encoder e(&lf);
    e.write<uint64_t>(buckets_.size());
    e.write_bytes((char*)&buckets_[0], buckets_.size() * sizeof(Bucket));
  } else {
    RecordFile rf(file, "w");
    Pair p;
    for (iterator i = begin(); i != end(); ++i) {
      key_marshaller(i.key(), p.mutable_key());
      value_marshaller(i.value(), p.mutable_value());
      rf.write(p);
    }
  }
}

template <class K, class V>
void HashMap<K, V>::restore(const string& file) {
  if (std::tr1::is_pod<K>::value && std::tr1::is_pod<V>::value) {
    LocalFile lf(file, "r");
    Decoder d(&lf);
    uint64_t size;
    d.read<uint64_t>(&size);
    buckets_.resize(size);
    d.read_bytes((char*)&buckets_[0], size * sizeof(Bucket));
  } else {
    Pair p;
    RecordFile rf(file, "r");

    while (rf.read(&p)) {
      put(data::from_string<K>(p.key()), data::from_string<V>(p.value()));
    }
  }
}

}
#endif /* HASHMAP_H_ */
