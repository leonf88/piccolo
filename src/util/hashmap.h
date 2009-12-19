/*
 * hashmap.h
 *
 * Simple generic hashmap using quadratic probing.
 *
 */

#ifndef HASHMAP_H_
#define HASHMAP_H_

#include "util/common.h"
#include "util/hash.h"

namespace upc {

template <class K>
static int simple_hash(K k) {
  k = (k ^ 61) ^ (k >> 16);
  k = k + (k << 3);
  k = k ^ (k >> 4);
  k = k * 0x27d4eb2d;
  return k ^ (k >> 15);
}

template<>
int simple_hash(string s) {
  return SuperFastHash(s.data(), s.size());
}

template <class K, class V>
class HashMap {
private:
  struct Bucket {
    K key;
    V value;
  };

  static const double kLoadFactor = 0.4;

public:
  HashMap(int size);
  ~HashMap() {
    delete end_;
  }

  V& operator[](const K& k);
  V& get(const K& k);
  bool contains(const K& k);
  V& put(const K& k, const V& v);

  void rehash(int size);

  bool empty() { return size() == 0; }
  int size() { return entries_; }

  void remove(const K& k) {}

  void clear() {
    for (int i = 0; i < filled_.size(); ++i) {
      filled_[i] = 0;
    }

    entries_ = 0;
  }

  struct iterator {
    iterator(vector<Bucket>& b, const vector<uint8_t>& f) : pos(-1), b_(b), f_(f) {
      ++(*this);
    }

    bool operator==(const iterator &o) { return o.pos == pos; }

    iterator& operator++() {
      do {
        ++pos;
      } while (pos < b_.size() && !f_[pos]);
      return *this;
    }

    const K& key() { return b_[pos].key; }
    V& value() { return b_[pos].value; }

    int pos;
    vector<Bucket>& b_;
    const vector<uint8_t>& f_;
  };

  iterator begin() {
    return iterator(buckets_, filled_);
  }

  const iterator& end() { return *end_; }

private:
  // The STL default hash for integers gives us terrible performance:
  // it trivially assigns hash(k) = k.
  int hash(K k) {
    return simple_hash<K>(k) % size_;
  }

  Bucket* bucket_for_key(const K& k) {
    static int misses = 0;
    int start = hash(k);
    int b = start;

    //LOG_EVERY_N(INFO, 1000000) << "Misses: " << double(100 * misses) / LOG_OCCURRENCES;

    do {
      if (!filled_[b]) {
        return NULL;
      }

      if (buckets_[b].key == k) {
        return &buckets_[b];
      }

      ++misses;
      b = (b + 1) % size_;
    } while(b != start);

    return NULL;
  }

  vector<Bucket> buckets_;
  vector<uint8_t> filled_;

  int entries_;
  int size_;

  iterator *end_;
};

template <class K, class V>
HashMap<K, V>::HashMap(int size) : buckets_(size), filled_(size), entries_(0), size_(size) {
  clear();

  end_ = new iterator(buckets_, filled_);
  end_->pos = size_;
}

template <class K, class V>
void HashMap<K, V>::rehash(int size) {
//  LOG(INFO) << "Rehashing... " << size << " : " << entries_;
  vector<Bucket> old_buckets = buckets_;
  vector<uint8_t> old_filled = filled_;

  buckets_.resize(size);
  filled_.resize(size);
  size_ = size;
  clear();

  for (int i = 0; i < old_buckets.size(); ++i) {
    if (old_filled[i]) {
      put(old_buckets[i].key, old_buckets[i].value);
    }
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
bool HashMap<K, V>::contains(const K& k) {
  return bucket_for_key(k) != NULL;
}

template <class K, class V>
V& HashMap<K, V>::get(const K& k) {
  Bucket *b = bucket_for_key(k);
  if (!b) {
    LOG(FATAL) << "No entry for key: " << k;
  }

  return b->value;
}

template <class K, class V>
V& HashMap<K, V>::put(const K& k, const V& v) {
  int start = hash(k);
  int b = start;

  do {
    if (!filled_[b] || buckets_[b].key == k) {
      break;
    }
    b = (b + 1) % size_;
  } while(b != start);

  if (!filled_[b]) {
    if (entries_ > size_ * kLoadFactor) {
      rehash(size_ * 3);
    }

    filled_[b] = 1;
    buckets_[b].key = k;
    buckets_[b].value = v;
    ++entries_;
  } else {
    buckets_[b].value = v;
  }

  return buckets_[b].value;
}

}
#endif /* HASHMAP_H_ */
