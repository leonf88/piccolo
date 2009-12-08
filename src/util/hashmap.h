/*
 * hashmap.h
 *
 * Simple generic hashmap using quadratic probing.
 *
 */

#ifndef HASHMAP_H_
#define HASHMAP_H_

#include "util/common.h"

namespace upc {
template <class K, class V>
class HashMap {
private:
  struct Bucket {
    K key;
    V value;
  };

  static const double kLoadFactor = 0.5;

public:
  HashMap(int size);

  V& operator[](const K& k);
  V& get(const K& k);
  bool contains(const K& k);
  void put(const K& k, const V& v);

  void rehash(int size);

  bool empty() { return size() == 0; }
  int size() { return entries_; }

  void remove(const K& k) {}

  void clear() {
    for (int i = 0; i < buckets_.size(); ++i) {
      filled_[i] = false;
    }

    entries_ = 0;
  }

  struct iterator {
    iterator(const vector<Bucket>& b, const vector<uint8_t>& f) : pos(-1), b_(b), f_(f) {
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
    const V& value() { return b_[pos].value; }

    int pos;
    const vector<Bucket>& b_;
    const vector<uint8_t>& f_;
  };

  iterator begin() { return iterator(buckets_, filled_); }
  const iterator& end() { return *end_; }

private:
  int hash(const K& k) {
    return k % size_;
  }

  Bucket* bucket_for_key(const K& k) { 
    int bucket = hash(k);
    int b = bucket;
    do {
      if (!filled_[b]) { return NULL; }
      if (buckets_[b].key == k) { return &buckets_[b]; }
      b = (b == size_ - 1) ? 0 : b + 1;
    } while(b != bucket);

    return NULL;
  }

  vector<Bucket> buckets_;
  vector<uint8_t> filled_;

  std::tr1::hash<K> hasher_;
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

  put(k, V());
  return get(k);
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
void HashMap<K, V>::put(const K& k, const V& v) {
  int bucket = hash(k);
  int b = bucket;
  do {
    if (!filled_[b] || buckets_[b].key == k) {
      break;
    }
    b = (b == size_ - 1) ? 0 : b + 1;
  } while(b != bucket);

  if (!filled_[b]) {
    filled_[b] = true;
    buckets_[b].key = k;
    buckets_[b].value = v;
    ++entries_;

    if (entries_ > size_ * kLoadFactor) {
      rehash(size_ * 2);
    }
  } else {
    buckets_[b].value = v;
  }
}

}
#endif /* HASHMAP_H_ */
