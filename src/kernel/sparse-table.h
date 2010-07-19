#ifndef SPARSE_MAP_H_
#define SPARSE_MAP_H_

#include "util/common.h"
#include "worker/worker.pb.h"
#include "kernel/table.h"
#include "kernel/local-table.h"
#include <boost/noncopyable.hpp>

namespace dsm {

static const double kLoadFactor = 0.8;

template <class K, class V>
class SparseTable :
  public LocalTable,
  public TypedTable<K, V>,
  private boost::noncopyable {
private:
#pragma pack(push, 1)
  struct Bucket {
    K k;
    V v;
    bool in_use;
  };
#pragma pack(pop)

public:
  struct Iterator : public TypedTableIterator<K, V> {
    Iterator(SparseTable<K, V>& parent) : pos(-1), parent_(parent) { Next(); }

    void Next() {
      do {
        ++pos;
      } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);
    }

    bool done() {
      return pos == parent_.size_;
    }

    const K& key() { return parent_.buckets_[pos].k; }
    V& value() { return parent_.buckets_[pos].v; }

    void key_str(string* k) {
      return ((Marshal<K>*)parent_.info_->key_marshal)->marshal(key(), k);
    }

    void value_str(string *v) {
      return ((Marshal<V>*)parent_.info_->value_marshal)->marshal(value(), v);
    }

    int pos;
    SparseTable<K, V> &parent_;
  };

  struct Factory : public TableFactory {
    TableBase* New() { return new SparseTable<K, V>(); }
  };

  // Construct a SparseTable with the given initial size; it will be expanded as necessary.
  SparseTable(int size=1);
  ~SparseTable() {}

  void Init(const TableDescriptor* td) {
    TableBase::Init(td);
  }

  V get(const K& k);
  bool contains(const K& k);
  void put(const K& k, const V& v);
  void update(const K& k, const V& v);
  void remove(const K& k) {
    LOG(FATAL) << "Not implemented.";
  }

  void resize(int64_t size);

  bool empty() { return size() == 0; }
  int64_t size() { return entries_; }

  void clear() {
    for (int i = 0; i < size_; ++i) { buckets_[i].in_use = 0; }
    entries_ = 0;
  }

  TableIterator *get_iterator() {
      return new Iterator(*this);
  }

  void Serialize(TableData *out);
  void ApplyUpdates(const TableData& req);

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

  std::vector<Bucket> buckets_;

  int64_t entries_;
  int64_t size_;

  std::tr1::hash<K> hashobj_;
};

template <class K, class V>
SparseTable<K, V>::SparseTable(int size)
  : buckets_(0), entries_(0), size_(0) {
  clear();

  resize(size);
}

template <class K, class V>
void SparseTable<K, V>::Serialize(TableData *out) {
  Iterator *i = (Iterator*)get_iterator();
  while (!i->done()) {
    Arg *kv = out->add_kv_data();
    ((Marshal<K>*)info_->key_marshal)->marshal(i->key(), kv->mutable_key());
    ((Marshal<V>*)info_->value_marshal)->marshal(i->value(), kv->mutable_value());
    i->Next();
  }

  out->set_done(true);
  delete i;
}

template <class K, class V>
void SparseTable<K, V>::ApplyUpdates(const TableData& req) {
  K k;
  V v;
  for (int i = 0; i < req.kv_data_size(); ++i) {
    const Arg& kv = req.kv_data(i);
    ((Marshal<K>*)info_->key_marshal)->unmarshal(kv.key(), &k);
    ((Marshal<V>*)info_->value_marshal)->unmarshal(kv.value(), &v);

    update(k, v);
  }
}


static int log2(int s) {
  int l = 0;
  while (s >>= 1) { ++l; }
  return l;
}

template <class K, class V>
void SparseTable<K, V>::resize(int64_t size) {
  if (size_ == size)
    return;

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
}

template <class K, class V>
bool SparseTable<K, V>::contains(const K& k) {
  return bucket_for_key(k) != -1;
}

template <class K, class V>
V SparseTable<K, V>::get(const K& k) {
  int b = bucket_for_key(k);
  CHECK_NE(b, -1) << "No entry for requested key: " << k;

  return buckets_[b].v;
}

template <class K, class V>
void SparseTable<K, V>::update(const K& k, const V& v) {
  int b = bucket_for_key(k);

  if (b != -1) {
    ((Accumulator<V>*)info_->accum)->Accumulate(&buckets_[b].v, v);
  } else {
    put(k, v);
  }
}

template <class K, class V>
void SparseTable<K, V>::put(const K& k, const V& v) {
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
      resize((int)(1 + size_ * 2));
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
}
}
#endif /* SPARSE_MAP_H_ */
