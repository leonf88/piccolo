#include "util/common.h"
#include "worker/accumulator.h"

namespace upc {
LocalHash::LocalHash(ShardingFunction sf, HashFunction hf, AccumFunction af, int thread, int id) :
  SharedTable(sf, hf, af, thread, id) {
}

StringPiece LocalHash::get(const StringPiece &k) {
  return data[k.AsString()];
}

void LocalHash::put(const StringPiece &k, const StringPiece &v) {
  data[k.AsString()] = v.AsString();
}

void LocalHash::remove(const StringPiece &k) {
  data.erase(data.find(k.AsString()));
}

void LocalHash::clear() {
  data.clear();
}

bool LocalHash::empty() {
  return data.empty();
}

int64_t LocalHash::size() {
  return data.size();
}

void LocalHash::applyUpdates(const HashUpdateRequest& req) {
  for (int i = 0; i < req.put_size(); ++i) {
    const Pair &p = req.put(i);
    put(p.key(), p.value());
  }

  for (int i = 0; i < req.remove_size(); ++i) {
    const string& k = req.remove(i);
    remove(k);
  }
}

LocalHash::Iterator *LocalHash::get_iterator() {
  return new Iterator(this);
}

LocalHash::Iterator::Iterator(LocalHash *owner) : owner_(owner), it_(owner_->data.begin()) {}

StringPiece LocalHash::Iterator::key() {
  return it_->first;
}

StringPiece LocalHash::Iterator::value() {
  return it_->second;
}

void LocalHash::Iterator::next() {
  ++it_;
}

bool LocalHash::Iterator::done() {
  return it_ == owner_->data.end();
}

void PartitionedHash::ApplyUpdates(const upc::HashUpdateRequest& req) {
  partitions[owner]->applyUpdates(req);
}

bool PartitionedHash::GetPendingUpdates(deque<LocalHash*> *out) {
  boost::recursive_mutex::scoped_lock sl(pending_lock);

  for (int i = 0; i < partitions.size(); ++i) {
    LocalHash *a = partitions[i];
    if (i != owner && !a->empty()) {
      partitions[i] = new LocalHash(sf_, hf_, af_, i, hash_id);
      out->push_back(a);
      while (accum_working[i]);
    }
  }

  return out->size() > 0;
}

int PartitionedHash::pending_write_bytes() {
  boost::recursive_mutex::scoped_lock sl(pending_lock);

  int64_t s = 0;
  for (int i = 0; i < partitions.size(); ++i) {
    LocalHash *a = partitions[i];
    if (i != owner) {
      s += a->size();
    }
  }

  return s;
}

void PartitionedHash::put(const StringPiece &k, const StringPiece &v) {
  int shard = sf_(k, partitions.size());
  accum_working[shard] = 1;
  LocalHash *h = partitions[shard];
  h->put(k, v);
  accum_working[shard] = 0;

  //LOG_EVERY_N(INFO, 100) << "Added key :: " << k.AsString() << " shard " << shard;
}

StringPiece PartitionedHash::get(const StringPiece &k) {
  int shard = sf_(k, partitions.size());
  accum_working[shard] = 1;
  LocalHash *h = partitions[shard];
  StringPiece data = h->get(k);
  accum_working[shard] = 0;
  return data;
}

void PartitionedHash::remove(const StringPiece &k) {
  LOG(FATAL) << "Not implemented!";
}

}
