#include "util/common.h"
#include "worker/accumulator.h"

namespace upc {
LocalTable::LocalTable(TableInfo tinfo) : Table(tinfo) {}

string LocalTable::get(const StringPiece &k) {
  boost::recursive_mutex::scoped_lock sl(write_lock_);
  return data_[k.AsString()];
}

void LocalTable::put(const StringPiece &k, const StringPiece &v) {
  boost::recursive_mutex::scoped_lock sl(write_lock_);
  StringMap::iterator i = data_.find(k.AsString());
  if (i != data_.end()) {
    LOG(INFO) << "Accumulating: " << k.AsString() << " : " << str_as_double(v.AsString()) << " : " << str_as_double(i->second);
    i->second = info_.af(i->second, v.AsString());
  } else {
    data_.insert(make_pair(k.AsString(), v.AsString()));
  }
}

void LocalTable::remove(const StringPiece &k) {
  boost::recursive_mutex::scoped_lock sl(write_lock_);
  data_.erase(data_.find(k.AsString()));
}

void LocalTable::clear() {
  boost::recursive_mutex::scoped_lock sl(write_lock_);
  data_.clear();
}

bool LocalTable::contains(const StringPiece &k) {
  return data_.find(k.AsString()) != data_.end();
}

bool LocalTable::empty() {
  return data_.empty();
}

int64_t LocalTable::size() {
  return data_.size();
}

void LocalTable::applyUpdates(const HashUpdate& req) {
  boost::recursive_mutex::scoped_lock sl(write_lock_);
  for (int i = 0; i < req.put_size(); ++i) {
    const Pair &p = req.put(i);
    put(p.key(), p.value());
  }

  for (int i = 0; i < req.remove_size(); ++i) {
    const string& k = req.remove(i);
    remove(k);
  }
}

LocalTable::Iterator *LocalTable::get_iterator() {
  return new Iterator(this);
}

LocalTable::Iterator::Iterator(LocalTable *owner) : owner_(owner), it_(owner_->data_.begin()) {}

string LocalTable::Iterator::key() {
  return it_->first;
}

string LocalTable::Iterator::value() {
  return it_->second;
}

void LocalTable::Iterator::next() {
  ++it_;
}

bool LocalTable::Iterator::done() {
  return it_ == owner_->data_.end();
}

PartitionedTable::PartitionedTable(TableInfo tinfo) : Table(tinfo) {
  partitions_.resize(info_.num_threads);

  for (int i = 0; i < partitions_.size(); ++i) {
    TableInfo linfo = info_;
    linfo.owner_thread = i;
    partitions_[i] = new LocalTable(linfo);
  }
}

void PartitionedTable::ApplyUpdates(const upc::HashUpdate& req) {
  partitions_[info_.owner_thread]->applyUpdates(req);
}

bool PartitionedTable::GetPendingUpdates(deque<LocalTable*> *out) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *a = partitions_[i];
    if (i != info_.owner_thread && !a->empty()) {
      TableInfo linfo = info_;
      linfo.owner_thread = i;
      partitions_[i] = new LocalTable(linfo);
      out->push_back(a);
    }
  }

  return out->size() > 0;
}

int PartitionedTable::pending_write_bytes() {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int64_t s = 0;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *a = partitions_[i];
    if (i != info_.owner_thread) {
      s += a->size();
    }
  }

  return s;
}

void PartitionedTable::put(const StringPiece &k, const StringPiece &v) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int shard = info_.sf(k, partitions_.size());
  LocalTable *h = partitions_[shard];
  h->put(k, v);
}

string PartitionedTable::get_local(const StringPiece &k) {
  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int shard = info_.sf(k, partitions_.size());
  CHECK_EQ(shard, info_.owner_thread);

  LocalTable *h = partitions_[shard];
  return h->get(k);
}

string PartitionedTable::get(const StringPiece &k) {
  int shard = info_.sf(k, partitions_.size());
  if (shard == info_.owner_thread) {
    return partitions_[shard]->get(k);
  }

  if (partitions_[shard]->contains(k)) {
    string data = partitions_[shard]->get(k);
    return data;
  }

  VLOG(1) << "Requesting key " << k.AsString() << " from shard " << shard;
  HashRequest req;
  HashUpdate resp;
  req.set_key(k.AsString());
  req.set_table_id(info_.table_id);

  info_.rpc->Send(shard, MTYPE_GET_REQUEST, req);
  info_.rpc->Read(shard, MTYPE_GET_RESPONSE, &resp);

  VLOG(1) << "Got key " << k.AsString() << " : " << resp.put(0).value();

  return resp.put(0).value();
}

void PartitionedTable::remove(const StringPiece &k) {
  LOG(FATAL) << "Not implemented!";
}

LocalTable* PartitionedTable::get_local() {
  return partitions_[info().owner_thread];
}

}
