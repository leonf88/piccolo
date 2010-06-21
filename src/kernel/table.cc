#include "kernel/local-table.h"
#include "kernel/global-table.h"
#include "worker/worker.h"

static const int kMaxNetworkChunk = 1 << 20;
static const int kMaxNetworkPending = 1 << 26;

namespace dsm {

static void SerializePartial(HashPut& r, TableBase::Iterator *it) {
  int bytes_used = 0;
  HashPutCoder h(&r);
  string k, v;
  for (; !it->done() && bytes_used < kMaxNetworkChunk; it->Next()) {
    it->key_str(&k);
    it->value_str(&v);
    h.add_pair(k, v);
    bytes_used += k.size() + v.size();
  }

  r.set_done(it->done());
}

void GlobalTable::UpdatePartitions(const ShardInfo& info) {
  partinfo_[info.shard()].sinfo.CopyFrom(info);
}

GlobalTable::~GlobalTable() {
  for (int i = 0; i < partitions_.size(); ++i) {
    delete partitions_[i];
  }
}

LocalTable *GlobalTable::get_partition(int shard) {
  return partitions_[shard];
}

TableIterator* GlobalTable::get_iterator(int shard) {
  return partitions_[shard]->get_iterator();
}

bool GlobalTable::is_local_shard(int shard) {
  return owner(shard) == worker_id_;
}

bool GlobalTable::is_local_key(const StringPiece &k) {
  return is_local_shard(get_shard_str(k));
}

void GlobalTable::Init(const dsm::TableDescriptor &info) {
  TableBase::Init(info);
  worker_id_ = -1;
  partitions_.resize(info.num_shards);
  partinfo_.resize(info.num_shards);
}

int64_t GlobalTable::shard_size(int shard) {
  if (is_local_shard(shard)) {
    return partitions_[shard]->size();
  } else {
    return partinfo_[shard].sinfo.entries();
  }
}

void GlobalTable::clear(int shard) {
  if (is_local_shard(shard)) {
    partitions_[shard]->clear();
  } else {
    LOG(FATAL) << "Tried to clear a non-local shard - this is not supported.";
  }
}

bool GlobalTable::empty() {
  for (int i = 0; i < partitions_.size(); ++i) {
    if (is_local_shard(i) && !partitions_[i]->empty()) {
      return false;
    }
  }
  return true;
}

void GlobalTable::resize(int64_t new_size) {
  for (int i = 0; i < partitions_.size(); ++i) {
    if (is_local_shard(i)) {
      partitions_[i]->resize(new_size / partitions_.size());
    }
  }
}

void GlobalTable::set_worker(Worker* w) {
  w_ = w;
  worker_id_ = w->id();
}

bool GlobalTable::get_remote(int shard, const StringPiece& k, string* v) {
  HashGet req;
  HashPut resp;

  req.set_key(k.AsString());
  req.set_table(info().table_id);
  req.set_shard(shard);

  int peer = w_->peer_for_shard(info().table_id, shard);

  DCHECK_GE(peer, 0);
  DCHECK_LT(peer, MPI::COMM_WORLD.Get_size() - 1);

  VLOG(2) << "Sending get request to: " << MP(peer, shard);
  NetworkThread::Get()->Send(peer + 1, MTYPE_GET_REQUEST, req);
  NetworkThread::Get()->Read(peer + 1, MTYPE_GET_RESPONSE, &resp);

  if (resp.missing_key()) {
    return false;
  }

  HashPutCoder h(&resp);
  v->assign(h.value(0).data, h.value(0).len);
  return true;
}

void GlobalTable::start_checkpoint(const string& f) {
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (is_local_shard(i)) {
      t->start_checkpoint(f + StringPrintf(".%05d-of-%05d", i, partitions_.size()));
    }
  }
}

void GlobalTable::write_delta(const HashPut& d) {
  if (!is_local_shard(d.shard())) {
    LOG_EVERY_N(INFO, 1000)
        << "Ignoring delta write for forwarded data";
    return;
  }

  partitions_[d.shard()]->write_delta(d);
}

void GlobalTable::finish_checkpoint() {
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (is_local_shard(i)) {
      t->finish_checkpoint();
    }
  }
}

void GlobalTable::restore(const string& f) {
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (is_local_shard(i)) {
      t->restore(f + StringPrintf(".%05d-of-%05d", i, partitions_.size()));
    } else {
      t->clear();
    }
  }
}

void GlobalTable::handle_get(const HashGet& get_req, HashPut *get_resp) {
  boost::recursive_mutex::scoped_lock sl(mutex());

  HashPutCoder h(get_resp);

  int shard = get_req.shard();
  if (!is_local_shard(shard)) {
    LOG_EVERY_N(WARNING, 1000) << "Not local for shard: " << shard;
  }

  LocalTable *t = (LocalTable*)partitions_[shard];
  if (!t->contains_str(get_req.key())) {
    get_resp->set_missing_key(true);
  } else {
    h.add_pair(get_req.key(), t->get_str(get_req.key()));
  }
}

void GlobalTable::HandlePutRequests() {
  w_->HandlePutRequests();
}

void GlobalTable::SendUpdates() {
  HashPut put;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (!is_local_shard(i) && (get_partition_info(i)->dirty || !t->empty())) {
      VLOG(2) << "Sending update for " << MP(t->id(), t->shard()) << " to " << owner(i);

      TableBase::Iterator *it = t->get_iterator();

      // Always send at least one chunk, to ensure that we clear taint on
      // tables we own.
      do {
        put.Clear();
        put.set_shard(i);
        put.set_source(w_->id());
        put.set_table(id());
        put.set_epoch(w_->epoch());

        SerializePartial(put, it);
        NetworkThread::Get()->Send(owner(i) + 1, MTYPE_PUT_REQUEST, put);
      } while(!it->done());
      delete it;

      VLOG(2) << "Done with update for " << MP(t->id(), t->shard());
      t->clear();
    }
  }

  VLOG(1) << "Sent " << pending_writes_ << " updates.";
  pending_writes_ = 0;
}

int GlobalTable::pending_write_bytes() {
  int64_t s = 0;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];
    if (!is_local_shard(i)) {
      s += t->size();
    }
  }

  return s;
}

void GlobalTable::ApplyUpdates(const dsm::HashPut& req) {
  boost::recursive_mutex::scoped_lock sl(mutex());

  if (!is_local_shard(req.shard())) {
    LOG_EVERY_N(INFO, 1000)
        << "Forwarding push request from: " << MP(id(), req.shard())
        << " to " << owner(req.shard());
  }

  partitions_[req.shard()]->ApplyUpdates(req);
}

void GlobalTable::get_local(const StringPiece &k, string* v) {
  int shard = get_shard_str(k);
  CHECK(is_local_shard(shard));

  LocalTable *h = (LocalTable*)partitions_[shard];

  v->assign(h->get_str(k));
}

HashPutCoder::HashPutCoder(HashPut *h) : h_(h) {
  h_->add_key_offset(0);
  h_->add_value_offset(0);
}

HashPutCoder::HashPutCoder(const HashPut& h) : h_((HashPut*)&h) {
  CHECK_EQ(h_->value_offset_size(), h_->key_offset_size());
}

void HashPutCoder::add_pair(const string& k, const string& v) {
  h_->mutable_key_data()->append(k);
  h_->mutable_value_data()->append(v);

  h_->add_key_offset(h_->key_data().size());
  h_->add_value_offset(h_->value_data().size());

//  CHECK_EQ(key(size() - 1).AsString(), k);
//  CHECK_EQ(value(size() - 1).AsString(), v);
}

StringPiece HashPutCoder::key(int i) {
  int start = h_->key_offset(i);
  int end = h_->key_offset(i + 1);
//  CHECK_GT(end, start);

  return StringPiece(h_->key_data().data() + start, end - start);
}

StringPiece HashPutCoder::value(int i) {
  int start = h_->value_offset(i);
  int end = h_->value_offset(i + 1);

//  CHECK_GT(end, start);
  return StringPiece(h_->value_data().data() + start, end - start);
}

int HashPutCoder::size() {
  return h_->key_offset_size() - 1;
}

void LocalTable::write_delta(const HashPut& req) {
  if (!delta_file_) {
    LOG_EVERY_N(ERROR, 100) << "Shard: " << this->info().shard << " is somehow missing it's delta file?";
  } else {
    delta_file_->write(req);
  }
}

void LocalTable::ApplyUpdates(const HashPut& req) {
  CHECK_EQ(req.key_offset_size(), req.value_offset_size());
  HashPutCoder h(req);

  for (int i = 0; i < h.size(); ++i) {
    update_str(h.key(i), h.value(i));
  }
}
}
