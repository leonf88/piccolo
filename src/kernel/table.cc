#include "kernel/table.h"
#include "worker/worker.h"

static const int kMaxNetworkChunk = 1 << 20;
static const int kMaxNetworkPending = 1 << 26;

namespace dsm {

GlobalTable::GlobalTable(const dsm::TableInfo &info) : Table(info) {
  partitions_.resize(info.num_shards);
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

bool GlobalTable::is_local_shard(int shard) {
  return partitions_[shard]->owner == info_.worker->id();
}

bool GlobalTable::is_local_key(const StringPiece &k) {
  return is_local_shard(get_shard_str(k));
}

void GlobalTable::set_owner(int shard, int w) {
  partitions_[shard]->owner = w;
}

int GlobalTable::get_owner(int shard) {
  return partitions_[shard]->owner;
}

void GlobalTable::get_remote(int shard, const StringPiece& k, string* v) {
  HashGet req;
  HashPut resp;

  req.set_key(k.AsString());
  req.set_table(info().table_id);
  req.set_shard(shard);

  Worker *w = info().worker;
  int peer = w->peer_for_shard(info().table_id, shard);

  VLOG(2) << "Sending get request to: " << MP(peer, shard);
  w->Send(peer, MTYPE_GET_REQUEST, req);
  w->Read(peer, MTYPE_GET_RESPONSE, &resp);

  HashPutCoder h(&resp);
  v->assign(h.value(0).data, h.value(0).len);
}

void GlobalTable::checkpoint(const string& f) {
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (is_local_shard(i)) {
      t->checkpoint(f + StringPrintf(".%05d-of-%05d", i, partitions_.size()));
    }
  }
}

void GlobalTable::restore(const string& f) {
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (!is_local_shard(i) && (t->dirty || !t->empty())) {
      t->restore(f + StringPrintf(".%05d-of-%05d", i, partitions_.size()));
    }
  }
}


void GlobalTable::SendUpdates() {
  HashPut put;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (!is_local_shard(i) && (t->dirty || !t->empty())) {
      VLOG(2) << "Sending update for " << MP(t->id(), t->shard()) << " to " << get_owner(i);

      Table::Iterator *it = t->get_iterator();

      // Always send at least one chunk, to ensure that we clear taint on
      // tables we own.
      do {
        put.Clear();
        put.set_shard(i);
        put.set_source(info().worker->id());
        put.set_table(id());
        put.set_epoch(info().worker->epoch());

        LocalTable::SerializePartial(put, it);
        info().worker->Send(get_owner(i), MTYPE_PUT_REQUEST, put);
      } while(!it->done());
      delete it;

      VLOG(2) << "Done with update for " << MP(t->id(), t->shard());
      t->clear();
    }
  }

  VLOG(1) << "Sent " << pending_writes_ << " updates.";

  pending_writes_ = 0;
}

void GlobalTable::CheckForUpdates() {
  do {
    info().worker->CheckForWorkerUpdates();
  } while (info().worker->pending_network_bytes() > kMaxNetworkPending);

  info().worker->CheckForMasterUpdates();
}

int GlobalTable::pending_write_bytes() {
  int64_t s = 0;
  for (int i = 0; i < partitions_.size(); ++i) {
    Table *t = partitions_[i];
    if (!is_local_shard(i)) {
      s += t->size();
    }
  }

  return s;
}

void GlobalTable::ApplyUpdates(const dsm::HashPut& req) {
  if (!is_local_shard(req.shard())) {
    LOG_EVERY_N(INFO, 1000)
        << "Received unexpected push request for: " << MP(id(), req.shard())
        << "; should have gone to " << get_owner(req.shard());
  }

  partitions_[req.shard()]->ApplyUpdates(req);
}

void GlobalTable::get_local(const StringPiece &k, string* v) {
  int shard = get_shard_str(k);
  CHECK(is_local_shard(shard));

  Table *h = partitions_[shard];

//  VLOG(1) << "Returning local result : " <<  h->get(Data::from_string<K>(k))
//          << " : " << Data::from_string<V>(h->get_str(k));

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

void LocalTable::ApplyUpdates(const HashPut& req) {
  CHECK_EQ(req.key_offset_size(), req.value_offset_size());
  HashPutCoder h(req);

  for (int i = 0; i < h.size(); ++i) {
    put_str(h.key(i), h.value(i));
  }
}

void LocalTable::SerializePartial(HashPut& r, Table::Iterator *it) {
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

}
