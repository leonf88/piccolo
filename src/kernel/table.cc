#include "kernel/table.h"
#include "worker/worker.h"

static const int kMaxNetworkChunk = 1 << 10;
static const int kMaxNetworkPending = 1 << 26;

namespace dsm {

GlobalTable::GlobalTable(const dsm::TableInfo &info) : Table(info) {
  partitions_.resize(info.num_shards);
}

void GlobalTable::clear() {
 for (int i = 0; i < partitions_.size(); ++i) {
    if (is_local_shard(i)) {
      partitions_[i]->clear();
    }
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
  HashRequest req;
  HashUpdate resp;

  req.set_key(k.AsString());
  req.set_table(info().table_id);
  req.set_shard(shard);

  Worker *w = info().worker;
  int peer = w->peer_for_shard(info().table_id, shard);

  VLOG(2) << "Sending get request to: " << MP(peer, shard);
  w->Send(peer, MTYPE_GET_REQUEST, req);
  w->Read(peer, MTYPE_GET_RESPONSE, &resp);

  v->assign(resp.value(0));
}

void GlobalTable::SendUpdates() {
  HashUpdate put;
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

        LocalTable::SerializePartial(put, it);
        info().worker->Send(get_owner(i), MTYPE_PUT_REQUEST, put);
      } while(!it->done());
      delete it;

      VLOG(2) << "Done with update for " << MP(t->id(), t->shard());
      t->clear();
    }
  }

  pending_writes_ = 0;
}

void GlobalTable::CheckForUpdates() {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);
  do {
    info().worker->PollWorkers();
  } while (info().worker->pending_network_bytes() > kMaxNetworkPending);
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

void GlobalTable::ApplyUpdates(const dsm::HashUpdate& req) {
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

void LocalTable::ApplyUpdates(const HashUpdate& req) {
  CHECK_EQ(req.key_size(), req.value_size());
  for (int i = 0; i < req.key_size(); ++i) {
    put_str(req.key(i), req.value(i));
  }
}

void LocalTable::SerializePartial(HashUpdate& r, Table::Iterator *it) {
  int bytes_used = 0;
  for (; !it->done() && bytes_used < kMaxNetworkChunk; it->Next()) {
    it->key_str(r.add_key());
    it->value_str(r.add_value());
    bytes_used += r.key(0).size() + r.value(0).size();
  }

  r.set_done(it->done());
}

}
