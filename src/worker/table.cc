#include "worker/table.h"
#include "worker/worker.h"

namespace dsm {

GlobalTable::GlobalTable(const dsm::TableInfo &info) : Table(info) {
  partitions_.resize(info.num_shards);
  worker_for_shard_.resize(info.num_shards);
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
  return worker_for_shard_[shard] == info_.worker->id();
}

bool GlobalTable::is_local_key(const StringPiece &k) {
  return is_local_shard(get_shard_str(k));
}

void GlobalTable::set_owner(int shard, int w) {
  worker_for_shard_[shard] = w;
}

void GlobalTable::get_remote(int shard, const StringPiece& k, string* v) {
  HashRequest req;
  HashUpdate resp;

  req.set_key(k.AsString());
  req.set_table_id(info().table_id);

  Worker *w = info().worker;
  int peer = w->peer_for_shard(info().table_id, shard) + 1;

//  LOG(INFO) << " peer " << peer << " : " << shard;
  w->Send(peer, MTYPE_GET_REQUEST, req);
  w->Read(peer, MTYPE_GET_RESPONSE, &resp);

  StringPiece vpiece = resp.value(0);
  v->assign(vpiece.data, vpiece.len);
}

void GlobalTable::SendUpdates() {
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (!is_local_shard(i) && !t->empty()) {
      info().worker->SendUpdate(t);
      t->clear();
    }
  }

  info().worker->PollPeers();
  pending_writes_ = 0;
}

void GlobalTable::CheckForUpdates() {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);
  info().worker->PollPeers();
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

}
