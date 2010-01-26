#include "worker/table.h"
#include "worker/worker.h"

namespace dsm {

void GlobalTable::clear() {
 for (int i = 0; i < local_shards_.size(); ++i) {
    if (local_shards_[i]) {
      partitions_[i]->clear();
    }
  }
}

bool GlobalTable::empty() {
  for (int i = 0; i < local_shards_.size(); ++i) {
    if (local_shards_[i] && !partitions_[i]->empty()) {
      return false;
    }
  }
  return true;
}


bool GlobalTable::is_local_shard(int shard) {
  return local_shards_[shard];
}

bool GlobalTable::is_local_key(const StringPiece &k) {
  return is_local_shard(get_shard_str(k));
}

vector<int> GlobalTable::local_shards() {
  vector<int> v;
  for (int i = 0; i < local_shards_.size(); ++i) {
    if (local_shards_[i]) {
      v.push_back(i);
    }
  }

  return v;
}

void GlobalTable::set_local(int s, bool local) {
  local_shards_[s] = local;
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
