#include "worker/table.h"
#include "worker/worker.h"

namespace dsm {

void GlobalTable::clear() {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);
  for (int i = 0; i < local_shards_.size(); ++i) {
    if (local_shards_[i]) {
      partitions_[i]->clear();
    }
  }
}

bool GlobalTable::empty() {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);
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

void GlobalTable::SendUpdates() {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (!is_local_shard(i) && !t->empty()) {
      info().worker->SendUpdate(t);
      t->clear();
    }
  }

  pending_writes_ = 0;
}

int GlobalTable::pending_write_bytes() {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);

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
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);
  partitions_[req.shard()]->ApplyUpdates(req);
}

string GlobalTable::get_local(const StringPiece &k) {
//  boost::recursive_mutex::scoped_lock sl(pending_lock_);

  int shard = get_shard_str(k);
  CHECK(is_local_shard(shard));

  Table *h = partitions_[shard];

//  VLOG(1) << "Returning local result : " <<  h->get(Data::from_string<K>(k))
//          << " : " << Data::from_string<V>(h->get_str(k));

  return h->get_str(k);
}

}
