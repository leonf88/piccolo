#include "kernel/global-table.h"
#include "worker/worker.h"

static const int kMaxNetworkPending = 1 << 26;
static const int kMaxNetworkChunk = 1 << 20;

namespace dsm {

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

void GlobalTable::Init(const dsm::TableDescriptor *info) {
  TableBase::Init(info);
  worker_id_ = -1;
  partitions_.resize(info->num_shards);
  partinfo_.resize(info->num_shards);
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
  TableData resp;

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

  *v = resp.kv_data(0).value();
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

void GlobalTable::write_delta(const TableData& d) {
  if (!is_local_shard(d.shard())) {
    LOG_EVERY_N(INFO, 1000) << "Ignoring delta write for forwarded data";
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

void GlobalTable::handle_get(const HashGet& get_req, TableData *get_resp) {
  boost::recursive_mutex::scoped_lock sl(mutex());

  int shard = get_req.shard();
  if (!is_local_shard(shard)) {
    LOG_EVERY_N(WARNING, 1000) << "Not local for shard: " << shard;
  }

  UntypedTable *t = (UntypedTable*)partitions_[shard];
  if (!t->contains_str(get_req.key())) {
    get_resp->set_missing_key(true);
  } else {
    Arg *kv = get_resp->add_kv_data();
    kv->set_key(get_req.key());
    kv->set_value(t->get_str(get_req.key()));
  }
}

void GlobalTable::HandlePutRequests() {
  w_->HandlePutRequests();
}

ProtoTableCoder::ProtoTableCoder(const TableData *in) : read_pos_(0), t_(const_cast<TableData*>(in)) {}

bool ProtoTableCoder::ReadEntry(string *k, string *v) {
  if (read_pos_ < t_->kv_data_size()) {
    k->assign(t_->kv_data(read_pos_).key());
    v->assign(t_->kv_data(read_pos_).value());
    ++read_pos_;
    return true;
  }

  return false;
}

void ProtoTableCoder::WriteEntry(StringPiece k, StringPiece v) {
  Arg *a = t_->add_kv_data();
  a->set_key(k.data, k.len);
  a->set_value(v.data, v.len);
}

void GlobalTable::SendUpdates() {
  TableData put;
  for (int i = 0; i < partitions_.size(); ++i) {
    LocalTable *t = partitions_[i];

    if (!is_local_shard(i) && (get_partition_info(i)->dirty || !t->empty())) {
      // Always send at least one chunk, to ensure that we clear taint on
      // tables we own.
      do {
        put.Clear();
        put.set_shard(i);
        put.set_source(w_->id());
        put.set_table(id());
        put.set_epoch(w_->epoch());

        ProtoTableCoder c(&put);
        t->Serialize(&c);
        t->clear();

        put.set_done(true);

        VLOG(2) << "Sending update for " << MP(t->id(), t->shard()) << " to " << owner(i) << " size " << put.kv_data_size();

        NetworkThread::Get()->Send(owner(i) + 1, MTYPE_PUT_REQUEST, put);
      } while(!t->empty());

      VLOG(2) << "Done with update for " << MP(t->id(), t->shard());
      t->clear();
    }
  }

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

void GlobalTable::ApplyUpdates(const dsm::TableData& req) {
  boost::recursive_mutex::scoped_lock sl(mutex());

  if (!is_local_shard(req.shard())) {
    LOG_EVERY_N(INFO, 1000)
        << "Forwarding push request from: " << MP(id(), req.shard())
        << " to " << owner(req.shard());
  }

  ProtoTableCoder c(&req);
  partitions_[req.shard()]->ApplyUpdates(&c);
}

void GlobalTable::get_local(const StringPiece &k, string* v) {
  int shard = get_shard_str(k);
  CHECK(is_local_shard(shard));

  UntypedTable *h = (UntypedTable*)partitions_[shard];
  v->assign(h->get_str(k));
}
}
