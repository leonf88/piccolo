#include <boost/bind.hpp>
#include <signal.h>

#include "util/common.h"
#include "worker/worker.h"
#include "kernel/kernel.h"
#include "kernel/table-registry.h"

DEFINE_double(sleep_hack, 0.0, "");
DEFINE_double(sleep_time, 0.001, "");
DEFINE_string(checkpoint_write_dir, "checkpoints", "");
DEFINE_string(checkpoint_read_dir, "checkpoints", "");

namespace dsm {

struct Worker::Stub : private boost::noncopyable {
  int32_t id;
  int32_t epoch;

  Stub(int id) : id(id), epoch(0) { }
};

Worker::Worker(const ConfigData &c) {
  epoch_ = 0;
  active_checkpoint_ = CP_NONE;

  network_ = NetworkThread::Get();

  config_.CopyFrom(c);
  config_.set_worker_id(network_->id() - 1);

  num_peers_ = config_.num_workers();
  peers_.resize(num_peers_);
  for (int i = 0; i < num_peers_; ++i) {
    peers_[i] = new Stub(i + 1);
  }

  running_ = true;

  // HACKHACKHACK - register ourselves with any existing tables
  Registry::TableMap &t = Registry::get_tables();
  for (Registry::TableMap::iterator i = t.begin(); i != t.end(); ++i) {
    i->second->set_worker(this);
  }

}

int Worker::peer_for_shard(int table, int shard) const {
  return Registry::get_tables()[table]->get_owner(shard);
}

void Worker::Run() {
  kernel_thread_ = new boost::thread(&Worker::KernelLoop, this);
  table_thread_ = new boost::thread(&Worker::TableLoop, this);
  table_thread_->join();
  kernel_thread_->join();
}

Worker::~Worker() {
  running_ = false;

  for (int i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}

void Worker::TableLoop() {
  while (running_) {
    HandleGetRequests();
    Sleep(FLAGS_sleep_time);
  }
}

void Worker::KernelLoop() {
  VLOG(1) << "Worker " << config_.worker_id() << " registering...";
  RegisterWorkerRequest req;
  req.set_id(id());
  req.set_slots(config_.slots());
  network_->Send(0, MTYPE_REGISTER_WORKER, req);


  KernelRequest kreq;

  while (running_) {
    Timer idle;

    while (!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)) {
      CheckNetwork();
      Sleep(FLAGS_sleep_time);

      if (!running_) {
        return;
      }
    }
    stats_.set_idle_time(stats_.idle_time() + idle.elapsed());

    VLOG(1) << "Received run request for " << kreq;

    if (peer_for_shard(kreq.table(), kreq.shard()) != config_.worker_id()) {
      LOG(FATAL) << "Received a shard I can't work on! : " << kreq.shard()
                 << " : " << peer_for_shard(kreq.table(), kreq.shard());
    }

    KernelInfo *helper = Registry::get_kernel(kreq.kernel());
    KernelId id(kreq.kernel(), kreq.table(), kreq.shard());
    DSMKernel* d = kernels_[id];

    if (!d) {
      d = helper->create();
      kernels_[id] = d;
      d->initialize_internal(this, kreq.table(), kreq.shard());
      d->InitKernel();
    }

    if (this->id() == 1 && FLAGS_sleep_hack > 0) {
      Sleep(FLAGS_sleep_hack);
    }

    helper->Run(d, kreq.method());

    // Flush any table updates leftover.
    for (Registry::TableMap::iterator i = Registry::get_tables().begin();
         i != Registry::get_tables().end(); ++i) {
      i->second->SendUpdates();
    }

    KernelDone kd;
    kd.mutable_kernel()->CopyFrom(kreq);
    Registry::TableMap &tmap = Registry::get_tables();
    for (Registry::TableMap::iterator i = tmap.begin(); i != tmap.end(); ++i) {
      GlobalView* t = i->second;
      for (int j = 0; j < t->num_shards(); ++j) {
        if (t->is_local_shard(j)) {
          ShardInfo *si = kd.add_shards();
          si->set_entries(t->shard_size(j));
          si->set_owner(this->id());
          si->set_table(i->first);
          si->set_shard(j);
        }
      }
    }
    network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

    VLOG(1) << "Kernel finished: " << kreq;
    DumpProfile();
  }
}

void Worker::Flush() {
  Timer idle;

  Registry::TableMap &tmap = Registry::get_tables();
  for (Registry::TableMap::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    i->second->SendUpdates();
  }

  network_->Flush();
  stats_.set_network_time(stats_.network_time() + idle.elapsed());
}

void Worker::CheckNetwork() {
  CheckForMasterUpdates();
  HandlePutRequests();

  // Flush any tables we no longer own.
  for (set<GlobalView*>::iterator i = dirty_tables_.begin(); i != dirty_tables_.end(); ++i) {
    (*i)->SendUpdates();
  }

  dirty_tables_.clear();
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  Registry::TableMap &tmap = Registry::get_tables();
  for (Registry::TableMap::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    t += i->second->pending_write_bytes();
  }

  return t;
}

bool Worker::network_idle() const {
  return network_->pending_bytes() == 0;
}

bool Worker::has_incoming_data() const {
  return true;
}

void Worker::UpdateEpoch(int peer, int peer_epoch) {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  VLOG(1) << "Got peer marker: " << MP(peer, MP(epoch_, peer_epoch));
  if (epoch_ < peer_epoch) {
    LOG(INFO) << "Received new epoch marker from peer:" << MP(epoch_, peer_epoch);

    vector<int> to_checkpoint;
    Registry::TableMap &t = Registry::get_tables();
    for (Registry::TableMap::iterator i = t.begin(); i != t.end(); ++i) {
      to_checkpoint.push_back(i->first);
    }

    StartCheckpoint(peer_epoch, CP_ROLLING, to_checkpoint);
  }

  peers_[peer]->epoch = peer_epoch;

  bool checkpoint_done = true;
  for (int i = 0; i < peers_.size(); ++i) {
    if (peers_[i]->epoch != epoch_) {
      checkpoint_done = false;
      VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
    }
  }

  if (checkpoint_done) {
    FinishCheckpoint();
  }
}

void Worker::StartCheckpoint(int epoch, CheckpointType type, vector<int> to_checkpoint) {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  if (epoch_ >= epoch) {
    LOG(INFO) << "Skipping checkpoint; " << MP(epoch_, epoch);
    return;
  }

  epoch_ = epoch;

  File::Mkdirs(StringPrintf("%s/epoch_%05d/",
                            FLAGS_checkpoint_write_dir.c_str(), epoch_));

  Registry::TableMap &t = Registry::get_tables();
  for (int i = 0; i < to_checkpoint.size(); ++i) {
    LOG(INFO) << "Starting checkpoint... " << MP(id(), epoch_, epoch)
        << " : " << to_checkpoint[i];
    GlobalView* table = t[to_checkpoint[i]];
    table->start_checkpoint(StringPrintf("%s/epoch_%05d/checkpoint.table_%d",
                                     FLAGS_checkpoint_write_dir.c_str(),
                                     epoch_, to_checkpoint[i]));
  }

  active_checkpoint_ = type;

  // For rolling checkpoints, send out a marker to other workers indicating
  // that we have switched epochs.
  if (type == CP_ROLLING) {
    HashPut epoch_marker;
    epoch_marker.set_source(id());
    epoch_marker.set_table(-1);
    epoch_marker.set_shard(-1);
    epoch_marker.set_done(true);
    epoch_marker.set_marker(epoch_);
    for (int i = 0; i < peers_.size(); ++i) {
      network_->Send(i + 1, MTYPE_PUT_REQUEST, epoch_marker);
    }
  }

  LOG(INFO) << "Starting delta logging... " << MP(id(), epoch_, epoch);
}

void Worker::FinishCheckpoint() {
  boost::recursive_mutex::scoped_lock sl(state_lock_);

  active_checkpoint_ = CP_NONE;
  LOG(INFO) << "Worker " << id() << " flushing checkpoint.";
  Registry::TableMap &t = Registry::get_tables();

  for (int i = 0; i < peers_.size(); ++i) {
    peers_[i]->epoch = epoch_;
  }

  for (Registry::TableMap::iterator i = t.begin(); i != t.end(); ++i) {
     GlobalView* t = i->second;
     t->finish_checkpoint();
  }

  EmptyMessage req;
  network_->Send(config_.master_id(), MTYPE_CHECKPOINT_DONE, req);
}

void Worker::Restore(int epoch) {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  LOG(INFO) << "Worker restoring state from epoch: " << epoch;
  epoch_ = epoch;

  Registry::TableMap &t = Registry::get_tables();
  for (Registry::TableMap::iterator i = t.begin(); i != t.end(); ++i) {
    GlobalView* t = i->second;
    t->restore(StringPrintf("%s/epoch_%05d/checkpoint.table_%d",
                            FLAGS_checkpoint_read_dir.c_str(), epoch_, i->first));
  }

  EmptyMessage req;
  network_->Send(config_.master_id(), MTYPE_RESTORE_DONE, req);
}

void Worker::HandlePutRequests() {
  HashPut put;
  while (network_->TryRead(MPI::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
    if (put.marker() != -1) {
      UpdateEpoch(put.source(), put.marker());
      continue;
    }

    VLOG(2) << "Read put request of size: "
            << put.key_data().size() << " for " << MP(put.table(), put.shard());

    stats_.set_put_in(stats_.put_in() + 1);
    stats_.set_bytes_in(stats_.bytes_in() + put.ByteSize());

    GlobalView *t = Registry::get_table(put.table());
    t->ApplyUpdates(put);

    // Record messages from our peer channel up until they checkpointed.
    if (active_checkpoint_ == CP_MASTER_CONTROLLED ||
        (active_checkpoint_ == CP_ROLLING && put.epoch() < epoch_)) {
      t->write_delta(put);
    }

    if (put.done() && t->tainted(put.shard())) {
      VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
      t->clear_tainted(put.shard());
    }
  }
}

void Worker::HandleGetRequests() {
  int source;
  HashGet get_req;
  HashPut get_resp;
  while (network_->TryRead(MPI::ANY_SOURCE, MTYPE_GET_REQUEST, &get_req, &source)) {
//    LOG(INFO) << "Get request: " << get_req;

    stats_.set_get_in(stats_.get_in() + 1);
    stats_.set_bytes_in(stats_.bytes_in() + get_req.ByteSize());

    get_resp.Clear();
    get_resp.set_source(config_.worker_id());
    get_resp.set_table(get_req.table());
    get_resp.set_shard(-1);
    get_resp.set_done(true);
    get_resp.set_epoch(epoch_);

    {
      GlobalView * t = Registry::get_table(get_req.table());
      int shard = t->get_shard_str(get_req.key());
//      CHECK(t->is_local_shard(shard))
//      << "Not local for shard: " << shard
//      << " get request from: " << source
//      << " for " << MP(get_req.table(), get_req.shard());

      t->handle_get(get_req.key(), &get_resp);
    }

    network_->Send(source, MTYPE_GET_RESPONSE, get_resp);
    VLOG(2) << "Returning result for " << MP(get_req.table(), get_req.shard());
  }


  ShardAssignmentRequest shard_req;
  while (network_->TryRead(config_.master_id(), MTYPE_SHARD_ASSIGNMENT, &shard_req)) {
    for (int i = 0; i < shard_req.assign_size(); ++i) {
      const ShardAssignment &a = shard_req.assign(i);
      GlobalView *t = Registry::get_table(a.table());
      int old_owner = t->get_owner(a.shard());
      t->set_owner(a.shard(), a.new_worker());
//      VLOG(2) << "Setting owner: " << MP(a.shard(), a.new_worker());

      if (a.new_worker() == id() && old_owner != id()) {
        VLOG(1)  << "Setting self as owner of " << MP(a.table(), a.shard());

        // Don't consider ourselves canonical for this shard until we receive updates
        // from the old owner.
        if (old_owner != -1) {
          LOG(INFO) << "Setting " << MP(a.table(), a.shard()) << " as tainted.  Old owner was: " << old_owner;
          t->set_tainted(a.shard());
        }
      } else if (old_owner == id() && a.new_worker() != id()) {
        VLOG(1) << "Lost ownership of " << MP(a.table(), a.shard()) << " to " << a.new_worker();
        // A new worker has taken ownership of this shard.  Flush our data out.
        t->set_dirty(a.shard());
        dirty_tables_.insert(t);
      }
    }

    EmptyMessage empty;
    network_->Send(config_.master_id(), MTYPE_SHARD_ASSIGNMENT_DONE, empty);
  }
}

void Worker::CheckForMasterUpdates() {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  // Check for shutdown.
  EmptyMessage empty;
  KernelRequest k;

  if (network_->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &empty)) {
    VLOG(1) << "Shutting down worker " << config_.worker_id();
    running_ = false;
    return;
  }

  CheckpointRequest checkpoint_msg;
  while (network_->TryRead(config_.master_id(), MTYPE_START_CHECKPOINT, &checkpoint_msg)) {
    vector<int> tablev;
    for (int i = 0; i < checkpoint_msg.table_size(); ++i) {
      tablev.push_back(checkpoint_msg.table(i));
    }

    StartCheckpoint(checkpoint_msg.epoch(),
                    (CheckpointType)checkpoint_msg.checkpoint_type(),
                    tablev);
  }
  
  while (network_->TryRead(config_.master_id(), MTYPE_FINISH_CHECKPOINT, &empty)) {
    FinishCheckpoint();
  }

  StartRestore restore_msg;
  while (network_->TryRead(config_.master_id(), MTYPE_RESTORE, &restore_msg)) {
    Restore(restore_msg.epoch());
  }

  // Check for new kernels to run, and report finished kernels to the master.
  while (network_->TryRead(config_.master_id(), MTYPE_WORKER_FLUSH, &empty)) {
    Flush();
    network_->Send(config_.master_id(), MTYPE_WORKER_FLUSH_DONE, empty);
  }
}

bool StartWorker(const ConfigData& conf) {
  if (NetworkThread::Get()->id() == 0)
    return false;

  Worker w(conf);
  w.Run();
  LOG(INFO) << "Worker stats: " << conf.worker_id() << " :: " << w.get_stats();
  exit(0);
}

} // end namespace
