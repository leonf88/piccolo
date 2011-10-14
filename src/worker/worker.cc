#include <boost/bind.hpp>
#include <signal.h>

#include "util/common.h"
#include "worker/worker.h"
#include "kernel/kernel.h"
#include "kernel/table-registry.h"

DECLARE_double(sleep_time);
DEFINE_double(sleep_hack, 0.0, "");
DEFINE_string(checkpoint_write_dir, "/var/tmp/piccolo-checkpoint", "");
DEFINE_string(checkpoint_read_dir, "/var/tmp/piccolo-checkpoint", "");

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

  running_ = true;		//this is WORKER running, not KERNEL running!
  krunning_ = false;	//and this is for KERNEL running
  handling_putreqs_ = false;
  iterator_id_ = 0;

  // HACKHACKHACK - register ourselves with any existing tables
  TableRegistry::Map &t = TableRegistry::Get()->tables();
  for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
    i->second->set_helper(this);
  }

  // Register RPC endpoints.
  RegisterCallback(MTYPE_GET,
                   new HashGet, new TableData,
                   &Worker::HandleGetRequest, this);

  RegisterCallback(MTYPE_SHARD_ASSIGNMENT,
                   new ShardAssignmentRequest, new EmptyMessage,
                   &Worker::HandleShardAssignment, this);

  RegisterCallback(MTYPE_ITERATOR,
                   new IteratorRequest, new IteratorResponse,
                   &Worker::HandleIteratorRequest, this);

  RegisterCallback(MTYPE_CLEAR_TABLE,
                   new ClearTable, new EmptyMessage,
                   &Worker::HandleClearRequest, this);

  RegisterCallback(MTYPE_SWAP_TABLE,
                   new SwapTable, new EmptyMessage,
                   &Worker::HandleSwapRequest, this);

  RegisterCallback(MTYPE_WORKER_FLUSH,
                   new EmptyMessage, new FlushResponse,
                   &Worker::HandleFlush, this);

  RegisterCallback(MTYPE_WORKER_APPLY,
                   new EmptyMessage, new EmptyMessage,
                   &Worker::HandleApply, this);

  RegisterCallback(MTYPE_WORKER_FINALIZE,
                   new EmptyMessage, new EmptyMessage,
                   &Worker::HandleFinalize, this);

  RegisterCallback(MTYPE_RESTORE,
                   new StartRestore, new EmptyMessage,
                   &Worker::HandleStartRestore, this);

/*
  RegisterCallback(MTYPE_ENABLE_TRIGGER,
                   new EnableTrigger, new EmptyMessage,
                   &Worker::HandleEnableTrigger, this);
*/

  NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_FLUSH);
  NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_APPLY);
}

int Worker::peer_for_shard(int table, int shard) const {
  return TableRegistry::Get()->tables()[table]->owner(shard);
}

void Worker::Run() {
  KernelLoop();
}

Worker::~Worker() {
  running_ = false;

  for (size_t i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}

void Worker::KernelLoop() {
  VLOG(1) << "Worker " << config_.worker_id() << " registering...";
  RegisterWorkerRequest req;
  req.set_id(id());
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
    krunning_ = true;	//a kernel is running
    stats_["idle_time"] += idle.elapsed();

    VLOG(1) << "Received run request for " << kreq;

    if (peer_for_shard(kreq.table(), kreq.shard()) != config_.worker_id()) {
      LOG(FATAL) << "Received a shard I can't work on! : " << kreq.shard()
                 << " : " << peer_for_shard(kreq.table(), kreq.shard());
    }

    KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
    KernelId id(kreq.kernel(), kreq.table(), kreq.shard());
    DSMKernel* d = kernels_[id];

    if (!d) {
      d = helper->create();
      kernels_[id] = d;
      d->initialize_internal(this, kreq.table(), kreq.shard());
      d->InitKernel();
    }

    MarshalledMap args;
    args.FromMessage(kreq.args());
    d->set_args(args);

    if (this->id() == 1 && FLAGS_sleep_hack > 0) {
      Sleep(FLAGS_sleep_hack);
    }

    // Run the user kernel
    helper->Run(d, kreq.method());

    KernelDone kd;
    kd.mutable_kernel()->CopyFrom(kreq);
    TableRegistry::Map &tmap = TableRegistry::Get()->tables();
    for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
      GlobalTable* t = i->second;
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
    krunning_ = false;
    network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

    VLOG(1) << "Kernel finished: " << kreq;
    DumpProfile();
  }
}

void Worker::CheckNetwork() {
  Timer net;
  CheckForMasterUpdates();
  HandlePutRequest();

  // Flush any tables we no longer own.
  for (unordered_set<GlobalTable*>::iterator i = dirty_tables_.begin(); i != dirty_tables_.end(); ++i) {
    MutableGlobalTable *mg = dynamic_cast<MutableGlobalTable*>(*i);
    if (mg) {
      mg->SendUpdates();
    }
  }

  dirty_tables_.clear();
  stats_["network_time"] += net.elapsed();
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  TableRegistry::Map &tmap = TableRegistry::Get()->tables();
  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    MutableGlobalTable *mg = dynamic_cast<MutableGlobalTable*>(i->second);
    if (mg) {
      t += mg->pending_write_bytes();
    }
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

  //continuous checkpointing behavior is a bit different
  if (active_checkpoint_ == CP_CONTINUOUS) {
    UpdateEpochContinuous(peer,peer_epoch);
    return;
  }

  if (epoch_ < peer_epoch) {
    LOG(INFO) << "Received new epoch marker from peer:" << MP(epoch_, peer_epoch);

    checkpoint_tables_.clear();
    TableRegistry::Map &t = TableRegistry::Get()->tables();
    for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
      checkpoint_tables_.insert(make_pair(i->first, true));
    }

    StartCheckpoint(peer_epoch, CP_INTERVAL,false);
  }

  peers_[peer]->epoch = peer_epoch;

  bool checkpoint_done = true;
  for (size_t i = 0; i < peers_.size(); ++i) {
    if (peers_[i]->epoch != epoch_) {
      checkpoint_done = false;
      VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
    }
  }

  if (checkpoint_done) {
    LOG(INFO) << "Finishing rolling checkpoint on worker " << id();
    FinishCheckpoint(false);
  }
}

void Worker::UpdateEpochContinuous(int peer, int peer_epoch) {
  peers_[peer]->epoch = peer_epoch;
  peers_[peer]->epoch = peer_epoch;
  bool checkpoint_done = true;
  for (size_t i = 0; i < peers_.size(); ++i) {
    if (peers_[i]->epoch != epoch_) {
      checkpoint_done = false;
      VLOG(1) << "Channel is out of date: " << i << " : " << MP(peers_[i]->epoch, epoch_);
    }
  }
}

void Worker::StartCheckpoint(int epoch, CheckpointType type, bool deltaOnly) {
  boost::recursive_mutex::scoped_lock sl(state_lock_);

  if (epoch_ >= epoch) {
    LOG(INFO) << "Skipping checkpoint; " << MP(epoch_, epoch);
    return;
  }

  epoch_ = epoch;

  File::Mkdirs(StringPrintf("%s/epoch_%05d/",
                            FLAGS_checkpoint_write_dir.c_str(), epoch_));

  TableRegistry::Map &t = TableRegistry::Get()->tables();
  for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
    if (checkpoint_tables_.find(i->first) != checkpoint_tables_.end()) {
      VLOG(1) << "Starting checkpoint... " << MP(id(), epoch_, epoch) << " : " << i->first;
      Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
      CHECK(t != NULL) << "Tried to checkpoint a read-only table?";

      t->start_checkpoint(StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
                                               FLAGS_checkpoint_write_dir.c_str(),
                                               epoch_, i->first),deltaOnly);
    }
  }

  active_checkpoint_ = type;
  VLOG(1) << "Checkpointing active with type " << type;

  // For rolling checkpoints, send out a marker to other workers indicating
  // that we have switched epochs.
  if (type == CP_INTERVAL) { // || type == CP_CONTINUOUS) {
    TableData epoch_marker;
    epoch_marker.set_source(id());
    epoch_marker.set_table(-1);
    epoch_marker.set_shard(-1);
    epoch_marker.set_done(true);
    epoch_marker.set_marker(epoch_);
    for (size_t i = 0; i < peers_.size(); ++i) {
      network_->Send(i + 1, MTYPE_PUT_REQUEST, epoch_marker);
    }
  }

  EmptyMessage req;
  network_->Send(config_.master_id(), MTYPE_START_CHECKPOINT_DONE, req);
  VLOG(1) << "Starting delta logging... " << MP(id(), epoch_, epoch);
}

void Worker::FinishCheckpoint(bool deltaOnly) {
  VLOG(1) << "Worker " << id() << " flushing checkpoint for epoch " << epoch_ << ".";
  boost::recursive_mutex::scoped_lock sl(state_lock_);	//important! We won't lose state for continuous

  active_checkpoint_ = (active_checkpoint_ == CP_CONTINUOUS)?CP_CONTINUOUS:CP_NONE;
  TableRegistry::Map &t = TableRegistry::Get()->tables();

  for (size_t i = 0; i < peers_.size(); ++i) {
    peers_[i]->epoch = epoch_;
  }

  for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
    Checkpointable *t = dynamic_cast<Checkpointable*>(i->second);
    if (t) {
      t->finish_checkpoint();
    } else {
      LOG(INFO) << "Skipping finish checkpoint for " << i->second->id();
    }
  }

  EmptyMessage req;
  network_->Send(config_.master_id(), MTYPE_FINISH_CHECKPOINT_DONE, req);

  if (active_checkpoint_ == CP_CONTINUOUS) {
    VLOG(1) << "Continuous checkpointing starting epoch " << epoch_+1;
    StartCheckpoint(epoch_+1,active_checkpoint_,deltaOnly);
  }
}

void Worker::HandleStartRestore(const StartRestore& req,
                                     EmptyMessage* resp,
                                     const RPCInfo& rpc) {
  int epoch = req.epoch();
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  LOG(INFO) << "Master instructing restore starting at epoch " << epoch;

  TableRegistry::Map &t = TableRegistry::Get()->tables();
  // CRM 2011-10-13: Look for latest and later epochs that might
  // contain deltas.  Master will have sent the epoch number of the
  // last FULL checkpoint
  epoch_ = epoch;
  do {
    VLOG(2) << "Worker restoring state from epoch: " << epoch_;
    if (!File::Exists(StringPrintf("%s/epoch_%05d/checkpoint.finished",FLAGS_checkpoint_read_dir.c_str(),epoch_))) {
      VLOG(2) << "Epoch " << epoch_ << " did not finish; restore process terminating normally.";
      break;
    }
    for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
      Checkpointable* t = dynamic_cast<Checkpointable*>(i->second);
      if (t) {
        t->restore(StringPrintf("%s/epoch_%05d/checkpoint.table-%d",
                                FLAGS_checkpoint_read_dir.c_str(), epoch_, i->first));
      }
    }
    epoch_++;
  } while(File::Exists(StringPrintf("%s/epoch_%05d",FLAGS_checkpoint_read_dir.c_str(),epoch_)));
  LOG(INFO) << "State restored. Current epoch is " << epoch_ << ".";
}

void Worker::HandlePutRequest() {
  boost::recursive_try_mutex::scoped_lock sl(state_lock_);
  if (!sl.owns_lock() || handling_putreqs_ == true)
    return;

  handling_putreqs_ = true;	//protected by state_lock_

  TableData put;
  while (network_->TryRead(MPI::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
    if (put.marker() != -1) {
      UpdateEpoch(put.source(), put.marker());
      continue;
    }

    VLOG(2) << "Read put request of size: "
            << put.kv_data_size() << " for " << MP(put.table(), put.shard());

    MutableGlobalTable *t = TableRegistry::Get()->mutable_table(put.table());
    t->ApplyUpdates(put);
    VLOG(3) << "Finished ApplyUpdate from HandlePutRequest" << endl;

    // Record messages from our peer channel up until they are checkpointed.
    if (active_checkpoint_ == CP_TASK_COMMIT || active_checkpoint_ == CP_CONTINUOUS || 
        (active_checkpoint_ == CP_INTERVAL && put.epoch() < epoch_)) {
      if (checkpoint_tables_.find(t->id()) != checkpoint_tables_.end()) {
        Checkpointable *ct = dynamic_cast<Checkpointable*>(t);
        ct->write_delta(put);
      }
    }

    if (put.done() && t->tainted(put.shard())) {
      VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
      t->get_partition_info(put.shard())->tainted = false;
    }
  }

  handling_putreqs_ = false;		//protected by state_lock_
}

void Worker::HandleGetRequest(const HashGet& get_req, TableData *get_resp, const RPCInfo& rpc) {
//    LOG(INFO) << "Get request: " << get_req;

  get_resp->Clear();
  get_resp->set_source(config_.worker_id());
  get_resp->set_table(get_req.table());
  get_resp->set_shard(-1);
  get_resp->set_done(true);
  get_resp->set_epoch(epoch_);

  {
    GlobalTable * t = TableRegistry::Get()->table(get_req.table());
    t->handle_get(get_req, get_resp);
  }

  VLOG(2) << "Returning result for " << MP(get_req.table(), get_req.shard()) << " - found? " << !get_resp->missing_key();
}

void Worker::HandleSwapRequest(const SwapTable& req, EmptyMessage *resp, const RPCInfo& rpc) {
  MutableGlobalTable *ta = TableRegistry::Get()->mutable_table(req.table_a());
  MutableGlobalTable *tb = TableRegistry::Get()->mutable_table(req.table_b());

  ta->local_swap(tb);
}

void Worker::HandleClearRequest(const ClearTable& req, EmptyMessage *resp, const RPCInfo& rpc) {
  MutableGlobalTable *ta = TableRegistry::Get()->mutable_table(req.table());

  for (int i = 0; i < ta->num_shards(); ++i) {
    if (ta->is_local_shard(i)) {
      ta->get_partition(i)->clear();
    }
  }
}

void Worker::HandleIteratorRequest(const IteratorRequest& iterator_req, IteratorResponse *iterator_resp, const RPCInfo& rpc) {
  int table = iterator_req.table();
  int shard = iterator_req.shard();

  GlobalTable * t = TableRegistry::Get()->table(table);
  TableIterator* it = NULL;
  if (iterator_req.id() == -1) {
    it = t->get_iterator(shard);
    uint32_t id = iterator_id_++;
    iterators_[id] = it;
    iterator_resp->set_id(id);
  } else {
    it = iterators_[iterator_req.id()];
    iterator_resp->set_id(iterator_req.id());
    CHECK_NE(it, (void *)NULL);
    it->Next();
  }

  iterator_resp->set_row_count(0);
  iterator_resp->clear_key();
  iterator_resp->clear_value();
  for(int i=1; i<=iterator_req.row_count(); i++) {
    iterator_resp->set_done(it->done());
    if (!it->done()) {
      std::string* respkey = iterator_resp->add_key();
      it->key_str(respkey);
      std::string* respvalue = iterator_resp->add_value();
      it->value_str(respvalue);
      iterator_resp->set_row_count(i);
      if (i<iterator_req.row_count())
        it->Next ();
    } else break;
  }
  VLOG(2) << "[PREFETCH] Returning " << iterator_resp->row_count()
	<< " rows in response to request for " << iterator_req.row_count() 
    << " rows in table " << table << ", shard " << shard << endl;
}

void Worker::HandleShardAssignment(const ShardAssignmentRequest& shard_req, EmptyMessage *resp, const RPCInfo& rpc) {
//  LOG(INFO) << "Shard assignment: " << shard_req.DebugString();
  for (int i = 0; i < shard_req.assign_size(); ++i) {
    const ShardAssignment &a = shard_req.assign(i);
    GlobalTable *t = TableRegistry::Get()->table(a.table());
    int old_owner = t->owner(a.shard());
    t->get_partition_info(a.shard())->sinfo.set_owner(a.new_worker());

    VLOG(3) << "Setting owner: " << MP(a.shard(), a.new_worker());

    if (a.new_worker() == id() && old_owner != id()) {
      VLOG(1)  << "Setting self as owner of " << MP(a.table(), a.shard());

      // Don't consider ourselves canonical for this shard until we receive updates
      // from the old owner.
      if (old_owner != -1) {
        LOG(INFO) << "Setting " << MP(a.table(), a.shard())
                 << " as tainted.  Old owner was: " << old_owner
                 << " new owner is :  " << id();
        t->get_partition_info(a.shard())->tainted = true;
      }
    } else if (old_owner == id() && a.new_worker() != id()) {
      VLOG(1) << "Lost ownership of " << MP(a.table(), a.shard()) << " to " << a.new_worker();
      // A new worker has taken ownership of this shard.  Flush our data out.
      t->get_partition_info(a.shard())->dirty = true;
      dirty_tables_.insert(t);
    }
  }
}


void Worker::HandleFlush(const EmptyMessage& req, FlushResponse *resp, const RPCInfo& rpc) {
  Timer net;

  TableRegistry::Map &tmap = TableRegistry::Get()->tables();
  int updatesdone = 0;
  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*>(i->second);
    if (t) {
      updatesdone += t->clearUpdateQueue();
      int sentupdates = 0;
      t->SendUpdates(&sentupdates);
      updatesdone += sentupdates;
      //updatesdone += t->clearUpdateQueue();
    }
  }
  network_->Flush();

  VLOG(2) << "Telling master: " << updatesdone << " updates done." << endl;
  resp->set_updatesdone(updatesdone);
  network_->Send(config_.master_id(), MTYPE_FLUSH_RESPONSE, *resp);

  network_->Flush();
  stats_["network_time"] += net.elapsed();
}


void Worker::HandleApply(const EmptyMessage& req, EmptyMessage *resp, const RPCInfo& rpc) {
  if (krunning_) {
    LOG(FATAL) << "Received APPLY message while still running!?!" << endl;
    return;
  }

  HandlePutRequest();
  network_->Send(config_.master_id(), MTYPE_WORKER_APPLY_DONE, *resp);
}

// For now, this only stops Long Trigger (aka retriggering).  Could be used for other
// kernel finalization tasks too, though.
void Worker::HandleFinalize(const EmptyMessage& req, EmptyMessage *resp, const RPCInfo& rpc) {
  Timer net;
  VLOG(2) << "Finalize request received from master; performing finalization." << endl;

  TableRegistry::Map &tmap = TableRegistry::Get()->tables();
  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    MutableGlobalTable* t = dynamic_cast<MutableGlobalTable*>(i->second);
    if (t) {
      t->KernelFinalize();	//just does retrigger_stop() for now
    }
  }

  if (active_checkpoint_ == CP_CONTINUOUS) {
    active_checkpoint_ = CP_INTERVAL; //this will let it finalize properly
  }

  VLOG(2) << "Telling master: Finalized." << endl;
  network_->Send(config_.master_id(), MTYPE_WORKER_FINALIZE_DONE, *resp);

  stats_["network_time"] += net.elapsed();
}

/*
void Worker::HandleEnableTrigger(const EnableTrigger& req, EmptyMessage *resp, const RPCInfo& rpc) {
  GlobalTable *t = TableRegistry::Get()->tables()[req.table()];
  CHECK(t != NULL);
  TriggerBase *trigger = t->trigger(req.trigger_id());
  CHECK(trigger != NULL);
  trigger->enable(req.enable());
}
*/

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
    for (int i = 0; i < checkpoint_msg.table_size(); ++i) {
      checkpoint_tables_.insert(make_pair(checkpoint_msg.table(i), true));
    }

    VLOG(1) << "Starting checkpoint type " << checkpoint_msg.checkpoint_type() << 
      ", epoch " << checkpoint_msg.epoch();
    StartCheckpoint(checkpoint_msg.epoch(),
                    (CheckpointType)checkpoint_msg.checkpoint_type(),false);
  }

  CheckpointFinishRequest checkpoint_finish_msg;
  while (network_->TryRead(config_.master_id(), MTYPE_FINISH_CHECKPOINT, &checkpoint_finish_msg)) {
    VLOG(1) << "Finishing checkpoint on master's instruction";
    FinishCheckpoint(checkpoint_finish_msg.next_delta_only());
  }
}

bool StartWorker(const ConfigData& conf) {
  if (NetworkThread::Get()->id() == 0)
    return false;

  Worker w(conf);
  w.Run();
  Stats s = w.get_stats();
  s.Merge(NetworkThread::Get()->stats);
  VLOG(1) << "Worker stats: \n" << s.ToString(StringPrintf("[W%d]", conf.worker_id()));
  exit(0);
}

} // end namespace
