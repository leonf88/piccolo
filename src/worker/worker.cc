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

// Represents an active RPC to a remote peer.
struct SendRequest : private boost::noncopyable {
  int target;
  int rpc_type;
  int failures;

  string payload;
  MPI::Request mpi_req;
  MPI::Status status;
  double start_time;

  SendRequest() {
    failures = 0;
  }

  ~SendRequest() {}

  bool finished() {
    return mpi_req.Test(status);
  }

  double elapsed() { return Now() - start_time; }

  void Send(RPCHelper* helper) {
    start_time = Now();
    mpi_req = helper->ISendData(target, rpc_type, payload);
  }
};

struct Worker::Stub : private boost::noncopyable {
  int32_t id;
  int32_t epoch;

  Stub(int id) : id(id), epoch(0) { }

  // Send the given message type and data to this peer.
  SendRequest* CreateRequest(int method, const Message& ureq) {
    SendRequest* r = new SendRequest();
    r->target = id;
    r->rpc_type = method;
    ureq.AppendToString(&r->payload);
    return r;
  }
};

// Hackery to get around mpi's unhappiness with threads.  This thread
// simply polls MPI continuously for any kind of update and adds it to
// a local queue.
class NetworkThread {
private:
  static const int kMaxHosts = 512;

  typedef deque<string> Queue;

  vector<SendRequest*> pending_sends_;
  unordered_set<SendRequest*> active_sends_;

  Queue incoming[MTYPE_MAX][kMaxHosts];

  RPCHelper *rpc_;
  MPI::Comm *world_;
  mutable boost::recursive_mutex send_lock;
  mutable boost::recursive_mutex q_lock[MTYPE_MAX];
public:
  bool running;

  NetworkThread(RPCHelper* rpc) {
    rpc_ = rpc;
    world_ = rpc->world();
    running = 1;
  }

  bool network_active() const {
    return active_sends_.size() + pending_sends_.size() > 0;
  }

  int64_t pending_bytes() const {
    boost::recursive_mutex::scoped_lock sl(send_lock);
    int64_t t = 0;

    for (unordered_set<SendRequest*>::const_iterator i = active_sends_.begin(); i != active_sends_.end(); ++i) {
      t += (*i)->payload.size();
    }

    for (int i = 0; i < pending_sends_.size(); ++i) {
      t += pending_sends_[i]->payload.size();
    }

    return t;
  }

  void CollectActive() {
    if (active_sends_.empty())
      return;

    boost::recursive_mutex::scoped_lock sl(send_lock);
    unordered_set<SendRequest*>::iterator i = active_sends_.begin();
    while (i != active_sends_.end()) {
      SendRequest *r = (*i);
      VLOG(2) << "Pending: " << MP(world_->Get_rank(), MP(r->target, r->rpc_type));
      if (r->finished()) {
        if (r->failures > 0) {
          LOG(INFO) << "Send " << MP(world_->Get_rank(), r->target) << " of size " << r->payload.size()
                    << " succeeded after " << r->failures << " failures.";
        }
        VLOG(2) << "Finished send to " << r->target << " of size " << r->payload.size();
        delete r;
        i = active_sends_.erase(i);
        continue;
      }
      ++i;
    }
  }

  void Run() {
    while (running) {
      MPI::Status st;
      if (world_->Iprobe(MPI::ANY_SOURCE, MPI::ANY_TAG, st)) {
        int tag = st.Get_tag();
        int source = st.Get_source();
        int bytes = st.Get_count(MPI::BYTE);

        string data;
        data.resize(bytes);

        world_->Recv(&data[0], bytes, MPI::BYTE, source, tag, st);

        boost::recursive_mutex::scoped_lock sl(q_lock[tag]);
        CHECK_LT(source, kMaxHosts);
        incoming[tag][source].push_back(data);
      } else {
        Sleep(FLAGS_sleep_time);
      }

      while (!pending_sends_.empty()) {
        boost::recursive_mutex::scoped_lock sl(send_lock);
        SendRequest* s = pending_sends_.back();
        pending_sends_.pop_back();
        s->Send(rpc_);
        active_sends_.insert(s);
      }

      CollectActive();

      PERIODIC(10., { DumpProfile(); });
    }
  }

  bool check_queue(int src, int type, Message* data) {
    boost::recursive_mutex::scoped_lock sl(q_lock[type]);
    CHECK_LT(src, kMaxHosts);
    Queue& q = incoming[type][src];
    if (!q.empty()) {
      data->ParseFromString(q.front());
      q.pop_front();
      return true;
    }
    return false;
  }

  // Blocking read for the given source and message type.
  void Read(int src, int type, Message* data) {
    while (!TryRead(src, type, data)) {
      Sleep(FLAGS_sleep_time);
    }
  }

  bool TryRead(int src, int type, Message* data, int *source=NULL) {
    if (src == MPI::ANY_SOURCE) {
      for (int i = 0; i < world_->Get_size(); ++i) {
        if (TryRead(i, type, data, source)) {
          return true;
        }
      }
    } else {
      if (check_queue(src, type, data)) {
        if (source) { *source = src; }
        return true;
      }
    }

    return false;
  }

  // Enqueue the given request for transmission.
  void Send(SendRequest *req) {
    boost::recursive_mutex::scoped_lock sl(send_lock);
//    LOG(INFO) << "Sending... " << MP(req->target, req->rpc_type);
    pending_sends_.push_back(req);
  }

  void Send(int dst, int method, const Message &msg) {
    SendRequest *r = new SendRequest();
    r->target = dst;
    r->rpc_type = method;
    msg.AppendToString(&r->payload);
    Send(r);
  }
};
NetworkThread *the_network;

Worker::Worker(const ConfigData &c) {
  epoch_ = 0;
  active_checkpoint_ = CP_NONE;

  config_.CopyFrom(c);
  config_.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

  world_ = MPI::COMM_WORLD;

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

  the_network = new NetworkThread(new RPCHelper(&world_));
}

int Worker::peer_for_shard(int table, int shard) const {
  return Registry::get_tables()[table]->get_owner(shard);
}

void Worker::Run() {
  kernel_thread_ = new boost::thread(&Worker::KernelLoop, this);
  table_thread_ = new boost::thread(&Worker::TableLoop, this);

  the_network->Run();

  table_thread_->join();
  kernel_thread_->join();
}

Worker::~Worker() {
  running_ = false;

  for (int i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}

void Worker::Send(int peer, int type, const Message& msg) {
  SendRequest *p = peers_[peer]->CreateRequest(type, msg);
  the_network->Send(p);
  stats_.set_bytes_out(stats_.bytes_out() + p->payload.size());
  stats_.set_put_out(stats_.put_out() + 1);
}

void Worker::Read(int peer, int type, Message* msg) {
  the_network->Read(peer + 1, type, msg);
}

void Worker::TableLoop() {
  while (running_) {
    HandleGetRequests();
    Sleep(FLAGS_sleep_time);
  }
}

void Worker::KernelLoop() {
  MPI::Intracomm world = MPI::COMM_WORLD;

  LOG(INFO) << "Worker " << config_.worker_id() << " registering...";
  RegisterWorkerRequest req;
  req.set_id(id());
  req.set_slots(config_.slots());
  the_network->Send(0, MTYPE_REGISTER_WORKER, req);
 
  double idle_start = Now();
  while (running_) {
    if (kernel_queue_.empty()) {
      CheckNetwork();
      Sleep(FLAGS_sleep_time);
      continue;
    }

    stats_.set_idle_time(stats_.idle_time() + Now() - idle_start);

    KernelRequest k = kernel_queue_.front();
    kernel_queue_.pop_front();

    VLOG(1) << "Received run request for " << k;

    if (peer_for_shard(k.table(), k.shard()) != config_.worker_id()) {
      LOG(FATAL) << "Received a shard I can't work on! : " << k.shard()
                 << " : " << peer_for_shard(k.table(), k.shard());
    }

    KernelInfo *helper = Registry::get_kernel(k.kernel());
    KernelId id(k.kernel(), k.table(), k.shard());
    DSMKernel* d = kernels_[id];

    if (!d) {
      d = helper->create();
      kernels_[id] = d;
      d->initialize_internal(this, k.table(), k.shard());
      d->InitKernel();
    }

    if (MPI::COMM_WORLD.Get_rank() == 1 && FLAGS_sleep_hack > 0) {
      Sleep(FLAGS_sleep_hack);
    }

    helper->Run(d, k.method());

    // Flush any table updates leftover.
    for (Registry::TableMap::iterator i = Registry::get_tables().begin();
         i != Registry::get_tables().end(); ++i) {
      i->second->SendUpdates();
    }

    idle_start = Now();
    while (the_network->pending_bytes()) {
      CheckNetwork();
      Sleep(FLAGS_sleep_time);
    }
    stats_.set_network_time(stats_.network_time() + Now() - idle_start);
    idle_start = Now();

    KernelDone kd;
    kd.mutable_kernel()->CopyFrom(k);
    Registry::TableMap &tmap = Registry::get_tables();
    for (Registry::TableMap::iterator i = tmap.begin(); i != tmap.end(); ++i) {
      GlobalTable* t = i->second;
      for (int j = 0; j < t->num_shards(); ++j) {
        if (t->is_local_shard(j)) {
          ShardInfo *si = kd.add_shards();
          si->set_entries(t->get_partition(j)->size());
          si->set_owner(this->id());
          si->set_table(i->first);
          si->set_shard(j);
        }
      }
    }

    kernel_done_.push_back(kd);

    VLOG(1) << "Kernel finished: " << k;
    DumpProfile();
  }
}

void Worker::CheckNetwork() {
  CheckForMasterUpdates();
  HandlePutRequests();
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
  return the_network->pending_bytes() == 0;
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
    GlobalTable* table = t[to_checkpoint[i]];
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
      the_network->Send(peers_[i]->CreateRequest(MTYPE_PUT_REQUEST, epoch_marker));
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
     GlobalTable* t = i->second;
     t->finish_checkpoint();
  }

  EmptyMessage req;
  the_network->Send(config_.master_id(), MTYPE_CHECKPOINT_DONE, req);
}

void Worker::Restore(int epoch) {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  LOG(INFO) << "Worker restoring state from epoch: " << epoch;
  epoch_ = epoch;

  Registry::TableMap &t = Registry::get_tables();
  for (Registry::TableMap::iterator i = t.begin(); i != t.end(); ++i) {
    GlobalTable* t = i->second;
    t->restore(StringPrintf("%s/epoch_%05d/checkpoint.table_%d",
                            FLAGS_checkpoint_read_dir.c_str(), epoch_, i->first));
  }

  EmptyMessage req;
  the_network->Send(config_.master_id(), MTYPE_RESTORE_DONE, req);
}

void Worker::HandlePutRequests() {
  HashPut put;
  while (the_network->TryRead(MPI::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
    if (put.marker() != -1) {
      UpdateEpoch(put.source(), put.marker());
      continue;
    }

    stats_.set_put_in(stats_.put_in() + 1);
    stats_.set_bytes_in(stats_.bytes_in() + put.ByteSize());

    GlobalTable *t = Registry::get_table(put.table());
    boost::recursive_mutex::scoped_lock sl(t->mutex());
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
  while (the_network->TryRead(MPI::ANY_SOURCE, MTYPE_GET_REQUEST, &get_req, &source)) {
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
      GlobalTable* t = Registry::get_table(get_req.table());
      boost::recursive_mutex::scoped_lock sl(t->mutex());

      t->handle_get(get_req.key(), &get_resp);
    }

    SendRequest *r = peers_[source - 1]->CreateRequest(MTYPE_GET_RESPONSE, get_resp);
    the_network->Send(r);
    VLOG(2) << "Returning result for " << MP(get_req.table(), get_req.shard());
  }
}

void Worker::CheckForMasterUpdates() {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  // Check for shutdown.
  EmptyMessage msg;
  KernelRequest k;

  if (the_network->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &msg)) {
    VLOG(1) << "Shutting down worker " << config_.worker_id();
    the_network->running = false;
    running_ = false;
    return;
  }

  CheckpointRequest checkpoint_msg;
  while (the_network->TryRead(config_.master_id(), MTYPE_START_CHECKPOINT, &checkpoint_msg)) {
    vector<int> tablev;
    for (int i = 0; i < checkpoint_msg.table_size(); ++i) {
      tablev.push_back(checkpoint_msg.table(i));
    }

    StartCheckpoint(checkpoint_msg.epoch(),
                    (CheckpointType)checkpoint_msg.checkpoint_type(),
                    tablev);
  }
  
  while (the_network->TryRead(config_.master_id(), MTYPE_FINISH_CHECKPOINT, &msg)) {
    FinishCheckpoint();
  }

  StartRestore restore_msg;
  while (the_network->TryRead(config_.master_id(), MTYPE_RESTORE, &restore_msg)) {
    Restore(restore_msg.epoch());
  }

  ShardAssignmentRequest shard_req;
  set<GlobalTable*> dirty_tables;
  while (the_network->TryRead(config_.master_id(), MTYPE_SHARD_ASSIGNMENT, &shard_req)) {
    for (int i = 0; i < shard_req.assign_size(); ++i) {
      const ShardAssignment &a = shard_req.assign(i);
      GlobalTable *t = Registry::get_table(a.table());
      int old_owner = t->get_owner(a.shard());
      t->set_owner(a.shard(), a.new_worker());
      VLOG(1) << "Setting owner: " << MP(a.shard(), a.new_worker());

      if (a.new_worker() == id() && old_owner != id()) {
        VLOG(1)  << "Setting self as owner of " << MP(a.table(), a.shard());

        // Don't consider ourselves canonical for this shard until we receive updates
        // from the old owner.
        if (old_owner != -1) {
          VLOG(1) << "Setting " << MP(a.table(), a.shard()) << " as tainted.  Old owner was: " << old_owner;
          t->set_tainted(a.shard());
        }
      } else if (old_owner == id() && a.new_worker() != id()) {
        VLOG(1) << "Lost ownership of " << MP(a.table(), a.shard()) << " to " << a.new_worker();
        // A new worker has taken ownership of this shard.  Flush our data out.
        t->set_dirty(a.shard());
        dirty_tables.insert(t);
      }
    }

    // Flush any tables we no longer own.
    for (set<GlobalTable*>::iterator i = dirty_tables.begin(); i != dirty_tables.end(); ++i) {
      (*i)->SendUpdates();
    }
  }

  // Check for new kernels to run, and report finished kernels to the master.
  while (the_network->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &k)) {
    kernel_queue_.push_back(k);
  }

  if (network_idle()) {
    while (!kernel_done_.empty()) {
      the_network->Send(config_.master_id(), MTYPE_KERNEL_DONE, kernel_done_.front());
      kernel_done_.pop_front();
    }
  }
}

} // end namespace
