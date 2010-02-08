#include <boost/bind.hpp>
#include <signal.h>
#ifdef CPUPROF
#include <google/profiler.h>
#endif

#include "util/common.h"
#include "worker/worker.h"
#include "kernel/kernel-registry.h"
#include "kernel/table-registry.h"

DEFINE_double(sleep_hack, 0.0, "");

namespace dsm {
static const int kNetworkTimeout = 100.0;

struct Worker::Peer {
  // An update request containing changes to apply to a remote table.
  struct Request {
    int target;
    int rpc_type;
    string payload;
    MPI::Request mpi_req;
    double start_time;

    Request() : start_time(Now()) {}
    ~Request() {}

    double elapsed() { return Now() - start_time; }
    double timed_out() { return elapsed() > kNetworkTimeout; }
  };

  unordered_set<Request*> outgoing_requests_;

  int32_t id;
  RPCHelper *helper;
  int64_t pending_out_;

  Peer(int id, RPCHelper* rpc) : id(id), helper(rpc), pending_out_(0) {}

  bool TryRead(int method, Message* msg) {
    return helper->TryRead(id, method, msg);
  }

  // Send the given message type and data to this peer.
  Request* Send(int method, const Message& ureq) {
    Request* r = new Request();
    r->target = id;
    r->rpc_type = method;
    ureq.AppendToString(&r->payload);

    r->mpi_req = helper->SendData(r->target, r->rpc_type, r->payload);
    outgoing_requests_.insert(r);
    pending_out_ += r->payload.size();

    return r;
  }

  void CollectPendingSends() {
    unordered_set<Request*>::iterator i = outgoing_requests_.begin();
    while (i != outgoing_requests_.end()) {
      Request *r = (*i);
      if (r->mpi_req.Test() || r->timed_out()) {
        VLOG(2) << "Finished request to " << id << " of size " << r->payload.size();

        if (r->timed_out()) {
          LOG_EVERY_N(INFO, 100) << "Send of " << r->payload.size()
                                 << " to " << r->target << " timed out.";
          r->mpi_req.Cancel();
        }

        pending_out_ -= r->payload.size();
        delete r;
        i = outgoing_requests_.erase(i);
      } else {
        ++i;
      }
    }
  }

  int64_t pending_out_bytes() const { return pending_out_; }
};

Worker::Worker(const ConfigData &c) {
  config_.CopyFrom(c);
  config_.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

  world_ = MPI::COMM_WORLD;
  rpc_ = new RPCHelper(&world_);

  num_peers_ = config_.num_workers();
  peers_.resize(num_peers_);
  for (int i = 0; i < num_peers_; ++i) {
    peers_[i] = new Peer(i + 1, rpc_);
  }

  running_ = true;

  // HACKHACKHACK - register ourselves with any existing tables
  Registry::TableMap &t = Registry::get_tables();
  for (Registry::TableMap::iterator i = t.begin(); i != t.end(); ++i) {
    i->second->info_.worker = this;
  }

  LOG(INFO) << "Worker " << config_.worker_id() << " started.";

  RegisterWorkerRequest req;
  req.set_id(id());
  req.set_slots(config_.slots());
  rpc_->Send(0, MTYPE_REGISTER_WORKER, req);
}

int Worker::peer_for_shard(int table, int shard) const {
  return Registry::get_tables()[table]->get_owner(shard);
}

void Worker::Run() {
  KernelLoop();
}

Worker::~Worker() {
  running_ = false;

  for (int i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}

void Worker::Send(int peer, int type, const Message& msg) {
  peers_[peer]->Send(type, msg);
}

void Worker::Read(int peer, int type, Message* msg) {
  while (!peers_[peer]->TryRead(type, msg)) {
    PollWorkers();
  }
}

void Worker::KernelLoop() {
  MPI::Intracomm world = MPI::COMM_WORLD;

  while (running_) {
    if (kernel_queue_.empty()) {
      PERIODIC(0.1,  { PollMaster(); } );
      PollWorkers();
      continue;
    }

    KernelRequest k = kernel_queue_.front();
    kernel_queue_.pop_front();

    VLOG(1) << "Received run request for " << k;

    if (peer_for_shard(k.table(), k.shard()) != config_.worker_id()) {
      LOG(FATAL) << "Received a shard I can't work on! : " << k.shard() << " : " << peer_for_shard(k.table(), k.shard());
    }

    GlobalTable *t = Registry::get_table(k.table());
    // We received a run request for a shard we own, but we haven't yet received
    // all the data for it.  Continue reading from other workers until we do.
    while (t->tainted(k.shard())) {
      PollWorkers();
      PollMaster();
    }

    KernelInfo *helper = Registry::get_kernel(k.kernel());

    KernelId id(k.kernel(), k.table(), k.shard());
    DSMKernel* d = kernels_[id];

    if (!d) {
      d = helper->create();
      kernels_[id] = d;
      d->Init(this, k.table(), k.shard());
      d->KernelInit();
    }

    if (MPI::COMM_WORLD.Get_rank() == 1 && FLAGS_sleep_hack > 0) {
      Sleep(FLAGS_sleep_hack);
    }

    helper->Run(d, k.method());
    kernel_done_.push_back(k);

    // Flush any table updates leftover.
    for (Registry::TableMap::iterator i = Registry::get_tables().begin();
         i != Registry::get_tables().end(); ++i) {
      i->second->SendUpdates();
    }

    VLOG(1) << "Kernel finished: " << k;
#ifdef CPUPROF
    ProfilerFlush();
#endif
  }
}

int64_t Worker::pending_network_bytes() const {
  int64_t t = 0;

  for (int i = 0; i < peers_.size(); ++i) {
    t += peers_[i]->pending_out_bytes();
  }

  return t;
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  Registry::TableMap &tmap = Registry::get_tables();
  for (Registry::TableMap::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    t += ((GlobalTable*)i->second)->pending_write_bytes();
  }

  return t;
}

bool Worker::network_idle() const {
  return pending_network_bytes() == 0;
}

void Worker::PollWorkers() {
  PERIODIC(10,
           LOG(INFO) << "Pending network: " << pending_network_bytes() << " rss: " << get_memory_rss());

  for (int i = 0; i < peers_.size(); ++i) {
    Peer *p = peers_[i];
    p->CollectPendingSends();
  }

  HashUpdate put;
  while (rpc_->TryRead(MPI::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
    stats_.set_put_in(stats_.put_in() + 1);
    stats_.set_bytes_in(stats_.bytes_in() + put.ByteSize());

    GlobalTable *t = Registry::get_table(put.table());
    t->ApplyUpdates(put);

    if (put.done() && t->tainted(put.shard())) {
      VLOG(1) << "Clearing taint on: " << MP(put.table(), put.shard());
      t->clear_tainted(put.shard());
    }
  }

  MPI::Status status;
  HashRequest get_req;
  HashUpdate get_resp;
  while (rpc_->HasData(MPI::ANY_SOURCE, MTYPE_GET_REQUEST, status)) {
    rpc_->Read(MPI::ANY_SOURCE, MTYPE_GET_REQUEST, &get_req);

    stats_.set_get_in(stats_.get_in() + 1);
    stats_.set_bytes_in(stats_.bytes_in() + get_req.ByteSize());
    VLOG(2) << "Returning result for " << get_req.key() << " :: table " << get_req.table();


    get_resp.Clear();
    get_resp.set_source(config_.worker_id());
    get_resp.set_table(get_req.table());
    get_resp.set_shard(-1);
    get_resp.set_done(true);
    get_resp.add_key(get_req.key());
    string *v = get_resp.add_value();
    Registry::get_table(get_req.table())->get_local(get_req.key(), v);

    peers_[status.Get_source() - 1]->Send(MTYPE_GET_RESPONSE, get_resp);
  }
}

void Worker::PollMaster() {
  // Check for shutdown.
  EmptyMessage msg;
  KernelRequest k;

  if (rpc_->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &msg)) {
    VLOG(1) << "Shutting down worker " << config_.worker_id();
    running_ = false;
    return;
  }

  ShardAssignmentRequest shard_req;
  set<GlobalTable*> dirty_tables;
  while (rpc_->TryRead(config_.master_id(), MTYPE_SHARD_ASSIGNMENT, &shard_req)) {

    for (int i = 0; i < shard_req.assign_size(); ++i) {
      const ShardAssignment &a = shard_req.assign(i);
      GlobalTable *t = Registry::get_table(a.table());
      int old_owner = t->get_owner(a.shard());
      t->set_owner(a.shard(), a.new_worker());

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

    world_.Barrier();

    // Flush any tables we no longer own.
    for (set<GlobalTable*>::iterator i = dirty_tables.begin(); i != dirty_tables.end(); ++i) {
      (*i)->SendUpdates();
    }
  }

  // Check for kernels to avoid running.
  while (rpc_->TryRead(config_.master_id(), MTYPE_STOP_KERNEL, &k)) {
    LOG(INFO) << "Got stop request for: " << k;
    for (int i = 0; i < kernel_queue_.size(); ++i) {
      const KernelRequest &c = kernel_queue_[i];
      if (c.table() == k.table() && c.shard() == k.shard()) {
        LOG(INFO) << "dropping: " << k << " from queue.";
        kernel_queue_.erase(kernel_queue_.begin() + i);
        break;
      }
    }
  }

  // Check for new kernels to run, and report finished kernels to the master.
  if (network_idle()) {
    while (rpc_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &k)) {
      kernel_queue_.push_back(k);
    }

    while (!kernel_done_.empty()) {
      rpc_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kernel_done_.front());
      kernel_done_.pop_front();
    }
  }
}

} // end namespace
