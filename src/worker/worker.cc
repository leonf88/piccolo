#include <boost/bind.hpp>
#include <signal.h>
#ifdef CPUPROF
#include <google/profiler.h>
#endif

#include "util/common.h"
#include "worker/worker.h"
#include "worker/registry.h"

namespace dsm {
static const int kMaxNetworkChunk = 1 << 10;
static const int kNetworkTimeout = 2.0;

struct Worker::Peer {

  // An update request containing changes to apply to a remote table.
  struct Request {
    int target;
    int rpc_type;
    string payload;
    MPI::Request mpi_req;
    double start_time;

    Request() {
      start_time = Now();
    }

    ~Request() {}
  };

  HashUpdate write_scratch;
  list<Request*> outgoing_requests_;

  // Incoming data from this peer that we have read from the network, but has yet to be
  // processed by the kernel.
  deque<HashUpdate*> incoming_data_;
  deque<HashRequest*> incoming_requests_;

  mutable boost::recursive_mutex pending_lock_;

  int32_t id;
  RPCHelper *helper;

  int64_t pending_out_;


  Peer(int id, RPCHelper* rpc) : id(id), helper(rpc), pending_out_(0) {}

  void CollectPendingSends() {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);
    for (list<Request*>::iterator i = outgoing_requests_.begin(); i != outgoing_requests_.end(); ++i) {
      Request *r = (*i);
      if (r->mpi_req.Test()) {
        VLOG(2) << "Request of size " << r->payload.size() << " finished.";
        pending_out_ -= r->payload.size();
        delete r;
        i = outgoing_requests_.erase(i);
      } else if (Now() - r->start_time > kNetworkTimeout) {
         LOG_EVERY_N(INFO, 100) << "Send of " << r->payload.size() << " to " << r->target << " timed out.";
         pending_out_ -= r->payload.size();
         r->mpi_req.Cancel();
         delete r;
         i = outgoing_requests_.erase(i);
      }
    }
  }

  void ReceiveIncomingData() {
    while (helper->HasData(id, MTYPE_PUT_REQUEST)) {
      HashUpdate *req = new HashUpdate;
      if (helper->Read(id, MTYPE_PUT_REQUEST, req) != -1) {
        incoming_data_.push_back(req);
      }
    }

    while (helper->HasData(id, MTYPE_GET_REQUEST)) {
      HashRequest *req = new HashRequest;
      if (helper->Read(id, MTYPE_GET_REQUEST, req) != -1) {
        VLOG(1) << "Read get request....";
        incoming_requests_.push_back(req);
      }
    }
  }

  int64_t pending_out_bytes() const {
    return pending_out_;
  }

  // Send the given message type and data to this peer.
  Request* Send(int rpc_type, RPCMessage* ureq) {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);

    Request* r = new Request();
    r->target = id;
    r->rpc_type = rpc_type;
    ureq->AppendToString(&r->payload);
//    ureq->ParseFromString(r->payload);

    r->mpi_req = helper->SendData(r->target, r->rpc_type, r->payload);
    outgoing_requests_.push_back(r);
    pending_out_ += r->payload.size();

    return r;
  }

  HashUpdate *pop_data() {
    HashUpdate *r = incoming_data_.front();
    incoming_data_.pop_front();
    return r;
  }

  HashRequest *pop_request() {
    HashRequest *r = incoming_requests_.front();
    incoming_requests_.pop_front();
    return r;
  }
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

  kernel_thread_ = network_thread_ = NULL;

  // HACKHACKHACK - register ourselves with any existing tables
  Registry::TableMap *t = Registry::get_tables();
  string local_tables;
  for (Registry::TableMap::iterator i = t->begin(); i != t->end(); ++i) {
    for (int j = 0; j < i->second->info().num_shards; ++j) {
      if (peer_for_shard(i->first, j) == config_.worker_id()) {
        i->second->set_local(j, true);
        local_tables += StringPrintf("%d,", j);
      } else {
        i->second->set_local(j, false);
      }
    }

    i->second->set_rpc_helper(rpc_);
  }

  LOG(INFO) << "Worker " << config_.worker_id() << " is local for " << local_tables;
}

void Worker::Run() {
  kernel_thread_ = new boost::thread(boost::bind(&Worker::KernelLoop, this));

  NetworkLoop();
  kernel_thread_->join();
}

Worker::~Worker() {
  running_ = false;
  delete kernel_thread_;
  delete network_thread_;

  for (int i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}


void Worker::NetworkLoop() {
  while (running_) {
    PERIODIC(10, {
        VLOG(1) << StringPrintf("Worker %d; K: %ld; N: %ld",
                                config_.worker_id(), pending_kernel_bytes(), pending_network_bytes());
    });

    PollMaster();
    PollPeers();

    if (pending_sends_.empty()) {
      Registry::TableMap *t = Registry::get_tables();
      for (Registry::TableMap::iterator i = t->begin(); i != t->end(); ++i) {
        ((GlobalTable*)i->second)->GetPendingUpdates(&pending_sends_);
      }
    }

    if (pending_sends_.empty() && pending_network_bytes() == 0) {
      Sleep(0.01);
      continue;
    }

    while (!pending_sends_.empty()) {
      LocalTable* t = pending_sends_.front();
      pending_sends_.pop_front();

      Peer *p = peers_[peer_for_shard(t->id(), t->shard())];

      Table::Iterator *i = t->get_iterator();
      while (!i->done()) {
        ComputeUpdates(p, i);
      }

      delete i;
      delete t;

      PollPeers();
    }
  }
}

void Worker::KernelLoop() {
  MPI::Intracomm world = MPI::COMM_WORLD;

  while (running_) {
    if (kernel_queue_.empty()) {
      Sleep(0.01);
      continue;
    }

    KernelRequest k;
    {
      boost::recursive_mutex::scoped_lock l(kernel_lock_);
      k = kernel_queue_.front();
      kernel_queue_.pop_front();
    }

    CHECK_EQ(pending_network_bytes(), 0);
    CHECK_EQ(pending_kernel_bytes(), 0);
    CHECK(pending_sends_.empty());

    VLOG(1) << "Received run request for kernel id: " << k.kernel() << ":" << k.method() << ":" << k.shard();

    if (peer_for_shard(k.table(), k.shard()) != config_.worker_id()) {
      LOG(FATAL) << "Received a shard I can't work on!";
    }

    KernelInfo *helper = Registry::get_kernel_info(k.kernel());

    KernelId id(k.kernel(), k.table(), k.shard());
    DSMKernel* d = kernels_[id];

    if (!d) {
      d = helper->create();
      kernels_[id] = d;
      d->Init(this, k.table(), k.shard());
      d->KernelInit();
    }

    helper->invoke_method(d, k.method());

    while (!network_idle()) {
      PERIODIC(5, { LOG(INFO) << "Waiting for network " << pending_network_bytes() << " : " 
                                                        << pending_kernel_bytes(); } );
      Sleep(0.1);
    }

      kernel_done_.push_back(k);

    VLOG(1) << "Kernel done.";
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

  Registry::TableMap *tmap = Registry::get_tables();
  for (Registry::TableMap::iterator i = tmap->begin(); i != tmap->end(); ++i) {
    t += ((GlobalTable*)i->second)->pending_write_bytes();
  }

  return t;
}

bool Worker::network_idle() const {
  return pending_network_bytes() == 0 && pending_sends_.empty() && pending_kernel_bytes() == 0;
}

void Worker::ComputeUpdates(Peer *p, Table::Iterator *it) {
  HashUpdate *r = &p->write_scratch;
  r->Clear();

  r->set_shard(it->owner()->shard());
  r->set_source(config_.worker_id());
  r->set_table_id(it->owner()->info().table_id);

  int bytesUsed = 0;
  int count = 0;
  string k, v;
  for (; !it->done() && bytesUsed < kMaxNetworkChunk; it->Next()) {
    it->key_str(&k);
    it->value_str(&v);

    r->add_put(k, v);
    ++count;
    bytesUsed += k.size() + v.size();
  }

  VLOG(2) << "Prepped " << count << " taking " << bytesUsed;

  p->Send(MTYPE_PUT_REQUEST, r);

  stats_.set_put_out(stats_.put_out() + 1);
  stats_.set_bytes_out(stats_.bytes_out() + r->ByteSize());
  ++count;
}

void Worker::PollPeers() {
  HashUpdate scratch;

  for (int i = 0; i < peers_.size(); ++i) {
    Peer *p = peers_[i];
    p->CollectPendingSends();
    p->ReceiveIncomingData();

    while (!p->incoming_data_.empty()) {
      HashUpdate *r = p->pop_data();
      stats_.set_put_in(stats_.put_in() + 1);
      stats_.set_bytes_in(stats_.bytes_in() + r->ByteSize());

      Table *t = Registry::get_table(r->table_id());
      t->ApplyUpdates(*r);
      delete r;
    }

    while (!p->incoming_requests_.empty()) {
      HashRequest *r = p->pop_request();
      stats_.set_get_in(stats_.get_in() + 1);
      stats_.set_bytes_in(stats_.bytes_in() + r->ByteSize());

      scratch.Clear();
      scratch.set_source(config_.worker_id());
      scratch.set_table_id(r->table_id());

      VLOG(1) << "Returning result for " << r->key() << " :: table " << r->table_id();
      string v = Registry::get_table(r->table_id())->get_local(r->key());

      scratch.add_put(r->key(), v);

      p->Send(MTYPE_GET_RESPONSE, &scratch);
      delete r;
    }
  }
}

void Worker::PollMaster() {
  // Check for shutdown.
  {
    EmptyMessage msg;
    ProtoWrapper wrapper(msg);

    if (rpc_->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &wrapper)) {
      VLOG(1) << "Shutting down worker " << config_.worker_id();
      running_ = false;
      return;
    }
  }

  boost::recursive_mutex::scoped_lock sl(kernel_lock_);

  // Check for new kernels to run, and report finished kernels to the master.
  {
    KernelRequest k;
    ProtoWrapper wrapper(k);
    if (rpc_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &wrapper)) {
      kernel_queue_.push_back(k);
    }
  }

  if (network_idle()) {
    while (!kernel_done_.empty()) {
      rpc_->Send(config_.master_id(), MTYPE_KERNEL_DONE, ProtoWrapper(kernel_done_.front()));
      kernel_done_.pop_front();
    }
  }
}

} // end namespace
