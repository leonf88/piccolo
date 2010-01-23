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
    double startTime;

    Request() {
      startTime = Now();
    }

    ~Request() {}
  };

  HashUpdate writeScratch;
  list<Request*> outgoingRequests;

  // Incoming data from this peer that we have read from the network, but has yet to be
  // processed by the kernel.
  deque<HashUpdate*> incomingData;
  deque<HashRequest*> incomingRequests;

  mutable boost::recursive_mutex pending_lock_;

  int32_t id;
  Peer(int id) : id(id) {
  }

  void CollectPendingSends() {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);
    for (list<Request*>::iterator i = outgoingRequests.begin(); i != outgoingRequests.end(); ++i) {
      Request *r = (*i);
      if (r->mpi_req.Test()) {
        VLOG(2) << "Request of size " << r->payload.size() << " finished.";
        delete r;
        i = outgoingRequests.erase(i);
      }
    }
  }

  void ReceiveIncomingData(RPCHelper *rpc) {
    while (rpc->HasData(id, MTYPE_PUT_REQUEST)) {
      HashUpdate *req = new HashUpdate;
      rpc->Read(id, MTYPE_PUT_REQUEST, req);
      incomingData.push_back(req);
    }

    while (rpc->HasData(id, MTYPE_GET_REQUEST)) {
      HashRequest *req = new HashRequest;
      rpc->Read(id, MTYPE_GET_REQUEST, req);
      VLOG(1) << "Read get request....";
      incomingRequests.push_back(req);
    }
  }

  bool write_pending() const {
    return !outgoingRequests.empty();
  }

  // Send the given message type and data to this peer.
  Request* Send(int rpc_type, RPCMessage* ureq, RPCHelper *rpc) {
    boost::recursive_mutex::scoped_lock sl(pending_lock_);

    Request* r = new Request();
    r->target = id;
    r->rpc_type = rpc_type;
    ureq->AppendToString(&r->payload);
    ureq->ParseFromString(r->payload);

    r->mpi_req = rpc->SendData(r->target, r->rpc_type, r->payload);
    outgoingRequests.push_back(r);

    return r;
  }

  HashUpdate *pop_data() {
    HashUpdate *r = incomingData.front();
    incomingData.pop_front();
    return r;
  }

  HashRequest *pop_request() {
    HashRequest *r = incomingRequests.front();
    incomingRequests.pop_front();
    return r;
  }
};

Worker::Worker(const ConfigData &c) {
  config.CopyFrom(c);
  config.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

  num_peers_ = config.num_workers();
  peers_.resize(num_peers_);
  for (int i = 0; i < num_peers_; ++i) {
    peers_[i] = new Peer(i + 1);
  }

  running_ = true;

  world_ = MPI::COMM_WORLD;
  rpc_ = new RPCHelper(&world_);

  kernel_thread_ = network_thread_ = NULL;

  // HACKHACKHACK - register ourselves with any existing tables
  Registry::TableMap *t = Registry::get_tables();
  for (Registry::TableMap::iterator i = t->begin(); i != t->end(); ++i) {
    for (int j = 0; j < i->second->info().num_shards; ++j) {
      if (peer_for_shard(i->first, j) == config.worker_id()) {
        LOG(INFO) << "Worker " << config.worker_id() << " is local for " << j;
        i->second->set_local(j, true);
      } else {
        i->second->set_local(j, false);
      }
    }

    i->second->set_rpc_helper(rpc_);
  }

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

  for (int i = 0; i < pending_writes_.size(); ++i) {delete pending_writes_[i];}
  for (int i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}


void Worker::NetworkLoop() {
  deque<LocalTable*> work;
  while (running_) {
    PERIODIC(10, {
        VLOG(1) << "Network loop working - " << pending_kernel_bytes() << " bytes in the processing queue.";
    });

    Poll();

    if (work.empty()) {
      Registry::TableMap *t = Registry::get_tables();
      for (Registry::TableMap::iterator i = t->begin(); i != t->end(); ++i) {
        ((GlobalTable*)i->second)->GetPendingUpdates(&work);
      }
    }

    if (work.empty() && pending_network_writes() == 0) {
      Sleep(0.01);
    }

    while (!work.empty()) {
      LocalTable* t = work.front();
      work.pop_front();

      Peer * p = peers_[peer_for_shard(t->id(), t->shard())];

      Table::Iterator *i = t->get_iterator();
      while (!i->done()) {
        ComputeUpdates(p, i);
        Poll();
      }
      delete i;
      delete t;

      Poll();
    }
  }
}

void Worker::KernelLoop() {
  MPI::Intracomm world = MPI::COMM_WORLD;

	while (running_) {
	  if (kernel_requests_.empty()) {
	    Sleep(0.01);
	    continue;
	  }

	  KernelRequest k;
	  {
	    boost::recursive_mutex::scoped_lock l(kernel_lock_);
	    k = kernel_requests_.front();
	    kernel_requests_.pop_front();
	  }

		VLOG(1) << "Received run request for kernel id: " << k.kernel() << ":" << k.method() << ":" << k.shard();

		if (peer_for_shard(k.table(), k.shard()) != config.worker_id()) {
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

		VLOG(1) << "Waiting for network to finish: " << pending_network_writes() << " : " << pending_kernel_bytes();

		// Wait for the network thread to catch up.
	  while (pending_kernel_bytes() > 0 || pending_network_writes()) {
	    Sleep(0.01);
	  }

	  kernel_done_.push_back(k);

	  VLOG(1) << "Kernel done.";
#ifdef CPUPROF
    ProfilerFlush();
#endif
	}
}

bool Worker::pending_network_writes() const {
  for (int i = 0; i < peers_.size(); ++i) {
    if (peers_[i]->write_pending()) {
      return true;
    }
  }
  return false;
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  Registry::TableMap *tmap = Registry::get_tables();
  for (Registry::TableMap::iterator i = tmap->begin(); i != tmap->end(); ++i) {
    t += ((GlobalTable*)i->second)->pending_write_bytes();
  }

  return t;
}

void Worker::ComputeUpdates(Peer *p, Table::Iterator *it) {
  HashUpdate *r = &p->writeScratch;
  r->Clear();

  r->set_shard(it->owner()->shard());
  r->set_source(config.worker_id());
  r->set_table_id(it->owner()->info().table_id);

  int bytesUsed = 0;
  int count = 0;
  string k, v;
  for (; !it->done() && bytesUsed < kMaxNetworkChunk; it->Next()) {
    it->key_str(&k);
    it->value_str(&v);

    r->add_put(k, v);
    ++count;
  }

  VLOG(1) << "Prepped " << count << " taking " << r->ByteSize();

  p->Send(MTYPE_PUT_REQUEST, r, rpc_);

  stats_.set_put_out(stats_.put_out() + 1);
  stats_.set_bytes_out(stats_.bytes_out() + r->ByteSize());
  ++count;
}

void Worker::Poll() {
  HashUpdate scratch;

  for (int i = 0; i < peers_.size(); ++i) {
    Peer *p = peers_[i];
    p->CollectPendingSends();
    p->ReceiveIncomingData(rpc_);

    while (!p->incomingData.empty()) {
      HashUpdate *r = p->pop_data();
      stats_.set_put_in(stats_.put_in() + 1);
      stats_.set_bytes_in(stats_.bytes_in() + r->ByteSize());

      Table *t = Registry::get_table(r->table_id());
      t->ApplyUpdates(*r);
      delete r;
    }

    while (!p->incomingRequests.empty()) {
      HashRequest *r = p->pop_request();
      stats_.set_get_in(stats_.get_in() + 1);
      stats_.set_bytes_in(stats_.bytes_in() + r->ByteSize());

      scratch.Clear();
      scratch.set_source(config.worker_id());
      scratch.set_table_id(r->table_id());

      VLOG(1) << "Returning result for " << r->key() << " :: table " << r->table_id();
      string v = Registry::get_table(r->table_id())->get_local(r->key());

      scratch.add_put(r->key(), v);

      p->Send(MTYPE_GET_RESPONSE, &scratch, rpc_);
      delete r;
    }
  }

  // Check for shutdown.
  {
    EmptyMessage msg;
    ProtoWrapper wrapper(msg);

    if (rpc_->TryRead(config.master_id(), MTYPE_WORKER_SHUTDOWN, &wrapper)) {
      VLOG(1) << "Shutting down worker " << config.worker_id();
      running_ = false;
      return;
    }
  }

  boost::recursive_mutex::scoped_lock sl(kernel_lock_);

  // Check for new kernels to run, and report finished kernels to the master.
  {
    KernelRequest k;
    ProtoWrapper wrapper(k);
    if (rpc_->TryRead(config.master_id(), MTYPE_RUN_KERNEL, &wrapper)) {
      kernel_requests_.push_back(k);
    }
  }

  while (!kernel_done_.empty()) {
    rpc_->Send(config.master_id(), MTYPE_KERNEL_DONE, ProtoWrapper(kernel_done_.front()));
    kernel_done_.pop_front();
  }
}

} // end namespace
