#include "util/common.h"

#include "worker/worker.h"
#include "worker/kernel.h"

#include <boost/bind.hpp>
#include <signal.h>
#include <google/profiler.h>

namespace upc {

static Pool<HashRequest> request_pool_;
static Pool<HashUpdate> update_pool_;

struct Worker::Peer {
  // An update request containing changes to apply to a remote table.
  struct Request {
    int target;
    int rpc_type;
    string payload;
    MPI::Request mpiReq;
    double startTime;

    Request() {
      startTime = Now();
    }

    void Send(MPI::Comm* world) {
      mpiReq = world->Isend(&payload[0], payload.size(), MPI::BYTE, target, rpc_type);
    }

    double timed_out() {
      return Now() - startTime > kNetworkTimeout;
    }
  };

  list<Request*> outgoingRequests;
  HashUpdate writeScratch;
  HashUpdate dataScratch;
  HashRequest reqScratch;

  // Incoming data from this peer that we have read from the network, but has yet to be
  // processed by the kernel.
  deque<HashUpdate*> incomingData;
  deque<HashRequest*> incomingRequests;

  int32_t id;
  Peer(int id) : id(id) {
  }

  void CollectPendingSends() {
    for (list<Request*>::iterator i = outgoingRequests.begin(); i
        != outgoingRequests.end(); ++i) {
      Request *r = (*i);
      if (r->mpiReq.Test() || r->timed_out()) {
        if (r->timed_out()) {
          LOG(INFO) << "Time out sending " << r;
        }
        delete r;
        i = outgoingRequests.erase(i);
      }
    }
  }

  void ReceiveIncomingData(RPCHelper *rpc) {
    do {
      HashUpdate *req = update_pool_.get();
      if (!rpc->TryRead(id, MTYPE_KERNEL_DATA, req)) {
        update_pool_.free(req);
        break;
      }

      VLOG(1) << "Read put request....";
      incomingData.push_back(req);
    } while(1);

    do {
      HashRequest *req = request_pool_.get();
      if (!rpc->TryRead(id, MTYPE_GET_REQUEST, req)) {
        request_pool_.free(req);
        break;
      }
      VLOG(1) << "Read get request....";
      incomingRequests.push_back(req);
    } while(1);
  }

  int64_t write_bytes_pending() const {
    int64_t t = 0;
    for (list<Request*>::const_iterator i = outgoingRequests.begin(); i != outgoingRequests.end(); ++i) {
      t += (*i)->payload.size();
    }
    return t;
  }

  Request* create_data_request(int rpc_type, RPCMessage* ureq) {
    Request* r = new Request();
    r->target = id;
    r->rpc_type = rpc_type;
    ureq->AppendToString(&r->payload);
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

  num_peers_ = config.num_workers();
  peers.resize(num_peers_);
  for (int i = 0; i < num_peers_; ++i) {
    peers[i] = new Peer(i);
  }

  running = true;

  world_ = MPI::COMM_WORLD;

  // Obtain a communicator that only includes workers.  This allows us to specify shards
  // conveniently (shard 0 == worker_comm.rank0), and express barriers on only workers.
  worker_comm_ = world_.Split(WORKER_COLOR, world_.Get_rank());

  kernel_thread_ = network_thread_ = NULL;
}

void Worker::Run() {
  kernel_thread_ = new boost::thread(boost::bind(&Worker::KernelLoop, this));
//  network_thread_ = new boost::thread(boost::bind(&Worker::NetworkLoop, this));
//  network_thread_->join();

  NetworkLoop();
  kernel_thread_->join();
}

Worker::~Worker() {
  running = false;
  delete kernel_thread_;
  delete network_thread_;

  for (int i = 0; i < pending_writes_.size(); ++i) {delete pending_writes_[i];}
  for (int i = 0; i < peers.size(); ++i) {
    delete peers[i];
  }
}


void Worker::NetworkLoop() {
  deque<Table*> work;
  while (running) {
    PERIODIC(10, {
        VLOG(1) << "Network loop working - " << pending_kernel_bytes() << " bytes in the processing queue.";
    });

    SendAndReceive();

    for (int i = 0; i < tables.size(); ++i) {
      tables[i]->GetPendingUpdates(&work);
    }

    if (work.empty()) {
      continue;
    }

    Table* old = work.front();
    work.pop_front();

    VLOG(2) << "Accum " << old->info().owner_thread << " : " << old->size();
    Peer * p = peers[old->info().owner_thread];

    Table::Iterator *i = old->get_iterator();
    while (!i->done()) {
      if (pending_network_bytes() < config.network_buffer()) {
        ComputeUpdates(p, i);
      }

      SendAndReceive();
    }

    delete i;
    delete old;
  }
}

void Worker::KernelLoop() {
  MPI::Intracomm world = MPI::COMM_WORLD;
  RunKernelRequest kernelRequest;
  RPCHelper rpc(&world);
	while (running) {
	  MPI::Status probe_result;

	  if (!world_.Iprobe(config.master_id(), MPI_ANY_TAG, probe_result)) {
	    Sleep(0.001);
	    continue;
	  }

	  if (probe_result.Get_tag() == MTYPE_WORKER_SHUTDOWN) {
	    VLOG(1) << "Shutting down worker " << config.worker_id();
	    running = false;
	    return;
	  }

	  ProtoWrapper wrapper(kernelRequest);
		rpc.Read(MPI_ANY_SOURCE, MTYPE_RUN_KERNEL, &wrapper);

		VLOG(1) << "Received run request for kernel id: " << kernelRequest.kernel_id();
		KernelFunction f = KernelRegistry::get_kernel(kernelRequest.kernel_id());
		f();
		VLOG(1) << "Waiting for network to finish: " << pending_network_bytes() + pending_kernel_bytes();

	  while (pending_kernel_bytes() + pending_network_bytes() > 0) {
	    Sleep(0.001);
	  }

    // Send a message to master indicating that we've completed our kernel
     // and we are done transmitting.
	  EmptyMessage m;
	  rpc.Send(config.master_id(), MTYPE_KERNEL_DONE, ProtoWrapper(m));

	  VLOG(1) << "Kernel done.";
    ProfilerFlush();
	}
}

int64_t Worker::pending_network_bytes() const {
  int64_t t = 0;
  for (int i = 0; i < peers.size(); ++i) {
    t += peers[i]->write_bytes_pending();
  }
  return t;
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  for (int i = 0; i < tables.size(); ++i) {
    t += tables[i]->pending_write_bytes();
  }

  return t;
}

void Worker::ComputeUpdates(Peer *p, Table::Iterator *it) {
  HashUpdate *r = &p->writeScratch;
  r->Clear();

  r->set_source(config.worker_id());
  r->set_table_id(it->owner()->info().table_id);

  int bytesUsed = 0;
  int count = 0;
  for (; !it->done() && bytesUsed < kNetworkChunkSize; it->Next()) {
    const string& k = it->key_str();
    const string& v = it->value_str();

//    LOG(INFO) << " k " << k.len << " v " << v.len;

    r->add_put(make_pair(k, v));
    ++count;

    bytesUsed += k.size() + v.size();
  }

  VLOG(2) << "Prepped " << count << " taking " << bytesUsed;

  Peer::Request *mpiReq = p->create_data_request(MTYPE_KERNEL_DATA, r);

  stats_.set_put_out(stats_.put_out() + 1);
  stats_.set_bytes_out(stats_.bytes_out() + r->ByteSize());

  mpiReq->Send(&worker_comm_);
  ++count;
}

void Worker::SendAndReceive() {
  RPCHelper rpc(&worker_comm_);

  HashUpdate scratch;

  for (int i = 0; i < peers.size(); ++i) {
    Peer *p = peers[i];
    p->CollectPendingSends();
    p->ReceiveIncomingData(&rpc);

    while (!p->incomingData.empty()) {
      HashUpdate *r = p->pop_data();
      stats_.set_put_in(stats_.put_in() + 1);
      stats_.set_bytes_in(stats_.bytes_in() + r->ByteSize());

      tables[r->table_id()]->ApplyUpdates(*r);
      update_pool_.free(r);
    }

    while (!p->incomingRequests.empty()) {
      HashRequest *r = p->pop_request();
      stats_.set_get_in(stats_.get_in() + 1);
      stats_.set_bytes_in(stats_.bytes_in() + r->ByteSize());

      VLOG(1) << "Returning result for " << r->key() << " :: table " << r->table_id();
      string v = tables[r->table_id()]->get_local(r->key());
      scratch.Clear();
      scratch.set_source(config.worker_id());
      scratch.set_table_id(r->table_id());
      scratch.add_put(std::make_pair(r->key(), v));
      p->create_data_request(MTYPE_GET_RESPONSE, &scratch)->Send(&worker_comm_);
      request_pool_.free(r);
    }
  }
}

} // end namespace
