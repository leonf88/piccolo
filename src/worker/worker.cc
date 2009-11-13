#include "util/common.h"
#include "util/fake-mpi.h"

#include "worker/worker.h"
#include "worker/kernel.h"

#include <boost/bind.hpp>
#include <signal.h>

namespace upc {

struct Worker::Peer {
  // An update request containing changes to apply to a remote table.
  struct Request {
    int target;
    string payload;
    MPI::Request mpiReq;
    double startTime;

    Request() {
      startTime = Now();
    }

    void Send(MPI::Comm* world) {
      mpiReq = world->Isend(&payload[0], payload.size(), MPI::BYTE, target,
                            MTYPE_KERNEL_DATA);
    }

    double timedOut() {
      return Now() - startTime > kNetworkTimeout;
    }
  };

  list<Request*> outgoingRequests;
  HashUpdateRequest writeScratch;
  HashUpdateRequest readScratch;
  HashUpdateRequest *accumulatedUpdates;

  // Incoming data from this peer that we have read from the network, but has yet to be
  // processed by the kernel.
  deque<HashUpdateRequest*> incomingRequests;

  int32_t id;
  Peer(int id) : id(id) {
    accumulatedUpdates = new HashUpdateRequest;
  }

  void CollectPendingSends() {
    for (list<Request*>::iterator i = outgoingRequests.begin(); i
        != outgoingRequests.end(); ++i) {
      Request *r = (*i);
      if (r->mpiReq.Test() || r->timedOut()) {
        if (r->timedOut()) {
          LOG(INFO) << "Time out sending " << r;
        }
        delete r;
        i = outgoingRequests.erase(i);
      }
    }
  }

  int ReceiveIncomingData(RPCHelper *rpc) {
    string data;
    int bytesProcessed = 0;
    int updateSize = 0;
    while ((updateSize = rpc->TryRead(id, MTYPE_KERNEL_DATA, &readScratch, &data))) {
    	bytesProcessed += updateSize;

      HashUpdateRequest *req = new HashUpdateRequest;
      req->CopyFrom(readScratch);
      incomingRequests.push_back(req);

      VLOG(2) << "Accepting update of size " << updateSize
              << " from peer " << readScratch.source();
    }

    return bytesProcessed;
  }

  int64_t write_bytes_pending() const {
    int64_t t = 0;
    for (list<Request*>::const_iterator i = outgoingRequests.begin(); i != outgoingRequests.end(); ++i) {
      t += (*i)->payload.size();
    }
    return t;
  }

  Request* get_request(HashUpdateRequest *ureq) {
    Request* r = new Request();
    r->target = id;
    ureq->SerializeToString(&r->payload);
    outgoingRequests.push_back(r);
    return r;
  }

  HashUpdateRequest *pop_incoming() {
    HashUpdateRequest *r = incomingRequests.front();
    incomingRequests.pop_front();
    return r;
  }
};

Worker::Worker(const ConfigData &c) {
  config.CopyFrom(c);

  numPeers = config.num_workers();
  peers.resize(numPeers);
  for (int i = 0; i < numPeers; ++i) {
    peers[i] = new Peer(i + 1);
  }

  bytesOut = bytesIn = 0;

  running = true;

  rpc = GetRPCHelper();
  world = GetMPIWorld();
  kernelThread = networkThread = NULL;
}

void Worker::Run() {
  kernelThread = new boost::thread(boost::bind(&Worker::KernelLoop, this));
  networkThread = new boost::thread(boost::bind(&Worker::NetworkLoop, this));

  kernelThread->join();
  networkThread->join();
}

Worker::~Worker() {
  running = false;
  delete kernelThread;
  delete networkThread;

  for (int i = 0; i < pendingWrites.size(); ++i) {delete pendingWrites[i];}
  for (int i = 0; i < peers.size(); ++i) {
    delete peers[i];
  }
}

SharedTable* Worker::CreateTable(ShardingFunction sf, HashFunction hf, AccumFunction af) {
  PartitionedHash *t = new PartitionedHash(sf, hf, af, config.worker_id(), tables.size(), config.num_workers(), rpc);
  tables.push_back(t);
  return t;
}

void Worker::NetworkLoop() {
  deque<LocalHash*> work;
  while (running) {
    PERIODIC(10,
        LOG(INFO) << "Network loop working - " << pending_kernel_bytes() << " bytes in the processing queue.");

    while (work.empty()) {
      Sleep(5.);
      SendAndReceive();

      for (int i = 0; i < tables.size(); ++i) {
        tables[i]->GetPendingUpdates(&work);
      }
    }

    LocalHash* old = work.front();
    work.pop_front();

    VLOG(2) << "Accum " << old->owner << " : " << old->size();
    Peer * p = peers[old->owner];

    LocalHash::Iterator *i = old->get_iterator();
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
  RunKernelRequest kernelRequest;

	while (running) {
		rpc->Read(MPI_ANY_SOURCE, MTYPE_RUN_KERNEL, &kernelRequest);

		LOG(INFO) << "Received run request for kernel id: " << kernelRequest.kernel_id();

		KernelFunction f = KernelRegistry::get_kernel(kernelRequest.kernel_id());
		f();

		LOG(INFO) << "Waiting for network to finish: " << pending_network_bytes() + pending_kernel_bytes();
	  while (pending_kernel_bytes() + pending_network_bytes() > 0) {
	    Sleep(0.1);
	  }

    // Send a message to master indicating that we've completed our kernel
     // and we are done transmitting.
	  EmptyMessage m;
	  rpc->Send(config.master_id(), MTYPE_KERNEL_DONE, m);

	  LOG(INFO) << "Kernel done.";
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

void Worker::ComputeUpdates(Peer *p, LocalHash::Iterator *it) {
  HashUpdateRequest *r = &p->writeScratch;
  r->Clear();

  r->set_source(config.worker_id());
  r->set_hash_id(it->owner()->hash_id);

  int bytesUsed = 0;
  int count = 0;
  for (; !it->done() && bytesUsed < kNetworkChunkSize; it->next()) {
    StringPiece k = it->key();
    StringPiece v = it->value();

//    LOG(INFO) << " k " << k.len << " v " << v.len;

    Pair &u = *r->add_put();
    u.set_key(k.data, k.len);
    u.set_value(v.data, v.len);
    ++count;

    bytesUsed += k.len + v.len;
  }

  VLOG(2) << "Prepped " << count << " taking " << bytesUsed;

  Peer::Request *mpiReq = p->get_request(r);
  bytesOut += mpiReq->payload.size();

  mpiReq->Send(world);
  ++count;
}

void Worker::SendAndReceive() {
  MPI::Status probeResult;

  string scratch;

  if (world->Iprobe(config.master_id(), MTYPE_WORKER_SHUTDOWN, probeResult)) {
    LOG(INFO) << "Shutting down worker " << config.worker_id();
    running = false;
  }

  for (int i = 0; i < peers.size(); ++i) {
    Peer *p = peers[i];
    p->CollectPendingSends();
    p->ReceiveIncomingData(rpc);
    while (!p->incomingRequests.empty()) {
      HashUpdateRequest *r = p->pop_incoming();

      tables[r->hash_id()]->ApplyUpdates(*r);
      delete r;
    }
  }
}

} // end namespace
