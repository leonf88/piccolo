#include "util/common.h"
#include "util/fake-mpi.h"

#include "worker/worker.h"
#include "worker/kernel.h"

#include <boost/bind.hpp>
#include <signal.h>

namespace upc {

FakeMPIWorld fakeWorld;

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

  void collectPendingSends() {
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

  int receiveIncomingData(RPCHelper *rpc) {
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

  int64_t writeBytesPending() const {
    int64_t t = 0;
    for (list<Request*>::const_iterator i = outgoingRequests.begin(); i != outgoingRequests.end(); ++i) {
      t += (*i)->payload.size();
    }
    return t;
  }

  Request* getRequest(HashUpdateRequest *ureq) {
    Request* r = new Request();
    r->target = id;
    ureq->SerializeToString(&r->payload);
    outgoingRequests.push_back(r);
    return r;
  }

  HashUpdateRequest *popIncoming() {
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
    peers[i] = new Peer(i);
  }

  bytesOut = bytesIn = 0;

  running = true;

  if (config.localtest()) {
    world = new FakeMPIComm(config.worker_id());
  } else {
    world = &MPI::COMM_WORLD;
  }

  rpc = new RPCHelper(world);

  kernelThread = new boost::thread(boost::bind(&Worker::kernelLoop, this));
  networkThread = new boost::thread(boost::bind(&Worker::networkLoop, this));
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

void Worker::networkLoop() {
  deque<LocalHash*> work;
  while (running) {
    PERIODIC(10,
        LOG(INFO) << "Network loop working - " << pendingKernelBytes() << " bytes in the processing queue.");

    while (work.empty()) {
      Sleep(0.1);
      sendAndReceive();

      for (int i = 0; i < tables.size(); ++i) {
        tables[i]->getPendingUpdates(&work);
      }
    }

    LocalHash* old = work.front();
    work.pop_front();

    VLOG(2) << "Accum " << old->ownerThread << " : " << old->size();
    Peer * p = peers[old->ownerThread];

    LocalHash::Iterator *i = old->getIterator();
    while (!i->done()) {
      if (pendingNetworkBytes() < config.network_buffer()) {
        computeUpdates(p, i);
      }

      sendAndReceive();
    }

    delete i;
    delete old;

    sendAndReceive();
  }
}

void Worker::kernelLoop() {
  RunKernelRequest kernelRequest;

	while (running) {
		rpc->Read(MPI_ANY_SOURCE, MTYPE_RUN_KERNEL, &kernelRequest);

		KernelFunction f = KernelRegistry::get()->GetKernel(kernelRequest.kernel_id());
		f();

	  while (pendingKernelBytes() + pendingNetworkBytes() > 0) {
	    Sleep(0.1);
	  }

    // Send a message to master indicating that we've completed our kernel
     // and we are done transmitting.
	  EmptyMessage m;
	  rpc->Send(config.master_id(), MTYPE_KERNEL_DONE, m);
	}
}

int64_t Worker::pendingNetworkBytes() const {
  int64_t t = 0;
  for (int i = 0; i < peers.size(); ++i) {
    t += peers[i]->writeBytesPending();
  }
  return t;
}

int64_t Worker::pendingKernelBytes() const {
  int64_t t = 0;

  for (int i = 0; i < tables.size(); ++i) {
    t += tables[i]->pendingBytes();
  }

  return t;
}

void Worker::computeUpdates(Peer *p, LocalHash::Iterator *it) {
  HashUpdateRequest *r = &p->writeScratch;
  r->Clear();

  r->set_source(config.worker_id());

  int bytesUsed = 0;
  int count = 0;
  for (; !it->done() && bytesUsed < kNetworkChunkSize; it->next()) {
    const StringPiece& k = it->key();
    const StringPiece& v = it->value();

    Pair &u = *r->add_update();
    u.set_key(k.data, k.len);
    u.set_value(v.data, v.len);
    ++count;

    bytesUsed += k.len + v.len;
  }

  VLOG(2) << "Prepped " << count << " taking " << bytesUsed;

  Peer::Request *mpiReq = p->getRequest(r);
  bytesOut += mpiReq->payload.size();

  mpiReq->Send(world);
  ++count;
}

void Worker::sendAndReceive() {
  MPI::Status probeResult;

  string scratch;

  if (world->Iprobe(config.master_id(), MTYPE_WORKER_SHUTDOWN, probeResult)) {
    LOG(INFO) << "Shutting down worker " << config.worker_id();
    running = false;
  }

  for (int i = 0; i < peers.size(); ++i) {
    Peer *p = peers[i];
    p->collectPendingSends();
    p->receiveIncomingData(rpc);
    while (!p->incomingRequests.empty()) {
      HashUpdateRequest *r = p->popIncoming();

      tables[r->hash_id()]->applyUpdates(*r);
      delete r;
    }
  }
}

} // end namespace
