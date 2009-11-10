#include "worker/worker.h"
#include "util/fake-mpi.h"

#include <boost/bind.hpp>
#include <signal.h>

DEFINE_bool(synchronous, false,
            "If true, the local kernel executes one iteration and blocks for receipt "
            "of all peer updates before beginning a new iteration.");

DEFINE_bool(accumulate_updates, true,
            "If true, workers will accumulate all updates for an iteration before handing them "
            "off to the kernel.  Otherwise, updates are applied as they arrive.");

DEFINE_int32(iterations, 10000000,
             "Terminate after running this number of iterations.");

DEFINE_double(sleep_time, 0.0, "");


namespace asyncgraph {

FakeMPIWorld fakeWorld;

struct Worker::Peer {
  // Represents an outgoing request to this worker.
  struct Request {
    int target;
    string payload;
    MPI::Request mpiReq;
    double startTime;

    Request() {
      startTime = Now();
    }

    void send(MPI::Comm* world) {
      mpiReq = world->Isend(&payload[0], payload.size(), MPI::BYTE, target, MTYPE_KERNEL_UPDATE);
    }

    double timedOut() {
      return Now() - startTime > kNetworkTimeout;
    }
  };

  list<Request*> outgoingRequests;
  UpdateRequest writeScratch;
  UpdateRequest readScratch;
  UpdateRequest *accumulatedUpdates;

  // Incoming data from this peer that we have read from the network, but has yet to be
  // processed by the kernel.
  deque<UpdateRequest*> incomingRequests;
  boost::mutex incomingLock;

  int32_t id;

  // The last iteration for which we have sent/received complete data from this peer.
  int32_t sendIteration;
  int32_t recvIteration;

  Peer(int id) : id(id), sendIteration(0), recvIteration(0) {
    accumulatedUpdates = new UpdateRequest;
  }

  void collectPendingSends() {
    for (list<Request*>::iterator i = outgoingRequests.begin(); i != outgoingRequests.end(); ++i) {
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

  int receiveIncomingData(MPI::Comm *world) {
    string data;
    MPI::Status probeResult;
    int bytesProcessed = 0;
    while (world->Iprobe(id, MTYPE_KERNEL_UPDATE, probeResult)) {
      int updateSize = probeResult.Get_count(MPI::BYTE);

      data.resize(updateSize);
      bytesProcessed += updateSize;

      world->Recv(&data[0], updateSize, MPI::BYTE, id, MTYPE_KERNEL_UPDATE);
      readScratch.ParseFromString(data);

      if (FLAGS_accumulate_updates) {
        accumulatedUpdates->MergeFrom(readScratch);
      } else {
        boost::mutex::scoped_lock sl(incomingLock);
        UpdateRequest *req = new UpdateRequest;
        req->CopyFrom(readScratch);
        incomingRequests.push_back(req);
      }

      VLOG(2) << "Accepting update of size " << updateSize  << " iter " << readScratch.iteration()
              << " from peer " << readScratch.source() << " done " << readScratch.done();


      if (readScratch.done()) {
        if (FLAGS_accumulate_updates) {
          boost::mutex::scoped_lock sl(incomingLock);
          incomingRequests.push_back(accumulatedUpdates);
          accumulatedUpdates = new UpdateRequest;
          VLOG(2) << "Finished reading iteration " << readScratch.iteration() << " from " << readScratch.source();
        }
      }
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

  Request* getRequest(UpdateRequest *ureq) {
    Request* r = new Request();
    r->target = id;
    ureq->SerializeToString(&r->payload);
    outgoingRequests.push_back(r);
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

  bytesOut = bytesIn = kernelIteration = 0;

  kernel = KernelRegistry::get()->getKernel(config.kernel());
  kernel->_init(this, config);

  pushAccumulators();

  bzero((void*)accumWriting, sizeof(bool) * kMaxPeers);

  running = true;

  if (config.localtest()) {
    world = new FakeMPIComm(config.worker_id());
  } else {
    world = &MPI::COMM_WORLD;
  }

  kernelThread = new boost::thread(boost::bind(&Worker::kernelLoop, this));
  networkThread = new boost::thread(boost::bind(&Worker::networkLoop, this));

  // Demote the kernel thread to allow networking events first priority.
  //pthread_setschedprio(kernelThread->native_handle(), 10);
}

Worker::~Worker() {
  running = false;
  delete kernelThread;
  delete networkThread;
  delete kernel;

  for (int i = 0; i < pendingWrites.size(); ++i) { delete pendingWrites[i]; }
  for (int i = 0; i < peers.size(); ++i) {
    delete peers[i];
    delete activeAccum[i];
  }
}

void Worker::wait() {
  kernelThread->join();
  networkThread->join();
}

void Worker::status(WorkerStatus *r) {
  r->set_bytes_in(bytesIn);
  r->set_bytes_out(bytesOut);
  r->set_current_iteration(kernelIteration);

  kernel->status(r->mutable_kernel_status());
}

void Worker::output(int shard, const StringPiece &k, const StringPiece &v) {
  // This is so horrible I might cry.  Yes, I'm crying now.  Don't want a full critical
  // section around writing; but need to ensure that kernel thread is out of this region
  // before the network thread can clear an accumulator.
  int peer = shard % numPeers;
  accumWriting[peer] = true;
  activeAccum[peer]->add(k, v);
  accumWriting[peer] = false;

  // If we're emitting a large amount of information from the kernel, go ahead and flush it to
  // the network. Accumulators are also flushed at the end of each iteration.
  EVERY_N(100000000, pushAccumulators());
}

bool Worker::popAccumulators(deque<Accumulator*> *out) {
  boost::recursive_mutex::scoped_lock sl(pendingLock);

  while (!pendingWrites.empty()) {
    Accumulator *a = pendingWrites.front();
    pendingWrites.pop_front();

    out->push_back(a);
    while (accumWriting[a->peer]) {}
  }

  return out->size() > 0;
}

void Worker::pushAccumulators() {
  boost::recursive_mutex::scoped_lock sl(pendingLock);

  if (activeAccum.empty()) {
    activeAccum.resize(numPeers);
  }

  for (int i = 0; i < numPeers; ++i) {
    if (activeAccum[i]) {
      pendingWrites.push_back(activeAccum[i]);
    }

    activeAccum[i] = kernel->newAccumulator();
    activeAccum[i]->peer = i;
    activeAccum[i]->iteration = kernelIteration;
  }
}

void Worker::networkLoop() {
  deque<Accumulator*> work;
  while (running || pendingKernelBytes() > 0) {
    PERIODIC(10,
             LOG(INFO) << "Network loop working - " << pendingKernelBytes() << " bytes in the processing queue.");

    if (work.empty()) {
      if (!popAccumulators(&work)) {
        sendAndReceive();
        Sleep(0.1);
        continue;
      }
    }

    while (!work.empty()) {
      Accumulator* old = work.front();
      work.pop_front();

      VLOG(2) << "Accum " << old->iteration << " : " << old->peer << " : " << old->size();
      Peer * p = peers[old->peer];
      bool finishedIteration = old->iteration != p->sendIteration;
      // Send a message to all peers indicating that they've received the full set of data from us.
      // HAXOR HAXOR HAXOR - conjure up a empty iterator just to send the message that we've finished.
      if (finishedIteration) {
        scoped_ptr<Accumulator> temp(kernel->newAccumulator());
        temp->iteration = p->sendIteration;
        temp->peer = p->id;
        scoped_ptr<Accumulator::Iterator> it(temp->get_iterator());
        computeUpdates(p, it.get(), true);
        p->sendIteration = old->iteration;
      }

      Accumulator::Iterator *i = old->get_iterator();
      while (!i->done()) {
        if (pendingNetworkBytes() < config.network_buffer()) {
          computeUpdates(p, i, false);
        }

        sendAndReceive();
      }

      delete i;
      delete old;
    }

    sendAndReceive();
  }
}

void Worker::kernelLoop() {
  while (kernelIteration < FLAGS_iterations && running) {
    processUpdates();

    PERIODIC(5,
             LOG(INFO) << "Kernel for " << config.worker_id() << " at iteration " << kernelIteration);

    // Even running asynchronously, allow peers to catch up before doing too much extra work.
    int lag = FLAGS_synchronous ? 0 : 10000000;
    int upToDatePeers = 0;

    for (int i = 0; i < numPeers; ++i) {
      if (peers[i]->recvIteration >= kernelIteration - 1 - lag) {
        ++upToDatePeers;
      }
    }

    if (upToDatePeers < numPeers) {
      PERIODIC(10,
        LOG(INFO) << config.worker_id() << " :: " << "Waiting for peers to catch up: " << upToDatePeers << " of " << numPeers);
      Sleep(0.1);
      continue;
    }

    if (config.worker_id() == 0) {
      Sleep(FLAGS_sleep_time);
    }

    if (pendingKernelBytes() < 1e7) {
      kernel->runIteration();
      ++kernelIteration;
      // Inform the network thread that we've completed the iteration; flush out the old accumulators.
      pushAccumulators();
      pushAccumulators();

      Sleep(0.01);
    } else {
      PERIODIC(10,
               LOG(INFO) << "Kernel is too far ahead of the network thread (" << pendingKernelBytes() << ")... sleeping for a bit.");
      Sleep(1);
      continue;
    }
  }

  pushAccumulators();
  running = false;
  kernel->flush();
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
  boost::recursive_mutex::scoped_lock sl(pendingLock);

  for (int i = 0; i < pendingWrites.size(); ++i) {
    t += pendingWrites[i]->size();
  }

  return t;
}

void Worker::computeUpdates(Peer *p, Accumulator::Iterator *it, bool final) {
  UpdateRequest *r = &p->writeScratch;
  r->Clear();

  r->set_source(config.worker_id());
  r->set_iteration(it->owner->iteration);

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

  VLOG(2) << "Prepped " << count << " updates for iteration " <<  it->owner->iteration
          << " taking " << bytesUsed << " bytes; completed send? " << it->done()
          << " final " << final;

  // Let the target know if this is the final update for this iteration.
  r->set_done(it->done() && final);

  Peer::Request *mpiReq = p->getRequest(r);
  bytesOut += mpiReq->payload.size();

  mpiReq->send(world);
  ++count;
}

void Worker::sendAndReceive() {
  MPI::Status probeResult;

  string scratch;

  PERIODIC(0.1, 
           {
            // Send the master an update about ourselves.
            WorkerStatus req;
            status(&req);
            req.AppendToString(&scratch);
            world->Send(&scratch[0], scratch.size(), MPI::BYTE, config.master_id(), MTYPE_MASTER_UPDATE);
           });

  if (world->Iprobe(config.master_id(), MTYPE_WORKER_EXIT, probeResult)) {
    scratch.resize(probeResult.Get_count(MPI::BYTE));
    world->Recv(&scratch, scratch.size(), MPI::BYTE, config.num_workers(), MTYPE_WORKER_EXIT);
    LOG(INFO) << "Shutting down worker " << config.worker_id();
    running = false;
  }

  for (int i = 0; i < peers.size(); ++i) {
    Peer *p = peers[i];
    p->collectPendingSends();
    p->receiveIncomingData(world);
  }
}

void Worker::processUpdates() {
  for (int i = 0; i < peers.size(); ++i) {
    Peer *p = peers[i];

    // We hand updates to the kernel in reverse chronological order; allowing them to handle the
    // freshest state of the world first.
    vector<UpdateRequest*> work;
    {
      boost::mutex::scoped_lock sl(p->incomingLock);
      while (!p->incomingRequests.empty()) {
        work.push_back(p->incomingRequests.back());
        p->incomingRequests.pop_back();
      }
    }

    for (int i = 0; i < work.size(); ++i) {
      UpdateRequest *r = work[i];
      VLOG(2) << "handling request: " << r->update_size();
      kernel->handleUpdates(*r);
      if (r->done()) {
        p->recvIteration = max(r->iteration(), p->recvIteration);
        VLOG(1) << "Peer " << p->id << " now at iteration " << p->recvIteration << " got " << r->iteration();
      }
      delete r;
    }
  }
}

} // end namespace
