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

    double timedOut() {
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
      if (r->mpiReq.Test() || r->timedOut()) {
        if (r->timedOut()) {
          LOG(INFO) << "Time out sending " << r;
        }
        delete r;
        i = outgoingRequests.erase(i);
      }
    }
  }

  void ReceiveIncomingData(RPCHelper *rpc) {
    while (rpc->TryRead(id, MTYPE_KERNEL_DATA, &dataScratch)) {
      VLOG(1) << "Read put request....";
      HashUpdate *req = new HashUpdate;
      req->CopyFrom(dataScratch);
      incomingData.push_back(req);
    }

    while (rpc->TryRead(id, MTYPE_GET_REQUEST, &reqScratch)) {
      VLOG(1) << "Read get request....";
      HashRequest *req = new HashRequest;
      req->CopyFrom(reqScratch);
      incomingRequests.push_back(req);
    }
  }

  int64_t write_bytes_pending() const {
    int64_t t = 0;
    for (list<Request*>::const_iterator i = outgoingRequests.begin(); i != outgoingRequests.end(); ++i) {
      t += (*i)->payload.size();
    }
    return t;
  }

  Request* create_data_request(int rpc_type, google::protobuf::Message* ureq) {
    Request* r = new Request();
    r->target = id;
    r->rpc_type = rpc_type;
    ureq->SerializeToString(&r->payload);
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

  MPI::Intracomm world = MPI::COMM_WORLD;
  worker_comm_ = world.Split(0, world.Get_rank());

  kernel_thread_ = network_thread_ = NULL;
}

void Worker::Run() {
  kernel_thread_ = new boost::thread(boost::bind(&Worker::KernelLoop, this));
  network_thread_ = new boost::thread(boost::bind(&Worker::NetworkLoop, this));

  kernel_thread_->join();
  network_thread_->join();
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

Table* Worker::CreateTable(ShardingFunction sf, HashFunction hf, AccumFunction af) {
  TableInfo tinfo;
  tinfo.af = af;
  tinfo.sf = sf;
  tinfo.hf = hf;
  tinfo.num_threads = config.num_workers();
  tinfo.table_id = tables.size();
  tinfo.rpc = new RPCHelper(&worker_comm_);
  tinfo.owner_thread = config.worker_id();

  PartitionedTable *t = new PartitionedTable(tinfo);
  tables.push_back(t);
  return t;
}

void Worker::NetworkLoop() {
  deque<LocalTable*> work;
  while (running) {
    PERIODIC(10,
        LOG(INFO) << "Network loop working - " << pending_kernel_bytes() << " bytes in the processing queue.");

    while (work.empty()) {
      Sleep(0.1);
      SendAndReceive();

      for (int i = 0; i < tables.size(); ++i) {
        tables[i]->GetPendingUpdates(&work);
      }
    }

    LocalTable* old = work.front();
    work.pop_front();

    VLOG(2) << "Accum " << old->info().owner_thread << " : " << old->size();
    Peer * p = peers[old->info().owner_thread];

    LocalTable::Iterator *i = old->get_iterator();
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
//	  if (master_comm_->Iprobe(config.master_id(), MTYPE_WORKER_SHUTDOWN, probeResult)) {
//	    LOG(INFO) << "Shutting down worker " << config.worker_id();
//	    running = false;
//	  }

		rpc.Read(MPI_ANY_SOURCE, MTYPE_RUN_KERNEL, &kernelRequest);

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
	  rpc.Send(config.master_id(), MTYPE_KERNEL_DONE, m);

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

void Worker::ComputeUpdates(Peer *p, LocalTable::Iterator *it) {
  HashUpdate *r = &p->writeScratch;
  r->Clear();

  r->set_source(config.worker_id());
  r->set_table_id(it->owner()->info().table_id);

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

  Peer::Request *mpiReq = p->create_data_request(MTYPE_KERNEL_DATA, r);

  mpiReq->Send(&worker_comm_);
  ++count;
}

void Worker::SendAndReceive() {
  MPI::Status probeResult;
  RPCHelper rpc(&worker_comm_);

  HashUpdate scratch;

  for (int i = 0; i < peers.size(); ++i) {
    Peer *p = peers[i];
    p->CollectPendingSends();
    p->ReceiveIncomingData(&rpc);

    while (!p->incomingData.empty()) {
      HashUpdate *r = p->pop_data();

      tables[r->table_id()]->ApplyUpdates(*r);
      delete r;
    }

    while (!p->incomingRequests.empty()) {
      HashRequest *r = p->pop_request();
      VLOG(1) << "Returning local result: " << r->key();
      string v = tables[r->table_id()]->get_local(r->key());
      scratch.Clear();
      scratch.set_source(config.worker_id());
      scratch.set_table_id(r->table_id());
      scratch.add_put();
      scratch.mutable_put(0)->set_key(r->key());
      scratch.mutable_put(0)->set_value(v);
      p->create_data_request(MTYPE_GET_RESPONSE, &scratch)->Send(&worker_comm_);
      delete r;
    }
  }
}

} // end namespace
