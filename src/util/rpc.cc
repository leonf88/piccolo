#include "util/rpc.h"
#include "util/common.h"
#include "util/common.pb.h"

DECLARE_bool(localtest);
DECLARE_double(sleep_time);
DEFINE_bool(rpc_log, false, "");

namespace dsm {


struct Header {
  Header() : sync_request(0), sync_reply(0) {}
  bool sync_request;
  bool sync_reply;
};

// Represents an active RPC to a remote peer.
struct RPCRequest : private boost::noncopyable {
  int target;
  int rpc_type;
  int failures;

  string payload;
  MPI::Request mpi_req;
  MPI::Status status;
  double start_time;

  RPCRequest(int target, int method, const Message& msg, Header h=Header());
  ~RPCRequest();

  bool finished();
  double elapsed();
};

RPCRequest::~RPCRequest() {}

bool RPCRequest::finished() { return mpi_req.Test(status); }
double RPCRequest::elapsed() { return Now() - start_time; }

// Send the given message type and data to this peer.
RPCRequest::RPCRequest(int tgt, int method, const Message& ureq, Header h) {
  failures = 0;
  target = tgt;
  rpc_type = method;

  payload.append((char*)&h, sizeof(Header));
  ureq.AppendToString(&payload);
}

NetworkThread::NetworkThread() {
  world_ = &MPI::COMM_WORLD;
  running = 1;
  t_ = new boost::thread(&NetworkThread::Run, this);
}

bool NetworkThread::active() const {
  return active_sends_.size() + pending_sends_.size() > 0;
}

int64_t NetworkThread::pending_bytes() const {
  boost::recursive_mutex::scoped_lock sl(send_lock);
  int64_t t = 0;

  for (unordered_set<RPCRequest*>::const_iterator i = active_sends_.begin(); i != active_sends_.end(); ++i) {
    t += (*i)->payload.size();
  }

  for (int i = 0; i < pending_sends_.size(); ++i) {
    t += pending_sends_[i]->payload.size();
  }

  return t;
}

void NetworkThread::CollectActive() {
  if (active_sends_.empty())
    return;

  boost::recursive_mutex::scoped_lock sl(send_lock);
  unordered_set<RPCRequest*>::iterator i = active_sends_.begin();
  while (i != active_sends_.end()) {
    RPCRequest *r = (*i);
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

void NetworkThread::Run() {
  while (running) {
    MPI::Status st;

    if (world_->Iprobe(MPI::ANY_SOURCE, MPI::ANY_TAG, st)) {
      int tag = st.Get_tag();
      int source = st.Get_source();
      int bytes = st.Get_count(MPI::BYTE);

      string data;
      data.resize(bytes);

      world_->Recv(&data[0], bytes, MPI::BYTE, source, tag, st);

      Header *h = (Header*)&data[0];
      if (h->sync_request) {
//        LOG(INFO) << "Got sync packet; replying...";
        EmptyMessage msg;
        Send(source, MTYPE_SYNC_REPLY, msg);
      }

      boost::recursive_mutex::scoped_lock sl(q_lock[tag]);
      CHECK_LT(source, kMaxHosts);
      incoming[tag][source].push_back(data);
    } else {
      Sleep(FLAGS_sleep_time);
    }

    while (!pending_sends_.empty()) {
      boost::recursive_mutex::scoped_lock sl(send_lock);
      RPCRequest* s = pending_sends_.back();
      pending_sends_.pop_back();
      s->start_time = Now();
      s->mpi_req = world_->Isend(
          s->payload.data(), s->payload.size(), MPI::BYTE, s->target, s->rpc_type);
      active_sends_.insert(s);
    }

    CollectActive();

    PERIODIC(10., { DumpProfile(); });
  }
}

bool NetworkThread::check_queue(int src, int type, Message* data) {
  CHECK_LT(src, kMaxHosts);

  Queue& q = incoming[type][src];
  if (!q.empty()) {
    boost::recursive_mutex::scoped_lock sl(q_lock[type]);
    if (q.empty())
      return false;

    const string& s = q.front();
    if (data) {
      data->ParseFromArray(s.data() + sizeof(Header), s.size() - sizeof(Header));
    }

    q.pop_front();
    return true;
  }
  return false;
}

  // Blocking read for the given source and message type.
void NetworkThread::Read(int desired_src, int type, Message* data, int *source) {
  while (!TryRead(desired_src, type, data, source)) {
    Sleep(FLAGS_sleep_time);
  }
}

bool NetworkThread::TryRead(int src, int type, Message* data, int *source) {
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
void NetworkThread::Send(RPCRequest *req) {
  boost::recursive_mutex::scoped_lock sl(send_lock);
//    LOG(INFO) << "Sending... " << MP(req->target, req->rpc_type);
  pending_sends_.push_back(req);
}

void NetworkThread::Send(int dst, int method, const Message &msg) {
  RPCRequest *r = new RPCRequest(dst, method, msg);
  Send(r);
}

void NetworkThread::Shutdown() {
  if (running) {
    Flush();
    running = false;
    MPI_Finalize();
  }
}

void NetworkThread::Flush() {
  while (active()) {
    Sleep(FLAGS_sleep_time);
  }
}

void NetworkThread::Broadcast(int method, const Message& msg) {
  for (int i = 1; i < world_->Get_size(); ++i) {
    Send(i, method, msg);
  }
}

void NetworkThread::SyncBroadcast(int method, const Message& msg) {
  for (int i = 1; i < world_->Get_size(); ++i) {
    Header h;
    h.sync_request = 1;

    RPCRequest *r = new RPCRequest(i, method, msg, h);
    Send(r);
  }

  WaitForSync(world_->Get_size() - 1);
}

void NetworkThread::WaitForSync(int count) {
  while (count > 0) {
    Read(MPI::ANY_SOURCE, MTYPE_SYNC_REPLY, NULL, NULL);
    --count;
  }
}

static NetworkThread* net = NULL;
NetworkThread* NetworkThread::Get() {
  if (!net) {
    net = new NetworkThread();
  }
  return net;
}

static void ShutdownMPI() {
  NetworkThread::Get()->Shutdown();
}

static void NetworkInit() {
  NetworkThread::Get();
  atexit(&ShutdownMPI);
}

REGISTER_INITIALIZER(NetworkInit, { NetworkInit(); });
}

