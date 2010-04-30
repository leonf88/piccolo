#include "util/rpc.h"
#include "util/common.h"

DECLARE_bool(localtest);
DEFINE_bool(rpc_log, false, "");

namespace dsm {

class MPIHelper : public RPCHelper, private boost::noncopyable {
public:
  MPIHelper() :
    mpi_world_(&MPI::COMM_WORLD), my_rank_(MPI::COMM_WORLD.Get_rank()) {
  }

  // Try to read a message from the given peer and rpc channel = 0; return false if no
  // message is immediately available.
  bool TryRead(int src, int method, Message *msg);
  bool HasData(int src, int method);
  bool HasData(int src, int method, MPI::Status &status);

  int Read(int src, int method, Message *msg);
  int ReadAny(int *src, int method, Message *msg);
  int ReadBytes(int src, int method, string *data, int *actual_src);

  void Send(int target, int method, const Message &msg);
  void SyncSend(int target, int method, const Message &msg);

  void SendData(int peer_id, int rpc_id, const string& data);
  MPI::Request ISendData(int peer_id, int rpc_id, const string& data);

  // For whatever reason, MPI doesn't offer tagged broadcasts, we simulate that
  // here.
  void Broadcast(int method, const Message &msg);
  void SyncBroadcast(int method, const Message &msg);
private:
  boost::recursive_mutex mpi_lock_;
  MPI::Comm *mpi_world_;
  int my_rank_;

  int _Read(int desired_src, int method, Message *msg, int *src);
  void _SyncSend(int target, int method, const Message& msg);
  void WaitForReplies(int count);

  struct Header {
    Header() : rpc_id(0), sync(0), sync_reply(0) {}
    int rpc_id;
    bool sync;
    bool sync_reply;
  };

  enum MethodTypes {
    SYNC_REPLY = 255,
  };
};

RPCHelper* get_rpc_helper() {
  static RPCHelper* helper = NULL;
  if (!helper) { helper = new MPIHelper(); }
  return helper;
}

#define rpc_log(logmsg, src, target, method) \
  VLOG_IF(2, FLAGS_rpc_log) << \
  StringPrintf("source %d target: %d rpc: %d %s", src, target, method, string((logmsg)).c_str());

#define rpc_lock \
  boost::recursive_mutex::scoped_lock sl(mpi_lock_);

bool MPIHelper::HasData(int src, int method) {
  MPI::Status st;
  return HasData(src, method, st);
}

bool MPIHelper::HasData(int src, int method, MPI::Status &status) {
  rpc_lock;
  return mpi_world_->Iprobe(src, method, status);
}

bool MPIHelper::TryRead(int src, int method, Message *msg) {
  if (!HasData(src, method)) {
    return false;
  }

  _Read(src, method, msg, NULL);
  return true;
}

int MPIHelper::ReadBytes(int desired_src, int method, string* data, int *src) {
  rpc_lock;

  MPI::Status probe_result;
  mpi_world_->Probe(desired_src, method, probe_result);

  int r_size = probe_result.Get_count(MPI::BYTE);
  int r_src = probe_result.Get_source();

  string scratch;
  scratch.resize(r_size);
  mpi_world_->Recv(&scratch[0], r_size, MPI::BYTE, r_src, method, probe_result);

  Header *h = (Header*) scratch.data();
  data->assign(scratch.begin() + sizeof(Header), scratch.end());

  if (src) { *src = r_src; }

//  LOG(INFO) << StringPrintf("method: %d; sync %d; reply %d", h->sync, h->sync_reply);

  if (h->sync) {
    Header hreply;
    hreply.sync_reply = 1;
    SendData(r_src, SYNC_REPLY, string((char*) &hreply, sizeof(hreply)));
  }

  return r_size;
}

int MPIHelper::_Read(int desired_src, int method, Message *msg, int *src) {
  string scratch;
  ReadBytes(desired_src, method, &scratch, src);
  msg->ParseFromString(scratch);
  return scratch.size();
}

int MPIHelper::Read(int src, int method, Message *msg) {
  return _Read(src, method, msg, NULL);
}

int MPIHelper::ReadAny(int *src, int method, Message *msg) {
  while (!HasData(MPI_ANY_SOURCE, method)) {
    sched_yield();
  }

  return _Read(MPI_ANY_SOURCE, method, msg, src);
}

void MPIHelper::Send(int target, int method, const Message &msg) {
  string scratch;
  Header h;
  scratch.append((char*)&h, sizeof(h));

  msg.AppendToString(&scratch);
  SendData(target, method, scratch);
}

void MPIHelper::SendData(int target, int method, const string& msg) {
  rpc_lock;
  rpc_log("SendData", my_rank_, target, method);
  mpi_world_->Send(&msg[0], msg.size(), MPI::BYTE, target, method);
}

MPI::Request MPIHelper::ISendData(int target, int method, const string& msg) {
  rpc_lock;
  rpc_log("ISendData", my_rank_, target, method);
  Header h;
  string scratch;
  scratch.append((char*)&h, sizeof(h));
  scratch.append(msg);
  return mpi_world_->Issend(&scratch[0], scratch.size(), MPI::BYTE, target, method);
}

void MPIHelper::SyncSend(int target, int method, const Message &msg) {
  rpc_lock;
  _SyncSend(target, method, msg);
  WaitForReplies(1);
}

void MPIHelper::_SyncSend(int target, int method, const Message& msg) {
  rpc_lock;
  rpc_log("SyncSendStart", my_rank_, target, method);

  string scratch;
  Header h;
  h.sync = 1;
  scratch.append((char*)&h, sizeof(h));

  msg.AppendToString(&scratch);
  SendData(target, method, scratch);
  rpc_log("SyncSendDone", my_rank_, target, method);
}

void MPIHelper::Broadcast(int method, const Message &msg) {
  rpc_lock;
  for (int i = 1; i < mpi_world_->Get_size(); ++i) {
    Send(i, method, msg);
  }
}

void MPIHelper::SyncBroadcast(int method, const Message &msg) {
  rpc_lock;

  VLOG(1) << "SyncBroadcast: " << method << "  :: " << msg;
  for (int i = 1; i < mpi_world_->Get_size(); ++i) {
    _SyncSend(i, method, msg);
  }

  WaitForReplies(mpi_world_->Get_size() - 1);
}

void MPIHelper::WaitForReplies(int count) {
  while (count > 0) {
    VLOG(1) << "Waiting for sync: " << count;
    MPI::Status probe_result;
    mpi_world_->Probe(MPI_ANY_SOURCE, SYNC_REPLY, probe_result);
    --count;
  }
}

}
