#include "util/fake-mpi.h"

namespace upc {

FakeMPIWorld fake_world;

void FakeMPIComm::Send(const void* c, int count, const MPI::Datatype& t, int target, int tag) const {
  Isend(c, count, t, target, tag);
  return;
}

MPI::Request FakeMPIComm::Isend(const void* c, int count, const MPI::Datatype& t, int target, int tag) const {
  DVLOG(3) << id << "::: " << " Send to " << target << " size " << count;

  boost::mutex::scoped_lock sl(fake_world.network[target].lock);
  FakeMPIWorld::Packet p(id, target, tag, string((const char*)c, count));
  fake_world.network[target].data.push_back(p);
  return Request();
}

void FakeMPIComm::Probe(int source, int tag) const {
  MPI::Status status;
  Probe(source, tag, status);
}

void FakeMPIComm::Probe(int source, int tag, MPI::Status &status) const {
  while (!Iprobe(source, tag, status)) {
    Sleep(0.01);
  }
}

bool FakeMPIComm::Iprobe(int source, int tag) const {
  MPI::Status s;
  return Iprobe(source, tag, s);
}

bool FakeMPIComm::Iprobe(int source, int tag, MPI::Status &result) const {
  deque<FakeMPIWorld::Packet> &d = fake_world.network[id].data;
  if (d.empty()) {
    return false;
  }
  DVLOG(3) << id << "::: " << " probing for " << source << " tag " << tag;

  boost::mutex::scoped_lock sl(fake_world.network[id].lock);
  for (int i = 0; i < d.size(); ++i) {
    FakeMPIWorld::Packet &p = d[i];
    DVLOG(3) << id << "::: " << " probe for " << source << " tag " << tag << " checking "
            << p.source << " : " << p.tag << " : " << p.payload.size();

    if ((p.source == source || source == MPI::ANY_SOURCE) &&
        (p.tag == tag || tag == MPI::ANY_TAG)) {
      result.Set_source(p.source);
      result.Set_tag(p.tag);
      result.Set_elements(MPI::BYTE, p.payload.size());
      result.Set_error(MPI::SUCCESS);

      DVLOG(3) << id << "::: " << " probe for " << source << " tag " << tag << " matched "
              << p.source << " : " << p.tag << " : " << p.payload.size();
      return true;
    }
  }

  return false;
}

void FakeMPIComm::Recv(void *tgt, int bytes, const MPI::Datatype& t, int source, int tag) const {
  MPI::Status s;
  return Recv(tgt, bytes, t, source, tag, s);
}

void FakeMPIComm::Recv(void *tgt, int bytes, const MPI::Datatype& t, int source, int tag, MPI::Status& result) const {
  deque<FakeMPIWorld::Packet> &d = fake_world.network[id].data;

  boost::mutex::scoped_lock sl(fake_world.network[id].lock);
  for (int i = 0; i < d.size(); ++i) {
    FakeMPIWorld::Packet &p = d[i];
    if ((p.source == source || source == MPI::ANY_SOURCE) &&
        (p.tag == tag || tag == MPI::ANY_TAG)) {
      result.Set_source(p.source);
      result.Set_tag(p.tag);
      result.Set_elements(MPI::BYTE, p.payload.size());
      result.Set_error(MPI::SUCCESS);

      memcpy(tgt, &p.payload[0], bytes);
      d.erase(d.begin() + i);
      DVLOG(3) << id << "::: " << " Recv " << source << " size " << p.payload.size();
      return;
    }
  }
}

}
