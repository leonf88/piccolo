#ifndef FAKEMPI_H_
#define FAKEMPI_H_

#include "util/common.h"
#include <mpi.h>
#include <boost/array.hpp>

namespace upc {

struct FakeMPIWorld {
  static const int kMaxWorkers = 256;

  struct Packet {
    int source;
    int dest;
    int tag;
    string payload;

    Packet(int source, int dest, int tag, const string& payload) :
      source(source), dest(dest), tag(tag), payload(payload) {}
  };

  struct Bucket {
    deque<Packet> data;
    boost::mutex lock;
  };

  boost::array<Bucket, kMaxWorkers> network;
  FakeMPIWorld() {}
};

struct FakeMPIComm : public MPI::Comm {
  int id;
  struct Request : public MPI::Request {
    bool Test() {
      return true;
    }

    bool Test(MPI::Status&) {
      return true;
    }
  };

  FakeMPIComm(int id) : id(id) {}
  MPI::Comm& Clone() const { return *(MPI::Comm*)this; }

  void Set_errhandler(const MPI::Errhandler &h) {}

  void Send(const void* c, int count, const MPI::Datatype& t, int target, int tag) const;
  MPI::Request Isend(const void* c, int count, const MPI::Datatype& t, int target, int tag) const;

  void Probe(int source, int tag) const;
  void Probe(int source, int tag, MPI::Status &status) const;
  bool Iprobe(int source, int tag) const;
  bool Iprobe(int source, int tag, MPI::Status &result) const;
  void Recv(void *tgt, int bytes, const MPI::Datatype& t, int source, int tag) const;
  void Recv(void *tgt, int bytes, const MPI::Datatype& t, int source, int tag, MPI::Status& s) const;
};


}

#endif /* FAKEMPI_H_ */
