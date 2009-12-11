#include <mpi.h>
#include <boost/thread.hpp>
#include <vector>
#include <string>
#include <stdio.h>

using namespace std;
struct SendReq {
  MPI::Request req;
  string data;

  SendReq() {
    data.assign(100000, 'a');
  }

  ~SendReq() {
  }

  void Send() {
    req = MPI::COMM_WORLD.Isend(&data[0], data.size(), MPI::BYTE, 1, 1);
  }
};

void* thread_1(void* data) {
  MPI::Intracomm world = MPI::COMM_WORLD;
  vector<SendReq*> reqs;
  for (int i = 0; i < 1000; ++i) {
    reqs.push_back(new SendReq);
    reqs.back()->Send();
  }

  while (!reqs.empty()) {
    for (int i = 0; i < reqs.size(); ++i) {
      if (reqs[i]->req.Test()) {
        reqs.erase(reqs.begin() + i);
      }

      world.Iprobe(MPI::ANY_SOURCE, 1);
    }
  }

  return NULL;
}

void* thread_2(void *data) {
  MPI::Intracomm world = MPI::COMM_WORLD;
  for (int i = 0; i < 1000; ++i) {
    do {
      MPI::Status s;
      if (world.Iprobe(MPI::ANY_SOURCE, 1, s)) {
        int count = s.Get_count(MPI::BYTE);
        string buf;
        buf.resize(count);
        world.Recv(&buf[0], count, MPI::BYTE, MPI::ANY_SOURCE, 1);
        string comp;
        comp.assign(100000, 'a');
        if (buf != comp) {
          fprintf(stderr, "WTF!\n");
        }

        fprintf(stderr, ".");
        break;
      }
    } while(1);

  }
  return NULL;
}

int main(int argc, char **argv) {
  MPI::Init_thread(argc, argv, MPI::THREAD_MULTIPLE);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    thread_1(NULL);
  } else {
    thread_2(NULL);
  }
}
