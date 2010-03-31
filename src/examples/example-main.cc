#include "client.h"

using namespace dsm;

DEFINE_string(runner, "", "");

DEFINE_int32(shards, 10, "");
DEFINE_int32(iterations, 10, "");

int main(int argc, char** argv) {
  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

  LOG(INFO) << "Running: " << FLAGS_runner;
  CHECK_NE(FLAGS_runner, "");
  Registry::get_runner(FLAGS_runner)(conf);
  LOG(INFO) << "Exiting.";
}
