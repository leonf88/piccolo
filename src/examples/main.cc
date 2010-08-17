#include "client/client.h"

using namespace dsm;

DEFINE_string(runner, "", "");

DEFINE_int32(shards, 10, "");
DEFINE_int32(iterations, 10, "");
DEFINE_int32(block_size, 10, "");
DEFINE_int32(edge_size, 1000, "");

DECLARE_bool(log_prefix);

int main(int argc, char** argv) {
  FLAGS_log_prefix = false;

  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

//  LOG(INFO) << "Running: " << FLAGS_runner;
  CHECK_NE(FLAGS_runner, "");
  RunnerRegistry::KernelRunner k = RunnerRegistry::Get()->runner(FLAGS_runner);
  CHECK(k != NULL);
  k(conf);
  LOG(INFO) << "Exiting.";
}
