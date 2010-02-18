#include "client.h"
#include "examples/crawler_support.h"

CrawlTable *the_table;
int the_shard;

using namespace dsm;
class PagerankKernel : public DSMKernel {
  void InitializeCrawl() {
    the_table = new CrawlTable(get_table<string, int>(0));
    the_shard = current_shard();
  }

  void RunCrawl() {
  }
};

CrawlTable& get_table() {
  return *the_table;
}

int get_shard() {
  return the_shard;
}

void Initialize(int argc, const char** argv) {
	Init(argc, (char**)argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_slots(1);

	Registry::create_table<string, int>(0, conf.num_workers(), &StringSharding, &Accumulator<int>::max);
}
