#include <python2.6/Python.h>
#include <boost/python.hpp>
#include <gflags/gflags.h>

#include "client.h"
#include "examples/python_support.i"
using namespace dsm;
using namespace boost::python;

DECLARE_bool(work_stealing);

static DSMKernel *the_kernel;

DEFINE_double(crawler_runtime, -1, "Amount of time to run, in seconds.");

class PythonKernel : public DSMKernel {
public:
  PythonKernel() {
    try {
      object sys_module = import("sys");
      object sys_ns = sys_module.attr("__dict__");
      exec("path += ['src/examples/crawler']", sys_ns, sys_ns);
      exec("path += ['bin/release/examples/crawler']", sys_ns, sys_ns);
      exec("path += ['bin/release/examples/']", sys_ns, sys_ns);
      exec("path += ['bin/debug/examples/crawler']", sys_ns, sys_ns);
      exec("path += ['bin/debug/examples/']", sys_ns, sys_ns);

      crawl_module_ = import("crawler");
      crawl_ns_ = crawl_module_.attr("__dict__");
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }
  }

  void initialize_crawl() {
    the_kernel = this;
    try {
      exec("initialize()\n", crawl_ns_, crawl_ns_);
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }
  }

  void run_crawl() {
    the_kernel = this;
    try {
      LOG(INFO) << "Starting crawler...";
      exec("crawl()\n", crawl_ns_, crawl_ns_);
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }
  }

private:
  object crawl_module_;
  object crawl_ns_;
};
REGISTER_KERNEL(PythonKernel);
REGISTER_METHOD(PythonKernel, initialize_crawl);
REGISTER_METHOD(PythonKernel, run_crawl);

DSMKernel* kernel() {
  return the_kernel;
}

int DomainSharding(const string& in, int num_shards) {
   int d_end = in.find(" ");
 //  LOG(INFO) << "Shard for " << in.substr(0, d_end) << " is "
 //            << SuperFastHash(in.data(), d_end) % num_shards;
   return SuperFastHash(in.data(), d_end) % num_shards;
}

double CrawlerRuntime() {
  return FLAGS_crawler_runtime;
}

extern "C" void init_python_support();
int main(int argc, const char* argv[]) {
  Init(argc, (char**)argv);
  FLAGS_work_stealing = false;

  Py_Initialize();
  PySys_SetArgv(argc, (char**)argv);
  init_python_support();

  ConfigData conf;
  conf.set_num_workers(NetworkThread::Get()->size() - 1);
  conf.set_slots(1);

  // Fetch, crawltime, robots, and counts
  Registry::create_table<string, int>(0, conf.num_workers(), &DomainSharding, &Accumulator<int>::max);
  Registry::create_table<string, int>(1, conf.num_workers(), &StringSharding, &Accumulator<int>::max);
  Registry::create_table<string, string>(2, conf.num_workers(), &DomainSharding, &Accumulator<string>::replace);
  Registry::create_table<string, int>(3, conf.num_workers(), &StringSharding, &Accumulator<int>::sum);
  Registry::create_table<string, int>(4, 1, &StringSharding, &Accumulator<int>::sum);

  if (!StartWorker(conf)) {
    Master m(conf);
    RUN_ALL(m, PythonKernel, initialize_crawl, Registry::get_table(0));
    RUN_ALL(m, PythonKernel, run_crawl, Registry::get_table(0));
  }
}
