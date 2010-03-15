#include "client.h"
#include "crawler_support.h"
#include "python2.6/Python.h"

#include <boost/python.hpp>

using namespace dsm;
using namespace boost::python;

static DSMKernel *the_kernel;

class PythonKernel : public DSMKernel {
public:
  PythonKernel() {
    try {
      object sys_module = import("sys");
      object sys_ns = sys_module.attr("__dict__");
      exec("path += ['src/examples/crawler']", sys_ns, sys_ns);
      exec("print path", sys_ns, sys_ns);

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

extern "C" void init_crawler_support();
int main(int argc, const char* argv[]) {
  Init(argc, (char**)argv);

  Py_Initialize();
  init_crawler_support();

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_slots(1);

  // Fetch, crawltime, robots, and counts
  Registry::create_table<string, int>(0, conf.num_workers(), &DomainSharding, &Accumulator<int>::max);
  Registry::create_table<string, int>(1, conf.num_workers(), &StringSharding, &Accumulator<int>::max);
  Registry::create_table<string, string>(2, conf.num_workers(), &DomainSharding, &Accumulator<string>::replace);
  Registry::create_table<string, int>(3, conf.num_workers(), &StringSharding, &Accumulator<int>::sum);

  if (MPI::COMM_WORLD.Get_rank() == 0) {
    Master m(conf);
    RUN_ALL(m, PythonKernel, initialize_crawl, 0);
    RUN_ALL(m, PythonKernel, run_crawl, 0);
  } else {
    Worker w(conf);
    w.Run();
  }
}
