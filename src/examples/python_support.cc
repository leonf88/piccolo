#include "examples/python_support.h"
#include <python2.6/Python.h>

#include "util/common.h"
#include "examples/examples.h"
#include "client.h"
#include "worker/worker.h"
#include "master/master.h"
#include <boost/python.hpp>
#include <gflags/gflags.h>

using namespace dsm;
using namespace boost::python;

DECLARE_bool(work_stealing);
DEFINE_double(crawler_runtime, -1, "Amount of time to run, in seconds.");

static DSMKernel *the_kernel;

int PythonSharding::operator()(const PyObjectPr& k, int shards) {
  PyObjectPr result = PyEval_CallObject(c_, Py_BuildValue("(o,d)", k, shards));
  long r = PyInt_AsLong(result);
  Py_DecRef(result);
  return r;
}

void PythonAccumulate::operator()(PyObjectPr* a, const PyObjectPr& b) {
  PyObjectPr result = PyEval_CallObject(c_, Py_BuildValue("(o,o)", a, b));
  Py_DecRef(*a);
  *a = result;
  Py_DecRef(const_cast<PyObjectPr>(b));
}

class PythonKernel : public DSMKernel {
public:
  PythonKernel() {
    try {
      object sys_module = import("sys");
      object sys_ns = sys_module.attr("__dict__");
      exec("path += ['src/examples/crawler']", sys_ns, sys_ns);
      exec("path += ['bin/release/examples/']", sys_ns, sys_ns);
      exec("path += ['bin/debug/examples/']", sys_ns, sys_ns);

      crawl_module_ = import("crawler");
      crawl_ns_ = crawl_module_.attr("__dict__");
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }
  }

  void run_python_method() {
    the_kernel = this;
    try {
      exec("initialize()\n", crawl_ns_, crawl_ns_);
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
REGISTER_METHOD(PythonKernel, run_python_method);

DSMKernel* kernel() {
  return the_kernel;
}

double CrawlerRuntime() {
  return FLAGS_crawler_runtime;
}
