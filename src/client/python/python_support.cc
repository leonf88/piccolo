#include "python_support.h"
#include <boost/python.hpp>

using namespace boost::python;
using namespace google::protobuf;
using namespace std;

DEFINE_double(crawler_runtime, -1, "Amount of time to run, in seconds.");
DEFINE_bool(crawler_triggers, false, "Use trigger-based crawler (t/f).");
static DSMKernel *the_kernel;

namespace dsm {

DSMKernel* kernel() {
  return the_kernel;
}

double crawler_runtime() {
  return FLAGS_crawler_runtime;
}

bool crawler_triggers() {
  return FLAGS_crawler_triggers;
}

int PythonSharder::operator()(const string& k, int shards) {
  PyObjectPtr result = PyEval_CallFunction(c_, "(si)", k.c_str(), shards);
  if (PyErr_Occurred()) {
    PyErr_Print();
    exit(1);
  }

  long r = PyInt_AsLong(result);
//  Py_DecRef(result);
  return r;
}

void PythonAccumulate::Accumulate(PyObjectPtr* a, const PyObjectPtr& b) {
  PyObjectPtr result = PyEval_CallFunction(c_, "OO", *a, b);
  if (PyErr_Occurred()) {
    PyErr_Print();
    exit(1);
  }

//  Py_DecRef(*a);
  *a = result;
//  Py_DecRef(const_cast<PyObjectPtr>(b));
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

  void run_python_code() {
    the_kernel = this;
    string python_code = get_arg<string>("python_code");
    LOG(INFO) << "Executing python code: " << python_code;
    try {
      exec(StringPrintf("%s\n", python_code.c_str()).c_str(), crawl_ns_, crawl_ns_);
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
REGISTER_METHOD(PythonKernel, run_python_code);

}
