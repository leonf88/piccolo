#include <python2.6/Python.h>
#include <boost/python.hpp>

#include <gflags/gflags.h>
#include "client/client.h"
#include "examples/examples.h"
#include "examples/examples.pb.h"
#include "util/common.h"
#include "worker/worker.h"
#include "kernel/table.h"
#include "master/master.h"

using namespace boost::python;
using namespace google::protobuf;
using namespace std;


namespace dsm {

DSMKernel* kernel();
template <class K, class V>
struct TriggerDescriptor : public Trigger<K,V> {
public:
  GlobalTable *table;
  MarshalledMap params;
  TriggerID trigid;

  TriggerDescriptor() {
    Init(-1);
  }
  TriggerDescriptor(GlobalTable* thistable) {
    Init(thistable);
  }

  void Init(GlobalTable* thistable) {
    try {
      object sys_module = import("sys");
      object sys_ns = sys_module.attr("__dict__");
//      exec("path += ['src/examples/crawler']", sys_ns, sys_ns);
//      exec("path += ['bin/release/examples/']", sys_ns, sys_ns);
//      exec("path += ['bin/debug/examples/']", sys_ns, sys_ns);

      crawl_module_ = import("crawler");
      crawl_ns_ = crawl_module_.attr("__dict__");
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }

    // Trigger setup
    table = thistable;
    trigid = table->register_trigger(this);
  }

  bool Fire(const K& k, const V& current, V& update) {
    string python_code = params.get<string>("python_code");
    LOG(INFO) << "Executing Python trigger: " << python_code;
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

double crawler_runtime();
bool crawler_triggers();

#ifndef SWIG
// gcc errors out if we don't use this hack - complaining about
// operator() not being implemented.  sigh.
typedef PyObject* PyObjectPtr;

struct PythonSharder : public Sharder<string> {
  PythonSharder(PyObjectPtr callback) : c_(callback) {}
  int operator()(const string& k, int shards);
private:
  PyObjectPtr c_;
};

struct PythonAccumulate : public Accumulator<PyObjectPtr> {
  PythonAccumulate(PyObjectPtr callback) : c_(callback) {}
  void Accumulate(PyObjectPtr* a, const PyObjectPtr& b);
private:
  PyObjectPtr c_;
};

struct PythonMarshal : public Marshal<PyObjectPtr> {
  PythonMarshal(PyObjectPtr pickle, PyObjectPtr unpickle) :
    pickler_(pickle), unpickler_(unpickle) {}
  void marshal(const PyObjectPtr& t, string *out) {
    PyObjectPtr result = PyEval_CallFunction(pickler_, "O", t);
    if (PyErr_Occurred()) {
      PyErr_Print();
      exit(1);
    }
    *out = PyString_AsString(result);
    Py_DecRef(result);
    return;
  }

  void unmarshal(const StringPiece& s, PyObjectPtr* t) {
    PyObjectPtr result = PyEval_CallFunction(unpickler_, "s#", s.data, s.len);
    if (PyErr_Occurred()) {
      PyErr_Print();
      exit(1);
    }
    *t = result;
  }
  PyObjectPtr pickler_;
  PyObjectPtr unpickler_;
};

#endif
}
