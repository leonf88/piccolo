#include <python2.6/Python.h>

#include <gflags/gflags.h>
#include "client/client.h"
#include "examples/examples.h"
#include "examples/examples.pb.h"
#include "kernel/table.h"
#include "master/master.h"
#include "util/common.h"
#include "worker/worker.h"

using namespace google::protobuf;
using namespace std;


namespace dsm {

DSMKernel* kernel();
double crawler_runtime();

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
