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

using namespace google::protobuf;
using namespace std;

namespace dsm {

DSMKernel* kernel();

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

template<class K, class V>
class PythonTrigger : public Trigger<K, V> {
public:
  PythonTrigger(dsm::GlobalTable* thistable, const string& code);
  void Init(dsm::GlobalTable* thistable);
  bool Fire(const K& k, const V& current, V& update);
  bool CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, const V& current, V& update);

  TriggerID trigid;
private:
  MarshalledMap params_;
  boost::python::object crawl_module_;
  boost::python::object crawl_ns_;
};


#endif
}
