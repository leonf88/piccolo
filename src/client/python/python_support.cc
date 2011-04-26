#include "glog/logging.h"
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

template<class K, class V>
PythonTrigger<K, V>::PythonTrigger(dsm::GlobalTable* thistable, const string& code) {
  Init(thistable);
  params_.put("python_code", code);
  trigid = thistable->register_trigger(this);
}

template<class K, class V>
void PythonTrigger<K, V>::Init(dsm::GlobalTable* thistable) {
  try {
    object sys_module = import("sys");
    object sys_ns = sys_module.attr("__dict__");
    crawl_module_ = import("crawler");
    crawl_ns_ = crawl_module_.attr("__dict__");
  } catch (error_already_set& e) {
    PyErr_Print();
    exit(1);
  }
}

template<class K, class V>
bool PythonTrigger<K, V>::Fire(const K& k, const V& current, V& update) {
  string python_code = params_.get<string> ("python_code");
  PyObject *key, *callable;
  callable = PyObject_GetAttrString(crawl_module_.ptr(), python_code.c_str());
  key = PyString_FromString(k.c_str());

  // Make sure all the callfunctionobjarg arguments are fine
  if (key == NULL || callable == NULL) {
    LOG(ERROR) << "Failed to launch trigger " << python_code << "!";
    if (key == NULL) LOG(ERROR) << "[FAIL] key was null";
    if (callable == NULL) LOG(ERROR) << "[FAIL] callable was null";
    return true;
  }

  bool rv = PythonTrigger<K, V>::CallPythonTrigger(callable, key, current, update);
  printf("returning %s from trigger\n",rv?"TRUE":"FALSE");
  return rv;
}

template<class K, class V>
bool PythonTrigger<K, V>::CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, const V& current, V& update) {
  LOG(FATAL) << "No such CallPythonTrigger for this key/value pair type!";
  exit(1);
}

template<>
bool PythonTrigger<string, int64_t>::CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, const int64_t& current, int64_t& update) {
  PyObjectPtr retval;

  PyObject* cur_obj = PyLong_FromLongLong(current);
  PyObject* upd_obj = PyLong_FromLongLong(update);

  if (cur_obj == NULL || upd_obj == NULL) {
    LOG(ERROR) << "Failed to bootstrap <string,string> trigger launch";
    return true;
  }
  try {
    retval = PyObject_CallFunctionObjArgs(
		callable, key, cur_obj, upd_obj, NULL);
    Py_DECREF(callable);
  } catch (error_already_set& e) {
    PyErr_Print();
    exit(1);
  }

  update = PyLong_AsLongLong(upd_obj);

  return (retval == Py_True);
}

template<>
bool PythonTrigger<string, string>::CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, const string& current, string& update) {
  PyObjectPtr retval;
 
  PyObject* cur_obj = PyString_FromString(current.c_str());
  PyObject* upd_obj = PyString_FromString(update.c_str());

  if (cur_obj == NULL || upd_obj == NULL) {
    LOG(ERROR) << "Failed to bootstrap <string,string> trigger launch";
    return true;
  }
  try {
    retval = PyObject_CallFunctionObjArgs(
		callable, key, cur_obj, upd_obj, NULL);
    Py_DECREF(callable);
  } catch (error_already_set& e) {
    PyErr_Print();
    exit(1);
  }

  update = PyString_AsString(upd_obj);

  return (retval == Py_True);
}

class PythonKernel: public DSMKernel {
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
    string python_code = get_arg<string> ("python_code");
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
REGISTER_KERNEL(PythonKernel)
;
REGISTER_METHOD(PythonKernel, run_python_code)
;

template class PythonTrigger<string, string>;
template class PythonTrigger<string, int64_t>;

}
