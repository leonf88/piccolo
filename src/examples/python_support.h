#ifndef PYTHON_SUPPORT_H
#define PYTHON_SUPPORT_H

#include "client.h"
#include "kernel/table.h"
#include "master/master.h"
#include "worker/worker.h"
#include "examples/examples.pb.h"
#include <python2.6/Python.h>

using namespace dsm;
using namespace google::protobuf;
using namespace std;

dsm::DSMKernel* kernel();
double CrawlerRuntime();

#ifndef SWIG
// gcc errors out if we don't use this back - complaining about
// operator() not being implemented.  sigh.
typedef PyObject* PyObjectPr;

struct PythonSharding : public Sharder<PyObjectPr> {
  PythonSharding(PyObjectPr callback) : c_(callback) {}
  int operator()(const PyObjectPr& k, int shards);
private:
  PyObjectPr c_;
};

struct PythonAccumulate : public Accumulator<PyObjectPr> {
  PythonAccumulate(PyObjectPr callback) : c_(callback) {}
  void operator()(PyObjectPr* a, const PyObjectPr& b);
private:
  PyObjectPr c_;
};

#endif

#endif
