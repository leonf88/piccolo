%module piccolo

%{
#include "examples/python_support.h"
%}

#define GOOGLE_PROTOBUF_VERSION 2003000
#define LIBPROTOBUF_EXPORT

%include "google/protobuf/message.h"

typedef google::protobuf::int32_t int32_t;
typedef long int64_t;

%include "std_string.i"

%include "util/common.pb.h"
%include "util/common.h"
%include "util/file.h"
%include "util/rpc.h"

%include "examples/examples.pb.h"
%include "worker/worker.pb.h"

%include "kernel/kernel.h"
%include "kernel/table.h"
%include "kernel/table-registry.h"

%include "master/master.h"
%include "worker/worker.h"
%include "examples/python_support.h"

using namespace dsm;
using namespace std;

%typemap(in) long& { $1 = PyInt_AsLong($input); }
%typemap(out) long& { $result = PyInt_FromLong(*$1); }

%typemap(in) long { $1 = PyInt_AsLong($input); }
%typemap(out) long { $result = PyInt_FromLong($1); }

%template(PythonTable) dsm::TypedGlobalTable<string, PyObject*>;
%template(PythonTable_Iterator) dsm::TypedIterator<string, PyObject*>;

%extend dsm::DSMKernel {
  %template(PythonTable) get_table<string, PyObject*>;
}

%extend dsm::TableRegistry {
  GlobalView* CreateTable(int id, int shards, PyObject* sharding, PyObject* accum) {
    return $self->create_table<string, PyObject*>(
        id, shards,
        new PythonSharding(sharding), new PythonAccumulate(accum));
  }
}