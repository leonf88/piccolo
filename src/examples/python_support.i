#ifndef PYTHON_SUPPORT_H
#define PYTHON_SUPPORT_H

#ifndef SWIG
#include <google/protobuf/message.h>
#include <mpi.h>

#include "examples.h"
#include "client.h"

using namespace google::protobuf;
using namespace std;
#endif

dsm::DSMKernel* kernel();
  
// Shard based on the domain contained within "in".  This is separated from the
// full url by a space.
int DomainSharding(const string& in, int num_shards);
double CrawlerRuntime();

#ifdef SWIG
%module python_support

%{
#include "examples/python_support.i"
#include <gflags/gflags.h>
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

%include "examples.pb.h"

%include "kernel/kernel.h"
%include "kernel/table.h"
%include "kernel/table-registry.h"

using namespace dsm;
using namespace std;

%typemap(in) long& { $1 = PyInt_AsLong($input); }
%typemap(out) long& { $result = PyInt_FromLong(*$1); }

%typemap(in) long { $1 = PyInt_AsLong($input); }
%typemap(out) long { $result = PyInt_FromLong($1); }

%template(CrawlTable) dsm::TypedGlobalTable<string, int64_t>;
%template(CrawlTable_Iterator) dsm::TypedTable_Iterator<string, int64_t>;

%template(RobotsTable) dsm::TypedGlobalTable<string, string>;
%template(RobotsTable_Iterator) dsm::TypedTable_Iterator<string, string>;

%extend dsm::DSMKernel {
  %template(robots_table) get_table<string, string>;
  %template(crawl_table) get_table<string, int64_t>;
}

#endif

#endif
