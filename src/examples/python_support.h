#ifndef PYTHON_SUPPORT_H
#define PYTHON_SUPPORT_H

#include <google/protobuf/message.h>
#include "util/common.pb.h"
#include "examples.h"
#include "client.h"

using namespace google::protobuf;
using namespace std;

dsm::DSMKernel* kernel();

// Shard based on the domain contained within "in".  This is separated from the
// full url by a space.
static int DomainSharding(const string& in, int num_shards) {
  int d_end = in.find(" ");
//  LOG(INFO) << "Shard for " << in.substr(0, d_end) << " is "
//            << SuperFastHash(in.data(), d_end) % num_shards;
  return SuperFastHash(in.data(), d_end) % num_shards;
}

#ifdef SWIG
%module python_support

%{
#include "python_support.h"
%}

#define GOOGLE_PROTOBUF_VERSION 2003000
#define LIBPROTOBUF_EXPORT

typedef google::protobuf::int32_t int32_t;

%include "std_string.i"

%include "google/protobuf/message_lite.h"

%include "util/common.pb.h"
%include "examples.pb.h"

%include "util/common.h"
%include "util/file.h"
%include "python_support.h"
%include "kernel/table.h"
%include "kernel/kernel-registry.h"

using namespace dsm;
using namespace std;

%typemap(in) int& {
  $1 = PyInt_AsLong($input);
}

%typemap(out) int& {
  $result = PyInt_FromLong(*$1);
}

%template(CrawlTable) dsm::TypedGlobalTable<string, int>;
%template(CrawlTable_Iterator) dsm::TypedTable_Iterator<string, int>;

%template(RobotsTable) dsm::TypedGlobalTable<string, string>;
%template(RobotsTable_Iterator) dsm::TypedTable_Iterator<string, string>;

%extend dsm::DSMKernel {
  %template(robots_table) get_table<string, string>;
  %template(crawl_table) get_table<string, int>;
}
#endif

#endif
