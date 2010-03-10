#ifndef CRAWLER_SUPPORT_H
#define CRAWLER_SUPPORT_H

#ifdef SWIG
%module crawler_support

%{
#include "client.h"
#include "crawler_support.h"
%}

%include "std_string.i"

%include "util/common.h"
%include "crawler_support.h"
%include "kernel/table.h"
%include "kernel/table-internal.h"
%include "kernel/kernel-registry.h"

using namespace dsm;
using namespace std;

%typemap(in) int& {
  $1 = PyInt_AsLong($input);
}

%typemap(out) int& {
  $result = PyInt_FromLong(*$1);
}
#endif

#include "client.h"

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
