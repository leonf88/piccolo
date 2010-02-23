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

DSMKernel* kernel();

%template(CrawlTable) dsm::TypedGlobalTable<string, int>;
%template(CrawlTable_Iterator) dsm::TypedTable_Iterator<string, int>;

%template(RobotsTable) dsm::TypedGlobalTable<string, string>;
%template(RobotsTable_Iterator) dsm::TypedTable_Iterator<string, string>;

%extend dsm::DSMKernel {
  %template(robots_table) get_table<string, string>;
  %template(crawl_table) get_table<string, int>;
}

#else
#include "client.h"
using namespace std;
dsm::DSMKernel* kernel();
#endif
