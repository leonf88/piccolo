%{
#include "crawler_support.h"
%}

%module crawler_support

%include "std_string.i"

%include "util/common.h"
%include "crawler_support.h"
%include "kernel/table.h"
%include "kernel/table-internal.h"

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

%nodefaultctor CrawlTable;
%nodefaultctor CrawlTable_Iterator;