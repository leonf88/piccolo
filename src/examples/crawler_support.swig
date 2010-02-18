%{
#include "crawler_support.h"
%}

%module crawler_support

%nodefaultctor CrawlTable_Iterator;
%nodefaultctor CrawlTable;
%include "std_string.i"
%include "crawler_support.h"
