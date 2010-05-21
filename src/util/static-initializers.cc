#include "util/static-initializers.h"
#include "util/hashmap.h"

#include <stdio.h>
#include <gflags/gflags.h>


using namespace std;
using namespace std::tr1;
namespace dsm {

typedef HashMap<string, StaticInitHelper*> HelperMap;
typedef HashMap<string, StaticTestHelper*> TestMap;

HelperMap* helpers() {
  static HelperMap* h = NULL;
  if (!h) { h = new HelperMap; }
  return h;
}

TestMap* tests() {
  static TestMap* h = NULL;
  if (!h) { h = new TestMap; }
  return h;
}

StaticInitHelper::StaticInitHelper(const string& name) {
  helpers()->put(name, this);
}

StaticTestHelper::StaticTestHelper(const string& name) {
  tests()->put(name, this);
}

void RunInitializers() {
//  fprintf(stderr, "Running %zd initializers... \n", helpers()->size());
  for (HelperMap::iterator i = helpers()->begin(); i != helpers()->end(); ++i) {
    i->second->Run();
  }
}

void RunTests() {
  fprintf(stderr, "Starting tests...\n");
  int c = 1;
  for (TestMap::iterator i = tests()->begin(); i != tests()->end(); ++i) {
    fprintf(stderr, "Running test %5d/%5d: %s\n", c, tests()->size(), i->first.c_str());
    i->second->Run();
    ++c;
  }
  fprintf(stderr, "Done.\n");
}


}
