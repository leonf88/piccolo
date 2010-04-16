#include "util/static-initializers.h"
#include <tr1/unordered_map>
#include <stdio.h>

using namespace std;
using namespace std::tr1;
namespace dsm {

typedef unordered_map<string, StaticInitHelper*> HelperMap;
typedef unordered_map<string, StaticTestHelper*> TestMap;

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
  helpers()->insert(make_pair(name, this));
}

StaticTestHelper::StaticTestHelper(const string& name) {
  tests()->insert(make_pair(name, this));
}

void RunInitializers() {
//  fprintf(stderr, "Running %zd initializers... \n", helpers()->size());
  for (HelperMap::iterator i = helpers()->begin(); i != helpers()->end(); ++i) {
    i->second->Run();
  }
}

void RunTests() {
  for (TestMap::iterator i = tests()->begin(); i != tests()->end(); ++i) {
    i->second->Run();
  }
}


}
