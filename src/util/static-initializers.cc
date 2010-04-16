#include "util/static-initializers.h"
#include <tr1/unordered_map>
#include <stdio.h>

using namespace std;
using namespace std::tr1;
namespace dsm {

typedef unordered_map<string, StaticInitHelper*> HelperMap;

HelperMap* helpers() {
  static HelperMap* h = NULL;
  if (!h) { h = new HelperMap; }
  return h;
}

StaticInitHelper::StaticInitHelper(const string& name) {
  helpers()->insert(make_pair(name, this));
}

void RunInitializers() {
//  fprintf(stderr, "Running %zd initializers... \n", helpers()->size());
  for (HelperMap::iterator i = helpers()->begin(); i != helpers()->end(); ++i) {
    i->second->Run();
  }
}

}
