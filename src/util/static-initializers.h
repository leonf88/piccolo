#ifndef STATIC_INITIALIZERS_H
#define STATIC_INITIALIZERS_H

#include <string>

namespace dsm {

void RunInitializers();

struct StaticInitHelper {
  StaticInitHelper(const std::string& name);
  virtual void Run() = 0;
};

}

#define REGISTER_INTIALIZER(name, code)\
  struct name ## StaticInitHelper : public dsm::StaticInitHelper {\
    name ## StaticInitHelper() : StaticInitHelper(#name) {}\
    void Run() {\
    code \
  }\
  };\
static name ## StaticInitHelper name ## helper;
#endif
