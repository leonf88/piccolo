#ifndef STATIC_INITIALIZERS_H
#define STATIC_INITIALIZERS_H

#include <string>

namespace dsm {

void RunInitializers();
void RunTests();

struct StaticInitHelper {
  StaticInitHelper(const std::string& name);
  virtual void Run() = 0;
};

struct StaticTestHelper {
  StaticTestHelper(const std::string& name);
  virtual void Run() = 0;
};

}

#define REGISTER_INITIALIZER(name, code)\
  struct name ## StaticInitHelper : public dsm::StaticInitHelper {\
    name ## StaticInitHelper() : StaticInitHelper(#name) {}\
    void Run() {\
    code; \
  }\
  };\
static name ## StaticInitHelper name ## helper;

#define REGISTER_TEST(name, code)\
  struct name ## StaticTestHelper : public dsm::StaticTestHelper {\
    name ## StaticTestHelper() : StaticTestHelper(#name) {}\
    void Run() {\
    code; \
  }\
  };\
static name ## StaticTestHelper name ## helper;

#endif
